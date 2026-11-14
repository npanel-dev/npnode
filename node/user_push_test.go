package node

import (
	"context"
	"net"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
)

func entry(id int, uuid string, device int) *nodev1.UserEntry {
	return &nodev1.UserEntry{Id: int64(id), Uuid: uuid, DeviceLimit: int64(device)}
}

// pushPanel 假面板：GetUserList 返回初始用户；Sync 里由测试驱动推送，并记录节点发来的订阅。
type pushPanel struct {
	nodev1.UnimplementedNodeServiceServer
	initial []*nodev1.UserEntry

	mu         sync.Mutex
	stream     nodev1.NodeService_SyncServer
	subscribes []*nodev1.Subscribe
	ready      chan struct{}
}

func (p *pushPanel) GetUserList(context.Context, *nodev1.GetUserListRequest) (*nodev1.GetUserListResponse, error) {
	return &nodev1.GetUserListResponse{Users: p.initial}, nil
}

func (p *pushPanel) Sync(stream nodev1.NodeService_SyncServer) error {
	p.mu.Lock()
	p.stream = stream
	p.mu.Unlock()
	if err := stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 6}}}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if sub := req.GetSubscribe(); sub != nil {
			p.mu.Lock()
			p.subscribes = append(p.subscribes, sub)
			first := len(p.subscribes) == 1
			p.mu.Unlock()
			if first {
				close(p.ready)
			}
		}
	}
}

func (p *pushPanel) push(t *testing.T, u *nodev1.UserListUpdate) {
	t.Helper()
	u.Protocol = "shadowsocks"
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_UserUpdate{UserUpdate: u}}); err != nil {
		t.Fatal(err)
	}
}

func (p *pushPanel) subscribeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subscribes)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func controllerState(c *Controller) (version uint64, ids []int, devices map[int]int) {
	c.userMu.Lock()
	defer c.userMu.Unlock()
	devices = map[int]int{}
	for _, u := range c.userList {
		ids = append(ids, u.Id)
		devices[u.Id] = u.DeviceLimit
	}
	sort.Ints(ids)
	return c.syncVersion, ids, devices
}

func idsEqual(got []int, want ...int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// 端到端：真实 Xray 内核 + 假面板。验证自动订阅、全量 / 差异应用、版本断档重新订阅、设备数变化生效。
func TestPushedUserUpdatesApplyToCore(t *testing.T) {
	limiter.Init()
	httpPanel := httptest.NewServer(&fakePanel{})
	defer httpPanel.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pp := &pushPanel{initial: []*nodev1.UserEntry{entry(1, "11111111-1111-1111-1111-111111111111", 0)}, ready: make(chan struct{})}
	gsrv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(gsrv, pp)
	go func() { _ = gsrv.Serve(lis) }()
	defer gsrv.Stop()

	gc, err := panel.NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "k", 6)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	api, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: httpPanel.URL, NodeID: 6, SecretKey: "k", NodeType: "shadowsocks"})
	if err != nil {
		t.Fatal(err)
	}

	xcore := vCore.New(conf.New(), nil)
	if err := xcore.Start(&panel.ServerConfigResponse{Data: &panel.Data{}}); err != nil {
		t.Fatal(err)
	}
	defer xcore.Close()

	port := freeTCPPort(t)
	info := &panel.NodeInfo{Id: 6, Type: "shadowsocks", PullInterval: 3600, PushInterval: 3600,
		Protocol: &panel.Protocol{Type: "shadowsocks", Port: port, Cipher: "aes-128-gcm", Enable: true}}
	c := NewController(xcore, api, info)
	c.SetGrpcClient(gc)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n := &Node{controllers: []*Controller{c}}
	gc.StartSync(context.Background(), n)

	select {
	case <-pp.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("node did not subscribe after the panel's Hello")
	}
	pp.mu.Lock()
	in := pp.subscribes[0].GetInbounds()
	pp.mu.Unlock()
	if len(in) != 1 || in[0].GetProtocol() != "shadowsocks" || int(in[0].GetPort()) != port {
		t.Fatalf("subscribed inbounds = %v, want shadowsocks:%d", in, port)
	}
	p := uint32(port)

	u1 := entry(1, "11111111-1111-1111-1111-111111111111", 0)
	u2 := entry(2, "22222222-2222-2222-2222-222222222222", 0)
	u3 := entry(3, "33333333-3333-3333-3333-333333333333", 0)
	u4 := entry(4, "44444444-4444-4444-4444-444444444444", 0)

	pp.push(t, &nodev1.UserListUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_FULL, Port: p, Version: 1, Users: []*nodev1.UserEntry{u1, u2}})
	waitFor(t, "FULL v1", func() bool { v, ids, _ := controllerState(c); return v == 1 && idsEqual(ids, 1, 2) })

	pp.push(t, &nodev1.UserListUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Port: p, Version: 2, Users: []*nodev1.UserEntry{u3}, Removed: []*nodev1.UserEntry{u1}})
	waitFor(t, "DIFF v2", func() bool { v, ids, _ := controllerState(c); return v == 2 && idsEqual(ids, 2, 3) })
	if _, ok := c.limiter.UserLimitInfo.Load(format.UserTag(c.tag, u1.Uuid)); ok {
		t.Fatal("removed user still in limiter")
	}

	// 版本断档：v4 不能接在 v2 上，节点应丢弃并重新订阅
	pp.push(t, &nodev1.UserListUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Port: p, Version: 4, Users: []*nodev1.UserEntry{u4}})
	waitFor(t, "resubscribe after version gap", func() bool { return pp.subscribeCount() == 2 })
	if _, ids, _ := controllerState(c); !idsEqual(ids, 2, 3) {
		t.Fatalf("out-of-order DIFF must not be applied, users = %v", ids)
	}
	pp.push(t, &nodev1.UserListUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_FULL, Port: p, Version: 5, Users: []*nodev1.UserEntry{u2, u3, u4}})
	waitFor(t, "FULL v5", func() bool { v, ids, _ := controllerState(c); return v == 5 && idsEqual(ids, 2, 3, 4) })

	// 只改设备数：必须删旧加新（原先只比 uuid + 限速，不会生效）
	u2dev := entry(2, u2.Uuid, 5)
	pp.push(t, &nodev1.UserListUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Port: p, Version: 6, Users: []*nodev1.UserEntry{u2dev}, Removed: []*nodev1.UserEntry{u2}})
	waitFor(t, "device limit change", func() bool { v, _, dev := controllerState(c); return v == 6 && dev[2] == 5 })
	if v, ok := c.limiter.UserLimitInfo.Load(format.UserTag(c.tag, u2.Uuid)); !ok || v.(*limiter.UserLimitInfo).DeviceLimit != 5 {
		t.Fatalf("limiter device limit not updated: %v", v)
	}

	// 推送正常时，定时拉取只做低频校对：此时不应去拉取
	c.userMu.Lock()
	healthy := c.pushHealthyLocked()
	c.userMu.Unlock()
	if !healthy {
		t.Fatal("push should be healthy after FULL")
	}
}

func TestDiffUserListDetectsDeviceLimitChange(t *testing.T) {
	old := []panel.UserInfo{{Id: 1, Uuid: "a", DeviceLimit: 0}}
	nu := []panel.UserInfo{{Id: 1, Uuid: "a", DeviceLimit: 3}}
	deleted, added := compareUserList(old, nu)
	if len(deleted) != 1 || len(added) != 1 || added[0].DeviceLimit != 3 {
		t.Fatalf("device limit change must be delete+add, got deleted=%v added=%v", deleted, added)
	}
}
