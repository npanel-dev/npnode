package node

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
)

// 端到端：面板把 FULL / DIFF 拆成多片推送，节点收齐后一次性应用；缺片时作废本地版本并重新订阅
func TestChunkedPushIsReassembledBeforeApplying(t *testing.T) {
	limiter.Init()
	httpPanel := httptest.NewServer(&fakePanel{})
	defer httpPanel.Close()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pp := &pushPanel{ready: make(chan struct{})}
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
		t.Fatal("node did not subscribe")
	}
	p := uint32(port)
	u1 := entry(1, "11111111-1111-1111-1111-111111111111", 0)
	u2 := entry(2, "22222222-2222-2222-2222-222222222222", 0)
	u3 := entry(3, "33333333-3333-3333-3333-333333333333", 0)
	u4 := entry(4, "44444444-4444-4444-4444-444444444444", 0)
	u5 := entry(5, "55555555-5555-5555-5555-555555555555", 0)

	push := func(action nodev1.UserUpdateAction, version uint64, no, total uint32, users, removed []*nodev1.UserEntry) {
		pp.push(t, &nodev1.UserListUpdate{Action: action, Port: p, Version: version, ChunkNo: no, ChunkTotal: total, Users: users, Removed: removed})
	}
	// FULL v3 分 3 片
	push(nodev1.UserUpdateAction_USER_UPDATE_FULL, 3, 1, 3, []*nodev1.UserEntry{u1}, nil)
	push(nodev1.UserUpdateAction_USER_UPDATE_FULL, 3, 2, 3, []*nodev1.UserEntry{u2}, nil)
	push(nodev1.UserUpdateAction_USER_UPDATE_FULL, 3, 3, 3, []*nodev1.UserEntry{u3}, nil)
	waitFor(t, "chunked FULL v3", func() bool { v, ids, _ := controllerState(c); return v == 3 && idsEqual(ids, 1, 2, 3) })

	// DIFF v4 分 2 片：第 1 片只有 removed，第 2 片是 users
	push(nodev1.UserUpdateAction_USER_UPDATE_DIFF, 4, 1, 2, nil, []*nodev1.UserEntry{u1})
	push(nodev1.UserUpdateAction_USER_UPDATE_DIFF, 4, 2, 2, []*nodev1.UserEntry{u4}, nil)
	waitFor(t, "chunked DIFF v4", func() bool { v, ids, _ := controllerState(c); return v == 4 && idsEqual(ids, 2, 3, 4) })

	// DIFF v5 缺第 2 片：不应用、本地版本作废、重新订阅
	subs := pp.subscribeCount()
	push(nodev1.UserUpdateAction_USER_UPDATE_DIFF, 5, 1, 3, nil, []*nodev1.UserEntry{u2})
	push(nodev1.UserUpdateAction_USER_UPDATE_DIFF, 5, 3, 3, []*nodev1.UserEntry{u5}, nil)
	waitFor(t, "resubscribe after missing chunk", func() bool { return pp.subscribeCount() == subs+1 })
	if v, ids, _ := controllerState(c); v != 0 || !idsEqual(ids, 2, 3, 4) {
		t.Fatalf("incomplete DIFF must not be applied and must reset version: v=%d users=%v", v, ids)
	}
	pp.mu.Lock()
	sub := pp.subscribes[len(pp.subscribes)-1].GetInbounds()
	pp.mu.Unlock()
	if len(sub) != 1 || int(sub[0].GetPort()) != port {
		t.Fatalf("resubscribed inbounds = %v", sub)
	}
}
