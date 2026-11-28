package node

import (
	"context"
	stdnet "net"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/common/rate"
	"github.com/npanel-dev/npnode/core/app/dispatcher"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"github.com/xtls/xray-core/common"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
)

// openUserLink 经内核的 dispatcher 为用户建一条真实链路（出站连到本地一个只接受、不关闭的监听），返回链路数查询函数
func openUserLink(t *testing.T, c *Controller, uuid, ip string) func() int {
	t.Helper()
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()
	d := c.server.Server.GetFeature(routing.DispatcherType()).(*dispatcher.DefaultDispatcher)
	email := format.UserTag(c.tag, uuid)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Tag:    c.tag,
		User:   &protocol.MemoryUser{Email: email},
		Source: xnet.TCPDestination(xnet.ParseAddress(ip), 50000),
	})
	link, err := d.Dispatch(ctx, xnet.TCPDestination(xnet.LocalHostIP, xnet.Port(ln.Addr().(*stdnet.TCPAddr).Port)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { common.Close(link.Writer) })
	count := func() int {
		v, ok := d.LinkManagers.Load(email)
		if !ok {
			return 0
		}
		return v.(*dispatcher.LinkManager).Len()
	}
	time.Sleep(50 * time.Millisecond) // 等出站建立
	if count() != 1 {
		t.Fatalf("link not established: %d", count())
	}
	return count
}

type allowChecker struct{}

func (allowChecker) Check(context.Context, panel.SessionRequest) (*panel.SessionResult, error) {
	return &panel.SessionResult{Allowed: true}, nil
}
func (allowChecker) Release(context.Context, panel.SessionRequest) error { return nil }

func withNodeAdmission(t *testing.T, on bool) {
	t.Helper()
	panel.SetSeatV1(true)
	panel.SetAdmissionV1(on)
	if on {
		limiter.EnsureAdmission(limiter.AdmissionConfig{Checker: allowChecker{}, ServerID: 1})
	}
	t.Cleanup(func() {
		limiter.DisableAdmission()
		panel.SetAdmissionV1(false)
		panel.SetSeatV1(false)
	})
}

// bucketRef 用户当前的 bucket 间接层（连接建立时创建）；没有返回 nil
func bucketRef(c *Controller, uuid string) *rate.Bucket {
	v, ok := c.limiter.SpeedLimiter.Load(format.UserTag(c.tag, uuid))
	if !ok {
		return nil
	}
	return v.(*rate.Bucket)
}

func userOf(c *Controller, id int) (panel.UserInfo, int) {
	c.userMu.Lock()
	defer c.userMu.Unlock()
	n := 0
	var out panel.UserInfo
	for _, u := range c.userList {
		if u.Id == id {
			out = u
			n++
		}
	}
	return out, n
}

// U5（admission_v1）：全量对齐中同一 id+uuid 只有限速 / 设备数变化 → 原地更新，不删用户（bucket 间接层保持同一个，已有连接立即按新速率）
func TestFullSyncUpdatesLimitInPlace(t *testing.T) {
	withNodeAdmission(t, true)
	fp := &fakePanel{}
	fp.setUsers([]panel.UserInfo{{Id: 1000000045, Uuid: isoA, SpeedLimit: 524288}, {Id: 2, Uuid: isoB}})
	c := startIsolationController(t, fp)
	if c.limiter.Admission == nil {
		t.Fatal("limiter must attach admission")
	}
	key := format.UserTag(c.tag, isoA)
	links := openUserLink(t, c, isoA, "203.0.113.7") // 一条真实的已有连接
	ref := bucketRef(c, isoA)
	if ref == nil || ref.Capacity() != 65536 {
		t.Fatalf("ref = %v", ref)
	}
	fp.setUsers([]panel.UserInfo{{Id: 1000000045, Uuid: isoA, SpeedLimit: 4194304, DeviceLimit: 1}, {Id: 2, Uuid: isoB}})
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bucketRef(c, isoA) != ref {
		t.Fatal("user was deleted and re-added: bucket ref replaced")
	}
	if links() != 1 {
		t.Fatal("existing connection was closed")
	}
	if ref.Capacity() != 524288 {
		t.Fatalf("existing connection rate = %d, want 524288", ref.Capacity())
	}
	u, n := userOf(c, 1000000045)
	if n != 1 || u.SpeedLimit != 4194304 || u.DeviceLimit != 1 {
		t.Fatalf("userList entry = %+v (count %d)", u, n)
	}
	v, _ := c.limiter.UserLimitInfo.Load(key)
	if info := v.(*limiter.UserLimitInfo); info.DeviceLimit != 1 || info.SpeedLimit != 4194304 {
		t.Fatalf("UserLimitInfo = %+v", info)
	}
	// 再一次相同的全量：没有变化
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, n := userOf(c, 1000000045); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

// U5（admission_v1）：DIFF 中同一 id 先 removed（旧内容）后 users（新内容）且只改设备数 → 原地更新；uuid 变化（重置名额）仍删旧加新
func TestDiffUpdatesLimitInPlace(t *testing.T) {
	withNodeAdmission(t, true)
	fp := &fakePanel{}
	old := panel.UserInfo{Id: 1000000045, Uuid: isoA, SpeedLimit: 524288, DeviceLimit: 1}
	fp.setUsers([]panel.UserInfo{old, {Id: 2, Uuid: isoB}})
	c := startIsolationController(t, fp)
	links := openUserLink(t, c, isoA, "203.0.113.7")
	ref := bucketRef(c, isoA)
	c.userMu.Lock()
	c.syncVersion = 1
	c.userMu.Unlock()
	nu := old
	nu.DeviceLimit = 0
	c.ApplyUserUpdate(panel.UserUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Version: 2,
		Removed: []panel.UserInfo{old}, Users: []panel.UserInfo{nu}})
	if bucketRef(c, isoA) != ref || links() != 1 {
		t.Fatal("device-only change must not delete the user or close its connection")
	}
	if u, n := userOf(c, 1000000045); n != 1 || u.DeviceLimit != 0 {
		t.Fatalf("entry = %+v count %d", u, n)
	}
	// 重置名额：同一 id、新 uuid → 删旧加新
	reset := panel.UserInfo{Id: 1000000045, Uuid: isoC, SpeedLimit: 524288}
	c.ApplyUserUpdate(panel.UserUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Version: 3,
		Removed: []panel.UserInfo{nu}, Users: []panel.UserInfo{reset}})
	if bucketRef(c, isoA) != nil {
		t.Fatal("old uuid must be removed")
	}
	if u, n := userOf(c, 1000000045); n != 1 || u.Uuid != isoC {
		t.Fatalf("entry = %+v count %d", u, n)
	}
	if v, _, _ := controllerState(c); v != 3 {
		t.Fatalf("version = %d", v)
	}
}

// 未声明 admission_v1：保持“删旧加新”
func TestLimitChangeStillReplacesWithoutAdmission(t *testing.T) {
	withNodeAdmission(t, false)
	fp := &fakePanel{}
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA, SpeedLimit: 524288}})
	c := startIsolationController(t, fp)
	links := openUserLink(t, c, isoA, "203.0.113.7")
	ref := bucketRef(c, isoA)
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA, SpeedLimit: 4194304}})
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := bucketRef(c, isoA); got == ref {
		t.Fatal("without admission_v1 a limit change must delete and re-add the user")
	}
	if links() != 0 {
		t.Fatal("without admission_v1 delete+add closes existing connections (main behavior)")
	}
	if u, n := userOf(c, 1); n != 1 || u.SpeedLimit != 4194304 {
		t.Fatalf("entry = %+v count %d", u, n)
	}
}
