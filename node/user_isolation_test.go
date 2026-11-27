package node

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

func startIsolationController(t *testing.T, fp *fakePanel) *Controller {
	t.Helper()
	limiter.Init()
	srv := httptest.NewServer(fp)
	t.Cleanup(srv.Close)
	api, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeID: 1, SecretKey: "k", NodeType: "shadowsocks"})
	if err != nil {
		t.Fatal(err)
	}
	core := vCore.New(conf.New(), nil)
	if err := core.Start(&panel.ServerConfigResponse{Data: &panel.Data{}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { core.Close() })
	info := &panel.NodeInfo{Id: 1, Type: "shadowsocks", PullInterval: 3600, PushInterval: 3600,
		Protocol: &panel.Protocol{Type: "shadowsocks", Port: freeTCPPort(t), Cipher: "aes-128-gcm", Enable: true}}
	c := NewController(core, api, info)
	if err := c.Start(); err != nil {
		t.Fatalf("Start must not fail because of a single invalid user: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func listIDs(c *Controller) []int {
	c.userMu.Lock()
	defer c.userMu.Unlock()
	out := make([]int, 0, len(c.userList))
	for _, u := range c.userList {
		out = append(out, u.Id)
	}
	sort.Ints(out)
	return out
}

const (
	isoA = "11111111-1111-1111-1111-111111111111"
	isoB = "22222222-2222-2222-2222-222222222222"
	isoC = "33333333-3333-3333-3333-333333333333"
)

// U3：启动与同步中单条无效只跳过这一条，本地用户集合与内核一致，修正后下一轮可以加入
func TestSingleInvalidUserDoesNotBlockOthers(t *testing.T) {
	fp := &fakePanel{}
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, {Id: 0, Uuid: isoB}, {Id: 3, Uuid: isoC}})
	c := startIsolationController(t, fp)
	if got := listIDs(c); !idsEqual(got, 1, 3) {
		t.Fatalf("after Start userList = %v, want [1 3]", got)
	}
	if _, ok := c.limiter.UserLimitInfo.Load(format.UserTag(c.tag, isoB)); ok {
		t.Fatal("rejected user must not stay in the limiter")
	}

	// 下一轮全量：无效那条仍无效，其余不受影响，也不会因 already exists 失败
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listIDs(c); !idsEqual(got, 1, 3) {
		t.Fatalf("after resync userList = %v, want [1 3]", got)
	}

	// 面板修正了那条：重试加入成功
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, {Id: 2, Uuid: isoB}, {Id: 3, Uuid: isoC}})
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listIDs(c); !idsEqual(got, 1, 2, 3) {
		t.Fatalf("after fix userList = %v, want [1 2 3]", got)
	}

	// 推送 DIFF 里夹带一条无效用户：其余照常生效，版本照常前进
	c.userMu.Lock()
	c.syncVersion = 5
	c.userMu.Unlock()
	c.ApplyUserUpdate(panel.UserUpdate{Protocol: "shadowsocks", Port: c.info.Protocol.Port, Version: 6, Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF,
		Removed: []panel.UserInfo{{Id: 1, Uuid: isoA}},
		Users:   []panel.UserInfo{{Id: 4, Uuid: ""}, {Id: 5, Uuid: "55555555-5555-5555-5555-555555555555"}}})
	if got := listIDs(c); !idsEqual(got, 2, 3, 5) {
		t.Fatalf("after DIFF userList = %v, want [2 3 5]", got)
	}
	c.userMu.Lock()
	v := c.syncVersion
	c.userMu.Unlock()
	if v != 6 {
		t.Fatalf("syncVersion = %d, want 6", v)
	}
}
