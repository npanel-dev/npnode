package node

import (
	"context"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

// countSkipWarns 统计“跳过某个用户”的 Warn 日志条数
func countSkipWarns(h *test.Hook) int {
	n := 0
	for _, e := range h.AllEntries() {
		if e.Level == log.WarnLevel && (e.Message == "用户无效，跳过" || e.Message == "用户加入内核失败，跳过") {
			n++
		}
	}
	return n
}

// U3 ④：同一条无效条目只在首次（或内容变化后）告警一次；面板移除后记忆清掉，再次出现重新告警
func TestRejectedUserWarnsOnlyOnce(t *testing.T) {
	hook := test.NewGlobal()
	t.Cleanup(hook.Reset)
	fp := &fakePanel{}
	bad := panel.UserInfo{Id: 0, Uuid: isoB}
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, bad})
	c := startIsolationController(t, fp)
	if n := countSkipWarns(hook); n != 1 {
		t.Fatalf("after Start warns = %d, want 1", n)
	}
	for i := 0; i < 3; i++ {
		if err := c.userListMonitor(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := countSkipWarns(hook); n != 1 {
		t.Fatalf("same invalid entry warned again on resync: warns = %d, want 1", n)
	}

	// 内容变化（同一 uuid 改了限速）：新内容首次告警一次
	changed := panel.UserInfo{Id: 0, Uuid: isoB, SpeedLimit: 8}
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, changed})
	_ = c.userListMonitor(context.Background())
	_ = c.userListMonitor(context.Background())
	if n := countSkipWarns(hook); n != 2 {
		t.Fatalf("after content change warns = %d, want 2", n)
	}

	// 面板移除该条目后再加回：记忆已清，重新告警一次
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}})
	_ = c.userListMonitor(context.Background())
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, changed})
	_ = c.userListMonitor(context.Background())
	if n := countSkipWarns(hook); n != 3 {
		t.Fatalf("after remove and re-add warns = %d, want 3", n)
	}

	// 推送 DIFF：同一条无效条目反复出现只告警一次；DIFF 移除它会清掉记忆
	c.userMu.Lock()
	c.syncVersion = 10
	c.userMu.Unlock()
	diffBad := panel.UserInfo{Id: 9, Uuid: ""}
	for v := uint64(11); v <= 13; v++ {
		c.ApplyUserUpdate(panel.UserUpdate{Protocol: "shadowsocks", Port: c.info.Protocol.Port, Version: v,
			Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Users: []panel.UserInfo{diffBad}})
	}
	if n := countSkipWarns(hook); n != 4 {
		t.Fatalf("after repeated DIFF warns = %d, want 4", n)
	}
	c.ApplyUserUpdate(panel.UserUpdate{Protocol: "shadowsocks", Port: c.info.Protocol.Port, Version: 14,
		Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Removed: []panel.UserInfo{diffBad}})
	c.ApplyUserUpdate(panel.UserUpdate{Protocol: "shadowsocks", Port: c.info.Protocol.Port, Version: 15,
		Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Users: []panel.UserInfo{diffBad}})
	if n := countSkipWarns(hook); n != 5 {
		t.Fatalf("after DIFF remove and re-add warns = %d, want 5", n)
	}

	// 无效条目仍会被静默重试：修正后照常加入，记忆清掉
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA}, {Id: 2, Uuid: isoB}})
	_ = c.userListMonitor(context.Background())
	if got := listIDs(c); !idsEqual(got, 1, 2) {
		t.Fatalf("after fix userList = %v, want [1 2]", got)
	}
	c.userMu.Lock()
	n := len(c.rejected)
	c.userMu.Unlock()
	if n != 0 {
		t.Fatalf("rejected memory keeps %d entries after the panel fixed them", n)
	}
}
