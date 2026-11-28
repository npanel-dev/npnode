package node

import (
	"context"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/common/rate"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

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
	ref, _ := c.limiter.CheckLimit(key, "203.0.113.7", true, true) // 模拟一条已有连接
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
	ref, _ := c.limiter.CheckLimit(format.UserTag(c.tag, isoA), "203.0.113.7", true, true)
	c.userMu.Lock()
	c.syncVersion = 1
	c.userMu.Unlock()
	nu := old
	nu.DeviceLimit = 0
	c.ApplyUserUpdate(panel.UserUpdate{Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF, Version: 2,
		Removed: []panel.UserInfo{old}, Users: []panel.UserInfo{nu}})
	if bucketRef(c, isoA) != ref {
		t.Fatal("device-only change must not delete the user")
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
	ref, _ := c.limiter.CheckLimit(format.UserTag(c.tag, isoA), "203.0.113.7", true, true)
	fp.setUsers([]panel.UserInfo{{Id: 1, Uuid: isoA, SpeedLimit: 4194304}})
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := bucketRef(c, isoA); got == ref {
		t.Fatal("without admission_v1 a limit change must delete and re-add the user")
	}
	if u, n := userOf(c, 1); n != 1 || u.SpeedLimit != 4194304 {
		t.Fatalf("entry = %+v count %d", u, n)
	}
}
