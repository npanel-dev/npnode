package core

import (
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/counter"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/conf"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// seatTestCore 启动真实内核并加一个入站
func seatTestCore(t *testing.T, info *panel.NodeInfo) (*XrayCore, string) {
	t.Helper()
	c := conf.New()
	c.LogConfig.Level = "error"
	vc := New(c, nil)
	if err := vc.Start(&panel.ServerConfigResponse{Data: &panel.Data{IPStrategy: "prefer_ipv4", PullInterval: 3600}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { vc.Close() })
	tag := "seat-test-" + info.Type
	if err := vc.AddNode(tag, info); err != nil {
		t.Fatal(err)
	}
	return vc, tag
}

func vlessInfo(t *testing.T) *panel.NodeInfo {
	return &panel.NodeInfo{Id: 1, Type: "vless", Protocol: &panel.Protocol{Type: "vless", Port: freePort(t), Transport: "tcp", Encryption: "none"}}
}

func userIDs(users []panel.UserInfo) []int {
	out := make([]int, 0, len(users))
	for _, u := range users {
		out = append(out, u.Id)
	}
	sort.Ints(out)
	return out
}

func equalInts(got []int, want ...int) bool {
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

func (vc *XrayCore) uidOf(tag, uuid string) (int, bool) {
	vc.users.mapLock.RLock()
	defer vc.users.mapLock.RUnlock()
	id, ok := vc.users.uidMap[format.UserTag(tag, uuid)]
	return id, ok
}

const (
	uuidA = "6e8fac3f-bf7a-4a48-b28a-43bb660e2a19"
	uuidB = "0b9e2f6c-1d3a-4c5e-8f7a-9b0c1d2e3f40"
	uuidC = "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f"
)

// U3：单条无效或加入内核失败只跳过这一条，其余照常加入；只为成功的用户记录 uid
func TestAddUsersIsolatesSingleFailures(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	// 模拟内核里已经有这个用户（本地记录与内核不一致），内核会拒绝再次加入
	man, err := vc.GetUserManager(tag)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := buildVlessUser(tag, &panel.UserInfo{Id: 3, Uuid: uuidC}, "").ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}
	if err := man.AddUser(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	users := []panel.UserInfo{
		{Id: 1, Uuid: uuidA},
		{Id: 0, Uuid: "11111111-1111-4111-8111-111111111111"}, // id 无效
		{Id: 2, Uuid: ""},    // uuid 为空
		{Id: 3, Uuid: uuidC}, // 内核拒绝（already exists）
		{Id: 4, Uuid: uuidA}, // 同批 uuid 重复
		{Id: 1000000045, Uuid: uuidB},
	}
	added, err := vc.AddUsers(&AddUsersParams{Tag: tag, Users: users, NodeInfo: info})
	if err != nil {
		t.Fatalf("single-user failures must not fail the batch: %v", err)
	}
	if got := userIDs(added); !equalInts(got, 1, 1000000045) {
		t.Fatalf("added = %v, want [1 1000000045]", got)
	}
	for _, u := range []string{uuidC, "", "11111111-1111-4111-8111-111111111111"} {
		if _, ok := vc.uidOf(tag, u); ok {
			t.Fatalf("failed user %q must not be recorded in uidMap", u)
		}
	}
	if id, _ := vc.uidOf(tag, uuidA); id != 1 {
		t.Fatalf("uid of %s = %d, want 1 (the duplicate must not overwrite it)", uuidA, id)
	}
	// 失败的那条没有记入本地，内核恢复后下一轮同步可以重试加入；已加入的不会因 already exists 让整批失败
	if err := man.RemoveUser(context.Background(), format.UserTag(tag, uuidC)); err != nil {
		t.Fatal(err)
	}
	added, err = vc.AddUsers(&AddUsersParams{Tag: tag, Users: []panel.UserInfo{{Id: 3, Uuid: uuidC}, {Id: 1, Uuid: uuidA}}, NodeInfo: info})
	if err != nil {
		t.Fatal(err)
	}
	if got := userIDs(added); !equalInts(got, 3) {
		t.Fatalf("retry added = %v, want [3]", got)
	}
}

// U3：SS2022 的 uuid 短于密钥长度时原先会 panic，现在只跳过这一条
func TestAddUsersSkipsShortSS2022UUID(t *testing.T) {
	info := &panel.NodeInfo{Id: 1, Type: "shadowsocks", Protocol: &panel.Protocol{Type: "shadowsocks", Port: freePort(t), Cipher: "2022-blake3-aes-256-gcm", ServerKey: strings.Repeat("k", 32)}}
	vc, tag := seatTestCore(t, info)
	added, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{
		{Id: 1, Uuid: "short"},
		{Id: 2, Uuid: uuidA},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := userIDs(added); !equalInts(got, 2) {
		t.Fatalf("added = %v, want [2]", got)
	}
}

// U3：删除时某条不存在视为已删除并继续，不影响其余用户
func TestDelUsersContinuesPastMissingUsers(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 1, Uuid: uuidA}, {Id: 2, Uuid: uuidB}}}); err != nil {
		t.Fatal(err)
	}
	removed, err := vc.DelUsers([]panel.UserInfo{{Id: 9, Uuid: uuidC}, {Id: 1, Uuid: uuidA}, {Id: 2, Uuid: uuidB}}, tag, info)
	if err != nil {
		t.Fatalf("missing user must not fail the batch: %v", err)
	}
	if got := userIDs(removed); !equalInts(got, 1, 2, 9) {
		t.Fatalf("removed = %v, want [1 2 9]", got)
	}
	if _, ok := vc.uidOf(tag, uuidB); ok {
		t.Fatal("user after a missing one was not removed")
	}
}

func userCounter(vc *XrayCore, tag, uuid string) *counter.TrafficStorage {
	v, _ := vc.dispatcher.Counter.LoadOrStore(tag, counter.NewTrafficCounter())
	return v.(*counter.TrafficCounter).GetCounter(format.UserTag(tag, uuid))
}

// U2 / T4：删除用户后，尚未上报的流量在下一次上报中以原 id 上报（不受上报阈值影响）
func TestDeletedUserTrafficIsReportedWithOriginalID(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 1000000045, Uuid: uuidA}}}); err != nil {
		t.Fatal(err)
	}
	storage := userCounter(vc, tag, uuidA)
	storage.UpCounter.Add(1000)
	storage.DownCounter.Add(5000)
	if _, err := vc.DelUsers([]panel.UserInfo{{Id: 1000000045, Uuid: uuidA}}, tag, info); err != nil {
		t.Fatal(err)
	}
	// 删除后仍在途的字节（连接关闭过程中）写进旧计数器，也要计入
	storage.DownCounter.Add(24)
	got, err := vc.GetUserTrafficSlice(tag, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (panel.UserTraffic{UID: 1000000045, Upload: 1000, Download: 5024}) {
		t.Fatalf("traffic after removal = %+v, want uid 1000000045 up 1000 down 5024", got)
	}
	if got, _ := vc.GetUserTrafficSlice(tag, 0); len(got) != 0 {
		t.Fatalf("residual traffic reported twice: %+v", got)
	}
}

// U2：修改（先删后加，同一 id）时旧条目的流量保留，与新条目流量合并为同一 id 一条
func TestModifiedUserTrafficMergesUnderSameID(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	old := panel.UserInfo{Id: 1000000045, Uuid: uuidA, SpeedLimit: 524288}
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{old}}); err != nil {
		t.Fatal(err)
	}
	userCounter(vc, tag, uuidA).UpCounter.Add(300)
	if _, err := vc.DelUsers([]panel.UserInfo{old}, tag, info); err != nil {
		t.Fatal(err)
	}
	// 重置名额：同一 id、新 uuid
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 1000000045, Uuid: uuidB}}}); err != nil {
		t.Fatal(err)
	}
	userCounter(vc, tag, uuidB).UpCounter.Add(200)
	got, _ := vc.GetUserTrafficSlice(tag, 0)
	if len(got) != 1 || got[0] != (panel.UserTraffic{UID: 1000000045, Upload: 500}) {
		t.Fatalf("traffic = %+v, want one entry uid 1000000045 up 500", got)
	}
}
