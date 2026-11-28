package limiter

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
)

// withAdmissionV1 打开 admission_v1 并建立进程级准入实例（假面板），结束后撤掉
func withAdmissionV1(t *testing.T, online bool) (*Admission, *fakeChecker, *closeRecorder) {
	t.Helper()
	panel.SetAdmissionV1(true)
	panel.SetOnlineV1(online)
	Init()
	fc := &fakeChecker{}
	cr := &closeRecorder{}
	a := EnsureAdmission(AdmissionConfig{Checker: fc, ServerID: 12, Closer: cr.close})
	a.ReleaseBackoff = []time.Duration{time.Millisecond}
	t.Cleanup(func() {
		DisableAdmission()
		panel.SetAdmissionV1(false)
		panel.SetOnlineV1(false)
	})
	return a, fc, cr
}

// admission_v1 需要连接表：即使 online_v1 关闭也建表并挂钩子，但不改变在线上报口径；
// 只用于准入时链路关闭后不保留空条目（没有 Snapshot 清理，否则会无限增长）
func TestAdmissionBuildsConnTableWithoutOnlineV1(t *testing.T) {
	a, _, _ := withAdmissionV1(t, false)
	const tag = "adm-noonline"
	l := AddInboundLimiter(tag, "vless", 443, []panel.UserInfo{{Id: 7, Uuid: "u-7", DeviceLimit: 1}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	if l.Conns == nil || l.Admission != a {
		t.Fatal("admission_v1 must build conn table and attach admission")
	}
	if l.ActiveOnline() {
		t.Fatal("online report must stay legacy without online_v1")
	}
	key := format.UserTag(tag, "u-7")
	if _, reject := l.CheckLimit(key, "203.0.113.7", true, true); reject {
		t.Fatal("allowed by panel")
	}
	rel := l.OpenConn(key, "203.0.113.7")
	if rel == nil || l.Conns.Active() != 1 {
		t.Fatal("link must be registered")
	}
	// 旧口径在线照旧：准入只接管设备判断，CheckLimit 仍登记 UserOnlineIP，在线按旧口径读出后清空
	got, _ := l.GetOnlineDevice()
	if !reflect.DeepEqual(*got, []panel.OnlineUser{{UID: 7, IP: "203.0.113.7"}}) {
		t.Fatalf("legacy online = %#v", *got)
	}
	rel()
	if l.Conns.Len() != 0 {
		t.Fatalf("conn table keeps %d idle entries without online_v1", l.Conns.Len())
	}
}

// 同时打开 online_v1：在线按活跃连接上报（与 P1 一致），准入共用同一个开、关钩子
func TestAdmissionWithOnlineV1SharesHooks(t *testing.T) {
	a, fc, _ := withAdmissionV1(t, true)
	a.Start()
	const tag = "adm-online"
	l := AddInboundLimiter(tag, "vless", 443, []panel.UserInfo{{Id: 7, Uuid: "u-7", DeviceLimit: 1}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	key := format.UserTag(tag, "u-7")
	l.CheckLimit(key, "::ffff:203.0.113.7", true, true)
	rel := l.OpenConn(key, "::ffff:203.0.113.7")
	if !l.ActiveOnline() {
		t.Fatal("online_v1 report")
	}
	got, _ := l.GetOnlineDevice()
	if !reflect.DeepEqual(*got, []panel.OnlineUser{{UID: 7, IP: "203.0.113.7"}}) {
		t.Fatalf("online = %v", *got)
	}
	if !a.HasSession(7, "203.0.113.7") {
		t.Fatal("admission session")
	}
	rel()
	rel()
	eventually(t, "release", func() bool { _, r := fc.counts(); return r == 1 })
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d", c)
	}
}

// 接入点 CheckLimit：admission_v1 且 device_limit > 0 时走准入，被拒即 Reject；device_limit = 0 不请求面板
func TestCheckLimitUsesAdmission(t *testing.T) {
	_, fc, _ := withAdmissionV1(t, false)
	fc.set(deny("seat_cooling"))
	const tag = "adm-check"
	l := AddInboundLimiter(tag, "trojan", 8443, []panel.UserInfo{
		{Id: 1000000045, Uuid: "seat", DeviceLimit: 1},
		{Id: 2, Uuid: "plain", DeviceLimit: 0},
	}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	if _, reject := l.CheckLimit(format.UserTag(tag, "seat"), "203.0.113.7", true, true); !reject {
		t.Fatal("denied by panel must reject")
	}
	req := fc.lastCheck()
	if req.Protocol != "trojan" || req.Port != 8443 || req.UserID != 1000000045 {
		t.Fatalf("req = %+v", req)
	}
	if _, reject := l.CheckLimit(format.UserTag(tag, "plain"), "203.0.113.7", true, true); reject {
		t.Fatal("device_limit 0 must not be rejected")
	}
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d, want 1", c)
	}
}

// 未声明 admission_v1：即使进程里还有准入实例，限速器也不挂准入，CheckLimit 保持原逻辑、不请求面板
func TestCheckLimitLegacyWithoutAdmissionV1(t *testing.T) {
	_, fc, _ := withAdmissionV1(t, false)
	panel.SetAdmissionV1(false)
	const tag = "adm-off"
	l := AddLimiter(tag, []panel.UserInfo{{Id: 3, Uuid: "u", DeviceLimit: 1}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	if l.Admission != nil || l.Conns != nil {
		t.Fatal("no admission / conn table when admission_v1 and online_v1 are off")
	}
	if _, reject := l.CheckLimit(format.UserTag(tag, "u"), "203.0.113.7", true, true); reject {
		t.Fatal("legacy local device limit never rejects (aliveIp = 0)")
	}
	if c, _ := fc.counts(); c != 0 {
		t.Fatalf("checks = %d, want 0", c)
	}
	if l.OpenConn(format.UserTag(tag, "u"), "203.0.113.7") != nil {
		t.Fatal("OpenConn must be a no-op")
	}
}

// uid → 各入站的 taguuid 反查（关闭某 (uid, ip) 的全部连接用），随增删维护
func TestTagUUIDsForUID(t *testing.T) {
	withAdmissionV1(t, false)
	la := AddInboundLimiter("a", "vless", 443, []panel.UserInfo{{Id: 5, Uuid: "x"}, {Id: 6, Uuid: "y"}}, nil)
	lb := AddInboundLimiter("b", "trojan", 8443, []panel.UserInfo{{Id: 5, Uuid: "z"}}, nil)
	t.Cleanup(func() { DeleteLimiter("a"); DeleteLimiter("b") })
	got := TagUUIDsForUID(5)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{format.UserTag("a", "x"), format.UserTag("b", "z")}) {
		t.Fatalf("uid 5 -> %v", got)
	}
	lb.UpdateUser("b", []panel.UserInfo{{Id: 5, Uuid: "w"}}, []panel.UserInfo{{Id: 5, Uuid: "z"}})
	la.UpdateUser("a", nil, []panel.UserInfo{{Id: 5, Uuid: "x"}})
	if got := TagUUIDsForUID(5); !reflect.DeepEqual(got, []string{format.UserTag("b", "w")}) {
		t.Fatalf("after update uid 5 -> %v", got)
	}
	DeleteLimiter("b")
	if got := TagUUIDsForUID(5); len(got) != 0 {
		t.Fatalf("after delete limiter uid 5 -> %v", got)
	}
}

// U5 与准入：device_limit 1→0 立即 release、不再保活；0→1 对仍活跃的 (uid, ip) 补建会话并 check
func TestUpdateUserLimitTogglesAdmission(t *testing.T) {
	a, fc, cr := withAdmissionV1(t, false)
	a.ScanInterval = 5 * time.Millisecond
	a.Start()
	const tag = "adm-u5"
	l := AddInboundLimiter(tag, "vless", 443, []panel.UserInfo{{Id: 9, Uuid: "u", DeviceLimit: 1}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	key := format.UserTag(tag, "u")
	l.CheckLimit(key, "203.0.113.9", true, true)
	defer l.OpenConn(key, "203.0.113.9")()
	l.UpdateUserLimit(key, 0, 0)
	eventually(t, "release", func() bool { _, r := fc.counts(); return r == 1 })
	if a.HasSession(9, "203.0.113.9") {
		t.Fatal("session dropped")
	}
	fc.set(deny("device_limit"))
	l.UpdateUserLimit(key, 0, 1)
	eventually(t, "adopt closes", func() bool { return len(cr.list()) == 1 })
}
