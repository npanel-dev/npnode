package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
)

// I1：释放队列满了丢弃最旧的条目时，对应会话同时移出会话表（面板成员 120 秒自然过期），不会永久泄漏
func TestQueueOverflowDropsSessions(t *testing.T) {
	a, _, _ := newTestAdmission(t)
	a.queue = newReleaseQueue(2)
	for uid := 1; uid <= 5; uid++ {
		a.Admit(uid, "203.0.113.1", 1, "vless", 443)
		a.Open(uid, "203.0.113.1", nil)()
	}
	st := a.Stats()
	if st.Queued != 2 || st.Sessions != 2 || st.Entries != 2 {
		t.Fatalf("stats = %+v, want 2 queued / 2 sessions / 2 entries", st)
	}
	if a.HasSession(1, "203.0.113.1") || a.HasSession(3, "203.0.113.1") || !a.HasSession(5, "203.0.113.1") {
		t.Fatal("dropped releases must remove their sessions")
	}
}

// I1：任何“等待释放”的会话都有退出路径：长期处于 releasing（例如队列条目丢失）也会被扫描移出
func TestStaleReleasingSessionIsDropped(t *testing.T) {
	a, _, _ := newTestAdmission(t)
	a.StaleReleasing = 30 * time.Millisecond
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	a.Open(1, "203.0.113.1", nil)()
	a.queue.pop() // 模拟条目丢失
	a.Start()
	eventually(t, "stale releasing dropped", func() bool { return a.Stats() == AdmissionStats{} })
}

// I2：IPv6 按 /64 归并（与面板名额成员口径一致）：同一 uid 同一 /64 内仍有活跃会话时不发 release
func TestIPv6SameSlash64KeepsMember(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Start()
	a.Admit(1, "2001:db8:1:2::a", 1, "vless", 443)
	r1 := a.Open(1, "2001:db8:1:2::a", nil)
	a.Admit(1, "2001:db8:1:2::b", 1, "vless", 443)
	r2 := a.Open(1, "2001:db8:1:2::b", nil)
	r1()
	time.Sleep(60 * time.Millisecond)
	if _, r := fc.counts(); r != 0 {
		t.Fatalf("releases = %d, want 0 while the same /64 is still active", r)
	}
	if a.HasSession(1, "2001:db8:1:2::a") {
		t.Fatal("closed address session must be removed locally")
	}
	r2()
	eventually(t, "release of the last address", func() bool { _, r := fc.counts(); return r == 1 })
	// IPv4 按完整 IP：各自 release
	a.Admit(2, "203.0.113.1", 2, "vless", 443)
	a.Admit(2, "203.0.113.2", 2, "vless", 443)
	a.Open(2, "203.0.113.1", nil)()
	a.Open(2, "203.0.113.2", nil)()
	eventually(t, "ipv4 releases", func() bool { _, r := fc.counts(); return r == 3 })
}

// I2：降级计数也按 /64 归并：同一 /64 的另一地址不算另一台设备
func TestDegradedCountsSlash64AsOneDevice(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.set(unavailable)
	a.Admit(1, "2001:db8:1:2::a", 1, "vless", 443)
	defer a.Open(1, "2001:db8:1:2::a", nil)()
	if !a.Admit(1, "2001:db8:1:2::b", 1, "vless", 443) {
		t.Fatal("same /64 must be allowed in degraded mode")
	}
	if a.Admit(1, "2001:db8:1:3::a", 1, "vless", 443) {
		t.Fatal("another /64 must be denied in degraded mode")
	}
}

// I3：release 先等宽限再发；宽限内同 (uid, ip) 又有新连接则撤销，复用原会话、不重新 check
func TestReleaseGraceCancels(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.ReleaseGrace = 80 * time.Millisecond
	a.KeepaliveMin, a.KeepaliveMax = time.Hour, time.Hour
	a.Start()
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	a.Open(1, "203.0.113.1", nil)()
	time.Sleep(20 * time.Millisecond)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	rel := a.Open(1, "203.0.113.1", nil)
	defer rel()
	time.Sleep(150 * time.Millisecond)
	if c, r := fc.counts(); c != 1 || r != 0 {
		t.Fatalf("checks=%d releases=%d, want 1/0", c, r)
	}
}

// blockingRelease 第一次 release 阻塞到 unblock 关闭
type blockingRelease struct {
	*fakeChecker
	entered chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func (b *blockingRelease) Release(ctx context.Context, req panel.SessionRequest) error {
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.entered)
		<-b.unblock
	}
	return b.fakeChecker.Release(ctx, req)
}

// I3：release 在途时同 (uid, ip) 建了新会话：release 成功后立即对新会话补发一次 check，重新登记面板成员
func TestReleaseInFlightRechecksNewSession(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.KeepaliveMin, a.KeepaliveMax = time.Hour, time.Hour
	br := &blockingRelease{fakeChecker: fc, entered: make(chan struct{}), unblock: make(chan struct{})}
	a.Configure(AdmissionConfig{Checker: br, ServerID: 12})
	a.Start()
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	a.Open(1, "203.0.113.1", nil)()
	<-br.entered
	if !a.Admit(1, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("new connection allowed")
	}
	defer a.Open(1, "203.0.113.1", nil)()
	if c, _ := fc.counts(); c != 2 {
		t.Fatalf("checks = %d, want 2", c)
	}
	close(br.unblock)
	eventually(t, "recheck after release", func() bool { c, _ := fc.counts(); return c == 3 })
}

// I3：强制 release（Forget）以及重载放回的条目，发送前同样检查：已有新会话就不发
func TestForcedReleaseSkippedWhenSuperseded(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	defer a.Open(1, "203.0.113.1", nil)()
	a.Forget(1)
	a.Admit(1, "203.0.113.1", 1, "vless", 443) // 新会话
	a.Start()
	time.Sleep(60 * time.Millisecond)
	if _, r := fc.counts(); r != 0 {
		t.Fatalf("releases = %d, want 0 (superseded by new session)", r)
	}
}

// blockingCheck 保活 check 阻塞到 unblock 关闭（首次准入不阻塞）
type blockingCheck struct {
	*fakeChecker
	mu      sync.Mutex
	events  []string
	entered chan struct{}
	unblock chan struct{}
	n       atomic.Int32
}

func (b *blockingCheck) Check(ctx context.Context, req panel.SessionRequest) (*panel.SessionResult, error) {
	if b.n.Add(1) == 2 {
		close(b.entered)
		<-b.unblock
		b.mu.Lock()
		b.events = append(b.events, "check-done")
		b.mu.Unlock()
	}
	return b.fakeChecker.Check(ctx, req)
}

func (b *blockingCheck) Release(ctx context.Context, req panel.SessionRequest) error {
	b.mu.Lock()
	b.events = append(b.events, "release")
	b.mu.Unlock()
	return b.fakeChecker.Release(ctx, req)
}

// M1：同 key 的保活在途时最后一条连接关闭，release 等保活完成后再发（否则晚到的保活会重新登记幽灵成员）
func TestReleaseWaitsForInflightKeepalive(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	bc := &blockingCheck{fakeChecker: fc, entered: make(chan struct{}), unblock: make(chan struct{})}
	a.Configure(AdmissionConfig{Checker: bc, ServerID: 12})
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	rel := a.Open(1, "203.0.113.1", nil)
	a.Start()
	<-bc.entered
	rel()
	time.Sleep(60 * time.Millisecond)
	if _, r := fc.counts(); r != 0 {
		t.Fatal("release must wait for the in-flight keepalive")
	}
	close(bc.unblock)
	eventually(t, "release after keepalive", func() bool { _, r := fc.counts(); return r == 1 })
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if len(bc.events) != 2 || bc.events[0] != "check-done" || bc.events[1] != "release" {
		t.Fatalf("events = %v", bc.events)
	}
	eventually(t, "empty", func() bool { return a.Stats() == AdmissionStats{} })
}

// M2：面板拒绝（allowed=false）短暂负缓存，期间不再请求；降级 / 出错不缓存
func TestDenyCache(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.DenyCacheTTL = 80 * time.Millisecond
	fc.set(deny("device_limit"))
	if a.Admit(1, "203.0.113.1", 1, "vless", 443) || a.Admit(1, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("must be denied")
	}
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d, want 1 (cached)", c)
	}
	time.Sleep(100 * time.Millisecond)
	fc.set(nil)
	if !a.Admit(1, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("after ttl must recheck and allow")
	}
	// 降级拒绝不缓存
	fc.set(unavailable)
	defer a.Open(1, "203.0.113.1", nil)()
	a.Admit(1, "203.0.113.2", 1, "vless", 443)
	before, _ := fc.counts()
	a.Admit(1, "203.0.113.2", 1, "vless", 443)
	if after, _ := fc.counts(); after != before+1 {
		t.Fatal("degraded denial must not be cached")
	}
}

// M4：保活被拒、会话已移除后才登记的链路判为已撤销，由调用方立即关闭；不受准入管理的不算
func TestRevoked(t *testing.T) {
	a, fc, cr := newTestAdmission(t)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	defer a.Open(1, "203.0.113.1", nil)()
	if a.Revoked(1, "203.0.113.1", 1) {
		t.Fatal("has session")
	}
	fc.set(deny("device_limit"))
	a.KeepaliveMin, a.KeepaliveMax = 0, 0
	a.Start()
	eventually(t, "denied", func() bool { return len(cr.list()) == 1 })
	if !a.Revoked(1, "::ffff:203.0.113.1", 1) {
		t.Fatal("session removed after Admit: must be revoked")
	}
	if a.Revoked(1, "203.0.113.1", 0) || a.Revoked(0, "203.0.113.1", 1) || a.Revoked(9_000_000_000_001, "203.0.113.1", 1) || a.Revoked(1, "bad", 1) {
		t.Fatal("unmanaged links are never revoked")
	}
}

// M6：内部账号（id ≥ 9_000_000_000_000，与面板 noderelay.RelayUserIDBase 相同）跳过准入
func TestInternalAccountSkipsAdmission(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.set(deny("device_limit"))
	if !a.Admit(9_000_000_000_000, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("internal account must bypass admission")
	}
	if a.Admit(8_999_999_999_999, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("below threshold is a normal entry")
	}
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d, want 1", c)
	}
}

// V2：保活被拒时按链路登记的关闭函数逐条关闭（不依赖当前 dispatcher，重载前建立的链路也能关掉），只关该 (uid, ip)
func TestDeniedClosesRegisteredLinks(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	var closedA, closedB, other atomic.Int32
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	var relA, relB func()
	relA = a.Open(1, "203.0.113.1", func() { closedA.Add(1); relA() })
	relB = a.Open(1, "203.0.113.1", func() { closedB.Add(1); relB() })
	a.Admit(1, "203.0.113.2", 2, "vless", 443)
	defer a.Open(1, "203.0.113.2", func() { other.Add(1) })()
	fc.set(func(req panel.SessionRequest) (*panel.SessionResult, error) {
		if req.ClientIP == "203.0.113.1" {
			return &panel.SessionResult{Allowed: false, Reason: "device_limit"}, nil
		}
		return &panel.SessionResult{Allowed: true}, nil
	})
	a.Start()
	eventually(t, "links closed", func() bool { return closedA.Load() == 1 && closedB.Load() == 1 })
	if other.Load() != 0 {
		t.Fatal("other ip must stay")
	}
	eventually(t, "entry gone", func() bool { return a.Stats().Entries == 1 })
}
