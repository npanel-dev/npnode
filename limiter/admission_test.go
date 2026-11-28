package limiter

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
)

// fakeChecker 假面板准入接口：check / release 的结果由测试设置，并记录全部请求
type fakeChecker struct {
	mu       sync.Mutex
	checks   []panel.SessionRequest
	releases []panel.SessionRequest
	// decide 返回 check 结果；nil 时一律放行
	decide func(req panel.SessionRequest) (*panel.SessionResult, error)
	// releaseErr 返回 release 结果；nil 时成功
	releaseErr func(n int) error
	delay      time.Duration
}

func (f *fakeChecker) Check(ctx context.Context, req panel.SessionRequest) (*panel.SessionResult, error) {
	f.mu.Lock()
	f.checks = append(f.checks, req)
	decide, delay := f.decide, f.delay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if decide == nil {
		return &panel.SessionResult{Allowed: true, Current: 1, Limit: 1}, nil
	}
	return decide(req)
}

func (f *fakeChecker) Release(ctx context.Context, req panel.SessionRequest) error {
	f.mu.Lock()
	f.releases = append(f.releases, req)
	n := len(f.releases)
	fn := f.releaseErr
	f.mu.Unlock()
	if fn != nil {
		return fn(n)
	}
	return nil
}

func (f *fakeChecker) set(decide func(panel.SessionRequest) (*panel.SessionResult, error)) {
	f.mu.Lock()
	f.decide = decide
	f.mu.Unlock()
}

func (f *fakeChecker) counts() (checks, releases int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.checks), len(f.releases)
}

func (f *fakeChecker) lastCheck() panel.SessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checks[len(f.checks)-1]
}

func deny(reason string) func(panel.SessionRequest) (*panel.SessionResult, error) {
	return func(panel.SessionRequest) (*panel.SessionResult, error) {
		return &panel.SessionResult{Allowed: false, Current: 1, Limit: 1, Reason: reason}, nil
	}
}

func unavailable(panel.SessionRequest) (*panel.SessionResult, error) {
	return nil, errors.New("panel down")
}

type closeRecorder struct {
	mu     sync.Mutex
	closed []string
}

func (c *closeRecorder) close(uid int, ip string) {
	c.mu.Lock()
	c.closed = append(c.closed, fmt.Sprintf("%d|%s", uid, ip))
	c.mu.Unlock()
}

func (c *closeRecorder) list() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.closed...)
}

// newTestAdmission 后台参数调小：保活 30～40ms、扫描 5ms、试探 20ms、release 退避 1ms
func newTestAdmission(t *testing.T) (*Admission, *fakeChecker, *closeRecorder) {
	t.Helper()
	fc := &fakeChecker{}
	cr := &closeRecorder{}
	a := NewAdmission(AdmissionConfig{Checker: fc, ServerID: 12, Closer: cr.close})
	a.KeepaliveMin = 30 * time.Millisecond
	a.KeepaliveMax = 40 * time.Millisecond
	a.ScanInterval = 5 * time.Millisecond
	a.ProbeInterval = 20 * time.Millisecond
	a.OrphanAfter = 50 * time.Millisecond
	a.ReleaseBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(a.Stop)
	return a, fc, cr
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A1：第一条连接 check，同 (uid, ip) 的后续连接复用；connection_id 为 {server_id}:{ip}:{毫秒时间戳}，请求带协议和端口
func TestAdmitChecksFirstConnectionOnly(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	before := time.Now().UnixMilli()
	if !a.Admit(1000000045, "203.0.113.7", 1, "vless", 443) {
		t.Fatal("first connection must be allowed")
	}
	rel := a.Open(1000000045, "203.0.113.7")
	defer rel()
	if !a.Admit(1000000045, "::ffff:203.0.113.7", 1, "trojan", 8443) {
		t.Fatal("reuse must be allowed")
	}
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d, want 1", c)
	}
	req := fc.lastCheck()
	if req.UserID != 1000000045 || req.ClientIP != "203.0.113.7" || req.Protocol != "vless" || req.Port != 443 {
		t.Fatalf("req = %+v", req)
	}
	parts := strings.Split(req.ConnectionID, ":")
	if len(parts) != 3 || parts[0] != "12" || parts[1] != "203.0.113.7" {
		t.Fatalf("connection_id = %q", req.ConnectionID)
	}
	var ms int64
	fmt.Sscan(parts[2], &ms)
	if ms < before || ms > time.Now().UnixMilli() {
		t.Fatalf("connection_id 时间戳 = %d", ms)
	}
}

// device_limit 为 0、uid 无效、IP 无法解析：不准入、不请求面板
func TestAdmitSkipsUnmanaged(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.set(deny("device_limit"))
	if !a.Admit(1, "203.0.113.7", 0, "vless", 443) || !a.Admit(0, "203.0.113.7", 1, "vless", 443) || !a.Admit(1, "not-an-ip", 1, "vless", 443) {
		t.Fatal("unmanaged connections must be allowed")
	}
	if c, _ := fc.counts(); c != 0 {
		t.Fatalf("checks = %d, want 0", c)
	}
}

// 同一 (uid, ip) 并发的首连接用 singleflight 合并成一次 check
func TestAdmitConcurrentFirstConnectionsSingleCheck(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.delay = 50 * time.Millisecond
	var wg sync.WaitGroup
	var allowed atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a.Admit(7, "198.51.100.1", 1, "vless", 443) {
				allowed.Add(1)
				a.Open(7, "198.51.100.1")
			}
		}()
	}
	wg.Wait()
	if c, _ := fc.counts(); c != 1 {
		t.Fatalf("checks = %d, want 1", c)
	}
	if allowed.Load() != 50 {
		t.Fatalf("allowed = %d", allowed.Load())
	}
	if st := a.Stats(); st.Sessions != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// A2：被拒时不建会话，同一 (uid, ip) 的下一条新连接重新 check
func TestAdmitDeniedThenRechecks(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.set(deny("device_limit"))
	if a.Admit(9, "203.0.113.9", 1, "vless", 443) {
		t.Fatal("must be denied")
	}
	if a.HasSession(9, "203.0.113.9") {
		t.Fatal("denied must not create session")
	}
	fc.set(nil)
	if !a.Admit(9, "203.0.113.9", 1, "vless", 443) {
		t.Fatal("recheck must be allowed")
	}
	if c, _ := fc.counts(); c != 2 {
		t.Fatalf("checks = %d, want 2", c)
	}
}

// A4 / A7：跨所有入站的最后一条链路关闭后异步 release 一次，connection_id 与首次 check 相同
func TestReleaseAfterLastLinkCloses(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Start()
	a.Admit(5, "203.0.113.5", 1, "vless", 443)
	r1 := a.Open(5, "203.0.113.5") // 入站 A
	r2 := a.Open(5, "203.0.113.5") // 入站 B
	r1()
	r1() // 重复调用只生效一次
	time.Sleep(20 * time.Millisecond)
	if _, rel := fc.counts(); rel != 0 {
		t.Fatalf("releases = %d, want 0 while a link is alive", rel)
	}
	r2()
	eventually(t, "release", func() bool { _, r := fc.counts(); return r == 1 })
	fc.mu.Lock()
	rel := fc.releases[0]
	connID := fc.checks[0].ConnectionID
	fc.mu.Unlock()
	if rel.ConnectionID != connID || rel.UserID != 5 || rel.ClientIP != "203.0.113.5" || rel.Port != 443 {
		t.Fatalf("release = %+v, check connection_id = %s", rel, connID)
	}
	eventually(t, "entry removed", func() bool { return a.Stats() == AdmissionStats{} })
}

// §7.6：release 发出前同一 (uid, ip) 又有新连接，撤销 release，复用原会话、不重新 check
func TestReleaseCancelledByNewConnection(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Admit(5, "203.0.113.5", 1, "vless", 443)
	a.Open(5, "203.0.113.5")()
	if st := a.Stats(); st.Queued != 1 {
		t.Fatalf("stats = %+v, want 1 queued", st)
	}
	if !a.Admit(5, "203.0.113.5", 1, "vless", 443) {
		t.Fatal("reuse must be allowed")
	}
	rel := a.Open(5, "203.0.113.5")
	defer rel()
	a.Start()
	time.Sleep(30 * time.Millisecond)
	checks, releases := fc.counts()
	if releases != 0 || checks != 1 {
		t.Fatalf("checks=%d releases=%d, want 1/0", checks, releases)
	}
	if !a.HasSession(5, "203.0.113.5") {
		t.Fatal("session must be kept")
	}
}

// A7：release 失败重试 3 次（共 4 次），之后丢弃；成功即停止重试
func TestReleaseRetriesThenDrops(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.releaseErr = func(n int) error { return errors.New("500") }
	a.Start()
	a.Admit(5, "203.0.113.5", 1, "vless", 443)
	a.Open(5, "203.0.113.5")()
	eventually(t, "4 attempts", func() bool { _, r := fc.counts(); return r == 4 })
	time.Sleep(20 * time.Millisecond)
	if _, r := fc.counts(); r != 4 {
		t.Fatalf("releases = %d, want 4", r)
	}

	b, fc2, _ := newTestAdmission(t)
	fc2.releaseErr = func(n int) error {
		if n < 2 {
			return errors.New("500")
		}
		return nil
	}
	b.Start()
	b.Admit(6, "203.0.113.6", 1, "vless", 443)
	b.Open(6, "203.0.113.6")()
	eventually(t, "2 attempts", func() bool { _, r := fc2.counts(); return r == 2 })
	time.Sleep(20 * time.Millisecond)
	if _, r := fc2.counts(); r != 2 {
		t.Fatalf("releases = %d, want 2", r)
	}
}

// 释放队列有界：满了丢弃最旧的
func TestReleaseQueueDropsOldest(t *testing.T) {
	q := newReleaseQueue(3)
	for i := 1; i <= 5; i++ {
		q.push(releaseItem{uid: i})
	}
	var got []int
	for {
		it, ok := q.pop()
		if !ok {
			break
		}
		got = append(got, it.uid)
	}
	if fmt.Sprint(got) != "[3 4 5]" {
		t.Fatalf("queue = %v", got)
	}
}

// A3：仍有活跃链路时按 KeepaliveMin～KeepaliveMax 保活，connection_id 不变；链路关闭后不再保活
func TestKeepaliveWhileLinksAlive(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Start()
	a.Admit(3, "203.0.113.3", 1, "vless", 443)
	rel := a.Open(3, "203.0.113.3")
	eventually(t, "keepalives", func() bool { c, _ := fc.counts(); return c >= 4 })
	fc.mu.Lock()
	for _, c := range fc.checks {
		if c.ConnectionID != fc.checks[0].ConnectionID {
			t.Errorf("keepalive connection_id changed: %s vs %s", c.ConnectionID, fc.checks[0].ConnectionID)
		}
	}
	fc.mu.Unlock()
	rel()
	eventually(t, "release", func() bool { _, r := fc.counts(); return r == 1 })
	c1, _ := fc.counts()
	time.Sleep(100 * time.Millisecond)
	if c2, _ := fc.counts(); c2 != c1 {
		t.Fatalf("keepalive continued after release: %d -> %d", c1, c2)
	}
}

// A2：保活被拒（含严格模式下另一台服务器顶掉）时关闭这个 (uid, ip) 的全部连接、删除会话；之后新连接重新 check
func TestKeepaliveDeniedClosesAllLinks(t *testing.T) {
	a, fc, cr := newTestAdmission(t)
	a.Admit(3, "203.0.113.3", 1, "vless", 443)
	rel := a.Open(3, "203.0.113.3")
	a.Admit(3, "203.0.113.4", 1, "vless", 443)
	keep := a.Open(3, "203.0.113.4")
	defer keep()
	fc.set(func(req panel.SessionRequest) (*panel.SessionResult, error) {
		if req.ClientIP == "203.0.113.3" {
			return &panel.SessionResult{Allowed: false, Current: 1, Limit: 1, Reason: "device_limit"}, nil
		}
		return &panel.SessionResult{Allowed: true}, nil
	})
	a.Start()
	eventually(t, "close", func() bool { return len(cr.list()) == 1 })
	if got := cr.list(); got[0] != "3|203.0.113.3" {
		t.Fatalf("closed = %v", got)
	}
	if a.HasSession(3, "203.0.113.3") || !a.HasSession(3, "203.0.113.4") {
		t.Fatal("only the denied (uid, ip) session must be removed")
	}
	rel() // Closer 关闭链路后释放钩子触发：没有会话，不发 release
	time.Sleep(20 * time.Millisecond)
	if _, r := fc.counts(); r != 0 {
		t.Fatalf("releases = %d, want 0 after denial", r)
	}
	fc.set(nil)
	before, _ := fc.counts()
	if !a.Admit(3, "203.0.113.3", 1, "vless", 443) {
		t.Fatal("new connection must be rechecked and allowed")
	}
	if after, _ := fc.counts(); after <= before {
		t.Fatal("new connection after denial must check again")
	}
}

// A5：面板不可用时降级。名额 device_limit=1：本机没有该 uid 的其他 IP 时放行并标记 degraded，已有时拒绝
func TestAdmitDegradesWhenPanelUnavailable(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	fc.set(unavailable)
	if !a.Admit(8, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("first IP must be allowed in degraded mode")
	}
	keep := a.Open(8, "203.0.113.1")
	defer keep()
	if st := a.Stats(); st.Degraded != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if a.Admit(8, "203.0.113.2", 1, "vless", 443) {
		t.Fatal("second IP must be denied when device_limit reached")
	}
	if !a.Admit(8, "203.0.113.2", 2, "vless", 443) {
		t.Fatal("device_limit=2 allows a second IP")
	}
	// 同一 IP 已有会话：复用，不再请求
	c1, _ := fc.counts()
	if !a.Admit(8, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("reuse")
	}
	if c2, _ := fc.counts(); c2 != c1 {
		t.Fatal("reuse must not check")
	}
}

// 降级期间保活失败不关闭连接；A6：面板恢复（试探成功）后立即对全部 degraded 会话补发 check，被拒的关闭
func TestRecoverRechecksDegradedSessions(t *testing.T) {
	a, fc, cr := newTestAdmission(t)
	a.KeepaliveMin, a.KeepaliveMax = time.Hour, time.Hour // 只看试探和补发
	fc.set(unavailable)
	for i, ip := range []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"} {
		if !a.Admit(100+i, ip, 1, "vless", 443) {
			t.Fatal("degraded allow")
		}
		defer a.Open(100+i, ip)()
	}
	a.Start()
	time.Sleep(60 * time.Millisecond) // 几轮试探都失败
	if len(cr.list()) != 0 || a.Stats().Degraded != 3 {
		t.Fatalf("closed=%v stats=%+v", cr.list(), a.Stats())
	}
	fc.set(func(req panel.SessionRequest) (*panel.SessionResult, error) {
		if req.UserID == 101 {
			return &panel.SessionResult{Allowed: false, Reason: "device_limit", Current: 1, Limit: 1}, nil
		}
		return &panel.SessionResult{Allowed: true}, nil
	})
	eventually(t, "recovered", func() bool { return a.Stats().Degraded == 0 && len(cr.list()) > 0 })
	if got := cr.list(); len(got) != 1 || got[0] != "101|203.0.113.2" {
		t.Fatalf("closed = %v", got)
	}
	if !a.HasSession(100, "203.0.113.1") || a.HasSession(101, "203.0.113.2") || !a.HasSession(102, "203.0.113.3") {
		t.Fatal("sessions after recovery")
	}
}

// 新连接的 check 成功（面板恢复）也会触发补发
func TestSuccessfulAdmitTriggersRecovery(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.KeepaliveMin, a.KeepaliveMax = time.Hour, time.Hour
	a.ProbeInterval = time.Hour
	fc.set(unavailable)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	defer a.Open(1, "203.0.113.1")()
	a.Start()
	fc.set(nil)
	a.Admit(2, "203.0.113.2", 1, "vless", 443)
	eventually(t, "recovered", func() bool { return a.Stats().Degraded == 0 })
}

// 放行后一直没有链路登记的会话（如放行后用户恰好被移除）按已关闭处理，不会永远保活
func TestOrphanSessionReleased(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	a.Start()
	a.Admit(4, "203.0.113.4", 1, "vless", 443)
	eventually(t, "orphan release", func() bool { _, r := fc.counts(); return r == 1 })
	eventually(t, "empty", func() bool { return a.Stats() == AdmissionStats{} })
}

// 会话表有上限：满了拒绝新的 (uid, ip)
func TestAdmitRejectsWhenTableFull(t *testing.T) {
	a, _, _ := newTestAdmission(t)
	a.MaxSessions = 2
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	a.Admit(2, "203.0.113.1", 1, "vless", 443)
	if a.Admit(3, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("must reject when full")
	}
	if !a.Admit(1, "203.0.113.1", 1, "vless", 443) {
		t.Fatal("existing session still reused")
	}
}

// U5：device_limit 改为 0 时立即 release 并停止保活；改回正数时对仍活跃的 (uid, ip) 建会话并 check，被拒的关闭
func TestForgetAndAdopt(t *testing.T) {
	a, fc, cr := newTestAdmission(t)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	rel := a.Open(1, "203.0.113.1")
	defer rel()
	a.Start()
	a.Forget(1)
	eventually(t, "forced release", func() bool { _, r := fc.counts(); return r == 1 })
	if a.HasSession(1, "203.0.113.1") {
		t.Fatal("session must be dropped")
	}
	c1, _ := fc.counts()
	time.Sleep(80 * time.Millisecond)
	if c2, _ := fc.counts(); c2 != c1 {
		t.Fatal("no keepalive after Forget")
	}
	fc.set(deny("device_limit"))
	a.Adopt(1, "vless", 443)
	eventually(t, "adopt check closes", func() bool { return len(cr.list()) == 1 })
	if got := cr.list(); got[0] != "1|203.0.113.1" {
		t.Fatalf("closed = %v", got)
	}
}

// 后台循环随 Stop 退出，Start / Stop 可以重复（重载）；会话表跨 Stop 保留
func TestStartStopNoLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	a, _, _ := newTestAdmission(t)
	a.Admit(1, "203.0.113.1", 1, "vless", 443)
	defer a.Open(1, "203.0.113.1")()
	for i := 0; i < 5; i++ {
		a.Start()
		a.Start()
		time.Sleep(10 * time.Millisecond)
		a.Stop()
		a.Stop()
	}
	if !a.HasSession(1, "203.0.113.1") {
		t.Fatal("session table must survive Stop")
	}
	eventually(t, "goroutines exit", func() bool { return runtime.NumGoroutine() <= base+1 })
}

// 进程级实例：EnsureAdmission 跨重载复用同一张表，只替换依赖；DisableAdmission 撤掉
func TestEnsureAdmissionReusesTable(t *testing.T) {
	t.Cleanup(DisableAdmission)
	a1 := EnsureAdmission(AdmissionConfig{ServerID: 1})
	a2 := EnsureAdmission(AdmissionConfig{ServerID: 2})
	if a1 != a2 || CurrentAdmission() != a1 {
		t.Fatal("must reuse the process-level instance")
	}
	DisableAdmission()
	if CurrentAdmission() != nil {
		t.Fatal("must be disabled")
	}
}

// 并发压测：开、关、准入、保活、拒绝交错进行，计数最终归零、无数据竞争（-race -count=20）
func TestAdmissionConcurrentStress(t *testing.T) {
	a, fc, _ := newTestAdmission(t)
	var n atomic.Int64
	fc.set(func(req panel.SessionRequest) (*panel.SessionResult, error) {
		switch n.Add(1) % 7 {
		case 0:
			return nil, errors.New("down")
		case 3:
			return &panel.SessionResult{Allowed: false, Reason: "device_limit"}, nil
		}
		return &panel.SessionResult{Allowed: true}, nil
	})
	a.Start()
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				uid := 1 + (g+i)%5
				ip := fmt.Sprintf("203.0.113.%d", i%3)
				if a.Admit(uid, ip, 1+i%2, "vless", 443) {
					rel := a.Open(uid, ip)
					if i%4 == 0 {
						time.Sleep(time.Millisecond)
					}
					rel()
				}
				if i%50 == 0 {
					a.Forget(uid)
					a.Adopt(uid, "vless", 443)
				}
			}
		}(g)
	}
	wg.Wait()
	eventually(t, "drained", func() bool {
		st := a.Stats()
		return st.Entries == 0 && st.Sessions == 0 && st.Degraded == 0
	})
}
