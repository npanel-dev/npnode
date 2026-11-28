package dispatcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/limiter"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

// CloseByIP 只关闭该 IP 的链路并释放登记，不置 closed：同一用户其他 IP 的链路保留，之后的新链路仍纳入管理
func TestCloseByIPClosesOnlyThatIP(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	var victims []*struct{ w, r any }
	for i := 0; i < 3; i++ {
		in, out, _, err := d.getLink(onlineCtx(context.Background(), "::ffff:203.0.113.9"), net.Network_TCP)
		if err != nil {
			t.Fatal(err)
		}
		victims = append(victims, &struct{ w, r any }{in.Writer, out.Reader})
	}
	keep, _, _, err := d.getLink(onlineCtx(context.Background(), "198.51.100.1"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer common.Close(keep.Writer)
	waitActive(t, l, 4)
	v, _ := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45"))
	lm := v.(*LinkManager)
	lm.CloseByIP("203.0.113.9")
	waitActive(t, l, 1)
	lm.mu.RLock()
	n, closed := len(lm.links), lm.closed
	lm.mu.RUnlock()
	if n != 1 || closed {
		t.Fatalf("links=%d closed=%v, want 1 / false", n, closed)
	}
	// 新链路仍加入同一个 LinkManager
	again, _, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.9"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer common.Close(again.Writer)
	lm.mu.RLock()
	n = len(lm.links)
	lm.mu.RUnlock()
	if n != 2 {
		t.Fatalf("links = %d, want 2", n)
	}
	for _, lk := range victims {
		common.Close(lk.w) // 重复释放不影响计数
	}
	waitActive(t, l, 2)
}

// admission_v1：getLink 在 CheckLimit 里准入，面板拒绝时不建链路；放行的链路登记到准入会话表
func TestGetLinkAdmission(t *testing.T) {
	panel.SetAdmissionV1(true)
	limiter.Init()
	fc := &denyChecker{deny: map[string]bool{"203.0.113.66": true}}
	limiter.EnsureAdmission(limiter.AdmissionConfig{Checker: fc, ServerID: 1})
	l := limiter.AddInboundLimiter(onlineTag, "vless", 443, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45", DeviceLimit: 1}}, nil)
	t.Cleanup(func() {
		limiter.DeleteLimiter(onlineTag)
		limiter.DisableAdmission()
		panel.SetAdmissionV1(false)
	})
	if _, _, _, err := d0().getLink(onlineCtx(context.Background(), "203.0.113.66"), net.Network_TCP); err == nil {
		t.Fatal("denied by panel must fail getLink")
	}
	in, _, _, err := d0().getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Admission.HasSession(1000000045, "203.0.113.7") || l.Admission.Stats().Entries != 1 {
		t.Fatalf("stats = %+v", l.Admission.Stats())
	}
	common.Close(in.Writer)
	if st := l.Admission.Stats(); st.Queued != 1 {
		t.Fatalf("stats after close = %+v, want release queued", st)
	}
}

func d0() *DefaultDispatcher { return &DefaultDispatcher{} }

type denyChecker struct{ deny map[string]bool }

func (c *denyChecker) Check(_ context.Context, req panel.SessionRequest) (*panel.SessionResult, error) {
	return &panel.SessionResult{Allowed: !c.deny[req.ClientIP]}, nil
}

func (c *denyChecker) Release(context.Context, panel.SessionRequest) error { return nil }

// CloseByIP 与建立、关闭、CloseAll 并发（-race -count=20），计数最终归零
func TestCloseByIPConcurrent(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 150; i++ {
				ip := []string{"192.0.2.1", "192.0.2.2"}[i%2]
				in, _, _, err := d.getLink(onlineCtx(context.Background(), ip), net.Network_TCP)
				if err != nil {
					t.Error(err)
					return
				}
				if v, ok := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45")); ok {
					switch i % 10 {
					case 3:
						v.(*LinkManager).CloseByIP(ip)
					case 7:
						v.(*LinkManager).CloseAll()
						d.LinkManagers.Delete(format.UserTag(onlineTag, "u-45"))
					}
				}
				common.Close(in.Writer)
			}
		}(w)
	}
	for w := 0; w < 8; w++ {
		<-done
	}
	waitActive(t, l, 0)
}

// scriptChecker 首次 check 放行，之后按 deny 决定（用于保活被拒）
type scriptChecker struct {
	mu   sync.Mutex
	deny bool
}

func (c *scriptChecker) set(deny bool) { c.mu.Lock(); c.deny = deny; c.mu.Unlock() }

func (c *scriptChecker) Check(context.Context, panel.SessionRequest) (*panel.SessionResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &panel.SessionResult{Allowed: !c.deny, Reason: "device_limit"}, nil
}

func (c *scriptChecker) Release(context.Context, panel.SessionRequest) error { return nil }

func setupAdmissionLimiter(t *testing.T, speed int) (*limiter.Limiter, *limiter.Admission, *scriptChecker) {
	t.Helper()
	panel.SetAdmissionV1(true)
	panel.SetSeatV1(true)
	limiter.Init()
	sc := &scriptChecker{}
	a := limiter.EnsureAdmission(limiter.AdmissionConfig{Checker: sc, ServerID: 1})
	a.KeepaliveMin, a.KeepaliveMax = 10*time.Millisecond, 10*time.Millisecond
	a.ScanInterval = 5 * time.Millisecond
	a.DenyCacheTTL = 0
	l := limiter.AddInboundLimiter(onlineTag, "vless", 443, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45", DeviceLimit: 1, SpeedLimit: speed}}, nil)
	t.Cleanup(func() {
		limiter.DeleteLimiter(onlineTag)
		limiter.DisableAdmission()
		panel.SetAdmissionV1(false)
		panel.SetSeatV1(false)
	})
	return l, a, sc
}

func linkCount(d *DefaultDispatcher) int {
	v, ok := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45"))
	if !ok {
		return 0
	}
	return v.(*LinkManager).Len()
}

// V2：保活被拒时按链路登记的关闭函数关闭，重载前旧 dispatcher 上的链路也能关掉（Closer 只认识当前内核）
func TestDeniedKeepaliveClosesLinksOnAllDispatchers(t *testing.T) {
	l, a, sc := setupAdmissionLimiter(t, 0)
	oldD, newD := &DefaultDispatcher{}, &DefaultDispatcher{}
	if _, _, _, err := oldD.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := newD.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP); err != nil {
		t.Fatal(err)
	}
	if linkCount(oldD) != 1 || linkCount(newD) != 1 {
		t.Fatal("links")
	}
	sc.set(true)
	a.Start()
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: old=%d new=%d active=%d", linkCount(oldD), linkCount(newD), l.Conns.Active())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(func() bool { return linkCount(oldD) == 0 && linkCount(newD) == 0 && l.Conns.Active() == 0 })
}

// M4：Admit 放行后、链路登记前该 (uid, ip) 已被拒绝移除：登记后发现已撤销，立即关闭并让 getLink 失败
func TestGetLinkRevokedAfterAdmit(t *testing.T) {
	l, a, _ := setupAdmissionLimiter(t, 0)
	key := format.UserTag(onlineTag, "u-45")
	if _, reject := l.CheckLimit(key, "203.0.113.7", true, true); reject {
		t.Fatal("allowed")
	}
	a.Forget(1000000045) // 模拟会话在登记前被移除（与保活被拒效果相同）
	if _, _, _, err := (&DefaultDispatcher{}).getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP); err != nil {
		t.Fatal("getLink re-admits through CheckLimit, must succeed:", err)
	}
	// 直接验证登记后撤销判断：没有会话的受管链路
	a.Forget(1000000045)
	if !l.Revoked(key, "203.0.113.7") {
		t.Fatal("must be revoked without session")
	}
	d := &DefaultDispatcher{}
	revokeNext = func() { a.Forget(1000000045) }
	t.Cleanup(func() { revokeNext = nil })
	if _, _, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.8"), net.Network_TCP); err == nil {
		t.Fatal("revoked link must fail")
	}
	if linkCount(d) != 0 {
		t.Fatalf("revoked link left in manager: %d", linkCount(d))
	}
}

func drain(r buf.Reader) {
	for {
		mb, err := r.ReadMultiBuffer()
		buf.ReleaseMulti(mb)
		if err != nil {
			return
		}
	}
}

func writeN(t *testing.T, w buf.Writer, n int) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	for n > 0 {
		b := buf.New()
		sz := min(n, int(b.Cap()))
		b.Extend(int32(sz))
		if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
			return time.Since(start), err
		}
		n -= sz
	}
	return time.Since(start), nil
}

// I4（裁定调整）：admission_v1 下原本不限速的已有连接（没有限速写端、可走 splice），改为限速时被关闭，重连后按新限速
func TestExistingUnlimitedLinkClosedWhenLimited(t *testing.T) {
	l, _, _ := setupAdmissionLimiter(t, 0)
	d := &DefaultDispatcher{}
	in, out, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer common.Close(in.Writer)
	go drain(out.Reader)
	if linkCount(d) != 1 {
		t.Fatal("link")
	}
	l.UpdateUserLimit(format.UserTag(onlineTag, "u-45"), 160000, 1)
	if linkCount(d) != 0 || l.Conns.Active() != 0 {
		t.Fatalf("unlimited→limited must close existing link: links=%d active=%d", linkCount(d), l.Conns.Active())
	}
	if _, err := writeN(t, in.Writer, 1024); err == nil {
		t.Fatal("closed link must not accept writes")
	}
}

// 限速 → 不限速：已有连接原地变为不限速、不断开
func TestExistingLimitedLinkUnlimitedInPlace(t *testing.T) {
	l, _, _ := setupAdmissionLimiter(t, 160000) // 20000 B/s
	d := &DefaultDispatcher{}
	in, out, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer common.Close(in.Writer)
	go drain(out.Reader)
	if dur, err := writeN(t, in.Writer, 40000); err != nil || dur < 700*time.Millisecond {
		t.Fatalf("limited write took %v err=%v", dur, err)
	}
	l.UpdateUserLimit(format.UserTag(onlineTag, "u-45"), 0, 1)
	if dur, err := writeN(t, in.Writer, 256<<10); err != nil || dur > 300*time.Millisecond {
		t.Fatalf("unlimited write took %v err=%v", dur, err)
	}
	if linkCount(d) != 1 {
		t.Fatal("connection must stay open")
	}
}
