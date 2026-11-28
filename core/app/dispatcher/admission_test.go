package dispatcher

import (
	"context"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/limiter"
	"github.com/xtls/xray-core/common"
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
