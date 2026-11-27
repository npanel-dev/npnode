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
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

const onlineTag = "[t]-vless:6:443"

func setupOnlineLimiter(t *testing.T) *limiter.Limiter {
	t.Helper()
	panel.SetOnlineV1(true)
	limiter.Init()
	l := limiter.AddLimiter(onlineTag, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45"}}, nil)
	t.Cleanup(func() {
		limiter.DeleteLimiter(onlineTag)
		panel.SetOnlineV1(false)
	})
	return l
}

func onlineCtx(parent context.Context, ip string) context.Context {
	return session.ContextWithInbound(parent, &session.Inbound{
		Tag:    onlineTag,
		User:   &protocol.MemoryUser{Email: format.UserTag(onlineTag, "u-45")},
		Source: net.TCPDestination(net.ParseAddress(ip), 50000),
	})
}

func waitActive(t *testing.T, l *limiter.Limiter, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for l.Conns.Active() != want {
		if time.Now().After(deadline) {
			t.Fatalf("active links = %d, want %d", l.Conns.Active(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 连接建立时登记，入站关闭写端时释放
func TestGetLinkRegistersAndReleasesOnClose(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	in, out, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, l, 1)
	got, _ := l.GetOnlineDevice()
	if len(*got) != 1 || (*got)[0].UID != 1000000045 || (*got)[0].IP != "203.0.113.7" {
		t.Fatalf("online = %v", *got)
	}
	common.Close(in.Writer)
	common.Interrupt(out.Reader)
	waitActive(t, l, 0)
}

// 连接上下文结束（入站处理返回）时释放：入站出错时只 Interrupt、不 Close 的路径也不会泄漏
func TestGetLinkReleasesOnContextDone(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	if _, _, _, err := d.getLink(onlineCtx(ctx, "203.0.113.8"), net.Network_UDP); err != nil {
		t.Fatal(err)
	}
	waitActive(t, l, 1)
	cancel()
	waitActive(t, l, 0)
}

// 用户被移除时 CloseAll 断开连接，同时释放登记；之后其他钩子再触发也不会重复减计数
func TestCloseAllReleasesRegisteredLinks(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var links []*struct{ w, r any }
	for i := 0; i < 3; i++ {
		in, out, _, err := d.getLink(onlineCtx(ctx, "203.0.113.9"), net.Network_TCP)
		if err != nil {
			t.Fatal(err)
		}
		links = append(links, &struct{ w, r any }{in.Writer, out.Reader})
	}
	// 同一用户另一个 IP 的连接
	keep, _, _, err := d.getLink(onlineCtx(context.Background(), "198.51.100.1"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, l, 4)
	v, _ := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45"))
	v.(*LinkManager).CloseAll()
	// CloseAll 关闭了同一用户的全部链路（包括 198.51.100.1 那条，它们同属一个用户）
	waitActive(t, l, 0)
	for _, lk := range links {
		common.Close(lk.w)
	}
	common.Close(keep.Writer)
	cancel()
	time.Sleep(20 * time.Millisecond)
	if n := l.Conns.Active(); n != 0 {
		t.Fatalf("active went to %d after duplicate releases", n)
	}
}

// 关闭 online_v1：不登记任何连接
func TestGetLinkNoRegistrationWhenOnlineV1Off(t *testing.T) {
	panel.SetOnlineV1(false)
	limiter.Init()
	l := limiter.AddLimiter(onlineTag, []panel.UserInfo{{Id: 7, Uuid: "u-45"}}, nil)
	t.Cleanup(func() { limiter.DeleteLimiter(onlineTag) })
	d := &DefaultDispatcher{}
	in, _, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.7"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	defer common.Close(in.Writer)
	if l.Conns != nil {
		t.Fatal("connection table must not exist when online_v1 is off")
	}
}

// 并发建立、关闭、取消上下文、CloseAll（go test -race）
func TestOnlineLinksConcurrent(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				in, out, _, err := d.getLink(onlineCtx(ctx, "192.0.2.1"), net.Network_TCP)
				if err != nil {
					t.Error(err)
					cancel()
					return
				}
				switch i % 3 {
				case 0:
					common.Close(in.Writer)
				case 1:
					cancel()
				default:
					common.Interrupt(in.Writer)
					common.Interrupt(out.Reader)
				}
				cancel()
				if i%50 == 0 {
					if v, ok := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45")); ok {
						v.(*LinkManager).CloseAll()
						d.LinkManagers.Delete(format.UserTag(onlineTag, "u-45"))
					}
				}
				_, _ = l.GetOnlineDevice()
			}
		}(w)
	}
	wg.Wait()
	waitActive(t, l, 0)
}

// LinkManager 已被 CloseAll（还没从 LinkManagers 删除）时新建的链路：不在 lm 里，CloseAll 管不到，
// 但仍要靠 Close / 上下文结束释放，不能泄漏
func TestLinkOnClosedManagerStillReleased(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	first, _, _, err := d.getLink(onlineCtx(context.Background(), "203.0.113.20"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := d.LinkManagers.Load(format.UserTag(onlineTag, "u-45"))
	lm := v.(*LinkManager)
	lm.CloseAll() // 不删除，模拟并发窗口
	common.Close(first.Writer)
	waitActive(t, l, 0)

	ctx, cancel := context.WithCancel(context.Background())
	in, _, _, err := d.getLink(onlineCtx(ctx, "203.0.113.21"), net.Network_TCP)
	if err != nil {
		t.Fatal(err)
	}
	lm.mu.RLock()
	tracked := len(lm.links)
	lm.mu.RUnlock()
	if tracked != 0 {
		t.Fatalf("closed LinkManager tracks %d links", tracked)
	}
	waitActive(t, l, 1)
	common.Close(in.Writer)
	waitActive(t, l, 0)

	if _, _, _, err := d.getLink(onlineCtx(ctx, "203.0.113.22"), net.Network_TCP); err != nil {
		t.Fatal(err)
	}
	waitActive(t, l, 1)
	cancel()
	waitActive(t, l, 0)
}

// XUDP：mux 给 XUDP 的 dispatch 上下文带 TimeoutOnly，链路会被迁移到新的 mux 连接继续转发，
// 原连接上下文结束时不能释放，只能等链路自身 Close / Interrupt（XUDP 过期清理）
func TestTimeoutOnlyLinkNotReleasedOnContextDone(t *testing.T) {
	l := setupOnlineLimiter(t)
	d := &DefaultDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	in, out, _, err := d.getLink(session.ContextWithTimeoutOnly(onlineCtx(ctx, "203.0.113.30"), true), net.Network_UDP)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, l, 1)
	cancel()
	time.Sleep(30 * time.Millisecond)
	if n := l.Conns.Active(); n != 1 {
		t.Fatalf("XUDP link released when the carrying connection ended: active = %d", n)
	}
	common.Interrupt(in.Writer)
	common.Interrupt(out.Reader)
	waitActive(t, l, 0)
}

// fakeOutbounds 只提供默认出站，出站把链路交给测试
type fakeOutbounds struct {
	outbound.Manager
	h *fakeHandler
}

func (m *fakeOutbounds) GetDefaultHandler() outbound.Handler { return m.h }
func (m *fakeOutbounds) GetHandler(string) outbound.Handler  { return nil }

type fakeHandler struct {
	outbound.Handler
	links chan *transport.Link
}

func (h *fakeHandler) Tag() string { return "" }
func (h *fakeHandler) Dispatch(_ context.Context, link *transport.Link) {
	h.links <- link
}

// DispatchLink 路径（hysteria2、tuic、socks、http CONNECT、vless 等）：登记，出站关闭写端或上下文结束时释放
func TestDispatchLinkRegistersAndReleases(t *testing.T) {
	l := setupOnlineLimiter(t)
	h := &fakeHandler{links: make(chan *transport.Link, 4)}
	d := &DefaultDispatcher{ohm: &fakeOutbounds{h: h}}
	dest := net.TCPDestination(net.ParseAddress("198.51.100.9"), 443)

	// ① 出站关闭写端
	r1, w1 := pipe.New()
	if err := d.DispatchLink(onlineCtx(context.Background(), "203.0.113.40"), dest, &transport.Link{Reader: r1, Writer: w1}); err != nil {
		t.Fatal(err)
	}
	got := <-h.links
	waitActive(t, l, 1)
	online, _ := l.GetOnlineDevice()
	if len(*online) != 1 || (*online)[0].IP != "203.0.113.40" {
		t.Fatalf("online = %v", *online)
	}
	common.Close(got.Writer)
	waitActive(t, l, 0)

	// ② 连接上下文结束
	ctx, cancel := context.WithCancel(context.Background())
	r2, w2 := pipe.New()
	if err := d.DispatchLink(onlineCtx(ctx, "203.0.113.41"), dest, &transport.Link{Reader: r2, Writer: w2}); err != nil {
		t.Fatal(err)
	}
	<-h.links
	waitActive(t, l, 1)
	cancel()
	waitActive(t, l, 0)
}
