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
					// 并发 CloseAll 之后 LinkManager 已关闭：链路不被跟踪，但仍要靠 Close / 上下文释放
					cancel()
					continue
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
