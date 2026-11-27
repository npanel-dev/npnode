package limiter

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
)

// 长连接：建立后不再新建连接，之后每个周期都要上报（O1）
func TestConnTableReportsLongLivedConnectionEveryCycle(t *testing.T) {
	ct := NewConnTable()
	release := ct.Open(1000000045, "203.0.113.7")
	for cycle := 0; cycle < 5; cycle++ {
		got := ct.Snapshot(64)
		want := []panel.OnlineUser{{UID: 1000000045, IP: "203.0.113.7"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("cycle %d: snapshot = %v, want %v", cycle, got, want)
		}
	}
	release()
	// 关闭发生在本周期内：本周期仍要上报一次，下一周期消失
	if got := ct.Snapshot(64); len(got) != 1 {
		t.Fatalf("closed-in-cycle connection must be reported once more, got %v", got)
	}
	if got := ct.Snapshot(64); len(got) != 0 {
		t.Fatalf("closed connection still reported: %v", got)
	}
}

// 本周期内建立又关闭的短连接也要计入，上报后清掉
func TestConnTableShortConnectionReportedOnce(t *testing.T) {
	ct := NewConnTable()
	ct.Open(7, "198.51.100.1")()
	if got := ct.Snapshot(64); !reflect.DeepEqual(got, []panel.OnlineUser{{UID: 7, IP: "198.51.100.1"}}) {
		t.Fatalf("short connection snapshot = %v", got)
	}
	if got := ct.Snapshot(64); len(got) != 0 {
		t.Fatalf("short connection reported twice: %v", got)
	}
}

// 同一 (uid, ip) 多条连接按引用计数：全部关闭后才消失；release 重复调用只减一次
func TestConnTableRefCountAndIdempotentRelease(t *testing.T) {
	ct := NewConnTable()
	r1 := ct.Open(7, "198.51.100.1")
	r2 := ct.Open(7, "198.51.100.1")
	r1()
	r1() // 重复释放不能把另一条连接的计数也减掉
	ct.Snapshot(64)
	if got := ct.Snapshot(64); len(got) != 1 {
		t.Fatalf("one connection still open, snapshot = %v", got)
	}
	r2()
	ct.Snapshot(64)
	if got := ct.Snapshot(64); len(got) != 0 {
		t.Fatalf("all connections closed, snapshot = %v", got)
	}
	if n := ct.Len(); n != 0 {
		t.Fatalf("table keeps %d stale entries", n)
	}
}

// 没有在线时返回空列表（非 nil，序列化为 []，O5）；结果按 uid、ip 排序
func TestConnTableEmptyAndSorted(t *testing.T) {
	ct := NewConnTable()
	if got := ct.Snapshot(64); got == nil || len(got) != 0 {
		t.Fatalf("empty snapshot = %#v, want non-nil empty", got)
	}
	ct.Open(9, "2001:db8::1")
	ct.Open(3, "203.0.113.9")
	ct.Open(3, "203.0.113.1")
	want := []panel.OnlineUser{{UID: 3, IP: "203.0.113.1"}, {UID: 3, IP: "203.0.113.9"}, {UID: 9, IP: "2001:db8::1"}}
	if got := ct.Snapshot(64); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot = %v, want %v", got, want)
	}
}

// 同一 uid 最多上报 64 个 IP（O4），活跃连接优先于本周期已关闭的
func TestConnTableCapsIPsPerUID(t *testing.T) {
	ct := NewConnTable()
	for i := 0; i < 10; i++ {
		ct.Open(5, fmt.Sprintf("10.0.0.%d", i))() // 已关闭
	}
	for i := 0; i < 100; i++ {
		ct.Open(5, fmt.Sprintf("10.1.%d.%d", i/250, i%250))
	}
	ct.Open(6, "10.9.9.9")
	got := ct.Snapshot(64)
	count := map[int]int{}
	for _, u := range got {
		count[u.UID]++
		if u.UID == 5 && u.IP[:5] == "10.0." {
			t.Fatalf("closed connection %s reported while active ones were capped", u.IP)
		}
	}
	if count[5] != 64 || count[6] != 1 {
		t.Fatalf("per-uid counts = %v, want uid 5 → 64, uid 6 → 1", count)
	}
}

// 并发建立、关闭、上报（go test -race）：计数不丢、不变负，全部关闭后表为空
func TestConnTableConcurrent(t *testing.T) {
	ct := NewConnTable()
	const workers, perWorker = 32, 500
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var snapWG sync.WaitGroup
	snapWG.Add(1)
	go func() {
		defer snapWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = ct.Snapshot(64)
			}
		}
	}()
	held := make([][]func(), workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				r := ct.Open(w%4+1, fmt.Sprintf("192.0.2.%d", i%8))
				if i%3 == 0 {
					held[w] = append(held[w], r)
					continue
				}
				// 同一连接的多个关闭钩子（连接关闭、上下文结束、CloseAll）可能并发触发
				var inner sync.WaitGroup
				for k := 0; k < 3; k++ {
					inner.Add(1)
					go func() { defer inner.Done(); r() }()
				}
				inner.Wait()
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	snapWG.Wait()
	want := 0
	for _, h := range held {
		want += len(h)
	}
	if got := ct.Active(); got != want {
		t.Fatalf("active connections = %d, want %d", got, want)
	}
	for _, h := range held {
		for _, r := range h {
			r()
		}
	}
	ct.Snapshot(64)
	ct.Snapshot(64)
	if ct.Len() != 0 || ct.Active() != 0 {
		t.Fatalf("table not empty after all closed: len=%d active=%d", ct.Len(), ct.Active())
	}
}

func withOnlineV1(t *testing.T, on bool) {
	t.Helper()
	panel.SetOnlineV1(on)
	t.Cleanup(func() { panel.SetOnlineV1(false) })
}

// 关闭 online_v1（默认）：不登记连接，在线口径与旧版一致（新建连接时登记、上报后清空）
func TestLimiterOnlineLegacyWhenOnlineV1Off(t *testing.T) {
	withOnlineV1(t, false)
	Init()
	const tag = "online-legacy"
	l := AddLimiter(tag, []panel.UserInfo{{Id: 7, Uuid: "u-7"}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	if l.OpenConn(format.UserTag(tag, "u-7"), "203.0.113.7") != nil {
		t.Fatal("OpenConn must be a no-op when online_v1 is off")
	}
	l.CheckLimit(format.UserTag(tag, "u-7"), "203.0.113.7", true, true)
	got, _ := l.GetOnlineDevice()
	if !reflect.DeepEqual(*got, []panel.OnlineUser{{UID: 7, IP: "203.0.113.7"}}) {
		t.Fatalf("legacy first report = %v", *got)
	}
	got, _ = l.GetOnlineDevice()
	if *got != nil {
		t.Fatalf("legacy second report must be empty (nil), got %#v", *got)
	}
}

// 打开 online_v1：按活跃连接上报；uid 取自限速器里的用户信息，IPv4 映射地址去掉 ::ffff:
func TestLimiterOnlineActiveConnections(t *testing.T) {
	withOnlineV1(t, true)
	Init()
	const tag = "online-active"
	l := AddLimiter(tag, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45"}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	if l.OpenConn(format.UserTag(tag, "unknown"), "203.0.113.7") != nil {
		t.Fatal("unknown user must not be registered")
	}
	if l.OpenConn(format.UserTag(tag, "u-45"), "<nil>") != nil {
		t.Fatal("invalid ip must not be registered")
	}
	release := l.OpenConn(format.UserTag(tag, "u-45"), "::ffff:203.0.113.7")
	if release == nil {
		t.Fatal("OpenConn returned nil for a known user")
	}
	for i := 0; i < 3; i++ {
		got, _ := l.GetOnlineDevice()
		if !reflect.DeepEqual(*got, []panel.OnlineUser{{UID: 1000000045, IP: "203.0.113.7"}}) {
			t.Fatalf("cycle %d report = %v", i, *got)
		}
	}
	release()
	l.GetOnlineDevice()
	got, _ := l.GetOnlineDevice()
	if got == nil || *got == nil || len(*got) != 0 {
		t.Fatalf("report after close = %#v, want non-nil empty", got)
	}
	// 重建限速器（重载、入站重启）沿用同一张连接表，已有连接不丢
	r2 := l.OpenConn(format.UserTag(tag, "u-45"), "203.0.113.8")
	l2 := AddLimiter(tag, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45"}}, nil)
	got, _ = l2.GetOnlineDevice()
	if len(*got) != 1 {
		t.Fatalf("rebuilt limiter lost active connection: %v", *got)
	}
	r2()
}
