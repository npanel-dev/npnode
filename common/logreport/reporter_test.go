package logreport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// fakeSender 记录每次调用，可注入错误或阻塞。
type fakeSender struct {
	mu    sync.Mutex
	calls []*nodev1.LogReport
	// fail 根据第 n 次调用（从 1 开始）返回错误；为 nil 表示总成功
	fail func(n int) error
	// block 为真时阻塞直到 ctx 取消
	block bool
	sent  chan *nodev1.LogReport
}

func (f *fakeSender) SendLogs(ctx context.Context, r *nodev1.LogReport) error {
	f.mu.Lock()
	f.calls = append(f.calls, proto.Clone(r).(*nodev1.LogReport))
	n := len(f.calls)
	fail, block, sent := f.fail, f.block, f.sent
	f.mu.Unlock()
	if sent != nil {
		select {
		case sent <- r:
		default:
		}
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if fail != nil {
		return fail(n)
	}
	return nil
}

func (f *fakeSender) snapshot() []*nodev1.LogReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*nodev1.LogReport(nil), f.calls...)
}

// fakeClock 是可手动推进的时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(base time.Time, d time.Duration) {
	c.mu.Lock()
	c.t = base.Add(d)
	c.mu.Unlock()
}

var errSend = errors.New("send failed")

func alwaysFail(int) error { return errSend }

func ev(code string, port int, msg string) *nodev1.LogEvent {
	return &nodev1.LogEvent{Level: "error", Code: code, Protocol: "vless", Port: uint32(port), Message: msg, Count: 1}
}

func TestThrottleMergesWithinWindow(t *testing.T) {
	clk := newClock()
	base := clk.Now()
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: clk.Now})
	ctx := context.Background()

	for _, sec := range []int{0, 30, 59} {
		clk.Set(base, time.Duration(sec)*time.Second)
		r.Add(ev(CodeInboundStart, 443, fmt.Sprintf("第 %d 秒", sec)))
	}
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 1 || len(calls[0].Events) != 1 || calls[0].Events[0].Count != 3 {
		t.Fatalf("首批应为 1 条 Count=3: %v", calls)
	}
	if calls[0].Events[0].Message != "第 59 秒" {
		t.Fatalf("Message 应更新为最后一次: %q", calls[0].Events[0].Message)
	}

	// 第 61 秒再次出现：处于发出后 60 秒窗口内，被暂扣
	clk.Set(base, 61*time.Second)
	r.Add(ev(CodeInboundStart, 443, "第 61 秒"))
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	if n := len(s.snapshot()); n != 1 {
		t.Fatalf("窗口内不应发送新批次，调用数 = %d", n)
	}

	// 窗口结束（发出时刻 59 秒 + 60 秒）后进入下一批，Count=1
	clk.Set(base, 119*time.Second)
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	calls = s.snapshot()
	if len(calls) != 2 || len(calls[1].Events) != 1 || calls[1].Events[0].Count != 1 {
		t.Fatalf("下一批应为 1 条 Count=1: %v", calls)
	}
}

func TestUnknownDifferentMessagesNotMerged(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	r.Add(ev(CodeUnknown, 0, "dial tcp 1.2.3.4:80 refused"))
	r.Add(ev(CodeUnknown, 0, "open file failed"))
	r.Add(ev(CodeUnknown, 0, "dial tcp 5.6.7.8:81 refused")) // 归一化后与第一条相同
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 1 || len(calls[0].Events) != 2 {
		t.Fatalf("应为 2 条: %v", calls)
	}
	if calls[0].Events[0].Count != 2 || calls[0].Events[1].Count != 1 {
		t.Fatalf("计数不符: %v", calls[0].Events)
	}
}

func TestAddFillsVersion(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Version: "v1.2.3", Now: newClock().Now})
	r.Add(ev(CodeCoreStart, 0, "a"))
	e2 := ev(CodeCertUnavailable, 1, "b")
	e2.Version = "custom"
	r.Add(e2)
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	got := s.snapshot()[0].Events
	if got[0].Version != "v1.2.3" || got[1].Version != "custom" {
		t.Fatalf("Version 不符: %v", got)
	}
}

func TestBatchOn20Events(t *testing.T) {
	s := &fakeSender{sent: make(chan *nodev1.LogReport, 4)}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	for i := 0; i < 19; i++ {
		r.Add(ev(CodeInboundStart, i+1, "x"))
	}
	select {
	case <-s.sent:
		t.Fatal("不足 20 条不应立即发送")
	case <-time.After(50 * time.Millisecond):
	}
	r.Add(ev(CodeInboundStart, 100, "x"))
	select {
	case <-s.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("满 20 条未立即发送")
	}
	calls := s.snapshot()
	if len(calls) != 1 || len(calls[0].Events) != 20 {
		t.Fatalf("应为 1 批 20 条: %d", len(calls))
	}
	cancel()
	<-done
}

func TestRetrySameReportID(t *testing.T) {
	s := &fakeSender{fail: func(n int) error {
		if n == 1 {
			return errSend
		}
		return nil
	}}
	clk := newClock()
	r := NewReporter(Options{Sender: s, Now: clk.Now})
	r.Add(ev(CodeCoreStart, 0, "a"))
	r.Resolve(&nodev1.LogResolve{Scope: ScopeServer})
	ctx := context.Background()
	if err := r.flush(ctx, false); err == nil {
		t.Fatal("第一次应失败")
	}
	clk.Set(clk.Now(), time.Minute) // 越过失败退避
	r.Add(ev(CodeCertUnavailable, 443, "后来的事件"))
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 3 {
		t.Fatalf("应调用 3 次（重发 + 新批）: %d", len(calls))
	}
	if calls[0].ReportId == "" || calls[1].ReportId != calls[0].ReportId || !proto.Equal(calls[0], calls[1]) {
		t.Fatalf("重发内容/ID 不一致: %v / %v", calls[0], calls[1])
	}
	if calls[2].ReportId == calls[0].ReportId || len(calls[2].Events) != 1 || calls[2].Events[0].Code != CodeCertUnavailable {
		t.Fatalf("新事件应进入后续批次: %v", calls[2])
	}
}

func TestNilSenderKeepsData(t *testing.T) {
	r := NewReporter(Options{Now: newClock().Now})
	r.Add(ev(CodeCoreStart, 0, "a"))
	if err := r.flush(context.Background(), false); err == nil {
		t.Fatal("无 Sender 应返回错误")
	}
	s := &fakeSender{}
	r.SetSender(s)
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if calls := s.snapshot(); len(calls) != 1 || len(calls[0].Events) != 1 {
		t.Fatalf("数据应保留至有 Sender: %v", calls)
	}
}

func TestQueueCap500DropsOldest(t *testing.T) {
	s := &fakeSender{fail: alwaysFail}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	ctx := context.Background()
	for i := 1; i <= 300; i++ {
		r.Add(ev(CodeInboundStart, i, "x"))
	}
	_ = r.flush(ctx, false) // 300 条进入未发批次
	for i := 301; i <= 600; i++ {
		r.Add(ev(CodeInboundStart, i, "x"))
	}

	ok := &fakeSender{}
	r.SetSender(ok)
	if err := r.FlushFatal(time.Second); err != nil {
		t.Fatal(err)
	}
	ports := map[uint32]bool{}
	total := 0
	for _, c := range ok.snapshot() {
		for _, e := range c.Events {
			ports[e.Port] = true
			total++
		}
	}
	if total != 500 {
		t.Fatalf("待发总数 = %d", total)
	}
	for i := 1; i <= 100; i++ {
		if ports[uint32(i)] {
			t.Fatalf("最早的事件 %d 应被丢弃", i)
		}
	}
	if !ports[101] || !ports[600] {
		t.Fatal("较新的事件应保留")
	}
}

func TestResolveKeepsLatestPerScope(t *testing.T) {
	clk := newClock()
	base := clk.Now()
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: clk.Now})
	r.Resolve(&nodev1.LogResolve{Scope: ScopeInbound, Protocol: "vless", Port: 443})
	clk.Set(base, 5*time.Second)
	r.Resolve(&nodev1.LogResolve{Scope: ScopeInbound, Protocol: "vless", Port: 443})
	r.Resolve(&nodev1.LogResolve{Scope: ScopeCert, Protocol: "vless", Port: 443, ResolvedAt: 42})
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 1 || len(calls[0].Resolves) != 2 {
		t.Fatalf("应为 1 批 2 条 resolve: %v", calls)
	}
	if got := calls[0].Resolves[0].ResolvedAt; got != base.Add(5*time.Second).UnixMilli() {
		t.Fatalf("ResolvedAt 应为后一次: %d", got)
	}
	if calls[0].Resolves[1].ResolvedAt != 42 {
		t.Fatal("已设置的 ResolvedAt 不应被覆盖")
	}
}

func TestPackageResolveDefault(t *testing.T) {
	old := Default
	defer func() { Default = old }()
	Default = nil
	Resolve(ScopeServer, "", 0, nil) // 不应 panic

	s := &fakeSender{}
	Default = NewReporter(Options{Sender: s, Now: newClock().Now})
	Resolve(ScopeServer, "", 0, []string{"vless:443"})
	if err := Default.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	rs := s.snapshot()[0].Resolves
	if len(rs) != 1 || rs[0].Scope != ScopeServer || rs[0].ActiveInbounds[0] != "vless:443" || rs[0].ResolvedAt == 0 {
		t.Fatalf("resolve 不符: %v", rs)
	}
}

func TestDefaultNewIDUnique(t *testing.T) {
	r := NewReporter(Options{})
	a, b := r.newID(), r.newID()
	if a == b || !strings.Contains(a, ":") {
		t.Fatalf("ID 不符: %q %q", a, b)
	}
}

func TestConcurrentAddAndFlush(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, SpoolPath: t.TempDir() + "/spool.json"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.Add(ev(CodeInboundStart, g*1000+i, "x"))
				r.Resolve(&nodev1.LogResolve{Scope: ScopeInbound, Port: uint32(i)})
			}
		}(g)
	}
	for i := 0; i < 5; i++ {
		_ = r.FlushFatal(time.Second)
	}
	wg.Wait()
	_ = r.FlushFatal(time.Second)
}

func TestResolveOnlyBatchesBounded(t *testing.T) {
	path := t.TempDir() + "/spool.json"
	s := &fakeSender{fail: alwaysFail}
	r := NewReporter(Options{Sender: s, SpoolPath: path, Now: newClock().Now})
	ctx := context.Background()
	r.Add(ev(CodeCoreStart, 0, "a"))
	for i := 0; i < 50; i++ {
		r.Resolve(&nodev1.LogResolve{Scope: ScopeInbound, Protocol: "vless", Port: uint32(i % 3)})
		_ = r.flush(ctx, false)
	}
	r.mu.Lock()
	nb, nr := len(r.batches), len(r.resolves)
	r.mu.Unlock()
	if nb != 1 || nr != 3 {
		t.Fatalf("批次数应有界: batches=%d resolves=%d", nb, nr)
	}

	// 留在待发中的 resolve 仍会被 FlushFatal 落盘
	if err := r.FlushFatal(200 * time.Millisecond); err == nil {
		t.Fatal("应返回错误")
	}
	reports, err := readSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, rep := range reports {
		n += len(rep.Resolves)
	}
	if n != 4 { // 首批已带 1 条 + 待发 3 条
		t.Fatalf("spool 中 resolve 数 = %d", n)
	}

	// 前面的批次发完后，待发 resolve 在下一次 flush 带出
	ok := &fakeSender{}
	r.SetSender(ok)
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx, false); err != nil {
		t.Fatal(err)
	}
	n = 0
	for _, c := range ok.snapshot() {
		n += len(c.Resolves)
	}
	if n != 4 {
		t.Fatalf("发送的 resolve 数 = %d", n)
	}
}
