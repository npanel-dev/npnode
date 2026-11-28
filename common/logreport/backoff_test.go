package logreport

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// tickHour 以 Run 的 10 秒节奏在模拟的一小时内反复 flush，返回每次实际调用 Sender 的时刻（秒）。
func tickHour(r *Reporter, s *fakeSender, clk *fakeClock, base time.Time, from int) []int {
	var at []int
	for sec := from; sec <= from+3600; sec += 10 {
		clk.Set(base, time.Duration(sec)*time.Second)
		before := len(s.snapshot())
		_ = r.flush(context.Background(), false)
		if len(s.snapshot()) > before {
			at = append(at, sec)
		}
	}
	return at
}

func TestRunBackoffExponentialAndReset(t *testing.T) {
	clk := newClock()
	base := clk.Now()
	s := &fakeSender{fail: alwaysFail}
	r := NewReporter(Options{Sender: s, Now: clk.Now})
	r.Add(ev(CodeCoreStart, 0, "a"))

	at := tickHour(r, s, clk, base, 0)
	want := []int{10, 20, 40, 80, 160, 300, 300}
	for i, w := range want {
		if i+1 >= len(at) || at[i+1]-at[i] != w {
			t.Fatalf("重试间隔应为 %v（封顶 5 分钟），实际时刻 %v", want, at)
		}
	}
	for i := len(want) + 1; i < len(at); i++ {
		if at[i]-at[i-1] != 300 {
			t.Fatalf("封顶后间隔应保持 300 秒: %v", at)
		}
	}

	// 恢复成功后退避清零：下一次 flush 立即发送
	s.mu.Lock()
	s.fail = nil
	s.mu.Unlock()
	last := at[len(at)-1]
	clk.Set(base, time.Duration(last+300)*time.Second)
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatalf("退避到期后应成功: %v", err)
	}
	r.Add(ev(CodeCertUnavailable, 443, "b"))
	n := len(s.snapshot())
	if err := r.flush(context.Background(), false); err != nil || len(s.snapshot()) != n+1 {
		t.Fatalf("成功后应立即可发送: err=%v calls=%d->%d", err, n, len(s.snapshot()))
	}
}

// 旧面板：每次都返回 ErrUnsupported（gRPC 无 Ack、HTTP 404 经 LogSender 合成的结果）。
func TestUnsupportedBacksOff10MinAndFlushFatalStillSpools(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	clk := newClock()
	base := clk.Now()
	s := &fakeSender{fail: func(int) error { return fmt.Errorf("%w: HTTP 404", ErrUnsupported) }}
	r := NewReporter(Options{Sender: s, Now: clk.Now, SpoolPath: path})
	r.Add(ev(CodeCoreStart, 0, "a"))

	at := tickHour(r, s, clk, base, 0)
	if len(at) > 8 {
		t.Fatalf("旧面板一小时内发送次数应有界（≤8），实际 %d: %v", len(at), at)
	}
	for i := 1; i < len(at); i++ {
		if at[i]-at[i-1] < 600 {
			t.Fatalf("不支持时应退避 10 分钟: %v", at)
		}
	}

	// FlushFatal 无视退避：仍尝试发送，失败后落盘
	r.Add(ev(CodeCertUnavailable, 443, "b"))
	n := len(s.snapshot())
	if err := r.FlushFatal(200 * time.Millisecond); err == nil {
		t.Fatal("应返回错误")
	}
	if len(s.snapshot()) == n {
		t.Fatal("FlushFatal 应无视退避尝试发送")
	}
	_, events := spoolEvents(t, path)
	if len(events) != 2 {
		t.Fatalf("spool 应保留 2 条事件，实际 %d", len(events))
	}
}

func TestBackoffDurationsInjectable(t *testing.T) {
	clk := newClock()
	base := clk.Now()
	s := &fakeSender{fail: alwaysFail}
	r := NewReporter(Options{Sender: s, Now: clk.Now, BackoffMin: time.Second, BackoffMax: 2 * time.Second})
	r.Add(ev(CodeCoreStart, 0, "a"))
	for sec := 0; sec <= 10; sec++ {
		clk.Set(base, time.Duration(sec)*time.Second)
		_ = r.flush(context.Background(), false)
	}
	// 0,1,3,5,7,9 共 6 次
	if n := len(s.snapshot()); n != 6 {
		t.Fatalf("注入的退避时长未生效，调用 %d 次", n)
	}
}
