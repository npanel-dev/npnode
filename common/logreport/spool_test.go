package logreport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

func spoolEvents(t *testing.T, path string) (reports []*nodev1.LogReport, events []*nodev1.LogEvent) {
	t.Helper()
	reports, err := readSpool(path)
	if err != nil {
		t.Fatalf("解析 spool 失败: %v", err)
	}
	for _, r := range reports {
		events = append(events, r.Events...)
	}
	return reports, events
}

func TestFlushFatalTimeoutWritesSpoolWithinBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "spool.json")
	s := &fakeSender{block: true}
	r := NewReporter(Options{Sender: s, SpoolPath: path, Now: newClock().Now})
	for i := 1; i <= 30; i++ {
		r.Add(ev(CodeInboundStart, i, "x"))
	}
	r.Resolve(&nodev1.LogResolve{Scope: ScopeServer})

	const timeout = 300 * time.Millisecond
	start := time.Now()
	err := r.FlushFatal(timeout)
	if el := time.Since(start); el > timeout+500*time.Millisecond {
		t.Fatalf("超出预算: %v", el)
	}
	if err == nil {
		t.Fatal("超时应返回错误")
	}
	reports, events := spoolEvents(t, path)
	if len(events) != 30 {
		t.Fatalf("spool 事件数 = %d", len(events))
	}
	// 30 条按每批 20 条分为 2 批，resolve 附在最后一批
	if len(reports) != 2 || reports[0].ReportId == "" || reports[1].ReportId == "" || len(reports[1].Resolves) != 1 {
		t.Fatalf("spool 报告不符: %v", reports)
	}
}

func TestFlushFatalWhileRunSendBlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	s := &fakeSender{block: true, sent: make(chan *nodev1.LogReport, 1)}
	r := NewReporter(Options{Sender: s, SpoolPath: path, Now: newClock().Now})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	for i := 1; i <= 20; i++ {
		r.Add(ev(CodeInboundStart, i, "x"))
	}
	select {
	case <-s.sent: // Run 的发送已开始并阻塞
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未发送")
	}
	r.Add(ev(CodeCoreStart, 0, "late"))

	const timeout = 300 * time.Millisecond
	start := time.Now()
	if err := r.FlushFatal(timeout); err == nil {
		t.Fatal("应返回错误")
	}
	if el := time.Since(start); el > timeout+500*time.Millisecond {
		t.Fatalf("超出预算: %v", el)
	}
	_, events := spoolEvents(t, path)
	if len(events) != 21 {
		t.Fatalf("spool 应包含全部 21 条: %d", len(events))
	}
}

func TestFlushFatalAllSentNoSpool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	s := &fakeSender{}
	clk := newClock()
	r := NewReporter(Options{Sender: s, SpoolPath: path, Now: clk.Now})
	r.Add(ev(CodeCoreStart, 0, "a"))
	_ = r.flush(context.Background(), false)
	r.Add(ev(CodeCoreStart, 0, "b")) // 处于暂扣窗口内，FlushFatal 也要发出
	if err := r.FlushFatal(time.Second); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 2 || calls[1].Events[0].Message != "b" {
		t.Fatalf("暂扣事件应被发出: %v", calls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("全部发送成功不应写 spool")
	}
}

func TestSpoolAppendPreservesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	r1 := NewReporter(Options{Sender: &fakeSender{fail: alwaysFail}, SpoolPath: path})
	r1.Add(ev(CodeCoreStart, 0, "first"))
	_ = r1.FlushFatal(200 * time.Millisecond)
	_ = r1.FlushFatal(200 * time.Millisecond) // 重复落盘同一 ReportId 不应重复

	r2 := NewReporter(Options{Sender: &fakeSender{fail: alwaysFail}, SpoolPath: path})
	r2.Add(ev(CodeCertUnavailable, 443, "second"))
	_ = r2.FlushFatal(200 * time.Millisecond)

	reports, events := spoolEvents(t, path)
	if len(reports) != 2 || len(events) != 2 || events[0].Message != "first" || events[1].Message != "second" {
		t.Fatalf("应保留旧报告并追加新报告: %v", reports)
	}
}

func TestSpoolCappedAt1MB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	r := NewReporter(Options{Sender: &fakeSender{fail: alwaysFail}, SpoolPath: path, Now: newClock().Now})
	// message 截断到 2000 字，靠上下文把单条撑到约 5 KB，400 条约 2 MB
	big := strings.Repeat("a", 2000)
	ctx := context.Background()
	for i := 1; i <= 400; i++ {
		e := ev(CodeInboundStart, i, big)
		e.Context = []string{strings.Repeat("b", 1000), strings.Repeat("c", 1000), strings.Repeat("d", 1000)}
		r.Add(e)
		if i%100 == 0 {
			_ = r.flush(ctx, false) // 形成多个批次
		}
	}
	if err := r.FlushFatal(200 * time.Millisecond); err == nil {
		t.Fatal("应返回错误")
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() > spoolMaxBytes {
		t.Fatalf("spool 大小 %d 超过上限", st.Size())
	}
	_, events := spoolEvents(t, path)
	if len(events) == 0 || len(events) >= 400 {
		t.Fatalf("事件数 = %d", len(events))
	}
	if events[len(events)-1].Port != 400 {
		t.Fatal("应保留最新事件")
	}
	for _, e := range events {
		if e.Port == 1 {
			t.Fatal("最旧事件应被丢弃")
		}
	}
}

// writeTwoReportSpool 构造包含两个报告的 spool 文件，返回两份报告 ID。
func writeTwoReportSpool(t *testing.T, path string) []string {
	t.Helper()
	r := NewReporter(Options{Sender: &fakeSender{fail: alwaysFail}, SpoolPath: path, Now: newClock().Now})
	r.Add(ev(CodeCoreStart, 0, "one"))
	_ = r.flush(context.Background(), false)
	r.Add(ev(CodeCertUnavailable, 443, "two"))
	_ = r.FlushFatal(200 * time.Millisecond)
	reports, _ := spoolEvents(t, path)
	if len(reports) != 2 {
		t.Fatalf("准备 spool 失败: %d", len(reports))
	}
	return []string{reports[0].ReportId, reports[1].ReportId}
}

func TestReplaySpoolDeletesOnSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	ids := writeTwoReportSpool(t, path)
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, SpoolPath: path})
	if err := r.ReplaySpool(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) != 2 || calls[0].ReportId != ids[0] || calls[1].ReportId != ids[1] {
		t.Fatalf("应按序用原 ID 重放: %v", calls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("全部成功后应删除 spool")
	}
	// 文件不存在时返回 nil
	if err := r.ReplaySpool(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReplaySpoolKeepsOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	ids := writeTwoReportSpool(t, path)
	s := &fakeSender{fail: func(n int) error {
		if n == 2 {
			return errSend
		}
		return nil
	}}
	r := NewReporter(Options{Sender: s, SpoolPath: path})
	if err := r.ReplaySpool(context.Background()); !errors.Is(err, errSend) {
		t.Fatalf("应返回发送错误: %v", err)
	}
	reports, _ := spoolEvents(t, path)
	if len(reports) != 1 || reports[0].ReportId != ids[1] {
		t.Fatalf("应只保留未发送的剩余报告: %v", reports)
	}
}
