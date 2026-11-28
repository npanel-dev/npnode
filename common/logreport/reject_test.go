package logreport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

func rejected() error { return fmt.Errorf("%w: HTTP 400", ErrRejected) }

func TestRejectedBatchDroppedAndNextSent(t *testing.T) {
	s := &fakeSender{fail: func(n int) error {
		if n == 1 {
			return rejected()
		}
		return nil
	}}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	for i := 1; i <= 25; i++ { // 分为 20 + 5 两批
		r.Add(ev(CodeInboundStart, i, "x"))
	}
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatalf("被拒批次应丢弃并继续发送后续批次: %v", err)
	}
	calls := s.snapshot()
	if len(calls) != 2 || len(calls[1].Events) != 5 {
		t.Fatalf("应发送 2 次（被拒 + 下一批）: %d", len(calls))
	}
	if !r.empty() {
		t.Fatal("被拒批次应被丢弃，不再重发")
	}
	// 被拒不计入失败退避：新事件可立即发送
	r.Add(ev(CodeCertUnavailable, 443, "y"))
	if err := r.flush(context.Background(), false); err != nil || len(s.snapshot()) != 3 {
		t.Fatalf("被拒后不应退避: err=%v calls=%d", err, len(s.snapshot()))
	}
}

func writeReports(t *testing.T, path string, ids ...string) {
	t.Helper()
	var reps []*nodev1.LogReport
	for i, id := range ids {
		reps = append(reps, &nodev1.LogReport{ReportId: id, Events: []*nodev1.LogEvent{ev(CodeInboundStart, i+1, id)}})
	}
	if err := writeSpool(path, reps); err != nil {
		t.Fatal(err)
	}
}

func TestReplaySpoolDropsRejectedAndContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	writeReports(t, path, "a", "b", "c")
	s := &fakeSender{fail: func(n int) error {
		if n == 1 {
			return rejected()
		}
		return nil
	}}
	r := NewReporter(Options{Sender: s, SpoolPath: path})
	if err := r.ReplaySpool(context.Background()); err != nil {
		t.Fatalf("被拒报告应丢弃并继续: %v", err)
	}
	if calls := s.snapshot(); len(calls) != 3 {
		t.Fatalf("应尝试全部 3 份: %d", len(calls))
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("全部处理后应删除 spool")
	}
}

func TestReplaySpoolRejectedThenFailureKeepsRemainder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	writeReports(t, path, "a", "b", "c")
	s := &fakeSender{fail: func(n int) error {
		switch n {
		case 1:
			return rejected()
		case 2:
			return errSend
		}
		return nil
	}}
	r := NewReporter(Options{Sender: s, SpoolPath: path})
	if err := r.ReplaySpool(context.Background()); !errors.Is(err, errSend) {
		t.Fatalf("应返回发送错误: %v", err)
	}
	reports, _ := spoolEvents(t, path)
	if len(reports) != 2 || reports[0].ReportId != "b" || reports[1].ReportId != "c" {
		t.Fatalf("应丢弃被拒的 a、保留 b c: %v", reports)
	}
}
