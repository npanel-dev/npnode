package logreport

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// bigEvent 构造截断后仍接近上限的大事件：消息 2000 字、50 行上下文各 1000 个三字节汉字（约 150 KB）。
func bigEvent(port int) *nodev1.LogEvent {
	e := ev(CodeInboundStart, port, strings.Repeat("错", 3000))
	for i := 0; i < 80; i++ {
		e.Context = append(e.Context, strings.Repeat("行", 1500))
	}
	return e
}

func TestBatchSplitsAt20Events(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, SpoolPath: path, Now: newClock().Now})
	for i := 1; i <= 50; i++ {
		r.Add(ev(CodeInboundStart, i, "x"))
	}
	if err := r.FlushFatal(time.Second); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	if len(calls) < 3 {
		t.Fatalf("50 条应至少分 3 批，实际 %d", len(calls))
	}
	total := 0
	for _, c := range calls {
		if len(c.Events) > 20 {
			t.Fatalf("单批 %d 条超过 20", len(c.Events))
		}
		total += len(c.Events)
	}
	if total != 50 {
		t.Fatalf("事件总数 = %d", total)
	}
}

func TestBatchNeverExceeds1MB(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	for i := 1; i <= 30; i++ {
		r.Add(bigEvent(i))
	}
	r.Resolve(&nodev1.LogResolve{Scope: ScopeServer, ActiveInbounds: []string{"vless:443"}})
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	calls := s.snapshot()
	total, resolves := 0, 0
	for _, c := range calls {
		b, err := spoolMarshal.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 1<<20 {
			t.Fatalf("批次 %d 字节超过 1 MB", len(b))
		}
		total += len(c.Events)
		resolves += len(c.Resolves)
	}
	if total != 30 || resolves != 1 || len(calls) < 2 {
		t.Fatalf("事件 %d / resolve %d / 批次 %d", total, resolves, len(calls))
	}
}

func TestAddTruncatesEvent(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	e := bigEvent(1)
	e.Context[len(e.Context)-1] = "最后一行"
	r.Add(e)
	if err := r.flush(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	got := s.snapshot()[0].Events[0]
	if n := utf8.RuneCountInString(got.Message); n != 2000 {
		t.Fatalf("message 应截断为 2000 字，实际 %d", n)
	}
	if len(got.Context) != 50 {
		t.Fatalf("context 应保留 50 行，实际 %d", len(got.Context))
	}
	if got.Context[49] != "最后一行" {
		t.Fatalf("应保留最近的上下文行: %q", got.Context[49])
	}
	for _, l := range got.Context {
		if utf8.RuneCountInString(l) > 1000 || !utf8.ValidString(l) {
			t.Fatalf("context 行未正确截断: %d 字", utf8.RuneCountInString(l))
		}
	}
}
