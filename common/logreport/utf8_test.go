package logreport

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

func TestSanitizeFixesInvalidUTF8(t *testing.T) {
	got := Sanitize("bad \xff\xfe byte 中文")
	if !utf8.ValidString(got) {
		t.Fatalf("Sanitize 输出应为合法 UTF-8: %q", got)
	}
}

// 复现评审：一条含非法字节的事件 + 一条正常事件一起 FlushFatal，spool 必须包含两条且为合法 JSON。
func TestFlushFatalInvalidUTF8EventStillSpooled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.json")
	r := NewReporter(Options{Sender: &fakeSender{fail: alwaysFail}, SpoolPath: path, Version: "v\xff1"})
	bad := ev(CodeUnknown, 1, "boom \xff tail")
	bad.Context = []string{"ctx \xff line"}
	bad.Stage = "st\xff"
	bad.Protocol = "pr\xff"
	r.Add(bad)
	r.Add(ev(CodeConfigLoad, 0, "normal"))
	r.Resolve(&nodev1.LogResolve{Scope: "server\xff", Protocol: "p\xff", ActiveInbounds: []string{"vless:\xff443"}})
	_ = r.FlushFatal(500 * time.Millisecond)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("应写出 spool: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("spool 不是合法 JSON: %s", data)
	}
	reports, events := spoolEvents(t, path)
	if len(events) != 2 {
		t.Fatalf("spool 应含 2 条事件，实际 %d", len(events))
	}
	for _, rep := range reports {
		if _, err := proto.Marshal(rep); err != nil {
			t.Fatalf("报告应可序列化: %v", err)
		}
	}
}

func TestHookInvalidUTF8RingLineYieldsValidEvent(t *testing.T) {
	c := &collector{}
	ring := NewRing(100)
	l := newTestLogger(ring, c)
	l.Debug("noise \xff line")
	l.WithField("err", errors.New("inner \xfe")).WithField("code", "C\xff").Error("final \xff")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	e := c.ev[0]
	if _, err := proto.Marshal(e); err != nil {
		t.Fatalf("事件应可序列化: %v", err)
	}
	for _, line := range e.Context {
		if !utf8.ValidString(line) {
			t.Fatalf("context 行非法: %q", line)
		}
	}
}

// Add 直接收到含非法字节的字段（不经 Hook）也必须修正。
func TestAddFixesInvalidUTF8Fields(t *testing.T) {
	s := &fakeSender{}
	r := NewReporter(Options{Sender: s, Now: newClock().Now})
	e := &nodev1.LogEvent{Level: "error\xff", Code: "C\xff", Stage: "s\xff", Protocol: "p\xff", Message: "m\xff", Context: []string{"c\xff"}, Version: "v\xff"}
	r.Add(e)
	r.Resolve(&nodev1.LogResolve{Scope: "server\xff", ActiveInbounds: []string{"a\xff"}})
	r.mu.Lock()
	r.buildBatchLocked(true)
	b := r.batches[0]
	r.mu.Unlock()
	if _, err := proto.Marshal(b); err != nil {
		t.Fatalf("批次应可序列化: %v", err)
	}
}
