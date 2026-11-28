package logreport

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

type collector struct {
	mu sync.Mutex
	ev []*nodev1.LogEvent
}

func (c *collector) add(e *nodev1.LogEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ev = append(c.ev, e)
}

func newTestLogger(ring *Ring, c *collector) *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	l.SetOutput(&bytes.Buffer{})
	l.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true, DisableQuote: true})
	l.AddHook(NewHook(ring, c.add))
	return l
}

func TestCodedWarnBecomesEvent(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.WithField("err", Wrap(CodeCertUnavailable, StageCert, errors.New("acme"))).Warn("证书不可用")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	e := c.ev[0]
	if e.Code != CodeCertUnavailable || e.Level != "warn" || e.Stage != StageCert || e.Count != 1 || e.Version != "" {
		t.Fatalf("事件不符: %+v", e)
	}
	if !strings.Contains(e.Message, "证书不可用") || !strings.Contains(e.Message, "acme") {
		t.Fatalf("message = %q", e.Message)
	}
	if e.OccurredAt == 0 {
		t.Fatal("OccurredAt 未设置")
	}
}

func TestUncodedWarnIgnoredUncodedErrorUnknown(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.Warn("证书申请成功")
	if len(c.ev) != 0 {
		t.Fatalf("无码 Warn 不应产生事件")
	}
	l.Error("boom")
	if len(c.ev) != 1 || c.ev[0].Code != CodeUnknown || c.ev[0].Level != "error" {
		t.Fatalf("事件不符: %+v", c.ev)
	}
}

func TestFieldsFallbackAndExiting(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.WithFields(logrus.Fields{"code": CodeReload, "stage": StageRuntime, "protocol": "vless", "port": 443, "exiting": true}).Warn("x")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	e := c.ev[0]
	if e.Code != CodeReload || e.Stage != StageRuntime || e.Protocol != "vless" || e.Port != 443 || !e.Exiting {
		t.Fatalf("事件不符: %+v", e)
	}
}

func TestEventContextFromRing(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	for i := 0; i < 60; i++ {
		l.Debug("noise")
	}
	l.Error("the-final-error")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	ctx := c.ev[0].Context
	if len(ctx) != 50 || !strings.Contains(ctx[49], "the-final-error") {
		t.Fatalf("context 长度 %d, 末行 %q", len(ctx), ctx[len(ctx)-1])
	}
}

func TestOutputFilteredButRingKeepsDebug(t *testing.T) {
	std := logrus.StandardLogger()
	oldLevel, oldOut, oldFmt := std.GetLevel(), std.Out, std.Formatter
	oldHooks := std.ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() {
		std.SetLevel(oldLevel)
		std.SetOutput(oldOut)
		std.SetFormatter(oldFmt)
		std.ReplaceHooks(oldHooks)
	})
	std.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true, DisableQuote: true})

	ring := NewRing(100)
	var buf bytes.Buffer
	Install(ring, func(*nodev1.LogEvent) {}, logrus.WarnLevel, &buf)
	logrus.Debug("dbg-line")
	logrus.Warn("warn-line")
	out := buf.String()
	if strings.Contains(out, "dbg-line") || !strings.Contains(out, "warn-line") {
		t.Fatalf("输出 = %q", out)
	}
	last := ring.Last(10)
	if len(last) != 2 || !strings.Contains(last[0], "dbg-line") || !strings.Contains(last[1], "warn-line") {
		t.Fatalf("ring = %v", last)
	}
}

func TestHookSanitizes(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.Error("Post https://p?secret_key=s3cr3t")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	if strings.Contains(c.ev[0].Message, "s3cr3t") {
		t.Fatalf("message 泄漏: %q", c.ev[0].Message)
	}
	for _, line := range c.ev[0].Context {
		if strings.Contains(line, "s3cr3t") {
			t.Fatalf("context 泄漏: %q", line)
		}
	}
}

func TestLevelWriter(t *testing.T) {
	var buf bytes.Buffer
	w := &LevelWriter{Out: &buf, Level: logrus.WarnLevel}
	for _, s := range []string{
		"level=debug msg=a\n", "level=info msg=b\n", "level=warning msg=c\n",
		"level=error msg=d\n", "plain line\n",
	} {
		if n, err := w.Write([]byte(s)); err != nil || n != len(s) {
			t.Fatalf("Write(%q) = %d, %v", s, n, err)
		}
	}
	got := buf.String()
	if got != "level=warning msg=c\nlevel=error msg=d\nplain line\n" {
		t.Fatalf("输出 = %q", got)
	}
}

func TestRingWraps(t *testing.T) {
	r := NewRing(3)
	for _, s := range []string{"1", "2", "3", "4"} {
		r.Add(s)
	}
	got := r.Last(10)
	if len(got) != 3 || got[0] != "2" || got[2] != "4" {
		t.Fatalf("got %v", got)
	}
	if g := r.Last(2); len(g) != 2 || g[0] != "3" {
		t.Fatalf("Last(2) = %v", g)
	}
}

func TestEmptyStageDefaultsToRuntime(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.Error("boom")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	if c.ev[0].Stage != StageRuntime {
		t.Fatalf("Stage = %q, 期望 %q", c.ev[0].Stage, StageRuntime)
	}
}

func TestExplicitStageKept(t *testing.T) {
	c := &collector{}
	l := newTestLogger(NewRing(100), c)
	l.WithFields(logrus.Fields{"code": CodeReload, "stage": StageCore}).Error("x")
	if len(c.ev) != 1 {
		t.Fatalf("事件数 = %d", len(c.ev))
	}
	if c.ev[0].Stage != StageCore {
		t.Fatalf("Stage = %q, 期望 %q", c.ev[0].Stage, StageCore)
	}
}
