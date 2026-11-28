package cmd

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/common/logreport"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// recSender 记录调用次数，block 为真时阻塞到 ctx 取消。
type recSender struct {
	mu    sync.Mutex
	n     int
	block bool
}

func (s *recSender) SendLogs(ctx context.Context, _ *nodev1.LogReport) error {
	s.mu.Lock()
	s.n++
	block := s.block
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (s *recSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func testEvent() *nodev1.LogEvent {
	return &nodev1.LogEvent{Level: "error", Code: logreport.CodePanelConfig, Message: "获取服务端配置失败", Exiting: true}
}

func TestFatalExitFlushesThenExits1(t *testing.T) {
	s := &recSender{}
	r := logreport.NewReporter(logreport.Options{Sender: s, SpoolPath: filepath.Join(t.TempDir(), "pending-logs.json")})
	r.Add(testEvent())

	got := -1
	sentBeforeExit := -1
	fatalExit(r, func(code int) {
		sentBeforeExit = s.count()
		got = code
	})
	if got != 1 {
		t.Fatalf("退出码 = %d，期望 1", got)
	}
	if sentBeforeExit < 1 {
		t.Fatalf("exit 之前 Sender 未被调用（调用次数 %d）", sentBeforeExit)
	}
}

func TestFatalExitWithoutSenderSpools(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "pending-logs.json")
	r := logreport.NewReporter(logreport.Options{SpoolPath: spool})
	r.Add(testEvent())

	got := -1
	fatalExit(r, func(code int) { got = code })
	if got != 1 {
		t.Fatalf("退出码 = %d，期望 1", got)
	}
	if _, err := os.Stat(spool); err != nil {
		t.Fatalf("无 Sender 时应落盘: %v", err)
	}
}

func TestReplaySpoolFailureDoesNotBlock(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "pending-logs.json")
	// 先用无 Sender 的 Reporter 落盘一份报告
	w := logreport.NewReporter(logreport.Options{SpoolPath: spool})
	w.Add(testEvent())
	_ = w.FlushFatal(time.Second)
	if _, err := os.Stat(spool); err != nil {
		t.Fatalf("准备 spool 失败: %v", err)
	}

	s := &recSender{block: true}
	r := logreport.NewReporter(logreport.Options{Sender: s, SpoolPath: spool})
	start := time.Now()
	replaySpool(context.Background(), r)
	if d := time.Since(start); d > 5500*time.Millisecond {
		t.Fatalf("replaySpool 耗时 %v，超过 5.5 秒", d)
	}
	if s.count() < 1 {
		t.Fatal("replaySpool 未尝试发送")
	}
	if _, err := os.Stat(spool); err != nil {
		t.Fatalf("发送失败后 spool 文件应保留: %v", err)
	}
}
