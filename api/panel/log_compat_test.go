package panel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/common/logreport"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func statusServer(t *testing.T, code int, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestReportLogsHTTPNotFoundIsUnsupported(t *testing.T) {
	for _, code := range []int{404, 405} {
		var hits atomic.Int32
		c := newLogClient(t, statusServer(t, code, &hits).URL)
		err := c.ReportLogs(context.Background(), sampleLogReport())
		if !errors.Is(err, logreport.ErrUnsupported) {
			t.Fatalf("HTTP %d 应返回 ErrUnsupported: %v", code, err)
		}
	}
	var hits atomic.Int32
	c := newLogClient(t, statusServer(t, 500, &hits).URL)
	if err := c.ReportLogs(context.Background(), sampleLogReport()); err == nil || errors.Is(err, logreport.ErrUnsupported) {
		t.Fatalf("HTTP 500 应为普通可重试错误: %v", err)
	}
}

// Ack 超时后本进程内关闭 gRPC 日志通道一段时间，后续直接走 HTTP；到期后恢复尝试 gRPC。
func TestLogSenderDisablesGrpcLogsAfterAckTimeout(t *testing.T) {
	gc, srv := startLogAckClient(t, "silent")
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	gc.logAckTimeout = 100 * time.Millisecond
	gc.now = clk.Now
	var hits atomic.Int32
	s := LogSender{Grpc: gc, HTTP: newLogClient(t, statusServer(t, 200, &hits).URL)}

	if err := s.SendLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatalf("gRPC 无 Ack 应回退 HTTP 成功: %v", err)
	}
	<-srv.got
	start := time.Now()
	if err := s.SendLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 80*time.Millisecond {
		t.Fatal("gRPC 日志通道应已关闭，不应再等待 Ack")
	}
	select {
	case <-srv.got:
		t.Fatal("关闭期间不应再经 gRPC 发送日志")
	default:
	}
	if hits.Load() != 2 {
		t.Fatalf("HTTP 调用次数 = %d", hits.Load())
	}

	// 关闭期间流量等其它 Sync 上报不受影响
	if !gc.IsSyncConnected() {
		t.Fatal("Sync 流应保持连接")
	}

	clk.Add(11 * time.Minute)
	_ = s.SendLogs(context.Background(), sampleLogReport())
	select {
	case <-srv.got:
	case <-time.After(time.Second):
		t.Fatal("到期后应重新尝试 gRPC")
	}
}

// 模拟旧面板（gRPC 永不 Ack + HTTP 404）：一小时内的发送尝试有界，FlushFatal 仍落盘。
func TestOldPanelBoundedAttemptsPerHour(t *testing.T) {
	gc, srv := startLogAckClient(t, "silent")
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	gc.logAckTimeout = 50 * time.Millisecond
	gc.now = clk.Now
	var hits atomic.Int32
	sender := LogSender{Grpc: gc, HTTP: newLogClient(t, statusServer(t, 404, &hits).URL)}

	path := t.TempDir() + "/spool.json"
	r := logreport.NewReporter(logreport.Options{Sender: sender, Now: clk.Now, SpoolPath: path})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Add(&nodev1.LogEvent{Level: "error", Code: logreport.CodeCoreStart, Message: "boom"})

	grpcSends := 0
	for i := 0; i <= 360; i++ { // 每 10 秒一轮，共一小时
		_ = r.Flush(ctx)
		for drained := false; !drained; {
			select {
			case <-srv.got:
				grpcSends++
			default:
				drained = true
			}
		}
		clk.Add(10 * time.Second)
	}
	t.Logf("一小时内 HTTP 尝试 %d 次，gRPC 尝试 %d 次", hits.Load(), grpcSends)
	if n := int(hits.Load()); n > 8 {
		t.Fatalf("一小时内 HTTP 尝试 %d 次，应 ≤ 8", n)
	}
	if grpcSends > 8 {
		t.Fatalf("一小时内 gRPC 尝试 %d 次，应 ≤ 8", grpcSends)
	}
	if err := r.FlushFatal(300 * time.Millisecond); err == nil {
		t.Fatal("旧面板 FlushFatal 应返回错误")
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "boom") {
		t.Fatalf("应落盘: %v %s", err, data)
	}
}
