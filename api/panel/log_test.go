package panel

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func sampleLogReport() *nodev1.LogReport {
	return &nodev1.LogReport{
		ReportId: "b:1",
		Events:   []*nodev1.LogEvent{{Code: "VLESS_ENCRYPTION_MODE_EMPTY"}},
	}
}

func newLogClient(t *testing.T, url string) *ClientV2 {
	t.Helper()
	return NewClientV2(&conf.ServerApiConfig{ApiHost: url, SecretKey: "k", ServerId: 9, Timeout: 2})
}

func TestReportLogsHTTP(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	var gotPath, gotCT string
	var gotQuery map[string]string
	var gotReport nodev1.LogReport
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		gotQuery = map[string]string{"server_id": r.URL.Query().Get("server_id"), "secret_key": r.URL.Query().Get("secret_key")}
		body, _ := io.ReadAll(r.Body)
		_ = protojson.Unmarshal(body, &gotReport)
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	c := newLogClient(t, srv.URL)

	if err := c.ReportLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatalf("200 应成功: %v", err)
	}
	if gotPath != "/v1/server/log" || gotQuery["server_id"] != "9" || gotQuery["secret_key"] != "k" {
		t.Fatalf("请求不符: %s %v", gotPath, gotQuery)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Fatalf("Content-Type=%q", gotCT)
	}
	if gotReport.GetReportId() != "b:1" || gotReport.GetEvents()[0].GetCode() != "VLESS_ENCRYPTION_MODE_EMPTY" {
		t.Fatalf("body 不符: %v", &gotReport)
	}

	status.Store(403)
	err := c.ReportLogs(context.Background(), sampleLogReport())
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("403 应返回含 HTTP 403 的错误: %v", err)
	}
}

// captureHook 收集 logrus 日志条目。
type captureHook struct {
	mu      sync.Mutex
	entries []*logrus.Entry
}

func (h *captureHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *captureHook) Fire(e *logrus.Entry) error {
	h.mu.Lock()
	h.entries = append(h.entries, e)
	h.mu.Unlock()
	return nil
}

func TestReportLogsNoLogrusOutput(t *testing.T) {
	hook := &captureHook{}
	logger := logrus.StandardLogger()
	oldLevel := logger.GetLevel()
	logger.SetLevel(logrus.DebugLevel)
	logger.AddHook(hook)
	t.Cleanup(func() {
		logger.SetLevel(oldLevel)
		logger.ReplaceHooks(make(logrus.LevelHooks))
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("boom"))
	}))
	c := newLogClient(t, srv.URL)
	if err := c.ReportLogs(context.Background(), sampleLogReport()); err == nil {
		t.Fatal("500 应返回错误")
	}
	srv.Close()
	// 服务已关闭：连接失败
	if err := c.ReportLogs(context.Background(), sampleLogReport()); err == nil {
		t.Fatal("不可达应返回错误")
	}
	hook.mu.Lock()
	defer hook.mu.Unlock()
	for _, e := range hook.entries {
		if e.Level <= logrus.InfoLevel {
			t.Fatalf("发送日志不得产生 Info 及以上 logrus 日志（会反馈成新日志）: %s %s", e.Level, e.Message)
		}
	}
}

func TestReportLogsHonorsContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	c := newLogClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.ReportLogs(ctx, sampleLogReport()); err == nil {
		t.Fatal("ctx 超时应返回错误")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("未及时响应 ctx: %s", time.Since(start))
	}
}

func TestLogSenderFallsBackToHTTPWhenSyncDown(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: "127.0.0.1:1"}, "secret", 9)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	s := LogSender{Grpc: gc, HTTP: newLogClient(t, srv.URL)}
	if err := s.SendLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("HTTP 应被调用 1 次，实际 %d", hits.Load())
	}
	// Grpc 为 nil 同样直接走 HTTP
	s = LogSender{HTTP: newLogClient(t, srv.URL)}
	if err := s.SendLogs(context.Background(), sampleLogReport()); err != nil || hits.Load() != 2 {
		t.Fatalf("Grpc=nil 应走 HTTP: err=%v hits=%d", err, hits.Load())
	}
}

// logAckServer 收到日志上报后按 mode 回复：ack / reject / silent（不回）。
type logAckServer struct {
	nodev1.UnimplementedNodeServiceServer
	mode string
	got  chan *nodev1.LogReport
}

func (s *logAckServer) Sync(stream nodev1.NodeService_SyncServer) error {
	if err := stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 9}}}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		lr := req.GetLog()
		if lr == nil {
			continue
		}
		s.got <- lr
		switch s.mode {
		case "silent":
			continue
		case "reject":
			_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: &nodev1.Ack{ReportId: lr.GetReportId(), Ok: false, Error: "nope"}}})
		default:
			_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: &nodev1.Ack{ReportId: lr.GetReportId(), Ok: true}}})
		}
	}
}

func startLogAckClient(t *testing.T, mode string) (*GrpcClient, *logAckServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &logAckServer{mode: mode, got: make(chan *nodev1.LogReport, 8)}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 9)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gc.Close() })
	gc.StartSync(t.Context(), nil)
	deadline := time.Now().Add(3 * time.Second)
	for !gc.IsSyncConnected() {
		if time.Now().After(deadline) {
			t.Fatal("Sync 未连接")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return gc, impl
}

func TestSendLogsWaitsForAck(t *testing.T) {
	gc, srv := startLogAckClient(t, "ack")
	if err := gc.SendLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatalf("Ack ok 应成功: %v", err)
	}
	if lr := <-srv.got; lr.GetReportId() != "b:1" {
		t.Fatalf("服务端收到的 report_id=%q", lr.GetReportId())
	}

	gc2, _ := startLogAckClient(t, "reject")
	err := gc2.SendLogs(context.Background(), sampleLogReport())
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("Ack ok=false 应返回错误: %v", err)
	}

	if err := gc.SendLogs(context.Background(), &nodev1.LogReport{}); err == nil {
		t.Fatal("report_id 为空应报错")
	}
}

func TestSendLogsHonorsContext(t *testing.T) {
	gc, _ := startLogAckClient(t, "silent")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := gc.SendLogs(ctx, sampleLogReport()); err == nil {
		t.Fatal("无 Ack 时 ctx 超时应返回错误")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("SendLogs 未及时响应 ctx: %s", time.Since(start))
	}
}

func TestLogSenderUsesGrpcWhenConnected(t *testing.T) {
	gc, _ := startLogAckClient(t, "ack")
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	s := LogSender{Grpc: gc, HTTP: newLogClient(t, srv.URL)}
	if err := s.SendLogs(context.Background(), sampleLogReport()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 0 {
		t.Fatal("gRPC 成功时不应调用 HTTP")
	}
}
