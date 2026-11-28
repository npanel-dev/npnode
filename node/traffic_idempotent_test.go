package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
)

type pushBody struct {
	Traffic  []panel.UserTraffic `json:"traffic"`
	BootID   string              `json:"boot_id"`
	Sequence uint64              `json:"sequence"`
	Raw      string              `json:"-"`
}

// T3：打开 TrafficIdempotent 后，HTTP 上报失败重试沿用同一批次号与同一内容，期间新采集的流量放进下一批
func TestHTTPTrafficRetryKeepsBatchIdentity(t *testing.T) {
	panel.SetTrafficIdempotent(true)
	t.Cleanup(func() { panel.SetTrafficIdempotent(false) })
	var mu sync.Mutex
	var got []pushBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		var b pushBody
		_ = json.Unmarshal(raw, &b)
		b.Raw = string(raw)
		mu.Lock()
		got = append(got, b)
		n := len(got)
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, `{"code":500,"msg":"queue unavailable"}`)
			return
		}
		fmt.Fprint(w, `{"code":200,"msg":"success"}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	if err := q.report(context.Background()); err == nil {
		t.Fatal("expected business failure")
	}
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("requests = %d, want 3 (retry + new batch)", len(got))
	}
	first, retry, next := got[0], got[1], got[2]
	if first.BootID == "" || first.Sequence == 0 {
		t.Fatalf("first request has no batch identity: %s", first.Raw)
	}
	if retry.Raw != first.Raw {
		t.Fatalf("retry changed the batch:\n first %s\n retry %s", first.Raw, retry.Raw)
	}
	if next.BootID != first.BootID || next.Sequence != first.Sequence+1 {
		t.Fatalf("next batch identity = %s/%d, want %s/%d", next.BootID, next.Sequence, first.BootID, first.Sequence+1)
	}
	if len(next.Traffic) != 1 || next.Traffic[0] != (panel.UserTraffic{UID: 7, Upload: 5, Download: 7}) {
		t.Fatalf("next batch traffic = %+v", next.Traffic)
	}
}

// 开关关闭（默认）：请求体与旧版一致，不带批次号，失败后与新流量合并重发
func TestHTTPTrafficLegacyWhenIdempotentOff(t *testing.T) {
	var mu sync.Mutex
	var raws []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		mu.Lock()
		raws = append(raws, string(raw))
		n := len(raws)
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, `{"code":500}`)
			return
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	_ = q.report(context.Background())
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{`{"traffic":[{"uid":7,"upload":10,"download":20}]}`, `{"traffic":[{"uid":7,"upload":15,"download":27}]}`}
	if len(raws) != 2 || raws[0] != want[0] || raws[1] != want[1] {
		t.Fatalf("bodies = %v, want %v", raws, want)
	}
}

// okAckServer 所有流量上报都回成功 Ack，记录每次上报的上行字节
type okAckServer struct {
	nodev1.UnimplementedNodeServiceServer
	mu      sync.Mutex
	uploads []int64
}

func (s *okAckServer) Sync(stream nodev1.NodeService_SyncServer) error {
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 1}}})
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		tr := req.GetTraffic()
		if tr == nil {
			continue
		}
		var up int64
		for _, d := range tr.GetTraffic() {
			up += d.GetUpload()
		}
		s.mu.Lock()
		s.uploads = append(s.uploads, up)
		s.mu.Unlock()
		_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: &nodev1.Ack{ReportId: tr.GetReportId(), Ok: true}}})
	}
}

func startOKSync(t *testing.T) (*okAckServer, *panel.GrpcClient) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &okAckServer{}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	gc, err := panel.NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gc.Close() })
	gc.StartSync(context.Background(), nil)
	deadline := time.Now().Add(3 * time.Second)
	for !gc.IsSyncConnected() {
		if time.Now().After(deadline) {
			t.Fatal("sync never ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return impl, gc
}

// 审查 I1：冻结的 HTTP 批次连续失败达到阈值、且 Sync 可用时，并回待上报流量改走 Sync，不能永久挡住流量
func TestFrozenHTTPBatchFallsBackToSyncAfterRepeatedFailures(t *testing.T) {
	panel.SetTrafficIdempotent(true)
	t.Cleanup(func() { panel.SetTrafficIdempotent(false) })
	var httpCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		w.WriteHeader(http.StatusBadGateway) // HTTP 路径被网关持续拦截
	}))
	defer server.Close()
	impl, gc := startOKSync(t)
	client, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: server.URL, SecretKey: "secret", NodeID: 1, NodeType: "vless", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{apiClient: client, grpcClient: gc, tag: "t", info: &panel.NodeInfo{Type: "vless", Protocol: &panel.Protocol{Port: 443}}}
	c.queueTrafficReport([]panel.UserTraffic{{UID: 5, Upload: 100}})
	c.flushPendingTraffic(context.Background(), false) // 冻结批次，第 1 次失败
	c.queueTrafficReport([]panel.UserTraffic{{UID: 5, Upload: 50}})
	for i := 1; i < frozenHTTPFallbackAfter; i++ {
		c.flushPendingTraffic(context.Background(), true) // 原样重发，仍失败
		impl.mu.Lock()
		n := len(impl.uploads)
		impl.mu.Unlock()
		if n != 0 {
			t.Fatalf("fell back to Sync after only %d failures", i+1)
		}
	}
	c.flushPendingTraffic(context.Background(), true) // 达到阈值：改走 Sync
	if got := httpCalls.Load(); got != int64(frozenHTTPFallbackAfter) {
		t.Fatalf("HTTP attempts = %d, want %d", got, frozenHTTPFallbackAfter)
	}
	impl.mu.Lock()
	uploads := fmt.Sprint(impl.uploads)
	impl.mu.Unlock()
	if uploads != "[150]" {
		t.Fatalf("Sync uploads = %s, want [150] (frozen batch merged with new traffic)", uploads)
	}
	if len(c.traffic.pending) != 0 {
		t.Fatal("acknowledged traffic must leave the queue")
	}
}

// 审查 M5：关闭开关后（重载继承了冻结批次），遗留批次并回待上报流量、按旧格式（不带批次号）发送
func TestFrozenHTTPBatchUsesLegacyFormatAfterSwitchOff(t *testing.T) {
	panel.SetTrafficIdempotent(true)
	t.Cleanup(func() { panel.SetTrafficIdempotent(false) })
	var mu sync.Mutex
	var raws []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		mu.Lock()
		raws = append(raws, string(raw))
		n := len(raws)
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, `{"code":500}`)
			return
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	_ = q.report(context.Background())
	panel.SetTrafficIdempotent(false)
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := `{"traffic":[{"uid":7,"upload":15,"download":27}]}`; len(raws) != 2 || raws[1] != want {
		t.Fatalf("bodies = %v, want second %s", raws, want)
	}
}
