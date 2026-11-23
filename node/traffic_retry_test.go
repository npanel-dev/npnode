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

// flakyAckServer 第一次上报回失败 Ack，之后都回成功，并记录收到的 report_id。
type flakyAckServer struct {
	nodev1.UnimplementedNodeServiceServer
	mu      sync.Mutex
	ids     []string
	uploads []int64
}

func (s *flakyAckServer) Sync(stream nodev1.NodeService_SyncServer) error {
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 6}}})
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		tr := req.GetTraffic()
		if tr == nil {
			continue
		}
		s.mu.Lock()
		s.ids = append(s.ids, tr.GetReportId()+"|"+tr.GetProtocol())
		var upload int64
		for _, delta := range tr.GetTraffic() {
			upload += delta.GetUpload()
		}
		s.uploads = append(s.uploads, upload)
		ok := len(s.ids) > 1
		s.mu.Unlock()
		_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: &nodev1.Ack{ReportId: tr.GetReportId(), Ok: ok, Error: "temporarily unavailable"}}})
	}
}

// 回归：流量读出后即从内核清零，上报失败必须保留并用同一个 report_id 重发（面板据此去重）。
func TestFailedTrafficReportIsRetriedWithSameReportID(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint("http-fallback-", fallback), func(t *testing.T) { testTrafficRetryAcrossReload(t, fallback) })
	}
}

func testTrafficRetryAcrossReload(t *testing.T, fallback bool) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &flakyAckServer{}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	gc, err := panel.NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 6)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	gc.StartSync(context.Background(), nil)
	deadline := time.Now().Add(3 * time.Second)
	for !gc.IsSyncConnected() {
		if time.Now().After(deadline) {
			t.Fatal("sync never ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var httpUpload atomic.Int64
	httpPanel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report panel.ServerPushUserTrafficRequest
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Error(err)
		}
		for _, delta := range report.Traffic {
			httpUpload.Add(delta.Upload)
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer httpPanel.Close()
	client, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: httpPanel.URL, SecretKey: "secret", NodeID: 6, NodeType: "hysteria", Port: 5559})
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{apiClient: client, grpcClient: gc, tag: "test", info: &panel.NodeInfo{Type: "hysteria", Protocol: &panel.Protocol{Port: 5559}}}
	c.queueTrafficReport([]panel.UserTraffic{{UID: 5, Upload: 100, Download: 200}})

	c.flushPendingTraffic(context.Background(), true)
	if len(c.traffic.pending) != 1 {
		t.Fatal("rejected batch must remain queued")
	}
	// A lost ACK must not be retried over HTTP without its report ID.
	c.flushPendingTraffic(context.Background(), false)
	c.queueTrafficReport([]panel.UserTraffic{{UID: 5, Upload: 50}})
	if fallback {
		c.flushPendingTraffic(context.Background(), false)
	}
	old := &Node{traffic: c.traffic}
	nextController := &Controller{apiClient: client, grpcClient: gc, info: c.info}
	next := &Node{controllers: []*Controller{nextController}}
	next.InheritTraffic(old)
	nextController.flushPendingTraffic(context.Background(), true)
	if len(next.traffic.pending) != 0 {
		t.Fatal("acknowledged batches must be removed")
	}

	impl.mu.Lock()
	ids := append([]string(nil), impl.ids...)
	impl.mu.Unlock()
	if len(ids) < 2 || ids[0] != ids[1] || ids[0] == "|hysteria" {
		t.Fatalf("retry must reuse report ID: %v", ids)
	}
	if fallback {
		if len(ids) != 2 || fmt.Sprint(impl.uploads) != "[100 100]" || httpUpload.Load() != 50 {
			t.Fatalf("fallback mixed pending and new traffic: ids=%v uploads=%v http=%d", ids, impl.uploads, httpUpload.Load())
		}
	} else if len(ids) != 3 || ids[1] == ids[2] || fmt.Sprint(impl.uploads) != "[100 100 50]" || httpUpload.Load() != 0 {
		t.Fatalf("retry changed immutable payload: ids=%v uploads=%v http=%d", ids, impl.uploads, httpUpload.Load())
	}
}

func TestPendingTrafficQueueCoalescesWithoutDroppingBytes(t *testing.T) {
	var observed atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report panel.ServerPushUserTrafficRequest
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Error(err)
		}
		for _, delta := range report.Traffic {
			observed.Add(delta.Upload)
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	c := &Controller{apiClient: trafficTestClient(t, server.URL, 5559)}
	for range 105 {
		c.queueTrafficReport([]panel.UserTraffic{{UID: 7, Upload: 1}})
	}
	c.queueTrafficReport(nil)
	c.flushPendingTraffic(context.Background(), false)
	if got := observed.Load(); got != 105 {
		t.Fatalf("received %d bytes, want 105", got)
	}
}
