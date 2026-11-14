package node

import (
	"context"
	"net"
	"sync"
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
	mu  sync.Mutex
	ids []string
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
		ok := len(s.ids) > 1
		s.mu.Unlock()
		_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: &nodev1.Ack{ReportId: tr.GetReportId(), Ok: ok, Error: "temporarily unavailable"}}})
	}
}

// 回归：流量读出后即从内核清零，上报失败必须保留并用同一个 report_id 重发（面板据此去重）。
func TestFailedTrafficReportIsRetriedWithSameReportID(t *testing.T) {
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

	c := &Controller{grpcClient: gc, tag: "test", info: &panel.NodeInfo{Type: "hysteria", Protocol: &panel.Protocol{Port: 5559}}}
	c.queueTrafficReport([]panel.UserTraffic{{UID: 5, Upload: 100, Download: 200}})

	c.flushPendingTraffic(context.Background(), true)
	if len(c.pendingTraffic) != 1 {
		t.Fatalf("rejected batch must stay pending, pending = %d", len(c.pendingTraffic))
	}
	c.flushPendingTraffic(context.Background(), true)
	if len(c.pendingTraffic) != 0 {
		t.Fatalf("acked batch must be removed, pending = %d", len(c.pendingTraffic))
	}

	impl.mu.Lock()
	ids := append([]string(nil), impl.ids...)
	impl.mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] || ids[0] == "|hysteria" {
		t.Fatalf("retry must reuse the same report_id (and carry the protocol): %v", ids)
	}
}

func TestPendingTrafficQueueIsBounded(t *testing.T) {
	c := &Controller{tag: "test"}
	for i := 0; i < maxPendingTrafficReports+5; i++ {
		c.queueTrafficReport([]panel.UserTraffic{{UID: i + 1, Upload: 1}})
	}
	if len(c.pendingTraffic) != maxPendingTrafficReports {
		t.Fatalf("pending = %d, want %d", len(c.pendingTraffic), maxPendingTrafficReports)
	}
	if c.pendingTraffic[0].traffic[0].UID != 6 {
		t.Fatalf("oldest batches should be dropped first, first UID = %d", c.pendingTraffic[0].traffic[0].UID)
	}
	c.queueTrafficReport(nil)
	if len(c.pendingTraffic) != maxPendingTrafficReports {
		t.Fatal("empty batches must not be queued")
	}
}
