package panel

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
	"net"
)

// ackServer 记录收到的流量上报，并按 decide 的结果回 Ack。
type ackServer struct {
	nodev1.UnimplementedNodeServiceServer
	mu      sync.Mutex
	reports []*nodev1.TrafficReport
	decide  func(n int, r *nodev1.TrafficReport) (ok bool, drop bool)
}

func (s *ackServer) Sync(stream nodev1.NodeService_SyncServer) error {
	if err := stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 6}}}); err != nil {
		return err
	}
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
		s.reports = append(s.reports, tr)
		n := len(s.reports)
		s.mu.Unlock()
		ok, drop := s.decide(n, tr)
		if drop {
			return nil // 模拟断流：不回 Ack
		}
		ack := &nodev1.Ack{ReportId: tr.GetReportId(), Ok: ok}
		if !ok {
			ack.Error = "queue unavailable"
		}
		if err := stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Ack{Ack: ack}}); err != nil {
			return err
		}
	}
}

func (s *ackServer) received() []*nodev1.TrafficReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*nodev1.TrafficReport(nil), s.reports...)
}

func StartAckTestClient(t *testing.T, decide func(int, *nodev1.TrafficReport) (bool, bool)) (*GrpcClient, *ackServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &ackServer{decide: decide}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 6)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gc.Close() })
	gc.StartSync(t.Context(), nil)
	deadline := time.Now().Add(3 * time.Second)
	for !gc.IsSyncConnected() {
		if time.Now().After(deadline) {
			t.Fatal("sync stream never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return gc, impl
}

func TestSendTrafficWaitsForAck(t *testing.T) {
	gc, srv := StartAckTestClient(t, func(n int, _ *nodev1.TrafficReport) (bool, bool) { return n != 2, false })
	traffic := []UserTraffic{{UID: 5, Upload: 100, Download: 200}}

	if err := gc.SendTraffic("r-1", "hysteria", 5559, traffic); err != nil {
		t.Fatalf("ok ack should succeed: %v", err)
	}
	if err := gc.SendTraffic("r-2", "hysteria", 5559, traffic); err == nil || !strings.Contains(err.Error(), "queue unavailable") {
		t.Fatalf("rejected ack must surface as error, got %v", err)
	}
	got := srv.received()
	if len(got) != 2 || got[0].GetProtocol() != "hysteria" || got[0].GetPort() != 5559 || got[0].GetReportId() != "r-1" || got[0].GetTraffic()[0].GetUid() != 5 {
		t.Fatalf("server received %v", got)
	}
}

func TestSendTrafficFailsFastWhenStreamDrops(t *testing.T) {
	gc, _ := StartAckTestClient(t, func(int, *nodev1.TrafficReport) (bool, bool) { return false, true })
	start := time.Now()
	err := gc.SendTraffic("r-1", "hysteria", 5559, []UserTraffic{{UID: 5, Upload: 1}})
	if err == nil {
		t.Fatal("dropped stream must not be reported as success")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("should fail when the stream closes, not wait for the %s ack timeout (took %s)", syncAckTimeout, time.Since(start))
	}
}

func TestTrafficReportIDsAreUnique(t *testing.T) {
	gc := &GrpcClient{bootID: newBootID()}
	other := &GrpcClient{bootID: newBootID()}
	a, b := gc.NewTrafficReportID(), gc.NewTrafficReportID()
	if a == b || gc.bootID == other.bootID || !strings.HasPrefix(a, gc.bootID+":") {
		t.Fatalf("ids not unique: %s %s (boot %s / %s)", a, b, gc.bootID, other.bootID)
	}
}
