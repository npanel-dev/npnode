package panel

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
)

type countingSyncServer struct {
	nodev1.UnimplementedNodeServiceServer
	calls     atomic.Int32
	implement bool
}

func (s *countingSyncServer) Sync(stream nodev1.NodeService_SyncServer) error {
	s.calls.Add(1)
	if !s.implement {
		return s.UnimplementedNodeServiceServer.Sync(stream)
	}
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_ConfigUpdate{ConfigUpdate: &nodev1.ConfigUpdate{Reason: "hello"}}})
	<-stream.Context().Done()
	return nil
}

func startCountingSyncServer(t *testing.T, implement bool) (*countingSyncServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &countingSyncServer{implement: implement}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return impl, lis.Addr().String()
}

// 回归：面板 Sync 未实现时，旧逻辑每秒重连一次（错误在 Recv 才返回，退避被提前重置），
// 且流建立瞬间就被视为可用，这期间的流量上报会走这条流并丢失。
func TestSyncUnimplementedBacksOffAndNeverReportsConnected(t *testing.T) {
	impl, addr := startCountingSyncServer(t, false)
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: addr}, "secret", 6)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	gc.StartSync(context.Background(), nil)

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if gc.IsSyncConnected() {
			t.Fatal("rejected Sync stream must never be reported as connected (traffic would be sent into it and lost)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := impl.calls.Load(); got != 1 {
		t.Fatalf("Sync attempts in 2.5s = %d, want 1 (must back off instead of retrying every second)", got)
	}
}

func TestSyncBecomesConnectedOnServerMessage(t *testing.T) {
	_, addr := startCountingSyncServer(t, true)
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: addr}, "secret", 6)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	gc.StartSync(context.Background(), nil)

	deadline := time.Now().Add(2 * time.Second)
	for !gc.IsSyncConnected() {
		if time.Now().After(deadline) {
			t.Fatal("stream that delivered a server message should be connected well before syncReadyDelay")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// certSyncServer 发 Hello 并记录节点发来的证书状态
type certSyncServer struct {
	nodev1.UnimplementedNodeServiceServer
	mu    sync.Mutex
	certs []*nodev1.CertReport
}

func (s *certSyncServer) Sync(stream nodev1.NodeService_SyncServer) error {
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 6}}})
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if c := req.GetCert(); c != nil {
			s.mu.Lock()
			s.certs = append(s.certs, c)
			s.mu.Unlock()
		}
	}
}

func (s *certSyncServer) domains() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.certs))
	for _, c := range s.certs {
		out = append(out, c.GetDomain())
	}
	return out
}

// 流连上之前记录的证书状态，连上（收到 Hello）后补发；之后的状态立即发送
func TestReportCertFlushesOnConnect(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &certSyncServer{}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 6)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	gc.ReportCert(&nodev1.CertReport{Protocol: "hysteria", Port: 5553, Domain: "old.example.com"})
	// 同一入站的新状态覆盖旧的，只补发最新一条
	gc.ReportCert(&nodev1.CertReport{Protocol: "hysteria", Port: 5553, Domain: "a.example.com", Error: "acme failed"})
	gc.StartSync(context.Background(), nil)

	waitUntil := func(what string, cond func() bool) {
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s (got %v)", what, impl.domains())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitUntil("pending report flushed", func() bool { return len(impl.domains()) == 1 })
	if d := impl.domains(); d[0] != "a.example.com" {
		t.Fatalf("flushed %v, want only the latest report", d)
	}
	waitUntil("stream connected", gc.IsSyncConnected)
	gc.ReportCert(&nodev1.CertReport{Protocol: "vless", Port: 443, Domain: "b.example.com", Ok: true})
	waitUntil("live report", func() bool { return len(impl.domains()) == 2 })
}
