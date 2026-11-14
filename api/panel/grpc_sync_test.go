package panel

import (
	"context"
	"net"
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
