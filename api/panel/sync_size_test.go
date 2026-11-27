package panel

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
)

// bigPushServer 收到订阅后在 Sync 流上推一条超过 4 MB 的 FULL（未声明 seat_v1 的通道不分片）
type bigPushServer struct {
	nodev1.UnimplementedNodeServiceServer
	users int
}

func (s *bigPushServer) Sync(stream nodev1.NodeService_SyncServer) error {
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 7}}})
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if req.GetSubscribe() == nil {
			continue
		}
		u := &nodev1.UserListUpdate{Protocol: "vless", Port: 443, Version: 1, Action: nodev1.UserUpdateAction_USER_UPDATE_FULL}
		for i := 0; i < s.users; i++ {
			u.Users = append(u.Users, &nodev1.UserEntry{Id: int64(1_000_000_001 + i), Uuid: strings.Repeat("a", 36), SpeedLimit: 10485760})
		}
		if err := stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_UserUpdate{UserUpdate: u}}); err != nil {
			return err
		}
	}
}

type recordingHandler struct {
	inboundsHandler
	mu  sync.Mutex
	got []UserUpdate
}

func (h *recordingHandler) OnUserUpdate(u UserUpdate) {
	h.mu.Lock()
	h.got = append(h.got, u)
	h.mu.Unlock()
}

func TestSyncStreamAcceptsMessagesAboveDefaultLimit(t *testing.T) {
	const users = 120_000 // 约 6 MB
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxSendMsgSize(128 << 20))
	nodev1.RegisterNodeServiceServer(srv, &bigPushServer{users: users})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: lis.Addr().String()}, "secret", 7)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	h := &recordingHandler{}
	gc.StartSync(context.Background(), h)
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.mu.Lock()
		n := len(h.got)
		var size int
		if n > 0 {
			size = len(h.got[0].Users)
		}
		h.mu.Unlock()
		if n > 0 {
			if size != users {
				t.Fatalf("got %d users, want %d", size, users)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("large Sync push was not received")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
