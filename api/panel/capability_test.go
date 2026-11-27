package panel

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// withSeatV1 在测试期间切换能力声明开关，结束后恢复关闭
func withSeatV1(t *testing.T, on bool, version string) {
	t.Helper()
	oldVersion := NodeVersion
	NodeVersion = version
	SetSeatV1(on)
	t.Cleanup(func() {
		SetSeatV1(false)
		NodeVersion = oldVersion
	})
}

type recordedHTTP struct {
	mu       sync.Mutex
	queries  map[string]string
	agents   map[string][]string
	hasAgent map[string]bool
}

func newRecordingPanel(t *testing.T) (*recordedHTTP, *httptest.Server) {
	t.Helper()
	rec := &recordedHTTP{queries: map[string]string{}, agents: map[string][]string{}, hasAgent: map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.queries[r.URL.Path] = r.URL.RawQuery
		rec.agents[r.URL.Path] = r.Header.Values("X-Node-Agent")
		_, rec.hasAgent[r.URL.Path] = r.Header["X-Node-Agent"]
		rec.mu.Unlock()
		switch {
		case r.URL.Path == "/v1/server/user":
			_, _ = w.Write([]byte(`{"users":[]}`))
		case strings.HasPrefix(r.URL.Path, "/v2/server/"):
			_, _ = w.Write([]byte(`{"code":200,"msg":"success","data":{"protocols":[],"total":0}}`))
		default:
			_, _ = w.Write([]byte(`{"code":200,"msg":"success"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return rec, srv
}

func exerciseHTTPClients(t *testing.T, apiHost string) {
	t.Helper()
	v1, err := NewClientV1(&conf.NodeApiConfig{APIHost: apiHost, NodeID: 7, SecretKey: "secret", NodeType: "vless", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v1.GetUserList(context.Background()); err != nil {
		t.Fatal(err)
	}
	users := []OnlineUser{}
	if err := v1.ReportNodeOnlineUsers(context.Background(), &users); err != nil {
		t.Fatal(err)
	}
	v2 := NewClientV2(&conf.ServerApiConfig{ApiHost: apiHost, ServerId: 7, SecretKey: "secret"})
	if _, err := GetServerConfig(context.Background(), v2); err != nil {
		t.Fatal(err)
	}
}

// 开关关闭（默认）时，HTTP 请求与改动前逐字节一致：没有 capabilities 参数，也没有 X-Node-Agent 头
func TestHTTPRequestsUnchangedWhenSeatV1Off(t *testing.T) {
	withSeatV1(t, false, "1.2.3")
	rec, srv := newRecordingPanel(t)
	exerciseHTTPClients(t, srv.URL)

	want := map[string]string{
		"/v1/server/user":   "port=443&protocol=vless&secret_key=secret&server_id=7",
		"/v1/server/online": "port=443&protocol=vless&secret_key=secret&server_id=7",
		"/v2/server/7":      "secret_key=secret",
	}
	for p, q := range want {
		if got := rec.queries[p]; got != q {
			t.Errorf("%s query = %q, want %q", p, got, q)
		}
		if rec.hasAgent[p] {
			t.Errorf("%s 不应带 X-Node-Agent（开关关闭时请求必须与旧版一致）", p)
		}
	}
}

func TestHTTPRequestsDeclareSeatV1WhenOn(t *testing.T) {
	withSeatV1(t, true, "1.2.3")
	rec, srv := newRecordingPanel(t)
	exerciseHTTPClients(t, srv.URL)

	want := map[string]string{
		"/v1/server/user":   "capabilities=seat_v1&port=443&protocol=vless&secret_key=secret&server_id=7",
		"/v1/server/online": "capabilities=seat_v1&port=443&protocol=vless&secret_key=secret&server_id=7",
		"/v2/server/7":      "capabilities=seat_v1&secret_key=secret",
	}
	for p, q := range want {
		if got := rec.queries[p]; got != q {
			t.Errorf("%s query = %q, want %q", p, got, q)
		}
		if got := rec.agents[p]; len(got) != 1 || got[0] != "npnode/1.2.3" {
			t.Errorf("%s X-Node-Agent = %v, want [npnode/1.2.3]", p, got)
		}
	}
}

// capturingNodeServer 记录一元请求、metadata 与订阅内容；GetUserList 返回 bigUsers 条用户
type capturingNodeServer struct {
	nodev1.UnimplementedNodeServiceServer
	mu        sync.Mutex
	configReq *nodev1.GetConfigRequest
	userReq   *nodev1.GetUserListRequest
	sub       *nodev1.Subscribe
	md        map[string]metadata.MD
	bigUsers  int
}

func (s *capturingNodeServer) record(ctx context.Context, name string) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.md[name] = md
	s.mu.Unlock()
}

func (s *capturingNodeServer) GetConfig(ctx context.Context, req *nodev1.GetConfigRequest) (*nodev1.GetConfigResponse, error) {
	s.record(ctx, "GetConfig")
	s.mu.Lock()
	s.configReq = req
	s.mu.Unlock()
	return &nodev1.GetConfigResponse{}, nil
}

func (s *capturingNodeServer) GetUserList(ctx context.Context, req *nodev1.GetUserListRequest) (*nodev1.GetUserListResponse, error) {
	s.record(ctx, "GetUserList")
	s.mu.Lock()
	s.userReq = req
	n := s.bigUsers
	s.mu.Unlock()
	resp := &nodev1.GetUserListResponse{}
	for i := 0; i < n; i++ {
		resp.Users = append(resp.Users, &nodev1.UserEntry{Id: int64(1_000_000_001 + i), Uuid: strings.Repeat("a", 36), SpeedLimit: 10485760})
	}
	return resp, nil
}

func (s *capturingNodeServer) Sync(stream nodev1.NodeService_SyncServer) error {
	s.record(stream.Context(), "Sync")
	_ = stream.Send(&nodev1.SyncResponse{Payload: &nodev1.SyncResponse_Hello{Hello: &nodev1.ServerHello{ServerId: 7}}})
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if sub := req.GetSubscribe(); sub != nil {
			s.mu.Lock()
			s.sub = sub
			s.mu.Unlock()
		}
	}
}

type inboundsHandler struct{}

func (inboundsHandler) OnUserUpdate(UserUpdate) {}
func (inboundsHandler) OnConfigUpdate(string)   {}
func (inboundsHandler) SyncInbounds() []Inbound {
	return []Inbound{{Protocol: "vless", Port: 443}, {Protocol: "trojan", Port: 8443}}
}
func (inboundsHandler) OnSyncDisconnected() {}

func startCapturingServer(t *testing.T, bigUsers int) (*capturingNodeServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := &capturingNodeServer{md: map[string]metadata.MD{}, bigUsers: bigUsers}
	srv := grpc.NewServer(grpc.MaxSendMsgSize(128 << 20))
	nodev1.RegisterNodeServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return impl, lis.Addr().String()
}

func exerciseGrpc(t *testing.T, addr string, impl *capturingNodeServer) {
	t.Helper()
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: addr}, "secret", 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gc.Close() })
	if err := gc.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := gc.GetUserList(context.Background(), "vless", 443); err != nil {
		t.Fatal(err)
	}
	gc.StartSync(context.Background(), inboundsHandler{})
	deadline := time.Now().Add(3 * time.Second)
	for {
		impl.mu.Lock()
		got := impl.sub != nil
		impl.mu.Unlock()
		if got {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Subscribe")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGrpcRequestsUnchangedWhenSeatV1Off(t *testing.T) {
	withSeatV1(t, false, "1.2.3")
	impl, addr := startCapturingServer(t, 0)
	exerciseGrpc(t, addr, impl)
	impl.mu.Lock()
	defer impl.mu.Unlock()
	if len(impl.configReq.GetCapabilities()) != 0 || len(impl.userReq.GetCapabilities()) != 0 {
		t.Fatalf("开关关闭时不应声明能力: config=%v user=%v", impl.configReq.GetCapabilities(), impl.userReq.GetCapabilities())
	}
	for _, in := range impl.sub.GetInbounds() {
		if len(in.GetCapabilities()) != 0 {
			t.Fatalf("开关关闭时 Inbound 不应带能力: %v", in)
		}
	}
	for name, md := range impl.md {
		if v := md.Get("x-node-agent"); len(v) != 0 {
			t.Fatalf("%s 开关关闭时不应带 x-node-agent: %v", name, v)
		}
	}
}

func TestGrpcRequestsDeclareSeatV1WhenOn(t *testing.T) {
	withSeatV1(t, true, "1.2.3")
	impl, addr := startCapturingServer(t, 0)
	exerciseGrpc(t, addr, impl)
	impl.mu.Lock()
	defer impl.mu.Unlock()
	if got := impl.configReq.GetCapabilities(); len(got) != 1 || got[0] != "seat_v1" {
		t.Fatalf("GetConfig capabilities = %v", got)
	}
	if got := impl.userReq.GetCapabilities(); len(got) != 1 || got[0] != "seat_v1" {
		t.Fatalf("GetUserList capabilities = %v", got)
	}
	if len(impl.sub.GetInbounds()) != 2 {
		t.Fatalf("inbounds = %v", impl.sub.GetInbounds())
	}
	for _, in := range impl.sub.GetInbounds() {
		if got := in.GetCapabilities(); len(got) != 1 || got[0] != "seat_v1" {
			t.Fatalf("Inbound %s:%d capabilities = %v", in.GetProtocol(), in.GetPort(), got)
		}
	}
	for _, name := range []string{"GetConfig", "GetUserList", "Sync"} {
		if v := impl.md[name].Get("x-node-agent"); len(v) != 1 || v[0] != "npnode/1.2.3" {
			t.Fatalf("%s x-node-agent = %v", name, v)
		}
	}
}

// 一元 GetUserList 不分片：超过 gRPC 默认 4 MB 接收上限的列表也要能收下（不论开关）
func TestGrpcGetUserListAcceptsResponsesAboveDefaultLimit(t *testing.T) {
	const users = 120_000 // 约 6 MB
	_, addr := startCapturingServer(t, users)
	gc, err := NewGrpcClient(&conf.GrpcConfig{Enable: true, Addr: addr}, "secret", 7)
	if err != nil {
		t.Fatal(err)
	}
	defer gc.Close()
	got, err := gc.GetUserList(context.Background(), "vless", 443)
	if err != nil {
		t.Fatalf("GetUserList with %d users: %v", users, err)
	}
	if len(got) != users {
		t.Fatalf("got %d users, want %d", len(got), users)
	}
}
