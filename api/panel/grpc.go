package panel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/npanel-dev/npnode/conf"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// GrpcClient wraps the gRPC connection and provides the same logical API
// as the HTTP-based ClientV1 and ClientV2.
type GrpcClient struct {
	conn      *grpc.ClientConn
	client    nodev1.NodeServiceClient
	serverID  int64
	secretKey string
	grpcAddr  string

	mu       sync.Mutex
	sync     nodev1.NodeService_SyncClient
	syncCtx  context.Context
	syncDone chan struct{}

	// sendMu 串行化 stream.Send
	sendMu sync.Mutex
	// ackWaiters 按 report_id 等待面板 Ack
	ackWaiters map[string]chan *nodev1.Ack
	// bootID + reportSeq 组成流量上报 ID
	bootID    string
	reportSeq atomic.Uint64
}

// parseGrpcAddr parses a URL-style address into a gRPC target and TLS status.
// Examples:
//
//	https://panel.example.com  →  tls:true,  addr: "panel.example.com:443"
//	http://panel.example.com:9090 → tls:false, addr: "panel.example.com:9090"
//	127.0.0.1:9090             →  tls:false, addr: "127.0.0.1:9090"
func parseGrpcAddr(raw string) (addr string, useTLS bool) {
	if raw == "" {
		return "127.0.0.1:9090", false
	}

	// If no scheme, treat as plain host:port
	if !strings.Contains(raw, "://") {
		return raw, false
	}

	u, err := url.Parse(raw)
	if err != nil {
		return raw, false
	}

	host := u.Host
	if u.Port() == "" {
		switch u.Scheme {
		case "https":
			host = u.Host + ":443"
		default:
			host = u.Host + ":9090"
		}
	}

	return host, u.Scheme == "https"
}

// NewGrpcClient creates a new gRPC client connected to the panel server.
func NewGrpcClient(cfg *conf.GrpcConfig, secretKey string, serverID int) (*GrpcClient, error) {
	addr, useTLS := parseGrpcAddr(cfg.Addr)

	connectParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  1.0 * time.Second,
			Multiplier: 2.0,
			MaxDelay:   30 * time.Second,
		},
		MinConnectTimeout: 5 * time.Second,
	}

	var creds credentials.TransportCredentials
	if useTLS {
		creds = credentials.NewTLS(nil) // use system cert pool
	} else {
		creds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(connectParams),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s failed: %w", addr, err)
	}

	// gRPC connects lazily on first RPC call, no need to verify here

	gc := &GrpcClient{
		conn:      conn,
		client:    nodev1.NewNodeServiceClient(conn),
		serverID:  int64(serverID),
		secretKey: secretKey,
		bootID:    newBootID(),
		grpcAddr:  addr,
	}

	log.Infof("grpc client connected to %s (server_id=%d)", addr, serverID)
	return gc, nil
}

// Close closes the gRPC connection and stops the Sync stream if active.
func (g *GrpcClient) Close() error {
	g.mu.Lock()
	if g.syncDone != nil {
		close(g.syncDone)
		g.syncDone = nil
	}
	g.mu.Unlock()
	return g.conn.Close()
}

// ========== Unary RPCs ==========

// GetConfig fetches server configuration (protocols, DNS, outbound, etc.).
func (g *GrpcClient) GetConfig(ctx context.Context, protocols []string) (*nodev1.GetConfigResponse, error) {
	ctx = g.authContext(ctx)
	resp, err := g.client.GetConfig(ctx, &nodev1.GetConfigRequest{
		ServerId:  g.serverID,
		SecretKey: g.secretKey,
		Protocols: protocols,
	})
	if err != nil {
		st := status.Convert(err)
		return nil, fmt.Errorf("GetConfig failed: %s (code=%s)", st.Message(), st.Code())
	}
	return resp, nil
}

// GetUserList fetches the authorized user list for a given protocol.
// port 为入站端口：同一服务器同协议可能有多个端口，用户列表按端口区分。
func (g *GrpcClient) GetUserList(ctx context.Context, protocol string, port int) ([]UserInfo, error) {
	ctx = g.authContext(ctx)
	req := &nodev1.GetUserListRequest{
		ServerId:  g.serverID,
		SecretKey: g.secretKey,
		Protocol:  protocol,
	}
	if port > 0 && port <= 65535 {
		req.Port = uint32(port)
	}
	resp, err := g.client.GetUserList(ctx, req)
	if err != nil {
		st := status.Convert(err)
		return nil, fmt.Errorf("GetUserList failed: %s (code=%s)", st.Message(), st.Code())
	}

	users := make([]UserInfo, 0, len(resp.Users))
	for _, u := range resp.Users {
		users = append(users, UserInfo{
			Id:          int(u.Id),
			Uuid:        u.Uuid,
			SpeedLimit:  int(u.SpeedLimit),
			DeviceLimit: int(u.DeviceLimit),
		})
	}
	return users, nil
}

// ReportStatus reports server health status.
func (g *GrpcClient) ReportStatus(ctx context.Context, protocol string, cpu, mem, disk float64) error {
	ctx = g.authContext(ctx)
	_, err := g.client.ReportStatus(ctx, &nodev1.ReportStatusRequest{
		ServerId:  g.serverID,
		SecretKey: g.secretKey,
		Protocol:  protocol,
		Cpu:       cpu,
		Mem:       mem,
		Disk:      disk,
		UpdatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		st := status.Convert(err)
		return fmt.Errorf("ReportStatus failed: %s (code=%s)", st.Message(), st.Code())
	}
	return nil
}

// ========== Sync Bidirectional Stream ==========

// StartSync starts a persistent bidirectional Sync stream.
// It runs in a background goroutine and auto-reconnects on failure.
func (g *GrpcClient) StartSync(ctx context.Context, handler SyncHandler) {
	g.mu.Lock()
	if g.syncDone != nil {
		g.mu.Unlock()
		return // already running
	}
	done := make(chan struct{})
	g.syncDone = done
	g.syncCtx = ctx
	g.mu.Unlock()

	// done 作为参数传入：Close 会把 g.syncDone 置空，循环里不能再读这个字段
	go g.runSyncLoop(handler, done)
}

// SyncHandler handles incoming messages from the Sync stream.
type SyncHandler interface {
	// OnUserUpdate 面板推送的某个入站的用户变更（全量或差异）
	OnUserUpdate(update UserUpdate)
	OnConfigUpdate(reason string)
	// SyncInbounds 面板确认连接后要订阅用户推送的入站
	SyncInbounds() []Inbound
	// OnSyncDisconnected 流断开：之后的差异不能再接在旧版本上，需等重连后的全量
	OnSyncDisconnected()
}

// Inbound 一个入站（协议 + 端口）
type Inbound struct {
	Protocol string
	Port     int
}

// UserUpdate 面板推送的用户变更。
// Action 为 FULL 时 Users 是完整列表；为 DIFF 时 Users 是新增/变更后的用户，Removed 是删除/变更前的用户。
// Version 是该入站用户集合的版本，DIFF 只能应用在“本地版本 + 1”上。
type UserUpdate struct {
	Protocol string
	Port     int
	Version  uint64
	Action   nodev1.UserUpdateAction
	Users    []UserInfo
	Removed  []UserInfo
}

func userEntriesToInfo(entries []*nodev1.UserEntry) []UserInfo {
	users := make([]UserInfo, 0, len(entries))
	for _, u := range entries {
		users = append(users, UserInfo{
			Id:          int(u.Id),
			Uuid:        u.Uuid,
			SpeedLimit:  int(u.SpeedLimit),
			DeviceLimit: int(u.DeviceLimit),
		})
	}
	return users
}

// Subscribe 订阅入站的用户推送；对已订阅的入站再次订阅会收到当前全量（用于版本断档后重新同步）。
func (g *GrpcClient) Subscribe(inbounds ...Inbound) error {
	if len(inbounds) == 0 {
		return nil
	}
	sub := &nodev1.Subscribe{}
	for _, in := range inbounds {
		sub.Inbounds = append(sub.Inbounds, &nodev1.Inbound{Protocol: in.Protocol, Port: clampPort(in.Port)})
	}
	return g.sendSync(&nodev1.SyncRequest{Payload: &nodev1.SyncRequest_Subscribe{Subscribe: sub}})
}

// syncAckTimeout 等待面板 Ack 的最长时间
const syncAckTimeout = 15 * time.Second

// NewTrafficReportID 生成流量上报 ID：本进程启动标识 + 自增序号。
// 节点未收到成功 Ack 时用同一个 ID 重发，面板据此去重，不会重复计费。
func (g *GrpcClient) NewTrafficReportID() string {
	return fmt.Sprintf("%s:%d", g.bootID, g.reportSeq.Add(1))
}

// sendSync 串行发送：gRPC 流的 Send 不支持并发调用，多个入站和心跳共用一条流。
func (g *GrpcClient) sendSync(req *nodev1.SyncRequest) error {
	g.mu.Lock()
	stream := g.sync
	g.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("sync stream not connected")
	}
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return stream.Send(req)
}

// SendTraffic 经 Sync 上报一批流量并等待面板 Ack。
// 返回 nil 表示面板已确认处理（含重复上报）；返回错误时调用方应保留这批流量，用同一个 reportID 重发。
func (g *GrpcClient) SendTraffic(reportID, protocol string, port int, traffic []UserTraffic) error {
	if reportID == "" {
		return fmt.Errorf("report id is required")
	}
	report := convertTrafficToProto(traffic)
	report.Protocol = protocol
	report.Port = clampPort(port)
	report.ReportId = reportID

	waiter := make(chan *nodev1.Ack, 1)
	g.mu.Lock()
	if g.ackWaiters == nil {
		g.ackWaiters = make(map[string]chan *nodev1.Ack)
	}
	g.ackWaiters[reportID] = waiter
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.ackWaiters, reportID)
		g.mu.Unlock()
	}()

	if err := g.sendSync(&nodev1.SyncRequest{Payload: &nodev1.SyncRequest_Traffic{Traffic: report}}); err != nil {
		return err
	}
	timer := time.NewTimer(syncAckTimeout)
	defer timer.Stop()
	select {
	case ack := <-waiter:
		if ack == nil {
			return fmt.Errorf("sync stream closed before ack")
		}
		if !ack.GetOk() {
			return fmt.Errorf("panel rejected traffic report %s: %s", reportID, ack.GetError())
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting for ack of traffic report %s", reportID)
	}
}

// deliverAck 把面板 Ack 交给等待中的 SendTraffic
func (g *GrpcClient) deliverAck(ack *nodev1.Ack) {
	g.mu.Lock()
	waiter := g.ackWaiters[ack.GetReportId()]
	g.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- ack:
		default:
		}
	}
}

// failAckWaiters 流断开时让所有等待中的上报立即失败（交给调用方重发），而不是等到超时
func (g *GrpcClient) failAckWaiters() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, waiter := range g.ackWaiters {
		select {
		case waiter <- nil:
		default:
		}
	}
}

// SendOnlineUsers 经 Sync 上报在线用户（快照，面板直接覆盖，无需 Ack）
func (g *GrpcClient) SendOnlineUsers(protocol string, port int, users []OnlineUser) error {
	report := convertOnlineToProto(users)
	report.Protocol = protocol
	report.Port = clampPort(port)
	return g.sendSync(&nodev1.SyncRequest{Payload: &nodev1.SyncRequest_OnlineUsers{OnlineUsers: report}})
}

// SendStatus 经 Sync 上报节点状态。protocol 必填：面板把没有协议的状态当作心跳忽略。
func (g *GrpcClient) SendStatus(protocol string, port int, cpu, mem, disk float64) error {
	return g.sendSync(&nodev1.SyncRequest{
		Payload: &nodev1.SyncRequest_Status{
			Status: &nodev1.StatusReport{
				Cpu:       cpu,
				Mem:       mem,
				Disk:      disk,
				UpdatedAt: time.Now().UnixMilli(),
				Protocol:  protocol,
				Port:      clampPort(port),
			},
		},
	})
}

func clampPort(port int) uint32 {
	if port <= 0 || port > 65535 {
		return 0
	}
	return uint32(port)
}

// IsSyncConnected returns true if the Sync stream is currently active.
func (g *GrpcClient) IsSyncConnected() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sync != nil
}

// ========== Internal ==========

func (g *GrpcClient) runSyncLoop(handler SyncHandler, done <-chan struct{}) {
	backoff := backoff.Config{
		BaseDelay:  1.0 * time.Second,
		Multiplier: 2.0,
		MaxDelay:   30 * time.Second,
	}
	delay := time.Duration(0)
	unimplementedLogged := false

	for {
		select {
		case <-done:
			return
		default:
		}

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-done:
				return
			}
		}

		ctx := g.authContext(g.syncCtx)
		stream, err := g.client.Sync(ctx)
		if err != nil {
			log.Errorf("[grpc.Sync] failed to start: %v", err)
			delay = nextBackoff(backoff, delay)
			continue
		}

		// 流建立后先观察一段时间再标记为可用：服务端拒绝（如 Unimplemented）时错误在第一次 Recv 才返回，
		// 若立刻标记可用，这期间的流量上报会走这条流并丢失。
		connectedAt := time.Now()
		var streamActive = true
		markReady := time.AfterFunc(syncReadyDelay, func() {
			g.mu.Lock()
			if streamActive {
				g.sync = stream
			}
			g.mu.Unlock()
		})

		log.Info("[grpc.Sync] stream opened")

		// Heartbeat: send keepalive Status every 30s to prevent Cloudflare 524 timeout
		streamDone := make(chan struct{})
		go g.syncHeartbeat(stream, streamDone)

		// Read loop: receive server pushes
		var recvErr error
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr = err
				switch {
				case status.Code(err) == codes.Unimplemented:
					if !unimplementedLogged {
						log.Warnf("[grpc.Sync] 面板未开启 Sync 推送，流量继续走 HTTP，每 %s 重试一次", syncUnimplementedRetry)
						unimplementedLogged = true
					}
				case err == io.EOF || status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded:
					log.Infof("[grpc.Sync] stream closed: %v", err)
				default:
					log.Errorf("[grpc.Sync] Recv error: %v", err)
				}
				break
			}
			// 收到服务端消息说明流可用
			g.mu.Lock()
			if streamActive && g.sync == nil {
				g.sync = stream
			}
			g.mu.Unlock()

			switch payload := resp.Payload.(type) {
			case *nodev1.SyncResponse_UserUpdate:
				if handler != nil {
					u := payload.UserUpdate
					handler.OnUserUpdate(UserUpdate{
						Protocol: u.GetProtocol(),
						Port:     int(u.GetPort()),
						Version:  u.GetVersion(),
						Action:   u.GetAction(),
						Users:    userEntriesToInfo(u.GetUsers()),
						Removed:  userEntriesToInfo(u.GetRemoved()),
					})
				}
			case *nodev1.SyncResponse_ConfigUpdate:
				if handler != nil {
					handler.OnConfigUpdate(payload.ConfigUpdate.Reason)
				}
			case *nodev1.SyncResponse_Ack:
				g.deliverAck(payload.Ack)
			case *nodev1.SyncResponse_Hello:
				log.Info("[grpc.Sync] 面板已确认连接，流量改走 gRPC 上报")
				if handler != nil {
					if err := g.Subscribe(handler.SyncInbounds()...); err != nil {
						log.Warnf("[grpc.Sync] 订阅用户推送失败，继续靠定时拉取: %v", err)
					}
				}
			}
		}

		close(streamDone)
		markReady.Stop()

		g.mu.Lock()
		streamActive = false
		g.sync = nil
		g.mu.Unlock()
		g.failAckWaiters()
		if handler != nil {
			handler.OnSyncDisconnected()
		}

		switch {
		case status.Code(recvErr) == codes.Unimplemented:
			// 面板不支持 Sync：低频重试，避免每秒请求面板
			delay = syncUnimplementedRetry
		case time.Since(connectedAt) >= syncStableDuration:
			// 稳定运行过一段时间后断开：从最小间隔重新开始退避
			unimplementedLogged = false
			delay = backoff.BaseDelay
		default:
			// 建立后很快断开：继续指数退避，不重置
			delay = nextBackoff(backoff, delay)
		}
	}
}

const (
	// syncReadyDelay 流建立后多久才视为可用（收到服务端消息会提前视为可用）
	syncReadyDelay = 5 * time.Second
	// syncStableDuration 流持续多久算稳定，断开后才重置退避
	syncStableDuration = time.Minute
	// syncUnimplementedRetry 面板不支持 Sync 时的重试间隔
	syncUnimplementedRetry = 10 * time.Minute
)

const syncHeartbeatInterval = 30 * time.Second

// syncHeartbeat sends periodic keepalive Status messages on the Sync stream
// to prevent intermediate proxies (Cloudflare/nginx) from timing out the connection.
func (g *GrpcClient) syncHeartbeat(stream nodev1.NodeService_SyncClient, done <-chan struct{}) {
	ticker := time.NewTicker(syncHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			g.sendMu.Lock()
			err := stream.Send(&nodev1.SyncRequest{
				Payload: &nodev1.SyncRequest_Status{
					Status: &nodev1.StatusReport{
						UpdatedAt: time.Now().UnixMilli(),
					},
				},
			})
			g.sendMu.Unlock()
			if err != nil {
				log.Debugf("[grpc.Sync] heartbeat send failed, stream may be closed: %v", err)
				return
			}
		}
	}
}

func (g *GrpcClient) authContext(ctx context.Context) context.Context {
	md := metadata.Pairs(
		"secret-key", g.secretKey,
		"server-id", fmt.Sprintf("%d", g.serverID),
	)
	return metadata.NewOutgoingContext(ctx, md)
}

func nextBackoff(bc backoff.Config, current time.Duration) time.Duration {
	if current == 0 {
		return bc.BaseDelay
	}
	next := time.Duration(float64(current) * bc.Multiplier)
	if next > bc.MaxDelay {
		next = bc.MaxDelay
	}
	return next
}

func convertTrafficToProto(traffic []UserTraffic) *nodev1.TrafficReport {
	entries := make([]*nodev1.TrafficEntry, 0, len(traffic))
	for _, t := range traffic {
		entries = append(entries, &nodev1.TrafficEntry{
			Uid:      int64(t.UID),
			Upload:   t.Upload,
			Download: t.Download,
		})
	}
	return &nodev1.TrafficReport{Traffic: entries}
}

func convertOnlineToProto(users []OnlineUser) *nodev1.OnlineUserReport {
	entries := make([]*nodev1.OnlineUserEntry, 0, len(users))
	for _, u := range users {
		entries = append(entries, &nodev1.OnlineUserEntry{
			Uid: int64(u.UID),
			Ip:  u.IP,
		})
	}
	return &nodev1.OnlineUserReport{Users: entries}
}

// newBootID 每次进程启动生成一个随机标识，与自增序号组成全局唯一的流量上报 ID
func newBootID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
