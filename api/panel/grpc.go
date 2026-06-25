package panel

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
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
func (g *GrpcClient) GetUserList(ctx context.Context, protocol string) ([]UserInfo, error) {
	ctx = g.authContext(ctx)
	resp, err := g.client.GetUserList(ctx, &nodev1.GetUserListRequest{
		ServerId:  g.serverID,
		SecretKey: g.secretKey,
		Protocol:  protocol,
	})
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
	g.syncDone = make(chan struct{})
	g.syncCtx = ctx
	g.mu.Unlock()

	go g.runSyncLoop(handler)
}

// SyncHandler handles incoming messages from the Sync stream.
type SyncHandler interface {
	OnUserUpdate(users []UserInfo, action nodev1.UserUpdateAction)
	OnConfigUpdate(reason string)
}

// SendTraffic sends a traffic report via the Sync stream.
func (g *GrpcClient) SendTraffic(traffic []UserTraffic) error {
	g.mu.Lock()
	stream := g.sync
	g.mu.Unlock()

	if stream == nil {
		return fmt.Errorf("sync stream not connected")
	}

	return stream.Send(&nodev1.SyncRequest{
		Payload: &nodev1.SyncRequest_Traffic{
			Traffic: convertTrafficToProto(traffic),
		},
	})
}

// SendOnlineUsers sends an online user report via the Sync stream.
func (g *GrpcClient) SendOnlineUsers(users []OnlineUser) error {
	g.mu.Lock()
	stream := g.sync
	g.mu.Unlock()

	if stream == nil {
		return fmt.Errorf("sync stream not connected")
	}

	return stream.Send(&nodev1.SyncRequest{
		Payload: &nodev1.SyncRequest_OnlineUsers{
			OnlineUsers: convertOnlineToProto(users),
		},
	})
}

// SendStatus sends a status report via the Sync stream.
func (g *GrpcClient) SendStatus(cpu, mem, disk float64) error {
	g.mu.Lock()
	stream := g.sync
	g.mu.Unlock()

	if stream == nil {
		return fmt.Errorf("sync stream not connected")
	}

	return stream.Send(&nodev1.SyncRequest{
		Payload: &nodev1.SyncRequest_Status{
			Status: &nodev1.StatusReport{
				Cpu:       cpu,
				Mem:       mem,
				Disk:      disk,
				UpdatedAt: time.Now().UnixMilli(),
			},
		},
	})
}

// IsSyncConnected returns true if the Sync stream is currently active.
func (g *GrpcClient) IsSyncConnected() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sync != nil
}

// ========== Internal ==========

func (g *GrpcClient) runSyncLoop(handler SyncHandler) {
	backoff := backoff.Config{
		BaseDelay:  1.0 * time.Second,
		Multiplier: 2.0,
		MaxDelay:   30 * time.Second,
	}
	delay := time.Duration(0)

	for {
		select {
		case <-g.syncDone:
			return
		default:
		}

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-g.syncDone:
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

		g.mu.Lock()
		g.sync = stream
		g.mu.Unlock()
		delay = 0 // reset on successful connection

		log.Info("[grpc.Sync] stream connected")

		// Heartbeat: send keepalive Status every 30s to prevent Cloudflare 524 timeout
		streamDone := make(chan struct{})
		go g.syncHeartbeat(stream, streamDone)

		// Read loop: receive server pushes
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
					log.Infof("[grpc.Sync] stream closed: %v", err)
				} else {
					log.Errorf("[grpc.Sync] Recv error: %v", err)
				}
				break
			}

			switch payload := resp.Payload.(type) {
			case *nodev1.SyncResponse_UserUpdate:
				users := make([]UserInfo, 0, len(payload.UserUpdate.Users))
				for _, u := range payload.UserUpdate.Users {
					users = append(users, UserInfo{
						Id:          int(u.Id),
						Uuid:        u.Uuid,
						SpeedLimit:  int(u.SpeedLimit),
						DeviceLimit: int(u.DeviceLimit),
					})
				}
				if handler != nil {
					handler.OnUserUpdate(users, payload.UserUpdate.Action)
				}
			case *nodev1.SyncResponse_ConfigUpdate:
				if handler != nil {
					handler.OnConfigUpdate(payload.ConfigUpdate.Reason)
				}
			}
		}

		close(streamDone)

		g.mu.Lock()
		g.sync = nil
		g.mu.Unlock()

		delay = nextBackoff(backoff, delay)
	}
}

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
			if err := stream.Send(&nodev1.SyncRequest{
				Payload: &nodev1.SyncRequest_Status{
					Status: &nodev1.StatusReport{
						UpdatedAt: time.Now().UnixMilli(),
					},
				},
			}); err != nil {
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
