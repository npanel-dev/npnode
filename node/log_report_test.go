package node

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/logreport"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// logSender 收集 Reporter 发出的批次
type logSender struct {
	mu    sync.Mutex
	calls []*nodev1.LogReport
}

func (s *logSender) SendLogs(_ context.Context, r *nodev1.LogReport) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r)
	return nil
}

// useTestReporter 把 logreport.Default 换成带假 Sender 的 Reporter，测试结束后还原
func useTestReporter(t *testing.T) *logSender {
	t.Helper()
	s := &logSender{}
	prev := logreport.Default
	logreport.Default = logreport.NewReporter(logreport.Options{Sender: s, Version: "test"})
	t.Cleanup(func() { logreport.Default = prev })
	return s
}

// 入站启动失败时，Node.Start 的错误带上错误码和入站信息
func TestNodeStartErrorCarriesInbound(t *testing.T) {
	limiter.Init()
	fp := &fakePanel{}
	srv := httptest.NewServer(fp)
	defer srv.Close()
	api, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeID: 1, SecretKey: "k", NodeType: "vless"})
	if err != nil {
		t.Fatal(err)
	}
	core := vCore.New(conf.New(), nil)
	if err := core.Start(&panel.ServerConfigResponse{Data: &panel.Data{}}); err != nil {
		t.Fatal(err)
	}
	defer core.Close()

	info := &panel.NodeInfo{
		Id: 1, Type: "vless", PullInterval: 3600, PushInterval: 3600,
		Protocol: &panel.Protocol{
			Type: "vless", Port: 443, Enable: true,
			Encryption: "mlkem768x25519plus", EncryptionMode: "",
		},
	}
	n := &Node{core: core, controllers: []*Controller{NewController(core, api, info)}}
	err = n.Start()
	if err == nil {
		t.Fatal("应启动失败")
	}
	code, stage, protocol, port, ok := logreport.Classify(err)
	if !ok || code != logreport.CodeVlessEncModeEmpty || stage != logreport.StageInbound || protocol != "vless" || port != 443 {
		t.Fatalf("Classify = %q %q %q %d %v", code, stage, protocol, port, ok)
	}
}

// 无协议服务器启动成功后发出 scope=server、active 为空的恢复信号
func TestNodeStartEmitsServerResolve(t *testing.T) {
	s := useTestReporter(t)
	n := &Node{
		core:         &vCore.XrayCore{ReloadCh: make(chan struct{}, 1)},
		statusClient: panel.NewClientV2(&conf.ServerApiConfig{ApiHost: "http://127.0.0.1:1", ServerId: 9, SecretKey: "k"}),
		pushInterval: 3600,
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	_ = logreport.Default.FlushFatal(2 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, r := range s.calls {
		for _, rs := range r.Resolves {
			if rs.Scope == logreport.ScopeServer && len(rs.ActiveInbounds) == 0 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("未发现 scope=server 的恢复信号: %v", s.calls)
	}
}
