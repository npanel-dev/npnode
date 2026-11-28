package node

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
)

func newAdmissionTestNode(t *testing.T) *Node {
	t.Helper()
	cfg := conf.New()
	cfg.ApiConfig = conf.ServerApiConfig{ApiHost: "http://127.0.0.1:1", ServerId: 12, SecretKey: "k"}
	n, err := New(&vCore.XrayCore{ReloadCh: make(chan struct{}, 1)}, cfg,
		&panel.ServerConfigResponse{Data: &panel.Data{Protocols: &[]panel.Protocol{}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// admission_v1：Node.Start 建立并启动进程级准入（入站启动前，限速器才能挂上），Close 停止；重载后沿用同一张会话表
func TestNodeStartsAndStopsAdmission(t *testing.T) {
	panel.SetAdmissionV1(true)
	t.Cleanup(func() {
		limiter.DisableAdmission()
		panel.SetAdmissionV1(false)
	})
	n := newAdmissionTestNode(t)
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	a := limiter.CurrentAdmission()
	if a == nil {
		t.Fatal("admission must be created on Start")
	}
	n.Close()
	n2 := newAdmissionTestNode(t)
	if err := n2.Start(); err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if limiter.CurrentAdmission() != a {
		t.Fatal("reload must reuse the session table")
	}
}

// 重载后关闭了 admission_v1：撤掉进程级准入，之后建立的限速器不再准入
func TestNodeDisablesAdmissionWhenSwitchedOff(t *testing.T) {
	panel.SetAdmissionV1(true)
	t.Cleanup(func() {
		limiter.DisableAdmission()
		panel.SetAdmissionV1(false)
	})
	n := newAdmissionTestNode(t)
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	n.Close()
	panel.SetAdmissionV1(false)
	n2 := newAdmissionTestNode(t)
	if err := n2.Start(); err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if limiter.CurrentAdmission() != nil {
		t.Fatal("admission must be disabled")
	}
}
