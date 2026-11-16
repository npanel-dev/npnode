package node

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
)

// 没有协议时节点照常启动，并按服务器上报状态让面板显示在线
func TestNodeWithoutProtocolsReportsServerStatus(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case got <- r.URL.Path + "?" + r.URL.Query().Get("server_id"):
		default:
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	n := &Node{
		core:         &vCore.XrayCore{ReloadCh: make(chan struct{}, 1)},
		statusClient: panel.NewClientV2(&conf.ServerApiConfig{ApiHost: srv.URL, ServerId: 9, SecretKey: "k"}),
		pushInterval: 60,
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	select {
	case req := <-got:
		if req != "/v1/server/status?9" {
			t.Fatalf("unexpected request %q", req)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server status was not reported")
	}
}
