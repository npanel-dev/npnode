package panel

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/npanel-dev/npnode/conf"
)

func TestNewClientV1IncludesPortQueryParam(t *testing.T) {
	client, err := NewClientV1(&conf.NodeApiConfig{
		APIHost:   "https://panel.example",
		NodeID:    7,
		SecretKey: "secret",
		NodeType:  "vless",
		Port:      8443,
	})
	if err != nil {
		t.Fatalf("NewClientV1 returned error: %v", err)
	}

	if got := client.NodePort; got != 8443 {
		t.Fatalf("NodePort = %d, want 8443", got)
	}
	if got := client.Client.QueryParam.Get("port"); got != "8443" {
		t.Fatalf("query port = %q, want 8443", got)
	}
}

func TestNewClientV1OmitsZeroPortQueryParam(t *testing.T) {
	client, err := NewClientV1(&conf.NodeApiConfig{
		APIHost:   "https://panel.example",
		NodeID:    7,
		SecretKey: "secret",
		NodeType:  "vless",
	})
	if err != nil {
		t.Fatalf("NewClientV1 returned error: %v", err)
	}

	if got := client.Client.QueryParam.Get("port"); got != "" {
		t.Fatalf("query port = %q, want empty", got)
	}
}

func TestReportNodeOnlineUsersSendsEmptySnapshot(t *testing.T) {
	var (
		body    string
		readErr error
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw []byte
		raw, readErr = io.ReadAll(r.Body)
		body = string(raw)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClientV1(&conf.NodeApiConfig{
		APIHost:   server.URL,
		NodeID:    7,
		SecretKey: "secret",
		NodeType:  "vless",
		Port:      8443,
	})
	if err != nil {
		t.Fatalf("NewClientV1 returned error: %v", err)
	}
	users := make([]OnlineUser, 0)
	if err := client.ReportNodeOnlineUsers(context.Background(), &users); err != nil {
		t.Fatalf("ReportNodeOnlineUsers returned error: %v", err)
	}
	if readErr != nil {
		t.Fatalf("read request body: %v", readErr)
	}
	if !strings.Contains(body, `"users":[]`) {
		t.Fatalf("request body = %s, want users: []", body)
	}
}
