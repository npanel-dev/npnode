package panel

import (
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
