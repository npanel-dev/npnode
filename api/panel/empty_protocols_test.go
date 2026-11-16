package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/npanel-dev/npnode/conf"
)

func serveConfig(t *testing.T, body string) *ClientV2 {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewClientV2(&conf.ServerApiConfig{ApiHost: srv.URL, ServerId: 7, SecretKey: "k"})
}

// 没有协议的服务器（面板下发 [] 或旧面板下发 null）都能正常对接
func TestGetServerConfigAcceptsEmptyProtocols(t *testing.T) {
	for _, body := range []string{
		`{"data":{"protocols":[],"total":0}}`,
		`{"data":{"protocols":null,"total":0}}`,
	} {
		resp, err := GetServerConfig(context.Background(), serveConfig(t, body))
		if err != nil {
			t.Fatalf("%s: unexpected error %v", body, err)
		}
		if resp.Data.Protocols == nil || len(*resp.Data.Protocols) != 0 {
			t.Fatalf("%s: expected empty protocol list, got %#v", body, resp.Data.Protocols)
		}
	}
}

func TestReportServerStatusSendsServerID(t *testing.T) {
	var gotQuery string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := NewClientV2(&conf.ServerApiConfig{ApiHost: srv.URL, ServerId: 7, SecretKey: "k"})
	if err := c.ReportServerStatus(&NodeStatus{CPU: 1, Mem: 2, Disk: 3}); err != nil {
		t.Fatal(err)
	}
	if gotQuery != "secret_key=k&server_id=7" {
		t.Fatalf("query = %q", gotQuery)
	}
	if gotBody["mem"] == nil {
		t.Fatalf("body missing status: %v", gotBody)
	}
}
