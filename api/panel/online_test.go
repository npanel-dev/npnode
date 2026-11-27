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

func newOnlineTestClient(t *testing.T, respBody string, gotBody *string) *ClientV1 {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		*gotBody = string(raw)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	client, err := NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeID: 7, SecretKey: "secret", NodeType: "vless", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// 面板按小写键 uid / ip 解析在线上报（旧版面板只认小写，大写会整批被拒）
func TestReportNodeOnlineUsersUsesLowercaseKeys(t *testing.T) {
	var body string
	client := newOnlineTestClient(t, `{"code":200,"msg":"success"}`, &body)
	users := []OnlineUser{{UID: 1000000045, IP: "203.0.113.7"}}
	if err := client.ReportNodeOnlineUsers(context.Background(), &users); err != nil {
		t.Fatal(err)
	}
	if want := `{"users":[{"uid":1000000045,"ip":"203.0.113.7"}]}`; body != want {
		t.Fatalf("request body = %s, want %s", body, want)
	}
	if strings.Contains(body, `"UID"`) || strings.Contains(body, `"IP"`) {
		t.Fatalf("request body still has uppercase keys: %s", body)
	}
}

// 面板业务失败时仍返回 HTTP 200，错误码在响应体 code 里，必须当作失败
func TestReportNodeOnlineUsersChecksBodyCode(t *testing.T) {
	var body string
	client := newOnlineTestClient(t, `{"code":500,"msg":"Internal Server Error"}`, &body)
	users := []OnlineUser{{UID: 7, IP: "203.0.113.7"}}
	if err := client.ReportNodeOnlineUsers(context.Background(), &users); err == nil {
		t.Fatal("body code 500 must be reported as an error")
	}
}
