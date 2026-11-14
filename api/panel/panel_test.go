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

func newUserListTestClient(t *testing.T, body string) *ClientV1 {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", "etag-1")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	client, err := NewClientV1(&conf.NodeApiConfig{
		APIHost:   server.URL,
		NodeID:    7,
		SecretKey: "secret",
		NodeType:  "vless",
	})
	if err != nil {
		t.Fatalf("NewClientV1 returned error: %v", err)
	}
	return client
}

// 空用户列表必须返回非 nil 切片：nil 在调用方表示 HTTP 304 未变化，
// 返回 nil 会导致用户被全部移除时节点不删除旧用户。
func TestGetUserListEmptyReturnsNonNilSlice(t *testing.T) {
	for name, body := range map[string]string{
		"empty array": `{"users":[]}`,
		"null":        `{"users":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := newUserListTestClient(t, body)
			users, err := client.GetUserList(context.Background())
			if err != nil {
				t.Fatalf("GetUserList returned error: %v", err)
			}
			if users == nil {
				t.Fatal("GetUserList returned nil for an empty list; callers treat nil as 304 Not Modified")
			}
			if len(users) != 0 {
				t.Fatalf("len(users) = %d, want 0", len(users))
			}
			if client.userEtag != "etag-1" {
				t.Fatalf("ETag not stored: %q", client.userEtag)
			}
		})
	}
}

func TestGetUserListParsesUsers(t *testing.T) {
	client := newUserListTestClient(t, `{"users":[{"id":1,"uuid":"a"},{"id":2,"uuid":"b"}]}`)
	users, err := client.GetUserList(context.Background())
	if err != nil {
		t.Fatalf("GetUserList returned error: %v", err)
	}
	if len(users) != 2 || users[0].Id != 1 || users[1].Uuid != "b" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

// 面板出错时返回 HTTP 200 + {"code":500}：必须当作失败，否则流量被当成已上报而丢失。
func TestReportUserTrafficTreatsBodyErrorCodeAsFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		body    string
		wantErr bool
	}{
		"panel error code": {`{"code":500,"msg":"traffic queue unavailable"}`, true},
		"success":          {`{"code":200,"msg":"success"}`, false},
		"empty body":       {``, false},
	} {
		t.Run(name, func(t *testing.T) {
			client := newUserListTestClient(t, tc.body)
			err := client.ReportUserTraffic(context.Background(), &[]UserTraffic{{UID: 1, Upload: 1}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
