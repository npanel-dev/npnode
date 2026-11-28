package panel

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
)

type sessionRecorder struct {
	mu     sync.Mutex
	paths  []string
	bodies []map[string]interface{}
	query  []string
	agents []string
	status int
	reply  string
	delay  time.Duration
}

func (r *sessionRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	raw, _ := io.ReadAll(req.Body)
	var body map[string]interface{}
	_ = json.Unmarshal(raw, &body)
	r.mu.Lock()
	r.paths = append(r.paths, req.Method+" "+req.URL.Path)
	r.bodies = append(r.bodies, body)
	r.query = append(r.query, req.URL.RawQuery)
	r.agents = append(r.agents, req.Header.Get("X-Node-Agent"))
	status, reply, delay := r.status, r.reply, r.delay
	r.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if status == 0 {
		status = 200
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, reply)
}

func newSessionTest(t *testing.T) (*sessionRecorder, *SessionClient) {
	t.Helper()
	SetSeatV1(true)
	SetOnlineV1(true)
	SetAdmissionV1(true)
	old := NodeVersion
	NodeVersion = "1.2.3"
	t.Cleanup(func() {
		SetSeatV1(false)
		SetOnlineV1(false)
		SetAdmissionV1(false)
		NodeVersion = old
	})
	rec := &sessionRecorder{}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	c := NewSessionClient(&conf.ServerApiConfig{ApiHost: srv.URL, ServerId: 12, SecretKey: "sk", Timeout: 30})
	return rec, c
}

// check 请求体字段按面板 compatLegacyParseSessionBody：server_id、secret_key 在请求体里，capabilities 为逗号分隔字符串；
// 不带查询参数（面板先读查询参数，查询里的 port 会覆盖请求体）；带 X-Node-Agent
func TestSessionCheckRequestAndResponse(t *testing.T) {
	rec, c := newSessionTest(t)
	rec.reply = `{"allowed":false,"current":1,"limit":1,"reason":"device_limit"}`
	res, err := c.Check(t.Context(), SessionRequest{Protocol: "vless", Port: 443, UserID: 1000000123, ClientIP: "203.0.113.7", ConnectionID: "12:203.0.113.7:1764200000123"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed || res.Current != 1 || res.Limit != 1 || res.Reason != "device_limit" {
		t.Fatalf("result = %+v", res)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.paths[0] != "POST /v1/server/session/check" || rec.query[0] != "" || rec.agents[0] != "npnode/1.2.3" {
		t.Fatalf("path=%s query=%q agent=%q", rec.paths[0], rec.query[0], rec.agents[0])
	}
	want := map[string]interface{}{
		"server_id": 12.0, "secret_key": "sk", "protocol": "vless", "port": 443.0, "user_id": 1000000123.0,
		"client_ip": "203.0.113.7", "capabilities": "seat_v1,online_v1,admission_v1", "connection_id": "12:203.0.113.7:1764200000123",
	}
	got := rec.bodies[0]
	if len(got) != len(want) {
		t.Fatalf("body = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("body[%s] = %v, want %v", k, got[k], v)
		}
	}
}

func TestSessionReleaseRequest(t *testing.T) {
	rec, c := newSessionTest(t)
	rec.reply = `{}`
	if err := c.Release(t.Context(), SessionRequest{Port: 443, UserID: 7, ClientIP: "2001:db8::1", ConnectionID: "12:2001:db8::1:1"}); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.paths[0] != "POST /v1/server/session/release" {
		t.Fatal(rec.paths[0])
	}
	if _, ok := rec.bodies[0]["protocol"]; ok {
		t.Error("release 未填 protocol 时不应出现该字段")
	}
	if rec.bodies[0]["user_id"] != 7.0 || rec.bodies[0]["client_ip"] != "2001:db8::1" || rec.bodies[0]["capabilities"] != "seat_v1,online_v1,admission_v1" {
		t.Fatalf("body = %v", rec.bodies[0])
	}
}

// 面板不可用：5xx、403、400、非法 JSON、缺少 allowed 都返回错误；400/403 标记为配置或实现错误
func TestSessionCheckErrors(t *testing.T) {
	cases := []struct {
		status    int
		reply     string
		misconfig bool
	}{
		{500, "Internal Server Error", false},
		{502, "", false},
		{403, "Forbidden", true},
		{400, "Invalid Params", true},
		{200, "not json", false},
		{200, `{"current":1}`, false},
	}
	for _, tc := range cases {
		rec, c := newSessionTest(t)
		rec.status, rec.reply = tc.status, tc.reply
		_, err := c.Check(t.Context(), SessionRequest{UserID: 1, ClientIP: "203.0.113.7"})
		if err == nil {
			t.Errorf("status=%d reply=%q: want error", tc.status, tc.reply)
			continue
		}
		var se *SessionError
		got := errors.As(err, &se) && se.Misconfigured()
		if got != tc.misconfig {
			t.Errorf("status=%d: misconfigured = %v, want %v (err=%v)", tc.status, got, tc.misconfig, err)
		}
	}
	rec, c := newSessionTest(t)
	rec.status = 500
	if err := c.Release(t.Context(), SessionRequest{UserID: 1, ClientIP: "203.0.113.7"}); err == nil {
		t.Error("release 500 must be an error")
	}
}

// 单次超时 2 秒（与 30 秒的业务客户端分开），不自动重试
func TestSessionCheckTimeoutAndNoRetry(t *testing.T) {
	rec, c := newSessionTest(t)
	rec.delay = 3 * time.Second
	start := time.Now()
	_, err := c.Check(t.Context(), SessionRequest{UserID: 1, ClientIP: "203.0.113.7"})
	if err == nil {
		t.Fatal("want timeout error")
	}
	if d := time.Since(start); d < 1900*time.Millisecond || d > 2800*time.Millisecond {
		t.Fatalf("check took %v, want about 2s", d)
	}
	rec.mu.Lock()
	n := len(rec.paths)
	rec.mu.Unlock()
	if n != 1 {
		t.Fatalf("requests = %d, want 1 (no retry)", n)
	}
}
