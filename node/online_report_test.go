package node

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/limiter"
)

// O2：在线上报周期取 min(PushInterval, 60s)
func TestOnlineReportIntervalCapsAt60s(t *testing.T) {
	for push, want := range map[int]time.Duration{0: 60 * time.Second, 30: 30 * time.Second, 60: 60 * time.Second, 120: 60 * time.Second, 600: 60 * time.Second} {
		if got := onlineReportInterval(push); got != want {
			t.Errorf("push=%d: interval = %v, want %v", push, got, want)
		}
	}
}

type onlineRecorder struct {
	mu     sync.Mutex
	bodies []string
	fail   bool
}

func newOnlineController(t *testing.T, online bool) (*Controller, *onlineRecorder, string) {
	t.Helper()
	rec := &onlineRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/server/online" {
			raw, _ := io.ReadAll(r.Body)
			rec.mu.Lock()
			rec.bodies = append(rec.bodies, string(raw))
			fail := rec.fail
			rec.mu.Unlock()
			if fail {
				_, _ = io.WriteString(w, `{"code":500,"msg":"Internal Server Error"}`)
				return
			}
		}
		_, _ = io.WriteString(w, `{"code":200,"msg":"success"}`)
	}))
	t.Cleanup(srv.Close)
	panel.SetOnlineV1(online)
	t.Cleanup(func() { panel.SetOnlineV1(false) })
	limiter.Init()
	tag := "[t]-vless:1:443"
	l := limiter.AddLimiter(tag, []panel.UserInfo{{Id: 1000000045, Uuid: "u-45"}}, nil)
	t.Cleanup(func() { limiter.DeleteLimiter(tag) })
	c := &Controller{apiClient: trafficTestClient(t, srv.URL, 443), limiter: l, tag: tag,
		info: &panel.NodeInfo{Type: "vless", PushInterval: 120, Protocol: &panel.Protocol{Port: 443}}}
	return c, rec, tag
}

// online_v1：在线上报独立于流量任务，按活跃连接上报，不再按“本周期流量 ≤0”过滤；没有在线时报空列表
func TestOnlineTaskReportsActiveConnectionsWithoutTrafficFilter(t *testing.T) {
	c, rec, tag := newOnlineController(t, true)
	if c.trafficTaskReportsOnline() {
		t.Fatal("with online_v1 the traffic task must not report online users")
	}
	release := c.limiter.OpenConn(format.UserTag(tag, "u-45"), "203.0.113.7")
	for i := 0; i < 2; i++ {
		if err := c.reportOnlineUsersTask(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	release()
	_ = c.reportOnlineUsersTask(context.Background())
	_ = c.reportOnlineUsersTask(context.Background())
	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := []string{
		`{"users":[{"uid":1000000045,"ip":"203.0.113.7"}]}`,
		`{"users":[{"uid":1000000045,"ip":"203.0.113.7"}]}`,
		`{"users":[{"uid":1000000045,"ip":"203.0.113.7"}]}`,
		`{"users":[]}`,
	}
	if len(rec.bodies) != len(want) {
		t.Fatalf("reports = %v", rec.bodies)
	}
	for i := range want {
		if rec.bodies[i] != want[i] {
			t.Fatalf("report %d = %s, want %s", i, rec.bodies[i], want[i])
		}
	}
}

// 关闭 online_v1（默认）：在线仍随流量任务上报，口径不变
func TestOnlineStaysInTrafficTaskWhenOnlineV1Off(t *testing.T) {
	c, _, _ := newOnlineController(t, false)
	if !c.trafficTaskReportsOnline() {
		t.Fatal("without online_v1 the traffic task must keep reporting online users")
	}
}

// 上报失败时本周期的短连接不丢，下一周期补报（审查 M3）
func TestOnlineTaskKeepsShortConnectionAfterFailedReport(t *testing.T) {
	c, rec, tag := newOnlineController(t, true)
	c.limiter.OpenConn(format.UserTag(tag, "u-45"), "203.0.113.9")()
	rec.mu.Lock()
	rec.fail = true
	rec.mu.Unlock()
	_ = c.reportOnlineUsersTask(context.Background())
	rec.mu.Lock()
	rec.fail = false
	rec.mu.Unlock()
	_ = c.reportOnlineUsersTask(context.Background())
	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := `{"users":[{"uid":1000000045,"ip":"203.0.113.9"}]}`
	if len(rec.bodies) != 2 || rec.bodies[1] != want {
		t.Fatalf("reports = %v, want second report %s", rec.bodies, want)
	}
}
