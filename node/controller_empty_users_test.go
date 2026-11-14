package node

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
)

type fakePanel struct {
	mu    sync.Mutex
	users []panel.UserInfo
}

func (f *fakePanel) setUsers(users []panel.UserInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users = users
}

func (f *fakePanel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/v1/server/user" {
		_, _ = w.Write([]byte(`{}`))
		return
	}
	f.mu.Lock()
	users := f.users
	f.mu.Unlock()
	if users == nil {
		users = []panel.UserInfo{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"users": users})
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// 回归：面板没有有效用户时节点曾直接报 "not have any user" 启动失败，
// 入站和定时任务都不建立，之后面板有了用户也不会自动恢复。
func TestControllerStartsWithoutUsersAndSyncsLater(t *testing.T) {
	limiter.Init() // 与 cmd/server.go 启动流程一致
	fp := &fakePanel{}
	srv := httptest.NewServer(fp)
	defer srv.Close()

	api, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeID: 1, SecretKey: "k", NodeType: "shadowsocks"})
	if err != nil {
		t.Fatal(err)
	}

	core := vCore.New(conf.New(), nil)
	if err := core.Start(&panel.ServerConfigResponse{Data: &panel.Data{}}); err != nil {
		t.Fatalf("start xray core: %v", err)
	}
	defer core.Close()

	info := &panel.NodeInfo{
		Id:           1,
		Type:         "shadowsocks",
		PullInterval: 3600,
		PushInterval: 3600,
		Protocol: &panel.Protocol{
			Type:   "shadowsocks",
			Port:   freeTCPPort(t),
			Cipher: "aes-128-gcm",
		},
	}
	c := NewController(core, api, info)
	if err := c.Start(); err != nil {
		t.Fatalf("Start with no users returned error: %v", err)
	}
	defer c.Close()

	// 面板加了用户：下一次同步应加进来
	fp.setUsers([]panel.UserInfo{{Id: 10, Uuid: "11111111-1111-1111-1111-111111111111"}})
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.userList) != 1 {
		t.Fatalf("after panel added a user: userList = %d, want 1", len(c.userList))
	}

	// 面板用户全部移除：下一次同步应删除，而不是当成 304 未变化
	fp.setUsers(nil)
	if err := c.userListMonitor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.userList) != 0 {
		t.Fatalf("after panel removed all users: userList = %d, want 0", len(c.userList))
	}
}
