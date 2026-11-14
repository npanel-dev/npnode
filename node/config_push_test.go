package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
)

// configPanel 假面板的 /v2/server/{id}：支持 ETag，记录请求次数
type configPanel struct {
	mu   sync.Mutex
	body string
	hits atomic.Int32
}

func (p *configPanel) set(pullInterval string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.body = `{"code":0,"data":{"pull_interval":` + pullInterval + `,"push_interval":60,"protocols":[]}}`
}

func (p *configPanel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.hits.Add(1)
	p.mu.Lock()
	body := p.body
	p.mu.Unlock()
	sum := sha256.Sum256([]byte(body))
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	_, _ = w.Write([]byte(body))
}

func TestOnConfigUpdateChecksImmediatelyAndReloadsOnlyOnChange(t *testing.T) {
	fake := &configPanel{}
	fake.set("10")
	srv := httptest.NewServer(fake)
	defer srv.Close()

	client := panel.NewClientV2(&conf.ServerApiConfig{ApiHost: srv.URL, ServerId: 6, SecretKey: "k"})
	// 与启动流程一致：先取一次配置，记下 ETag
	if _, err := panel.GetServerConfig(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	reload := make(chan struct{}, 1)
	n := &Node{core: &vCore.XrayCore{Client: client, ReloadCh: reload}, configCheck: make(chan struct{}, 1)}

	// 面板通知了，但配置其实没变（ETag 命中）：检查但不重载
	n.OnConfigUpdate("test")
	waitFor(t, "config check request", func() bool { return fake.hits.Load() >= 2 })
	select {
	case <-reload:
		t.Fatal("unchanged config must not reload")
	case <-time.After(200 * time.Millisecond):
	}

	// 配置变了：立即重载
	fake.set("20")
	n.OnConfigUpdate("test")
	select {
	case <-reload:
	case <-time.After(3 * time.Second):
		t.Fatal("changed config did not trigger reload")
	}

	// 连续通知合并，不会一口气打出一堆请求
	before := fake.hits.Load()
	for i := 0; i < 20; i++ {
		n.OnConfigUpdate("burst")
	}
	time.Sleep(300 * time.Millisecond)
	if extra := fake.hits.Load() - before; extra > 3 {
		t.Fatalf("burst of 20 notices caused %d config requests", extra)
	}
}
