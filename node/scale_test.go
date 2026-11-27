package node

import (
	"fmt"
	"net/http/httptest"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
)

// U4 规模压测：单入站 10 万条用户（80% 名额条目），走全量替换路径，输出耗时与内存。
// 默认跳过；手动运行：NPNODE_SCALE_TEST=1 go test ./node/ -run TestScale -v -count=1
// 可用 NPNODE_SCALE_USERS 调整条数。

const seatSIDBase = 1_000_000_000

func scaleUsers(n, round int) []panel.UserInfo {
	users := make([]panel.UserInfo, 0, n)
	seats := n * 8 / 10
	for i := 0; i < n; i++ {
		var u panel.UserInfo
		if i < seats {
			u = panel.UserInfo{Id: seatSIDBase + i + 1, Uuid: fmt.Sprintf("5ea70000-0000-4000-8000-%012d", i)}
			// 每一轮“重置”20% 的名额（同一 SID、新 UUID）
			if round > 0 && i%5 == 0 {
				u.Uuid = fmt.Sprintf("5ea7%04d-0000-4000-8000-%012d", round, i)
			}
		} else {
			u = panel.UserInfo{Id: i + 1, Uuid: fmt.Sprintf("50b50000-0000-4000-8000-%012d", i), DeviceLimit: 3}
			// 每一轮改 10% 订阅条目的限速（删旧加新）
			if round > 0 && i%10 == 0 {
				u.SpeedLimit = 10485760 * round
			}
		}
		users = append(users, u)
	}
	return users
}

type memSample struct {
	heap, total uint64
}

func sampleMem() memSample {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memSample{heap: m.HeapAlloc, total: m.TotalAlloc}
}

func maxRSSMiB() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return float64(ru.Maxrss) / (1 << 20) // 字节
	}
	return float64(ru.Maxrss) / 1024 // KiB
}

func mib(b uint64) float64 { return float64(b) / (1 << 20) }

func scaleN() int {
	n := 100000
	if v := os.Getenv("NPNODE_SCALE_USERS"); v != "" {
		_, _ = fmt.Sscan(v, &n)
	}
	return n
}

func startScaleController(t *testing.T, typ string) *Controller {
	t.Helper()
	limiter.Init()
	srv := httptest.NewServer(&fakePanel{})
	t.Cleanup(srv.Close)
	api, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeID: 1, SecretKey: "k", NodeType: typ})
	if err != nil {
		t.Fatal(err)
	}
	core := vCore.New(conf.New(), nil)
	if err := core.Start(&panel.ServerConfigResponse{Data: &panel.Data{}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { core.Close() })
	info := &panel.NodeInfo{Id: 1, Type: typ, PullInterval: 3600, PushInterval: 3600,
		Protocol: &panel.Protocol{Type: typ, Port: freeTCPPort(t), Enable: true, Transport: "tcp"}}
	c := NewController(core, api, info)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func scaleStep(t *testing.T, c *Controller, name string, users []panel.UserInfo) {
	t.Helper()
	before := sampleMem()
	start := time.Now()
	c.userMu.Lock()
	added, deleted, err := c.applyUserSetLocked(users)
	c.userMu.Unlock()
	elapsed := time.Since(start)
	after := sampleMem()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Logf("%-28s 耗时 %8.0f ms  新增 %6d 删除 %6d  分配 %7.1f MiB  常驻堆 %7.1f MiB  峰值 RSS %7.1f MiB",
		name, float64(elapsed.Microseconds())/1000, added, deleted, mib(after.total-before.total), mib(after.heap), maxRSSMiB())
}

func TestScaleFullReplace(t *testing.T) {
	if os.Getenv("NPNODE_SCALE_TEST") == "" {
		t.Skip("规模压测默认跳过；设置 NPNODE_SCALE_TEST=1 运行")
	}
	log.SetLevel(log.ErrorLevel)
	t.Cleanup(func() { log.SetLevel(log.InfoLevel) })
	n := scaleN()
	for _, typ := range []string{"vless", "socks"} {
		t.Run(typ, func(t *testing.T) {
			c := startScaleController(t, typ)
			t.Logf("%s 单入站 %d 条（名额 %d 条）", typ, n, n*8/10)
			scaleStep(t, c, "首次全量（空 → N）", scaleUsers(n, 0))
			scaleStep(t, c, "全量无变化（只比对）", scaleUsers(n, 0))
			scaleStep(t, c, "全量替换（重置20%名额+改10%限速）", scaleUsers(n, 1))

			// compareUserList 单独计时：每轮全量都要建 map
			old, next := scaleUsers(n, 1), scaleUsers(n, 2)
			before := sampleMem()
			start := time.Now()
			del, add := compareUserList(old, next)
			t.Logf("%-28s 耗时 %8.0f ms  新增 %6d 删除 %6d  分配 %7.1f MiB", "compareUserList", float64(time.Since(start).Microseconds())/1000, len(add), len(del), mib(sampleMem().total-before.total))

			// 推送 DIFF：只改 1 条
			c.userMu.Lock()
			c.syncVersion = 1
			c.userMu.Unlock()
			one := scaleUsers(n, 1)[0]
			changed := one
			changed.Uuid = "5ea7ffff-0000-4000-8000-000000000000"
			start = time.Now()
			c.ApplyUserUpdate(panel.UserUpdate{Protocol: typ, Port: c.info.Protocol.Port, Version: 2, Action: nodev1.UserUpdateAction_USER_UPDATE_DIFF,
				Removed: []panel.UserInfo{one}, Users: []panel.UserInfo{changed}})
			t.Logf("%-28s 耗时 %8.0f ms", "DIFF 改 1 条", float64(time.Since(start).Microseconds())/1000)
			if got := len(c.userList); got != n {
				t.Fatalf("userList = %d, want %d", got, n)
			}
		})
	}
}
