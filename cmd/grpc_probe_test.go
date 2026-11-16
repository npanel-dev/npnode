package cmd

import (
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
)

// 面板没有开放 gRPC：探测失败，返回 nil，节点改走 HTTP（不会卡住启动太久）
func TestCheckedGrpcClientFallsBackWhenUnavailable(t *testing.T) {
	start := time.Now()
	if gc := newCheckedGrpcClient(&conf.GrpcConfig{Enable: true, Addr: "127.0.0.1:1"}, "k", 1); gc != nil {
		t.Fatal("expected nil client for unreachable panel")
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("probe took too long: %s", d)
	}
}
