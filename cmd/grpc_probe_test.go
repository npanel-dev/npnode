package cmd

import (
	"os"
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

// 配置关闭 gRPC、或面板 gRPC 仍不可用时不切换
func TestGrpcAvailableAgain(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := dir + "/" + name
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if grpcAvailableAgain(write("off.yml", "Api:\n  ApiHost: https://x.example\nGrpc:\n  Enable: false\n")) {
		t.Fatal("grpc disabled in config but reported available")
	}
	if grpcAvailableAgain(write("down.yml", "Api:\n  ApiHost: https://x.example\nGrpc:\n  Enable: true\n  Addr: 127.0.0.1:1\n")) {
		t.Fatal("unreachable panel reported available")
	}
	if grpcRecoveryInterval != 4*time.Hour {
		t.Fatalf("interval = %s", grpcRecoveryInterval)
	}
}
