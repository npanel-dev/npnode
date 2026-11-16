package conf

import (
	"os"
	"path/filepath"
	"testing"
)

func load(t *testing.T, body string) *Conf {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c := New()
	if err := c.LoadFromPath(p); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGrpcDefaultsOnWithAddrFromApiHost(t *testing.T) {
	// 安装脚本生成的配置没有 Grpc 段
	c := load(t, "Api:\n  ApiHost: https://admin.bearru.cc/api\n  ServerID: 6\n  SecretKey: k\n")
	if !c.GrpcConfig.Enable || c.GrpcConfig.Addr != "https://admin.bearru.cc" {
		t.Fatalf("grpc = %+v", c.GrpcConfig)
	}
	// 明确关闭的保持关闭
	c = load(t, "Api:\n  ApiHost: https://admin.bearru.cc/api\nGrpc:\n  Enable: false\n")
	if c.GrpcConfig.Enable {
		t.Fatal("explicit Enable: false was overridden")
	}
	// 手动写了地址的不改
	c = load(t, "Api:\n  ApiHost: https://admin.bearru.cc/api\nGrpc:\n  Enable: true\n  Addr: https://grpc.example.com\n")
	if c.GrpcConfig.Addr != "https://grpc.example.com" {
		t.Fatalf("addr overridden: %q", c.GrpcConfig.Addr)
	}
	// 没有 ApiHost 时推不出地址，关闭
	c = load(t, "Api:\n  ServerID: 1\n")
	if c.GrpcConfig.Enable {
		t.Fatal("grpc enabled without address")
	}
}
