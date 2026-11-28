package core

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/logreport"
	coreConf "github.com/xtls/xray-core/infra/conf"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

func TestBuildInboundTreatsNoneObfsAsEmpty(t *testing.T) {
	_, err := buildInbound(&panel.NodeInfo{
		Id:   1,
		Type: "shadowsocks",
		Protocol: &panel.Protocol{
			Type:   "shadowsocks",
			Port:   65535,
			Cipher: "aes-128-gcm",
			Obfs:   "none",
		},
	}, "test-shadowsocks-none-obfs")
	if err != nil {
		t.Fatalf("buildInbound returned error: %v", err)
	}
}

func TestNormalizeOptionalSetting(t *testing.T) {
	for _, value := range []string{"", "none", "NONE", " null ", "false"} {
		if got := normalizeOptionalSetting(value); got != "" {
			t.Fatalf("normalizeOptionalSetting(%q) = %q, want empty", value, got)
		}
	}
}

func TestBuildStreamSettingEnablesProxyProtocol(t *testing.T) {
	stream, err := buildStreamSetting(&panel.Protocol{
		Transport:     "tcp",
		ProxyProtocol: true,
	})
	if err != nil {
		t.Fatalf("buildStreamSetting returned error: %v", err)
	}
	if !acceptProxyProtocolEnabled(stream) {
		t.Fatal("proxy protocol should enable sockopt.acceptProxyProtocol")
	}
}

func TestBuildStreamSettingLeavesProxyProtocolDisabled(t *testing.T) {
	stream, err := buildStreamSetting(&panel.Protocol{
		Transport: "tcp",
	})
	if err != nil {
		t.Fatalf("buildStreamSetting returned error: %v", err)
	}
	if acceptProxyProtocolEnabled(stream) {
		t.Fatal("proxy protocol should stay disabled by default")
	}
}

func TestBuildAnyTLSEnablesProxyProtocol(t *testing.T) {
	inbound := &coreConf.InboundDetourConfig{}
	err := buildAnyTLS(&panel.NodeInfo{
		Type: "anytls",
		Protocol: &panel.Protocol{
			Type:          "anytls",
			ProxyProtocol: true,
		},
	}, inbound)
	if err != nil {
		t.Fatalf("buildAnyTLS returned error: %v", err)
	}
	if !acceptProxyProtocolEnabled(inbound.StreamSetting) {
		t.Fatal("anytls proxy protocol should enable sockopt.acceptProxyProtocol")
	}
}

func TestBuildHysteria2IgnoresProxyProtocol(t *testing.T) {
	inbound := &coreConf.InboundDetourConfig{}
	err := buildHysteria2(&panel.NodeInfo{
		Type: "hysteria2",
		Protocol: &panel.Protocol{
			Type:          "hysteria2",
			ProxyProtocol: true,
		},
	}, inbound)
	if err != nil {
		t.Fatalf("buildHysteria2 returned error: %v", err)
	}
	if acceptProxyProtocolEnabled(inbound.StreamSetting) {
		t.Fatal("hysteria2 should not enable proxy protocol in P0")
	}
}

func TestBuildTuicIgnoresProxyProtocol(t *testing.T) {
	inbound := &coreConf.InboundDetourConfig{}
	err := buildTuic(&panel.NodeInfo{
		Type: "tuic",
		Protocol: &panel.Protocol{
			Type:          "tuic",
			ProxyProtocol: true,
		},
	}, inbound)
	if err != nil {
		t.Fatalf("buildTuic returned error: %v", err)
	}
	if acceptProxyProtocolEnabled(inbound.StreamSetting) {
		t.Fatal("tuic should not enable proxy protocol in P0")
	}
}

func acceptProxyProtocolEnabled(stream *coreConf.StreamConfig) bool {
	return stream != nil &&
		stream.SocketSettings != nil &&
		stream.SocketSettings.AcceptProxyProtocol
}

func TestApplyQUICALPNSetsH3ForHysteria(t *testing.T) {
	for _, nodeType := range []string{"hysteria", "hysteria2"} {
		tlsSettings := &coreConf.TLSConfig{}
		applyQUICALPN(nodeType, tlsSettings)
		built, err := tlsSettings.Build()
		if err != nil {
			t.Fatalf("%s: build tls config: %v", nodeType, err)
		}
		got := built.(*xtls.Config).NextProtocol
		if len(got) != 1 || got[0] != "h3" {
			t.Fatalf("%s: ALPN = %v, want [h3]", nodeType, got)
		}
	}
}

func TestApplyQUICALPNLeavesOtherProtocolsOnDefault(t *testing.T) {
	for _, nodeType := range []string{"vless", "vmess", "trojan", "anytls"} {
		tlsSettings := &coreConf.TLSConfig{}
		applyQUICALPN(nodeType, tlsSettings)
		if tlsSettings.ALPN != nil {
			t.Fatalf("%s: ALPN should stay unset, got %v", nodeType, *tlsSettings.ALPN)
		}
	}
}

func TestApplyQUICALPNKeepsExplicitALPN(t *testing.T) {
	alpn := coreConf.StringList{"custom"}
	tlsSettings := &coreConf.TLSConfig{ALPN: &alpn}
	applyQUICALPN("hysteria", tlsSettings)
	if got := *tlsSettings.ALPN; len(got) != 1 || got[0] != "custom" {
		t.Fatalf("explicit ALPN overwritten: %v", got)
	}
}

// vlessDecryption 三处错误带有错误码
func TestVlessDecryptionCodes(t *testing.T) {
	cases := []struct {
		name string
		p    *panel.Protocol
		want string
	}{
		{"模式为空", &panel.Protocol{Encryption: "mlkem768x25519plus", EncryptionMode: ""}, logreport.CodeVlessEncModeEmpty},
		{"私钥为空", &panel.Protocol{Encryption: "mlkem768x25519plus", EncryptionMode: "native"}, logreport.CodeVlessEncKeyEmpty},
		{"不支持的加密", &panel.Protocol{Encryption: "foo"}, logreport.CodeVlessEncUnsupported},
	}
	for _, c := range cases {
		_, err := vlessDecryption(c.p)
		if err == nil {
			t.Fatalf("%s: 应报错", c.name)
		}
		code, stage, _, _, ok := logreport.Classify(err)
		if !ok || code != c.want || stage != logreport.StageInbound {
			t.Fatalf("%s: code=%q stage=%q ok=%v, 期望 %q/inbound", c.name, code, stage, ok, c.want)
		}
	}
}
