package core

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

// 32 字节 X25519 私钥（base64url），仅用于测试
const testVLESSPrivateKey = "aGVsbG9oZWxsb2hlbGxvaGVsbG9oZWxsb2hlbGxvMTI"

func mlkemProtocol(rtt, ticket, padding, key string) *panel.Protocol {
	return &panel.Protocol{
		Type:                    "vless",
		Port:                    443,
		Encryption:              "mlkem768x25519plus",
		EncryptionMode:          "native",
		EncryptionRTT:           rtt,
		EncryptionTicket:        ticket,
		EncryptionServerPadding: padding,
		EncryptionPrivateKey:    key,
	}
}

func TestVLESSDecryption(t *testing.T) {
	k := testVLESSPrivateKey
	cases := []struct {
		name string
		p    *panel.Protocol
		want string
	}{
		{"未开启加密", &panel.Protocol{Encryption: ""}, "none"},
		{"加密为 none", &panel.Protocol{Encryption: "none"}, "none"},
		{"1rtt 票据为空", mlkemProtocol("1rtt", "", "", k), "mlkem768x25519plus.native.0s." + k},
		{"1rtt 忽略残留票据", mlkemProtocol("1rtt", "600", "", k), "mlkem768x25519plus.native.0s." + k},
		{"RTT 为空按 1rtt", mlkemProtocol("", "", "", k), "mlkem768x25519plus.native.0s." + k},
		{"0rtt 纯数字", mlkemProtocol("0rtt", "600", "", k), "mlkem768x25519plus.native.600s." + k},
		{"0rtt 带 s 后缀", mlkemProtocol("0rtt", " 600s ", "", k), "mlkem768x25519plus.native.600s." + k},
		{"0rtt 区间", mlkemProtocol("0rtt", "300-600s", "", k), "mlkem768x25519plus.native.300-600s." + k},
		{"0rtt 票据为空用默认", mlkemProtocol("0rtt", "", "", k), "mlkem768x25519plus.native.600s." + k},
		{"带服务端填充", mlkemProtocol("1rtt", "", "100-111-1111", k), "mlkem768x25519plus.native.0s.100-111-1111." + k},
	}
	for _, c := range cases {
		got, err := vlessDecryption(c.p)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestVLESSDecryptionRejectsInvalid(t *testing.T) {
	cases := map[string]*panel.Protocol{
		"私钥为空": mlkemProtocol("1rtt", "", "", ""),
		"模式为空": {Encryption: "mlkem768x25519plus", EncryptionPrivateKey: testVLESSPrivateKey},
		"未知加密": {Encryption: "aes"},
	}
	for name, p := range cases {
		if _, err := vlessDecryption(p); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

// 面板默认值（native + 1rtt + 票据留空）必须能被 Xray 构建成功
func TestBuildInboundVLESSWithPanelDefaultEncryption(t *testing.T) {
	for _, p := range []*panel.Protocol{
		mlkemProtocol("1rtt", "", "", testVLESSPrivateKey),
		mlkemProtocol("0rtt", "600s", "", testVLESSPrivateKey),
	} {
		if _, err := buildInbound(&panel.NodeInfo{Id: 1, Type: "vless", Protocol: p}, "test-vless-encryption"); err != nil {
			t.Fatalf("rtt=%s ticket=%q: buildInbound error: %v", p.EncryptionRTT, p.EncryptionTicket, err)
		}
	}
}
