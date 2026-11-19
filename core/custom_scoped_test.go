package core

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

func TestScopedRulesLimitToInbounds(t *testing.T) {
	cfg := &panel.ServerConfigResponse{Data: &panel.Data{}}
	cfg.Data.Block = &[]string{"suffix:ads.com"}
	cfg.Data.ScopedRules = &[]panel.ScopedRule{
		{Type: "block", Inbounds: []panel.ScopedInbound{{Type: "vless", Port: 443}}, Domains: []string{"suffix:netflix.com"}},
		{Type: "outbound", Inbounds: []panel.ScopedInbound{{Type: "hysteria", Port: 5555}}, Domains: []string{"suffix:disney.com"},
			Outbound: &panel.Outbound{Name: "unlock", Protocol: "socks", Address: "10.0.0.1", Port: 1080}},
		{Type: "block", Inbounds: nil, Domains: []string{"x.com"}}, // 没有入站：忽略
	}
	tag := func(typ string, port int) string { return InboundTag("https://panel", 6, typ, port) }
	_, outbounds, router, err := GetCustomConfig(cfg, tag)
	if err != nil {
		t.Fatal(err)
	}
	if !hasOutboundWithTag(outbounds, "unlock") {
		t.Fatal("scoped outbound not added")
	}
	var scopedBlock, scopedOut bool
	for _, r := range router.Rule {
		tags := r.GetInboundTag()
		if len(tags) == 1 && tags[0] == "[https://panel]-vless:6:443" && r.GetTag() == "block" {
			scopedBlock = true
		}
		if len(tags) == 1 && tags[0] == "[https://panel]-hysteria:6:5555" && r.GetTag() == "unlock" {
			scopedOut = true
		}
	}
	if !scopedBlock || !scopedOut {
		t.Fatalf("scoped rules missing: block=%v outbound=%v rules=%d", scopedBlock, scopedOut, len(router.Rule))
	}
	// 与节点控制器的入站标签格式一致
	if got := InboundTag("https://panel", 6, "vless", 443); got != "[https://panel]-vless:6:443" {
		t.Fatalf("tag = %s", got)
	}
	if _, _, _, err := GetCustomConfig(cfg, nil); err != nil {
		t.Fatalf("nil tag func should ignore scoped rules: %v", err)
	}
}

func TestRawOutboundFromPanel(t *testing.T) {
	// 与面板 noderelay.BuildOutbound 生成的格式一致
	raw := []byte(`{"protocol":"vless","settings":{"vnext":[{"address":"1.2.3.4","port":443,"users":[{"id":"0b6b8a0c-3f1e-4c2a-9d1e-5a7b8c9d0e1f","encryption":"none","flow":"xtls-rprx-vision"}]}]},"streamSettings":{"network":"raw","security":"reality","realitySettings":{"serverName":"www.apple.com","publicKey":"qZLT8bbEtLX0w9TkS9NSz2N0nI0hR1h7l1dM8ZQyVzE","shortId":"ab","fingerprint":"chrome"}}}`)
	ob, err := buildCustomOutbound(&panel.Outbound{Name: "relay-hk", Raw: raw})
	if err != nil || ob == nil || ob.Tag != "relay-hk" {
		t.Fatalf("raw outbound: %v %v", ob, err)
	}
	ws := []byte(`{"protocol":"trojan","settings":{"servers":[{"address":"a.com","port":8443,"password":"pw"}]},"streamSettings":{"network":"ws","wsSettings":{"path":"/ws","host":"cdn.a.com"},"security":"tls","tlsSettings":{"serverName":"cdn.a.com","allowInsecure":false}}}`)
	if _, err := buildCustomOutbound(&panel.Outbound{Name: "t", Raw: ws}); err != nil {
		t.Fatalf("trojan ws: %v", err)
	}
	if _, err := buildCustomOutbound(&panel.Outbound{Name: "bad", Raw: []byte(`{`)}); err == nil {
		t.Fatal("broken raw should fail")
	}
}
