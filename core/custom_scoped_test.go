package core

import (
	"net"
	"strconv"
	"strings"
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

func TestSplitRuleItems(t *testing.T) {
	got := splitRuleItems([]string{
		"example.com", "suffix:a.com", "keyword:ads", "regex:^x\\.", "geosite:netflix",
		"1.2.3.4", "10.0.0.0/8", "2001:db8::/32", "geoip:private", "ip:8.8.8.8",
		"protocol:BitTorrent", " ",
	})
	wantDomains := []string{"full:example.com", "domain:a.com", "ads", "regexp:^x\\.", "geosite:netflix"}
	wantIPs := []string{"1.2.3.4", "10.0.0.0/8", "2001:db8::/32", "geoip:private", "8.8.8.8"}
	if len(got.domains) != len(wantDomains) || len(got.ips) != len(wantIPs) || len(got.protocols) != 1 || got.protocols[0] != "bittorrent" {
		t.Fatalf("got %+v", got)
	}
	for i := range wantDomains {
		if got.domains[i] != wantDomains[i] {
			t.Fatalf("domain %d = %s", i, got.domains[i])
		}
	}
	for i := range wantIPs {
		if got.ips[i] != wantIPs[i] {
			t.Fatalf("ip %d = %s", i, got.ips[i])
		}
	}
}

func TestBlockIPAndProtocolRules(t *testing.T) {
	cfg := &panel.ServerConfigResponse{Data: &panel.Data{
		Block: &[]string{"suffix:ads.com", "10.0.0.0/8", "protocol:bittorrent"},
		DNS:   &[]panel.DNSItem{{Address: "8.8.8.8:5353", Domains: []string{"suffix:google.com"}}, {Address: "https://1.1.1.1/dns-query"}},
	}}
	dnsCfg, _, router, err := GetCustomConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	var hasDomain, hasIP, hasProto bool
	for _, r := range router.Rule {
		if r.GetTag() != "block" {
			continue
		}
		hasDomain = hasDomain || len(r.GetDomain()) > 0
		hasIP = hasIP || len(r.GetIp()) > 0
		hasProto = hasProto || len(r.GetProtocol()) > 0
	}
	if !hasDomain || !hasIP || !hasProto {
		t.Fatalf("block rules: domain=%v ip=%v protocol=%v", hasDomain, hasIP, hasProto)
	}
	var udpPort uint32
	for _, ns := range dnsCfg.NameServer {
		if ns.GetAddress().GetPort() == 5353 {
			udpPort = ns.GetAddress().GetPort()
		}
	}
	if udpPort != 5353 {
		t.Fatalf("dns port not split from host:port")
	}
	if a, p := dnsServerAddress("tcp://8.8.8.8:53"); a != "tcp://8.8.8.8:53" || p != 0 {
		t.Fatalf("url address changed: %s %d", a, p)
	}
}

// dnsOrder 返回 DNS 服务器地址顺序和各自的 finalQuery
func dnsOrder(t *testing.T, items []panel.DNSItem) (string, string) {
	t.Helper()
	dnsCfg, _, _, err := GetCustomConfig(&panel.ServerConfigResponse{Data: &panel.Data{DNS: &items}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var addrs, finals []string
	for _, ns := range dnsCfg.NameServer {
		addr := ns.GetAddress().GetAddress()
		if d := addr.GetDomain(); d != "" {
			addrs = append(addrs, d)
		} else {
			addrs = append(addrs, net.IP(addr.GetIp()).String())
		}
		finals = append(finals, strconv.FormatBool(ns.GetFinalQuery()))
	}
	return strings.Join(addrs, ","), strings.Join(finals, ",")
}

// 不带域名的 DNS 是全局 DNS：取代系统 DNS（不再有 localhost），最后一个全局 DNS 是 finalQuery，
// 空应答不会回落；带域名的排在后面，靠域名优先匹配
func TestGlobalDNSReplacesLocalhost(t *testing.T) {
	addrs, finals := dnsOrder(t, []panel.DNSItem{
		{Address: "8.8.8.8", Domains: []string{"suffix:google.com"}},
		{Address: "9.9.9.9"},
		{Address: "1.1.1.1"},
	})
	if addrs != "9.9.9.9,1.1.1.1,8.8.8.8" {
		t.Fatalf("dns order = %s", addrs)
	}
	if finals != "false,true,false" {
		t.Fatalf("finalQuery = %s", finals)
	}
}

// 只有带域名的 DNS：系统 DNS 仍是主 DNS，行为与之前一致
func TestDomainDNSKeepsLocalhost(t *testing.T) {
	addrs, finals := dnsOrder(t, []panel.DNSItem{{Address: "8.8.8.8", Domains: []string{"suffix:google.com"}}})
	if addrs != "localhost,8.8.8.8" || finals != "false,false" {
		t.Fatalf("dns order = %s, finalQuery = %s", addrs, finals)
	}
}

func TestNoCustomDNSUsesLocalhost(t *testing.T) {
	dnsCfg, _, _, err := GetCustomConfig(&panel.ServerConfigResponse{Data: &panel.Data{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dnsCfg.NameServer) != 1 || dnsCfg.NameServer[0].GetAddress().GetAddress().GetDomain() != "localhost" {
		t.Fatalf("default dns = %v", dnsCfg.NameServer)
	}
}
