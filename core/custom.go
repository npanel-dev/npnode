package core

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/app/router"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

// hasPublicIPv6 checks if the machine has a public IPv6 address
func hasPublicIPv6() bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNet.IP
		// Check if it's IPv6, not loopback, not link-local, not private/ULA
		if ip.To4() == nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsPrivate() {
			return true
		}
	}
	return false
}

func hasOutboundWithTag(list []*core.OutboundHandlerConfig, tag string) bool {
	for _, o := range list {
		if o != nil && o.Tag == tag {
			return true
		}
	}
	return false
}

// InboundTag 入站标签。节点控制器和按入站限定的路由规则都用它，两边必须一致。
func InboundTag(apiHost string, serverID int, protocolType string, port int) string {
	if port > 0 {
		return fmt.Sprintf("[%s]-%s:%d:%d", apiHost, protocolType, serverID, port)
	}
	return fmt.Sprintf("[%s]-%s:%d", apiHost, protocolType, serverID)
}

// parseDomainRules 面板的域名规则写法 → Xray 的 domain 匹配写法（只保留域名类，IP / 协议条目忽略）
func parseDomainRules(items []string) []string {
	return splitRuleItems(items).domains
}

type ruleItems struct {
	domains   []string
	ips       []string
	protocols []string
}

// splitRuleItems 把面板的规则条目分成域名、IP、协议三类：
//   - 域名：example.com（精确）、suffix:、keyword:、regex:、full:、domain:、geosite:
//   - IP：1.2.3.4、1.2.3.0/24、IPv6、ip:...、geoip:cn / geoip:private
//   - 协议：protocol:bittorrent（以及 http / tls / quic）
func splitRuleItems(items []string) ruleItems {
	var out ruleItems
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		prefix, value, hasPrefix := strings.Cut(item, ":")
		if hasPrefix {
			switch strings.ToLower(prefix) {
			case "keyword":
				out.domains = append(out.domains, value)
				continue
			case "suffix", "domain":
				out.domains = append(out.domains, "domain:"+value)
				continue
			case "regex", "regexp":
				out.domains = append(out.domains, "regexp:"+value)
				continue
			case "full":
				out.domains = append(out.domains, "full:"+value)
				continue
			case "geosite":
				out.domains = append(out.domains, "geosite:"+value)
				continue
			case "geoip":
				out.ips = append(out.ips, "geoip:"+value)
				continue
			case "ip", "cidr":
				out.ips = append(out.ips, value)
				continue
			case "protocol":
				out.protocols = append(out.protocols, strings.ToLower(value))
				continue
			}
		}
		// 不带前缀：IP / CIDR 按 IP 处理（IPv6 自带冒号，所以先判断），否则按精确域名
		if _, _, err := net.ParseCIDR(item); err == nil {
			out.ips = append(out.ips, item)
			continue
		}
		if net.ParseIP(item) != nil {
			out.ips = append(out.ips, item)
			continue
		}
		out.domains = append(out.domains, "full:"+item)
	}
	return out
}

// routeRules 一组条目 → 指向 outboundTag 的路由规则（域名 / IP / 协议各一条：同一条规则里的条件是“且”）
func routeRules(items []string, outboundTag string, inboundTags []string) []json.RawMessage {
	parts := splitRuleItems(items)
	var rules []json.RawMessage
	add := func(field string, values []string) {
		if len(values) == 0 {
			return
		}
		rule := map[string]interface{}{field: values, "outboundTag": outboundTag}
		if len(inboundTags) > 0 {
			rule["inboundTag"] = inboundTags
		}
		if raw, err := json.Marshal(rule); err == nil {
			rules = append(rules, raw)
		}
	}
	add("domain", parts.domains)
	add("ip", parts.ips)
	add("protocol", parts.protocols)
	return rules
}

// dnsServerAddress 面板的 DNS 地址 → Xray 地址和端口。
// 带协议前缀（https:// tcp:// quic+local:// 等）原样交给 Xray；普通 host:port 拆出端口（不拆会被当成域名）。
func dnsServerAddress(address string) (string, uint16) {
	address = strings.TrimSpace(address)
	if strings.Contains(address, "://") {
		return address, 0
	}
	if host, port, err := net.SplitHostPort(address); err == nil {
		if p, err := strconv.Atoi(port); err == nil && p > 0 && p < 65536 {
			return host, uint16(p)
		}
	}
	return address, 0
}

// GetCustomConfig 构建 DNS、出站和路由。inboundTag 把面板下发的入站（协议+端口）换成本机的入站标签，
// 用于只对部分入站生效的规则；为 nil 时忽略这类规则。
func GetCustomConfig(serverconfig *panel.ServerConfigResponse, inboundTag func(protocolType string, port int) string) (*dns.Config, []*core.OutboundHandlerConfig, *router.Config, error) {
	var ip_strategy string
	if serverconfig.Data.IPStrategy != "" {
		switch serverconfig.Data.IPStrategy {
		case "prefer_ipv4":
			ip_strategy = "UseIPv4v6"
		case "prefer_ipv6":
			ip_strategy = "UseIPv6v4"
		default:
			ip_strategy = "UseIPv4v6"
		}
	} else {
		if hasPublicIPv6() {
			ip_strategy = "UseIPv4v6"
		} else {
			ip_strategy = "UseIPv4"
		}
	}
	dnsConfig := serverconfig.Data.DNS
	blockList := serverconfig.Data.Block
	outboundList := serverconfig.Data.Outbound

	//default dns
	queryStrategy := "UseIPv4v6"
	if !hasPublicIPv6() {
		queryStrategy = "UseIPv4"
	}
	coreDnsConfig := &coreConf.DNSConfig{QueryStrategy: queryStrategy}

	// custom dns：带域名的只解析这些域名；不带域名的是全局 DNS，取代系统 DNS。
	var globalServers, domainServers []*coreConf.NameServerConfig
	if dnsConfig != nil {
		for _, item := range *dnsConfig {
			address, port := dnsServerAddress(item.Address)
			server := &coreConf.NameServerConfig{
				Address: &coreConf.Address{
					Address: xnet.ParseAddress(address),
				},
				Port:          port,
				QueryStrategy: ip_strategy,
				Domains:       parseDomainRules(item.Domains),
			}
			if len(server.Domains) == 0 {
				globalServers = append(globalServers, server)
			} else {
				domainServers = append(domainServers, server)
			}
		}
	}
	if len(globalServers) > 0 {
		// 全局 DNS 等同于把它写进 /etc/resolv.conf：它返回什么就用什么。
		// 最后一个全局 DNS 设为 finalQuery，查询到此为止，空应答 / NXDOMAIN 也不再回落到后面的 DNS。
		// 解锁 DNS 常对部分域名故意返回空，回落会拿到真实 IP，让同一业务的请求从两个出口出去（如 Netflix 登录报错）。
		globalServers[len(globalServers)-1].FinalQuery = true
		coreDnsConfig.Servers = append(coreDnsConfig.Servers, globalServers...)
	} else {
		// 没有全局 DNS：用系统 DNS（/etc/resolv.conf）
		coreDnsConfig.Servers = append(coreDnsConfig.Servers, &coreConf.NameServerConfig{
			Address: &coreConf.Address{
				Address: xnet.ParseAddress("localhost"),
			},
		})
	}
	coreDnsConfig.Servers = append(coreDnsConfig.Servers, domainServers...)

	//default outbound
	defaultoutbound, _ := buildDefaultOutbound()
	coreOutboundConfig := append([]*core.OutboundHandlerConfig{}, defaultoutbound)
	block, _ := buildBlockOutbound()
	coreOutboundConfig = append(coreOutboundConfig, block)
	dns, _ := buildDnsOutbound()
	coreOutboundConfig = append(coreOutboundConfig, dns)

	//default route
	domainStrategy := "AsIs"
	dnsRule, _ := json.Marshal(map[string]interface{}{
		"port":        "53",
		"network":     "udp",
		"outboundTag": "dns_out",
	})
	coreRouterConfig := &coreConf.RouterConfig{
		RuleList:       []json.RawMessage{dnsRule},
		DomainStrategy: &domainStrategy,
	}

	//custom block：域名 / IP / 协议都可以
	if blockList != nil {
		coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, routeRules(*blockList, "block", nil)...)
	}

	//scoped rules: 只对部分入站生效（屏蔽在前，出站在后，都排在全局出站规则之前）
	var scopedOutbounds []*panel.ScopedRule
	if serverconfig.Data.ScopedRules != nil && inboundTag != nil {
		for i := range *serverconfig.Data.ScopedRules {
			sr := &(*serverconfig.Data.ScopedRules)[i]
			var tags []string
			for _, in := range sr.Inbounds {
				tags = append(tags, inboundTag(in.Type, in.Port))
			}
			if len(tags) == 0 || len(sr.Domains) == 0 {
				continue
			}
			switch sr.Type {
			case "block":
				coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, routeRules(sr.Domains, "block", tags)...)
			case "outbound":
				if sr.Outbound != nil {
					scopedOutbounds = append(scopedOutbounds, sr)
				}
			}
		}
	}
	for _, sr := range scopedOutbounds {
		custom, err := buildCustomOutbound(sr.Outbound)
		if err != nil || custom == nil {
			continue
		}
		var tags []string
		for _, in := range sr.Inbounds {
			tags = append(tags, inboundTag(in.Type, in.Port))
		}
		coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, routeRules(sr.Domains, custom.Tag, tags)...)
		if !hasOutboundWithTag(coreOutboundConfig, custom.Tag) {
			coreOutboundConfig = append(coreOutboundConfig, custom)
		}
	}

	//custom outbound
	if outboundList != nil {
		for i := range *outboundList {
			outbounditem := &(*outboundList)[i]
			custom_outbound, err := buildCustomOutbound(outbounditem)
			if err != nil || custom_outbound == nil {
				continue
			}
			coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, routeRules(outbounditem.Rules, custom_outbound.Tag, nil)...)
			if hasOutboundWithTag(coreOutboundConfig, custom_outbound.Tag) {
				continue
			}
			coreOutboundConfig = append(coreOutboundConfig, custom_outbound)
		}
	}
	//build config
	DnsConfig, err := coreDnsConfig.Build()
	if err != nil {
		return nil, nil, nil, err
	}
	RouterConfig, err := coreRouterConfig.Build()
	if err != nil {
		return nil, nil, nil, err
	}
	return DnsConfig, coreOutboundConfig, RouterConfig, nil
}

// buildCustomOutbound 面板下发的自定义出站 → Xray 出站；不支持的协议返回 nil
func buildCustomOutbound(o *panel.Outbound) (*core.OutboundHandlerConfig, error) {
	if len(o.Raw) > 0 {
		// 面板按目标节点的协议配置生成的完整出站（TLS / Reality / 传输方式都在里面）
		var detour coreConf.OutboundDetourConfig
		if err := json.Unmarshal(o.Raw, &detour); err != nil {
			return nil, fmt.Errorf("parse raw outbound %s: %w", o.Name, err)
		}
		detour.Tag = o.Name
		return detour.Build()
	}
	jsonsettings := map[string]interface{}{
		"address": o.Address,
		"port":    o.Port,
	}
	streamSettings := &coreConf.StreamConfig{}
	switch o.Protocol {
	case "http":
		//jsonsettings["user"] = o.User
		jsonsettings["pass"] = o.Password
	case "socks":
		//jsonsettings["user"] = o.User
		jsonsettings["pass"] = o.Password
	case "shadowsocks":
		//jsonsettings["method"] = o.Method
		jsonsettings["password"] = o.Password
		jsonsettings["uot"] = true
		jsonsettings["UoTVersion"] = 2
	case "trojan":
		jsonsettings["password"] = o.Password
		proto := coreConf.TransportProtocol("tcp")
		streamSettings.Network = &proto
		streamSettings.Security = "tls"
		streamSettings.TLSSettings = &coreConf.TLSConfig{
			//ServerName: o.SNI,
			//Insecure: o.Insecure,
		}
	case "vmess":
		jsonsettings["uuid"] = o.Password
		proto := coreConf.TransportProtocol("tcp")
		streamSettings.Network = &proto
		/*if o.Security != "" && o.Security == "tls" {
			streamSettings.Security = "tls"
			streamSettings.TLSSettings = &coreConf.TLSConfig{
				ServerName: o.SNI,
				Insecure: o.Insecure,
		}*/
	case "vless":
		jsonsettings["uuid"] = o.Password
		proto := coreConf.TransportProtocol("tcp")
		streamSettings.Network = &proto
		/*if o.Security != "" && o.Security == "tls" {
			streamSettings.Security = "tls"
			streamSettings.TLSSettings = &coreConf.TLSConfig{
				ServerName: o.SNI,
				Insecure: o.Insecure,
		}*/
	//case "wireguard":
	default:
		return nil, nil
	}

	settings, _ := json.Marshal(jsonsettings)
	rawSettings := json.RawMessage(settings)
	outbound := &coreConf.OutboundDetourConfig{
		Tag:           o.Name,
		Protocol:      o.Protocol,
		Settings:      &rawSettings,
		StreamSetting: streamSettings,
	}
	return outbound.Build()
}
