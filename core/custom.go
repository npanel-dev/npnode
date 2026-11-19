package core

import (
	"encoding/json"
	"fmt"
	"net"
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

// parseDomainRules 面板的域名规则写法 → Xray 的 domain 匹配写法
func parseDomainRules(items []string) []string {
	var domains []string
	for _, item := range items {
		data := strings.SplitN(item, ":", 2)
		if len(data) == 2 {
			switch data[0] {
			case "keyword":
				domains = append(domains, data[1])
			case "suffix":
				domains = append(domains, "domain:"+data[1])
			case "regex":
				domains = append(domains, "regexp:"+data[1])
			default:
				domains = append(domains, data[1])
			}
		} else {
			domains = append(domains, "full:"+item)
		}
	}
	return domains
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
	coreDnsConfig := &coreConf.DNSConfig{
		Servers: []*coreConf.NameServerConfig{
			{
				Address: &coreConf.Address{
					Address: xnet.ParseAddress("localhost"),
				},
			},
		},
		QueryStrategy: queryStrategy,
	}

	//custom dns
	if dnsConfig != nil {
		for _, item := range *dnsConfig {
			var domains []string
			for _, domainitem := range item.Domains {
				data := strings.Split(domainitem, ":")
				if len(data) == 2 {
					switch data[0] {
					case "keyword":
						domains = append(domains, data[1])
					case "suffix":
						domains = append(domains, "domain:"+data[1])
					case "regex":
						domains = append(domains, "regexp:"+data[1])
					default:
						domains = append(domains, data[1])
					}
				} else {
					domains = append(domains, "full:"+domainitem)
				}
			}
			/*switch item.Proto {
			case "udp":
				item.Address = item.Address
			case "tcp":
				item.Address = "tcp://" + item.Address
			case "tls":
				item.Address = "tls://" + item.Address
			case "https":
				item.Address = "https://" + item.Address
			case "quic":
				item.Address = "quic://" + item.Address
			}*/
			server := &coreConf.NameServerConfig{
				Address: &coreConf.Address{
					Address: xnet.ParseAddress(item.Address),
				},
				QueryStrategy: ip_strategy,
				Domains:       domains,
			}
			coreDnsConfig.Servers = append(coreDnsConfig.Servers, server)
		}
	}

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

	//custom block
	if blockList != nil {
		var domains []string
		for _, bitem := range *blockList {
			data := strings.Split(bitem, ":")
			if len(data) == 2 {
				switch data[0] {
				case "keyword":
					domains = append(domains, data[1])
				case "suffix":
					domains = append(domains, "domain:"+data[1])
				case "regex":
					domains = append(domains, "regexp:"+data[1])
				default:
					domains = append(domains, data[1])
				}
			} else {
				domains = append(domains, "full:"+bitem)
			}
		}
		rule := map[string]interface{}{
			"domain":      domains,
			"outboundTag": "block",
		}
		rawRule, err := json.Marshal(rule)
		if err == nil {
			coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, rawRule)
		}
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
			domains := parseDomainRules(sr.Domains)
			if len(tags) == 0 || len(domains) == 0 {
				continue
			}
			switch sr.Type {
			case "block":
				rawRule, err := json.Marshal(map[string]interface{}{
					"inboundTag":  tags,
					"domain":      domains,
					"outboundTag": "block",
				})
				if err == nil {
					coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, rawRule)
				}
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
		rawRule, err := json.Marshal(map[string]interface{}{
			"inboundTag":  tags,
			"domain":      parseDomainRules(sr.Domains),
			"outboundTag": custom.Tag,
		})
		if err == nil {
			coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, rawRule)
		}
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
			rule := map[string]interface{}{
				"domain":      parseDomainRules(outbounditem.Rules),
				"outboundTag": custom_outbound.Tag,
			}
			rawRule, err := json.Marshal(rule)
			if err == nil {
				coreRouterConfig.RuleList = append(coreRouterConfig.RuleList, rawRule)
			}
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
