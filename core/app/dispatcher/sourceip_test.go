package dispatcher

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

func TestBindDefaultSource(t *testing.T) {
	for _, test := range []struct {
		name, entry, target, tag, gateway, want string
	}{
		{"public-ipv4-domain", "203.0.113.12", "example.com", "Default", "", "203.0.113.12"},
		{"public-ipv4-literal", "203.0.113.12", "198.51.100.1", "Default", "", "203.0.113.12"},
		{"private-ipv4", "10.0.0.12", "198.51.100.1", "Default", "", "10.0.0.12"},
		{"ipv6-domain", "2001:db8::12", "example.com", "Default", "", "2001:db8::12"},
		{"ipv6-literal", "2001:db8::12", "2001:db8::1", "Default", "", "2001:db8::12"},
		{"loopback-proxy-domain", "127.0.0.1", "example.com", "Default", "", ""},
		{"loopback-proxy-ipv4", "127.0.0.1", "198.51.100.1", "Default", "", ""},
		{"loopback-proxy-ipv6", "::1", "2001:db8::1", "Default", "", ""},
		{"local-loopback-target", "127.0.0.2", "127.0.0.1", "Default", "", "127.0.0.2"},
		{"wildcard-ipv4-udp", "0.0.0.0", "198.51.100.1", "Default", "", ""},
		{"wildcard-ipv6-udp", "::", "2001:db8::1", "Default", "", ""},
		{"link-local-ipv6", "fe80::1", "2001:db8::1", "Default", "", ""},
		{"ipv4-to-ipv6-literal", "203.0.113.12", "2001:db8::1", "Default", "", ""},
		{"ipv6-to-ipv4-literal", "2001:db8::12", "198.51.100.1", "Default", "", ""},
		{"custom-proxy-outbound", "203.0.113.12", "example.com", "upstream", "", ""},
		{"explicit-source", "203.0.113.12", "example.com", "Default", "203.0.113.99", "203.0.113.99"},
		{"missing-inbound", "", "example.com", "Default", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var inbound *session.Inbound
			if test.entry != "" {
				inbound = &session.Inbound{
					Local:  net.TCPDestination(net.ParseAddress(test.entry), 443),
					Source: net.TCPDestination(net.ParseAddress("192.0.2.99"), 12345),
				}
			}
			outbound := &session.Outbound{
				Tag: test.tag, Target: net.TCPDestination(net.ParseAddress(test.target), 443),
			}
			if test.gateway != "" {
				outbound.Gateway = net.ParseAddress(test.gateway)
			}
			bindDefaultSource(inbound, outbound)
			got := ""
			if outbound.Gateway != nil {
				got = outbound.Gateway.IP().String()
			}
			if got != test.want {
				t.Fatalf("outbound source = %q, want %q", got, test.want)
			}
		})
	}
}
