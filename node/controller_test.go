package node

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

func TestBuildNodeTagIncludesProtocolPort(t *testing.T) {
	controller := NewController(nil, &panel.ClientV1{APIHost: "https://panel.example"}, nil)
	tag := controller.buildNodeTag(&panel.NodeInfo{
		Id:   7,
		Type: "vless",
		Protocol: &panel.Protocol{
			Port: 8443,
		},
	})

	if tag != "[https://panel.example]-vless:7:8443" {
		t.Fatalf("tag = %q", tag)
	}
}

func TestBuildNodeTagFallsBackWithoutPort(t *testing.T) {
	controller := NewController(nil, &panel.ClientV1{APIHost: "https://panel.example"}, nil)
	tag := controller.buildNodeTag(&panel.NodeInfo{
		Id:   7,
		Type: "vless",
	})

	if tag != "[https://panel.example]-vless:7" {
		t.Fatalf("tag = %q", tag)
	}
}

func TestHasSameProtocolDifferentPorts(t *testing.T) {
	protocols := []panel.Protocol{
		{Type: "vless", Port: 8443, Enable: true},
		{Type: "vless", Port: 9443, Enable: true},
		{Type: "trojan", Port: 443, Enable: true},
	}

	if !hasSameProtocolDifferentPorts(&protocols) {
		t.Fatal("expected same protocol with different ports to be detected")
	}
}

func TestHasSameProtocolDifferentPortsIgnoresDisabledProtocols(t *testing.T) {
	protocols := []panel.Protocol{
		{Type: "vless", Port: 8443, Enable: true},
		{Type: "vless", Port: 9443, Enable: false},
	}

	if hasSameProtocolDifferentPorts(&protocols) {
		t.Fatal("disabled protocols should not trigger multi-port detection")
	}
}
