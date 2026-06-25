package core

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
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
