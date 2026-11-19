package core

import (
	"strings"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

func TestBuildPlainProxyInbounds(t *testing.T) {
	for _, typ := range []string{"socks", "http"} {
		info := &panel.NodeInfo{Id: 6, Type: typ, Protocol: &panel.Protocol{Type: typ, Port: 1080}}
		cfg, err := buildInboundWithUsers(info, "tag-"+typ, []string{"uuid-a", "uuid-b"})
		if err != nil || cfg == nil || cfg.Tag != "tag-"+typ {
			t.Fatalf("%s: %v %v", typ, cfg, err)
		}
		// 没有用户时也要能建出来，并且不能变成开放代理（放一个随机占位账号）
		if _, err := buildInboundWithUsers(info, "tag-empty", nil); err != nil {
			t.Fatalf("%s empty: %v", typ, err)
		}
		in := &coreConf.InboundDetourConfig{}
		if err := buildPlainProxy(info, in, nil); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(*in.Settings), "npnode-empty-") {
			t.Fatalf("%s: empty inbound has no placeholder account: %s", typ, *in.Settings)
		}
	}
}
