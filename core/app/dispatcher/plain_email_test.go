package dispatcher

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

func TestNormalizePlainProxyEmail(t *testing.T) {
	in := &session.Inbound{Tag: "[h]-socks:6:1080", User: &protocol.MemoryUser{Email: "uuid-1"}}
	normalizePlainProxyEmail(in)
	if in.User.Email != "[h]-socks:6:1080|uuid-1" {
		t.Fatalf("email = %s", in.User.Email)
	}
	normalizePlainProxyEmail(in) // 已经是标准格式：不再改
	if in.User.Email != "[h]-socks:6:1080|uuid-1" {
		t.Fatalf("email changed twice: %s", in.User.Email)
	}
	normalizePlainProxyEmail(&session.Inbound{Tag: "t"}) // 没有用户：不 panic
}
