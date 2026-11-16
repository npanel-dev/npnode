package core

import (
	"reflect"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

func TestBuildStreamSettingAppliesTrustedXForwardedFor(t *testing.T) {
	stream, err := buildStreamSetting(&panel.Protocol{Transport: "websocket", Path: "/ws", TrustedXForwardedFor: []string{"X-From-CDN"}})
	if err != nil {
		t.Fatal(err)
	}
	if stream.SocketSettings == nil || !reflect.DeepEqual(stream.SocketSettings.TrustedXForwardedFor, []string{"X-From-CDN"}) {
		t.Fatalf("sockopt not set: %+v", stream.SocketSettings)
	}
	if _, err := stream.Build(); err != nil {
		t.Fatalf("xray rejected stream config: %v", err)
	}

	plain, err := buildStreamSetting(&panel.Protocol{Transport: "websocket", Path: "/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if plain.SocketSettings != nil {
		t.Fatalf("unexpected sockopt without trusted headers: %+v", plain.SocketSettings)
	}
}
