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

func TestOnlineUsersForReportReturnsNonNilEmptySnapshot(t *testing.T) {
	report := onlineUsersForReport(nil, nil)
	if report == nil {
		t.Fatal("empty online snapshot must be a non-nil slice so HTTP reports users: []")
	}
	if len(report) != 0 {
		t.Fatalf("empty online snapshot length = %d, want 0", len(report))
	}
}

func TestOnlineUsersForReportPreservesFiltering(t *testing.T) {
	report := onlineUsersForReport(
		[]panel.OnlineUser{
			{UID: 1, IP: "203.0.113.1"},
			{UID: 2, IP: "203.0.113.2"},
		},
		[]panel.UserTraffic{
			{UID: 1, Upload: 0, Download: 0},
			{UID: 2, Upload: 1, Download: 0},
		},
	)
	if len(report) != 1 || report[0].UID != 2 {
		t.Fatalf("online report = %+v, want only UID 2", report)
	}
}
