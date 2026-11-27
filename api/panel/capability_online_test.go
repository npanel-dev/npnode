package panel

import (
	"reflect"
	"testing"
)

// withCaps 在测试期间设置两项能力开关，结束后恢复关闭
func withCaps(t *testing.T, seat, online bool) {
	t.Helper()
	SetSeatV1(seat)
	SetOnlineV1(online)
	t.Cleanup(func() {
		SetSeatV1(false)
		SetOnlineV1(false)
	})
}

// seat_v1 与 online_v1 相互独立；能力列表按开关组合生成，顺序固定 seat_v1,online_v1
func TestCapabilityCombinations(t *testing.T) {
	cases := []struct {
		seat, online bool
		param        string
		list         []string
		agent        bool
	}{
		{false, false, "", nil, false},
		{true, false, "seat_v1", []string{"seat_v1"}, true},
		{false, true, "online_v1", []string{"online_v1"}, true},
		{true, true, "seat_v1,online_v1", []string{"seat_v1", "online_v1"}, true},
	}
	for _, tc := range cases {
		withCaps(t, tc.seat, tc.online)
		if got := CapabilityParam(); got != tc.param {
			t.Errorf("seat=%v online=%v: CapabilityParam = %q, want %q", tc.seat, tc.online, got, tc.param)
		}
		if got := CapabilityList(); !reflect.DeepEqual(got, tc.list) {
			t.Errorf("seat=%v online=%v: CapabilityList = %v, want %v", tc.seat, tc.online, got, tc.list)
		}
		if got := NodeAgent() != ""; got != tc.agent {
			t.Errorf("seat=%v online=%v: NodeAgent present = %v, want %v", tc.seat, tc.online, got, tc.agent)
		}
		if OnlineV1Enabled() != tc.online || SeatV1Enabled() != tc.seat {
			t.Errorf("seat=%v online=%v: switches not independent", tc.seat, tc.online)
		}
	}
}

// 两项都打开时 HTTP 请求带 capabilities=seat_v1,online_v1（逗号经查询编码为 %2C，面板解码后按逗号拆分）
func TestHTTPRequestsDeclareOnlineV1(t *testing.T) {
	withCaps(t, true, true)
	rec, srv := newRecordingPanel(t)
	exerciseHTTPClients(t, srv.URL)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := "capabilities=seat_v1%2Conline_v1&port=443&protocol=vless&secret_key=secret&server_id=7"
	if got := rec.queries["/v1/server/user"]; got != want {
		t.Fatalf("/v1/server/user query = %s, want %s", got, want)
	}
	if got := rec.queries["/v2/server/7"]; got != "capabilities=seat_v1%2Conline_v1&secret_key=secret" {
		t.Fatalf("/v2/server/7 query = %s", got)
	}
}
