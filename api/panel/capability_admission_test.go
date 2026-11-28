package panel

import (
	"reflect"
	"testing"
)

// admission_v1 与 seat_v1、online_v1 相互独立（面板按能力分别渲染条目，准入接口只看 admission_v1）；
// 顺序固定 seat_v1,online_v1,admission_v1，未开启的不出现
func TestCapabilityCombinationsWithAdmission(t *testing.T) {
	cases := []struct {
		seat, online, adm bool
		param             string
	}{
		{false, false, false, ""},
		{false, false, true, "admission_v1"},
		{true, false, true, "seat_v1,admission_v1"},
		{false, true, true, "online_v1,admission_v1"},
		{true, true, true, "seat_v1,online_v1,admission_v1"},
		{true, true, false, "seat_v1,online_v1"},
	}
	for _, tc := range cases {
		SetSeatV1(tc.seat)
		SetOnlineV1(tc.online)
		SetAdmissionV1(tc.adm)
		t.Cleanup(func() {
			SetSeatV1(false)
			SetOnlineV1(false)
			SetAdmissionV1(false)
		})
		if got := CapabilityParam(); got != tc.param {
			t.Errorf("%+v: CapabilityParam = %q", tc, got)
		}
		var want []string
		if tc.param != "" {
			want = splitComma(tc.param)
		}
		if got := CapabilityList(); !reflect.DeepEqual(got, want) {
			t.Errorf("%+v: CapabilityList = %v, want %v", tc, got, want)
		}
		if got := NodeAgent() != ""; got != (tc.param != "") {
			t.Errorf("%+v: NodeAgent present = %v", tc, got)
		}
		if AdmissionV1Enabled() != tc.adm {
			t.Errorf("%+v: AdmissionV1Enabled = %v", tc, AdmissionV1Enabled())
		}
	}
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}
