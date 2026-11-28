package logreport

import (
	"encoding/json"
	"os"
	"testing"
)

func TestNormalizeCases(t *testing.T) {
	b, err := os.ReadFile("testdata/normalize_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ In, Out string }
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if got := Normalize(c.In); got != c.Out {
			t.Errorf("Normalize(%q)=%q want %q", c.In, got, c.Out)
		}
	}
}

func TestMergeKeyStableAcrossVariables(t *testing.T) {
	if MergeKey(CodeUnknown, "", 0, "dial tcp 1.2.3.4:443: timeout") != MergeKey(CodeUnknown, "", 0, "dial tcp 5.6.7.8:80: timeout") {
		t.Fatal("unknown keys should match")
	}
	if MergeKey(CodeVlessEncModeEmpty, "vless", 443, "a") != MergeKey(CodeVlessEncModeEmpty, "vless", 443, "b") {
		t.Fatal("coded keys should ignore message")
	}
	if got := MergeKey(CodeVlessEncModeEmpty, "vless", 443, "a"); got != "VLESS_ENCRYPTION_MODE_EMPTY|vless|443" {
		t.Fatal(got)
	}
	if got := MergeKey(CodeUnknown, "", 0, "x 1"); got != "UNKNOWN||0|x <n>" {
		t.Fatal(got)
	}
}
