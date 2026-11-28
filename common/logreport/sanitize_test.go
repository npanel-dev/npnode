package logreport

import (
	"strings"
	"testing"
)

func TestSanitizeQuerySecretAndKeys(t *testing.T) {
	got := Sanitize("Post https://p/v1/server/status?secret_key=abc123&server_id=1")
	if !strings.Contains(got, "secret_key=<redacted>") || strings.Contains(got, "abc123") || !strings.Contains(got, "server_id=1") {
		t.Fatal(got)
	}
	key := strings.Repeat("A", 20) + "-_" + strings.Repeat("b", 21)
	if len(key) != 43 {
		t.Fatal("bad key len")
	}
	if got := Sanitize("key " + key + " end"); got != "key <key> end" {
		t.Fatal(got)
	}
	if got := Sanitize("u 3f2b1c9e-1a2b-4c3d-9e8f-001122334455"); got != "u <uuid>" {
		t.Fatal(got)
	}
	if got := Sanitize(`{"secret_key":"zzz"}`); got != `{"secret_key":"<redacted>"}` {
		t.Fatal(got)
	}
}

func TestSanitizePrefixedAndEdges(t *testing.T) {
	cases := map[string]string{
		"a?client_secret=abc&x=1": "a?client_secret=<redacted>&x=1",
		"x_secret_key=abc":        "x_secret_key=<redacted>",
		"access_token=abc def":    "access_token=<redacted> def",
		`{"secret_key": "zzz"}`:   `{"secret_key": "<redacted>"}`,
		`{"client_secret":"zzz"}`: `{"client_secret":"<redacted>"}`,
	}
	for in, want := range cases {
		if got := Sanitize(in); got != want {
			t.Errorf("Sanitize(%q)=%q want %q", in, got, want)
		}
	}
	k1 := strings.Repeat("a", 43)
	k2 := strings.Repeat("B", 43)
	if got := Sanitize(k1); got != "<key>" {
		t.Error(got)
	}
	if got := Sanitize("k=" + k1); got != "k=<key>" {
		t.Error(got)
	}
	if got := Sanitize(k1 + " " + k2); got != "<key> <key>" {
		t.Error(got)
	}
	if got := Sanitize(strings.Repeat("a", 44)); got != strings.Repeat("a", 44) {
		t.Error(got)
	}
}
