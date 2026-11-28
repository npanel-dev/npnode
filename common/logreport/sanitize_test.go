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
