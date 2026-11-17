package upgrade

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

type fakeRelease struct {
	latest  string
	zipData []byte
	sums    string
}

func newFake(t *testing.T, version string, binary []byte, corrupt bool) (*httptest.Server, *fakeRelease) {
	t.Helper()
	asset, _ := assetName()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("npnode")
	w.Write(binary)
	zw.Close()
	sum := sha256.Sum256(buf.Bytes())
	h := hex.EncodeToString(sum[:])
	if corrupt {
		h = hex.EncodeToString(make([]byte, 32))
	}
	fr := &fakeRelease{latest: version, zipData: buf.Bytes(), sums: fmt.Sprintf("%s  %s\n", h, asset)}
	mux := http.NewServeMux()
	mux.HandleFunc("/latest", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/tag/"+fr.latest, http.StatusFound)
	})
	mux.HandleFunc("/download/"+version+"/checksums.txt", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(fr.sums)) })
	mux.HandleFunc("/download/"+version+"/"+asset, func(w http.ResponseWriter, r *http.Request) { w.Write(fr.zipData) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, fr
}

func setup(t *testing.T, base string) (exe string, execCalls *int) {
	t.Helper()
	dir := t.TempDir()
	exe = filepath.Join(dir, "npnode")
	os.WriteFile(exe, []byte("old"), 0o755)
	ReleaseBase, StateDir = base, dir
	executable = func() (string, error) { return exe, nil }
	n := 0
	execCalls = &n
	execFn = func(string, []string, []string) error { n++; return nil }
	return exe, execCalls
}

func TestUpgradeReplacesBinaryAndRestarts(t *testing.T) {
	srv, _ := newFake(t, "1.0.9", []byte("new-binary"), false)
	exe, calls := setup(t, srv.URL)
	if err := Handle(&panel.UpgradeRequest{RequestID: "r1", Version: "latest"}, "1.0.8"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(exe); string(b) != "new-binary" || *calls != 1 {
		t.Fatalf("binary=%q exec=%d", b, *calls)
	}
	// 同一个请求不再处理
	os.WriteFile(exe, []byte("old"), 0o755)
	if err := Handle(&panel.UpgradeRequest{RequestID: "r1", Version: "latest"}, "1.0.8"); err != nil || *calls != 1 {
		t.Fatalf("repeated request handled again: %v calls=%d", err, *calls)
	}
}

func TestUpgradeRejectsChecksumMismatch(t *testing.T) {
	srv, _ := newFake(t, "1.0.9", []byte("tampered"), true)
	exe, calls := setup(t, srv.URL)
	if err := Handle(&panel.UpgradeRequest{RequestID: "r2", Version: "1.0.9"}, "1.0.8"); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" || *calls != 0 {
		t.Fatalf("binary replaced despite mismatch: %q", b)
	}
}

func TestUpgradeSkipsCurrentAndInvalid(t *testing.T) {
	srv, _ := newFake(t, "1.0.8", []byte("x"), false)
	exe, calls := setup(t, srv.URL)
	if err := Handle(&panel.UpgradeRequest{RequestID: "r3", Version: "latest"}, "1.0.8"); err != nil || *calls != 0 {
		t.Fatalf("latest==current should skip: %v %d", err, *calls)
	}
	if err := Handle(&panel.UpgradeRequest{RequestID: "r4", Version: "../../etc/passwd"}, "1.0.8"); err == nil {
		t.Fatal("invalid version accepted")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatal("binary changed")
	}
}
