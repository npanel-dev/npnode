package upgrade

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// flakyRelease 前 fails 次请求返回 503，之后正常提供发布文件
func flakyRelease(t *testing.T, version string, binary []byte, fails int) (*httptest.Server, *int) {
	t.Helper()
	good, _ := newFake(t, version, binary, false)
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n <= fails {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		resp, err := http.Get(good.URL + r.URL.Path)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// fakeClock 让重试的退避时间可控
func fakeClock(t *testing.T) *time.Time {
	t.Helper()
	cur := time.Unix(1_700_000_000, 0)
	now = func() time.Time { return cur }
	t.Cleanup(func() { now = time.Now; pending = nil })
	return &cur
}

func cfgWith(req *panel.UpgradeRequest) *panel.ServerConfigResponse {
	return &panel.ServerConfigResponse{Data: &panel.Data{Upgrade: req}}
}

func TestUpgradeRetriesAfterNetworkFailure(t *testing.T) {
	srv, _ := flakyRelease(t, "1.0.9", []byte("new-binary"), 1)
	exe, calls := setup(t, srv.URL)
	clock := fakeClock(t)
	panel.NodeVersion = "1.0.8"

	Check(cfgWith(&panel.UpgradeRequest{RequestID: "r5", Version: "1.0.9"}))
	if b, _ := os.ReadFile(exe); string(b) != "old" || *calls != 0 {
		t.Fatalf("first attempt should fail: binary=%q exec=%d", b, *calls)
	}
	if b, _ := os.ReadFile(filepath.Join(StateDir, stateFileName)); len(b) != 0 {
		t.Fatalf("network failure must not mark request handled, got %q", b)
	}
	// 退避时间未到不重试
	Retry()
	if *calls != 0 {
		t.Fatal("retried before backoff elapsed")
	}
	*clock = clock.Add(retryBase)
	Retry()
	if b, _ := os.ReadFile(exe); string(b) != "new-binary" || *calls != 1 {
		t.Fatalf("retry should upgrade: binary=%q exec=%d", b, *calls)
	}
	if pending != nil {
		t.Fatal("pending not cleared after success")
	}
}

func TestUpgradeGivesUpAfterMaxAttempts(t *testing.T) {
	srv, hits := flakyRelease(t, "1.0.9", []byte("new-binary"), 1000)
	_, calls := setup(t, srv.URL)
	clock := fakeClock(t)
	panel.NodeVersion = "1.0.8"

	Check(cfgWith(&panel.UpgradeRequest{RequestID: "r6", Version: "1.0.9"}))
	for i := 0; i < maxAttempts+3; i++ {
		*clock = clock.Add(retryMax)
		Retry()
	}
	if *hits != maxAttempts || *calls != 0 {
		t.Fatalf("attempts=%d want %d, exec=%d", *hits, maxAttempts, *calls)
	}
	if b, _ := os.ReadFile(filepath.Join(StateDir, stateFileName)); string(b) != "r6" {
		t.Fatalf("exhausted request should be marked handled, got %q", b)
	}
	// 重启后同一请求也不再尝试
	Check(cfgWith(&panel.UpgradeRequest{RequestID: "r6", Version: "1.0.9"}))
	if *hits != maxAttempts {
		t.Fatal("handled request attempted again")
	}
}

func TestUpgradeWithdrawnStopsRetry(t *testing.T) {
	srv, hits := flakyRelease(t, "1.0.9", []byte("new-binary"), 1000)
	setup(t, srv.URL)
	clock := fakeClock(t)
	panel.NodeVersion = "1.0.8"

	Check(cfgWith(&panel.UpgradeRequest{RequestID: "r7", Version: "1.0.9"}))
	Check(cfgWith(nil)) // 面板撤回（请求过期）
	*clock = clock.Add(retryMax)
	Retry()
	if *hits != 1 {
		t.Fatalf("withdrawn request retried: hits=%d", *hits)
	}
}

func TestUpgradeChecksumMismatchNotRetried(t *testing.T) {
	srv, _ := newFake(t, "1.0.9", []byte("tampered"), true)
	setup(t, srv.URL)
	fakeClock(t)
	panel.NodeVersion = "1.0.8"
	Check(cfgWith(&panel.UpgradeRequest{RequestID: "r8", Version: "1.0.9"}))
	if pending != nil {
		t.Fatal("checksum mismatch should not be retried")
	}
	if b, _ := os.ReadFile(filepath.Join(StateDir, stateFileName)); string(b) != "r8" {
		t.Fatalf("permanent failure should be marked handled, got %q", b)
	}
}
