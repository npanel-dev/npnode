package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
)

type pushBody struct {
	Traffic  []panel.UserTraffic `json:"traffic"`
	BootID   string              `json:"boot_id"`
	Sequence uint64              `json:"sequence"`
	Raw      string              `json:"-"`
}

// T3：打开 TrafficIdempotent 后，HTTP 上报失败重试沿用同一批次号与同一内容，期间新采集的流量放进下一批
func TestHTTPTrafficRetryKeepsBatchIdentity(t *testing.T) {
	panel.SetTrafficIdempotent(true)
	t.Cleanup(func() { panel.SetTrafficIdempotent(false) })
	var mu sync.Mutex
	var got []pushBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		var b pushBody
		_ = json.Unmarshal(raw, &b)
		b.Raw = string(raw)
		mu.Lock()
		got = append(got, b)
		n := len(got)
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, `{"code":500,"msg":"queue unavailable"}`)
			return
		}
		fmt.Fprint(w, `{"code":200,"msg":"success"}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	if err := q.report(context.Background()); err == nil {
		t.Fatal("expected business failure")
	}
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("requests = %d, want 3 (retry + new batch)", len(got))
	}
	first, retry, next := got[0], got[1], got[2]
	if first.BootID == "" || first.Sequence == 0 {
		t.Fatalf("first request has no batch identity: %s", first.Raw)
	}
	if retry.Raw != first.Raw {
		t.Fatalf("retry changed the batch:\n first %s\n retry %s", first.Raw, retry.Raw)
	}
	if next.BootID != first.BootID || next.Sequence != first.Sequence+1 {
		t.Fatalf("next batch identity = %s/%d, want %s/%d", next.BootID, next.Sequence, first.BootID, first.Sequence+1)
	}
	if len(next.Traffic) != 1 || next.Traffic[0] != (panel.UserTraffic{UID: 7, Upload: 5, Download: 7}) {
		t.Fatalf("next batch traffic = %+v", next.Traffic)
	}
}

// 开关关闭（默认）：请求体与旧版一致，不带批次号，失败后与新流量合并重发
func TestHTTPTrafficLegacyWhenIdempotentOff(t *testing.T) {
	var mu sync.Mutex
	var raws []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		mu.Lock()
		raws = append(raws, string(raw))
		n := len(raws)
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, `{"code":500}`)
			return
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	_ = q.report(context.Background())
	q.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{`{"traffic":[{"uid":7,"upload":10,"download":20}]}`, `{"traffic":[{"uid":7,"upload":15,"download":27}]}`}
	if len(raws) != 2 || raws[0] != want[0] || raws[1] != want[1] {
		t.Fatalf("bodies = %v, want %v", raws, want)
	}
}
