package panel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/conf"
)

func newTrafficBodyClient(t *testing.T, bodies *[]string) *ClientV1 {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		*bodies = append(*bodies, string(raw))
		_, _ = io.WriteString(w, `{"code":200,"msg":"success"}`)
	}))
	t.Cleanup(srv.Close)
	client, err := NewClientV1(&conf.NodeApiConfig{APIHost: srv.URL, NodeType: "vless", NodeID: 1, Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// 旧接口的请求体保持不变（开关关闭时走它）
func TestReportUserTrafficBodyUnchanged(t *testing.T) {
	var bodies []string
	client := newTrafficBodyClient(t, &bodies)
	traffic := []UserTraffic{{UID: 7, Upload: 1, Download: 2}}
	if err := client.ReportUserTraffic(context.Background(), &traffic); err != nil {
		t.Fatal(err)
	}
	if want := `{"traffic":[{"uid":7,"upload":1,"download":2}]}`; bodies[0] != want {
		t.Fatalf("body = %s, want %s", bodies[0], want)
	}
}

// T3：带批次号的上报携带 boot_id、递增的 sequence 与 reported_at（字段名与面板 /v1/server/push 一致）
func TestReportUserTrafficBatchCarriesIdentity(t *testing.T) {
	var bodies []string
	client := newTrafficBodyClient(t, &bodies)
	b1 := NewHTTPTrafficBatch([]UserTraffic{{UID: 7, Download: 2}})
	b2 := NewHTTPTrafficBatch([]UserTraffic{{UID: 8, Download: 3}})
	if b1.BootID == "" || b1.BootID != b2.BootID || len(b1.BootID) > 128 {
		t.Fatalf("boot ids %q %q", b1.BootID, b2.BootID)
	}
	if b1.Sequence == 0 || b2.Sequence != b1.Sequence+1 {
		t.Fatalf("sequences %d %d", b1.Sequence, b2.Sequence)
	}
	if d := time.Now().Unix() - b1.ReportedAt; d < 0 || d > 5 {
		t.Fatalf("reported_at = %d", b1.ReportedAt)
	}
	for i := 0; i < 2; i++ { // 重试：同一批次号、同一内容
		if err := client.ReportUserTrafficBatch(context.Background(), b1); err != nil {
			t.Fatal(err)
		}
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("retry body differs: %s vs %s", bodies[0], bodies[1])
	}
	var got struct {
		Traffic    []UserTraffic `json:"traffic"`
		BootID     string        `json:"boot_id"`
		Sequence   uint64        `json:"sequence"`
		ReportedAt int64         `json:"reported_at"`
	}
	if err := json.Unmarshal([]byte(bodies[0]), &got); err != nil {
		t.Fatal(err)
	}
	if got.BootID != b1.BootID || got.Sequence != b1.Sequence || got.ReportedAt != b1.ReportedAt || len(got.Traffic) != 1 || got.Traffic[0].UID != 7 {
		t.Fatalf("decoded body = %+v from %s", got, bodies[0])
	}
}
