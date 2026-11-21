package panel

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/npanel-dev/npnode/conf"
)

func TestTrafficReportChecksAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		name      string
		body      string
		status    int
		wantError bool
	}{
		{"legacy empty success", "", 200, false},
		{"zero code", `{"code":0}`, 200, false},
		{"success code", `{"code":200,"msg":"success"}`, 200, false},
		{"business failure", `{"code":500,"msg":"queue unavailable"}`, 200, true},
		{"HTTP failure", `{"code":200}`, 503, true},
		{"invalid response", `<html>login</html>`, 200, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/server/push" || r.URL.Query().Get("protocol") != "vless" || r.URL.Query().Get("port") != "8443" {
					t.Errorf("wrong report destination: %s", r.URL)
				}
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			client, err := NewClientV1(&conf.NodeApiConfig{APIHost: server.URL, NodeType: "vless", NodeID: 1, Port: 8443})
			if err != nil {
				t.Fatal(err)
			}
			traffic := []UserTraffic{{UID: 7, Download: 100}}
			err = client.ReportUserTraffic(context.Background(), &traffic)
			if (err != nil) != test.wantError {
				t.Fatalf("ReportUserTraffic error = %v, wantError=%v", err, test.wantError)
			}
		})
	}
}
