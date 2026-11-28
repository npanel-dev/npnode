package nodev1

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestLogReportProtoJSONNames 校验日志上报消息的 JSON 字段名（HTTP 兜底使用 proto 名）以及往返序列化一致。
func TestLogReportProtoJSONNames(t *testing.T) {
	report := &LogReport{
		ReportId: "rid-1",
		Events: []*LogEvent{{
			Level:      "error",
			Code:       "VLESS_ENCRYPTION_MODE_EMPTY",
			Stage:      "inbound",
			Protocol:   "vless",
			Port:       443,
			Message:    "encryption mode empty",
			Context:    []string{"line1", "line2"},
			OccurredAt: 1700000000000,
			Version:    "v1.0.0",
			Exiting:    true,
			Count:      3,
		}},
		Resolves: []*LogResolve{{
			Scope:          "server",
			ActiveInbounds: []string{"vless:443"},
			ResolvedAt:     1700000001000,
		}},
	}

	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(data)
	for _, key := range []string{"report_id", "occurred_at", "active_inbounds", "resolved_at", "count"} {
		if !strings.Contains(js, `"`+key+`"`) {
			t.Errorf("JSON 缺少字段 %q：%s", key, js)
		}
	}

	var decoded LogReport
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(report, &decoded) {
		t.Errorf("往返序列化后不一致：got %v want %v", &decoded, report)
	}
}

// TestSyncRequestLogPayload 校验 SyncRequest 的 oneof 中可以承载 LogReport（字段号 6）。
func TestSyncRequestLogPayload(t *testing.T) {
	req := &SyncRequest{Payload: &SyncRequest_Log{Log: &LogReport{ReportId: "rid-2"}}}
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded SyncRequest
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := decoded.GetLog()
	if got == nil || got.GetReportId() != "rid-2" {
		t.Fatalf("oneof log 载荷丢失：%v", &decoded)
	}
}
