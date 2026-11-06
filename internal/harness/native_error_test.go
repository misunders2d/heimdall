package harness

import (
	"strings"
	"testing"
)

func TestNativeFailedMCPErrorSummary(t *testing.T) {
	t.Setenv("HEIMDALL_TEST_SECRET", "synthetic-secret-value")
	for _, tc := range []struct {
		style  string
		events []string
	}{
		{"codex", []string{`{"method":"item/completed","params":{"item":{"type":"mcpToolCall","server":"test","tool":"list_workspaces","status":"failed","error":{"message":"requested limit 100; maximum 50. token=synthetic-token https://example.test/private?signature=SYNTHETIC\nPRIVATE CONTENT"},"arguments":{"secret":"PRIVATE ARGS"}}}}`}},
		{"codex", []string{`{"method":"item/completed","params":{"item":{"type":"mcpToolCall","server":"test","tool":"list_workspaces","status":"completed","result":{"isError":true,"content":[{"type":"text","text":"requested limit 100; maximum 50. synthetic-secret-value\nPRIVATE CONTENT"}]}}}}`}},
		{"pi", []string{`{"type":"tool_execution_end","toolName":"mcp__test__list_workspaces","isError":true,"result":{"content":[{"type":"text","text":"requested limit 100; maximum 50.\nPRIVATE CONTENT"}]}}`}},
		{"claude", []string{`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"mcp__test__list_workspaces"}]}}`, `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":true,"content":"requested limit 100; maximum 50.\nPRIVATE CONTENT"}]}}`}},
	} {
		t.Run(tc.style, func(t *testing.T) {
			var tracker progressTracker
			var got Progress
			for _, e := range tc.events {
				tracker.event(tc.style, []byte(e), func(p Progress) {
					if p.Completed {
						got = p
					}
				})
			}
			if !got.Error || !strings.Contains(got.ErrorSummary, "requested limit 100; maximum 50") {
				t.Fatal(got)
			}
			for _, bad := range []string{"synthetic-token", "SYNTHETIC", "PRIVATE CONTENT", "PRIVATE ARGS", "synthetic-secret-value"} {
				if strings.Contains(got.ErrorSummary, bad) {
					t.Fatal("leak", got)
				}
			}
		})
	}
	for _, raw := range []string{`{"message":"` + strings.Repeat("é", 500) + `"}`, `{"content":[{"type":"text","text":"` + strings.Repeat("🦉", 500) + `"}]}`} {
		if got := nativeError(true, []byte(raw)); len(got) > 233 || !strings.HasSuffix(got, "…") {
			t.Fatal("unbounded", got)
		}
	}
	if got := nativeError(false, []byte(`{"message":"private successful content"}`)); got != "" {
		t.Fatal(got)
	}
	if got := toolError("Bash", true, []byte(`{"content":[{"type":"text","text":"private stderr dump"}]}`)); got != "" {
		t.Fatal(got)
	}
	if got := nativeError(true, []byte(`{"content":[{"type":"text","text":"{\"error\":\"limit 200; maximum 100\",\"data\":\"PRIVATE CONTENT\"}"}]}`)); strings.Contains(got, "PRIVATE") || !strings.Contains(got, "maximum 100") {
		t.Fatal(got)
	}
}
