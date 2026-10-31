package harness

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestNativeProgressTracksOnlyCorrelatedPublicThinking(t *testing.T) {
	for _, tc := range []struct {
		style  string
		events []string
	}{
		{"pi", []string{`{"type":"message_update","assistantMessageEvent":{"type":"thinking_start","contentIndex":0}}`, `{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":1,"delta":"wrong index"}}`, `{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"Checking public sources"}}`, `{"type":"tool_execution_start","toolName":"web_search"}`}},
		{"claude", []string{`{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}}`, `{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"wrong index"}}}`, `{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Checking public sources"}}}`, `{"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"web_search"}}}`}},
		{"codex", []string{`{"method":"item/started","params":{"item":{"id":"r1","type":"reasoning"}}}`, `{"method":"item/reasoning/summaryTextDelta","params":{"itemId":"other","summaryIndex":0,"delta":"wrong index"}}`, `{"method":"item/reasoning/summaryTextDelta","params":{"itemId":"r1","summaryIndex":0,"delta":"Checking public sources"}}`, `{"method":"item/reasoning/textDelta","params":{"itemId":"r1","contentIndex":0,"delta":"raw private reasoning"}}`, `{"method":"item/started","params":{"item":{"id":"tool1","type":"webSearch"}}}`}},
	} {
		t.Run(tc.style, func(t *testing.T) {
			var tracker progressTracker
			var texts, tools []string
			emit := func(p Progress) {
				texts = append(texts, p.Text)
				if p.Tool != "" {
					tools = append(tools, p.Tool)
				}
			}
			for _, event := range tc.events {
				tracker.event(tc.style, []byte(event), emit)
			}
			joined := strings.Join(texts, " ")
			if !strings.Contains(joined, "Checking public sources") || strings.Contains(joined, "wrong index") || strings.Contains(joined, "raw private") {
				t.Fatalf("telemetry %q", joined)
			}
			if len(tools) != 1 {
				t.Fatalf("tool events %v", tools)
			}
		})
	}
}
func TestCodexProgressRejectsOtherTurn(t *testing.T) {
	b := codexTestBridge()
	var got []Progress
	b.progress = func(p Progress) { got = append(got, p) }
	b.event([]byte(`{"method":"item/started","params":{"threadId":"other","turnId":"turn-1","item":{"id":"r1","type":"reasoning"}}}`))
	if len(got) != 0 {
		t.Fatal("foreign progress")
	}
}

func TestCompletedPublicSummaryAndDuplicatePart(t *testing.T) {
	var tracker progressTracker
	var got []Progress
	emit := func(p Progress) { got = append(got, p) }
	tracker.event("codex", []byte(`{"method":"item/started","params":{"item":{"id":"r1","type":"reasoning"}}}`), emit)
	tracker.event("codex", []byte(`{"method":"item/reasoning/summaryTextDelta","params":{"itemId":"r1","summaryIndex":0,"delta":"Checking sources"}}`), emit)
	n := len(got)
	tracker.event("codex", []byte(`{"method":"item/reasoning/summaryPartAdded","params":{"itemId":"r1","summaryIndex":0}}`), emit)
	if len(got) != n {
		t.Fatal("duplicate part erased thinking")
	}
	tracker.event("codex", []byte(`{"method":"item/completed","params":{"item":{"id":"r1","type":"reasoning","summary":["Final public summary"]}}}`), emit)
	if got[len(got)-1].Text != "Final public summary" {
		t.Fatal("completed reasoning summary ignored")
	}
	tracker.event("claude", []byte(`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"Public Claude thinking"}]}}`), emit)
	if got[len(got)-1].Text != "Public Claude thinking" {
		t.Fatal("complete assistant thinking ignored")
	}
}
func TestProgressWiredIntoNativeAdapters(t *testing.T) {
	var got []Progress
	emit := func(p Progress) { got = append(got, p) }
	pi := &piRPCBridge{ctx: context.Background(), ready: make(chan struct{}), input: discard{io.Discard}, progress: emit}
	pi.event([]byte(`{"type":"tool_execution_start","toolName":"web_search"}`))
	claude := &claudeStreamBridge{ctx: context.Background(), session: "session-1", progress: emit}
	claude.event([]byte(`{"type":"assistant","session_id":"session-1","message":{"content":[{"type":"thinking","thinking":"Checking public sources"}]}}`))
	codex := codexTestBridge()
	codex.progress = emit
	codex.event([]byte(`{"method":"item/started","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"r1","type":"reasoning"}}}`))
	codex.event([]byte(`{"method":"item/reasoning/summaryTextDelta","params":{"threadId":"t1","turnId":"turn-1","itemId":"r1","summaryIndex":0,"delta":"Checking more sources"}}`))
	if len(got) != 4 || got[0].Tool != "web_search" || got[1].Text != "Checking public sources" || got[3].Text != "Checking more sources" {
		t.Fatalf("native wiring %+v", got)
	}
}
