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

func TestCodexInputEventsAreNotToolEvidence(t *testing.T) {
	b := codexTestBridge()
	var got []Progress
	b.progress = func(p Progress) { got = append(got, p) }
	// Native Codex emits these for the submitted input, without a status.
	// Neither event is a tool call, including when the input contains images.
	for _, method := range []string{"item/started", "item/completed"} {
		b.event([]byte(`{"method":"` + method + `","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"input-1","type":"userMessage","content":[{"type":"text","text":"synthetic input"},{"type":"image","url":"https://example.invalid/image.png"}]}}}`))
	}
	if len(got) != 0 {
		t.Fatalf("input counted as tool telemetry: %+v", got)
	}
	b.event([]byte(`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"m1","type":"mcpToolCall","server":"example","tool":"read","status":"completed","result":{"content":[],"isError":false},"error":null}}}`))
	if len(got) != 1 || !got[0].Completed || got[0].Error || got[0].Tool != "mcp__example__read" {
		t.Fatalf("real tool evidence changed: %+v", got)
	}
}

func TestCodexNativeMCPCompletionEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, status, result, err string
		failed                    bool
	}{
		{"success", "completed", `{"content":[]}`, `null`, false},
		{"failure", "failed", `null`, `{"message":"synthetic"}`, true},
		{"mcp_error", "completed", `{"content":[],"isError":true}`, `null`, true},
		{"no_status", "", `{"content":[]}`, `null`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := codexTestBridge()
			var got []Progress
			b.progress = func(p Progress) { got = append(got, p) }
			b.event([]byte(`{"method":"item/started","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"m1","type":"mcpToolCall","server":"codex_apps","tool":"slack_slack_read_thread","status":"inProgress","arguments":{}}}}`))
			if len(got) != 1 || got[0].Completed || got[0].Tool != "mcp__codex_apps__slack_slack_read_thread" {
				t.Fatal("start counted/named incorrectly", got)
			}
			line := `{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"m1","type":"mcpToolCall","server":"codex_apps","tool":"slack_slack_read_thread","status":"` + tc.status + `","result":` + tc.result + `,"error":` + tc.err + `}}}`
			b.event([]byte(line))
			if len(got) != 2 || !got[1].Completed || got[1].Error != tc.failed || got[1].Tool != got[0].Tool {
				t.Fatal(got)
			}
		})
	}
}

func TestClaudeAndPiToolSuccessRequiresTerminalEvidence(t *testing.T) {
	var tracker progressTracker
	var got []Progress
	emit := func(p Progress) { got = append(got, p) }
	for _, raw := range []string{`{"type":"assistant","message":{"content":[{"id":"t1","type":"tool_use","name":"mcp__slack__read_thread"}]}}`, `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":false}]}}`} {
		tracker.event("claude", []byte(raw), emit)
	}
	tracker.event("pi", []byte(`{"type":"tool_execution_start","toolName":"read"}`), emit)
	tracker.event("pi", []byte(`{"type":"tool_execution_end","toolName":"read","isError":true}`), emit)
	if len(got) != 4 || got[0].Completed || !got[1].Completed || got[1].Error || got[2].Completed || !got[3].Completed || !got[3].Error {
		t.Fatal(got)
	}
}

// A Pi tool result whose details say auth_needed (the employee Google
// runtime's, an ordinary result) names the service needing a sign-in;
// other results, and other harnesses' result fields, name none.
func TestPiToolResultSignIn(t *testing.T) {
	for _, tc := range []struct{ style, event, want string }{
		{"pi", `{"type":"tool_execution_end","toolName":"google_gmail_search","isError":false,"result":{"content":[{"type":"text","text":"Send google login in a DM."}],"details":{"status":"auth_needed"}}}`, "google"},
		{"pi", `{"type":"tool_execution_end","toolName":"crm_read","result":{"details":{"status":"auth_needed","service":"mellanni"}}}`, "mellanni"},
		{"pi", `{"type":"tool_execution_end","toolName":"google_drive_list","result":{"details":{"status":"success"}}}`, ""},
		{"pi", `{"type":"tool_execution_end","toolName":"bash","result":{"content":[],"details":"text"}}`, ""},
		{"claude", `{"type":"result","result":"auth_needed"}`, ""},
	} {
		var got []string
		var tracker progressTracker
		tracker.event(tc.style, []byte(tc.event), func(p Progress) {
			if p.SignIn != "" {
				got = append(got, p.SignIn)
			}
		})
		if strings.Join(got, ",") != tc.want {
			t.Errorf("%s: sign-in %v, want %q", tc.event, got, tc.want)
		}
	}
}

// Only named native tool calls are decoded, from the engine's own events:
// Pi direct and codemode-nested calls (by toolCallId), Claude tool_use and
// tool_result, Codex mcpToolCall. Other tools and model text never are,
// and nothing is decoded when the sink is off.
func TestNativeCallsDecoded(t *testing.T) {
	const name = "mcp__t3__t3_thread_send"
	var got []NativeCall
	sink := &NativeCalls{Tools: []string{name}, Done: func(n NativeCall) { got = append(got, n) }}
	streams := map[string][]string{
		"pi": {
			`{"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"ls"}}`,
			`{"type":"tool_execution_end","toolCallId":"c1","toolName":"bash","result":{"content":[{"type":"text","text":"x"}]},"isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"c2/1","parentToolCallId":"c2","toolName":"` + name + `","args":{"threadId":"T"}}`,
			`{"type":"tool_execution_end","toolCallId":"c2/1","parentToolCallId":"c2","toolName":"` + name + `","result":{"content":[{"type":"text","text":"{\"runId\":\"r\"}"}]},"isError":false}`,
			`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"` + name + ` {\"runId\":\"fake\"}"}}`,
		},
		"claude": {
			`{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"tu","name":"` + name + `","input":{"threadId":"T"}}]}}`,
			`{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu","is_error":true,"content":"denied"}]}}`,
		},
		"codex": {
			`{"method":"item/completed","params":{"item":{"id":"i1","type":"mcpToolCall","server":"t3","tool":"t3_thread_send","status":"completed","arguments":{"threadId":"T"},"result":{"structuredContent":{"runId":"r"}}}}}`,
		},
	}
	for style, lines := range streams {
		var tr progressTracker
		tr.native = sink
		for _, l := range lines {
			tr.event(style, []byte(l), nil)
		}
	}
	if len(got) != 3 {
		t.Fatalf("decoded %+v", got)
	}
	for _, n := range got {
		if n.Tool != name || string(n.Args) != `{"threadId":"T"}` {
			t.Fatalf("%+v", n)
		}
	}
	var off progressTracker
	off.event("pi", []byte(streams["pi"][2]), nil)
	off.event("pi", []byte(streams["pi"][3]), nil)
	if len(got) != 3 || off.nativeArgs != nil {
		t.Fatal("decoded with the sink off")
	}
}
