package harness

import (
	"context"
	"encoding/json"
	"io"
	"testing"
)

type discardClaudeInput struct{ io.Writer }

func (discardClaudeInput) Close() error { return nil }
func claudeFixture() *claudeStreamBridge {
	return &claudeStreamBridge{ctx: context.Background(), input: discardClaudeInput{io.Discard}, session: "s", ready: make(chan struct{}), answered: make(chan struct{}), pending: map[string]*pendingSteer{}, seen: map[string]string{}}
}
func claudeEvent(b *claudeStreamBridge, typ, id, parent, text string, replay bool) {
	e := map[string]any{"type": typ, "session_id": "s", "parent_tool_use_id": nil, "isReplay": replay, "message": map[string]any{"id": id, "role": "assistant", "content": []any{map[string]any{"type": "text", "text": text}}}}
	if parent != "" {
		e["parent_tool_use_id"] = parent
	}
	raw, _ := json.Marshal(e)
	b.event(raw)
}
func TestClaudeAssistantCommitsAndEmptyResultUsesFinalText(t *testing.T) {
	b := claudeFixture()
	// No exact initial replay: native hooks may have rewritten input.
	claudeEvent(b, "assistant", "comment", "", "commentary", false)
	select {
	case <-b.ready:
	default:
		t.Fatal("assistant did not commit turn; fallback would replay work")
	}
	b.event([]byte(`{"type":"assistant","session_id":"s","message":{"id":"comment","role":"assistant","content":[{"type":"tool_use"}]}}`))
	claudeEvent(b, "assistant", "final", "", "final ", false)
	claudeEvent(b, "assistant", "final", "", "answer", false)
	select {
	case <-b.answered:
		t.Fatal("published before result")
	default:
	}
	b.event([]byte(`{"type":"result","session_id":"s","result":"","is_error":false}`))
	o := b.outcome()
	if o.answer != "final answer" || o.failure != "" {
		t.Fatalf("outcome: %+v", o)
	}
}
func TestClaudeAssistantEvidenceBoundaries(t *testing.T) {
	for _, kind := range []string{"replay", "subagent", "session", "error", "tool", "explicit", "late"} {
		t.Run(kind, func(t *testing.T) {
			b := claudeFixture()
			switch kind {
			case "replay":
				claudeEvent(b, "assistant", "m", "", "stale", true)
			case "subagent":
				claudeEvent(b, "assistant", "m", "tool", "private", false)
			case "session":
				b.event([]byte(`{"type":"assistant","session_id":"other","message":{"role":"assistant","content":[]}}`))
			default:
				claudeEvent(b, "assistant", "m", "", "answer", false)
				switch kind {
				case "error":
					b.event([]byte(`{"type":"result","session_id":"s","is_error":true}`))
				case "tool":
					b.event([]byte(`{"type":"assistant","session_id":"s","message":{"id":"m","role":"assistant","content":[{"type":"tool_use"}]}}`))
					b.event([]byte(`{"type":"result","session_id":"s","result":""}`))
				case "explicit":
					b.event([]byte(`{"type":"result","session_id":"s","result":"canonical"}`))
					if b.outcome().answer != "canonical" {
						t.Fatal("result ignored")
					}
					return
				case "late":
					b.pending["late"] = &pendingSteer{result: make(chan SteerOutcome, 1)}
					b.event([]byte(`{"type":"result","session_id":"s","result":"first"}`))
					delete(b.pending, "late")
					b.event([]byte(`{"type":"result","session_id":"s","result":""}`))
					if len(b.answers) != 1 || b.answers[0] != "first" {
						t.Fatal("stale text reused for late result")
					}
					return
				}
			}
			if b.outcome().answer != "" {
				t.Fatal("untrusted/stale text used")
			}
			if kind == "replay" || kind == "subagent" {
				select {
				case <-b.ready:
					t.Fatal("untrusted event committed")
				default:
				}
			}
		})
	}
}
