package harness

import (
	"encoding/json"
	"strings"
)

// Progress is optional native public telemetry, never an answer or readiness
// acknowledgement. Transport observers must return promptly and never log it.
type Progress struct {
	Phase, Text, Tool string
	Reset             bool
	Error             bool
}

// Ported telemetry correlation from AgentNet 3b14cf7d56c78de62c311587409cc6db925e1af5:
// internal/client/{pirpc,claudestream,codexroom}.go. Only public summary
// fields are used for Codex; raw reasoning text is never consumed.
type progressTracker struct {
	open    bool
	started bool
	key     string
	index   int
	text    string
}

func (t *progressTracker) start(key string, index int, emit func(Progress)) {
	t.open = true
	t.started = false
	t.key = key
	t.index = index
	t.text = ""
	emit(Progress{Phase: "thinking", Reset: true})
}
func (t *progressTracker) add(key string, index int, text string, emit func(Progress)) {
	if !t.open || t.key != key || t.index != index {
		return
	}
	t.started = true
	t.text = tailProgress(t.text+text, 2000)
	emit(Progress{Phase: "thinking", Text: t.text, Reset: true})
}
func tailProgress(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[len(r)-n:])
	}
	return s
}
func (t *progressTracker) event(style string, raw []byte, emit func(Progress)) {
	if emit == nil {
		return
	}
	var e struct {
		Type, Method, ToolName string
		IsError                bool
		AssistantMessageEvent  json.RawMessage
		Event                  json.RawMessage
		Params                 json.RawMessage
		Message                struct {
			Role    string
			Content []struct{ Type, Name, Thinking string }
		}
	}
	if json.Unmarshal(raw, &e) != nil {
		return
	}
	switch style {
	case "pi":
		switch e.Type {
		case "agent_start":
			t.open = false
			emit(Progress{Phase: "running"})
		case "agent_end":
			emit(Progress{Phase: "finalizing"})
		case "auto_retry_start", "auto_compaction_start":
			emit(Progress{Phase: "finalizing"})
		case "tool_execution_start":
			emit(Progress{Phase: "tools", Tool: e.ToolName})
		case "tool_execution_end":
			emit(Progress{Phase: "tools", Tool: e.ToolName, Error: e.IsError})
		case "message_update":
			var u struct {
				Type, Delta, Content string
				ContentIndex         *int
			}
			if json.Unmarshal(e.AssistantMessageEvent, &u) != nil || u.ContentIndex == nil {
				return
			}
			i := *u.ContentIndex
			if i < 0 {
				return
			}
			switch u.Type {
			case "thinking_start":
				t.start("", i, emit)
			case "thinking_delta":
				t.add("", i, u.Delta, emit)
			case "thinking_end":
				if t.open && t.index == i {
					emit(Progress{Phase: "thinking", Text: tailProgress(u.Content, 2000), Reset: true})
					t.open = false
				}
			}
		}
	case "claude":
		if e.Type == "assistant" {
			for _, b := range e.Message.Content {
				if b.Type == "thinking" && b.Thinking != "" {
					emit(Progress{Phase: "thinking", Text: tailProgress(b.Thinking, 2000), Reset: true})
				}
				if b.Type == "tool_use" {
					emit(Progress{Phase: "tools", Tool: b.Name})
				}
			}
		}
		if e.Type == "result" {
			emit(Progress{Phase: "finalizing"})
		}
		if e.Type != "stream_event" {
			return
		}
		var u struct {
			Type         string
			Index        *int
			ContentBlock struct{ Type, Thinking, Name string } `json:"content_block"`
			Delta        struct{ Type, Thinking string }
		}
		if json.Unmarshal(e.Event, &u) != nil {
			return
		}
		switch u.Type {
		case "message_start", "message_stop":
			t.open = false
		case "content_block_start":
			t.open = false
			if u.ContentBlock.Type == "thinking" && u.Index != nil && *u.Index >= 0 {
				t.start("", *u.Index, emit)
				t.add("", *u.Index, u.ContentBlock.Thinking, emit)
			}
			if u.ContentBlock.Type == "tool_use" {
				emit(Progress{Phase: "tools", Tool: u.ContentBlock.Name})
			}
		case "content_block_delta":
			if u.Index != nil && u.Delta.Type == "thinking_delta" {
				t.add("", *u.Index, u.Delta.Thinking, emit)
			}
		case "content_block_stop":
			if u.Index != nil && t.index == *u.Index {
				t.open = false
			}
		}
	case "codex":
		var p struct {
			Item struct {
				ID, Type, Name, Tool string
				Summary              json.RawMessage
			}
			ItemID       string
			SummaryIndex *int
			Delta        string
		}
		if json.Unmarshal(e.Params, &p) != nil {
			return
		}
		switch e.Method {
		case "item/started":
			t.open = false
			if p.Item.Type == "reasoning" {
				t.start(p.Item.ID, 0, emit)
			} else if p.Item.Type == "agentMessage" {
				emit(Progress{Phase: "finalizing"})
			} else {
				tool := p.Item.Name
				if tool == "" {
					tool = p.Item.Tool
				}
				if tool == "" {
					tool = p.Item.Type
				}
				emit(Progress{Phase: "tools", Tool: tool})
			}
		case "item/reasoning/summaryPartAdded":
			if p.SummaryIndex != nil && t.open && t.key == p.ItemID && *p.SummaryIndex >= t.index && !(*p.SummaryIndex == t.index && t.started) {
				t.start(p.ItemID, *p.SummaryIndex, emit)
			}
		case "item/reasoning/summaryTextDelta":
			if p.SummaryIndex != nil {
				t.add(p.ItemID, *p.SummaryIndex, p.Delta, emit)
			}
		case "item/completed":
			if p.Item.ID == t.key {
				t.open = false
			}
			if p.Item.Type == "reasoning" {
				var parts []string
				if json.Unmarshal(p.Item.Summary, &parts) != nil {
					var blocks []struct{ Text string }
					if json.Unmarshal(p.Item.Summary, &blocks) == nil {
						for _, b := range blocks {
							parts = append(parts, b.Text)
						}
					}
				}
				if len(parts) > 0 {
					emit(Progress{Phase: "thinking", Text: tailProgress(strings.Join(parts, "\n"), 2000), Reset: true})
				}
			} else if p.Item.Type == "agentMessage" {
				emit(Progress{Phase: "finalizing"})
			} else {
				emit(Progress{Phase: "tools", Tool: p.Item.Type})
			}
		}
	}
}
