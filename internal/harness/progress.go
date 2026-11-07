package harness

import (
	"encoding/json"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
)

// Progress is optional native public telemetry, never an answer or readiness
// acknowledgement. Transport observers must return promptly and never log it.
type Progress struct {
	Phase, Text, Tool string
	Reset             bool
	Error             bool
	ErrorSummary      string // bounded sanitized native error field only, never public progress
	Completed         bool   // terminal native tool event; starts never prove success
	// SignIn names the service a tool's result said needs a sign-in: a
	// structured auth_needed result (details.status), which Pi reports as an
	// ordinary tool result, not an error. Scheduled runs fail on it.
	SignIn string
}

// Ported telemetry correlation from AgentNet 3b14cf7d56c78de62c311587409cc6db925e1af5:
// internal/client/{pirpc,claudestream,codexroom}.go. Only public summary
// fields are used for Codex; raw reasoning text is never consumed.
type progressTracker struct {
	open        bool
	started     bool
	key         string
	index       int
	text        string
	claudeTools map[string]string
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

// signIn is the service a Pi tool result asks a sign-in for, or "": its
// details.status is auth_needed (the employee Google runtime), with
// details.service, else the google_ tool prefix, else the tool's name.
func signIn(tool string, result json.RawMessage) string {
	var r struct {
		Details struct{ Status, Service string }
	}
	if json.Unmarshal(result, &r) != nil || r.Details.Status != "auth_needed" {
		return ""
	}
	switch {
	case r.Details.Service != "":
		return r.Details.Service
	case strings.HasPrefix(tool, "google_"):
		return "google"
	}
	return tool
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
		Result                 json.RawMessage
		AssistantMessageEvent  json.RawMessage
		Event                  json.RawMessage
		Params                 json.RawMessage
		Message                struct {
			Role    string
			Content []struct {
				Type, ID, Name, Thinking string
				ToolUseID                string          `json:"tool_use_id"`
				IsError                  bool            `json:"is_error"`
				Content                  json.RawMessage `json:"content"`
			}
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
			emit(Progress{Phase: "tools", Tool: e.ToolName, Completed: true, Error: e.IsError, ErrorSummary: toolError(e.ToolName, e.IsError, e.Result), SignIn: signIn(e.ToolName, e.Result)})
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
		if e.Type == "user" {
			for _, b := range e.Message.Content {
				if name := t.claudeTools[b.ToolUseID]; b.Type == "tool_result" && name != "" {
					emit(Progress{Phase: "tools", Tool: name, Completed: true, Error: b.IsError, ErrorSummary: toolError(name, b.IsError, b.Content)})
					delete(t.claudeTools, b.ToolUseID)
				}
			}
		}
		if e.Type == "assistant" {
			for _, b := range e.Message.Content {
				if b.Type == "tool_use" && b.ID != "" {
					if t.claudeTools == nil {
						t.claudeTools = map[string]string{}
					}
					t.claudeTools[b.ID] = b.Name
				}
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
				ID, Type, Name, Tool, Server, Status string
				Summary                              json.RawMessage
				Error                                json.RawMessage
				Result                               json.RawMessage
				ExitCode                             *int
			}
			ItemID       string
			SummaryIndex *int
			Delta        string
		}
		if json.Unmarshal(e.Params, &p) != nil {
			return
		}
		// Input items have no tool status. Treating their completion as a
		// tool failure rejects otherwise successful scheduled checkpoints.
		if p.Item.Type == "userMessage" {
			return
		}
		// Installed Codex 0.166.0 v2.ThreadItem (generated schema): MCP
		// calls carry server/tool/status, not a synthesized name field.
		tool := p.Item.Name
		if p.Item.Type == "mcpToolCall" {
			tool = ""
			if p.Item.Server != "" && p.Item.Tool != "" {
				tool = "mcp__" + p.Item.Server + "__" + p.Item.Tool
			}
		} else if tool == "" {
			tool = p.Item.Tool
			if tool == "" {
				tool = p.Item.Type
			}
		}
		switch e.Method {
		case "item/started":
			t.open = false
			if p.Item.Type == "reasoning" {
				t.start(p.Item.ID, 0, emit)
			} else if p.Item.Type == "agentMessage" {
				emit(Progress{Phase: "finalizing"})
			} else {
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
				var result struct {
					IsError bool `json:"isError"`
				}
				_ = json.Unmarshal(p.Item.Result, &result)
				failed := p.Item.Status != "completed" || len(p.Item.Error) > 0 && string(p.Item.Error) != "null" || result.IsError || p.Item.ExitCode != nil && *p.Item.ExitCode != 0
				summary := ""
				if p.Item.Type == "mcpToolCall" {
					summary = nativeError(failed, p.Item.Error, p.Item.Result)
				}
				emit(Progress{Phase: "tools", Tool: tool, Completed: true, Error: failed, ErrorSummary: summary})
			}
		}
	}
}

// Only explicit native error/message fields or failed tool text blocks count.
// Never serialize a whole result, command output, arguments or stderr.
func nativeError(failed bool, fields ...json.RawMessage) string {
	if !failed {
		return ""
	}
	for _, raw := range fields {
		if text := errorText(raw, 0); text != "" {
			return core.SanitizeNativeError(text, "")
		}
	}
	return ""
}
func errorText(raw json.RawMessage, depth int) string {
	if depth > 6 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		trimmed := strings.TrimSpace(text)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			return errorText(json.RawMessage(trimmed), depth+1)
		}
		return text
	}
	var obj struct {
		Message string
		Error   json.RawMessage
		Content json.RawMessage
	}
	if json.Unmarshal(raw, &obj) == nil {
		if obj.Message != "" {
			return obj.Message
		}
		if text := errorText(obj.Error, depth+1); text != "" {
			return text
		}
		return errorText(obj.Content, depth+1)
	}
	var blocks []struct{ Type, Text string }
	if json.Unmarshal(raw, &blocks) == nil {
		for _, b := range blocks {
			if b.Type == "text" {
				return errorText(json.RawMessage(mustErrorJSON(b.Text)), depth+1)
			}
		}
	}
	return ""
}

func toolError(name string, failed bool, raw json.RawMessage) string {
	switch strings.ToLower(name) {
	case "bash", "shell", "exec_command", "commandexecution":
		return ""
	}
	return nativeError(failed, raw)
}

func mustErrorJSON(s string) []byte { b, _ := json.Marshal(s); return b }
