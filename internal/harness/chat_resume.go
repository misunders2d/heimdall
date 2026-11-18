package harness

import (
	"bufio"
	"encoding/json"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

type toolRecord struct {
	ID, Name, Args string
	Done           bool
	Aborted        bool `json:",omitempty"`
	At             time.Time
}

func claudeToolPath(l Launch, id string) string {
	return filepath.Join(l.Home, ".local", "state", "heimdall", "sessions", "claude", id, "tools.jsonl")
}

// ClaudeToolRecord is a synchronous native hook. A start is on disk before
// Claude can run the tool. Store metadata only; never return tool decisions.
func ClaudeToolRecord(in io.Reader, path string) error {
	var e struct {
		Event     string          `json:"hook_event_name"`
		ID        string          `json:"tool_use_id"`
		Input     json.RawMessage `json:"tool_input"`
		Name      string          `json:"tool_name"`
		Interrupt bool            `json:"is_interrupt"`
	}
	if err := json.NewDecoder(in).Decode(&e); err != nil {
		return err
	}
	if e.ID == "" || !toolName(e.Name) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	record := toolRecord{ID: e.ID, Name: e.Name, Args: toolArgs(e.Input), At: time.Now()}
	record.Aborted = e.Event == "PostToolUseFailure" && e.Interrupt
	record.Done = !record.Aborted && (e.Event == "PostToolUse" || e.Event == "PostToolUseFailure")
	if err := json.NewEncoder(f).Encode(record); err != nil {
		return err
	}
	return f.Sync()
}

// Keep locators and operation names. Shell operands, text bodies and
// arbitrary tool input can contain credentials or private content.
func toolArgs(raw json.RawMessage) string {
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		raw = json.RawMessage(encoded)
	}
	var args map[string]string
	_ = json.Unmarshal(raw, &args)
	var parts []string
	for _, key := range []string{"action", "file_path", "filePath", "path", "repository", "repo", "branch", "ref", "channel", "target", "workdir"} {
		value := args[key]
		if toolName(value) && core.SanitizeNativeError(value, "") == value {
			parts = append(parts, key+"="+value)
			if len(parts) == 2 {
				break
			}
		}
	}
	if len(parts) < 2 {
		cmd := args["command"]
		if cmd == "" {
			cmd = args["cmd"]
		}
		if words := strings.Fields(cmd); len(words) > 0 && toolName(words[0]) {
			program := filepath.Base(words[0])
			if core.SanitizeNativeError(program, "") == program {
				parts = append(parts, "cmd="+program)
			}
		}
	}
	return strings.Join(parts, ", ")
}

func toolName(name string) bool {
	return name != "" && len(name) <= 128 && strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:-") == ""
}

type nativeToolStatus struct {
	Aborted, Canceled, Cancelled bool
	Status                       string
}

func (s nativeToolStatus) interrupted() bool {
	switch s.Status {
	case "aborted", "canceled", "cancelled", "interrupted":
		return true
	}
	return s.Aborted || s.Canceled || s.Cancelled
}

// Some engines record a synthetic text result so the provider accepts the
// history. Match their interruption markers, not arbitrary failure output.
func abortResultText(raw json.RawMessage, match func(string) bool) bool {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return match(strings.TrimSpace(text))
	}
	var blocks []struct{ Type, Text string }
	if json.Unmarshal(raw, &blocks) == nil {
		for _, b := range blocks {
			if (b.Type == "text" || b.Type == "input_text") && match(strings.TrimSpace(b.Text)) {
				return true
			}
		}
	}
	return false
}

func codexToolAbort(text string) bool {
	// Codex tools/parallel.rs emits these abort results. History repair
	// also uses the plain "aborted" placeholder for an unmatched call.
	if first, rest, ok := strings.Cut(text, "\n"); ok && strings.HasPrefix(first, "Wall time: ") {
		return rest == "aborted by user"
	}
	return text == "aborted" || text == "aborted before dispatch" ||
		text == "aborted by user" || strings.HasPrefix(text, "aborted by user after ")
}

func ompToolAbort(text string) bool {
	return text == "Tool execution was aborted" || text == "Command aborted" ||
		text == "[Command cancelled]" || strings.HasPrefix(text, "[Command cancelled]\n") ||
		text == "[Command aborted]" || strings.HasSuffix(text, "\n[Command aborted]")
}

func claudeToolAbort(text string) bool {
	return text == "[Request interrupted by user]" || text == "[Request interrupted by user for tool use]" ||
		strings.HasPrefix(text, "[Tool call interrupted:")
}

// ChatResumeRecord finds the original session and reads tool calls with
// missing or interrupted results. Claude needs more than transcript headers.
func ChatResumeRecord(name string, l Launch, id string, since time.Time) (record string, actions []string) {
	path := SessionPath(name, l, id)
	if name == "omp" && path != "" {
		paths, _ := filepath.Glob(filepath.Join(path, "*.jsonl"))
		if len(paths) != 1 {
			return "", nil
		}
		path = paths[0]
	}
	calls := map[string]string{}
	results := map[string]bool{} // an explicit interruption dominates a generic result for the same call
	apply := func(t toolRecord) {
		if t.ID == "" {
			return
		}
		if t.Aborted {
			results[t.ID] = false
		} else if _, recorded := results[t.ID]; t.Done && !recorded {
			results[t.ID] = true
		}
		if toolName(t.Name) && !t.At.Before(since) && (t.Args != "" || calls[t.ID] == "") {
			label := t.Name
			if t.Args != "" {
				label += " (" + t.Args + ")"
			}
			calls[t.ID] = label
		}
	}
	result := func(id string, aborted bool) {
		apply(toolRecord{ID: id, Aborted: aborted, Done: !aborted})
	}
	hasHistory := false
	if name == "claude" {
		readRecord(claudeToolPath(l, id), func(data []byte) {
			var t toolRecord
			if json.Unmarshal(data, &t) == nil {
				apply(t)
			}
		})
	}
	if readRecord(path, func(data []byte) {
		var e struct {
			Type      string
			Timestamp time.Time
			Message   struct {
				nativeToolStatus
				Role       string
				ToolCallID string
				Content    json.RawMessage
				Details    json.RawMessage
				IsError    bool
			}
			Payload struct {
				nativeToolStatus
				Type, Name string
				Arguments  json.RawMessage
				Output     json.RawMessage
				CallID     string `json:"call_id"`
			}
		}
		if json.Unmarshal(data, &e) != nil {
			return
		}
		if name == "codex" && e.Type == "response_item" {
			hasHistory = true
			switch e.Payload.Type {
			case "function_call", "custom_tool_call":
				apply(toolRecord{ID: e.Payload.CallID, Name: e.Payload.Name, Args: toolArgs(e.Payload.Arguments), At: e.Timestamp})
			case "function_call_output", "custom_tool_call_output":
				p := e.Payload
				result(p.CallID, p.interrupted() || abortResultText(p.Output, codexToolAbort))
			}
		}
		if e.Message.Role == "user" || e.Message.Role == "assistant" {
			hasHistory = true
		}
		if e.Message.Role == "toolResult" {
			m := e.Message
			var details nativeToolStatus
			_ = json.Unmarshal(m.Details, &details)
			result(m.ToolCallID, m.interrupted() || details.interrupted() || m.IsError && abortResultText(m.Content, ompToolAbort))
		}
		var blocks []struct {
			nativeToolStatus
			Type, ID, Name   string
			Input, Arguments json.RawMessage
			Content          json.RawMessage
			ToolUseID        string `json:"tool_use_id"`
			IsError          bool   `json:"is_error"`
		}
		_ = json.Unmarshal(e.Message.Content, &blocks)
		for _, b := range blocks {
			switch b.Type {
			case "toolCall", "tool_use":
				input := b.Arguments
				if len(input) == 0 {
					input = b.Input
				}
				apply(toolRecord{ID: b.ID, Name: b.Name, Args: toolArgs(input), At: e.Timestamp})
			case "tool_result":
				result(b.ToolUseID, b.interrupted() || b.IsError && abortResultText(b.Content, claudeToolAbort))
			}
		}
	}) && hasHistory {
		record = path
	}
	maps.DeleteFunc(calls, func(id, _ string) bool { return results[id] })
	actions = slices.Sorted(maps.Values(calls))
	return record, actions
}

// A trailing partial JSON row after a kill is rejected by the decoder.
func readRecord(path string, read func([]byte)) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64<<10)
	for {
		data, err := r.ReadBytes('\n')
		if len(data) != 0 {
			read(data)
		}
		if err != nil {
			return err == io.EOF
		}
	}
}
