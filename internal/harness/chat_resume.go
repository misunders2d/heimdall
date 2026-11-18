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
	At             time.Time
}

func claudeToolPath(l Launch, id string) string {
	return filepath.Join(l.Home, ".local", "state", "heimdall", "sessions", "claude", id, "tools.jsonl")
}

// ClaudeToolRecord is a synchronous native hook. A start is on disk before
// Claude can run the tool. Store metadata only; never return tool decisions.
func ClaudeToolRecord(in io.Reader, path string) error {
	var e struct {
		Event string          `json:"hook_event_name"`
		ID    string          `json:"tool_use_id"`
		Input json.RawMessage `json:"tool_input"`
		Name  string          `json:"tool_name"`
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
	if err := json.NewEncoder(f).Encode(toolRecord{e.ID, e.Name, toolArgs(e.Input), e.Event != "PreToolUse", time.Now()}); err != nil {
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

// ChatResumeRecord finds the original session and reads tool calls lacking
// results. A transcript with only headers is insufficient to resume Claude.
func ChatResumeRecord(name string, l Launch, id string, since time.Time) (record string, actions []string) {
	path := SessionPath(name, l, id)
	if name == "omp" && path != "" {
		paths, _ := filepath.Glob(filepath.Join(path, "*.jsonl"))
		if len(paths) != 1 {
			return "", nil
		}
		path = paths[0]
	}
	pending := map[string]string{}
	done := map[string]bool{}
	apply := func(t toolRecord) {
		if t.ID == "" {
			return
		}
		if t.Done {
			done[t.ID] = true
			delete(pending, t.ID)
		} else if toolName(t.Name) && !done[t.ID] && !t.At.Before(since) {
			label := t.Name
			if t.Args != "" {
				label += " (" + t.Args + ")"
			}
			pending[t.ID] = label
		}
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
				Role       string
				ToolCallID string
				Content    json.RawMessage
			}
			Payload struct {
				Type, Name string
				Arguments  json.RawMessage
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
				apply(toolRecord{e.Payload.CallID, e.Payload.Name, toolArgs(e.Payload.Arguments), false, e.Timestamp})
			case "function_call_output", "custom_tool_call_output":
				apply(toolRecord{ID: e.Payload.CallID, Done: true})
			}
		}
		if e.Message.Role == "user" || e.Message.Role == "assistant" {
			hasHistory = true
		}
		if e.Message.Role == "toolResult" {
			apply(toolRecord{ID: e.Message.ToolCallID, Done: true})
		}
		var blocks []struct {
			Type, ID, Name   string
			Input, Arguments json.RawMessage
			ToolUseID        string `json:"tool_use_id"`
		}
		_ = json.Unmarshal(e.Message.Content, &blocks)
		for _, b := range blocks {
			switch b.Type {
			case "toolCall", "tool_use":
				input := b.Arguments
				if len(input) == 0 {
					input = b.Input
				}
				apply(toolRecord{b.ID, b.Name, toolArgs(input), false, e.Timestamp})
			case "tool_result":
				apply(toolRecord{ID: b.ToolUseID, Done: true})
			}
		}
	}) && hasHistory {
		record = path
	}
	actions = slices.Sorted(maps.Values(pending))
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
