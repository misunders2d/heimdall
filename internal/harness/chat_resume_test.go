package harness

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestChatResumeRecordPairsNativeToolCallsAndResults(t *testing.T) {
	const old = "2026-01-01T00:00:00Z"
	const recent = "2026-01-02T00:00:00Z"
	const session = "12345678-aaaa-1234-aaaa-123456789abc"
	since, _ := time.Parse(time.RFC3339, recent)
	for _, tc := range []struct {
		name, path string
		record     string
	}{
		{"omp", ".local/state/heimdall/sessions/omp/" + session + "/native.jsonl", `{"type":"message","timestamp":"OLD","message":{"role":"assistant","content":[{"type":"toolCall","id":"old","name":"oldTool"}]}}
{"type":"message","timestamp":"RECENT","message":{"role":"assistant","content":[{"type":"toolCall","id":"finished","name":"read"},{"type":"toolCall","id":"open","name":"write","arguments":{"path":"notes.txt","content":"omit this"}}]}}
{"type":"message","timestamp":"RECENT","message":{"role":"toolResult","toolCallId":"finished"}}
{"type":"partial"`},
		{"codex", ".codex/sessions/2026/01/02/rollout-" + session + ".jsonl", `{"type":"response_item","timestamp":"OLD","payload":{"type":"function_call","call_id":"old","name":"oldTool"}}
{"type":"response_item","timestamp":"RECENT","payload":{"type":"function_call","call_id":"finished","name":"read"}}
{"type":"response_item","timestamp":"RECENT","payload":{"type":"function_call","call_id":"open","name":"write","arguments":"{\"path\":\"notes.txt\",\"content\":\"omit this\"}"}}
{"type":"response_item","timestamp":"RECENT","payload":{"type":"function_call_output","call_id":"finished"}}
`},
		{"claude", ".claude/projects/work/" + session + ".jsonl", `{"type":"assistant","timestamp":"OLD","message":{"role":"assistant","content":[{"type":"tool_use","id":"old","name":"oldTool"}]}}
{"type":"assistant","timestamp":"RECENT","message":{"role":"assistant","content":[{"type":"tool_use","id":"finished","name":"read"},{"type":"tool_use","id":"open","name":"write","input":{"file_path":"notes.txt","content":"omit this"}}]}}
{"type":"user","timestamp":"RECENT","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"finished"}]}}
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := Launch{Home: t.TempDir()}
			path := filepath.Join(l.Home, tc.path)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			content := strings.NewReplacer("OLD", old, "RECENT", recent).Replace(tc.record)
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			arg := "path"
			if tc.name == "claude" {
				arg = "file_path"
			}
			if got, tools := ChatResumeRecord(tc.name, l, session, since); got != path || !slices.Equal(tools, []string{"write (" + arg + "=notes.txt)"}) {
				t.Fatalf("record %q tools %v", got, tools)
			}
		})
	}
}

func TestClaudeJournalRecordsToolsWithoutAResumableSession(t *testing.T) {
	l := Launch{Home: t.TempDir()}
	const id = "12345678-aaaa-1234-aaaa-123456789abc"
	path := claudeToolPath(l, id)
	since := time.Now()
	for _, event := range []string{
		`{"hook_event_name":"PreToolUse","tool_use_id":"open","tool_name":"Bash","tool_input":{"command":"git merge private-argument","workdir":"/scratch/repo"}}`,
		`{"hook_event_name":"PreToolUse","tool_use_id":"complete","tool_name":"Read"}`,
		`{"hook_event_name":"PostToolUseFailure","tool_use_id":"complete","tool_name":"Read"}`,
	} {
		if err := ClaudeToolRecord(strings.NewReader(event), path); err != nil {
			t.Fatal(err)
		}
	}
	if record, tools := ChatResumeRecord("claude", l, id, since); record != "" || !slices.Equal(tools, []string{"Bash (workdir=/scratch/repo, cmd=git)"}) {
		t.Fatalf("record %q tools %v", record, tools)
	}
	if data, err := os.ReadFile(path); err != nil || strings.Contains(string(data), "private-argument") {
		t.Fatalf("non-metadata tool journal: %v", err)
	}
}
