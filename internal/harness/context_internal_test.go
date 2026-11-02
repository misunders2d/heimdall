package harness

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestContextHookRepeatedHumanText(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HD_CONTEXT_DIR", dir)
	human := "hello\n[Heimdall | human-written marker]\nattachment: /tmp/photo.png"
	for _, context := range []string{"sender one; untrusted history A", "sender two; untrusted history B"} {
		if _, err := stageContext(dir, human, context); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"sender one; untrusted history A", "sender two; untrusted history B"} {
		data, _ := json.Marshal(map[string]string{"prompt": human})
		var out bytes.Buffer
		if err := ContextHook(bytes.NewReader(data), &out); err != nil {
			t.Fatal(err)
		}
		var got struct {
			Hook struct {
				Context string `json:"additionalContext"`
			} `json:"hookSpecificOutput"`
		}
		if err := json.Unmarshal(out.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Hook.Context != want {
			t.Fatalf("got %q want %q", got.Hook.Context, want)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("context handovers retained")
	}
}
func TestClaudeContextPreservesExistingGuards(t *testing.T) {
	spec := Spec{Harness: "claude", Prompt: "hello", Context: "sender", Instructions: "transport"}
	dir, args, err := prepareContext(spec, []string{"--settings", `{"hooks":{"PreToolUse":[{"hooks":[{"type":"command","command":"employee guard"}]}]},"permissions":{"deny":["Read(//root/**)"]}}`, "--settings", `{"hooks":{"PreToolUse":[{"hooks":[{"type":"command","command":"slack guard"}]}]}}`})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	joined := strings.Join(args, " ")
	for _, want := range []string{"employee guard", "slack guard", "Read(//root/**)", "UserPromptSubmit", "input-context", "transport"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("lost %q", want)
		}
	}
}
