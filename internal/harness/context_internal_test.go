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

// The attachment fix must not relax strict hook lookup: a changed descriptor,
// different text, another process/caller/session directory, and an already
// consumed handover all fail without consuming the intended context.
func TestPhotoContextStrictLookup(t *testing.T) {
	dir := t.TempDir()
	foreign := t.TempDir()
	prompt := "[File saved]\nsource: message\npath: /tmp/synthetic/in/photo.png\nname: photo.png\nmime: image/png\nkind: photo\nbytes: 68\n[Image attached inline where the harness takes images; otherwise open the file at path.]"
	if _, err := stageContext(dir, prompt, "current generation, verified caller A/session A"); err != nil {
		t.Fatal(err)
	}
	lookup := func(p string) (string, error) {
		raw, _ := json.Marshal(map[string]string{"prompt": p})
		var out bytes.Buffer
		err := ContextHook(bytes.NewReader(raw), &out)
		return out.String(), err
	}
	t.Setenv("HD_CONTEXT_DIR", dir)
	for _, wrong := range []string{"\n" + prompt, prompt + "\n", strings.Replace(prompt, "photo.png", "other.png", 1), "caption\n" + prompt} {
		if out, err := lookup(wrong); err == nil || out != "" {
			t.Fatal("mismatched prompt accepted")
		}
	}
	t.Setenv("HD_CONTEXT_DIR", foreign)
	if out, err := lookup(prompt); err == nil || out != "" {
		t.Fatal("context crossed process directory")
	}
	t.Setenv("HD_CONTEXT_DIR", dir)
	out, err := lookup(prompt)
	if err != nil || !strings.Contains(out, "current generation, verified caller A/session A") {
		t.Fatal("correct handover lost")
	}
	if out, err := lookup(prompt); err == nil || out != "" {
		t.Fatal("consumed context reused")
	}
	if _, err := stageContext(dir, prompt, "next generation"); err != nil {
		t.Fatal(err)
	}
	out, err = lookup(prompt)
	if err != nil || !strings.Contains(out, "next generation") || strings.Contains(out, "caller A") {
		t.Fatal("stale generation reused")
	}
}
