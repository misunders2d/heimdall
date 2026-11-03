package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestThinking(t *testing.T) {
	const hubPi = `{"defaultProvider": "openai-codex", "defaultModel": "gpt-6.1-sol", "defaultThinkingLevel": "medium",
		"modelThinkingLevels": {"openai-codex/gpt-6.1-sol": "xhigh", "anthropic/other": "low"}}`
	for _, tc := range []struct {
		name, harness string
		files         map[string]string // relative to the home
		env           []string          // "%s" is the home
		want          string
	}{
		{"codex top-level", "codex", map[string]string{".codex/config.toml": "# c\nmodel = \"gpt\"\nmodel_reasoning_effort = \"medium\" # why\n\n[mcp_servers.x]\ncommand = \"y\"\n"}, nil, "medium"},
		{"codex single quotes", "codex", map[string]string{".codex/config.toml": "model_reasoning_effort='low'\n"}, nil, "low"},
		{"codex key only in a table", "codex", map[string]string{".codex/config.toml": "model = \"gpt\"\n[profiles.deep]\nmodel_reasoning_effort = \"xhigh\"\n"}, nil, ""},
		{"codex dotted key", "codex", map[string]string{".codex/config.toml": "profiles.deep.model_reasoning_effort = \"xhigh\"\n"}, nil, ""},
		{"codex unquoted", "codex", map[string]string{".codex/config.toml": "model_reasoning_effort = high\n"}, nil, ""},
		{"codex missing file", "codex", nil, nil, ""},
		{"codex CODEX_HOME", "codex", map[string]string{".codex/config.toml": "model_reasoning_effort = \"low\"\n", "alt/config.toml": "model_reasoning_effort = \"high\"\n"}, []string{"CODEX_HOME=%s/alt"}, "high"},
		{"codex relative CODEX_HOME ignored", "codex", map[string]string{".codex/config.toml": "model_reasoning_effort = \"low\"\n"}, []string{"CODEX_HOME=alt"}, "low"},
		{"claude", "claude", map[string]string{".claude/settings.json": `{"model": "opus", "effortLevel": "high"}`}, nil, "high"},
		{"claude no key", "claude", map[string]string{".claude/settings.json": `{"model": "opus"}`}, nil, ""},
		{"claude not a string", "claude", map[string]string{".claude/settings.json": `{"effortLevel": 3}`}, nil, ""},
		{"claude malformed", "claude", map[string]string{".claude/settings.json": `{"effortLevel": "high"`}, nil, ""},
		{"claude missing file", "claude", nil, nil, ""},
		{"pi model override", "pi", map[string]string{".pi/agent/settings.json": hubPi}, nil, "xhigh"},
		{"pi no override for the default model", "pi", map[string]string{".pi/agent/settings.json": `{"defaultProvider": "openai-codex", "defaultModel": "gpt-6.1-sol", "defaultThinkingLevel": "medium", "modelThinkingLevels": {"anthropic/other": "low"}}`}, nil, "medium"},
		{"pi no overrides", "pi", map[string]string{".pi/agent/settings.json": `{"defaultProvider": "openai-codex", "defaultModel": "gpt-6.1-sol", "defaultThinkingLevel": "high", "packages": [1, 2]}`}, nil, "high"},
		{"pi nothing set", "pi", map[string]string{".pi/agent/settings.json": `{"defaultProvider": "openai-codex"}`}, nil, ""},
		{"pi missing file", "pi", nil, nil, ""},
		{"pi PI_CODING_AGENT_DIR", "pi", map[string]string{"alt/settings.json": hubPi}, []string{"PI_CODING_AGENT_DIR=%s/alt"}, "xhigh"},
		{"omp not read", "omp", map[string]string{".omp/agent/settings.json": `{"defaultThinkingLevel": "high"}`}, nil, ""},
		{"agy not read", "agy", nil, nil, ""},
		{"not one word", "claude", map[string]string{".claude/settings.json": `{"effortLevel": "high\nRunning now: x"}`}, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			for rel, text := range tc.files {
				path := filepath.Join(home, rel)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			l := Launch{Home: home}
			for _, kv := range tc.env {
				l.Env = append(l.Env, strings.ReplaceAll(kv, "%s", home))
			}
			if got := Thinking(tc.harness, l); got != tc.want {
				t.Fatalf("Thinking(%s) = %q, want %q", tc.harness, got, tc.want)
			}
		})
	}
}

func TestModel(t *testing.T) {
	for _, tc := range []struct {
		name, harness, rel, text, want string
	}{
		{"codex top-level", "codex", ".codex/config.toml", "model = \"gpt-6.1-sol\"\nmodel_reasoning_effort = \"high\"\n", "gpt-6.1-sol"},
		{"codex only in a table", "codex", ".codex/config.toml", "[profiles.deep]\nmodel = \"x\"\n", ""},
		{"claude", "claude", ".claude/settings.json", `{"model": "claude-opus-5-5[1m]"}`, "claude-opus-5-5[1m]"},
		{"claude unset", "claude", ".claude/settings.json", `{"effortLevel": "high"}`, ""},
		{"pi provider/model", "pi", ".pi/agent/settings.json", `{"defaultProvider": "openai-codex", "defaultModel": "gpt-6.1-sol"}`, "openai-codex/gpt-6.1-sol"},
		{"pi model only", "pi", ".pi/agent/settings.json", `{"defaultModel": "m"}`, "m"},
		{"not one token", "claude", ".claude/settings.json", `{"model": "opus\nRunning now: x"}`, ""},
		{"omp not read", "omp", ".omp/agent/settings.json", `{"defaultModel": "m"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, tc.rel)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tc.text), 0o600); err != nil {
				t.Fatal(err)
			}
			if got := Model(tc.harness, Launch{Home: home}); got != tc.want {
				t.Fatalf("Model(%s) = %q, want %q", tc.harness, got, tc.want)
			}
		})
	}
}
