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

func TestOMPSettings(t *testing.T) {
	const model = "google-antigravity/gemini-3.8-flash"
	for _, tc := range []struct {
		name, text, model, level string
	}{
		{"with thinking", "modelRoles:\n  default: " + model + ":high\n", model, "high"},
		{"high abbreviation", "modelRoles:\n  default: " + model + ":hi\n", model, "high"},
		{"medium abbreviation", "modelRoles:\n  default: " + model + ":med\n", model, "medium"},
		{"xhigh abbreviation", "modelRoles:\n  default: " + model + ":xhi\n", model, "xhigh"},
		{"single letter not thinking", "modelRoles:\n  default: " + model + ":m\n", model + ":m", ""},
		{"auto sentinel", "modelRoles:\n  default: " + model + ":auto\n", model, "auto"},
		{"auto abbreviation not thinking", "modelRoles:\n  default: " + model + ":au\n", model + ":au", ""},
		{"without thinking", "modelRoles:\n  default: " + model + "\n", model, ""},
		{"double quoted", "modelRoles:\n  default: \"" + model + ":high\" # saved\n", model, "high"},
		{"single quoted", "modelRoles: # saved\n    smol: other/model\n    default: '" + model + ":low'\n", model, "low"},
		{"comments and siblings", "# settings\nmodelRoles:\n\n  smol: other/model\n  # default model\n  default: " + model + ":medium # saved\nother: value\n", model, "medium"},
		{"literal model colon", "modelRoles:\n  default: openrouter/model:free\n", "openrouter/model:free", ""},
		{"missing file", "", "", ""},
		{"missing key", "modelRoles:\n  smol: other/model:high\n", "", ""},
		{"wrong parent", "other:\n  default: " + model + ":high\n", "", ""},
		{"nested key", "modelRoles:\n  other:\n    default: " + model + ":high\n", "", ""},
		{"after parent", "modelRoles:\n  smol: other/model\nother:\n  default: " + model + ":high\n", "", ""},
		{"missing quote", "modelRoles:\n  default: \"" + model + ":high\n", "", ""},
		{"trailing junk", "modelRoles:\n  default: '" + model + ":high' junk\n", "", ""},
		{"malformed token", "modelRoles:\n  default: bad model:high\n", "", ""},
		{"escaped newline", "modelRoles:\n  default: \"model\\nnotice:high\"\n", "", ""},
		{"empty model", "modelRoles:\n  default: :high\n", "", ""},
		{"empty level", "modelRoles:\n  default: model:\n", "", ""},
		{"null", "modelRoles:\n  default: null\n", "", ""},
		{"sequence", "modelRoles:\n  default: [model:high]\n", "", ""},
		{"block scalar", "modelRoles:\n  default: |\n    model:high\n", "", ""},
		{"model too long", "modelRoles:\n  default: " + strings.Repeat("m", 65) + ":high\n", "", ""},
		{"model at limit", "modelRoles:\n  default: " + strings.Repeat("m", 64) + ":high\n", strings.Repeat("m", 64), "high"},
		{"level too long", "modelRoles:\n  default: model:" + strings.Repeat("h", 65) + "\n", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			if tc.text != "" {
				dir := filepath.Join(home, ".omp", "agent")
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte(tc.text), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			l := Launch{Home: home}
			if got := Model("omp", l); got != tc.model {
				t.Fatalf("Model = %q, want %q", got, tc.model)
			}
			if got := Thinking("omp", l); got != tc.level {
				t.Fatalf("Thinking = %q, want %q", got, tc.level)
			}
		})
	}
}

func TestOMPAgentDir(t *testing.T) {
	home := t.TempDir()
	pinned := filepath.Join(home, ".omp", "agent")
	other := filepath.Join(t.TempDir(), "agent")
	for _, dir := range []string{pinned, other} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte("modelRoles:\n  default: provider/model:high\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	launcher := filepath.Join(t.TempDir(), "omp-native-cli.sh")
	link := filepath.Join(t.TempDir(), "omp")
	if err := os.Symlink(launcher, link); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, script, home, override, want string
	}{
		{"native default", "", home, "", pinned},
		{"native override", "", home, other, other},
		{"native relative override", "", home, ".omp/agent", pinned},
		{"literal launcher wins", "#!/bin/sh\nunset PI_CONFIG_DIR\nexport PI_CODING_AGENT_DIR=" + pinned + "\nexec /example/runtime/omp \"$@\"\n", home, other, pinned},
		{"launcher cannot select owner for employee", "#!/bin/sh\nunset PI_CONFIG_DIR\nexport PI_CODING_AGENT_DIR=" + pinned + "\nexec /example/runtime/omp \"$@\"\n", other, "", filepath.Join(other, ".omp", "agent")},
		{"no shell evaluation", "#!/bin/sh\nunset PI_CONFIG_DIR\nexport PI_CODING_AGENT_DIR=$(echo " + pinned + ")\nexec /example/runtime/omp \"$@\"\n", home, other, other},
		{"later override not recognized", "#!/bin/sh\nunset PI_CONFIG_DIR\nexport PI_CODING_AGENT_DIR=" + pinned + "\nexport PI_CODING_AGENT_DIR=/other\nexec /example/runtime/omp \"$@\"\n", home, other, other},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(launcher, []byte(tc.script), 0o700); err != nil {
				t.Fatal(err)
			}
			l := Launch{Path: link, Home: tc.home, Env: []string{"PI_CODING_AGENT_DIR=" + tc.override}}
			if got := ompAgentDir(l); got != tc.want {
				t.Fatalf("directory = %q, want %q", got, tc.want)
			}
			want := "provider/model"
			if tc.home != home {
				want = ""
			}
			if got := Model("omp", l); got != want {
				t.Fatalf("Model = %q, want %q", got, want)
			}
		})
	}
}

func TestOMPConfigRootAndRelativeAgentDir(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		env  []string
		want string
	}{
		{[]string{"PI_CONFIG_DIR=.other"}, filepath.Join(home, ".other", "agent")},
		{[]string{"PI_CONFIG_DIR=.other", "PI_CODING_AGENT_DIR=relative"}, filepath.Join(cwd, "relative")},
	} {
		l := Launch{Home: home, Dir: cwd, Env: tc.env}
		if got := ompAgentDir(l); got != tc.want {
			t.Fatalf("directory = %q, want %q", got, tc.want)
		}
	}
}

func TestClaudeSessionModel(t *testing.T) {
	home := t.TempDir()
	id := "0f1e2d3c-aaaa-bbbb-cccc-123456789abc"
	dir := filepath.Join(home, ".claude", "projects", "-root")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	lines := `{"type":"user","message":{"role":"user"}}
{"type":"assistant","message":{"model":"claude-sonnet-5-5"}}
{"type":"assistant","message":{"model":"claude-opus-5-5"}}
{"type":"assistant","message":{"model":"<synthetic>"}}
`
	if err := os.WriteFile(filepath.Join(dir, id+".jsonl"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	l := Launch{Home: home}
	if got := ClaudeSessionModel(l, id); got != "claude-opus-5-5" {
		t.Fatalf("got %q", got)
	}
	for _, bad := range []string{"", "*", "../x", "0f1e2d3c-aaaa-bbbb-cccc-000000000000"} {
		if got := ClaudeSessionModel(l, bad); got != "" {
			t.Fatalf("id %q gave %q", bad, got)
		}
	}
}
