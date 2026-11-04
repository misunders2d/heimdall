package harness

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Thinking is the thinking level a harness is configured with under the
// launch's home, or "" when it is not known: no such file or key, an
// unreadable file, or a harness whose level is not read (omp, agy). Agents
// change their own level by editing these files; Heimdall only reads them.
func Thinking(name string, l Launch) string {
	var level string
	switch name {
	case "codex":
		level = codexKey(filepath.Join(configDir(l, "CODEX_HOME", ".codex"), "config.toml"), "model_reasoning_effort")
	case "claude":
		level = jsonString(readObject(filepath.Join(l.Home, ".claude", "settings.json")), "effortLevel")
	case "pi":
		level = piThinking(filepath.Join(configDir(l, "PI_CODING_AGENT_DIR", filepath.Join(".pi", "agent")), "settings.json"))
	}
	if len(level) > 32 || strings.TrimLeft(level, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		return "" // shown in chat as is: one plain word only
	}
	return level
}

// Model is the model a harness is configured with under the launch's home,
// or "" when not set there (the harness then uses its own default): Codex
// config.toml top-level `model`, Claude settings.json `model`, Pi
// settings.json "<defaultProvider>/<defaultModel>". Like Thinking, it only
// reads the files the agents themselves edit.
func Model(name string, l Launch) string {
	var model string
	switch name {
	case "codex":
		model = codexKey(filepath.Join(configDir(l, "CODEX_HOME", ".codex"), "config.toml"), "model")
	case "claude":
		model = jsonString(readObject(filepath.Join(l.Home, ".claude", "settings.json")), "model")
	case "pi":
		cfg := readObject(filepath.Join(configDir(l, "PI_CODING_AGENT_DIR", filepath.Join(".pi", "agent")), "settings.json"))
		if m := jsonString(cfg, "defaultModel"); m != "" {
			model = m
			if p := jsonString(cfg, "defaultProvider"); p != "" {
				model = p + "/" + m
			}
		}
	}
	if len(model) > 64 || strings.TrimLeft(model, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:/[]") != "" {
		return "" // shown in chat as is: one plain token only
	}
	return model
}

// ClaudeSessionModel is the model of the last assistant message in Claude
// Code's transcript for session id under the launch's home, or "". Claude
// writes ~/.claude/projects/<cwd>/<id>.jsonl; only the tail is read.
func ClaudeSessionModel(l Launch, id string) string {
	path := SessionPath("claude", l, id)
	if path == "" {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	const tail = 256 << 10
	if st, err := f.Stat(); err == nil && st.Size() > tail {
		_, _ = f.Seek(st.Size()-tail, 0)
	}
	data, _ := io.ReadAll(io.LimitReader(f, tail))
	model := ""
	for _, line := range strings.Split(string(data), "\n") {
		var entry struct {
			Type    string `json:"type"`
			Message struct {
				Model string `json:"model"`
			} `json:"message"`
		}
		if json.Unmarshal([]byte(line), &entry) == nil && entry.Type == "assistant" && entry.Message.Model != "" && !strings.HasPrefix(entry.Message.Model, "<") {
			model = entry.Message.Model
		}
	}
	if len(model) > 64 || strings.TrimLeft(model, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:/[]") != "" {
		return ""
	}
	return model
}

// configDir is the directory the launch environment names in variable, if
// absolute, else rel under the launch's home.
func configDir(l Launch, variable, rel string) string {
	if dir := envValue(l.Env, variable); filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(l.Home, rel)
}

// codexKey is a top-level string key of a Codex config.toml; keys in tables
// (profiles included) do not count.
func codexKey(path, want string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if strings.HasPrefix(line, "[") {
			return ""
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || strings.Trim(strings.TrimSpace(key), `"'`) != want {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) < 2 || value[0] != '"' && value[0] != '\'' {
			return ""
		}
		end := strings.IndexByte(value[1:], value[0])
		if end < 0 {
			return ""
		}
		return value[1 : 1+end]
	}
	return ""
}

// piThinking is Pi's level for its default model: modelThinkingLevels
// ["<defaultProvider>/<defaultModel>"] when set, else defaultThinkingLevel.
func piThinking(path string) string {
	cfg := readObject(path)
	var levels map[string]json.RawMessage
	_ = json.Unmarshal(cfg["modelThinkingLevels"], &levels)
	if level := jsonString(levels, jsonString(cfg, "defaultProvider")+"/"+jsonString(cfg, "defaultModel")); level != "" {
		return level
	}
	return jsonString(cfg, "defaultThinkingLevel")
}

// readObject is a JSON file's top-level object, or nil.
func readObject(path string) map[string]json.RawMessage {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var cfg map[string]json.RawMessage
	if json.Unmarshal(data, &cfg) != nil {
		return nil
	}
	return cfg
}

// jsonString is obj[key] when it is a JSON string, else "".
func jsonString(obj map[string]json.RawMessage, key string) string {
	var s string
	if json.Unmarshal(obj[key], &s) != nil {
		return ""
	}
	return s
}
