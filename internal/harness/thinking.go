package harness

import (
	"bufio"
	"encoding/json"
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
		level = codexThinking(filepath.Join(configDir(l, "CODEX_HOME", ".codex"), "config.toml"))
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

// configDir is the directory the launch environment names in variable, if
// absolute, else rel under the launch's home.
func configDir(l Launch, variable, rel string) string {
	if dir := envValue(l.Env, variable); filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(l.Home, rel)
}

// codexThinking is the top-level model_reasoning_effort string of a Codex
// config.toml; keys in tables (profiles included) do not count.
func codexThinking(path string) string {
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
		if !ok || strings.Trim(strings.TrimSpace(key), `"'`) != "model_reasoning_effort" {
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
