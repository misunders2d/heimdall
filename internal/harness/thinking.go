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
// unreadable file, or a harness whose level is not read (agy). Agents
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
	case "omp":
		_, level = ompDefault(l)
	}
	if len(level) > 32 || strings.TrimLeft(level, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		return "" // shown in chat as is: one plain word only
	}
	return level
}

// Model is the model a harness is configured with under the launch's home,
// or "" when not set there (the harness then uses its own default): Codex
// config.toml top-level `model`, Claude settings.json `model`, Pi
// settings.json "<defaultProvider>/<defaultModel>", OMP config.yml
// modelRoles.default (without its thinking suffix). Like Thinking, it only
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
	case "omp":
		model, _ = ompDefault(l)
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

// ompDefault reads only the simple block mapping OMP writes for modelRoles.
// Directory and selector syntax checked against oh-my-pi v18.16.0:
// packages/utils/src/dirs.ts and packages/tui/src/overlays/model-selector.ts.
// More complex YAML/selectors are unknown; never render them as chat text.
func ompDefault(l Launch) (model, level string) {
	value := ompDefaultValue(filepath.Join(ompAgentDir(l), "config.yml"))
	if len(value) > 97 || strings.HasPrefix(value, "[") || strings.TrimLeft(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:/[]") != "" {
		return "", ""
	}
	if i := strings.LastIndexByte(value, ':'); i >= 0 {
		if i == 0 || i == len(value)-1 {
			return "", ""
		}
		suffix := value[i+1:]
		// OMP accepts unambiguous abbreviations of at least two letters;
		// all these concrete names have distinct two-letter prefixes. Auto
		// is a separate sentinel and only its full spelling is recognized.
		for _, candidate := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max", "inherit", "auto"} {
			if suffix == candidate || len(suffix) >= 2 && candidate != "auto" && strings.HasPrefix(candidate, suffix) {
				if i > 64 {
					return "", ""
				}
				return value[:i], candidate
			}
		}
	}
	return value, ""
}

// The canonical hub OMP launcher pins its own agent directory after removing
// Pi's inherited config root. Recognize only its final three literal lines;
// never evaluate shell text or accept a pinned path outside this caller's home.
// Native installs keep the usual launch environment/default resolution.
func ompAgentDir(l Launch) string {
	rel := filepath.Join(".omp", "agent")
	if root := envValue(l.Env, "PI_CONFIG_DIR"); root != "" {
		rel = filepath.Join(root, "agent")
	}
	dir := configDir(l, "PI_CODING_AGENT_DIR", rel)
	if override := envValue(l.Env, "PI_CODING_AGENT_DIR"); override != "" && !filepath.IsAbs(override) {
		cwd := l.Dir
		if cwd == "" {
			cwd = l.Home
		}
		dir = filepath.Join(cwd, override) // OMP uses path.resolve, unlike Pi's reader.
	}
	path, err := filepath.EvalSymlinks(l.Path)
	if err != nil || filepath.Base(path) != "omp-native-cli.sh" {
		return dir
	}
	f, err := os.Open(path)
	if err != nil {
		return dir
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil || len(data) > 8192 {
		return dir
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 3 {
		return dir
	}
	tail := lines[len(lines)-3:]
	pinned := filepath.Join(l.Home, ".omp", "agent")
	target := strings.TrimSuffix(strings.TrimPrefix(tail[2], "exec "), ` "$@"`)
	if tail[0] == "unset PI_CONFIG_DIR" && tail[1] == "export PI_CODING_AGENT_DIR="+pinned &&
		tail[2] == "exec "+target+` "$@"` && filepath.IsAbs(target) && filepath.Base(target) == "omp" &&
		strings.TrimLeft(target, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-/.") == "" {
		return pinned
	}
	return dir
}

func ompDefaultValue(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	s := bufio.NewScanner(io.LimitReader(f, 1<<20))
	inRoles, childIndent := false, 0
	for s.Scan() {
		line := s.Text()
		text := strings.TrimSpace(line)
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if strings.HasPrefix(line[indent:], "\t") {
			return "" // tabs are not YAML indentation
		}
		key, value, ok := strings.Cut(text, ":")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if indent == 0 {
			inRoles = ok && key == "modelRoles" && (value == "" || strings.HasPrefix(value, "#"))
			childIndent = 0
			continue
		}
		if !inRoles {
			continue
		}
		if childIndent == 0 {
			childIndent = indent
		}
		if indent != childIndent || !ok || key != "default" {
			continue
		}
		if strings.HasPrefix(value, "\"") || strings.HasPrefix(value, "'") {
			end := strings.IndexByte(value[1:], value[0]) + 1
			if end == 0 {
				return ""
			}
			rest := value[end+1:]
			if rest != "" && (!strings.ContainsAny(rest[:1], " \t") || !strings.HasPrefix(strings.TrimSpace(rest), "#") && strings.TrimSpace(rest) != "") {
				return ""
			}
			return value[1:end]
		}
		if i := strings.Index(value, " #"); i >= 0 {
			value = value[:i]
		}
		if i := strings.Index(value, "\t#"); i >= 0 {
			value = value[:i]
		}
		value = strings.TrimSpace(value)
		switch value {
		case "null", "Null", "NULL", "true", "True", "TRUE", "false", "False", "FALSE", "[]":
			return ""
		}
		return value
	}
	return ""
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
