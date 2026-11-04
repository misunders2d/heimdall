package harness

import (
	"os"
	"path/filepath"
	"strings"
)

// SessionPath locates native transcripts without reading or copying them.
// Pi/OMP use sessionArgs' directory; Claude matches ClaudeSessionModel;
// Codex app-server threads use rollout files under CODEX_HOME/sessions.
func SessionPath(name string, l Launch, id string) string {
	if id == "" || len(id) > 64 || strings.TrimLeft(id, "abcdefABCDEF0123456789-") != "" {
		return ""
	}
	var pattern string
	switch name {
	case "claude":
		pattern = filepath.Join(l.Home, ".claude", "projects", "*", id+".jsonl")
	case "codex":
		pattern = filepath.Join(configDir(l, "CODEX_HOME", ".codex"), "sessions", "*", "*", "*", "*"+id+"*.jsonl")
	case "pi", "omp":
		pattern = filepath.Join(l.Home, ".local", "state", "heimdall", "sessions", name, id, "*.jsonl")
	default:
		return ""
	}
	paths, _ := filepath.Glob(pattern)
	// A Pi/OMP session directory can hold several native transcript files.
	if (name == "pi" || name == "omp") && len(paths) > 0 {
		return filepath.Dir(paths[0])
	}
	if len(paths) != 1 {
		return ""
	}
	if st, err := os.Stat(paths[0]); err != nil || !st.Mode().IsRegular() {
		return ""
	}
	return paths[0]
}
