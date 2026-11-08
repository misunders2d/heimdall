package harness

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
)

//go:embed native-context.ts
var piContextExtension string
var contextSequence atomic.Uint64

type inputContext struct {
	Prompt  string `json:"prompt"`
	Context string `json:"context"`
}

// Files are disposable process-local handovers, never a second transcript.
// Immutable files let a hook match repeated prompts without overwriting a steer.
func stageContext(dir, prompt, context string) (string, error) {
	if dir == "" || context == "" {
		return "", nil
	}
	sum := sha256.Sum256([]byte(prompt))
	data, _ := json.Marshal(inputContext{prompt, context})
	f, err := os.CreateTemp(dir, fmt.Sprintf("%s-%020d-*.pending", hex.EncodeToString(sum[:]), contextSequence.Add(1)))
	if err != nil {
		return "", err
	}
	path := f.Name()
	if _, err = f.Write(data); err == nil {
		err = f.Close()
	} else {
		f.Close()
	}
	if err != nil {
		os.Remove(path)
		return "", err
	}
	if st, e := os.Stat(dir); e == nil {
		if owner, ok := st.Sys().(*syscall.Stat_t); ok && os.Getuid() == 0 {
			if err := os.Chown(path, int(owner.Uid), int(owner.Gid)); err != nil {
				os.Remove(path)
				return "", err
			}
		}
	}
	final := strings.TrimSuffix(path, ".pending") + ".json"
	if err := os.Rename(path, final); err != nil {
		os.Remove(path)
		return "", err
	}
	return final, nil
}

// ContextHook is a Claude UserPromptSubmit hook. It returns only additionalContext;
// native prompt and native user transcript remain untouched.
func ContextHook(in io.Reader, out io.Writer) error {
	var event struct {
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(io.LimitReader(in, 16<<20)).Decode(&event); err != nil {
		return err
	}
	dir := os.Getenv("HD_CONTEXT_DIR")
	if dir == "" {
		return errors.New("native context directory missing")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(event.Prompt))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), hex.EncodeToString(sum[:])+"-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var c inputContext
		if err := json.Unmarshal(data, &c); err != nil {
			return err
		}
		if c.Prompt != event.Prompt {
			continue
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]any{"hookSpecificOutput": map[string]any{"hookEventName": "UserPromptSubmit", "additionalContext": c.Context}})
	}
	return errors.New("native input context unavailable")
}

func prepareContext(s Spec, args []string) (string, []string, error) {
	if s.Harness != "pi" && s.Harness != "claude" {
		return "", args, nil
	}
	if s.Context == "" && s.Instructions == "" && s.WarmContext == nil {
		return "", args, nil
	}
	parent := ""
	if s.Caller.Role == "employee" {
		parent = s.Launch.Dir
	}
	dir, err := os.MkdirTemp(parent, "heimdall-context-")
	if err != nil {
		return "", nil, err
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(dir)
		}
	}()
	if _, err := stageContext(dir, s.Prompt, s.Context); err != nil {
		return "", nil, err
	}
	if s.Instructions != "" {
		args = append(args, "--append-system-prompt", s.Instructions)
	}
	if s.Harness == "pi" {
		path := filepath.Join(dir, "context.ts")
		if err := os.WriteFile(path, []byte(piContextExtension), 0600); err != nil {
			return "", nil, err
		}
		args = append(args, "--extension", path)
	} else {
		exe, err := os.Executable()
		if err != nil {
			return "", nil, err
		}
		command := "'" + strings.ReplaceAll(exe, "'", "'\\''") + "' hook input-context"
		// Merge all CLI settings so employee and owner Slack guards stay installed.
		settings := map[string]any{}
		filtered := []string{}
		for i := 0; i < len(args); i++ {
			if args[i] == "--settings" && i+1 < len(args) {
				var part map[string]any
				if err := json.Unmarshal([]byte(args[i+1]), &part); err != nil {
					return "", nil, err
				}
				for k, v := range part {
					if k == "hooks" {
						hooks, _ := settings[k].(map[string]any)
						if hooks == nil {
							hooks = map[string]any{}
						}
						for event, groups := range v.(map[string]any) {
							old, _ := hooks[event].([]any)
							hooks[event] = append(old, groups.([]any)...)
						}
						settings[k] = hooks
					} else {
						settings[k] = v
					}
				}
				i++
			} else {
				filtered = append(filtered, args[i])
			}
		}
		hooks, _ := settings["hooks"].(map[string]any)
		if hooks == nil {
			hooks = map[string]any{}
		}
		old, _ := hooks["UserPromptSubmit"].([]any)
		hooks["UserPromptSubmit"] = append(old, map[string]any{"hooks": []any{map[string]any{"type": "command", "command": command, "onFailure": "block"}}})
		settings["hooks"] = hooks
		data, _ := json.Marshal(settings)
		args = append(filtered, "--settings", string(data))
	}
	// A confined native process must be able to read only its own handover files.
	if s.Launch.Cred != nil {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return "", nil, err
		}
		for _, e := range entries {
			if err := os.Chown(filepath.Join(dir, e.Name()), int(s.Launch.Cred.Uid), int(s.Launch.Cred.Gid)); err != nil {
				return "", nil, err
			}
		}
		if err := os.Chown(dir, int(s.Launch.Cred.Uid), int(s.Launch.Cred.Gid)); err != nil {
			return "", nil, err
		}
	}
	ok = true
	return dir, args, nil
}
