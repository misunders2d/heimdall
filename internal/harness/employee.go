package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

const SharedRoot = "/opt/assistant"

// Employee tools are an allowlist. Ambient native MCP configuration is never trusted.
// Business MCP remains empty until a connector can enforce per-caller credentials.
func employeeArgs(name string, l Launch) ([]string, error) {
	switch name {
	case "agy":
		return nil, errors.New("employee " + name + " turns are unavailable: native isolation has not been validated")
	case "pi", "omp":
		return []string{"--tools", "read", "--exclude-tools", "bash,powershell,edit,write,grep,find,ls,mcp__*", "--no-approve", "--mcp-config", SharedRoot + "/heimdall/empty-mcp.json", "--extension", SharedRoot + "/heimdall/employee-guard.ts"}, nil
	case "claude":
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		command := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "' hook employee-guard"
		deny := []string{"Read(/root/**)"}
		for _, d := range []string{".pi", ".codex", ".claude", ".gemini", ".local/state/heimdall/sessions"} {
			deny = append(deny, "Read("+filepath.Join(l.Home, d)+"/**)")
		}
		settings, _ := json.Marshal(map[string]any{"disableSkillShellExecution": true, "autoMemoryEnabled": false, "permissions": map[string]any{"deny": deny}, "hooks": map[string]any{"PreToolUse": []any{map[string]any{"matcher": ".*", "hooks": []any{map[string]any{"type": "command", "command": command, "onFailure": "block"}}}}}})
		return []string{"--permission-mode", "dontAsk", "--tools", "Skill,Read,WebSearch", "--add-dir", SharedRoot, "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--disallowedTools", "Bash,PowerShell,Edit,Write,Glob,Grep,NotebookEdit,Agent,Task,mcp__*", "--settings", string(settings)}, nil
	case "codex":
		args := []string{"-c", `features.shell_tool=true`, "-c", `approval_policy="never"`, "-c", `default_permissions="employee"`, "-c", `permissions.employee.network.enabled=false`}
		// Exact subtrees, not unbounded globs: Linux deny-read glob expansion is bounded.
		for _, p := range []string{l.Home, "/root", "/proc", "/sys"} {
			args = append(args, "-c", "permissions.employee.filesystem."+strconv.Quote(p)+`="deny"`)
		}
		for _, p := range []string{SharedRoot, l.Dir, "/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache", "/dev/null"} {
			args = append(args, "-c", "permissions.employee.filesystem."+strconv.Quote(p)+`="read"`)
		}
		for _, d := range []string{".codex", ".claude", ".pi", ".gemini"} {
			args = append(args, "-c", "permissions.employee.filesystem."+strconv.Quote(filepath.Join(l.Home, d))+`="deny"`)
		}
		for _, feature := range []string{"multi_agent", "apps", "plugins", "hooks", "memories", "computer_use", "browser_use", "skill_mcp_dependency_install"} {
			args = append(args, "--disable", feature)
		}
		// Empty the default map; native enumeration below also disables merged ambient entries.
		args = append(args, "-c", "mcp_servers={}")
		return args, nil
	}
	return nil, errors.New("employee harness unsupported")
}

func employeeEnv(env []string, c Caller) []string {
	if c.Role != core.RoleEmployee {
		return env
	}
	for k, v := range map[string]string{"MEMENTO_PROCESSING": "1", "PI_RESTRICTED_EXTERNAL_USER": "1", "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1", "CLAUDE_CODE_SKIP_MODEL_ACCESS_MEMORY": "1"} {
		env = setEnv(env, k, v)
	}
	caps, _ := json.Marshal(c.Capabilities)
	return setEnv(env, "HEIMDALL_CALLER_CAPABILITIES", string(caps))
}

// ReadWithin resolves symlinks before accepting a native reader's path.
func ReadWithin(path string, roots ...string) bool {
	if !filepath.IsAbs(path) {
		cwd, _ := os.Getwd()
		path = filepath.Join(cwd, path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	for _, root := range roots {
		if root == "" {
			continue
		}
		real, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(real, resolved)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// EmployeeGuard is Claude's fail-closed reader/tool hook, not a memory hook.
func EmployeeGuard(r io.Reader, w io.Writer) error {
	var in struct {
		Tool  string `json:"tool_name"`
		Input struct {
			Path string `json:"file_path"`
		} `json:"tool_input"`
	}
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	valid := dec.Decode(&in) == nil && dec.Decode(new(any)) == io.EOF
	allow := valid && (in.Tool == "WebSearch" || in.Tool == "Skill" || in.Tool == "Read" && ReadWithin(in.Input.Path, SharedRoot, os.Getenv("HEIMDALL_TURN_DIR")))
	decision := "deny"
	if allow {
		decision = "allow"
	}
	return json.NewEncoder(w).Encode(map[string]any{"hookSpecificOutput": map[string]any{"hookEventName": "PreToolUse", "permissionDecision": decision, "permissionDecisionReason": "Employee tools and file roots are confined"}})
}

// Inspect the native MCP catalog as the employee OS user, before any model runs.
// Config table merge semantics must not leave an ambient/renamed server enabled.
func employeeCodexMCPArgs(ctx context.Context, l Launch) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, l.Path, "mcp", "list", "--json")
	cmd.Dir, cmd.Env = l.Dir, l.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: l.Cred}
	raw, err := cmd.Output()
	if err != nil {
		return nil, errors.New("cannot inspect employee Codex MCP catalog; launch refused")
	}
	var servers []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &servers) != nil {
		return nil, errors.New("invalid employee Codex MCP catalog; launch refused")
	}
	names := []string{"t3", "memento", "slack", "workspace", "workspace-mcp"}
	for _, s := range servers {
		if s.Name == "" {
			return nil, errors.New("invalid MCP server name")
		}
		names = append(names, s.Name)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	var args []string
	for _, name := range names {
		// Missing servers need a valid dummy transport; disabled entries never connect.
		if !slices.ContainsFunc(servers, func(s struct {
			Name string `json:"name"`
		}) bool {
			return s.Name == name
		}) {
			args = append(args, "-c", "mcp_servers."+strconv.Quote(name)+`={enabled=false,url="https://disabled.invalid/mcp"}`)
		} else {
			args = append(args, "-c", "mcp_servers."+strconv.Quote(name)+".enabled=false")
		}
	}
	return args, nil
}
