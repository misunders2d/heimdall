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
	"github.com/misunders2d/heimdall/internal/mcpoauth"
)

const SharedRoot = "/opt/assistant"

// EmployeeCapable reports the engines that have an employee launch boundary.
// Listing one in employee_harnesses stays the owner's per-engine decision,
// made after its hub probe (deploy/contabo/employee-probe.sh).
func EmployeeCapable(name string) bool {
	return slices.Contains([]string{"omp", "codex", "claude", "pi"}, name)
}

// employeeServer is one approved business MCP server with the caller's own
// access-only token, handed to the engine's native MCP configuration.
type employeeServer struct{ Name, URL, Token string }

func serverNames(servers []employeeServer) []string {
	names := make([]string, 0, len(servers))
	for _, s := range servers {
		names = append(names, s.Name)
	}
	return names
}

// Owner-only MCP namespaces: never reachable from an employee turn, even if
// an ambient native configuration names them.
var ownerMCP = []string{"t3", "memento", "slack", "workspace", "workspace-mcp", "workspace_cli"}

// Employee tools are an allowlist. Ambient native MCP configuration is never trusted.
// Business MCP is limited to the caller's approved servers (Mellanni, and Linear
// with mcp.linear), each carrying the caller's own access-only token. Pi keeps its
// native caller-scoped store; the other engines get the root broker's snapshot.
// lease is the turn's disposable native config directory (empty for Pi).
func employeeArgs(name string, l Launch, servers []employeeServer, lease string) ([]string, error) {
	switch name {
	case "agy":
		return nil, errors.New("employee " + name + " turns are unavailable: native isolation has not been validated")
	case "pi":
		return []string{"--no-extensions", "--extension", "builtin:mcp", "--no-builtin-tools", "--tools", "read,google_*,employee_mcp_status,mcp__mellanni__*,mcp__linear__*,list_mcp_resources,list_mcp_resource_templates,read_mcp_resource", "--no-prompt-templates", "--no-themes"}, nil
	case "omp":
		exclude := "bash,powershell,edit,write,grep,find,ls"
		for _, o := range ownerMCP {
			exclude += ",mcp__" + o + "_*"
		}
		args := []string{"--no-extensions"}
		if len(servers) == 0 {
			// Nothing approved and connected: no MCP at all.
			args = append(args, "--no-mcp")
			exclude += ",mcp__*"
		}
		return append(args, "--extension", SharedRoot+"/pi/extensions/privileged-tool-guard.ts", "--extension", SharedRoot+"/pi/extensions/admin-lifecycle-guard.ts", "--tools", "read", "--exclude-tools", exclude, "--no-approve", "--extension", SharedRoot+"/heimdall/employee-guard.ts"), nil
	case "claude":
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		command := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "' hook employee-guard"
		deny := []string{"Read(//root/**)"}
		for _, d := range []string{".pi", ".codex", ".claude", ".gemini", ".local/state/heimdall/sessions"} {
			deny = append(deny, "Read(/"+filepath.Join(l.Home, d)+"/**)")
		}
		settings, _ := json.Marshal(map[string]any{"disableSkillShellExecution": true, "autoMemoryEnabled": false, "permissions": map[string]any{"deny": deny}, "hooks": map[string]any{"PreToolUse": []any{map[string]any{"matcher": ".*", "hooks": []any{map[string]any{"type": "command", "command": command, "onFailure": "block"}}}}}})
		disallowed := "Bash,PowerShell,Edit,Write,Glob,Grep,NotebookEdit,Agent,Task"
		for _, o := range ownerMCP {
			disallowed += ",mcp__" + o
		}
		mcpConfig := `{"mcpServers":{}}`
		if len(servers) == 0 {
			disallowed += ",mcp__*"
		} else {
			if lease == "" {
				return nil, errors.New("employee Claude MCP needs its disposable config directory")
			}
			// A file, never argv: /proc/PID/cmdline is world-readable.
			mcpConfig = filepath.Join(lease, "mcp.json")
		}
		return []string{"--restricted", "--setting-sources", "", "--permission-mode", "dontAsk", "--tools", "Skill,Read,WebSearch", "--add-dir", SharedRoot, "--strict-mcp-config", "--mcp-config", mcpConfig, "--disallowedTools", disallowed, "--settings", string(settings)}, nil
	case "codex":
		args := []string{"--strict-config", "-c", `features.shell_tool=true`, "-c", `approval_policy="never"`, "-c", `default_permissions="employee"`, "-c", `cli_auth_credentials_store="file"`}
		// One TOML table: quoted dotted CLI path keys are treated literally by
		// Codex 0.166.0 and rejected as non-absolute paths.
		fs := map[string]string{}
		for _, p := range []string{l.Home, "/root", "/proc", "/sys"} {
			fs[p] = "deny"
		}
		for _, p := range []string{SharedRoot, l.Dir, "/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache", "/dev/null"} {
			fs[p] = "read"
		}
		for _, d := range []string{".codex", ".claude", ".pi", ".gemini"} {
			fs[filepath.Join(l.Home, d)] = "deny"
		}
		if lease != "" {
			fs[lease] = "deny"
		}
		keys := make([]string, 0, len(fs))
		for p := range fs {
			keys = append(keys, p)
		}
		slices.Sort(keys)
		entries := make([]string, 0, len(keys))
		for _, p := range keys {
			entries = append(entries, strconv.Quote(p)+"="+strconv.Quote(fs[p]))
		}
		args = append(args, "-c", "permissions.employee={network={enabled=false},filesystem={"+strings.Join(entries, ",")+"}}")

		// apps/plugins are ChatGPT connectors that ride the owner's account token.
		for _, feature := range []string{"multi_agent", "apps", "plugins", "remote_plugin", "plugin_sharing", "tool_suggest", "hooks", "memories", "computer_use", "browser_use", "browser_use_external", "image_generation", "skill_mcp_dependency_install"} {
			args = append(args, "--disable", feature)
		}
		// The single mcp_servers table comes from employeeCodexMCPArgs.
		return args, nil
	}
	return nil, errors.New("employee harness unsupported")
}

func employeeEnv(env []string, c Caller, servers []employeeServer) []string {
	if c.Role != core.RoleEmployee {
		return env
	}
	// Even synthetic/custom Launch envs cannot leak a Google client or old store.
	clean := env[:0:0]
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "GOOGLE_") || strings.HasPrefix(k, "PI_GOOGLE_") || strings.HasPrefix(k, "HEIMDALL_GOOGLE_") || strings.HasPrefix(k, "INFISICAL_") {
			continue
		}
		clean = append(clean, kv)
	}
	env = clean
	for k, v := range map[string]string{"MEMENTO_PROCESSING": "1", "PI_RESTRICTED_EXTERNAL_USER": "1", "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1", "CLAUDE_CODE_SKIP_MODEL_ACCESS_MEMORY": "1"} {
		env = setEnv(env, k, v)
	}
	// claude.ai connectors ride the owner's subscription login: never load them.
	env = setEnv(env, "ENABLE_CLAUDEAI_MCP_SERVERS", "false")
	names, _ := json.Marshal(serverNames(servers))
	// Read by the employee guards; set by Heimdall, outside model control.
	env = setEnv(env, "HEIMDALL_EMPLOYEE_MCP", string(names))
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
	allow := valid && (in.Tool == "WebSearch" || in.Tool == "Skill" || in.Tool == "Read" && ReadWithin(in.Input.Path, SharedRoot, os.Getenv("HEIMDALL_TURN_DIR")) || approvedMCPTool(in.Tool, "__"))
	decision := "deny"
	if allow {
		decision = "allow"
	}
	return json.NewEncoder(w).Encode(map[string]any{"hookSpecificOutput": map[string]any{"hookEventName": "PreToolUse", "permissionDecision": decision, "permissionDecisionReason": "Employee tools and file roots are confined"}})
}

// approvedMCPTool admits only tools of the caller's approved business servers,
// as Heimdall listed them in the turn environment (never model input).
// sep is the engine's separator after the server name: "__" (Claude), "_" (OMP).
func approvedMCPTool(tool, sep string) bool {
	var approved []string
	if json.Unmarshal([]byte(os.Getenv("HEIMDALL_EMPLOYEE_MCP")), &approved) != nil {
		return false
	}
	rest, ok := strings.CutPrefix(tool, "mcp__")
	if !ok {
		return false
	}
	for _, s := range approved {
		if _, ok := mcpoauth.Servers[s]; !ok {
			continue
		}
		if sep == "__" {
			// Claude/Codex name tools mcp__<server>__<tool>: the server
			// segment must equal the approved name exactly.
			if server, name, cut := strings.Cut(rest, "__"); cut && server == s && name != "" {
				return true
			}
			continue
		}
		// mcp__<server>_<tool> (OMP) cannot separate a server named
		// mellanni_x from mellanni; OMP loads only the lease's mcp.json
		// (ompEmployeeSettings), which names approved servers only.
		if name, cut := strings.CutPrefix(rest, s+sep); cut && name != "" {
			return true
		}
	}
	return false
}

// Inspect the native MCP catalog as the employee OS user, before any model runs.
// Config table merge semantics must not leave an ambient/renamed server enabled.
// Approved servers are fully defined here, so an ambient entry of the same
// name cannot supply their URL or credentials; every other one is disabled.
func employeeCodexMCPArgs(ctx context.Context, l Launch, approved []employeeServer) ([]string, error) {
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
	var entries []string
	for _, a := range approved {
		names = slices.DeleteFunc(names, func(n string) bool { return n == a.Name })
		entries = append(entries, strconv.Quote(a.Name)+"={enabled=true,url="+strconv.Quote(a.URL)+",bearer_token_env_var="+strconv.Quote(codexTokenEnv(a.Name))+"}")
	}
	for _, name := range names {
		// Missing servers need a valid dummy transport; disabled entries never connect.
		if !slices.ContainsFunc(servers, func(s struct {
			Name string `json:"name"`
		}) bool {
			return s.Name == name
		}) {
			entries = append(entries, strconv.Quote(name)+`={enabled=false,url="https://disabled.invalid/mcp"}`)
		} else {
			entries = append(entries, strconv.Quote(name)+"={enabled=false}")
		}
	}
	return []string{"-c", "mcp_servers={" + strings.Join(entries, ",") + "}"}, nil
}

// codexTokenEnv names the variable carrying one server's access-only token.
func codexTokenEnv(server string) string {
	return "EMPLOYEE_MCP_TOKEN_" + strings.ToUpper(server)
}
