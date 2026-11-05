package harness

// Native Codex 0.166.0 app/read, app/list and AppConfig, checked against
// installed generated schemas and openai/codex rust-v0.166.0. This profile
// changes one launch only; ordinary owner automatic mode is untouched.
import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type monitorApp struct {
	ID, Name      string
	IsEnabled     bool
	ToolSummaries []struct {
		Name                  string
		IsEnabled, IsReadOnly bool
	}
	ConfiguredTools []string
}

// These are exact native Slack tool IDs, with the optional connector prefix
// used by Codex Apps. Unknown/new tools default deny, including control tools.
func monitorSlackTool(name string) bool {
	name = strings.TrimPrefix(strings.TrimPrefix(name, "slack_"), "slack_")
	switch name {
	case "read_thread", "read_channel", "read_user_profile", "search", "search_messages", "search_public", "search_public_and_private", "search_channels", "search_users", "get_profile":
		return true
	}
	return false
}

// Metadata only: no thread/start, model, business tool, auth or config write.
// Every child exits and is waited for before returning. Missing metadata refuses
// the launch instead of substituting names or relying on model instructions.
func monitorApps(ctx context.Context, l Launch, args []string) ([]monitorApp, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, l.Path, append([]string{"app-server"}, args...)...)
	cmd.Dir, cmd.Env = l.Dir, l.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: l.Cred}
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		return nil, errors.New("monitor metadata process unavailable")
	}
	defer func() { input.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	enc := json.NewEncoder(input)
	scan := bufio.NewScanner(io.LimitReader(output, 16<<20))
	scan.Buffer(make([]byte, 4096), 2<<20)
	id := 0
	rpc := func(method string, params any, result any) error {
		id++
		if enc.Encode(map[string]any{"id": id, "method": method, "params": params}) != nil {
			return errors.New("monitor metadata input failed")
		}
		for scan.Scan() {
			var r struct {
				ID     int
				Result json.RawMessage
				Error  json.RawMessage
			}
			if json.Unmarshal(scan.Bytes(), &r) != nil || r.ID != id {
				continue
			}
			if len(r.Error) > 0 && string(r.Error) != "null" {
				return errors.New("monitor metadata request refused; launch blocked")
			}
			if json.Unmarshal(r.Result, result) != nil {
				return errors.New("monitor metadata schema unavailable")
			}
			return nil
		}
		return errors.New("monitor metadata unavailable or timed out; launch blocked")
	}
	var initialized map[string]any
	if err = rpc("initialize", map[string]any{"clientInfo": map[string]string{"name": "heimdall-monitor-profile", "version": clientVersion}, "capabilities": map[string]bool{"experimentalApi": true}}, &initialized); err != nil {
		return nil, err
	}
	if err = enc.Encode(map[string]any{"method": "initialized"}); err != nil {
		return nil, err
	}
	// Enumerate configuration names, never auth material. AppConfig defaults
	// enabled=true even for a table containing only per-tool overrides. Include
	// configured-but-unlisted apps and tool/title keys so none can leak through
	// native table merge or title-fallback semantics.
	var config struct {
		Config struct {
			Apps map[string]struct {
				Enabled bool
				Tools   map[string]json.RawMessage
			}
		}
	}
	if err = rpc("config/read", map[string]any{"includeLayers": false, "cwd": l.Dir}, &config); err != nil {
		return nil, err
	}
	var apps []monitorApp
	cursor := ""
	for page := 0; page < 25; page++ {
		var list struct {
			Data       []monitorApp
			NextCursor string
		}
		params := map[string]any{"limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		if err = rpc("app/list", params, &list); err != nil {
			return nil, err
		}
		apps = append(apps, list.Data...)
		if list.NextCursor == "" {
			break
		}
		if list.NextCursor == cursor || page == 24 {
			return nil, errors.New("monitor app discovery incomplete")
		}
		cursor = list.NextCursor
	}
	var slackIDs []string
	for _, a := range apps {
		if a.ID == "" {
			return nil, errors.New("monitor app metadata has no ID")
		}
		if a.Name == "Slack" {
			slackIDs = append(slackIDs, a.ID)
		}
	}
	if len(slackIDs) != 1 {
		return nil, errors.New("monitor requires exactly one installed Slack Apps connector")
	}
	var read struct {
		Apps          []monitorApp
		MissingAppIDs []string
	}
	if err = rpc("app/read", map[string]any{"appIds": slackIDs, "includeTools": true}, &read); err != nil {
		return nil, err
	}
	if len(read.MissingAppIDs) != 0 || len(read.Apps) != 1 || read.Apps[0].ID != slackIDs[0] || len(read.Apps[0].ToolSummaries) == 0 {
		return nil, errors.New("monitor Slack tool metadata unavailable")
	}
	for i := range apps {
		if apps[i].ID == slackIDs[0] {
			apps[i].ToolSummaries = read.Apps[0].ToolSummaries
		}
	}
	for id, cfg := range config.Config.Apps {
		if id == "_default" {
			continue
		}
		index := -1
		for i := range apps {
			if apps[i].ID == id {
				index = i
				break
			}
		}
		if index < 0 {
			apps = append(apps, monitorApp{ID: id, IsEnabled: cfg.Enabled})
			index = len(apps) - 1
		}
		for key := range cfg.Tools {
			apps[index].ConfiguredTools = append(apps[index].ConfiguredTools, key)
		}
		sort.Strings(apps[index].ConfiguredTools)
	}
	sort.Slice(apps, func(i, j int) bool { return apps[i].ID < apps[j].ID })
	return apps, nil
}

func monitorAppArgs(apps []monitorApp) ([]string, error) {
	entries := []string{`_default={enabled=false}`}
	reads, searches := 0, 0
	for _, a := range apps {
		if a.Name != "Slack" {
			entries = append(entries, strconv.Quote(a.ID)+`={enabled=false}`)
			continue
		}
		var tools []string
		seen := map[string]bool{}
		for _, t := range a.ToolSummaries {
			if t.Name == "" || seen[t.Name] {
				return nil, errors.New("invalid monitor tool metadata")
			}
			seen[t.Name] = true
			allow := t.IsReadOnly && monitorSlackTool(t.Name)
			tools = append(tools, strconv.Quote(t.Name)+"={enabled="+strconv.FormatBool(allow)+"}")
			if allow {
				if strings.Contains(t.Name, "read_thread") {
					reads++
				}
				if strings.Contains(t.Name, "search") {
					searches++
				}
			}
		}
		for _, key := range a.ConfiguredTools {
			if !seen[key] {
				tools = append(tools, strconv.Quote(key)+"={enabled=false}")
			}
		}
		entries = append(entries, strconv.Quote(a.ID)+`={enabled=true,default_tools_enabled=false,tools={`+strings.Join(tools, ",")+`}}`)
	}
	if reads == 0 || searches == 0 {
		return nil, errors.New("monitor requires verified Slack read_thread and search tool IDs")
	}
	return []string{"-c", "apps={" + strings.Join(entries, ",") + "}"}, nil
}

func monitorArgs(ctx context.Context, l Launch) ([]string, error) {
	apps, err := monitorApps(ctx, l, nil)
	if err != nil {
		return nil, err
	}
	args, err := monitorAppArgs(apps)
	if err != nil {
		return nil, err
	}
	// Explicitly disable all discovered ambient MCP servers; only Memento's
	// exact recall operation survives. Native enabled_tools prevents execution.
	mcpCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(mcpCtx, l.Path, "mcp", "list", "--json")
	cmd.Env, cmd.Dir = l.Env, l.Dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: l.Cred}
	raw, err := cmd.Output()
	if err != nil {
		return nil, errors.New("monitor MCP catalog unavailable")
	}
	var servers []struct{ Name string }
	if json.Unmarshal(raw, &servers) != nil {
		return nil, errors.New("invalid monitor MCP catalog")
	}
	var entries []string
	recall := false
	for _, s := range servers {
		if s.Name == "" {
			return nil, errors.New("monitor MCP server has no name")
		}
		if s.Name == "memento" {
			recall = true
			entries = append(entries, `memento={enabled=true,enabled_tools=["recall"],disabled_tools=[]}`)
		} else {
			entries = append(entries, strconv.Quote(s.Name)+`={enabled=false}`)
		}
	}
	if !recall {
		return nil, errors.New("monitor requires existing native Memento recall server")
	}
	args = append(args, "-c", "mcp_servers={"+strings.Join(entries, ",")+"}")
	args = append(args, monitorBaseArgs()...)
	// Effective metadata verification catches merge semantics and inherited
	// per-tool enables (including unrelated company/personal app overrides).
	checked, err := monitorApps(ctx, l, args)
	if err != nil {
		return nil, err
	}
	for _, a := range checked {
		if a.Name != "Slack" && a.IsEnabled {
			return nil, errors.New("monitor inherited an enabled non-Slack app")
		}
		if a.Name == "Slack" {
			for _, t := range a.ToolSummaries {
				want := t.IsReadOnly && monitorSlackTool(t.Name)
				if t.IsEnabled != want {
					return nil, errors.New("monitor native Slack tool filtering not effective")
				}
			}
		}
	}
	return args, nil
}

func monitorBaseArgs() []string {
	args := []string{"--strict-config", "-c", `sandbox_mode="read-only"`, "-c", `approval_policy="never"`, "-c", `web_search="disabled"`, "-c", `agents.enabled=false`}
	// Keep native policy hooks (including the shared APPEND_SYSTEM addendum).
	// MEMENTO_PROCESSING=1 independently suppresses memory hooks/capture.
	for _, f := range []string{"shell_tool", "multi_agent", "multi_agent_v2", "plugins", "memories", "computer_use", "browser_use", "skill_mcp_dependency_install", "tool_suggest", "image_generation", "view_image", "js_repl", "code_mode", "code_mode_host", "executor_capability_discovery", "stable_environment_tools"} {
		args = append(args, "--disable", f)
	}
	return args
}
