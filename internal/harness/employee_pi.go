package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Owner auth is read by the root broker only. Never hand off refresh.
// The owner's existing Pi login/refresh lifecycle remains owned by Pi.
type employeeAccess struct {
	Type      string `json:"type"`
	Access    string `json:"access"`
	Refresh   string `json:"refresh"`
	Expires   int64  `json:"expires"`
	AccountID string `json:"accountId"`
}

func employeePiSnapshot(agent string, now time.Time) (employeeAccess, map[string]any, error) {
	var providers map[string]employeeAccess
	raw, err := os.ReadFile(filepath.Join(agent, "auth.json"))
	if err != nil || json.Unmarshal(raw, &providers) != nil {
		return employeeAccess{}, nil, errors.New("employee Pi: owner model access unavailable")
	}
	auth := providers["openai-codex"]
	auth.Refresh = ""
	if auth.Type != "oauth" || auth.Access == "" || auth.AccountID == "" || auth.Expires < now.Add(30*time.Minute).UnixMilli() {
		return employeeAccess{}, nil, errors.New("employee Pi: owner access token needs renewal by the owner Pi runtime")
	}
	// Local lease; provider token's cryptographic lifetime is unchanged.
	auth.Expires = now.Add(30 * time.Minute).UnixMilli()
	raw, err = os.ReadFile(filepath.Join(agent, "settings.json"))
	var owner map[string]any
	if err != nil || json.Unmarshal(raw, &owner) != nil || owner["defaultProvider"] != "openai-codex" {
		return employeeAccess{}, nil, errors.New("employee Pi: owner model settings unavailable")
	}
	settings := map[string]any{"packages": []string{}, "enableSkillCommands": false, "quietStartup": true}
	for _, k := range []string{"defaultProvider", "defaultModel", "defaultThinkingLevel", "modelThinkingLevels"} {
		if v, ok := owner[k]; ok {
			settings[k] = v
		}
	}
	return auth, settings, nil
}

func prepareEmployeePi(ctx context.Context, spec Spec, env []string) ([]string, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	l := spec.Launch
	if os.Getuid() != 0 || l.Cred == nil || l.Cred.Uid == 0 || l.Home == "/root" {
		return nil, nil, errors.New("employee Pi requires the root broker and a separate non-root uid")
	}
	if l.Path != SharedRoot+"/bin/pi" {
		return nil, nil, errors.New("employee Pi requires /opt/assistant/bin/pi; the root wrapper is forbidden")
	}
	env, err := piMCPEnv(l, spec.Caller, env)
	if err != nil || envValue(env, "MCP_OAUTH_DIR") == mcpDenyPath {
		return nil, nil, errors.New("employee Pi requires a verified caller OAuth scope")
	}
	auth, settings, err := employeePiSnapshot("/root/.pi/agent", time.Now())
	if err != nil {
		return nil, nil, err
	}
	parent := filepath.Join(l.Home, ".pi", "turn-agents")
	if err = privateMCPDir(parent, l); err != nil {
		return nil, nil, errors.New("employee Pi agent root unavailable")
	}
	dir, err := os.MkdirTemp(parent, "turn-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	if err = os.Chown(dir, int(l.Cred.Uid), int(l.Cred.Gid)); err != nil {
		return nil, nil, err
	}
	servers := piMCPServers(l.Home, spec.Caller, env, spec.Caller.Principal.Platform, envValue(env, "PI_MCP_OAUTH_ACTOR"), spec.Caller.OAuthInstallation)
	catalog := map[string]any{}
	for _, server := range servers {
		url := "https://mcp.bezosapp.uk/mcp"
		if server == "linear" {
			url = "https://mcp.linear.app/mcp"
		}
		catalog[server] = map[string]any{"url": url, "exposure": "direct"}
	}
	for name, value := range map[string]any{"auth.json": map[string]any{"openai-codex": auth}, "settings.json": settings, "mcp.json": map[string]any{"mcpServers": catalog, "autoEnableCodemode": false}} {
		raw, _ := json.Marshal(value)
		path := filepath.Join(dir, name)
		if err = os.WriteFile(path, raw, 0600); err != nil {
			return nil, nil, err
		}
		if err = os.Chown(path, int(l.Cred.Uid), int(l.Cred.Gid)); err != nil {
			return nil, nil, err
		}
	}
	if err = os.Symlink(SharedRoot+"/AGENTS.md", filepath.Join(dir, "AGENTS.md")); err != nil {
		return nil, nil, err
	}
	env = setEnv(env, "PI_CODING_AGENT_DIR", dir)
	env = setEnv(env, "HEIMDALL_MCP_SCOPE_ROOT", filepath.Join(l.Home, ".pi", "agent", "mcp-oauth-users"))
	ok = true
	return env, cleanup, nil
}
