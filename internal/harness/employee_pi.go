package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/googleoauth"
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
	if err = chownEmployeeAgent(dir, l); err != nil {
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
		if err = writeEmployeeJSON(dir, name, value, l); err != nil {
			return nil, nil, err
		}
	}
	access := googleoauth.Access{}
	if spec.Google != nil {
		c := spec.Caller
		// Failure is auth-needed for Google only; other employee tools still run.
		var gerr error
		access, gerr = spec.Google.Snapshot(ctx, googleoauth.Caller{Principal: c.Principal, Actor: c.OAuthActor, Installation: c.OAuthInstallation})
		if gerr != nil && spec.Log != nil {
			spec.Log("employee Google unavailable: " + gerr.Error())
		}
	}
	if err = writeEmployeeGoogleAccess(dir, access, l); err != nil {
		return nil, nil, err
	}
	env = setEnv(env, "HEIMDALL_GOOGLE_ACCESS_FILE", filepath.Join(dir, "google-access.json"))
	if err = os.Symlink(SharedRoot+"/AGENTS.md", filepath.Join(dir, "AGENTS.md")); err != nil {
		return nil, nil, err
	}
	env = setEnv(env, "PI_CODING_AGENT_DIR", dir)
	env = setEnv(env, "HEIMDALL_MCP_SCOPE_ROOT", filepath.Join(l.Home, ".pi", "agent", "mcp-oauth-users"))
	ok = true
	return env, cleanup, nil
}

// The handoff schema has no field capable of carrying renewal credentials.
func writeEmployeeGoogleAccess(dir string, access googleoauth.Access, l Launch) error {
	return writeEmployeeJSON(dir, "google-access.json", access, l)
}

// The shared uid can modify its own home. Never follow a planted handoff
// symlink or chown through a pathname after publishing ownership.
func chownEmployeeAgent(dir string, l Launch) error {
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return errors.New("employee agent directory is unsafe")
	}
	f := os.NewFile(uintptr(fd), dir)
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || st.Mode().Perm() != 0700 {
		return errors.New("employee agent directory is unsafe")
	}
	return f.Chown(int(l.Cred.Uid), int(l.Cred.Gid))
}
func writeEmployeeJSON(dir, name string, value any, l Launch) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(raw); err != nil {
		return err
	}
	return f.Chown(int(l.Cred.Uid), int(l.Cred.Gid))
}
