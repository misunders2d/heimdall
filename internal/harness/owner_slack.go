package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// OwnerSlack is an opt-in native MCP configuration. Secrets are references only.
type OwnerSlack struct {
	Enabled            bool     `json:"enabled"`
	ServerURL          string   `json:"server_url,omitempty"`
	Scopes             []string `json:"scopes,omitempty"`
	ClientIDEnv        string   `json:"client_id_env,omitempty"`
	ClientSecretEnv    string   `json:"client_secret_env,omitempty"`
	PiCallbackPort     int      `json:"pi_callback_port,omitempty"`
	ClaudeCallbackPort int      `json:"claude_callback_port,omitempty"`
	CodexCallbackPort  int      `json:"codex_callback_port,omitempty"`
}

var DefaultSlackScopes = []string{"search:read.public", "search:read.private", "search:read.im", "search:read.mpim", "channels:read", "channels:history", "groups:read", "groups:history", "im:read", "im:history", "mpim:read", "mpim:history", "users:read"}
var slackReadTools = []string{"slack_search_public", "slack_search_public_and_private", "slack_search_channels", "slack_search_users", "slack_read_channel", "slack_read_thread", "slack_read_canvas", "slack_read_user_profile"}

func (c *OwnerSlack) Validate() error {
	if c.ServerURL == "" {
		c.ServerURL = "https://mcp.slack.com/mcp"
	}
	u, e := url.Parse(c.ServerURL)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("owner_slack.server_url must be an HTTPS URL")
	}
	if c.Scopes == nil {
		c.Scopes = slices.Clone(DefaultSlackScopes)
	}
	allowed := append(slices.Clone(DefaultSlackScopes), "search:read.users", "search:read.files", "files:read", "users:read.email", "canvases:read", "reactions:read")
	if len(c.Scopes) == 0 {
		return errors.New("owner_slack.scopes must not be empty")
	}
	for i, s := range c.Scopes {
		if !slices.Contains(allowed, s) || slices.Contains(c.Scopes[:i], s) {
			return errors.New("owner_slack.scopes must be distinct read scopes")
		}
	}
	if c.ClientIDEnv == "" {
		c.ClientIDEnv = "SLACK_CLIENT_ID"
	}
	if c.ClientSecretEnv == "" {
		c.ClientSecretEnv = "SLACK_CLIENT_SECRET"
	}
	envName := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	if !envName.MatchString(c.ClientIDEnv) || !envName.MatchString(c.ClientSecretEnv) || c.ClientIDEnv == c.ClientSecretEnv {
		return errors.New("owner_slack client references must be distinct environment variable names")
	}
	ports := []*int{&c.PiCallbackPort, &c.ClaudeCallbackPort, &c.CodexCallbackPort}
	seen := map[int]bool{}
	for i, p := range ports {
		if *p == 0 {
			*p = 18765 + i
		}
		if *p < 1024 || *p > 65535 || seen[*p] {
			return errors.New("owner_slack callback ports must be distinct ports between 1024 and 65535")
		}
		seen[*p] = true
	}
	return nil
}
func SlackReadAllowed(c *OwnerSlack, in core.Inbound, role core.Role, harness, team string) bool {
	if c == nil || !c.Enabled || role != core.RoleOwner || !in.Private || in.Ambient || in.Origin != core.OriginLive || !slices.Contains([]string{"pi", "claude", "codex"}, harness) {
		return false
	}
	switch in.From.Platform {
	case "telegram":
		return in.Conv.Interface == "telegram" && telegramActor.MatchString("tg_user_"+in.From.ID)
	case "slack":
		return in.SlackReadVerified && in.Conv.Interface == "slack" && team != "" && in.From.Team == team && in.From.Email != "" && slackActor.MatchString(in.OAuthActor) && slackActor.MatchString(in.OAuthInstallation)
	}
	return false
}
func ownerSlackArgs(name string, c Caller, settings *OwnerSlack, env []string) ([]string, error) {
	if name == "claude" {
		if !c.SlackRead {
			return []string{"--disallowedTools", "mcp__slack__*"}, nil
		}
		if settings == nil {
			return nil, errors.New("missing owner Slack config")
		}
		id := envValue(env, settings.ClientIDEnv)
		if id == "" {
			return nil, errors.New("owner Slack client ID environment reference is unset")
		}
		config, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"slack": map[string]any{"type": "http", "url": settings.ServerURL, "oauth": map[string]any{"clientId": id, "callbackPort": settings.ClaudeCallbackPort, "scopes": strings.Join(settings.Scopes, " ")}}}})
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		// Shell quoting protects installation paths, including spaces and apostrophes.
		command := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "' hook slack-guard"
		hooks, _ := json.Marshal(map[string]any{"hooks": map[string]any{"PreToolUse": []any{map[string]any{"matcher": "mcp__slack__.*", "hooks": []any{map[string]any{"type": "command", "command": command, "onFailure": "block"}}}}}})
		return []string{"--mcp-config", string(config), "--allowedTools", "mcp__slack__slack_read_*", "mcp__slack__slack_search_*", "--settings", string(hooks)}, nil
	}
	if name == "codex" {
		if c.SlackRead && settings == nil {
			return nil, errors.New("missing owner Slack config")
		}
		if !c.SlackRead {
			// Start inspects native config before applying transport-neutral disable.
			return nil, nil
		}
		scopes, _ := json.Marshal(settings.Scopes)
		tools, _ := json.Marshal(slackReadTools)
		return []string{"-c", "mcp_servers.slack.enabled=true", "-c", "mcp_servers.slack.url=" + strconv.Quote(settings.ServerURL), "-c", "mcp_servers.slack.scopes=" + string(scopes), "-c", "mcp_servers.slack.enabled_tools=" + string(tools), "-c", "mcp_servers.slack.default_tools_approval_mode=\"auto\"", "-c", "mcp_servers.slack.oauth.callback_port=" + strconv.Itoa(settings.CodexCallbackPort)}, nil
	}
	return nil, nil
}

// SlackGuard is Claude's PreToolUse hook. Unknown/malformed inputs deny.
func SlackGuard(r io.Reader, w io.Writer) error {
	var in struct {
		ToolName string `json:"tool_name"`
	}
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	err := dec.Decode(&in)
	allowed := err == nil && dec.Decode(new(any)) == io.EOF && regexp.MustCompile(`^mcp__slack__slack_(read|search)_[A-Za-z0-9_]+$`).MatchString(in.ToolName)
	decision := "deny"
	if allowed {
		decision = "allow"
	}
	return json.NewEncoder(w).Encode(map[string]any{"hookSpecificOutput": map[string]any{"hookEventName": "PreToolUse", "permissionDecision": decision, "permissionDecisionReason": "Owner Slack permits read/search tools only"}})
}

// Pi's reviewed seam consumes its native shared config; never rewrite it.
// Fail on configuration drift rather than silently requesting other scopes.
func validatePiSlackConfig(l Launch, c *OwnerSlack) error {
	if c == nil {
		return errors.New("missing owner Slack config")
	}
	agent := envValue(l.Env, "PI_CODING_AGENT_DIR")
	if agent == "" {
		agent = filepath.Join(l.Home, ".pi", "agent")
	}
	raw, err := os.ReadFile(filepath.Join(agent, "mcp.json"))
	if err != nil {
		return errors.New("Pi owner Slack requires its native mcp.json setup")
	}
	var config struct {
		Servers map[string]struct {
			URL   string `json:"url"`
			OAuth struct {
				Scope string `json:"scope"`
				Port  int    `json:"callbackPort"`
			} `json:"oauth"`
		} `json:"mcpServers"`
	}
	if json.Unmarshal(raw, &config) != nil {
		return errors.New("invalid native Pi MCP config")
	}
	entry, ok := config.Servers["slack"]
	scopes := strings.Fields(entry.OAuth.Scope)
	if !ok || entry.URL != c.ServerURL || entry.OAuth.Port != c.PiCallbackPort || len(scopes) != len(c.Scopes) {
		return errors.New("native Pi Slack URL, scopes and callback port must match owner_slack")
	}
	for _, scope := range c.Scopes {
		if !slices.Contains(scopes, scope) {
			return errors.New("native Pi Slack scopes must match owner_slack")
		}
	}
	return nil
}

// Native inspection avoids assuming Slack's transport or parsing owner TOML.
// It runs as the turn user, with no model or MCP auth discovery operation.
func codexDisabledSlackArgs(ctx context.Context, l Launch) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, l.Path, "mcp", "get", "slack", "--json")
	cmd.Dir, cmd.Env = l.Dir, l.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: l.Cred}
	raw, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && strings.Contains(string(exit.Stderr), "No MCP server named 'slack' found.") {
			return []string{"-c", `mcp_servers.slack={enabled=false,url="https://mcp.slack.com/mcp"}`}, nil
		}
		return nil, errors.New("cannot inspect native Codex MCP config")
	}
	var server struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &server) != nil || server.Name != "slack" {
		return nil, errors.New("invalid native Codex MCP config entry")
	}
	return []string{"-c", "mcp_servers.slack.enabled=false"}, nil
}
