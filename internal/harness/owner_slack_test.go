package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// Golden digests produced by running the hub's actual TypeScript function at
// af9ffff with Node 26 --experimental-strip-types, not a reimplementation.
func TestMCPOAuthScopeHubGoldenVectors(t *testing.T) {
	for _, v := range []struct {
		platform, actor, installation, digest string
		servers                               []string
	}{
		{"telegram", "tg_user_42", "", "e6162cda1c270a6a8f129fa49f55f20152501f673d14068f6fea03c46b59f02c", []string{"mellanni"}},
		{"slack", "sl_user_U123", "sl_user_B456", "a9eb2226ade416a518c26fb98c0ac6ebc3343b5bd0b22ba4d716b536e83d8a0c", []string{"linear", "mellanni"}},
		{"telegram", "tg_-42", "", "0cf8cd7449319f19715d10a2cf93ad8fcc484d0f9f8b3cf97d18df25da595342", []string{"slack"}},
		{"slack", "sl_user_U123", "sl_user_B457", "002af551312ce32e8139e544464ebe5a9da46c0a234967ae149509e4fd3e85e4", []string{"slack"}},
	} {
		got := mcpOAuthScopePath("/root/.pi/agent/mcp-oauth-users", v.platform, v.actor, v.installation, v.servers)
		if got != "/root/.pi/agent/mcp-oauth-users/sha256-"+v.digest {
			t.Errorf("%s %s: %s", v.platform, v.actor, got)
		}
	}
}
func TestMCPOAuthScopeDenyAndStableServerSet(t *testing.T) {
	root := t.TempDir()
	path := mcpOAuthScopePath(root, "slack", "sl_user_U1", "sl_user_B1", []string{"mellanni"})
	if got := mcpOAuthScopePath(root, "slack", "sl_user_U1", "sl_user_B1", []string{"linear", "mellanni", "slack"}); got != path {
		t.Fatal("server changes moved existing token store")
	}
	for _, servers := range [][]string{nil, {}, {"helium10"}, {"mellanni", "mellanni"}} {
		if mcpOAuthScopePath(root, "slack", "sl_user_U1", "sl_user_B1", servers) != mcpDenyPath {
			t.Fatal("invalid server list admitted")
		}
	}
	for _, actor := range []string{"", "owner@example.com", "sl_user_../x"} {
		if mcpOAuthScopePath(root, "slack", actor, "sl_user_B1", []string{"slack"}) != mcpDenyPath {
			t.Fatal("invalid actor admitted")
		}
	}
}
func TestPiMCPScopeEnvAndHubPolicy(t *testing.T) {
	home := t.TempDir()
	access := filepath.Join(home, ".pi", "agent", "access")
	if err := os.MkdirAll(access, 0700); err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string]string{"identities.json": `{"principals":{"owner@example.com":{"aliases":["tg_user_42","sl_user_U1"]}}}`, "capabilities.json": `{"owner@example.com":["mcp.linear"]}`} {
		if err := os.WriteFile(filepath.Join(access, file), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	c := Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, Private: true, SlackRead: true}
	env, err := WithIdentity([]string{"MCP_OAUTH_DIR=wrong", "PI_MCP_OAUTH_ACTOR=wrong", "SLACK_CLIENT_SECRET=wrong", "SLACK_CLIENT_ID=wrong"}, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range env {
		if strings.Contains(v, "wrong") {
			t.Fatal("inherited OAuth claim or secret survived")
		}
	}
	env, err = piMCPEnv(Launch{Home: home}, c, env)
	if err != nil {
		t.Fatal(err)
	}
	if envValue(env, "PI_MCP_OAUTH_SERVERS") != `["linear","mellanni","slack"]` {
		t.Fatal(env)
	}
	path := envValue(env, "MCP_OAUTH_DIR")
	if !strings.HasSuffix(path, "e6162cda1c270a6a8f129fa49f55f20152501f673d14068f6fea03c46b59f02c") {
		t.Fatal(path)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0700 {
		t.Fatal("scope not private", err)
	}
	// Existing hub record remains at exactly the same path, no copying.
	if err := os.WriteFile(filepath.Join(path, "mcp-auth.json"), []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	c.SlackRead = false
	env, err = piMCPEnv(Launch{Home: home}, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if envValue(env, "MCP_OAUTH_DIR") != path || envValue(env, "PI_MCP_OAUTH_SERVERS") != `["linear","mellanni"]` {
		t.Fatal("existing store not reused")
	}
	c.Principal.ID = "43"
	env, err = piMCPEnv(Launch{Home: home}, c, nil)
	if err != nil || envValue(env, "PI_MCP_OAUTH_SERVERS") != `["mellanni"]` {
		t.Fatal("ungranted linear", err, env)
	}
}
func TestMCPDirectoryRejectsLinksAndPermissions(t *testing.T) {
	home := t.TempDir()
	l := Launch{Home: home}
	target := t.TempDir()
	p := filepath.Join(home, "link")
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
	if privateMCPDir(p, l) == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.Mkdir(filepath.Join(home, "public"), 0755); err != nil {
		t.Fatal(err)
	}
	if privateMCPDir(filepath.Join(home, "public"), l) == nil {
		t.Fatal("public scope accepted")
	}
}
func TestOwnerSlackGateMatrix(t *testing.T) {
	cfg := &OwnerSlack{Enabled: true}
	_ = cfg.Validate()
	base := core.Inbound{Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, From: core.Principal{Platform: "telegram", ID: "42"}, Private: true, Origin: core.OriginLive}
	for _, name := range []string{"pi", "claude", "codex", "omp", "agy"} {
		want := name == "pi" || name == "claude" || name == "codex"
		if SlackReadAllowed(cfg, base, core.RoleOwner, name, "") != want {
			t.Fatal(name)
		}
	}
	for _, change := range []func(*core.Inbound){func(i *core.Inbound) { i.Private = false }, func(i *core.Inbound) { i.Ambient = true }, func(i *core.Inbound) { i.Origin = core.OriginScheduled }, func(i *core.Inbound) { i.Conv.Interface = "local" }} {
		in := base
		change(&in)
		if SlackReadAllowed(cfg, in, core.RoleOwner, "pi", "") {
			t.Fatal("gate admitted ineligible turn")
		}
	}
	if SlackReadAllowed(cfg, base, core.RoleEmployee, "pi", "") {
		t.Fatal("employee admitted")
	}
	cfg.Enabled = false
	if SlackReadAllowed(cfg, base, core.RoleOwner, "pi", "") {
		t.Fatal("disabled admitted")
	}
	cfg.Enabled = true
	base.From = core.SlackPrincipal("owner@example.com", "T1", "")
	base.Conv.Interface = "slack"
	base.OAuthActor = "sl_user_U1"
	base.OAuthInstallation = "sl_user_B1"
	if !SlackReadAllowed(cfg, base, core.RoleOwner, "pi", "T1") {
		t.Fatal("verified DM denied")
	}
	for _, team := range []string{"", "T2"} {
		if SlackReadAllowed(cfg, base, core.RoleOwner, "pi", team) {
			t.Fatal("wrong team admitted")
		}
	}
	base.OAuthActor = ""
	if SlackReadAllowed(cfg, base, core.RoleOwner, "pi", "T1") {
		t.Fatal("missing live actor admitted")
	}
}
func TestOwnerSlackConfigValidation(t *testing.T) {
	var c OwnerSlack
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Enabled || len(c.Scopes) != 13 || c.PiCallbackPort != 18765 || c.ClaudeCallbackPort != 18766 || c.CodexCallbackPort != 18767 {
		t.Fatal(c)
	}
	for _, raw := range []string{`{"scopes":["chat:write"]}`, `{"scopes":[]}`, `{"server_url":"http://slack.com/mcp"}`, `{"client_secret_env":"secret-value!"}`, `{"pi_callback_port":18766}`} {
		var c OwnerSlack
		_ = json.Unmarshal([]byte(raw), &c)
		if c.Validate() == nil {
			t.Fatal(raw)
		}
	}
}
func TestOwnerSlackNativeArgsAndGuard(t *testing.T) {
	cfg := &OwnerSlack{Enabled: true}
	_ = cfg.Validate()
	env := []string{"SLACK_CLIENT_ID=synthetic-id", "SLACK_CLIENT_SECRET=synthetic-secret"}
	for _, name := range []string{"claude", "codex"} {
		off, err := ownerSlackArgs(name, Caller{}, cfg, env)
		if err != nil {
			t.Fatal(err)
		}
		on, err := ownerSlackArgs(name, Caller{SlackRead: true}, cfg, env)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(on, " ")
		if strings.Contains(joined, "synthetic-secret") {
			t.Fatal("secret in argv")
		}
		if name == "claude" {
			if strings.Join(off, " ") != "--disallowedTools mcp__slack__*" || !strings.Contains(joined, "slack-guard") || !strings.Contains(joined, "slack_search_*") {
				t.Fatal(off, on)
			}
		} else {
			if !strings.Contains(strings.Join(off, " "), "enabled=false") || !strings.Contains(strings.Join(off, " "), "mcp_servers.slack.url=") || !strings.Contains(joined, "enabled_tools=") || !strings.Contains(joined, "scopes=") || !strings.Contains(joined, `approval_mode="auto"`) {
				t.Fatal(off, on)
			}
		}
	}
	for _, name := range []string{"mcp__slack__slack_read_thread", "mcp__slack__slack_search_public", "mcp__slack__slack_send_message", "mcp__slack__new_write", "", "Read"} {
		raw, _ := json.Marshal(map[string]any{"tool_name": name})
		var out bytes.Buffer
		if err := SlackGuard(bytes.NewReader(raw), &out); err != nil {
			t.Fatal(err)
		}
		allow := strings.Contains(name, "__slack_read_") || strings.Contains(name, "__slack_search_")
		if strings.Contains(out.String(), `"permissionDecision":"allow"`) != allow {
			t.Fatal(name, out.String())
		}
	}
	var out bytes.Buffer
	_ = SlackGuard(strings.NewReader(`invalid`), &out)
	if !strings.Contains(out.String(), `"deny"`) {
		t.Fatal(out.String())
	}
}

type mcpBuffer struct{ bytes.Buffer }

func (b *mcpBuffer) Close() error { return nil }
func TestPiNativeMCPUIRelayAndCallerBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := &mcpBuffer{}
	b := &piRPCBridge{ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), closed: make(chan struct{}), pending: map[string]*pendingSteer{}}
	notices := []string{}
	b.mcpNotify = func(s string) { notices = append(notices, s) }
	b.event([]byte(`{"type":"extension_ui_request","method":"notify","message":"Sign in to MCP server \"slack\" in your browser:\nhttps://example.invalid/authorize?state=synthetic"}`))
	b.event([]byte(`{"type":"extension_ui_request","method":"input","id":"oauth","title":"Waiting for sign-in to \"slack\"."}`))
	if b.done || b.mcpInput != "oauth" || len(notices) != 1 {
		t.Fatal("native sign-in not held")
	}
	caller := Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner, Private: true, Conv: core.ConvKey{Interface: "telegram", Chat: "42"}}
	run := &Run{harness: "pi", caller: caller, bridge: b}
	callback := "http://localhost:18765/callback?state=synthetic&code=synthetic"
	stranger := caller.Principal
	stranger.ID = "43"
	if run.RespondMCP(ctx, stranger, caller.Conv, callback) {
		t.Fatal("other caller completed login")
	}
	wrong := caller.Conv
	wrong.Chat = "43"
	if run.RespondMCP(ctx, caller.Principal, wrong, callback) {
		t.Fatal("other chat completed login")
	}
	if run.RespondMCP(ctx, caller.Principal, caller.Conv, strings.Replace(callback, "state=synthetic", "state=wrong", 1)) {
		t.Fatal("wrong state consumed pending callback")
	}
	if !run.RespondMCP(ctx, caller.Principal, caller.Conv, callback) {
		t.Fatal("callback refused")
	}
	var response map[string]any
	if json.Unmarshal(input.Bytes(), &response) != nil || response["type"] != "extension_ui_response" || response["value"] != callback {
		t.Fatal("callback not native UI response")
	}
	if run.RespondMCP(ctx, caller.Principal, caller.Conv, callback) {
		t.Fatal("callback replayed")
	}
	b.event([]byte(`{"type":"response","id":"heimdall-initial","success":true,"data":{"disposition":"handled"}}`))
	if !b.done || !b.published || strings.Contains(b.answer, "synthetic") {
		t.Fatal("MCP command not settled safely")
	}
	b2 := &piRPCBridge{ctx: ctx, input: &mcpBuffer{}, ready: make(chan struct{}), closed: make(chan struct{})}
	b2.event([]byte(`{"type":"extension_ui_request","method":"input","id":"other","title":"Approve write?"}`))
	if b2.human == "" {
		t.Fatal("unrelated input auto-answered")
	}
	b.mcpExpires = time.Now().Add(-time.Second)
}

func TestPiSlackNativeConfigMustMatch(t *testing.T) {
	home := t.TempDir()
	agent := filepath.Join(home, ".pi", "agent")
	if err := os.MkdirAll(agent, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := &OwnerSlack{Enabled: true}
	_ = cfg.Validate()
	l := Launch{Home: home}
	if validatePiSlackConfig(l, cfg) == nil {
		t.Fatal("missing native setup accepted")
	}
	data, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"slack": map[string]any{"url": cfg.ServerURL, "oauth": map[string]any{"callbackPort": cfg.PiCallbackPort, "scope": strings.Join(cfg.Scopes, " ")}}}})
	if err := os.WriteFile(filepath.Join(agent, "mcp.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validatePiSlackConfig(l, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.PiCallbackPort++
	if validatePiSlackConfig(l, cfg) == nil {
		t.Fatal("port drift accepted")
	}
}

func TestHubLinearAdminAndCapabilityRules(t *testing.T) {
	for _, v := range []struct {
		name, env, admins, caps string
		want                    bool
	}{
		{"environment admin", "42", `{"users":[]}`, `{}`, true},
		{"file admin", "", `{"users":["tg_42"]}`, `{}`, true},
		{"capability alias", "", `{"users":[]}`, `{"owner@example.com":["mcp.linear"]}`, true},
		{"ungranted owner", "", `{"users":[]}`, `{}`, false},
	} {
		t.Run(v.name, func(t *testing.T) {
			home := t.TempDir()
			access := filepath.Join(home, ".pi", "agent", "access")
			if err := os.MkdirAll(access, 0700); err != nil {
				t.Fatal(err)
			}
			for name, data := range map[string]string{"admins.json": v.admins, "capabilities.json": v.caps, "identities.json": `{"principals":{"owner@example.com":{"aliases":["tg_user_42"]}}}`} {
				if err := os.WriteFile(filepath.Join(access, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if got := hubLinear([]string{"ADMIN_USER_IDS=" + v.env}, home, "tg", "tg_user_42"); got != v.want {
				t.Fatal("hub linear approval differs", got)
			}
		})
	}
}
