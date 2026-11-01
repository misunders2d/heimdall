package harness_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

func TestOwnerSlackLaunchIsolationAcrossResumedTurns(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi", "claude", "codex")
	cfg := &harness.OwnerSlack{Enabled: true}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pi", "claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			capture := filepath.Join(t.TempDir(), "capture.json")
			l.Env = append(l.Env, "FAKE_CAPTURE_LAUNCH="+capture, "SLACK_CLIENT_ID=synthetic-client", "SLACK_CLIENT_SECRET=synthetic-secret", "MCP_OAUTH_DIR=inherited", "PI_MCP_OAUTH_ACTOR=inherited")
			if name == "pi" {
				agent := filepath.Join(l.Home, ".pi", "agent")
				if err := os.MkdirAll(agent, 0700); err != nil {
					t.Fatal(err)
				}
				data, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"slack": map[string]any{"url": cfg.ServerURL, "oauth": map[string]any{"callbackPort": cfg.PiCallbackPort, "scope": strings.Join(cfg.Scopes, " ")}}}})
				if err := os.WriteFile(filepath.Join(agent, "mcp.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			c := harness.Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "telegram", Chat: "42"}, Private: true}
			session := harness.Session{ID: harness.NewSessionID(name)}
			for _, enabled := range []bool{true, false} {
				c.SlackRead = enabled
				run, err := harness.Start(context.Background(), harness.Spec{Harness: name, Prompt: "synthetic", Session: session, Launch: l, Caller: c, OwnerSlack: cfg})
				if err != nil {
					t.Fatal(err)
				}
				result := run.Result()
				<-run.Exited()
				if result.Failure != "" || result.Answer == "" {
					t.Fatal(result)
				}
				session = harness.Session{ID: result.SessionID, Resume: true}
				raw, err := os.ReadFile(capture)
				if err != nil {
					t.Fatal(err)
				}
				var got struct {
					Args                           []string `json:"args"`
					OAuth, Servers, Client, Secret string
				}
				if err := json.Unmarshal(raw, &got); err != nil {
					t.Fatal(err)
				}
				args := strings.Join(got.Args, " ")
				if strings.Contains(args, "synthetic-secret") {
					t.Fatal("secret leaked into argv")
				}
				switch name {
				case "pi":
					if strings.Contains(got.Servers, `"slack"`) != enabled || got.OAuth == "inherited" || got.OAuth == "" {
						t.Fatal("Pi scope not isolated", got)
					}
					if enabled {
						if got.Client != "synthetic-client" || got.Secret != "synthetic-secret" {
							t.Fatal("gated Pi missing credentials")
						}
					} else if got.Client != "" || got.Secret != "" {
						t.Fatal("ungated Pi inherited credentials")
					}
				case "claude":
					if strings.Contains(args, "--mcp-config") != enabled || strings.Contains(args, "--disallowedTools") == enabled {
						t.Fatal(args)
					}
				case "codex":
					if !strings.Contains(args, "mcp_servers.slack.url=") {
						t.Fatal("disabled Slack needs a valid transport even when native config is absent")
					}
					if !strings.Contains(args, "mcp_servers.slack.enabled="+map[bool]string{true: "true", false: "false"}[enabled]) {
						t.Fatal(args)
					}
				}
				if name != "pi" && (got.Client != "" || got.Secret != "" || got.OAuth != "") {
					t.Fatal("OAuth env leaked to other harness")
				}
			}
		})
	}
}
