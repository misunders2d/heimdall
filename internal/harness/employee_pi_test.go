package harness

import (
	"encoding/json"
	"github.com/misunders2d/heimdall/internal/core"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEmployeeAccessOnlySnapshot(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	auth := employeeAccess{Type: "oauth", Access: "synthetic-access", Refresh: "NEVER_HAND_OFF", Expires: now.Add(time.Hour).UnixMilli(), AccountID: "synthetic-account"}
	raw, _ := json.Marshal(map[string]employeeAccess{"openai-codex": auth})
	os.WriteFile(filepath.Join(dir, "auth.json"), raw, 0600)
	os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"defaultProvider":"openai-codex","defaultModel":"fixture","defaultThinkingLevel":"high","packages":["owner-memory"],"extensions":["owner-tools"],"modelThinkingLevels":{"openai-codex/fixture":"xhigh"}}`), 0600)
	got, settings, err := employeePiSnapshot(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(map[string]any{"auth": got, "settings": settings})
	if got.Refresh != "" || strings.Contains(string(encoded), "NEVER_HAND_OFF") || strings.Contains(string(encoded), "owner-memory") || strings.Contains(string(encoded), "owner-tools") || got.Expires != now.Add(30*time.Minute).UnixMilli() {
		t.Fatal("owner credential/config escaped filter")
	}
	if settings["defaultModel"] != "fixture" || settings["defaultThinkingLevel"] != "high" || settings["modelThinkingLevels"] == nil {
		t.Fatal("model preference lost")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "auth.json"))
	if string(after) != string(raw) {
		t.Fatal("owner auth mutated")
	}
	auth.Expires = now.Add(time.Minute).UnixMilli()
	raw, _ = json.Marshal(map[string]employeeAccess{"openai-codex": auth})
	os.WriteFile(filepath.Join(dir, "auth.json"), raw, 0600)
	if _, _, err = employeePiSnapshot(dir, now); err == nil {
		t.Fatal("near expiry handed off")
	}
}

func TestEmployeeBusinessMCPScope(t *testing.T) {
	for _, platform := range []string{"telegram", "slack"} {
		home := t.TempDir()
		for _, linear := range []bool{false, true} {
			c := Caller{Role: core.RoleEmployee, Principal: core.Principal{Platform: platform, ID: "42", Email: "fixture@example.com"}, OAuthActor: "sl_user_U1", OAuthInstallation: "sl_user_B1", SlackRead: true}
			if linear {
				c.Capabilities = []string{"mcp.linear"}
			}
			servers := EmployeeMCPServers(c)
			if len(servers) != 1+map[bool]int{true: 1}[linear] || servers[0] != "mellanni" {
				t.Fatal(servers)
			}
			env, err := piMCPEnv(Launch{Home: home}, c, nil)
			if err != nil {
				t.Fatal(err)
			}
			path := envValue(env, "MCP_OAUTH_DIR")
			if path == mcpDenyPath || !strings.HasPrefix(path, home+"/") {
				t.Fatal("caller scope missing")
			}
			c.Principal.ID = "43"
			c.OAuthActor = "sl_user_U2"
			other, _ := piMCPEnv(Launch{Home: home}, c, nil)
			if path == envValue(other, "MCP_OAUTH_DIR") {
				t.Fatal("callers share OAuth store")
			}
		}
	}
	c := Caller{Role: core.RoleEmployee, Principal: core.Principal{Platform: "slack", Email: "fixture@example.com"}}
	if len(EmployeeMCPServers(c)) != 0 {
		t.Fatal("Slack without verified actor/installation admitted")
	}
}
