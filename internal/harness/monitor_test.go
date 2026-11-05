package harness

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
)

func TestMonitorNativeProfileFiltersExactCatalogAndLeavesOwnerMode(t *testing.T) {
	bin, _ := fakeharness.Install(t, "codex")
	path := filepath.Join(bin, "codex")
	// Install returns launch scripts; native metadata and final turn run as
	// the same owner with fake native protocols, never a business connector.
	home := t.TempDir()
	capture := filepath.Join(home, "launch.json")
	l := Launch{Path: path, Home: home, Dir: home, Env: append(os.Environ(), "HOME="+home, "FAKE_MONITOR_METADATA=1", "FAKE_CAPTURE_LAUNCH="+capture)}
	caller := Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "telegram", Chat: "42"}}
	run, err := Start(context.Background(), Spec{Harness: "codex", Caller: caller, Launch: l, Prompt: "synthetic", OneShot: true, ReadOnlyMonitor: true})
	if err != nil {
		t.Fatal(err)
	}
	result := run.Result()
	<-run.Exited()
	if result.Failure != "" {
		t.Fatal(result)
	}
	raw, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Args   []string
		Memory string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(got.Args, " ")
	for _, want := range []string{`"app-github"={enabled=false}`, `"app-hidden"={enabled=false}`, `"slack_new_write"={enabled=false}`, `default_tools_enabled=false`, `"slack_slack_post_message"={enabled=false}`, `"slack_slack_read_thread"={enabled=true}`, `"heimdall-scheduler"={enabled=false}`, `memento={enabled=true,enabled_tools=["recall"]`, `shell_tool`, `sandbox_mode="read-only"`, `approval_policy="never"`} {
		if !strings.Contains(args, want) {
			t.Errorf("missing policy %s", want)
		}
	}
	if got.Memory != "1" {
		t.Fatal("capture not disabled", got.Memory)
	}
	for i, arg := range got.Args {
		if arg == "--disable" && i+1 < len(got.Args) && got.Args[i+1] == "hooks" {
			t.Fatal("monitor disabled shared native policy hooks")
		}
	}
	var out strings.Builder
	b := codexTestBridge()
	b.monitor = true
	b.input = discard{&out}
	b.event([]byte(`{"id":1,"result":{}}`))
	if !strings.Contains(out.String(), `"approvalPolicy":"never"`) || !strings.Contains(out.String(), `"sandbox":"read-only"`) || strings.Contains(out.String(), "auto_review") {
		t.Fatal(out.String())
	}
}

func TestMonitorProfileRefusesWrongLaunchAndUnknownReadTool(t *testing.T) {
	apps := []monitorApp{{ID: "slack", Name: "Slack"}}
	if _, err := monitorAppArgs(apps); err == nil {
		t.Fatal("missing real tools accepted")
	}
	for _, tool := range []string{"slack_slack_post_message", "slack_slack_update_message", "slack_slack_unknown_read", "heimdall_scheduler", "recall"} {
		if monitorSlackTool(tool) {
			t.Fatal("unknown/write tool allowed", tool)
		}
	}
	if _, err := Start(context.Background(), Spec{Harness: "codex", Launch: Launch{Path: "/bin/false"}, ReadOnlyMonitor: true}); err == nil {
		t.Fatal("unscoped monitor allowed")
	}
}

// Explicit opt-in, synthetic HOME only. No login, model or connector runs.
func TestNativeMonitorCodexConfig(t *testing.T) {
	bin := os.Getenv("HEIMDALL_TEST_CODEX")
	if bin == "" {
		t.Skip("set HEIMDALL_TEST_CODEX for auth-free installed parser validation")
	}
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(`[apps.app-hidden.tools.inherited_write]
enabled=true
[apps.app-slack.tools.slack_new_write]
enabled=true
[mcp_servers.memento]
command="/bin/false"
[mcp_servers.heimdall-scheduler]
command="/bin/false"
`), 0600); err != nil {
		t.Fatal(err)
	}
	apps := []monitorApp{{ID: "app-hidden"}, {ID: "app-slack", Name: "Slack", ConfiguredTools: []string{"slack_new_write"}}}
	apps[1].ToolSummaries = append(apps[1].ToolSummaries, struct {
		Name                  string
		IsEnabled, IsReadOnly bool
	}{Name: "slack_slack_read_thread", IsReadOnly: true}, struct {
		Name                  string
		IsEnabled, IsReadOnly bool
	}{Name: "slack_slack_search", IsReadOnly: true})
	args, err := monitorAppArgs(apps)
	if err != nil {
		t.Fatal(err)
	}
	args = append(args, monitorBaseArgs()...)
	args = append(args, "-c", `mcp_servers={memento={enabled=true,enabled_tools=["recall"]},heimdall-scheduler={enabled=false}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, append([]string{"app-server", "--print-startup-config"}, args...)...)
	cmd.Env = []string{"HOME=" + home, "CODEX_HOME=" + home, "PATH=/usr/bin:/bin", "MEMENTO_PROCESSING=1"}
	cmd.Dir = home
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native parser: %v: %s", err, out)
	}
	if !strings.Contains(string(out), `"appServerVersion":"0.166.0"`) {
		t.Fatal(string(out))
	}
	t.Log("Codex 0.166.0 strict parser accepts monitor App/MCP/restriction overrides; no live connector claim")
}
