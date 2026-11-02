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
)

// Opt-in, auth-free parser checks. Never reads a real harness home or runs a model.
func TestNativeEmployeeCodexConfig(t *testing.T) {
	bin := os.Getenv("HEIMDALL_TEST_CODEX")
	if bin == "" {
		t.Skip("set HEIMDALL_TEST_CODEX for installed-binary validation")
	}
	home := t.TempDir()
	l := Launch{Home: home, Dir: filepath.Join(home, "turn")}
	if err := os.Mkdir(l.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(`[mcp_servers."synthetic.owner"]
url="https://disabled.invalid/mcp"
`), 0600); err != nil {
		t.Fatal(err)
	}
	l.Path = bin
	l.Env = []string{"HOME=" + home, "CODEX_HOME=" + home, "PATH=/usr/bin:/bin"}
	args, err := employeeArgs("codex", l)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	disabled, err := employeeCodexMCPArgs(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	args = append(args, disabled...)
	cmd := exec.CommandContext(ctx, bin, append([]string{"app-server", "--print-startup-config"}, args...)...)
	cmd.Dir = l.Dir
	cmd.Env = []string{"HOME=" + home, "CODEX_HOME=" + home, "PATH=/usr/bin:/bin"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !strings.Contains(string(out), `"appServerVersion":"0.166.0"`) {
		t.Fatal(string(out))
	}
	features := exec.CommandContext(ctx, bin, "features", "list")
	features.Env = cmd.Env
	features.Dir = l.Dir
	out, err = features.Output()
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range args {
		if a == "--disable" && !strings.Contains(string(out), args[i+1]+" ") {
			t.Errorf("unknown feature %s", args[i+1])
		}
	}
	catalog := exec.CommandContext(ctx, bin, append([]string{"mcp", "list", "--json"}, disabled...)...)
	catalog.Env = l.Env
	catalog.Dir = l.Dir
	raw, err := catalog.Output()
	if err != nil {
		t.Fatal(err)
	}
	var servers []struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err = json.Unmarshal(raw, &servers); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, server := range servers {
		if server.Enabled {
			t.Fatalf("ambient MCP enabled: %s", server.Name)
		}
		if server.Name == "synthetic.owner" {
			found = true
		}
	}
	if !found {
		t.Fatal("synthetic MCP disappeared instead of disabling")
	}
	t.Log("0.166.0 strict parser and disabled feature names accepted")
}

func TestNativePiExplicitExtensionsOnly(t *testing.T) {
	bin := os.Getenv("HEIMDALL_TEST_PI")
	if bin == "" {
		t.Skip("set HEIMDALL_TEST_PI for installed-binary validation")
	}
	home := t.TempDir()
	agent := filepath.Join(home, "agent")
	if err := os.MkdirAll(filepath.Join(agent, "extensions"), 0700); err != nil {
		t.Fatal(err)
	}
	ambient := `console.log("UNSAFE_AMBIENT_EXTENSION_LOADED"); export default function(pi) {}`
	if err := os.WriteFile(filepath.Join(agent, "extensions", "memory.ts"), []byte(ambient), 0600); err != nil {
		t.Fatal(err)
	}
	explicit := filepath.Join(home, "guard.ts")
	if err := os.WriteFile(explicit, []byte(`console.log("EXPLICIT_GUARD_LOADED"); export default function(pi) {}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--offline", "--no-extensions", "--no-mcp", "-e", explicit, "--help")
	cmd.Dir = home
	cmd.Env = []string{"HOME=" + home, "PI_CODING_AGENT_DIR=" + agent, "PATH=/usr/bin:/bin:/root/.nvm/versions/node/v22.23.4/bin", "PI_TELEMETRY=0", "MEMENTO_PROCESSING=1"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(string(out), "UNSAFE_AMBIENT_EXTENSION_LOADED") || !strings.Contains(string(out), "EXPLICIT_GUARD_LOADED") {
		t.Fatal(string(out))
	}
	t.Log("ambient memory extension skipped; explicit guard extension loaded")
}
