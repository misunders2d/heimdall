package harness

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestWarmFingerprintLaunchFields(t *testing.T) {
	base := Spec{Harness: "claude", Launch: Launch{Home: t.TempDir(), Dir: t.TempDir(), Env: []string{"A=1"}}, Caller: Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner}}
	tests := map[string]func(*Spec){
		"os-user":     func(s *Spec) { s.Launch.Cred = &syscall.Credential{Uid: 123, Gid: 456} },
		"home":        func(s *Spec) { s.Launch.Home = t.TempDir() },
		"executable":  func(s *Spec) { s.Launch.Path = "/other/claude" },
		"memory":      func(s *Spec) { s.Launch.Env = []string{"A=1", "MEMENTO_PROCESSING=1"} },
		"owner-slack": func(s *Spec) { s.Caller.SlackRead = true },
		"oauth":       func(s *Spec) { s.Caller.OAuthActor = "another" },
		"harness":     func(s *Spec) { s.Harness = "pi" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := base
			mutate(&s)
			if launchFingerprint(base) == launchFingerprint(s) {
				t.Fatal("launch change missed")
			}
		})
	}
	before := launchFingerprint(base)
	path := filepath.Join(base.Launch.Home, ".claude", "settings.json")
	os.MkdirAll(filepath.Dir(path), 0700)
	os.WriteFile(path, []byte(`{"model":"changed"}`), 0600)
	if launchFingerprint(base) == before {
		t.Fatal("native model/settings change missed")
	}
	// Claude updates project telemetry itself; that is not a launch change.
	path = filepath.Join(base.Launch.Home, ".claude.json")
	os.WriteFile(path, []byte(`{"numStartups":1,"mcpServers":{}}`), 0600)
	before = launchFingerprint(base)
	os.WriteFile(path, []byte(`{"numStartups":2,"mcpServers":{}}`), 0600)
	if launchFingerprint(base) != before {
		t.Fatal("runtime telemetry invalidates worker")
	}
}
func TestWarmConfigDefaultsAndValidation(t *testing.T) {
	c := (WarmConfig{}).defaults()
	if c.IdleTimeoutSeconds != 900 || c.MaxLifetimeSeconds != 14400 || c.MaxWarm != 8 || c.RunTimeoutSeconds != 1200 {
		t.Fatalf("defaults %+v", c)
	}
	for _, c := range []WarmConfig{{IdleTimeoutSeconds: -1}, {MaxLifetimeSeconds: -1}, {MaxWarm: -1}, {RunTimeoutSeconds: -1}} {
		if c.Validate() == nil {
			t.Fatal("negative accepted")
		}
	}
}
