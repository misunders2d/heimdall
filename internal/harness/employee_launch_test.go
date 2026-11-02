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

func TestFakeEmployeePolicyAndOwnerSeparation(t *testing.T) {
	bin, _ := fakeharness.Install(t, "pi", "claude", "codex", "agy")
	for _, name := range []string{"pi", "claude", "codex", "agy"} {
		t.Run(name, func(t *testing.T) {
			l := launch(t, bin, name, true)
			capture := filepath.Join(t.TempDir(), "capture.json")
			l.Env = append(l.Env, "FAKE_CAPTURE_LAUNCH="+capture)
			c := caller
			c.Capabilities = []string{"mcp.linear"}
			r, e := harness.Start(context.Background(), harness.Spec{Harness: name, Caller: c, Launch: l, Prompt: "synthetic", Session: harness.Session{ID: harness.NewSessionID(name)}})
			if name == "agy" {
				if e == nil {
					r.Cancel()
					t.Fatal("employee AGY launched")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			res := r.Result()
			<-r.Exited()
			if res.Failure != "" {
				t.Fatal(res)
			}
			b, e := os.ReadFile(capture)
			if e != nil {
				t.Fatal(e)
			}
			var got struct {
				Args                        []string
				Memory, Restricted, Servers string
			}
			if e = json.Unmarshal(b, &got); e != nil {
				t.Fatal(e)
			}
			if got.Memory != "1" || got.Restricted != "1" {
				t.Fatal("employee memory/role missing", got)
			}
			args := strings.Join(got.Args, " ")
			switch name {
			case "pi":
				if got.Servers != "[]" || !strings.Contains(args, "--tools read") {
					t.Fatal(got)
				}
			case "claude":
				if strings.Count(args, "--permission-mode") != 1 || !strings.Contains(args, "--restricted") || !strings.Contains(args, "--strict-mcp-config") || !strings.Contains(args, "Skill,Read,WebSearch") {
					t.Fatal(got)
				}
			case "codex":
				if !strings.Contains(args, `"unexpected-owner-server"={enabled=false}`) || !strings.Contains(args, `approval_policy="never"`) {
					t.Fatal(got)
				}
			}
			// Native owner launch does not inherit employee restrictions.
			c.Role = core.RoleOwner
			r, e = harness.Start(context.Background(), harness.Spec{Harness: name, Caller: c, Launch: l, Prompt: "synthetic", Session: harness.Session{ID: harness.NewSessionID(name)}})
			if e != nil {
				t.Fatal(e)
			}
			res = r.Result()
			<-r.Exited()
			if res.Failure != "" {
				t.Fatal(res)
			}
			b, e = os.ReadFile(capture)
			if e != nil {
				t.Fatal(e)
			}
			if e = json.Unmarshal(b, &got); e != nil {
				t.Fatal(e)
			}
			if got.Restricted != "0" || strings.Contains(strings.Join(got.Args, " "), "--tools read") || strings.Contains(strings.Join(got.Args, " "), "default_permissions") {
				t.Fatal("owner restricted", got)
			}
		})
	}
}
