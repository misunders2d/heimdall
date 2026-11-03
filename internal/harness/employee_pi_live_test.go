package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/misunders2d/heimdall/internal/core"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit opt-in: real model; no chat adapters, owner hooks, writes or sign-ins.
func TestEmployeePiLive(t *testing.T) {
	if os.Getenv("HEIMDALL_EMPLOYEE_LIVE") != "1" {
		t.Skip("explicit isolated employee model probe")
	}
	a := Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: "root"}, core.RoleEmployee: {User: "heimdall-employee", Path: "/opt/assistant/bin:/usr/bin:/bin"}}}
	l, err := a.Prepare(core.RoleEmployee, false, "pi")
	if err != nil {
		t.Fatal(err)
	}
	l.Dir, err = os.MkdirTemp(l.Home, "probe-turn-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.Dir)
	if err = os.Chown(l.Dir, int(l.Cred.Uid), int(l.Cred.Gid)); err != nil {
		t.Fatal(err)
	}
	id := NewSessionID("pi")
	defer os.RemoveAll(filepath.Join(l.Home, ".local/state/heimdall/sessions/pi", id))
	c := Caller{Role: core.RoleEmployee, Principal: core.Principal{Platform: "telegram", ID: "999000111"}, Conv: core.ConvKey{Interface: "telegram", Chat: "999000111"}, Private: true}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	prompt := os.Getenv("HEIMDALL_EMPLOYEE_PROBE_PROMPT")
	if prompt == "" {
		prompt = "Reply with exactly EMPLOYEE_MODEL_OK."
	}
	r, err := Start(ctx, Spec{Harness: "pi", Launch: l, Caller: c, Prompt: prompt, Session: Session{ID: id}, Progress: func(p Progress) {
		if p.Tool != "" {
			t.Logf("tool=%s error=%v", p.Tool, p.Error)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	res := r.Result()
	<-r.Exited()
	if res.Failure != "" || res.NeedsHuman != "" {
		t.Fatalf("failure=%s human=%s", res.Failure, res.NeedsHuman)
	}
	if os.Getenv("HEIMDALL_EMPLOYEE_BOUNDARY") == "1" {
		verifyEmployeeNativeResults(t, filepath.Join(l.Home, ".local/state/heimdall/sessions/pi", id))
	}
	if prompt == "Reply with exactly EMPLOYEE_MODEL_OK." && strings.TrimSpace(res.Answer) != "EMPLOYEE_MODEL_OK" {
		t.Fatal("model did not answer probe")
	}
	t.Log("answer:", res.Answer)
}

func verifyEmployeeNativeResults(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]string{}
	results := map[string]bool{}
	status := false
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		f, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		scan := bufio.NewScanner(f)
		scan.Buffer(make([]byte, 4096), 8<<20)
		for scan.Scan() {
			var record struct {
				Type    string
				Message struct {
					Role, ToolCallID, ToolName string
					IsError                    bool
					Content                    []struct {
						Type, ID, Name, Text string
						Arguments            struct{ Path, Server string }
					}
				}
			}
			if json.Unmarshal(scan.Bytes(), &record) != nil || record.Type != "message" {
				continue
			}
			m := record.Message
			for _, block := range m.Content {
				if block.Type == "toolCall" && block.Name == "read" {
					calls[block.ID] = block.Arguments.Path
				}
			}
			if m.Role == "toolResult" {
				if path, ok := calls[m.ToolCallID]; ok {
					results[path] = !m.IsError
				}
				if m.ToolName == "employee_mcp_status" && !m.IsError {
					for _, b := range m.Content {
						status = status || strings.Contains(b.Text, "mcp login mellanni")
					}
				}
			}
		}
		f.Close()
		if err := scan.Err(); err != nil {
			t.Fatal(err)
		}
	}
	for path, want := range map[string]bool{"/root/.pi/agent/auth.json": false, "/var/lib/heimdall-employee/.pi/agent/auth.json": false, "/opt/assistant/AGENTS.md": true, "/opt/assistant/skills/caveman/SKILL.md": true} {
		if got, ok := results[path]; !ok || got != want {
			t.Fatalf("native read result %s: observed=%v success=%v want=%v", path, ok, got, want)
		}
	}
	if !status {
		t.Fatal("no successful native Mellanni sign-in status call")
	}
	t.Log("Native records verified: credential reads denied, shared reads succeeded, Mellanni sign-in guidance returned")
}
