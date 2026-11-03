package harness

import (
	"bytes"
	"github.com/misunders2d/heimdall/internal/core"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmployeeLaunchPolicy(t *testing.T) {
	l := Launch{Home: "/home/employee", Dir: "/home/employee/turn"}
	for name, wants := range map[string][]string{
		"pi":     {"--no-extensions", "builtin:mcp", "--no-builtin-tools", "--tools|read,employee_mcp_status,mcp__mellanni__*,mcp__linear__*"},
		"claude": {"--restricted", "Read(//root/**)", `"onFailure":"block"`, "Skill,Read,WebSearch", "--strict-mcp-config", `"disableSkillShellExecution":true`, `"autoMemoryEnabled":false`, "--permission-mode|dontAsk"},
		"codex":  {`default_permissions="employee"`, `approval_policy="never"`, `"/home/employee/.codex"="deny"`, "--disable|hooks", "--disable|memories", "mcp_servers={}"},
	} {
		args, e := employeeArgs(name, l)
		if e != nil {
			t.Fatal(e)
		}
		got := strings.Join(args, "|")
		for _, want := range wants {
			if !strings.Contains(got, want) {
				t.Errorf("%s missing %s", name, want)
			}
		}
	}
	if _, e := employeeArgs("agy", l); e == nil {
		t.Fatal("employee AGY enabled")
	}
	env := employeeEnv([]string{"MEMENTO_PROCESSING=0"}, Caller{Role: core.RoleEmployee, Capabilities: []string{"scheduler"}})
	if envValue(env, "MEMENTO_PROCESSING") != "1" || envValue(env, "PI_RESTRICTED_EXTERNAL_USER") != "1" {
		t.Fatal(env)
	}
	// Owner launch environment stays untouched.
	if got := employeeEnv([]string{"MEMENTO_PROCESSING=0"}, Caller{Role: core.RoleOwner}); got[0] != "MEMENTO_PROCESSING=0" {
		t.Fatal(got)
	}
}
func TestEmployeeReaderSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "auth.json")
	if e := os.WriteFile(outside, []byte("synthetic"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(outside, filepath.Join(root, "link")); e != nil {
		t.Fatal(e)
	}
	if ReadWithin(filepath.Join(root, "link"), root) {
		t.Fatal("symlink escaped")
	}
	t.Setenv("HEIMDALL_TURN_DIR", root)
	for _, input := range []string{`{}`, `{"tool_name":"Bash"}`, `{"tool_name":"Read","tool_input":{"file_path":"` + outside + `"}}`} {
		var out bytes.Buffer
		if e := EmployeeGuard(strings.NewReader(input), &out); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(out.String(), `"permissionDecision":"deny"`) {
			t.Fatal(out.String())
		}
	}
}
