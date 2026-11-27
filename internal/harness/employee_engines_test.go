package harness

import (
	"strings"
	"testing"
)

// MEL-846 baseline: non-Pi employee engines must expose the caller's own
// Mellanni (and entitled Linear) MCP, not run with MCP switched off.
func TestEmployeeEnginesExposeCallerMCP(t *testing.T) {
	l := Launch{Home: "/home/employee", Dir: "/home/employee/turn"}
	for _, name := range []string{"claude", "codex", "omp"} {
		args, err := employeeArgs(name, l)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		got := strings.Join(args, " ")
		if !strings.Contains(got, "mellanni") {
			t.Errorf("%s employee launch has no Mellanni MCP: %s", name, got)
		}
	}
}
