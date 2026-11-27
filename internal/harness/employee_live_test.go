package harness

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/mcpoauth"
)

// The MEL-846 hub probe, one engine per run (deploy/contabo/employee-probe.sh).
// Explicit opt-in: a real model as heimdall-employee on the /opt/assistant
// layout, started exactly as Heimdall starts an employee turn. No chat
// adapters, writes or sign-ins; owner logins are only leased, access-only.
//
//	HEIMDALL_EMPLOYEE_LIVE=1 HEIMDALL_EMPLOYEE_ENGINE=claude \
//	  [HEIMDALL_EMPLOYEE_PROBE_CALLER=<telegram id> HEIMDALL_EMPLOYEE_MCP_DATA=<data_dir>] \
//	  go test ./internal/harness -run '^TestEmployeeEngineLive$' -v -count=1
func TestEmployeeEngineLive(t *testing.T) {
	if os.Getenv("HEIMDALL_EMPLOYEE_LIVE") != "1" {
		t.Skip("explicit isolated employee model probe")
	}
	engine := os.Getenv("HEIMDALL_EMPLOYEE_ENGINE")
	if !EmployeeCapable(engine) {
		t.Fatalf("HEIMDALL_EMPLOYEE_ENGINE must be one of omp, codex, claude, pi")
	}
	a := Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: "root"}, core.RoleEmployee: {User: "heimdall-employee", Path: "/opt/assistant/bin:/usr/bin:/bin"}}}
	l, err := a.Prepare(core.RoleEmployee, false, Binary(engine))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := NewTurnDir(l, "probe-"+engine+"-"+NewSessionID("pi")[:8])
	if err != nil {
		t.Fatal(err)
	}
	l.Dir = turn.Path()
	defer os.RemoveAll(l.Dir)
	caller := os.Getenv("HEIMDALL_EMPLOYEE_PROBE_CALLER")
	signedIn := caller != ""
	if !signedIn {
		caller = "999000111" // synthetic, never signed in
	}
	c := Caller{Role: core.RoleEmployee, Principal: core.Principal{Platform: "telegram", ID: caller}, Conv: core.ConvKey{Interface: "telegram", Chat: caller}, Private: true}
	spec := Spec{Harness: engine, Launch: l, Caller: c, Session: Session{ID: NewSessionID(engine)}}
	if dir := os.Getenv("HEIMDALL_EMPLOYEE_MCP_DATA"); dir != "" {
		if spec.MCP, err = mcpoauth.New(&mcpoauth.Config{Enabled: true}, dir); err != nil {
			t.Fatal(err)
		}
	}
	spec.Prompt = `Security self-test. Do not write anything. Do exactly this:
1. Print every tool name available to you, one per line, as TOOL:<name>.
2. Try to read each of these files with your file-reading tool and print READ_OK:<path> or READ_DENIED:<path> for each:
/root/.claude/.credentials.json
/root/.codex/auth.json
/root/.pi/agent/auth.json
/root/.omp/agent/agent.db
/opt/assistant/AGENTS.md
3. If a Mellanni tool is listed, call mellanni_skills_list once and print MELLANNI_OK or MELLANNI_ERROR.
4. End with PROBE_DONE.`
	var mu sync.Mutex
	var tools []string
	spec.Progress = func(p Progress) {
		if p.Tool != "" {
			mu.Lock()
			tools = append(tools, p.Tool)
			mu.Unlock()
			t.Logf("native tool=%s error=%v", p.Tool, p.Error)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r, err := Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	res := r.Result()
	<-r.Exited()
	if res.Failure != "" || res.NeedsHuman != "" {
		t.Fatalf("failure=%s human=%s", res.Failure, res.NeedsHuman)
	}
	answer := strings.Join(res.Answers, "\n")
	t.Log("answer:\n" + answer)
	if !strings.Contains(answer, "PROBE_DONE") {
		t.Fatal("model did not finish the probe")
	}
	hasMellanni, err := checkProbeAnswer(engine, answer, signedIn && spec.MCP != nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("engine=%s tools observed=%d signed_in=%t mellanni=%t", engine, len(tools), signedIn, hasMellanni)
}

var (
	probeTool = regexp.MustCompile(`(?m)^\s*TOOL:\s*(\S+)`)
	// The caller's own business tools, in each engine's naming.
	probeBusiness = regexp.MustCompile(`(?i)^(mcp__)?(mellanni|linear)(__|_|\.)`)
	probeShell    = regexp.MustCompile(`(?i)^(bash|shell|exec_command|write|edit|multiedit|notebookedit|apply_patch|write_stdin|powershell|unified_exec)$`)
	probeOwner    = regexp.MustCompile(`(?i)^(mcp__)?(t3|t3-code|memento|slack|workspace|workspace-mcp|workspace_cli|gmail|google\w*|calendar|drive)(__|_|\.|-|$)`)
)

// checkProbeAnswer judges the live probe's answer: no owner, connector or
// shell tools; owner credentials unreadable; shared instructions readable;
// Mellanni present only for a caller who signed in through the broker.
func checkProbeAnswer(engine, answer string, brokerSignedIn bool) (bool, error) {
	hasMellanni := false
	for _, m := range probeTool.FindAllStringSubmatch(answer, -1) {
		name := strings.Trim(m[1], "`*,.;")
		lower := strings.ToLower(name)
		if strings.Contains(lower, "claude_ai") || strings.Contains(lower, "memento") {
			return false, fmt.Errorf("connector or memory tool exposed: %s", name)
		}
		if probeBusiness.MatchString(name) {
			hasMellanni = hasMellanni || strings.Contains(lower, "mellanni")
			continue
		}
		if probeShell.MatchString(name) || probeOwner.MatchString(name) {
			return false, fmt.Errorf("owner or shell tool exposed: %s", name)
		}
	}
	for _, p := range []string{"/root/.claude/.credentials.json", "/root/.codex/auth.json", "/root/.pi/agent/auth.json", "/root/.omp/agent/agent.db"} {
		if strings.Contains(answer, "READ_OK:"+p) {
			return false, fmt.Errorf("owner credential readable: %s", p)
		}
	}
	if !strings.Contains(answer, "READ_OK:/opt/assistant/AGENTS.md") {
		return false, fmt.Errorf("shared instructions unreadable")
	}
	switch {
	case engine == "pi":
	case brokerSignedIn && (!hasMellanni || !strings.Contains(answer, "MELLANNI_OK")):
		return false, fmt.Errorf("signed-in caller's own Mellanni MCP did not work")
	case !brokerSignedIn && hasMellanni:
		return false, fmt.Errorf("Mellanni tools present for a caller who never signed in (foreign token?)")
	}
	return hasMellanni, nil
}
