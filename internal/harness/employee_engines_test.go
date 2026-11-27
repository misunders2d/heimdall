package harness

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

var engineServers = []employeeServer{{Name: "mellanni", URL: "https://mcp.bezosapp.uk/mcp", Token: "CALLER_ACCESS_M"}, {Name: "linear", URL: "https://mcp.linear.app/mcp", Token: "CALLER_ACCESS_L"}}

// MEL-846: every non-Pi engine exposes the caller's own approved business
// MCP and nothing owner-only; tokens never appear on argv.
func TestEmployeeEnginesExposeCallerMCP(t *testing.T) {
	l := Launch{Home: "/home/employee", Dir: "/home/employee/turn"}
	lease := "/home/employee/.heimdall/turn-agents/x-1"
	for _, name := range []string{"claude", "codex", "omp"} {
		args, err := employeeArgs(name, l, engineServers, lease)
		if err != nil {
			t.Fatal(name, err)
		}
		got := strings.Join(args, "|")
		if strings.Contains(got, "CALLER_ACCESS") {
			t.Fatalf("%s token on argv", name)
		}
		switch name {
		case "claude":
			for _, want := range []string{"--strict-mcp-config", "--mcp-config|" + lease + "/mcp.json", "mcp__t3", "mcp__memento", "mcp__slack", "mcp__workspace"} {
				if !strings.Contains(got, want) {
					t.Errorf("claude missing %s: %s", want, got)
				}
			}
			if strings.Contains(got, "mcp__*") {
				t.Error("claude still denies approved MCP")
			}
		case "codex":
			for _, want := range []string{`cli_auth_credentials_store="file"`, "--disable|apps", "--disable|plugins", "--disable|tool_suggest", "--disable|remote_plugin", strconvQuote(lease) + `="deny"`} {
				if !strings.Contains(got, want) {
					t.Errorf("codex missing %s", want)
				}
			}
			if strings.Contains(got, "mcp_servers") {
				t.Error("codex emits a second mcp_servers table")
			}
		case "omp":
			if strings.Contains(got, "--no-mcp") || strings.Contains(got, ",mcp__*") {
				t.Error("omp MCP switched off", got)
			}
			for _, want := range []string{"mcp__t3_*", "mcp__memento_*", "mcp__slack_*", "/opt/assistant/heimdall/employee-guard.ts", "--tools|read"} {
				if !strings.Contains(got, want) {
					t.Errorf("omp missing %s", want)
				}
			}
		}
		// Nothing approved and connected: MCP stays fully off.
		args, _ = employeeArgs(name, l, nil, lease)
		got = strings.Join(args, "|")
		if name == "claude" && (!strings.Contains(got, "mcp__*") || !strings.Contains(got, `{"mcpServers":{}}`)) || name == "omp" && !strings.Contains(got, "--no-mcp") {
			t.Errorf("%s without servers keeps MCP: %s", name, got)
		}
	}
}

func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestEmployeeCodexMCPTable(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "codex")
	os.WriteFile(bin, []byte("#!/bin/sh\necho '[{\"name\":\"t3\"},{\"name\":\"mellanni\"},{\"name\":\"ambient.owner\"}]'\n"), 0700)
	args, err := employeeCodexMCPArgs(context.Background(), Launch{Path: bin, Dir: dir}, engineServers[:1])
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(args, " ")
	for _, want := range []string{`"mellanni"={enabled=true,url="https://mcp.bezosapp.uk/mcp",bearer_token_env_var="EMPLOYEE_MCP_TOKEN_MELLANNI"}`, `"t3"={enabled=false}`, `"ambient.owner"={enabled=false}`, `"memento"={enabled=false,url="https://disabled.invalid/mcp"}`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
	if strings.Count(got, "mcp_servers=") != 1 || strings.Count(got, `"mellanni"=`) != 1 || strings.Contains(got, "CALLER_ACCESS") {
		t.Fatal(got)
	}
}

func TestApprovedMCPTool(t *testing.T) {
	t.Setenv("HEIMDALL_EMPLOYEE_MCP", `["mellanni"]`)
	for tool, want := range map[string]bool{"mcp__mellanni__search": true, "mcp__mellanni__": false, "mcp__linear__list": false, "mcp__t3__launch": false, "mcp__mellannix__a": false, "mcp__memento__recall": false} {
		if approvedMCPTool(tool, "__") != want {
			t.Errorf("%s: want %t", tool, want)
		}
	}
	if !approvedMCPTool("mcp__mellanni_search", "_") {
		t.Error("omp naming")
	}
	t.Setenv("HEIMDALL_EMPLOYEE_MCP", `["t3","memento"]`) // never approvable
	if approvedMCPTool("mcp__t3__launch", "__") || approvedMCPTool("mcp__memento__recall", "__") {
		t.Fatal("owner server admitted")
	}
	t.Setenv("HEIMDALL_EMPLOYEE_MCP", `not json`)
	if approvedMCPTool("mcp__mellanni__search", "__") {
		t.Fatal("malformed list admitted")
	}
	// The Claude hook admits approved MCP, still denies everything else.
	t.Setenv("HEIMDALL_EMPLOYEE_MCP", `["mellanni"]`)
	for tool, want := range map[string]string{"mcp__mellanni__search": "allow", "mcp__claude_ai_Gmail__send": "deny", "mcp__t3__launch": "deny", "Bash": "deny"} {
		var out bytes.Buffer
		EmployeeGuard(strings.NewReader(`{"tool_name":"`+tool+`","tool_input":{}}`), &out)
		if !strings.Contains(out.String(), `"permissionDecision":"`+want+`"`) {
			t.Errorf("%s: %s", tool, out.String())
		}
	}
}

func TestEmployeeEnvDisablesConnectors(t *testing.T) {
	env := employeeEnv([]string{"MEMENTO_PROCESSING=0"}, Caller{Role: core.RoleEmployee}, engineServers[:1])
	if envValue(env, "ENABLE_CLAUDEAI_MCP_SERVERS") != "false" || envValue(env, "HEIMDALL_EMPLOYEE_MCP") != `["mellanni"]` || envValue(env, "MEMENTO_PROCESSING") != "1" {
		t.Fatal(env)
	}
}

// leaseLaunch is an employee launch under the test's own uid, so the lease
// code's chown/ownership checks run for real without root.
func leaseLaunch(t *testing.T) (Launch, string) {
	t.Helper()
	home := t.TempDir()
	owner := t.TempDir()
	old := ownerHome
	ownerHome = owner
	t.Cleanup(func() { ownerHome = old })
	l := Launch{Home: home, Dir: filepath.Join(home, ".local", "state", "heimdall", "turns", "t1"), Cred: &syscall.Credential{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()), NoSetGroups: true}}
	os.MkdirAll(l.Dir, 0700)
	dir := filepath.Join(home, "lease")
	os.Mkdir(dir, 0700)
	return l, dir
}

func readJSONFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Lstat(path); st.Mode().Perm() != 0600 {
		t.Fatalf("%s mode %v", path, st.Mode())
	}
	return string(raw)
}

func TestClaudeLeaseAccessOnly(t *testing.T) {
	l, dir := leaseLaunch(t)
	now := time.Now()
	os.MkdirAll(filepath.Join(ownerHome, ".claude"), 0700)
	cred, _ := json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "OWNER_ACCESS", "refreshToken": "OWNER_REFRESH", "expiresAt": now.Add(2 * time.Hour).UnixMilli(), "scopes": []string{"user:inference"}}})
	os.WriteFile(filepath.Join(ownerHome, ".claude", ".credentials.json"), cred, 0600)
	sess := Session{ID: "0b3c9e1a-1111-2222-3333-444455556666", Resume: true}
	env, err := claudeLease(dir, l, sess, nil, engineServers, now)
	if err != nil {
		t.Fatal(err)
	}
	got := readJSONFile(t, filepath.Join(dir, ".credentials.json"))
	if strings.Contains(got, "OWNER_REFRESH") || !strings.Contains(got, "OWNER_ACCESS") {
		t.Fatal("lease carries renewal material", got)
	}
	mcp := readJSONFile(t, filepath.Join(dir, "mcp.json"))
	if !strings.Contains(mcp, "Bearer CALLER_ACCESS_M") || !strings.Contains(mcp, "https://mcp.linear.app/mcp") || strings.Contains(mcp, "t3") {
		t.Fatal(mcp)
	}
	if envValue(env, "CLAUDE_CONFIG_DIR") != dir || envValue(env, "ENABLE_CLAUDEAI_MCP_SERVERS") != "false" {
		t.Fatal(env)
	}
	if target, _ := os.Readlink(filepath.Join(dir, "CLAUDE.md")); target != SharedRoot+"/AGENTS.md" {
		t.Fatal(target)
	}
	link := filepath.Join(dir, "projects", claudeProjectSlug(l.Dir))
	if target, _ := os.Readlink(link); target != filepath.Join(l.Home, ".claude", "heimdall-sessions", sess.ID) {
		t.Fatal("session store link", target)
	}
	// An owner token that could expire mid-turn is never leased.
	cred, _ = json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "OWNER_ACCESS", "expiresAt": now.Add(10 * time.Minute).UnixMilli()}})
	os.WriteFile(filepath.Join(ownerHome, ".claude", ".credentials.json"), cred, 0600)
	if _, err = claudeLease(t.TempDir(), l, Session{}, nil, nil, now); err == nil {
		t.Fatal("near-expiry owner token leased")
	}
}

func fakeJWT(exp time.Time) string {
	claims, _ := json.Marshal(map[string]any{"exp": exp.Unix()})
	return "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".sig"
}

func TestCodexLeaseAccessOnly(t *testing.T) {
	l, dir := leaseLaunch(t)
	now := time.Now()
	os.MkdirAll(filepath.Join(ownerHome, ".codex"), 0700)
	auth, _ := json.Marshal(map[string]any{"auth_mode": "chatgpt", "tokens": map[string]any{"id_token": fakeJWT(now.Add(time.Hour)), "access_token": fakeJWT(now.Add(2 * time.Hour)), "refresh_token": "OWNER_REFRESH", "account_id": "acct"}})
	os.WriteFile(filepath.Join(ownerHome, ".codex", "auth.json"), auth, 0600)
	env, err := codexLease(dir, l, nil, engineServers[:1], now)
	if err != nil {
		t.Fatal(err)
	}
	got := readJSONFile(t, filepath.Join(dir, "auth.json"))
	if strings.Contains(got, "OWNER_REFRESH") || !strings.Contains(got, `"refresh_token":""`) {
		t.Fatal(got)
	}
	if envValue(env, "CODEX_HOME") != dir || envValue(env, "EMPLOYEE_MCP_TOKEN_MELLANNI") != "CALLER_ACCESS_M" || envValue(env, "EMPLOYEE_MCP_TOKEN_LINEAR") != "" {
		t.Fatal(env)
	}
	if target, _ := os.Readlink(filepath.Join(dir, "sessions")); target != filepath.Join(l.Home, ".codex", "sessions") {
		t.Fatal(target)
	}
	auth, _ = json.Marshal(map[string]any{"tokens": map[string]any{"id_token": "x", "access_token": fakeJWT(now.Add(5 * time.Minute))}})
	os.WriteFile(filepath.Join(ownerHome, ".codex", "auth.json"), auth, 0600)
	if _, err = codexLease(t.TempDir(), l, nil, nil, now); err == nil {
		t.Fatal("near-expiry owner token leased")
	}
}

func TestOMPLeaseAccessOnly(t *testing.T) {
	l, dir := leaseLaunch(t)
	now := time.Now()
	agent := filepath.Join(ownerHome, ".pi", "agent")
	os.MkdirAll(agent, 0700)
	raw, _ := json.Marshal(map[string]employeeAccess{"openai-codex": {Type: "oauth", Access: "OWNER_ACCESS", Refresh: "OWNER_REFRESH", Expires: now.Add(time.Hour).UnixMilli(), AccountID: "acct"}})
	os.WriteFile(filepath.Join(agent, "auth.json"), raw, 0600)
	os.WriteFile(filepath.Join(agent, "settings.json"), []byte(`{"defaultProvider":"openai-codex","defaultModel":"fixture"}`), 0600)
	// A stand-in for `omp config list`, which creates OMP's own schema.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "agent.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE auth_credentials (id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, credential_type TEXT NOT NULL, data TEXT NOT NULL)")
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	l.Path = filepath.Join(t.TempDir(), "omp")
	os.WriteFile(l.Path, []byte("#!/bin/sh\n[ \"$1 $2\" = \"config list\" ] && [ -n \"$PI_CODING_AGENT_DIR\" ]\n"), 0700)
	env, args, err := ompLease(context.Background(), dir, l, nil, engineServers[:1], now)
	if err != nil {
		t.Fatal(err)
	}
	if envValue(env, "PI_CODING_AGENT_DIR") != dir || strings.Join(args, " ") != "--model openai-codex/fixture" {
		t.Fatal(env, args)
	}
	db, _ = sql.Open("sqlite", "file:"+filepath.Join(dir, "agent.db"))
	var data string
	db.QueryRow("SELECT data FROM auth_credentials WHERE provider='openai-codex'").Scan(&data)
	db.Close()
	if !strings.Contains(data, "OWNER_ACCESS") || strings.Contains(data, "OWNER_REFRESH") || !strings.Contains(data, `"refresh":""`) {
		t.Fatal(data)
	}
	if mcp := readJSONFile(t, filepath.Join(dir, "mcp.json")); !strings.Contains(mcp, "Bearer CALLER_ACCESS_M") {
		t.Fatal(mcp)
	}
	// A planted agent.db symlink is refused.
	dir2 := filepath.Join(l.Home, "lease2")
	os.Mkdir(dir2, 0700)
	os.Symlink(filepath.Join(ownerHome, ".pi", "agent", "auth.json"), filepath.Join(dir2, "agent.db"))
	if _, _, err = ompLease(context.Background(), dir2, l, nil, nil, now); err == nil {
		t.Fatal("symlinked agent.db accepted")
	}
}

func TestEmployeeRuntimePreflight(t *testing.T) {
	root := t.TempDir()
	os.Chmod(filepath.Dir(root), 0755)
	os.Chmod(root, 0755)
	bin := filepath.Join(root, "engine")
	os.WriteFile(bin, []byte("#!/bin/sh\n"), 0755)
	if err := employeeRuntimePreflight("claude", bin); err != nil {
		t.Fatal(err)
	}
	// A harness upgrade that leaves its install 0700 (the 2026-11-26 Pi case).
	hidden := filepath.Join(root, "pkg")
	os.Mkdir(hidden, 0700)
	os.WriteFile(filepath.Join(hidden, "engine"), []byte("#!/bin/sh\n"), 0755)
	err := employeeRuntimePreflight("omp", filepath.Join(hidden, "engine"))
	if err == nil || !strings.Contains(err.Error(), hidden) || !strings.Contains(err.Error(), "employee-layout.sh install") {
		t.Fatal(err)
	}
	os.Chmod(bin, 0700)
	if err := employeeRuntimePreflight("claude", bin); err == nil {
		t.Fatal("owner-only binary accepted")
	}
}

func TestLeasedEmployeesAreNeverWarm(t *testing.T) {
	cred := &syscall.Credential{Uid: 997, Gid: 997}
	for _, tc := range []struct {
		name string
		role core.Role
		cred *syscall.Credential
		warm bool
	}{{"omp", core.RoleEmployee, cred, false}, {"claude", core.RoleEmployee, cred, false}, {"codex", core.RoleEmployee, cred, false}, {"pi", core.RoleEmployee, nil, false}, {"claude", core.RoleOwner, cred, true}, {"claude", core.RoleEmployee, nil, true}, {"agy", core.RoleOwner, nil, false}} {
		if got := warmable(Spec{Harness: tc.name, Caller: Caller{Role: tc.role}, Launch: Launch{Cred: tc.cred}}); got != tc.warm {
			t.Errorf("%+v: warm=%t", tc, got)
		}
	}
}
