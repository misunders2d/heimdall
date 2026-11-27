package harness

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/mcpoauth"

	_ "modernc.org/sqlite" // OMP keeps logins in its agent.db
)

// MEL-846: Claude, Codex and OMP employee turns get the same shape of access
// as employee Pi: an access-only lease of the owner's model login (never a
// refresh token) and the caller's own access-only business MCP tokens, in a
// disposable 0700 directory removed when the native process exits. The model
// can read nothing there: readers are confined to /opt/assistant and the turn.

// ownerHome is where the owner's native logins live (root in production).
var ownerHome = "/root"

// employeeLeaseLimit is the model lease and employee process limit.
const employeeLeaseLimit = 25 * time.Minute

// employeeMCPAccess resolves the caller's approved servers and fetches their
// access-only tokens from the root broker. Unconnected servers are left out;
// the model then has no such tools and guides the caller to `mcp login`.
func employeeMCPAccess(ctx context.Context, spec Spec) []employeeServer {
	approved := EmployeeMCPServers(spec.Caller)
	if spec.MCP == nil || len(approved) == 0 {
		return nil
	}
	c := spec.Caller
	got, failed := spec.MCP.Snapshot(ctx, mcpoauth.Caller{Principal: c.Principal, Actor: c.OAuthActor, Installation: c.OAuthInstallation}, approved)
	var out []employeeServer
	for _, name := range approved {
		if err := failed[name]; err != nil && spec.Log != nil {
			spec.Log("employee MCP " + name + " unavailable: " + err.Error())
		}
		if a, ok := got[name]; ok {
			out = append(out, employeeServer{Name: name, URL: mcpoauth.Servers[name], Token: a.Token})
		}
	}
	return out
}

// newEmployeeLease makes the disposable directory, root-created then handed
// to the employee uid without following a planted symlink.
func newEmployeeLease(l Launch, engine string) (string, func(), error) {
	if os.Getuid() != 0 || l.Cred == nil || l.Cred.Uid == 0 || l.Home == ownerHome || l.Home == "/root" {
		return "", nil, errors.New("employee " + engine + " requires the root broker and a separate non-root uid")
	}
	parent := filepath.Join(l.Home, ".heimdall", "turn-agents")
	if err := privateMCPDir(parent, l); err != nil {
		return "", nil, errors.New("employee agent root unavailable")
	}
	dir, err := os.MkdirTemp(parent, engine+"-")
	if err != nil {
		return "", nil, err
	}
	if err = chownEmployeeAgent(dir, l); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	return dir, func() { os.RemoveAll(dir) }, nil
}

// linkEmployee makes an employee-owned symlink without following one.
func linkEmployee(target, link string, l Launch) error {
	if err := os.Symlink(target, link); err != nil {
		return err
	}
	return os.Lchown(link, int(l.Cred.Uid), int(l.Cred.Gid))
}

// prepareEmployeeLease returns the turn environment, extra native arguments,
// the lease directory and its cleanup.
func prepareEmployeeLease(ctx context.Context, spec Spec, env []string, servers []employeeServer) ([]string, []string, string, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, "", nil, err
	}
	l := spec.Launch
	dir, cleanup, err := newEmployeeLease(l, spec.Harness)
	if err != nil {
		return nil, nil, "", nil, err
	}
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	var args []string
	switch spec.Harness {
	case "claude":
		env, err = claudeLease(dir, l, spec.Session, env, servers, time.Now())
	case "codex":
		env, err = codexLease(dir, l, env, servers, time.Now())
	case "omp":
		env, args, err = ompLease(ctx, dir, l, env, servers, time.Now())
	default:
		err = errors.New("employee harness unsupported")
	}
	if err != nil {
		return nil, nil, "", nil, err
	}
	ok = true
	return env, args, dir, cleanup, nil
}

func mcpCatalog(servers []employeeServer, typ string) map[string]any {
	catalog := map[string]any{}
	for _, s := range servers {
		catalog[s.Name] = map[string]any{"type": typ, "url": s.URL, "headers": map[string]string{"Authorization": "Bearer " + s.Token}}
	}
	return map[string]any{"mcpServers": catalog}
}

var claudeSessionID = regexp.MustCompile(`^[A-Za-z0-9-]{8,64}$`)

// claudeProjectSlug is Claude Code's per-cwd transcript directory name.
func claudeProjectSlug(dir string) string {
	return regexp.MustCompile(`[^A-Za-z0-9]`).ReplaceAllString(dir, "-")
}

func claudeLease(dir string, l Launch, sess Session, env []string, servers []employeeServer, now time.Time) ([]string, error) {
	var owner struct {
		OAuth struct {
			Access       string   `json:"accessToken"`
			Expires      int64    `json:"expiresAt"`
			Scopes       []string `json:"scopes"`
			Subscription string   `json:"subscriptionType"`
		} `json:"claudeAiOauth"`
	}
	raw, err := os.ReadFile(filepath.Join(ownerHome, ".claude", ".credentials.json"))
	if err != nil || json.Unmarshal(raw, &owner) != nil || owner.OAuth.Access == "" {
		return nil, errors.New("employee Claude: owner model access unavailable")
	}
	if owner.OAuth.Expires < now.Add(employeeLeaseLimit+5*time.Minute).UnixMilli() {
		return nil, errors.New("employee Claude: owner access token needs renewal by the owner Claude runtime")
	}
	// Access only: no refresh token. The real expiry stays, so Claude never
	// tries an early refresh it cannot perform.
	cred := map[string]any{"claudeAiOauth": map[string]any{"accessToken": owner.OAuth.Access, "refreshToken": "", "expiresAt": owner.OAuth.Expires, "scopes": owner.OAuth.Scopes, "subscriptionType": owner.OAuth.Subscription}}
	if err = writeEmployeeJSON(dir, ".credentials.json", cred, l); err != nil {
		return nil, err
	}
	if len(servers) > 0 {
		if err = writeEmployeeJSON(dir, "mcp.json", mcpCatalog(servers, "http"), l); err != nil {
			return nil, err
		}
	}
	for name, target := range map[string]string{"CLAUDE.md": SharedRoot + "/AGENTS.md", "skills": SharedRoot + "/skills"} {
		if err = linkEmployee(target, filepath.Join(dir, name), l); err != nil {
			return nil, err
		}
	}
	// Transcripts outlive the lease: each turn's new cwd points at the
	// session's persistent directory, so --resume finds it.
	if sess.ID != "" {
		if !claudeSessionID.MatchString(sess.ID) {
			return nil, errors.New("employee Claude: invalid session ID")
		}
		store := filepath.Join(l.Home, ".claude", "heimdall-sessions", sess.ID)
		if err = privateMCPDir(store, l); err != nil {
			return nil, errors.New("employee Claude session store unavailable")
		}
		projects := filepath.Join(dir, "projects")
		if err = os.Mkdir(projects, 0700); err != nil {
			return nil, err
		}
		if err = os.Lchown(projects, int(l.Cred.Uid), int(l.Cred.Gid)); err != nil {
			return nil, err
		}
		if err = linkEmployee(store, filepath.Join(projects, claudeProjectSlug(l.Dir)), l); err != nil {
			return nil, err
		}
	}
	env = setEnv(env, "CLAUDE_CONFIG_DIR", dir)
	return setEnv(env, "ENABLE_CLAUDEAI_MCP_SERVERS", "false"), nil
}

// jwtExpiry reads a JWT's exp claim without trusting it for anything else.
func jwtExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err != nil || json.Unmarshal(raw, &claims) != nil || claims.Exp == 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0), true
}

func codexLease(dir string, l Launch, env []string, servers []employeeServer, now time.Time) ([]string, error) {
	var owner struct {
		Mode   string `json:"auth_mode"`
		Tokens struct {
			ID      string `json:"id_token"`
			Access  string `json:"access_token"`
			Account string `json:"account_id"`
		} `json:"tokens"`
	}
	raw, err := os.ReadFile(filepath.Join(ownerHome, ".codex", "auth.json"))
	if err != nil || json.Unmarshal(raw, &owner) != nil || owner.Tokens.Access == "" || owner.Tokens.ID == "" {
		return nil, errors.New("employee Codex: owner model access unavailable")
	}
	if exp, ok := jwtExpiry(owner.Tokens.Access); !ok || exp.Before(now.Add(employeeLeaseLimit+5*time.Minute)) {
		return nil, errors.New("employee Codex: owner access token needs renewal by the owner Codex runtime")
	}
	// Access only. A fresh last_refresh keeps Codex from attempting a refresh.
	auth := map[string]any{"OPENAI_API_KEY": nil, "tokens": map[string]any{"id_token": owner.Tokens.ID, "access_token": owner.Tokens.Access, "refresh_token": "", "account_id": owner.Tokens.Account}, "last_refresh": now.UTC().Format(time.RFC3339)}
	if owner.Mode != "" {
		auth["auth_mode"] = owner.Mode
	}
	if err = writeEmployeeJSON(dir, "auth.json", auth, l); err != nil {
		return nil, err
	}
	if err = linkEmployee(SharedRoot+"/AGENTS.md", filepath.Join(dir, "AGENTS.md"), l); err != nil {
		return nil, err
	}
	// Rollouts outlive the lease so thread/resume finds them.
	sessions := filepath.Join(l.Home, ".codex", "sessions")
	if err = privateMCPDir(sessions, l); err != nil {
		return nil, errors.New("employee Codex session store unavailable")
	}
	if err = linkEmployee(sessions, filepath.Join(dir, "sessions"), l); err != nil {
		return nil, err
	}
	env = setEnv(env, "CODEX_HOME", dir)
	for _, s := range servers {
		env = setEnv(env, codexTokenEnv(s.Name), s.Token)
	}
	return env, nil
}

// ompLease seeds a disposable OMP agent directory. OMP creates its own
// agent.db schema (as the employee uid); Heimdall then adds the one
// access-only openai-codex login, the same lease employee Pi gets.
func ompLease(ctx context.Context, dir string, l Launch, env []string, servers []employeeServer, now time.Time) ([]string, []string, error) {
	auth, settings, err := employeePiSnapshot(filepath.Join(ownerHome, ".pi", "agent"), now)
	if err != nil {
		return nil, nil, errors.New(strings.Replace(err.Error(), "employee Pi", "employee OMP", 1))
	}
	env = setEnv(env, "PI_CODING_AGENT_DIR", dir)
	initCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(initCtx, l.Path, "config", "list")
	cmd.Dir, cmd.Env = l.Dir, env
	setProcAttr(cmd, l.Cred)
	if cmd.Run() != nil {
		return nil, nil, errors.New("employee OMP: agent directory initialisation failed")
	}
	db := filepath.Join(dir, "agent.db")
	if st, err := os.Lstat(db); err != nil || !st.Mode().IsRegular() || !ownedOnce(st, l.Cred.Uid) {
		return nil, nil, errors.New("employee OMP: agent database is unsafe")
	}
	data, _ := json.Marshal(map[string]any{"access": auth.Access, "refresh": "", "expires": auth.Expires, "accountId": auth.AccountID})
	conn, err := sql.Open("sqlite", "file:"+db+"?_pragma=busy_timeout(5000)")
	if err == nil {
		_, err = conn.ExecContext(ctx, "INSERT INTO auth_credentials(provider, credential_type, data) VALUES('openai-codex', 'oauth', ?)", string(data))
		conn.Close()
	}
	if err != nil {
		return nil, nil, errors.New("employee OMP: model lease could not be written")
	}
	if len(servers) > 0 {
		if err = writeEmployeeJSON(dir, "mcp.json", mcpCatalog(servers, "http"), l); err != nil {
			return nil, nil, err
		}
	}
	for name, target := range map[string]string{"AGENTS.md": SharedRoot + "/AGENTS.md", "skills": SharedRoot + "/skills"} {
		if err = linkEmployee(target, filepath.Join(dir, name), l); err != nil {
			return nil, nil, err
		}
	}
	var args []string
	if m, _ := settings["defaultModel"].(string); m != "" {
		args = append(args, "--model", "openai-codex/"+m)
	}
	return env, args, nil
}

// employeeRuntimePreflight fails a turn whose engine the employee uid could
// not load, naming the path and the fix, instead of a native crash such as
// "Cannot find module 'jiti'". It never changes permissions itself.
func employeeRuntimePreflight(engine, path string) error {
	paths := []string{path}
	if engine == "pi" {
		paths = append(paths, SharedRoot+"/runtime/node", SharedRoot+"/runtime/pi", SharedRoot+"/runtime/pi/dist", SharedRoot+"/runtime/pi/node_modules")
	}
	for _, p := range paths {
		if err := worldReadable(p); err != nil {
			return fmt.Errorf("employee %s runtime %s is not readable by the employee account (%v); as root rerun deploy/contabo/employee-layout.sh install after every harness upgrade", engine, p, err)
		}
	}
	return nil
}

// worldReadable: every ancestor searchable by others, the target readable
// (and searchable/executable) by others, after resolving symlinks.
func worldReadable(path string) error {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return errors.New("missing")
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(real), "/"), "/") {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		st, err := os.Stat(current)
		if err != nil || st.Mode().Perm()&0001 == 0 {
			return fmt.Errorf("%s is not world-searchable", current)
		}
	}
	st, err := os.Stat(real)
	if err != nil {
		return errors.New("missing")
	}
	need := os.FileMode(0004)
	if st.IsDir() || st.Mode().Perm()&0100 != 0 {
		need |= 0001
	}
	if st.Mode().Perm()&need != need {
		return fmt.Errorf("mode %04o", st.Mode().Perm())
	}
	return nil
}

func ownedOnce(st os.FileInfo, uid uint32) bool {
	stat, ok := st.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uid && stat.Nlink == 1
}
