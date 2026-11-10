package harness

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// checkerRig is a checkers directory owned by the test's own user, trusted
// like root (RootUID), with checkers running as that same user.
func checkerRig(t *testing.T) Checkers {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	dir := filepath.Join(t.TempDir(), "checkers")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return Checkers{Dir: dir, RootUID: uint32(os.Getuid()), Accounts: Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Dir: t.TempDir()}}}}
}

func writeChecker(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

var checkCaller = Caller{Principal: core.Principal{Platform: core.PlatformTelegram, ID: "1"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: core.PlatformTelegram, Chat: "-100"}}

func runChecker(t *testing.T, c Checkers, name string, timeout time.Duration) CheckResult {
	t.Helper()
	p, err := c.Resolve(name)
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	l, err := c.Prepare(checkCaller)
	if err != nil {
		t.Fatal(err)
	}
	return RunChecker(context.Background(), p, l, timeout)
}

// TestCheckerResolve: a checker is resolved at every call from the
// directory, which must be root's and not writable by others; the file must
// be a regular executable, not writable by others, never a symlink out.
func TestCheckerResolve(t *testing.T) {
	c := checkerRig(t)
	if _, err := (Checkers{}).Resolve("x"); err == nil || !strings.Contains(err.Error(), "checkers_dir is not set") {
		t.Fatalf("no directory: %v", err)
	}
	if _, err := c.Resolve("later"); err == nil {
		t.Fatal("missing checker resolved")
	}
	// Added after start: resolved without anything reloading.
	want := writeChecker(t, c.Dir, "later", "true")
	if p, err := c.Resolve("later"); err != nil || p != want {
		t.Fatalf("added checker: %q %v", p, err)
	}
	if err := os.Symlink("later", filepath.Join(c.Dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Resolve("alias"); err != nil || p != want {
		t.Fatalf("symlink inside the directory: %q %v", p, err)
	}
	outside := writeChecker(t, t.TempDir(), "outside", "true")
	if err := os.Symlink(outside, filepath.Join(c.Dir, "escape")); err != nil {
		t.Fatal(err)
	}
	writeChecker(t, c.Dir, "plain", "true")
	if err := os.WriteFile(filepath.Join(c.Dir, "noexec"), []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeChecker(t, c.Dir, "loose", "true")
	if err := os.Chmod(filepath.Join(c.Dir, "loose"), 0o757); err != nil {
		t.Fatal(err)
	}
	writeChecker(t, c.Dir, "group", "true")
	if err := os.Chmod(filepath.Join(c.Dir, "group"), 0o775); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(c.Dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"escape": "symlink out",
		"noexec": "not executable",
		"loose":  "group- or world-writable",
		"group":  "group- or world-writable",
		"sub":    "not a regular file",
		"Bad":    "checker names",
		"../x":   "checker names",
		"-x":     "checker names",
	} {
		if _, err := c.Resolve(name); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}
	// The directory: root's (RootUID), not group- or world-writable.
	if err := os.Chmod(c.Dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Resolve("plain"); err == nil || !strings.Contains(err.Error(), "directory must be owned by root") {
		t.Fatalf("world-writable directory: %v", err)
	}
	if err := os.Chmod(c.Dir, 0o775); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Resolve("plain"); err == nil {
		t.Fatal("group-writable directory accepted")
	}
	if err := os.Chmod(c.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	other := c
	other.RootUID = c.RootUID + 1
	if _, err := other.Resolve("plain"); err == nil || !strings.Contains(err.Error(), "owned by root") {
		t.Fatalf("directory of another uid: %v", err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(c.Dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := (Checkers{Dir: link, RootUID: c.RootUID, Accounts: c.Accounts}).Resolve("plain"); err == nil {
		t.Fatal("symlinked directory accepted")
	}
	if os.Getuid() == 0 {
		// A file owned by neither root nor the owner's account.
		if err := os.Chown(filepath.Join(c.Dir, "plain"), 65534, 65534); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Resolve("plain"); err == nil || !strings.Contains(err.Error(), "owned by root or the owner") {
			t.Fatalf("foreign file: %v", err)
		}
	}
}

// TestRunCheckerResults: exit 0 without output is quiet, with output its
// text; failures carry a fixed cause and stderr's first line as detail.
func TestRunCheckerResults(t *testing.T) {
	c := checkerRig(t)
	writeChecker(t, c.Dir, "quiet", "echo noise >&2")
	writeChecker(t, c.Dir, "text", "printf '\\n  deal: 20%% off  \\n\\n'")
	writeChecker(t, c.Dir, "fail", "echo 'first problem' >&2; echo second >&2; exit 3")
	writeChecker(t, c.Dir, "binary", `printf '\377\376'`)
	writeChecker(t, c.Dir, "flood", "head -c 300000 /dev/zero | tr '\\0' a")
	if r := runChecker(t, c, "quiet", 5*time.Second); r.Cause != "" || r.Output != "" {
		t.Fatalf("quiet: %+v", r)
	}
	if r := runChecker(t, c, "text", 5*time.Second); r.Cause != "" || r.Output != "deal: 20% off" {
		t.Fatalf("text: %+v", r)
	}
	if r := runChecker(t, c, "fail", 5*time.Second); r.Cause != "checker exited with status 3" || r.Detail != "first problem" || r.Output != "" {
		t.Fatalf("fail: %+v", r)
	}
	if r := runChecker(t, c, "binary", 5*time.Second); r.Cause != "checker output is not UTF-8" {
		t.Fatalf("binary: %+v", r)
	}
	if r := runChecker(t, c, "flood", 5*time.Second); r.Cause != "checker output over 256 KiB" {
		t.Fatalf("flood: %+v", r)
	}
	writeChecker(t, c.Dir, "exact", "head -c 262144 /dev/zero | tr '\\0' a")
	if r := runChecker(t, c, "exact", 5*time.Second); r.Cause != "" || len(r.Output) != CheckerReadMax {
		t.Fatalf("exactly the cap: %q %d", r.Cause, len(r.Output))
	}
}

func pidFrom(t *testing.T, path string) int {
	t.Helper()
	for range 100 {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 && b[len(b)-1] == '\n' {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err == nil {
				return pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no pid written")
	return 0
}

func waitDead(t *testing.T, pid int) {
	t.Helper()
	for range 100 {
		if !alive(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process %d still alive", pid)
}

// TestRunCheckerTimeoutKillsGrandchild: at the time limit the whole group
// is killed, a grandchild holding stdout included (also one that left the
// group), and the run returns at once.
func TestRunCheckerTimeoutKillsGrandchild(t *testing.T) {
	c := checkerRig(t)
	tmp := t.TempDir()
	writeChecker(t, c.Dir, "hang", "sleep 300 &\necho $! > "+tmp+"/child\nsleep 300")
	start := time.Now()
	r := runChecker(t, c, "hang", 300*time.Millisecond)
	if r.Cause != "checker timed out after 300ms" || r.Stopped {
		t.Fatalf("timeout: %+v", r)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %s", d)
	}
	waitDead(t, pidFrom(t, tmp+"/child"))
	if _, err := exec.LookPath("setsid"); err == nil {
		writeChecker(t, c.Dir, "escape", "setsid sleep 300 &\necho $! > "+tmp+"/escaped\nsleep 300")
		start = time.Now()
		r = runChecker(t, c, "escape", 300*time.Millisecond)
		if !strings.HasPrefix(r.Cause, "checker timed out") || time.Since(start) > 3*time.Second {
			t.Fatalf("escaped child: %+v after %s", r, time.Since(start))
		}
		waitDead(t, pidFrom(t, tmp+"/escaped"))
	}
}

// TestRunCheckerHeldOutput: a checker that exits 0 while a child keeps its
// stdout open fails after the wait delay, and the child is killed.
func TestRunCheckerHeldOutput(t *testing.T) {
	old := checkerWaitDelay
	checkerWaitDelay = 200 * time.Millisecond
	t.Cleanup(func() { checkerWaitDelay = old })
	c := checkerRig(t)
	tmp := t.TempDir()
	writeChecker(t, c.Dir, "held", "sleep 300 &\necho $! > "+tmp+"/child\necho hi")
	start := time.Now()
	r := runChecker(t, c, "held", 10*time.Second)
	if r.Cause != "checker left a process holding its output" || time.Since(start) > 3*time.Second {
		t.Fatalf("held: %+v after %s", r, time.Since(start))
	}
	waitDead(t, pidFrom(t, tmp+"/child"))
}

// TestRunCheckerStopped: cancelling ctx (stop or shutdown) kills the check
// and reports it stopped, not failed.
func TestRunCheckerStopped(t *testing.T) {
	c := checkerRig(t)
	writeChecker(t, c.Dir, "slow", "sleep 300")
	p, _ := c.Resolve("slow")
	l, err := c.Prepare(checkCaller)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	if r := RunChecker(ctx, p, l, time.Minute); !r.Stopped {
		t.Fatalf("stopped: %+v", r)
	}
}

// TestCheckerEnvironment: a checker gets a minimal environment rebuilt from
// run_as.owner, the caller's identity variables and MEMENTO_PROCESSING=1;
// none of Heimdall's own variables, Infisical's or anything else inherited.
func TestCheckerEnvironment(t *testing.T) {
	t.Setenv("HEIMDALL_TELEGRAM_TOKEN", "secret-token-value")
	t.Setenv("INFISICAL_TOKEN", "infisical-secret")
	t.Setenv("SOME_OTHER_SECRET", "other-secret")
	t.Setenv("PI_EXTERNAL_USER_ID", "forged")
	t.Setenv("MEMENTO_PROCESSING", "0")
	t.Setenv("LANG", "C.UTF-8")
	c := checkerRig(t)
	writeChecker(t, c.Dir, "env", "env")
	r := runChecker(t, c, "env", 5*time.Second)
	if r.Cause != "" {
		t.Fatalf("env: %+v", r)
	}
	vars := map[string]string{}
	for _, line := range strings.Split(r.Output, "\n") {
		k, v, _ := strings.Cut(line, "=")
		vars[k] = v
	}
	for k := range vars {
		if strings.HasPrefix(k, "HEIMDALL_") || strings.HasPrefix(k, "INFISICAL_") || k == "SOME_OTHER_SECRET" {
			t.Errorf("inherited %s", k)
		}
	}
	u, _ := user.Current()
	if vars["MEMENTO_PROCESSING"] != "1" || vars["PI_EXTERNAL_USER_ID"] != "tg_user_1" || vars["PI_EXTERNAL_CHAT_ID"] != "tg_chat_-100" ||
		vars["PI_RESTRICTED_EXTERNAL_USER"] != "0" || vars["HOME"] != u.HomeDir || vars["USER"] != u.Username || vars["PATH"] == "" || vars["LANG"] != "C.UTF-8" {
		t.Fatalf("environment: %v", vars)
	}
}
