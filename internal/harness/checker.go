package harness

// Watch checkers (docs/scheduler.md §6): owner-installed executables in one
// directory that Heimdall runs on a schedule, with no AI turn. A checker
// says something only by printing it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
)

// CheckerName is a checker's name: a file name in the checkers directory.
var CheckerName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

const (
	// CheckerReadMax caps what is read from a checker's stdout; more fails
	// the run.
	CheckerReadMax = 256 << 10
	// CheckerTimeout is a check's time limit unless its job sets one.
	CheckerTimeout = 60 * time.Second
	checkerErrMax  = 4 << 10
)

// checkerWaitDelay bounds the wait for a checker's output pipes once it has
// exited (or was killed), so a child holding stdout cannot hang the run.
var checkerWaitDelay = 5 * time.Second

// Checkers is where watch checkers live and who runs them.
type Checkers struct {
	Dir      string   // absolute; "" = watch jobs are off
	Accounts Accounts // checkers run as run_as.owner
	// RootUID is the uid trusted like root: the directory must be owned by
	// it, and files by it or the owner's account. 0 in production; tests
	// set their own uid.
	RootUID uint32
}

// CheckerError is why a checker cannot be used: a fixed text, safe for
// shared notices (no path, no output).
type CheckerError struct{ msg string }

func (e *CheckerError) Error() string { return e.msg }

func checkerErr(msg string) error { return &CheckerError{msg: msg} }

// Resolve finds name in the directory, at every run, so a checker added
// later needs no restart. The directory must be a real directory owned by
// root and writable only by it; the checker a regular executable file in
// it (a symlink only to a file directly in the directory), owned by root or
// the owner's account, not group- or world-writable.
func (c Checkers) Resolve(name string) (string, error) {
	if c.Dir == "" {
		return "", checkerErr("watch jobs are off: scheduler.checkers_dir is not set")
	}
	if !CheckerName.MatchString(name) {
		return "", checkerErr("checker names are a-z, 0-9 and -, at most 63 characters")
	}
	st, err := os.Lstat(c.Dir)
	if err != nil || !st.IsDir() {
		return "", checkerErr("the checkers directory is missing or not a directory")
	}
	if uid, ok := ownerOf(st); !ok || uid != c.RootUID || st.Mode().Perm()&0o022 != 0 {
		return "", checkerErr("the checkers directory must be owned by root and not group- or world-writable")
	}
	p := filepath.Join(c.Dir, name)
	real, err := filepath.EvalSymlinks(p)
	if err != nil {
		return "", checkerErr("checker not found")
	}
	dir, err := filepath.EvalSymlinks(c.Dir)
	if err != nil || filepath.Dir(real) != dir {
		return "", checkerErr("checker is a symlink out of the checkers directory")
	}
	st, err = os.Stat(real)
	if err != nil || !st.Mode().IsRegular() {
		return "", checkerErr("checker is not a regular file")
	}
	if st.Mode().Perm()&0o111 == 0 {
		return "", checkerErr("checker is not executable")
	}
	if st.Mode().Perm()&0o022 != 0 {
		return "", checkerErr("checker is group- or world-writable")
	}
	_, owner, err := c.Accounts.account(core.RoleOwner)
	if err != nil {
		return "", checkerErr("run_as.owner is unusable")
	}
	if uid, ok := ownerOf(st); !ok || uid != c.RootUID && uid != owner.uid {
		return "", checkerErr("checker must be owned by root or the owner's account")
	}
	return real, nil
}

func ownerOf(st os.FileInfo) (uint32, bool) {
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return sys.Uid, true
}

// Prepare is a check's launch as run_as.owner, with a minimal environment
// rebuilt from that account: HOME, USER, PATH, LANG and TZ (when Heimdall
// has them), the caller's identity variables and MEMENTO_PROCESSING=1.
// Nothing else of Heimdall's environment reaches it, so no HEIMDALL_* or
// INFISICAL_* variable either. Like a turn, it is refused when Heimdall
// cannot switch to that account.
func (c Checkers) Prepare(caller Caller) (Launch, error) {
	r, acct, err := c.Accounts.account(core.RoleOwner)
	if err != nil {
		return Launch{}, err
	}
	getuid := c.Accounts.Getuid
	if getuid == nil {
		getuid = os.Getuid
	}
	self := getuid()
	l := Launch{Home: acct.u.HomeDir}
	switch {
	case int(acct.uid) == self:
	case self == 0:
		l.Cred = c.Accounts.credential(r, acct)
	default:
		return Launch{}, accountErr("cannot run as OS user %q: Heimdall runs as uid %d, not root", r.User, self)
	}
	env := []string{"HOME=" + acct.u.HomeDir, "USER=" + acct.u.Username, "PATH=" + runPath(r, acct.u)}
	for _, name := range []string{"LANG", "TZ"} {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	env = append(env, "MEMENTO_PROCESSING=1")
	if l.Env, err = WithIdentity(env, caller); err != nil {
		return Launch{}, err
	}
	l.Dir = r.Dir
	if l.Dir == "" {
		l.Dir = l.Home
	}
	if !isDir(l.Dir) {
		return Launch{}, accountErr("working directory %q for OS user %q does not exist", l.Dir, r.User)
	}
	return l, nil
}

// CheckResult is how one check ended. Cause is "" when the checker exited 0
// with valid output; otherwise a fixed text, with Detail the first line of
// its stderr, scrubbed (only for private destinations).
type CheckResult struct {
	Output  string // trimmed stdout; "" = nothing to say
	Cause   string
	Detail  string
	Stopped bool // ctx was cancelled: stopped or shut down, not a failure
}

// RunChecker runs the executable at path, with no arguments and no stdin,
// in its own process group that dies with Heimdall. At the time limit, or
// when ctx is cancelled, the whole group is killed; the output pipes are
// waited for at most checkerWaitDelay after that, or after it exits.
func RunChecker(ctx context.Context, path string, l Launch, timeout time.Duration) CheckResult {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(tctx, path)
	cmd.Dir, cmd.Env = l.Dir, l.Env
	out, errOut := &capped{max: CheckerReadMax}, &capped{max: checkerErrMax}
	cmd.Stdout, cmd.Stderr = out, errOut
	setProcAttr(cmd, l.Cred)
	cmd.WaitDelay = checkerWaitDelay
	if err := cmd.Start(); err != nil {
		return CheckResult{Cause: "checker did not start", Detail: scrubLine(err.Error())}
	}
	err := cmd.Wait()
	stopGroup(cmd)
	detail := firstLine(errOut.String())
	var exit *exec.ExitError
	switch {
	case ctx.Err() != nil:
		return CheckResult{Stopped: true, Cause: "stopped"}
	case errors.Is(tctx.Err(), context.DeadlineExceeded):
		return CheckResult{Cause: fmt.Sprintf("checker timed out after %s", timeout), Detail: detail}
	case errors.As(err, &exit) && exit.ExitCode() >= 0:
		return CheckResult{Cause: fmt.Sprintf("checker exited with status %d", exit.ExitCode()), Detail: detail}
	case errors.As(err, &exit):
		return CheckResult{Cause: "checker was killed by a signal", Detail: detail}
	case errors.Is(err, exec.ErrWaitDelay):
		return CheckResult{Cause: "checker left a process holding its output", Detail: detail}
	case err != nil:
		return CheckResult{Cause: "checker failed", Detail: scrubLine(err.Error())}
	case out.over:
		return CheckResult{Cause: fmt.Sprintf("checker output over %d KiB", CheckerReadMax>>10), Detail: detail}
	case !utf8.Valid(out.b):
		return CheckResult{Cause: "checker output is not UTF-8", Detail: detail}
	}
	return CheckResult{Output: strings.TrimSpace(string(out.b))}
}

// capped keeps the first max bytes written and notes whether more came;
// the rest is read and dropped, so the writer never blocks.
type capped struct {
	b    []byte
	max  int
	over bool
}

func (c *capped) Write(p []byte) (int, error) {
	room := c.max - len(c.b)
	if len(p) > room {
		c.over = true
		c.b = append(c.b, p[:max(room, 0)]...)
	} else {
		c.b = append(c.b, p...)
	}
	return len(p), nil
}

func (c *capped) String() string { return string(c.b) }

// firstLine is the first non-empty line of s, scrubbed.
func firstLine(s string) string {
	for line := range strings.SplitSeq(s, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return scrubLine(line)
		}
	}
	return ""
}

func scrubLine(s string) string {
	s = core.SanitizeNativeError(strings.ToValidUTF8(s, "?"), "")
	if len(s) > 300 {
		s = s[:300]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		s += "…"
	}
	return s
}
