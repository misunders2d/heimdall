package harness

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/misunders2d/heimdall/internal/core"
)

// Launch is how one turn's native process starts: as which OS user, where
// and with which environment.
type Launch struct {
	Path string // executable found on the run-as PATH; always executed as is
	Dir  string
	Home string
	Env  []string
	Cred *syscall.Credential // nil: Heimdall's own OS user
}

// AccountError means the run-as account itself is unusable, so every
// harness would fail the same way.
type AccountError struct{ msg string }

func (e *AccountError) Error() string { return e.msg }

func accountErr(format string, args ...any) error {
	return &AccountError{msg: fmt.Sprintf(format, args...)}
}

// Accounts are the OS accounts each role's turns run as.
type Accounts struct {
	RunAs map[core.Role]core.RunAs
	// Lookup and Getuid replace os/user.Lookup and os.Getuid in tests.
	Lookup func(name string) (*user.User, error)
	Getuid func() int
}

type account struct {
	u        *user.User
	uid, gid uint32
}

func (a Accounts) account(role core.Role) (core.RunAs, account, error) {
	r, ok := a.RunAs[role]
	if !ok || r.User == "" {
		return r, account{}, accountErr("no OS user is configured for %s turns", role)
	}
	lookup := a.Lookup
	if lookup == nil {
		lookup = user.Lookup
	}
	u, err := lookup(r.User)
	if err != nil {
		return r, account{}, accountErr("OS user %q: %v", r.User, err)
	}
	uid, err1 := strconv.ParseUint(u.Uid, 10, 32)
	gid, err2 := strconv.ParseUint(u.Gid, 10, 32)
	if err1 != nil || err2 != nil {
		return r, account{}, accountErr("OS user %q has a non-numeric uid/gid", r.User)
	}
	return r, account{u: u, uid: uint32(uid), gid: uint32(gid)}, nil
}

// Check verifies that each configured role's OS user exists and that the
// employee's is neither root nor the owner's. Accounts are compared by uid,
// so no other name for either passes.
func (a Accounts) Check() error {
	_, owner, err := a.account(core.RoleOwner)
	if err != nil {
		return err
	}
	if _, ok := a.RunAs[core.RoleEmployee]; !ok {
		return nil
	}
	r, emp, err := a.account(core.RoleEmployee)
	if err != nil {
		return err
	}
	return checkEmployee(r.User, emp, owner)
}

// checkEmployee refuses an employee account with root's or the owner's
// uid: employee turns never run with either's privileges.
func checkEmployee(name string, emp, owner account) error {
	switch {
	case emp.uid == 0:
		return accountErr("employee OS user %q is root (uid 0); employee turns never run as root", name)
	case emp.uid == owner.uid:
		return accountErr("employee OS user %q has the owner's uid %d; employee turns never run as the owner", name, emp.uid)
	}
	return nil
}

// Prepare resolves the OS account role's turns run as and finds the harness
// binary on that account's PATH.
//
// Owner turns on Heimdall's own uid get Heimdall's environment, home
// included, minus its own HEIMDALL_* variables. Every other turn, and every
// employee turn even on Heimdall's own uid, gets an environment rebuilt from
// its account, so nothing of Heimdall's user (home, logins, secrets) reaches
// it; switching to another uid needs Heimdall to be root. Otherwise the turn
// is refused: it never runs as a different user than its role's.
func (a Accounts) Prepare(role core.Role, memory bool, bin string) (Launch, error) {
	r, acct, err := a.account(role)
	if err != nil {
		return Launch{}, err
	}
	switch role {
	case core.RoleOwner:
	case core.RoleEmployee:
		// Re-checked every turn: an account changed after startup still
		// never gives an employee turn root's or the owner's uid.
		_, owner, err := a.account(core.RoleOwner)
		if err != nil {
			return Launch{}, err
		}
		if err := checkEmployee(r.User, acct, owner); err != nil {
			return Launch{}, err
		}
	default:
		return Launch{}, accountErr("unknown role %q", role)
	}
	getuid := a.Getuid
	if getuid == nil {
		getuid = os.Getuid
	}
	self := getuid()
	u := acct.u
	l := Launch{Home: u.HomeDir}
	switch {
	case role == core.RoleOwner && int(acct.uid) == self:
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "HEIMDALL_") {
				l.Env = append(l.Env, kv)
			}
		}
		if r.Path != "" {
			l.Env = setEnv(l.Env, "PATH", r.Path)
		}
		if h := envValue(l.Env, "HOME"); filepath.IsAbs(h) {
			l.Home = h
		}
	case int(acct.uid) == self || self == 0:
		if int(acct.uid) != self {
			var groups []uint32
			if ids, err := u.GroupIds(); err == nil {
				for _, id := range ids {
					if g, err := strconv.ParseUint(id, 10, 32); err == nil {
						groups = append(groups, uint32(g))
					}
				}
			}
			l.Cred = &syscall.Credential{Uid: acct.uid, Gid: acct.gid, Groups: groups}
		}
		path := r.Path
		if path == "" {
			path = strings.Join([]string{filepath.Join(u.HomeDir, ".local", "bin"), filepath.Join(u.HomeDir, ".local", "share", "mise", "shims"), "/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}, ":")
		}
		l.Env = []string{"HOME=" + u.HomeDir, "USER=" + u.Username, "LOGNAME=" + u.Username, "SHELL=" + loginShell(u.Username), "PATH=" + path}
		for _, name := range []string{"LANG", "LC_ALL", "TZ"} {
			if v, ok := os.LookupEnv(name); ok {
				l.Env = append(l.Env, name+"="+v)
			}
		}
		if dir := "/run/user/" + u.Uid; isDir(dir) {
			l.Env = append(l.Env, "XDG_RUNTIME_DIR="+dir)
		}
	default:
		return Launch{}, accountErr("cannot run as OS user %q: Heimdall runs as uid %d, not root; the turn was refused rather than run as another user", r.User, self)
	}
	// With memory the native setup decides; Heimdall never force-enables it.
	if !memory {
		l.Env = setEnv(l.Env, "MEMENTO_PROCESSING", "1")
	}
	l.Dir = r.Dir
	if l.Dir == "" {
		l.Dir = l.Home
	}
	if !isDir(l.Dir) {
		return Launch{}, accountErr("working directory %q for OS user %q does not exist", l.Dir, r.User)
	}
	l.Path, err = lookPath(bin, envValue(l.Env, "PATH"))
	if err != nil {
		return Launch{}, fmt.Errorf("%s not found on the PATH of OS user %q", bin, r.User)
	}
	return l, nil
}

// lookPath finds bin on path without consulting Heimdall's own PATH.
func lookPath(bin, path string) (string, error) {
	if strings.Contains(bin, "/") {
		return "", fmt.Errorf("harness binary %q must be a name", bin)
	}
	for _, dir := range filepath.SplitList(path) {
		if dir == "" || !filepath.IsAbs(dir) {
			continue
		}
		p := filepath.Join(dir, bin)
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0o111 != 0 {
			return p, nil
		}
	}
	return "", os.ErrNotExist
}

func setEnv(env []string, name, value string) []string {
	out := env[:0:0]
	for _, kv := range env {
		if k, _, _ := strings.Cut(kv, "="); k != name {
			out = append(out, kv)
		}
	}
	return append(out, name+"="+value)
}

func envValue(env []string, name string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if k, v, _ := strings.Cut(env[i], "="); k == name {
			return v
		}
	}
	return ""
}

func isDir(p string) bool { st, err := os.Stat(p); return err == nil && st.IsDir() }

// loginShell is the account's shell from /etc/passwd, or /bin/sh.
func loginShell(name string) string {
	f, err := os.Open("/etc/passwd")
	if err != nil {
		return "/bin/sh"
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Split(s.Text(), ":")
		if len(fields) == 7 && fields[0] == name && fields[6] != "" {
			return fields[6]
		}
	}
	return "/bin/sh"
}
