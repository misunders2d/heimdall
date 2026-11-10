package scheduler

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/misunders2d/heimdall/internal/core"
)

// TrustedLauncher is the owner-trusted T3 launcher (owner, 2026-11-09:
// "just make T3 auto accepted"). A control peer that descends from the
// running process of exactly this executable may register durable tasks
// for the deployment owner and, only with Manage (opt-in), manage that
// owner's existing jobs (list/show/history/run/pause/resume/stop) and tasks
// (pause/resume/close); never create, edit, remove, confirm or reroute. It
// is a source the owner trusts, not an attestation that a human typed the
// request.
type TrustedLauncher struct {
	Path   string // configured launcher path (a stable symlink is fine); resolved per request
	UID    uint32 // the OS user the launcher process runs as
	Manage bool   // owner opt-in: existing-job and task management as above
	root   string // tests only: where path protection checks start ("/" when empty)
}

const trustedT3 = "trusted T3"

// procExe and procUIDs read a process's executable and its real, effective,
// saved and filesystem UIDs from the kernel. Variables so tests can model
// lineage.
var procExe = func(pid int) (string, error) {
	return os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
}

var procUIDs = func(pid int) ([]uint32, error) {
	f, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		rest, ok := strings.CutPrefix(scan.Text(), "Uid:")
		if !ok {
			continue
		}
		var out []uint32
		for _, s := range strings.Fields(rest) {
			n, err := strconv.ParseUint(s, 10, 32)
			if err != nil {
				return nil, err
			}
			out = append(out, uint32(n))
		}
		if len(out) == 4 {
			return out, nil
		}
	}
	return nil, errors.New("malformed process status")
}

var pathLstat = os.Lstat

// resolveTrustedT3 binds a peer that no Heimdall turn or configured launcher
// owns, and that descends from the trusted T3 launcher process, to the
// deployment owner's registered private conversation, task-only. Principal
// and route come from configuration and verified ingress, never the request.
func (c *Control) resolveTrustedT3(pid int, alive func() bool) (Caller, func() bool, error) {
	t := c.s.TrustedT3
	exe, err := trustedExecutable(t)
	if err != nil {
		return Caller{}, nil, fmt.Errorf("trusted T3 launcher unusable: %v", err)
	}
	chain, at, err := t3Lineage(t, exe, pid)
	if err != nil {
		return Caller{}, nil, err
	}
	p, k, err := c.s.st.ControlBinding(c.owner, c.source)
	if err != nil {
		return Caller{}, nil, fmt.Errorf("trusted T3: deployment owner %s: %v", c.owner, err)
	}
	role, _, ok := c.s.access.Decide(p, k)
	if !ok || role != core.RoleOwner {
		return Caller{}, nil, fmt.Errorf("trusted T3: deployment owner %s is not a current owner", c.owner)
	}
	if !alive() {
		return Caller{}, nil, errors.New("request origin is no longer active")
	}
	valid := func() bool {
		if !alive() {
			return false
		}
		if again, err := trustedExecutable(t); err != nil || again != exe {
			return false
		}
		now, at2, err := t3Lineage(t, exe, pid)
		if err != nil || at2 != at {
			return false
		}
		for i := 0; i <= at; i++ {
			if now[i] != chain[i] {
				return false
			}
		}
		return true
	}
	// caller() re-verifies Slack live (OAuth actor from that check only) and
	// re-decides the role per call; executeLocked keeps it task-only.
	return Caller{Principal: p, Role: role, Conv: k, Private: true, External: trustedT3, Registered: trustedT3}, valid, nil
}

// t3Lineage finds the nearest strict ancestor of pid that runs exe as the
// launcher user. A peer inside Heimdall's own process tree is never a T3
// registration: Heimdall turns resolve through ResolveControl or not at all.
func t3Lineage(t *TrustedLauncher, exe string, pid int) ([]procEntry, int, error) {
	chain, err := processChain(pid)
	if err != nil {
		return nil, 0, err
	}
	self := os.Getpid()
	for _, e := range chain {
		if e.pid == self {
			return nil, 0, errors.New("a process Heimdall started is not a trusted T3 registration")
		}
	}
	for i := 1; i < len(chain); i++ {
		if got, err := procExe(chain[i].pid); err != nil || got != exe {
			continue
		}
		uids, err := procUIDs(chain[i].pid)
		if err != nil {
			continue
		}
		same := true
		for _, u := range uids {
			same = same && u == t.UID
		}
		if !same {
			return nil, 0, errors.New("the T3 launcher ancestor does not run as the configured launcher user")
		}
		// The ancestor must still be the same process after reading its exe.
		if ppid, start, err := procStat(chain[i].pid); err != nil || ppid != chain[i].ppid || start != chain[i].start {
			return nil, 0, errors.New("the T3 launcher ancestor changed during verification")
		}
		return chain, i, nil
	}
	return nil, 0, errNoT3Launcher
}

// errNoT3Launcher: the trusted T3 launcher is not an ancestor of the peer.
var errNoT3Launcher = errors.New("no active Heimdall turn, configured launcher or trusted T3 launcher started this process")

// trustedExecutable resolves the configured launcher to its real file and
// checks that neither path can be changed by anyone but root or the
// launcher user.
func trustedExecutable(t *TrustedLauncher) (string, error) {
	real, err := filepath.EvalSymlinks(t.Path)
	if err != nil {
		return "", err
	}
	for _, p := range []string{t.Path, real} {
		if err := protectedPath(p, t.root, t.UID); err != nil {
			return "", err
		}
	}
	if info, err := pathLstat(real); err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", real)
	}
	return real, nil
}

// protectedPath walks path from root: every component must be writable by
// no group or others and owned by root or the launcher user. Below a
// directory so owned that group and others cannot search (for example a
// packaged 0700 version directory), components may have another owner:
// nobody else can reach them.
func protectedPath(path, root string, uid uint32) error {
	if root == "" {
		root = "/"
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") || !filepath.IsAbs(path) {
		return fmt.Errorf("%s is outside %s", path, root)
	}
	cur, parts := root, []string{}
	if rel != "." {
		parts = strings.Split(rel, string(filepath.Separator))
	}
	sealed := false
	for i := -1; i < len(parts); i++ {
		if i >= 0 {
			cur = filepath.Join(cur, parts[i])
		}
		info, err := pathLstat(cur)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("%s has no owner", cur)
		}
		trusted := st.Uid == 0 || st.Uid == uid
		link := info.Mode()&os.ModeSymlink != 0
		if !sealed && !trusted {
			return fmt.Errorf("%s is owned by uid %d, not root or the launcher user", cur, st.Uid)
		}
		if !link && info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("%s is writable by group or others", cur)
		}
		if trusted && info.IsDir() && info.Mode().Perm()&0o011 == 0 {
			sealed = true
		}
	}
	return nil
}
