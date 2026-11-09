package scheduler

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// ExternalOrigin is a trusted local launcher (for example the AgentNet
// daemon) that vouches, live, for the harness run it started and the person
// who asked for it (docs/external-origin.md). Heimdall imports nothing of
// the launcher; this is the whole contract.
type ExternalOrigin struct {
	Name   string                    // label used in errors and the run binding
	Socket string                    // the launcher's own Unix socket
	UID    uint32                    // the OS user the launcher runs as
	People map[string]ExternalPerson // launcher person -> Heimdall identity
}

// ExternalPerson maps a launcher person to an existing Heimdall principal.
// Role is never configured: it is evaluated live from access.
type ExternalPerson struct {
	Principal string        // platform:id
	Source    *core.ConvKey // which registered private conversation, optional
}

const (
	externalMax     = 4096
	externalTimeout = 2 * time.Second
)

type externalQuery struct {
	V     int    `json:"v"`
	PID   int    `json:"pid"`
	Start uint64 `json:"start"`
}

type externalAnswer struct {
	V      int    `json:"v"`
	Run    string `json:"run,omitempty"`
	Person string `json:"person,omitempty"`
	Author string `json:"author,omitempty"`
	Error  string `json:"error,omitempty"`
}

type procEntry struct {
	pid, ppid int
	start     uint64
}

// procStat reads a process's parent and start time (field 22, clock ticks
// since boot) from the kernel. A variable so tests can model lineage.
var procStat = func(pid int) (ppid int, start uint64, err error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, 0, errors.New("malformed process stat")
	}
	f := strings.Fields(string(data)[end+1:])
	if len(f) < 20 {
		return 0, 0, errors.New("malformed process stat")
	}
	if ppid, err = strconv.Atoi(f[1]); err != nil {
		return 0, 0, err
	}
	start, err = strconv.ParseUint(f[19], 10, 64)
	return ppid, start, err
}

// processChain is pid's kernel ancestry, nearest first, bounded.
func processChain(pid int) ([]procEntry, error) {
	var out []procEntry
	seen := map[int]bool{}
	for i := 0; pid > 1 && i < 128 && !seen[pid]; i++ {
		ppid, start, err := procStat(pid)
		if err != nil {
			return nil, errors.New("process ancestry unreadable")
		}
		seen[pid] = true
		out = append(out, procEntry{pid, ppid, start})
		pid = ppid
	}
	return out, nil
}

// errNotLaunched: no configured launcher is an ancestor of the peer.
var errNotLaunched = errors.New("no active Heimdall turn and no configured launcher started this process")

// resolveExternal binds a peer that no Heimdall turn owns to a configured
// launcher's verified run. Errors name what is missing.
func (c *Control) resolveExternal(pid int, alive func() bool) (Caller, func() bool, error) {
	if len(c.s.External) == 0 {
		return Caller{}, nil, errors.New("no active Heimdall turn owns this process and no external origin is configured")
	}
	err := errNotLaunched
	for _, o := range c.s.External {
		who, valid, e := c.externalOrigin(o, pid, alive)
		if e == nil {
			return who, valid, nil
		}
		if !errors.Is(e, errNotLaunched) {
			err = e // a launcher in the ancestry explains more than "none"
		}
	}
	return Caller{}, nil, err
}

func (c *Control) externalOrigin(o ExternalOrigin, pid int, alive func() bool) (Caller, func() bool, error) {
	ans, err := askLauncher(o, pid)
	if err != nil {
		return Caller{}, nil, err
	}
	person, ok := o.People[ans.Person]
	if !ok {
		return Caller{}, nil, fmt.Errorf("%s person %q has no Heimdall identity mapping", o.Name, ans.Person)
	}
	if ans.Author != "human" {
		return Caller{}, nil, fmt.Errorf("%s run %s was not requested by the person themself (author %s)", o.Name, ans.Run, ans.Author)
	}
	// The mapped identity's real registered private conversation, as for
	// the deployment binding: Slack email/team come from verified ingress.
	p, k, err := c.s.st.ControlBinding(person.Principal, person.Source)
	if err != nil {
		return Caller{}, nil, fmt.Errorf("%s person %q maps to %s: %v", o.Name, ans.Person, person.Principal, err)
	}
	role, _, ok := c.s.access.Decide(p, k)
	if !ok || role != core.RoleOwner {
		return Caller{}, nil, fmt.Errorf("%s person %q maps to %s, which is not a current owner", o.Name, ans.Person, person.Principal)
	}
	if !alive() {
		return Caller{}, nil, errors.New("request origin is no longer active")
	}
	valid := func() bool {
		again, err := askLauncher(o, pid)
		return err == nil && alive() && again == ans
	}
	// caller() re-verifies Slack live and re-decides the role per call.
	return Caller{Principal: p, Role: role, Conv: k, Private: true, External: o.Name + " " + ans.Run}, valid, nil
}

// askLauncher finds the launcher's direct child in pid's ancestry and asks
// the launcher, over its own socket, who that run is for.
func askLauncher(o ExternalOrigin, pid int) (externalAnswer, error) {
	var none externalAnswer
	if err := launcherSocket(o); err != nil {
		return none, fmt.Errorf("%s origin socket unusable: %v", o.Name, err)
	}
	conn, err := net.DialTimeout("unix", o.Socket, externalTimeout)
	if err != nil {
		return none, fmt.Errorf("%s origin socket unavailable: %v", o.Name, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(externalTimeout))
	cred, err := peerCredentials(conn.(*net.UnixConn))
	if err != nil {
		return none, fmt.Errorf("%s origin socket credentials unavailable", o.Name)
	}
	if cred.Uid != o.UID {
		return none, fmt.Errorf("%s origin socket is served by uid %d, not the configured launcher", o.Name, cred.Uid)
	}
	server := int(cred.Pid)
	chain, err := processChain(pid)
	if err != nil {
		return none, err
	}
	var child procEntry
	for _, p := range chain {
		if p.ppid == server {
			child = p
			break
		}
	}
	if child.pid == 0 {
		return none, errNotLaunched
	}
	raw, _ := json.Marshal(externalQuery{V: 1, PID: child.pid, Start: child.start})
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		return none, fmt.Errorf("%s origin query failed", o.Name)
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 512), externalMax)
	var ans externalAnswer
	if !scan.Scan() || strictJSON(scan.Bytes(), &ans) != nil || ans.V != 1 {
		return none, fmt.Errorf("%s origin answer is not valid", o.Name)
	}
	if ans.Error != "" {
		return none, fmt.Errorf("%s does not vouch for this process: %s", o.Name, clipLine(ans.Error, 200))
	}
	if !externalField(ans.Run) || !externalField(ans.Person) || (ans.Author != "human" && ans.Author != "agent") {
		return none, fmt.Errorf("%s origin answer is not valid", o.Name)
	}
	// The run must still be that launcher's same child after the answer.
	if ppid, start, err := procStat(child.pid); err != nil || ppid != server || start != child.start {
		return none, fmt.Errorf("%s run ended during verification", o.Name)
	}
	return ans, nil
}

// launcherSocket checks the configured endpoint is the launcher user's own
// socket in a directory only that user can change.
func launcherSocket(o ExternalOrigin) error {
	for _, path := range []string{filepath.Dir(o.Socket), o.Socket} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != o.UID || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s is not owned by the launcher user", path)
		}
		if path == o.Socket && info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("%s is not a socket", path)
		}
		if path != o.Socket && (!info.IsDir() || info.Mode().Perm()&0o022 != 0) {
			return fmt.Errorf("%s must be a directory not writable by group or others", path)
		}
	}
	return nil
}

func externalField(s string) bool {
	return s != "" && len(s) <= 128 && !strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f })
}

func clipLine(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	if len(s) > n {
		s = s[:n]
	}
	return s
}
