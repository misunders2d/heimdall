package scheduler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

const ControlMaxBytes = 65536

// ControlRequest deliberately has no identity, role, conversation, shell or
// confirmation fields. These cannot be granted by tool arguments.
type ControlRequest struct {
	Key        string          `json:"key,omitempty"`
	Revision   string          `json:"revision,omitempty"`
	Outbound   int64           `json:"outbound_id,omitempty"`
	Action     string          `json:"action"`
	ID         string          `json:"id,omitempty"`
	Definition json.RawMessage `json:"definition,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
	Title      string          `json:"title,omitempty"` // task_add only: the task's plain title
}

type ControlResult struct {
	Reply   string `json:"reply,omitempty"`
	DraftID string `json:"draft_id,omitempty"`
	JobID   string `json:"job_id,omitempty"` // a job applied at once (external owner origin)
	Changed bool   `json:"changed"`
	Error   string `json:"error,omitempty"`
}

type Control struct {
	s      *Scheduler
	source *core.ConvKey // optional trusted deployment source, verified against real ingress
	owner  string        // trusted deployment binding selector, never a request argument
	uid    uint32        // derived from existing run_as.owner OS account
	mu     sync.Mutex
}

// ListenControl adds a private local management endpoint to the existing process.
// No independent scheduler, network listener, token or admission list is created.
func (s *Scheduler) ListenControl(ctx context.Context, owner string, uid uint32, dir string, source ...*core.ConvKey) (*net.UnixListener, error) {
	if _, err := core.ParsePrincipalKey(owner); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0700 || (stat.Uid != uint32(os.Geteuid()) && stat.Uid != uid) {
		return nil, errors.New("scheduler control directory must be private and owned by runtime or owner UID")
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(dir, int(uid), -1); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, "control.sock")
	if err := recoverControlSocket(ctx, path, uid); err != nil {
		return nil, err
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	} // never unlink a potentially live endpoint
	if err = os.Chmod(l.Addr().String(), 0600); err == nil && os.Geteuid() == 0 {
		err = os.Chown(l.Addr().String(), int(uid), -1)
	}
	if err != nil {
		l.Close()
		return nil, err
	}
	c := &Control{s: s, owner: owner, uid: uid}
	if len(source) > 0 {
		c.source = source[0]
	}
	go func() { <-ctx.Done(); l.Close() }()
	go func() {
		for {
			conn, err := l.AcceptUnix()
			if err != nil {
				return
			}
			go c.serve(ctx, conn)
		}
	}()
	return l, nil
}

// Only ECONNREFUSED proves an owned socket is stale. A timeout, permission
// failure or a live listener never licenses unlinking somebody's endpoint.
func recoverControlSocket(ctx context.Context, path string, uid uint32) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || (stat.Uid != uint32(os.Geteuid()) && stat.Uid != uid) {
		return errors.New("scheduler control endpoint is not an owned Unix socket")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", path)
	if err == nil {
		conn.Close()
		return errors.New("scheduler control endpoint already has a live listener")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("scheduler control endpoint not proven stale: %w", err)
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return errors.New("scheduler control endpoint changed during stale check")
	}
	return os.Remove(path)
}

func peerCredentials(c *net.UnixConn) (*syscall.Ucred, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *syscall.Ucred
	var inner error
	err = raw.Control(func(fd uintptr) {
		cred, inner = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil {
		return nil, err
	}
	if inner != nil {
		return nil, inner
	}
	if cred == nil {
		return nil, errors.New("missing peer credentials")
	}
	return cred, nil
}

func (c *Control) serve(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	cred, err := peerCredentials(conn)
	result := ControlResult{Error: "operator not authorized"}
	if err == nil && (cred.Uid == 0 || cred.Uid == c.uid) {
		// SO_PEERPIDFD binds the original connecting process, including after
		// exit/reaping. Never reopen a numeric PID to establish peer lifetime.
		fd, lifeErr := peerPIDFD(conn)
		if lifeErr != nil {
			_ = json.NewEncoder(conn).Encode(ControlResult{Error: "kernel peer lifetime unavailable"})
			return
		}
		defer unix.Close(fd)
		alive := func() bool { return peerAlive(fd) }
		who, valid, originErr := c.resolvePeer(int(cred.Pid), alive)
		var req ControlRequest
		scan := bufio.NewScanner(conn)
		scan.Buffer(make([]byte, 4096), ControlMaxBytes)
		if !scan.Scan() || strictJSON(scan.Bytes(), &req) != nil {
			result.Error = "invalid control request"
		} else {
			if originErr != nil {
				result.Error = "trusted active request origin unavailable: " + originErr.Error()
			} else {
				result = c.executePeer(ctx, cred, req, who, valid)
			}
		}
	}
	_ = json.NewEncoder(conn).Encode(result)
}

func strictJSON(raw []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("one JSON object required")
	}
	return nil
}

func (c *Control) caller(ctx context.Context, origin []Caller) (Caller, error) {
	if len(origin) != 1 || origin[0].Role != core.RoleOwner {
		return Caller{}, errors.New("trusted active request origin required")
	}
	who := origin[0]
	if who.Job != "" {
		return c.scheduledCaller(ctx, who)
	}
	p, k := who.Principal, who.Conv
	actor, installation := who.OAuthActor, who.OAuthInstallation
	// An external origin's conversation is the mapped principal's registered
	// private ingress (ControlBinding at resolution), not an active turn.
	if who.External == "" {
		if err := c.s.st.ControlOrigin(p, k, who.Private); err != nil {
			return Caller{}, err
		}
	}
	if p.Platform == core.PlatformSlack {
		if c.s.Reverify == nil {
			return Caller{}, errors.New("live Slack verification unavailable")
		}
		v, ok, err := c.s.Reverify(ctx, p)
		if err != nil || !ok || v.Principal.Key() != p.Key() || v.Principal.Team != p.Team {
			return Caller{}, errors.New("live owner identity verification failed")
		}
		p = v.Principal
		if who.External != "" {
			// An external origin has no stored message: its native OAuth
			// actor is the live verification's, never a launcher's.
			actor, installation = v.OAuthActor, v.OAuthInstallation
		}
	}
	r, _, ok := c.s.access.Decide(p, k)
	if !ok || r != core.RoleOwner {
		return Caller{}, errors.New("bound caller is not a current owner")
	}
	return Caller{Principal: p, Role: r, Conv: k, Private: who.Private, OAuthActor: actor, OAuthInstallation: installation, External: who.External, Registered: who.Registered}, nil
}

// scheduledCaller binds a router-started scheduled run. The router already
// tied the socket peer to that run's harness process (kernel pidfd, ancestry,
// live generation). Here the store must still show that exact run running at
// the job's current version, created by this principal, who is re-verified
// (Slack live) and is an owner now for the job's target. Conversation and
// privacy come from the stored job, never from the harness or request.
func (c *Control) scheduledCaller(ctx context.Context, who Caller) (Caller, error) {
	refused := errors.New("scheduled origin is not a current owner run")
	j, ok, err := c.s.st.Job(who.Job)
	if err != nil || !ok || j.Creator.Key() != who.Principal.Key() || (j.State != store.JobActive && j.State != store.JobPaused) {
		return Caller{}, refused
	}
	run, err := c.s.st.Run(who.Run)
	if err != nil || run.Job != j.ID || run.State != store.RunRunning || run.Version != j.Version {
		return Caller{}, refused
	}
	d, _, err := parse(j)
	if err != nil || d.Kind != KindAgent {
		return Caller{}, refused
	}
	v, tgt, err := c.s.verify(ctx, j, d, c.s.now())
	if err != nil || v.Principal.Key() != j.Creator.Key() {
		return Caller{}, refused
	}
	if r, _, ok := c.s.access.Decide(v.Principal, tgt.Conv); !ok || r != core.RoleOwner {
		return Caller{}, refused
	}
	// Private when the stored target is the creator's private chat, or when
	// that exact target is the creator's own current-owner identity's live
	// private conversation (e.g. a Slack creator's linked Telegram DM): the
	// same verified owner-ingress check as alert destinations. Never from the
	// request or the harness.
	private := tgt.Private && tgt.AgentNet == ""
	if !private && tgt.AgentNet == "" {
		if senders := c.s.senders(j.ID); len(senders) > 0 && c.s.st.OwnerIngress(tgt.Conv, senders) == nil {
			private = true
		}
	}
	return Caller{Principal: v.Principal, Role: core.RoleOwner, Conv: tgt.Conv, Private: private, Job: j.ID, Run: run.ID}, nil
}

// readable is a job the bound caller may read: its own (exact creator), or,
// for a trusted scheduled run's read actions only, one created by the same
// person's identity alias that is an owner now. Live person turns keep exact
// creator reads.
func (c *Control) readable(j store.Job, who Caller, read bool) bool {
	if j.Creator.Key() == who.Principal.Key() {
		return true
	}
	if !read || who.Job == "" || !c.s.access.SamePerson(j.Creator, who.Principal) {
		return false
	}
	r, ok := c.s.access.Role(j.Creator)
	return ok && r == core.RoleOwner
}

// peerPIDFD requires Linux SO_PEERPIDFD; unsupported kernels fail closed.
func peerPIDFD(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, err
	}
	fd := -1
	var inner error
	err = raw.Control(func(socket uintptr) { fd, inner = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD) })
	if err != nil {
		return -1, err
	}
	return fd, inner
}
func peerAlive(fd int) bool {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	return err == nil && n == 0
}

// resolvePeer: a Heimdall-started live turn first; only a peer no Heimdall
// turn owns may be vouched for by a configured external launcher.
func (c *Control) resolvePeer(pid int, alive func() bool) (Caller, func() bool, error) {
	if !alive() {
		return Caller{}, nil, errors.New("no active peer")
	}
	if c.s.ResolveControl != nil {
		who, current, err := c.s.ResolveControl(pid)
		if err == nil && current != nil && alive() && current() {
			return who, func() bool { return alive() && current() }, nil
		}
		if err == nil {
			return Caller{}, nil, errors.New("no active origin")
		}
	}
	who, valid, err := c.resolveExternal(pid, alive)
	if err == nil || c.s.TrustedT3 == nil || (len(c.s.External) > 0 && !errors.Is(err, errNotLaunched) && !errors.Is(err, errLauncherDown)) {
		return who, valid, err // a launcher in the ancestry that refused never falls through
	}
	return c.resolveTrustedT3(pid, alive)
}

// Capture an eligible native input before waiting; never upgrade a queued idle
// request to a later turn. Recheck that same generation after live verification.
func (c *Control) executePeer(ctx context.Context, cred *syscall.Ucred, req ControlRequest, who Caller, valid func() bool) ControlResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	if valid == nil || !valid() {
		return ControlResult{Error: "request origin is no longer active"}
	}
	return c.executeLocked(ctx, cred.Uid, req, []Caller{who}, valid)
}

func (c *Control) execute(ctx context.Context, uid uint32, req ControlRequest, origin ...Caller) ControlResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.executeLocked(ctx, uid, req, origin)
}

func (c *Control) executeLocked(ctx context.Context, uid uint32, req ControlRequest, origin []Caller, valid ...func() bool) ControlResult {
	fail := func(err error) ControlResult { return ControlResult{Error: err.Error()} }
	if uid != 0 && uid != c.uid {
		return fail(errors.New("operator not authorized"))
	}
	who, err := c.caller(ctx, origin)
	if err != nil {
		return fail(err)
	}
	if len(valid) > 0 && !valid[0]() {
		return fail(errors.New("request origin expired during verification"))
	}
	if strings.HasPrefix(req.Action, "task_") {
		return c.task(uid, req, who, valid)
	}
	if who.Registered != "" {
		return fail(errors.New(who.Registered + " may only task_add, task_list and task_show"))
	}
	if req.Title != "" {
		return fail(errors.New("title is only valid for task_add"))
	}
	read := req.Action == "list" || req.Action == "show" || req.Action == "history" || req.Action == "checkpoint" || req.Action == "discover"
	if who.Job != "" && !read {
		return fail(errors.New("scheduled runs may only read the scheduler; drafts, changes and confirmations need a live human request"))
	}
	if !who.Private && (req.Action == "checkpoint" || req.Action == "show" || req.Action == "history") {
		return fail(errors.New("job details require a private originating conversation"))
	}
	if !read && (req.RequestID == "" || len(req.RequestID) > 128 || strings.ContainsAny(req.RequestID, "\r\n")) {
		return fail(errors.New("mutation requires a stable request_id (1-128 bytes); reuse only for the same request"))
	}
	if len(req.Definition) > 16000 || len(req.ID) > 64 {
		return fail(errors.New("control request too large"))
	}
	needsID := req.Action == "show" || req.Action == "history" || req.Action == "checkpoint" || req.Action == "edit" || req.Action == "remove" || req.Action == "run" || req.Action == "pause" || req.Action == "resume" || req.Action == "stop" || req.Action == "acknowledge"
	if needsID && req.ID == "" {
		return fail(errors.New("job id required"))
	}
	if !needsID && req.ID != "" {
		return fail(errors.New("id is not valid for this action"))
	}
	if req.Action != "create" && req.Action != "edit" && len(req.Definition) > 0 {
		return fail(errors.New("definition is only valid for create/edit"))
	}
	if req.Action == "acknowledge" {
		if req.Key == "" || len(req.Key) > 128 || req.Revision == "" || len(req.Revision) > 128 || req.Outbound <= 0 {
			return fail(errors.New("acknowledge requires exact key, revision and outbound_id"))
		}
	} else if req.Key != "" || req.Revision != "" || req.Outbound != 0 {
		return fail(errors.New("receipt fields require acknowledge"))
	}
	if req.Action == "discover" {
		return ControlResult{Reply: "list, show, history, checkpoint, create, edit, run, pause, resume, stop, remove, acknowledge; task_add, task_list, task_show for durable tasks (task_add: id = Linear issue, title, request_id; worked by your own assistant until verified; no worker, resource, gate or next step). acknowledge requires the exact failed receipt key/revision/outbound_id, retains history and never replays. create/edit/remove prepare a draft in the originating conversation; only its real human can confirm there. Other operations use existing job commands. Identity is bound by OS peer credentials and the active verified ingress turn."}
	}
	var job store.Job
	if req.ID != "" {
		j, ok, err := c.s.st.Job(req.ID)
		if err != nil {
			return fail(err)
		}
		if !ok || !c.readable(j, who, read) {
			return fail(errors.New("no job of the bound creator"))
		}
		job = j
	}
	if req.Action == "list" {
		return ControlResult{Reply: c.s.list(who, false)}
	}
	if req.Action == "show" || req.Action == "history" {
		text, _ := c.s.Command(who, "job "+req.Action+" "+req.ID)
		return ControlResult{Reply: text}
	}
	if req.Action == "checkpoint" {
		cp, err := c.s.st.Checkpoint(req.ID, job.Creator.Key())
		if err != nil {
			return fail(err)
		}
		data, _ := json.Marshal(cp)
		return ControlResult{Reply: string(data)}
	}
	raw, _ := json.Marshal(struct {
		Request ControlRequest
		Source  core.ConvKey
	}{req, who.Conv})
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if saved, ok, err := c.s.st.ControlResponse(who.Principal.Key(), req.RequestID, digest); err != nil {
		return fail(err)
	} else if ok {
		var result ControlResult
		_ = json.Unmarshal([]byte(saved), &result)
		return result
	}
	var change *store.JobChange
	var outs []core.Outbound
	var result ControlResult
	switch req.Action {
	case "create", "edit", "remove":
		var fields map[string]json.RawMessage
		if req.Action != "remove" {
			if json.Unmarshal(req.Definition, &fields) != nil || fields == nil {
				return fail(errors.New("definition must be an object"))
			}
			if _, ok := fields["edit"]; ok {
				return fail(errors.New("edit is an action, not a definition field"))
			}
			if _, ok := fields["remove"]; ok {
				return fail(errors.New("remove is an action, not a definition field"))
			}
		} else {
			fields = map[string]json.RawMessage{}
		}
		if req.Action == "edit" || req.Action == "remove" {
			if req.ID == "" {
				return fail(errors.New("job id required"))
			}
			key := "edit"
			if req.Action == "remove" {
				key = "remove"
			}
			fields[key], _ = json.Marshal(req.ID)
		}
		data, _ := json.Marshal(fields)
		j, err := c.s.proposal(who, data)
		if err != nil {
			return fail(err)
		}
		if req.Action == "create" {
			if msg := c.s.capped(who); msg != "" {
				return fail(errors.New(msg))
			}
		}
		result = ControlResult{Reply: c.s.card(who, j), DraftID: j.ID, Changed: true}
		change = &store.JobChange{Add: &j}
		// An external owner origin has no Heimdall chat to confirm in: like a
		// direct owner chat request, create/edit apply at once. Removal and
		// rerun_on_timeout keep the draft and its human confirmation.
		if d, _, _ := parse(j); who.External != "" && req.Action != "remove" && !d.RerunOnTimeout {
			msg, applied := c.s.confirmJob(who, j)
			if applied == nil {
				return fail(errors.New(strings.ReplaceAll(msg, "\n", " ")))
			}
			applied.Add = &j
			id := j.ID
			if j.EditOf != "" {
				id = j.EditOf
			}
			result, change = ControlResult{Reply: msg + "\n— heimdall", JobID: id, Changed: true}, applied
		}
		outs = []core.Outbound{{Conv: who.Conv, Text: result.Reply, Formatted: true}}
	case "acknowledge":
		change = &store.JobChange{Acknowledge: &store.AlertAcknowledgement{Job: req.ID, Creator: who.Principal.Key(), Key: req.Key, Revision: req.Revision, Outbound: req.Outbound}}
		result = ControlResult{Reply: "Failed receipt acknowledged; history retained, nothing replayed. — heimdall", Changed: true}
	case "run", "pause", "resume", "stop":
		if req.ID == "" {
			return fail(errors.New("job id required"))
		}
		result.Reply, change = c.s.Command(who, "job "+req.Action+" "+req.ID)
		result.Changed = change != nil
	default:
		return fail(errors.New("unsupported action; confirmation is accepted only from real platform human input"))
	}
	response, _ := json.Marshal(result)
	if len(valid) > 0 && !valid[0]() {
		return fail(errors.New("request origin expired before commit"))
	}
	if err := c.s.st.ControlCommit(who.Principal.Key(), req.RequestID, digest, req.Action, req.ID, string(response), uid, change, outs); err != nil {
		return fail(err)
	}
	if change != nil && change.StopJob != "" {
		c.s.stopRun(change.StopJob)
	}
	c.s.notify(outs...)
	c.s.Wake()
	c.s.log.Printf("scheduler control: operator uid %d, action %s, changed %t", uid, req.Action, result.Changed)
	return result
}

// CallControl is shared by the CLI and stdio adapter. It never opens the store.
// task admits or reads durable tasks for a bound live owner turn only:
// add/list/show, nothing else (no verdicts, retries, pauses or worker).
// The creator, conversation, privacy and OAuth actor are the bound
// caller's; task_add needs a stable request_id and is answered once per
// request (the existing control response record); a changed request under
// the same id is refused.
func (c *Control) task(uid uint32, req ControlRequest, who Caller, valid []func() bool) ControlResult {
	fail := func(msg string) ControlResult { return ControlResult{Error: msg} }
	if c.s.Tasks == nil {
		return fail("durable tasks are not enabled here")
	}
	if who.Job != "" {
		return fail("scheduled runs cannot admit or read durable tasks; a live human request is required")
	}
	if req.Key != "" || req.Revision != "" || req.Outbound != 0 || len(req.Definition) > 0 {
		return fail("task actions take only id, title and request_id")
	}
	bad := func(s string, n int) bool { return len(s) > n || strings.ContainsAny(s, "\r\n") }
	switch req.Action {
	case "task_list":
		if req.ID != "" || req.Title != "" || req.RequestID != "" {
			return fail("task_list takes no arguments")
		}
		reply, _ := c.s.Tasks(who, "list", "", "")
		return ControlResult{Reply: reply}
	case "task_show":
		if req.ID == "" || bad(req.ID, 64) || strings.ContainsAny(req.ID, " \t") || req.Title != "" || req.RequestID != "" {
			return fail("task_show takes only an issue id")
		}
		reply, _ := c.s.Tasks(who, "show", req.ID, "")
		return ControlResult{Reply: reply}
	case "task_add":
	default:
		return fail("unsupported task action (task_add, task_list, task_show only)")
	}
	if req.ID == "" || bad(req.ID, 64) || strings.ContainsAny(req.ID, " \t") || strings.TrimSpace(req.Title) == "" || bad(req.Title, 500) {
		return fail("task_add needs an issue id and a one-line title (up to 500 bytes)")
	}
	if req.RequestID == "" || bad(req.RequestID, 128) {
		return fail("mutation requires a stable request_id (1-128 bytes); reuse only for the same request")
	}
	if who.External != "" && who.Principal.Platform == core.PlatformSlack && (who.OAuthActor == "" || who.OAuthInstallation == "") {
		return fail("live Slack verification gave no native OAuth actor; the task was not admitted")
	}
	raw, _ := json.Marshal(struct {
		Request ControlRequest
		Source  core.ConvKey
	}{req, who.Conv})
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if saved, ok, err := c.s.st.ControlResponse(who.Principal.Key(), req.RequestID, digest); err != nil {
		return fail(err.Error())
	} else if ok {
		var result ControlResult
		_ = json.Unmarshal([]byte(saved), &result)
		return result
	}
	if len(valid) > 0 && !valid[0]() {
		return fail("request origin expired before admission")
	}
	// Admission is idempotent by issue: a crash before the record below
	// leaves a retry of the same request answering "already tracked".
	reply, admitted := c.s.Tasks(who, "add", req.ID, req.Title)
	result := ControlResult{Reply: reply, Changed: admitted}
	response, _ := json.Marshal(result)
	if err := c.s.st.ControlCommit(who.Principal.Key(), req.RequestID, digest, req.Action, req.ID, string(response), uid, nil, nil); err != nil {
		return fail(err.Error())
	}
	c.s.log.Printf("scheduler control: operator uid %d, action %s, changed %t", uid, req.Action, admitted)
	return result
}

func CallControl(ctx context.Context, socket string, req ControlRequest) (ControlResult, error) {
	var out ControlResult
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return out, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if err = json.NewEncoder(conn).Encode(req); err != nil {
		return out, err
	}
	// A maximum-size checkpoint includes JSON-escaped state and 512 receipts.
	// Keep the response bounded without truncating a valid snapshot.
	err = json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&out)
	if err == nil && out.Error != "" {
		err = fmt.Errorf("scheduler control: %s", out.Error)
	}
	return out, err
}
