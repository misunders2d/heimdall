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
	Action     string          `json:"action"`
	ID         string          `json:"id,omitempty"`
	Definition json.RawMessage `json:"definition,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
}

type ControlResult struct {
	Reply   string `json:"reply,omitempty"`
	DraftID string `json:"draft_id,omitempty"`
	Changed bool   `json:"changed"`
	Error   string `json:"error,omitempty"`
}

type Control struct {
	s     *Scheduler
	owner string // trusted deployment binding selector, never a request argument
	uid   uint32 // derived from existing run_as.owner OS account
	mu    sync.Mutex
}

// ListenControl adds a private local management endpoint to the existing process.
// No independent scheduler, network listener, token or admission list is created.
func (s *Scheduler) ListenControl(ctx context.Context, owner string, uid uint32, dir string) (*net.UnixListener, error) {
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
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "control.sock"), Net: "unix"})
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

func peerUID(c *net.UnixConn) (uint32, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var inner error
	err = raw.Control(func(fd uintptr) {
		cred, inner = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil {
		return 0, err
	}
	if inner != nil {
		return 0, inner
	}
	if cred == nil {
		return 0, errors.New("missing peer credentials")
	}
	return cred.Uid, nil
}

func (c *Control) serve(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	uid, err := peerUID(conn)
	result := ControlResult{Error: "operator not authorized"}
	if err == nil && (uid == 0 || uid == c.uid) {
		var req ControlRequest
		scan := bufio.NewScanner(conn)
		scan.Buffer(make([]byte, 4096), ControlMaxBytes)
		if !scan.Scan() || strictJSON(scan.Bytes(), &req) != nil {
			result.Error = "invalid control request"
		} else {
			result = c.execute(ctx, uid, req)
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

func (c *Control) caller(ctx context.Context) (Caller, error) {
	p, k, err := c.s.st.ControlBinding(c.owner)
	if err != nil {
		return Caller{}, err
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
	}
	r, _, ok := c.s.access.Decide(p, k)
	if !ok || r != core.RoleOwner {
		return Caller{}, errors.New("bound caller is not a current owner")
	}
	return Caller{Principal: p, Role: r, Conv: k, Private: true}, nil
}

func (c *Control) execute(ctx context.Context, uid uint32, req ControlRequest) ControlResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	fail := func(err error) ControlResult { return ControlResult{Error: err.Error()} }
	if uid != 0 && uid != c.uid {
		return fail(errors.New("operator not authorized"))
	}
	who, err := c.caller(ctx)
	if err != nil {
		return fail(err)
	}
	read := req.Action == "list" || req.Action == "show" || req.Action == "history" || req.Action == "checkpoint" || req.Action == "discover"
	if !read && (req.RequestID == "" || len(req.RequestID) > 128 || strings.ContainsAny(req.RequestID, "\r\n")) {
		return fail(errors.New("mutation requires a stable request_id (1-128 bytes); reuse only for the same request"))
	}
	if len(req.Definition) > 16000 || len(req.ID) > 64 {
		return fail(errors.New("control request too large"))
	}
	needsID := req.Action == "show" || req.Action == "history" || req.Action == "checkpoint" || req.Action == "edit" || req.Action == "remove" || req.Action == "run" || req.Action == "pause" || req.Action == "resume" || req.Action == "stop"
	if needsID && req.ID == "" {
		return fail(errors.New("job id required"))
	}
	if !needsID && req.ID != "" {
		return fail(errors.New("id is not valid for this action"))
	}
	if req.Action != "create" && req.Action != "edit" && len(req.Definition) > 0 {
		return fail(errors.New("definition is only valid for create/edit"))
	}
	if req.Action == "discover" {
		return ControlResult{Reply: "list, show, history, checkpoint, create, edit, run, pause, resume, stop, remove. create/edit/remove prepare a draft in the bound private chat; only its real human can confirm there. Other operations use existing job commands. Identity is bound by OS peer credentials and deployment configuration."}
	}
	if req.ID != "" {
		j, ok, err := c.s.st.Job(req.ID)
		if err != nil {
			return fail(err)
		}
		if !ok || j.Creator.Key() != who.Principal.Key() {
			return fail(errors.New("no job of the bound creator"))
		}
	}
	if req.Action == "list" {
		return ControlResult{Reply: c.s.list(who, false)}
	}
	if req.Action == "show" || req.Action == "history" {
		text, _ := c.s.Command(who, "job "+req.Action+" "+req.ID)
		return ControlResult{Reply: text}
	}
	if req.Action == "checkpoint" {
		cp, err := c.s.st.Checkpoint(req.ID, who.Principal.Key())
		if err != nil {
			return fail(err)
		}
		data, _ := json.Marshal(cp)
		return ControlResult{Reply: string(data)}
	}
	raw, _ := json.Marshal(req)
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if saved, ok, err := c.s.st.ControlResponse(who.Principal.Key(), req.RequestID, digest); err != nil {
		return fail(err)
	} else if ok {
		var result ControlResult
		_ = json.Unmarshal([]byte(saved), &result)
		if req.Action == "stop" {
			c.s.stopRun(req.ID)
		}
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
		outs = []core.Outbound{{Conv: who.Conv, Text: result.Reply}}
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
