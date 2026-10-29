// Package harness runs one native agent turn in a fresh harness process,
// resuming the conversation's native session, and passes mid-turn input to
// it where the harness supports steering.
//
// The adapters are ported from AgentNet (github.com/misunders2d/agentnet,
// internal/client, revision b26fc68b91f6dd896a9c6c46350ad08b9b25d729,
// Apache-2.0). Native settings, skills, MCP servers and permissions stay the
// run-as user's own; Heimdall adds no tool exclusions or overrides.
package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// SteerOutcome is a harness's answer to mid-turn input. Only Accepted means
// the native run took it.
type SteerOutcome string

const (
	SteerAccepted    SteerOutcome = "accepted"    // native acknowledgement observed
	SteerQueued      SteerOutcome = "queued"      // not handed over: no active turn
	SteerUnsupported SteerOutcome = "unsupported" // refused or not steerable: not applied
	SteerUnknown     SteerOutcome = "unknown"     // may have reached the run: never replay
)

// HandedOver reports whether input with this outcome may have reached the
// native run, so it must not be sent again as a follow-up.
func (o SteerOutcome) HandedOver() bool { return o == SteerAccepted || o == SteerUnknown }

const (
	maxAnswer = 256 << 10 // reply text kept
	lineMax   = 4 << 20   // one native protocol line
)

type sessionStyle int

const (
	sessionDir          sessionStyle = iota // pi/omp: --session-dir DIR --continue
	sessionID                               // claude: --session-id UUID creates, --resume UUID continues
	sessionThread                           // codex: app-server thread/start, thread/resume
	sessionConversation                     // agy: reports conversation_id; --conversation ID continues
)

type def struct {
	bin   string
	args  []string // native launch, before session arguments
	style sessionStyle
	steer bool
}

// The native launch flags follow AgentNet's Harnesses table: native
// automatic approval or review where the harness has one, nothing else.
var defs = map[string]def{
	"omp":    {bin: "omp", args: []string{"--mode", "rpc", "--auto-approve"}, style: sessionDir, steer: true},
	"pi":     {bin: "pi", args: []string{"--mode", "rpc"}, style: sessionDir, steer: true},
	"claude": {bin: "claude", args: []string{"-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose", "--replay-user-messages", "--permission-mode", "auto"}, style: sessionID, steer: true},
	"codex":  {bin: "codex", args: []string{"app-server"}, style: sessionThread, steer: true},
	"agy":    {bin: "agy", args: []string{"--input-format", "stream-json", "--output-format", "stream-json"}, style: sessionConversation},
}

// Known reports whether name is a supported harness.
func Known(name string) bool { _, ok := defs[name]; return ok }

// Names lists supported harnesses, sorted.
func Names() []string {
	var out []string
	for k := range defs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Binary is the executable a harness is launched with.
func Binary(name string) string { return defs[name].bin }

// Steering reports whether the harness accepts input mid-turn.
func Steering(name string) bool { return defs[name].steer }

// NewSessionID returns the ID a new native session is created with, or ""
// when the harness names new sessions itself (codex, agy).
func NewSessionID(name string) string {
	switch defs[name].style {
	case sessionDir, sessionID:
		return newUUID()
	}
	return ""
}

// Session is the native session a turn runs in.
type Session struct {
	ID     string // native session ID; pi/omp: session directory name
	Resume bool   // continue ID; otherwise create a new session
}

// Spec is one turn.
type Spec struct {
	Harness string
	Prompt  string
	Session Session
	Launch  Launch
}

// Result is how a turn ended. Exactly one of Answer, Failure, NeedsHuman is
// set, unless the turn was cancelled.
type Result struct {
	Answer     string
	Failure    string // technical reason the turn ended without a native outcome
	NeedsHuman string // native approval or input required; nothing was approved
	Cancelled  bool
	Ready      bool   // the harness accepted the turn: side effects were possible
	SessionID  string // native session to resume next time, if known
}

// Run is one started native turn.
type Run struct {
	harness string
	cmd     *exec.Cmd
	bridge  bridge
	ctx     context.Context
	cancel  context.CancelFunc
	stderr  limitedBuffer
	done    chan struct{}
	result  Result
}

type outcome struct {
	answer, failure, human, session string
}

// bridge is one native protocol adapter: it reads the harness's stdout and
// owns its stdin.
type bridge interface {
	io.Writer
	readyCh() <-chan struct{}
	steer(ctx context.Context, id, text string) SteerOutcome
	settled() bool // a terminal native outcome was observed
	finish()       // the process exited: a missing terminal is a failure
	close()
	outcome() outcome
}

func sessionArgs(name string, s Session, home string) ([]string, error) {
	d := defs[name]
	if s.Resume && s.ID == "" {
		return nil, errors.New("resume without a native session ID")
	}
	switch d.style {
	case sessionDir:
		if s.ID == "" || strings.ContainsAny(s.ID, `/\`) || s.ID == "." || s.ID == ".." {
			return nil, fmt.Errorf("invalid %s session directory name %q", name, s.ID)
		}
		// An empty directory starts a new session; the same directory resumes.
		return []string{"--session-dir", filepath.Join(home, ".local", "state", "heimdall", "sessions", name, s.ID), "--continue"}, nil
	case sessionID:
		if s.ID == "" {
			return nil, errors.New("claude session needs an ID")
		}
		if s.Resume {
			return []string{"--resume", s.ID}, nil
		}
		return []string{"--session-id", s.ID}, nil
	case sessionConversation:
		if s.Resume {
			return []string{"--conversation", s.ID}, nil
		}
	}
	return nil, nil
}

// Start launches one turn. A returned error means nothing ran.
func Start(ctx context.Context, spec Spec) (*Run, error) {
	d, ok := defs[spec.Harness]
	if !ok {
		return nil, fmt.Errorf("unknown harness %q", spec.Harness)
	}
	if spec.Launch.Path == "" {
		return nil, fmt.Errorf("%s: no executable resolved", spec.Harness)
	}
	extra, err := sessionArgs(spec.Harness, spec.Session, spec.Launch.Home)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, spec.Launch.Path, append(slices.Clone(d.args), extra...)...)
	cmd.Dir, cmd.Env = spec.Launch.Dir, spec.Launch.Env
	setProcAttr(cmd, spec.Launch.Cred)
	cmd.WaitDelay = 5 * time.Second
	r := &Run{harness: spec.Harness, cmd: cmd, ctx: runCtx, cancel: cancel, done: make(chan struct{})}
	r.stderr.max = 4 << 10
	cmd.Stderr = &r.stderr
	expected := ""
	if spec.Session.Resume {
		expected = spec.Session.ID
	}
	switch d.style {
	case sessionDir:
		r.bridge, err = newPiRPCBridge(runCtx, cmd, spec.Prompt, spec.Harness == "omp")
	case sessionID:
		r.bridge, err = newClaudeStreamBridge(runCtx, cmd, spec.Prompt, spec.Session.ID)
	case sessionThread:
		r.bridge, err = newCodexBridge(runCtx, cmd, spec.Prompt, spec.Session)
	case sessionConversation:
		r.bridge = newAgyBridge(runCtx, cmd, spec.Prompt, expected)
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%s transport unavailable: %w", spec.Harness, err)
	}
	cmd.Stdout = r.bridge
	if err := cmd.Start(); err != nil {
		r.bridge.close()
		cancel()
		return nil, fmt.Errorf("%s did not start: %w", spec.Harness, err)
	}
	go r.wait()
	return r, nil
}

func (r *Run) wait() {
	runErr := r.cmd.Wait()
	// Only an observed native outcome supersedes a process error; finish's
	// missing-terminal fallback must not hide an actual exit failure.
	settled := r.bridge.settled()
	r.bridge.finish()
	r.bridge.close()
	stopGroup(r.cmd)
	o := r.bridge.outcome()
	res := Result{Answer: o.answer, Failure: o.failure, NeedsHuman: o.human, SessionID: o.session}
	select {
	case <-r.bridge.readyCh():
		res.Ready = true
	default:
	}
	switch {
	case r.ctx.Err() != nil && !settled:
		res = Result{Cancelled: true, Ready: res.Ready, SessionID: res.SessionID}
	case settled && res.Failure == "":
	case runErr != nil:
		why := fmt.Sprintf("%s exited: %v", r.harness, runErr)
		if res.Failure != "" {
			why = res.Failure + " (" + why + ")"
		}
		if tail := clip([]byte(r.stderr.String())); tail != "" {
			why += "; stderr: " + tail
		}
		res.Answer, res.Failure = "", why
	case res.Failure == "" && res.NeedsHuman == "" && strings.TrimSpace(res.Answer) == "":
		res.Failure = r.harness + " finished without an answer"
	}
	if len(res.Answer) > maxAnswer {
		res.Answer = res.Answer[:maxAnswer] + "\n[answer truncated by Heimdall]"
	}
	r.result = res
	close(r.done)
	r.cancel()
}

// Harness names the running harness.
func (r *Run) Harness() string { return r.harness }

// Ready is closed once the harness accepted the turn. Before that nothing
// can have happened, so a failure may fall back to another harness.
func (r *Run) Ready() <-chan struct{} { return r.bridge.readyCh() }

// Done is closed when the turn has ended and Result is final.
func (r *Run) Done() <-chan struct{} { return r.done }

// Result is the final outcome; call after Done.
func (r *Run) Result() Result { <-r.done; return r.result }

// SessionID is the native session as currently known.
func (r *Run) SessionID() string { return r.bridge.outcome().session }

// Cancel stops the turn and its process group.
func (r *Run) Cancel() { r.cancel() }

// Steer passes text into the running turn. id must be unique per input.
func (r *Run) Steer(ctx context.Context, id, text string) SteerOutcome {
	if !defs[r.harness].steer {
		return SteerUnsupported
	}
	return r.bridge.steer(ctx, id, text)
}

// limitedBuffer keeps the first max bytes written and notes truncation.
type limitedBuffer struct {
	mu        sync.Mutex
	b         strings.Builder
	max       int
	truncated bool
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if room := l.max - l.b.Len(); room < len(p) {
		l.truncated = true
		if room > 0 {
			l.b.Write(p[:room])
		}
		return len(p), nil
	}
	return l.b.Write(p)
}

func (l *limitedBuffer) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func clip(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
