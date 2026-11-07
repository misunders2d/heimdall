// Warm transport lifecycle compared with AgentNet dc39af639f18d8e452a54bf4417f7c60814ebd4d.
// Claude interrupt envelope verified against anthropics/claude-agent-sdk-python
// src/claude_agent_sdk/_internal/query.py (2026-11-01).
package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

// WarmConfig bounds retained processes and each individual native run. Zero
// values select defaults. A pool belongs to one immutable router config.
type WarmConfig struct {
	IdleTimeoutSeconds int `json:"idle_timeout_seconds,omitempty"`
	MaxLifetimeSeconds int `json:"max_lifetime_seconds,omitempty"`
	MaxWarm            int `json:"max_warm,omitempty"`
	RunTimeoutSeconds  int `json:"run_timeout_seconds,omitempty"`
}

func (c WarmConfig) defaults() WarmConfig {
	if c.IdleTimeoutSeconds == 0 {
		c.IdleTimeoutSeconds = 900
	}
	if c.MaxLifetimeSeconds == 0 {
		c.MaxLifetimeSeconds = 14400
	}
	if c.MaxWarm == 0 {
		c.MaxWarm = 8
	}
	if c.RunTimeoutSeconds == 0 {
		c.RunTimeoutSeconds = 1200
	}
	return c
}
func (c WarmConfig) Validate() error {
	if c.IdleTimeoutSeconds < 0 || c.MaxLifetimeSeconds < 0 || c.MaxWarm < 0 || c.RunTimeoutSeconds < 0 {
		return errors.New("harness limits must be non-negative")
	}
	return nil
}

type warmInput struct {
	io.WriteCloser
	thread  string
	start   context.Context
	gate    <-chan struct{}
	retired atomic.Bool
}

func (p *warmInput) Read([]byte) (int, error) { return 0, io.EOF }
func (p *warmInput) Write(b []byte) (int, error) {
	if p.start != nil && p.start.Err() != nil {
		return 0, p.start.Err()
	}
	if p.gate != nil {
		if p.start == nil {
			<-p.gate
		} else {
			select {
			case <-p.gate:
			case <-p.start.Done():
				return 0, p.start.Err()
			}
		}
	}
	if p.start != nil && p.start.Err() != nil {
		return 0, p.start.Err()
	}
	return p.WriteCloser.Write(b)
}
func (p *warmInput) Close() error {
	if p.retired.Load() {
		return nil
	}
	return p.WriteCloser.Close()
}
func isWarmInput(p io.WriteCloser) bool { _, ok := p.(*warmInput); return ok }
func turnInput(cmd *exec.Cmd) (io.WriteCloser, error) {
	if p, ok := cmd.Stdin.(*warmInput); ok {
		return p, nil
	}
	cmd.Stdin = nil
	return cmd.StdinPipe()
}
func bridgeStdin(b bridge) io.WriteCloser {
	switch b := b.(type) {
	case *piRPCBridge:
		return b.input
	case *claudeStreamBridge:
		return b.input
	case *codexBridge:
		return b.input
	}
	return nil
}

type worker struct {
	mu          sync.Mutex
	current     *Run
	root        *Run
	cmd         *exec.Cmd
	input       io.WriteCloser
	exited      chan struct{}
	dead        bool
	buf         []byte
	frames      [][]byte
	frameBytes  int
	pending     *Run
	background  *passiveTurn
	continuing  map[string]*passiveTurn
	tasks       map[string]*Run
	notified    map[string]bool
	seq         uint64
	retiring    bool
	telemetry   progressTracker
	seenResults map[string]bool
	resultIDs   []string
}
type workerOutput struct{ w *worker }

func (o *workerOutput) Write(p []byte) (int, error) {
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	for rest := p; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			i = len(rest)
		}
		if len(o.w.buf)+i > lineMax {
			o.w.lossLocked("native protocol line exceeded the size bound")
			o.w.current.Stop("oversized native output")
			o.w.buf = nil
			return len(p), nil
		}
		o.w.buf = append(o.w.buf, rest[:i]...)
		if i == len(rest) {
			break
		}
		o.w.lineLocked(bytes.TrimSuffix(o.w.buf, []byte{'\r'}))
		o.w.buf = nil
		rest = rest[i+1:]
	}
	return len(p), nil
}
func (w *worker) wait() {
	err := w.cmd.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dead = true
	defer w.input.(*warmInput).WriteCloser.Close()
	w.lossLocked("native process exited before background work was delivered")
	if w.pending != nil {
		w.pending.finishExit(err)
		w.pending = nil
		return
	}
	w.current.finishExit(err)
	if p, ok := w.input.(*warmInput); ok {
		_ = p.WriteCloser.Close()
	}
}
func (w *worker) next(spec Spec, deadline time.Time) (*Run, error) {
	for {
		w.mu.Lock()
		if w.current.harness == "claude" || w.background == nil {
			break
		}
		done := w.background.done
		w.mu.Unlock()
		timer := time.NewTimer(max(time.Nanosecond, time.Until(deadline)))
		select {
		case <-done:
			timer.Stop()
		case <-spec.StartContext.Done():
			timer.Stop()
			return nil, spec.StartContext.Err()
		case <-w.root.ctx.Done():
			timer.Stop()
			return nil, errors.New("worker ended while background turn was running")
		case <-timer.C:
			w.root.Stop("background turn reached its native wait limit")
			return nil, errors.New("background turn did not settle before the native wait limit")
		}
	}
	defer w.mu.Unlock()
	if spec.StartContext.Err() != nil {
		return nil, spec.StartContext.Err()
	}
	if w.dead || w.retiring || w.current.cancelled.Load() {
		return nil, errors.New("worker exited")
	}
	old := w.current
	bridgeStdin(old.bridge).(*warmInput).retired.Store(true)
	if old.harness != "claude" && old.bridgeCancel != nil {
		old.bridgeCancel()
	}
	ctx, cancel := context.WithCancel(old.ctx)
	gate := make(chan struct{})
	input := w.input.(*warmInput)
	if w.root.contextDir != "" {
		entries, err := os.ReadDir(w.root.contextDir)
		if err != nil {
			cancel()
			return nil, err
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".json") {
				if err := os.Remove(filepath.Join(w.root.contextDir, entry.Name())); err != nil {
					cancel()
					return nil, err
				}
			}
		}
	}
	if _, err := stageContext(w.root.contextDir, spec.Prompt, spec.Context); err != nil {
		cancel()
		return nil, err
	}
	cmd := &exec.Cmd{Env: []string{"HD_CONTEXT_DIR=" + w.root.contextDir}, Dir: spec.Launch.Dir, Stdin: &warmInput{WriteCloser: input.WriteCloser, thread: spec.Session.ID, gate: gate, start: spec.StartContext}}
	r := &Run{contextDir: w.root.contextDir, worker: w, caller: spec.Caller, harness: spec.Harness, cmd: w.cmd, ctx: old.ctx, cancel: old.cancel, bridgeCancel: cancel, done: make(chan struct{}), exited: w.exited, term: old.term, log: spec.Log, continuation: spec.Continuation, progress: spec.Progress}
	var err error
	switch spec.Harness {
	case "pi", "omp":
		r.bridge, err = newPiRPCBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session.ID, spec.Harness == "omp")
	case "claude":
		r.bridge, err = newClaudeStreamBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session.ID)
	case "codex":
		r.bridge, err = newCodexBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session, spec.Instructions, spec.Context)
	}
	if err != nil {
		cancel()
		close(gate)
		return nil, err
	}
	switch b := r.bridge.(type) {
	case *piRPCBridge:
		b.progress = spec.Progress
		if spec.Caller.Role == core.RoleOwner && spec.Caller.Private {
			b.mcpNotify = spec.MCPNotify
		}
	case *claudeStreamBridge:
		b.progress = spec.Progress
	case *codexBridge:
		b.progress = spec.Progress
	}
	if spec.Harness == "claude" {
		w.pending = r // exact native replay chooses the new decoder, never timing
	} else {
		w.current = r
	}
	close(gate)
	go r.early()
	return r, nil
}
func (r *Run) interrupt() {
	switch b := r.bridge.(type) {
	case *piRPCBridge:
		_ = b.send(r.ctx, "", nil, map[string]any{"type": "abort"})
	case *claudeStreamBridge:
		_ = b.send(r.ctx, "", nil, map[string]any{"type": "control_request", "request_id": "heimdall-interrupt", "request": map[string]any{"subtype": "interrupt"}})
	case *codexBridge:
		b.stateMu.Lock()
		thread, turn := b.thread, b.turn
		b.stateMu.Unlock()
		b.send(map[string]any{"id": "heimdall-interrupt", "method": "turn/interrupt", "params": map[string]any{"threadId": thread, "turnId": turn}})
	}
}

type warmKey struct {
	Conv         core.ConvKey
	Who, Harness string
}
type warmEntry struct {
	key         warmKey
	fingerprint [32]byte
	run         *Run
	born, idle  time.Time
	retired     chan struct{}
}

// Pool retains only clean, completed turns. Active runs remain router-owned.
type Pool struct {
	mu       sync.Mutex
	cfg      WarmConfig
	entries  map[warmKey]*warmEntry
	retiring map[warmKey]*Run
	epochs   map[core.ConvKey]uint64
}

func NewPool(c WarmConfig) *Pool {
	return &Pool{cfg: c.defaults(), entries: map[warmKey]*warmEntry{}, retiring: map[warmKey]*Run{}, epochs: map[core.ConvKey]uint64{}}
}
func launchFingerprint(s Spec) [32]byte {
	// Prompt, images, callbacks and Resume change per turn, not at launch.
	b, _ := json.Marshal(struct {
		Harness        string
		Instructions   string
		ContextEnabled bool
		Launch         Launch
		Caller         Caller
		Slack          *OwnerSlack
		Session        string
		Telemetry      bool
		NativeSettings string
	}{s.Harness, s.Instructions, s.Context != "" || s.Instructions != "", s.Launch, s.Caller, s.OwnerSlack, "", s.Progress != nil, nativeSettings(s)})
	return sha256.Sum256(b)
}
func (p *Pool) Start(ctx context.Context, s Spec) (*Run, error) {
	if s.StartContext == nil {
		s.StartContext = ctx
	}
	if err := s.StartContext.Err(); err != nil {
		return nil, err
	}
	key := warmKey{s.Caller.Conv, s.Caller.Principal.Key(), s.Harness}
	fp := launchFingerprint(s)
	if s.OneShot {
		r, err := Start(ctx, s)
		if err == nil {
			p.watch(r, key, fp, time.Now(), 0, s.Timeout)
		}
		return r, err
	}
	p.mu.Lock()
	epoch := p.epochs[key.Conv]
	retiring := p.retiring[key]
	e := p.entries[key]
	claimed := e != nil
	if e != nil {
		delete(p.entries, key)
		e.run.retained.Store(false)
		close(e.retired)
		select {
		case <-e.run.Exited():
			e = nil
		default:
		}
	}
	p.mu.Unlock()
	if retiring != nil {
		select {
		case <-retiring.Exited():
		case <-s.StartContext.Done():
			return nil, s.StartContext.Err()
		}
		if s.Session.Resume && s.Session.ID == retiring.SessionID() {
			freshAfterWorkerLoss(&s)
		}
	}
	if claimed && e == nil || s.LiveWorkerOnly != nil && (e == nil || e.run != s.LiveWorkerOnly) {
		freshAfterWorkerLoss(&s)
	}
	if e != nil {
		if !e.run.cancelled.Load() && s.Session.Resume && s.Session.ID == e.run.Result().SessionID && e.fingerprint == fp && time.Since(e.born)+p.runLimit(s.Timeout) < time.Duration(p.cfg.MaxLifetimeSeconds)*time.Second {
			deadline := minTime(e.born.Add(time.Duration(p.cfg.MaxLifetimeSeconds)*time.Second), time.Now().Add(p.runLimit(s.Timeout)))
			r, err := e.run.worker.next(s, deadline)
			if err == nil {
				p.watch(r, key, fp, e.born, epoch, s.Timeout)
				return r, nil
			}
		}
		if s.StartContext.Err() != nil {
			// The new input never reached native stdin. Keep the original
			// process supervised; cancellation of this request is not a rerun.
			p.watch(e.run, key, e.fingerprint, e.born, epoch, 0)
			return nil, s.StartContext.Err()
		}
		// Every failed claim/reuse is a fresh session. Retirement may already
		// have consumed background flags, so those flags are not a safe gate.
		freshAfterWorkerLoss(&s)
		e.run.Stop("launch changed or expired")
		select {
		case <-e.run.Exited():
		case <-s.StartContext.Done():
			return nil, s.StartContext.Err()
		}
	}
	if err := s.StartContext.Err(); err != nil {
		return nil, err
	}
	// Employee Pi gets a new access-only handoff and confined cwd every turn.
	s.warm = s.Harness != "agy" && !(s.Harness == "pi" && s.Caller.Role == core.RoleEmployee)
	r, err := Start(ctx, s)
	if err == nil {
		p.watch(r, key, fp, time.Now(), epoch, s.Timeout)
	}
	return r, err
}
func (p *Pool) runLimit(limit time.Duration) time.Duration {
	if limit > 0 {
		return limit
	}
	return time.Duration(p.cfg.RunTimeoutSeconds) * time.Second
}

func (p *Pool) watch(r *Run, key warmKey, fp [32]byte, born time.Time, epoch uint64, limit time.Duration) {
	pooled := make(chan struct{})
	if r.pooled == nil {
		r.pooled = pooled
	}
	go func() {
		defer func() {
			select {
			case <-pooled:
			default:
				close(pooled)
			}
		}()
		if limit <= 0 {
			limit = time.Duration(p.cfg.RunTimeoutSeconds) * time.Second
		}
		timer := time.NewTimer(limit)
		defer timer.Stop()
		life := time.NewTimer(max(time.Nanosecond, time.Until(born.Add(time.Duration(p.cfg.MaxLifetimeSeconds)*time.Second))))
		var lifetime <-chan time.Time
		if r.worker != nil {
			lifetime = life.C
		}
		defer life.Stop()
		select {
		case <-r.Done():
		case <-timer.C:
			if r.cancelFor(CauseTimeout) {
				return
			}
		case <-lifetime:
			if !r.cancelFor(CauseLifetime) {
				r.Stop("worker lifetime reached after answer")
			}
			return
		case <-r.Exited():
			return
		}
		res := r.Result()
		if r.worker == nil {
			return
		}
		if res.Cancelled || res.Unclean || res.Failure != "" || res.NeedsHuman != "" {
			r.Stop("unclean turn")
			return
		}
		e := &warmEntry{key: key, fingerprint: fp, run: r, born: born, idle: time.Now(), retired: make(chan struct{})}
		p.mu.Lock()
		if p.epochs[key.Conv] != epoch || r.cancelled.Load() {
			p.retire(e, "session changed during turn")
			p.mu.Unlock()
			return
		}
		p.entries[key] = e
		r.retained.Store(true)
		for len(p.entries) > p.cfg.MaxWarm {
			var oldest *warmEntry
			for _, v := range p.entries {
				if oldest == nil || v.idle.Before(oldest.idle) {
					oldest = v
				}
			}
			delete(p.entries, oldest.key)
			close(oldest.retired)
			p.retire(oldest, "warm LRU eviction")
		}
		p.mu.Unlock()
		close(pooled)
		idle := time.NewTimer(time.Duration(p.cfg.IdleTimeoutSeconds) * time.Second)
		defer idle.Stop()
		select {
		case <-e.retired:
			return
		case <-idle.C:
		case <-life.C:
		case <-r.Exited():
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.entries[key] == e {
			delete(p.entries, key)
			p.retire(e, "warm idle or lifetime expiry")
		}
	}()
}

// Invalidate discards idle processes at an explicit authority/session change.
func (p *Pool) Invalidate(conv core.ConvKey, who string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.epochs[conv]++
	for k, e := range p.entries {
		if k.Conv == conv && (who == "" || k.Who == who) {
			delete(p.entries, k)
			close(e.retired)
			p.retire(e, "session invalidated")
		}
	}
}

type WarmStatus struct {
	Harness string
	Idle    time.Duration
}

func (p *Pool) Status(conv core.ConvKey) []WarmStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []WarmStatus
	for k, e := range p.entries {
		if k.Conv == conv {
			select {
			case <-e.run.Exited():
				continue
			default:
			}
			out = append(out, WarmStatus{k.Harness, time.Since(e.idle)})
		}
	}
	return out
}

// Warm reports a reusable transport after its result has been retained.
func (r *Run) Warm() bool {
	if r.worker == nil {
		return false
	}
	if r.pooled != nil {
		<-r.pooled
	}
	res := r.Result()
	return r.retained.Load() && !r.cancelled.Load() && !res.Cancelled && !res.Unclean && res.Failure == "" && res.NeedsHuman == ""
}

// retire is called under mu; a new process for this same session waits for exit.
func (p *Pool) retire(e *warmEntry, why string) {
	e.run.retained.Store(false)
	p.retiring[e.key] = e.run
	e.run.Stop(why)
	go func() {
		<-e.run.Exited()
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.retiring[e.key] == e.run {
			delete(p.retiring, e.key)
		}
	}()
}

func (r *Run) Process() *Run {
	if r.worker != nil {
		return r.worker.root
	}
	return r
}

// Hash only native configuration, never transcripts or credentials. Credential
// refresh is additionally bounded by the worker's hard lifetime.
func nativeSettings(s Spec) string {
	paths := []string{}
	switch s.Harness {
	case "claude":
		paths = []string{filepath.Join(s.Launch.Home, ".claude", "settings.json"), filepath.Join(s.Launch.Home, ".claude.json"), filepath.Join(s.Launch.Dir, ".claude", "settings.json"), filepath.Join(s.Launch.Dir, ".claude", "settings.local.json"), filepath.Join(s.Launch.Dir, ".mcp.json")}
	case "codex":
		paths = []string{filepath.Join(s.Launch.Home, ".codex", "config.toml"), filepath.Join(s.Launch.Dir, ".codex", "config.toml")}
	case "pi", "omp":
		paths = []string{filepath.Join(s.Launch.Home, "."+s.Harness, "agent", "settings.json"), filepath.Join(s.Launch.Dir, "."+s.Harness, "settings.json")}
	}
	h := sha256.New()
	for _, path := range paths {
		h.Write([]byte(path))
		data, err := os.ReadFile(path)
		if err == nil {
			if filepath.Base(path) == ".claude.json" {
				var cfg map[string]json.RawMessage
				if json.Unmarshal(data, &cfg) == nil {
					data = cfg["mcpServers"]
				}
			}
			h.Write(data)
		} else if !os.IsNotExist(err) {
			h.Write([]byte(time.Now().String()))
		}
	}
	if info, err := os.Stat(s.Launch.Path); err == nil {
		h.Write([]byte(info.ModTime().String()))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// NativeSession is usable only while its already-owned warm process exists.
// The durable binding stays unclean while native background work is outstanding,
// so a restart can never mistake it for a clean resumable session.
func (p *Pool) NativeSession(conv core.ConvKey, who, name string) (string, *Run) {
	return p.retainedSession(conv, who, name, true)
}

// RetainedSession captures the exact process before a clean-binding snapshot.
// Losing that process after the snapshot cannot turn into a cold dirty resume.
func (p *Pool) RetainedSession(conv core.ConvKey, who, name string) (string, *Run) {
	return p.retainedSession(conv, who, name, false)
}
func (p *Pool) retainedSession(conv core.ConvKey, who, name string, requireBackground bool) (string, *Run) {
	key := warmKey{conv, who, name}
	p.mu.Lock()
	e := p.entries[key]
	p.mu.Unlock()
	if e == nil || e.run.worker == nil || e.run.cancelled.Load() || !e.run.worker.available(requireBackground) {
		return "", nil
	}
	select {
	case <-e.run.Exited():
		return "", nil
	default:
	}
	id := e.run.SessionID()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries[key] != e || e.run.cancelled.Load() {
		return "", nil
	}
	return id, e.run
}
func (w *worker) available(requireBackground bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.dead && !w.retiring && (!requireBackground || len(w.tasks) > 0 || w.background != nil || len(w.frames) > 0 || len(w.continuing) > 0)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func freshAfterWorkerLoss(s *Spec) {
	if s.Session.Resume {
		s.freshWarning = "The native worker ended or changed; this input started a fresh session. Existing work was not re-run."
	}
	s.Session = Session{ID: NewSessionID(s.Harness)}
}
