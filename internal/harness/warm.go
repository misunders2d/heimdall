// Warm transport lifecycle compared with AgentNet dc39af639f18d8e452a54bf4417f7c60814ebd4d.
// Claude interrupt envelope verified against anthropics/claude-agent-sdk-python
// src/claude_agent_sdk/_internal/query.py (2026-11-01).
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	gate    <-chan struct{}
	retired atomic.Bool
}

func (p *warmInput) Read([]byte) (int, error) { return 0, io.EOF }
func (p *warmInput) Write(b []byte) (int, error) {
	if p.gate != nil {
		<-p.gate
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
	mu      sync.Mutex
	current *Run
	root    *Run
	cmd     *exec.Cmd
	input   io.WriteCloser
	exited  chan struct{}
	dead    bool
}
type workerOutput struct{ w *worker }

func (o *workerOutput) Write(p []byte) (int, error) {
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	return o.w.current.bridge.Write(p)
}
func (w *worker) wait() {
	err := w.cmd.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dead = true
	w.current.finishExit(err)
	if p, ok := w.input.(*warmInput); ok {
		_ = p.WriteCloser.Close()
	}
}
func (w *worker) next(spec Spec) (*Run, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dead {
		return nil, errors.New("worker exited")
	}
	old := w.current
	bridgeStdin(old.bridge).(*warmInput).retired.Store(true)
	if old.bridgeCancel != nil {
		old.bridgeCancel()
	}
	ctx, cancel := context.WithCancel(old.ctx)
	gate := make(chan struct{})
	input := w.input.(*warmInput)
	cmd := &exec.Cmd{Dir: spec.Launch.Dir, Stdin: &warmInput{WriteCloser: input.WriteCloser, thread: spec.Session.ID, gate: gate}}
	r := &Run{worker: w, caller: spec.Caller, harness: spec.Harness, cmd: w.cmd, ctx: old.ctx, cancel: old.cancel, bridgeCancel: cancel, done: make(chan struct{}), exited: w.exited, term: old.term, log: spec.Log}
	var err error
	switch spec.Harness {
	case "pi", "omp":
		r.bridge, err = newPiRPCBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session.ID, spec.Harness == "omp")
	case "claude":
		r.bridge, err = newClaudeStreamBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session.ID)
	case "codex":
		r.bridge, err = newCodexBridge(ctx, cmd, spec.Prompt, spec.Images, spec.Session)
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
	w.current = r
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
		Launch         Launch
		Caller         Caller
		Slack          *OwnerSlack
		Session        string
		Telemetry      bool
		NativeSettings string
	}{s.Harness, s.Launch, s.Caller, s.OwnerSlack, "", s.Progress != nil, nativeSettings(s)})
	return sha256.Sum256(b)
}
func (p *Pool) Start(ctx context.Context, s Spec) (*Run, error) {
	p.mu.Lock()
	key := warmKey{s.Caller.Conv, s.Caller.Principal.Key(), s.Harness}
	fp := launchFingerprint(s)
	epoch := p.epochs[key.Conv]
	retiring := p.retiring[key]
	e := p.entries[key]
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
		<-retiring.Exited()
	}
	if e != nil {
		if !e.run.cancelled.Load() && s.Session.Resume && s.Session.ID == e.run.Result().SessionID && e.fingerprint == fp && time.Since(e.born) < time.Duration(p.cfg.MaxLifetimeSeconds)*time.Second {
			r, err := e.run.worker.next(s)
			if err == nil {
				p.watch(r, key, fp, e.born, epoch)
				return r, nil
			}
		}
		e.run.Stop("launch changed or expired")
		<-e.run.Exited()
	}
	s.warm = s.Harness != "agy"
	r, err := Start(ctx, s)
	if err == nil {
		p.watch(r, key, fp, time.Now(), epoch)
	}
	return r, err
}
func (p *Pool) watch(r *Run, key warmKey, fp [32]byte, born time.Time, epoch uint64) {
	r.pooled = make(chan struct{})
	go func() {
		defer func() {
			select {
			case <-r.pooled:
			default:
				close(r.pooled)
			}
		}()
		timer := time.NewTimer(time.Duration(p.cfg.RunTimeoutSeconds) * time.Second)
		defer timer.Stop()
		life := time.NewTimer(max(time.Nanosecond, time.Until(born.Add(time.Duration(p.cfg.MaxLifetimeSeconds)*time.Second))))
		defer life.Stop()
		select {
		case <-r.Done():
		case <-timer.C:
			r.Cancel()
			return
		case <-life.C:
			r.Cancel()
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
		close(r.pooled)
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
