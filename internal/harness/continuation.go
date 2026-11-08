package harness

// Native between-turn dispatch. Claude lifecycle/result origin fields were
// observed with Claude Code 2.1.319 and a loopback synthetic Messages server
// (2026-11-07); task types also compared with anthropics/claude-agent-sdk-python
// src/claude_agent_sdk/types.py. task_updated.patch.status and optional run_id
// checked against https://code.claude.com/docs/en/agent-sdk/typescript
// (SDKTaskUpdatedMessage, 2026-11-08). No task output_file is opened or delivered.
import (
	"encoding/json"
	"strings"
)

// Continuation is native output from the same owned process, without a fresh
// person request. A notice has no native result; a result retains the normal
// adapter's session, replay, subagent and terminal-event checks.
type Continuation struct {
	LossID      uint64 // one process loss, shared by all source records
	ID          uint64
	Phase       string // start, result, notice, settled
	Result      Result
	Notice      string
	Interrupted bool
}
type passiveTurn struct {
	bridge   bridge
	source   *Run
	id       uint64
	done     chan struct{}
	terminal string // aggregate task terminal status; never inferred from an answer
}

func bridgeDone(b bridge) bool {
	select {
	case <-b.answeredCh():
		return true
	default:
	}
	o := b.outcome()
	return o.failure != "" || o.human != ""
}
func (w *worker) emitLocked(r *Run, e Continuation) {
	if r.continuation != nil {
		if !r.continuation(e) && e.Phase == "start" {
			w.retiring = true
			w.current.Stop("native continuation could not be recorded")
		}
	}
}
func (w *worker) lossLocked(why string) {
	owners := map[*Run]bool{}
	pending := map[*passiveTurn]bool{}
	if len(w.frames) > 0 {
		owners[w.current] = true
	}
	for _, r := range w.tasks {
		owners[r] = true
	}
	for _, p := range w.continuing {
		pending[p] = true
		owners[p.source] = true
	}
	if w.background != nil {
		pending[w.background] = true
		owners[w.background.source] = true
	}
	// Consume every piece of loss bookkeeping before callbacks. Stop and Wait
	// may both run, but a frame/task/continuation is reported at most once.
	w.tasks = nil
	w.finished = nil
	w.notified = nil
	w.continuing = nil
	w.background = nil
	w.frames = nil
	w.frameBytes = 0
	w.seq++
	lossID := w.seq
	for r := range owners {
		w.seq++
		w.emitLocked(r, Continuation{ID: w.seq, LossID: lossID, Phase: "notice", Interrupted: true, Notice: "Pending background work was interrupted: " + why + ". It was not re-run; check existing results before resending.\n— heimdall"})
	}
	for p := range pending {
		close(p.done)
		w.emitLocked(p.source, Continuation{ID: p.id, Phase: "result", Interrupted: true, Notice: why, Result: Result{Cancelled: true, Unclean: true, SessionID: p.source.SessionID()}})
	}
}

// Terminal ownership is a bounded tombstone, not pending work. It routes a
// later native notification back to its source and rejects lifecycle replay.
func (w *worker) rememberFinishedLocked(key string, r *Run) {
	if w.finished == nil {
		w.finished = map[string]*Run{}
	}
	if len(w.finished) >= 128 && w.finished[key] == nil {
		for k := range w.finished {
			delete(w.finished, k)
			delete(w.notified, k)
			break
		}
	}
	w.finished[key] = r
}
func (w *worker) passiveLocked(source *Run) *passiveTurn {
	w.seq++
	p := &passiveTurn{source: source, id: w.seq, done: make(chan struct{})}
	input := &warmInput{WriteCloser: w.input.(*warmInput).WriteCloser}
	// A passive decoder must never close the process's shared stdin.
	input.retired.Store(true)
	ctx := source.ctx
	session := source.SessionID()
	switch source.harness {
	case "claude":
		p.bridge = &claudeStreamBridge{warm: true, ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), closed: make(chan struct{}), session: session, pending: map[string]*pendingSteer{}, seen: map[string]string{}}
	case "pi", "omp":
		p.bridge = &piRPCBridge{warm: true, ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), closed: make(chan struct{}), session: session, omp: source.harness == "omp", pending: map[string]*pendingSteer{}, seen: map[string]string{}}
	case "codex":
		p.bridge = &codexBridge{warm: true, ctx: ctx, input: input, ready: make(chan struct{}), answered: make(chan struct{}), thread: session, steering: map[string]chan SteerOutcome{}, contextRequests: map[string]map[string]any{}}
	}
	w.emitLocked(source, Continuation{ID: p.id, Phase: "start", Result: Result{SessionID: session}})
	return p
}
func (w *worker) finishPassiveLocked(p *passiveTurn) {
	defer close(p.done)
	o := p.bridge.outcome()
	res := Result{Answer: o.answer, Answers: o.answers, Failure: o.failure, NeedsHuman: o.human, SessionID: o.session, Warning: o.warning, Unclean: o.unclean, Ready: true}
	if len(res.Answers) == 0 && res.Answer != "" {
		res.Answers = []string{res.Answer}
	}
	res.Answers = clipAnswers(res.Answers, maxAnswer)
	res.Answer = joinAnswers(res.Answers)
	w.emitLocked(p.source, Continuation{ID: p.id, Phase: "result", Result: res})
	if res.Failure != "" || res.NeedsHuman != "" || res.Unclean {
		w.retiring = true
		w.current.Stop("unclean native continuation")
	}
}
func writeLine(b bridge, raw []byte) { _, _ = b.Write(append(append([]byte(nil), raw...), '\n')) }

func (w *worker) lineLocked(raw []byte) {
	if w.retiring {
		return
	}
	if w.current.harness == "claude" {
		w.claudeLineLocked(raw)
		return
	}
	if w.background != nil {
		writeLine(w.background.bridge, raw)
		if bridgeDone(w.background.bridge) {
			w.finishPassiveLocked(w.background)
			w.background = nil
		}
		return
	}
	if bridgeDone(w.current.bridge) {
		var e struct {
			Type   string `json:"type"`
			Method string `json:"method"`
			Params struct {
				Thread string `json:"threadId"`
				Turn   struct {
					ID string `json:"id"`
				} `json:"turn"`
			} `json:"params"`
		}
		if json.Unmarshal(raw, &e) == nil && (e.Type == "agent_start" || e.Method == "turn/started" && e.Params.Thread == w.current.SessionID()) {
			w.background = w.passiveLocked(w.current)
			writeLine(w.background.bridge, raw)
		}
		return
	}
	writeLine(w.current.bridge, raw)
}

// Native run IDs identify a shared decoder. Without one, only the task's
// composite key identifies its decoder; empty prefixes never group tasks.
func taskOrigin(key string) string {
	run, _, _ := strings.Cut(key, "\x00")
	if run == "" {
		return key
	}
	return run
}
func terminalStatus(old, next string) string {
	rank := map[string]int{"completed": 1, "killed": 2, "failed": 3}
	if rank[next] > rank[old] {
		return next
	}
	return old
}

func (w *worker) claudeLineLocked(raw []byte) {
	var e struct {
		Type    string          `json:"type"`
		Subtype string          `json:"subtype"`
		Session string          `json:"session_id"`
		UUID    string          `json:"uuid"`
		Replay  bool            `json:"isReplay"`
		Parent  json.RawMessage `json:"parent_tool_use_id"`
		Task    string          `json:"task_id"`
		RunID   string          `json:"run_id"`
		Patch   struct {
			Status string `json:"status"`
		} `json:"patch"`
		Message json.RawMessage `json:"message"`
		Origin  struct {
			Kind string `json:"kind"`
			Run  string `json:"runId"`
		} `json:"origin"`
	}
	if json.Unmarshal(raw, &e) != nil {
		writeLine(w.current.bridge, raw)
		return
	}
	// Never let a foreign session or a subagent influence dispatch/task ownership.
	if e.Session != "" && e.Session != w.current.SessionID() {
		writeLine(w.current.bridge, raw)
		return
	}
	if len(e.Parent) > 0 && string(e.Parent) != "null" {
		return
	}
	if e.Type == "result" && e.UUID != "" {
		if w.seenResults == nil {
			w.seenResults = map[string]bool{}
		}
		if w.seenResults[e.UUID] {
			return
		}
		w.seenResults[e.UUID] = true
		w.resultIDs = append(w.resultIDs, e.UUID)
		if len(w.resultIDs) > 256 {
			delete(w.seenResults, w.resultIDs[0])
			w.resultIDs = w.resultIDs[1:]
		}
	}
	if e.Type == "system" && e.Task != "" {
		if e.Replay {
			return
		}
		if w.tasks == nil {
			w.tasks = map[string]*Run{}
		}
		if w.notified == nil {
			w.notified = map[string]bool{}
		}
		key := e.RunID + "\x00" + e.Task
		switch e.Subtype {
		case "task_updated":
			switch e.Patch.Status {
			case "completed", "failed", "killed":
			default:
				return
			}
			// Older native updates omit run_id. Resolve only a unique owned
			// task; an ambiguous task ID cannot settle another run.
			matched := ""
			for k := range w.tasks {
				if k == key || e.RunID == "" && strings.HasSuffix(k, "\x00"+e.Task) {
					if matched != "" {
						return
					}
					matched = k
				}
			}
			if matched == "" || w.notified[matched] {
				return
			}
			source := w.tasks[matched]
			delete(w.tasks, matched)
			w.rememberFinishedLocked(matched, source)
			nativeKey := taskOrigin(matched)
			p := w.continuing[nativeKey]
			if p != nil {
				p.terminal = terminalStatus(p.terminal, e.Patch.Status)
			}
			// Settle only this decoder's group, with the aggregate outcome.
			for k := range w.tasks {
				if taskOrigin(k) == nativeKey {
					return
				}
			}
			if p != nil {
				delete(w.continuing, nativeKey)
				close(p.done)
				w.emitLocked(p.source, Continuation{ID: p.id, Phase: "settled", Notice: p.terminal, Result: Result{SessionID: source.SessionID()}})
			}
		case "task_started":
			if w.tasks[key] != nil || w.notified[key] || w.finished[key] != nil {
				return
			}
			if len(w.tasks) >= 128 {
				w.lossLocked("native task tracking limit reached")
				w.current.Stop("background task limit")
				return
			}
			if w.continuing == nil {
				w.continuing = map[string]*passiveTurn{}
			}
			nativeKey := taskOrigin(key)
			source := w.current
			if p := w.continuing[nativeKey]; p != nil {
				source = p.source
			}
			w.tasks[key] = source
			if w.continuing[nativeKey] == nil {
				w.continuing[nativeKey] = w.passiveLocked(source)
			}
		case "task_notification":
			if e.RunID == "" {
				match := ""
				for _, owners := range []map[string]*Run{w.tasks, w.finished} {
					for k := range owners {
						if strings.HasSuffix(k, "\x00"+e.Task) {
							if match != "" && match != k {
								return
							}
							match = k
						}
					}
				}
				if match != "" {
					key = match
				}
			}
			nativeKey := taskOrigin(key)
			source := w.tasks[key]
			if source == nil {
				source = w.finished[key]
			}
			if p := w.continuing[nativeKey]; p != nil {
				source = p.source
			}
			if source == nil {
				source = w.current
			}
			if !w.notified[key] && len(w.tasks) >= 128 {
				w.lossLocked("native task tracking limit reached")
				w.current.Stop("background task limit")
				return
			}
			if !w.notified[key] {
				delete(w.finished, key) // pending follow-through is no longer a terminal tombstone
				w.notified[key] = true
				// Keep ownership until the spontaneous result, including the gap between
				// task_notification and its init/assistant/result. Never read output_file.
				w.tasks[key] = source
				if w.continuing == nil {
					w.continuing = map[string]*passiveTurn{}
				}

				if w.continuing[nativeKey] == nil {
					w.continuing[nativeKey] = w.passiveLocked(source)
				}
				// Native task summaries may repeat tool inputs/descriptions.
				// Only assistant text and its result are chat output.
			}
		}
		return
	}
	// Replayed person input, rather than the first byte after next(), chooses
	// the pending decoder. A completion can win this race and still belongs to
	// the earlier input's caller/outbox.
	if e.Type == "user" && e.Replay && e.UUID != "" && string(e.Parent) == "null" && w.pending != nil {
		var m struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		b := w.pending.bridge.(*claudeStreamBridge)
		if json.Unmarshal(e.Message, &m) == nil && m.Role == "user" {
			if text, ok := claudeText(m.Content); ok && text == b.prompt {
				old := w.current
				w.current = w.pending
				w.pending = nil
				if old.bridgeCancel != nil {
					old.bridgeCancel()
				}
			}
		}
	}
	switch e.Type {
	case "assistant":
		if e.Replay {
			return
		}
		var message struct {
			Role string `json:"role"`
		}
		if e.Session == "" || json.Unmarshal(e.Message, &message) != nil || message.Role != "assistant" {
			return
		}
		taken := w.current
		if w.pending != nil {
			taken = w.pending
		}
		if b, ok := taken.bridge.(*claudeStreamBridge); ok && !bridgeDone(b) {
			b.mu.Lock()
			b.active = true
			b.readyOnce.Do(func() { close(b.ready) })
			b.mu.Unlock()
		}
		if bridgeDone(w.current.bridge) && len(w.continuing) == 0 && w.background == nil {
			w.background = w.passiveLocked(w.current)
		}
		w.telemetry.event("claude", raw, w.current.progress)
		// Results carry the native origin. Delay assistant blocks until that
		// boundary so an autonomous answer cannot contaminate a pending user run.
		if w.frameBytes+len(raw) > lineMax {
			w.lossLocked("native answer buffering exceeded the size bound")
			w.current.bridge.(*claudeStreamBridge).fail("Claude answer buffering exceeded the size bound")
			w.current.Stop("answer buffering limit")
			return
		}
		w.frames = append(w.frames, append([]byte(nil), raw...))
		w.frameBytes += len(raw)
	case "stream_event":
		// Public telemetry is optional; decoder content/answers remain held for
		// origin attribution. No private native frame is exposed directly.
		if w.current.progress != nil {
			w.telemetry.event("claude", raw, w.current.progress)
		}
	case "result":
		if w.pending != nil && e.Origin.Kind != "task-notification" {
			old := w.current
			w.current = w.pending
			w.pending = nil
			if old.bridgeCancel != nil {
				old.bridgeCancel()
			}
		}
		source := w.current
		autonomous := bridgeDone(w.current.bridge) || e.Origin.Kind == "task-notification"
		nativeKey := e.Origin.Run
		if e.Origin.Kind == "task-notification" {
			if nativeKey == "" {
				for key := range w.tasks {
					if w.notified[key] {
						origin := taskOrigin(key)
						if nativeKey != "" && nativeKey != origin {
							w.lossLocked("native result omitted an ambiguous task origin")
							w.current.Stop("ambiguous native task result")
							return
						}
						nativeKey = origin
					}
				}
			}
			if p := w.continuing[nativeKey]; p != nil {
				source = p.source
			}
			for key, r := range w.tasks {
				if taskOrigin(key) == nativeKey && w.notified[key] {
					delete(w.tasks, key)
					w.rememberFinishedLocked(key, r)
				}
			}
		}
		var target bridge
		var p *passiveTurn
		if autonomous {
			p = w.continuing[nativeKey]
			if p != nil {
				delete(w.continuing, nativeKey)
			}
			if p == nil {
				p = w.background
				w.background = nil
			}
			if p == nil {
				p = w.passiveLocked(source)
			}
			target = p.bridge
		} else {
			target = w.current.bridge
		}
		for _, frame := range w.frames {
			writeLine(target, frame)
		}
		w.frames = nil
		w.frameBytes = 0
		writeLine(target, raw)
		if p != nil {
			w.finishPassiveLocked(p)
			for key, owner := range w.tasks {
				if taskOrigin(key) == nativeKey {
					w.continuing[nativeKey] = w.passiveLocked(owner)
					break
				}
			}
		}
	case "control_request":
		if w.pending != nil {
			old := w.current
			w.current = w.pending
			w.pending = nil
			if old.bridgeCancel != nil {
				old.bridgeCancel()
			}
			b := w.current.bridge.(*claudeStreamBridge)
			b.mu.Lock()
			b.active = true
			b.readyOnce.Do(func() { close(b.ready) })
			b.mu.Unlock()
		}
		if bridgeDone(w.current.bridge) {
			p := w.passiveLocked(w.current)
			writeLine(p.bridge, raw)
			w.finishPassiveLocked(p)
			w.current.Stop("unsolicited approval request")
		} else {
			writeLine(w.current.bridge, raw)
		}
	default:
		writeLine(w.current.bridge, raw)
	}
}
