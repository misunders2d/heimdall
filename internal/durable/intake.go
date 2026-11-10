package durable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/misunders2d/heimdall/internal/core"
)

// T3 request intake (owner, 2026-11-10: "all my requests through t3 land
// in a job queue"). A T3 session's prompt-submit and stop hooks call the
// scheduler control (task_intake, task_intake_done) with the thread id, a
// pointer only. The control binds the caller to the owner through the
// trusted T3 lineage. Here the owner check is T3's own record: the
// thread's latest message T3 marks createdBy=user, read through the
// configured T3 CLI. Each owner request becomes a tracking-only task
// T3-<n> (no wake, dispatch or message). It closes quietly when its T3 run
// ends completed or cancelled (the stop hook asks for a look), or when a
// newer owner request in the same thread supersedes it. A run that ends
// interrupted or failed makes it an active assistant task carrying the
// request and the thread pointer, under the wake rules.

// OriginTrustedT3 is the registration source of the trusted T3 launcher.
const OriginTrustedT3 = "trusted T3"

const intakeName = "heimdall.t3intake"

// Intake is a T3 request a commitment follows.
type Intake struct {
	Thread  string `json:"thread"`
	Message string `json:"message,omitempty"` // T3's message id, when its CLI shows one
	Run     string `json:"run"`
}

// IntakeThread is a T3 thread as its host CLI shows it.
type IntakeThread struct {
	Title     string         `json:"title,omitempty"`
	Status    string         `json:"status"`
	LatestRun string         `json:"latest_run,omitempty"`
	ActiveRun string         `json:"active_run,omitempty"`
	Request   *IntakeMessage `json:"request,omitempty"` // the latest message T3 marks createdBy=user
	Agent     bool           `json:"agent,omitempty"`   // the latest user-role message is not the owner's
}

type IntakeMessage struct {
	ID   string `json:"id,omitempty"`
	Run  string `json:"run"`
	Text string `json:"text"`
}

// IntakeReader reads a T3 thread for intake, read-only. Implemented by T3
// (the configured CLI); a reader error refuses the intake.
type IntakeReader interface {
	ReadIntake(ctx context.Context, thread string) (IntakeThread, error)
}

// ErrNoCreatedBy: the CLI's messages carry no createdBy mark, so no owner
// request can be told apart; the intake is refused, never guessed.
var ErrNoCreatedBy = errors.New("t3 thread show: messages carry no createdBy")

// intakeTimeout bounds one CLI read for intake.
const intakeTimeout = 10 * time.Second

// ReadIntake runs `thread show <id> --json --messages 50` and reads the
// thread and the latest owner message. Output is parsed defensively: the
// CLI may print log lines before the JSON object.
func (t T3) ReadIntake(ctx context.Context, thread string) (IntakeThread, error) {
	id, err := t.thread("t3:" + thread)
	if err != nil {
		return IntakeThread{}, err
	}
	tt := t
	if tt.Timeout <= 0 || tt.Timeout > intakeTimeout {
		tt.Timeout = intakeTimeout
	}
	out, err := tt.run(ctx, "", "thread", "show", id, "--json", "--messages", "50")
	if err != nil {
		return IntakeThread{}, fmt.Errorf("t3 thread show: %w", err)
	}
	return parseIntake(out)
}

func parseIntake(out []byte) (IntakeThread, error) {
	var v struct {
		Thread *struct {
			Title       string  `json:"title"`
			Status      string  `json:"status"`
			LatestRunID *string `json:"latestRunId"`
			ActiveRunID *string `json:"activeRunId"`
		} `json:"thread"`
		Messages []map[string]json.RawMessage `json:"messages"`
	}
	if !firstObject(out, &v) || v.Thread == nil {
		return IntakeThread{}, errors.New("t3 thread show: unexpected output")
	}
	th := IntakeThread{Title: v.Thread.Title, Status: v.Thread.Status}
	if v.Thread.LatestRunID != nil {
		th.LatestRun = *v.Thread.LatestRunID
	}
	if v.Thread.ActiveRunID != nil {
		th.ActiveRun = *v.Thread.ActiveRunID
	}
	str := func(m map[string]json.RawMessage, k string) (string, bool) {
		var s string
		raw, ok := m[k]
		return s, ok && json.Unmarshal(raw, &s) == nil
	}
	marked := false
	for _, m := range v.Messages {
		if _, ok := m["createdBy"]; ok {
			marked = true
		}
		if role, _ := str(m, "role"); role != "user" {
			continue
		}
		by, _ := str(m, "createdBy")
		if by != "user" {
			th.Agent, th.Request = true, nil
			continue
		}
		run, _ := str(m, "runId")
		text, _ := str(m, "text")
		mid, ok := str(m, "id")
		if !ok {
			mid, _ = str(m, "messageId")
		}
		th.Agent, th.Request = false, &IntakeMessage{ID: mid, Run: run, Text: text}
	}
	if !marked && len(v.Messages) > 0 {
		return IntakeThread{}, ErrNoCreatedBy
	}
	return th, nil
}

// firstObject decodes the first line-initial JSON object of out that
// decodes into v (log lines may come first, and may hold braces).
func firstObject(out []byte, v any) bool {
	for off := 0; off < len(out); {
		line := out[off:]
		if n := bytes.IndexByte(line, '\n'); n >= 0 {
			line = line[:n+1]
		}
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("{")) {
			if json.NewDecoder(bytes.NewReader(out[off:])).Decode(v) == nil {
				return true
			}
		}
		off += len(line)
	}
	return false
}

// validThread is a T3 thread id as a pointer: no flags, spaces or control
// characters. Agent-delegated threads (mcp:launch:*) are skipped by the
// caller: that is a filter, never identity.
func validThread(id string) bool {
	if id == "" || len(id) > 128 || strings.HasPrefix(id, "-") {
		return false
	}
	for _, r := range id {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// Delegated reports an agent-delegated T3 thread, which intake skips.
func Delegated(thread string) bool { return strings.HasPrefix(thread, "mcp:launch:") }

type intakeProbe struct {
	Thread     string         `json:"thread"`
	Creator    string         `json:"creator"`
	Owner      core.Principal `json:"owner"`
	Route      core.ConvKey   `json:"route"`
	Private    bool           `json:"private,omitempty"`
	OAuthActor string         `json:"oauth_actor,omitempty"`
	OAuthInst  string         `json:"oauth_installation,omitempty"`
}

// intake answers task_intake at once and leaves the T3 read to a durable
// probe workflow, so a slow CLI never outlives the hook or holds the
// control.
func (r *Runtime) intake(c Caller, thread string) (string, bool) {
	if c.Origin != OriginTrustedT3 || !c.Live || c.Role != core.RoleOwner {
		return "T3 intake is only for the trusted T3 hooks of the owner.", false
	}
	if !validThread(thread) {
		return "Not a T3 thread id.", false
	}
	if Delegated(thread) {
		return "Delegated T3 thread: not an owner request.", false
	}
	if _, ok := r.opt.Observer.(IntakeReader); !ok {
		r.opt.Log.Printf("durable: T3 intake refused: no T3 CLI configured (thread %s)", thread)
		return "T3 intake is not configured here.", false
	}
	in := intakeProbe{Thread: thread, Creator: c.Principal.Key(), Owner: c.Principal, Route: c.Conv, Private: c.Private, OAuthActor: c.OAuthActor, OAuthInst: c.OAuthInstallation}
	id := "t3intake/" + thread + "/" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := dbos.RunWorkflow(r.ctx, r.probe, in, dbos.WithWorkflowID(id)); err != nil {
		r.opt.Log.Printf("durable: T3 intake not started (thread %s): %v", thread, err)
		return "T3 intake unavailable.", false
	}
	return "T3 request noted.", false
}

// intakeRetries: the prompt-submit hook may run before T3 shows the new
// message; the probe reads again a few times (1, 2, 4, 8 s).
const intakeRetries = 4

var intakePause = time.Second

type intakeRead struct {
	Th   IntakeThread `json:"th"`
	Err  string       `json:"err,omitempty"`
	Kind string       `json:"kind,omitempty"` // why it was refused: cli, created_by
}

// probe reads the thread once the control has answered, and admits the
// latest owner request (createdBy=user) as T3-<n>, once per T3 message and
// run. Anything it cannot verify is refused with one metadata-only log
// line, never a task.
func (r *Runtime) probe(ctx dbos.Context, in intakeProbe) (string, error) {
	rd, ok := r.opt.Observer.(IntakeReader)
	if !ok {
		return "no reader", nil
	}
	self, err := dbos.GetWorkflowID(ctx)
	if err != nil {
		return "", err
	}
	for i := 0; ; i++ {
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (intakeRead, error) {
			th, err := rd.ReadIntake(sctx, in.Thread)
			switch {
			case errors.Is(err, ErrNoCreatedBy):
				return intakeRead{Err: err.Error(), Kind: "created_by"}, nil
			case err != nil:
				return intakeRead{Err: err.Error(), Kind: "cli"}, nil
			}
			return intakeRead{Th: th}, nil
		}, dbos.WithStepName("intake-read"))
		if err != nil {
			return "", err
		}
		switch {
		case got.Err != "":
			r.opt.Log.Printf("durable: T3 intake refused (%s), thread %s", got.Kind, in.Thread)
			return "refused: " + got.Kind, nil
		case Delegated(got.Th.Title):
			return "delegated", nil
		case got.Th.Agent:
			return "not the owner's message", nil // an agent's prompt: nothing to track
		case got.Th.Request == nil || got.Th.Request.Run == "":
			if i < intakeRetries {
				break
			}
			r.opt.Log.Printf("durable: T3 intake refused (no owner message), thread %s", in.Thread)
			return "refused: no owner message", nil
		default:
			m := got.Th.Request
			current := m.Run == got.Th.ActiveRun || m.Run == got.Th.LatestRun
			out, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
				if !current {
					return "", nil
				}
				return r.admitIntake(in, *m, self)
			}, dbos.WithStepName("intake-admit"))
			if err != nil {
				return "", err
			}
			if out != "" {
				return out, nil
			}
			if i >= intakeRetries {
				return "nothing new", nil
			}
		}
		if _, err := dbos.Sleep(ctx, intakePause<<i); err != nil {
			return "", err
		}
	}
}

// admitIntake numbers the request (one row per thread, T3 message and run;
// a re-executed step of the same probe gets its own row back), admits
// T3-<n> and supersedes the thread's older intakes. "" means the request
// was already taken by another probe (nothing new yet).
func (r *Runtime) admitIntake(in intakeProbe, m IntakeMessage, probe string) (string, error) {
	// Look up, then insert, in one (immediate) transaction: a refused
	// insert would still use up an AUTOINCREMENT number.
	tx, err := r.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var n int64
	var owner string
	err = tx.QueryRow(`SELECT n,probe FROM heimdall_t3_intakes WHERE thread=? AND message=? AND run=?`, in.Thread, m.ID, m.Run).Scan(&n, &owner)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		res, err := tx.Exec(`INSERT INTO heimdall_t3_intakes(thread,message,run,creator,probe,at) VALUES(?,?,?,?,?,?)`, in.Thread, m.ID, m.Run, in.Creator, probe, time.Now().UnixMilli())
		if err != nil {
			return "", err
		}
		if n, err = res.LastInsertId(); err != nil {
			return "", err
		}
		owner = probe
	case err != nil:
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	if owner != probe {
		return "", nil
	}
	issue := fmt.Sprintf("T3-%d", n)
	text := strings.Join(strings.Fields(m.Text), " ")
	cm := Commitment{Issue: issue, Title: or(clip(text, 80), "T3 request"), Creator: in.Creator, Owner: in.Owner, OAuthActor: in.OAuthActor, OAuthInstallation: in.OAuthInst,
		Route: in.Route, Private: in.Private, Resource: defaultRes, Origin: OriginTrustedT3, Intake: &Intake{Thread: in.Thread, Message: m.ID, Run: m.Run},
		Brief: fmt.Sprintf("%s: the owner's request in T3 thread %s (run %s): %s", issue, in.Thread, m.Run, clip(m.Text, 4000))}
	if _, err := r.Admit(cm); err != nil {
		return "", err
	}
	rows, err := r.db.Query(`SELECT n FROM heimdall_t3_intakes WHERE thread=? AND n<? ORDER BY n DESC LIMIT 20`, in.Thread, n)
	if err != nil {
		return "", err
	}
	var older []int64
	for rows.Next() {
		var o int64
		if rows.Scan(&o) == nil {
			older = append(older, o)
		}
	}
	rows.Close()
	for _, o := range older {
		// Only a tracking intake acts on it; an active task ignores it.
		_ = dbos.Send(r.ctx, fmt.Sprintf("T3-%d", o), Event{Kind: "supersede", By: in.Creator}, topic, dbos.WithIdempotencyKey(fmt.Sprintf("supersede/T3-%d/%s", o, issue)))
	}
	return "admitted " + issue, nil
}

// intakeDone (the stop hook) asks the thread's latest intakes of this
// creator to look at their run now; only a tracking intake acts, and only
// on what the T3 CLI shows. It never closes another creator's task or a
// task that is not an intake.
func (r *Runtime) intakeDone(c Caller, thread string) (string, bool) {
	if c.Origin != OriginTrustedT3 || !c.Live || c.Role != core.RoleOwner {
		return "T3 intake is only for the trusted T3 hooks of the owner.", false
	}
	if !validThread(thread) {
		return "Not a T3 thread id.", false
	}
	rows, err := r.db.Query(`SELECT n FROM heimdall_t3_intakes WHERE thread=? AND creator=? ORDER BY n DESC LIMIT 5`, thread, c.Principal.Key())
	if err != nil {
		return "T3 intake unavailable.", false
	}
	var ns []int64
	for rows.Next() {
		var n int64
		if rows.Scan(&n) == nil {
			ns = append(ns, n)
		}
	}
	rows.Close()
	key := strconv.FormatInt(time.Now().UnixNano(), 36)
	for _, n := range ns {
		_ = dbos.Send(r.ctx, fmt.Sprintf("T3-%d", n), Event{Kind: "intake_done", By: c.Principal.Key()}, topic, dbos.WithIdempotencyKey("intake_done/"+key+fmt.Sprintf("/%d", n)))
	}
	return "T3 run end noted.", false
}

// track follows an intake's T3 run (tracking phase): nothing is woken,
// dispatched or sent. It closes quietly when the run ends completed,
// cancelled or rolled back, when a later run follows in the thread, or
// when superseded; done=false with no error means the run ended
// interrupted or failed and the request becomes an active task.
func (r *Runtime) track(ctx dbos.Context, w *run) (done bool, err error) {
	in := w.c.Intake
	rd, _ := r.opt.Observer.(IntakeReader)
	if err := r.publish(ctx, w, PhaseTracking, "follows the owner's T3 run "+in.Run+" in "+in.Thread+"; closes quietly when it ends"); err != nil {
		return false, err
	}
	poll := r.opt.Observe
	for {
		ev, ok, err := r.next(ctx, w, poll)
		if err != nil {
			return false, err
		}
		if ok {
			switch ev.Kind {
			case "close":
				return false, closed{ev}
			case "supersede":
				return true, r.quiet(ctx, w, "superseded by a newer request in the same T3 thread")
			case "intake_done":
				poll = r.opt.Observe
			default:
				continue
			}
		} else {
			poll = min(poll*2, 10*time.Minute)
		}
		if rd == nil {
			continue
		}
		got, err := dbos.RunAsStep(ctx, func(sctx context.Context) (intakeRead, error) {
			th, err := rd.ReadIntake(sctx, in.Thread)
			if err != nil {
				return intakeRead{Err: clip(err.Error(), 200)}, nil
			}
			return intakeRead{Th: th}, nil
		}, dbos.WithStepName("intake-watch"))
		if err != nil {
			return false, err
		}
		note := ""
		if got.Err != "" {
			note = "T3 unreadable: " + got.Err
		}
		if note != w.st.Note {
			w.st.Note = note
			if err := r.publish(ctx, w, w.st.Phase, w.st.Next); err != nil {
				return false, err
			}
		}
		th := got.Th
		switch {
		case got.Err != "", th.ActiveRun == in.Run:
			continue
		case th.LatestRun != in.Run:
			return true, r.quiet(ctx, w, "a later run followed in the T3 thread")
		case slices.Contains([]string{"completed", "cancelled", "rolled_back"}, th.Status):
			return true, r.quiet(ctx, w, "finished in T3 ("+th.Status+")")
		case th.Status == "failed" || th.Status == "interrupted":
			w.c.Brief += fmt.Sprintf("\nIts T3 run %s ended %s before finishing, so part of it may already be done: read the thread first, do not repeat what was done, then finish the request.", in.Run, th.Status)
			w.st.Note = ""
			return false, nil
		}
	}
}

// quiet closes an intake without any message.
func (r *Runtime) quiet(ctx dbos.Context, w *run, why string) error {
	w.st.Verdict, w.st.Note = why, ""
	return r.publish(ctx, w, PhaseClosed, "none: "+why)
}

// SetIntakePause sets the probe's first re-read pause and returns the
// restore (tests in other packages; set before Open).
func SetIntakePause(d time.Duration) (restore func()) {
	old := intakePause
	intakePause = d
	return func() { intakePause = old }
}
