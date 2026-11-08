package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

var (
	boss    = core.Principal{Platform: core.PlatformTelegram, ID: "1", Name: "boss"}
	worker  = core.Principal{Platform: core.PlatformTelegram, ID: "2", Name: "worker"}
	bossDM  = core.ConvKey{Interface: core.PlatformTelegram, Chat: "1"}
	workDM  = core.ConvKey{Interface: core.PlatformTelegram, Chat: "2"}
	team    = core.ConvKey{Interface: core.PlatformTelegram, Chat: "-100"}
	idRE    = regexp.MustCompile(`\bj[a-z0-9]{6}\b`)
	asBoss  = Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true}
	asWork  = Caller{Principal: worker, Role: core.RoleEmployee, Conv: workDM, Private: true}
	inGroup = Caller{Principal: boss, Role: core.RoleOwner, Conv: team}
)

type rig struct {
	dbPath string
	t      *testing.T
	s      *Scheduler
	st     *store.Store
	now    time.Time
	rows   []store.InboundRow // dispatched run messages
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "h.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r := &rig{t: t, st: st, dbPath: dbPath, now: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	access := core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{boss.Key(): true}}
	r.s = New(Config{DefaultTZ: "Europe/Kyiv", MaxConcurrent: 2, PerCreator: 1, GraceRecurring: 2 * time.Hour, GraceOneOff: 24 * time.Hour, PauseAfter: 3,
		EmployeeHarnesses: []string{"pi"}, Interfaces: []string{core.PlatformTelegram}}, st, access, log.New(io.Discard, "", 0))
	r.s.now = func() time.Time { return r.now }
	r.s.Attach(func(row store.InboundRow) { r.rows = append(r.rows, row) }, func(string) {}, func(string) bool { return false })
	return r
}

// cmd runs a job command and stores its change, as the router does with
// the command's answer.
func (r *rig) cmd(c Caller, text string) string {
	r.t.Helper()
	out, change := r.s.Command(c, text)
	r.apply(change)
	return out
}

// answer is a bare yes or no, stored as the router stores it.
func (r *rig) answer(c Caller, text string) (string, bool) {
	r.t.Helper()
	out, change, ok := r.s.Answer(c, text)
	r.apply(change)
	return out, ok
}

func (r *rig) apply(change *store.JobChange) {
	r.t.Helper()
	if err := r.st.AnswerCommand(0, core.ConvKey{}, store.CommandEnd{Detail: "job", Job: change}); err != nil {
		r.t.Fatal(err)
	}
	if change != nil && change.StopJob != "" {
		r.s.stopRun(change.StopJob)
	}
}

func (r *rig) add(c Caller, def string) string {
	r.t.Helper()
	out, change := r.s.Command(c, "job add "+def)
	if change == nil || change.Add == nil {
		r.t.Fatalf("not created: %s", out)
	}
	r.apply(change)
	return change.Add.ID
}

func (r *rig) tick() { r.s.tick(context.Background()) }

// run runs a job now (!job run), a second later than the last one.
func (r *rig) run(id string) {
	r.now = r.now.Add(time.Second)
	r.cmd(asBoss, "job run "+id)
	r.tick()
}

// sent drains the outbound queue: what Heimdall would deliver.
func (r *rig) sent() []core.Outbound {
	var out []core.Outbound
	for {
		row, ok, err := r.st.NextOutbound(core.PlatformTelegram)
		if err != nil || !ok {
			return out
		}
		_ = r.st.Sent(row.ID)
		out = append(out, row.Outbound)
	}
}

func (r *rig) runs(job string) []store.Run {
	runs, err := r.st.Runs(store.RunFilter{Job: job, Limit: 100})
	if err != nil {
		r.t.Fatal(err)
	}
	return runs
}

func (r *rig) job(id string) store.Job {
	j, _, _ := r.st.Job(id)
	return j
}

// finish ends the newest dispatched run as its turn would, with answer.
func (r *rig) finish(answer string, o Outcome) store.TurnEnd {
	r.t.Helper()
	row := r.rows[len(r.rows)-1]
	t, ok := r.s.Begin(row)
	if !ok {
		r.t.Fatal("run may not begin")
	}
	o.Turn, o.Result.Answer = t, answer
	turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
	if err != nil {
		r.t.Fatal(err)
	}
	e := r.s.End(o)
	if _, err := r.st.EndTurn(turn, row.Conv, row.From.Key(), e); err != nil {
		r.t.Fatal(err)
	}
	return e
}

func texts(outs []core.Outbound) string {
	var b strings.Builder
	for _, o := range outs {
		b.WriteString(o.Conv.Chat + ": " + o.Text + "\n")
	}
	return b.String()
}

func TestOneRunPerSlotGraceAndDowntime(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"hourly","schedule":{"cron":["0 * * * *"]},"prompt":"p"}`)
	r.now = r.now.Add(time.Hour + 30*time.Second) // 11:00:30
	r.tick()
	r.tick()
	if len(r.rows) != 1 || len(r.runs(id)) != 1 {
		t.Fatalf("dispatched %d, runs %d: one run per slot", len(r.rows), len(r.runs(id)))
	}
	// The same slot can never be recorded twice.
	if created, err := r.st.CreateRun(store.Run{Job: id, Slot: r.runs(id)[0].Slot, Due: r.now, Trigger: "schedule", State: store.RunPending}, r.job(id).NextSlot, r.job(id).NextDue); created || err != nil {
		t.Fatalf("second run of a slot: %v %v", created, err)
	}
	r.finish("done", Outcome{Harness: "pi"})
	// Clock back: nothing fires twice.
	r.now = r.now.Add(-10 * time.Minute)
	r.tick()
	if len(r.rows) != 1 {
		t.Fatal("a clock step back fired again")
	}
	// Down for 3 slots: only the newest runs, with one missed notice.
	r.sent()
	r.now = time.Date(2026, 11, 3, 14, 5, 0, 0, time.UTC)
	r.tick()
	if len(r.rows) != 2 || !strings.Contains(texts(r.sent()), "missed 2 run(s)") {
		t.Fatalf("after downtime: %d dispatched, runs %+v, job %+v", len(r.rows), r.runs(id), r.job(id))
	}
	r.finish("done", Outcome{Harness: "pi"})
	// Later than grace: missed, with a notice, not run.
	daily := r.add(asBoss, `{"name":"daily","schedule":{"cron":["0 9 * * *"]},"prompt":"p"}`)
	_ = r.st.UpdateJob(id, store.Set{"state": store.JobPaused})
	r.now = time.Date(2026, 11, 4, 10, 30, 0, 0, time.UTC) // 09:00 EET was 3.5 h ago
	r.tick()
	if len(r.rows) != 2 || r.runs(daily)[0].State != store.RunMissed || !strings.Contains(texts(r.sent()), "missed") {
		t.Fatalf("late slot ran or was not reported: %+v", r.runs(daily)[0])
	}
}

func TestOverlapCapsAndHold(t *testing.T) {
	r := newRig(t)
	skip := r.add(asBoss, `{"name":"a","schedule":{"every":"1m"},"prompt":"p"}`)
	queue := r.add(asWork, `{"name":"b","schedule":{"every":"1h"},"overlap":"queue_one","prompt":"p"}`)
	r.now = r.now.Add(61 * time.Minute)
	r.tick()
	if len(r.rows) != 2 {
		t.Fatalf("dispatched %d", len(r.rows))
	}
	// Both still running at their next slots.
	r.now = r.now.Add(time.Hour)
	r.tick()
	if got := r.runs(skip)[0]; got.State != store.RunSkipped {
		t.Fatalf("overlap skip: %+v", got)
	}
	if got := r.runs(queue)[0]; got.State != store.RunPending {
		t.Fatalf("queue_one: %+v", got)
	}
	r.now = r.now.Add(time.Hour)
	r.tick()
	if got := r.runs(queue)[0]; got.State != store.RunSkipped {
		t.Fatalf("queue_one keeps one: %+v", got)
	}
	// per_creator: a second job of a running creator waits.
	r2 := newRig(t)
	r2.add(asBoss, `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}`)
	r2.add(asBoss, `{"name":"y","schedule":{"every":"1h"},"prompt":"p"}`)
	// Held by drain: due runs wait.
	_ = r2.st.SetMeta(MetaHold, r2.now.Add(2*time.Hour).UnixMilli())
	r2.now = r2.now.Add(time.Hour)
	r2.tick()
	if len(r2.rows) != 0 {
		t.Fatal("a held scheduler started a run")
	}
	_ = r2.st.SetMeta(MetaHold, 0)
	r2.tick()
	if len(r2.rows) != 1 {
		t.Fatalf("per_creator 1: dispatched %d", len(r2.rows))
	}
}

func TestRecoverAfterRestart(t *testing.T) {
	r := newRig(t)
	plain := r.add(asWork, `{"name":"plain","schedule":{"every":"1h"},"prompt":"p"}`)
	idem := r.add(asBoss, `{"name":"idem","schedule":{"every":"1h"},"prompt":"p"}`)
	early := r.add(Caller{Principal: core.Principal{Platform: core.PlatformTelegram, ID: "3"}, Role: core.RoleEmployee, Conv: core.ConvKey{Interface: core.PlatformTelegram, Chat: "3"}, Private: true}, `{"name":"early","schedule":{"every":"1h"},"prompt":"p"}`)
	_ = r.st.UpdateJob(idem, store.Set{"def": strings.Replace(r.job(idem).Def, `"session"`, `"idempotent":true,"session"`, 1)})
	r.now = r.now.Add(time.Hour)
	r.tick()
	if len(r.rows) != 2 {
		t.Fatalf("dispatched %d", len(r.rows))
	}
	r.s.cfg.MaxConcurrent = 3
	r.tick()
	if len(r.rows) != 3 {
		t.Fatalf("dispatched %d", len(r.rows))
	}
	for _, row := range r.rows {
		turn, _ := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
		if row.Run.Job != early { // early's harness never signalled ready
			_ = r.st.TurnReady(turn)
		}
	}
	if _, err := r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := r.runs(plain)[0]; got.State != store.RunInterrupted || !strings.Contains(texts(r.sent()), "interrupted by a restart") {
		t.Fatalf("plain: %+v", got)
	}
	if got := r.runs(idem)[0]; got.State != store.RunPending || got.Cause != "re-run after restart" {
		t.Fatalf("idempotent: %+v", got)
	}
	if got := r.runs(early)[0]; got.State != store.RunPending || !strings.HasPrefix(got.Cause, "queued again") || got.Attempt != 1 {
		t.Fatalf("interrupted before ready: %+v", got)
	}
	// Its one re-run, interrupted again, is never re-run.
	r.rows = nil
	r.tick()
	r.rows = slices.DeleteFunc(r.rows, func(row store.InboundRow) bool { return row.Run.Job != idem })
	turn, _ := r.st.StartTurn(r.rows[0].Conv, core.RoleOwner, r.rows[0].From.Key(), "", []int64{r.rows[0].ID})
	_ = r.st.TurnReady(turn)
	_, _ = r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} })
	_ = r.s.Recover()
	if got := r.runs(idem)[0]; got.State != store.RunInterrupted {
		t.Fatalf("idempotent twice: %+v", got)
	}
	// A run handed over but never started stays for the router, which
	// starts it within grace.
	r.rows = nil
	r.now = r.now.Add(time.Hour)
	r.tick()
	_ = r.s.Recover()
	if _, ok := r.s.Begin(r.rows[0]); !ok {
		t.Fatal("a run not yet started was not run after the restart")
	}
}

func TestRetriesIdentityAndRole(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"often","schedule":{"every":"1m"},"prompt":"p"}`)
	r.now = r.now.Add(time.Minute)
	r.tick()
	// No harness started: retried with backoff, then failed.
	for attempt := range 3 {
		e := r.finish("", Outcome{Harness: "", Notes: []string{"pi failed before starting: connection refused"}})
		if e.Run.Retry.IsZero() {
			t.Fatalf("attempt %d not retried", attempt)
		}
		r.now = e.Run.Retry
		r.rows = nil
		r.s.start(context.Background(), r.now)
		if len(r.rows) != 1 {
			t.Fatalf("retry %d not started", attempt)
		}
	}
	if e := r.finish("", Outcome{Harness: ""}); !e.Run.Retry.IsZero() || e.Run.State != store.RunFailed {
		t.Fatalf("after 3 retries: %+v", e.Run)
	}
	_ = r.st.UpdateJob(id, store.Set{"state": store.JobDeleted})
	// The creator lost the owner role: an every-minute job is refused and
	// paused, and the owners are told.
	r2 := newRig(t)
	id = r2.add(asBoss, `{"name":"often","schedule":{"every":"1m"},"prompt":"p"}`)
	if _, _, err := r2.st.AddInbound(core.Inbound{Conv: bossDM, MsgID: "m", From: boss, Origin: core.OriginLive, Text: "hi", Private: true, At: r2.now}); err != nil {
		t.Fatal(err)
	}
	r2.s.access.Owners = map[string]bool{}
	r2.now = r2.now.Add(time.Minute)
	r2.tick()
	if j := r2.job(id); len(r2.rows) != 0 || j.State != store.JobPaused || !strings.Contains(texts(r2.sent()), "refused") {
		t.Fatalf("role change: %+v", j)
	}
	// Slack creators: a temporary error retries, a failed check refuses.
	r3 := newRig(t)
	r3.s.cfg.Interfaces = append(r3.s.cfg.Interfaces, core.PlatformSlack)
	slackBoss := core.SlackPrincipal("boss@example.com", "T1", "boss")
	r3.s.access.Owners[slackBoss.Key()] = true
	sid := r3.add(Caller{Principal: slackBoss, Role: core.RoleOwner, Conv: core.ConvKey{Interface: core.PlatformSlack, Chat: "D1"}, Private: true}, `{"name":"s","schedule":{"every":"1h"},"prompt":"p"}`)
	verify := func(context.Context, core.Principal) (Verified, bool, error) {
		return Verified{}, false, errors.New("timeout")
	}
	r3.s.Reverify = func(ctx context.Context, p core.Principal) (Verified, bool, error) { return verify(ctx, p) }
	r3.now = r3.now.Add(time.Hour)
	r3.tick()
	if got := r3.runs(sid)[0]; got.State != store.RunPending || got.Attempt != 1 {
		t.Fatalf("temporary error: %+v", got)
	}
	verify = func(context.Context, core.Principal) (Verified, bool, error) { return Verified{}, false, nil }
	r3.now = r3.runs(sid)[0].NotBefore
	r3.tick()
	if got := r3.runs(sid)[0]; got.State != store.RunFailed || got.Cause != "identity" || r3.job(sid).State != store.JobPaused {
		t.Fatalf("refused: %+v", got)
	}
	verify = func(_ context.Context, p core.Principal) (Verified, bool, error) {
		return Verified{Principal: p, OAuthActor: "sl_user_U1", OAuthInstallation: "sl_user_B1"}, true, nil
	}
	r3.cmd(Caller{Principal: slackBoss, Role: core.RoleOwner}, "job resume "+sid)
	r3.cmd(Caller{Principal: slackBoss, Role: core.RoleOwner}, "job run "+sid)
	r3.tick()
	if len(r3.rows) != 1 || r3.rows[0].OAuthActor != "sl_user_U1" || r3.rows[0].Run.Target.Interface != core.PlatformSlack {
		t.Fatalf("verified run: %+v", r3.rows)
	}
}

func TestEndOutcomes(t *testing.T) {
	r := newRig(t)
	quiet := r.add(asBoss, `{"name":"q","schedule":{"every":"1h"},"quiet":true,"prompt":"p"}`)
	r.run(quiet)
	if e := r.finish("NO_UPDATE", Outcome{Harness: "pi"}); e.Reply != nil || e.Run.State != store.RunQuiet {
		t.Fatalf("quiet: %+v", e)
	}
	if !strings.Contains(r.cmd(asBoss, "job history "+quiet), "quiet") {
		t.Fatal("history")
	}
	// The job's own blocks are dropped (jobs never create jobs); [job: done]
	// ends the job.
	once := r.add(asBoss, `{"name":"o","schedule":{"every":"1h"},"prompt":"p"}`)
	r.run(once)
	e := r.finish("all good\n[job: done]\n```heimdall-job\n{}\n```", Outcome{Harness: "pi"})
	if e.Reply == nil || e.Reply.Text != "all good" || e.Reply.Conv != bossDM || e.Run.JobState != store.JobDone {
		t.Fatalf("done: %+v %+v", e.Reply, e.Run)
	}
	// AgentNet: the instruction is in the turn; the marker makes it ok and
	// nothing is posted; without it, unverified with a notice.
	an := r.add(asBoss, `{"name":"a","schedule":{"every":"1h"},"to":"agentnet:peer/x","prompt":"p"}`)
	for _, c := range []struct{ answer, state string }{{"sent\nAGENTNET_SENT abc123", store.RunOK}, {"sent it", store.RunUnverified}} {
		r.run(an)
		turn, ok := r.s.Begin(r.rows[len(r.rows)-1])
		if !ok {
			t.Fatalf("runs %+v rows %d", r.runs(an), len(r.rows))
		}
		if !strings.Contains(strings.Join(turn.Notes, " "), "agentnet` CLI to `peer/x`") {
			t.Fatalf("notes %v %+v", ok, turn)
		}
		if e := r.finish(c.answer, Outcome{Harness: "pi"}); e.Reply != nil || e.Run.State != c.state {
			t.Fatalf("agentnet %q: %+v %+v", c.answer, e.Reply, e.Run)
		}
	}
	// Unverified (expect.tools) to a private target: delivered under the
	// warning; three in a row pause the job.
	ck := r.add(asBoss, `{"name":"c","schedule":{"every":"1h"},"prompt":"p","expect":{"tools":["web_*"]}}`)
	for i := range 3 {
		r.run(ck)
		e := r.finish("result", Outcome{Harness: "pi", Tools: []string{"bash"}})
		if e.Run.State != store.RunUnverified || !strings.HasPrefix(e.Reply.Text, "⚠ unverified: no tool matched web_*") {
			t.Fatalf("unverified %d: %+v %+v", i, e.Run, e.Reply)
		}
	}
	if r.job(ck).State != store.JobPaused {
		t.Fatalf("not paused after 3 unverified: %+v", r.job(ck))
	}
	r.cmd(asBoss, "job resume "+ck)
	r.run(ck)
	if e := r.finish("result", Outcome{Harness: "pi", Tools: []string{"web_search"}}); e.Run.State != store.RunOK || e.Reply.Text != "result" {
		t.Fatalf("tool matched: %+v", e.Run)
	}
	// An empty answer fails (a backstop: adapters already fail blank
	// native answers).
	r.run(ck)
	if e := r.finish("  ", Outcome{Harness: "pi", Tools: []string{"web_search"}}); e.Run.State != store.RunFailed || e.Run.Cause != "empty answer" || e.Reply != nil {
		t.Fatalf("empty: %+v", e.Run)
	}
	// Stopped by command; native failure details are sanitized.
	r.run(ck)
	if e := r.finish("", Outcome{Stopped: true}); e.Run.State != store.RunCancelled || e.Reply != nil {
		t.Fatalf("stopped: %+v", e.Run)
	}
	r.sent()
	r.run(ck)
	e = r.finish("", Outcome{Harness: "pi", Result: harness.Result{Failure: "HTTP 401 token sk-secret expired"}})
	if notes := texts(e.Notices); e.Run.State != store.RunFailed || strings.Contains(notes, "sk-secret") || !strings.Contains(notes, "sign-in or credentials") {
		t.Fatalf("failure notice %q", notes)
	}
}

func TestRoleRulesAndCommands(t *testing.T) {
	r := newRig(t)
	for _, def := range []string{
		`{"name":"x","schedule":{"every":"30m"},"prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"to":"tg:-100","prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"to":"agentnet:peer","prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"idempotent":true,"prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"harness":"codex","prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"timeout":"10m","prompt":"p"}`,
	} {
		if out := r.cmd(asWork, "job add "+def); !strings.Contains(out, "not created") {
			t.Errorf("employee %s: %s", def, out)
		}
	}
	for _, def := range []string{
		`{"name":"x","kind":"notify","schedule":{"every":"1h"},"to":"agentnet:peer","text":"t"}`,
		`{"name":"x","schedule":{"every":"1h"},"to":"slack:C1","prompt":"p"}`,
		`{"name":"x","kind":"watch","schedule":{"every":"1h"},"prompt":"p"}`,
		`{"name":"x","schedule":{"every":"1h"},"prompt":"p","bogus":1}`,
	} {
		if out := r.cmd(asBoss, "job add "+def); !strings.Contains(out, "not created") && !strings.Contains(out, "Usage") {
			t.Errorf("owner %s: %s", def, out)
		}
	}
	// A command changes nothing itself: the router stores its change with
	// the answer.
	if _, change := r.s.Command(asWork, `job add {"name":"x","schedule":{"every":"1h"},"prompt":"p"}`); change == nil || change.Add == nil {
		t.Fatal("job add returned no change")
	} else if jobs, _ := r.st.Jobs(store.JobFilter{}); len(jobs) != 0 {
		t.Fatalf("job add wrote %d job(s) before its answer", len(jobs))
	}
	mine := r.add(asWork, `{"name":"worker job","schedule":{"every":"1h"},"prompt":"p"}`)
	for range 9 {
		r.add(asWork, `{"name":"more","schedule":{"every":"1h"},"prompt":"p"}`)
	}
	if out := r.cmd(asWork, `job add {"name":"11th","schedule":{"every":"1h"},"prompt":"p"}`); !strings.Contains(out, "at most 10") {
		t.Fatalf("cap: %s", out)
	}
	boss := r.add(asBoss, `{"name":"boss job","schedule":{"cron":["0 9 * * MON"]},"prompt":"secret plan"}`)
	if out := r.cmd(asWork, "job show "+boss); !strings.Contains(out, "no job") {
		t.Fatalf("employee sees owner job: %s", out)
	}
	if out := r.cmd(asBoss, "jobs all"); !strings.Contains(out, "worker job") || !strings.Contains(out, "by telegram:2") {
		t.Fatalf("owner list: %s", out)
	}
	if out := r.cmd(inGroup, "jobs"); strings.Contains(out, "next") || !strings.Contains(out, "Scheduled task") {
		t.Fatalf("shared list shows more than names: %s", out)
	}
	if out := r.cmd(asBoss, "job show "+boss); !strings.Contains(out, "2026-11-09 09:00 EET (07:00 UTC)") || !strings.Contains(out, "secret plan") {
		t.Fatalf("show: %s", out)
	}
	// An edit applies from the next run, with its diff.
	out := r.cmd(asWork, "job edit "+mine+` {"prompt":"q"}`)
	if !strings.Contains(out, `Instructions updated.`) || r.job(mine).Version != 2 {
		t.Fatalf("edit: %s %+v", out, r.job(mine))
	}
	if out := r.cmd(asWork, "job pause "+mine); !strings.Contains(out, "Paused") || r.job(mine).State != store.JobPaused {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job resume "+mine); !strings.Contains(out, "Scheduled task resumed") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job renew "+mine+" 30d"); !strings.Contains(out, "3 Dec 2026") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job frobnicate"); !strings.Contains(out, "Job commands") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job rm "+mine); !strings.Contains(out, "Removed") || r.job(mine).State != store.JobDeleted {
		t.Fatal(out)
	}
	// Expiry ends a job with a notice.
	exp := r.add(asBoss, `{"name":"short","schedule":{"every":"1h"},"until":"90m","prompt":"p"}`)
	r.sent()
	r.now = r.now.Add(2 * time.Hour)
	r.tick()
	notices := ""
	for _, o := range r.sent() {
		if strings.Contains(o.Text, "\n\n*short*\n\n") {
			notices = o.Text
			break
		}
	}
	if r.job(exp).State != store.JobDone || !strings.Contains(notices, "✅ Scheduled task expired") || !strings.Contains(notices, "*✅ Finished*") || strings.Contains(notices, "*🟢 Active*") {
		t.Fatalf("expiry: %+v", r.job(exp))
	}
}

func TestDrafts(t *testing.T) {
	r := newRig(t)
	block := "ok\n```heimdall-job\n{\"name\":\"standup\",\"schedule\":{\"cron\":[\"0 9 * * MON-FRI\"]},\"prompt\":\"summarize\"}\n```"
	clean, draft, card := r.s.Draft(asBoss, []string{block})
	if clean[0] != "ok" || draft == nil || !strings.Contains(card, "Reply *Yes*") || !strings.HasSuffix(card, "— heimdall") {
		t.Fatalf("draft %q %q", clean, card)
	}
	r.apply(&store.JobChange{Add: draft})
	// Only the same caller, in the same conversation.
	for _, caller := range []Caller{asWork, inGroup} {
		if show := r.cmd(caller, "job show "+draft.ID); !strings.Contains(show, "no job") {
			t.Fatalf("draft shown to another caller/conversation: %s", show)
		}
	}
	other := Caller{Principal: worker, Role: core.RoleEmployee, Conv: bossDM}
	if _, handled := r.answer(other, "yes."); handled {
		t.Fatal("someone else's yes was taken")
	}
	if out := r.cmd(Caller{Principal: boss, Role: core.RoleOwner, Conv: team}, "job confirm "+draft.ID); !strings.Contains(out, "No draft") {
		t.Fatalf("confirmed elsewhere: %s", out)
	}
	if _, handled := r.answer(inGroup, "yes."); handled {
		t.Fatal("punctuated reply confirmed elsewhere")
	}
	// Two drafts: a bare yes asks which.
	_, second, _ := r.s.Draft(asBoss, []string{block})
	r.apply(&store.JobChange{Add: second})
	if out, _ := r.answer(asBoss, " YES! "); !strings.Contains(out, "Several requests") {
		t.Fatalf("two drafts: %s", out)
	}
	if out, _ := r.answer(asBoss, "ні…"); !strings.Contains(out, "Several") {
		t.Fatal(out)
	}
	r.cmd(asBoss, "job discard "+second.ID)
	if out, ok := r.answer(asBoss, "так"); !ok || !strings.Contains(out, "*🟢 Active*.\n*Next scheduled check:*") || r.job(draft.ID).State != store.JobActive {
		t.Fatalf("confirm: %s", out)
	}
	// An edit draft shows its diff; an expired draft cannot be confirmed.
	_, edit, card := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"edit\":\"" + draft.ID + "\",\"prompt\":\"shorter\"}\n```"})
	if edit == nil || !strings.Contains(card, `Instructions updated.`) {
		t.Fatalf("edit card %q", card)
	}
	r.apply(&store.JobChange{Add: edit})
	r.now = r.now.Add(25 * time.Hour)
	if out := r.cmd(asBoss, "job confirm "+edit.ID); !strings.Contains(out, "expired") {
		t.Fatalf("expired: %s", out)
	}
	if _, handled := r.answer(asBoss, "yes."); handled {
		t.Fatal("punctuated reply confirmed expired draft")
	}
	// Invalid or several blocks: an error card, nothing stored.
	if _, d, card := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"name\":1}\n```"}); d != nil || !strings.Contains(card, "not valid") {
		t.Fatal(card)
	}
	if _, d, card := r.s.Draft(asBoss, []string{block, block}); d != nil || !strings.Contains(card, "more than one") {
		t.Fatal(card)
	}
	// An employee's draft follows the employee's rules.
	if _, d, card := r.s.Draft(asWork, []string{"```heimdall-job\n{\"name\":\"x\",\"schedule\":{\"every\":\"5m\"},\"prompt\":\"p\"}\n```"}); d != nil || !strings.Contains(card, "every 1h0m0s") {
		t.Fatal(card)
	}
}

func TestDrainHealthExport(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if idle, err := Drain(ctx, r.st, time.Second, 10*time.Millisecond); !idle || err != nil {
		t.Fatalf("idle drain: %v %v", idle, err)
	}
	row, _, _ := r.st.AddInbound(core.Inbound{Conv: bossDM, MsgID: "m", From: boss, Origin: core.OriginLive, Text: "hi", At: r.now})
	if _, err := r.st.StartTurn(bossDM, core.RoleOwner, boss.Key(), "m", []int64{row.ID}); err != nil {
		t.Fatal(err)
	}
	if idle, err := Drain(ctx, r.st, 50*time.Millisecond, 10*time.Millisecond); idle || err != nil {
		t.Fatalf("busy drain: %v %v", idle, err)
	}
	if hold, _ := r.st.Meta(MetaHold); hold == 0 {
		t.Fatal("drain set no hold")
	}
	id := r.add(asBoss, `{"name":"h","schedule":{"every":"1h"},"prompt":"p"}`)
	_ = r.st.SetMeta(MetaTick, r.now.UnixMilli())
	if h := r.s.Health(20 * time.Minute); !h.OK {
		t.Fatalf("health %+v", h)
	}
	r.now = r.now.Add(5 * time.Hour) // the loop stopped: stale tick, overdue job
	h := r.s.Health(20 * time.Minute)
	if h.OK || len(h.Issues) != 2 || !strings.Contains(h.Issues[1], id+" is overdue") {
		t.Fatalf("health %+v", h)
	}
	raw, err := r.s.Export()
	var jobs []map[string]any
	if err != nil || json.Unmarshal(raw, &jobs) != nil || len(jobs) != 1 || jobs[0]["id"] != id {
		t.Fatalf("export %s", raw)
	}
}

// An idempotent run is re-run once after a restart cut it off after its
// harness was ready. That one re-run is recorded apart from the run's
// cause: a crash before ready in between does not allow a second one.
func TestIdempotentReplayOnce(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"idem","schedule":{"every":"1h"},"idempotent":true,"prompt":"p"}`)
	crash := func(ready bool) store.Run {
		t.Helper()
		r.rows = nil
		r.tick()
		if len(r.rows) != 1 {
			t.Fatalf("dispatched %d, runs %+v", len(r.rows), r.runs(id))
		}
		turn, err := r.st.StartTurn(r.rows[0].Conv, core.RoleOwner, r.rows[0].From.Key(), "", []int64{r.rows[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			_ = r.st.TurnReady(turn)
		}
		if _, err := r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} }); err != nil {
			t.Fatal(err)
		}
		if err := r.s.Recover(); err != nil {
			t.Fatal(err)
		}
		return r.runs(id)[0]
	}
	r.now = r.now.Add(time.Hour)
	if got := crash(true); got.State != store.RunPending {
		t.Fatalf("first crash after ready: %+v", got)
	}
	if got := crash(false); got.State != store.RunPending {
		t.Fatalf("crash before ready: %+v", got)
	}
	if got := crash(true); got.State != store.RunInterrupted {
		t.Fatalf("a second crash after ready re-ran the run again: %+v", got)
	}
}

// A due one-off starts: its slot being past is no reason to refuse it.
// Only creating or changing a job needs a future run (or a future until).
func TestDueOneOffStarts(t *testing.T) {
	r := newRig(t) // 12:00 in Kyiv
	agent := r.add(asBoss, `{"name":"once","schedule":{"at":"2026-11-03T12:30"},"prompt":"p"}`)
	note := r.add(asWork, `{"name":"ping","kind":"notify","schedule":{"at":"2026-11-03T12:30"},"text":"stand up"}`)
	if out := r.cmd(asBoss, `job add {"name":"past","schedule":{"at":"2026-11-03T11:00"},"prompt":"p"}`); !strings.Contains(out, "no future run") {
		t.Fatalf("a past one-off was created: %s", out)
	}
	r.sent()
	r.now = r.now.Add(31 * time.Minute)
	r.tick()
	if j := r.job(agent); len(r.rows) != 1 || j.State != store.JobActive {
		t.Fatalf("one-off agent job: dispatched %d, job %s (%s)", len(r.rows), j.State, j.Reason)
	}
	if j, sent := r.job(note), texts(r.sent()); j.State != store.JobDone || !strings.Contains(sent, "stand up") {
		t.Fatalf("one-off notify job: %s (%s), sent %q", j.State, j.Reason, sent)
	}
	if e := r.finish("done", Outcome{Harness: "pi"}); e.Run.State != store.RunOK || e.Run.JobState != store.JobDone {
		t.Fatalf("one-off end: %+v", e.Run)
	}
}

// A job can be created paused (§7.5 cutover: never both firing): by
// !job add, or by a draft that carries it. It fires only once resumed.
func TestCreatePaused(t *testing.T) {
	r := newRig(t)
	out, change := r.s.Command(asBoss, `job add {"name":"cutover","schedule":{"every":"1h"},"paused":true,"prompt":"p"}`)
	r.apply(change)
	id := change.Add.ID
	if j := r.job(id); !strings.Contains(out, "Paused") || strings.Contains(out, "Next") || j.State != store.JobPaused || strings.Contains(j.Def, "Paused") {
		t.Fatalf("add paused: %q %+v", out, j)
	}
	r.now = r.now.Add(3 * time.Hour)
	r.tick()
	if len(r.rows) != 0 {
		t.Fatal("a job created paused fired")
	}
	if h := r.s.Health(20 * time.Minute); slices.ContainsFunc(h.Issues, func(s string) bool { return strings.Contains(s, id) }) {
		t.Fatalf("created paused counts as a scheduler pause: %v", h.Issues)
	}
	r.cmd(asBoss, "job resume "+id)
	r.now = r.now.Add(time.Hour)
	r.tick()
	if len(r.rows) != 1 {
		t.Fatal("not fired after resume")
	}
	_, d, card := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"name\":\"x\",\"schedule\":{\"every\":\"1h\"},\"paused\":true,\"prompt\":\"p\"}\n```"})
	if d == nil || !strings.Contains(card, "paused") {
		t.Fatalf("paused draft: %q", card)
	}
	r.apply(&store.JobChange{Add: d})
	if out := r.cmd(asBoss, "job confirm "+d.ID); !strings.Contains(out, "Paused") || r.job(d.ID).State != store.JobPaused {
		t.Fatalf("confirm paused draft: %q %+v", out, r.job(d.ID))
	}
	if out := r.cmd(asBoss, "job edit "+id+` {"paused":true}`); !strings.Contains(out, "not changed") {
		t.Fatalf("paused in an edit: %q", out)
	}
}

// In a shared chat no job reply carries a prompt or text (§4); only a
// draft's card previews it there (§5.3).
func TestSharedRepliesHidePrompt(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"private plan","schedule":{"every":"1h"},"prompt":"the secret prompt"}`)
	if out := r.cmd(inGroup, "job show "+id); strings.Contains(out, "secret") || !strings.Contains(out, "private plan") {
		t.Fatalf("shared show: %q", out)
	}
	if out := r.cmd(inGroup, "job edit "+id+` {"prompt":"another secret"}`); strings.Contains(out, "secret") || !strings.Contains(out, "Instructions updated.") {
		t.Fatalf("shared edit: %q", out)
	}
	if out := r.cmd(asBoss, "job show "+id); !strings.Contains(out, "another secret") {
		t.Fatalf("private show: %q", out)
	}
	long := strings.Repeat("a", 300)
	if _, d, card := r.s.Draft(inGroup, []string{"```heimdall-job\n{\"name\":\"x\",\"schedule\":{\"every\":\"1h\"},\"prompt\":\"" + long + "\"}\n```"}); d == nil || strings.Contains(card, long[:200]) || strings.Contains(card, "Prompt:") {
		t.Fatalf("shared card: %q", card)
	}
}

// A drain's hold stops every run not yet started, at its turn's start: a
// run handed to the router before the hold (waiting for a slot) goes back
// to waiting, and drain counts it as busy until then.
func TestHoldAtTurnStart(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"h","schedule":{"every":"1h"},"prompt":"p"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	if len(r.rows) != 1 {
		t.Fatalf("dispatched %d", len(r.rows))
	}
	if idle, err := Drain(context.Background(), r.st, 50*time.Millisecond, 10*time.Millisecond); idle || err != nil {
		t.Fatalf("drain idle while a handed-over run waits for its turn: %v %v", idle, err)
	}
	_ = r.st.SetMeta(MetaHold, r.now.Add(time.Hour).UnixMilli())
	if _, ok := r.s.Begin(r.rows[0]); ok {
		t.Fatal("a held run started")
	}
	if got := r.runs(id)[0]; got.State != store.RunPending || got.Attempt != 0 || got.Inbound != 0 {
		t.Fatalf("held run: %+v", got)
	}
	if idle, err := Drain(context.Background(), r.st, 50*time.Millisecond, 10*time.Millisecond); !idle || err != nil {
		t.Fatalf("drain not idle once the run was held: %v %v", idle, err)
	}
	if err := r.s.Recover(); err != nil { // a restart clears the hold
		t.Fatal(err)
	}
	r.rows = nil
	r.tick()
	if len(r.rows) != 1 {
		t.Fatalf("held run not handed over again: %+v", r.runs(id))
	}
	if _, ok := r.s.Begin(r.rows[0]); !ok {
		t.Fatalf("held run did not start after the hold: %+v", r.runs(id))
	}
}

// Admit, as a turn starts: a temporary Slack error retries the run before
// the ready signal; an expiry that passed after the hand-over is no refusal.
func TestAdmitAtTurnStart(t *testing.T) {
	r := newRig(t)
	r.s.cfg.Interfaces = append(r.s.cfg.Interfaces, core.PlatformSlack)
	slackBoss := core.SlackPrincipal("boss@example.com", "T1", "boss")
	r.s.access.Owners[slackBoss.Key()] = true
	var verr error
	r.s.Reverify = func(_ context.Context, p core.Principal) (Verified, bool, error) {
		return Verified{Principal: p, OAuthActor: "sl_user_U1"}, true, verr
	}
	r.add(Caller{Principal: slackBoss, Role: core.RoleOwner, Conv: core.ConvKey{Interface: core.PlatformSlack, Chat: "D1"}, Private: true}, `{"name":"s","schedule":{"every":"1h"},"until":"61m","prompt":"p"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	turn, ok := r.s.Begin(r.rows[0])
	if !ok {
		t.Fatal("not begun")
	}
	r.now = r.now.Add(2 * time.Minute)
	if v, err := r.s.Admit(context.Background(), turn); err != nil || v.OAuthActor != "sl_user_U1" {
		t.Fatalf("admit after until: %+v %v", v, err)
	}
	verr = errors.New("timeout")
	_, err := r.s.Admit(context.Background(), turn)
	if e := r.s.End(Outcome{Turn: turn, Identity: err}); !errors.Is(err, ErrTemporary) || e.Run.Retry.IsZero() || e.Run.Cause != ErrTemporary.Error() {
		t.Fatalf("temporary error: %v %+v", err, e.Run)
	}
}

// A drain that begins while a notify run waits on Slack re-verification
// holds it: no text is enqueued behind the drain, and the run stays pending
// until the hold clears (re-review finding: notify bypassed the hold).
func TestNotifyHeldDuringVerification(t *testing.T) {
	r := newRig(t)
	r.s.cfg.Interfaces = append(r.s.cfg.Interfaces, core.PlatformSlack)
	slackBoss := core.SlackPrincipal("boss@example.com", "T1", "boss")
	r.s.access.Owners[slackBoss.Key()] = true
	hold := true
	r.s.Reverify = func(_ context.Context, p core.Principal) (Verified, bool, error) {
		if hold { // the drain starts while Slack answers
			_ = r.st.SetMeta(MetaHold, r.now.Add(time.Hour).UnixMilli())
		}
		return Verified{Principal: p, OAuthActor: "sl_user_U1"}, true, nil
	}
	id := r.add(Caller{Principal: slackBoss, Role: core.RoleOwner, Conv: core.ConvKey{Interface: core.PlatformSlack, Chat: "D1"}, Private: true}, `{"name":"n","kind":"notify","schedule":{"every":"1h"},"text":"ping"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	if out := slackSent(r); len(out) != 0 {
		t.Fatalf("notify enqueued behind a drain: %+v", out)
	}
	if got := r.runs(id)[0]; got.State != store.RunPending {
		t.Fatalf("held notify run: %+v", got)
	}
	if idle, err := Drain(context.Background(), r.st, 50*time.Millisecond, 10*time.Millisecond); !idle || err != nil {
		t.Fatalf("drain: %v %v", idle, err)
	}
	hold = false
	if err := r.s.Recover(); err != nil { // a restart clears the hold
		t.Fatal(err)
	}
	r.tick()
	if out := slackSent(r); len(out) != 1 || out[0].Text != "ping" {
		t.Fatalf("notify after the hold: %+v runs %+v", out, r.runs(id))
	}
}

// slackSent drains the Slack outbound queue.
func slackSent(r *rig) []core.Outbound {
	var out []core.Outbound
	for {
		row, ok, err := r.st.NextOutbound(core.PlatformSlack)
		if err != nil || !ok {
			return out
		}
		_ = r.st.Sent(row.ID)
		out = append(out, row.Outbound)
	}
}

func timeoutJob(t *testing.T) (*rig, string) {
	t.Helper()
	r := newRig(t)
	id := r.add(asBoss, `{"name":"repeat-safe","schedule":{"every":"1h"},"prompt":"read report","idempotent":true,"rerun_on_timeout":true,"timeout":"1m"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	return r, id
}

func TestRepeatSafeTimeoutOnce(t *testing.T) {
	r, _ := timeoutJob(t)
	r.now = r.now.Add(time.Minute)
	e := r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout, Ready: true}})
	if e.Run.Retry.IsZero() || !e.Run.Replay {
		t.Fatalf("not retried: %+v", e.Run)
	}
	r.rows = nil
	r.s.start(context.Background(), r.now)
	if len(r.rows) != 1 {
		t.Fatal("retry not dispatched")
	}
	turn, ok := r.s.Begin(r.rows[0])
	if !ok || !turn.Replayed || turn.Timeout != time.Minute {
		t.Fatalf("%+v", turn)
	}
	e = r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout, Ready: true}})
	if !e.Run.Retry.IsZero() || e.Run.State != store.RunFailed {
		t.Fatalf("unbounded: %+v", e.Run)
	}
}

func TestTimeoutRetryCommandAndRestartFences(t *testing.T) {
	for _, action := range []string{"stop", "pause", "rm", "restart"} {
		t.Run(action, func(t *testing.T) {
			r, id := timeoutJob(t)
			r.now = r.now.Add(time.Minute)
			e := r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
			if e.Run.Retry.IsZero() {
				t.Fatal("missing retry")
			}
			if action == "restart" {
				if err := r.s.Recover(); err != nil {
					t.Fatal(err)
				}
			} else {
				r.cmd(asBoss, "!job "+action+" "+id)
			}
			r.rows = nil
			r.s.start(context.Background(), r.now)
			if len(r.rows) != 0 {
				t.Fatal("cancelled retry dispatched")
			}
		})
	}
}

func TestTimeoutRetryBudgetAndSafetyGates(t *testing.T) {
	for _, kind := range []string{"budget", "stop", "approval", "auth", "auth_failure", "lifetime", "authority"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := timeoutJob(t)
			o := Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}}
			switch kind {
			case "budget":
				// Begin precedes the elapsed execution budget; End observes it later.
				row := r.rows[0]
				turn, ok := r.s.Begin(row)
				if !ok {
					t.Fatal("begin")
				}
				r.now = r.now.Add(2 * time.Minute)
				o.Turn = turn
				e := r.s.End(o)
				if !e.Run.Retry.IsZero() {
					t.Fatal("budget ignored")
				}
				return
			case "stop":
				o.Stopped = true
			case "approval":
				o.Result.NeedsHuman = "approval needed"
			case "auth_failure":
				o.Result.Failure = "sign-in or credentials"
			case "auth":
				o.SignIn = []string{"google"}
			case "lifetime":
				o.Result.Cause = harness.CauseLifetime
			case "authority":
				r.s.access.Owners = map[string]bool{}
			}
			e := r.finish("", o)
			if !e.Run.Retry.IsZero() {
				t.Fatalf("unsafe retry %s: %+v", kind, e.Run)
			}
		})
	}
}

func TestTimeoutNoOptInNeverRetriesEvenBeforeReady(t *testing.T) {
	r := newRig(t)
	r.add(asBoss, `{"name":"normal","schedule":{"every":"1h"},"prompt":"p"}`)
	r.now = r.now.Add(time.Hour)
	r.tick()
	e := r.finish("", Outcome{Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	if !e.Run.Retry.IsZero() || e.Detail != "timeout" {
		t.Fatalf("%+v", e)
	}
}

func TestTimeoutRetryStopFenceWinsLateEnd(t *testing.T) {
	r, id := timeoutJob(t)
	row := r.rows[0]
	turn, ok := r.s.Begin(row)
	if !ok {
		t.Fatal("begin")
	}
	// Command commits while the timeout result is on its way to EndTurn.
	r.cmd(asBoss, "!job stop "+id)
	e := r.s.End(Outcome{Turn: turn, Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	if err := r.st.EndRun(*e.Run); err != nil {
		t.Fatal(err)
	}
	got, _ := r.st.Run(turn.Run)
	if got.State != store.RunCancelled {
		t.Fatalf("resurrected: %+v", got)
	}
}

func TestTimeoutRetryCannotRecoverInterruptedTurn(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			r, id := timeoutJob(t)
			row := r.rows[0]
			turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				if err := r.st.TurnReady(turn); err != nil {
					t.Fatal(err)
				}
			}
			_, err = r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} })
			if err != nil {
				t.Fatal(err)
			}
			if err := r.s.Recover(); err != nil {
				t.Fatal(err)
			}
			want := store.RunPending
			if ready {
				want = store.RunInterrupted
			}
			if got := r.runs(id)[0]; got.State != want {
				t.Fatalf("unexpected recovery: %+v", got)
			}
		})
	}
}

func TestTimeoutRetryConfigRequiresRepeatSafeOwner(t *testing.T) {
	for _, change := range []string{"employee", "idempotent", "timeout", "session", "target"} {
		t.Run(change, func(t *testing.T) {
			r := newRig(t)
			d := `{"name":"x","schedule":{"every":"1h"},"prompt":"p","idempotent":true,"rerun_on_timeout":true,"timeout":"1m"}`
			caller := asBoss
			switch change {
			case "employee":
				caller = asWork
			case "idempotent":
				d = strings.Replace(d, `"idempotent":true`, `"idempotent":false`, 1)
			case "timeout":
				d = strings.Replace(d, `,"timeout":"1m"`, ``, 1)
			case "session":
				d = strings.Replace(d, `"prompt":"p"`, `"prompt":"p","session":"job"`, 1)
			case "target":
				d = strings.Replace(d, `"prompt":"p"`, `"prompt":"p","to":"agentnet:peer"`, 1)
			}
			if out := r.cmd(caller, "job add "+d); !strings.Contains(out, "not created") {
				t.Fatal(out)
			}
		})
	}
}

func TestTimeoutRetryCardShowsOwnerAssertion(t *testing.T) {
	r := newRig(t)
	j, err := r.s.proposal(asBoss, []byte(`{"name":"x","schedule":{"every":"1h"},"prompt":"p","idempotent":true,"rerun_on_timeout":true,"timeout":"1m"}`))
	if err != nil {
		t.Fatal(err)
	}
	card := r.s.card(asBoss, j)
	for _, want := range []string{"Repeat-safe rerun:", "safe to repeat", "timeout (1m)", "One full rerun"} {
		if !strings.Contains(card, want) {
			t.Fatal(card)
		}
	}
	if !strings.Contains(card, "\n\n*⚠️ Repeat-safe rerun:*") || !strings.Contains(card, "\n\n👉 Reply *Yes* to apply or *No* to cancel.") {
		t.Fatal("repeat-safe approval lost paragraph hierarchy", card)
	}
}

func TestQueuedFirstTimeoutJobGetsFullLimit(t *testing.T) {
	r, _ := timeoutJob(t)
	r.now = r.now.Add(3 * time.Minute)
	turn, ok := r.s.Begin(r.rows[0])
	if !ok || turn.Timeout != time.Minute {
		t.Fatalf("queued first execution refused: %+v", turn)
	}
}

func TestDisablingTimeoutRetryCancelsPendingReplay(t *testing.T) {
	r, id := timeoutJob(t)
	r.now = r.now.Add(time.Minute)
	e := r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	if e.Run.Retry.IsZero() {
		t.Fatal("no retry")
	}
	text := r.cmd(asBoss, `job edit `+id+` {"rerun_on_timeout":false}`)
	if !strings.Contains(text, "Scheduled task updated") {
		t.Fatal(text)
	}
	r.rows = nil
	r.s.start(context.Background(), r.now)
	if len(r.rows) != 0 {
		t.Fatal("disabled retry dispatched")
	}
}

func TestStopFenceSuppressesLateSuccessfulDelivery(t *testing.T) {
	r, id := timeoutJob(t)
	row := r.rows[0]
	turn, err := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	r.cmd(asBoss, "job stop "+id)
	e := store.TurnEnd{State: store.TurnDone, ItemState: store.InDone, Reply: &core.Outbound{Conv: bossDM, Text: "MUST NOT SEND"}, Run: &store.RunEnd{ID: row.Run.Run, Job: id, State: store.RunOK}}
	if _, err := r.st.EndTurn(turn, row.Conv, row.From.Key(), e); err != nil {
		t.Fatal(err)
	}
	got, _ := r.st.Run(row.Run.Run)
	if got.State != store.RunCancelled {
		t.Fatalf("late success revived cancelled run: %+v", got)
	}
	out, ok, err := r.st.NextOutbound(bossDM.Interface)
	if err != nil {
		t.Fatal(err)
	}
	if ok && out.Text == "MUST NOT SEND" {
		t.Fatal("cancelled result sent")
	}
}

func TestPauseFencesDispatchedTimeoutRetry(t *testing.T) {
	r, id := timeoutJob(t)
	r.now = r.now.Add(time.Minute)
	r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	r.rows = nil
	r.s.start(context.Background(), r.now)
	if len(r.rows) != 1 {
		t.Fatal("retry not dispatched")
	}
	row := r.rows[0]
	r.cmd(asBoss, "job pause "+id)
	if _, ok := r.s.Begin(row); ok {
		t.Fatal("paused dispatched retry started")
	}
}

func TestTimeoutRetryRunningRestartHasNotice(t *testing.T) {
	r, _ := timeoutJob(t)
	r.now = r.now.Add(time.Minute)
	r.finish("", Outcome{Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	r.sent() // the retry notice preceded the restart
	r.rows = nil
	r.s.start(context.Background(), r.now)
	row := r.rows[0]
	_, err := r.st.StartTurn(row.Conv, core.RoleOwner, row.From.Key(), "", []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.st.Recover(func(store.Interrupted) core.Outbound { return core.Outbound{} }, func(store.InboundRow) core.Outbound { return core.Outbound{} })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.s.Recover(); err != nil {
		t.Fatal(err)
	}
	out, ok, err := r.st.NextOutbound(bossDM.Interface)
	if err != nil || !ok || !strings.Contains(out.Text, "interrupted by a restart") {
		t.Fatalf("silent restart: %+v %v", out, err)
	}
}

func TestPauseResumeDoesNotReviveCurrentTimeoutRetry(t *testing.T) {
	r, id := timeoutJob(t)
	row := r.rows[0]
	turn, ok := r.s.Begin(row)
	if !ok {
		t.Fatal("begin")
	}
	r.cmd(asBoss, "job pause "+id)
	r.cmd(asBoss, "job resume "+id)
	e := r.s.End(Outcome{Turn: turn, Harness: "pi", Result: harness.Result{Cancelled: true, Cause: harness.CauseTimeout}})
	if !e.Run.Retry.IsZero() {
		t.Fatal("pause/resume restored retry permission")
	}
	if err := r.st.EndRun(*e.Run); err != nil {
		t.Fatal(err)
	}
	got, _ := r.st.Run(turn.Run)
	if got.State != store.RunFailed {
		t.Fatalf("run left active: %+v", got)
	}
}

func TestRemovalDrafts(t *testing.T) {
	for _, tc := range []struct {
		name              string
		creator, proposer Caller
	}{
		{"owner", asBoss, asBoss},
		{"employee own job", asWork, asWork},
		{"owner removes employee job", asWork, asBoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			id := r.add(tc.creator, `{"name":"standup","schedule":{"every":"1h"},"prompt":"p"}`)
			stops := 0
			r.s.stop = func(job string) bool {
				if job != id {
					t.Fatalf("stopped %s", job)
				}
				stops++
				return true
			}
			propose := func() *store.Job {
				clean, d, card := r.s.Draft(tc.proposer, []string{"Sure.\n```heimdall-job\n{\"remove\":\"" + id + "\"}\n```"})
				if d == nil || clean[0] != "Sure." {
					t.Fatalf("draft: %v %s", d, card)
				}
				for _, want := range []string{"Remove scheduled task?", "When:", "Every 1 hour", "Europe/Kyiv", "Future checks and pending retries", "Yes", "No", "Details"} {
					if !strings.Contains(card, want) {
						t.Fatalf("missing %q: %s", want, card)
					}
				}
				r.apply(&store.JobChange{Add: d})
				if r.job(id).State != store.JobActive || stops != 0 {
					t.Fatal("proposal changed target")
				}
				return d
			}
			d := propose()
			other := tc.proposer
			other.Principal = core.Principal{Platform: core.PlatformTelegram, ID: "999"}
			if out := r.cmd(other, "job confirm "+d.ID); !strings.Contains(out, "No draft") {
				t.Fatal(out)
			}
			elsewhere := tc.proposer
			elsewhere.Conv = team
			if out := r.cmd(elsewhere, "job confirm "+d.ID); !strings.Contains(out, "No draft") {
				t.Fatal(out)
			}
			if out, ok := r.answer(tc.proposer, "no"); !ok || !strings.Contains(out, "cancelled") {
				t.Fatal(out)
			}
			if r.job(id).State != store.JobActive || stops != 0 {
				t.Fatal("discard changed target")
			}
			d = propose()
			if out, ok := r.answer(tc.proposer, "yes"); !ok || !strings.Contains(out, "Removed") {
				t.Fatal(out)
			}
			if r.job(id).State != store.JobDeleted || stops != 1 {
				t.Fatal("confirm did not remove/stop target")
			}
			if _, ok, err := r.st.Job(d.ID); ok || err != nil {
				t.Fatalf("draft retained: %v %v", ok, err)
			}
			if out := r.cmd(tc.proposer, "job confirm "+d.ID); !strings.Contains(out, "No draft") || stops != 1 {
				t.Fatal(out)
			}
		})
	}
}

func TestRemovalDraftRefusals(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"owner job","schedule":{"every":"1h"},"prompt":"p"}`)
	for _, tc := range []struct {
		caller Caller
		raw    string
	}{
		{asWork, `{"remove":"` + id + `"}`},
		{asBoss, `{"remove":"missing"}`},
		{asBoss, `{"remove":true}`},
		{asBoss, `{"remove":""}`},
		{asBoss, `{"remove":"` + id + `","edit":"` + id + `"}`},
		{asBoss, `{"remove":"` + id + `","paused":true}`},
	} {
		_, d, card := r.s.Draft(tc.caller, []string{"```heimdall-job\n" + tc.raw + "\n```"})
		if d != nil || !strings.Contains(card, "not valid") || r.job(id).State != store.JobActive {
			t.Fatalf("%s: %v %s", tc.raw, d, card)
		}
	}
	_, d, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"remove\":\"" + id + "\"}\n```"})
	r.apply(&store.JobChange{Add: d})
	// Losing owner authority after proposal must be caught at confirmation.
	demoted := asBoss
	demoted.Role = core.RoleEmployee
	// An owner proposes removing another caller's job, then loses owner role.
	otherID := r.add(asWork, `{"name":"employee job","schedule":{"every":"1h"},"prompt":"p"}`)
	_, other, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"remove\":\"" + otherID + "\"}\n```"})
	r.apply(&store.JobChange{Add: other})
	if out := r.cmd(demoted, "job confirm "+other.ID); !strings.Contains(out, "no job") || r.job(otherID).State != store.JobActive {
		t.Fatal(out)
	}
	r.cmd(asBoss, "job rm "+id)
	if out := r.cmd(asBoss, "job confirm "+d.ID); !strings.Contains(out, "no job") {
		t.Fatal(out)
	}
	// Expired removal must leave its target alone.
	r.now = r.now.Add(25 * time.Hour)
	if out := r.cmd(asBoss, "job confirm "+other.ID); !strings.Contains(out, "expired") || r.job(otherID).State != store.JobActive {
		t.Fatal(out)
	}
}

func TestDraftReplyPunctuation(t *testing.T) {
	for _, reply := range []string{"yes.", "Yes!", "ok.", "да.", "так…", "confirm?!", " YES , …  ", "no.", "cancel!", "нет…", "ні,"} {
		t.Run(reply, func(t *testing.T) {
			r := newRig(t)
			_, draft, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n" + `{"name":"x","schedule":{"every":"5m"},"prompt":"p"}` + "\n```"})
			if draft == nil {
				t.Fatal("no draft")
			}
			r.apply(&store.JobChange{Add: draft})
			out, handled := r.answer(asBoss, reply)
			want := store.JobActive
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(reply)), "no") || strings.HasPrefix(reply, "cancel") || strings.HasPrefix(reply, "нет") || strings.HasPrefix(reply, "ні") {
				want = "" // Discard drops the draft row.
			}
			if !handled || r.job(draft.ID).State != want {
				t.Fatalf("reply %q: %q, state %s, want %s", reply, out, r.job(draft.ID).State, want)
			}
		})
	}
	for _, reply := range []string{"ok?", "yes?", "да?", "no?", "  Yes?  ", "no!?", "yes, but maybe in a separate folder", "yes. please", "no, keep it", "not ok.", "!yes", "yesterday", "..."} {
		t.Run(reply, func(t *testing.T) {
			r := newRig(t)
			_, draft, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n" + `{"name":"x","schedule":{"every":"5m"},"prompt":"p"}` + "\n```"})
			r.apply(&store.JobChange{Add: draft})
			if _, handled := r.answer(asBoss, reply); handled || r.job(draft.ID).State != store.JobDraft {
				t.Fatalf("non-answer %q changed draft", reply)
			}
		})
	}
}

func TestShortDraftCards(t *testing.T) {
	for _, c := range []Caller{asBoss, inGroup} {
		for _, kind := range []string{KindAgent, KindNotify} {
			t.Run(fmt.Sprintf("private=%t/%s", c.Private, kind), func(t *testing.T) {
				r := newRig(t)
				body := strings.Repeat("界&<>", 600)
				d := Def{Name: "short card", Kind: kind, Schedule: schedule.Spec{Every: "5m"}}
				if kind == KindNotify {
					d.Text = body
				} else {
					d.Prompt = body
				}
				_, draft, card := r.s.Draft(c, []string{"```heimdall-job\n" + mustJSON(d) + "\n```"})
				if draft == nil {
					t.Fatal(card)
				}
				preview := string([]rune(body)[:200]) + "…"
				for _, want := range []string{"Details", "When:", "Every 5 minutes", "Europe/Kyiv"} {
					if !strings.Contains(card, want) {
						t.Fatalf("missing %q: %s", want, card)
					}
				}
				// Slack escapes these characters before splitting at its 3000-rune budget.
				escaped := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(card)
				if len([]rune(escaped)) > 3000 || strings.Contains(card, body) {
					t.Fatalf("card exceeds one Slack message: %d runes", len([]rune(escaped)))
				}
				if strings.Contains(card, " UTC)") || strings.Contains(card, preview) || strings.Contains(card, draft.ID) {
					t.Fatalf("card must show only one next run: %s", card)
				}
				r.apply(&store.JobChange{Add: draft})
				private := c
				private.Private = true
				if show := r.cmd(private, "job show "+draft.ID); !strings.Contains(show, body) || strings.Count(show, " UTC)") != 3 {
					t.Fatal("private show lost full content or next three runs")
				}
				if show := r.cmd(inGroup, "job show "+draft.ID); strings.Contains(show, preview) {
					t.Fatal("shared show exposed content")
				}
			})
		}
	}
}

func TestScheduleWords(t *testing.T) {
	for _, tc := range []struct {
		spec schedule.Spec
		want string
	}{
		{schedule.Spec{Every: "5m"}, "Every 5 minutes"},
		{schedule.Spec{Every: "2d", At: "09:00", From: "2026-11-05"}, "Every 2 days at 09:00, starting 5 Nov 2026"},
		{schedule.Spec{Cron: []string{"0 9 * * MON", "0 17 * * FRI"}}, "Custom schedule"},
		{schedule.Spec{At: "2026-11-05T09:00"}, "Once, 5 Nov 2026 at 09:00"},
	} {
		if got := scheduleWords(tc.spec); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}

func TestPrepareOwnerAndConfirmationExceptions(t *testing.T) {
	for _, tc := range []struct {
		name, raw     string
		caller        Caller
		direct, apply bool
	}{
		{"new", `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}`, asBoss, true, true},
		{"paused", `{"name":"x","schedule":{"every":"1h"},"prompt":"p","paused":true}`, asBoss, true, true},
		{"employee", `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}`, asWork, true, false},
		{"mcp control", `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}`, asBoss, false, false},
		{"timeout", `{"name":"x","schedule":{"every":"1h"},"prompt":"p","idempotent":true,"rerun_on_timeout":true,"timeout":"1m"}`, asBoss, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			_, draft, change, notice := r.s.Prepare(tc.caller, []string{"```heimdall-job\n" + tc.raw + "\n```"}, tc.direct)
			if draft == nil || (change != nil) != tc.apply {
				t.Fatalf("draft=%+v change=%+v notice=%s", draft, change, notice)
			}
			if tc.apply {
				if !Formatted(notice) || strings.Contains(notice, "Reply *Yes*") {
					t.Fatal(notice)
				}
				if _, err := r.st.EndTurn(0, tc.caller.Conv, tc.caller.Principal.Key(), store.TurnEnd{Draft: draft, Job: change, Reply: &core.Outbound{Conv: tc.caller.Conv, Text: notice}}); err != nil {
					t.Fatal(err)
				}
				j := r.job(draft.ID)
				want := store.JobActive
				if tc.name == "paused" {
					want = store.JobPaused
				}
				if j.State != want {
					t.Fatalf("job: %+v", j)
				}
				if tc.name == "new" {
					r.now = r.now.Add(time.Hour)
					r.tick()
					if len(r.rows) != 1 {
						t.Fatalf("activated job did not fire: %+v", r.rows)
					}
				}

			} else if !strings.Contains(notice, "Reply *Yes*") {
				t.Fatal(notice)
			}
		})
	}
}

func TestPrepareRefusesInvalidMultipleAndCap(t *testing.T) {
	for _, name := range []string{"invalid", "multiple", "cap"} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			block := "```heimdall-job\n" + `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}` + "\n```"
			switch name {
			case "invalid":
				block = "```heimdall-job\n{}\n```"
			case "multiple":
				block += "\n" + block
			case "cap":
				for range 10 {
					r.add(asWork, `{"name":"x","schedule":{"every":"1h"},"prompt":"p"}`)
				}
			}
			c := asBoss
			if name == "cap" {
				c = asWork
			}
			_, draft, change, notice := r.s.Prepare(c, []string{block}, true)
			if draft != nil || change != nil || notice == "" || strings.Contains(notice, "\n") {
				t.Fatalf("%+v %+v %q", draft, change, notice)
			}
			rows, _ := r.st.Jobs(store.JobFilter{States: []string{store.JobDraft}})
			if len(rows) != 0 {
				t.Fatal(rows)
			}
		})
	}
}

func TestQuietControlIsWholeAnswerOnly(t *testing.T) {
	for _, text := range []string{"NO_UPDATE", "Marker exists → already reported.\n\nNO_UPDATE", "The user asked what NO_UPDATE means.", "NO_UPDATE\nUseful result"} {
		t.Run(text, func(t *testing.T) {
			r := newRig(t)
			id := r.add(asBoss, `{"name":"q","schedule":{"every":"1h"},"quiet":true,"prompt":"p"}`)
			r.run(id)
			e := r.finish(text, Outcome{Harness: "claude"})
			if text == "NO_UPDATE" {
				if e.Run.State != store.RunQuiet || e.Reply != nil {
					t.Fatal("quiet control leaked")
				}
			} else if e.Reply == nil || e.Reply.Text != text {
				t.Fatal("legitimate mixed content suppressed")
			}
		})
	}
}
