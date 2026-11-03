package scheduler

import (
	"context"
	"encoding/json"
	"errors"
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
	t    *testing.T
	s    *Scheduler
	st   *store.Store
	now  time.Time
	rows []store.InboundRow // dispatched run messages
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r := &rig{t: t, st: st, now: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
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
}

func (r *rig) add(c Caller, def string) string {
	r.t.Helper()
	out := r.cmd(c, "job add "+def)
	id := idRE.FindString(out)
	if !strings.Contains(out, "created") || id == "" {
		r.t.Fatalf("not created: %s", out)
	}
	return id
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
	// Stopped by command; native failures get a fixed category only.
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
	if out := r.cmd(inGroup, "jobs"); strings.Contains(out, "next") || !strings.Contains(out, "boss job") {
		t.Fatalf("shared list shows more than names: %s", out)
	}
	if out := r.cmd(asBoss, "job show "+boss); !strings.Contains(out, "2026-11-09 09:00 EET (07:00 UTC)") || !strings.Contains(out, "secret plan") {
		t.Fatalf("show: %s", out)
	}
	// An edit applies from the next run, with its diff.
	out := r.cmd(asWork, "job edit "+mine+` {"prompt":"q"}`)
	if !strings.Contains(out, `prompt: "p" → "q"`) || r.job(mine).Version != 2 {
		t.Fatalf("edit: %s %+v", out, r.job(mine))
	}
	if out := r.cmd(asWork, "job pause "+mine); !strings.Contains(out, "paused") || r.job(mine).State != store.JobPaused {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job resume "+mine); !strings.Contains(out, "resumed") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job renew "+mine+" 30d"); !strings.Contains(out, "renewed until 2026-12-03") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job frobnicate"); !strings.Contains(out, "Job commands") {
		t.Fatal(out)
	}
	if out := r.cmd(asWork, "job rm "+mine); !strings.Contains(out, "deleted") || r.job(mine).State != store.JobDeleted {
		t.Fatal(out)
	}
	// Expiry ends a job with a notice.
	exp := r.add(asBoss, `{"name":"short","schedule":{"every":"1h"},"until":"90m","prompt":"p"}`)
	r.sent()
	r.now = r.now.Add(2 * time.Hour)
	r.tick()
	if r.job(exp).State != store.JobDone || !strings.Contains(texts(r.sent()), "expired") {
		t.Fatalf("expiry: %+v", r.job(exp))
	}
}

func TestDrafts(t *testing.T) {
	r := newRig(t)
	block := "ok\n```heimdall-job\n{\"name\":\"standup\",\"schedule\":{\"cron\":[\"0 9 * * MON-FRI\"]},\"prompt\":\"summarize\"}\n```"
	clean, draft, card := r.s.Draft(asBoss, []string{block})
	if clean[0] != "ok" || draft == nil || !strings.Contains(card, "Confirm: !job confirm "+draft.ID) || !strings.HasSuffix(card, "— heimdall") {
		t.Fatalf("draft %q %q", clean, card)
	}
	r.apply(&store.JobChange{Add: draft})
	// Only the same caller, in the same conversation.
	other := Caller{Principal: worker, Role: core.RoleEmployee, Conv: bossDM}
	if _, handled := r.answer(other, "yes"); handled {
		t.Fatal("someone else's yes was taken")
	}
	if out := r.cmd(Caller{Principal: boss, Role: core.RoleOwner, Conv: team}, "job confirm "+draft.ID); !strings.Contains(out, "No draft") {
		t.Fatalf("confirmed elsewhere: %s", out)
	}
	// Two drafts: a bare yes asks which.
	_, second, _ := r.s.Draft(asBoss, []string{block})
	r.apply(&store.JobChange{Add: second})
	if out, _ := r.answer(asBoss, " YES "); !strings.Contains(out, "Several job drafts") {
		t.Fatalf("two drafts: %s", out)
	}
	if out, _ := r.answer(asBoss, "ні"); !strings.Contains(out, "Several") {
		t.Fatal(out)
	}
	r.cmd(asBoss, "job discard "+second.ID)
	if out, ok := r.answer(asBoss, "так"); !ok || !strings.Contains(out, "is active") || r.job(draft.ID).State != store.JobActive {
		t.Fatalf("confirm: %s", out)
	}
	// An edit draft shows its diff; an expired draft cannot be confirmed.
	_, edit, card := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"edit\":\"" + draft.ID + "\",\"prompt\":\"shorter\"}\n```"})
	if edit == nil || !strings.Contains(card, `prompt: "summarize" → "shorter"`) {
		t.Fatalf("edit card %q", card)
	}
	r.apply(&store.JobChange{Add: edit})
	r.now = r.now.Add(25 * time.Hour)
	if out := r.cmd(asBoss, "job confirm "+edit.ID); !strings.Contains(out, "expired") {
		t.Fatalf("expired: %s", out)
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
