package scheduler

import (
	"context"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/store"
)

// watchRig is a rig with a checkers directory owned by the test's own user
// (trusted like root) and checkers running as that user.
func watchRig(t *testing.T) (*rig, string) {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	dir := filepath.Join(t.TempDir(), "checkers")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newRig(t)
	r.s.cfg.Checkers = harness.Checkers{Dir: dir, RootUID: uint32(os.Getuid()),
		Accounts: harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Dir: t.TempDir()}}}}
	return r, dir
}

func script(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// settle waits for running checks: their goroutines read the rig's clock.
func (r *rig) settle() { r.s.running.Wait() }

// check runs a job's check now (!job run) and waits for it.
func (r *rig) check(id string) {
	r.run(id)
	r.settle()
}

func watchDef(checker, extra string) string {
	return `{"name":"deals","kind":"watch","check":{"checker":"` + checker + `"},"schedule":{"every":"15m"},"to":"tg:-100"` + extra + `}`
}

// TestWatchDefinition: owner watch jobs name an installed checker and
// nothing an agent would use; everything else is refused.
func TestWatchDefinition(t *testing.T) {
	r, dir := watchRig(t)
	script(t, dir, "competitor-deals", "true")
	id := r.add(asBoss, watchDef("competitor-deals", `,"summary":"Competitor deals","timeout":"30s"`))
	if j := r.job(id); j.Kind != KindWatch || j.State != store.JobActive {
		t.Fatalf("watch job: %+v", j)
	}
	if out := r.cmd(asBoss, "job show "+id); !strings.Contains(out, "Checker: competitor-deals") || strings.Contains(out, "harness") {
		t.Fatalf("show: %s", out)
	}
	for _, def := range []string{
		watchDef("competitor-deals", `,"prompt":"p"`),
		watchDef("competitor-deals", `,"text":"t"`),
		watchDef("competitor-deals", `,"harness":"pi"`),
		watchDef("competitor-deals", `,"quiet":true`),
		watchDef("competitor-deals", `,"checkpoint":true`),
		watchDef("competitor-deals", `,"idempotent":true`),
		watchDef("competitor-deals", `,"session":"job"`),
		watchDef("competitor-deals", `,"expect":{"prefix":"x"}`),
		watchDef("competitor-deals", `,"timeout":"5s"`),
		watchDef("competitor-deals", `,"timeout":"11m"`),
		watchDef("missing", ``),
		watchDef("Bad/Name", ``),
		strings.Replace(watchDef("competitor-deals", ``), `"checker":"competitor-deals"`, `"checker":"competitor-deals","params":{"x":1}`, 1),
		strings.Replace(watchDef("competitor-deals", ``), `"check":{"checker":"competitor-deals"},`, ``, 1),
		strings.Replace(watchDef("competitor-deals", ``), `"tg:-100"`, `"agentnet:peer"`, 1),
		`{"name":"x","schedule":{"every":"1h"},"prompt":"p","check":{"checker":"competitor-deals"}}`,
	} {
		if out := r.cmd(asBoss, "job add "+def); !strings.Contains(out, "not created") && !strings.Contains(out, "Usage") {
			t.Errorf("owner %s: %s", def, out)
		}
	}
	if out := r.cmd(asWork, "job add "+strings.Replace(watchDef("competitor-deals", ``), `"tg:-100"`, `"here"`, 1)); !strings.Contains(out, "owner jobs only") {
		t.Errorf("employee watch: %s", out)
	}
	// Without scheduler.checkers_dir, watch jobs are refused with the reason.
	off := newRig(t)
	if out := off.cmd(asBoss, "job add "+watchDef("competitor-deals", ``)); !strings.Contains(out, "checkers_dir is not set") {
		t.Fatalf("no checkers_dir: %s", out)
	}
	// A direct owner request in a private chat applies at once; in a group
	// the confirmation card applies.
	block := []string{"```heimdall-job\n" + watchDef("competitor-deals", ``) + "\n```"}
	if _, draft, change, notice := r.s.Prepare(asBoss, block, true); draft == nil || change == nil {
		t.Fatalf("direct owner watch not applied: %s", notice)
	}
	if _, draft, change, notice := r.s.Prepare(inGroup, block, false); draft == nil || change != nil || !strings.Contains(notice, "Yes") {
		t.Fatalf("group watch applied without a card: %v %s", change, notice)
	}
	// Swapping the script is shown wherever an edit is confirmed or reported.
	script(t, dir, "other-script", "true")
	swap := []string{"```heimdall-job\n{\"edit\":\"" + id + "\",\"check\":{\"checker\":\"other-script\"}}\n```"}
	if _, _, change, notice := r.s.Prepare(inGroup, swap, false); change != nil || !strings.Contains(notice, "competitor-deals") || !strings.Contains(notice, "other-script") {
		t.Fatalf("checker swap hidden in the group card: %s", notice)
	}
	if out := r.cmd(asBoss, `job edit `+id+` {"check":{"checker":"other-script"}}`); !strings.Contains(out, "other-script") {
		t.Fatalf("checker swap hidden in the edit reply: %s", out)
	}
}

// TestWatchResults: exit 0 without output is quiet; with output the text
// goes to the target as plain text; a failure's stderr never reaches a
// shared chat, and only the pause_after-th failure speaks, once.
func TestWatchResults(t *testing.T) {
	r, dir := watchRig(t)
	script(t, dir, "quiet", "exit 0")
	script(t, dir, "deals", "echo 'Deal: 20% off **now**'")
	script(t, dir, "broken", "echo 'token=SECRETSECRET broke at line 3' >&2; exit 4")
	q := r.add(asBoss, watchDef("quiet", ``))
	r.check(q)
	if got := r.runs(q)[0]; got.State != store.RunQuiet || len(r.sent()) != 0 {
		t.Fatalf("quiet: %+v", got)
	}
	d := r.add(asBoss, watchDef("deals", ``))
	r.check(d)
	sent := r.sent()
	if got := r.runs(d)[0]; got.State != store.RunOK || len(sent) != 1 || sent[0].Conv != team || sent[0].Text != "Deal: 20% off **now**" || sent[0].Formatted {
		t.Fatalf("text: %+v %+v", got, sent)
	}
	b := r.add(asBoss, watchDef("broken", ``))
	for i := 1; i <= 2; i++ {
		r.check(b)
		if got := r.runs(b)[0]; got.State != store.RunFailed || !strings.HasPrefix(got.Cause, "checker exited with status 4") {
			t.Fatalf("failure %d: %+v", i, got)
		}
		if out := r.sent(); len(out) != 0 {
			t.Fatalf("failure %d spoke: %s", i, texts(out))
		}
	}
	r.check(b)
	if j := r.job(b); j.State != store.JobPaused {
		t.Fatalf("not paused after 3 failures: %+v", j)
	}
	out := r.sent()
	if len(out) != 2 {
		t.Fatalf("want one notice in the target and one in the notice chat: %s", texts(out))
	}
	for _, o := range out {
		if !strings.Contains(o.Text, "was paused") || !strings.Contains(o.Text, "checker exited with status 4") {
			t.Fatalf("notice: %s", o.Text)
		}
		switch o.Conv {
		case team:
			if strings.Contains(o.Text, "broke at line") || strings.Contains(o.Text, "SECRET") {
				t.Fatalf("stderr in a shared chat: %s", o.Text)
			}
		case bossDM:
			if !strings.Contains(o.Text, "broke at line 3") || strings.Contains(o.Text, "SECRETSECRET") {
				t.Fatalf("private notice lacks the scrubbed detail: %s", o.Text)
			}
		default:
			t.Fatalf("notice to %v", o.Conv)
		}
	}
	if h := r.cmd(inGroup, "job history "+b); strings.Contains(h, "broke at line") {
		t.Fatalf("stderr in shared history: %s", h)
	}
}

// TestWatchLongOutputCut: a chatty checker's alert is delivered, cut at a
// UTF-8 boundary with a last line saying so; the run counts as delivered.
func TestWatchLongOutputCut(t *testing.T) {
	r, dir := watchRig(t)
	script(t, dir, "chatty", "head -c 3499 /dev/zero | tr '\\0' a\ni=0\nwhile [ $i -lt 2000 ]; do printf 'é'; i=$((i+1)); done")
	id := r.add(asBoss, watchDef("chatty", ``))
	r.check(id)
	sent := r.sent()
	if got := r.runs(id)[0]; got.State != store.RunOK || len(sent) != 1 {
		t.Fatalf("long output: %+v %d", got, len(sent))
	}
	text := sent[0].Text
	body, ok := strings.CutSuffix(text, "\n… (cut: output too long)")
	if !ok || len(body) != 3499 || !utf8.ValidString(text) {
		t.Fatalf("cut: %d bytes, suffix %v", len(body), ok)
	}
}

// TestWatchTimeoutAndNewChecker: the job's timeout reaches the runner
// (60 s by default), and a checker added to the directory after start runs
// without a restart.
func TestWatchTimeoutAndNewChecker(t *testing.T) {
	r, dir := watchRig(t)
	var mu sync.Mutex
	var got []time.Duration
	r.s.runChecker = func(ctx context.Context, path string, l harness.Launch, timeout time.Duration) harness.CheckResult {
		mu.Lock()
		got = append(got, timeout)
		mu.Unlock()
		return harness.RunChecker(ctx, path, l, timeout)
	}
	script(t, dir, "first", "true")
	a := r.add(asBoss, watchDef("first", ``))
	b := r.add(asBoss, watchDef("first", `,"timeout":"45s"`))
	r.check(a)
	r.check(b)
	if len(got) != 2 || got[0] != 60*time.Second || got[1] != 45*time.Second {
		t.Fatalf("timeouts: %v", got)
	}
	// Installed now, after the scheduler started.
	script(t, dir, "added-later", "echo hello from a new checker")
	c := r.add(asBoss, watchDef("added-later", ``))
	r.check(c)
	if sent := r.sent(); len(sent) != 1 || sent[0].Text != "hello from a new checker" {
		t.Fatalf("new checker: %s", texts(sent))
	}
}

// TestWatchRefusesLooseFiles: a world-writable checker or directory is
// refused at creation, and fails a run when it becomes so later.
func TestWatchRefusesLooseFiles(t *testing.T) {
	r, dir := watchRig(t)
	script(t, dir, "loose", "echo should not run")
	id := r.add(asBoss, watchDef("loose", ``))
	if err := os.Chmod(filepath.Join(dir, "loose"), 0o757); err != nil {
		t.Fatal(err)
	}
	if out := r.cmd(asBoss, "job add "+watchDef("loose", ``)); !strings.Contains(out, "world-writable") {
		t.Fatalf("world-writable checker accepted: %s", out)
	}
	r.check(id)
	if got := r.runs(id)[0]; got.State != store.RunFailed || got.Cause != "checker unusable: checker is group- or world-writable" {
		t.Fatalf("loose checker ran: %+v", got)
	}
	if err := os.Chmod(filepath.Join(dir, "loose"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if out := r.cmd(asBoss, "job add "+watchDef("loose", ``)); !strings.Contains(out, "not group- or world-writable") {
		t.Fatalf("world-writable directory accepted: %s", out)
	}
	r.check(id)
	if got := r.runs(id)[0]; got.State != store.RunFailed || !strings.Contains(got.Cause, "checkers directory must be owned by root") {
		t.Fatalf("loose directory ran: %+v", got)
	}
	if out := r.sent(); strings.Contains(texts(out), "should not run") {
		t.Fatal("a refused checker ran")
	}
}

// gate is a checker that waits for a file before it prints, and records
// that it started.
func gate(t *testing.T, dir, name string) (started, release string) {
	tmp := t.TempDir()
	started, release = filepath.Join(tmp, "started"), filepath.Join(tmp, "go")
	script(t, dir, name, "echo x >> "+started+"\nwhile [ ! -f "+release+" ]; do sleep 0.02; done\necho alert")
	return started, release
}

func waitFile(t *testing.T, p string) {
	t.Helper()
	for range 250 {
		if _, err := os.Stat(p); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", p)
}

func touch(t *testing.T, p string) {
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestWatchConcurrency: a slow check delays no other job, checks have
// their own cap of 2, and agent runs filling max_concurrent and per_creator
// never block a check.
func TestWatchConcurrency(t *testing.T) {
	r, dir := watchRig(t)
	r.s.cfg.MaxConcurrent = 1
	started1, release1 := gate(t, dir, "slow1")
	started2, release2 := gate(t, dir, "slow2")
	started3, release3 := gate(t, dir, "slow3")
	w1 := r.add(asBoss, watchDef("slow1", ``))
	w2 := r.add(asBoss, watchDef("slow2", ``))
	w3 := r.add(asBoss, watchDef("slow3", ``))
	note := r.add(asBoss, `{"name":"reminder","kind":"notify","schedule":{"every":"15m"},"to":"tg:-100","text":"stand-up"}`)
	agent := r.add(asBoss, `{"name":"agent","schedule":{"at":"2026-11-03T12:20"},"prompt":"p"}`) // 10:20 UTC
	// 10:15: three checks and a reminder are due. The due loop does not
	// wait for checks; the reminder goes out while they run.
	r.now = r.now.Add(15 * time.Minute)
	begin := time.Now()
	r.tick()
	if d := time.Since(begin); d > 10*time.Second {
		t.Fatalf("the due loop waited %s for checks", d)
	}
	if sent := r.sent(); len(sent) != 1 || sent[0].Text != "stand-up" || r.runs(note)[0].State != store.RunOK {
		t.Fatalf("a slow check delayed the notify job: %s", texts(sent))
	}
	// Two of the three checks run (their own cap); the third waits.
	jobs := []string{w1, w2, w3}
	starts, releases := []string{started1, started2, started3}, []string{release1, release2, release3}
	waiting := -1
	for i, id := range jobs {
		switch r.runs(id)[0].State {
		case store.RunRunning:
			waitFile(t, starts[i])
		case store.RunPending:
			if waiting >= 0 {
				t.Fatal("more than one check waiting")
			}
			waiting = i
		}
	}
	if waiting < 0 {
		t.Fatal("a third check started beyond the cap of 2")
	}
	// 10:20: the agent run starts beside the checks and fills
	// max_concurrent (1) and per_creator (1).
	r.now = r.now.Add(5 * time.Minute)
	r.tick()
	if len(r.rows) != 1 || r.runs(agent)[0].State != store.RunRunning {
		t.Fatalf("agent run not handed over beside the checks: %d", len(r.rows))
	}
	// One check ends: the waiting one starts at the next look, the agent
	// run still filling both caps.
	first := (waiting + 1) % 3
	touch(t, releases[first])
	touch(t, releases[waiting])
	for r.runs(jobs[first])[0].State == store.RunRunning {
		time.Sleep(10 * time.Millisecond)
	}
	r.tick()
	waitFile(t, starts[waiting])
	touch(t, releases[(waiting+2)%3])
	r.settle()
	for _, id := range jobs {
		if got := r.runs(id)[0]; got.State != store.RunOK {
			t.Fatalf("check %s: %+v", id, got)
		}
	}
}

// TestWatchStop: !job stop cancels the running check, whether the
// scheduler's stop is called (control) or only the run's row is cancelled
// by the command (the router's path).
func TestWatchStop(t *testing.T) {
	r, dir := watchRig(t)
	started, _ := gate(t, dir, "slow")
	id := r.add(asBoss, watchDef("slow", ``))
	r.run(id)
	waitFile(t, started)
	begin := time.Now()
	r.cmd(asBoss, "job stop "+id)
	r.settle()
	if got := r.runs(id)[0]; got.State != store.RunCancelled || time.Since(begin) > 3*time.Second {
		t.Fatalf("stop: %+v after %s", got, time.Since(begin))
	}
	if out := r.sent(); strings.Contains(texts(out), "alert") {
		t.Fatal("a stopped check delivered")
	}
	// The router stores the command's change and wakes the due loop; it
	// never calls the scheduler's stop.
	_ = os.Remove(started)
	r.run(id)
	waitFile(t, started)
	_, change := r.s.Command(asBoss, "job stop "+id)
	if err := r.st.AnswerCommand(0, core.ConvKey{}, store.CommandEnd{Detail: "job", Job: change}); err != nil {
		t.Fatal(err)
	}
	r.tick()
	r.settle()
	if got := r.runs(id)[0]; got.State != store.RunCancelled {
		t.Fatalf("router-path stop: %+v", got)
	}
}

// TestWatchDroppedWhenChanged: output of a check whose job was edited,
// paused or removed meanwhile is dropped.
func TestWatchDroppedWhenChanged(t *testing.T) {
	for _, change := range []string{"edit", "pause"} {
		t.Run(change, func(t *testing.T) {
			r, dir := watchRig(t)
			started, release := gate(t, dir, "slow")
			id := r.add(asBoss, watchDef("slow", ``))
			r.now = r.now.Add(15 * time.Minute)
			r.tick()
			waitFile(t, started)
			if change == "edit" {
				r.cmd(asBoss, `job edit `+id+` {"summary":"changed"}`)
			} else {
				r.cmd(asBoss, "job pause "+id)
			}
			touch(t, release)
			r.settle()
			if got := r.runs(id)[0]; got.State != store.RunCancelled || !strings.Contains(got.Cause, "output dropped") {
				t.Fatalf("%s: %+v", change, got)
			}
			if out := r.sent(); strings.Contains(texts(out), "alert") {
				t.Fatal("changed job's output delivered")
			}
		})
	}
}

// TestWatchRestartHoldWindow: a check caught by a restart is interrupted
// and never re-run; a drain's hold keeps checks from starting; outside the
// delivery window the run is skipped and the checker does not run; a
// one-off ends done.
func TestWatchRestartHoldWindow(t *testing.T) {
	r, dir := watchRig(t)
	started, release := gate(t, dir, "slow")
	id := r.add(asBoss, watchDef("slow", ``))
	r.now = r.now.Add(15 * time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	r.s.tick(ctx)
	waitFile(t, started)
	cancel() // shutdown: the check is killed, nothing is written
	r.settle()
	if got := r.runs(id)[0]; got.State != store.RunRunning {
		t.Fatalf("shutdown settled the run: %+v", got)
	}
	touch(t, release)
	s2 := New(r.s.cfg, r.st, r.s.access, log.New(io.Discard, "", 0))
	s2.now = r.s.now
	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := r.runs(id)[0]; got.State != store.RunInterrupted {
		t.Fatalf("after restart: %+v", got)
	}
	if out := texts(r.sent()); !strings.Contains(out, "interrupted by a restart and was not re-run") {
		t.Fatalf("restart notice: %s", out)
	}
	_ = os.Remove(started)
	s2.tick(context.Background())
	s2.running.Wait()
	if _, err := os.Stat(started); err == nil || len(r.runs(id)) != 1 {
		t.Fatal("an interrupted check was re-run")
	}

	// Drain's hold: due checks wait; after it they run.
	h, hdir := watchRig(t)
	script(t, hdir, "ping", "echo ping")
	hid := h.add(asBoss, watchDef("ping", ``))
	_ = h.st.SetMeta(MetaHold, h.now.Add(time.Hour).UnixMilli())
	h.now = h.now.Add(15 * time.Minute)
	h.tick()
	h.settle()
	if got := h.runs(hid)[0]; got.State != store.RunPending || len(h.sent()) != 0 {
		t.Fatalf("held check: %+v", got)
	}
	// An edit while the run waits is not an edit during the check.
	h.cmd(asBoss, `job edit `+hid+` {"summary":"edited while held"}`)
	_ = h.st.SetMeta(MetaHold, 0)
	h.tick()
	h.settle()
	if got := h.runs(hid)[0]; got.State != store.RunOK {
		t.Fatalf("after the hold: %+v", got)
	}
	if sent := h.sent(); len(sent) != 1 || sent[0].Text != "ping" {
		t.Fatalf("held check's output after an edit: %s", texts(sent))
	}
	// The hold checked where the check starts.
	pending := store.Run{Job: hid, Version: 1, Slot: "manual:1", Due: h.now, Trigger: "manual", State: store.RunPending}
	if _, err := h.st.CreateRun(pending, "", time.Time{}); err != nil {
		t.Fatal(err)
	}
	runs := h.runs(hid)
	_ = h.st.SetMeta(MetaHold, h.now.Add(time.Hour).UnixMilli())
	if held, ok, err := h.st.StartLocalRun(MetaHold, h.now, runs[0].ID); !held || ok || err != nil {
		t.Fatalf("StartLocalRun under a hold: %v %v %v", held, ok, err)
	}

	// Outside the delivery window (Sundays only; the rig is a Tuesday):
	// skipped, the checker never runs; a one-off ends done.
	w, wdir := watchRig(t)
	marker := filepath.Join(t.TempDir(), "ran")
	script(t, wdir, "marked", "touch "+marker+"\necho should not be sent")
	wid := w.add(asBoss, `{"name":"once","kind":"watch","check":{"checker":"marked"},"schedule":{"at":"2026-11-03T13:00"},"to":"tg:-100","delivery_window":{"tz":"UTC","days":[0],"start":"09:00","end":"18:00"}}`)
	w.now = w.now.Add(time.Hour + time.Minute)
	w.tick()
	w.settle()
	if got := w.runs(wid)[0]; got.State != store.RunSkipped || got.Cause != "outside delivery window" {
		t.Fatalf("outside window: %+v", got)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the checker ran outside its delivery window")
	}
	if j := w.job(wid); j.State != store.JobDone {
		t.Fatalf("one-off not done: %+v", j)
	}
}
