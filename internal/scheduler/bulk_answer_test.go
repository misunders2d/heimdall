package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/store"
)

// draft stores a proposal as a waiting draft of c, as a turn's answer does,
// a second after the last one (drafts are answered in creation order).
func (r *rig) draft(c Caller, def string) store.Job {
	r.t.Helper()
	r.now = r.now.Add(time.Second)
	j, err := r.s.proposal(c, []byte(def))
	if err != nil {
		r.t.Fatal(err)
	}
	r.apply(&store.JobChange{Add: &j})
	return j
}

// A plain yes with several drafts waiting confirms every one of them (owner,
// 2026-11-11: "I have to spend 10 minutes typing in those ids?"), one line
// each, no ids; a draft that fails is reported and never blocks the others.
func TestBulkAnswer(t *testing.T) {
	t.Run("yes confirms four drafts", func(t *testing.T) {
		r := newRig(t)
		var ids []string
		for _, n := range []string{"alpha", "bravo", "charlie", "delta"} {
			ids = append(ids, r.draft(asBoss, `{"name":"`+n+`","schedule":{"every":"1h"},"prompt":"p"}`).ID)
		}
		out, ok := r.answer(asBoss, "yes")
		want := "*📋 Requests answered*\n• *alpha* — created\n• *bravo* — created\n• *charlie* — created\n• *delta* — created"
		if !ok || out != want {
			t.Fatalf("got %q, want %q", out, want)
		}
		for _, id := range ids {
			if r.job(id).State != store.JobActive {
				t.Fatal("not confirmed", id, r.job(id).State)
			}
		}
		if _, handled := r.answer(asBoss, "yes"); handled {
			t.Fatal("answered drafts still bound")
		}
	})
	t.Run("one draft fails validation, the others apply", func(t *testing.T) {
		r := newRig(t)
		good := r.draft(asBoss, `{"name":"hourly","schedule":{"every":"1h"},"prompt":"p"}`)
		late := r.draft(asBoss, `{"name":"soon","schedule":{"at":"2026-11-03T12:30"},"prompt":"p"}`)
		paused := r.draft(asBoss, `{"name":"quiet","schedule":{"every":"1h"},"prompt":"p","paused":true}`)
		r.now = r.now.Add(time.Hour) // the one-off is past now
		out, ok := r.answer(asBoss, "OK!")
		if !ok || !strings.Contains(out, "• *hourly* — created") || !strings.Contains(out, "• *soon* — not approved: schedule: no future run") || !strings.Contains(out, "• *quiet* — created, paused") {
			t.Fatal(out)
		}
		if r.job(good.ID).State != store.JobActive || r.job(late.ID).State != store.JobDraft || r.job(paused.ID).State != store.JobPaused {
			t.Fatal(r.job(good.ID).State, r.job(late.ID).State, r.job(paused.ID).State)
		}
	})
	t.Run("no discards every draft", func(t *testing.T) {
		r := newRig(t)
		id := r.add(asBoss, `{"name":"kept","schedule":{"every":"1h"},"prompt":"p"}`)
		a := r.draft(asBoss, `{"name":"new","schedule":{"every":"1h"},"prompt":"p"}`)
		b := r.draft(asBoss, `{"remove":"`+id+`"}`)
		out, ok := r.answer(asBoss, "нет")
		if !ok || out != "*📋 Requests cancelled*\n• *new* — cancelled\n• *kept* — cancelled" {
			t.Fatalf("%q", out)
		}
		if _, ok, _ := r.st.Job(a.ID); ok {
			t.Fatal("draft kept")
		}
		if _, ok, _ := r.st.Job(b.ID); ok || r.job(id).State != store.JobActive {
			t.Fatal("removal draft kept or applied")
		}
	})
	t.Run("mixed create, edit and removal", func(t *testing.T) {
		r := newRig(t)
		gone := r.add(asBoss, `{"name":"old","schedule":{"every":"1h"},"prompt":"p"}`)
		changed := r.add(asBoss, `{"name":"report","schedule":{"every":"1h"},"prompt":"p"}`)
		var stopped []string
		r.s.stop = func(id string) bool { stopped = append(stopped, id); return true }
		// The removal is asked first; it still goes last.
		r.draft(asBoss, `{"remove":"`+gone+`"}`)
		created := r.draft(asBoss, `{"name":"fresh","schedule":{"every":"1h"},"prompt":"p"}`)
		r.draft(asBoss, `{"edit":"`+changed+`","prompt":"shorter"}`)
		out, ok := r.answer(asBoss, "так")
		if !ok || out != "*📋 Requests answered*\n• *fresh* — created\n• *report* — updated\n• *old* — removed" {
			t.Fatalf("%q", out)
		}
		if r.job(gone).State != store.JobDeleted || r.job(created.ID).State != store.JobActive {
			t.Fatal(r.job(gone).State, r.job(created.ID).State)
		}
		if d, _, _ := parse(r.job(changed)); d.Prompt != "shorter" {
			t.Fatal("edit not applied", d)
		}
		if len(stopped) != 1 || stopped[0] != gone {
			t.Fatal("removed job's current run not stopped", stopped)
		}
		if drafts := r.s.drafts(asBoss); len(drafts) != 0 {
			t.Fatal("drafts left", drafts)
		}
	})
	t.Run("removal of a job edited in the same reply", func(t *testing.T) {
		r := newRig(t)
		id := r.add(asBoss, `{"name":"both","schedule":{"every":"1h"},"prompt":"p"}`)
		r.draft(asBoss, `{"remove":"`+id+`"}`)
		r.draft(asBoss, `{"edit":"`+id+`","prompt":"late"}`)
		out, ok := r.answer(asBoss, "yes")
		if !ok || out != "*📋 Requests answered*\n• *both* — updated\n• *both* — removed" || r.job(id).State != store.JobDeleted {
			t.Fatalf("%q %s", out, r.job(id).State)
		}
	})
	t.Run("employee cap counts creates above", func(t *testing.T) {
		r := newRig(t)
		for i := 0; i < 9; i++ {
			r.add(asWork, `{"name":"n","kind":"notify","schedule":{"every":"1h"},"text":"t"}`)
		}
		a := r.draft(asWork, `{"name":"tenth","kind":"notify","schedule":{"every":"1h"},"text":"t"}`)
		b := r.draft(asWork, `{"name":"eleventh","kind":"notify","schedule":{"every":"1h"},"text":"t"}`)
		out, ok := r.answer(asWork, "yes")
		if !ok || !strings.Contains(out, "• *tenth* — created") || !strings.Contains(out, "• *eleventh* — not approved: Task not created: at most 10") {
			t.Fatal(out)
		}
		if r.job(a.ID).State != store.JobActive || r.job(b.ID).State != store.JobDraft {
			t.Fatal(r.job(a.ID).State, r.job(b.ID).State)
		}
	})
	t.Run("only the caller's own drafts in this conversation", func(t *testing.T) {
		r := newRig(t)
		mine := r.draft(asBoss, `{"name":"mine","schedule":{"every":"1h"},"prompt":"p"}`)
		r.draft(asBoss, `{"name":"mine too","schedule":{"every":"1h"},"prompt":"p"}`)
		group := r.draft(inGroup, `{"name":"group","schedule":{"every":"1h"},"prompt":"p"}`)
		theirs := r.draft(asWork, `{"name":"theirs","kind":"notify","schedule":{"every":"1h"},"text":"t"}`)
		if out, _ := r.answer(asBoss, "yes"); strings.Contains(out, "group") || strings.Contains(out, "theirs") || r.job(mine.ID).State != store.JobActive {
			t.Fatal(out)
		}
		if r.job(group.ID).State != store.JobDraft || r.job(theirs.ID).State != store.JobDraft {
			t.Fatal("another conversation's or caller's draft answered")
		}
	})
	t.Run("group shows no names", func(t *testing.T) {
		r := newRig(t)
		r.draft(inGroup, `{"name":"Private name","summary":"Private purpose","schedule":{"every":"1h"},"prompt":"p"}`)
		r.draft(inGroup, `{"name":"Other private","schedule":{"every":"1h"},"prompt":"p"}`)
		details, _, _ := r.s.Answer(inGroup, "details")
		out, _ := r.answer(inGroup, "yes")
		for _, text := range []string{details, out} {
			if strings.Contains(text, "Private") || !strings.Contains(text, "Scheduled task") || idRE.MatchString(text) {
				t.Fatal(text)
			}
		}
	})
}
