package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"syscall"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// running starts a run of job and returns its scheduled control origin, as
// the router builds it from the harness run (Conv/Private deliberately wrong:
// the scheduler must take them from the stored job, never from the turn).
func running(t *testing.T, r *rig, id string) Caller {
	t.Helper()
	r.run(id)
	for _, run := range r.runs(id) {
		if run.State == store.RunRunning {
			return Caller{Principal: boss, Role: core.RoleOwner, Conv: team, Job: id, Run: run.ID}
		}
	}
	t.Fatal("no running run")
	return Caller{}
}

func TestScheduledControlOriginBinding(t *testing.T) {
	ctx := context.Background()
	r, id, self, other := destRig(t)
	c := &Control{s: r.s, owner: boss.Key(), uid: 1234}
	r.s.Reverify = func(_ context.Context, p core.Principal) (Verified, bool, error) {
		return Verified{Principal: p}, true, nil
	}
	slackDM := core.ConvKey{Interface: "slack", Chat: "DSELF", Thread: "1000.3"}
	aliasJob := r.add(Caller{Principal: self, Role: core.RoleOwner, Conv: slackDM, Private: true}, `{"name":"alias job","schedule":{"every":"1h"},"prompt":"p"}`)
	otherJob := r.add(Caller{Principal: other, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "DOTHER"}, Private: true}, `{"name":"other owner","schedule":{"every":"1h"},"prompt":"p"}`)
	who := running(t, r, id)
	ok := func(req ControlRequest, origin Caller) ControlResult {
		t.Helper()
		got := c.execute(ctx, 1234, req, origin)
		if got.Error != "" || got.Reply == "" {
			t.Fatalf("%s refused: %s", req.Action, got.Error)
		}
		return got
	}
	refused := func(req ControlRequest, origin Caller, want string) {
		t.Helper()
		if got := c.execute(ctx, 1234, req, origin); !strings.Contains(got.Error, want) {
			t.Fatalf("%s %+v: got %+v, want %q", req.Action, origin, got, want)
		}
	}

	// Authorized: its own job and the same person's alias job, read only.
	for _, a := range []string{"show", "history", "checkpoint"} {
		ok(ControlRequest{Action: a, ID: id}, who)
	}
	ok(ControlRequest{Action: "list"}, who)
	ok(ControlRequest{Action: "show", ID: aliasJob}, who)
	refused(ControlRequest{Action: "show", ID: otherJob}, who, "no job of the bound creator")

	// Drafts, changes and acknowledgements stay with live humans.
	for _, req := range []ControlRequest{
		{Action: "pause", ID: id, RequestID: "p1"},
		{Action: "run", ID: id, RequestID: "r1"},
		{Action: "stop", ID: id, RequestID: "s1"},
		{Action: "remove", ID: id, RequestID: "x1"},
		{Action: "create", RequestID: "c1", Definition: json.RawMessage(`{"name":"n","schedule":{"every":"1h"},"prompt":"p"}`)},
		{Action: "acknowledge", ID: id, RequestID: "a1", Key: "k", Revision: "v", Outbound: 1},
	} {
		refused(req, who, "scheduled runs may only read")
	}
	if j := r.job(id); j.State != store.JobActive {
		t.Fatal("scheduled control changed the job", j.State)
	}

	// Wrong creator, run or job: nothing in the origin is taken on trust.
	for name, bad := range map[string]Caller{
		"employee":    {Principal: worker, Role: core.RoleOwner, Job: id, Run: who.Run},
		"other owner": {Principal: other, Role: core.RoleOwner, Job: id, Run: who.Run},
		"alias":       {Principal: self, Role: core.RoleOwner, Job: id, Run: who.Run},
		"run":         {Principal: boss, Role: core.RoleOwner, Job: id, Run: who.Run + 1000},
		"job":         {Principal: boss, Role: core.RoleOwner, Job: aliasJob, Run: who.Run},
		"role":        {Principal: boss, Role: core.RoleEmployee, Job: id, Run: who.Run},
	} {
		t.Run(name, func(t *testing.T) { refused(ControlRequest{Action: "show", ID: id}, bad, "origin") })
	}

	// Generation/epoch: a stale peer or replaced turn is refused before work.
	cred := &syscall.Ucred{Uid: 1234}
	if got := c.executePeer(ctx, cred, ControlRequest{Action: "show", ID: id}, who, func() bool { return false }); !strings.Contains(got.Error, "no longer active") {
		t.Fatal("stale generation admitted", got)
	}

	// Revoked creator: owner admission is evaluated per call.
	t.Setenv("ADMIN_USER_IDS", "other@example.test")
	refused(ControlRequest{Action: "show", ID: id}, who, "origin")
	t.Setenv("ADMIN_USER_IDS", "1,other@example.test")
	ok(ControlRequest{Action: "show", ID: id}, who)

	// Session/version: an edit while running makes the run stale.
	r.cmd(asBoss, `job edit `+id+` {"prompt":"edited"}`)
	if r.job(id).Version == 1 {
		t.Fatal("edit did not change the job version")
	}
	refused(ControlRequest{Action: "show", ID: id}, who, "origin")

	// Ended run: no origin remains after the turn finishes.
	r.finish("done", Outcome{Harness: "codex"})
	refused(ControlRequest{Action: "show", ID: id}, who, "origin")
	who2 := running(t, r, id)
	ok(ControlRequest{Action: "show", ID: id}, who2)
	r.finish("done", Outcome{Harness: "codex"})
	refused(ControlRequest{Action: "show", ID: id}, who2, "origin")
}

// A scheduled job delivering to a group gets no private details, whatever
// the harness turn claimed; listing stays the shared-chat rendering.
func TestScheduledControlPrivacyFromStoredTarget(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)
	c := &Control{s: r.s, owner: boss.Key(), uid: 1234}
	id := r.add(asBoss, `{"name":"group job","schedule":{"every":"1h"},"to":"tg:-100","prompt":"p"}`)
	who := running(t, r, id)
	who.Conv, who.Private = bossDM, true // a forged private claim
	if got := c.execute(ctx, 1234, ControlRequest{Action: "checkpoint", ID: id}, who); !strings.Contains(got.Error, "private originating conversation") {
		t.Fatal("shared target exposed details", got)
	}
	if got := c.execute(ctx, 1234, ControlRequest{Action: "list"}, who); got.Error != "" {
		t.Fatal(got)
	}
}

// Alias reads are a scheduled-run-only extension: a live person turn keeps
// exact-creator reads, while the trusted scheduled run of the same person may
// read the alias's job (reads only).
func TestAliasReadsOnlyForScheduledOrigin(t *testing.T) {
	ctx := context.Background()
	r, id, self, _ := destRig(t)
	c := &Control{s: r.s, owner: boss.Key(), uid: 1234}
	slackDM := core.ConvKey{Interface: "slack", Chat: "DSELF", Thread: "1000.3"}
	aliasJob := r.add(Caller{Principal: self, Role: core.RoleOwner, Conv: slackDM, Private: true}, `{"name":"alias job","schedule":{"every":"1h"},"prompt":"p"}`)
	ingress(t, r, bossDM, boss, true, core.OriginLive)
	live := asBoss
	if got := c.execute(ctx, 1234, ControlRequest{Action: "show", ID: id}, live); got.Error != "" {
		t.Fatal("live exact-creator read refused", got)
	}
	for _, a := range []string{"show", "history", "checkpoint"} {
		if got := c.execute(ctx, 1234, ControlRequest{Action: a, ID: aliasJob}, live); !strings.Contains(got.Error, "no job of the bound creator") {
			t.Fatalf("live alias %s admitted: %+v", a, got)
		}
	}
	scheduled := running(t, r, id)
	if got := c.execute(ctx, 1234, ControlRequest{Action: "show", ID: aliasJob}, scheduled); got.Error != "" || got.Reply == "" {
		t.Fatal("scheduled alias read refused", got)
	}
	if got := c.execute(ctx, 1234, ControlRequest{Action: "pause", ID: aliasJob, RequestID: "ap"}, scheduled); !strings.Contains(got.Error, "scheduled runs may only read") {
		t.Fatal("scheduled alias mutation admitted", got)
	}
}
