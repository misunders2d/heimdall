package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// Owner, 2026-11-20: "my agents can assign, other people's agents can't";
// "card is good option"; "just make sure it's clear what I'm being asked
// of. clearly, concisely and without tons of technical babble."

type taskCalls struct {
	calls []Caller
	reply string
	ok    bool
}

// fakeTasks admits every add (as durable would) and records the callers.
func fakeTasks(r *rig) *taskCalls {
	tc := &taskCalls{}
	r.s.Tasks = func(who Caller, action, issue, title, requestID string) (string, bool) {
		if action != "add" {
			return "Sent " + action + " to " + issue + ".", action == "wake"
		}
		tc.calls = append(tc.calls, who)
		if tc.reply != "" {
			return tc.reply, tc.ok
		}
		return "Tracking " + issue + " (https://linear.app/x). It stays visible until closed.", true
	}
	return tc
}

func agentRig(t *testing.T) (*rig, *Control, *fakeLauncher, Caller, func() bool) {
	t.Helper()
	r, c, f, run := externalRig(t)
	r.s.External[0].Agents = true
	r.s.External[0].Label = "AgentNet"
	f.set(vouch(run, "person:sergey", "agent"))
	who, valid, err := c.resolvePeer(run.Process.Pid, alwaysAlive)
	if err != nil {
		t.Fatal(err)
	}
	return r, c, f, who, valid
}

var (
	op      = &syscall.Ucred{Uid: 1234}
	fixWin  = ControlRequest{Action: "task_add", ID: "MEL-900", Title: "Fix the Windows save bug in AgentNet", RequestID: "agent-1"}
	noWords = []string{"MEL-900", "req-1", "person:sergey", "agentnet req", "request_id", "run "}
)

func TestExternalAgentNeedsOptIn(t *testing.T) {
	_, c, f, run := externalRig(t)
	f.set(vouch(run, "person:sergey", "agent"))
	if _, _, err := c.resolvePeer(run.Process.Pid, alwaysAlive); err == nil || !strings.Contains(err.Error(), "was not requested by the person themself") {
		t.Fatal("agent answer admitted without the launcher's opt-in", err)
	}
}

func TestExternalAgentMayOnlyProposeAndWake(t *testing.T) {
	r, c, _, who, valid := agentRig(t)
	tc := fakeTasks(r)
	ctx := context.Background()
	for _, req := range []ControlRequest{
		daily,
		{Action: "task_list"},
		{Action: "task_show", ID: "MEL-1"},
		{Action: "task_close", ID: "MEL-1", RequestID: "c1"},
		{Action: "list"},
	} {
		if got := c.executePeer(ctx, op, req, who, valid); !strings.Contains(got.Error, "agent requests may only propose a task") {
			t.Fatalf("%s from an agent: %+v", req.Action, got)
		}
	}
	if got := c.executePeer(ctx, op, ControlRequest{Action: "task_wake", ID: "MEL-1", RequestID: "w1"}, who, valid); got.Error != "" || !got.Changed {
		t.Fatal("agent task_wake refused", got)
	}
	got := c.executePeer(ctx, op, fixWin, who, valid)
	if got.Error != "" || !got.Changed || got.DraftID == "" {
		t.Fatal(got)
	}
	if len(tc.calls) != 0 {
		t.Fatal("task admitted before the owner's Yes", tc.calls)
	}
	if jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key(), States: []string{store.JobActive, store.JobPaused}}); len(jobs) != 0 {
		t.Fatal("a proposal became a job", jobs)
	}
}

func TestExternalOtherPeoplesAgentsRefused(t *testing.T) {
	t.Run("unmapped person", func(t *testing.T) {
		_, c, f, run := externalRig(t)
		c.s.External[0].Agents = true
		f.set(vouch(run, "person:alice", "agent"))
		if _, _, err := c.resolvePeer(run.Process.Pid, alwaysAlive); err == nil || !strings.Contains(err.Error(), "has no Heimdall identity mapping") {
			t.Fatal(err)
		}
	})
	t.Run("mapped non-owner", func(t *testing.T) {
		r, c, f, run := externalRig(t)
		c.s.External[0].Agents = true
		if _, _, err := r.st.AddInbound(core.Inbound{Conv: workDM, From: worker, Origin: core.OriginLive, Private: true, MsgID: "w"}); err != nil {
			t.Fatal(err)
		}
		f.set(vouch(run, "person:worker", "agent"))
		if _, _, err := c.resolvePeer(run.Process.Pid, alwaysAlive); err == nil || !strings.Contains(err.Error(), "not a current owner") {
			t.Fatal(err)
		}
	})
}

// The card face says who asks, what, and where the report goes, plainly;
// no issue id, run, person or draft id. Details has them.
func TestExternalProposalCardFace(t *testing.T) {
	r, c, _, who, valid := agentRig(t)
	fakeTasks(r)
	req := fixWin
	req.Title = "Fix *the* Windows   save bug <!here> [link](http://x)"
	got := c.executePeer(context.Background(), op, req, who, valid)
	if got.Error != "" {
		t.Fatal(got)
	}
	outs := r.sent()
	if len(outs) != 1 || outs[0].Conv != bossDM {
		t.Fatal("one card in the owner's private chat", outs)
	}
	card := outs[0].Text
	for _, want := range []string{"Your agent on AgentNet wants to start a job for you", "“Fix \\*the\\* Windows save bug \\<!here\\> \\[link\\](http://x)”", "When done, I'll tell you in: this private chat", "Yes", "No", "— heimdall"} {
		if !strings.Contains(card, want) {
			t.Fatalf("card lacks %q:\n%s", want, card)
		}
	}
	for _, bad := range append(noWords, got.DraftID) {
		if strings.Contains(card, bad) {
			t.Fatalf("card face shows %q:\n%s", bad, card)
		}
	}
	if idRE.MatchString(card) {
		t.Fatal("card shows a raw id", card)
	}
	details, ok := r.answer(asBoss, "details")
	if !ok || !strings.Contains(details, "MEL-900") || !strings.Contains(details, "req-1") || !strings.Contains(details, "person:sergey") || !strings.Contains(details, "author agent") {
		t.Fatal("details lack the request's provenance", details)
	}
}

// No, a Yes from a group or from someone else, expiry and a restart each
// leave the one draft row and at most one line.
func TestExternalProposalAnswers(t *testing.T) {
	t.Run("no", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		r.sent()
		if out, ok := r.answer(asBoss, "no"); !ok || !strings.Contains(out, "cancelled") {
			t.Fatal(out)
		}
		if len(r.s.proposals(boss)) != 0 || len(tc.calls) != 0 {
			t.Fatal("No kept the draft or admitted it")
		}
		r.now = r.now.Add(25 * time.Hour)
		r.tick()
		if outs := r.sent(); len(outs) != 0 {
			t.Fatal("a cancelled proposal also expired", outs)
		}
		// The same run cannot propose the issue again after a No.
		if got := c.executePeer(context.Background(), op, ControlRequest{Action: "task_add", ID: "MEL-900", Title: "Fix the Windows save bug in AgentNet", RequestID: "agent-2"}, who, valid); got.DraftID == "" || len(r.sent()) != 0 || len(r.s.proposals(boss)) != 0 {
			t.Fatal("the run raised a second card after No", got)
		}
	})
	t.Run("group or another caller", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		for _, other := range []Caller{inGroup, {Principal: worker, Role: core.RoleOwner, Conv: bossDM, Private: true}} {
			if out, ok := r.answer(other, "yes"); ok {
				t.Fatal("answered by", other, out)
			}
			if out := r.cmd(other, "job confirm "+r.s.proposals(boss)[0].ID); !strings.Contains(out, "No draft") {
				t.Fatal("confirmed by", other, out)
			}
		}
		if len(tc.calls) != 0 || len(r.s.proposals(boss)) != 1 {
			t.Fatal("someone else's Yes did something")
		}
	})
	t.Run("expiry", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		r.sent()
		r.now = r.now.Add(24*time.Hour + 2*time.Minute)
		r.tick()
		outs := r.sent()
		if len(outs) != 1 || outs[0].Conv != bossDM || !strings.Contains(outs[0].Text, "expired") || !strings.Contains(outs[0].Text, "Nothing was started") || strings.Contains(outs[0].Text, "MEL-900") {
			t.Fatal("one plain expiry line", outs)
		}
		r.tick()
		if outs := r.sent(); len(outs) != 0 || len(r.s.proposals(boss)) != 0 {
			t.Fatal("expiry repeated", outs)
		}
		if jobs, _ := r.st.Jobs(store.JobFilter{Creator: boss.Key()}); len(jobs) != 0 {
			t.Fatal("expired draft row kept", jobs)
		}
		if out, ok := r.answer(asBoss, "yes"); ok || len(tc.calls) != 0 {
			t.Fatal("an expired proposal was answered", out)
		}
	})
	t.Run("restart", func(t *testing.T) {
		r, c, f, who, valid := agentRig(t)
		fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		if outs := r.sent(); len(outs) != 1 {
			t.Fatal(outs)
		}
		r.st.Close()
		st, err := store.Open(r.dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		r.st = st
		r.s = New(r.s.cfg, st, r.s.access, log.New(io.Discard, "", 0))
		r.s.now = func() time.Time { return r.now }
		tc := fakeTasks(r)
		if err := r.s.Recover(); err != nil {
			t.Fatal(err)
		}
		r.tick()
		if outs := r.sent(); len(outs) != 0 || len(r.s.proposals(boss)) != 1 {
			t.Fatal("restart resent the card or lost the draft", outs)
		}
		asked := len(f.asked)
		if out, ok := r.answer(asBoss, "yes"); !ok || !strings.Contains(out, "Started") || len(tc.calls) != 1 {
			t.Fatal("Yes after restart", out)
		}
		if len(f.asked) != asked {
			t.Fatal("the tap asked the launcher")
		}
		w := tc.calls[0]
		if w.Principal.Key() != boss.Key() || w.Conv != bossDM || !w.Private || w.Vouch != nil || !strings.Contains(w.Proposed, "AgentNet agent proposal") {
			t.Fatalf("admitted as %+v", w)
		}
	})
}

// A bare Yes never sweeps up a proposal that arrived beside another draft.
func TestExternalProposalBesideOtherDraftsNeedsItsOwnAnswer(t *testing.T) {
	r, c, _, who, valid := agentRig(t)
	tc := fakeTasks(r)
	_, j, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"name\":\"mine\",\"schedule\":{\"every\":\"1h\"},\"prompt\":\"p\"}\n```"})
	r.apply(&store.JobChange{Add: j})
	c.executePeer(context.Background(), op, fixWin, who, valid)
	out, ok := r.answer(asBoss, "yes")
	if !ok || !strings.Contains(out, "a plain Yes or No answers none") || len(tc.calls) != 0 {
		t.Fatal(out)
	}
	if drafts := r.s.drafts(asBoss); len(drafts) != 2 {
		t.Fatal("a plain Yes changed drafts", drafts)
	}
	p := r.s.proposals(boss)[0]
	if !strings.Contains(out, "!job confirm "+p.ID) {
		t.Fatal("the list does not say how to answer each", out)
	}
	if out := r.cmd(asBoss, "job confirm "+p.ID); !strings.Contains(out, "Started") || len(tc.calls) != 1 {
		t.Fatal(out)
	}
	if out, ok := r.answer(asBoss, "yes"); !ok || !strings.Contains(out, "created") {
		t.Fatal("the remaining own draft answers as usual", out)
	}
}

func TestExternalProposalDedupeAndCap(t *testing.T) {
	r, c, f, who, valid := agentRig(t)
	fakeTasks(r)
	ctx := context.Background()
	c.executePeer(ctx, op, fixWin, who, valid)
	// A second run of the same person: the waiting issue is not sent twice.
	run2 := startRun(t)
	f.set(vouchRun(run2, "req-2", "person:sergey", "agent"))
	who2, valid2, err := c.resolvePeer(run2.Process.Pid, alwaysAlive)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.executePeer(ctx, op, fixWin, who2, valid2); got.Changed || !strings.Contains(got.Reply, "already waits") {
		t.Fatal(got)
	}
	for i := 2; i <= maxProposals; i++ {
		req := fixWin
		req.ID, req.RequestID = "MEL-90"+string(rune('0'+i)), "agent-x"
		if got := c.executePeer(ctx, op, req, who2, valid2); got.Error != "" {
			t.Fatal(i, got)
		}
	}
	over := fixWin
	over.ID = "MEL-999"
	if got := c.executePeer(ctx, op, over, who2, valid2); !strings.Contains(got.Error, "already wait") {
		t.Fatal("proposal cap", got)
	}
	if n := len(r.s.proposals(boss)); n != maxProposals {
		t.Fatal(n)
	}
	// Launcher request ids never meet a live turn's: the same id works in
	// both namespaces.
	live := ControlRequest{Action: "create", RequestID: "agent-1", Definition: json.RawMessage(`{"name":"live","schedule":{"every":"1h"},"prompt":"p"}`)}
	if got := c.executeBound(ctx, 1234, live); got.Error != "" {
		t.Fatal("live request id collided with a launcher's", got)
	}
}

// startRun starts another "run" process: a direct child of this test.
func startRun(t *testing.T) *exec.Cmd {
	t.Helper()
	run := exec.Command("sleep", "60")
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { run.Process.Kill(); run.Wait() })
	return run
}

// vouchRun is vouch with another run id.
func vouchRun(run *exec.Cmd, id, person, author string) func(externalQuery) externalAnswer {
	inner := vouch(run, person, author)
	return func(q externalQuery) externalAnswer {
		a := inner(q)
		if a.Error == "" {
			a.Run = id
		}
		return a
	}
}
