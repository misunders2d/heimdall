package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// A proposal arrives unasked in the chat where the owner talks to his
// assistant: a bare word answers it only when it is exactly Yes, No or
// Details and the card is still the latest thing there. Otherwise the word
// is an ordinary message, and !jobs shows how to answer.
func TestExternalProposalBareWordOnlyWhileCardIsLatest(t *testing.T) {
	propose := func(t *testing.T) (*rig, *taskCalls) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		if got := c.executePeer(context.Background(), op, fixWin, who, valid); got.Error != "" {
			t.Fatal(got)
		}
		r.sent()
		// Store timestamps are milliseconds: what follows is later.
		time.Sleep(2 * time.Millisecond)
		return r, tc
	}
	t.Run("other yes and no words", func(t *testing.T) {
		r, tc := propose(t)
		for _, w := range []string{"ok", "OK!", "confirm", "да", "так", "cancel", "нет", "ні"} {
			if out, ok := r.answer(asBoss, w); ok || len(tc.calls) != 0 || len(r.s.proposals(boss)) != 1 {
				t.Fatalf("%q answered the proposal: %s", w, out)
			}
		}
		if out, ok := r.answer(asBoss, "Yes."); !ok || !strings.Contains(out, "Started") || len(tc.calls) != 1 {
			t.Fatal("exact Yes", out)
		}
	})
	t.Run("mid-turn", func(t *testing.T) {
		r, tc := propose(t)
		turn, err := r.st.StartTurn(bossDM, core.RoleOwner, boss.Key(), "m1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if out, ok := r.answer(asBoss, "yes"); ok || len(tc.calls) != 0 {
			t.Fatal("a yes sent while the assistant works answered the proposal", out)
		}
		if _, err := r.st.EndTurn(turn, bossDM, boss.Key(), store.TurnEnd{State: store.TurnDone, ItemState: store.InDone}); err != nil {
			t.Fatal(err)
		}
		// 20 hours later, after that answer: still an ordinary message.
		r.now = r.now.Add(20 * time.Hour)
		if out, ok := r.answer(asBoss, "yes"); ok || len(tc.calls) != 0 {
			t.Fatal("a yes after the assistant's answer answered the proposal", out)
		}
		if out, ok := r.answer(asBoss, "no"); ok || len(r.s.proposals(boss)) != 1 {
			t.Fatal("a no after the assistant's answer discarded the proposal", out)
		}
		// !jobs shows how to answer it.
		p := r.s.proposals(boss)[0]
		list := r.cmd(asBoss, "jobs")
		if !strings.Contains(list, "1 request is waiting for your answer") || !strings.Contains(list, "“Fix the Windows save bug in AgentNet”") || !strings.Contains(list, "`!job confirm "+p.ID+"`") {
			t.Fatal("!jobs does not show the waiting proposal", list)
		}
		if out := r.cmd(asBoss, "job confirm "+p.ID); !strings.Contains(out, "Started") || len(tc.calls) != 1 {
			t.Fatal(out)
		}
	})
	t.Run("unprompted delivery after the card", func(t *testing.T) {
		for name, out := range map[string]core.Outbound{
			"scheduled output": {Conv: bossDM, Text: "daily check: all fine"},
			// A task notice replies to the task's original, older message.
			"task notice": {Conv: bossDM, ReplyTo: "old", Text: "MEL-1 needs you: a question"},
		} {
			t.Run(name, func(t *testing.T) {
				r, tc := propose(t)
				if _, _, err := r.st.AddInbound(core.Inbound{Conv: bossDM, MsgID: "old", From: boss, Origin: core.OriginLive, Text: "do MEL-1", At: time.Now().Add(-time.Hour), Private: true}); err != nil {
					t.Fatal(err)
				}
				if _, err := r.st.Enqueue(out); err != nil {
					t.Fatal(err)
				}
				if out, ok := r.answer(asBoss, "yes"); ok || len(tc.calls) != 0 {
					t.Fatal("a yes after another delivery answered the proposal", out)
				}
			})
		}
	})
	t.Run("own draft after a stale card", func(t *testing.T) {
		r, tc := propose(t)
		turn, err := r.st.StartTurn(bossDM, core.RoleOwner, boss.Key(), "m1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.st.EndTurn(turn, bossDM, boss.Key(), store.TurnEnd{State: store.TurnDone, ItemState: store.InDone}); err != nil {
			t.Fatal(err)
		}
		_, j, _ := r.s.Draft(asBoss, []string{"```heimdall-job\n{\"name\":\"mine\",\"schedule\":{\"every\":\"1h\"},\"prompt\":\"p\"}\n```"})
		r.apply(&store.JobChange{Add: j})
		// The owner's own draft answers as usual; the proposal never.
		if out, ok := r.answer(asBoss, "ok"); !ok || !strings.Contains(out, "task created") || len(tc.calls) != 0 || len(r.s.proposals(boss)) != 1 {
			t.Fatal("own draft after a stale card", out)
		}
	})
	t.Run("details then yes", func(t *testing.T) {
		r, tc := propose(t)
		// The router stores the message, then the command's reply to it.
		if _, _, err := r.st.AddInbound(core.Inbound{Conv: bossDM, MsgID: "m2", From: boss, Origin: core.OriginLive, Text: "Details", Private: true}); err != nil {
			t.Fatal(err)
		}
		if out, ok := r.answer(asBoss, "Details"); !ok || !strings.Contains(out, "MEL-900") {
			t.Fatal(out)
		}
		if _, err := r.st.Enqueue(core.Outbound{Conv: bossDM, ReplyTo: "m2", Text: "details"}); err != nil {
			t.Fatal(err)
		}
		if out, ok := r.answer(asBoss, "yes"); !ok || !strings.Contains(out, "Started") || len(tc.calls) != 1 {
			t.Fatal("Yes after Details", out)
		}
	})
}

// The card shows the whole title, and the Yes admits exactly that text: a
// title the card could not show in full is refused, not altered.
func TestExternalProposalTitleIsWhatTheCardShows(t *testing.T) {
	r, c, _, who, valid := agentRig(t)
	var admitted []string
	r.s.Tasks = func(_ Caller, action, _, title, _ string) (string, bool) {
		admitted = append(admitted, title)
		return "Tracking.", action == "add"
	}
	for _, bad := range []string{
		"Fix the Windows save bug\U000E0061\U000E006C\U000E0073\U000E006F", // invisible tag characters
		"Fix the bug​ now",    // zero-width space
		"Fix the bug ‮meti",   // bidi override
		"Fix the bug ",       // private use
		"Fix the bug\x00 now", // control
		strings.Repeat("x", maxTitle+1),
	} {
		req := fixWin
		req.Title, req.RequestID = bad, "bad"
		if got := c.executePeer(context.Background(), op, req, who, valid); got.Error == "" || !strings.Contains(got.Error, "task_add not sent") {
			t.Fatalf("title %q accepted: %+v", bad, got)
		}
	}
	if outs := r.sent(); len(outs) != 0 || len(r.s.proposals(boss)) != 0 {
		t.Fatal("a refused title raised a card", outs)
	}
	req := fixWin
	req.Title = "  Fix   the " + strings.Repeat("é", maxTitle-12) + " "
	if got := c.executePeer(context.Background(), op, req, who, valid); got.Error != "" {
		t.Fatal(got)
	}
	outs := r.sent()
	if len(outs) != 1 {
		t.Fatal(outs)
	}
	if out, ok := r.answer(asBoss, "yes"); !ok || len(admitted) != 1 {
		t.Fatal(out)
	}
	want := "Fix the " + strings.Repeat("é", maxTitle-12)
	if admitted[0] != want || !strings.Contains(outs[0].Text, "“"+want+"”") || strings.Contains(outs[0].Text, "…") {
		t.Fatalf("admitted %q; card:\n%s", admitted[0], outs[0].Text)
	}
}

// A launcher removed from the config, or one whose agent opt-in was turned
// off (both take effect on restart), can no longer have its waiting cards
// answered Yes: the Yes says so, and the next tick withdraws the rest.
func TestExternalProposalRevokedLauncher(t *testing.T) {
	t.Run("yes after removal", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		r.sent()
		r.s.External = nil
		out, ok := r.answer(asBoss, "yes")
		if !ok || !strings.Contains(out, "no longer configured") || !strings.Contains(out, "Nothing was started") || len(tc.calls) != 0 {
			t.Fatal(out)
		}
		if len(r.s.proposals(boss)) != 0 {
			t.Fatal("revoked proposal kept")
		}
		r.tick()
		if outs := r.sent(); len(outs) != 0 {
			t.Fatal("answered proposal also withdrawn", outs)
		}
	})
	t.Run("launcher unusable at this start keeps its cards", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		r.sent()
		// Still configured, but its OS user did not resolve this time.
		r.s.External, r.s.Launchers = nil, map[string]bool{"agentnet": true}
		r.tick()
		if outs := r.sent(); len(outs) != 0 || len(r.s.proposals(boss)) != 1 {
			t.Fatal("an unusable launcher's card was withdrawn", outs)
		}
		if out, ok := r.answer(asBoss, "yes"); !ok || !strings.Contains(out, "Started") || len(tc.calls) != 1 {
			t.Fatal(out)
		}
	})
	t.Run("opt-in off withdraws on the next tick", func(t *testing.T) {
		r, c, _, who, valid := agentRig(t)
		tc := fakeTasks(r)
		c.executePeer(context.Background(), op, fixWin, who, valid)
		r.sent()
		r.s.External[0].Agents = false
		r.tick()
		outs := r.sent()
		if len(outs) != 1 || outs[0].Conv != bossDM || !strings.Contains(outs[0].Text, "withdrawn") || !strings.Contains(outs[0].Text, "no longer allowed to send agent requests") || len(r.s.proposals(boss)) != 0 || len(tc.calls) != 0 {
			t.Fatal("one withdrawal line", outs)
		}
		r.tick()
		if outs := r.sent(); len(outs) != 0 {
			t.Fatal("withdrawal repeated", outs)
		}
	})
}
