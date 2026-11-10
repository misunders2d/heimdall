package router

import (
	"path/filepath"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/store"
)

// A wake turn cut off by a restart (store.Recover's real record) is
// "cut off": the durable runtime continues it with a may-have-acted
// prompt, never a blind replay.
func TestWakeInterruptedIsCutOff(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	conv := core.ConvKey{Interface: "fake", Chat: core.ReviewChat("MEL-60")}
	in := core.Inbound{Conv: conv, MsgID: "MEL-60/1/work/1", From: core.Principal{Platform: "fake", ID: "owner"}, Origin: core.OriginCommitment, Text: "wake",
		Review: &core.Review{Issue: "MEL-60", Attempt: 1, Key: "MEL-60/1/work/1", Target: core.ConvKey{Interface: "fake", Chat: "c4"}, Work: true}}
	row, _, err := st.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartTurn(conv, core.RoleOwner, "fake:owner", in.MsgID, []int64{row.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recover(interruptedNotice, unconfirmedNotice); err != nil {
		t.Fatal(err)
	}
	o, _, err := st.InboundOutcome(conv, in.MsgID)
	if err != nil {
		t.Fatal(err)
	}
	if got := turnClass(o); got != durable.WakeCutOff {
		t.Fatalf("class %q for %+v", got, o)
	}
}

// Every recorded wake outcome maps to one class from typed state and the
// router's own held constants; only a turn no harness took is "not
// started", and anything unrecognised may have acted.
func TestWakeClassFromOutcome(t *testing.T) {
	for _, c := range []struct {
		o    store.InboundOutcome
		want string
	}{
		{store.InboundOutcome{State: store.InFailed, Turn: 1, TurnState: store.TurnFailed, TurnDetail: noHarness + "claude failed before starting: x"}, durable.WakeNotStarted},
		{store.InboundOutcome{State: store.InFailed, Turn: 1, TurnState: store.TurnFailed, TurnDetail: "claude: provider error"}, durable.WakeCutOff},
		{store.InboundOutcome{State: store.InCancelled, Turn: 1, TurnState: store.TurnCancelled, TurnDetail: "cancelled: timeout"}, durable.WakeCutOff},
		{store.InboundOutcome{State: store.InCancelled, Turn: 1, TurnState: store.TurnCancelled, TurnDetail: heldStopped}, durable.WakeStopped},
		{store.InboundOutcome{State: store.InNeedsHuman, Turn: 1, TurnState: store.TurnNeedsHuman, TurnDetail: heldApproval}, durable.WakeApproval},
		{store.InboundOutcome{State: store.InFailed, Turn: 1, TurnState: store.TurnFailed, TurnDetail: heldRefused}, durable.WakeRefused},
		{store.InboundOutcome{State: store.InRejected, Detail: "caller not authorized"}, durable.WakeRefused},
		{store.InboundOutcome{State: store.InDone, Turn: 1, TurnState: store.TurnDone, TurnDetail: "malformed answer: no report or next action"}, durable.WakeMalformed},
		{store.InboundOutcome{State: store.InDone, Turn: 1, TurnState: store.TurnDone, TurnDetail: heldStale + "work report: x"}, durable.WakeCutOff},
		{store.InboundOutcome{State: store.InFailed, Detail: "something new"}, durable.WakeCutOff},
	} {
		if got := turnClass(c.o); got != c.want {
			t.Fatalf("%+v: %q, want %q", c.o, got, c.want)
		}
	}
}
