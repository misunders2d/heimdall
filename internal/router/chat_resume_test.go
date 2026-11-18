package router

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestOwnerChatRecoveryMidToolAndOriginalTarget(t *testing.T) {
	testOwnerChatRecovery(t, true, false)
}

func TestOwnerChatRecoveryMidGeneration(t *testing.T) {
	testOwnerChatRecovery(t, false, false)
}

func TestOwnerChatRecoverySecondCrashKeepsTurnAndCapStops(t *testing.T) {
	testOwnerChatRecovery(t, false, true)
}

func testOwnerChatRecovery(t *testing.T, tool, crashAgain bool) {
	t.Helper()
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	st, err := store.Open(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	conv := core.ConvKey{Interface: "fake", Chat: "room", Thread: "thread"}
	in := core.Inbound{Conv: conv, MsgID: "original", From: core.Principal{Platform: "fake", ID: "owner"}, Origin: core.OriginLive, Text: "Continue the investigation."}
	if crashAgain {
		in.Text = "SLEEP"
	}
	row, _, err := st.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := st.StartTurn(conv, core.RoleOwner, owner, in.MsgID, []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	const id = "fa112233"
	if err := st.BindTurn(turn, conv, owner, "omp", id); err != nil {
		t.Fatal(err)
	}
	in.MsgID, in.Text = "steer", "Use the corrected source."
	steer, _, err := st.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Steering(turn, steer.ID); err != nil {
		t.Fatal(err)
	}
	st.Close()

	dir := filepath.Join(os.Getenv("HOME"), ".local", "state", "heimdall", "sessions", "omp", id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fake-session"), []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "session.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(map[string]any{"type": "message", "timestamp": time.Now(), "message": map[string]any{"role": "user"}}); err != nil {
		t.Fatal(err)
	}
	if tool {
		call := map[string]any{"type": "toolCall", "id": "call1", "name": "write", "arguments": map[string]string{"path": "notes.txt", "content": "tool arguments stay private"}}
		if err := enc.Encode(map[string]any{"type": "message", "timestamp": time.Now(), "message": map[string]any{"role": "assistant", "content": []any{call}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	e.start()
	out := e.reply()
	if out.Conv != conv || out.ReplyTo != "original" {
		t.Fatalf("notice target: %+v", out)
	}
	if tool {
		want(t, out, "partly run: write", "Verify before continuing")
		if strings.Contains(out.Text, "notes.txt") {
			t.Fatalf("private locator in shared notice: %q", out.Text)
		}
	} else {
		want(t, out, "No tool calls without results")
	}
	want(t, e.reply(), "may or may not have reached it")
	if crashAgain {
		for attempt := 1; attempt < store.ChatRestartLimit; attempt++ {
			e.waitInputs(attempt)
			outcome, _, err := e.st.InboundOutcome(conv, "original")
			if err != nil || outcome.Turn != turn || outcome.TurnState != store.TurnRunning {
				t.Fatalf("after restart %d: %+v %v", attempt, outcome, err)
			}
			e.waitRecorded()
			e.stop()
			e.start()
			out = e.reply()
			if attempt+1 < store.ChatRestartLimit {
				want(t, out, "Heimdall restarted")
			}
		}
		want(t, out, "stopped recovery after 3 interruptions")
		e.noReply(100 * time.Millisecond)
		outcome, _, err := e.st.InboundOutcome(conv, "original")
		if err != nil || outcome.Turn != turn || outcome.TurnState != store.TurnInterrupted {
			t.Fatalf("after cap: %+v %v", outcome, err)
		}
		if in := fakeharness.Inputs(e.state); len(in) != store.ChatRestartLimit-1 {
			t.Fatalf("native inputs after cap: %+v", in)
		}
		return
	}
	out = e.reply()
	want(t, out, "omp#2 resumed=true", "Heimdall recovered this interrupted turn", "unconfirmed delivery, for context only", "Use the corrected source.")
	if out.Conv != conv || out.ReplyTo != "original" || out.TurnID != turn {
		t.Fatalf("answer target or turn: %+v", out)
	}
	outcome, _, err := e.st.InboundOutcome(conv, "original")
	if err != nil || outcome.Turn != turn || outcome.TurnState != store.TurnDone {
		t.Fatalf("recovered outcome: %+v %v", outcome, err)
	}
	if session, clean, err := e.st.Session(conv, owner, "omp"); err != nil || session != id || !clean {
		t.Fatalf("recovered session: %s %v %v", session, clean, err)
	}
	inputs := fakeharness.Inputs(e.state)
	if len(inputs) != 1 || inputs[0].Kind != "prompt" {
		t.Fatalf("wanted one recovery prompt: %+v", inputs)
	}
	input := inputs[0].Text
	for _, rule := range []string{"recorded result is completed work", "check the real effect before retrying", "Never blindly repeat", "report the result and stop", "untrusted context"} {
		if !strings.Contains(input, rule) {
			t.Errorf("recovery prompt missing %q", rule)
		}
	}
	if tool && (!strings.Contains(input, "write (path=notes.txt)") || strings.Contains(input, "tool arguments stay private") || strings.Contains(out.Text, "notes.txt")) {
		t.Fatalf("in-doubt argument handling: input=%q answer=%q", input, out.Text)
	}
	e.noReply(100 * time.Millisecond)
}
