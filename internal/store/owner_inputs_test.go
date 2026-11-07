package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func failedInput(t *testing.T, s *Store, in core.Inbound, role core.Role, state string) int64 {
	t.Helper()
	row, _, err := s.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.StartTurn(in.Conv, role, in.From.Key(), in.MsgID, []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	itemState := InFailed
	if state == TurnInterrupted {
		itemState = InInterrupted
	}
	if _, err := s.EndTurn(id, in.Conv, in.From.Key(), TurnEnd{State: state, ItemState: itemState}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestFailedOwnerInputChain(t *testing.T) {
	for _, boundary := range []string{TurnDone, TurnCancelled, TurnNeedsHuman, "reset", "cancel", "stop"} {
		t.Run(boundary, func(t *testing.T) {
			s := open(t)
			failedInput(t, s, msg("old"), core.RoleOwner, TurnFailed)
			if boundary == "reset" || boundary == "cancel" || boundary == "stop" {
				in := msg("control")
				in.Text = "!" + boundary
				row, _, _ := s.AddInbound(in)
				if err := s.AnswerCommand(row.ID, conv, CommandEnd{Detail: boundary}); err != nil {
					t.Fatal(err)
				}
			} else {
				failedInput(t, s, msg("boundary"), core.RoleOwner, boundary)
			}
			failedInput(t, s, msg("first"), core.RoleOwner, TurnFailed)
			id := failedInput(t, s, msg("second"), core.RoleOwner, TurnInterrupted)
			got, err := s.FailedOwnerInputs(conv, "x:u", id+1)
			if err != nil || len(got) != 2 || got[0].MessageID != "second" || got[1].Text != "hi first" || got[0].At.IsZero() {
				t.Fatalf("inputs=%+v err=%v", got, err)
			}
		})
	}
}

func TestFailedOwnerInputIsolation(t *testing.T) {
	for _, kind := range []string{"caller", "chat", "thread", "interface", "employee", "scheduled", "mcp", "run", "interface reply", "unconfirmed", "unbound", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			s := open(t)
			in, role := msg("excluded"), core.RoleOwner
			switch kind {
			case "caller":
				in.From.ID = "other"
			case "chat":
				in.Conv.Chat = "other"
			case "thread":
				in.Conv.Thread = "other"
			case "interface":
				in.Conv.Interface = "other"
			case "employee":
				role = core.RoleEmployee
			case "scheduled":
				in.Origin = core.OriginScheduled
			case "mcp":
				in.MCPControl = true
			case "run":
				in.Run = &core.ScheduledRun{Job: "j"}
			case "interface reply":
				in.InterfaceReply = "warning"
			case "oversized":
				in.Text = strings.Repeat("x", OwnerInputBytes+1)
			}
			id := failedInput(t, s, in, role, TurnFailed)
			if kind == "unconfirmed" || kind == "unbound" {
				if _, err := s.db.Exec(`UPDATE inbound SET state=?,turn_id=? WHERE turn_id=?`, InUnconfirmed, map[bool]any{true: nil, false: id}[kind == "unbound"], id); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.FailedOwnerInputs(conv, "x:u", id+1)
			if err != nil || len(got) != 0 {
				t.Fatalf("inputs=%+v err=%v", got, err)
			}
		})
	}
}

func TestFailedOwnerInputCount(t *testing.T) {
	s := open(t)
	var id int64
	for i := 0; i < OwnerInputLimit+5; i++ {
		in := msg(fmt.Sprint(i))
		in.Text = fmt.Sprintf("exact %d\nDo not publish.  ", i)
		in.Context = "assistant/tool output must not appear"
		id = failedInput(t, s, in, core.RoleOwner, TurnFailed)
	}
	got, err := s.FailedOwnerInputs(conv, "x:u", id+1)
	if err != nil || len(got) != OwnerInputLimit || got[len(got)-1].MessageID != "5" {
		t.Fatalf("inputs=%+v err=%v", got, err)
	}
	if got[0].Text != fmt.Sprintf("exact %d\nDo not publish.  ", OwnerInputLimit+4) {
		t.Fatal("text changed")
	}
}
