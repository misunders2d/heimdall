package store

import (
	"path/filepath"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

func TestChatRecoveryClaimSurvivesCrashBeforeLaunchAndStopsAtCap(t *testing.T) {
	db := filepath.Join(t.TempDir(), "recovery.db")
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	turn := recordChatCutoff(t, s, core.RoleOwner, core.OriginLive, false, false)
	for count := 1; count <= ChatRestartLimit+1; count++ {
		rec, err := s.Recover(chatTestNotice, nil, func(r *ChatResume) (core.Outbound, bool) {
			return chatTestNotice(r.Interrupted), true
		})
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case count < ChatRestartLimit:
			if len(rec.Chat) != 1 || rec.Chat[0].Turn != turn || rec.Chat[0].Session != "aa11" || rec.Chat[0].Cutoffs != count {
				t.Fatalf("cutoff %d: %+v", count, rec)
			}
		case count == ChatRestartLimit:
			if len(rec.Chat) != 0 || len(rec.Turns) != 1 || rec.Turns[0].Turn != turn || rec.Turns[0].Cutoffs != count {
				t.Fatalf("cap cutoff: %+v", rec)
			}
		default:
			if len(rec.Chat)+len(rec.Turns) != 0 {
				t.Fatalf("terminal turn recovered again: %+v", rec)
			}
		}
		var rows, got int
		if err := s.db.QueryRow("SELECT count(*), max(restart_count) FROM turns").Scan(&rows, &got); err != nil || rows != 1 || got != min(count, ChatRestartLimit) {
			t.Fatalf("persisted claim: rows=%d cutoff=%d err=%v", rows, got, err)
		}
		s.Close() // New router after a crash before any call can be made.
		s, err = Open(db)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
}

func TestChatRecoveryExcludesWakeScheduledEmployeeAndStoppedTurns(t *testing.T) {
	for _, tc := range []struct {
		name, origin string
		role         core.Role
		control      bool
		stopped      bool
	}{
		{"wake", core.OriginCommitment, core.RoleOwner, false, false},
		{"scheduled", core.OriginScheduled, core.RoleOwner, false, false},
		{"employee", core.OriginLive, core.RoleEmployee, false, false},
		{"control", core.OriginLive, core.RoleOwner, true, false},
		{"stopped", core.OriginLive, core.RoleOwner, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t)
			turn := recordChatCutoff(t, s, tc.role, tc.origin, tc.control, tc.stopped)
			rec, err := s.Recover(chatTestNotice, nil, func(*ChatResume) (core.Outbound, bool) {
				t.Fatal("excluded turn reached engine preflight")
				return core.Outbound{}, false
			})
			if err != nil || len(rec.Chat) != 0 || len(rec.Turns) != 1 || rec.Turns[0].Turn != turn || rec.Turns[0].Cutoffs != 0 {
				t.Fatalf("excluded recovery: %+v %v", rec, err)
			}
			if _, clean, err := s.Session(chatCutoffConv(tc.origin), "x:u", "claude"); err != nil || clean {
				t.Fatalf("excluded session became clean: %v %v", clean, err)
			}
		})
	}
}

func recordChatCutoff(t *testing.T, s *Store, role core.Role, origin string, control, stop bool) int64 {
	t.Helper()
	in := msg("1")
	in.Conv, in.Origin, in.MCPControl = chatCutoffConv(origin), origin, control
	if origin == core.OriginCommitment {
		in.Review = &core.Review{Issue: "MEL-1", Key: in.MsgID, Target: conv, Work: true}
	}
	if origin == core.OriginScheduled {
		in.Run = &core.ScheduledRun{Job: "job"}
	}
	row, _, err := s.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.StartTurn(in.Conv, role, "x:u", in.MsgID, []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindTurn(id, in.Conv, "x:u", "claude", "aa11"); err != nil {
		t.Fatal(err)
	}
	if stop {
		if err := s.FenceChatResume(id); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func chatCutoffConv(origin string) core.ConvKey {
	key := conv
	switch origin {
	case core.OriginCommitment:
		key.Chat = core.ReviewChat("MEL-1")
	case core.OriginScheduled:
		key.Chat = core.SchedChat("job")
	}
	return key
}

func chatTestNotice(t Interrupted) core.Outbound {
	return core.Outbound{Conv: t.Conv, ReplyTo: t.ReplyTo, Text: "interrupted"}
}
