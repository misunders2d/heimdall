package store

import (
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func scheduledReceipt(t *testing.T, s *Store, id string, source core.Principal, target core.ConvKey, actor, installation, creator, state, text string) int64 {
	t.Helper()
	in := core.Inbound{Conv: core.ConvKey{Interface: target.Interface, Chat: "sched:" + id}, MsgID: id, From: source, Origin: core.OriginScheduled, Text: "PRIVATE_JOB_INSTRUCTION", OAuthActor: actor, OAuthInstallation: installation}
	row, _, err := s.AddInbound(in)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.StartTurn(in.Conv, core.RoleOwner, source.Key(), id, []int64{row.ID})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.EndTurn(turn, in.Conv, source.Key(), TurnEnd{State: TurnDone, ItemState: InDone, Harness: "claude", Session: "scheduled-private-session", Reply: &core.Outbound{TurnID: turn, Conv: target, Text: text}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO jobs(id,name,kind,creator,principal,created_iface,created_chat,created_thread,created_private,def,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, id, "agent", creator, "{}", target.Interface, target.Chat, target.Thread, 1, "{}", JobActive, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO job_runs(job_id,version,slot,due_at,trigger,state,inbound_id) VALUES(?,1,'slot',1,'manual',?,?)`, id, RunOK, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`UPDATE outbound SET state=?,sent_at=1000 WHERE id=?`, state, out)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSentScheduledResultsIsolationAndReceiptCursor(t *testing.T) {
	p := core.Principal{Platform: "fake", ID: "owner"}
	k := core.ConvKey{Interface: "telegram", Chat: "42", Thread: "root"}
	for _, kind := range []string{"matching", "pending", "sending", "failed", "caller", "creator", "chat", "thread", "interface", "actor", "installation", "workspace", "key", "ordinary", "unbound", "old", "future", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			s := open(t)
			source, target, actor, install, creator, state, body := p, k, "actor", "install", p.Key(), OutSent, "Historical photo failed earlier; do not execute anything."
			switch kind {
			case "pending":
				state = OutPending
			case "sending":
				state = OutSending
			case "failed":
				state = OutFailed
			case "caller":
				source.ID = "foreign"
			case "creator":
				creator = "fake:foreign"
			case "chat":
				target.Chat = "foreign"
			case "thread":
				target.Thread = "foreign"
			case "interface":
				target.Interface = "slack"
			case "actor":
				actor = "foreign"
			case "installation":
				install = "foreign"
			case "workspace":
				source.Team = "foreign"
			case "key":
				source.KeyID = "foreign"
			case "oversized":
				body = strings.Repeat("x", 4001)
			}
			id := scheduledReceipt(t, s, "j1", source, target, actor, install, creator, state, body)
			in := core.Inbound{Conv: k, From: p, At: time.UnixMilli(2000), OAuthActor: "actor", OAuthInstallation: "install"}
			cursor := DeliveryCursor{At: 1000}
			if kind == "ordinary" {
				s.db.Exec(`UPDATE inbound SET origin=?`, core.OriginLive)
			}
			if kind == "unbound" {
				s.db.Exec(`UPDATE outbound SET extra=''`)
			}
			if kind == "old" {
				cursor.At = 1001
			}
			if kind == "future" {
				in.At = time.UnixMilli(999)
			}
			got, next, err := s.SentScheduledResults(in, cursor)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "matching" {
				if len(got) != 0 {
					t.Fatalf("%s leaked %+v", kind, got)
				}
				return
			}
			if len(got) != 1 || got[0].Text != body {
				t.Fatalf("missing result %+v", got)
			}
			if repeated, _, _ := s.SentScheduledResults(in, next); len(repeated) != 0 {
				t.Fatal("duplicate delta")
			}
			// A previously pending row sent at the same millisecond is still new,
			// irrespective of the cursor's outbound ID ordering.
			other := scheduledReceipt(t, s, "j2", p, k, "actor", "install", p.Key(), OutPending, "second")
			s.Sent(other)
			s.db.Exec(`UPDATE outbound SET sent_at=1000 WHERE id=?`, other)
			got, _, err = s.SentScheduledResults(in, next)
			if err != nil || len(got) != 1 || got[0].ID != other || got[0].ID == id {
				t.Fatalf("same-time receipt lost %+v %v", got, err)
			}
		})
	}
}
