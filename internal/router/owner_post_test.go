package router

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// refusesOwnerPosts is the fake interface with Slack refusing every post
// as the owner for good.
type refusesOwnerPosts struct{ *fakeIface }

func (f refusesOwnerPosts) Send(ctx context.Context, out core.Outbound) error {
	if out.OwnerPost != nil {
		return PermanentError{Err: errors.New("slack API: not_in_channel")}
	}
	return f.fakeIface.Send(ctx, out)
}

// A post as the owner that fails is reported once, by a fixed notice in
// the owner's private chat with the bot, never at the post's target; its
// file copies are removed.
func TestFailedOwnerPostIsReportedPrivately(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.wrap = func(f *fakeIface) Interface { return refusesOwnerPosts{f} }
	e.start()
	copyPath := filepath.Join(t.TempDir(), "copy")
	if err := os.WriteFile(copyPath, []byte("synthetic"), 0o600); err != nil {
		t.Fatal(err)
	}
	notice := core.ConvKey{Interface: "fake", Chat: "owner-dm"}
	target := core.ConvKey{Interface: "fake", Chat: "team"}
	if _, err := e.st.Enqueue(core.Outbound{Conv: target, Text: "the post", Formatted: true, OwnerPost: &core.OwnerPost{ID: "pabc", Label: "#team", Notice: notice, Files: []core.File{{Path: copyPath, Name: "a.pdf"}}}}); err != nil {
		t.Fatal(err)
	}
	e.r.Wake("fake")
	got := e.reply()
	if got.Conv != notice || got.OwnerPost != nil || !strings.Contains(got.Text, "Your post as you to #team was not sent") || !strings.HasSuffix(got.Text, "— heimdall") {
		t.Fatalf("failure notice: %+v", got)
	}
	if _, err := os.Stat(copyPath); !os.IsNotExist(err) {
		t.Fatal("file copy kept after a permanent failure")
	}
}

// reportsRefs is the fake interface reporting a platform message ID.
type reportsRefs struct{ *fakeIface }

func (f reportsRefs) Send(ctx context.Context, out core.Outbound) error {
	out.Report("41")
	out.Report("42")
	return f.fakeIface.Send(ctx, out)
}

// A sent card records the last delivered part's platform message ID.
func TestSentCardRecordsItsPlatformMessageID(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp"}, "omp")
	e.stop()
	stopOnce.Delete(e.st)
	e.wrap = func(f *fakeIface) Interface { return reportsRefs{f} }
	e.start()
	card := core.ConvKey{Interface: "fake", Chat: "owner-dm"}
	p := store.OwnerPost{ID: "pref", Creator: "fake:owner", Card: card, Target: core.ConvKey{Interface: "slack", Chat: "C1"}, Label: "#team", Text: "x", Origin: "test",
		State: store.OwnerPostPending, Expires: time.Now().Add(time.Hour), CardMessage: &core.Outbound{Conv: card, Text: "card"}}
	if err := e.st.AnswerCommand(0, core.ConvKey{}, store.CommandEnd{Detail: "job", Job: &store.JobChange{NewOwnerPost: &p}}); err != nil {
		t.Fatal(err)
	}
	e.r.Wake("fake")
	e.reply()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _, err := e.st.OwnerPostByID("pref")
		if err == nil && got.CardRef == "42" && got.CardMark >= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("card ref not recorded: %+v %v", got, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A post's file copies stay until its outcome is recorded: a crash before
// that resends the row, whose retry may still need them.
func TestOwnerPostCopiesKeptUntilSentIsRecorded(t *testing.T) {
	e, db := answerEnv(t, 5)
	copyPath := filepath.Join(t.TempDir(), "copy")
	if err := os.WriteFile(copyPath, []byte("synthetic"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec(t, db, `CREATE TRIGGER no_sent BEFORE UPDATE OF state ON outbound WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END`)
	id, err := e.st.Enqueue(core.Outbound{Conv: core.ConvKey{Interface: "fake", Chat: "team"}, Text: "the post", OwnerPost: &core.OwnerPost{ID: "pkeep", Label: "#team", Files: []core.File{{Path: copyPath, Name: "a.pdf"}}}})
	if err != nil {
		t.Fatal(err)
	}
	e.r.Wake("fake")
	e.reply()
	time.Sleep(300 * time.Millisecond) // the sent record keeps failing
	if _, err := os.Stat(copyPath); err != nil {
		t.Fatal("file copy removed before the post was recorded sent", err)
	}
	exec(t, db, `DROP TRIGGER no_sent`)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var state string
		_ = db.QueryRow(`SELECT state FROM outbound WHERE id=?`, id).Scan(&state)
		if _, err := os.Stat(copyPath); state == store.OutSent && os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state %q, copy not removed after the sent record", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
