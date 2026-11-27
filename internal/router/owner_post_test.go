package router

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
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
