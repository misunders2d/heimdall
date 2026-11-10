package slack

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
)

// Files a Claude or AGY turn leaves in its outbox reach the turn's own
// channel thread through the external upload flow, with the harness's file
// name, a MIME from that name and the exact bytes, and no local path in any
// upload call; another caller's later turn gets no file of it.
func TestSlackOutboxFilesReachTheTurnsThread(t *testing.T) {
	for _, h := range []string{"claude", "agy"} {
		t.Run(h, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("MEMENTO_PROCESSING", "1")
			f := newSlackFake(t)
			s, st := testSlack(t, f)
			bin, _ := fakeharness.Install(t, h)
			u, err := user.Current()
			if err != nil {
				t.Fatal(err)
			}
			outbox := s.opt.DataDir + "/outbox"
			r := router.New(router.Options{EmployeeHarnesses: []string{h}, Access: core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"slack:uowner@example.test": true}}, Order: []string{h}, MaxParallel: 2, Accounts: sameUser{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}}, Log: log.New(io.Discard, "", 0), OutboxDir: outbox}, st, s)
			cancel, done := startRouter(t, r)
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-f.connected:
			case <-time.After(3 * time.Second):
				t.Fatal("socket did not start")
			}
			ask := f.event("C1", "channel", "Uowner", "<@Ubot> make WRITE=chart.png:PNGBYTES WRITE=notes.txt:hello-notes", "1000.000099")
			f.push(ask)
			waitFor(t, "both uploads completed", func() bool { return len(f.all("files.completeUploadExternal")) == 2 })
			var got []string
			grants, completes := f.all("files.getUploadURLExternal"), f.all("files.completeUploadExternal")
			f.mu.Lock()
			uploads := append([]string(nil), f.uploads...)
			f.mu.Unlock()
			for i := range completes {
				got = append(got, fmt.Sprintf("%v/%v %s %v/%v %v", grants[i].Body["filename"], grants[i].Body["length"], uploads[i], completes[i].Body["channel_id"], completes[i].Body["thread_ts"], completes[i].Body["files"]))
			}
			want := []string{
				"chart.png/8 image/png=PNGBYTES C1/1000.000099 [map[id:Fout title:chart.png]]",
				"notes.txt/11 text/plain=hello-notes C1/1000.000099 [map[id:Fout title:notes.txt]]",
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("uploads:\n%s", strings.Join(got, "\n"))
			}
			for _, c := range append(grants, completes...) {
				if b := fmt.Sprint(c.Body); strings.Contains(b, harness.TurnsDir) || strings.Contains(b, outbox) {
					t.Fatalf("upload call leaks a local path: %s", b)
				}
			}
			waitFor(t, "sent copies removed", func() bool { entries, _ := os.ReadDir(outbox); return len(entries) == 0 })

			// Another caller's later private turn (for AGY, refused: employee
			// AGY is unavailable) leaves no file: the first turn's never reach it.
			f.push(f.event("D2", "im", "Uemployee", "plain question", ""))
			waitFor(t, "second caller's answer", func() bool {
				for _, c := range f.all("chat.postMessage") {
					if text := str(c.Body["text"]); str(c.Body["channel"]) == "D2" && (strings.Contains(text, "plain question") || strings.Contains(text, "No harness could take this turn")) {
						return true
					}
				}
				return false
			})
			waitFor(t, "router idle", r.Idle)
			if n := len(f.all("files.completeUploadExternal")); n != 2 {
				t.Fatalf("a file was sent to another caller's turn: %d uploads", n)
			}
		})
	}
}

// A file's .caption sidecar becomes the upload's initial comment in the
// turn's thread, formatted as a normal Slack reply; the sidecar is never
// uploaded itself.
func TestSlackOutboxCaptionIsInitialComment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	f := newSlackFake(t)
	s, st := testSlack(t, f)
	bin, _ := fakeharness.Install(t, "claude")
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	outbox := s.opt.DataDir + "/outbox"
	r := router.New(router.Options{Access: core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"slack:uowner@example.test": true}}, Order: []string{"claude"}, MaxParallel: 2, Accounts: sameUser{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}}, Log: log.New(io.Discard, "", 0), OutboxDir: outbox}, st, s)
	cancel, done := startRouter(t, r)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-f.connected:
	case <-time.After(3 * time.Second):
		t.Fatal("socket did not start")
	}
	caption := "**Draft** post <@U1> ||hidden||"
	f.push(f.event("C1", "channel", "Uowner", "<@Ubot> make WRITE=post.gif:GIF89a WRITE64=post.gif.caption:"+base64.StdEncoding.EncodeToString([]byte(caption)), "1000.000099"))
	waitFor(t, "upload completed", func() bool { return len(f.all("files.completeUploadExternal")) == 1 })
	waitFor(t, "router idle", r.Idle)
	grants, completes := f.all("files.getUploadURLExternal"), f.all("files.completeUploadExternal")
	if len(grants) != 1 || len(completes) != 1 || grants[0].Body["filename"] != "post.gif" {
		t.Fatalf("uploads: %v %v", grants, completes)
	}
	c := completes[0].Body
	if c["channel_id"] != "C1" || c["thread_ts"] != "1000.000099" || c["initial_comment"] != mrkdwn(caption) || !strings.Contains(str(c["initial_comment"]), "&lt;@U1&gt;") {
		t.Fatalf("complete: %v (want comment %q)", c, mrkdwn(caption))
	}
}
