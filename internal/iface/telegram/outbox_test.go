package telegram

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	osuser "os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

// Files a Claude or AGY turn leaves in its outbox reach the turn's own
// forum topic as replies to the asking message, with the harness's file
// name, a MIME from that name and the exact bytes, and no caption or local
// path; a later caller's turn gets no file of it.
func TestTelegramOutboxFilesReachTheTurnsTopicReply(t *testing.T) {
	for _, h := range []string{"claude", "agy"} {
		t.Run(h, func(t *testing.T) {
			bin, _ := fakeharness.Install(t, h)
			u, err := osuser.Current()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("HOME", t.TempDir())
			t.Setenv("MEMENTO_PROCESSING", "1")
			st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			api := newFakeAPI(t)
			var mu sync.Mutex
			var uploads []map[string]any
			for _, m := range []string{"sendPhoto", "sendDocument"} {
				api.set(m, func(w http.ResponseWriter, b map[string]any) bool {
					mu.Lock()
					b["method"] = m
					uploads = append(uploads, b)
					mu.Unlock()
					ok(w, map[string]any{"message_id": 900 + len(uploads)})
					return true
				})
			}
			var logs syncBuffer
			tg := newIface(t, api.srv.URL, botName, &logs)
			access := core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"telegram:42": true, "telegram:43": true}}
			tg.opt.Access = &access
			base := harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}
			// Cold processes: the fake's warm mode writes no outbox files.
			var accounts router.Accounts = plainAccounts{base}
			outbox := filepath.Join(t.TempDir(), "outbox")
			r := router.New(router.Options{Access: access, Order: []string{h}, MaxParallel: 2, Accounts: accounts, OutboxDir: outbox, Log: log.New(io.Discard, "", 0)}, st, tg)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()

			ask := msg(31, -1002, "supergroup", 42, "@"+botName+" make WRITE=chart.png:PNGBYTES WRITE=notes.txt:hello-notes")
			ask["is_topic_message"], ask["message_thread_id"] = true, 7
			api.push(upd(1, ask))
			eventually(t, "answer and both files", func() bool { mu.Lock(); defer mu.Unlock(); return len(uploads) == 2 && len(api.sentBodies()) >= 1 })
			mu.Lock()
			var got []string
			for _, b := range uploads {
				field := "document"
				if b["method"] == "sendPhoto" {
					field = "photo"
				}
				got = append(got, fmt.Sprintf("%s chat=%v topic=%v reply=%v %v", b["method"], b["chat_id"], b["message_thread_id"], b["reply_parameters"], b[field]))
			}
			mu.Unlock()
			want := []string{
				`sendPhoto chat=-1002 topic=7 reply={"allow_sending_without_reply":true,"message_id":31} map[data:PNGBYTES name:chart.png type:image/png]`,
				`sendDocument chat=-1002 topic=7 reply={"allow_sending_without_reply":true,"message_id":31} map[data:hello-notes name:notes.txt type:text/plain]`,
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("uploads:\n%s", strings.Join(got, "\n"))
			}
			// The answer keeps its prose but never this turn's local paths
			// (the cold fake echoes its prompt head, which names the outbox).
			answer := fmt.Sprint(api.sentBodies()[0]["text"])
			if strings.Contains(answer, harness.TurnsDir) || !strings.Contains(answer, h+"#1") || h == "claude" && !strings.Contains(answer, "save them in out before your final answer") {
				t.Fatalf("answer: %q", answer)
			}

			// The uploads carry only the file: no caption, no local path.
			mu.Lock()
			for _, b := range uploads {
				for k, v := range b {
					if k == "caption" || strings.Contains(fmt.Sprint(v), harness.TurnsDir) || strings.Contains(fmt.Sprint(v), outbox) {
						t.Fatalf("upload leaks %s: %v", k, v)
					}
				}
			}
			mu.Unlock()

			// A path-only answer: the fenced outbox path goes, the file still
			// arrives as a real attachment, labelled "Attachment".
			say := "SAY=" + base64.StdEncoding.EncodeToString([]byte("```\n{OUTBOX}/only.txt\n```"))
			sent := len(api.sentBodies())
			api.push(upd(3, msg(33, 42, "private", 42, "WRITE=only.txt:only-bytes "+say)))
			eventually(t, "path-only answer and file", func() bool { mu.Lock(); defer mu.Unlock(); return len(uploads) == 3 && len(api.sentBodies()) > sent })
			if b := api.sentBodies()[sent]; b["text"] != "Attachment" || b["chat_id"] != float64(42) {
				t.Fatalf("path-only answer: %v", b)
			}
			mu.Lock()
			if u := uploads[2]; u["chat_id"] != "42" || fmt.Sprint(u["document"]) != "map[data:only-bytes name:only.txt type:text/plain]" {
				t.Fatalf("path-only file: %v", u)
			}
			mu.Unlock()

			// Another caller's later turn (private, so no group context the
			// fake could act on) leaves no file: the first turn's files and
			// copies never reach it.
			sent = len(api.sentBodies())
			api.push(upd(4, msg(34, 43, "private", 43, "plain question")))
			eventually(t, "second answer", func() bool { return len(api.sentBodies()) > sent })
			mu.Lock()
			n := len(uploads)
			mu.Unlock()
			if n != 3 {
				t.Fatalf("a file was sent to another caller's turn: %d uploads", n)
			}
			if entries, _ := os.ReadDir(outbox); len(entries) != 0 {
				t.Fatalf("sent copies kept: %v", entries)
			}
		})
	}
}
