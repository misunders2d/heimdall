package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

// ownerPostRig is a ready Slack with the owner's verified user token
// (UOWNER, "test-work-user") and the owner's chat with the bot (Dbot).
type ownerPostRig struct {
	s         *Slack
	st        *store.Store
	f         *slackFake
	uploads   int
	completes int
	// ambiguous: how the next completes answer: "lands" (shared, then HTTP
	// 500) or "lost" (not shared, HTTP 500); "" answers ok.
	ambiguous []string
}

func newOwnerPostRig(t *testing.T) *ownerPostRig {
	f := newSlackFake(t)
	f.allowWorkUser = true
	for _, id := range []string{"UOWNER", "UCOL"} {
		u := User{ID: id, TeamID: "Ttest", Name: id}
		u.Profile.Email = strings.ToLower(id) + "@example.test"
		u.Profile.DisplayName = id + " Example"
		f.users[id] = u
	}
	s, st := readySlack(t, f)
	s.opt.WorkOwnerID = "UOWNER"
	s.workUser = newAPI(f.srv.URL, "test-work-user", nil)
	s.workUser.pause = s.bot.pause
	s.ownerBotDM = "Dbot"
	r := &ownerPostRig{s: s, st: st, f: f}
	f.setHook(func(w http.ResponseWriter, req *http.Request, args map[string]any) bool {
		switch req.URL.Path {
		case "/conversations.open":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": map[string]string{"id": "DCOL1"}})
			return true
		case "/files.getUploadURLExternal":
			f.mu.Lock()
			r.uploads++
			n := r.uploads
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "upload_url": f.srv.URL + "/upload", "file_id": fmt.Sprintf("F%d", n)})
			return true
		case "/files.completeUploadExternal":
			f.mu.Lock()
			r.completes++
			mode := ""
			if len(r.ambiguous) > 0 {
				mode, r.ambiguous = r.ambiguous[0], r.ambiguous[1:]
			}
			if mode != "lost" {
				var files []map[string]any
				list, _ := args["files"].([]any)
				for _, item := range list {
					m, _ := item.(map[string]any)
					files = append(files, map[string]any{"id": m["id"]})
				}
				f.seq++
				message := map[string]any{"channel": args["channel_id"], "ts": fmt.Sprintf("1000.%06d", f.seq), "user": "UOWNER", "text": args["initial_comment"], "files": files}
				if args["thread_ts"] != nil {
					message["thread_ts"] = args["thread_ts"]
				}
				f.messages = append(f.messages, message)
			}
			f.mu.Unlock()
			if mode != "" {
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
			return true
		}
		return false
	})
	return r
}

// post stores an approved post with files and returns its delivery row.
func (r *ownerPostRig) post(t *testing.T, conv core.ConvKey, text string, files int) core.Outbound {
	t.Helper()
	var list []core.File
	for i := range files {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("copy-%d", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("synthetic file %d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		list = append(list, core.File{Path: path, Name: fmt.Sprintf("report-%d.pdf", i), MIME: "application/pdf", Size: 16})
	}
	id := fmt.Sprintf("p%d", time.Now().UnixNano())
	p := store.OwnerPost{ID: id, Creator: "slack:uowner@example.test", Card: core.ConvKey{Interface: "slack", Chat: "Dbot", Thread: "1.1"}, Target: conv, Label: conv.Chat, Text: text, Files: list, Origin: "test", State: store.OwnerPostApproved, Expires: time.Now().Add(time.Hour)}
	if err := r.st.AnswerCommand(0, core.ConvKey{}, store.CommandEnd{Detail: "job", Job: &store.JobChange{NewOwnerPost: &p}}); err != nil {
		t.Fatal(err)
	}
	return core.Outbound{ID: 7, Conv: conv, Text: text, Formatted: true, CreatedAt: time.Now(), OwnerPost: &core.OwnerPost{ID: id, Label: conv.Chat, Files: list, Notice: p.Card}}
}

// botPosts are calls that would put something in Slack as the bot.
func (r *ownerPostRig) botPosts() []apiCall {
	var out []apiCall
	for _, m := range []string{"chat.postMessage", "chat.update", "files.getUploadURLExternal", "files.completeUploadExternal", "conversations.open"} {
		for _, c := range r.f.all(m) {
			if c.Token != "test-work-user" {
				out = append(out, c)
			}
		}
	}
	return out
}

func TestOwnerPostGoesOutWithTheOwnersTokenOnly(t *testing.T) {
	r := newOwnerPostRig(t)
	ctx := context.Background()
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C1", Thread: "1700000000.000100"}, "Q3 numbers attached", 2)
	if err := r.s.Send(ctx, out); err != nil {
		t.Fatal(err)
	}
	if bot := r.botPosts(); len(bot) != 0 {
		t.Fatal("bot token used for a post as the owner", bot)
	}
	if r.uploads != 2 || r.completes != 1 || len(r.f.uploads) != 2 {
		t.Fatalf("want 2 uploads in one completed message, got %d/%d/%d", r.uploads, r.completes, len(r.f.uploads))
	}
	complete := r.f.all("files.completeUploadExternal")[0].Body
	if complete["channel_id"] != "C1" || complete["thread_ts"] != "1700000000.000100" || complete["initial_comment"] != nil {
		t.Fatal("files not completed in the thread without an untagged comment", complete)
	}
	posts := r.f.all("chat.postMessage")
	if len(posts) != 1 || !strings.Contains(str(posts[0].Body["text"]), "Q3 numbers attached") {
		t.Fatal("text not posted as one tagged message", posts)
	}
	if meta, _ := posts[0].Body["metadata"].(map[string]any); meta["event_type"] != ownerPostEvent {
		t.Fatal("text part without its delivery tag", posts[0].Body)
	}
	// A later attempt (a crash after Slack shared it) checks Slack first.
	out.Attempt = 1
	if err := r.s.Send(ctx, out); err != nil || r.completes != 1 || r.uploads != 2 || len(r.f.all("chat.postMessage")) != 1 {
		t.Fatalf("resent after the post was shared: %v %d %d", err, r.completes, r.uploads)
	}
}

func TestOwnerPostFilesAreCheckedBeforeAnyResend(t *testing.T) {
	r := newOwnerPostRig(t)
	ctx := context.Background()
	// Ambiguous, but it landed: the settle check finds it; nothing again.
	r.ambiguous = []string{"lands"}
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C2"}, "", 1)
	if err := r.s.Send(ctx, out); err != nil || r.completes != 1 {
		t.Fatalf("landed ambiguous upload: %v, completes %d", err, r.completes)
	}
	// Ambiguous and lost: the attempt fails; the retry checks, then
	// completes the same recorded upload once (no new upload).
	r2 := newOwnerPostRig(t)
	r2.ambiguous = []string{"lost"}
	out = r2.post(t, core.ConvKey{Interface: "slack", Chat: "C3"}, "", 1)
	if err := r2.s.Send(ctx, out); err == nil {
		t.Fatal("unconfirmed upload reported as sent")
	}
	ids, _ := r2.st.OwnerPostUploads(out.OwnerPost.ID)
	if len(ids) != 1 {
		t.Fatal("file IDs not recorded before completing", ids)
	}
	out.Attempt = 1
	if err := r2.s.Send(ctx, out); err != nil || r2.completes != 2 || r2.uploads != 1 {
		t.Fatalf("retry: %v, completes %d, uploads %d", err, r2.completes, r2.uploads)
	}
	if len(r2.f.all("conversations.history")) == 0 {
		t.Fatal("retry completed without checking Slack first")
	}
	// When the check itself fails, nothing is completed again.
	r2.f.setHook(func(w http.ResponseWriter, req *http.Request, _ map[string]any) bool {
		if req.URL.Path == "/conversations.history" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "missing_scope"})
			return true
		}
		if req.URL.Path == "/files.completeUploadExternal" {
			t.Error("completed again without a successful check")
		}
		return false
	})
	if err := r2.s.Send(ctx, out); err == nil {
		t.Fatal("failed check reported as sent")
	}
	if bot := r2.botPosts(); len(bot) != 0 {
		t.Fatal("bot token used", bot)
	}
}

func TestOwnerPostLongTextIsReconciledByItsParts(t *testing.T) {
	r := newOwnerPostRig(t)
	ctx := context.Background()
	long := strings.Repeat("A long owner update line.\n", 200)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C4"}, long, 1)
	if err := r.s.Send(ctx, out); err != nil {
		t.Fatal(err)
	}
	posts := r.f.all("chat.postMessage")
	if len(posts) < 2 || r.completes != 1 {
		t.Fatalf("long text: %d parts, %d completes", len(posts), r.completes)
	}
	for _, p := range posts {
		meta, _ := p.Body["metadata"].(map[string]any)
		if p.Token != "test-work-user" || meta["event_type"] != ownerPostEvent {
			t.Fatal("part not posted as the owner with its delivery key", p)
		}
	}
	out.Attempt = 1
	if err := r.s.Send(ctx, out); err != nil || len(r.f.all("chat.postMessage")) != len(posts) || r.completes != 1 {
		t.Fatal("parts or files resent after they were delivered", err)
	}
}

func TestOwnerPostTargetsAndRefusals(t *testing.T) {
	r := newOwnerPostRig(t)
	ctx := context.Background()
	for _, target := range []string{"slack:Dbot", "Ubot", "slack:Ubot", "slack:C1/not-a-ts", "#general", "slack:C1/1700000000.000100/x"} {
		if _, _, err := r.s.OwnerPostTarget(ctx, target); err == nil {
			t.Errorf("%s admitted", target)
		}
	}
	k, label, err := r.s.OwnerPostTarget(ctx, "UCOL")
	if err != nil || k.Chat != "DCOL1" || !strings.Contains(label, "UCOL Example") {
		t.Fatal(k, label, err)
	}
	if c := r.f.all("conversations.open"); len(c) != 1 || c[0].Token != "test-work-user" {
		t.Fatal("DM not opened with the owner's token", c)
	}
	if k, _, err := r.s.OwnerPostTarget(ctx, "slack:C0TEAM1/1700000000.000100"); err != nil || k.Chat != "C0TEAM1" || k.Thread != "1700000000.000100" {
		t.Fatal(k, err)
	}
	if _, err := r.s.OwnerPostText("yes <@Ubot>"); err == nil {
		t.Fatal("bot mention admitted")
	}
	if p, err := r.s.OwnerPostOwner(ctx); err != nil || p.SlackUserID != "UOWNER" {
		t.Fatal(p, err)
	}
	// Unknown bot chat: no DM can be told apart from it.
	r.s.ownerBotDM = ""
	if _, _, err := r.s.OwnerPostTarget(ctx, "slack:D123"); err == nil {
		t.Fatal("DM admitted while the bot chat is unknown")
	}
	// No verified owner token: never the bot.
	r.s.ownerBotDM, r.s.workUser = "Dbot", nil
	before := len(r.f.calls)
	err = r.s.Send(ctx, core.Outbound{ID: 9, Conv: core.ConvKey{Interface: "slack", Chat: "C1"}, Text: "x", OwnerPost: &core.OwnerPost{ID: "p"}})
	var perm router.PermanentError
	if !errors.As(err, &perm) || len(r.f.calls) != before {
		t.Fatal("post without the owner's token was attempted", err)
	}
	if _, _, err := r.s.OwnerPostTarget(ctx, "slack:C0TEAM1"); err == nil {
		t.Fatal("target admitted without the owner's token")
	}
}

// A post Heimdall made as the owner never re-enters as the owner's input.
func TestOwnerPostEchoIsNotOwnerInput(t *testing.T) {
	r := newOwnerPostRig(t)
	echo := func(meta string) json.RawMessage {
		m := map[string]any{"type": "message", "channel": "Dbot", "channel_type": "im", "user": "UOWNER", "text": "yes", "ts": "1000.1"}
		if meta != "" {
			m["metadata"] = map[string]any{"event_type": meta, "event_payload": map[string]string{"delivery_id": "post-1"}}
		}
		raw, _ := json.Marshal(map[string]any{"event": m, "authorizations": []map[string]any{{"team_id": "Ttest", "user_id": "UOWNER", "is_bot": false}}})
		return raw
	}
	if ok, _, _ := r.s.eventAdmission(echo(ownerPostEvent)); ok {
		t.Fatal("owner post echo admitted as owner input")
	}
	if ok, _, _ := r.s.eventAdmission(echo("")); !ok {
		t.Fatal("the owner's own message in the bot chat refused")
	}
}
