package slack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	s.opt.OwnerPost = true
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
		body := []byte(fmt.Sprintf("synthetic file %d", i))
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(body)
		list = append(list, core.File{Path: path, Name: fmt.Sprintf("report-%d.pdf", i), MIME: "application/pdf", Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])})
	}
	id := fmt.Sprintf("p%d", time.Now().UnixNano())
	p := store.OwnerPost{ID: id, Creator: "slack:uowner@example.test", Card: core.ConvKey{Interface: "slack", Chat: "Dbot", Thread: "1.1"}, Target: conv, Label: conv.Chat, Text: text, Files: list, Origin: "test", State: store.OwnerPostApproved, Expires: time.Now().Add(time.Hour), SlackUser: "UOWNER", SlackTeam: "Ttest"}
	if err := r.st.AnswerCommand(0, core.ConvKey{}, store.CommandEnd{Detail: "job", Job: &store.JobChange{NewOwnerPost: &p}}); err != nil {
		t.Fatal(err)
	}
	return core.Outbound{ID: 7, Conv: conv, Text: text, Formatted: true, CreatedAt: time.Now(), OwnerPost: &core.OwnerPost{ID: id, Label: conv.Chat, Files: list, Notice: p.Card, SlackUser: "UOWNER", SlackTeam: "Ttest"}}
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

// The files of a post complete without a comment and carry no metadata;
// their echo, seen through the bot in a channel it is in, is recognized by
// the recorded upload IDs. The owner's own file shares still pass.
func TestOwnerPostFileEchoIsNotOwnerInput(t *testing.T) {
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C2", Thread: "1000.1"}, "", 1)
	if err := r.s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	ids, _ := r.st.OwnerPostUploads(out.OwnerPost.ID)
	if len(ids) != 1 {
		t.Fatal("uploads", ids)
	}
	shareIn := func(channel, thread string, upload bool, text string, files ...string) json.RawMessage {
		fs := []map[string]string{}
		for _, id := range files {
			fs = append(fs, map[string]string{"id": id})
		}
		m := map[string]any{"type": "message", "subtype": "file_share", "channel": channel, "channel_type": "channel",
			"user": "UOWNER", "text": text, "ts": "1000.5", "files": fs, "upload": upload}
		if thread != "" {
			m["thread_ts"] = thread
		}
		raw, _ := json.Marshal(map[string]any{"event": m, "authorizations": []map[string]any{{"team_id": "Ttest", "user_id": r.s.botID, "is_bot": true}}})
		return raw
	}
	share := func(text string, files ...string) json.RawMessage {
		return shareIn("C2", "1000.1", true, text, files...)
	}
	if ok, _, err := r.s.eventAdmission(share("", ids...)); ok || err != nil {
		t.Fatal("owner post file echo admitted", ok, err)
	}
	// Slack's documented upload example carries upload: true and no
	// subtype: the same echo. Without upload it is a share: the owner's.
	noSubtype := func(upload bool) json.RawMessage {
		var p map[string]map[string]any
		_ = json.Unmarshal(share("", ids...), &p)
		delete(p["event"], "subtype")
		p["event"]["upload"] = upload
		raw, _ := json.Marshal(p)
		return raw
	}
	if ok, _, err := r.s.eventAdmission(noSubtype(true)); ok || err != nil {
		t.Fatal("owner post file echo without a subtype admitted", ok, err)
	}
	if ok, _, err := r.s.eventAdmission(noSubtype(false)); !ok || err != nil {
		t.Fatal("the owner's own share without a subtype refused", ok, err)
	}
	// The documented shape exactly (no subtype, no thread_ts), for a post
	// outside a thread.
	flat := r.post(t, core.ConvKey{Interface: "slack", Chat: "C3"}, "", 1)
	if err := r.s.Send(context.Background(), flat); err != nil {
		t.Fatal(err)
	}
	flatIDs, _ := r.st.OwnerPostUploads(flat.OwnerPost.ID)
	var doc map[string]map[string]any
	_ = json.Unmarshal(shareIn("C3", "", true, "", flatIDs...), &doc)
	delete(doc["event"], "subtype")
	documented, _ := json.Marshal(doc)
	if len(flatIDs) != 1 {
		t.Fatal("uploads", flatIDs)
	}
	if ok, _, err := r.s.eventAdmission(documented); ok || err != nil {
		t.Fatal("documented-shape file echo admitted", ok, err)
	}
	if ok, _, _ := r.s.eventAdmission(share("", ids[0], "FOTHER")); !ok {
		t.Fatal("a share with another file refused")
	}
	if ok, _, _ := r.s.eventAdmission(share("look", ids...)); !ok {
		t.Fatal("the owner's own commented share refused")
	}
	if ok, _, _ := r.s.eventAdmission(share("", "FOTHER")); !ok {
		t.Fatal("the owner's own file share refused")
	}
	// The owner's own later, commentless re-share of those files is theirs:
	// not at upload time, another destination, or the bot DM (review
	// finding 7).
	for name, raw := range map[string]json.RawMessage{
		"re-share later":   shareIn("C2", "1000.1", false, "", ids...),
		"other channel":    shareIn("C9", "1000.1", true, "", ids...),
		"other thread":     shareIn("C2", "", true, "", ids...),
		"bot DM re-share":  shareIn("Dbot", "", false, "", ids...),
		"bot DM at upload": shareIn("Dbot", "", true, "", ids...),
	} {
		if ok, _, err := r.s.eventAdmission(raw); !ok || err != nil {
			t.Errorf("%s dropped as an owner post echo (%v)", name, err)
		}
	}
}

// Turned off: a queued post is refused without a call, and no event is
// filtered as an echo (review finding 1).
func TestOwnerPostOffSendsNothingAndFiltersNothing(t *testing.T) {
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C1"}, "queued before off", 1)
	r.s.opt.OwnerPost = false
	before := len(r.f.calls)
	err := r.s.Send(context.Background(), out)
	var perm router.PermanentError
	if !errors.As(err, &perm) || len(r.f.calls) != before {
		t.Fatal("post sent while off", err, len(r.f.calls)-before)
	}
	m := map[string]any{"type": "message", "channel": "Dbot", "channel_type": "im", "user": "UOWNER", "text": "yes", "ts": "1000.1",
		"metadata": map[string]any{"event_type": ownerPostEvent, "event_payload": map[string]string{"delivery_id": "post-1"}}}
	raw, _ := json.Marshal(map[string]any{"event": m, "authorizations": []map[string]any{{"team_id": "Ttest", "user_id": "UOWNER", "is_bot": false}}})
	if ok, _, _ := r.s.eventAdmission(raw); !ok {
		t.Fatal("event filtered as an owner post echo while off")
	}
}

// A post approved for one Slack account never goes out under another's
// token: the owner configured since is refused (review finding 3).
func TestOwnerPostRefusesAnotherSlackAccount(t *testing.T) {
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C1"}, "for UOWNER", 0)
	r.s.opt.WorkOwnerID = "UNEW" // config changed before restart
	before := len(r.f.calls)
	err := r.s.Send(context.Background(), out)
	var perm router.PermanentError
	if !errors.As(err, &perm) || len(r.f.calls) != before {
		t.Fatal("post sent under another owner's token", err)
	}
	r.s.opt.WorkOwnerID = "UOWNER"
	unbound := out
	op := *out.OwnerPost
	op.SlackUser = ""
	unbound.OwnerPost = &op
	if err := r.s.Send(context.Background(), unbound); !errors.As(err, &perm) || len(r.f.calls) != before {
		t.Fatal("post without a recorded Slack account sent", err)
	}
}

// A file copy changed after the owner approved it is never uploaded, and
// nothing of the post is sent (review finding 4).
func TestOwnerPostRefusesAChangedFileCopy(t *testing.T) {
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C1"}, "with a file", 2)
	// Same size, other bytes: only the checksum tells.
	if err := os.WriteFile(out.OwnerPost.Files[1].Path, []byte("tampered file 1!"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := r.s.Send(context.Background(), out)
	var perm router.PermanentError
	if !errors.As(err, &perm) {
		t.Fatal("changed copy not refused", err)
	}
	if r.uploads != 0 || len(r.f.uploads) != 0 || len(r.f.all("chat.postMessage")) != 0 || r.completes != 0 {
		t.Fatal("something of the post went out with a changed file", r.uploads, r.completes)
	}
	// No recorded checksum: refused too.
	out = r.post(t, core.ConvKey{Interface: "slack", Chat: "C1"}, "", 1)
	out.OwnerPost.Files[0].SHA256 = ""
	if err := r.s.Send(context.Background(), out); !errors.As(err, &perm) || r.uploads != 0 {
		t.Fatal("file without a checksum uploaded", err)
	}
}

// A text reply reports each part's ts: a post as the owner's card is
// answered only by a later message (docs/owner-post.md).
func TestSendReportsDeliveredTimestamps(t *testing.T) {
	r := newOwnerPostRig(t)
	var refs []string
	out := core.Outbound{ID: 3, Conv: core.ConvKey{Interface: "slack", Chat: "Dbot", Thread: "999.1"}, Text: "card", Delivered: func(ref string) { refs = append(refs, ref) }}
	if err := r.s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || !store.PlatformBefore("999.1", refs[0]) {
		t.Fatal("delivered ts", refs)
	}
}

// A hub clock ahead of Slack's never hides a landed post from the resend
// check: the bound is the thread's own ts, or a day before the row's local
// creation (review finding 6).
func TestOwnerPostResendCheckToleratesAClockAhead(t *testing.T) {
	slackNow := time.Now()
	landed := fmt.Sprintf("%d.000001", slackNow.Unix()) // Slack's ts of the post
	ahead := core.Outbound{Conv: core.ConvKey{Interface: "slack", Chat: "C1"}, CreatedAt: slackNow.Add(2 * time.Hour)}
	if oldest := ownerPostOldest(ahead); !store.PlatformBefore(oldest, landed) {
		t.Fatalf("bound %s excludes the landed post %s", oldest, landed)
	}
	if store.PlatformBefore(reconciliationOldest(ahead), landed) {
		t.Fatal("the replies' 30 s bound tolerates this skew; the test no longer shows it")
	}
	threaded := ahead
	threaded.Conv.Thread = "1700000000.000100"
	if got := ownerPostOldest(threaded); got != threaded.Conv.Thread {
		t.Fatal("thread post bound is not the thread ts", got)
	}
	// The retry's history check uses that bound.
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C5"}, "after a restart", 0)
	out.CreatedAt, out.Attempt = slackNow.Add(2*time.Hour), 1
	if err := r.s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	h := r.f.all("conversations.history")
	if len(h) == 0 || !store.PlatformBefore(str(h[0].Body["oldest"]), landed) {
		t.Fatal("resend check bound excludes a post that landed", h)
	}
}

// A resend check that Slack answers with more history but no cursor is
// incomplete: the post may have landed, so it fails permanently (the owner
// is told privately) and nothing of it is posted or completed again.
func TestOwnerPostIncompleteHistoryRefusesResend(t *testing.T) {
	incomplete := func(r *ownerPostRig) {
		r.f.mu.Lock()
		prev := r.f.hook
		r.f.mu.Unlock()
		r.f.setHook(func(w http.ResponseWriter, req *http.Request, args map[string]any) bool {
			if req.URL.Path == "/conversations.history" || req.URL.Path == "/conversations.replies" {
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{}, "has_more": true})
				return true
			}
			return prev(w, req, args)
		})
	}
	ctx := context.Background()
	var perm router.PermanentError

	// Text: a later attempt must not post the part again.
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C5", Thread: "1700000000.000100"}, "status update", 0)
	incomplete(r)
	out.Attempt = 1
	if err := r.s.Send(ctx, out); !errors.As(err, &perm) || len(r.f.all("chat.postMessage")) != 0 {
		t.Fatalf("text resent over incomplete history: %v, posts %d", err, len(r.f.all("chat.postMessage")))
	}

	// Files: recorded uploads are never completed again.
	r2 := newOwnerPostRig(t)
	r2.ambiguous = []string{"lost"}
	out = r2.post(t, core.ConvKey{Interface: "slack", Chat: "C6"}, "", 1)
	if err := r2.s.Send(ctx, out); err == nil {
		t.Fatal("unconfirmed upload reported as sent")
	}
	incomplete(r2)
	out.Attempt = 1
	if err := r2.s.Send(ctx, out); !errors.As(err, &perm) || r2.completes != 1 || r2.uploads != 1 {
		t.Fatalf("files completed over incomplete history: %v, completes %d, uploads %d", err, r2.completes, r2.uploads)
	}
}

// A crash after a post went out but before its row was recorded sent may
// leave its copies removed: the retry checks Slack by the recorded upload
// IDs first and reports the post delivered, never failed for a copy it no
// longer needs; recorded files not shared are completed from their IDs.
func TestOwnerPostRetryAfterCopiesRemovedChecksSlackFirst(t *testing.T) {
	ctx := context.Background()
	r := newOwnerPostRig(t)
	out := r.post(t, core.ConvKey{Interface: "slack", Chat: "C7", Thread: "1700000000.000100"}, "with a file", 1)
	if err := r.s.Send(ctx, out); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(out.OwnerPost.Files[0].Path); err != nil {
		t.Fatal(err)
	}
	out.Attempt = 1
	if err := r.s.Send(ctx, out); err != nil || len(r.f.all("chat.postMessage")) != 1 || r.completes != 1 || r.uploads != 1 {
		t.Fatalf("delivered post failed on retry: %v, posts %d, completes %d, uploads %d", err, len(r.f.all("chat.postMessage")), r.completes, r.uploads)
	}

	r2 := newOwnerPostRig(t)
	r2.ambiguous = []string{"lost"}
	out = r2.post(t, core.ConvKey{Interface: "slack", Chat: "C8"}, "", 1)
	if err := r2.s.Send(ctx, out); err == nil {
		t.Fatal("unconfirmed upload reported as sent")
	}
	if err := os.Remove(out.OwnerPost.Files[0].Path); err != nil {
		t.Fatal(err)
	}
	out.Attempt = 1
	if err := r2.s.Send(ctx, out); err != nil || r2.completes != 2 || r2.uploads != 1 {
		t.Fatalf("recorded upload not completed: %v, completes %d, uploads %d", err, r2.completes, r2.uploads)
	}
}
