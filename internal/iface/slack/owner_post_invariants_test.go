package slack

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

// The owner-post delivery invariants (docs/owner-post.md, "Delivery"):
//
//	I2 each stage (a text part, the files' completed upload) is recorded
//	   done in the store before the next starts; a retry never writes a
//	   stage recorded done; recorded uploads prove the text done.
//	I3 after any write that may have happened, a retry writes again only
//	   when a complete history check proves absence; incomplete history is
//	   a permanent failure, through every caller.
//	I4 only Heimdall's own posts as the owner are dropped as echoes, from
//	   events and from the watch's owner-reply check.

// invRig is the owner post rig with a Slack fake that answers the owner
// token's writes as Slack would and its history as each case needs.
type invRig struct {
	*ownerPostRig
	// postFaults and completeFaults answer the next writes: "" ok,
	// "lands" (written, then HTTP 500), "lost" (not written, HTTP 500),
	// "refused" (not written, a clear Slack error).
	postFaults, completeFaults []string
	// history answers history reads: "" complete (metadata only when asked
	// for, as Slack does), "has_more" (more without a cursor), "error",
	// "repeat" (a repeated cursor), "pages" (a cursor on every page),
	// "strip" (complete, but every message without its metadata).
	history      string
	posts        map[string]int // landed text writes per delivery key
	postCalls    int
	completeHits int // landed completes
	historyCalls int
}

func newInvRig(t *testing.T) *invRig {
	r := &invRig{ownerPostRig: newOwnerPostRig(t), posts: map[string]int{}}
	f := r.f
	pop := func(q *[]string) string {
		if len(*q) == 0 {
			return ""
		}
		m := (*q)[0]
		*q = (*q)[1:]
		return m
	}
	answer := func(w http.ResponseWriter, mode string, body map[string]any) {
		switch mode {
		case "lands", "lost":
			w.WriteHeader(http.StatusInternalServerError)
		case "refused":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "not_in_channel"})
		default:
			_ = json.NewEncoder(w).Encode(body)
		}
	}
	f.setHook(func(w http.ResponseWriter, req *http.Request, args map[string]any) bool {
		owner := req.Header.Get("Authorization") == "Bearer test-work-user"
		f.mu.Lock()
		defer f.mu.Unlock()
		switch req.URL.Path {
		case "/files.getUploadURLExternal":
			r.uploads++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "upload_url": f.srv.URL + "/upload", "file_id": fmt.Sprintf("F%d", r.uploads)})
			return true
		case "/chat.postMessage":
			if !owner {
				return false
			}
			r.postCalls++
			mode := pop(&r.postFaults)
			f.seq++
			ts := fmt.Sprintf("1000.%06d", f.seq)
			if mode == "" || mode == "lands" {
				meta, _ := args["metadata"].(map[string]any)
				key, _ := meta["event_payload"].(map[string]any)["delivery_id"].(string)
				r.posts[key]++
				m := map[string]any{"channel": args["channel"], "ts": ts, "user": "UOWNER", "text": args["text"], "metadata": meta}
				if args["thread_ts"] != nil {
					m["thread_ts"] = args["thread_ts"]
				}
				f.messages = append(f.messages, m)
			}
			answer(w, mode, map[string]any{"ok": true, "ts": ts})
			return true
		case "/files.completeUploadExternal":
			r.completes++
			mode := pop(&r.completeFaults)
			if mode == "" || mode == "lands" {
				r.completeHits++
				var files []map[string]any
				list, _ := args["files"].([]any)
				for _, item := range list {
					m, _ := item.(map[string]any)
					files = append(files, map[string]any{"id": m["id"]})
				}
				f.seq++
				m := map[string]any{"channel": args["channel_id"], "ts": fmt.Sprintf("1000.%06d", f.seq), "user": "UOWNER", "files": files, "upload": true}
				if args["thread_ts"] != nil {
					m["thread_ts"] = args["thread_ts"]
				}
				f.messages = append(f.messages, m)
			}
			answer(w, mode, map[string]any{"ok": true})
			return true
		case "/conversations.history", "/conversations.replies":
			if !owner {
				return false
			}
			r.historyCalls++
			switch r.history {
			case "has_more":
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{}, "has_more": true})
				return true
			case "error":
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "missing_scope"})
				return true
			case "repeat":
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{}, "has_more": true, "response_metadata": map[string]string{"next_cursor": "same"}})
				return true
			case "pages":
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{}, "has_more": true, "response_metadata": map[string]string{"next_cursor": fmt.Sprintf("c%d", r.historyCalls)}})
				return true
			}
			withMeta := args["include_all_metadata"] == "true" && r.history != "strip"
			var found []map[string]any
			for _, m := range f.messages {
				if m["channel"] != args["channel"] {
					continue
				}
				thread, _ := m["thread_ts"].(string)
				if req.URL.Path == "/conversations.replies" && thread != args["ts"] && m["ts"] != args["ts"] {
					continue
				}
				if req.URL.Path == "/conversations.history" && thread != "" {
					continue
				}
				c := map[string]any{}
				for k, v := range m {
					if k != "metadata" || withMeta {
						c[k] = v
					}
				}
				found = append(found, c)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": found})
			return true
		}
		return false
	})
	return r
}

// deliver is the router's loop for one row: send until it is sent or fails
// for good, at most six attempts.
func (r *invRig) deliver(out core.Outbound) (error, int) {
	var err error
	for attempt := 0; attempt < 6; attempt++ {
		out.Attempt = attempt
		err = r.s.Send(context.Background(), out)
		var perm router.PermanentError
		if err == nil || errors.As(err, &perm) {
			return err, attempt + 1
		}
	}
	return err, 6
}

// parts are out's text parts and their delivery keys, as sent.
func ownerParts(out core.Outbound) ([]string, []string) {
	parts := splitReply(mrkdwn(router.Scrub(out.Text)))
	keys := make([]string, len(parts))
	for i := range parts {
		keys[i] = ownerPartKey(out, len(parts), i)
	}
	return parts, keys
}

// land puts part i of out in Slack as if an earlier attempt had written it.
func (r *invRig) land(out core.Outbound, i int) {
	parts, keys := ownerParts(out)
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	r.f.seq++
	m := map[string]any{"channel": out.Conv.Chat, "ts": fmt.Sprintf("1000.%06d", r.f.seq), "user": "UOWNER", "text": parts[i], "metadata": metadata(keys[i], ownerPostEvent)}
	if out.Conv.Thread != "" {
		m["thread_ts"] = out.Conv.Thread
	}
	r.f.messages = append(r.f.messages, m)
	r.posts[keys[i]]++
}

// landFiles shares the recorded uploads as if an earlier complete landed.
func (r *invRig) landFiles(out core.Outbound, ids []string) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	r.f.seq++
	var files []map[string]any
	for _, id := range ids {
		files = append(files, map[string]any{"id": id})
	}
	m := map[string]any{"channel": out.Conv.Chat, "ts": fmt.Sprintf("1000.%06d", r.f.seq), "user": "UOWNER", "files": files, "upload": true}
	if out.Conv.Thread != "" {
		m["thread_ts"] = out.Conv.Thread
	}
	r.f.messages = append(r.f.messages, m)
	r.completeHits++
}

// invText is long enough to split into parts, each with its own text.
var invText = func() string {
	var b strings.Builder
	for i := range 200 {
		fmt.Fprintf(&b, "Owner update line %03d, long enough to split the post.\n", i)
	}
	return b.String()
}()

// invPost is an approved post with long text (several parts) and one file.
func (r *invRig) invPost(t *testing.T, conv core.ConvKey) core.Outbound {
	t.Helper()
	out := r.post(t, conv, invText, 1)
	if parts, _ := ownerParts(out); len(parts) < 2 {
		t.Fatalf("want several parts, got %d", len(parts))
	}
	return out
}

func mustStage(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var invConvs = []struct {
	name string
	conv core.ConvKey
}{
	{"channel", core.ConvKey{Interface: "slack", Chat: "C5"}},
	{"thread", core.ConvKey{Interface: "slack", Chat: "C6", Thread: "1700000000.000100"}},
}

// I2 and I3: a crash at every stage boundary, then the router's retries,
// under every kind of history. Each stage is written at most once overall;
// a stage that may have been written is written again only after a
// complete check proved it absent; any incomplete check fails the post for
// good with nothing more written.
func TestOwnerPostCrashAtEveryStageBoundary(t *testing.T) {
	type boundary struct {
		name string
		// at puts the store and Slack where the crash left them.
		at func(t *testing.T, r *invRig, out core.Outbound)
		// check: the retry must read history before writing; landed: what
		// it finds there is ours (so a check that loses metadata cannot
		// prove absence).
		check, landed bool
	}
	id := func(out core.Outbound) string { return out.OwnerPost.ID }
	textDone := func(t *testing.T, r *invRig, out core.Outbound) {
		parts, _ := ownerParts(out)
		for i := range parts {
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), i))
			r.land(out, i)
			mustStage(t, r.st.OwnerPostPartDone(id(out), i, fmt.Sprintf("1000.%06d", 900+i)))
		}
	}
	boundaries := []boundary{
		{name: "before anything", at: func(*testing.T, *invRig, core.Outbound) {}},
		{name: "part 1 started, not written", check: true, at: func(t *testing.T, r *invRig, out core.Outbound) {
			parts, _ := ownerParts(out)
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), 0))
		}},
		{name: "part 1 written, not recorded", check: true, landed: true, at: func(t *testing.T, r *invRig, out core.Outbound) {
			parts, _ := ownerParts(out)
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), 0))
			r.land(out, 0)
		}},
		{name: "part 1 done, before part 2", at: func(t *testing.T, r *invRig, out core.Outbound) {
			parts, _ := ownerParts(out)
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), 0))
			r.land(out, 0)
			mustStage(t, r.st.OwnerPostPartDone(id(out), 0, "1000.000900"))
		}},
		{name: "part 2 written, not recorded", check: true, landed: true, at: func(t *testing.T, r *invRig, out core.Outbound) {
			parts, _ := ownerParts(out)
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), 0))
			r.land(out, 0)
			mustStage(t, r.st.OwnerPostPartDone(id(out), 0, "1000.000900"))
			mustStage(t, r.st.StartOwnerPostPart(id(out), len(parts), 1))
			r.land(out, 1)
		}},
		{name: "text done, before uploads", at: textDone},
		{name: "uploads recorded, not completed", check: true, at: func(t *testing.T, r *invRig, out core.Outbound) {
			textDone(t, r, out)
			parts, _ := ownerParts(out)
			mustStage(t, r.st.SetOwnerPostUploads(id(out), len(parts), []string{"FPRE"}))
		}},
		{name: "completed, not recorded", check: true, at: func(t *testing.T, r *invRig, out core.Outbound) {
			textDone(t, r, out)
			parts, _ := ownerParts(out)
			mustStage(t, r.st.SetOwnerPostUploads(id(out), len(parts), []string{"FPRE"}))
			r.landFiles(out, []string{"FPRE"})
		}},
		{name: "files done", at: func(t *testing.T, r *invRig, out core.Outbound) {
			textDone(t, r, out)
			parts, _ := ownerParts(out)
			mustStage(t, r.st.SetOwnerPostUploads(id(out), len(parts), []string{"FPRE"}))
			r.landFiles(out, []string{"FPRE"})
			mustStage(t, r.st.OwnerPostFilesDone(id(out)))
		}},
	}
	for _, conv := range invConvs {
		for _, b := range boundaries {
			for _, history := range []string{"", "strip", "has_more", "error", "repeat", "pages"} {
				t.Run(conv.name+"/"+b.name+"/history="+history, func(t *testing.T) {
					r := newInvRig(t)
					out := r.invPost(t, conv.conv)
					b.at(t, r, out)
					r.history = history
					pre, _ := r.st.OwnerPostStage(out.OwnerPost.ID)
					postsBefore, completesBefore := r.postCalls, r.completes
					err, _ := r.deliver(out)
					incomplete := history != "" && history != "strip"
					var perm router.PermanentError
					if b.check && (incomplete || history == "strip" && b.landed) {
						// Absence not proven: nothing more is written, for good.
						if !errors.As(err, &perm) {
							t.Fatalf("incomplete check: %v, want a permanent failure", err)
						}
						if r.postCalls != postsBefore || r.completes != completesBefore || r.uploads != 0 {
							t.Fatalf("wrote after an incomplete check: posts %d→%d, completes %d→%d, uploads %d", postsBefore, r.postCalls, completesBefore, r.completes, r.uploads)
						}
						return
					}
					if err != nil {
						t.Fatalf("delivery failed: %v", err)
					}
					_, keys := ownerParts(out)
					for _, k := range keys {
						if r.posts[k] != 1 {
							t.Fatalf("part %s in Slack %d times, want exactly once", k, r.posts[k])
						}
					}
					if r.completeHits != 1 {
						t.Fatalf("files completed in Slack %d times, want once", r.completeHits)
					}
					if want := map[bool]int{true: 0, false: 1}[len(pre.Uploads) > 0]; r.uploads != want {
						t.Fatalf("uploaded %d times, want %d", r.uploads, want)
					}
					g, _ := r.st.OwnerPostStage(out.OwnerPost.ID)
					if !g.TextDone(len(keys)) || !g.Files {
						t.Fatalf("stages not all recorded done: %+v", g)
					}
					if !b.check && r.historyCalls != 0 {
						t.Fatalf("history read at a boundary that needs no check (%d reads)", r.historyCalls)
					}
					if b.name == "files done" && (r.postCalls != postsBefore || r.completes != completesBefore) {
						t.Fatal("a delivered post was written again")
					}
					if bot := r.botPosts(); len(bot) != 0 {
						t.Fatal("bot token used", bot)
					}
				})
			}
		}
	}
}

// I2/I3: a write answered ambiguously at every stage (each text part, the
// completed upload), landed or lost, under complete and incomplete history.
// Landed: never written again. Lost: written once more, only after a
// complete check. Incomplete: permanent at once, nothing more written.
func TestOwnerPostAmbiguousWriteAtEveryStage(t *testing.T) {
	type stage struct {
		name     string
		post     []string // faults for chat.postMessage, in order
		complete []string
	}
	stages := []stage{
		{"part 1 lands", []string{"lands"}, nil},
		{"part 1 lost", []string{"lost"}, nil},
		{"part 2 lands", []string{"", "lands"}, nil},
		{"part 2 lost", []string{"", "lost"}, nil},
		{"part 1 lost twice", []string{"lost", "lost"}, nil},
		{"complete lands", nil, []string{"lands"}},
		{"complete lost", nil, []string{"lost"}},
		{"complete lost twice", nil, []string{"lost", "lost"}},
	}
	for _, conv := range invConvs {
		for _, st := range stages {
			for _, history := range []string{"", "strip", "has_more", "error"} {
				t.Run(conv.name+"/"+st.name+"/history="+history, func(t *testing.T) {
					r := newInvRig(t)
					out := r.invPost(t, conv.conv)
					r.postFaults, r.completeFaults = append([]string(nil), st.post...), append([]string(nil), st.complete...)
					// Fresh writes need no check: history is read only after
					// the ambiguous one.
					r.history = history
					err, attempts := r.deliver(out)
					_, keys := ownerParts(out)
					var perm router.PermanentError
					incomplete := history == "has_more" || history == "error"
					landedPart := st.post != nil && st.post[len(st.post)-1] == "lands"
					if incomplete || history == "strip" && landedPart {
						if !errors.As(err, &perm) || attempts != 1 {
							t.Fatalf("ambiguous write over incomplete history: %v after %d attempts, want permanent at once", err, attempts)
						}
						// Nothing after the first ambiguous write.
						want := len(keys) + 1
						for i, f := range st.post {
							if f != "" {
								want = i + 1
								break
							}
						}
						if got := r.postCalls + r.completes; got != want {
							t.Fatalf("%d writes, want %d: nothing after the ambiguous one", got, want)
						}
						return
					}
					if err != nil {
						t.Fatalf("delivery failed: %v", err)
					}
					for _, k := range keys {
						if r.posts[k] != 1 {
							t.Fatalf("part %s in Slack %d times, want once", k, r.posts[k])
						}
					}
					if r.completeHits != 1 || r.uploads != 1 {
						t.Fatalf("files completed %d times, uploaded %d times", r.completeHits, r.uploads)
					}
				})
			}
		}
	}
}

// I2, the reported case: with the uploads recorded and their share
// visible, a history that lost the text's metadata never causes a second
// text post: recorded uploads prove the text done, so it is not checked.
func TestOwnerPostRecordedUploadsProveTextDone(t *testing.T) {
	for _, conv := range invConvs {
		t.Run(conv.name, func(t *testing.T) {
			r := newInvRig(t)
			out := r.invPost(t, conv.conv)
			r.completeFaults = []string{"lands"} // completed, answer lost
			r.history = "strip"
			err, _ := r.deliver(out)
			if err != nil {
				t.Fatal(err)
			}
			_, keys := ownerParts(out)
			for _, k := range keys {
				if r.posts[k] != 1 {
					t.Fatalf("part %s posted %d times", k, r.posts[k])
				}
			}
			if r.completeHits != 1 || r.completes != 1 {
				t.Fatalf("completes %d (%d landed)", r.completes, r.completeHits)
			}
		})
		// A row from before the stages were recorded: uploads recorded, its
		// text parts not. Its text landed, but history lost the metadata;
		// the uploads alone prove the text done, so none is posted again.
		t.Run(conv.name+"/legacy row", func(t *testing.T) {
			r := newInvRig(t)
			out := r.invPost(t, conv.conv)
			_, keys := ownerParts(out)
			for i := range keys {
				r.land(out, i)
			}
			r.landFiles(out, []string{"FLEGACY"})
			db, err := sql.Open("sqlite", "file:"+filepath.Join(r.s.opt.DataDir, "heimdall.db")+"?_pragma=busy_timeout(5000)")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`UPDATE owner_posts SET uploads='["FLEGACY"]',stages='' WHERE id=?`, out.OwnerPost.ID); err != nil {
				t.Fatal(err)
			}
			r.history = "strip"
			if err, _ := r.deliver(out); err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if r.posts[k] != 1 {
					t.Fatalf("part %s posted %d times", k, r.posts[k])
				}
			}
			if r.postCalls != 0 || r.completes != 0 || r.uploads != 0 {
				t.Fatalf("legacy post written again: posts %d, completes %d, uploads %d", r.postCalls, r.completes, r.uploads)
			}
		})
	}
}

// I3: a shutdown during the check after an ambiguous write is no answer
// from Slack: the attempt is retried, not failed, and the retry checks
// again before writing.
func TestOwnerPostCancelledCheckIsRetriedAndChecksAgain(t *testing.T) {
	r := newInvRig(t)
	out := r.invPost(t, invConvs[0].conv)
	r.postFaults = []string{"lands"}
	ctx, cancel := context.WithCancel(context.Background())
	r.s.workUser.pause = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	err := r.s.Send(ctx, out)
	var perm router.PermanentError
	if err == nil || errors.As(err, &perm) {
		t.Fatalf("cancelled check: %v, want a retryable error", err)
	}
	r.s.workUser.pause = r.s.bot.pause
	if err, _ := r.deliver(out); err != nil {
		t.Fatal(err)
	}
	_, keys := ownerParts(out)
	if r.posts[keys[0]] != 1 {
		t.Fatalf("landed part written %d times", r.posts[keys[0]])
	}
}

// I3: a write Slack clearly refused did not happen. It fails for good, as
// before, and leaves the part unstarted.
func TestOwnerPostClearRefusalIsPermanent(t *testing.T) {
	r := newInvRig(t)
	out := r.invPost(t, invConvs[0].conv)
	r.postFaults = []string{"refused"}
	err, attempts := r.deliver(out)
	var perm router.PermanentError
	if !errors.As(err, &perm) || attempts != 1 || r.historyCalls != 0 {
		t.Fatalf("clear refusal: %v after %d attempts, %d history reads", err, attempts, r.historyCalls)
	}
	if g, _ := r.st.OwnerPostStage(out.OwnerPost.ID); g.Started != 0 || len(g.Done) != 0 {
		t.Fatalf("refused part left started: %+v", g)
	}
}

// I4: only Heimdall's own posts as the owner are its echoes, as events and
// in the watch's live owner-reply check. Off, nothing is.
func TestOwnerPostEchoOnlyHeimdallsOwn(t *testing.T) {
	r := newInvRig(t)
	thread := "1700000000.000100"
	conv := core.ConvKey{Interface: "slack", Chat: "C9", Thread: thread}
	out := r.post(t, conv, "", 1)
	if err := r.s.Send(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	g, _ := r.st.OwnerPostStage(out.OwnerPost.ID)
	// The post's delivery row, as the router queued it.
	row, err := r.st.Enqueue(core.Outbound{Conv: conv, Text: "posted", OwnerPost: out.OwnerPost})
	if err != nil {
		t.Fatal(err)
	}
	other := r.post(t, core.ConvKey{Interface: "slack", Chat: "C8"}, "elsewhere", 0)
	otherRow, err := r.st.Enqueue(core.Outbound{Conv: other.Conv, Text: "elsewhere", OwnerPost: other.OwnerPost})
	if err != nil {
		t.Fatal(err)
	}
	tagged := func(key string) map[string]any {
		return map[string]any{"user": "UOWNER", "text": "posted", "metadata": metadata(key, ownerPostEvent)}
	}
	files := func(ids ...string) map[string]any {
		var fs []map[string]any
		for _, id := range ids {
			fs = append(fs, map[string]any{"id": id})
		}
		return map[string]any{"user": "UOWNER", "subtype": "file_share", "upload": true, "files": fs}
	}
	for _, c := range []struct {
		name string
		m    map[string]any
		echo bool
	}{
		{"tagged part of this post's row", tagged(fmt.Sprintf("post-%d", row)), true},
		{"tagged later part of this post's row", tagged(fmt.Sprintf("post-%d#2", row)), true},
		{"tagged row of a post to another channel", tagged(fmt.Sprintf("post-%d", otherRow)), false},
		{"tagged unknown row", tagged("post-999999"), false},
		{"tagged, not a delivery key", tagged("reply-1"), false},
		{"recorded uploads shared at upload", files(g.Uploads...), true},
		{"recorded uploads plus another file", files(append(g.Uploads, "FOTHER")...), false},
		{"the owner's own message", map[string]any{"user": "UOWNER", "text": "I'm back"}, false},
	} {
		for _, on := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/on=%t", c.name, on), func(t *testing.T) {
				r.s.opt.OwnerPost = on
				defer func() { r.s.opt.OwnerPost = true }()
				m := map[string]any{"type": "message", "channel": "C9", "channel_type": "channel", "thread_ts": thread, "ts": "1700000100.000001"}
				for k, v := range c.m {
					m[k] = v
				}
				raw, _ := json.Marshal(map[string]any{"event": m, "authorizations": []map[string]any{{"team_id": "Ttest", "user_id": r.s.botID, "is_bot": true}}})
				admitted, _, err := r.s.eventAdmission(raw)
				if err != nil {
					t.Fatal(err)
				}
				want := !(c.echo && on)
				if admitted != want {
					t.Fatalf("event admitted %t, want %t", admitted, want)
				}
				// The watch's live check: the same message in the thread.
				r.f.mu.Lock()
				saved := r.f.messages
				hist := map[string]any{"channel": "C9", "thread_ts": thread, "ts": "1700000100.000001"}
				for k, v := range c.m {
					hist[k] = v
				}
				r.f.messages = []map[string]any{hist}
				r.f.mu.Unlock()
				defer func() { r.f.mu.Lock(); r.f.messages = saved; r.f.mu.Unlock() }()
				err = r.s.CheckSlackOwnerReply(context.Background(), conv, "1700000000.000200", "user")
				if replied := errors.Is(err, core.ErrSlackOwnerReplied); replied != want {
					t.Fatalf("owner reply %t (%v), want %t", replied, err, want)
				}
				h := r.f.all("conversations.replies")
				if asked := h[len(h)-1].Body["include_all_metadata"] == "true"; asked != on {
					t.Fatalf("include_all_metadata sent %t with owner_post %t", asked, on)
				}
			})
		}
	}
}
