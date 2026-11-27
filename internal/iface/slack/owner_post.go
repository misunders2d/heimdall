package slack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

// Posts as the owner (docs/owner-post.md): only the owner's verified user
// token (HEIMDALL_SLACK_WORK_USER_TOKEN) ever posts or uploads them, never
// the bot's, and nothing about them is sent to their target but the post.

// ownerPostEvent is the metadata type on every text part of a post as the
// owner: the delivery's reconciliation key, and how Heimdall knows its own
// post when Slack sends it back as an event.
const ownerPostEvent = "heimdall_owner_post"

var (
	slackChannelID = regexp.MustCompile(`^[CGD][A-Z0-9]{2,}$`)
	slackThreadTS  = regexp.MustCompile(`^[0-9]{6,12}\.[0-9]{1,8}$`)
)

// OwnerPostOwner is the person whose user token posts, verified live.
func (s *Slack) OwnerPostOwner(parent context.Context) (core.Principal, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	if err := s.await(ctx); err != nil {
		return core.Principal{}, err
	}
	if s.workUser == nil || s.opt.WorkOwnerID == "" {
		return core.Principal{}, errors.New("the owner's Slack user token is not configured or not verified")
	}
	u, err := s.liveUser(ctx, s.opt.WorkOwnerID)
	if err != nil {
		return core.Principal{}, err
	}
	p, ok := Verify(u, s.opt.TeamID)
	if !ok {
		return core.Principal{}, errors.New("the owner's Slack account is not a live member of this workspace")
	}
	return p, nil
}

// OwnerPostTarget checks a target with the owner's own token. A member ID
// opens (or finds) the owner's DM with that person. The owner's chat with
// the bot, and DMs to the bot, are refused: a post there would read as
// the owner's own live input.
func (s *Slack) OwnerPostTarget(parent context.Context, target string) (core.ConvKey, string, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	if err := s.await(ctx); err != nil {
		return core.ConvKey{}, "", err
	}
	api := s.workClient("user")
	if api == nil {
		return core.ConvKey{}, "", errors.New("the owner's Slack user token is not configured or not verified")
	}
	target = strings.TrimPrefix(strings.TrimSpace(target), "slack:")
	if slackMemberID.MatchString(target) {
		if target == s.botID {
			return core.ConvKey{}, "", errors.New("a DM to the bot would read as your own message to it")
		}
		var open struct {
			Channel struct {
				ID string `json:"id"`
			} `json:"channel"`
		}
		if err := api.call(ctx, "conversations.open", map[string]any{"users": target, "return_im": true}, &open); err != nil {
			return core.ConvKey{}, "", fmt.Errorf("could not open the DM: %s", uploadReason(err))
		}
		if !strings.HasPrefix(open.Channel.ID, "D") || !slackChannelID.MatchString(open.Channel.ID) {
			return core.ConvKey{}, "", errors.New("Slack returned no direct message channel")
		}
		k := core.ConvKey{Interface: core.PlatformSlack, Chat: open.Channel.ID}
		if err := s.ownerPostAllowed(k); err != nil {
			return core.ConvKey{}, "", err
		}
		label := "DM with " + target
		if u, err := s.liveUser(ctx, target); err == nil {
			if name := strings.Join(strings.Fields(router.Scrub(first(u.Profile.DisplayName, u.Profile.RealName, u.Name))), " "); name != "" {
				label = "DM with " + clip(name, 80) + " (" + target + ")"
			}
		}
		return k, label, nil
	}
	chat, thread, _ := strings.Cut(target, "/")
	if !slackChannelID.MatchString(chat) || (thread != "" && !slackThreadTS.MatchString(thread)) {
		return core.ConvKey{}, "", errors.New("use slack:<channel>[/<thread ts>] or a Slack member ID")
	}
	k := core.ConvKey{Interface: core.PlatformSlack, Chat: chat, Thread: thread}
	if err := s.ownerPostAllowed(k); err != nil {
		return core.ConvKey{}, "", err
	}
	label := chat
	var info struct {
		Channel struct {
			Name string `json:"name"`
			IM   bool   `json:"is_im"`
		} `json:"channel"`
	}
	// Best effort, for the card only; the post itself shows whether the
	// owner may write there.
	if api.call(ctx, "conversations.info", map[string]any{"channel": chat}, &info) == nil && info.Channel.Name != "" && !info.Channel.IM {
		label = "#" + clip(safeName(info.Channel.Name), 80) + " (" + chat + ")"
	}
	if thread != "" {
		label += ", thread " + thread
	}
	return k, label, nil
}

// ownerPostAllowed refuses the owner's chat with the bot, and any DM while
// that chat is unknown (it cannot be told apart).
func (s *Slack) ownerPostAllowed(k core.ConvKey) error {
	if !strings.HasPrefix(k.Chat, "D") {
		return nil
	}
	if s.ownerBotDM == "" {
		return errors.New("your direct chat with the bot is not verified, so no DM can be told apart from it")
	}
	if k.Chat == s.ownerBotDM {
		return errors.New("a post in your chat with the bot would read as your own message to it")
	}
	return nil
}

// OwnerPostText is the text as it will be posted: secrets scrubbed, as in
// every reply. A mention of the bot is refused: it would start a turn as
// the owner.
func (s *Slack) OwnerPostText(text string) (string, error) {
	text = router.Scrub(text)
	if s.botID != "" && strings.Contains(text, "<@"+s.botID) {
		return "", errors.New("a post as you may not mention the bot; it would start a turn as you")
	}
	return text, nil
}

// sendOwnerPost delivers an approved post with the owner's user token only:
// its text (chunked like a reply, each part tagged with its delivery key),
// then its files in one completed upload without a comment. Nothing goes
// to the bot or to the target but the post; every failure is the router's,
// which tells the owner privately.
//
// Each visible write is a stage recorded in the store before the next one
// starts (store.OwnerPostStages): a stage recorded done is never written
// again, a stage that may have started is written again only when a
// complete check of Slack's history proves it absent, and an incomplete
// check fails the post for good (docs/owner-post.md, "Delivery").
func (s *Slack) sendOwnerPost(ctx context.Context, out core.Outbound) error {
	if !s.opt.OwnerPost {
		return router.PermanentError{Err: errors.New("slack: posting as the owner is turned off (slack.owner_post); nothing was sent")}
	}
	api := s.workClient("user")
	if api == nil || s.opt.WorkOwnerID == "" {
		return router.PermanentError{Err: errors.New("slack: the owner's user token is unavailable; posts as the owner never use the bot")}
	}
	// Only the Slack account verified when the post was requested sends it:
	// a token configured for someone else since never does.
	if p := out.OwnerPost; p.SlackUser == "" || p.SlackUser != s.opt.WorkOwnerID || p.SlackTeam != s.opt.TeamID {
		return router.PermanentError{Err: errors.New("slack: the post was approved for another Slack account than the configured owner's; nothing was sent")}
	}
	if err := s.ownerPostAllowed(out.Conv); err != nil {
		return router.PermanentError{Err: err}
	}
	text := ""
	if out.Text != "" {
		text = mrkdwn(router.Scrub(out.Text))
	}
	if s.botID != "" && strings.Contains(text, "<@"+s.botID) {
		return router.PermanentError{Err: errors.New("slack: a post as the owner may not mention the bot")}
	}
	var parts []string
	if text != "" {
		parts = splitReply(text)
	}
	g, err := s.opt.Store.OwnerPostStage(out.OwnerPost.ID)
	if err != nil {
		return stageError(err)
	}
	if g.Parts != 0 && g.Parts != len(parts) {
		return router.PermanentError{Err: errors.New("slack: the post's text no longer splits as it was sent; nothing more was sent")}
	}
	// Recorded uploads mean the text is done and the verified bytes are
	// already in Slack: the copies may be gone (removed once the row was
	// sent, before a crash recorded it), and only the recorded IDs are
	// needed. Otherwise every copy is checked before anything more is sent.
	if len(g.Uploads) == 0 && len(out.OwnerPost.Files) > 0 {
		if err := ownerFilesIntact(out.OwnerPost.Files); err != nil {
			return err
		}
	}
	// The text always goes as tagged messages, never as an upload's
	// untagged comment: the tag is how its echo is known (ownerPostEcho).
	if !g.TextDone(len(parts)) {
		for i := len(g.Done); i < len(parts); i++ {
			if err := s.ownerPostPart(ctx, api, out, parts, i, g.Started > i); err != nil {
				return err
			}
		}
	}
	if len(out.OwnerPost.Files) == 0 || g.Files {
		return nil
	}
	return s.ownerPostFiles(ctx, api, out, len(parts), g.Uploads)
}

// stageError is a store refusal of a delivery step: out of order, or the
// post no longer approved, is permanent; anything else (the database) is
// retried, and nothing was written.
func stageError(err error) error {
	if errors.Is(err, store.ErrOwnerPostStage) || errors.Is(err, store.ErrOwnerPostNotApproved) {
		return router.PermanentError{Err: fmt.Errorf("slack: %w; nothing more was sent", err)}
	}
	return err
}

// notSent reports a write Slack clearly refused: it did not happen. Any
// other failure (a timeout, a lost answer, a cancelled request) may have.
func notSent(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && !ae.Ambiguous
}

// ownerPartKey is a text part's delivery key, its metadata on Slack.
func ownerPartKey(out core.Outbound, parts, index int) string {
	id := "post-" + strconv.FormatInt(out.ID, 10)
	if parts > 1 {
		id += fmt.Sprintf("#%d", index+1)
	}
	return id
}

// ownerPostPart writes text part index once. started: an earlier attempt
// may have written it, so Slack's history is checked first.
func (s *Slack) ownerPostPart(ctx context.Context, api *apiClient, out core.Outbound, parts []string, index int, started bool) error {
	id, key, part := out.OwnerPost.ID, ownerPartKey(out, len(parts), index), parts[index]
	check := func() (string, error) { return s.ownerHistoryPart(ctx, api, out, key, part) }
	done := func(ts string) error { return stageError(s.opt.Store.OwnerPostPartDone(id, index, ts)) }
	if index > 0 {
		if err := api.pause(ctx, s.partEvery); err != nil {
			return err
		}
	}
	if started {
		ts, err := check()
		if err != nil {
			return err
		}
		if ts != "" {
			return done(ts)
		}
		// Proven absent by a complete check: written once more.
	} else if err := s.opt.Store.StartOwnerPostPart(id, len(parts), index); err != nil {
		return stageError(err)
	}
	args := map[string]any{"channel": out.Conv.Chat, "thread_ts": out.Conv.Thread, "text": part, "unfurl_links": false, "unfurl_media": false, "metadata": metadata(key, ownerPostEvent)}
	if out.Conv.Thread == "" {
		delete(args, "thread_ts")
	}
	var response struct {
		TS string `json:"ts"`
	}
	err := api.call(ctx, "chat.postMessage", args, &response)
	if err == nil && response.TS != "" {
		return done(response.TS)
	}
	if err == nil {
		err = &apiError{Code: "missing_timestamp", Ambiguous: true}
	}
	if notSent(err) {
		// Not written: a retry writes it without a check. If this record
		// fails, the retry checks Slack first, which is only slower.
		_ = s.opt.Store.OwnerPostPartNotSent(id, index)
		return err
	}
	ts, checkErr := s.settle(ctx, api, check)
	if checkErr != nil {
		return checkErr // incomplete history: permanent; cancelled: retried, checked again
	}
	if ts != "" {
		return done(ts)
	}
	return err // proven absent so far: the retry checks again before writing
}

// ownerPostFiles uploads the files with the owner's token and completes
// them as one message. The file IDs are recorded before completing, only
// once the text is done, so a later attempt checks Slack for them and
// completes again only when a complete check proves they were not shared.
func (s *Slack) ownerPostFiles(ctx context.Context, api *apiClient, out core.Outbound, parts int, ids []string) error {
	upload := *api
	upload.timeout = 60 * time.Second
	if len(ids) > 0 {
		shared, err := s.ownerFilesShared(ctx, api, out, ids)
		if err != nil {
			return err // never complete again unchecked
		}
		if shared {
			return stageError(s.opt.Store.OwnerPostFilesDone(out.OwnerPost.ID))
		}
		return s.completeOwnerFiles(ctx, &upload, out, ids)
	}
	for _, f := range out.OwnerPost.Files {
		id, err := s.ownerUpload(ctx, &upload, f)
		if err != nil {
			return err // nothing was completed: a retry uploads afresh
		}
		ids = append(ids, id)
	}
	if err := s.opt.Store.SetOwnerPostUploads(out.OwnerPost.ID, parts, ids); err != nil {
		return stageError(err) // unrecorded uploads are never completed
	}
	return s.completeOwnerFiles(ctx, &upload, out, ids)
}

// errOwnerFileChanged refuses a file copy that is not what the owner
// approved; it is never uploaded.
var errOwnerFileChanged = router.PermanentError{Err: errors.New("slack: a file of the post changed after it was checked; nothing more was sent")}

// ownerFileBytes reads a post's file copy and returns its bytes only when
// they are exactly the size and SHA-256 recorded when it was checked.
func ownerFileBytes(f core.File) ([]byte, error) {
	if f.Size <= 0 || f.Size > 50<<20 || len(f.SHA256) != 64 {
		return nil, errOwnerFileChanged
	}
	in, err := os.Open(f.Path)
	if err != nil {
		return nil, router.PermanentError{Err: errors.New("slack: the post's file copy is unavailable")}
	}
	defer in.Close()
	b, err := io.ReadAll(io.LimitReader(in, f.Size+1))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	if int64(len(b)) != f.Size || hex.EncodeToString(sum[:]) != f.SHA256 {
		return nil, errOwnerFileChanged
	}
	return b, nil
}

// ownerFilesIntact checks every file copy before anything of the post is
// sent, so a changed file never leaves a text-only post behind.
func ownerFilesIntact(files []core.File) error {
	for _, f := range files {
		if _, err := ownerFileBytes(f); err != nil {
			return err
		}
	}
	return nil
}

// ownerUpload uploads exactly the bytes it verified against the recorded
// size and SHA-256, never a reopened file.
func (s *Slack) ownerUpload(ctx context.Context, api *apiClient, f core.File) (string, error) {
	data, err := ownerFileBytes(f)
	if err != nil {
		return "", err
	}
	var grant struct {
		URL string `json:"upload_url"`
		ID  string `json:"file_id"`
	}
	if err := api.call(ctx, "files.getUploadURLExternal", map[string]any{"filename": f.Name, "length": f.Size}, &grant); err != nil {
		return "", err
	}
	if !s.fileHost(grant.URL) || grant.ID == "" {
		return "", errors.New("slack: untrusted file upload endpoint")
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, grant.URL, bytes.NewReader(data))
	if err != nil {
		return "", errors.New("slack: invalid file upload endpoint")
	}
	req.ContentLength = f.Size
	req.Header.Set("Content-Type", first(f.MIME, mime.TypeByExtension(filepath.Ext(f.Name)), "application/octet-stream"))
	response, err := api.http.Do(req)
	if err != nil {
		return "", errors.New("slack: file upload failed")
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("slack: file upload rejected")
	}
	return grant.ID, nil
}

func (s *Slack) completeOwnerFiles(ctx context.Context, api *apiClient, out core.Outbound, ids []string) error {
	var files []map[string]string
	for i, id := range ids {
		title := id
		if i < len(out.OwnerPost.Files) {
			title = out.OwnerPost.Files[i].Name
		}
		files = append(files, map[string]string{"id": id, "title": title})
	}
	args := map[string]any{"files": files, "channel_id": out.Conv.Chat}
	if out.Conv.Thread != "" {
		args["thread_ts"] = out.Conv.Thread
	}
	done := func() error { return stageError(s.opt.Store.OwnerPostFilesDone(out.OwnerPost.ID)) }
	err := api.call(ctx, "files.completeUploadExternal", args, nil)
	if err == nil {
		return done()
	}
	if notSent(err) {
		return err // the retry checks Slack by the recorded IDs first
	}
	shared, checkErr := s.settle(ctx, api, func() (string, error) {
		found, err := s.ownerFilesShared(ctx, api, out, ids)
		if found {
			return "shared", err
		}
		return "", err
	})
	if checkErr != nil {
		return checkErr
	}
	if shared != "" {
		return done()
	}
	return err // proven absent so far: the retry checks again before completing
}

// ownerHistoryMessage is what a resend check reads of a message.
type ownerHistoryMessage struct {
	TS       string `json:"ts"`
	User     string `json:"user"`
	Text     string `json:"text"`
	Metadata struct {
		Type    string `json:"event_type"`
		Payload struct {
			ID string `json:"delivery_id"`
		} `json:"event_payload"`
	} `json:"metadata"`
	Files []struct {
		ID string `json:"id"`
	} `json:"files"`
}

// errOwnerHistoryIncomplete refuses a resend over a check that could not
// read all of the post's history: the post may have landed, so nothing of
// it is sent again and the owner is told privately. It is permanent
// through every caller.
func errOwnerHistoryIncomplete(why string) error {
	return router.PermanentError{Err: errors.New("slack: Slack's history could not be read completely (" + why + "), so a resend could post twice; nothing more was sent")}
}

// ownerHistory reads every message of the post's conversation (its thread,
// when it has one) since ownerPostOldest, with metadata, and gives each to
// visit until it returns true. Anything short of the complete history (an
// API error, more history without a cursor, a repeated cursor, the page
// limit) is errOwnerHistoryIncomplete; a cancelled context is returned as
// it is, so the retry checks again.
func (s *Slack) ownerHistory(ctx context.Context, api *apiClient, out core.Outbound, visit func(ownerHistoryMessage) bool) error {
	cursor := ""
	for page := 0; page < 100; page++ {
		args := map[string]any{"channel": out.Conv.Chat, "limit": 100, "cursor": cursor, "oldest": ownerPostOldest(out), "include_all_metadata": true}
		method := "conversations.history"
		if out.Conv.Thread != "" {
			method, args["ts"] = "conversations.replies", out.Conv.Thread
		}
		var result struct {
			Messages []ownerHistoryMessage `json:"messages"`
			HasMore  bool                  `json:"has_more"`
			Meta     struct {
				Cursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		if err := api.call(ctx, method, args, &result); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var ae *apiError
			if errors.As(err, &ae) {
				return errOwnerHistoryIncomplete(ae.Code)
			}
			return errOwnerHistoryIncomplete("request failed")
		}
		for _, m := range result.Messages {
			if visit(m) {
				return nil
			}
		}
		next := strings.TrimSpace(result.Meta.Cursor)
		if next == "" {
			if result.HasMore {
				return errOwnerHistoryIncomplete("more history without a cursor")
			}
			return nil
		}
		if next == cursor {
			return errOwnerHistoryIncomplete("repeated cursor")
		}
		cursor = next
	}
	return errOwnerHistoryIncomplete("page limit")
}

// ownerHistoryPart finds a text part by its delivery key and returns its
// ts, or "" when the complete history proves it absent. A message of the
// owner with exactly the part's text but no delivery tag means Slack left
// the metadata out: absence is not proven, so the check is incomplete.
func (s *Slack) ownerHistoryPart(ctx context.Context, api *apiClient, out core.Outbound, key, part string) (string, error) {
	found, untagged := "", false
	err := s.ownerHistory(ctx, api, out, func(m ownerHistoryMessage) bool {
		if m.Metadata.Type == ownerPostEvent && m.Metadata.Payload.ID == key {
			found = m.TS
			return true
		}
		if m.User == s.opt.WorkOwnerID && m.Metadata.Type == "" && strings.TrimSpace(m.Text) == strings.TrimSpace(part) {
			untagged = true
		}
		return false
	})
	switch {
	case err != nil:
		return "", err
	case found != "":
		return found, nil
	case untagged:
		return "", errOwnerHistoryIncomplete("a message with the part's text came back without its metadata")
	}
	return "", nil
}

// ownerFilesShared reports whether a message of the owner in the post's
// conversation (its thread, when it has one) already carries any of ids;
// false only when the complete history proves none does.
func (s *Slack) ownerFilesShared(ctx context.Context, api *apiClient, out core.Outbound, ids []string) (bool, error) {
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	shared := false
	err := s.ownerHistory(ctx, api, out, func(m ownerHistoryMessage) bool {
		if m.User != s.opt.WorkOwnerID {
			return false
		}
		for _, f := range m.Files {
			if want[f.ID] {
				shared = true
			}
		}
		return shared
	})
	return shared && err == nil, err
}

// ownerPostSkew is how far the hub's clock may run ahead of Slack's before
// a resend check could miss a post that landed.
const ownerPostSkew = 24 * time.Hour

// ownerPostOldest is the lower bound of a post's resend checks. In a
// thread it is the thread's own ts, Slack's clock: the whole thread is
// checked. Otherwise it is the row's local creation time less
// ownerPostSkew, so a hub clock up to a day ahead still finds a post that
// landed (the replies' 30 s margin would miss it and post twice). The
// unique delivery tag and file IDs keep the wide window exact.
func ownerPostOldest(out core.Outbound) string {
	if out.Conv.Thread != "" || out.CreatedAt.IsZero() {
		return out.Conv.Thread
	}
	return fmt.Sprintf("%.6f", float64(out.CreatedAt.Add(-ownerPostSkew).UnixMicro())/1e6)
}

// settle checks a few times, with growing pauses, whether an ambiguous
// write took effect after all: what check found, or "" when each complete
// check proved it absent. A check that fails ends it with that error.
func (s *Slack) settle(ctx context.Context, api *apiClient, check func() (string, error)) (string, error) {
	for _, delay := range []time.Duration{250 * time.Millisecond, 750 * time.Millisecond, 1500 * time.Millisecond} {
		if err := api.pause(ctx, delay); err != nil {
			return "", err
		}
		found, err := check()
		if err != nil || found != "" {
			return found, err
		}
	}
	return "", nil
}
