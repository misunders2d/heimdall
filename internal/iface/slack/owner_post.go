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
// its text (chunked like a reply, each part reconciled by its metadata),
// then its files in one completed upload without a comment. Nothing goes to the bot or to the target but the
// post; every failure is the router's, which tells the owner privately.
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
	if err := ownerFilesIntact(out.OwnerPost.Files); err != nil {
		return err
	}
	// The text always goes as tagged messages, never as an upload's
	// untagged comment: the tag is how its echo is known (eventAdmission).
	if err := s.ownerPostParts(ctx, api, out, parts); err != nil {
		return err
	}
	if len(out.OwnerPost.Files) == 0 {
		return nil
	}
	return s.ownerPostFiles(ctx, api, out)
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

// ownerPostParts posts the text parts. Before any resend (a later attempt,
// or an ambiguous post) Slack is checked for the part first, as replies are.
func (s *Slack) ownerPostParts(ctx context.Context, api *apiClient, out core.Outbound, parts []string) error {
	for index, part := range parts {
		id := "post-" + strconv.FormatInt(out.ID, 10)
		if len(parts) > 1 {
			id += fmt.Sprintf("#%d", index+1)
		}
		if out.Attempt > 0 {
			ts, err := api.findDelivery(ctx, out.Conv, id, ownerPostEvent, ownerPostOldest(out))
			if err != nil {
				return err // never resend unchecked
			}
			if ts != "" {
				continue
			}
		}
		if index > 0 {
			if err := api.pause(ctx, s.partEvery); err != nil {
				return err
			}
		}
		args := map[string]any{"channel": out.Conv.Chat, "thread_ts": out.Conv.Thread, "text": part, "unfurl_links": false, "unfurl_media": false, "metadata": metadata(id, ownerPostEvent)}
		if out.Conv.Thread == "" {
			delete(args, "thread_ts")
		}
		var response struct {
			TS string `json:"ts"`
		}
		err := api.call(ctx, "chat.postMessage", args, &response)
		if err == nil && response.TS != "" {
			continue
		}
		if err == nil {
			err = &apiError{Code: "missing_timestamp", Ambiguous: true}
		}
		var ae *apiError
		if !errors.As(err, &ae) || !ae.Ambiguous {
			return err
		}
		found, checkErr := s.settle(ctx, api, func() (bool, error) {
			ts, err := api.findDelivery(ctx, out.Conv, id, ownerPostEvent, ownerPostOldest(out))
			return ts != "", err
		})
		if checkErr == nil && found {
			continue
		}
		return err // the router's retry checks Slack again before any resend
	}
	return nil
}

// ownerPostFiles uploads the files with the owner's token and completes
// them as one message. The file IDs are recorded before completing, so a
// later attempt checks Slack for them and completes again only when they
// were not shared.
func (s *Slack) ownerPostFiles(ctx context.Context, api *apiClient, out core.Outbound) error {
	ids, err := s.opt.Store.OwnerPostUploads(out.OwnerPost.ID)
	if err != nil {
		return err
	}
	if len(ids) > 0 {
		shared, err := s.ownerFilesShared(ctx, api, out, ids)
		if err != nil {
			return err // never complete again unchecked
		}
		if shared {
			return nil
		}
		return s.completeOwnerFiles(ctx, api, out, ids)
	}
	upload := *api
	upload.timeout = 60 * time.Second
	for _, f := range out.OwnerPost.Files {
		id, err := s.ownerUpload(ctx, &upload, f)
		if err != nil {
			return err // nothing was completed: a retry uploads afresh
		}
		ids = append(ids, id)
	}
	if err := s.opt.Store.SetOwnerPostUploads(out.OwnerPost.ID, ids); err != nil {
		return err // unrecorded uploads are never completed
	}
	return s.completeOwnerFiles(ctx, &upload, out, ids)
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
	err := api.call(ctx, "files.completeUploadExternal", args, nil)
	var ae *apiError
	if err == nil || !errors.As(err, &ae) || !ae.Ambiguous {
		return err
	}
	if found, checkErr := s.settle(ctx, api, func() (bool, error) { return s.ownerFilesShared(ctx, api, out, ids) }); checkErr == nil && found {
		return nil
	}
	return err // the router's retry checks Slack again before completing
}

// ownerFilesShared reports whether a message of the owner in the post's
// conversation (its thread, when it has one) already carries any of ids.
func (s *Slack) ownerFilesShared(ctx context.Context, api *apiClient, out core.Outbound, ids []string) (bool, error) {
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	cursor := ""
	for page := 0; page < 100; page++ {
		args := map[string]any{"channel": out.Conv.Chat, "limit": 100, "cursor": cursor, "oldest": ownerPostOldest(out)}
		method := "conversations.history"
		if out.Conv.Thread != "" {
			method, args["ts"] = "conversations.replies", out.Conv.Thread
		}
		var result struct {
			Messages []struct {
				User  string `json:"user"`
				Files []struct {
					ID string `json:"id"`
				} `json:"files"`
			} `json:"messages"`
			HasMore bool `json:"has_more"`
			Meta    struct {
				Cursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		if err := api.call(ctx, method, args, &result); err != nil {
			return false, err
		}
		for _, m := range result.Messages {
			if m.User != s.opt.WorkOwnerID {
				continue
			}
			for _, f := range m.Files {
				if want[f.ID] {
					return true, nil
				}
			}
		}
		next := strings.TrimSpace(result.Meta.Cursor)
		if next == "" {
			if result.HasMore {
				return false, errOwnerHistoryIncomplete
			}
			return false, nil
		}
		if next == cursor {
			return false, errors.New("slack: repeated history cursor")
		}
		cursor = next
	}
	return false, errors.New("slack: reconciliation page limit reached")
}

// errOwnerHistoryIncomplete refuses a resend check that Slack answered with
// more history and no cursor to read it: the post may have landed, so
// nothing of it is sent again and the owner is told privately.
var errOwnerHistoryIncomplete = router.PermanentError{Err: errors.New("slack: Slack's history came back incomplete, so a resend could post twice; nothing more was sent")}

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
// call took effect after all.
func (s *Slack) settle(ctx context.Context, api *apiClient, check func() (bool, error)) (bool, error) {
	var last error
	for _, delay := range []time.Duration{250 * time.Millisecond, 750 * time.Millisecond, 1500 * time.Millisecond} {
		if err := api.pause(ctx, delay); err != nil {
			return false, err
		}
		found, err := check()
		if err == nil && found {
			return true, nil
		}
		last = err
	}
	return false, last
}
