package scheduler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// Posts as the owner (docs/owner-post.md; owner, 2026-11-27): the owner's
// verified agents ask through this control to post to a Slack channel,
// group or DM under the owner's own Slack user token, with files. The
// same admission as scheduler control binds who asks. With approval
// "confirm" (the default) the exact post waits for the owner's own Yes in
// the owner's private chat with the bot; with "immediate" it is queued at
// once. Delivery checks Slack before any resend.

const (
	postAsOwner = "post_as_owner"
	// The outbox limits (router.maxOutFiles, router.maxOutFile).
	maxOwnerPostFiles = 5
	maxOwnerPostFile  = 50 << 20
	maxOwnerPostText  = 12000
	ownerPostTTL      = 24 * time.Hour
)

// OwnerPoster is the Slack side of posts as the owner, using the owner's
// verified user token only (never the bot's).
type OwnerPoster interface {
	// OwnerPostOwner is the person whose user token posts, verified live
	// now (same workspace, not deleted, not a bot).
	OwnerPostOwner(ctx context.Context) (core.Principal, error)
	// OwnerPostTarget checks a target ("slack:<channel>[/<thread>]" or a
	// member ID for a DM) with the owner's token and returns where the post
	// goes and how its card names it. The owner's own chat with the bot is
	// refused: a post there would read as the owner's live input.
	OwnerPostTarget(ctx context.Context, target string) (core.ConvKey, string, error)
	// OwnerPostText is the text as it will be posted (secrets scrubbed);
	// an error refuses it (for example a mention of the bot).
	OwnerPostText(text string) (string, error)
}

// OwnerPosts configures posts as the owner; nil on the Scheduler: off.
type OwnerPosts struct {
	Poster OwnerPoster
	// Immediate is approval mode A: queue at once. False (mode B, the
	// default) asks the owner first.
	Immediate bool
	// Dir holds Heimdall's own checked copies of the files (0700). Files
	// under Deny (the data directory, the config file's directory) are
	// refused, by their resolved path.
	Dir  string
	Deny []string
}

// postAsOwner handles the post_as_owner control action for a bound owner
// origin: a live owner turn, a verified external launcher run (human or the
// owner's agent; owner, 2026-11-27) or trusted T3 with manage. Scheduled
// runs never post.
func (c *Control) postAsOwner(ctx context.Context, uid uint32, req ControlRequest, who Caller, valid []func() bool) ControlResult {
	fail := func(msg string) ControlResult { return ControlResult{Error: msg} }
	cfg := c.s.OwnerPost
	if cfg == nil || cfg.Poster == nil {
		return fail("posting as the owner is not enabled here")
	}
	switch {
	case who.Job != "":
		return fail("scheduled runs cannot post as the owner; a live request is required")
	case who.Registered != "" && !c.manages(who, postAsOwner):
		return fail(who.Registered + " may post as the owner only with manage")
	case req.ID != "" || req.Title != "" || len(req.Definition) > 0 || req.Key != "" || req.Revision != "" || req.Outbound != 0:
		return fail("post_as_owner takes only target, text, files and request_id")
	case req.RequestID == "" || len(req.RequestID) > 128 || strings.ContainsAny(req.RequestID, "\r\n"):
		return fail("post_as_owner requires a stable request_id (1-128 bytes); reuse only for the same request")
	case strings.TrimSpace(req.Target) == "" || len(req.Target) > 128:
		return fail("post_as_owner needs a target: slack:<channel>[/<thread>] or a Slack member ID for a DM")
	case strings.TrimSpace(req.Text) == "" && len(req.Files) == 0:
		return fail("post_as_owner needs text, files or both")
	case len(req.Text) > maxOwnerPostText:
		return fail(fmt.Sprintf("post text is longer than %d bytes", maxOwnerPostText))
	case len(req.Files) > maxOwnerPostFiles:
		return fail(fmt.Sprintf("at most %d files per post", maxOwnerPostFiles))
	}
	// Owner only, and only the person whose Slack account posts: another
	// owner never posts under this owner's name.
	owner, err := cfg.Poster.OwnerPostOwner(ctx)
	if err != nil {
		return fail("the owner's Slack account is unavailable: " + err.Error())
	}
	if owner.Key() != who.Principal.Key() && !c.s.access.SamePerson(owner, who.Principal) {
		return fail("only the owner whose Slack account posts may request posts as the owner")
	}
	// The card and every notice go to the owner's private chat with the bot.
	card := who.Conv
	if !who.Private {
		_, k, err := c.s.st.ControlBinding(who.Principal.Key())
		if err != nil {
			return fail("posting as the owner needs your private chat with Heimdall: " + err.Error())
		}
		card = k
	}
	raw, _ := json.Marshal(struct {
		Request ControlRequest
		Source  core.ConvKey
	}{req, who.Conv})
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if saved, ok, err := c.s.st.ControlResponse(who.Principal.Key(), req.RequestID, digest); err != nil {
		return fail(err.Error())
	} else if ok {
		var result ControlResult
		_ = json.Unmarshal([]byte(saved), &result)
		return result
	}
	target, label, err := cfg.Poster.OwnerPostTarget(ctx, req.Target)
	if err != nil {
		return fail("target refused: " + err.Error())
	}
	text, err := cfg.Poster.OwnerPostText(strings.TrimSpace(req.Text))
	if err != nil {
		return fail("text refused: " + err.Error())
	}
	files, err := copyOwnerFiles(req.Files, uid, cfg.Dir, cfg.Deny)
	if err != nil {
		return fail(err.Error())
	}
	keep := false
	defer func() {
		if !keep {
			removeOwnerFiles(files)
		}
	}()
	now := c.s.now()
	p := store.OwnerPost{ID: newPostID(), Creator: who.Principal.Key(), Card: card, Target: target, Label: label, Text: text, Files: files,
		Origin: postOrigin(who), State: store.OwnerPostPending, Expires: now.Add(ownerPostTTL), Created: now}
	var outs, wake []core.Outbound
	result := ControlResult{PostID: p.ID, Changed: true}
	if cfg.Immediate {
		p.State = store.OwnerPostApproved
		outs = []core.Outbound{ownerPostOut(p), {Conv: card, Text: fmt.Sprintf("📤 Posting as you to %s (%s, %s). — heimdall", plainLabel(label), postOrigin(who), fileCount(len(files)))}}
		wake = outs
		result.Reply = "Queued: posting as the owner to " + plainLabel(label) + " now (approval mode immediate). — heimdall"
	} else {
		// The card is queued linked to the post: only a reply after its
		// confirmed delivery answers it.
		p.CardMessage = &core.Outbound{Conv: card, Text: postCard(card, p), Formatted: true}
		wake = []core.Outbound{*p.CardMessage}
		result.Reply = "Waiting for the owner's Yes in their private chat with Heimdall; nothing is posted until then (expires in 24 h). — heimdall"
	}
	response, _ := json.Marshal(result)
	if len(valid) > 0 && !valid[0]() {
		return fail("request origin expired before commit")
	}
	if err := c.s.st.ControlCommit(who.Principal.Key(), req.RequestID, digest, postAsOwner, p.ID, string(response), uid, &store.JobChange{NewOwnerPost: &p}, outs); err != nil {
		return fail(err.Error())
	}
	keep = true
	c.s.notify(wake...)
	c.s.log.Printf("scheduler control: operator uid %d, action %s, post %s, files %d, immediate %t", uid, postAsOwner, p.ID, len(files), cfg.Immediate)
	return result
}

// ownerPostOut is a post's delivery row: one post to its target, sent
// under the owner's user token only.
func ownerPostOut(p store.OwnerPost) core.Outbound {
	return core.Outbound{Conv: p.Target, Text: p.Text, Formatted: true,
		OwnerPost: &core.OwnerPost{ID: p.ID, Label: p.Label, Files: p.Files, Notice: p.Card}}
}

func postOrigin(who Caller) string {
	switch {
	case who.Registered != "":
		return who.Registered
	case who.External != "":
		name, _, _ := strings.Cut(who.External, " ")
		if who.externalAgent {
			return "your agent via " + name
		}
		return name
	case who.Private:
		return "your agent in this chat"
	}
	return "your agent in another chat"
}

func newPostID() string { return "p" + newID()[1:] }

// postCard shows the exact post: destination, who asked, the text as it
// will be posted (quoted, so it cannot pose as card lines), and each file's
// name and size.
func postCard(k core.ConvKey, p store.OwnerPost) string {
	lines := []string{bold(k, "📤 Post this as you?"), "", bold(k, "To:") + " " + literal(p.Label), bold(k, "Asked by:") + " " + literal(p.Origin)}
	if p.Text != "" {
		lines = append(lines, bold(k, "Message:"))
		for _, line := range strings.Split(p.Text, "\n") {
			lines = append(lines, "> "+literal(line))
		}
	} else {
		lines = append(lines, bold(k, "Message:")+" none, files only")
	}
	if len(p.Files) > 0 {
		lines = append(lines, bold(k, "Files:"))
		for _, f := range p.Files {
			lines = append(lines, "• "+literal(f.Name)+" ("+sizeText(f.Size)+")")
		}
	}
	return strings.Join(lines, "\n") + "\n\n👉 Reply " + bold(k, "Yes") + " to post it as you or " + bold(k, "No") + " to cancel. It expires in 24 h."
}

func plainLabel(s string) string { return strings.Join(strings.Fields(s), " ") }

func fileCount(n int) string {
	switch n {
	case 0:
		return "no files"
	case 1:
		return "1 file"
	}
	return fmt.Sprintf("%d files", n)
}

func sizeText(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}

// ownerPosts are the caller's posts waiting in this conversation.
func (s *Scheduler) ownerPosts(c Caller) []store.OwnerPost {
	if c.Role != core.RoleOwner {
		return nil
	}
	posts, _ := s.st.PendingOwnerPosts(c.Principal.Key(), c.Conv, s.now())
	return posts
}

// answerPosts approves or declines every waiting post here whose card was
// confirmed sent before the answer; each approval queues its delivery row
// with the reply, in one transaction. A post whose card had not reached the
// owner yet is left waiting and named. With posting as the owner turned
// off, any answer declines them all: nothing is posted.
func (s *Scheduler) answerPosts(c Caller, posts []store.OwnerPost, approve bool) (string, *store.JobChange) {
	off := s.OwnerPost == nil
	if off {
		approve = false
	}
	var lines []string
	all := &store.JobChange{}
	for _, p := range posts {
		if !off && !p.CardSeenBefore(c.Inbound) {
			lines = append(lines, "Post to "+literal(p.Label)+" still waits: its card had not reached you before this answer. Answer it after you see it.")
			continue
		}
		d := &store.OwnerPostDecision{ID: p.ID, Creator: p.Creator, Approve: approve, At: s.now()}
		switch {
		case off:
			lines = append(lines, "Posting as you is turned off here; the post to "+literal(p.Label)+" was cancelled and nothing was posted.")
		case approve:
			out := ownerPostOut(p)
			d.Out = &out
			lines = append(lines, "📤 Posting as you to "+literal(p.Label)+" now.")
		default:
			// The copies age out with the outbox (7 days).
			lines = append(lines, "Post to "+literal(p.Label)+" cancelled; nothing was posted.")
		}
		all.More = append(all.More, &store.JobChange{OwnerPost: d})
	}
	title := "📤 Posts as you approved"
	switch {
	case len(all.More) == 0:
		title = "📤 Posts as you"
	case !approve:
		title = "📤 Posts as you cancelled"
	}
	return s.heading(c, title) + "\n" + strings.Join(lines, "\n"), all
}

var postFileName = regexp.MustCompile(`[^A-Za-z0-9._ -]`)

// copyOwnerFiles checks each named file the way an outbox file is checked
// (a regular file, single link, owned by the control peer's kernel UID,
// not empty, at most 50 MB; never a symlink or FIFO) and copies it into
// Heimdall's own directory. The copies, not the originals, are shown and
// sent, so a file cannot change after the owner saw it.
func copyOwnerFiles(paths []string, uid uint32, dir string, deny []string) ([]core.File, error) {
	var files []core.File
	for _, path := range paths {
		f, err := copyOwnerFile(path, uid, dir, deny)
		if err != nil {
			removeOwnerFiles(files)
			return nil, fmt.Errorf("file %q refused: %w", filepath.Base(path), err)
		}
		files = append(files, f)
	}
	return files, nil
}

func copyOwnerFile(path string, uid uint32, dir string, deny []string) (core.File, error) {
	if len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return core.File{}, errors.New("must be an absolute, clean path on the hub")
	}
	if denied(path, deny) {
		return core.File{}, errors.New("Heimdall's own data and configuration are never posted")
	}
	// O_NOFOLLOW: never a symlink; O_NONBLOCK: a FIFO cannot hang the call.
	in, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return core.File{}, errors.New("cannot be opened")
	}
	// A symlinked directory on the way must not reach a denied directory:
	// check where the opened file really is.
	real, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", in.Fd()))
	if err != nil || !filepath.IsAbs(real) || denied(real, deny) {
		in.Close()
		return core.File{}, errors.New("Heimdall's own data and configuration are never posted")
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return core.File{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !info.Mode().IsRegular():
		return core.File{}, errors.New("not a regular file")
	case !ok || st.Uid != uid:
		return core.File{}, errors.New("not owned by the requesting process's user")
	case st.Nlink != 1:
		return core.File{}, errors.New("has more than one link")
	case info.Size() == 0:
		return core.File{}, errors.New("empty")
	case info.Size() > maxOwnerPostFile:
		return core.File{}, errors.New("larger than 50 MB")
	}
	if dir == "" {
		return core.File{}, errors.New("no private file directory configured")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return core.File{}, err
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	copyPath := filepath.Join(dir, fmt.Sprintf("post-%d-%x", time.Now().UnixMilli(), b))
	out, err := os.OpenFile(copyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return core.File{}, err
	}
	n, err := io.Copy(out, io.LimitReader(in, maxOwnerPostFile+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil && (n > maxOwnerPostFile || n == 0) {
		err = errors.New("changed while being copied")
	}
	if err != nil {
		os.Remove(copyPath)
		return core.File{}, err
	}
	name := strings.TrimSpace(postFileName.ReplaceAllString(filepath.Base(path), "_"))
	if name == "" || name == "." || name == ".." {
		name = "file"
	}
	if len(name) > 160 {
		name = name[len(name)-160:]
	}
	m := mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
	m, _, _ = strings.Cut(m, ";")
	if m == "" {
		m = "application/octet-stream"
	}
	return core.File{Path: copyPath, Name: name, MIME: m, Size: n}, nil
}

// denied reports path inside one of dirs, comparing lexically and with
// each directory's symlinks resolved.
func denied(path string, dirs []string) bool {
	for _, d := range dirs {
		if d == "" {
			continue
		}
		roots := []string{filepath.Clean(d)}
		if r, err := filepath.EvalSymlinks(d); err == nil {
			roots = append(roots, r)
		}
		for _, root := range roots {
			if rel, err := filepath.Rel(root, path); err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
				return true
			}
		}
	}
	return false
}

func removeOwnerFiles(files []core.File) {
	for _, f := range files {
		os.Remove(f.Path)
	}
}
