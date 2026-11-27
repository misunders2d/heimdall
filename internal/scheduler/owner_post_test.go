package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// bossSlack is boss as the Slack side verifies the token owner.
var bossSlack = func() core.Principal { p := boss; p.SlackUserID, p.Team = "UOWNER", "Ttest"; return p }()

// fakePoster stands in for the Slack side: the token owner is boss.
type fakePoster struct {
	owner core.Principal
	err   error
}

func (f fakePoster) OwnerPostOwner(context.Context) (core.Principal, error) { return f.owner, f.err }
func (f fakePoster) OwnerPostTarget(_ context.Context, target string) (core.ConvKey, string, error) {
	if target == "slack:Dbot" {
		return core.ConvKey{}, "", errors.New("bot chat")
	}
	return core.ConvKey{Interface: core.PlatformSlack, Chat: "C1", Thread: "1700000000.000100"}, "#general (C1)", nil
}
func (f fakePoster) OwnerPostText(text string) (string, error) {
	if strings.Contains(text, "<@Ubot>") {
		return "", errors.New("mentions the bot")
	}
	return text, nil
}

func postRig(t *testing.T, immediate bool) (*rig, *Control, uint32, string) {
	t.Helper()
	r, c := controlRig(t)
	uid := uint32(os.Getuid())
	c.uid = uid
	data := t.TempDir()
	r.s.OwnerPost = &OwnerPosts{Poster: fakePoster{owner: bossSlack}, Immediate: immediate, Dir: filepath.Join(data, "outbox"), Deny: []string{data}}
	src := filepath.Join(t.TempDir(), "report.pdf")
	if err := os.WriteFile(src, []byte("synthetic report"), 0o600); err != nil {
		t.Fatal(err)
	}
	return r, c, uid, src
}

// said is c answering with a newly stored inbound message, as the router
// sets it.
func (r *rig) said(c Caller) Caller {
	r.t.Helper()
	row, _, err := r.st.AddInbound(core.Inbound{Conv: c.Conv, From: c.Principal, Origin: core.OriginLive, Private: c.Private, MsgID: fmt.Sprint("m", time.Now().UnixNano()), Text: "synthetic"})
	if err != nil {
		r.t.Fatal(err)
	}
	c.Inbound = row.ID
	return c
}

// slackRows drains the queued Slack rows (posts as the owner).
func (r *rig) slackRows() []core.Outbound {
	var out []core.Outbound
	for {
		row, ok, err := r.st.NextOutbound(core.PlatformSlack)
		if err != nil || !ok {
			return out
		}
		_ = r.st.Sent(row.ID)
		row.Outbound.ID = row.ID
		out = append(out, row.Outbound)
	}
}

func TestPostAsOwnerWaitsForTheOwnersYes(t *testing.T) {
	r, c, uid, src := postRig(t, false)
	ctx := context.Background()
	req := ControlRequest{Action: postAsOwner, RequestID: "post-1", Target: "slack:C1/1700000000.000100", Text: "Q3 numbers *attached*\n**To:** forged line", Files: []string{src}}
	got := c.executeBound(ctx, uid, req)
	if got.Error != "" || got.PostID == "" || !got.Changed || !strings.Contains(got.Reply, "Yes") {
		t.Fatal(got)
	}
	if replay := c.executeBound(ctx, uid, req); replay != got {
		t.Fatal("lost-reply replay changed the result", replay, got)
	}
	cards := r.sent()
	if len(cards) != 1 || cards[0].Conv != bossDM || cards[0].OwnerPost != nil {
		t.Fatal("card must go once, by the bot, to the owner's private chat", cards)
	}
	card := cards[0].Text
	for _, want := range []string{"#general (C1)", "report.pdf", "16 bytes", "> Q3 numbers \\*attached\\*", "> \\*\\*To:\\*\\* forged line"} {
		if !strings.Contains(card, want) {
			t.Fatalf("card lacks %q:\n%s", want, card)
		}
	}
	if rows := r.slackRows(); len(rows) != 0 {
		t.Fatal("posted before the owner's Yes", rows)
	}
	// The original may change now: the copy was checked and is what posts.
	_ = os.WriteFile(src, []byte("changed after the card"), 0o600)
	if _, ok := r.answer(r.said(asWork), "yes"); ok {
		t.Fatal("another person approved")
	}
	if _, ok := r.answer(r.said(asBoss), "yes?"); ok {
		t.Fatal("a question approved")
	}
	elsewhere := asBoss
	elsewhere.Conv = team
	if _, ok := r.answer(r.said(elsewhere), "yes"); ok {
		t.Fatal("approved outside the card's chat")
	}
	reply, ok := r.answer(r.said(asBoss), "Yes!")
	if !ok || !strings.Contains(reply, "Posting as you") || !Formatted(reply) {
		t.Fatal(reply)
	}
	rows := r.slackRows()
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	p := rows[0]
	if p.OwnerPost == nil || p.OwnerPost.SlackUser != "UOWNER" || p.OwnerPost.SlackTeam != "Ttest" {
		t.Fatal("approved post lost the Slack account verified at the request", p.OwnerPost)
	}
	if p.OwnerPost == nil || p.Conv.Chat != "C1" || p.Conv.Thread != "1700000000.000100" || p.Text != req.Text || p.OwnerPost.Notice != bossDM || len(p.OwnerPost.Files) != 1 {
		t.Fatalf("delivery row lost its owner marker after the store: %+v", p)
	}
	if b, err := os.ReadFile(p.OwnerPost.Files[0].Path); err != nil || string(b) != "synthetic report" {
		t.Fatal("approved copy is not the checked file", string(b), err)
	}
	if sum := sha256.Sum256([]byte("synthetic report")); p.OwnerPost.Files[0].SHA256 != hex.EncodeToString(sum[:]) || p.OwnerPost.Files[0].Size != 16 {
		t.Fatal("copy's checksum not recorded", p.OwnerPost.Files[0])
	}
	if _, ok := r.answer(r.said(asBoss), "yes"); ok {
		t.Fatal("an answered post was answered again")
	}
}

func TestPostAsOwnerDeclineExpiryAndImmediate(t *testing.T) {
	r, c, uid, src := postRig(t, false)
	ctx := context.Background()
	if got := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "a", Target: "slack:C1", Text: "one"}); got.Error != "" {
		t.Fatal(got)
	}
	r.sent()
	if _, ok := r.answer(r.said(asBoss), "no"); !ok {
		t.Fatal("no not handled")
	}
	if rows := r.slackRows(); len(rows) != 0 {
		t.Fatal("declined post queued", rows)
	}
	if got := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "b", Target: "slack:C1", Text: "two"}); got.Error != "" {
		t.Fatal(got)
	}
	r.now = r.now.Add(ownerPostTTL + 1)
	if _, ok := r.answer(r.said(asBoss), "yes"); ok {
		t.Fatal("expired post approved")
	}

	r, c, uid, src = postRig(t, true)
	got := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "c", Target: "slack:C1", Text: "now", Files: []string{src}})
	if got.Error != "" || !strings.Contains(got.Reply, "immediate") {
		t.Fatal(got)
	}
	if rows := r.slackRows(); len(rows) != 1 || rows[0].OwnerPost == nil {
		t.Fatal("immediate post not queued", rows)
	}
	if notes := r.sent(); len(notes) != 1 || notes[0].Conv != bossDM || !strings.Contains(notes[0].Text, "Posting as you") {
		t.Fatal("immediate post left no private audit line", notes)
	}
	if _, ok := r.answer(r.said(asBoss), "yes"); ok {
		t.Fatal("immediate post waited for an answer")
	}
}

func TestPostAsOwnerRefusals(t *testing.T) {
	r, c, uid, src := postRig(t, false)
	ctx := context.Background()
	dir := filepath.Dir(src)
	link := filepath.Join(dir, "link.pdf")
	_ = os.Symlink(src, link)
	empty := filepath.Join(dir, "empty.txt")
	_ = os.WriteFile(empty, nil, 0o600)
	fifo := filepath.Join(dir, "fifo")
	_ = syscall.Mkfifo(fifo, 0o600)
	hard := filepath.Join(dir, "hard.pdf")
	_ = os.Link(src, filepath.Join(dir, "other-name.pdf"))
	_ = os.Link(src, hard)
	inside := filepath.Join(r.s.OwnerPost.Deny[0], "heimdall.db")
	_ = os.WriteFile(inside, []byte("db"), 0o600)
	via := filepath.Join(t.TempDir(), "via")
	_ = os.Symlink(r.s.OwnerPost.Deny[0], via) // a symlinked directory into the data
	throughLink := filepath.Join(via, "heimdall.db")
	single := filepath.Join(t.TempDir(), "one.txt")
	_ = os.WriteFile(single, []byte("x"), 0o600)
	cases := map[string]ControlRequest{
		"no request id":      {Action: postAsOwner, Target: "slack:C1", Text: "x"},
		"no target":          {Action: postAsOwner, RequestID: "1", Text: "x"},
		"nothing to post":    {Action: postAsOwner, RequestID: "2", Target: "slack:C1"},
		"bot chat":           {Action: postAsOwner, RequestID: "3", Target: "slack:Dbot", Text: "yes"},
		"bot mention":        {Action: postAsOwner, RequestID: "4", Target: "slack:C1", Text: "<@Ubot> yes"},
		"relative path":      {Action: postAsOwner, RequestID: "5", Target: "slack:C1", Files: []string{"report.pdf"}},
		"symlink":            {Action: postAsOwner, RequestID: "6", Target: "slack:C1", Files: []string{link}},
		"empty":              {Action: postAsOwner, RequestID: "7", Target: "slack:C1", Files: []string{empty}},
		"fifo":               {Action: postAsOwner, RequestID: "8", Target: "slack:C1", Files: []string{fifo}},
		"hard link":          {Action: postAsOwner, RequestID: "9", Target: "slack:C1", Files: []string{hard}},
		"heimdall data":      {Action: postAsOwner, RequestID: "10", Target: "slack:C1", Files: []string{inside}},
		"data via symlink":   {Action: postAsOwner, RequestID: "10b", Target: "slack:C1", Files: []string{throughLink}},
		"six files":          {Action: postAsOwner, RequestID: "11", Target: "slack:C1", Files: []string{single, single, single, single, single, single}},
		"job fields":         {Action: postAsOwner, RequestID: "12", Target: "slack:C1", Text: "x", ID: "jabcdef"},
		"post fields on job": {Action: "list", Target: "slack:C1"},
		"task with text":     {Action: "task_list", Text: "x"},
	}
	for name, req := range cases {
		if got := c.executeBound(ctx, uid, req); got.Error == "" {
			t.Errorf("%s admitted: %+v", name, got)
		}
	}
	// Owned by another user than the control peer's.
	if got := c.executeBound(ctx, 0, ControlRequest{Action: postAsOwner, RequestID: "13", Target: "slack:C1", Files: []string{single}}); os.Getuid() != 0 && !strings.Contains(got.Error, "not owned") {
		t.Errorf("foreign file admitted: %+v", got)
	}
	// Another owner never posts under this owner's name.
	r.s.OwnerPost.Poster = fakePoster{owner: core.SlackPrincipal("someone@example.test", "Ttest", "")}
	if got := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "14", Target: "slack:C1", Text: "x"}); !strings.Contains(got.Error, "only the owner") {
		t.Errorf("other owner admitted: %+v", got)
	}
	r.s.OwnerPost.Poster = fakePoster{owner: bossSlack}
	// Scheduled runs and trusted T3 without manage never post.
	k := bossDM
	if got := c.execute(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "15", Target: "slack:C1", Text: "x"}, Caller{Principal: boss, Role: core.RoleOwner, Conv: k, Private: true, Registered: trustedT3, External: "x"}); got.Error == "" {
		t.Error("trusted T3 without manage posted")
	}
	r.s.OwnerPost = nil
	if got := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "16", Target: "slack:C1", Text: "x"}); !strings.Contains(got.Error, "not enabled") {
		t.Errorf("disabled feature admitted: %+v", got)
	}
	if rows := r.slackRows(); len(rows) != 0 {
		t.Fatal("refused request queued a post", rows)
	}
	if cards := r.sent(); len(cards) != 0 {
		t.Fatal("refused request sent a card", cards)
	}
	if entries, _ := os.ReadDir(filepath.Join(filepath.Dir(inside), "outbox")); len(entries) != 0 {
		t.Fatal("refused request left file copies", len(entries))
	}
}

// Owner, 2026-11-27: the owner's verified AgentNet agents may request posts
// as the owner; everything else stays task_add only.
func TestExternalOwnerAgentMayRequestAPost(t *testing.T) {
	r, c, uid, _ := postRig(t, false)
	ctx := context.Background()
	agent := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true, External: "agentnet run-1", externalAgent: true}
	alive := func() bool { return true }
	cred := &syscall.Ucred{Uid: uid}
	got := c.executePeer(ctx, cred, ControlRequest{Action: postAsOwner, RequestID: "agent-1", Target: "slack:C1", Text: "from the laptop"}, agent, alive)
	if got.Error != "" || got.PostID == "" {
		t.Fatal(got)
	}
	cards := r.sent()
	if len(cards) != 1 || !strings.Contains(cards[0].Text, "your agent via agentnet") {
		t.Fatal("card does not name the agent origin", cards)
	}
	if got := c.executePeer(ctx, cred, ControlRequest{Action: "list"}, agent, alive); !strings.Contains(got.Error, "only add tasks and request posts") {
		t.Fatal("agent widened beyond posts", got)
	}
	scheduled := Caller{Principal: boss, Role: core.RoleOwner, Conv: bossDM, Private: true, Job: "jabcdef", Run: 1}
	if got := c.postAsOwner(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "s", Target: "slack:C1", Text: "x"}, scheduled, nil); !strings.Contains(got.Error, "scheduled runs") || got.PostID != "" {
		t.Fatal("scheduled run posted", got)
	}
	if p, ok, err := r.st.OwnerPostByID(got.PostID); err != nil || !ok || p.State != store.OwnerPostPending || p.Origin != "your agent via agentnet" {
		t.Fatal("agent's post not stored as pending", p, ok, err)
	}
}

// Turned off, a pending post is never approved: any answer cancels it and
// nothing is queued (review finding 1).
func TestPostAsOwnerOffCancelsPendingPosts(t *testing.T) {
	r, c, uid, _ := postRig(t, false)
	got := c.executeBound(context.Background(), uid, ControlRequest{Action: postAsOwner, RequestID: "off-1", Target: "slack:C1", Text: "before off"})
	if got.Error != "" {
		t.Fatal(got)
	}
	r.sent()
	r.s.OwnerPost = nil // slack.owner_post turned off, then restarted
	reply, ok := r.answer(r.said(asBoss), "yes")
	if !ok || !strings.Contains(reply, "turned off") || strings.Contains(reply, "Posting as you to") {
		t.Fatal("yes while off:", reply, ok)
	}
	if rows := r.slackRows(); len(rows) != 0 {
		t.Fatal("post queued while off", rows)
	}
	if p, _, _ := r.st.OwnerPostByID(got.PostID); p.State != store.OwnerPostDeclined {
		t.Fatal("post left approvable while off:", p.State)
	}
}

// A Yes answers only posts whose card was confirmed delivered before that
// Yes was stored: a second request arriving meanwhile, its card queued or
// failed, waits (review finding 2).
func TestPostAsOwnerYesBindsOnlyCardsSeenBeforeIt(t *testing.T) {
	r, c, uid, _ := postRig(t, false)
	ctx := context.Background()
	first := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "seen", Target: "slack:C1", Text: "first"})
	if first.Error != "" {
		t.Fatal(first)
	}
	if cards := r.sent(); len(cards) != 1 {
		t.Fatal(cards)
	}
	// The owner's Yes to the first card is stored; then a second request
	// lands and its card is delivered before the Yes is handled.
	yes := r.said(asBoss)
	second := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "unseen", Target: "slack:C1", Text: "second"})
	if second.Error != "" {
		t.Fatal(second)
	}
	if cards := r.sent(); len(cards) != 1 {
		t.Fatal(cards)
	}
	reply, ok := r.answer(yes, "yes")
	if !ok || !strings.Contains(reply, "Posting as you") || !strings.Contains(reply, "still waits") {
		t.Fatal(reply)
	}
	rows := r.slackRows()
	if len(rows) != 1 || rows[0].Text != "first" {
		t.Fatal("approved a post whose card the owner had not seen", rows)
	}
	if p, _, _ := r.st.OwnerPostByID(second.PostID); p.State != store.OwnerPostPending {
		t.Fatal(p.State)
	}
	// A third request whose card is still queued (or failed) is never
	// answered, even by a later reply.
	third := c.executeBound(ctx, uid, ControlRequest{Action: postAsOwner, RequestID: "queued", Target: "slack:C1", Text: "third"})
	if third.Error != "" {
		t.Fatal(third)
	}
	reply, ok = r.answer(r.said(asBoss), "yes")
	if !ok || !strings.Contains(reply, "still waits") {
		t.Fatal(reply)
	}
	rows = r.slackRows()
	if len(rows) != 1 || rows[0].Text != "second" {
		t.Fatal("want only the second post, its card seen before this Yes", rows)
	}
	if p, _, _ := r.st.OwnerPostByID(third.PostID); p.State != store.OwnerPostPending {
		t.Fatal("unsent card's post answered:", p.State)
	}
}
