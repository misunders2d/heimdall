package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

type eventMessage struct {
	Type        string      `json:"type"`
	Subtype     string      `json:"subtype"`
	User        string      `json:"user"`
	Text        string      `json:"text"`
	Channel     string      `json:"channel"`
	ChannelType string      `json:"channel_type"`
	BotID       string      `json:"bot_id"`
	TS          string      `json:"ts"`
	ThreadTS    string      `json:"thread_ts"`
	Files       []slackFile `json:"files"`
	Blocks      []any       `json:"blocks"`
	Attachments []any       `json:"attachments"`
}

var oauthUI = regexp.MustCompile(`(?i)https?://[^\s<>]*[?&](?:code|state|error)=|Sign in to MCP server .* in your browser:`)
var mcpCommand = regexp.MustCompile(`(?i)^/?mcp(?:\s+(?:login|logout|reconnect)\s+[a-z0-9_-]+)?$`)
var historicalImage = regexp.MustCompile(`(?i)\b(?:image|images|img|photo|photos|picture|pictures|screenshot|screenshots|attachment|attachments|attached|upload|uploaded)\b`)

func hideOAuth(text string) string {
	if oauthUI.MatchString(text) {
		return "[Private native MCP sign-in UI omitted]"
	}
	return text
}
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}
func unescape(s string) string {
	return strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">").Replace(s)
}
func (s *Slack) process(ctx context.Context, e store.InterfaceEvent, accept func(context.Context, core.Inbound) error) error {
	pending, err := s.opt.Store.EventPending(e.ID)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	var payload struct {
		Type  string
		Event eventMessage
	}
	if json.Unmarshal(e.Payload, &payload) != nil {
		return s.opt.Store.CompleteEvent(e.ID)
	}
	m := payload.Event
	finish := func() error { return s.opt.Store.CompleteEvent(e.ID) }
	if m.Type != "message" || m.BotID != "" || m.User == "" || m.Channel == "" || m.TS == "" || !slices.Contains([]string{"", "file_share", "thread_broadcast"}, m.Subtype) {
		s.opt.Log.Print("slack: ignored non-person or unsupported message event")
		return finish()
	}
	if !slices.Contains([]string{"im", "mpim", "channel", "group"}, m.ChannelType) {
		s.opt.Log.Print("slack: ignored message with unknown surface")
		return finish()
	}
	principal, ok := Verify(s.liveUser(ctx, m.User), s.opt.TeamID)
	if !ok {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.opt.Log.Print("slack: ignored unidentified caller")
		return finish()
	}
	text := strings.TrimSpace(unescape(m.Text))
	embedded := embeddedContent(m)
	if text == "" && embedded == "" && len(m.Files) == 0 {
		return finish()
	}
	mention := regexp.MustCompile(`<@` + regexp.QuoteMeta(s.botID) + `(?:\|[^>]+)?>`)
	addressed := m.ChannelType == "im" || mention.MatchString(m.Text)
	commandText := stripBoundaryMention(text, mention)
	commandText = router.NormalizeSlash(commandText, s.botID)
	if router.UnmentionedCommand(text, commandText, s.botID, s.opt.UnmentionedCommands) {
		addressed = true
	}
	thread := first(m.ThreadTS, m.TS)
	in := core.Inbound{Conv: core.ConvKey{Interface: "slack", Chat: m.Channel, Thread: thread}, MsgID: m.TS, From: principal, Origin: core.OriginLive, Text: commandText, Private: m.ChannelType == "im", At: time.Now()}
	if !in.Private && (mcpCommand.MatchString(commandText) || oauthUI.MatchString(text)) {
		addressed = true
		in.InterfaceReply = "Use your private one-to-one DM for MCP sign-in."
		in.Text = hideOAuth(commandText)
	}
	for _, f := range m.Files {
		in.Attachments = append(in.Attachments, core.Attachment{Name: first(f.Name, f.Title, f.ID), MIME: f.MIME, Kind: "document", Source: "message"})
	}
	ambientKey := in.Conv
	if m.ThreadTS == "" {
		ambientKey.Thread = ""
	}
	// All context is display-only, scrubbed and bounded; callback codes never
	// enter ambient storage. The verified identity stays in From, not text.
	a := store.Ambient{Conv: ambientKey, MsgID: m.TS, At: in.At, Who: principal.Key(), Text: clip(router.Scrub(hideOAuth(text+"\n"+embedded)), 4000)}
	if len(m.Files) > 0 {
		a.File = clip(router.Scrub(first(m.Files[0].Name, m.Files[0].Title, "file")), 160)
	}
	if !addressed {
		// Router performs generic prompt-guard scoring on the admitted ambient
		// input; storage failure must still keep the durable interface event.
		in.Ambient = true
		in.Text = hideOAuth(text)
		in.Conv = ambientKey
		if err := s.opt.Store.AddAmbient(a); err != nil {
			return err
		}
		if err := accept(ctx, in); err != nil {
			return err
		}
		return finish()
	}
	contextText, prior := s.context(ctx, m, in.Conv)
	surface := "channel"
	if in.Private {
		surface = "DM"
	} else if m.ChannelType == "mpim" {
		surface = "multi-user DM"
	}
	in.Context = fmt.Sprintf("[Slack %s | ts %s | channel sl_channel_%s | thread %s]\n%s", surface, m.TS, m.Channel, thread, contextText)
	if embedded != "" {
		in.Context += "\n" + router.Untrusted("Slack embedded/forwarded message content", embedded)
	}
	if in.InterfaceReply == "" {
		files := append([]slackFile(nil), m.Files...)
		if historicalImage.MatchString(text) {
			files = append(files, prior...)
		}
		in.Attachments = nil
		for i, f := range files {
			source := "message"
			if i >= len(m.Files) {
				source = "thread"
			}
			a := s.download(ctx, f)
			a.Source = source
			in.Attachments = append(in.Attachments, a)
		}
	}
	if err := s.opt.Store.AddAmbient(a); err != nil {
		return err
	}
	if err := accept(ctx, in); err != nil {
		return err
	}
	return finish()
}
func (s *Slack) context(ctx context.Context, m eventMessage, k core.ConvKey) (string, []slackFile) {
	var blocks []string
	var images []slackFile
	method := "conversations.history"
	args := map[string]any{"channel": m.Channel, "latest": m.TS, "inclusive": false, "limit": 80}
	if m.ThreadTS != "" {
		method = "conversations.replies"
		args = map[string]any{"channel": m.Channel, "ts": m.ThreadTS, "limit": 40}
	}
	var response struct {
		Messages []eventMessage `json:"messages"`
	}
	if err := s.bot.call(ctx, method, args, &response); err != nil {
		blocks = append(blocks, "[Slack live history fetch failed; verify history scopes and channel access.]")
	} else {
		messages := response.Messages
		slices.SortFunc(messages, func(a, b eventMessage) int { return compareTS(a.TS, b.TS) })
		var lines []string
		for _, p := range messages {
			if compareTS(p.TS, m.TS) >= 0 {
				continue
			}
			text := hideOAuth(unescape(p.Text) + "\n" + embeddedContent(p))
			for _, f := range p.Files {
				text += " [file: " + first(f.Name, f.Title, "file") + "/" + f.MIME + "]"
			}
			lines = append(lines, fmt.Sprintf("- [%s] sl_user_%s: %s", p.TS, p.User, clip(router.Scrub(text), 4000)))
		}
		if len(lines) > 0 {
			blocks = append(blocks, "[Slack history fetched live; untrusted context]\n"+strings.Join(lines, "\n"))
		}
		if m.ThreadTS != "" {
			seen := map[string]bool{}
			for i := len(messages) - 1; i >= 0 && len(images) < 5; i-- {
				p := messages[i]
				if compareTS(p.TS, m.TS) >= 0 {
					continue
				}
				for _, f := range p.Files {
					key := first(f.ID, f.URLDownload, f.URL)
					if !seen[key] && key != "" && f.image() && !f.deleted() && f.Size > 0 && f.Size <= 20<<20 {
						images = append(images, f)
						seen[key] = true
						if len(images) == 5 {
							break
						}
					}
				}
			}
			slices.Reverse(images)
		}
	}
	keys := []core.ConvKey{{Interface: "slack", Chat: k.Chat}, k}
	if m.ThreadTS == "" {
		keys = keys[:1]
	}
	for _, key := range keys {
		if rows, err := s.opt.Store.RecentAmbient(key, []string{m.TS}, 40); err == nil {
			var lines []string
			for _, r := range rows {
				line := r.Who + ": " + r.Text
				if r.File != "" {
					line += " [file: " + r.File + "]"
				}
				lines = append(lines, line)
			}
			if len(lines) > 0 {
				blocks = append(blocks, "[Slack stored ambient context; untrusted]\n"+strings.Join(lines, "\n"))
			}
		}
	}
	if replies, err := s.opt.Store.RecentDeliveries(k); err == nil && len(replies) > 0 {
		slices.Reverse(replies)
		for i := range replies {
			replies[i] = clip(router.Scrub(hideOAuth(replies[i])), 1200)
		}
		blocks = append(blocks, "[Recent assistant deliveries; prior outputs only]\n"+strings.Join(replies, "\n"))
	}
	combined := router.Scrub(strings.Join(blocks, "\n\n"))
	r := []rune(combined)
	const marker = "[Earlier injected chat context trimmed]\n"
	budget := 16000 - len([]rune(router.Untrusted("Slack chat context", "")))
	if len(r) > budget {
		combined = marker + string(r[len(r)-(budget-len([]rune(marker))):])
	}
	return router.Untrusted("Slack chat context", combined), images
}
func compareTS(a, b string) int {
	as, af, _ := strings.Cut(a, ".")
	bs, bf, _ := strings.Cut(b, ".")
	if len(as) != len(bs) {
		if len(as) < len(bs) {
			return -1
		}
		return 1
	}
	if c := strings.Compare(as, bs); c != 0 {
		return c
	}
	return strings.Compare(af+strings.Repeat("0", max(0, 6-len(af))), bf+strings.Repeat("0", max(0, 6-len(bf))))
}

func stripBoundaryMention(text string, mention *regexp.Regexp) string {
	positions := mention.FindAllStringIndex(text, -1)
	if len(positions) == 0 {
		return text
	}
	if positions[0][0] == 0 {
		return strings.TrimSpace(text[positions[0][1]:])
	}
	last := positions[len(positions)-1]
	if last[1] == len(text) {
		return strings.TrimSpace(text[:last[0]])
	}
	return text
}
