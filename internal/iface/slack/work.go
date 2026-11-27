package slack

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

var slackMemberID = regexp.MustCompile(`^[UW][A-Z0-9]+$`)

var coverageMention = regexp.MustCompile(`&(?:amp;)?lt;@[UW][A-Z0-9]+(?:\|[^\r\n]*?)?&(?:amp;)?gt;`)
var coverageNamePunctuation = strings.NewReplacer("*", "∗", "_", "＿", "`", "ˋ", "~", "∼")

// coverageNames runs on already escaped text. Only the separately appended
// attribution may produce a real Slack mention.
func (s *Slack) coverageNames(ctx context.Context, text string) string {
	if !coverageMention.MatchString(text) {
		return text
	}
	const genericName = "a Slack member"
	plain := coverageMention.ReplaceAllString(text, genericName)
	// Slack delivery reconciles multipart replies by their part IDs. A failed
	// name lookup must never change the number or boundaries of those parts.
	if len(plain) > slackReplyTokenRunes {
		return plain
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	names := make(map[string]string)
	resolved := coverageMention.ReplaceAllStringFunc(text, func(token string) string {
		_, id, _ := strings.Cut(token, "@")
		id, _, _ = strings.Cut(id, "&")
		id, _, _ = strings.Cut(id, "|")
		if name, ok := names[id]; ok {
			return name
		}
		name := genericName
		if len(names) < 8 && ctx.Err() == nil {
			if user, err := s.liveUser(ctx, id); err == nil {
				if person, verified := Verify(user, s.opt.TeamID); verified {
					display := strings.Join(strings.Fields(router.Scrub(person.Name)), " ")
					if display != "" {
						name = escapeText(coverageNamePunctuation.Replace(display))
					}
				}
			}
		}
		names[id] = name
		return name
	})
	if len(resolved) > slackReplyTokenRunes {
		return plain
	}
	return resolved
}

func (s *Slack) initializeWorkUser(ctx context.Context) {
	if s.opt.WorkUserToken == "" || s.opt.WorkOwnerID == "" {
		return
	}
	api := newAPI(s.opt.APIBase, s.opt.WorkUserToken, s.opt.HTTPClient)
	var auth struct {
		TeamID string `json:"team_id"`
		UserID string `json:"user_id"`
		BotID  string `json:"bot_id"`
	}
	if err := api.call(ctx, "auth.test", map[string]string{}, &auth); err != nil ||
		auth.TeamID != s.opt.TeamID || !slackMemberID.MatchString(auth.UserID) ||
		auth.UserID != s.opt.WorkOwnerID || auth.BotID != "" || auth.UserID == s.botID {
		s.opt.Log.Print("slack: conversation-work user token could not be verified for the configured owner and workspace")
		return
	}
	s.workUser = api
	// Slack's event envelope may list the owner alone even in the bot DM.
	// Resolve the actual bot peer before opening the event stream.
	lookup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for cursor := ""; ; {
		var page struct {
			Channels []struct {
				ID   string `json:"id"`
				User string `json:"user"`
				IsIM bool   `json:"is_im"`
			} `json:"channels"`
			Metadata struct {
				Next string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		args := map[string]any{"types": "im", "limit": 200, "cursor": cursor}
		if err := api.call(lookup, "users.conversations", args, &page); err != nil {
			s.opt.Log.Print("slack: could not verify the owner's direct conversation with the bot")
			break
		}
		for _, ch := range page.Channels {
			if ch.IsIM && ch.User == s.botID && strings.HasPrefix(ch.ID, "D") {
				s.ownerBotDM = ch.ID
				return
			}
		}
		if page.Metadata.Next == "" || page.Metadata.Next == cursor {
			break
		}
		cursor = page.Metadata.Next
	}
}

func (s *Slack) workClient(via string) *apiClient {
	switch via {
	case "user":
		return s.workUser // never fall back from an owner-only route
	case "":
		if s.workUser != nil {
			return s.workUser // legacy durable work, before source was recorded
		}
	}
	return s.bot
}

func (s *Slack) watch(chat, thread string) (core.SlackWatch, error) {
	if s.opt.WorkOwnerID == "" {
		return core.SlackWatch{}, nil
	}
	w, err := s.opt.Store.SlackWatch(chat, thread)
	if err == nil && w.Revision == 0 && s.opt.ConversationWork[chat] != "" {
		w.Mode, w.Rule = core.SlackWatchWork, s.opt.ConversationWork[chat]
	}
	return w, err
}

func workAttribution(id string) string {
	return "AI assistant covering for <@" + id + "> while they’re away."
}

// eventAdmission filters user-authorized events before persistence. The
// verified DM with the bot can still use ordinary addressed handling.
func (s *Slack) eventAdmission(raw json.RawMessage) (ok, ownerOnly bool, err error) {
	var p struct {
		Event          eventMessage
		Authorizations []struct {
			TeamID string `json:"team_id"`
			UserID string `json:"user_id"`
			IsBot  bool   `json:"is_bot"`
		}
	}
	if json.Unmarshal(raw, &p) != nil {
		return false, false, nil
	}
	m := p.Event
	m.Text = sentUsing.ReplaceAllString(m.Text, "")
	if m.User != "" && m.User == s.opt.WorkOwnerID && strings.HasPrefix(m.Text, workAttribution(m.User)) {
		return false, false, nil // our own attributed reply via the user token
	}
	if len(p.Authorizations) == 0 {
		return true, false, nil // legacy and bot-only envelopes without authorizations
	}
	workUser := false
	for _, a := range p.Authorizations {
		if a.TeamID != s.opt.TeamID {
			continue
		}
		if a.IsBot && a.UserID == s.botID {
			return true, false, nil
		}
		if !a.IsBot && s.workUser != nil && a.UserID == s.opt.WorkOwnerID {
			workUser = true
		}
	}
	if !workUser || m.Type != "message" || m.BotID != "" || m.User == "" || m.User == s.botID {
		return false, false, nil
	}
	if s.ownerBotDM != "" && m.Channel == s.ownerBotDM && m.ChannelType == "im" && m.User == s.opt.WorkOwnerID {
		return true, false, nil
	}
	command := router.NormalizeSlash(stripBoundaryMention(strings.TrimSpace(unescape(m.Text)), s.mention()), s.botID)
	w, err := s.watch(m.Channel, first(m.ThreadTS, m.TS))
	if err != nil {
		return false, true, err
	}
	// A human sender in the owner's 1:1 DM is talking to the owner.
	// A bot mention there may be a reference, so leave it to the brief.
	return s.watchPost(m, command, w.Mode, m.ChannelType != "im" && s.mention().MatchString(m.Text)), true, nil
}

// watchPost keeps commands and sign-in on the normal path. Answer watches
// cover the owner, leaving existing bot conversations on their normal path.
func (s *Slack) watchPost(m eventMessage, commandText, mode string, addressedToBot bool) bool {
	if addressedToBot || mode == core.SlackWatchAnswer && m.User == s.opt.WorkOwnerID {
		return false
	}
	return (mode == core.SlackWatchAnswer || mode == core.SlackWatchWork && m.ChannelType != "im") &&
		commandText != "" && !router.IsCommand(commandText) &&
		!mcpCommand.MatchString(commandText) && core.GoogleCommand(commandText) == ""
}

// CheckSlackOwnerReply reads the thread live so replies received during a
// restart or after the model started still suppress away coverage.
func (s *Slack) CheckSlackOwnerReply(ctx context.Context, route core.ConvKey, posted, via string) error {
	api := s.workUser // includes shared-channel thread history
	if api == nil {
		api = s.workClient(via)
	}
	if api == nil {
		return errors.New("slack: no client for owner reply check")
	}
	methods := []string{"conversations.replies"}
	if strings.HasPrefix(route.Chat, "D") {
		methods = append(methods, "conversations.history") // a 1:1 DM can continue outside the thread
	}
	for _, method := range methods {
		args := map[string]any{"channel": route.Chat, "oldest": posted, "limit": 200}
		if method == "conversations.replies" {
			args["ts"] = route.Thread
		}
		for page, cursor := 0, ""; ; page++ {
			args["cursor"] = cursor
			var result struct {
				Messages []eventMessage `json:"messages"`
				HasMore  bool           `json:"has_more"`
				Metadata struct {
					Next string `json:"next_cursor"`
				} `json:"response_metadata"`
			}
			if err := api.call(ctx, method, args, &result); err != nil {
				return err
			}
			for _, m := range result.Messages {
				if m.User == s.opt.WorkOwnerID && m.BotID == "" && compareTS(m.TS, posted) > 0 &&
					!strings.HasPrefix(m.Text, workAttribution(m.User)) {
					return core.ErrSlackOwnerReplied
				}
			}
			next := result.Metadata.Next
			if next == "" && !result.HasMore {
				break
			}
			if next == "" || next == cursor || page == 24 {
				return errors.New("slack: owner reply check could not read complete history")
			}
			cursor = next
		}
	}
	return nil
}

// ConversationWork returns the current rule and its live-verified owner for
// a verified member of this conversation. A post alone is not a membership check.
func (s *Slack) ConversationWork(ctx context.Context, route core.ConvKey, p core.Principal, via string) (core.Principal, core.SlackWatch, error) {
	var none core.SlackWatch
	if route.Interface != "slack" || p.Platform != core.PlatformSlack || p.Team != s.opt.TeamID || !slackMemberID.MatchString(p.SlackUserID) {
		return core.Principal{}, none, nil
	}
	w, err := s.watch(route.Chat, route.Thread)
	if err != nil || (w.Mode != core.SlackWatchWork && w.Mode != core.SlackWatchAnswer) {
		return core.Principal{}, none, err
	}
	if err := s.await(ctx); err != nil {
		return core.Principal{}, none, err
	}
	api := s.workClient(via)
	if api == nil {
		return core.Principal{}, none, nil
	}
	for cursor := ""; ; {
		var page struct {
			Members  []string `json:"members"`
			Metadata struct {
				Next string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		args, _ := json.Marshal(map[string]any{"channel": route.Chat, "limit": 200, "cursor": cursor})
		if err, _, after := api.once(ctx, "conversations.members", args, &page); err != nil {
			if after > 0 {
				return core.Principal{}, none, &identityRetryError{err, after}
			}
			return core.Principal{}, none, err
		}
		for _, id := range page.Members {
			if id == p.SlackUserID {
				u, err := s.liveUser(ctx, s.opt.WorkOwnerID)
				owner, ok := Verify(u, s.opt.TeamID)
				if err != nil || !ok {
					return core.Principal{}, none, err
				}
				return owner, w, nil
			}
		}
		if page.Metadata.Next == "" || cursor == page.Metadata.Next {
			return core.Principal{}, none, nil
		}
		cursor = page.Metadata.Next
	}
}
