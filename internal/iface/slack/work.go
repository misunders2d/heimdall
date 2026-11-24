package slack

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

var slackMemberID = regexp.MustCompile(`^[UW][A-Z0-9]+$`)

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

// eventAdmission filters user-authorized events before persistence. Those
// events can never fall through to ordinary addressed handling.
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
	if !workUser || m.Type != "message" || m.BotID != "" {
		return false, false, nil
	}
	command := router.NormalizeSlash(stripBoundaryMention(strings.TrimSpace(unescape(m.Text)), s.mention()), s.botID)
	w, err := s.watch(m.Channel, first(m.ThreadTS, m.TS))
	if err != nil {
		return false, true, err
	}
	if w.Mode == core.SlackWatchAnswer && m.User == s.opt.WorkOwnerID {
		return false, true, nil
	}
	return workPost(m, command, w.Mode), true, nil
}

// workPost keeps commands and sign-in out of conversation work.
func workPost(m eventMessage, commandText, mode string) bool {
	return (mode == core.SlackWatchAnswer || mode == core.SlackWatchWork && m.ChannelType != "im") &&
		commandText != "" && router.IsConversationMessage(commandText) &&
		!mcpCommand.MatchString(commandText) && core.GoogleCommand(commandText) == ""
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
