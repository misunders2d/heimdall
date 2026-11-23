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
	if s.opt.WorkUserToken == "" || len(s.opt.ConversationWork) == 0 {
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

func (s *Slack) workClient(channel string) *apiClient {
	if s.workUser != nil && s.opt.ConversationWork[channel] != "" {
		return s.workUser
	}
	return s.bot
}

func workAttribution(id string) string {
	return "AI assistant covering for <@" + id + "> while they’re away."
}

// acceptEvent discards user-authorized events outside the grant before they
// are persisted. Slack bot events continue through ordinary admission.
func (s *Slack) acceptEvent(raw json.RawMessage) bool {
	var p struct {
		Event          eventMessage
		Authorizations []struct {
			TeamID string `json:"team_id"`
			UserID string `json:"user_id"`
			IsBot  bool   `json:"is_bot"`
		}
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	m := p.Event
	if m.User != "" && m.User == s.opt.WorkOwnerID && strings.HasPrefix(m.Text, workAttribution(m.User)) {
		return false // our own attributed reply via the user token
	}
	if len(p.Authorizations) == 0 {
		return true // legacy and bot-only envelopes without authorizations
	}
	workUser := false
	for _, a := range p.Authorizations {
		if a.TeamID != s.opt.TeamID {
			continue
		}
		if a.IsBot && a.UserID == s.botID {
			return true
		}
		if !a.IsBot && s.workUser != nil && a.UserID == s.opt.WorkOwnerID {
			workUser = true
		}
	}
	if !workUser || m.Type != "message" || m.BotID != "" {
		return false
	}
	command := router.NormalizeSlash(stripBoundaryMention(strings.TrimSpace(unescape(m.Text)), s.mention()), s.botID)
	return s.workPost(m, command)
}

// workPost keeps commands and sign-in out of conversation work.
func (s *Slack) workPost(m eventMessage, commandText string) bool {
	return m.ChannelType != "im" && s.opt.ConversationWork[m.Channel] != "" &&
		commandText != "" && router.IsConversationMessage(commandText) &&
		!mcpCommand.MatchString(commandText) && core.GoogleCommand(commandText) == ""
}

// ConversationWork returns the current rule and its live-verified owner for
// a verified member of this conversation. A post alone is not a membership check.
func (s *Slack) ConversationWork(ctx context.Context, route core.ConvKey, p core.Principal) (core.Principal, string, error) {
	rule := s.opt.ConversationWork[route.Chat]
	if rule == "" || route.Interface != "slack" || p.Platform != core.PlatformSlack || p.Team != s.opt.TeamID || !slackMemberID.MatchString(p.SlackUserID) {
		return core.Principal{}, "", nil
	}
	if err := s.await(ctx); err != nil {
		return core.Principal{}, "", err
	}
	for cursor := ""; ; {
		var page struct {
			Members  []string `json:"members"`
			Metadata struct {
				Next string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		args, _ := json.Marshal(map[string]any{"channel": route.Chat, "limit": 200, "cursor": cursor})
		if err, _, after := s.workClient(route.Chat).once(ctx, "conversations.members", args, &page); err != nil {
			if after > 0 {
				return core.Principal{}, "", &identityRetryError{err, after}
			}
			return core.Principal{}, "", err
		}
		for _, id := range page.Members {
			if id == p.SlackUserID {
				u, err := s.liveUser(ctx, s.opt.WorkOwnerID)
				owner, ok := Verify(u, s.opt.TeamID)
				if err != nil || !ok {
					return core.Principal{}, "", err
				}
				return owner, rule, nil
			}
		}
		if page.Metadata.Next == "" || cursor == page.Metadata.Next {
			return core.Principal{}, "", nil
		}
		cursor = page.Metadata.Next
	}
}
