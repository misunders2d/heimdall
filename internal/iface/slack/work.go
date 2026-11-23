package slack

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/router"
)

var slackMemberID = regexp.MustCompile(`^[UW][A-Z0-9]+$`)

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
		if err, _, after := s.bot.once(ctx, "conversations.members", args, &page); err != nil {
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
