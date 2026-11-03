package slack

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
)

func (s *Slack) privateOwner(ctx context.Context, m eventMessage, p core.Principal) bool {
	if m.ChannelType != "im" || !s.owner(p) {
		return false
	}
	return s.privateCaller(ctx, m)
}

// A live same-workspace human may sign into approved business MCP in a 1:1 DM.
// Identity is already verified by liveUser/Verify; never trust channel_type alone.
func (s *Slack) privateCaller(ctx context.Context, m eventMessage) bool {
	if m.ChannelType != "im" {
		return false
	}
	var result struct {
		Channel struct {
			IsIM   bool   `json:"is_im"`
			IsMPIM bool   `json:"is_mpim"`
			User   string `json:"user"`
		} `json:"channel"`
	}
	if s.bot.call(ctx, "conversations.info", map[string]any{"channel": m.Channel}, &result) != nil {
		return false
	}
	return result.Channel.IsIM && !result.Channel.IsMPIM && result.Channel.User == m.User
}

// Relay runs after ack; callbackValue bypasses raw-event persistence first.
// Input stays ephemeral: the one-time code never enters either durable queue.
func (s *Slack) mcpCallback(ctx context.Context, raw json.RawMessage, accept func(context.Context, core.Inbound) error) (bool, error) {
	m := decodeEvent(raw)
	value := callbackValue(raw)
	if value == "" {
		return false, nil
	}
	if m.Type != "message" || m.BotID != "" || m.Subtype != "" || m.ChannelType != "im" || m.User == "" {
		return true, nil
	}
	u, err := s.liveUser(ctx, m.User)
	if err != nil {
		return true, nil
	}
	p, ok := Verify(u, s.opt.TeamID)
	if !ok {
		return true, nil
	}
	privateOwner := s.privateOwner(ctx, m, p)
	private := privateOwner
	if !s.owner(p) {
		private = s.privateCaller(ctx, m)
	}
	in := core.Inbound{Conv: core.ConvKey{Interface: "slack", Chat: m.Channel, Thread: first(m.ThreadTS, m.TS)}, From: p, MsgID: m.TS, Origin: core.OriginLive, At: time.Now(), Text: value, Private: private, SlackPrivateOwnerVerified: privateOwner, SlackReadVerified: privateOwner && s.opt.OwnerSlackEnabled, OAuthActor: "sl_user_" + u.ID, OAuthInstallation: "sl_user_" + s.botID}
	if m.Type != "message" || m.BotID != "" || m.Subtype != "" {
		return true, nil
	}
	_ = accept(ctx, in) // Relay errors cannot terminate Socket Mode or the router.
	return true, nil
}

func callbackValue(raw json.RawMessage) string {
	value := strings.TrimSpace(unescape(decodeEvent(raw).Text))
	if strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "<"), ">")
		value, _, _ = strings.Cut(value, "|")
	}
	if core.MCPCallback(value) {
		return value
	}
	return ""
}

// Scrub callback URLs in all JSON strings before staging raw event data.
func redactMCPEvent(raw json.RawMessage) json.RawMessage {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return raw
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case string:
			if mcpCommand.MatchString(strings.TrimSpace(x)) {
				return core.RedactMCPCallbacks(x)
			}
			return hideOAuth(x)
		case []any:
			for i := range x {
				x[i] = walk(x[i])
			}
		case map[string]any:
			for k, v := range x {
				x[k] = walk(v)
			}
		}
		return v
	}
	out, err := json.Marshal(walk(v))
	if err != nil {
		return nil
	}
	return out
}

// All owner-only transport controls share the router's admin authority.
func (s *Slack) owner(p core.Principal) bool {
	if s.opt.Access != nil {
		r, ok := s.opt.Access.Role(p)
		return ok && r == core.RoleOwner
	}
	return slices.Contains(s.opt.Owners, p.Key())
}
