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
	if !s.opt.OwnerSlackEnabled || m.ChannelType != "im" || !slices.Contains(s.opt.Owners, p.Key()) {
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
	in := core.Inbound{Conv: core.ConvKey{Interface: "slack", Chat: m.Channel, Thread: first(m.ThreadTS, m.TS)}, From: p, MsgID: m.TS, Origin: core.OriginLive, At: time.Now(), Text: value, Private: true, SlackReadVerified: s.privateOwner(ctx, m, p), OAuthActor: "sl_user_" + u.ID, OAuthInstallation: "sl_user_" + s.botID}
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
