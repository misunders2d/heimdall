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
	if m.ChannelType != "im" || !slices.Contains(s.opt.Owners, p.Key()) {
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

// Callback input is consumed before raw-event persistence. It is ephemeral;
// the one-time code never enters either durable queue.
func (s *Slack) mcpCallback(ctx context.Context, raw json.RawMessage, accept func(context.Context, core.Inbound) error) (bool, error) {
	m := decodeEvent(raw)
	value := strings.TrimSpace(unescape(m.Text))
	if strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "<"), ">")
		value, _, _ = strings.Cut(value, "|")
	}
	if !core.MCPCallback(value) {
		return false, nil
	}
	if m.ChannelType != "im" {
		return true, nil
	}
	u, err := s.liveUser(ctx, m.User)
	if err != nil {
		return true, err
	}
	p, ok := Verify(u, s.opt.TeamID)
	if !ok {
		return true, nil
	}
	in := core.Inbound{Conv: core.ConvKey{Interface: "slack", Chat: m.Channel, Thread: first(m.ThreadTS, m.TS)}, From: p, MsgID: m.TS, Origin: core.OriginLive, At: time.Now(), Text: value, Private: s.privateOwner(ctx, m, p), OAuthActor: "sl_user_" + u.ID, OAuthInstallation: "sl_user_" + s.botID}
	if m.Type != "message" || m.BotID != "" || m.Subtype != "" {
		return true, nil
	}
	return true, accept(ctx, in)
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
				return x
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
