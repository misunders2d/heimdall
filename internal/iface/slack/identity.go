// Package slack is Heimdall's Socket Mode Slack interface. Every admitted
// caller must pass live same-workspace identity verification.
package slack

import (
	"github.com/misunders2d/heimdall/internal/core"
)

// User is the part of Slack's users.info "user" object a caller's identity
// rests on.
type User struct {
	ID      string `json:"id"`
	TeamID  string `json:"team_id"`
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
	IsBot   bool   `json:"is_bot"`
	BotID   string `json:"bot_id"`
	Profile struct {
		Email       string `json:"email"`
		DisplayName string `json:"display_name"`
		RealName    string `json:"real_name"`
	} `json:"profile"`
}

// Verify returns the sender of a message as a principal, from the live
// users.info answer for that sender (nil when the lookup failed) and the
// bot's own workspace from auth.test. It follows the Pi hub's check
// (misunderstood-pi-pack ec72c7d, extensions/slack.ts slackUserEmail and
// the liveVerified + team_id test): the lookup returned an email, the user
// is not deleted and not a bot, and belongs to the bot's own workspace.
// Anyone else, Slack Connect guests from other workspaces included, cannot
// be identified. Heimdall keeps no email cache: an email seen in an earlier
// lookup never verifies a later message.
func Verify(u *User, workspace string) (core.Principal, bool) {
	if u == nil || workspace == "" {
		return core.Principal{}, false
	}
	email := core.NormalizeEmail(u.Profile.Email)
	live := email != "" && !u.Deleted && !u.IsBot && u.BotID == ""
	if !live || u.TeamID != workspace {
		return core.Principal{}, false
	}
	name := u.Profile.DisplayName
	if name == "" {
		name = u.Profile.RealName
	}
	if name == "" {
		name = u.Name
	}
	return core.SlackPrincipal(email, u.TeamID, name), true
}
