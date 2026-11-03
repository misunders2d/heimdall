package slack

import (
	"context"
	"errors"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/scheduler"
)

// Reverify checks a scheduled job's Slack creator live at each fire
// (docs/scheduler.md §4), exactly as a message's sender: users.info for the
// creator's Slack user must return the same email, live and human, in the
// bot's own workspace. ok false refuses the run; an error is temporary.
func (s *Slack) Reverify(parent context.Context, p core.Principal) (scheduler.Verified, bool, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	if err := s.await(ctx); err != nil {
		return scheduler.Verified{}, false, err
	}
	email := core.NormalizeEmail(p.Email)
	if p.Platform != core.PlatformSlack || email == "" || p.Team != s.opt.TeamID {
		return scheduler.Verified{}, false, nil
	}
	id := p.SlackUserID
	if id == "" {
		var found struct {
			User User `json:"user"`
		}
		if err := s.bot.call(ctx, "users.lookupByEmail", map[string]string{"email": email}, &found); err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.Code == "users_not_found" {
				return scheduler.Verified{}, false, nil
			}
			return scheduler.Verified{}, false, err
		}
		id = found.User.ID
	}
	u, err := s.liveUser(ctx, id)
	if err != nil {
		return scheduler.Verified{}, false, err
	}
	live, ok := Verify(u, s.opt.TeamID)
	if !ok || live.Email != email {
		return scheduler.Verified{}, false, nil
	}
	return scheduler.Verified{Principal: live, OAuthActor: "sl_user_" + u.ID, OAuthInstallation: "sl_user_" + s.botID}, true, nil
}
