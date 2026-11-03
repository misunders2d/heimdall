package router

import (
	"context"
	"slices"
	"strings"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/googleoauth"
)

const googleNotice = "Employee Google sign-in is available in private DMs using Pi. Send `google login` in a DM; never paste the callback elsewhere."

// Controls bypass all durable queues, guard logs, sessions and model context.
// Eligibility is exactly employee mcp login's live/private/Pi/interface gate.
func (r *Router) googleControl(ctx context.Context, in core.Inbound, role core.Role) bool {
	text := strings.TrimSpace(in.Text)
	if strings.HasPrefix(text, "!pi ") {
		text = strings.TrimSpace(strings.TrimPrefix(text, "!pi "))
	}
	command := core.GoogleCommand(text)
	callback := core.GoogleCallback(text)
	if !callback && command == "" {
		return false
	}
	if in.Ambient {
		return true
	}
	reply := googleNotice
	eligible := role == core.RoleEmployee && slices.Contains(r.opt.EmployeeHarnesses, "pi") && in.Private && in.Origin == core.OriginLive && (in.Conv.Interface == "slack" || in.Conv.Interface == "telegram") && in.From.Platform == in.Conv.Interface
	if eligible && r.opt.Google == nil {
		reply = "Employee Google is not configured. Send `google login` in a DM after deployment."
	}
	if eligible && r.opt.Google != nil {
		caller := googleoauth.Caller{Principal: in.From, Actor: in.OAuthActor, Installation: in.OAuthInstallation}
		var err error
		switch {
		case callback:
			var email string
			email, err = r.opt.Google.Complete(ctx, caller, in.Conv, text)
			reply = "Google connected as " + email + "."
		case command == "google login":
			var link string
			link, err = r.opt.Google.Login(caller, in.Conv)
			reply = "Open this link and approve access with your own Google account:\n" + link + "\nThe final page may say Authorization failed; that is expected. Copy its entire address and paste it here in this same DM within 10 minutes. Do not post it elsewhere."
		case command == "google status":
			var email string
			email, err = r.opt.Google.Status(caller)
			reply = googleoauth.LoginNeeded
			if email != "" {
				reply = "Google connected as " + email + "."
			}
		case command == "google logout":
			err = r.opt.Google.Logout(caller)
			reply = "Google disconnected for future turns. Access already handed to an active turn expires with Google's token; stop that turn to end its access sooner."
		}
		if err != nil {
			reply = err.Error()
		}
		// Metadata only. Never include links, callback, codes or provider responses.
		r.log.Printf("employee Google control: caller=%s result_ok=%t", in.From.Key(), err == nil)
	}
	if iface := r.ifaces[in.Conv.Interface]; iface != nil {
		_ = iface.Send(ctx, core.Outbound{Conv: in.Conv, ReplyTo: in.MsgID, Text: reply})
	}
	return true
}
