package slack

import (
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

const workspace = "T0WORK"

func member(email string) *User {
	u := &User{ID: "U1", TeamID: workspace, Name: "member"}
	u.Profile.Email = email
	return u
}

func TestVerifyMemberOfOwnWorkspace(t *testing.T) {
	p, ok := Verify(member(" Worker@Example.com"), workspace)
	if !ok || p.Key() != "slack:worker@example.com" || p.Team != workspace || p.Email != "worker@example.com" {
		t.Fatalf("Verify = %+v, %v", p, ok)
	}
	if role, ok := (core.Access{Owners: map[string]bool{}}).Role(p); !ok || role != core.RoleEmployee {
		t.Fatalf("verified member: role %q, %v; want non-root", role, ok)
	}
}

func TestVerifyRejects(t *testing.T) {
	guest := member("guest@other.example")
	guest.TeamID = "T0OTHER"
	deleted := member("gone@example.com")
	deleted.Deleted = true
	bot := member("bot@example.com")
	bot.IsBot = true
	app := member("app@example.com")
	app.BotID = "B1"
	for name, u := range map[string]*User{
		"guest from another workspace": guest,
		"deleted user":                 deleted,
		"bot":                          bot,
		"bot ID":                       app,
		"no live email":                member(""),
		"lookup failed":                nil,
	} {
		if p, ok := Verify(u, workspace); ok {
			t.Errorf("%s: verified as %+v", name, p)
		}
	}
	if _, ok := Verify(member("worker@example.com"), ""); ok {
		t.Error("verified without knowing the bot's workspace")
	}
}

func TestEarlierEmailIsNotProof(t *testing.T) {
	if _, ok := Verify(member("worker@example.com"), workspace); !ok {
		t.Fatal("first message not verified")
	}
	// The same user's next lookup returns no email or fails: what an
	// earlier lookup said does not count.
	for _, u := range []*User{member(""), nil} {
		if p, ok := Verify(u, workspace); ok {
			t.Fatalf("verified from an earlier email: %+v", p)
		}
	}
}
