package router

import (
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

type hookedAccounts struct{ Accounts }

func (a hookedAccounts) Prepare(role core.Role, memory bool, bin string) (harness.Launch, error) {
	l, err := a.Accounts.Prepare(role, memory, bin)
	l.Env = append(l.Env, "FAKE_CONTEXT_HOOK=1")
	return l, err
}

func TestWarmReceivesSentScheduledDelta(t *testing.T) {
	e := schedEnv(t, []string{"claude"}, "claude")
	e.stop()
	stopOnce.Delete(e.st)
	e.opt.Accounts = hookedAccounts{warmAccounts{e.opt.Accounts}}
	e.start()
	e.ask(ownerDM, "seed current task")
	id := e.addJob(`{"name":"review","schedule":{"every":"1h"},"harness":"claude","prompt":"` + say("SYNTHETIC_REVIEW_DELIVERED") + `"}`)
	e.ask(ownerDM, "!job run "+id)
	delivered := e.reply()
	want(t, delivered, "SAY=")
	eventuallyTrue(t, "delivery acknowledged", e.r.Idle)
	e.ask(group, "other conversation")
	foreign := fakeharness.Inputs(e.state)
	if strings.Contains(foreign[len(foreign)-1].Context, delivered.Text) {
		t.Fatal("private result leaked into another conversation")
	}
	e.ask(ownerDM, "what?")
	inputs := fakeharness.Inputs(e.state)
	last := inputs[len(inputs)-1]
	if !last.HookContext || last.Text != "what?" || !strings.Contains(last.Context, delivered.Text) || !strings.Contains(last.Context, "historical") {
		t.Fatalf("missing delivered delta or changed current input: %+v", last)
	}

	e.ask(ownerDM, "next question")
	inputs = fakeharness.Inputs(e.state)
	if strings.Contains(inputs[len(inputs)-1].Context, delivered.Text) {
		t.Fatal("delivered delta repeated")
	}
}

func TestDeliveredContextRejectsNonPersonScopes(t *testing.T) {
	c := &conv{}
	for _, kind := range []string{"employee", "shared", "scheduled", "mcp", "ambient", "interface notice"} {
		t.Run(kind, func(t *testing.T) {
			tr := &turn{role: core.RoleOwner, private: true}
			in := core.Inbound{Origin: core.OriginLive}
			switch kind {
			case "employee":
				tr.role = core.RoleEmployee
			case "shared":
				tr.private = false
			case "scheduled":
				in.Origin = core.OriginScheduled
			case "mcp":
				in.MCPControl = true
			case "ambient":
				in.Ambient = true
			case "interface notice":
				in.InterfaceReply = "notice"
			}
			if c.deliveredContext(tr, "claude", in) != nil {
				t.Fatal("historical context enabled outside direct owner person scope")
			}
		})
	}
}
