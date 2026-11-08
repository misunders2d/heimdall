package telegram

import (
	"context"
	"io"
	"log"
	"os"
	osuser "os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/router"
	"github.com/misunders2d/heimdall/internal/store"
)

type quoteAccounts struct{ harness.Accounts }

func (a quoteAccounts) Prepare(_ core.Role, memory bool, bin string) (harness.Launch, error) {
	l, err := a.Accounts.Prepare(core.RoleOwner, memory, bin)
	l.Env = append(l.Env, "FAKE_WARM=1", "FAKE_CONTEXT_HOOK=1")
	return l, err
}

func TestTelegramSelectedQuoteThroughNativeClaudeContext(t *testing.T) {
	bin, state := fakeharness.Install(t, "claude")
	u, err := osuser.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MEMENTO_PROCESSING", "1")
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	api := newFakeAPI(t)
	var logs syncBuffer
	tg := newIface(t, api.srv.URL, botName, &logs)
	access := core.Access{Mode: core.ModeCorporate, Owners: map[string]bool{"telegram:42": true, "telegram:43": true}}
	tg.opt.Access = &access
	r := router.New(router.Options{Access: access, Order: []string{"claude"}, MaxParallel: 2, Accounts: quoteAccounts{harness.Accounts{RunAs: map[core.Role]core.RunAs{core.RoleOwner: {User: u.Username, Path: bin}}}}, Log: log.New(io.Discard, "", 0)}, st, tg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	quotes := []string{"new GitHub repos get created through the Zenbook", "  多行 😀\n keep\tspacing\n" + strings.Repeat("界", 1300) + "  "}
	for n, selected := range quotes {
		m := msg(101+n, 42, "private", 42, "why?")
		m["reply_to_message"] = msg(80+n, 42, "private", 999, "Restart competing topic. "+strings.Repeat("old ", 400)+selected)
		m["quote"] = map[string]any{"text": selected, "position": 3, "is_manual": true}
		api.push(upd(1+n, m))
		eventually(t, "native quote answer", func() bool { return len(api.sentBodies()) >= n+1 })
		inputs := fakeharness.Inputs(state)
		if len(inputs) != n+1 {
			t.Fatalf("inputs=%d", len(inputs))
		}
		in := inputs[n]
		q := selectedFromContext(t, in.Context)
		if in.Text != "why?" || !in.HookContext || q.Text != selected || q.ParentMessage != strconv.Itoa(80+n) || !strings.Contains(in.Context, "current message remains authoritative") {
			t.Fatalf("native quote input changed: text=%q hook=%v quoteEqual=%v", in.Text, in.HookContext, q.Text == selected)
		}
		t.Logf("native phase=%d prompt=%q UserPromptSubmit.additionalContext=true selected_bytes=%d decoded_quote_exact=true parent=%s", n+1, in.Text, len(q.Text), q.ParentMessage)
	}
	// A later ordinary reply on that warm process receives no stale selection.
	m := msg(103, 42, "private", 42, "ordinary")
	m["reply_to_message"] = msg(82, 42, "private", 999, "ordinary preview")
	api.push(upd(3, m))
	eventually(t, "ordinary answer", func() bool { return len(api.sentBodies()) >= 3 })
	inputs := fakeharness.Inputs(state)
	if strings.Contains(inputs[2].Context, quoteMarker) || !strings.Contains(inputs[2].Context, "ordinary preview") {
		t.Fatal("stale selected context leaked into ordinary reply")
	}
	// A different verified caller/conversation cannot inherit the selection.
	api.push(upd(4, msg(104, 43, "private", 43, "other caller")))
	eventually(t, "other caller answer", func() bool { return len(api.sentBodies()) >= 4 })
	inputs = fakeharness.Inputs(state)
	if strings.Contains(inputs[3].Context, quoteMarker) || strings.Contains(inputs[3].Context, "Zenbook") {
		t.Fatal("selected context crossed callers")
	}
	pids, _ := os.ReadFile(filepath.Join(state, "pids"))
	if len(strings.Fields(string(pids))) != 2 {
		t.Fatal("cold/warm process boundary not exercised")
	}
}
