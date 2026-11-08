package router

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestAliasedCheckpointResultReachesWarmRecipient(t *testing.T) {
	for _, kind := range []string{"linked owner", "foreign owner", "revoked owner", "stale alias", "stale workspace", "stale actor", "stale installation", "missing source binding"} {
		t.Run(kind, func(t *testing.T) {
			e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
			e.stop()
			stopOnce.Delete(e.st)
			dir := t.TempDir()
			write := func(name string, v any) {
				raw, _ := json.Marshal(v)
				if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"whitelist.json", "blacklist.json", "admins.json", "capabilities.json"} {
				write(name, map[string]any{})
			}
			identities := map[string]any{"principals": map[string]any{"owner@example.test": map[string]any{"aliases": []string{"tg_user_101"}}}}
			write("identities.json", identities)
			t.Setenv("ADMIN_USER_IDS", "101,foreign@example.test")
			e.opt.Access = core.Access{Mode: core.ModeCorporate, Dir: dir}
			e.opt.Accounts = hookedAccounts{warmAccounts{e.opt.Accounts}}
			e.platform = "telegram"
			e.start()
			recipient := core.Principal{Platform: core.PlatformTelegram, ID: "101"}
			target := core.ConvKey{Interface: core.PlatformTelegram, Chat: "101"}
			ask := func(text string) {
				e.f.in <- core.Inbound{Conv: target, From: recipient, Origin: core.OriginLive, Private: true, MsgID: fmt.Sprint(e.seq.Add(1)), Text: text}
				e.reply()
			}
			ask("seed current task")
			source := core.SlackPrincipal("owner@example.test", "Tsource", "")
			if kind == "foreign owner" {
				source = core.SlackPrincipal("foreign@example.test", "Tsource", "")
			}
			live := core.Inbound{Conv: core.ConvKey{Interface: "slack", Chat: "Dsource"}, From: source, Origin: core.OriginLive, Private: true, MsgID: "source-live", OAuthActor: "sl_user_source", OAuthInstallation: "sl_user_bot"}
			if kind != "missing source binding" {
				if _, _, err := e.st.AddInbound(live); err != nil {
					t.Fatal(err)
				}
			}
			job := store.Job{ID: "jalias", Creator: source, CreatedIn: live.Conv, CreatedPrivate: true, State: store.JobActive, Def: `{"prompt":"PRIVATE_JOB_INSTRUCTION"}`}
			if err := e.st.AnswerCommand(0, live.Conv, store.CommandEnd{Job: &store.JobChange{Add: &job}}); err != nil {
				t.Fatal(err)
			}
			if _, err := e.st.CreateRun(store.Run{Job: job.ID, Version: 1, Slot: "slot", Due: time.Now(), State: store.RunPending}, "", time.Time{}); err != nil {
				t.Fatal(err)
			}
			runs, err := e.st.Runs(store.RunFilter{Job: job.ID})
			if err != nil || len(runs) != 1 {
				t.Fatal(runs, err)
			}
			in := live
			in.Conv = core.ConvKey{Interface: "slack", Chat: "sched:" + job.ID}
			in.Origin = core.OriginScheduled
			in.Private = false
			in.MsgID = "run"
			in.Text = "PRIVATE_JOB_INSTRUCTION"
			in.At = time.Now()
			row, err := e.st.StartRun(runs[0], in)
			if err != nil {
				t.Fatal(err)
			}
			turn, err := e.st.StartTurn(in.Conv, core.RoleOwner, source.Key(), "", []int64{row.ID})
			if err != nil {
				t.Fatal(err)
			}
			const body = "Synthetic review delivered. No action needed."
			update := &store.CheckpointUpdate{Complete: true, Through: time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), State: json.RawMessage(`{}`), Target: live.Conv, Senders: []string{recipient.Key(), source.Key()}, Alerts: []store.Alert{{Key: "review", Revision: "v1", Text: body, To: "tg:101", Conv: target}}}
			if _, err := e.st.EndTurn(turn, in.Conv, source.Key(), store.TurnEnd{State: store.TurnDone, ItemState: store.InDone, Run: &store.RunEnd{ID: runs[0].ID, Job: job.ID, State: store.RunOK}, Checkpoint: update}); err != nil {
				t.Fatal(err)
			}
			e.r.wake(target.Interface)
			want(t, e.reply(), body)
			eventuallyTrue(t, "confirmed checkpoint receipt", e.r.Idle)
			switch kind {
			case "revoked owner":
				t.Setenv("ADMIN_USER_IDS", "101")
				write("identities.json", map[string]any{})
			case "stale alias":
				t.Setenv("ADMIN_USER_IDS", "101,owner@example.test")
				write("identities.json", map[string]any{})
			case "stale workspace", "stale actor", "stale installation":
				live.MsgID = "new-source-binding"
				if kind == "stale workspace" {
					live.From.Team = "Tother"
				}
				if kind == "stale actor" {
					live.OAuthActor = "sl_user_other"
				}
				if kind == "stale installation" {
					live.OAuthInstallation = "sl_user_other_bot"
				}
				if _, _, err := e.st.AddInbound(live); err != nil {
					t.Fatal(err)
				}
			}
			ask("what?")
			inputs := fakeharness.Inputs(e.state)
			last := inputs[len(inputs)-1]
			contains := strings.Contains(last.Context, body)
			if last.Text != "what?" || !last.HookContext || contains != (kind == "linked owner") {
				t.Fatalf("%s native input=%+v", kind, last)
			}
			if strings.Contains(last.Context, "PRIVATE_JOB_INSTRUCTION") || strings.Contains(last.Context, "sl_user_source") || strings.Contains(last.Context, "Tsource") {
				t.Fatal("source instructions/account authority imported")
			}
		})
	}
}
