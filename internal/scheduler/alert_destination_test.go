package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

// destRig is a checkpoint supervisor job of boss (Telegram) with Pi access
// files: boss's own Slack identity is an alias; other@ is a different owner.
func destRig(t *testing.T) (*rig, string, core.Principal, core.Principal) {
	t.Helper()
	r := newRig(t)
	r.s.cfg.Interfaces = append(r.s.cfg.Interfaces, core.PlatformSlack)
	dir := t.TempDir()
	write := func(name string, v any) {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("whitelist.json", map[string]any{"users": []string{"staff@example.test"}})
	write("blacklist.json", map[string]any{})
	write("admins.json", map[string]any{})
	write("identities.json", map[string]any{"principals": map[string]any{"owner@example.test": map[string]any{"aliases": []string{"tg_user_1"}}}})
	write("capabilities.json", map[string]any{})
	t.Setenv("ADMIN_USER_IDS", "1,other@example.test")
	r.s.access = core.Access{Mode: core.ModeCorporate, Dir: dir}
	self := core.SlackPrincipal("owner@example.test", "Ttest", "")
	other := core.SlackPrincipal("other@example.test", "Ttest", "")
	id := r.add(asBoss, `{"name":"supervisor","schedule":{"every":"30m"},"harness":"codex","prompt":"synthetic","quiet":true,"checkpoint":true}`)
	return r, id, self, other
}

func ingress(t *testing.T, r *rig, k core.ConvKey, p core.Principal, private bool, origin string) {
	t.Helper()
	if _, _, err := r.st.AddInbound(core.Inbound{Conv: k, From: p, Origin: origin, Private: private, MsgID: fmt.Sprint(time.Now().UnixNano())}); err != nil {
		t.Fatal(err)
	}
}

func (r *rig) scan(id string, alerts []store.Alert) store.TurnEnd {
	r.t.Helper()
	r.run(id)
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil {
		r.t.Fatal(err)
	}
	return r.finish(checkpointAnswer(cp.Version, true, r.now.UTC().Format(time.RFC3339Nano), alerts), Outcome{Harness: "codex"})
}

func drain(r *rig, iface string) []store.OutboundRow {
	var out []store.OutboundRow
	for {
		row, ok, err := r.st.NextOutbound(iface)
		if err != nil || !ok {
			return out
		}
		_ = r.st.Sent(row.ID)
		out = append(out, row)
	}
}

// Spoofed, foreign, shared or unverifiable destinations never route; the scan
// fails visibly and commits no state, receipt or outbound row.
func TestAlertDestinationRefusals(t *testing.T) {
	cases := []struct {
		name, to string
		seed     func(*rig, core.Principal, core.Principal)
	}{
		{"no_ingress_spoofed_chat", "tg:999", nil},
		{"foreign_user_private_chat", "tg:2", func(r *rig, _, _ core.Principal) { ingress(r.t, r, workDM, worker, true, core.OriginLive) }},
		{"group_with_creator_ingress", "tg:-100", func(r *rig, _, _ core.Principal) { ingress(r.t, r, team, boss, false, core.OriginLive) }},
		{"other_owner_private_dm", "slack:DOTHER/1000.1", func(r *rig, _, o core.Principal) {
			ingress(r.t, r, core.ConvKey{Interface: "slack", Chat: "DOTHER", Thread: "1000.1"}, o, true, core.OriginLive)
		}},
		{"shared_slack_thread_creator_alias", "slack:CSHARED/1000.2", func(r *rig, s, _ core.Principal) {
			ingress(r.t, r, core.ConvKey{Interface: "slack", Chat: "CSHARED", Thread: "1000.2"}, s, false, core.OriginLive)
		}},
		{"wrong_thread", "slack:DSELF/1000.9", func(r *rig, s, _ core.Principal) {
			ingress(r.t, r, core.ConvKey{Interface: "slack", Chat: "DSELF", Thread: "1000.3"}, s, true, core.OriginLive)
		}},
		{"scheduled_not_live", "tg:1/5", func(r *rig, _, _ core.Principal) {
			ingress(r.t, r, core.ConvKey{Interface: "telegram", Chat: "1", Thread: "5"}, boss, true, core.OriginScheduled)
		}},
		{"malformed", "slack:bad id", nil},
		{"agentnet", "agentnet:peer", nil},
		{"here", "here", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, id, self, other := destRig(t)
			if tc.seed != nil {
				tc.seed(r, self, other)
			}
			e := r.scan(id, []store.Alert{{Key: "MEL-1", Revision: "delivered", Text: "Result: synthetic.", To: tc.to}})
			cp, err := r.st.Checkpoint(id, boss.Key())
			if err != nil || e.Run.State != store.RunFailed || cp.Version != 0 || len(cp.Receipts) != 0 {
				t.Fatal("refused destination committed", e.Run, cp, err)
			}
			if outs := drain(r, core.PlatformSlack); len(outs) != 0 {
				t.Fatal("slack row queued", outs)
			}
			outs := drain(r, core.PlatformTelegram)
			if len(outs) != 1 || outs[0].Conv != bossDM || strings.Contains(outs[0].Text, "synthetic.") {
				t.Fatal("want one fixed failure notice to the creator only", outs)
			}
		})
	}
}

// The creator's own private conversations (Telegram topic, aliased Slack DM
// thread) receive results with outbox receipts naming them; duplicates,
// restarts and failed sends neither re-send nor fall back to the job target.
func TestAlertDestinationOriginalConversation(t *testing.T) {
	r, id, self, _ := destRig(t)
	tgTopic := core.ConvKey{Interface: "telegram", Chat: "1", Thread: "3"}
	slackDM := core.ConvKey{Interface: "slack", Chat: "DSELF", Thread: "1000.3"}
	ingress(t, r, tgTopic, boss, true, core.OriginLive)
	ingress(t, r, slackDM, self, true, core.OriginLive)
	alerts := []store.Alert{
		{Key: "MEL-754", Revision: "delivered", Text: "Dashboard result (synthetic).", To: "tg:1/3"},
		{Key: "MEL-755", Revision: "delivered", Text: "Repair result (synthetic).", To: "slack:DSELF/1000.3"},
		{Key: "MEL-756", Revision: "blocked", Text: "Decision needed (synthetic)."},
	}
	if e := r.scan(id, alerts); e.Run.State != store.RunOK {
		t.Fatal(e.Run)
	}
	cp, _ := r.st.Checkpoint(id, boss.Key())
	want := map[string]string{"MEL-754": "tg:1/3", "MEL-755": "slack:DSELF/1000.3", "MEL-756": "tg:1"}
	if len(cp.Receipts) != 3 {
		t.Fatal(cp)
	}
	for _, rc := range cp.Receipts {
		if rc.To != want[rc.Key] || rc.Status != store.OutPending {
			t.Fatal("receipt destination", rc)
		}
	}
	ids := map[int64]string{}
	for _, rc := range cp.Receipts {
		ids[rc.Outbound] = rc.To
	}
	// Pending dedupe: same key/revision with swapped (still verified) to.
	swapped := append([]store.Alert(nil), alerts...)
	swapped[0].To, swapped[1].To = "slack:DSELF/1000.3", "tg:1/3"
	if e := r.scan(id, swapped); e.Run.State != store.RunQuiet && e.Run.State != store.RunOK {
		t.Fatal(e.Run)
	}
	again, _ := r.st.Checkpoint(id, boss.Key())
	if len(again.Receipts) != 3 {
		t.Fatal("pending duplicate created a row", again)
	}
	for _, rc := range again.Receipts {
		if ids[rc.Outbound] != rc.To || rc.To != want[rc.Key] || rc.Status != store.OutPending {
			t.Fatal("pending duplicate re-routed or falsified", rc)
		}
	}
	// The destination is delivery only: the supervisor's own turn stays the
	// creator's scheduled conversation, never the destination's caller/session.
	if row := r.rows[len(r.rows)-1]; row.From.Key() != boss.Key() || row.Conv == tgTopic || row.Conv == slackDM {
		t.Fatal("destination conferred turn authority", row.From, row.Conv)
	}
	tg := drain(r, core.PlatformTelegram)
	sl, _, _ := r.st.NextOutbound(core.PlatformSlack)
	if len(tg) != 2 || sl.Conv != slackDM || sl.Text != "Repair result (synthetic)." || ids[sl.ID] != "slack:DSELF/1000.3" {
		t.Fatal("wrong conversations", tg, sl)
	}
	for _, row := range tg {
		if ids[row.ID] != store.ConvTarget(row.Conv) {
			t.Fatal("receipt does not name its actual outbox row", row.ID, row.Conv, ids)
		}
	}
	_ = r.st.Failed(sl.ID, "synthetic permanent failure")

	// Restart, then the same key/revision again, even naming another chat.
	if err := r.st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(r.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	r.st, r.s.st = reopened, reopened
	alerts[0].To, alerts[1].To = "slack:DSELF/1000.3", "tg:1/3" // verified, but swapped
	if e := r.scan(id, alerts); e.Run.State != store.RunQuiet && e.Run.State != store.RunOK {
		t.Fatal(e.Run)
	}
	cp, _ = r.st.Checkpoint(id, boss.Key())
	for _, rc := range cp.Receipts {
		if rc.To != want[rc.Key] {
			t.Fatal("duplicate re-routed", rc)
		}
		if rc.Key == "MEL-755" && rc.Status != store.OutFailed {
			t.Fatal("failed send hidden or replayed", rc)
		}
		if rc.Key != "MEL-755" && rc.Status != store.OutSent {
			t.Fatal("sent receipt lost", rc)
		}
	}
	if len(cp.Receipts) != 3 {
		t.Fatal("duplicate created a row", cp)
	}
	if outs := drain(r, core.PlatformSlack); len(outs) != 0 {
		t.Fatal("failed slack row replayed", outs)
	}
	for _, o := range drain(r, core.PlatformTelegram) {
		if strings.Contains(o.Text, "(synthetic).") {
			t.Fatal("duplicate re-sent", o)
		}
	}
}

// A revoked creator cannot use destinations even to its own chats.
func TestAlertDestinationRevokedCreator(t *testing.T) {
	r, id, _, _ := destRig(t)
	tgTopic := core.ConvKey{Interface: "telegram", Chat: "1", Thread: "3"}
	ingress(t, r, tgTopic, boss, true, core.OriginLive)
	r.run(id)
	t.Setenv("ADMIN_USER_IDS", "other@example.test")
	if got := r.s.senders(id); len(got) != 0 {
		t.Fatal("revoked creator keeps destinations", got)
	}
}

// One unverifiable destination rolls back the whole scan, including a valid
// alert before it: no checkpoint, receipt or outbound row.
func TestAlertDestinationInvalidAfterValidRollsBack(t *testing.T) {
	r, id, _, _ := destRig(t)
	ingress(t, r, core.ConvKey{Interface: "telegram", Chat: "1", Thread: "3"}, boss, true, core.OriginLive)
	e := r.scan(id, []store.Alert{
		{Key: "MEL-754", Revision: "delivered", Text: "Valid (synthetic).", To: "tg:1/3"},
		{Key: "MEL-999", Revision: "x", Text: "Injected (synthetic).", To: "tg:999"},
	})
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil || e.Run.State != store.RunFailed || cp.Version != 0 || len(cp.Receipts) != 0 {
		t.Fatal("partial commit", e.Run, cp, err)
	}
	for _, o := range drain(r, core.PlatformTelegram) {
		if strings.Contains(o.Text, "(synthetic).") {
			t.Fatal("alert escaped rollback", o)
		}
	}
}

// The store rechecks in the commit transaction: a destination that passed the
// scheduler's read is still refused if the committing senders do not match.
func TestAlertDestinationStoreRecheck(t *testing.T) {
	r, _, _, _ := destRig(t)
	k := core.ConvKey{Interface: "telegram", Chat: "2"}
	ingress(t, r, k, worker, true, core.OriginLive)
	if err := r.st.OwnerIngress(k, []string{boss.Key()}); err == nil {
		t.Fatal("foreign private chat admitted")
	}
	if err := r.st.OwnerIngress(k, nil); err == nil {
		t.Fatal("empty senders admitted")
	}
	if err := r.st.OwnerIngress(core.ConvKey{}, []string{worker.Key()}); err == nil {
		t.Fatal("empty destination admitted")
	}
}

// A stale alias: the creator's Slack DM ingress was valid, then the alias
// mapping is removed while the Telegram creator stays owner. The actual scan
// path must refuse it with no checkpoint, receipt or result outbox row.
func TestAlertDestinationStaleAliasScan(t *testing.T) {
	r, id, self, _ := destRig(t)
	slackDM := core.ConvKey{Interface: "slack", Chat: "DSELF", Thread: "1000.3"}
	ingress(t, r, slackDM, self, true, core.OriginLive)
	if got := r.s.senders(id); len(got) != 2 {
		t.Fatal("alias not admitted before removal", got)
	}
	b, _ := json.Marshal(map[string]any{"principals": map[string]any{}})
	if err := os.WriteFile(filepath.Join(r.s.access.Dir, "identities.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if got := r.s.senders(id); len(got) != 1 || got[0] != boss.Key() {
		t.Fatal("creator must remain the only sender", got)
	}
	e := r.scan(id, []store.Alert{{Key: "MEL-755", Revision: "delivered", Text: "Stale alias result (synthetic).", To: "slack:DSELF/1000.3"}})
	cp, err := r.st.Checkpoint(id, boss.Key())
	if err != nil || e.Run.State != store.RunFailed || cp.Version != 0 || len(cp.Receipts) != 0 {
		t.Fatal("stale alias destination committed", e.Run, cp, err)
	}
	if outs := drain(r, core.PlatformSlack); len(outs) != 0 {
		t.Fatal("slack result row queued", outs)
	}
	outs := drain(r, core.PlatformTelegram)
	if len(outs) != 1 || outs[0].Conv != bossDM || strings.Contains(outs[0].Text, "(synthetic).") {
		t.Fatal("want one fixed failure notice to the creator only", outs)
	}
}
