package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/schedule"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestReadableRequestAndReceiptGoldens(t *testing.T) {
	for _, platform := range []string{core.PlatformTelegram, core.PlatformSlack} {
		for _, paused := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/paused=%t", platform, paused), func(t *testing.T) {
				r := newRig(t)
				r.s.cfg.Interfaces = []string{core.PlatformTelegram, core.PlatformSlack}
				c := asBoss
				if platform == core.PlatformSlack {
					c.Principal = core.SlackPrincipal("owner@example.test", "T1", "U1")
					c.Conv = core.ConvKey{Interface: platform, Chat: "D1", Thread: "1000.1"}
				}
				raw := mustJSON(Def{Name: "Drive watch", Summary: "Review new requests.", Schedule: schedule.Spec{Every: "2w", At: "09:00", From: "2026-11-20"}, To: "tg:99", Quiet: true, Paused: paused, Prompt: "PRIVATE INSTRUCTIONS", Until: "2027-01-01"})
				_, j, card := r.s.Draft(c, []string{"```heimdall-job\n" + raw + "\n```"})
				if j == nil {
					t.Fatal(card)
				}
				want := "*📅 Schedule task?*\n\n*Drive watch*\nReview new requests.\n\n*🕒 When:* Every 2 weeks at 09:00, starting 20 Nov 2026 · Europe/Kyiv\n*📬 Delivery:* Telegram"
				if paused {
					want += "\n\n*⏸️ Starts paused*. No automatic checks."
				}
				want += "\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.\n— heimdall"
				if platform == core.PlatformSlack {
					want = strings.ReplaceAll(want, "*", "**")
				}
				if card != want {
					t.Fatalf("approval golden\nwant:\n%s\ngot:\n%s", want, card)
				}
				r.apply(&store.JobChange{Add: j})
				out, ok := r.answer(c, "Yes")
				want = "*📅 Scheduled task created*\n\n*Drive watch*\nReview new requests.\n\n*🕒 When:* Every 2 weeks at 09:00, starting 20 Nov 2026 · Europe/Kyiv\n*📬 Delivery:* Telegram\n\n*🟢 Active*.\n*Next scheduled check:* 20 Nov 2026 at 09:00 · Europe/Kyiv"
				if paused {
					want = "*📅 Scheduled task created*\n\n*Drive watch*\nReview new requests.\n\n*🕒 When:* Every 2 weeks at 09:00, starting 20 Nov 2026 · Europe/Kyiv\n*📬 Delivery:* Telegram\n\n*⏸️ Paused*; no automatic checks."
				}
				if platform == core.PlatformSlack {
					want = strings.ReplaceAll(want, "*", "**")
				}
				if !ok || out != want {
					t.Fatalf("receipt golden\nwant:\n%s\ngot:\n%s", want, out)
				}
				list := r.cmd(c, "jobs")
				if wantList := strings.Replace(out, "📅 Scheduled task created", "📋 Scheduled task status", 1); list != wantList {
					t.Fatalf("list lost paragraph hierarchy:\n%s", list)
				}
				for _, bad := range []string{"PRIVATE INSTRUCTIONS", "Routine checks", "successful test", "2027-01-01", j.ID, "your DM", "Your private"} {
					if strings.Contains(card+out+list, bad) {
						t.Fatalf("unexpected boilerplate or private data %q", bad)
					}
				}
				if paused && strings.Contains(out+list, "Next scheduled") {
					t.Fatal("paused job advertised an automatic run")
				}
				if details := r.cmd(c, "job show "+j.ID); !strings.Contains(details, "PRIVATE INSTRUCTIONS") || !strings.Contains(details, "2027-01-01") {
					t.Fatal("operator details lost", details)
				}
			})
		}
	}
}

func TestReadableRenewalReceiptKeepsNewEndTime(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"Drive watch","schedule":{"every":"1h"},"prompt":"PRIVATE","paused":true}`)
	out := r.cmd(asBoss, "job renew "+id+" 30d")
	want := "*📅 Scheduled task renewed*\n\n*Drive watch*\n\n*🕒 When:* Every 1 hour · Europe/Kyiv\n*📬 Delivery:* This private conversation\n*Ends:* 3 Dec 2026 at 12:00 · Europe/Kyiv\n\n*⏸️ Paused*; no automatic checks."
	if out != want {
		t.Fatalf("renewal golden\nwant:\n%s\ngot:\n%s", want, out)
	}
	d, _, _ := parse(r.job(id))
	if d.Until != "2026-12-03T10:00:00Z" || r.job(id).State != store.JobPaused {
		t.Fatal("renewal changed state or lost expiry", r.job(id))
	}
}

func TestReadableEditKeepsMaterialChangesAndPausedState(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"Drive watch","summary":"Review new requests.","schedule":{"every":"1h"},"prompt":"PRIVATE","paused":true}`)
	_, j, card := r.s.Draft(asBoss, []string{"```heimdall-job\n" + `{"edit":"` + id + `","schedule":{"every":"2h"},"to":"tg:99","quiet":true,"prompt":"PRIVATE CHANGED","timeout":"2m"}` + "\n```"})
	if j == nil {
		t.Fatal(card)
	}
	want := "*📝 Update scheduled task?*\n\n*Drive watch*\nReview new requests.\n\n*🕒 When:* Every 1 hour · Europe/Kyiv → Every 2 hours · Europe/Kyiv\n*📬 Delivery:* This private conversation → Telegram\n\n*📋 Changes*\nSilent when no update: false → true\n*Timeout:* Configured limit → 2m\nInstructions updated. Details shows the full text.\n\n*⏸️ Still paused*. No automatic checks.\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.\n— heimdall"
	if card != want {
		t.Fatalf("edit golden\nwant:\n%s\ngot:\n%s", want, card)
	}
	r.apply(&store.JobChange{Add: j})
	out, ok := r.answer(asBoss, "Yes")
	want = "*📝 Scheduled task updated*\n\n*Drive watch*\nReview new requests.\n\n*🕒 When:* Every 1 hour · Europe/Kyiv → Every 2 hours · Europe/Kyiv\n*📬 Delivery:* This private conversation → Telegram\n\n*📋 Changes*\nSilent when no update: false → true\n*Timeout:* Configured limit → 2m\nInstructions updated. Details shows the full text.\n\n*⏸️ Paused*; no automatic checks."
	if !ok || out != want {
		t.Fatalf("edit receipt golden\nwant:\n%s\ngot:\n%s", want, out)
	}
	if r.job(id).State != store.JobPaused {
		t.Fatal("UI edit resumed paused job")
	}
}

func TestReadableMaterialChangesKeepSeparateLinesAndPrivacy(t *testing.T) {
	r := newRig(t)
	a := Def{Name: "Old name", Summary: "Old purpose", Prompt: "PRIVATE OLD"}
	b := Def{Name: "New name", Summary: "New purpose", Prompt: "PRIVATE NEW", Until: "2027-01-01", Quiet: true, Timeout: "2m", RerunOnTimeout: true, Harness: "codex"}
	want := "*Name:* Old name → New name\n*Purpose:* Old purpose → New purpose\n*Ends:* No expiry → 2027-01-01\nSilent when no update: false → true\n*Timeout:* Configured limit → 2m\nInstructions updated. Details shows the full text.\nRepeat-safe timeout rerun: false → true\nExecution or delivery settings: changed (see Details)."
	if got := r.s.material(asBoss, a, b); got != want {
		t.Fatalf("material changes\nwant:\n%s\ngot:\n%s", want, got)
	}
	shared := r.s.material(inGroup, a, b)
	if want = strings.Join(strings.Split(want, "\n")[2:], "\n"); shared != want {
		t.Fatalf("shared changes lost privacy or material settings:\n%s", shared)
	}
	if got := r.s.material(asBoss, Def{Text: "PRIVATE OLD"}, Def{Text: "PRIVATE NEW"}); got != "Reminder text updated. Details shows the full text." {
		t.Fatal(got)
	}
}

func TestConciseRequestDetailsAndState(t *testing.T) {
	r := newRig(t)
	if _, _, ok := r.s.Answer(asBoss, "Details"); ok {
		t.Fatal("unrelated Details intercepted")
	}
	raw := `{"name":"Monitor *x* _y_ [click](https://example.test) <@U1>","summary":"Find requests needing attention.","schedule":{"cron":["0,30 12-20 * * MON-FRI","45 20 * * MON-FRI"]},"prompt":"PRIVATE PROMPT","paused":true}`
	j, err := r.s.proposal(asBoss, []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	card := r.s.card(asBoss, j)
	for _, want := range []string{"*📅 Schedule task?*", "Find requests needing attention.", "12:00–20:30, plus 20:45", "Europe/Kyiv", "This private conversation", "Starts paused", "*Yes*", "*No*", "*Details*", `\**x*\*`, `\[*click*\]`} {
		if !strings.Contains(card, want) {
			t.Fatalf("missing %q: %s", want, card)
		}
	}
	for _, bad := range []string{j.ID, "PRIVATE PROMPT", "Next", "!job", "harness", "0,30", "job draft"} {
		if strings.Contains(card, bad) {
			t.Fatalf("leak %q: %s", bad, card)
		}
	}
	r.apply(&store.JobChange{Add: &j})
	if text, _, ok := r.s.Answer(asBoss, "Details"); !ok || !strings.Contains(text, "PRIVATE PROMPT") || !strings.Contains(text, "Cron:") {
		t.Fatal(text, ok)
	}
	for _, c := range []Caller{asWork, inGroup} {
		if _, _, ok := r.s.Answer(c, "Details"); ok {
			t.Fatal("foreign Details accepted")
		}
	}
	if _, _, ok := r.s.Answer(asBoss, "Yes?"); ok {
		t.Fatal("question confirmed")
	}
	out, ok := r.answer(asBoss, "Yes")
	if !ok || r.job(j.ID).State != store.JobPaused || strings.Contains(out, "Next") || !strings.Contains(out, "⏸️ Paused") {
		t.Fatal(out)
	}
	show := r.cmd(asBoss, "job show "+j.ID)
	if strings.Contains(show, "Next:") {
		t.Fatal("paused projection", show)
	}
	out = r.cmd(asBoss, "job resume "+j.ID)
	if !Formatted(out) || !strings.Contains(out, "Next scheduled check:") {
		t.Fatal(out)
	}
	if text, _, ok := r.s.Answer(asBoss, "Details"); ok {
		t.Fatal("consumed request still bound", text)
	}
}

func TestConciseEditRemovalSharedAndAmbiguity(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"Private name","summary":"Private purpose","schedule":{"every":"1h"},"prompt":"PRIVATE"}`)
	j, err := r.s.proposal(asBoss, []byte(`{"edit":"`+id+`","schedule":{"every":"2h"},"prompt":"PRIVATE CHANGED"}`))
	if err != nil {
		t.Fatal(err)
	}
	card := r.s.card(asBoss, j)
	if !strings.Contains(card, "Every 1 hour · Europe/Kyiv → Every 2 hours · Europe/Kyiv") || strings.Contains(card, "PRIVATE CHANGED") {
		t.Fatal(card)
	}
	group := r.s.card(inGroup, j)
	if strings.Contains(group, "Private name") || strings.Contains(group, "Private purpose") || strings.Contains(group, "PRIVATE") {
		t.Fatal(group)
	}
	r.apply(&store.JobChange{Add: &j})
	removal, err := r.s.proposal(asBoss, []byte(`{"remove":"`+id+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	r.apply(&store.JobChange{Add: &removal})
	for _, w := range []string{"Yes", "No", "Details"} {
		text, change, ok := r.s.Answer(asBoss, w)
		if !ok || change != nil || !strings.Contains(text, "Several requests") || strings.Contains(text, id) {
			t.Fatal(w, text)
		}
	}
	r.cmd(asBoss, "job discard "+j.ID)
	card = r.s.card(asBoss, removal)
	if !strings.Contains(card, "Future checks and pending retries will stop") || strings.Contains(card, id) {
		t.Fatal(card)
	}
	if details, _, ok := r.s.Answer(asBoss, "Details"); !ok || !strings.Contains(details, "PRIVATE") {
		t.Fatal(details)
	}
	out, ok := r.answer(asBoss, "Yes")
	if !ok || !strings.Contains(out, "Already delivered messages remain") || r.job(id).State != store.JobDeleted {
		t.Fatal(out)
	}
}

func TestControlSourceIsProtectedRealIngressBinding(t *testing.T) {
	r, c := controlRig(t)
	source := bossDM
	source.Thread = "10"
	_, _, err := r.st.AddInbound(core.Inbound{Conv: source, From: boss, Origin: core.OriginLive, Private: true, MsgID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	other := bossDM
	other.Thread = "20"
	_, _, _ = r.st.AddInbound(core.Inbound{Conv: other, From: boss, Origin: core.OriginLive, Private: true, MsgID: "unrelated-newer"})
	c.source = &source
	got := c.executeBound(context.Background(), 1234, ControlRequest{Action: "create", RequestID: "bound", Definition: json.RawMessage(`{"name":"monitor","schedule":{"every":"1h"},"prompt":"synthetic","paused":true}`)})
	if got.Error != "" || r.job(got.DraftID).CreatedIn != source {
		t.Fatal(got)
	}
	outs := r.sent()
	if len(outs) != 1 || outs[0].Conv != source || !outs[0].Formatted {
		t.Fatal(outs)
	}
	if _, _, ok := r.s.Answer(asBoss, "Yes"); ok {
		t.Fatal("confirmed in unrelated topic")
	}
	missing := source
	missing.Thread = "unknown"
	c.source = &missing
	if _, err := c.caller(context.Background(), []Caller{{Principal: boss, Role: core.RoleOwner, Conv: missing, Private: true}}); err == nil {
		t.Fatal("missing source fell back")
	}
	c.source = &source
	if change := r.cmd(Caller{Principal: boss, Role: core.RoleOwner, Conv: source, Private: true}, "job confirm "+got.DraftID); !strings.Contains(change, "Paused") {
		t.Fatal(change)
	}
	if r.job(got.DraftID).CreatedIn != source {
		t.Fatal("source lost")
	}
}

func TestNativeErrorPrivateCheckpointFailure(t *testing.T) {
	r := newRig(t)
	id := r.add(asBoss, `{"name":"scan","schedule":{"every":"1h"},"prompt":"PRIVATE PROMPT","quiet":true,"checkpoint":true}`)
	r.run(id)
	r.sent()
	detail := core.SanitizeNativeError("mcp__test__list_workspaces: requested limit 100; maximum 50. token=synthetic-token https://example.test/private?signature=SYNTHETIC\nPRIVATE CONTENT", "")
	e := r.finish("model recovered", Outcome{Harness: "codex", ToolErrors: []string{detail}, Result: harness.Result{Ready: true}})
	if e.Run.State != store.RunFailed || !e.Run.Retry.IsZero() {
		t.Fatal(e.Run)
	}
	cp, _ := r.st.Checkpoint(id, boss.Key())
	if cp.Version != 0 {
		t.Fatal("failed scan committed", cp)
	}
	notices := r.sent()
	if len(notices) == 0 {
		t.Fatal("missing failure notice")
	}
	body := texts(notices)
	for _, w := range []string{"requested limit 100; maximum 50", "Automatic retry: no", "Next scheduled run:", "scan not committed"} {
		if !strings.Contains(body, w) {
			t.Fatal(w, body)
		}
	}
	for _, bad := range []string{"synthetic-token", "SYNTHETIC", "PRIVATE CONTENT", id} {
		if strings.Contains(body, bad) {
			t.Fatal("leak", body)
		}
	}
	if !notices[0].Formatted {
		t.Fatal("unformatted failure")
	}
	if h := r.cmd(asBoss, "job history "+id); !strings.Contains(h, "requested limit 100") {
		t.Fatal(h)
	}
	if h := r.cmd(inGroup, "job history "+id); strings.Contains(h, "requested limit 100") {
		t.Fatal(h)
	}
	// The next scheduled check is not a retry, even when it remains active.
	if r.job(id).State != store.JobActive {
		t.Fatal("falsely paused")
	}
	r.now = r.now.Add(25 * time.Hour)
	if _, _, ok := r.s.Answer(asBoss, "Details"); ok {
		t.Fatal("no request")
	}
}

func TestPresentationSummaryBounds(t *testing.T) {
	r := newRig(t)
	for _, summary := range []string{strings.Repeat("x", 161), "first\nsecond", "first\u2028second", "first\u2029second", "first\u202esecond"} {
		raw := mustJSON(map[string]any{"name": "x", "summary": summary, "schedule": map[string]string{"every": "1h"}, "prompt": "synthetic"})
		if _, err := r.s.proposal(asBoss, []byte(raw)); err == nil {
			t.Fatal("invalid summary accepted")
		}
	}
	if got := literal("a\u2028b\u2029c\u202ed"); strings.ContainsAny(got, "\u2028\u2029\u202e") {
		t.Fatal(got)
	}
}
