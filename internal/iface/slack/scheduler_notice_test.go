package slack

import (
	"context"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

type noticeTransport func(*http.Request) (*http.Response, error)

func (f noticeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSchedulerNoticeFakePayload(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := scheduler.Caller{Principal: core.SlackPrincipal("owner@example.test", "T1", ""), Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "D1", Thread: "1000.1"}, Private: true}
	jobs := scheduler.New(scheduler.Config{DefaultTZ: "UTC", Interfaces: []string{"slack"}}, st, core.Access{}, log.New(io.Discard, "", 0))
	_, draft, card := jobs.Draft(c, []string{"```heimdall-job\n" + `{"name":"Literal C:\\temp *star* _u_ ` + "`code`" + `","summary":"[click](https://example.test) <https://example.test|link> <!channel> &","schedule":{"every":"1h"},"prompt":"PRIVATE PROMPT","paused":true}` + "\n```"})
	if draft == nil {
		t.Fatal(card)
	}
	var payload map[string]any
	transport := noticeTransport(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/chat.postMessage") {
			t.Fatalf("unexpected call %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		payload = map[string]any{}
		for k, v := range r.PostForm {
			payload[k] = v[0]
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"ts":"1000.2"}`))}, nil
	})
	s, err := New(Options{BotToken: "synthetic", AppToken: "synthetic", TeamID: "T1", Store: st, DataDir: t.TempDir(), APIBase: "https://fake.test", HTTPClient: &http.Client{Transport: transport}, Log: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	close(s.ready)
	if err = s.Send(context.Background(), core.Outbound{ID: 1, Conv: c.Conv, Text: card, Formatted: scheduler.Formatted(card)}); err != nil {
		t.Fatal(err)
	}
	text, _ := payload["text"].(string)
	want := "*📅 Schedule task?*\n\n*Literal C:\\temp ∗star∗ ＿u＿ ˋcodeˋ*\n[click](https://example.test) &lt;https://example.test|link&gt; &lt;!channel&gt; &amp;\n\n*🕒 When:* Every 1 hour · UTC\n*📬 Delivery:* This private conversation topic\n\n*⏸️ Starts paused*. No automatic checks.\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.\n— heimdall"
	if text != want {
		t.Fatalf("approval payload golden\nwant:\n%s\ngot:\n%s", want, text)
	}
	for _, bad := range []string{"<!channel>", "<https://", "PRIVATE PROMPT", draft.ID, "\\*", "\\[", "**Set up"} {
		if strings.Contains(text, bad) {
			t.Fatal("unsafe formatting", text)
		}
	}
	if payload["thread_ts"] != "1000.1" {
		t.Fatal("thread lost", payload)
	}
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: &store.JobChange{Add: draft}}); err != nil {
		t.Fatal(err)
	}
	receipt, change, ok := jobs.Answer(c, "Yes")
	if !ok || change == nil {
		t.Fatal(receipt)
	}
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: change}); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), core.Outbound{ID: 2, Conv: c.Conv, Text: receipt, Formatted: scheduler.Formatted(receipt)}); err != nil {
		t.Fatal(err)
	}
	want = "*📅 Scheduled task created*\n\n*Literal C:\\temp ∗star∗ ＿u＿ ˋcodeˋ*\n[click](https://example.test) &lt;https://example.test|link&gt; &lt;!channel&gt; &amp;\n\n*🕒 When:* Every 1 hour · UTC\n*📬 Delivery:* This private conversation topic\n\n*⏸️ Paused*; no automatic checks."
	if payload["text"] != want || payload["thread_ts"] != "1000.1" {
		t.Fatal("receipt payload golden or thread changed", payload)
	}

	// Exercise edit and shared removal through the real renderer and adapter.
	nextID := int64(2)
	sendCheck := func(caller scheduler.Caller, body string, fragments ...string) {
		t.Helper()

		nextID++
		if err := s.Send(context.Background(), core.Outbound{ID: nextID, Conv: caller.Conv, Text: body, Formatted: scheduler.Formatted(body)}); err != nil {
			t.Fatal(err)
		}
		got := payload["text"].(string)
		if payload["thread_ts"] != caller.Conv.Thread {
			t.Fatal("thread lost", payload)
		}
		for _, want := range fragments {
			if !strings.Contains(got, want) {
				t.Fatalf("payload lost %q:\n%s", want, got)
			}
		}
		for _, bad := range []string{"PRIVATE PROMPT", "PRIVATE CHANGED", draft.ID} {
			if strings.Contains(got, bad) {
				t.Fatal("private payload leak", got)
			}
		}
		if !caller.Private && strings.Contains(got, "Literal") {
			t.Fatal("shared task name leak", got)
		}
	}
	_, edit, editCard := jobs.Draft(c, []string{"```heimdall-job\n" + `{"edit":"` + draft.ID + `","prompt":"PRIVATE CHANGED","quiet":true,"timeout":"2m"}` + "\n```"})
	if edit == nil {
		t.Fatal(editCard)
	}
	sendCheck(c, editCard, "*📝 Update scheduled task?*\n\n*Literal", "\n\n*📋 Changes*\nSilent when no update: false → true\n*Timeout:* Configured limit → 2m\nInstructions updated.", "\n\n*⏸️ Still paused*. No automatic checks.\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.")
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: &store.JobChange{Add: edit}}); err != nil {
		t.Fatal(err)
	}
	receipt, change, ok = jobs.Answer(c, "Yes")
	if !ok || change == nil {
		t.Fatal(receipt)
	}
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: change}); err != nil {
		t.Fatal(err)
	}
	sendCheck(c, receipt, "*📝 Scheduled task updated*\n\n*Literal", "\n\n*📋 Changes*\nSilent when no update: false → true\n*Timeout:* Configured limit → 2m\nInstructions updated.", "\n\n*⏸️ Paused*; no automatic checks.")
	job, _, err := st.Job(draft.ID)
	if err != nil || job.State != store.JobPaused || job.Version != 2 {
		t.Fatal("presentation changed paused state", job, err)
	}
	// Enabled and finished are distinct persisted states, never validation claims.
	receipt, change = jobs.Command(c, "job resume "+draft.ID)
	if change == nil {
		t.Fatal(receipt)
	}
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: change}); err != nil {
		t.Fatal(err)
	}
	sendCheck(c, receipt, "*🟢 Scheduled task resumed*", "\n\n*🟢 Active*.\n*Next scheduled check:*")
	if err := st.AnswerCommand(0, c.Conv, store.CommandEnd{Job: &store.JobChange{Update: draft.ID, Set: store.Set{"state": store.JobDone}}}); err != nil {
		t.Fatal(err)
	}
	receipt, _ = jobs.Command(c, "jobs")
	sendCheck(c, receipt, "*📋 Scheduled task status*", "\n\n*✅ Finished*; no automatic checks.")
	if strings.Contains(receipt, "Next scheduled") || strings.Contains(receipt, "Reply ") || strings.Contains(receipt, "Details") {
		t.Fatal("finished receipt advertised a run or pending controls", receipt)
	}
	shared := c
	shared.Private = false
	shared.Conv.Chat = "C1"
	_, removal, removalCard := jobs.Draft(shared, []string{"```heimdall-job\n" + `{"remove":"` + draft.ID + `"}` + "\n```"})
	if removal == nil {
		t.Fatal(removalCard)
	}
	sendCheck(shared, removalCard, "*🗑️ Remove scheduled task?*\n\n*Scheduled task*\n\n*🕒 When:*", "\n\nFuture checks and pending retries will stop.\nAny current check will be stopped.\nAlready delivered messages remain.\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.")
	if err := st.AnswerCommand(0, shared.Conv, store.CommandEnd{Job: &store.JobChange{Add: removal}}); err != nil {
		t.Fatal(err)
	}
	receipt, change, ok = jobs.Answer(shared, "Yes")
	if !ok || change == nil {
		t.Fatal(receipt)
	}
	sendCheck(shared, receipt, "*🗑️ Scheduled task removed*\n\n*Scheduled task*\n\n*🕒 When:*", "\n\n*🗑️ Removed*.\nFuture checks and pending retries cancelled.\nAny current check is being stopped.\nAlready delivered messages remain.")
}
