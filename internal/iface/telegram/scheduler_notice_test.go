package telegram

import (
	"context"
	"encoding/json"
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

func TestSchedulerNoticeFakePayloadAndFallback(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := scheduler.Caller{Principal: core.Principal{Platform: "telegram", ID: "42"}, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "telegram", Chat: "42", Thread: "7"}, Private: true}
	jobs := scheduler.New(scheduler.Config{DefaultTZ: "UTC", Interfaces: []string{"telegram"}}, st, core.Access{}, log.New(io.Discard, "", 0))
	_, draft, card := jobs.Draft(c, []string{"```heimdall-job\n" + `{"name":"Literal C:\\temp *star* _u_ [click](https://example.test)","schedule":{"every":"1h"},"prompt":"PRIVATE PROMPT","paused":true}` + "\n```"})
	if draft == nil {
		t.Fatal(card)
	}
	var payloads []map[string]any
	i, err := New(Options{Token: "synthetic", BotUsername: "testbot", API: "https://fake.test", Log: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	i.http = &http.Client{Transport: noticeTransport(func(r *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, body)
		status := 200
		result := `{"ok":true,"result":{"message_id":10}}`
		if body["parse_mode"] != nil {
			status = 400
			result = `{"ok":false,"description":"Bad Request: can't parse entities"}`
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(result))}, nil
	})}
	if err := i.Send(context.Background(), core.Outbound{Conv: c.Conv, Text: card, Formatted: scheduler.Formatted(card)}); err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 || payloads[0]["parse_mode"] != "Markdown" || payloads[1]["parse_mode"] != nil {
		t.Fatal(payloads)
	}
	text := payloads[0]["text"].(string)
	want := "*📅 Schedule task?*\n\n" + `*Literal C:*\\*temp *\**star*\* \_*u*\_ \[*click*\]*(https://example.test)*` + "\n\n*🕒 When:* Every 1 hour · UTC\n*📬 Delivery:* This private conversation topic\n\n*⏸️ Starts paused*. No automatic checks.\n\n👉 Reply *Yes* to apply or *No* to cancel. *Details* shows full settings.\n— heimdall"
	if text != want {
		t.Fatalf("approval payload golden\nwant:\n%s\ngot:\n%s", want, text)
	}
	if payloads[0]["message_thread_id"] != float64(7) || payloads[1]["text"] != text {
		t.Fatal(payloads)
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
	if err := i.Send(context.Background(), core.Outbound{Conv: c.Conv, Text: receipt, Formatted: scheduler.Formatted(receipt)}); err != nil {
		t.Fatal(err)
	}
	want = "*📅 Scheduled task created*\n\n" + `*Literal C:*\\*temp *\**star*\* \_*u*\_ \[*click*\]*(https://example.test)*` + "\n\n*🕒 When:* Every 1 hour · UTC\n*📬 Delivery:* This private conversation topic\n\n*⏸️ Paused*; no automatic checks."
	if len(payloads) != 4 || payloads[2]["text"] != want || payloads[3]["text"] != want || payloads[2]["parse_mode"] != "Markdown" || payloads[3]["parse_mode"] != nil {
		t.Fatal("receipt payload or fallback changed", payloads)
	}

	// Exercise edit and shared removal through the real renderer and adapter.
	nextID := int64(2)
	sendCheck := func(caller scheduler.Caller, body string, fragments ...string) {
		t.Helper()

		nextID++
		before := len(payloads)
		if err := i.Send(context.Background(), core.Outbound{ID: nextID, Conv: caller.Conv, Text: body, Formatted: scheduler.Formatted(body)}); err != nil {
			t.Fatal(err)
		}
		if len(payloads) != before+2 || payloads[before]["parse_mode"] != "Markdown" || payloads[before+1]["parse_mode"] != nil || payloads[before]["text"] != payloads[before+1]["text"] {
			t.Fatal("paragraphs or fallback changed", payloads[before:])
		}
		got := payloads[before]["text"].(string)
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
	shared.Conv.Chat = "-100"
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
