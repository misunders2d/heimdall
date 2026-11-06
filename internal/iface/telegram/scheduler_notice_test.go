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
	want := `*Schedule?* Literal C:\\temp \*star\* \_u\_ \[click\](https://example.test)` + "\n*When:* Every 1 hour · UTC\n*Delivery:* This private conversation topic\nStarts paused; no automatic checks.\nReply *Yes* · *No* · *Details*\n— heimdall"
	if text != want || len(strings.Split(text, "\n"))-1 > 6 {
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
	want = `*Created:* Literal C:\\temp \*star\* \_u\_ \[click\](https://example.test)` + "\n*Status:* Paused; no automatic checks."
	if len(payloads) != 4 || payloads[2]["text"] != want || payloads[3]["text"] != want || payloads[2]["parse_mode"] != "Markdown" || payloads[3]["parse_mode"] != nil {
		t.Fatal("receipt payload or fallback changed", payloads)
	}
}
