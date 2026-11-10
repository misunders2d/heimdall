package telegram

import (
	"encoding/json"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// Item 8: a forwarded message is someone else's words. Its body reaches the
// turn only as untrusted context; the message has no text of its own, so a
// command or a bare yes inside it never runs as the sender's. In a group the
// body does not count as an unmentioned command either. A forward alone
// still runs as input.
func TestForwardedTextIsQuotedNotACommand(t *testing.T) {
	i := &Interface{opt: Options{Log: log.New(io.Discard, "", 0), Access: &core.Access{Owners: map[string]bool{"telegram:42": true}}}}
	inbound := func(m map[string]any) (core.Inbound, bool) {
		t.Helper()
		raw, _ := json.Marshal(upd(1, m))
		var u update
		if err := json.Unmarshal(raw, &u); err != nil {
			t.Fatal(err)
		}
		in, _, ok := i.inbound(u)
		return in, ok
	}
	forward := func(m map[string]any) map[string]any {
		m["forward_origin"] = map[string]any{"type": "user", "date": 1700000000, "sender_user": map[string]any{"id": 9, "first_name": "Sender"}}
		return m
	}
	for _, body := range []string{"!use codex", "yes", "/stop", "!job rm j123456"} {
		in, ok := inbound(forward(msg(1, 42, "private", 42, body)))
		if !ok {
			t.Fatalf("forward-only %q dropped", body)
		}
		if in.Text != "" {
			t.Fatalf("forwarded %q became the sender's text %q", body, in.Text)
		}
		if !strings.Contains(in.Context, "[BEGIN UNTRUSTED TELEGRAM FORWARDED MESSAGE") || !strings.Contains(in.Context, "\n"+body+"\n") {
			t.Fatalf("forwarded %q not quoted as untrusted context: %q", body, in.Context)
		}
	}
	// A forwarded caption is quoted too.
	m := forward(msg(2, 42, "private", 42, ""))
	m["caption"] = "!reset"
	if in, ok := inbound(m); !ok || in.Text != "" || !strings.Contains(in.Context, "\n!reset\n") {
		t.Fatalf("forwarded caption: ok %v %+v", ok, in)
	}
	// In a group, a forwarded "stop" is not an unmentioned command: it
	// stays context, while the sender's own "stop" is still a control.
	if in, ok := inbound(forward(msg(3, -1002, "group", 42, "stop"))); !ok || !in.Ambient {
		t.Fatalf("forwarded group stop addressed the bot: ok %v %+v", ok, in)
	}
	if in, ok := inbound(msg(4, -1002, "group", 42, "stop")); !ok || in.Ambient || in.Text != "stop" {
		t.Fatalf("own group stop: ok %v %+v", ok, in)
	}
}
