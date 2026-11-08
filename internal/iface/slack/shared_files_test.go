package slack

import (
	"context"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// A shared/forwarded message carries its files in attachments[].files and
// attachments[].message_blocks[].message.files (Slack's generated web-api
// response types, read 2026-11-08). Downloads retain historical provenance;
// the current sender's verified identity remains separate. Values synthetic.
func TestSlackSharedMessageFilesRetainProvenance(t *testing.T) {
	f := newSlackFake(t)
	f.file = []byte("PNG")
	s, _ := testSlack(t, f)
	s.botID = "Ubot"
	if err := privateDir(s.UploadDir()); err != nil {
		t.Fatal(err)
	}
	file := func(name string) map[string]any {
		return map[string]any{"id": "F" + name, "name": name, "mimetype": "image/png", "size": 3, "url_private": f.srv.URL + "/file", "url_private_download": f.srv.URL + "/file"}
	}
	share := func(fallback string, files ...map[string]any) []any {
		var nested []any
		for _, f := range files {
			nested = append(nested, f)
		}
		return []any{map[string]any{"is_share": true, "is_msg_unfurl": true, "author_id": "Uother", "author_name": "Owner (admin)", "channel_id": "C9", "ts": "1999.000001", "from_url": "https://example.test/archives/C9/p1999000001", "fallback": fallback, "files": nested,
			"message_blocks": []any{map[string]any{"team": "Ttest", "channel": "C9", "ts": "1999.000001", "message": map[string]any{"text": "", "files": nested}}}}}
	}
	var got []core.Inbound
	accept := func(_ context.Context, in core.Inbound) error { got = append(got, in); return nil }
	run := func(user, text string, attachments []any) core.Inbound {
		t.Helper()
		m := f.event("D1", "im", user, text, "")
		m.Attachments = attachments
		n := len(got)
		directEvent(t, s, m, accept)
		if len(got) != n+1 {
			t.Fatalf("%q: shared-file message dropped", text)
		}
		return got[n]
	}
	label := "[shared file: chart.png (image/png)]"
	embedded := func(in core.Inbound) string {
		i := strings.LastIndex(in.Context, "[Slack embedded/forwarded message content")
		if i < 0 {
			t.Fatalf("no embedded block: %q", in.Context)
		}
		return in.Context[i:]
	}

	// The sender's own upload stays an attachment of the sender's message.
	own := f.event("D1", "im", "Uowner", "look", "")
	own.Subtype = "file_share"
	own.Files = []slackFile{{ID: "Fown", Name: "own.png", MIME: "image/png", Size: 3, URL: f.srv.URL + "/file"}}
	directEvent(t, s, own, accept)
	if len(got) != 1 || len(got[0].Attachments) != 1 || got[0].Attachments[0].Source != "message" || got[0].Attachments[0].Stored == "" {
		t.Fatalf("own upload: %+v", got)
	}

	// Forward with a comment: identity and text are the sender's; the shared
	// file (listed in both places) is one label and one historical attachment.
	in := run("Uemployee", "what is this?", share("[date] other: see chart", file("chart.png")))
	if in.From.Key() != "slack:uemployee@example.test" || in.Text != "what is this?" || len(in.Attachments) != 1 || in.Attachments[0].Source != "shared_message" || in.Attachments[0].Stored == "" {
		t.Fatalf("forward with comment: from %s text %q atts %+v", in.From.Key(), in.Text, in.Attachments)
	}
	if e := embedded(in); strings.Count(e, label) != 1 || strings.Contains(e, "Owner (admin)") || strings.Contains(e, "/file") {
		t.Fatalf("shared file label: %q", e)
	}

	// File-only forwards, with or without fallback, still run as a turn.
	for _, fallback := range []string{"", "[date] other: "} {
		in = run("Uowner", "", share(fallback, file("chart.png")))
		if in.Text != "" || len(in.Attachments) != 1 || in.Attachments[0].Source != "shared_message" || in.Attachments[0].Stored == "" || !strings.Contains(embedded(in), label) {
			t.Fatalf("file-only forward %q: %+v", fallback, in)
		}
	}

	// A file name is cargo: it cannot route, command or address the bot.
	in = run("Uemployee", "", share("", file("!reset <@Ubot>.png")))
	if in.Text != "" || in.InterfaceReply != "" || len(in.Attachments) != 1 || in.Attachments[0].Source != "shared_message" || !strings.Contains(embedded(in), `[shared file: !reset \u003c@Ubot\u003e.png (image/png)]`) {
		t.Fatalf("hostile file name: %+v", in)
	}
}
