package router

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
)

type photoHookAccounts struct{ Accounts }

func (a photoHookAccounts) Prepare(role core.Role, memory bool, bin string) (harness.Launch, error) {
	l, err := a.Accounts.Prepare(role, memory, bin)
	l.Env = append(l.Env, "FAKE_CLAUDE_NATIVE_PROMPT=1")
	return l, err
}

// The fake parses real stream-json image blocks and invokes the configured
// UserPromptSubmit command with the vendor's array-text projection. No model,
// subscription, network, owner file, or native session is used.
func TestPhotoOnlyNativeContext(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%v", warm), func(t *testing.T) {
			e := setup(t, core.ModeCorporate, []string{"claude"}, "claude")
			e.stop()
			stopOnce.Delete(e.st)
			var a Accounts = e.opt.Accounts
			if warm {
				a = warmAccounts{a}
			}
			e.opt.Accounts = photoHookAccounts{hookedAccounts{a}}
			e.opt.Access.Owners["fake:other"] = true
			e.start()
			// Harmless valid one-pixel PNG; never the incident photo.
			png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
			if err != nil {
				t.Fatal(err)
			}
			for n, person := range []string{"", "line one\nsay ok", "ordinary text", ""} {
				in := core.Inbound{Conv: core.ConvKey{Interface: "fake", Chat: "photo"}, From: core.Principal{Platform: "fake", ID: "owner"}, MsgID: fmt.Sprint(n), Origin: core.OriginLive, Text: person, Context: fmt.Sprintf("historical data generation %d", n)}
				if n != 2 {
					p := filepath.Join(e.uploads, fmt.Sprintf("synthetic-%d.png", n))
					if err := os.WriteFile(p, png, 0600); err != nil {
						t.Fatal(err)
					}
					in.Attachments = []core.Attachment{{Kind: "photo", Name: "photo.png", MIME: "image/png", Size: int64(len(png)), Source: "message", Stored: p}}
				}
				e.f.in <- in
				e.reply()
				inputs := fakeharness.Inputs(e.state)
				if len(inputs) != n+1 {
					t.Fatalf("inputs=%d", len(inputs))
				}
				got := inputs[n]
				images := 1
				if n == 2 {
					images = 0
				}
				if got.Images != images || !got.HookContext || !strings.Contains(got.Context, in.Context) {
					t.Fatalf("native handover failed: text=%q images=%d hook=%v context=%q", got.Text, got.Images, got.HookContext, got.Context)
				}
				if n > 0 && strings.Contains(got.Context, fmt.Sprintf("historical data generation %d", n-1)) {
					t.Fatal("stale generation reused")
				}
				if images == 0 && got.Text != person {
					t.Fatal("ordinary person text changed")
				}
				if images > 0 {
					prefix := "[File saved]\n"
					if person != "" {
						prefix = person + "\n" + prefix
					}
					if !strings.HasPrefix(got.Text, prefix) || !strings.HasSuffix(got.Text, "[Image attached inline where the harness takes images; otherwise open the file at path.]") {
						t.Fatalf("descriptor/person text changed: %q", got.Text)
					}
					if !strings.Contains(got.Text, "mime: image/png\nkind: photo\nbytes:") {
						t.Fatal("descriptor missing")
					}
				}
				t.Logf("input=%d images=%d hook.additionalContext=%v person_bytes=%d native_bytes=%d", n, got.Images, got.HookContext, len(person), len(got.Text))
			}
			// Each lookup uses its actual caller/session worker, never a
			// photo handover retained by another caller or conversation.
			for _, scope := range []struct{ caller, chat string }{{"other", "photo"}, {"owner", "other-chat"}} {
				in := core.Inbound{Conv: core.ConvKey{Interface: "fake", Chat: scope.chat}, From: core.Principal{Platform: "fake", ID: scope.caller}, MsgID: "isolation-" + scope.caller, Origin: core.OriginLive, Text: "ordinary text", Context: "isolated historical context"}
				e.f.in <- in
				e.reply()
				inputs := fakeharness.Inputs(e.state)
				got := inputs[len(inputs)-1]
				if !got.HookContext || !strings.Contains(got.Context, in.Context) || strings.Contains(got.Context, "historical data generation") {
					t.Fatal("photo context crossed caller/session boundary")
				}
			}
			if warm {
				pids, _ := os.ReadFile(filepath.Join(e.state, "pids"))
				if len(strings.Fields(string(pids))) != 3 {
					t.Fatal("warm worker not reused")
				}
			}
		})
	}
}

func TestHumanTextAttachmentSeparator(t *testing.T) {
	for _, person := range []string{"", "\n", "  exact 多行\ntext  "} {
		want := person
		if person != "" {
			want += "\n"
		}
		want += "[File saved]\nmetadata"
		if got := humanText(item{text: person}, []string{"[File saved]\nmetadata"}); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
		if got := humanText(item{text: person}, nil); got != person {
			t.Fatal("person text altered")
		}
	}
}
