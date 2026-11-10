package router

import (
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
)

// Item 6: !use changes the owner's setup, so only the owner may send it. An
// employee in a shared chat gets one refusal line, and nothing changes:
// no sticky harness, no new generation, the owner's session still resumes.
func TestEmployeeCannotUse(t *testing.T) {
	e := setup(t, core.ModeCorporate, []string{"omp", "codex"}, "omp", "codex")
	e.send(owner, "c1", "hello")
	want(t, e.reply(), "omp#1 resumed=false")
	for _, cmd := range []string{"!use codex", "!use default"} {
		e.send(employee, "c1", cmd)
		out := e.reply()
		want(t, out, "Only the owner can change this conversation's harness.")
		if strings.Contains(out.Text, "now uses") || strings.Contains(out.Text, "back to the default") {
			t.Fatalf("employee %s applied: %q", cmd, out.Text)
		}
	}
	if h, _ := e.st.ChatHarness(core.ConvKey{Interface: "fake", Chat: "c1"}); h != "" {
		t.Fatalf("employee !use set the harness: %q", h)
	}
	e.send(owner, "c1", "again")
	want(t, e.reply(), "omp#2 resumed=true")
	// The owner still can.
	e.send(owner, "c1", "!use codex")
	want(t, e.reply(), "now uses codex")
}
