package router

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/fakeharness"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/scheduler"
	"github.com/misunders2d/heimdall/internal/store"
)

func TestOwnerRequestContextNativeRecovery(t *testing.T) {
	for _, name := range []string{"codex", "claude"} {
		t.Run(name, func(t *testing.T) {
			e := setup(t, core.ModeCorporate, []string{name}, name)
			first := "Read synthetic child status only. Do not publish. FAILPOST=" + name
			e.send(owner, "recovery", first)
			e.reply()
			e.send(owner, "recovery", "what? FAILPOST="+name)
			e.reply()
			e.send(owner, "recovery", "status now?")
			e.reply()
			inputs := fakeharness.Inputs(e.state)
			if len(inputs) != 3 {
				t.Fatalf("inputs=%+v", inputs)
			}
			if inputs[2].Text != "status now?" || !strings.Contains(inputs[2].Context, first) || !strings.Contains(inputs[2].Context, "what? FAILPOST="+name) {
				t.Fatalf("native input=%+v", inputs[2])
			}
			e.send(owner, "recovery", "clean resume")
			e.reply()
			inputs = fakeharness.Inputs(e.state)
			if strings.Contains(inputs[3].Context, "Earlier direct messages") {
				t.Fatal("recovery on clean resume")
			}
		})
	}
}

func TestOwnerRequestContextGatesAndWholeMessages(t *testing.T) {
	// This unit test owns its persisted transitions. Startup recovery must
	// not race the synthetic pending rows it deliberately settles itself.
	st, err := store.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r := New(Options{}, st)
	k := core.ConvKey{Interface: "fake", Chat: "limits"}
	var last int64
	texts := []string{"older whole", strings.Repeat("z", store.OwnerInputBytes-300) + " Do not publish.", "!codex command text", "!next queued command", "!job add {}", "reset", "latest\nDo not write.  "}
	for i, text := range texts {
		in := core.Inbound{Conv: k, MsgID: string(rune('a' + i)), From: core.Principal{Platform: "fake", ID: "owner"}, Origin: core.OriginLive, Text: text, Context: "NEVER_COPY_ASSISTANT"}
		row, _, err := st.AddInbound(in)
		if err != nil {
			t.Fatal(err)
		}
		id, err := st.StartTurn(k, core.RoleOwner, owner, in.MsgID, []int64{row.ID})
		if err != nil {
			t.Fatal(err)
		}
		_, err = st.EndTurn(id, k, owner, store.TurnEnd{State: store.TurnFailed, ItemState: store.InFailed})
		if err != nil {
			t.Fatal(err)
		}
		last = id
	}
	c := &conv{r: r, key: k}
	tr := &turn{id: last + 1, role: core.RoleOwner, who: owner}
	in := core.Inbound{Origin: core.OriginLive}
	packet := c.ownerRequestContext(tr, "codex", harness.Session{}, "previous", false, in)
	if len(packet) > store.OwnerInputBytes || strings.Contains(packet, "zzzzz") || strings.Contains(packet, "command text") || strings.Contains(packet, "!job") || strings.Contains(packet, "NEVER_COPY_ASSISTANT") {
		t.Fatal(packet)
	}
	lines := strings.Split(packet, "\n")
	if len(lines) != 3 {
		t.Fatalf("packet=%s", packet)
	}
	var decoded store.OwnerInput
	if err := json.Unmarshal([]byte(lines[2]), &decoded); err != nil || decoded.Text != texts[len(texts)-1] {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	for _, gate := range []string{"clean", "resume", "missing", "employee", "job", "mcp", "scheduled", "ambient", "other caller", "other chat", "other thread", "pi"} {
		t.Run(gate, func(t *testing.T) {
			cp := &turn{id: tr.id, role: tr.role, who: tr.who}
			ci, cc, prev, clean, sess, h := in, &conv{r: c.r, key: c.key}, "previous", false, harness.Session{}, "codex"
			switch gate {
			case "clean":
				clean = true
			case "resume":
				sess.Resume = true
			case "missing":
				prev = ""
			case "employee":
				cp.role = core.RoleEmployee
			case "job":
				cp.job = &scheduler.Turn{}
			case "mcp":
				ci.MCPControl = true
			case "scheduled":
				ci.Origin = core.OriginScheduled
			case "ambient":
				ci.Ambient = true
			case "other caller":
				cp.who = "fake:other"
			case "other chat":
				cc.key.Chat = "elsewhere"
			case "other thread":
				cc.key.Thread = "elsewhere"
			case "pi":
				h = "pi"
			}
			if got := cc.ownerRequestContext(cp, h, sess, prev, clean, ci); got != "" {
				t.Fatal(got)
			}
		})
	}
}
