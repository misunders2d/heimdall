package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misunders2d/heimdall/internal/core"
	"github.com/misunders2d/heimdall/internal/store"
)

func controlRig(t *testing.T) (*rig, *Control) {
	t.Helper()
	r := newRig(t)
	_, _, err := r.st.AddInbound(core.Inbound{Conv: bossDM, From: boss, Origin: core.OriginLive, Private: true, MsgID: "private-binding", Text: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	return r, &Control{s: r.s, owner: boss.Key(), uid: 1234}
}

func TestControlAuthorityAndCanonicalDrafts(t *testing.T) {
	r, c := controlRig(t)
	ctx := context.Background()
	request := ControlRequest{Action: "create", RequestID: "create-1", Definition: json.RawMessage(`{"name":"monitor","schedule":{"every":"30m"},"prompt":"synthetic","harness":"codex","quiet":true,"checkpoint":true,"paused":true}`)}
	if got := c.execute(ctx, 9999, request); got.Error == "" {
		t.Fatal("untrusted OS caller admitted")
	}
	got := c.execute(ctx, 1234, request)
	if got.Error != "" || got.DraftID == "" || !got.Changed {
		t.Fatal(got)
	}
	j := r.job(got.DraftID)
	if j.State != store.JobDraft || j.Creator.Key() != boss.Key() {
		t.Fatal(j)
	}
	if second := c.execute(ctx, 1234, request); second != got {
		t.Fatal("uncertain response replay changed result", second, got)
	}
	if outs := r.sent(); len(outs) != 1 || outs[0].Conv != bossDM {
		t.Fatal("duplicate or wrongly routed confirmation card", outs)
	}
	spoof := request
	spoof.Definition = json.RawMessage(`{"name":"spoof","creator":"telegram:2","role":"owner","schedule":{"every":"30m"},"prompt":"p"}`)
	spoof.RequestID = "spoof"
	if got := c.execute(ctx, 1234, spoof); got.Error == "" {
		t.Fatal("model-selected identity accepted")
	}
	if got := c.execute(ctx, 1234, ControlRequest{Action: "confirm", ID: j.ID, RequestID: "confirm"}); got.Error == "" {
		t.Fatal("operator synthesized human consent")
	}
	if out, ok := r.answer(asWork, "yes"); ok {
		t.Fatal("other creator confirmed", out)
	}
	if out, ok := r.answer(asBoss, "yes?"); ok {
		t.Fatal("question confirmed", out)
	}
	if out, ok := r.answer(asBoss, "yes"); !ok || !strings.Contains(out, "paused") {
		t.Fatal("real human path failed", out)
	}
	if r.job(j.ID).State != store.JobPaused {
		t.Fatal("paused creation lost")
	}
	if got := c.execute(ctx, 1234, ControlRequest{Action: "resume", ID: j.ID, RequestID: "resume"}); got.Error != "" || !got.Changed {
		t.Fatal(got)
	}
	bad := request
	bad.Definition = json.RawMessage(`{"name":"different"}`)
	if got := c.execute(ctx, 1234, bad); got.Error == "" {
		t.Fatal("idempotency key rebound")
	}
	employee := r.add(asWork, `{"name":"private","schedule":{"every":"1h"},"prompt":"employee prompt"}`)
	for _, action := range []string{"show", "history", "checkpoint", "run", "edit", "remove"} {
		if got := c.execute(ctx, 1234, ControlRequest{Action: action, ID: employee, RequestID: action, Definition: json.RawMessage(`{"name":"x"}`)}); got.Error == "" || strings.Contains(got.Reply, "employee prompt") {
			t.Fatal("cross-creator access", action, got)
		}
	}
	r.s.access.Owners = map[string]bool{}
	if got := c.execute(ctx, 1234, request); got.Error == "" {
		t.Fatal("revoked owner replay admitted")
	}
}

func TestControlBindingFailsClosed(t *testing.T) {
	r := newRig(t)
	c := &Control{s: r.s, owner: boss.Key(), uid: 1234}
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error == "" {
		t.Fatal("unregistered identity accepted")
	}
	_, _, _ = r.st.AddInbound(core.Inbound{Conv: bossDM, From: boss, Origin: core.OriginLive, Private: true, MCPControl: true, MsgID: "mcp", Text: "synthetic"})
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error == "" {
		t.Fatal("MCP control established human binding")
	}
	p := core.SlackPrincipal("owner@example.test", "Ttest", "")
	k := core.ConvKey{Interface: "slack", Chat: "Dtest", Thread: "1000.1"}
	_, _, _ = r.st.AddInbound(core.Inbound{Conv: k, From: p, Origin: core.OriginLive, Private: true, MsgID: "slack-binding", Text: "synthetic"})
	r.s.access.Owners[p.Key()] = true
	c.owner = p.Key()
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error == "" {
		t.Fatal("cached Slack identity counted as live")
	}
	r.s.Reverify = func(context.Context, core.Principal) (Verified, bool, error) {
		return Verified{Principal: p}, true, nil
	}
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error != "" {
		t.Fatal(got)
	}
	r.s.Reverify = func(context.Context, core.Principal) (Verified, bool, error) {
		return Verified{Principal: core.SlackPrincipal("other@example.test", "Ttest", "")}, true, nil
	}
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error == "" {
		t.Fatal("different live principal rebound")
	}
}

func TestControlFileAdminNeverBecomesOwner(t *testing.T) {
	r, c := controlRig(t)
	dir := t.TempDir()
	for _, name := range []string{"whitelist.json", "blacklist.json", "identities.json", "capabilities.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "admins.json"), []byte(`{"users":["1"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ADMIN_USER_IDS", "999")
	r.s.access.Dir = dir // configured owners fallback must be ignored with Pi authority
	if got := c.execute(context.Background(), 1234, ControlRequest{Action: "list"}); got.Error == "" {
		t.Fatal("file admin or fallback owner elevated", got)
	}
}

func TestControlSocketMCPAndSpoofedArguments(t *testing.T) {
	r, _ := controlRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := filepath.Join(t.TempDir(), "ctl")
	l, err := r.s.ListenControl(ctx, boss.Key(), uint32(os.Geteuid()), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	socket := filepath.Join(dir, "control.sock")
	info, _ := os.Stat(socket)
	if info.Mode().Perm() != 0600 {
		t.Fatal("socket permissions", info.Mode())
	}
	if result, err := CallControl(ctx, socket, ControlRequest{Action: "list"}); err != nil || result.Reply != "No jobs." {
		t.Fatal(result, err)
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(conn, `{"action":"list","role":"owner","owner":"telegram:1"}`)
	var denied ControlResult
	_ = json.NewDecoder(conn).Decode(&denied)
	conn.Close()
	if denied.Error == "" {
		t.Fatal("wire identity spoof accepted")
	}
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fake","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"heimdall_scheduler","arguments":{"action":"list"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"heimdall_scheduler","arguments":{"action":"list","creator":"telegram:1"}}}`,
	}, "\n")
	var output bytes.Buffer
	if err := ServeControlMCP(ctx, socket, strings.NewReader(input), &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 4 || !strings.Contains(lines[1], "heimdall_scheduler") || !strings.Contains(lines[2], "No jobs.") || !strings.Contains(lines[3], "Invalid tool arguments") {
		t.Fatal(output.String())
	}
	// An actual Unix peer is checked by the kernel, separately from unit calls.
	if os.Geteuid() != 0 {
		other := filepath.Join(t.TempDir(), "other")
		l, err := r.s.ListenControl(ctx, boss.Key(), uint32(os.Geteuid()+1), other)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		if _, err := CallControl(ctx, filepath.Join(other, "control.sock"), ControlRequest{Action: "list"}); err == nil {
			t.Fatal("foreign peer UID accepted")
		}
	}
}

func TestControlReadsMaximumCheckpointSnapshot(t *testing.T) {
	cp := store.Checkpoint{Version: 1, Through: "2026-11-05T10:00:00Z", State: json.RawMessage(`{"value":"` + strings.Repeat(`\"`, 12000) + `"}`)}
	for i := 0; i < 512; i++ {
		cp.Receipts = append(cp.Receipts, store.AlertReceipt{Key: fmt.Sprintf("%0128d", i), Revision: strings.Repeat("r", 128), ContentHash: strings.Repeat("h", 64), Outbound: 123456789012345, Status: store.OutPending})
	}
	raw, _ := json.Marshal(cp)
	want := ControlResult{Reply: string(raw)}
	socket := filepath.Join(t.TempDir(), "large.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	served := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			served <- err
			return
		}
		defer conn.Close()
		var req ControlRequest
		if err = json.NewDecoder(conn).Decode(&req); err == nil {
			err = json.NewEncoder(conn).Encode(want)
		}
		served <- err
	}()
	got, err := CallControl(context.Background(), socket, ControlRequest{Action: "checkpoint", ID: "synthetic"})
	if err != nil || got != want {
		t.Fatal("bounded checkpoint snapshot truncated", err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
