package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type discard struct{ io.Writer }

func (discard) Close() error { return nil }

func codexTestBridge() *codexBridge {
	return &codexBridge{ctx: context.Background(), input: discard{io.Discard}, ready: make(chan struct{}), answered: make(chan struct{}), steering: map[string]chan SteerOutcome{}, thread: "t1", turn: "turn-1"}
}

// A steer acknowledgement that arrives after Codex completed the turn
// leaves the published answer alone (a regression of 47135e8 found in
// review: it reset the answer to empty), and the steer counts as unknown:
// the answer went out without it (review of d05b748: it counted as
// accepted, so the sender was never told).
func TestCodexSteerAckAfterCompletionKeepsTheAnswer(t *testing.T) {
	b := codexTestBridge()
	ack := make(chan SteerOutcome, 1)
	b.steering[`"heimdall-steer-1"`] = ack
	for _, line := range []string{
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"ALPHA","phase":"final_answer"}}}`,
		`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`,
		`{"id":"heimdall-steer-1","result":{"turnId":"turn-1"}}`,
	} {
		b.event([]byte(line))
	}
	if o := b.outcome(); o.answer != "ALPHA" || strings.Join(o.answers, "|") != "ALPHA" {
		t.Fatalf("outcome %+v", o)
	}
	if got := <-ack; got != SteerUnknown {
		t.Fatalf("steer acknowledged after the turn completed: %s, want %s", got, SteerUnknown)
	}
}

// Acknowledged before the turn completed, the steer is accepted and gets
// its own answer after the one given before it.
func TestCodexSteerAckBeforeCompletionIsAccepted(t *testing.T) {
	b := codexTestBridge()
	ack := make(chan SteerOutcome, 1)
	b.steering[`"heimdall-steer-1"`] = ack
	for _, line := range []string{
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i1","type":"agentMessage","text":"ALPHA","phase":"final_answer"}}}`,
		`{"id":"heimdall-steer-1","result":{"turnId":"turn-1"}}`,
		`{"method":"item/completed","params":{"threadId":"t1","turnId":"turn-1","item":{"id":"i2","type":"agentMessage","text":"BRAVO","phase":"final_answer"}}}`,
		`{"method":"turn/completed","params":{"threadId":"t1","turn":{"id":"turn-1","status":"completed"}}}`,
	} {
		b.event([]byte(line))
	}
	if got := <-ack; got != SteerAccepted {
		t.Fatalf("steer acknowledged before completion: %s, want %s", got, SteerAccepted)
	}
	if o := b.outcome(); strings.Join(o.answers, "|") != "ALPHA|BRAVO" {
		t.Fatalf("outcome %+v", o)
	}
}

func TestCodexEmployeeThreadNeverRequestsApproval(t *testing.T) {
	var out strings.Builder
	b := codexTestBridge()
	b.employee = true
	b.input = discard{&out}
	b.event([]byte(`{"id":1,"result":{}}`))
	if !strings.Contains(out.String(), `"approvalPolicy":"never"`) || strings.Contains(out.String(), "auto_review") {
		t.Fatal(out.String())
	}
}

// fakeAppServer answers the bridge's requests asynchronously over the
// real line framing, as codex app-server 0.167.0 does for these methods:
// thread/resume hydrates thread.turns (here past the line bound) unless
// excludeTurns is true (ThreadResumeParams, generate-json-schema).
type fakeAppServer struct {
	b      *codexBridge
	thread string // id thread/resume answers with ("" = malformed result)
	mu     sync.Mutex
	sent   []map[string]any
	out    chan []byte
}

func (f *fakeAppServer) Close() error { return nil }
func (f *fakeAppServer) Write(p []byte) (int, error) {
	var m map[string]any
	if json.Unmarshal(p, &m) != nil {
		return len(p), nil
	}
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	params, _ := m["params"].(map[string]any)
	switch m["method"] {
	case "initialize":
		f.out <- []byte(`{"id":1,"result":{}}` + "\n")
	case "thread/resume":
		thread := map[string]any{"id": f.thread, "turns": []any{}}
		if params["excludeTurns"] != true {
			thread["turns"] = []any{map[string]any{"id": "old", "items": []any{map[string]any{"type": "agentMessage", "text": strings.Repeat("x", lineMax)}}}}
		}
		result := map[string]any{"thread": thread, "cwd": "/w", "model": "m", "modelProvider": "p", "approvalPolicy": "on-request", "approvalsReviewer": "auto_review", "sandbox": map[string]any{"type": "readOnly"}}
		if f.thread == "" {
			result = map[string]any{"cwd": "/w"}
		}
		raw, _ := json.Marshal(map[string]any{"id": 2, "result": result})
		f.out <- append(raw, '\n')
	}
	return len(p), nil
}
func (f *fakeAppServer) methods() (out []string, resume map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.sent {
		method, _ := m["method"].(string)
		out = append(out, method)
		if method == "thread/resume" {
			resume, _ = m["params"].(map[string]any)
		}
	}
	return out, resume
}

// Resuming a long native thread must not ask Codex for its whole history:
// the response would exceed one protocol line and the run would die before
// its session started. Only the intended stored thread continues; a
// mismatched or malformed envelope is still refused, and nothing beyond
// the one prompt is sent.
func TestCodexResumeOmitsNativeHistory(t *testing.T) {
	for _, tc := range []struct {
		name, thread, failure string
		starts                bool
	}{
		{name: "intended", thread: "thread-old", starts: true},
		{name: "other-thread", thread: "thread-other", failure: "Codex returned no usable session"},
		{name: "malformed", thread: "", failure: "Codex returned no usable session"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAppServer{thread: tc.thread, out: make(chan []byte, 8)}
			b := &codexBridge{ctx: context.Background(), input: f, cwd: "/w", prompt: "person", resume: "thread-old", ready: make(chan struct{}), answered: make(chan struct{}), steering: map[string]chan SteerOutcome{}, contextRequests: map[string]map[string]any{}}
			f.b = b
			done := make(chan struct{})
			go func() {
				defer close(done)
				for line := range f.out {
					b.Write(line)
				}
			}()
			b.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
			deadline := time.After(10 * time.Second)
			for {
				o := b.outcome()
				methods, _ := f.methods()
				if o.failure != "" || slices.Contains(methods, "turn/start") {
					break
				}
				select {
				case <-deadline:
					t.Fatalf("no start or failure: %v", methods)
				case <-time.After(5 * time.Millisecond):
				}
			}
			close(f.out)
			<-done
			methods, resume := f.methods()
			o := b.outcome()
			if resume["excludeTurns"] != true || resume["threadId"] != "thread-old" || resume["cwd"] != "/w" || resume["approvalPolicy"] != "on-request" || resume["approvalsReviewer"] != "auto_review" {
				t.Fatalf("resume params %v", resume)
			}
			if tc.starts {
				if o.failure != "" || b.thread != "thread-old" || !slices.Equal(methods, []string{"initialize", "initialized", "thread/resume", "turn/start"}) {
					t.Fatalf("intended session did not continue: failure %q thread %q methods %v", o.failure, b.thread, methods)
				}
				return
			}
			if o.failure != tc.failure || slices.Contains(methods, "turn/start") {
				t.Fatalf("envelope accepted: failure %q methods %v", o.failure, methods)
			}
		})
	}
}

// A warm worker cut off by an oversized native line reports that, not
// "exited before starting a session".
func TestWarmCodexOversizedLineIsReported(t *testing.T) {
	b := codexTestBridge()
	b.thread, b.turn = "", ""
	exited := make(chan struct{})
	close(exited)
	r := &Run{bridge: b, ctx: context.Background(), harness: "codex", exited: exited}
	w := &worker{current: r, root: r, input: &warmInput{WriteCloser: discard{io.Discard}}}
	r.worker = w
	(&workerOutput{w}).Write(append(bytes.Repeat([]byte("x"), lineMax+1), '\n'))
	b.finish()
	if o := b.outcome(); o.failure != "Codex sent a protocol line over the size bound" {
		t.Fatalf("failure %q", o.failure)
	}
}
