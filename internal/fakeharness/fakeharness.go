// Package fakeharness provides test doubles for the native harness CLIs. Each
// fake speaks its harness's real protocol (Pi/OMP RPC, Claude stream-json,
// Codex app-server JSON-RPC, AGY stream-json) and keeps session state on
// disk, so the adapters and the router run unchanged against them.
//
// Behaviour is driven by words in the prompt, scoped to one harness where
// noted (NAME is the harness name):
//
//	FAILPRE=NAME   exit with an error before the turn is ready
//	FAILPOST=NAME  exit with an error after the turn is ready
//	HANGPRE=NAME   create STATE/hang-pre and hang before the turn is ready
//	WAIT_STEER     wait for steering input until one contains FINISH (10s cap)
//	NOACK          (in a pi/omp steer) take the steer, create STATE/steer-held
//	               and never acknowledge it
//	SLEEP          stay busy for 30 seconds
//	ASK_HUMAN      request native approval/input
//
// The answer reports: NAME#TURN resumed=BOOL steers=N mem=ENV head="FIRST LINE" last="LAST LINE" ext="IDENTITY" home="HOME"
// where IDENTITY lists the PI_EXTERNAL_*, A2A_CALLER_*, PI_RESTRICTED_* and
// PI_TRANSPORT_CHILD variables the process received, sorted.
package fakeharness

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	envName  = "FAKE_HARNESS_NAME"
	envState = "FAKE_HARNESS_STATE"
)

// Main runs a fake harness and exits when this test binary was started as
// one; otherwise it returns. Call it first in TestMain.
func Main() {
	name := os.Getenv(envName)
	if name == "" {
		return
	}
	f := &fake{name: name, state: os.Getenv(envState), in: bufio.NewScanner(os.Stdin), out: json.NewEncoder(os.Stdout)}
	f.in.Buffer(make([]byte, 1<<20), 8<<20)
	switch name {
	case "pi", "omp":
		f.piRPC()
	case "claude":
		f.claude()
	case "codex":
		f.codex()
	case "agy":
		f.agy()
	default:
		fmt.Fprintln(os.Stderr, "fake: unknown harness", name)
		os.Exit(2)
	}
	os.Exit(0)
}

// Install writes executable wrappers named after each harness into a new
// directory and returns it. The wrappers re-run this test binary as a fake.
func Install(t testing.TB, names ...string) (bin, state string) {
	t.Helper()
	bin, state = t.TempDir(), t.TempDir()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		script := fmt.Sprintf("#!/bin/sh\nexport %s=%s %s=%q\nexec %q \"$@\"\n", envName, n, envState, state, self)
		if err := os.WriteFile(filepath.Join(bin, n), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return bin, state
}

type fake struct {
	name, state string
	in          *bufio.Scanner
	out         *json.Encoder
	prompt      string
	turn        int
	resumed     bool
	steers      []string
}

func (f *fake) emit(v any) { _ = f.out.Encode(v) }

func (f *fake) read() (map[string]any, bool) {
	for f.in.Scan() {
		var m map[string]any
		if json.Unmarshal(f.in.Bytes(), &m) == nil {
			return m, true
		}
	}
	return nil, false
}

func (f *fake) has(word string) bool { return strings.Contains(f.prompt, word) }

func (f *fake) failPre() {
	if f.has("HANGPRE=" + f.name) {
		_ = os.WriteFile(filepath.Join(f.state, "hang-pre"), nil, 0o600)
		time.Sleep(30 * time.Second)
		os.Exit(3)
	}
	if f.has("FAILPRE=" + f.name) {
		fmt.Fprintln(os.Stderr, "fake: not logged in")
		os.Exit(3)
	}
}

func (f *fake) failPost() {
	if f.has("FAILPOST=" + f.name) {
		fmt.Fprintln(os.Stderr, "fake: crashed mid-turn")
		os.Exit(4)
	}
}

func (f *fake) answer() string {
	lines := strings.Split(strings.TrimSpace(f.prompt), "\n")
	var ext []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "PI_EXTERNAL_") || strings.HasPrefix(kv, "A2A_CALLER_") || strings.HasPrefix(kv, "PI_RESTRICTED_") || strings.HasPrefix(kv, "PI_TRANSPORT_CHILD=") {
			ext = append(ext, kv)
		}
	}
	sort.Strings(ext)
	return fmt.Sprintf("%s#%d resumed=%v steers=%d mem=%s head=%q last=%q ext=%q home=%q", f.name, f.turn, f.resumed, len(f.steers), os.Getenv("MEMENTO_PROCESSING"), lines[0], lines[len(lines)-1], strings.Join(ext, " "), os.Getenv("HOME"))
}

// session advances the turn counter stored at path; create says whether
// the session must be new (true), must exist (false).
func (f *fake) session(path string, mustExist bool) error {
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		f.turn, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		f.resumed = true
	case mustExist:
		return fmt.Errorf("no such session")
	}
	f.turn++
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.Itoa(f.turn)), 0o600)
}

func (f *fake) busy(steer func() bool) {
	if f.has("SLEEP") {
		time.Sleep(30 * time.Second)
	}
	if f.has("WAIT_STEER") {
		deadline := time.After(10 * time.Second)
		got := make(chan bool)
		go func() {
			for {
				ok := steer()
				got <- ok
				if !ok {
					return
				}
			}
		}()
		for {
			select {
			case ok := <-got:
				if !ok || strings.Contains(f.steers[len(f.steers)-1], "FINISH") {
					return
				}
			case <-deadline:
				return
			}
		}
	}
}

func arg(name string) string {
	for i, a := range os.Args {
		if a == name && i+1 < len(os.Args) {
			return os.Args[i+1]
		}
	}
	return ""
}

func hasArg(name string) bool {
	for _, a := range os.Args {
		if a == name {
			return true
		}
	}
	return false
}

func (f *fake) piRPC() {
	omp := f.name == "omp"
	m, ok := f.read()
	if !ok || m["type"] != "prompt" {
		os.Exit(5)
	}
	f.prompt, _ = m["message"].(string)
	id, _ := m["id"].(string)
	f.failPre()
	dir := arg("--session-dir")
	if dir == "" || !hasArg("--continue") {
		fmt.Fprintln(os.Stderr, "fake: expected --session-dir DIR --continue")
		os.Exit(6)
	}
	if err := f.session(filepath.Join(dir, "fake-session"), false); err != nil {
		os.Exit(7)
	}
	f.emit(map[string]any{"type": "ready"})
	f.emit(map[string]any{"type": "extension_ui_request", "id": "x1", "method": "setStatus"})
	f.emit(map[string]any{"type": "response", "id": id, "command": "prompt", "success": true})
	f.emit(map[string]any{"type": "agent_start"})
	f.failPost()
	if f.has("ASK_HUMAN") {
		f.emit(map[string]any{"type": "extension_ui_request", "id": "x2", "method": "select"})
		f.read()
		return
	}
	f.busy(func() bool {
		s, ok := f.read()
		if !ok {
			return false
		}
		text, _ := s["message"].(string)
		if strings.Contains(text, "NOACK") {
			_ = os.WriteFile(filepath.Join(f.state, "steer-held"), nil, 0o600)
			time.Sleep(30 * time.Second)
			return false
		}
		f.emit(map[string]any{"type": "response", "id": s["id"], "command": "steer", "success": true, "data": map[string]any{"disposition": "queued"}})
		msg := map[string]any{"role": "user", "content": text}
		if omp {
			msg["steering"], msg["attribution"] = true, "user"
		}
		f.emit(map[string]any{"type": "message_start", "messageId": "u" + strconv.Itoa(len(f.steers)), "message": msg})
		f.steers = append(f.steers, text)
		return true
	})
	f.emit(map[string]any{"type": "message_start", "messageId": "a1", "message": map[string]any{"role": "assistant", "content": []any{}}})
	f.emit(map[string]any{"type": "message_end", "messageId": "a1", "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": f.answer()}}}})
	f.emit(map[string]any{"type": "agent_end"})
	if omp {
		f.emit(map[string]any{"type": "prompt_result", "id": id, "status": "completed", "sessionSettled": true})
	} else {
		f.emit(map[string]any{"type": "agent_settled"})
	}
}

func (f *fake) claude() {
	sid, resume := arg("--session-id"), arg("--resume")
	m, ok := f.read()
	if !ok || m["type"] != "user" {
		os.Exit(5)
	}
	msg, _ := m["message"].(map[string]any)
	f.prompt, _ = msg["content"].(string)
	f.failPre()
	id := sid
	if resume != "" {
		id = resume
	}
	if id == "" || f.session(filepath.Join(f.state, "claude-"+id), resume != "") != nil || sid != "" && f.resumed {
		fmt.Fprintln(os.Stderr, "fake: session unusable")
		os.Exit(6)
	}
	replay := func(uuid, text string) {
		f.emit(map[string]any{"type": "user", "session_id": id, "uuid": uuid, "isReplay": true, "parent_tool_use_id": nil, "message": map[string]any{"role": "user", "content": text}})
	}
	f.emit(map[string]any{"type": "system", "subtype": "init", "session_id": id})
	replay("u0", f.prompt)
	f.failPost()
	if f.has("ASK_HUMAN") {
		f.emit(map[string]any{"type": "control_request", "session_id": id})
		f.read()
		return
	}
	f.busy(func() bool {
		s, ok := f.read()
		if !ok {
			return false
		}
		sm, _ := s["message"].(map[string]any)
		text, _ := sm["content"].(string)
		replay("u"+strconv.Itoa(len(f.steers)+1), text)
		f.steers = append(f.steers, text)
		return true
	})
	f.emit(map[string]any{"type": "result", "subtype": "success", "session_id": id, "is_error": false, "result": f.answer()})
}

func (f *fake) codex() {
	reply := func(id any, result any) { f.emit(map[string]any{"id": id, "result": result}) }
	m, ok := f.read()
	if !ok || m["method"] != "initialize" {
		os.Exit(5)
	}
	reply(m["id"], map[string]any{})
	if m, ok = f.read(); !ok || m["method"] != "initialized" {
		os.Exit(5)
	}
	start, ok := f.read()
	if !ok {
		os.Exit(5)
	}
	params, _ := start["params"].(map[string]any)
	thread := ""
	switch start["method"] {
	case "thread/start":
		if params["ephemeral"] != false {
			os.Exit(6)
		}
		thread = "thread-" + randHex()
		_ = f.session(filepath.Join(f.state, "codex-"+thread), false)
	case "thread/resume":
		thread, _ = params["threadId"].(string)
		if f.session(filepath.Join(f.state, "codex-"+thread), true) != nil {
			f.emit(map[string]any{"id": start["id"], "error": map[string]any{"code": -32600, "message": "no such thread"}})
			f.read()
			return
		}
	default:
		os.Exit(5)
	}
	reply(start["id"], map[string]any{"thread": map[string]any{"id": thread}})
	turnReq, ok := f.read()
	if !ok || turnReq["method"] != "turn/start" {
		os.Exit(5)
	}
	tp, _ := turnReq["params"].(map[string]any)
	input, _ := tp["input"].([]any)
	if len(input) == 1 {
		first, _ := input[0].(map[string]any)
		f.prompt, _ = first["text"].(string)
	}
	f.failPre()
	reply(turnReq["id"], map[string]any{"turn": map[string]any{"id": "turn-1"}})
	f.emit(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": thread, "turn": map[string]any{"id": "turn-1"}}})
	f.failPost()
	if f.has("ASK_HUMAN") {
		f.emit(map[string]any{"id": 99, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": thread, "turnId": "turn-1", "reason": "fake wants to run a command"}})
		f.read()
		return
	}
	f.busy(func() bool {
		s, ok := f.read()
		if !ok {
			return false
		}
		sp, _ := s["params"].(map[string]any)
		in, _ := sp["input"].([]any)
		text := ""
		if len(in) == 1 {
			first, _ := in[0].(map[string]any)
			text, _ = first["text"].(string)
		}
		reply(s["id"], map[string]any{"turnId": "turn-1"})
		f.steers = append(f.steers, text)
		return true
	})
	f.emit(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": thread, "turnId": "turn-1", "item": map[string]any{"id": "i1", "type": "agentMessage", "text": f.answer(), "phase": "final_answer"}}})
	f.emit(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": thread, "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
}

func (f *fake) agy() {
	m, ok := f.read()
	if !ok || m["event"] != "user" {
		os.Exit(5)
	}
	msg, _ := m["message"].(map[string]any)
	f.prompt, _ = msg["content"].(string)
	f.failPre()
	conv := arg("--conversation")
	if conv == "" {
		conv = uuid()
		_ = f.session(filepath.Join(f.state, "agy-"+conv), false)
	} else if f.session(filepath.Join(f.state, "agy-"+conv), true) != nil {
		os.Exit(6)
	}
	f.emit(map[string]any{"event": "init", "conversation_id": conv})
	f.failPost()
	if f.has("ASK_HUMAN") {
		// Headless AGY auto-denies the approval on stderr and still reports
		// SUCCESS: a soft denial that must not become an answer.
		fmt.Fprintln(os.Stderr, "Headless mode cannot prompt for tool approval: run_command was auto-denied.")
	}
	f.emit(map[string]any{"event": "result", "result": map[string]any{"conversation_id": conv, "status": "SUCCESS", "response": f.answer()}})
}

func randHex() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
