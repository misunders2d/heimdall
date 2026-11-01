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
//	HANGTERM       wait before ready, recording SIGTERM (NOTERM ignores it)
//	HANGPRE=NAME   create STATE/hang-pre and hang before the turn is ready
//	BADLINE=NAME   before the turn is ready, send an unreadable protocol
//	               line, then stay 1 s with STATE/lingering present and
//	               exit with an error
//	EARLYRESULT    (claude) report a result before replaying the prompt
//	EARLYASK       (claude) ask for approval before replaying the prompt
//	WAIT_STEER     wait for steering input until one contains FINISH (10s cap)
//	NOACK          (in a pi/omp steer) take the steer, create STATE/steer-held
//	               and never acknowledge it
//	SLEEP          stay busy for 30 seconds
//	ASK_HUMAN      request native approval/input
//	SLOWACK        (pi/omp) take each steer 1 s after it arrived
//	STEERREPLY     (pi/omp) before taking each steer, give the input before
//	               it its own final text, "reply N: LAST LINE", as OMP
//	               answers the prompt and then each steer
//	STEERLATE      (claude) take the first steer only after the prompt's
//	               result, as Claude's next turn in the stream; that turn
//	               then hangs (LATEHANG), exits 4 (LATECRASH), ends with an
//	               error result (LATEFAIL), asks for approval (LATEASK) or
//	               exits 0 without a result (LATEEXIT)
//	FINALFIRST     (codex) complete a final answer, "early answer: LAST
//	               LINE", before taking steers, as Codex can before it
//	               completes the turn
//	LINGER=SECONDS keep running that long after the final answer, with
//	               STATE/lingering present; a fake started meanwhile
//	               creates STATE/overlap. SIGTERM then creates
//	               STATE/terminated and exits, unless NOTERM ignores it
//
// The answer reports: NAME#TURN resumed=BOOL steers=N mem=ENV head="FIRST LINE" frame="FIRST HEIMDALL FRAME LINE" last="LAST LINE" ext="IDENTITY" home="HOME"
// where IDENTITY lists the PI_EXTERNAL_*, A2A_CALLER_*, PI_RESTRICTED_* and
// PI_TRANSPORT_CHILD variables the process received, sorted.
package fakeharness

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
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
	if len(os.Args) > 3 && os.Args[1] == "mcp" && os.Args[2] == "get" {
		fmt.Fprintln(os.Stderr, "No MCP server named 'slack' found.")
		os.Exit(1)
	}
	if capture := os.Getenv("FAKE_CAPTURE_LAUNCH"); capture != "" {
		data, _ := json.Marshal(map[string]any{"args": os.Args[1:], "oauth": os.Getenv("MCP_OAUTH_DIR"), "servers": os.Getenv("PI_MCP_OAUTH_SERVERS"), "client": os.Getenv("SLACK_CLIENT_ID"), "secret": os.Getenv("SLACK_CLIENT_SECRET")})
		_ = os.WriteFile(capture, data, 0600)
	}
	f := &fake{name: name, state: os.Getenv(envState), in: bufio.NewScanner(os.Stdin), out: json.NewEncoder(os.Stdout)}
	f.in.Buffer(make([]byte, 1<<20), 8<<20)
	if _, err := os.Stat(filepath.Join(f.state, "lingering")); err == nil {
		_ = os.WriteFile(filepath.Join(f.state, "overlap"), nil, 0o600)
	}
	if os.Getenv("FAKE_WARM") == "1" {
		f.warm()
		os.Exit(0)
	}
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
	f.linger()
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

// record appends one input the harness received (kind "prompt" or "steer")
// to STATE/inputs.jsonl, for tests that check the whole text.
func (f *fake) record(kind, text string, images int) {
	if kind == "prompt" && strings.Contains(text, "LINGER=") {
		// Before any answer: the process is lingering until it exits.
		_ = os.WriteFile(filepath.Join(f.state, "lingering"), nil, 0o600)
	}
	file, err := os.OpenFile(filepath.Join(f.state, "inputs.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_ = json.NewEncoder(file).Encode(Input{Harness: f.name, Kind: kind, Text: text, Images: images})
}

// Input is one recorded harness input.
type Input struct {
	Harness string `json:"harness"`
	Kind    string `json:"kind"`
	Text    string `json:"text"`
	Images  int    `json:"images"`
}

// Inputs reads the inputs recorded in state, oldest first.
func Inputs(state string) []Input {
	raw, _ := os.ReadFile(filepath.Join(state, "inputs.jsonl"))
	var out []Input
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var in Input
		if json.Unmarshal([]byte(line), &in) == nil {
			out = append(out, in)
		}
	}
	return out
}

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
	if f.has("HANGTERM") {
		f.waitForTerm(30 * time.Second)
		os.Exit(0)
	}
	if f.has("HANGPRE=" + f.name) {
		_ = os.WriteFile(filepath.Join(f.state, "hang-pre"), nil, 0o600)
		time.Sleep(30 * time.Second)
		os.Exit(3)
	}
	if f.has("BADLINE=" + f.name) {
		lingering := filepath.Join(f.state, "lingering")
		_ = os.WriteFile(lingering, nil, 0o600)
		fmt.Println("not a protocol line")
		time.Sleep(time.Second)
		_ = os.Remove(lingering)
		os.Exit(3)
	}
	if f.has("FAILPRE=" + f.name) {
		fmt.Fprintln(os.Stderr, "fake: not logged in")
		os.Exit(3)
	}
}

func (f *fake) failPost() {
	f.writeOut()
	if f.has("FAILPOST=" + f.name) {
		fmt.Fprintln(os.Stderr, "fake: crashed mid-turn")
		os.Exit(4)
	}
}

// linger keeps the process alive after its answer for LINGER=SECONDS.
func (f *fake) linger() {
	_, rest, ok := strings.Cut(f.prompt, "LINGER=")
	if !ok {
		return
	}
	n, _ := strconv.Atoi(strings.Fields(rest + " ")[0])
	f.waitForTerm(time.Duration(n) * time.Second)
	_ = os.Remove(filepath.Join(f.state, "lingering"))
}

func (f *fake) waitForTerm(d time.Duration) {
	term := make(chan os.Signal, 1)
	if f.has("NOTERM") {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	_ = os.WriteFile(filepath.Join(f.state, "signal-ready"), nil, 0o600)
	select {
	case <-time.After(d):
	case <-term:
		_ = os.WriteFile(filepath.Join(f.state, "terminated"), nil, 0o600)
	}
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
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
	frame := ""
	for _, l := range lines {
		if strings.HasPrefix(l, "[Heimdall |") {
			frame = l
			break
		}
	}
	answer := fmt.Sprintf("%s#%d resumed=%v steers=%d mem=%s head=%q frame=%q last=%q ext=%q home=%q", f.name, f.turn, f.resumed, len(f.steers), os.Getenv("MEMENTO_PROCESSING"), lines[0], frame, lines[len(lines)-1], strings.Join(ext, " "), os.Getenv("HOME"))
	return f.react(answer)
}

var outbox = regexp.MustCompile(`save them in (\S+) before`)

// writeOut saves WRITE=<name>:<content> files (no spaces) into the outbox
// the prompt names, as a harness's own tools would.
func (f *fake) writeOut() {
	m := outbox.FindStringSubmatch(f.prompt)
	if m == nil {
		return
	}
	for _, w := range strings.Fields(f.prompt) {
		if spec, ok := strings.CutPrefix(w, "WRITE="); ok {
			name, body, _ := strings.Cut(spec, ":")
			_ = os.WriteFile(filepath.Join(m[1], name), []byte(body), 0o600)
		}
	}
}

var reactRef = regexp.MustCompile(`\[react: ([a-f0-9-]{36}) (?:<emoji>|emoji_name)\]`)

// react ends the answer with a reaction directive for each message ref the
// harness was given (REACT=<emoji>), or answers with the directives only
// (REACTONLY).
func (f *fake) react(answer string) string {
	all := f.prompt + "\n" + strings.Join(f.steers, "\n")
	_, rest, ok := strings.Cut(all, "REACT=")
	if !ok {
		return answer
	}
	emoji, _, _ := strings.Cut(rest, " ")
	emoji, _, _ = strings.Cut(emoji, "\n")
	var lines []string
	seen := map[string]bool{}
	for _, m := range reactRef.FindAllStringSubmatch(all, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			lines = append(lines, "[react: "+m[1]+" "+emoji+"]")
		}
	}
	if strings.Contains(all, "REACTONLY") {
		return strings.Join(lines, "\n")
	}
	return answer + "\n\n" + strings.Join(lines, "\n")
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
		for steer() {
			if len(f.steers) > 0 && strings.Contains(f.steers[len(f.steers)-1], "FINISH") {
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
	for {
		f.steers = nil

		omp := f.name == "omp"
		m, ok := f.read()
		if !ok {
			return
		}
		if m["type"] != "prompt" {
			os.Exit(5)
		}
		f.prompt, _ = m["message"].(string)
		f.record("prompt", f.prompt, piImages(m))
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
		if f.has("PROGRESS") {
			for _, event := range []map[string]any{{"type": "message_update", "assistantMessageEvent": map[string]any{"type": "thinking_start", "contentIndex": 0}}, {"type": "message_update", "assistantMessageEvent": map[string]any{"type": "thinking_delta", "contentIndex": 0, "delta": "Checking public sources"}}, {"type": "tool_execution_start", "toolName": "web_search"}} {
				f.emit(event)
			}
		}
		f.failPost()
		if f.has("MCP_UI") {
			f.emit(map[string]any{"type": "extension_ui_request", "method": "notify", "message": "Sign in to MCP server slack in your browser: https://example.invalid/?state=x"})
		}
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
			f.record("steer", text, piImages(s))
			if f.has("SLOWACK") {
				time.Sleep(time.Second)
			}
			if strings.Contains(text, "NOACK") {
				_ = os.WriteFile(filepath.Join(f.state, "steer-held"), nil, 0o600)
				time.Sleep(30 * time.Second)
				return false
			}
			if f.has("STEERREPLY") {
				prev := f.prompt
				if len(f.steers) > 0 {
					prev = f.steers[len(f.steers)-1]
				}
				n := strconv.Itoa(len(f.steers) + 1)
				f.emit(map[string]any{"type": "message_start", "messageId": "r" + n, "message": map[string]any{"role": "assistant", "content": []any{}}})
				f.emit(map[string]any{"type": "message_end", "messageId": "r" + n, "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": "reply " + n + ": " + lastLine(prev)}}}})
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
		if f.has("LINGER=") {
			return
		}
	}
}

func (f *fake) claude() {
	for {
		f.steers = nil

		sid, resume := arg("--session-id"), arg("--resume")
		m, ok := f.read()
		if !ok {
			return
		}
		if m["type"] != "user" {
			os.Exit(5)
		}
		msg, _ := m["message"].(map[string]any)
		var images int
		f.prompt, images = claudeContent(msg["content"])
		f.record("prompt", f.prompt, images)
		f.failPre()
		id := sid
		if resume != "" {
			id = resume
		}
		fresh := f.turn == 0
		if id == "" || f.session(filepath.Join(f.state, "claude-"+id), resume != "") != nil || fresh && sid != "" && f.resumed && f.turn == 1 {
			fmt.Fprintln(os.Stderr, "fake: session unusable")
			os.Exit(6)
		}
		// Claude replays the user message's content as it was sent.
		replay := func(uuid string, content any) {
			f.emit(map[string]any{"type": "user", "session_id": id, "uuid": uuid, "isReplay": true, "parent_tool_use_id": nil, "message": map[string]any{"role": "user", "content": content}})
		}
		result := func(text string) {
			f.emit(map[string]any{"type": "result", "subtype": "success", "session_id": id, "is_error": false, "result": text})
		}
		f.emit(map[string]any{"type": "system", "subtype": "init", "session_id": id})
		switch {
		case f.has("EARLYRESULT"):
			result(f.answer())
			return
		case f.has("EARLYASK"):
			f.emit(map[string]any{"type": "control_request", "session_id": id})
			f.read()
			return
		}
		replay("u0", msg["content"])
		f.failPost()
		if f.has("STEERLATE") {
			s, ok := f.read()
			if !ok {
				result(f.answer())
				return
			}
			sm, _ := s["message"].(map[string]any)
			text, images := claudeContent(sm["content"])
			f.record("steer", text, images)
			result(f.answer())
			replay("u1", sm["content"])
			f.steers = append(f.steers, text)
			switch {
			case f.has("LATEHANG"):
				time.Sleep(30 * time.Second)
				return
			case f.has("LATECRASH"):
				fmt.Fprintln(os.Stderr, "fake: crashed in the late turn")
				os.Exit(4)
			case f.has("LATEFAIL"):
				f.emit(map[string]any{"type": "result", "subtype": "error_during_execution", "session_id": id, "is_error": true, "result": "fake late failure"})
			case f.has("LATEASK"):
				f.emit(map[string]any{"type": "control_request", "session_id": id})
			case f.has("LATEEXIT"):
				return
			default:
				result("late reply: " + lastLine(text))
			}
			for ok {
				_, ok = f.read()
			}
			return
		}
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
			text, images := claudeContent(sm["content"])
			f.record("steer", text, images)
			replay("u"+strconv.Itoa(len(f.steers)+1), sm["content"])
			f.steers = append(f.steers, text)
			return true
		})
		result(f.answer())
		if f.has("LINGER=") {
			return
		}
	}
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
	seenTurn := false
	for {
		f.steers = nil
		turnReq, ok := f.read()
		if !ok {
			return
		}
		if turnReq["method"] != "turn/start" {
			os.Exit(5)
		}
		if seenTurn {
			_ = f.session(filepath.Join(f.state, "codex-"+thread), true)
		}
		seenTurn = true
		turnID := fmt.Sprintf("turn-%d", f.turn)
		tp, _ := turnReq["params"].(map[string]any)
		var images int
		f.prompt, images = codexInput(tp["input"])
		f.record("prompt", f.prompt, images)
		f.failPre()
		reply(turnReq["id"], map[string]any{"turn": map[string]any{"id": turnID}})
		f.emit(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": thread, "turn": map[string]any{"id": turnID}}})
		f.failPost()
		if f.has("FINALFIRST") {
			f.emit(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": thread, "turnId": turnID, "item": map[string]any{"id": "i0", "type": "agentMessage", "text": "early answer: " + lastLine(f.prompt), "phase": "final_answer"}}})
		}
		if f.has("ASK_HUMAN") {
			f.emit(map[string]any{"id": 99, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": thread, "turnId": turnID, "reason": "fake wants to run a command"}})
			f.read()
			return
		}
		f.busy(func() bool {
			s, ok := f.read()
			if !ok {
				return false
			}
			sp, _ := s["params"].(map[string]any)
			text, images := codexInput(sp["input"])
			f.record("steer", text, images)
			reply(s["id"], map[string]any{"turnId": turnID})
			f.steers = append(f.steers, text)
			return true
		})
		f.emit(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": thread, "turnId": turnID, "item": map[string]any{"id": "i1", "type": "agentMessage", "text": f.answer(), "phase": "final_answer"}}})
		f.emit(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": thread, "turn": map[string]any{"id": turnID, "status": "completed"}}})
		if f.has("LINGER=") {
			return
		}
	}
}

func (f *fake) agy() {
	m, ok := f.read()
	if !ok || m["event"] != "user" {
		os.Exit(5)
	}
	msg, _ := m["message"].(map[string]any)
	f.prompt, _ = msg["content"].(string)
	f.record("prompt", f.prompt, 0)
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
	if f.has("SLOW") {
		time.Sleep(2 * time.Second)
	}
	if f.has("ASK_HUMAN") {
		// Headless AGY auto-denies the approval on stderr and still reports
		// SUCCESS: a soft denial that must not become an answer.
		fmt.Fprintln(os.Stderr, "Headless mode cannot prompt for tool approval: run_command was auto-denied.")
	}
	f.emit(map[string]any{"event": "result", "result": map[string]any{"conversation_id": conv, "status": "SUCCESS", "response": f.answer()}})
}

// piImages counts a Pi/OMP RPC prompt or steer's images, each
// {type:"image", data:<base64>, mimeType}; a malformed one ends the fake.
func piImages(m map[string]any) int {
	list, _ := m["images"].([]any)
	for _, raw := range list {
		im, _ := raw.(map[string]any)
		if im["type"] != "image" || !isImageType(im["mimeType"]) || !isBase64(im["data"]) {
			fmt.Fprintln(os.Stderr, "fake: malformed image")
			os.Exit(8)
		}
	}
	return len(list)
}

// claudeContent is a stream-json user message's text and image count: a
// string, or text blocks and {type:"image", source:{type:"base64", ...}}.
func claudeContent(raw any) (string, int) {
	if text, ok := raw.(string); ok {
		return text, 0
	}
	blocks, _ := raw.([]any)
	text, images := "", 0
	for _, b := range blocks {
		block, _ := b.(map[string]any)
		switch block["type"] {
		case "text":
			t, _ := block["text"].(string)
			text += t
		case "image":
			src, _ := block["source"].(map[string]any)
			if src["type"] != "base64" || !isImageType(src["media_type"]) || !isBase64(src["data"]) {
				fmt.Fprintln(os.Stderr, "fake: malformed image")
				os.Exit(8)
			}
			images++
		default:
			os.Exit(8)
		}
	}
	return text, images
}

// codexInput is an app-server turn input's text and image count: one text
// item, then {type:"localImage", path} items naming files that exist.
func codexInput(raw any) (string, int) {
	items, _ := raw.([]any)
	text, images := "", 0
	for i, it := range items {
		item, _ := it.(map[string]any)
		switch {
		case i == 0 && item["type"] == "text":
			text, _ = item["text"].(string)
		case i > 0 && item["type"] == "localImage":
			path, _ := item["path"].(string)
			if _, err := os.Stat(path); err != nil || !filepath.IsAbs(path) {
				fmt.Fprintln(os.Stderr, "fake: image path unreadable")
				os.Exit(8)
			}
			images++
		default:
			os.Exit(8)
		}
	}
	return text, images
}

func isImageType(v any) bool {
	s, _ := v.(string)
	return strings.HasPrefix(s, "image/")
}

func isBase64(v any) bool {
	s, _ := v.(string)
	_, err := base64.StdEncoding.DecodeString(s)
	return s != "" && err == nil
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
