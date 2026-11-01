package fakeharness

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// warm speaks multiple native turns on one stdin/stdout transport. Its startup
// delay and PID ledger distinguish process reuse from native session resume.
func (f *fake) warm() {
	if os.Getenv("FAKE_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
	}
	if ms, _ := strconv.Atoi(os.Getenv("FAKE_STARTUP_MS")); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	file, _ := os.OpenFile(filepath.Join(f.state, "pids"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	fmt.Fprintln(file, os.Getpid())
	file.Close()
	session := arg("--session-id")
	if session == "" {
		session = arg("--resume")
	}
	if session == "" {
		session = "warm-thread"
	}
	turn := 0
	exitOnNext := false
	for {
		m, ok := f.read()
		if !ok {
			return
		}
		typ, _ := m["type"].(string)
		method, _ := m["method"].(string)
		reply := func(v any) { f.emit(map[string]any{"id": m["id"], "result": v}) }
		switch method {
		case "initialize":
			reply(map[string]any{})
			continue
		case "initialized":
			continue
		case "thread/start", "thread/resume":
			reply(map[string]any{"thread": map[string]any{"id": session}})
			continue
		}
		if typ == "abort" || typ == "control_request" || method == "turn/interrupt" {
			_ = os.WriteFile(filepath.Join(f.state, "interrupt"), []byte(typ+method), 0600)
			return
		}
		if typ != "prompt" && typ != "user" && method != "turn/start" {
			continue
		}
		turn++
		if exitOnNext {
			return
		}
		id := "turn-" + strconv.Itoa(turn)
		text, _ := m["message"].(string)
		if typ == "user" {
			msg, _ := m["message"].(map[string]any)
			text, _ = claudeContent(msg["content"])
		}
		if method == "turn/start" {
			params, _ := m["params"].(map[string]any)
			text, _ = codexInput(params["input"])
		}
		if strings.Contains(text, "EXIT_ON_NEXT") {
			exitOnNext = true
		}
		switch f.name {
		case "pi", "omp":
			f.emit(map[string]any{"type": "response", "id": m["id"], "command": "prompt", "success": true})
			f.emit(map[string]any{"type": "agent_start"})
		case "claude":
			f.emit(map[string]any{"type": "user", "session_id": session, "uuid": id, "isReplay": true, "parent_tool_use_id": nil, "message": map[string]any{"role": "user", "content": text}})
		case "codex":
			reply(map[string]any{"turn": map[string]any{"id": id}})
			f.emit(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": id}}})
		}
		if strings.Contains(text, "HANG") {
			continue
		}
		answer := fmt.Sprintf("pid=%d turn=%d %s", os.Getpid(), turn, text)
		if strings.Contains(text, "DUPLICATE_TAG") {
			answer += "\n\n— " + f.name
		}
		switch f.name {
		case "pi", "omp":
			f.emit(map[string]any{"type": "message_start", "messageId": id, "message": map[string]any{"role": "assistant"}})
			f.emit(map[string]any{"type": "message_end", "messageId": id, "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": answer}}}})
			f.emit(map[string]any{"type": "agent_end"})
			if f.name == "omp" {
				f.emit(map[string]any{"type": "prompt_result", "id": m["id"], "status": "completed", "sessionSettled": true})
			} else {
				f.emit(map[string]any{"type": "agent_settled"})
			}
		case "claude":
			f.emit(map[string]any{"type": "result", "session_id": session, "result": answer})
		case "codex":
			f.emit(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": session, "turnId": id, "item": map[string]any{"type": "agentMessage", "text": answer, "phase": "final_answer"}}})
			f.emit(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": id, "status": "completed"}}})
		}
		if strings.Contains(text, "EXIT_IDLE") {
			return
		}
	}
}
