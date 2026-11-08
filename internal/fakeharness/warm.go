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
		if f.name == "codex" {
			session = fmt.Sprintf("warm-thread-%d", os.Getpid())
		}
	}
	turn := 0
	exitOnNext := false
	var collision func()
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
			if method == "thread/resume" {
				params, _ := m["params"].(map[string]any)
				if id, ok := params["threadId"].(string); ok {
					session = id
				}
			}
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
		f.record("prompt", text, 0)
		if collision != nil {
			call := collision
			collision = nil
			call()
		}
		if strings.Contains(text, "EXIT_ON_NEXT") {
			exitOnNext = true
		}
		if f.name == "claude" && strings.Contains(text, "ASK_PRE_REPLAY") {
			f.emit(map[string]any{"type": "control_request", "session_id": session, "request_id": "ask", "request": map[string]any{"subtype": "can_use_tool"}})
			continue
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
		if strings.Contains(text, "SCHEDULER_HOLD") {
			for {
				if _, err := os.Stat(filepath.Join(f.state, "scheduler-release")); err == nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
		if strings.Contains(text, "HANG") {
			continue
		}
		if f.name == "pi" && strings.Contains(text, "MCP_LOGIN") {
			f.emit(map[string]any{"type": "extension_ui_request", "method": "notify", "message": "Sign in to MCP server t3 in your browser: https://example.invalid/authorize?state=" + id})
			f.emit(map[string]any{"type": "extension_ui_request", "method": "input", "id": id, "title": "Waiting for sign-in to t3."})
			if !strings.Contains(text, "EXPIRE") {
				response, ok := f.read()
				if !ok || response["type"] != "extension_ui_response" || response["id"] != id || response["value"] != "http://localhost:18765/callback?state="+id+"&code=synthetic" {
					return
				}
			}
			// Native command completion, including Pi's own input timeout,
			// leaves the transport available for another login.
			f.emit(map[string]any{"type": "response", "id": m["id"], "success": true, "data": map[string]any{"disposition": "handled"}})
			continue
		}
		var background func()
		if strings.Contains(text, "BACKGROUND") {
			task := id + "-task"
			runID := id + "-run"
			f.emit(map[string]any{"type": "system", "subtype": "task_started", "session_id": session, "task_id": task, "run_id": runID})
			contextText := f.inputContext + "\n" + text
			background = func() {
				f.emit(map[string]any{"type": "system", "subtype": "task_updated", "session_id": session, "task_id": task, "patch": map[string]any{"status": "completed"}})
				if strings.Contains(text, "BG_TERMINAL_ONLY") {
					return
				}
				f.emit(map[string]any{"type": "system", "subtype": "task_notification", "session_id": session, "task_id": task, "run_id": runID, "summary": "Synthetic background task completed"})
				answer := "BACKGROUND_DELIVERED"
				if strings.Contains(text, "BG_JOB") {
					answer += "\n```heimdall-job\n{\"name\":\"unsolicited\",\"schedule\":{\"every\":\"1h\"},\"kind\":\"notify\",\"text\":\"no\",\"tz\":\"UTC\",\"to\":\"here\"}\n```"
				}
				if strings.Contains(text, "BG_REACT") {
					for _, m := range reactRef.FindAllStringSubmatch(contextText, -1) {
						answer += "\n[react: " + m[1] + " 👍]"
					}
				}
				if strings.Contains(text, "BG_FILE") {
					if m := outbox.FindStringSubmatch(contextText); m != nil {
						_ = os.WriteFile(filepath.Join(m[1], "late.txt"), []byte("late artifact"), 0600)
					}
				}
				f.emit(map[string]any{"type": "assistant", "session_id": session, "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "id": "background-" + task, "content": []any{map[string]any{"type": "text", "text": answer}}}})
				if strings.Contains(text, "BG_NORESULT") {
					return
				}
				result := map[string]any{"type": "result", "session_id": session, "uuid": task + "-result", "result": answer, "origin": map[string]any{"kind": "task-notification", "runId": runID}}
				f.emit(result)
				if strings.Contains(text, "BG_DUPLICATE") {
					f.emit(result)
				}
			}
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

		if strings.Contains(text, "AUTONOMOUS") && (f.name == "pi" || f.name == "codex") {
			go func() {
				time.Sleep(20 * time.Millisecond)
				nativeID := "autonomous-turn"
				if f.name == "pi" {
					f.emit(map[string]any{"type": "agent_start"})
				} else {
					f.emit(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": nativeID}}})
				}
				for {
					if _, err := os.Stat(filepath.Join(f.state, "release-autonomous")); err == nil {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if f.name == "pi" {
					f.emit(map[string]any{"type": "message_start", "messageId": nativeID, "message": map[string]any{"role": "assistant"}})
					f.emit(map[string]any{"type": "message_end", "messageId": nativeID, "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": "AUTONOMOUS_ANSWER"}}}})
					f.emit(map[string]any{"type": "agent_end"})
					f.emit(map[string]any{"type": "agent_settled"})
				} else {
					f.emit(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": session, "turnId": nativeID, "item": map[string]any{"type": "agentMessage", "phase": "final_answer", "text": "AUTONOMOUS_ANSWER"}}})
					f.emit(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": nativeID, "status": "completed"}}})
				}
			}()
		}
		if background != nil {
			switch {
			case strings.Contains(text, "BACKGROUND_COLLISION"):
				collision = background
			case strings.Contains(text, "BACKGROUND_NEVER"):
			case strings.Contains(text, "BACKGROUND_SAME_WRITE"):
				background()
			default:
				delay := 100 * time.Millisecond
				if strings.Contains(text, "BG_SLOW") {
					delay = 1500 * time.Millisecond
				}
				go func() { time.Sleep(delay); background() }()
			}
		}
		if strings.Contains(text, "EXIT_IDLE") {
			return
		}
	}
}
