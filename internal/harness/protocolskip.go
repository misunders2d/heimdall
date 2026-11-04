package harness

import (
	"encoding/json"
	"errors"
	"log"
)

// Never log raw protocol data or error strings (which can contain values).
func skippedProtocolLine(name string, raw []byte, err error) string {
	kind := "syntax"
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		kind = "type"
	}
	var e struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &e)
	event := "unknown"
	switch e.Type {
	case "system", "assistant", "user", "result", "stream_event", "response", "agent_end", "message_end", "message_update", "control_request":
		event = e.Type
	}
	log.Printf("native protocol skipped harness=%s length=%d event=%s error=%s", name, len(raw), event, kind)
	return event
}
