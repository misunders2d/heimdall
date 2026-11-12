package scheduler

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
)

// ControlTool is one shared catalog entry. Hosts choose whether to expose this
// stdio adapter; employees must not receive it. The socket still authenticates
// every operation independently of the catalog or model.
var ControlTool = map[string]any{
	"name":        "heimdall_scheduler",
	"description": "Manage the existing Heimdall scheduler as the active interface-verified owner. Discover/list/show/history/checkpoint; propose create/edit/remove for real human confirmation in the originating conversation (from trusted T3 with manage, create/edit apply at once; removal is refused there); run/pause/resume/stop existing jobs. Acknowledge an exact failed receipt after destination reconciliation, without replaying or hiding history. task_add/task_list/task_show track a durable task of this owner request (id = Linear issue, one-line title): it is worked by the owner's own assistant and stays unaccepted until verified; nothing else (no verdict, worker or permission) is set here. task_pause/task_resume/task_close (id + request_id) change only your own task; close ends it. task_wake (id + request_id) wakes your own assistant task now instead of at its next timed wake, e.g. when the work it waits for is done. No identity or confirmation arguments. For a mutation generate one stable request_id; reuse it after an uncertain response, never create another retry id.",
	"inputSchema": map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"action"},
		"properties": map[string]any{
			"action":      map[string]any{"type": "string", "enum": []string{"discover", "list", "show", "history", "checkpoint", "create", "edit", "remove", "run", "pause", "resume", "stop", "acknowledge", "task_add", "task_list", "task_show", "task_wake", "task_pause", "task_resume", "task_close"}},
			"key":         map[string]any{"type": "string", "maxLength": 128},
			"revision":    map[string]any{"type": "string", "maxLength": 128},
			"outbound_id": map[string]any{"type": "integer", "minimum": 1},
			"id":          map[string]any{"type": "string", "maxLength": 64},
			"definition":  map[string]any{"type": "object", "description": "Existing canonical job definition, or changed fields for edit. Never a creator/role; unknown fields rejected by scheduler."},
			"request_id":  map[string]any{"type": "string", "minLength": 1, "maxLength": 128},
			"title":       map[string]any{"type": "string", "maxLength": 500, "description": "task_add only: the task's plain one-line title."},
		},
	},
	"annotations": map[string]any{"readOnlyHint": false, "destructiveHint": true, "idempotentHint": true, "openWorldHint": false},
}

// ServeControlMCP implements only the standard newline-delimited stdio tool
// surface. It owns no store or business connector and emits JSON-RPC only.
func ServeControlMCP(ctx context.Context, socket string, in io.Reader, out io.Writer) error {
	scan := bufio.NewScanner(in)
	scan.Buffer(make([]byte, 4096), ControlMaxBytes)
	enc := json.NewEncoder(out)
	initialized := false
	for scan.Scan() {
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scan.Bytes(), &req) != nil || req.JSONRPC != "2.0" {
			if err := enc.Encode(map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": -32700, "message": "Invalid JSON-RPC request"}}); err != nil {
				return err
			}
			continue
		}
		if len(req.ID) == 0 {
			continue
		}
		response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		rpcError := func(code int, message string) { response["error"] = map[string]any{"code": code, "message": message} }
		switch req.Method {
		case "initialize":
			initialized = true
			response["result"] = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "heimdall-scheduler", "version": "1"}}
		case "ping":
			response["result"] = map[string]any{}
		case "tools/list":
			if !initialized {
				rpcError(-32000, "Initialize first")
			} else {
				response["result"] = map[string]any{"tools": []any{ControlTool}}
			}
		case "tools/call":
			if !initialized {
				rpcError(-32000, "Initialize first")
				break
			}
			var call struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if json.Unmarshal(req.Params, &call) != nil || call.Name != "heimdall_scheduler" {
				rpcError(-32602, "Unknown tool")
				break
			}
			var args ControlRequest
			if err := strictJSON(call.Arguments, &args); err != nil {
				rpcError(-32602, "Invalid tool arguments")
				break
			}
			result, err := CallControl(ctx, socket, args)
			body, _ := json.Marshal(result)
			if err != nil {
				body, _ = json.Marshal(ControlResult{Error: err.Error()})
			}
			response["result"] = map[string]any{"content": []any{map[string]any{"type": "text", "text": string(body)}}, "isError": err != nil}
		default:
			rpcError(-32601, "Method not found")
		}
		if err := enc.Encode(response); err != nil {
			return err
		}
	}
	return scan.Err()
}
