# Native output between person turns

Implementation branch: `fix/between-turn-delivery`, based on `aed77cd`.
No deployment or live transport connection is part of this change.

## Reproduction and protocol evidence (2026-11-07)

Real installed binaries: Claude Code 2.1.319, Codex CLI 0.167.0, Pi 1.5.0.
All probes used a scratch HOME/config and working directory, synthetic input,
`MEMENTO_PROCESSING=1`, no owner credential copies, no live chat transport.

A real Claude stream-json process with warm stdin was driven by a loopback
synthetic Anthropic Messages SSE server and a dummy local-only key. The model
responses were synthetic; Bash and the CLI's task lifecycle were real. Bash ran
`sleep 5; printf BETWEEN_TURN_PROBE` with `run_in_background:true`.
Observed sequence, with no new stdin after the initial person message:

| Elapsed | Native event |
| --- | --- |
| 0.805 s | `system/background_tasks_changed`, then `system/task_started` |
| 0.860 s | `result`, `result:"STARTED"` |
| 5.814 s | `system/task_updated`, patch status `completed` |
| 5.814 s | `system/task_notification`, status `completed`, summary of successful Bash completion |
| 5.814 s | `system/background_tasks_changed`, `tasks:[]` |
| 5.837 s | `system/init`, same session |
| 5.856 s | root `assistant`, text `FINISHED BETWEEN_TURN_PROBE` |
| 5.861 s | `result`, `result:"FINISHED BETWEEN_TURN_PROBE"` |

Relevant shapes (identifiers replaced):

```json
{"type":"system","subtype":"task_started","session_id":"SESSION","task_id":"TASK","run_id":"RUN","tool_use_id":"TOOL","task_type":"local_bash","is_backgrounded":true}
{"type":"system","subtype":"task_notification","session_id":"SESSION","task_id":"TASK","run_id":"RUN","status":"completed","summary":"Background command completed (exit code 0)","output_file":"NATIVE_PATH"}
{"type":"assistant","session_id":"SESSION","parent_tool_use_id":null,"message":{"id":"MESSAGE","role":"assistant","content":[{"type":"text","text":"FINISHED BETWEEN_TURN_PROBE"}]}}
{"type":"result","session_id":"SESSION","uuid":"RESULT_ID","subtype":"success","is_error":false,"result":"FINISHED BETWEEN_TURN_PROBE","origin":{"kind":"task-notification","producer":"session-task","runId":"RUN"}}
```

The `origin.runId` matches lifecycle `run_id`. Native `output_file` is never
opened or used as an outbound path. Task field definitions were also compared
with the [official SDK types](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/types.py).

Unauthenticated real-provider probes: Claude emitted `assistant` with
`authentication_failed` then `result:"Not logged in · Please run /login"`.
Pi RPC returned `response` with `success:false`, error beginning
`No API key found for the selected model.` Codex accepted `initialize`,
`thread/start`, `turn/start`, emitted `turn/started` and then reported
`401 Unauthorized: Missing bearer or basic authentication in header`.
Consequently no authenticated-provider background completion was tested for
Codex or Pi. These probes do **not** prove those harnesses cannot emit idle
output. Their generic native `agent_start` / `turn/started` continuation paths
remain unverified against real autonomous work.

## Dispatch and delivery

Warm output is split at complete protocol lines under the worker lock. It can
no longer disappear into a finished bridge. Claude assistant blocks wait for
the native result's origin; first valid root assistant output still commits
the side-effect boundary, so buffering never permits fallback after work began.
The exact replay of pending person input selects the new decoder. A task result
can therefore arrive before that replay or while the new person turn runs
without changing that turn's answer or reply target. Pi/Codex native input waits
for an already active passive turn's result before being sent, within the
existing native run/lifetime limits. Pending person starts have their own
cancellation scope: stop/reset and router shutdown cancel that request before
releasing the native gate. They cannot launch or send stopped input after the
background process exits. A failed warm reuse always starts a new session, even
if retirement already consumed its background flags; the service log says so, never the chat. Native result UUIDs are
deduplicated within a bounded recent window. Session, replay and subagent checks
remain enforced by the existing adapters.

Native task starts are persisted in one transaction as separate turns, with
harness and the matching native session marked unclean. A failed transaction
refuses the native continuation, stops its process and reports the loss. A restart reports those turns
as interrupted and never re-runs them. The exact live retained worker can still
accept another person input while its background record is outstanding. That
exact worker requirement travels with the launch request and is rechecked when
the pool claims it; eviction or exit between lookup and claim always starts a
new native session. An unclean session without that worker always starts fresh. Native results use the existing answer,
reaction, checked-outbox, redaction, lifecycle and outbound delivery flows.
They retain the originating verified principal, role, conversation, reply
reference and outbox. Admission/role and reset/harness-selection generations
are checked again on delivery. They never apply or propose scheduler changes,
and never overwrite a newer person turn's native session binding. Original
reply publication precedes a continuation even if both arrive in one stdout
write. Unchanged files already collected from an outbox are not sent again;
new or changed files still pass all existing checks. An empty Claude result may
use legitimate final assistant text from that native turn. Pre-tool commentary
is cleared by a tool call; tool-only output without a final remains a failure,
with no invented answer or automatic replay after native acceptance.

Resource limits remain bounded. Known background tasks are not silently lost:
idle expiry, lifetime expiry, LRU eviction, launch replacement and process exit
produce an interruption notice, without retrying work. Interrupted matching
native sessions are marked unclean. The ordinary idle timeout remains 900 s,
maximum lifetime 14400 s; no promise of indefinite background execution is
made. Caller/owner stop also reaches idle workers with outstanding background work,
with one acknowledgement and no cancelled native final. Reset, harness changes
and revoked admission settle accepted continuation rows and stop their typing.
Shutdown leaves unsettled native work for the existing recovery flow.
Outbound delivery retains its existing at-least-once semantics.

## Verification and limits

Focused fake-native tests cover Telegram and Slack destinations, a second
person input before replay, a running later person turn, different callers and
conversations, same-write/fragmented output, duplicate native result UUIDs,
reaction references, delayed checked files, scheduler-block suppression,
reset/harness/role/admission invalidation and row cleanup, restart while a task
is running or between notification and result, idle stop authority, pre-replay
native approval without fallback, exactly one loss notice after a buffered
answer, idle/lifetime/LRU/exit/launch interruption notices, the atomic continuation transaction/refusal, eviction/exit between live-worker
lookup and claim, generic Pi/Codex passive/new-input collisions, and the
assistant-only side-effect boundary. These are fake chat interfaces, not live
Telegram or Slack acceptance. The real-Claude transcript above validates the
native trigger/protocol with a synthetic model. A separate integration probe
ran that real CLI through the patched router and fake Telegram/Slack outbound
interfaces: Telegram delivered STARTED at 0.967 s and FINISHED at 3.222 s; Slack
at 0.734 s and 2.939 s. Both retained the original conversation/thread/reply
reference with no new person input. This is not paid-provider or live-bot
validation.

Local synthetic transcripts and probe programs are retained outside the repo;
no raw transcript, generated report, secret, host detail or binary is committed.

## Proposed deployment — requires owner approval, not executed

Use the approved production release procedure and actual paths supplied by the
operator; the placeholders below are intentional. No schema migration or config
change is required by this patch.

1. Fetch the branch over SSH and check out the reviewed commit in a release
   clone. Confirm `git status --short --branch` is clean and HEAD equals the
   reviewed SHA. Run `go vet ./...`, `go test -race -count=1 ./...` and
   `CGO_ENABLED=0 go build -ldflags "-X main.version=REVIEWED_SHA" -o RELEASE ./cmd/heimdall`.
2. Before the separately approved production stop, inspect scheduler state:
   `CURRENT_BINARY sched health -config CONFIG`,
   `CURRENT_BINARY sched list -config CONFIG` and
   `CURRENT_BINARY sched show JOB_ID -config CONFIG` for each relevant job's
   run history. Require no running job and none due within three minutes;
   health alone does not establish those facts. If deferred, obtain fresh
   restart approval.
3. Run `CURRENT_BINARY drain -config CONFIG` and wait for idle. Stop the approved
   production unit once: `systemctl stop APPROVED_UNIT`. Now preserve the
   current binary and config in an owner-only release backup, and make a
   consistent database backup with SQLite's backup API/CLI, for example
   `sqlite3 DATA_DB ".backup 'BACKUP_DB'"`. Preserve rollback material; never
   copy an active WAL database blindly.
4. Install `RELEASE` at the existing binary path, run
   `RELEASE check-config -config CONFIG`, then
   `systemctl start APPROVED_UNIT` through its existing secret wrapper/unit.
   Never run another poller or Socket Mode consumer. No config change or
   credential copy to another account is needed.
5. Independently verify `systemctl is-active APPROVED_UNIT`, the installed
   binary's `version`, and application health. In an approved single-consumer
   window, run a synthetic delayed background completion in Telegram and Slack:
   initial reply, completion without a new person message, subsequent normal
   turn, collision with another input, and stop/reset. Verify correct thread,
   one copy of each answer, no leaked paths, status cleanup, file/reaction
   behavior and durable outbound receipts. Recheck scheduler health/list/show.
   The existing scheduler recovery clears the drain hold during startup; there
   is no additional manual release command.
6. If acceptance fails, separately approve rollback: drain/stop safely, restore
   the saved binary (schema unchanged), start via the existing wrapper and
   verify health. Preserve the new database/outbound evidence; do not restore a
   database backup or discard queued messages without an explicit decision.
