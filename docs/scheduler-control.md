# Owner scheduler control and checkpoint workflows

This extends Heimdall's existing scheduler. It adds no scheduler process,
business connector, memory store, poller or default network service. Not activated
by these source changes. Deployment, bot connections and live probes require the
separate owner-approved live window.

## Control binding

Opt in in the existing private config:

```json
{"scheduler":{"enabled":true,"default_tz":"Europe/Kyiv","control":{"owner":"slack:OWNER_EMAIL"}}}
```

Merge this fragment with the existing scheduler section; preserve its other
settings. The selector must identify an actual owner with a recorded real private
platform conversation. It is not authority. The server uses the kernel Unix peer
UID (root or the existing `run_as.owner` UID), loads the stored platform binding
without message text, re-verifies Slack live, then re-evaluates existing access
authority (`ADMIN_USER_IDS` on hub) on **every call**, including replayed calls.
Unknown/employee/revoked/unbound callers fail closed. No new admission list.

Endpoint: `<data_dir>/scheduler-control/control.sock`, private directory 0700,
socket 0600. Only the existing Heimdall process hosts it. An existing socket is
never silently removed. After a crash, an operator must verify no process owns
the endpoint before removing a stale socket during the approved recovery window.
Keep parent data/config directories protected too; no employee traversal rights.

The control binding selects one owner and that owner's latest recorded private
conversation. It cannot list/read/manage another creator's jobs, even though
the bound owner has owner role. There are no caller, email, role, conversation,
shell, token, confirmation or approval arguments. Rebinding is private deployment
configuration, followed by an approved restart.

## CLI and shared tool catalog

```sh
heimdall sched control -socket "$CONTROL_SOCKET" < request.json
heimdall sched mcp -socket "$CONTROL_SOCKET"
```

CLI accepts exactly one JSON object. MCP exposes one tool, `heimdall_scheduler`,
with the same arguments:

```json
{"action":"discover|list|show|history|checkpoint|create|edit|run|pause|resume|stop|remove","id":"optional job id","definition":{},"request_id":"stable unique mutation id"}
```

Use `definition` only for create or edit. Create uses the existing full job
definition; edit uses changed fields. Canonical schedule/role/target/timeout
validation is shared with chat commands. `id` is required for operations on a
job. `request_id` is required for every mutation; reuse the **same complete
request** after a lost response. Reusing its ID for different input fails.
Mutation, result and operator UID/action provenance commit atomically. The audit
does not invent platform human ingress. Logs contain metadata, never prompts.

Create/edit/remove prepare ordinary 24-hour drafts and queue the normal private
confirmation card atomically. Only the same real person in the same platform
conversation can confirm. The adapter cannot confirm, synthesize a yes, or assert
a repeat-safe rerun. Pause/resume/run/stop use existing canonical command changes;
stop callbacks run after commit. No direct SQLite control from tools or CLI.
Normal chat owner creation/edit behavior is preserved.

[Catalog entry](examples/scheduler-catalog.json) uses the standard stdio MCP
command shape. Add **one entry to the existing shared owner tool catalog** used
by Codex/T3/Pi; keep it absent from employee routes. Replace both path placeholders
in private deployment configuration. If the shared catalog runs on another host,
its existing authorized SSH stdio command route may launch this same adapter on
the hub; the hub socket authenticates the resulting OS peer. Do not expose the
socket over TCP or add a daemon. Shared-catalog registration belongs to its owning
configuration repository and is a remaining root activation dependency; this
repository does not change installed harness/catalog configuration.

Protocol basis, checked 2026-11-05: [MCP stdio transport](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports),
[Linux Unix socket peer credentials](https://man7.org/linux/man-pages/man7/unix.7.html).
The adapter negotiates MCP 2025-03-26 and provides initialize/ping/tools list/call.

## Durable workflow results

An owner private-target agent job may opt into `checkpoint:true`, `quiet:true`,
fresh sessions. Employees and shared/AgentNet targets cannot use this surface.
The native run receives a bounded checkpoint snapshot, current version, previous
successful scan watermark, alert receipts and a fixed scan upper bound.

````text
NO_UPDATE
```heimdall-checkpoint
{"version":0,"complete":true,"through":"2026-11-05T10:00:00Z","state":{"unresolved":[]},"alerts":[],"retire":[]}
```
````

The outer example is illustrative: actual output has optional `NO_UPDATE` line
followed by one `heimdall-checkpoint` fenced block. With alerts, omit NO_UPDATE.
No other final text or files. A bare NO_UPDATE cannot prove scan completion and
fails rather than advancing state. `complete:false`, invalid/stale/future/backward
watermarks, missing required native tools, approvals, unclean results and failed
scans do not commit checkpoint progress or silently claim quiet success.

`state` is workflow-owned JSON, at most 24 KiB; Heimdall does not interpret Slack
triage rules. Maximum 20 alerts per scan, 4000 bytes per text, stable key/revision
at most 128 bytes each, 512 current alert keys. Exceeding limits fails without
advancing the watermark. `retire` may name up to 20 resolved keys with confirmed
sent receipts; pending/failed/ambiguous receipts cannot retire. Workflow must keep
enough minimal resolved fingerprints for safe overlapping source reads.

Checkpoint state, successful run and new alert outbox rows commit in **one existing
turn transaction**. The store independently checks job/creator/run/turn binding,
running state, version and watermark. A stop fence discards a late checkpoint.
One current receipt per job+creator+alert key refers to the existing outbox row.
Same key/revision/text reuses it regardless of pending/sending/sent/failed status;
changing text without a material revision fails. A material revision can enqueue
a new alert. Retired outbox rows remain normal delivery records.

Reader progress may advance after a complete scan while delivery is pending:
findings remain in workflow state and durable outbox/receipts. This prevents a
failed send from losing the scan. Receipts distinguish pending, sending, sent and
failed; only the router's successful whole-row delivery marks sent. Slack's existing
stable row/chunk IDs and ambiguous-send reconciliation remain delivery owner.
Partial chunk delivery never becomes a confirmed receipt. Do not create a new
alert key/revision to replay an uncertain or permanently failed delivery; root
must reconcile the actual destination first. Transport remains at-least-once;
an unresolvable crash/ack gap can still duplicate a platform message.

Optional `delivery_window` gates run starts and outbox attempts, including retries
and each Slack text chunk. End time is exclusive. Closed windows defer pending
rows to next opening, including weekends/DST; live chat replies can bypass deferred
windowed rows. An in-flight platform request may complete after cutoff; no transport
can retract a successful request already accepted before cutoff.

## Prepared owner Slack workflow

[Paused monitor definition](examples/owner-slack-monitor.json) preserves the
owner's request in context and keeps triage in the workflow. It uses existing
Codex/native owner model settings; no provider fallback override, new login or
owner_slack opt-in. Schedule union is exactly:

```json
{"cron":["0,30 12-20 * * MON-FRI","45 20 * * MON-FRI"]}
```

Zone Europe/Kyiv: opening 12:00, every half-hour through 20:30, closing 20:45.
Alert attempts only Monday–Friday 12:00–21:00. Opening catches up since last
successful scan, including overnight/weekend. Genuine discovery and edited-message
coverage must be demonstrated with the actual connector; unsupported coverage is
incomplete, never a quiet success. Bootstrap requires complete available backlog
discovery, not an invented 24-hour cutoff. No raw DM archive or secrets in state.

Only the captured owner private Bezos DM receives Heimdall bot output. The model
never posts, reacts, edits or sends as the owner via Slack connector tools. Root
must verify the configured binding really is that owner's bot DM before staging.

## Bounded native hub probe — prepared, not executed

Local fake tests prove launch preservation, not actual Apps/recall availability.
`owner_slack` native MCP remains disabled on scheduled turns; Codex Apps is a
different existing native path. `MEMENTO_PROCESSING=1` suppresses capture; it does
not itself prove manual recall works. No harness policy was weakened.

1. Root obtains the separate live-window/deployment approval, drains before the
   approved restart, installs the built binary and opt-in binding, then registers
   the one shared owner catalog entry. Preserve existing bot credentials, native
   owner configs and employee routes. Do not launch another bot consumer.
2. Copy [probe definition](examples/scheduled-codex-probe.json) to private task
   storage. Replace `<THREAD_REFERENCE>` with one owner-authorized source thread
   and `<RELEVANT_PROFESSIONAL_QUERY>` with one narrow, relevant recall query.
   Verify owner/workspace and private bot DM through real connector/platform data.
3. Through `sched control`, send `{"action":"create","request_id":"<unique>",
   "definition":<private probe definition>}`. A real owner confirms its draft in
   the captured bot conversation. It remains paused. No fake Slack ingress.
4. `{"action":"run","id":"<probe-id>","request_id":"<unique-run-id>"}` runs a
   real **scheduled Codex profile**, fresh session, nonprivate harness scope,
   capture off. Probe must actually read the selected Slack thread, call native
   `mcp__memento__recall`, and verify capture sentinel without revealing private
   source content. Tool visibility alone is not acceptance. Any native approval,
   auth failure or missing capability ends proof; do not log in or bypass it.
5. `{"action":"history","id":"<probe-id>"}` must report success, and
   `{"action":"checkpoint","id":"<probe-id>"}` must show advanced watermark,
   minimal capability flags and exactly one **sent** receipt. Independently inspect
   the owner's private bot DM via its authorized read interface for that one alert
   and inspect minimal native tool/result metadata for successful reads. No private
   transcript dumps. Rerun once with another run request ID: checkpoint version
   advances while the same capability alert outbound ID remains, no new alert.
6. Stage the monitor definition by the same supported create path, confirm paused,
   verify real discovery/update/pagination coverage and resulting checkpoint with
   one approved manual scan. Resume only after that evidence passes. Remove the
   probe using control `remove` plus real human confirmation. No T3 schedule.

## Build, installation and rollback boundary

```sh
CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -o /tmp/heimdall-owner-monitor ./cmd/heimdall
GOMAXPROCS=2 go vet -p 2 ./...
GOMAXPROCS=2 go test -race -p 2 ./...
```

Schema migration adds checkpoint/receipt/control tables only, through normal
store open. No production DB edited by this implementation task. Before an approved
deployment, use the existing operational backup procedure for a consistent store
backup. To disable the extension, pause the monitor, remove the catalog entry and
control opt-in, then use the separately approved drain/restart procedure. Older
binaries reject a newer schema version: full binary rollback requires the matching
consistent pre-upgrade store backup; restoring it loses subsequent writes. Never
edit SQLite/version metadata to force rollback. No deployment or rollback executed
here.
