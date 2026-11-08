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

Merge this fragment with the existing scheduler section; preserve other settings.
The owner selector remains accepted for configuration compatibility; it grants no
request identity. On every call, the server checks kernel Unix peer UID (root or
`run_as.owner`), walks kernel PID ancestry to a registered harness process, and
resolves that worker's **current active person turn**. Its verified principal and
exact conversation (including thread) supply the origin. Slack is re-verified
live and current owner admission is checked, including on idempotent replays.
A request captures the native input generation before waiting for other control
operations and validates the same generation after live identity checks and before
commit. Kernel pidfds bind both socket peer and harness process lifetime, preventing
PID reuse from selecting another conversation. Linux SO_PEERPIDFD support (6.5+)
is required; unsupported/restricted kernels disable controls by failing closed.

Unknown, employee, revoked, idle, scheduled and passive continuation callers fail
closed. No model argument, environment identity, or result destination supplies
origin. Warm MCP servers resolve anew on every call; accepted steers share the
same verified caller/conversation. Ambiguous background/foreground overlap is
refused. Detached or reparented MCP servers also fail closed. Shared contexts cannot read
raw checkpoints, show or history; list uses the existing shared-chat renderer.

Endpoint: `<data_dir>/scheduler-control/control.sock`, private directory 0700,
socket 0600. Only the existing Heimdall process hosts it. Startup refuses non-sockets, symlinks and foreign owners. Under the validated
private directory, it recovers an owned socket only after a bounded dial proves
ECONNREFUSED and a second Lstat verifies the same inode. A live listener, timeout
or other error never permits removal. Control errors are logged as unavailable;
the otherwise-functional router continues.
Keep parent data/config directories protected too; no employee traversal rights.

Legacy `scheduler.control.source` remains parseable but is no longer used to
route or authorize control requests. There is no latest-conversation fallback.
Create/edit/remove cards go to the originating conversation, independently from
`to`; only real platform human input can consume their confirmation. Shared
conversation privacy rules still apply. A request ID reused from another origin
is rejected, rather than replaying a card from another chat.

Existing jobs/drafts are not moved, recreated or reconfirmed. Their correction
requires a separate explicit operator decision. Standalone CLI calls outside an
active harness process now fail closed; direct ingress job commands remain
available. No caller, role, source, confirmation or approval tool fields exist.

## CLI and shared tool catalog

```sh
heimdall sched control -socket "$CONTROL_SOCKET" < request.json
heimdall sched mcp -socket "$CONTROL_SOCKET"
```

CLI accepts exactly one JSON object. MCP exposes one tool, `heimdall_scheduler`,
with the same arguments:

```json
{"action":"discover|list|show|history|checkpoint|create|edit|run|pause|resume|stop|remove|acknowledge","id":"optional job id","definition":{},"request_id":"stable unique mutation id"}
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
stop callbacks run once after the original commit; a cached reply never stops a newer run. No direct SQLite control from tools or CLI.
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
fails rather than advancing state. Valid partial coverage uses `complete:false`,
`through` equal to the prior successful watermark (empty initially), and `cause`
one of `pagination`, `budget`, `coverage`, `overflow`. State/cursors and findings
commit atomically; the global successful watermark stays unchanged. The stored
scan upper bound stays fixed across subsequent partial runs/restarts. Partial
runs have explicit `partial` status/cause and never claim quiet/full success.
Only an actual state/cursor or new finding change resets consecutive failures;
unchanged partials emit a fixed stall notice and reach normal `pause_after`.
Invalid/stale/future/backward watermarks, missing successful required native tools,
any native tool failure, approvals, auth denial and unclean results do not commit.
Tool starts are telemetry only; `expect.tools` uses completed successful calls.
Codex MCP names use native `server`/`tool`, as `mcp__<server>__<tool>`.

`state` is workflow-owned JSON, at most 24 KiB; Heimdall does not interpret Slack
triage rules. At most 512 active receipt revisions/findings, 4000 bytes per text,
stable key/revision at most 128 bytes each; the result block is bounded at 2.2 MB.
All findings receive durable outbox IDs in the same transaction. At most twenty
new/held rows become pending per clean scan; overflow stays `held` across restart
until later scans release it, retaining original text and IDs. Exceeding limits fails without
advancing the watermark. `retire` may name up to 20 resolved keys with confirmed
sent receipts; pending/failed/ambiguous receipts cannot retire. Workflow must keep
enough minimal resolved fingerprints for safe overlapping source reads.

Checkpoint state, successful run and new alert outbox rows commit in **one existing
turn transaction**. The store independently checks job/creator/run/turn binding,
running state, version and watermark. A stop fence discards a late checkpoint.
Receipts are keyed by job+creator+key+revision. Same key/revision always reuses
the original stored text and outbox ID, even when a later model paraphrases it.
A material revision can enqueue a new alert while older unresolved receipts
remain visible. Retired sent fingerprints remain stored for deduplication;
retired outbox rows remain normal delivery records.

Reader progress may advance after a complete scan while delivery is pending:
findings remain in workflow state and durable outbox/receipts. This prevents a
failed send from losing the scan. Receipts distinguish pending, sending, sent and
failed (plus held overflow); only the router's successful whole-row delivery marks sent. Slack's existing
stable row/chunk IDs and ambiguous-send reconciliation remain delivery owner.
Partial chunk delivery never becomes a confirmed receipt. Do not create a new
alert key/revision to replay an uncertain or permanently failed delivery. A
terminal alert failure atomically queues one fixed notice to the creator's private
bot chat with the same delivery window. Failure of that notice cannot recurse.
Root must reconcile the actual destination first. Then this authenticated control
action acknowledges exactly one failed receipt without changing/replaying its
outbox row; failed history remains visible and the operator action is audited:

```json
{"action":"acknowledge","id":"JOB_ID","key":"ITEM_KEY","revision":"REVISION","outbound_id":123,"request_id":"STABLE_ACK_ID"}
```

Pending, held and sending receipts cannot be acknowledged or retired. A new
revision cannot hide an older failed receipt. Transport remains at-least-once;
an unresolvable crash/ack gap can still duplicate a platform message.

Supervised requests (2026-11-08): an alert may carry `"to"` (`tg:<chat>[/<topic>]`
or `slack:<channel>[/<thread ts>]`) so a request's result returns to its original
conversation. A destination is never authority. It is accepted only when the
job's creator, or its own identity alias in the current access snapshot that is
an owner now, sent live verified input in that exact **private** conversation
and thread; the scheduler checks before the run ends and the store again in the
commit transaction. Group, public, other-person, scheduled-only, malformed or
unverifiable destinations fail the whole scan (no checkpoint, receipt or outbox
row), with the usual fixed failure notice. Omitted `to` keeps the job target.
Each receipt's `to` is read from its actual outbox row; key/revision dedupe is
unchanged, so a repeat naming another chat neither re-sends nor re-routes. The
task list and its stages live in the tracker (Linear); Heimdall keeps no task state.

Optional `delivery_window` gates run starts and outbox attempts, including retries
and each Slack text chunk. End time is exclusive. Closed windows defer pending
rows to next opening, including weekends/DST; live chat replies can bypass deferred
windowed rows. Drain excludes only pending rows whose window is currently closed;
in-window backoff, ordinary future retries and every sending row still count.
Deferred rows survive restart. An in-flight platform request may complete after cutoff; no transport
can retract a successful request already accepted before cutoff.

## Prepared owner Slack workflow

[Paused monitor definition](examples/owner-slack-monitor.json) preserves the
owner's request in context and keeps triage in the workflow. It uses the
selected agent's native owner settings; no provider fallback override, new login or
owner_slack opt-in. Schedule union is exactly:

```json
{"cron":["0,30 12-20 * * MON-FRI","45 20 * * MON-FRI"]}
```

Monitor timeout is 10 minutes, below the 15-minute gap before closing.
Zone Europe/Kyiv: opening 12:00, every half-hour through 20:30, closing 20:45.
Alert attempts only Monday–Friday 12:00–21:00. Opening catches up since last
successful scan, including overnight/weekend. Genuine discovery and edited-message
coverage must be demonstrated with the actual connector; unsupported coverage is
incomplete, never a quiet success. Bootstrap requires complete available backlog
discovery, not an invented 24-hour cutoff. No raw DM archive or secrets in state.

Prepared definitions do not pin a harness. At turn start, the normal router
reads the current `!use` preference of the creator-bound source conversation
(interface + chat + thread). Later `!use` changes apply to future wakeups. A job's
explicit `harness` override still wins; `!use default` uses the configured default
order. The synthetic `sched:<job>` session conversation never supplies routing
preference, and changes in other conversations do not affect this binding.
Creator admission and live Slack identity are still checked at fire and turn
start. No global last-agent registry, connector discovery or delegation layer
is added.

The selected agent owns tool choice, deliberate recall and any delegation to an
existing capable agent such as Codex. Missing capabilities produce its truthful
outcome or blocker. Slack-specific instructions belong only to these job prompts
or the agent's own configuration. Heimdall adds no native tool allowlist, plugin
lookup, app catalog scan, special sandbox or approval mode for this workflow.
Ordinary owner automatic modes, employee confinement and native policy hooks
remain unchanged. Scheduled turns retain `MEMENTO_PROCESSING=1`, fresh sessions
by default and nonprivate harness scope; the `owner_slack` MCP opt-in remains
live-only. No native-profile contract is required.

Generic fakes cover checkpoint/delivery behavior, successful-tool telemetry and
routing after `!use` changes, including explicit overrides and unrelated
conversation isolation. Prepared jobs do not require direct Slack/Memento tool
names in the parent's telemetry: delegated work belongs to the agent. Optional
generic `expect.tools` checks remain available for jobs that need direct tool
completion evidence. Fakes do not prove actual Slack/recall access, edited-message
coverage or delivery. Those remain the separately approved acceptance below.

Only the captured owner private Bezos DM receives Heimdall bot output. The model
never posts, reacts, edits or sends as the owner via Slack connector tools. Root
must verify the configured binding really is that owner's bot DM before staging.

## Bounded native hub probe — prepared, not executed

Local fake tests prove routing and launch preservation, not actual Slack/recall availability.
`owner_slack` native MCP remains disabled on scheduled turns. The selected agent
uses its existing capabilities and owns any required delegation. `MEMENTO_PROCESSING=1` suppresses capture; it does
not itself prove manual recall works. No harness policy was weakened.

1. Root obtains the separate live-window/deployment approval, drains before the
   approved restart, installs the built binary and opt-in binding, then registers
   the one shared owner catalog entry. Preserve existing bot credentials, native
   owner configs and employee routes. Do not launch another bot consumer.
2. Copy [probe definition](examples/scheduled-agent-probe.json) to private task
   storage. Replace `<THREAD_REFERENCE>` with one owner-authorized source thread
   and `<RELEVANT_PROFESSIONAL_QUERY>` with one narrow, relevant recall query.
   Verify owner/workspace and private bot DM through real connector/platform data.
3. Through `sched control`, send `{"action":"create","request_id":"<unique>",
   "definition":<private probe definition>}`. A real owner confirms its draft in
   the captured bot conversation. It remains paused. No fake Slack ingress.
4. `{"action":"run","id":"<probe-id>","request_id":"<unique-run-id>"}` runs a
   real **scheduled wakeup of the currently selected agent**, fresh session, nonprivate harness scope,
   capture off. Probe must actually read the selected Slack thread, perform deliberate owner/professional recall, and use the enforced capture-off launch context without revealing private
   source content. Independently inspect the launch receipt for capture-off env. Tool visibility alone is not acceptance. Any native approval,
   auth failure or missing capability ends proof; do not log in or bypass it.
5. `{"action":"history","id":"<probe-id>"}` must report success, and
   `{"action":"checkpoint","id":"<probe-id>"}` must show advanced watermark,
   minimal capability flags and exactly one **sent** receipt. Independently inspect
   the owner's private bot DM via its authorized read interface for that one alert
   and inspect minimal tool/result evidence from the executing agent for successful
   reads. A parent's delegation tool alone is not proof. No private
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

Schema migration adds/resumes checkpoint range/status and retains per-revision
receipt/acknowledgement/notice fields in the existing store, through normal
store open. No production DB edited by this implementation task. Before an approved
deployment, use the existing operational backup procedure for a consistent store
backup. To disable the extension, pause the monitor, remove the catalog entry and
control opt-in, then use the separately approved drain/restart procedure. Older
binaries reject a newer schema version: full binary rollback requires the matching
consistent pre-upgrade store backup; restoring it loses subsequent writes. Never
edit SQLite/version metadata to force rollback. No deployment or rollback executed
here.


## Narrow Pi Durable reliability adoption

Inspected pinned public Pi Durable 1.5 source only; no package installed or runtime
ported: [effect sandwich and unknown commit](https://github.com/earendil-works/pi/blob/5a31a6d5f418ae6591d219bbaf2f45fe37ff4ee2/packages/durable/docs/spec.md),
[stored/current safe-replay checks](https://github.com/earendil-works/pi/blob/5a31a6d5f418ae6591d219bbaf2f45fe37ff4ee2/packages/durable/src/harness/tool.ts#L175),
[stable schedule submission ID](https://github.com/earendil-works/pi-durable-cloudflare/blob/fc596b2422ac9fe303bd17d7b538f78c0d958810/src/schedule.ts#L235).

Heimdall reuses its existing job/outbox transactions for durable intent, external
send and durable receipt; stable control request IDs survive lost replies/reopen.
Partial cursors are now distinct from the completed watermark. Unknown sends are
never promoted to success or re-created as fresh alerts. Existing persisted
`next_at`/`not_before` retry deadlines and retry eligibility stay unchanged; no
model-driven successor scheduling, stream batching or generic tool replay added.
Repeated no-progress partials visibly stall and pause, matching the current
Durable principle of ending repeated crash loops rather than old parked-task
behavior. Job-specific errors do not shut down the hub.

Adoption evidence: `TestLostControlReplyReopenAndCachedStopCannotKillNewRun`,
`TestCheckpointOverflowParaphraseAndCrashBoundary`,
`TestCheckpointOutboxPrecommitFailureRollsBackAndReopenKeepsReceipt`,
`TestPartialBootstrapResumesAcrossReopenAndStalls`, and
`TestDrainCountsOnlyEligibleWindowedPendingAndAllInFlight`; existing outbox
stable row/chunk reconciliation remains the delivery mechanism. These are fake
and local transaction regressions, not live platform delivery claims.
