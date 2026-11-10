# AGENTS.md — working on Heimdall

Heimdall is a thin, reliable message router between chat interfaces
(Telegram, Slack, future UIs) and agent harnesses (OMP, Codex,
Claude, Pi, AGY). One static Go binary, one config file, optional systemd
unit. It is one of three independent universal-install modules: Heimdall
routes, AgentNet connects agents, Memento remembers. AgentNet stays
separate; memory belongs to each harness's native hooks.

**Goal:** replace the Pi hub integration (today's always-on assistant) as the
single interface for every agent installed on the server, so any compatible
harness provides the current assistant's functionality. Tools on those agents
are gated per user, so the core requirement is conveying the
interface-verified caller identity to each harness process reliably, in the
form those tools read, never only as prompt text.

Its whole job, per message:

1. receive it from an interface;
2. attach the interface-verified caller identity and ignore callers it
   cannot identify;
3. pick the harness (default order or a `!` override);
4. call that harness with the correct native session for this conversation;
5. pass messages that arrive mid-turn to the running turn as steer or
   follow-up, where the harness supports it;
6. deliver the reply to the same interface, chat and thread, reliably.

- **Product overview, configuration and local testing**: [README.md](README.md).
- **Design of record** is kept by the owner outside this repository; this
  file records the rules the code must keep.

## Product rules that code must keep

Owner, phase 2: Heimdall has no memory intake or capture; memory belongs
only to harness native hooks. No-memory turns retain `MEMENTO_PROCESSING=1`.
Slack tests use fakes only; live app is shared with the hub and must never
be connected without the orchestrator arranging a live window. Never touch
the `heimdall-live` unit.

- **Keep it thin.** No invented subsystems: no activity boards, registries,
  job trackers or delegation layers. Memory is Memento's job; agent-to-agent
  work and job tracking are AgentNet's job; business connectors and tool
  restrictions are selected by caller role. Owners retain native automatic
  modes. Employees use confined readers and explicit tool/MCP restrictions;
  unsupported or unvalidated launches are refused. No extra services.
  Exception: opt-in `owner_slack` exposes Slack MCP read/search only to owners
  in private Telegram chats or live-verified same-workspace Slack 1:1 DMs,
  on Pi/Claude/Codex; never ambient or scheduled turns. Other turns have
  Slack disabled. See [setup runbook](docs/owner-slack-read.md).
  Exception, durable tasks (owner, approved 2026-11-08/09): owner tasks are
  durable commitments on embedded DBOS Transact Go with its SQLite system
  database (`internal/durable`, `<data_dir>/durable.db`,
  [docs/durable.md](docs/durable.md)). The hub orchestrates all owner tasks:
  only Hub (root) is an orchestrator, and the orchestration stays on the
  Heimdall swarm. Owner, 2026-11-10: follow the "Pi Durable" path, a
  reliable, disruption- and restart-surviving system that continues
  interrupted tasks. Off by default in code. Production runs it with
  `supervisor_review` and `assistant_work` on and `t3_dispatch` off.
- **Identity comes from the interface, never from text.** Callers never log
  in. Each interface supplies a platform-verified principal: the numeric
  Telegram user ID; for Slack the verified email, which is the Slack
  identity (a Slack user ID alone never counts).
  A Slack caller needs the Pi hub live check plus a workspace check
  (`internal/iface/slack`): users.info returned a live email, the user is
  not deleted and not a bot, and belongs to the bot's own workspace
  (auth.test team ID). Slack Connect guests from other workspaces, and
  emails remembered from earlier lookups, never count. Display names,
  usernames and message text are never identity. A caller the interface
  cannot identify is ignored and logged, fail closed. The verified identity is carried on
  every message handed to a harness, and to the harness process in the
  hub's identity variables (`docs/hub-parity.md`), including scheduled jobs
  (later phase), which store their creator and run under that creator's
  role and mode evaluated at fire time.
- **External verified origins** (owner, 2026-11-09): Heimdall is not coupled
  to AgentNet but receives identity from it. Scheduler control may also bind a
  run that a configured trusted local launcher started, only through
  [docs/external-origin.md](docs/external-origin.md): Heimdall connects to the
  launcher's own socket, checks its kernel credentials and that the peer
  descends from its direct child, and asks live (again before commit). Only
  `author: human` runs of people explicitly mapped to an existing principal
  that is a current owner count; environment variables are never identity;
  no AgentNet code or import. Create/edit apply at once; removal and
  `rerun_on_timeout` keep human confirmation. A verified external human
  owner may add, list and show durable tasks (a Slack owner only with the
  live-verified OAuth actor); employee, agent, scheduled and unknown
  origins stay refused.
- **Roles.**
  - *Owner (root)*: `ADMIN_USER_IDS` authority only. File admins are admitted employees.
    Config owners are only an explicit non-hub fallback.
  - *Employee (non-root)*: admitted by Pi's existing user OR chat whitelist
    after blacklist denial. Unknown callers are ignored, metadata logged only.
    Employees run under a separate UID with confined skill readers, no owner
    MCP, no Memento and no server administration. Native harness validation
    is required before listing a harness in `employee_harnesses`.
  - The employee OS account is checked by uid: never root's or the
    owner's, at startup and on every turn.
  - When Heimdall cannot switch to a turn's OS user (it is not root and the
    user differs from its own), the turn is refused. An employee turn never
    runs as the owner.
  - Employee turns use Sergey's existing harness installs/logins. Employees
    never log in; no second login. Isolation is implementation work. Never
    copy credentials or expose owner homes. Keep employee turns disabled until
    native restrictions and caller-owned business MCP OAuth are validated.
    MEL-724 signed caller-token delivery remains separate connector work.
- **Modes.** Both corporate and personal modes keep Memento owner-only.
  Employee turns force `MEMENTO_PROCESSING=1`, remove memory MCP and disable
  native memory hooks. ACL-safe company knowledge belongs to Mellanni.
- **Prompt-injection scoring is log only** (owner, 2026-10-30;
  `internal/router/guard.go`). The hub's scoring of every admitted message
  (ambient ones included), on every interface, never blocks or alters a
  message. A message at the hub's block score (4) is logged as one JSON
  line: score, pattern names, interface, chat, thread, caller key and
  message ID, never the text, to `<data_dir>/prompt-guard.log`, rotated to
  one `.1` file at `prompt_guard.log_max_bytes` (default 1 MiB).
- **Sessions per verified caller.** Native sessions are kept per
  (conversation, verified caller, harness), as the Pi hub does. A harness
  process serves exactly one caller: a mid-turn message steers only the
  sender's own running turn, a turn batches only that caller's messages, and
  anyone else's message queues. Only the turn's caller or an owner can
  `!stop` it.
  Owner interactive turns may receive a pointer to the same caller's earlier
  native transcript when starting fresh or resuming behind another harness.
  No transcript is copied or summarized; reset removes these bindings.
  Employees, scheduled jobs and MCP controls receive no pointer.
- **Conversations.** Key = interface + chat + thread. Same key: one turn at a
  time. Different keys run in parallel up to `max_parallel` running turns. Idle warm processes use no turn slot.
- **Telegram** (as the hub's connector). A private chat is always handled.
  In a group, exact controls allowed by `commands.unmentioned` work without
  addressing. Other messages are handled only if they mention `@<bot username>`, replies to one of the bot's
  messages, or is a later part of an album whose first part did. Every other
  group message from a person is never run or answered; it is kept only as
  bounded, untrusted context for that conversation's later turns (the newest
  1000 kept, 40 shown, 4000 chars each and 16000 in all, scrubbed; files
  only by label). `/start` is `!help`. The text, else the caption. Files
  (the message's and the replied-to message's, at most 20 MB) are downloaded
  into Heimdall's 0700 upload directory and, at turn start, copied into a
  per-turn directory owned by the turn's OS user; pictures also go natively
  to harnesses that take images. Album parts are held 1.5 s and run as one
  message. Answers go as Telegram Markdown with a plain retry; a harness may
  end one (each answer of a steered turn counts) with
  `[react: <ref> <emoji>]` lines to react. A harness sends
  files by leaving them in its turn's `out/` directory; Heimdall checks and
  copies them itself (never a path the model names) and sends them after
  the answer. Before polling,
  `getMe` must report the configured `telegram.bot_username`; Heimdall never
  calls `deleteWebhook` or `setWebhook`. The token comes only from
  `HEIMDALL_TELEGRAM_TOKEN` and never appears in errors or logs.
- **Shared addressing** (owner, phase 2): one config list,
  `commands.unmentioned`, controls Slack channels/threads and Telegram groups.
  Default `["stop","cancel"]`: exact standalone `stop`, `cancel`, `/stop`,
  `/stop@<bot>`, `/cancel`, `abort`, `nevermind` need no mention. Everything
  else needs addressing. `["all"]` permits every recognized command without
  a mention; `[]` requires addressing for every command. Private chats/DMs
  never require a mention. Stop affects only the requester's turn (or the
  owner's target); cancel preserves current behavior: callers may clear their
  own queued work while someone else runs, never that other turn or queue.
- **Slack**: Socket Mode only; `auth.test` must match `slack.team_id` before
  connecting. Tokens only from `HEIMDALL_SLACK_BOT_TOKEN` and
  `HEIMDALL_SLACK_APP_TOKEN`. One data-directory lock; no live connection
  during development. Every reply uses the root thread, DMs included. A
  shared message needs a bot mention unless the shared command policy
  admits it; replying in a bot thread alone does not count. Persist the raw
  event before ack; verify callers and fetch history/files after ack. Only
  verified same-workspace humans enter the router. Message subtypes are
  allowlisted. Live history, stored ambient and recent assistant deliveries
  share one bounded untrusted context budget. Thread history uses cursor paging
  (at most 25 pages of 200), retaining the newest 40 messages before the
  inbound timestamp; incomplete paging reports live history unavailable.
  Inbound files default to
  200000000 bytes each (`slack.max_file_bytes`), reserving 1 GiB free disk
  space across concurrent downloads; bearer tokens go only to allowlisted
  Slack file hosts. Progress includes hub-filtered public thinking summaries
  (NFKC, privacy filters and display budgets), tools and phases via optional
  native telemetry. Final replies are fresh, escaped, chunked thread messages;
  outbound row IDs reconcile ambiguous sends. Delivery remains at-least-once.
  Status is deleted after confirmed text/reaction delivery, compacted if
  deletion fails, and recovered after restart. Files out use checked outboxes
  and the supported external-upload flow; reactions use Slack names.
- **Harness choice.** Default order `codex → claude → pi`
  (configurable). Fall back to the next harness only when the previous one
  failed before its turn started (before any side effect is possible).
  Explicit choices (`!use <harness>`, `!<harness> <msg>`) never fall back.
  Harness replies carry no signature (owner, 2026-11-03); `!which` names the
  conversation's harness and its configured thinking level (read from the
  harness's own config; agents change it themselves), the running turn's,
  and the last reply's when it differs. Heimdall's own notices end `— heimdall`.
- **Steer and follow-up.** Mid-turn messages steer by default; `!next`
  queues instead. Steers that wait while another is handed over go to the
  harness together, as one steer framed in order with all their images (at
  most 10 natively, every file by path). A steer counts only on the
  harness's own acknowledgement; when acceptance is unknown the sender is
  told and nothing is replayed.
  The reply keeps what the harness answered to every input it took (the
  prompt, then each accepted steer), never only the last answer. A turn
  ends, and its reply goes out, at the native result event. Pi/OMP, Claude
  and Codex retain a process per conversation + verified caller + harness;
  AGY remains per-turn because its adapter has no multi-turn protocol.
  Launch fingerprints cover caller/role/private/Slack scope, OS credentials,
  environment, executable/directory/home and native settings. Changed launch,
  reset or harness selection discards the worker. Config reload is restart.
  `harness.idle_timeout_seconds` defaults to 900, `max_lifetime_seconds`
  to 14400, `max_warm` to 8 (idle LRU), `run_timeout_seconds` to 1200.
  Idle workers hold no `max_parallel` slot; native results release slots.
  Stop sends native interrupt (Pi/OMP abort, Claude control request, Codex
  turn/interrupt), then discards the unclean process/session with bounded
  SIGTERM/SIGKILL fallback. Exactly one stop command reply; no duplicate
  cancelled-turn final. All workers are terminated and waited for at shutdown.
  Per-turn hooks remain native; session-end hooks move to worker exit.
- **Reliability.** Inbound and outbound queues are persisted (SQLite) before
  an interface acknowledges a message upstream. A turn interrupted by a
  restart is reported to its chat and never re-run; messages that never
  started run after the restart. A native session is resumed only when its
  last turn finished cleanly (AgentNet's rule); otherwise a fresh session
  starts and the reply says so. Outbound delivery is at-least-once: a crash
  between a successful platform send and its local acknowledgement can repeat
  that one message. Say so; never claim exactly-once.
- **Commands** use the `!` prefix on every interface (Slack forbids `/` in
  threads): `!help`, `!status`, `!which`, `!stop`, `!cancel`, `!reset`, `!chatid`,
  `!use <harness>` (sticky per conversation: chat + thread), `!use default`,
  `!jobs`, `!job …` (scheduler),
  `!<harness> <msg>` (that message only), `!next <msg>`. The hub's control
  words work alone in a message without the `!`: `stop`, `cancel`
  (`abort`, `nevermind`), `reset`, `chatid`. Stop is limited to the turn's
  caller or an owner; cancel clears only the caller's queued work (the owner
  may clear everyone's). `reset` forgets only
  the caller's own sessions in that conversation.
  Unknown `!word` commands are answered with help, never sent to a harness.

- **Scheduler** (phase 3a; design of record [docs/scheduler.md](docs/scheduler.md)).
  - Jobs run as their creator; role and admission are evaluated at every
    fire, where the run's turn starts. Slack creators are re-verified live.
    Never another identity.
  - One run per job and slot: `UNIQUE(job_id, slot)` in the same
    transaction that creates the run. Delivery is at-least-once.
  - Scheduled turns: their own conversation `sched:<job_id>`, delivering to
    the captured target; `Private=false`; no `owner_slack`;
    `MEMENTO_PROCESSING=1` (never automatic capture); owner-approved read-only
    workflows may deliberately recall native Memento memory and use existing
    authorized connector tools. The selected agent owns tools and delegation;
    the scheduler adds no special harness profile.
    Shared native policy hooks remain enabled; `owner_slack` stays live-only.
    A fresh session by
    default; no progress messages; never a warm worker.
  - Retry only before the ready signal, except explicit owner-only
    `rerun_on_timeout` jobs: declared idempotent, fresh session, explicit
    timeout, platform target; one repeat-safe rerun within twice the timeout
    from due time, after old process exit. Stop/pause/delete fence retries;
    timeout retries never cross restart. This repeats work, not checkpoints.
    Other interrupted runs are never re-run, except `idempotent` jobs (once).
  - Models propose jobs only when asked, through the answer block. Direct owner
    person turns apply new jobs and edits with the reply in one transaction,
    without a card. Non-owners, removals and any `rerun_on_timeout` job still
    require a human confirmation in
    the same conversation within 24 h (`!job confirm`, or a bare yes/no that
    Heimdall matches itself, ignoring surrounding whitespace and trailing
    punctuation, whole replies only). Exactly one waiting draft, the same
    caller and conversation are required for word replies. Cards use native
    Slack/Telegram Markdown, a short purpose, exact human-readable schedule,
    destination, truthful state/effects and Yes/No actions. Raw IDs, cron,
    harness fields and prompts stay in opt-in Details/operator views; private
    prompt visibility remains caller-scoped. Paused cards never imply an active
    next run. Shared-chat privacy stays unchanged. Blocks from
    scheduled turns are ignored; MCP controls never auto-apply.
  - Notices are fixed templates ending `— heimdall`; no model text in
    shared channels.
  - AgentNet: owner jobs may name an address; the agent delivers; Heimdall
    never talks to AgentNet.
  - Watches (3b): deterministic checkers from config only, running as the
    creator, with an expiry. One Slack consumer and one Telegram poller.
  - `PI_SCHEDULER_TOOLS=0` on Pi and OMP turns while the scheduler is
    enabled. Production restarts run `heimdall drain` first; its hold
    applies where a run's turn starts.
  - A job command's change is stored with its answer, in one transaction.

## Harness adapters

`internal/harness` ports AgentNet's proven adapters (`internal/client`:
`pirpc.go`, `claudestream.go`, the Codex app-server bridge in
`codexroom.go`, `agystream.go`, `harnesslaunch.go`, `proc_unix.go`). Port
behaviour; do not reinvent. When a harness protocol changes, compare with
AgentNet's current adapter first and record the source revision in the file
header. Left out on purpose: AgentNet's room tool, approval relay UI,
thinking stream and live-session reply registration.

- Anything the native automatic mode still escalates (an approval or input
  request) ends the turn as *needs human*. Exception: Pi native MCP sign-in
  UI is relayed for owners and enabled Pi employees in private chats; the
  same verified caller supplies the callback, never Heimdall. Callback URLs/codes are ephemeral and never
  persisted, logged, echoed, or included in model context. Slack callback
  controls bypass raw-event staging; ordinary events retain persist-before-ack.
- Sessions: Pi and OMP use a per-session `--session-dir` with `--continue`
  (verified: an empty directory starts fresh, the same directory resumes);
  Claude `--session-id` / `--resume`; Codex app-server `thread/start` /
  `thread/resume`; AGY `--conversation`.
- Steering: Pi/OMP RPC `steer`, Claude stream-json user input confirmed by
  its replay, Codex `turn/steer`. AGY has none (follow-up only). OMP answers
  the prompt and each steer with separate final texts; all are kept. A
  Claude steer written while Claude produced its result runs as Claude's
  next turn in the stream: no more input is sent and that turn is waited
  for. A stop meanwhile stops the turn (never an answer, session not
  resumed); if that late turn fails, exits non-zero or without its
  result, or asks for approval, the answers already given stand with a
  note and the session is not resumed. Codex folds a steer taken while it works into one final
  answer; a steer it accepts after a final answer, before the turn
  completed, gets a final answer of its own (both checked live, 0.166.0),
  and both are kept. A steer Codex acknowledges only after the turn
  completed counts as unknown: the sender is told, nothing is replayed.
- Unused or undecodable native protocol lines are skipped with metadata-only
  diagnostics (length, allowlisted event type, error kind). Claude optional
  system message fields do not invalidate answers. Native results remain
  required; replay, subagent and session-switch checks remain enforced.
- Heimdall keeps its own adapters. T3 Code is for coding only, never a
  route for chat turns (owner, 2026-10-30).
- The turn's ready signal is the side-effect boundary used for fallback. A
  harness that answers, or asks for an approval, before that signal has
  taken the turn too: no fallback.

## Code layout

| Path | What |
|---|---|
| `cmd/heimdall` | CLI: `run`, `local`, `harnesses`, `check-config`, `sched`, `drain`, `version` |
| `internal/core` | principals, roles, modes, conversation keys, messages |
| `internal/config` | the one JSON config file |
| `internal/store` | SQLite: inbound/outbound queues, turns, sessions, chat settings, jobs and runs |
| `internal/schedule` | cron, interval and one-off slots in a zone, the DST rule |
| `internal/scheduler` | due loop, job rules, commands, drafts, outcomes, notices, `sched`/`drain` |
| `internal/durable` | durable owner tasks on DBOS + SQLite (`durable.db`): admission, review, assistant work, dispatch |
| `internal/harness` | ported native adapters, launch as an OS user |
| `internal/router` | conversations, `!` commands, steer/follow-up, fallback, delivery |
| `internal/iface/local` | local stdin/stdout interface for end-to-end tests |
| `internal/iface/telegram` | Telegram Bot API long-poll interface (`heimdall run`) |
| `internal/iface/slack` | Socket Mode, live identity, context, progress, delivery and files |
| `docs/` | `HANDOFF.md` (state and next steps), `hub-parity.md` (hub variables) |

## Working rules

- Development/testing normally runs on a laptop. Explicit owner authorization
  permits isolated work-clone coding/fake tests on the hub, never live runtime.
- Secrets come from the environment (Infisical in production) or owner-only
  files; never commit, log or print them. Harness environments never inherit
  Heimdall's own `HEIMDALL_*` variables (tokens, Google client); Heimdall
  sets only non-secret ones: the turn directory, caller capabilities and,
  for employee Pi, the access-only Google token file path and MCP scope root
  (docs/scheduler.md §17).
- Never commit private deployment details (user IDs, emails, hostnames,
  server paths or specs). Example configs use placeholders. Private context
  documents stay outside the repository.
- Logs carry metadata only (conversation, turn, harness, state), never
  message text or native output.
- Keep it lean: standard library, pure-Go SQLite, pinned coder/websocket
  and owner-approved x/text for NFKC (static binary,
  `CGO_ENABLED=0`). Add a dependency only when a requirement needs it.
  Direct dependencies today (`go.mod`): `modernc.org/sqlite`,
  `coder/websocket`, `golang.org/x/text`, `golang.org/x/sys` (Linux peer
  credentials), `adhocore/gronx` (cron) and DBOS Transact Go v1.8.0 (durable
  tasks, owner-approved 2026-11-08/09; it brings its own indirect modules).
- CI (`.github/workflows/ci.yml`) runs vet, race tests and the static build
  on every push and pull request; the repository is public, so CI uses no
  secrets. Owner, 2026-11-10: only the main branch may run in production; before
  a release, check it against the real engines through `heimdall local`
  (release steps: [INSTALL.md](deploy/contabo/INSTALL.md)).
- Tests: `go vet ./...` and `go test -race ./...` (fake harnesses speak each
  real protocol). Live harness checks are manual: `heimdall local` (or
  `heimdall run` with a separate test bot's token, never the production
  bot's) with a scratch config and data dir outside the repo, synthetic
  prompts, and `MEMENTO_PROCESSING=1` in the environment so test turns are
  not captured as memory. Remove the scratch state afterwards.
- Commit and push regularly; leave the tree clean. Credit participating
  coding agents in commit trailers.
- Claims must match evidence: a fake-harness test is not a live harness run.

## Scope and phases

1. **Now (framework):** core router, identity and modes, persisted queues,
   session store, harness selection and `!` commands, steer/follow-up,
   delivery, ported adapters, local CLI interface, and the Telegram
   interface (end to end on the development machine).
2. Slack interface. AgentNet stays separate and is never a Heimdall interface.
3. Scheduler (jobs carry and run as their creator): 3a built (jobs,
   drafts, Pi tool switch); 3b watches next.
4. Production cutover (done: production runs Heimdall).
