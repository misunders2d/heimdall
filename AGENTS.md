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
  restrictions belong to each harness's native configuration. Heimdall
  launches each harness in its own native automatic approval/review mode
  (AgentNet's launch table: OMP `--auto-approve`, Claude
  `--permission-mode auto`, Codex automatic review), adds no tool
  exclusions, sandbox changes or other restrictions, and runs no extra
  services.
  Exception: opt-in `owner_slack` exposes Slack MCP read/search only to owners
  in private Telegram chats or live-verified same-workspace Slack 1:1 DMs,
  on Pi/Claude/Codex; never ambient or scheduled turns. Other turns have
  Slack disabled. See [setup runbook](docs/owner-slack-read.md).
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
- **Roles.**
  - *Owner (root)*: only the principals listed in the config's `owners`
    (Telegram user IDs, Slack verified emails). Owner turns run as the owner
    OS user (root in production) with everything enabled, except the
    private-owner Slack read gate described above.
  - *Employee (non-root)*: every other caller the interface identified, with
    no allowlist: any Telegram user who is not an owner, any verified member
    of the owner's Slack workspace who is not an owner. Employee turns always run
    as a separate non-privileged OS user, with all of that user's tools, and
    tell tools they are restricted (`PI_RESTRICTED_EXTERNAL_USER=1`).
    The first time one writes, each owner the interface can reach
    privately is told once: name, verified key and conversation, never the
    message.
  - The employee OS account is checked by uid: never root's or the
    owner's, at startup and on every turn.
  - When Heimdall cannot switch to a turn's OS user (it is not root and the
    user differs from its own), the turn is refused. An employee turn never
    runs as the owner.
  - Harness logins are done manually by the owner, per OS user and machine.
    Heimdall never copies credentials and never reuses one user's home or
    environment for another (the employee environment is rebuilt from that
    user's account).
- **Modes.** `corporate`: the owner and verified Slack callers get memory.
  `personal`: only the owner gets memory. Telegram callers who are not
  owners never get memory, in either mode. Memory means Memento's native integration for that
  harness run; for a no-memory turn Heimdall sets `MEMENTO_PROCESSING=1`,
  which suppresses Memento's own hooks/extensions. Heimdall never enables
  memory the native setup lacks. Memento MCP tools configured natively for
  the employee user and native transcript storage are outside this switch.
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
- **Conversations.** Key = interface + chat + thread. Same key: one turn at a
  time. Different keys run in parallel up to `max_parallel` live harness
  processes.
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
  share one bounded untrusted context budget. Inbound files default to
  200000000 bytes each (`slack.max_file_bytes`), reserving 1 GiB free disk
  space across concurrent downloads; bearer tokens go only to allowlisted
  Slack file hosts. Progress includes hub-filtered public thinking summaries
  (NFKC, privacy filters and display budgets), tools and phases via optional
  native telemetry. Final replies are fresh, escaped, chunked thread messages;
  outbound row IDs reconcile ambiguous sends. Delivery remains at-least-once.
  Status is deleted after confirmed text/reaction delivery, compacted if
  deletion fails, and recovered after restart. Files out use checked outboxes
  and the supported external-upload flow; reactions use Slack names.
- **Harness choice.** Default order `omp → codex → claude → pi`
  (configurable). Fall back to the next harness only when the previous one
  failed before its turn started (before any side effect is possible).
  Explicit choices (`!use <harness>`, `!<harness> <msg>`) never fall back.
  Every reply is tagged with the harness that produced it.
- **Steer and follow-up.** Mid-turn messages steer by default; `!next`
  queues instead. Steers that wait while another is handed over go to the
  harness together, as one steer framed in order with all their images (at
  most 10 natively, every file by path). A steer counts only on the
  harness's own acknowledgement; when acceptance is unknown the sender is
  told and nothing is replayed.
  The reply keeps what the harness answered to every input it took (the
  prompt, then each accepted steer), never only the last answer. A turn
  ends, and its reply goes out, at the harness's answer, not when its
  process exits; the conversation's next turn starts only after that
  process exited, so two processes never share a session. It gets 60 s,
  then SIGTERM to its process group and SIGKILL 5 s later (logged with the
  turn); a stop by the turn's caller or an owner sends the SIGTERM at
  once. A process holds its `max_parallel` slot until it exits. At
  shutdown every live process gets the SIGTERM at once and is waited for
  (60 + 5 s at most, then its group is killed); none outlives Heimdall.
- **Reliability.** Inbound and outbound queues are persisted (SQLite) before
  an interface acknowledges a message upstream. A turn interrupted by a
  restart is reported to its chat and never re-run; messages that never
  started run after the restart. A native session is resumed only when its
  last turn finished cleanly (AgentNet's rule); otherwise a fresh session
  starts and the reply says so. Outbound delivery is at-least-once: a crash
  between a successful platform send and its local acknowledgement can repeat
  that one message. Say so; never claim exactly-once.
- **Commands** use the `!` prefix on every interface (Slack forbids `/` in
  threads): `!help`, `!status`, `!stop`, `!cancel`, `!reset`, `!chatid`,
  `!use <harness>` (sticky per conversation: chat + thread), `!use default`,
  `!<harness> <msg>` (that message only), `!next <msg>`. The hub's control
  words work alone in a message without the `!`: `stop`, `cancel`
  (`abort`, `nevermind`), `reset`, `chatid`. Stop is limited to the turn's
  caller or an owner; cancel clears only the caller's queued work (the owner
  may clear everyone's). `reset` forgets only
  the caller's own sessions in that conversation.
  Unknown `!word` commands are answered with help, never sent to a harness.

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
  UI is relayed only for owners in private chats; the owner supplies the
  callback, never Heimdall. Callback URLs/codes are ephemeral and never
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
- Heimdall keeps its own adapters. T3 Code is for coding only, never a
  route for chat turns (owner, 2026-10-30).
- The turn's ready signal is the side-effect boundary used for fallback. A
  harness that answers, or asks for an approval, before that signal has
  taken the turn too: no fallback.

## Code layout

| Path | What |
|---|---|
| `cmd/heimdall` | CLI: `run`, `local`, `harnesses`, `version` |
| `internal/core` | principals, roles, modes, conversation keys, messages |
| `internal/config` | the one JSON config file |
| `internal/store` | SQLite: inbound/outbound queues, turns, sessions, chat settings |
| `internal/harness` | ported native adapters, launch as an OS user |
| `internal/router` | conversations, `!` commands, steer/follow-up, fallback, delivery |
| `internal/iface/local` | local stdin/stdout interface for end-to-end tests |
| `internal/iface/telegram` | Telegram Bot API long-poll interface (`heimdall run`) |
| `internal/iface/slack` | Socket Mode, live identity, context, progress, delivery and files |
| `docs/` | `HANDOFF.md` (state and next steps), `hub-parity.md` (hub variables) |

## Working rules

- Test only on the development machine. Never test on the production server.
- Secrets come from the environment (Infisical in production) or owner-only
  files; never commit, log or print them. Harness environments never inherit
  `HEIMDALL_*` variables.
- Never commit private deployment details (user IDs, emails, hostnames,
  server paths or specs). Example configs use placeholders. Private context
  documents stay outside the repository.
- Logs carry metadata only (conversation, turn, harness, state), never
  message text or native output.
- Keep it lean: standard library, pure-Go SQLite, pinned coder/websocket
  and owner-approved x/text for NFKC (static binary,
  `CGO_ENABLED=0`). Add a dependency only when a requirement needs it.
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
3. Scheduler (jobs carry and run as their creator).
4. Production cutover.
