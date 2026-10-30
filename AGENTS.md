# AGENTS.md — working on Heimdall

Heimdall is a thin, reliable message router between chat interfaces
(Telegram, Slack, AgentNet, future UIs) and agent harnesses (OMP, Codex,
Claude, Pi, AGY). One static Go binary, one config file, optional systemd
unit. It is one of three independent universal-install modules: Heimdall
routes, AgentNet connects agents, Memento remembers. Anyone installs
Heimdall, connects their own AgentNet and Memento, and goes.

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

- **Keep it thin.** No invented subsystems: no activity boards, registries,
  job trackers or delegation layers. Memory is Memento's job; agent-to-agent
  work and job tracking are AgentNet's job; business connectors and tool
  restrictions belong to each harness's native configuration. Heimdall
  launches each harness in its own native automatic approval/review mode
  (AgentNet's launch table: OMP `--auto-approve`, Claude
  `--permission-mode auto`, Codex automatic review), adds no tool
  exclusions, sandbox changes or other restrictions, and runs no extra
  services.
- **Identity comes from the interface, never from text.** Callers never log
  in. Each interface supplies a platform-verified principal: the numeric
  Telegram user ID; for Slack the verified email, which is the Slack
  identity (a Slack user ID alone never counts); the AgentNet verified key.
  A Slack caller is verified only as the Pi hub checks it
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
    OS user (root in production) with everything enabled; Heimdall never
    restricts owner turns.
  - *Employee (non-root)*: every other caller the interface identified, with
    no allowlist: any Telegram user who is not an owner, any verified member
    of the owner's Slack workspace who is not an owner. Employee turns always run
    as a separate non-privileged OS user, with all of that user's tools, and
    tell tools they are restricted (`PI_RESTRICTED_EXTERNAL_USER=1`).
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
- **Sessions per verified caller.** Native sessions are kept per
  (conversation, verified caller, harness), as the Pi hub does. A harness
  process serves exactly one caller: a mid-turn message steers only the
  sender's own running turn, a turn batches only that caller's messages, and
  anyone else's message queues. Only the turn's caller or an owner can
  `!stop` it.
- **Conversations.** Key = interface + chat + thread. Same key: one turn at a
  time. Different keys run in parallel up to `max_parallel`.
- **Telegram** (as the hub's connector). A private chat is always handled.
  In a group, a message (`!` commands and control words included) is handled
  only if it mentions `@<bot username>`, replies to one of the bot's
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
  end one with `[react: <ref> <emoji>]` lines to react. A harness sends
  files by leaving them in its turn's `out/` directory; Heimdall checks and
  copies them itself (never a path the model names) and sends them after
  the answer. Before polling,
  `getMe` must report the configured `telegram.bot_username`; Heimdall never
  calls `deleteWebhook` or `setWebhook`. The token comes only from
  `HEIMDALL_TELEGRAM_TOKEN` and never appears in errors or logs.
- **Harness choice.** Default order `omp → codex → claude → pi`
  (configurable). Fall back to the next harness only when the previous one
  failed before its turn started (before any side effect is possible).
  Explicit choices (`!use <harness>`, `!<harness> <msg>`) never fall back.
  Every reply is tagged with the harness that produced it.
- **Steer and follow-up.** Mid-turn messages steer by default; `!next`
  queues instead. A steer counts only on the harness's own acknowledgement;
  when acceptance is unknown the sender is told and nothing is replayed.
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
  (`abort`, `nevermind`), `reset`, `chatid`; `stop` and `cancel` follow
  the `!stop` rule (the turn's caller or an owner). `reset` forgets only
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
  request) ends the turn as *needs human*; Heimdall answers no such request
  itself.
- Sessions: Pi and OMP use a per-session `--session-dir` with `--continue`
  (verified: an empty directory starts fresh, the same directory resumes);
  Claude `--session-id` / `--resume`; Codex app-server `thread/start` /
  `thread/resume`; AGY `--conversation`.
- Steering: Pi/OMP RPC `steer`, Claude stream-json user input confirmed by
  its replay, Codex `turn/steer`. AGY has none (follow-up only).
- The turn's ready signal is the side-effect boundary used for fallback.

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
| `internal/iface/slack` | Slack caller verification (the interface comes in phase 2) |
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
- Keep it lean: standard library plus pure-Go SQLite (static binary,
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
2. Slack and AgentNet interfaces.
3. Scheduler (jobs carry and run as their creator).
4. Production cutover.
