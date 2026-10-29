# AGENTS.md — working on Heimdall

Heimdall is a thin, reliable message router between chat interfaces
(Telegram, Slack, AgentNet, future UIs) and agent harnesses (OMP, Codex,
Claude, Pi, AGY). One static Go binary, one config file, optional systemd
unit. It is one of three independent universal-install modules: Heimdall
routes, AgentNet connects agents, Memento remembers. Anyone installs
Heimdall, connects their own AgentNet and Memento, and goes.

Its whole job, per message:

1. receive it from an interface;
2. attach the interface-verified caller identity and reject unknown callers;
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
  injects no tool exclusions, sandbox changes or permission overrides, and
  runs no extra services.
- **Identity comes from the interface, never from text.** Callers never log
  in. Each interface supplies a platform-verified principal (Telegram user
  ID, Slack team + user ID, AgentNet verified key). Display names are never
  authority. Unknown callers are rejected (fail closed). The verified
  identity is carried on every message handed to a harness, including
  scheduled jobs (later phase), which store their creator and run under that
  creator's role and mode evaluated at fire time.
- **Roles.**
  - *Owner*: recognised only by verified platform IDs listed in the config.
    Owner turns run as the owner OS user (root in production) with
    everything enabled; Heimdall never restricts owner turns.
  - *Employees*: listed verified platform IDs. Employee turns always run as a
    separate non-privileged OS user, with all of that user's tools.
  - When Heimdall cannot switch to a turn's OS user (it is not root and the
    user differs from its own), the turn is refused. An employee turn never
    runs as the owner.
  - Harness logins are done manually by the owner, per OS user and machine.
    Heimdall never copies credentials and never reuses one user's home or
    environment for another (the employee environment is rebuilt from that
    user's account).
- **Modes.** `corporate`: employees also get memory. `personal`: only the
  owner gets memory. Memory means Memento's native integration for that
  harness run; for a no-memory turn Heimdall sets `MEMENTO_PROCESSING=1`,
  which suppresses Memento's own hooks/extensions. Heimdall never enables
  memory the native setup lacks. Memento MCP tools configured natively for
  the employee user and native transcript storage are outside this switch.
- **Separate sessions in mixed chats.** Native sessions are kept per
  (conversation, role, harness). An employee message never steers an owner
  turn; a mid-turn message steers only a turn of the same role, otherwise it
  queues as a follow-up.
- **Conversations.** Key = interface + chat + thread. Same key: one turn at a
  time. Different keys run in parallel up to `max_parallel`.
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
  threads): `!help`, `!status`, `!stop`, `!use <harness>` (sticky per chat),
  `!use default`, `!<harness> <msg>` (that message only), `!next <msg>`.
  Unknown `!word` commands are answered with help, never sent to a harness.

## Harness adapters

`internal/harness` ports AgentNet's proven adapters (`internal/client`:
`pirpc.go`, `claudestream.go`, the Codex app-server bridge in
`codexroom.go`, `agystream.go`, `harnesslaunch.go`, `proc_unix.go`). Port
behaviour; do not reinvent. When a harness protocol changes, compare with
AgentNet's current adapter first and record the source revision in the file
header. Left out on purpose: AgentNet's room tool, approval relay UI,
thinking stream and live-session reply registration.

- Native approval or input requests end the turn as *needs human*; nothing
  is approved on anyone's behalf.
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
| `cmd/heimdall` | CLI: `local`, `harnesses`, `version` |
| `internal/core` | principals, roles, modes, conversation keys, messages |
| `internal/config` | the one JSON config file |
| `internal/store` | SQLite: inbound/outbound queues, turns, sessions, chat settings |
| `internal/harness` | ported native adapters, launch as an OS user |
| `internal/router` | conversations, `!` commands, steer/follow-up, fallback, delivery |
| `internal/iface/local` | local stdin/stdout interface for end-to-end tests |

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
  real protocol). Live harness checks are manual: `heimdall local` with a
  scratch config and data dir outside the repo, synthetic prompts, and
  `MEMENTO_PROCESSING=1` in the environment so test turns are not captured as
  memory. Remove the scratch state afterwards.
- Commit and push regularly; leave the tree clean. Credit participating
  coding agents in commit trailers.
- Claims must match evidence: a fake-harness test is not a live harness run.

## Scope and phases

1. **Now (framework):** core router, identity and modes, persisted queues,
   session store, harness selection and `!` commands, steer/follow-up,
   delivery, ported adapters, local CLI interface.
2. Telegram, Slack and AgentNet interfaces.
3. Scheduler (jobs carry and run as their creator).
4. Production cutover.
