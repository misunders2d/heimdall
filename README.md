# Heimdall

A thin, reliable message router between chat interfaces and agent
harnesses. Heimdall receives a message, attaches the caller identity its
interface verified, picks a harness (OMP, Codex, Claude, Pi or AGY), runs
the turn in that conversation's native session, steers the running turn
with mid-turn messages, and delivers the reply back to the same chat.

```
Telegram ─┐                      ┌─ OMP
Slack    ─┼─► Heimdall ──────────┼─ Codex
AgentNet ─┤  (identity, harness, ├─ Claude
local CLI ┘   session, delivery) └─ Pi / AGY
```

Heimdall does not do memory (use [Memento](https://github.com/misunders2d/memento)),
agent-to-agent work (use [AgentNet](https://github.com/misunders2d/agentnet)),
business connectors or tool restrictions (each harness's native configuration).

**Status:** framework. The core router, identity and modes, persisted queues,
session store, `!` commands, steer/follow-up, delivery, the harness adapters
and a local terminal interface work. Telegram, Slack, AgentNet interfaces and
the scheduler come next. Linux only.

## Build

```sh
CGO_ENABLED=0 go build -o heimdall ./cmd/heimdall   # one static binary
```

## Configure

One JSON file (see [examples/heimdall.example.json](examples/heimdall.example.json)).
Secrets never go in it; interfaces read their tokens from the environment.

| Key | Meaning |
|---|---|
| `data_dir` | SQLite queues and state |
| `mode` | `corporate` (employees get memory) or `personal` (only the owner does) |
| `owners`, `employees` | verified principals as `platform:id` (e.g. `telegram:123`, `slack:T1/U2`); everyone else is rejected |
| `harness_order` | default order, `omp, codex, claude, pi` |
| `max_parallel` | conversations running a turn at once |
| `run_as.owner`, `run_as.employee` | OS `user`, optional working `dir` and `path` for each role |

Owner turns run as the owner OS user (root in production) with everything
the native harness setup allows. Employee turns always run as a separate
non-privileged OS user; Heimdall must run as root to switch to it and
refuses employee turns otherwise. Log each harness in for each OS user
yourself; Heimdall never copies credentials.

`heimdall harnesses -config FILE` shows which harnesses each role's OS user
can launch.

## Use

Commands work the same on every interface:

```
!help              list commands
!status            what is running and queued in this chat
!stop              cancel the current turn
!use <harness>     this chat uses <harness> until !use default
!use default       back to the default order
!<harness> <msg>   only this message goes to <harness>
!next <msg>        queue as follow-up instead of steering
```

Messages sent while a turn runs steer it (same role only); everything else
queues as a follow-up. Replies end with `— <harness>`.

## Try it locally

```sh
heimdall local -config my.json -as owner
```

Type one message per line. Prefix `@user` and `#chat/thread` to switch the
sender and conversation for that line (e.g. `@employee #team/7 hello`). The
local interface trusts whoever types, so list `local:...` principals only in
test configs.

## Reliability

- Conversation key = interface + chat + thread; one turn at a time per key.
- Inbound and outbound queues are persisted in SQLite before acknowledging.
- A turn interrupted by a restart is reported to its chat, never re-run.
- Fallback to the next harness only if the previous one failed before its
  turn started.
- Native sessions are resumed only after a cleanly finished turn.
- Delivery is at-least-once: a crash right after a platform accepted a reply
  but before Heimdall recorded it can repeat that reply.

## License

Apache-2.0. The harness adapters are ported from AgentNet; see [NOTICE](NOTICE).
