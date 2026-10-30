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
session store, `!` commands, steer/follow-up, delivery, the harness adapters,
a local terminal interface and the Telegram interface work (Telegram is
tested against a fake Bot API and checked live for start-up and polling;
real chats are next). Slack, AgentNet interfaces and the scheduler come
next. Linux only.

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
| `mode` | `corporate` (the owner and Slack callers get memory) or `personal` (only the owner does); Telegram callers who are not owners never do |
| `owners` | root callers as `platform:id`: `telegram:<numeric user ID>`, `slack:<verified email>` |
| `harness_order` | default order, `omp, codex, claude, pi` |
| `max_parallel` | conversations running a turn at once |
| `run_as.owner`, `run_as.employee` | OS `user`, optional working `dir` and `path` for each role |
| `telegram.bot_username` | the bot the token must belong to; checked with `getMe` before polling |

Every other caller the interface verified is non-root: any Telegram user who
is not an owner, any member of the bot's own Slack workspace whose email
Slack verifies live (not deleted, not a bot). Other Slack callers, guests
from other workspaces included, are ignored. There is no allowlist beyond
`owners`.

Owner turns run as the owner OS user (root in production) with everything
the native harness setup allows. Employee turns always run as a separate
non-privileged OS user with an environment rebuilt from that account;
Heimdall must run as root to switch to it and refuses employee turns
otherwise. An employee account whose uid is root's or the owner's is
refused at startup and on every turn. Log each harness in for each OS user
yourself; Heimdall never copies credentials.

`heimdall harnesses -config FILE` shows which harnesses each role's OS user
can launch.

## Run

```sh
HEIMDALL_TELEGRAM_TOKEN=... heimdall run -config heimdall.json   # logs on stderr
```

The token comes only from the environment (`HEIMDALL_*` variables never
reach a harness) and never appears in logs or errors. If the token belongs
to another bot than `telegram.bot_username`, Heimdall refuses to poll and
exits. It never touches webhooks: a webhook or a second poller on the same
token shows as a logged conflict. Use a separate bot for testing; two
pollers on one token conflict.

On Telegram, a private chat with the bot is always answered. In a group,
Heimdall answers only messages that mention `@<bot username>` or reply to
one of its messages, and ignores (never stores) everything else. With
Telegram's privacy mode on, a group bot receives replies to it and
`/command@bot`, but plain mentions may not reach it: make it a group admin,
or turn privacy off in BotFather and add it to the group again. `/start`
shows the help. Text or a caption is used; attachments are not handled yet.

## Use

Commands work the same on every interface:

```
!help              list commands
!status            what is running and queued in this conversation
!stop              cancel the current turn
!use <harness>     this conversation uses <harness> until !use default
!use default       back to the default order
!<harness> <msg>   only this message goes to <harness>
!next <msg>        queue as follow-up instead of steering
```

A conversation is a chat, or one thread in it: `!use` in a thread changes
neither the other threads nor the chat itself. Each verified caller has
their own native session per conversation. A message sent
while the sender's own turn runs steers it; everything else queues as a
follow-up. Replies end with `— <harness>`.

## Try it locally

```sh
heimdall local -config my.json -as owner
```

Type one message per line. Prefix `@user` and `#chat/thread` to switch the
sender and conversation for that line (e.g. `@employee #team/7 hello`); any
sender not listed in `owners` is non-root. The local interface trusts
whoever types, so list `local:...` owners only in test configs.

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
