# Heimdall

A thin, reliable message router between chat interfaces and agent
harnesses. Heimdall receives a message, attaches the caller identity its
interface verified, picks a harness (OMP, Codex, Claude, Pi or AGY), runs
the turn in that conversation's native session, steers the running turn
with mid-turn messages, and delivers the reply back to the same chat.

```
Telegram ─┐                      ┌─ OMP
Slack    ─┼─► Heimdall ──────────┼─ Codex
Future UI─┤  (identity, harness, ├─ Claude
local CLI ┘   session, delivery) └─ Pi / AGY
```

Heimdall does not do memory (use [Memento](https://github.com/misunders2d/memento)),
agent-to-agent work (use [AgentNet](https://github.com/misunders2d/agentnet)),
business connectors or tool restrictions (each harness's native configuration).

**Status:** Telegram and Slack interfaces, durable queues, caller identity,
native sessions, steering and file delivery are implemented. Slack is tested
against fake Web API/Socket Mode servers and fake native harnesses; the live
Slack window is arranged separately by the orchestrator. Scheduler is phase 3.
Linux only.

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
| `commands.unmentioned` | shared addressing policy; default `["stop","cancel"]`, `["all"]` allows every command, `[]` requires addressing for every command |
| `slack.team_id` | workspace the bot token must belong to; checked with `auth.test` before Socket Mode |
| `slack.event_dedupe_hours` | completed-event dedupe retention, default `72` hours, range `1`–`720`; pending events never pruned |
| `slack.max_file_bytes` | inbound cap per file, default `200000000` (200 MB); downloads also reserve 1 GiB free disk space across concurrent downloads |
| `slack.status_name`, `slack.status_updates` | optional progress display name; set `status_updates: false` to disable progress messages |
| `telegram.bot_username` | the bot the token must belong to; checked with `getMe` before polling |
| `prompt_guard.log_max_bytes` | optional: size of the prompt-injection log before it is rotated to one `.1` file (default 1 MiB, at least 4096) |

Every admitted message is scored with the hub's prompt-injection patterns.
A message the hub would have blocked (score 4 or more) runs unchanged; only
its metadata (score, pattern names, interface, chat, thread, caller and
message ID, never the text) is logged to `<data_dir>/prompt-guard.log`.

Every other caller the interface verified is non-root: any Telegram user who
is not an owner, any member of the bot's own Slack workspace whose email
Slack verifies live (not deleted, not a bot). Other Slack callers, guests
from other workspaces included, are ignored. There is no allowlist beyond
`owners`. The first time a non-root caller writes, each owner is told so
once in their private Telegram chat with the bot: the caller's name,
verified ID and conversation, never the message.

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
commands allowed by `commands.unmentioned` work without addressing. Other
messages must mention `@<bot username>`, reply to one
of its messages, or be later parts of an album whose first part did. It
never answers other group messages; it keeps the recent ones (the newest
1000, scrubbed, files by label only) and shows the newest 40, marked
untrusted, to the group's next turns. With Telegram's privacy mode on, a
group bot receives replies to it and `/command@bot`, but plain mentions may
not reach it: make it a group admin, or turn privacy off in BotFather and
add it to the group again. `/start` shows the help; `/help`, `/status`,
`/stop`, `/cancel`, `/reset` and `/chatid` work too.

The text, else the caption, is used, with the replied-to or forwarded
message as context. Files (the message's and the replied-to message's, up
to 20 MB) are downloaded and copied, when the turn starts, into a directory
of that turn's own owned by its OS user; pictures also reach the harnesses
that take images (Pi, OMP, Claude, Codex) natively. The parts of an album
run as one message. Answers are sent as Telegram Markdown (plain text if
Telegram cannot parse it), cut into parts of 3500 characters. A harness may
react to a message with an emoji, and can send files back by saving them in
its turn's outbox (`out/`, named in its prompt): Heimdall sends up to 5
files of at most 50 MB after the answer.

## Shared-chat commands

`commands.unmentioned` applies to Slack channels/threads and Telegram groups.
The default `["stop","cancel"]` admits only the exact standalone control words
`stop`, `cancel`, and their `/stop`, `/stop@<bot>`, `/cancel`, `abort`,
`nevermind` aliases without addressing. Reset, chatid, start, help, status,
harness choice and follow-ups still require a mention (or Telegram reply to
the bot). `["all"]` allows every recognized command without a mention;
`[]` requires addressing for every command. Private chats and DMs always work.
A Telegram `/command@<bot>` is itself addressed to that bot.

Stop remains limited to the running turn's requester or an owner. Cancel
clears the caller's own queued work, including while another caller runs;
only the owner can clear everybody's queue. Reset stops only the caller's
own turn, forgets their bindings, and preserves queued messages.

## Slack live window

```json
"slack": {"team_id": "T_PLACEHOLDER", "max_file_bytes": 200000000},
"commands": {"unmentioned": ["stop", "cancel"]}
```

Use an internal custom Slack app with Socket Mode enabled, an app-level
token with `connections:write`, and bot event subscriptions `message.channels`,
`message.groups`, `message.im`, `message.mpim`. Enable App Home's Messages tab
and invite the bot into the intended channels. No slash-command registration
or interactivity is required. Bot scopes:

`chat:write`, `channels:history`, `groups:history`, `im:history`, `mpim:history`,
`users:read`, `users:read.email`, `files:read`, `files:write`, `reactions:write`,
`im:write`.

Set `HEIMDALL_SLACK_BOT_TOKEN` and `HEIMDALL_SLACK_APP_TOKEN` in the launch
environment, then use `heimdall run -config FILE`. Either interface block
may be omitted. Slack refuses a workspace mismatch and locks its data directory.
Owners use `slack:<verified email>` in `owners`; other live same-workspace
members run as the employee OS user. Slack Connect externals are silent.

Every message, including DMs, replies in its root thread. Shared messages
need a bot mention (`<@bot-user-id>` on the wire), except permitted commands;
replying in a bot thread alone does not address the bot. Non-addressed messages
are bounded ambient context. Live history, stored ambient context and recent
assistant deliveries share one 16000-character budget. Embedded/forwarded
content is untrusted and cannot supply commands or caller identity.

Progress shows phases, tool labels and filtered public thinking summaries
where the native protocol supplies them. Final answers are fresh thread messages,
escaped against pings and chunked with code fences preserved. Stable outbound
row metadata reconciles uncertain posts and retries; delivery remains
at-least-once. Files enter through allowlisted Slack file hosts and leave
through the checked per-turn outbox using Slack's external-upload flow.

Development uses fakes only. The production app is shared with the hub:
only the orchestrator arranges a live window with a single consumer. Never
connect an additional Socket Mode client during normal hub operation and
never touch `heimdall-live`. Live checks (including token scopes and metadata
visibility) remain unverified until that window. See [parity evidence](docs/slack-parity.md).

## Use

Commands work the same on every interface:

```
!help              list commands
!status            what is running and queued in this conversation
!stop              stop the current turn and clear your queued messages
!cancel            clear your queued messages; the turn keeps running
!reset             your next message here starts fresh native sessions
!chatid            show the conversation's and your IDs as tools see them
!use <harness>     this conversation uses <harness> until !use default
!use default       back to the default order
!<harness> <msg>   only this message goes to <harness>
!next <msg>        queue as follow-up instead of steering
```

`stop`, `cancel` (or `abort`, `nevermind`), `reset` and `chatid` alone in a
message work without the `!`. Only the turn's sender or the owner can stop
a turn; the owner's `stop` and `cancel` clear everyone's queued messages.
A turn's reply goes out at the harness's answer; its process may take a
while to exit, and the conversation's next turn waits for that (at most
60 s, then SIGTERM, then SIGKILL 5 s later). `stop` then ends that wait at
once. `max_parallel` counts harness processes until they exit. At shutdown
every harness process still running gets SIGTERM, and Heimdall waits for
it (at most 65 s; SIGKILL 5 s after the SIGTERM if needed) before it exits; `heimdall
local` exits once the last one has exited.

A conversation is a chat, or one thread in it: `!use` in a thread changes
neither the other threads nor the chat itself. Each verified caller has
their own native session per conversation. A message sent
while the sender's own turn runs steers it (messages that wait while
another is handed over go together, as one steer); everything else queues
as a follow-up. Replies end with `— <harness>`.

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

Accepted `commands.unmentioned` entries: `stop`, `cancel`, `reset`, `chatid`, `help`, `status`, `all`. Exact standalone controls accept bare/slash forms (including `/abort`, `/nevermind`); `help`/`status` use `/help`, `/status`, `!help`, `!status`. `use` and `next` require addressing unless `all` is selected.
