# Heimdall handoff (2026-11-01, phase 2)

Read [AGENTS.md](../AGENTS.md) first: it holds the product rules. This file
records where the work stands, what was decided, and what comes next. It
supersedes the earlier handoffs of the same day.

## Phase 2 implementation (2026-11-01)

Slack is complete, developed on `wip/slack-parity` and merged to `main`,
with fake Web API/Socket Mode and native-protocol end-to-end tests. No live Slack connection was made; `heimdall-live` was not touched.
[Parity table](slack-parity.md) covers all 97 numbered inventory items, six
business-policy rows and all 16 independent amendments with code evidence.

- Startup/config/workspace validation and per-data-directory lock; raw events
  commit to SQLite before ack. Four bounded post-ack workers preserve normal
  channel order; a separate verified stop/cancel lane bypasses heavy work.
  Workers verify live same-workspace email and fetch history/download files.
- Live history plus stored ambient plus recent delivered assistant outputs
  share one 16000-character untrusted context budget. Embedded content cannot
  supply identity/commands; OAuth callbacks are hidden from context.
- Inbound files default to 200000000 bytes each, configurable, with 1 GiB
  reserved free disk and remaining-byte concurrent reservations; turn copies
  check destination allocation. File tokens only reach the
  allowed Slack file hosts; checked per-user turn staging remains generic.
- Optional generic lifecycle hook; Pi/OMP RPC, Claude stream-json and Codex
  public-summary/tool progress. Thinking stays, using hub NFKC/privacy/display
  rules. `golang.org/x/text v0.43.0` was explicitly approved for normalization;
  `github.com/coder/websocket v1.8.15` is pinned. Telegram receives no telemetry
  callback or extra Claude streaming flag unless it implements the hook.
- Fresh final posts, escaping/chunking, stable outbound row/part IDs and
  reconciliation before resend. Rejected posts fall back to existing status
  when the complete answer fits Slack's update limit. At-least-once remains.
  Progress timestamps/phase persist and restart recovery tidies them.
- Native outbox uploads, reactions, verified owner DM notices and generic
  commands. Schema 10 stages interface events; 11 persists progress; 12 adds
  completion timestamps and indexed event expiry. No
  migration rewrites existing shipped steps.
- Lifecycle leftovers fixed in main: early answers get the post-answer exit
  deadline; active stop sends group SIGTERM then SIGKILL after five seconds.

### Final verification

- `go vet ./...` and uncached `go test -race -count=1 ./...` passed.
- `CGO_ENABLED=0 go build ./cmd/heimdall` passed with its output in temporary
  storage; no generated binaries or test state are committed.
- Seven behavioral mutation checks caught regressions in shared command
  defaults, NFKC filtering, escaping, final reconciliation, concurrent disk
  reservations, native progress and persist-before-ack ordering. The lifecycle
  fixes were also checked against the original code and failed there.
- Final duplicate-event regression queues a redelivery before the first copy
  finishes, and proves a completed event is not processed again.
- Live Slack/harness validation is unverified; orchestrator owns that window.

### Owner decisions in force

- Slack only; AgentNet stays separate, never a Heimdall interface. No Heimdall
  memory capture/intake; native hooks and `MEMENTO_PROCESSING=1` remain.
- Shared `commands.unmentioned` controls Slack channels/threads and Telegram
  groups: default `["stop","cancel"]`, `["all"]`, or `[]`. Exact stop/cancel
  aliases work unmentioned under the default; other commands need addressing.
  DMs/private chats always work. This supersedes the earlier Telegram rule.
- Cancel preserves existing behavior: callers can clear only their own queued
  work even while another caller runs; only owners can clear everyone's queue.
  It never cancels another caller's running turn. Confirmed by orchestrator.
- Thinking summaries stay with no thinking flag. Inbound cap is 200 MB by
  default with the hub's 1 GiB free-space reserve. No pending transport flags.
- Fakes only during development. Production Slack app is shared with the hub;
  orchestrator arranges a single-consumer live window. Exact scopes/settings,
  secret names and config are in README's Slack live-window section.

## Re-review fixes (2026-11-01)

- Verified stop/cancel fences older ingress messages in the same conversation
  by event timestamp: own caller only, or every caller for an owner. Later
  messages remain. Suppression and control receipt persist together; normal
  admission rechecks completion under the same short lock. Replies count
  dropped ingress alongside router work. Telegram ingestion is serial and has
  no bypass lane, so this race does not apply there.
- Identity retries use single requests and timer rescheduling, three attempts
  maximum; full queues retain durable backlog without blocking socket ack.
  User-scoped permanent failures finish silently; token/scope failures log a
  rate-limited operator alert. No unverified caller is admitted.
- MCP filtering redacts native HTTP loopback callbacks in place and preserves
  ordinary URLs and useful surrounding content. Copy I/O runs outside disk
  reservation lock; recovered done turns normalize stale progress phases
  before pending delivery, preserving success cleanup and failure status.

Verification: `go vet ./...`, `go test -race -count=1 ./...`, and
`CGO_ENABLED=0 go build` passed. Seven temporary mutation checks removed each
fix in turn and the corresponding regression failed. New coverage lives in
`internal/iface/slack/rereview_test.go`, router ingress-control receipt tests,
and the strengthened store completion-migration test. Fakes only; live
validation remains with the orchestrator.

## Earlier phase-1 state (historical)

- `main` = the code HEAD (the review fixes of `fd72734..d05b748`; see
  "Sixth session"). Pushed, tree clean. No WIP branch.
- `go vet ./...` clean. `go test -race -count=1 ./...` all pass at that HEAD
  (config, core, harness, iface/slack, iface/telegram, router, store). The
  router package takes about 100-110 s, harness about 35 s, store and
  iface/telegram about 13 s each.
- Store schema: steps 1-9 are shipped (5 `inbound.private`, 6
  `outbound.extra`, 7 `inbound.extra`, 8 `ambient`, 9 `callers`). Steps are
  append-only; add new ones, never edit shipped steps.
- Telegram has the hub connector's transport behaviour; the parity table,
  what is not ported and why, and two open recommendations are in
  [`docs/hub-parity.md`](hub-parity.md) (Telegram). Phase 1 is complete
  except the live Telegram chat check with a human (see "Remaining work" 1).
- The live dev instance runs as its own systemd user unit `heimdall-live`
  (started by the orchestrator with `systemd-run --user`, so it outlives
  T3 sessions), binary `~/.cache/heimdall-live/heimdall`, the test bot,
  config `~/.cache/heimdall-live/config.json` (mode 600) with
  `max_parallel` 3, the hub's `PI_CHAT_MAX_CONCURRENT=3` (orchestrator,
  sixth session; it was 2). Restart it with `systemctl --user restart
  heimdall-live` only when its log shows no turn running; never `setsid
  nohup`. A stop now waits for its harness processes (at most 65 s).

## Incident: steering in the owner's live test (2026-10-30 21:13)

Private chat, owner, omp. "hey there, testing again." started turn 3; a
photo with a caption came while it seemed to run; then a text and a
forward. Turn 3 answered only the greeting; turn 4, replying to the photo,
answered only about the forward.

Evidence (live log, a copy of the live database, OMP's session JSONL):

- OMP answered turn 3 at 19:13:39 UTC and recorded its exit at 19:13:40,
  but its process ran until 19:14:08. Heimdall ended a turn only at process
  exit, so the reply (and the typing indicator) waited 28 s. The photo came
  at 19:13:50, after OMP's agent had ended: the steer returned `queued` (not
  handed over) and the photo became turn 4. Steering into an agent that
  already answered is impossible; the hub would have delivered the reply
  at `agent_end` and run the photo as the next turn too.
- Turn 4: the photo reached OMP natively (an `image` block) and OMP
  described it (19:14:17). The text and the forward then steered turn 4
  (both acknowledged, at 19:14:18 and 19:14:25) and OMP answered each
  separately. Heimdall kept only OMP's last assistant text, so the image
  description and the first steer's answer were dropped. The hub's
  `extractAssistantText` and AgentNet's adapter also keep only the last.
- Live probes (harness directly, real OMP/Claude/Codex): OMP's process ran
  6-28 s after its answer; Claude ran a steer written during its result as
  its next turn, whose answer Heimdall threw away (and told the sender "may
  or may not have reached"); Codex folded both steers into one answer.

Fix `5b3c62f`: the reply keeps the final text for each input; Claude's
late steer is waited for; a turn ends at the harness's answer, its process
gets 60 s (`harness.ExitGrace`) to exit, and the conversation's next turn
starts only after it exited; steers that become follow-ups are logged.

## Done

### Sixth session (this one): review of `fd72734..d05b748`

| Commit | What |
|---|---|
| `edd06ea` | Finding 1: a Codex steer acknowledged after `turn/completed` is unknown (the "may or may not have reached" notice, never replayed); acknowledged before completion it is accepted (bridge tests for both). |
| `1901028` | Finding 2: harness processes run in the router's own context and count as live until they exit. `Idle` is false while one runs, so `heimdall local` exits after the last. At shutdown each gets `Run.Stop` (SIGTERM to its group, SIGKILL `TermGrace` later); `Router.Run` waits up to `ExitGrace` + `TermGrace` (65 s, under the 90 s stop timeout; `heimdall-live` checked: `TimeoutStopUSec=1min 30s`, `KillMode=control-group`, so systemd also sends the SIGTERM to every harness process), then kills the groups left. Nothing starts once shutdown began. |
| `fd31af4` | Finding 3: an answer or approval request before ready commits the turn to that harness (no fallback; `Result.Ready` set). Its reply goes out at process exit: no ExitGrace timer runs before ready (a rare case). Also a router guard test that a fallback starts only after the failed process exited (fake `BADLINE`). It does not fail when the router's wait is removed today: before ready, Done closes only once the process exited. |
| `a9e25fe` | Finding 4: Claude replayed a late steer, then exited 0 without that turn's result: the answers stand with a note and the session is not resumed (was clean, no note). |
| `80cc95b` | Finding 5: guard patterns fold case between ASCII letters only, as JavaScript `/i` without `u` (RE2 `(?i)` also folded `ſ` and the Kelvin sign); checked against node. |
| this file | State, decisions, the orchestrator's answers. |

Each fix's test fails on the old code (checked by putting the old file
back): 1 `accepted`; 2 no SIGTERM at shutdown, idle while the process ran;
3 fell back to codex; 4 clean, no note; 5 score 4.

### Fifth session: guard, review of the steering fix, lingering processes

| Commit | What |
|---|---|
| `2269945` | Prompt-injection guard, log only (owner decision 1): `internal/router/guard.go`, `<data_dir>/prompt-guard.log`, config `prompt_guard.log_max_bytes` (default 1 MiB). |
| `d13b7e5` | Review findings 1, 2: a stop during a Claude drain is a stop (was an empty `— claude` and a clean session); a bad late turn (error, denial, approval, non-zero exit) keeps the answers with a note and `Result.Unclean`. |
| `47135e8` | Review finding 3: Codex keeps a final answer given before an accepted steer (checked live, see below). |
| `57f0d88` | Steps 3, 4 and review findings 4, 6: ExitGrace then SIGTERM, SIGKILL `TermGrace` (5 s) later, logged with the turn; `!stop` after the answer sends the SIGTERM at once; `max_parallel` counts live processes. |
| `311b605` | Review finding 5: reactions are parsed per answer (`Result.Answers`). |
| `6379042` | Review finding 7: an answer before ready never ends a turn early; a fallback waits for the failed candidate's exit. |
| `32bd194` | Step 2: steers that wait while another is handed over go as one steer, in order, with all their images. |
| `d05b748` | Regression of `47135e8`: a Codex steer acknowledged after `turn/completed` emptied the published answer; guarded (bridge-level test). Such a steer still counted as accepted (made unknown in the sixth session). |
| this file | State and decisions. |

Each has a test that fails on the old code (checked by putting the old
code back). Review tests added: cancel, crash, error result and approval
during a Claude drain (harness and router); a Codex steer after its final
answer; router multi-answer coverage for claude and codex.

Live check (no human), real `codex-cli 0.166.0` through its app-server
directly, an ephemeral thread, a synthetic prompt, `MEMENTO_PROCESSING=1`:
a `turn/steer` sent right after `final_answer` "ALPHA" was accepted for the
same turn (`turnId` matched), and Codex completed a second `final_answer`
"BRAVO" before `turn/completed`. The old code kept only "BRAVO". No rollout
was written (ephemeral); Codex's own sqlite files were touched as before.

### Fourth session: steering fix, owner decisions

| Commit | What |
|---|---|
| `0683d92` | Test fix: `TestMessagesBecomeVerifiedInbound` waits for the confirmed offset (failed once under `-race`). |
| `5b3c62f` | The steering fix above, with tests that fail on the old code (checked by mutation): `TestPhotoThenForwardSteerTheRunningTurn` (the owner's sequence, omp/pi/claude/codex), `TestReplyAtAnswerNextTurnAfterProcessExit`, `TestAnswerKeepsTheReplyToEachInput`, `TestClaudeLateSteerIsAnsweredNotDropped`, `TestAnswerIsFinalBeforeTheProcessExits`, `TestLingeringProcessIsStoppedAfterGrace`. |
| this file | Incident, decisions, AGENTS.md and `docs/hub-parity.md` updated. |

Live checks of `5b3c62f` through the router with a fake interface, real
harnesses as the developer's OS user, `MEMENTO_PROCESSING=1`, synthetic
prompts and a generated red PNG:

- Sequence A (a text turn running `sleep 15`, then photo+caption, then
  text, then a forward): omp, claude and codex each named the colour and
  quoted the forward. Claude took the photo and the text; the forward came
  after Claude's turn had ended and ran as the logged follow-up, batched
  with the next message.
- Sequence B (the incident's turn 4: photo+caption as the prompt, then text
  and forward at once): omp's reply now has all three answers (image, the
  text, the forward); claude and codex answered both in one.
- After each, a follow-up message ran as the next turn and answered.

### Third session: Telegram parity with the hub

| Commit | What |
|---|---|
| `197c092` | Typing while a turn runs (`router.Typer`, every 4 s). |
| `e8347e6` | Harness answers as Telegram Markdown with a plain retry, the hub's transport note, the hub's reply splitting. |
| `caed3f6` | Control words `stop`, `cancel`/`abort`/`nevermind`, `reset`, `chatid` and their `/` forms; stop and cancel only by the turn's caller or an owner. |
| `ba61965` | The hub's failure message; secrets scrubbed from everything sent to a chat. |
| `10a783d` | Reply and forward context, marked untrusted. |
| `fc83681` | Attachments: 20 MB cap, 0700 upload dir, per-turn copy owned by the turn's OS user, native images for Pi/OMP/Claude/Codex. |
| `fff5f94` | Albums run as one message (1.5 s). |
| `d6db1c2` | Reactions (`[react: <ref> <emoji>]`), the hub's emoji and parse. |
| `156b031` | An album caption's harness choice applies to every part. |
| `248ea50` | Recent group messages are context for later turns (ambient, store step 8). |
| `df1bddc` | A shutdown before the harness is ready keeps the turn's binding (flaky restart test). |
| `d5c281d` | Design of outbound files, written before building. |
| `bbe83af` | Harnesses send files back through a per-turn outbox. |
| `05c5c32` | Review findings 1, 4, 7: a message's files live until it is final; only files inside the upload dir are used; uploads pruned hourly. |
| `5c98492` | Review finding 2: each Telegram request retried in place; a reply fails after 50 attempts. |
| `50c72cc` | Review finding 3: an album part still downloading holds the album. |
| `71ae0d0` | Review findings 5, 6: README, AGENTS and this file describe Telegram as built; finding 6 accepted and documented. |
| `7bb6376` | Owners are told once when a new non-root caller writes (store step 9, `router.OwnerNotifier`). |
| `8205b59` | A Markdown fallback is logged (metadata only). |
| `8743721` | `docs/hub-parity.md` Telegram section; README and AGENTS name the new-caller notice. |
| `0ad219d` | This handoff. |
| `3ecd26b` | Test fix: `TestOutboxFilesAreSentAfterTheAnswer` read the outbox copy after the router may have removed it (flaked once). |

### Second session

| Commit | What |
|---|---|
| `61f3a61` | `!use` is sticky per conversation (chat + thread): replies, help, AGENTS.md and README say so; `TestUseIsStickyPerConversation` checks `!use` in one thread changes neither another thread nor the channel. |
| `d2c34fd` | AGY in `TestNativeApprovalRequestNeedsHuman`: the fake AGY's `ASK_HUMAN` auto-denies on stderr and still reports SUCCESS, as headless AGY does. |
| `9e6deee` | `harness.Accounts.Prepare` logs a warning when an employee's supplementary groups cannot be read (the turn keeps its primary group only). `Accounts.GroupIDs`/`Accounts.Log` are injectable. |
| `510843e` | Every turn gets `PI_TRANSPORT_CHILD=1` and `PI_EXTERNAL_MCP_PRIVATE` (`1` only in the caller's one-to-one chat). `core.Inbound.Private` set by the interface, stored (schema step 5), carried to `harness.Caller.Private`. Both stripped from the inherited environment first. |
| `c20c539` | [`docs/hub-parity.md`](hub-parity.md): every hub turn variable, its readers and Heimdall's value or why not set. |
| `a51160a` | Telegram interface `internal/iface/telegram` and `heimdall run -config FILE`; config `telegram.bot_username`. Tested against an `httptest` fake Bot API, including an end-to-end test through the router and a fake harness. |
| `ba27cd1` | AGENTS.md and README: Telegram rules, `heimdall run`, code layout (`iface/telegram`, `docs/`), status line. |

Every change has a test that fails on the old behaviour (checked by putting
the old code back temporarily, or by mutation for the new package).

### First session (summary)

Caller identity in the hub's variables (`internal/harness/identity.go`);
run-as accounts compared by uid and the employee account never root's or
the owner's; steers recorded before hand-over and never replayed after a
restart; owners are root and every other verified caller is non-root (no
allowlist); harness/session bound before the harness gets input; Slack caller
verification as the hub does it (`internal/iface/slack`); `Router.Run`
cancels on interface failure; `!` command, setting and reply in one
transaction; `store.NextDue` without busy-polling. Commits `3822a13` through
`b6edf62`; see `git log`.

## Live checks (no human), 2026-10-30

Third session, the development machine, real harnesses as the developer's
OS user, `MEMENTO_PROCESSING=1`, synthetic prompts:

- **Images** (review finding 8): field shapes checked against Pi 1.4.0
  `docs/rpc-commands.md` (`images: [{type: "image", data, mimeType}]` on
  prompt and steer), OMP 18.14.0's bundled RPC schema (`images?:
  ImageContent[]`) and Codex 0.166.0's generated schema (`UserInput`
  `{type: "localImage", path}` in `turn/start` and `turn/steer`). One turn
  per harness through `harness.Start` with a red PNG, the prompt not naming
  the colour: omp, pi, claude and codex each answered "red".
- **Outbox**: one turn per harness through the router with a file-sending
  fake interface: omp, codex, claude and pi each wrote `check.txt`
  (`pong`) into `out/` in automatic mode, without a human, and Heimdall
  collected and delivered it after the answer.
- Residue removed: session dirs, turn dirs, the Claude project dirs, the
  Codex rollouts, the OMP memory banks of the scratch working dirs. Left:
  rows for those Codex threads in Codex's live `state_5.sqlite` and
  `thread_history_1.sqlite`.

Second session, done on the development machine with a scratch config outside the repo,
`MEMENTO_PROCESSING=1`, the throwaway test bot (not production):

- `heimdall harnesses`: all five harnesses found for the owner OS user;
  every employee harness "unavailable": Heimdall is not root, so employee
  turns are refused (as designed).
- `heimdall run` with the right `bot_username`: `telegram: polling as
  @<test bot>`, no 409/conflict, SIGTERM after 20 s → `stopped`, exit 0.
- `heimdall run` with a wrong `bot_username`: exit 1, `the token belongs to
  @<test bot>, not the configured @<wrong>; refusing to poll`; no
  `getUpdates`.
- The token appeared 0 times in either log. Scratch data dir and logs
  deleted afterwards; the scratch config and a built binary were kept for
  the human check, and the token file was left alone.

## Decisions in force

Owner and orchestrator, 2026-10-30. Values below are placeholders; real IDs,
usernames and tokens live only outside the repository.

- **Identity variables are the Pi hub's** (names and formats), mapped only in
  `internal/harness/identity.go`; full list in `docs/hub-parity.md`.
  `PI_EXTERNAL_PLATFORM`, `PI_EXTERNAL_USER_ID` (`tg_user_<id>`; Slack the
  verified email; others `<platform>_user_<id>`, never bare `local`/`system`),
  `PI_EXTERNAL_CHAT_ID` (`tg_chat_<chat>`, `sl_channel_<channel>`,
  `<platform>_chat_<chat>`), `PI_EXTERNAL_USER_EMAIL` (Slack only).
- **`PI_TRANSPORT_CHILD=1` on every harness turn** (orchestrator answer 1).
  Why: the hub sets it on every child (`external-pi-runner.ts:800`); without
  it hub extensions act as the transport owner and run their scheduler
  runtime, chat bridges and pollers inside every turn.
- **`PI_EXTERNAL_MCP_PRIVATE`** (answer 2): `1` for a private one-to-one chat
  (Telegram `chat.type == "private"`, later the Slack DM), `0` otherwise; the
  interface sets `core.Inbound.Private`. Mirrors the hub's
  `nativeMcpUi.privateChat`.
- **`PI_RESTRICTED_EXTERNAL_USER`**: `0` for owner turns, `1` for every
  non-root and every AgentNet turn. Provisional (hub scheduler side effects in
  `docs/hub-parity.md`); revisit with Heimdall's own scheduler (phase 3).
- **Strip, then set**: inherited `PI_EXTERNAL_*`, `A2A_CALLER_*`,
  `PI_RESTRICTED_*`, `BIGQUERY_CALLER_PRINCIPAL` and `PI_TRANSPORT_CHILD`
  are removed before the turn's own values are set.
- **Caller roles**: owners listed in config `owners` (`telegram:<numeric
  ID>`, `slack:<verified email>`) are root; every other caller its interface
  verified is non-root. Slack callers count only as `internal/iface/slack`
  verifies them. Never by name, username or text.
- **Memory**: owner always; in `corporate` mode also verified Slack callers;
  Telegram non-owners never. No-memory turns get `MEMENTO_PROCESSING=1`.
- **`!use` is sticky per conversation** (interface + chat + thread).
  **`!stop`** keeps the session only if the harness confirmed the cancel;
  today the process group is always killed, so a stopped session is never
  resumed.
- **Launch flags stay** (OMP `--auto-approve`, Claude `--permission-mode
  auto`, Codex `approvalsReviewer: auto_review`).
- **AgentNet stays separate**, never a Heimdall interface. The inert
  identity branch is retained for compatibility, with no planned transport.
- **Unknown callers** are ignored silently with a metadata log line.
- **Telegram** (answers 3-6, then the parity brief of 2026-10-30):
  - Private chat: always handled. Group/supergroup: handled only if the text
    contains `@<bot username>`, the message replies to one of the bot's own
    messages (the hub's `isMentioned`), or it is a later part of an album
    whose first part, from the same sender within a minute, was handled;
    `!` commands and control words follow the same rule.
  - Every other group message from a person is *ambient*: never run or
    answered, kept as context (orchestrator reversed "no ambient context").
    Bounds in [`docs/hub-parity.md`](hub-parity.md) (Telegram).
  - `/start` and `/start@<bot>` map to `!help` (also with a deep-link
    payload); `/help`, `/status`, `/stop`, `/cancel`, `/reset`, `/chatid`
    (alone or `@<bot>`) are those commands.
  - Text, else caption, plus reply/forward context. Neither text nor a file:
    ignored with one log line.
  - Token only from `HEIMDALL_TELEGRAM_TOKEN`; config `telegram.bot_username`
    (5-32 letters, digits, `_`; a leading `@` is dropped); `getMe` must
    report that username (case-insensitive) before any `getUpdates`, or `Run`
    returns an error and Heimdall exits 1. Never `deleteWebhook`/`setWebhook`.
  - Use a separate test bot; never the production bot's token.
- **Telegram implementation choices** (mine, closest to the hub; the
  orchestrator may change them):
  - Mention matching is case-insensitive and needs a username boundary:
    `@<bot>_fan` does not count (the hub's `includes` would match it).
  - One leading or trailing `@<bot>` is stripped before command parsing (as
    the hub's `chat-control.ts`), so `@bot !status` and `!status @bot` work.
  - Offset: advanced past an update only after `accept` returned nil or the
    update was deliberately ignored; an `accept` error stops the batch and
    re-polls from that update after 1 s. The offset is not stored: Telegram
    keeps unconfirmed updates, and the store deduplicates by (interface,
    chat, message ID); a duplicate's new download is removed at once.
  - `getMe` failure at start: exit (fail fast). `getUpdates` 401/404 (token
    revoked or malformed): exit. 409: logged as "another poller or a webhook"
    and retried with backoff 1 s → 60 s. 429: wait `retry_after`. Other
    errors: backoff.
  - `sendMessage`: a harness's answer with `parse_mode` Markdown (the hub's
    legacy dialect; the prompt carries the hub's transport note), sent again
    as plain text on a 400; Heimdall's own text plain. `message_thread_id`
    for topics, `reply_parameters{message_id, allow_sending_without_reply}`
    on the first part only. Parts as the hub's `splitTelegramText`: at most
    3500 UTF-16 units, cut at a paragraph, line or space break that keeps
    at least half the part, `(i/n)` appended. Each request is retried in
    place up to 5 times (429: after `retry_after`, at most 60 s; 5xx and
    network: short backoff), so a rate limit never resends earlier parts.
    Then: other 4xx `PermanentError`; else transient, and the router's retry
    resends every part (at-least-once). The router fails a reply after 50
    delivery attempts (about 3.6 h).
  - Thread = `message_thread_id` only when `is_topic_message`; a reply to a
    forum topic's root message is not a reply.
  - Skipped: no `from`, `from.is_bot`, `sender_chat`,
    `is_automatic_forward`, channel chats, `allowed_updates` is `["message"]`
    only (no channel posts, no edits).
  - Accepted limitation (review finding 6): which group albums addressed
    the bot is kept in memory only. After a restart, the not yet received
    parts of such an album arrive unaddressed and become ambient context.
- **Owner decisions, 2026-10-30** (also in AGENTS.md and
  `docs/hub-parity.md` "Decisions"):
  1. Prompt-injection guard: the hub's scoring, log only, router level,
     capped dedicated log file (score, pattern names, conversation, caller
     key, message ID; never text). Built (`2269945`): every admitted
     message, ambient included, logged at the hub's block score 4.
  2. Superseded: memory belongs only to native harness hooks; Heimdall
     performs no capture or intake.
  3. Groups: the Pi Telegram bot is the reference (mention or reply to the
     bot); already ported.
  4. T3 Code is for coding only; Heimdall keeps its own harness adapters.
     The earlier open question is closed.
- **Turn end** (steering fix, then this session): Result is final at the
  harness's answer (only once the turn is ready); `Run.Exited` follows.
  `harness.ExitGrace` 60 s after the answer the process group gets
  SIGTERM, and SIGKILL `harness.TermGrace` 5 s later; both are logged with
  the turn. A stop by the turn's caller or an owner after the answer sends
  the SIGTERM at once (`Run.Stop`) and clears queued messages as any stop.
  The process holds its `max_parallel` slot until it exits; the
  conversation's next turn takes a slot only after it exited. At shutdown
  every live process gets `Run.Stop` at once and is waited for (65 s at
  most), then its group is killed; `Idle` counts live processes.
- **Answer before ready** (sixth session): a harness that answers or asks
  for an approval before it signalled ready took the turn; no fallback.
- **Codex late acknowledgement** (orchestrator, sixth session): a steer
  Codex acknowledges after `turn/completed` is unknown: the sender gets
  the notice, nothing is replayed.
- **Guard case folding**: ASCII letters only, as the hub's JavaScript
  `/i` without `u`.
- **Steers that wait go together**: every steer waiting while another is
  handed over is one steer (frames in order, images capped at 10 per
  input as before). An unknown outcome is one notice to the first message.
- **Claude drain**: a stop is a stop (Cancelled, session not resumed); a
  late turn that fails keeps the answers given, notes why, and leaves the
  session unclean (`Result.Unclean`, `Result.Warning`). So does an exit
  after Claude replayed a late steer but before its result (sixth
  session). Exit 0 with a steer never replayed stays clean; the steer is
  reported unknown.
- **Orchestrator answers, sixth session** (to the fifth session's
  questions):
  - More than 10 images in one combined steer: the rest go as file paths.
    OK.
  - A stop on an exiting process clears the caller's queue, as any stop
    does. OK.
  - SIGTERM before SIGKILL on `!stop`: confirmed.
  - Leftover rows in Codex's sqlite files: leave them.
  - Live config `max_parallel` 3, to match the hub's
    `PI_CHAT_MAX_CONCURRENT=3`; the file stays mode 600, nothing else
    changed. Done.
  - A Codex steer acknowledged after `turn/completed`: unknown (above).
- claude.ai connectors for the employee user are not phase 1.

## Remaining work, in order

1. **Live Telegram chat check with a human** (orchestrator runs it with the
   owner). Scratch config outside the repo: `data_dir` in a scratch dir,
   `mode` `personal`, `owners` = `telegram:<owner numeric ID>`,
   `run_as.owner` = the developer's OS user, `run_as.employee` = any
   existing non-root account with another uid (the dev check used `nobody`;
   employee turns are refused because Heimdall is not root),
   `telegram.bot_username` = the test bot. Start:
   `set -a; . <token env file>; set +a; MEMENTO_PROCESSING=1 heimdall run
   -config <scratch config> 2>> <scratch log>`. The owner sends, in a private
   chat with the test bot: a short synthetic prompt, `!status`, `!help`,
   `/start`; optionally a group test (reply to a bot message; for plain
   `@bot` mentions see item 2). Expected: each answered as a reply, tagged
   `— <harness>` or `— heimdall`; the log shows `turn N ... ended: done`.
   Then the Telegram behaviour, each checked once: typing while a turn
   runs; a message mid-turn (steers); `stop`; `cancel`; `/reset`; a reply to
   an earlier message; a forwarded message; a photo ("what colour?"); a text
   document; an album of two photos (one turn); a reaction ("react with 👍
   to my message"); a group mention; a file back ("send me a file
   hello.txt"); a second, non-owner account writing once (each owner gets
   one new-caller notice; that caller's turn is refused while Heimdall is
   not root).
   Afterwards delete the scratch data dir and log, and any harness session
   files the turns created under the owner's home
   (`~/.local/state/heimdall/sessions/` for pi/omp,
   `~/.local/state/heimdall/turns/`, plus the native session stores of
   Claude/Codex for the scratch working directory).
2. **Group privacy mode, unverified.** Telegram docs: with privacy mode on,
   a group bot receives `/command@bot` and replies to its messages; whether
   plain `@bot` mentions arrive is not documented. For the mention rule to
   work in groups the bot may need to be a group admin, or privacy off in
   BotFather (`/setprivacy`) and re-added. Check in the live group test.
3. **Owner decisions**: the further improvements in
   [`docs/hub-parity.md`](hub-parity.md) (Telegram).
4. **Employee turns live** need Heimdall running as root (production) or a
   dedicated dev setup. No OS users were created; decision is the owner's.
5. Phase 2: Slack interface (verification exists in `internal/iface/slack`;
   set `Inbound.Private` for DMs). AgentNet stays separate.
6. Phase 3: scheduler (revisit `PI_RESTRICTED_EXTERNAL_USER` and
   `PI_EXTERNAL_DEFAULT_DELIVERY`).

## Design: outbound files (Telegram gap item 10)

Written before it was built, as the orchestrator asked. Goal: a harness
can send files back to the chat, on every harness, without Heimdall ever
reading a path the model chose.

- **Outbox.** Every turn on an interface that sends files gets its
  per-turn directory (`~/.local/state/heimdall/turns/<turn>-<hex>/`, the
  one attachments use) with an `out/` directory, created through `os.Root`
  in the run-as user's home and owned by that user (0700). A note at the top
  of the prompt gives its absolute path: write files there before the final
  answer; Heimdall sends the regular files it finds there after the answer
  (at most 5, each at most 50 MB); nothing else is sent.
- **No model-chosen paths.** Heimdall parses no path or directive from the
  answer. When a turn ends with an answer (not failed, stopped or
  needs-human), Heimdall lists `out/` itself: top level only, in name order.
- **Checks per entry**, through `os.Root` on the user's home (nothing
  outside it is reachable, symlinks included): directories, devices and
  FIFOs are skipped; the opened file's own `fstat` must show a regular file
  owned by the turn's uid, `nlink == 1` (no hard link to a file the user
  cannot read) and 1 byte to 50 MB. Beyond 5 files the rest are reported.
- **Copy, then send.** Each accepted file is copied from the open
  descriptor into Heimdall's `<data_dir>/outbox/` (0700 directory, 0600
  file, random name), so the user cannot change what is sent afterwards and
  a restart can still send it. The answer and one outbound row per file
  (`core.Outbound.File`: the copy, a display name, MIME by extension) are
  stored in the turn's end transaction, in that order. A file not sent is a
  footer note on the answer: `file <name> not sent: <reason>`.
- **Delivery.** Telegram sends a file as `sendPhoto` (JPEG/PNG/WebP up to
  10 MB) or else `sendDocument`, multipart, replying to the turn's first
  message in its topic, as the hub's `sendLocalArtifact`. The retry rules
  are those of text: 429, 5xx and network errors are transient, other 4xx
  permanent. The copy is removed once its row is sent or failed; copies
  older than 7 days are pruned at start.
- **Harness-agnostic.** The harness writes files with its own tools as the
  turn's OS user. Interfaces opt in (`router.FileSender`); others get no
  note and no outbox.
- **Not done:** files sent mid-turn, files from failed or stopped turns,
  captions.
- **Hub difference:** the hub sends the artifacts OMP and Pi tools report in
  their telemetry (path, device, inode, SHA-256; at most 5). That is
  harness-specific and is not ported.

## Findings and gotchas

Hub source read at `misunders2d/misunderstood-pi-pack` `ec72c7d` (branch
`hub`), memento at `misunders2d/memento` `d944c7d`, agentnet at `22cb8c1`.
Clone them into a temp dir to re-check; do not edit them. Line references
are in `docs/hub-parity.md`.

- Hub Telegram (`extensions/telegram/index.ts`): `isMentioned` ~L724,
  `processMessage` ~L735, poll loop ~L851 (the hub calls `deleteWebhook`;
  Heimdall does not). Hub Slack (`extensions/slack.ts`): `slackUserEmail`
  L206-217, `auth.test` L1091, live verification + team check ~L1150,
  `nativeMcpPrivate: isDirectSlackConversation(event)` L1163.
- Hub admin checks read the hub's own admin list (`ADMIN_USER_IDS`, or
  `admins.json` under `$PI_ACCESS_DIR`, default `$HOME/.pi/agent/access/` of
  the turn's OS user), not Heimdall's role. Owner IDs must be listed there on
  the Heimdall host.
- `native-mcp-auth.ts:18-19`: MCP sign-in only with
  `PI_EXTERNAL_MCP_PRIVATE=1`, now set in private chats.
- Memento: `MEMENTO_PROCESSING=1` suppresses hooks (`bridge.py:1659`) and
  the Pi/OMP extension (`native_shared.ts:153`). **Not verified live**: that
  each harness passes its environment to hook processes. An owner turn on
  Heimdall's own uid inherits Heimdall's environment, so setting it for
  Heimdall disables memory for every turn (intended for live tests).
- Gotchas:
  - Config load looks up both OS users and requires `run_as.employee`; the
    example config's `telegram:OWNER_TELEGRAM_USER_ID` placeholder fails
    validation on purpose (IDs must be numeric). Unknown config keys are
    rejected, so a token can never be put in the config.
  - Router tests run both roles as the test user through `sameUser`;
    account rules are tested with stubbed `harness.Accounts` (`stubbed` in
    router_test, `stubAccounts` in harness_test). The Telegram package has
    its own `sameUser` and `TestMain` calling `fakeharness.Main()`.
  - `waitReady` polls `!status`: a session row exists before ready.
  - A test that edits the SQLite file through a second connection must wait
    until the router answers once and use `busy_timeout`.
  - Fake-harness prompt words: `FAILPRE=`, `FAILPOST=`, `HANGPRE=`,
    `WAIT_STEER`, `NOACK`, `SLEEP`, `ASK_HUMAN` (AGY: stderr auto-deny +
    SUCCESS), `REACT=<emoji>` and `REACTONLY` (reaction directives),
    `WRITE=<name>:<content>` (a file in the outbox the prompt names), `SLOW`
    (AGY sleeps 2 s), `STEERREPLY` (pi/omp: a final text for each input
    before the next steer is taken), `SLOWACK` (pi/omp: each steer taken
    1 s after it arrived), `STEERLATE` (claude: the steer runs after the
    prompt's result; with `LATEHANG`, `LATECRASH`, `LATEFAIL`, `LATEASK`
    that late turn hangs, exits 4, ends in an error result or asks for
    approval), `FINALFIRST` (codex: a final answer before steers are
    taken), `BADLINE=<name>` (an unreadable protocol line before ready,
    then 1 s with `STATE/lingering`, then exit 3), `EARLYRESULT` and
    `EARLYASK` (claude: a result or an approval request before the prompt's
    replay), `LATEEXIT` (claude, with `STEERLATE`: exit 0 after the late
    steer's replay, without its result), `LINGER=<s>` (the process stays
    after its answer with `STATE/lingering`; a fake started meanwhile
    writes `STATE/overlap`; SIGTERM writes `STATE/terminated` and exits,
    unless `NOTERM`). The fake harness also records the images it got.
  - Router tests keep the router's log in `env.logs`, the
    prompt-injection log at `env.guard` and the running router at `env.r`;
    `setGrace` shortens ExitGrace and TermGrace for one test. `env.stop`
    now waits for the harness processes still running (SIGTERM).
  - A process started with `setsid nohup` from a T3 session stays in that
    session's systemd scope and is stopped with it.
  - The router's `fakeIface` opts in to each optional interface with an
    atomic toggle (`react`, `group`, `files`, `notify`); it is recreated on
    every `env.start`, so set toggles again after a restart.
  - Album parts the interface is still downloading arrive first as
    `Announce` rows (never stored), then as the part itself.
  - `heimdall run` keeps Telegram uploads in `<data_dir>/uploads/telegram`
    and files to send in `<data_dir>/outbox`; harness turns use
    `~/.local/state/heimdall/turns/<turn>-<hex>/{in,out}` in the run-as
    user's home (kept 7 days).
  - Build with `-ldflags "-X main.version=$(git rev-parse --short HEAD)"`
    or the start line says `dev`.
  - `pkill -f '<pattern>'` from a shell whose own command line contains the
    pattern kills that shell too; anchor it (`'^/path/heimdall run'`) or
    kill by PID. The answer ends with `ext="..."`, now listing
    `PI_EXTERNAL_*`, `A2A_CALLER_*`, `PI_RESTRICTED_*` and
    `PI_TRANSPORT_CHILD`, and `home="..."`.
  - `internal/iface/telegram` tests use a fake Bot API (`fakeAPI`) with
    per-method overrides; `newIface` shortens backoffs and the poll timeout.
  - A reference clone of the hub (`pi-pack` at `ec72c7d`) and three `.bak`
    copies of earlier Heimdall sources sit in a temp dir from the first
    session (`/tmp/heimdall-src`); not Heimdall test artifacts, left alone.
  - The first session's leftover live-test sessions (one Claude project dir,
    two Codex rollouts, synthetic prompts only) were deleted on 2026-10-30.

## Review corrections (2026-11-01)

A1–A12 and B1–B8 addressed inline against pinned hub `ec72c7d`:
transient identity lookup retries retain durable events; final failure status
survives recovery without leaking memory; idle download deadline and scaled
absolute cap replace 60-second total; controls bypass blocked heavy workers;
chunking preserves inline fences and atomic angle tokens; disk accounting uses
remaining bytes and checks turn copies; unusable config entries rejected;
upload completion ambiguity gets explicit uncertainty, with sanitized reasons;
completed event dedupe retention defaults to 72 hours (configurable 1–720);
thinking filtering moved off stdout callback; Retry-After capped at five minutes.

Slash abort/nevermind normalize through shared Slack/Telegram router. No ready
status flicker. Slack header includes verified from identity. Both transports
set hub-format default delivery; thread ambient excludes channel ambient.
MCP commands/callbacks omitted from harness inputs and context. Shared notice
requires addressing; 1:1 DM notice also supported, with no sign-in implementation:
“MCP sign-in is not supported through Heimdall. Use your native harness directly;
do not paste OAuth codes here.” Unverified/foreign callers remain silent.

Deferred identity scope: website-only PI_EXTERNAL_TENANT_ID; MCP OAuth variables
PI_MCP_OAUTH_ACTOR and PI_MCP_OAUTH_INSTALLATION (not PI_EXTERNAL_* names).
Native authentication/scoping remains owner decision. No credentials copied.
Regression tests added per correction. Fakes only; no live Slack/Telegram or
heimdall-live operations. Required vet/race/static verification recorded with
implementation commit; live transport parity remains unverified.
