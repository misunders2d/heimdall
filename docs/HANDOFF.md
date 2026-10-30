# Heimdall handoff (2026-10-30, second session)

Read [AGENTS.md](../AGENTS.md) first: it holds the product rules. This file
records where the work stands, what was decided, and what comes next. It
supersedes the first handoff of the same day.

## State

- `main` = the commit that adds this file; code HEAD `a51160a` (docs at
  `ba27cd1`). Pushed, tree clean. No WIP branch.
- `go vet ./...` clean. `go test -race -count=1 ./...` all pass at `ba27cd1`
  (config, core, harness, iface/slack, iface/telegram, router, store). The
  router package takes about 55 s, harness about 28 s.
- Store schema: steps 1-5 are shipped (5 = `inbound.private`). Steps are
  append-only; add new ones, never edit shipped steps.
- Phase 1 is complete except the live Telegram chat check with a human
  (see "Remaining work" 1). The no-human live check passed (below).

## Done

### Second session (this one)

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

## Live check (no human), 2026-10-30

Done on the development machine with a scratch config outside the repo,
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
- **AgentNet principal format is deferred to phase 2** (TODO in
  `identity.go`). Do not invent one.
- **Unknown callers** are ignored silently with a metadata log line.
- **Telegram** (answers 3-6):
  - Private chat: always handled. Group/supergroup: handled only if the text
    contains `@<bot username>` or the message replies to one of the bot's own
    messages (copy of the hub's `isMentioned`); `!` commands follow the same
    rule. Everything else is ignored silently and never stored (no ambient
    context in phase 1).
  - `/start` and `/start@<bot>` map to `!help`. Implementation choice: also
    with a deep-link payload (`/start abc`).
  - Text, else caption; neither: ignored with one log line. Attachments are
    not in phase 1.
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
    chat, message ID).
  - `getMe` failure at start: exit (fail fast). `getUpdates` 401/404 (token
    revoked or malformed): exit. 409: logged as "another poller or a webhook"
    and retried with backoff 1 s → 60 s. 429: wait `retry_after`. Other
    errors: backoff.
  - `sendMessage`: plain text (no `parse_mode`), `message_thread_id` for
    topics, `reply_parameters{message_id, allow_sending_without_reply}` on the
    first part only; parts of at most 4096 UTF-16 units, cut at a line break
    in the second half of a part when there is one. 429 sleeps
    `min(retry_after, 60 s)` inside `Send`, then reports a transient failure
    (the router's retry API takes no hint). Other 4xx: `PermanentError`. 5xx
    and network: transient. A failure in a later part makes the retry resend
    every part (at-least-once).
  - Thread = `message_thread_id` only when `is_topic_message`.
  - Skipped: no `from`, `from.is_bot`, `sender_chat`,
    `is_automatic_forward`, channel chats, `allowed_updates` is `["message"]`
    only (no channel posts, no edits).
- Not decided yet, do not act: whether Heimdall keeps its own adapters or
  hands turns to T3 (the orchestrator raises this when phase 1 closes);
  claude.ai connectors for the employee user are not phase 1.

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
   Afterwards delete the scratch data dir and log, and any harness session
   files the turns created under the owner's home
   (`~/.local/state/heimdall/sessions/` for pi/omp, plus the native session
   stores of Claude/Codex for the scratch working directory).
2. **Group privacy mode, unverified.** Telegram docs: with privacy mode on,
   a group bot receives `/command@bot` and replies to its messages; whether
   plain `@bot` mentions arrive is not documented. For the mention rule to
   work in groups the bot may need to be a group admin, or privacy off in
   BotFather (`/setprivacy`) and re-added. Check in the live group test.
3. **Employee turns live** need Heimdall running as root (production) or a
   dedicated dev setup. No OS users were created; decision is the owner's.
4. Phase 2: Slack interface (verification exists in `internal/iface/slack`;
   set `Inbound.Private` for DMs), AgentNet interface and its principal
   format (`identity.go` TODO).
5. Phase 3: scheduler (revisit `PI_RESTRICTED_EXTERNAL_USER` and
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
    SUCCESS). The answer ends with `ext="..."`, now listing
    `PI_EXTERNAL_*`, `A2A_CALLER_*`, `PI_RESTRICTED_*` and
    `PI_TRANSPORT_CHILD`, and `home="..."`.
  - `internal/iface/telegram` tests use a fake Bot API (`fakeAPI`) with
    per-method overrides; `newIface` shortens backoffs and the poll timeout.
  - A reference clone of the hub (`pi-pack` at `ec72c7d`) and three `.bak`
    copies of earlier Heimdall sources sit in a temp dir from the first
    session (`/tmp/heimdall-src`); not Heimdall test artifacts, left alone.
  - The first session's leftover live-test sessions (one Claude project dir,
    two Codex rollouts, synthetic prompts only) were deleted on 2026-10-30.
