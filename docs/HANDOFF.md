# Heimdall handoff (2026-10-30)

Read [AGENTS.md](../AGENTS.md) first: it holds the product rules. This file
records where the work stands, what was decided, and what comes next.

## State

- `main` = the commit that adds this file; code HEAD `b6edf62`. Pushed, tree
  clean. No WIP branch.
- `go vet ./...` clean. `go test -race -count=1 ./...` all pass at `b6edf62`
  (config, core, harness, iface/slack, router, store). The router package
  takes about 55 s.
- Store schema: steps 3 (`user_email`, `user_key_id`) and 4 (`user_team`)
  are shipped. Steps are append-only; add new ones, never edit these.

## Done this session

| Commit | What |
|---|---|
| `3822a13` | Caller record keeps the verified Slack email and the AgentNet key ID (core.Principal, store schema step 3). |
| `17f34a2` | Every harness process gets the caller identity in the Pi hub's variables (`internal/harness/identity.go`); `harness.Start` refuses a turn without a `Caller`. |
| `045c873` | Codex fix 1: run-as accounts compared by uid; the employee account must be neither root nor the owner's (checked at config load and every turn); employee environments always rebuilt. `harness.Accounts` with injectable lookup for tests. |
| `beb971c` | Codex fix 2: a steer is recorded as `steering` before the hand-over; restart marks unresolved ones `unconfirmed` and tells the sender, never replays. |
| `df467ea` | Caller-role rewrite: owners are root, every other verified caller is non-root; no employee allowlist; `run_as.employee` required. |
| `5a819e6` | Codex fix 3: harness and session bound (not clean) before the harness gets input; turn refused if that write fails; binding restored when a harness fails before ready. |
| `4be70ab` | Slack caller verification as the hub does it (`internal/iface/slack/identity.go`), workspace kept in the caller record (schema step 4); Telegram non-owners never get memory. |
| `587b409` | Codex fix 4: `Router.Run` cancels its own context when an interface fails, then joins; interface error at shutdown is not a failure. |
| `4da108c` | Codex fix 5: a `!` command's state, a `!use` setting and its reply in one transaction (`store.AnswerCommand`). |
| `32f1410` | Test hardening: tests that edit the database wait for the router and use a busy timeout. |
| `fc0a98f` | Codex item 7: AGENTS.md now says harnesses run in their native automatic approval/review mode. |
| `b6edf62` | AGY fix: `store.NextDue` counts only replies `NextOutbound` could offer (no 10 ms polling during a chat's backoff). |

Every fix above has a test that was confirmed to fail with the old behaviour
put back temporarily.

## Decisions in force

Made by the owner and the orchestrator on 2026-10-30.

- **Identity variable names and formats are the Pi hub's**, so existing hub
  tools work unchanged. `PI_EXTERNAL_PLATFORM` (`telegram`, `slack`, `a2a`;
  other platforms use their own name), `PI_EXTERNAL_USER_ID`
  (`tg_user_<id>`; Slack: the verified email; other platforms
  `<platform>_user_<id>`, never bare `local`/`system`, which hub guards
  trust as their own admin), `PI_EXTERNAL_CHAT_ID` (`tg_chat_<chat>`,
  `sl_channel_<channel>`, `<platform>_chat_<chat>`), `PI_EXTERNAL_USER_EMAIL`
  (Slack verified email, also used as the user ID). The mapping lives only
  in `internal/harness/identity.go`; Heimdall's own `platform:id` keys stay
  in the store.
- **`PI_RESTRICTED_EXTERNAL_USER`**: `0` for owner turns, `1` for every
  non-root turn and every AgentNet turn. Why: the hub sets 1 for non-admins
  and its A2A check requires 1. Side effect accepted for now: the hub
  scheduler extension creates jobs without built-in tools for restricted
  callers and scopes their jobs per caller (employees share one OS user).
  Provisional; revisit when Heimdall's own scheduler arrives (phase 3); the
  owner may overrule.
- **Strip, then set**: inherited `PI_EXTERNAL_*`, `A2A_CALLER_*`,
  `PI_RESTRICTED_*` and `BIGQUERY_CALLER_PRINCIPAL` are removed from every
  harness environment before the turn's own are set. Why: owner turns on
  Heimdall's uid inherit its environment; neither that nor message text may
  supply an identity.
- **Caller roles** (`internal/core/core.go` `Access.Role`): owners are listed
  in config `owners` as `telegram:<numeric user ID>` or
  `slack:<verified email>` and are root. Every other caller the interface
  verified is non-root: any Telegram user (numeric ID), any Slack member of
  the bot's own workspace. A Slack caller counts only if users.info returned
  a live email, the user is neither deleted nor a bot, and `team_id` equals
  the bot's `auth.test` team ID (`internal/iface/slack.Verify`); no email
  cache counts as proof. Any other Slack caller is ignored silently and
  logged. Never by display name, username or message text. Why: owner rule,
  copying the hub's Slack check.
- **Memory**: owner always; in `corporate` mode also verified Slack callers;
  Telegram non-owners never, in either mode (final). Other non-owners
  (local test interface, AgentNet) get none. No-memory turns get
  `MEMENTO_PROCESSING=1`.
- **`!use` is sticky per conversation** (interface + chat + thread); the
  code already works that way, the docs still say "chat" (see remaining
  work). **`!stop`**: keep the session if the harness confirmed the cancel,
  otherwise start a fresh one. Today Heimdall always kills the process
  group, so a stopped session is never resumed (the "otherwise" branch).
- **Launch flags stay** (OMP `--auto-approve`, Claude `--permission-mode
  auto`, Codex `approvalsReviewer: auto_review`): they select each
  harness's own automatic mode from AgentNet's table. AGENTS.md wording was
  fixed in `fc0a98f`.
- **AgentNet principal format is deferred to phase 2.** AgentNet turns set
  only `PI_EXTERNAL_PLATFORM=a2a` and `PI_RESTRICTED_EXTERNAL_USER=1`, so hub
  tools treat them as unverified A2A callers and fail closed. TODO in
  `internal/harness/identity.go`. Do not invent a format.
- **Unknown callers**: ignored silently with a log line (platform and
  conversation only). Now only Slack callers that fail verification, or a
  principal without an ID, are unknown.
- **Telegram is phase 1** and must work end to end on the development
  machine. Use a separate test bot token, never the production bot's (two
  pollers on one token conflict).
- Not decided yet, do not act: whether Heimdall keeps its own adapters or
  hands turns to T3 (the orchestrator raises this when phase 1 closes);
  claude.ai connectors for the employee user are not phase 1.

## Remaining work, in order

Identity steps 2-3, the caller-role rewrite, Codex fixes 1-5 and the missing
coverage (different OS users per role via stubbed accounts, crash-window
restart tests, interface error at shutdown) are done, see above. Left:

1. **AGY: `!use` scope docs and test.** Say "conversation (chat + thread)"
   instead of "chat" in AGENTS.md:109, README.md:68-70,
   `internal/router/command.go:80-82` and the `!use` replies at
   `internal/router/router.go:411-413`. Rename `TestUseIsStickyPerChat`
   (`internal/router/router_test.go:265`) to `...PerConversation` and add
   the assertion that `!use` in thread T1 changes neither thread T2 nor the
   channel itself (`e.send` has no thread: send `core.Inbound` with
   `Conv.Thread` set, as `sendAs` does for principals).
2. **AGY: AGY in the approval test.** Add `agy` to
   `TestNativeApprovalRequestNeedsHuman`
   (`internal/harness/harness_test.go:145`). First check that the fake's
   `agy()` in `internal/fakeharness/fakeharness.go` handles `ASK_HUMAN`;
   add it the way the AGY protocol reports an approval request
   (`internal/harness/agy.go`).
3. **Optional (AGY): log a warning when `u.GroupIds()` fails** in
   `harness.Accounts.Prepare` (`internal/harness/launch.go:152`); a lookup
   failure must not silently drop the employee's own groups. Root's groups
   are dropped anyway (Go calls `setgroups` whenever Credential is set).
4. **Step 4: `docs/hub-parity.md`** (`internal/harness/identity.go` already
   links to it). For every environment variable the hub sets, give who sets
   it, who reads it and whether Heimdall sets it. Sources are under
   "Findings" below. Include: the provisional `PI_RESTRICTED_EXTERNAL_USER`
   rule and its scheduler side effects; `PI_TRANSPORT_CHILD`,
   `PI_EXTERNAL_MCP_PRIVATE`, `PI_EXTERNAL_DEFAULT_DELIVERY`,
   `PI_EXTERNAL_TENANT_ID`, `MCP_OAUTH_DIR`/`PI_MCP_OAUTH_*`,
   `OMP_RESTRICTED_NATIVE_IMAGE`, `PI_GOOGLE_WORKSPACE_MODE`,
   `PI_PLANE_BROKER_ENABLED`, `PI_SCHEDULER_REPAIR_READONLY`,
   `PI_WEBSITE_XLSX_*` as "not set by Heimdall"; that hub admin checks read
   the hub's own admin list (owner IDs must be in it on the Heimdall host);
   and the memento finding. Placeholders only, no real IDs or hosts.
5. **AGENTS.md and README**: add `internal/iface/telegram` and `docs/` to
   the code-layout table when they land, and keep the README status line
   current.
6. **Telegram interface** (`internal/iface/telegram`, stdlib `net/http`,
   no new dependency) plus a `heimdall run -config FILE` command
   (`cmd/heimdall/main.go`; only `local` exists today). Proposed config,
   not yet implemented or approved:
   - environment `HEIMDALL_TELEGRAM_TOKEN` (the test bot's token; the
     `HEIMDALL_` prefix keeps it out of harness environments);
   - config block `"telegram": {"bot_username": "<test bot username>"}`;
     at start call `getMe` and refuse to poll if the username differs, so a
     wrong (production) token is never used;
   - `owners`: `telegram:<your numeric user ID>`; `run_as.owner` and
     `run_as.employee` must name existing OS users with distinct uids.

   Behaviour to build and test against an `httptest` fake Bot API:
   - long-poll `getUpdates` with `allowed_updates: ["message"]`. Call
     `accept` per update in order, and advance `offset` only past updates
     whose `accept` returned nil: that offset is the upstream acknowledgement.
     Duplicates are already deduplicated by (iface, chat, msg_id).
   - principal `telegram:<from.id>`, Name from first/last name. Skip updates
     without `from`, with `from.is_bot`, `sender_chat` or
     `is_automatic_forward`, and channel posts.
   - conversation: `Chat` = `chat.id`, `Thread` = `message_thread_id` only
     when `is_topic_message`.
   - `sendMessage`: plain text, no `parse_mode`; `message_thread_id`;
     `reply_parameters{message_id, allow_sending_without_reply: true}`;
     split at 4096 UTF-16 units.
   - error mapping: 429 is transient (honour `retry_after`), other 4xx are
     `router.PermanentError`, 5xx and network errors are transient. A 409
     (another poller or a webhook) is logged and backed off; never call
     `deleteWebhook`/`setWebhook`.
   - Go's `*url.Error` text contains the request URL, which contains the
     token: scrub it from every error and log. Test that the token never
     appears in errors.
   - logs carry metadata only, never message text.
   - Open questions for the orchestrator: in groups, answer only on
     @mention, reply to the bot, or a `!` command (proposed default), or
     every message? Map `/start` to `!help`? Non-text messages: use the
     caption, otherwise ignore with a log line?
   - Live check after the token arrives: scratch config and data dir outside
     the repo, `MEMENTO_PROCESSING=1`, a synthetic prompt from a private
     chat; remove the scratch state afterwards.
7. **Leftover test artifacts outside the repo** (orchestrator: delete them
   if they are only Heimdall test artifacts; look before deleting): the
   Claude project dir whose name ends `...-scratchpad-live-work` under
   `~/.claude/projects/` (3 files), and 2 Codex rollouts under
   `~/.codex/sessions` that mention `scratchpad/live/work`.
8. **Open questions for the orchestrator** (not blocking 1-6): set
   `PI_TRANSPORT_CHILD=1` and `PI_EXTERNAL_MCP_PRIVATE` like the hub does? See
   Findings for what they change.

## Findings and gotchas

Hub source read at `misunders2d/misunderstood-pi-pack` `ec72c7d` (branch
`hub`), memento at `misunders2d/memento` `d944c7d`, agentnet at `22cb8c1`.
Clone them into a temp dir to re-check; do not edit them.

- The hub's per-turn environment is set in
  `extensions/.shared/external-pi-runner.ts` ~L796-827 and
  `external-omp-runner.ts` ~L955-970; `buildMcpOAuthChildEnv`
  (`extensions/.shared/mcp-oauth-scope.ts:83`) adds `MCP_OAUTH_DIR` and
  `PI_MCP_OAUTH_*`.
- The hub never sets `PI_EXTERNAL_USER_EMAIL`; only `mellanni-bigquery`
  (`extensions/mellanni-bigquery/index.ts:24`) reads it, after
  `A2A_CALLER_PRINCIPAL` and `BIGQUERY_CALLER_PRINCIPAL`. Nothing in
  pi-pack `ec72c7d` or agentnet `22cb8c1` sets `A2A_CALLER_*`; only
  `scripts/omp-a2a-agent-run.sh` checks them.
- `extensions/.shared/a2a-external-identity.ts`: an A2A caller is verified
  only if platform `a2a`, principal, source `a2a_signed`, key ID,
  `PI_EXTERNAL_USER_ID == principal` and `PI_RESTRICTED_EXTERNAL_USER=1`.
  Its admin list only accepts entries containing `@`.
- `extensions/scheduler.ts:970` (OMP `omp-extensions/scheduler.ts:641`):
  `restricted = a2a.indicated ? !a2a.admin : PI_RESTRICTED_EXTERNAL_USER === "1"`.
  Restricted callers' jobs get `allowBuiltinTools: false` (L1010, L1029),
  failure routing is denied, and job listing/editing is limited to the
  caller's own scope.
- `extensions/privileged-tool-guard.ts` `actor()` (~L135): an unset
  `PI_EXTERNAL_USER_ID`, or the raw value `local`/`system`, counts as the
  hub's trusted local admin. Admin otherwise comes from the hub's own list
  (`access-control.ts` `isAdmin`, `ADMIN_USER_IDS`/admins file), not from
  Heimdall's role.
- `extensions/native-mcp-auth.ts:18-19`: with any `PI_EXTERNAL_*` set, MCP
  sign-in is refused unless `PI_EXTERNAL_MCP_PRIVATE=1`. Heimdall does not
  set it, so MCP OAuth sign-in inside Heimdall turns fails today.
- Several hub extensions branch on `PI_TRANSPORT_CHILD=1` (scheduler
  runtime, helium10-hub); Heimdall does not set it.
- Hub Telegram (`extensions/telegram/index.ts:740-752`): user ID
  `canonUser("tg", from.id)`; non-admins get `--no-builtin-tools` and
  `PI_RESTRICTED_EXTERNAL_USER=1`. Hub Slack (`extensions/slack.ts`):
  `slackUserEmail` L206-217, `auth.test` L1091, liveVerified + team check
  ~L1150.
- Memento `d944c7d`: `src/memento/bridge.py:1659` returns early for every
  operation except `install` when `MEMENTO_PROCESSING=1` (Claude, Codex and
  AGY hooks); `src/memento/native_shared.ts:153` makes the Pi/OMP extension
  install nothing. **Not verified live**: that each harness passes its
  environment to hook processes.
- Gotchas:
  - Config load now looks up both OS users and requires
    `run_as.employee`. The example config's
    `telegram:OWNER_TELEGRAM_USER_ID` placeholder fails validation on
    purpose (IDs must be numeric).
  - Router tests run both roles as the test user through `sameUser`;
    account rules are tested with stubbed `harness.Accounts` (`stubbed` in
    router_test, `stubAccounts` in harness_test).
  - `waitReady` polls `!status`: a session row now exists before ready.
  - A test that edits the SQLite file through a second connection must
    wait until the router answers once and must use `busy_timeout`, or it
    aborts the router's startup recovery.
  - New fake-harness prompt words: `NOACK` (pi/omp steer taken but never
    acknowledged), `HANGPRE=NAME` (hang before ready). Its answer now ends
    with `ext="..."` (identity variables seen) and `home="..."`.
  - The owner's earlier journal handoff note (2026-10-29, outside the repo)
    is superseded by this file.
