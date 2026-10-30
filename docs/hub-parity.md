# Hub parity

Two parts: the environment of a harness turn (below), and the Telegram
connector ([Telegram](#telegram)).

## The environment of a harness turn

Heimdall replaces the Pi hub as the interface to every harness, so tools
written for the hub must read the same caller identity from a Heimdall turn.
This file lists every variable the hub sets on the children it spawns, who
reads it, and whether Heimdall sets it. The mapping lives only in
[`internal/harness/identity.go`](../internal/harness/identity.go).

Sources (read-only): `misunders2d/misunderstood-pi-pack` `ec72c7d` (branch
`hub`), `misunders2d/memento` `d944c7d`, `misunders2d/agentnet` `22cb8c1`.
The hub sets a turn's environment in `extensions/.shared/external-pi-runner.ts`
(~L796-827, Pi) and `extensions/.shared/external-omp-runner.ts` (~L955-970,
OMP); `buildMcpOAuthChildEnv` (`extensions/.shared/mcp-oauth-scope.ts:83`)
adds the MCP OAuth variables. Re-check against the current hub before relying
on a line number.

### How Heimdall sets them

- **Strip, then set.** Every inherited `PI_EXTERNAL_*`, `A2A_CALLER_*`,
  `PI_RESTRICTED_*`, `BIGQUERY_CALLER_PRINCIPAL` and `PI_TRANSPORT_CHILD` is
  removed from the harness environment, then the turn's own values are set.
  Neither Heimdall's own environment nor message text can supply them.
- `HEIMDALL_*` variables (Heimdall's secrets) never reach a harness.
- Values come from the caller its interface verified (`core.Principal`), the
  role Heimdall gave it, the conversation and the interface's one-to-one chat
  flag (`core.Inbound.Private`). Message text is never used.

### Variables Heimdall sets

| Variable | Heimdall's value | Hub sets it | Read by (hub `ec72c7d`) |
|---|---|---|---|
| `PI_EXTERNAL_PLATFORM` | `telegram`, `slack`, `a2a` (AgentNet); any other interface its own name (e.g. `local`) | yes, every turn | privileged-tool-guard, admin-lifecycle-guard, native-mcp-auth, neo4j-memory-tools/-security, a2a-external-identity, helium10-hub, keepa-h10, plane-broker, slack-canvas-core, website-* |
| `PI_EXTERNAL_USER_ID` | Telegram `tg_user_<numeric id>`; Slack the verified email; others `<platform>_user_<id>`, never a bare `local`/`system`. Not set for AgentNet yet (see below) | yes | privileged-tool-guard, admin-lifecycle-guard, scheduler (job owner), neo4j-memory-tools/-security, mellanni-bigquery, google_workspace_sa, clickup-mcp, native-mcp-auth, direct-ping-auth, a2a-external-identity |
| `PI_EXTERNAL_CHAT_ID` | Telegram `tg_chat_<chat id>`; Slack `sl_channel_<channel>`; others `<platform>_chat_<chat>`. Not set for AgentNet yet | yes | scheduler (job scope), privileged-tool-guard, website-professional-knowledge |
| `PI_EXTERNAL_USER_EMAIL` | Slack only: the verified email | **no** (Heimdall adds it) | mellanni-bigquery (`index.ts:24`), after `A2A_CALLER_PRINCIPAL` and `BIGQUERY_CALLER_PRINCIPAL` |
| `PI_RESTRICTED_EXTERNAL_USER` | `0` for owner turns; `1` for every non-root turn and every AgentNet turn | yes: `0` for admins, `1` otherwise | scheduler, a2a-external-identity (an A2A caller needs `1`) |
| `PI_EXTERNAL_MCP_PRIVATE` | `1` in the caller's one-to-one chat with the bot (Telegram `chat.type == "private"`, Slack DM), else `0` | yes (`nativeMcpUi.privateChat`) | native-mcp-auth (`:18-19`): with any `PI_EXTERNAL_*` set, MCP sign-in is refused unless this is `1` |
| `PI_TRANSPORT_CHILD` | `1` on every turn | yes, `1` on every child | see below |
| `MEMENTO_PROCESSING` | `1` on turns without memory; otherwise not set (the native setup decides) | no | Memento hooks and extensions (below) |

#### `PI_TRANSPORT_CHILD=1`

It tells hub extensions that the process is a chat turn, not the process that
owns the chat transports. Without it, every Heimdall turn would act as the
transport owner and start duplicate schedulers, pollers and chat bridges:

- `extensions/scheduler.ts:983,1089` (OMP `omp-extensions/scheduler.ts:647,741`):
  the scheduler runtime starts only when it is not `1`.
- `omp-extensions/slack.ts:241`, `omp-extensions/telegram/index.ts:117`,
  `omp-extensions/website.ts:49`: the chat bridges return early when it is `1`.
- `extensions/.shared/transport-owner.ts:2`: never the managed transport owner.
- `omp-extensions/google_workspace_sa.ts:168`: no OAuth callback server.
- `extensions/neo4j-memory-tools.ts:177`, `.shared/neo4j-memory-security.ts:101`:
  with it set, an unset `PI_EXTERNAL_USER_ID` no longer counts as the hub's
  trusted local admin (fail closed if identity propagation breaks).
- `extensions/helium10-hub.ts:11`, `.shared/website-professional-knowledge.ts:470`:
  behaviour reserved for Slack and website children.

#### `PI_RESTRICTED_EXTERNAL_USER` (provisional)

The hub sets `1` for callers who are not admins and its A2A check requires
`1`, so Heimdall sets `1` for every non-root turn and every AgentNet turn,
`0` for owners. Side effects in the hub scheduler
(`extensions/scheduler.ts:970`, OMP `omp-extensions/scheduler.ts:641`:
`restricted = a2a.indicated ? !a2a.admin : PI_RESTRICTED_EXTERNAL_USER === "1"`):
restricted callers' jobs get `allowBuiltinTools: false` (L1010, L1029),
failure routing is denied, and listing/editing is limited to the caller's
own scope. Employees share one OS user, so their jobs are separated only by
that scope. Revisit when Heimdall's own scheduler arrives (phase 3); the
owner may overrule.

#### AgentNet (phase 2)

AgentNet turns set only `PI_EXTERNAL_PLATFORM=a2a`,
`PI_RESTRICTED_EXTERNAL_USER=1`, `PI_EXTERNAL_MCP_PRIVATE=0` and
`PI_TRANSPORT_CHILD=1`. `extensions/.shared/a2a-external-identity.ts` verifies
an A2A caller only with platform `a2a`, `A2A_CALLER_PRINCIPAL`,
`A2A_CALLER_SOURCE=a2a_signed`, `A2A_CALLER_KEY_ID`,
`PI_EXTERNAL_USER_ID == A2A_CALLER_PRINCIPAL` and restricted `1`, so hub tools
treat Heimdall's AgentNet callers as unverified and fail closed. The
principal format is decided with the AgentNet interface; nothing in pi-pack
`ec72c7d` or agentnet `22cb8c1` sets `A2A_CALLER_*` today (only
`scripts/omp-a2a-agent-run.sh` checks them). Its admin list accepts only
entries containing `@`.

### Variables Heimdall does not set

| Variable | Hub sets it for | Effect of its absence in a Heimdall turn |
|---|---|---|
| `PI_EXTERNAL_DEFAULT_DELIVERY` | JSON of the chat a turn came from | scheduler (`parseDefaultDelivery`, L357): a job created without an explicit target has no default delivery chat. Heimdall's scheduler (phase 3) owns delivery. |
| `PI_EXTERNAL_TENANT_ID` | website tenants (hashed scope) | website-professional-knowledge refuses; Heimdall has no website interface. |
| `MCP_OAUTH_DIR`, `PI_MCP_OAUTH_ACTOR`, `PI_MCP_OAUTH_INSTALLATION`, `PI_MCP_OAUTH_SERVERS` | per-caller MCP OAuth scope (`buildMcpOAuthChildEnv`) | MCP OAuth tokens are the run-as OS user's native ones; no per-caller scope inside one OS user. |
| `OMP_RESTRICTED_NATIVE_IMAGE` | OMP: `1` for non-admins | `omp-extensions/genai-image.ts:173` registers the native image tool only when `1`. |
| `PI_GOOGLE_WORKSPACE_MODE` | Slack shared-draft mode | google_workspace_sa: no shared-draft-only mode. |
| `PI_PLANE_BROKER_ENABLED` | hub Plane broker | no Plane broker. |
| `PI_SCHEDULER_REPAIR_READONLY` | scheduler repair turns | not applicable. |
| `PI_WEBSITE_XLSX_REF`, `_RUN_ID`, `_CUSTODY_DIR`, `_FILES` | website uploads | not applicable. |
| `PI_CODING_AGENT_DIR` | Pi children, from the hub's `PI_CHILD_AGENT_DIR` | Pi uses the run-as user's own agent directory. |
| `PI_SCHEDULER_RUNTIME_ENABLED=0`, `PI_GOOGLE_OAUTH_SERVER_ENABLED=0` | OMP children | covered by `PI_TRANSPORT_CHILD=1` for the same extensions. |
| `A2A_CALLER_*`, `BIGQUERY_CALLER_PRINCIPAL` | nothing in the hub | stripped; see AgentNet above. |

### Admin checks read the hub's own list

Heimdall's role does not make a caller a hub admin. Hub guards
(`extensions/privileged-tool-guard.ts` `actor()` ~L135,
`.shared/access-control.ts` `isAdmin`) decide admin from `ADMIN_USER_IDS` in
the turn's environment and from `admins.json` in `$PI_ACCESS_DIR` (default
`$HOME/.pi/agent/access/`) of the turn's OS user. On the Heimdall host, each
owner's hub identity (`tg_user_<OWNER_TELEGRAM_USER_ID>`, the owner's Slack
email) must be in that list for the owner OS user, or owner turns lose
admin-only tools. An unset `PI_EXTERNAL_USER_ID`, or the raw value
`local`/`system`, counts as the hub's trusted local admin; Heimdall never
sends either.

### Memento

Memento `d944c7d`: `src/memento/bridge.py:1659` returns early for every
operation except `install` when `MEMENTO_PROCESSING=1` (Claude, Codex and AGY
hooks); `src/memento/native_shared.ts:153` makes the Pi/OMP extension install
nothing. Heimdall sets it on every turn without memory and never sets it
otherwise; an owner turn on Heimdall's own uid inherits Heimdall's value, so
setting it for Heimdall itself (as live tests do) suppresses memory for every
turn. **Not verified live**: that each harness passes its environment
to its hook processes.

## Telegram

Hub source: `extensions/telegram/index.ts` at `ec72c7d` (with
`.shared/telegram-context.ts`, `context-budget.ts`, `prompt-injection-guard.ts`,
`telegram-memento.ts`). Heimdall: Bot API details in `internal/iface/telegram`;
everything another interface can reuse (typing, albums, reactions, group
context, files, control words) in `internal/router` behind optional
interfaces (`Typer`, `Prompter`, `Reactor`, `GroupContext`, `FileSender`,
`Uploader`, `OwnerNotifier`).

### Ported

| Hub behaviour | Heimdall |
|---|---|
| Private chat always handled; in a group only a mention or a reply to the bot (`isMentioned` ~L724) | Same; `!` commands and control words too. Later parts of an addressed album count. |
| `/start` | `!help` (also `/start@bot`, deep-link payload). |
| Typing | `sendChatAction typing` at turn start and every 4 s while it runs. |
| Answers as legacy Markdown, plain retry on a parse error, transport note in the prompt | Same, note verbatim; Heimdall's own text is always plain. |
| `splitTelegramText` | 3500 UTF-16 units, cut at a paragraph, line or space break that keeps at least half the part, `(i/n)` appended; only the first part replies. |
| Reply and forward context | The replied-to message's text or caption and file, and the forward origin, marked untrusted. |
| Attachments | Photo, document, voice, audio, video and video note, the message's own and the replied-to message's; 20 MB cap; kept in a 0700 upload directory, copied at turn start into the turn's `in/` directory owned by the turn's OS user; text files' first 12000 characters inline, untrusted. |
| Images to the model | JPEG/PNG/GIF/WebP up to 3.75 MB, at most 10 per input, natively: Pi/OMP RPC `images`, Claude content blocks, Codex `localImage` (start and steer). AGY gets the path only. |
| Albums (`media_group_id`, 1.5 s) | Held 1.5 s after the last part, run as one message (or one steer); the first part's `!harness`/`!next` applies to every part. |
| Reactions (`[react: <ref> <emoji>]`) | The hub's note, its 73 emoji, its all-or-nothing parse; set after the text; the hub's failure notices. |
| Control words | `stop`, `/stop`, `cancel`/`abort`/`nevermind`, `reset`, `/reset`, `chatid`, `/chatid`, alone in a message; stop and cancel only by the turn's caller or an owner. |
| Group chat history as context | Newest 1000 kept per conversation, the 40 latest not in the turn shown at its start, 4000 characters each and 16000 in all (oldest trimmed, the hub's marker), secrets scrubbed, files by label, under the hub's untrusted header. |
| Failure message | `⚠ <harness> failed: <reason>. Please retry, or send reset if this keeps happening.`, secrets scrubbed. |
| Sending files back (`sendLocalArtifact`) | Per-turn outbox instead of tool telemetry; see "Design: outbound files" in [HANDOFF](HANDOFF.md). `sendPhoto` up to 10 MB, else `sendDocument`; at most 5 files, 50 MB each. |

### Not ported, and why

| Hub feature | Why not |
|---|---|
| Access whitelist and admin commands (`isAllowed`, whitelist/blacklist commands) | Heimdall has no allowlist (AGENTS.md roles): owners come from the config, every other verified caller is a non-root employee. Nothing to administer by chat. |
| Google Workspace guard (`enforceGoogleWorkspaceOutputBoundary`) | Tool and output restrictions belong to each harness's native configuration; hub guard extensions still run inside a harness that loads them, reading the identity variables Heimdall sets. |
| Admin-action guard | Same: a harness-side guard. Owner turns are never restricted by Heimdall. |
| MCP OAuth UI | Harness-native. Heimdall sets `PI_EXTERNAL_MCP_PRIVATE=1` in private chats, so `native-mcp-auth` works as under the hub. |
| Caption artifacts (`mellanni_drive_transcript` captions sent as `captions.txt`) | Tool-specific. A harness can leave the file in its outbox, which works for every tool and harness. |
| Scheduled-delivery context (`PI_EXTERNAL_DEFAULT_DELIVERY`) | Phase 3 scheduler owns delivery. |
| Unauthorized-user notices (`notifyAdminsOnce`, `notifyUnauthorizedTelegramUser`) | Every identified caller is allowed, so there is nobody to refuse; unidentified callers are ignored and logged (fail closed). Replaced by a one-time new-caller notice to the owners, without the hub's message snippet. |
| PID lock and heartbeat | One Heimdall per systemd unit; a second poller on the same token gets Telegram's 409, which is logged and retried. A data-directory lock is a cheap later addition if two instances ever share one. |
| `deleteWebhook` | AGENTS.md: Heimdall never calls `deleteWebhook` or `setWebhook`; a webhook shows as a logged 409. |
| Channel posts (`channel_post`) | A channel post has no verified user, and identity is per user (fail closed). `allowed_updates` is `["message"]`. |
| Injection-line removal (`sanitizeUntrustedText`) | Untrusted text is wrapped as the hub wraps it, and Heimdall's own frame markers in it are defanged, but lines are not removed by pattern (see recommendation 1). |
| Prompt-injection guard's block, Memento Telegram intake | Decided by the owner: see "Decisions" below. |

### Improved over the hub

- Mention match is case-insensitive and needs a username boundary
  (`@bot_fan` does not count; the hub's `includes` matches it).
- Each Telegram request is retried in place (429 after `retry_after`, at
  most 60 s; 5xx and network errors with backoff, 5 tries), so a rate
  limit never resends earlier parts; a reply is failed after 50 delivery
  attempts.
- Files back to the chat work on every harness and never from a path the
  model names; ownership, link count and type are checked on the opened
  file.
- Stored uploads are used only from the interface's upload directory,
  removed once their message is final, pruned hourly after 7 days.
- An album part still downloading holds the album (up to 3 minutes)
  instead of splitting it.
- Group context is kept per topic, not per chat.
- Owners are told once of each new non-root caller: name, verified key and
  conversation, never message text.
- A Markdown fallback is logged (metadata only), so its rate can decide
  whether HTML is worth it.

### Decisions (owner, 2026-10-30)

1. **Prompt-injection guard: log only, capped file.** Port the hub's
   scoring (`maybeLogDirectPromptGuard`, `ec72c7d`) at router level so every
   interface gets it. Never block, never alter the message. Log score,
   pattern names, conversation, caller key and message ID, never text, to a
   dedicated file in the data directory, rotated to one `.1` at a
   configurable cap (small default, about 1 MiB). Not built yet.
2. **Memento intake: per chat, off by default.** A config list of enabled
   conversations, empty by default, independent of the memory mode; for
   those chats every admitted message (addressed and ambient) is written to
   Memento's intake before acknowledgement; nothing for other chats. On
   hold: Memento (`29dc07b`) has no intake contract; the hub's format and
   worker live in pi-pack (see HANDOFF).
3. **Groups**: the current Pi Telegram bot is the reference: it answers in
   a group only when mentioned or when someone replies to its message.
   Ported as such (table above); no change.
4. **T3 Code is for coding only.** Heimdall keeps its own adapters.

### Earlier recommendations (kept for the reasoning)

1. **Prompt-injection guard** (`maybeLogDirectPromptGuard`, block at score
   4). Recommendation: do not port the block. It scores the caller's own
   message, and that caller is verified and gated per user already: the
   run-as OS user, `PI_RESTRICTED_EXTERNAL_USER` and the harness's tool
   rules are the protection. The patterns refuse ordinary requests ("ignore
   the previous rules in this config") and are easy to evade. Third-party
   text (group context, replies, forwards, files) is already marked
   untrusted. If the owner wants visibility, a log-only port is cheap:
   score and pattern names, never text.
2. **Memento Telegram intake** (`createTelegramIntake`: every message of one
   configured supergroup written durably to a queue before the update is
   acknowledged, delivered by a Python worker every 15 s). Recommendation:
   keep Memento's logic in Memento, and only if the owner wants group
   intake add one generic, interface-agnostic hook: per configured chat,
   each accepted or ambient message written as an untrusted event file into
   a 0700 spool directory before acknowledgement; Memento's own worker
   (today's `telegram-memento-delivery.py`) delivers it. It needs an owner
   decision first: it stores non-owner Telegram text in memory, which
   today's mode rules never do.

### Further improvements, not built

| Idea | Recommendation |
|---|---|
| Edited messages | Later, small: with `edited_message` allowed, an edit of a message still queued replaces its text; edits of messages already run are ignored. Low value. |
| HTML formatting with escaping | Not now. Models write Markdown, so HTML needs a Markdown-to-HTML converter with escaping; the plain fallback already keeps every answer readable. Decide from the fallback log rate. |
| `setMyCommands` (command menu) | Owner's choice, once, in BotFather: it changes the bot's public profile. Heimdall's commands are `!` words; the menu would list the `/` forms. |
| Voice transcription, PDF text | Leave to the harness: the file is saved in the turn's directory and its path given; no new dependency or service in Heimdall. |
| Local Bot API server (files over 20 MB) | No: an extra service. |
| Skip the download of a duplicate update | Not worth a router hook: duplicates arrive only after a restart before the offset was confirmed, and their download is removed at once. |
| Remember addressed albums across a restart (review finding 6) | Accepted limitation; a store row per album would fix it if it matters. |
| Group context for steers | Leave: messages that arrive mid-turn are context for the next turn. |
