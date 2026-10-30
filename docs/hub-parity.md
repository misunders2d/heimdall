# Hub parity: the environment of a harness turn

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

## How Heimdall sets them

- **Strip, then set.** Every inherited `PI_EXTERNAL_*`, `A2A_CALLER_*`,
  `PI_RESTRICTED_*`, `BIGQUERY_CALLER_PRINCIPAL` and `PI_TRANSPORT_CHILD` is
  removed from the harness environment, then the turn's own values are set.
  Neither Heimdall's own environment nor message text can supply them.
- `HEIMDALL_*` variables (Heimdall's secrets) never reach a harness.
- Values come from the caller its interface verified (`core.Principal`), the
  role Heimdall gave it, the conversation and the interface's one-to-one chat
  flag (`core.Inbound.Private`). Message text is never used.

## Variables Heimdall sets

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

### `PI_TRANSPORT_CHILD=1`

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

### `PI_RESTRICTED_EXTERNAL_USER` (provisional)

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

### AgentNet (phase 2)

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

## Variables Heimdall does not set

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

## Admin checks read the hub's own list

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

## Memento

Memento `d944c7d`: `src/memento/bridge.py:1659` returns early for every
operation except `install` when `MEMENTO_PROCESSING=1` (Claude, Codex and AGY
hooks); `src/memento/native_shared.ts:153` makes the Pi/OMP extension install
nothing. Heimdall sets it on every turn without memory and never sets it
otherwise; an owner turn on Heimdall's own uid inherits Heimdall's value, so
setting it for Heimdall itself (as live tests do) suppresses memory for every
turn. **Not verified live**: that each harness passes its environment
to its hook processes.
