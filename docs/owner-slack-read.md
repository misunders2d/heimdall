# Owner Slack read/search setup

Opt-in, default disabled. Only owners in Telegram private chats or freshly
verified same-workspace Slack 1:1 DMs receive Slack read/search tools, on Pi,
Claude and Codex. Channels, groups, MPIMs, employees, ambient messages and
scheduled jobs never qualify. Other native tools and memory policy remain as
configured. This is the single exception to Heimdall's no-tool-exclusions rule.

No live Slack authorization was performed during implementation. Development
used fake APIs/protocols and a synthetic loopback OAuth server. Production
activation and account/workspace confirmation belong to Sergey.

## Configuration

Add to Heimdall's JSON configuration:

```json
{
  "owner_slack": {
    "enabled": false,
    "server_url": "https://mcp.slack.com/mcp",
    "client_id_env": "SLACK_CLIENT_ID",
    "client_secret_env": "SLACK_CLIENT_SECRET",
    "pi_callback_port": 18765,
    "claude_callback_port": 18766,
    "codex_callback_port": 18767
  }
}
```

Omitted scopes default to the hub's 13:

```text
search:read.public search:read.private search:read.im search:read.mpim
channels:read channels:history groups:read groups:history
im:read im:history mpim:read mpim:history users:read
```

`scopes` may explicitly list these plus `search:read.users`,
`search:read.files`, `files:read`, `users:read.email`, `canvases:read`, and
`reactions:read`. Write and unknown scopes are rejected. Current hub Pi seam
(af9ffff) accepts only its original 13; extending Pi scopes also requires a
separately reviewed hub change. Never loosen native guards to bypass a failure.
Pi's native URL, scopes and callback port must match this configuration.

Environment names are references, never secret values. Supply them to Heimdall
through the owner's normal secret manager. Pi receives canonical
`SLACK_CLIENT_ID`/`SLACK_CLIENT_SECRET` only on eligible turns. Claude uses the
client secret saved during native registration; Codex uses its native owner-only
configuration. All harnesses strip inherited `SLACK_CLIENT_*`, `MCP_OAUTH_DIR`
and `PI_MCP_OAUTH_*`; configured client references are stripped too.

## Slack app

Use the owner's existing internal or directory-published app, approved by its
workspace admin for MCP. Keep bot authorization and Socket Mode settings intact.
Register all six exact redirect URLs (adjust ports if configured differently):

```text
http://localhost:18765/callback
http://127.0.0.1:18765/callback
http://localhost:18766/callback
http://127.0.0.1:18766/callback
http://localhost:18767/callback
http://127.0.0.1:18767/callback
```

Keep listeners local. Do not expose or publicly forward callback ports. Preserve
PKCE and token rotation. Confirm consent contains only the configured read scopes.
Slack bot `conversations.info` must return `is_im=true`, `is_mpim=false` and the
verified caller's user ID before owner Slack access is granted in a Slack DM.

## Pi: existing hub wrapper and caller stores

Use the pack's `pi` wrapper → `pi-native-cli.mjs` → `native-mcp-compat.mjs`,
not stock Pi. Merge the hub's `config/mcp/slack-owner-example.json` Slack entry
into the owner's native `~/.pi/agent/mcp.json`, preserving other servers.
Set `hubOwnerPrincipal` to the existing verified identity mapping; retain hidden
exposure, read/search-only tool exposure and literal environment references.
File ownership and native owner/admin checks remain enforced by the seam.

The actor must resolve through native `access/identities.json` and be in
`ADMIN_USER_IDS`; file admins alone do not grant Slack ownership. Linear approval
uses the hub's `mcp.linear` capability/admin rules, and Mellanni remains approved
for identified callers. Heimdall never copies tokens or credentials.

Root uses the existing `/root/.pi/agent/mcp-oauth-users/sha256-*` directories.
Telegram digest input is `telegram:tg_user_<id>`; Slack uses
`slack:sl_user_<bot>:sl_user_<caller>:servers=helium10`. The legacy label must stay.
Golden-vector tests prove path compatibility, not production token validity.
Slack email remains role authority; user/bot IDs only select the OAuth store.

Enable `owner_slack`, then send `mcp login slack` in an owner private chat.
Open the native browser link and paste the loopback callback into that same
private chat. `mcp logout [server]` and `mcp reconnect [server]` also route to Pi.
Callback input is ephemeral, never queued, logged, echoed or shown to the model.
The native input expires after 300 seconds. A restart loses the pending login;
start it again. Telegram and Slack use separate stores and require separate
consents. Existing hub Telegram consent is reused when actor/home/config match.

## Claude: one-time root login

Connect with `ssh -t root@<host>`. Work in a dedicated login directory that
Heimdall never uses as its turn working directory. Build identical server JSON
for login and Heimdall: type `http`, configured URL, and `oauth` containing
`clientId`, `callbackPort:18766`, and `scopes` as the space-separated scope string.
Use the client ID's resolved value; no secret belongs in JSON or argv.

With `MCP_CLIENT_SECRET` supplied securely in the environment:

```sh
claude mcp add-json -s local slack '<server JSON>' --client-secret
claude mcp login slack --no-browser
```

Never register this server at user scope. Local scope confines registration to
the login directory. The native token/client-secret store belongs to root.
Heimdall supplies matching server JSON via `--mcp-config` only for eligible turns,
plus read/search `--allowedTools` and a PreToolUse hook invoking the same
Heimdall binary. Unknown/write Slack tools fail closed. Every other Claude turn
gets `--disallowedTools 'mcp__slack__*'`.

Claude 2.1.314 mock spike: local-scope registration and PTY `--no-browser` login
performed one synthetic token exchange. A print invocation from another cwd,
using per-turn config, made four authenticated mock MCP requests without another
consent. Its model endpoint was also mocked and deliberately returned HTTP 400.
No real OAuth or model API request was made.

## Codex: one-time root login

Over `ssh -t root@<host>`, configure root's native `~/.codex/config.toml` with
mode 0600. Keep the secret in that file, populated through the existing secure
setup process; never pass it to `codex mcp add --oauth-client-secret` or `-c`.

```toml
mcp_oauth_credentials_store = "file"

[mcp_servers.slack]
url = "https://mcp.slack.com/mcp"
enabled = false
scopes = ["<each configured read scope>"]
enabled_tools = ["slack_search_public", "slack_search_public_and_private", "slack_search_channels", "slack_search_users", "slack_read_channel", "slack_read_thread", "slack_read_canvas", "slack_read_user_profile"]
default_tools_approval_mode = "auto"

[mcp_servers.slack.oauth]
client_id = "<client ID>"
client_secret = "<securely provisioned secret>"
callback_port = 18767
```

Then:

```sh
codex mcp login slack --no-browser -c mcp_servers.slack.enabled=true
```

Heimdall starts a fresh app-server process for each turn. Eligible processes get
`enabled=true`, exact read/search tool allowlist, pinned scopes, URL, callback
port and automatic tool approval. Every other process explicitly gets
`enabled=false` and an inert HTTP URL, even if native config drifted or no Slack
entry exists. Native 0.166.0 rejects an enabled-only entry without a transport;
a disposable-home CLI check verified this behavior. Sessions remain per conversation,
verified caller and harness; processes never move between conversations. Native
approval/input requests still end as needs-human; Heimdall never waits for an
operator response or invents one. Live Slack approval/refresh remains unverified.

## Re-test after consent

Authorize Pi Telegram, optional Pi Slack, Claude, then Codex separately. After
the last consent, re-test Pi Telegram, Pi Slack (if used), Claude, then Codex;
a later consent may revoke an earlier authorization. Do not copy tokens between
stores. Use one owner-selected read/search query in each private chat, confirm
account/workspace, then verify a shared owner turn and an employee turn lack
Slack tools. Missing scopes should fail explicitly, never trigger write consent.

Default scopes omit file/canvas/reaction reads and user-search scope; tools
requiring those may return insufficient_scope. Native transcripts and Memento
follow existing policy: eligible owner turns still have memory. No automatic
cross-harness token repair or production activation is implied by tests.

Sources checked 2026-11-01: hub af9ffff; [Claude MCP](https://code.claude.com/docs/en/mcp),
[Codex config](https://learn.chatgpt.com/docs/config-file/config-reference),
[Slack MCP](https://docs.slack.dev/ai/slack-mcp-server/). Live deployment unverified.
