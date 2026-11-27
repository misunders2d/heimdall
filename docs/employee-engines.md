# Employee turns on every engine (MEL-846)

An employee turn can run on any engine that has an employee launch boundary:
OMP, Codex, Claude and Pi (AGY stays refused). Each one gets the same
capability set employee Pi has:

- the caller's **own** Mellanni MCP, and Linear only with `mcp.linear`;
- shared instructions and skills under `/opt/assistant`, readable;
- no owner tools: T3, owner Slack, Memento, workspace-cli, claude.ai or
  ChatGPT connectors, shell, edit or write;
- Memento fully off (`MEMENTO_PROCESSING=1`, no memory MCP or hooks);
- no owner credentials, refresh tokens or transcripts;
- the `heimdall-employee` OS account, a 25-minute process limit, no warm reuse.

`employee_harnesses` may list any of `omp`, `codex`, `claude`, `pi`. Nothing
is enabled by default. Listing an engine is the owner's decision, made after
that engine's hub probe passes (below). **Codex stays off** until its probe
passes: its bubblewrap sandbox failed on the hub before
([employee-native-validation.md](employee-native-validation.md)), so its
read/network enforcement is unverified.

## Business MCP: one sign-in, root-side broker

Optional `employee_mcp` turns on Heimdall's own sign-in broker
(`internal/mcpoauth`, the same shape as `employee_google`):

```json
"employee_mcp": { "enabled": true }
```

`redirect_uri` defaults to `http://127.0.0.1:33418/callback`, a loopback
address nothing listens on. The employee, in a private DM, sends
`mcp login mellanni` (or `linear` with `mcp.linear`), opens the link, signs in
with their own account, and pastes the final address the browser could not
load back into the same DM within 10 minutes. `mcp status` lists their
connected servers; `mcp logout <server>` removes one.

- Discovery follows the MCP authorization spec (protected-resource metadata,
  then authorization-server metadata), dynamic client registration, PKCE
  S256 and the `resource` parameter. Every endpoint must be HTTPS. The
  protected-resource metadata must name exactly the configured server URL,
  and the authorization server's `issuer` must equal the one it names.
- A token without `expires_in` counts as short-lived and is renewed before
  every turn. A renewal that cannot be stored (its rotated refresh token
  would be lost) fails that server for the turn, is logged, and the caller
  is asked to sign in again.
- Refresh tokens and client registrations stay root-only:
  `<data_dir>/mcp-employees/<caller-digest>/` (0700 directories, 0600
  files, no symlinks). The digest rules are `employee_google`'s.
- Each turn gets only the current access token, renewed as root when it
  could expire mid-turn, written into the engine's native MCP config:
  - Claude: `--strict-mcp-config --mcp-config <lease>/mcp.json` (an `http`
    server with an `Authorization` header). A file, never argv.
  - Codex: one `mcp_servers` table; approved servers fully defined with
    `bearer_token_env_var`, every other one disabled.
  - OMP: `mcp.json` in the disposable agent directory, and the only MCP
    source: the lease's `config.yml` sets `mcp.enableProjectConfig: false`
    and disables the Claude, Codex, Cursor, Gemini, OpenCode, VS Code,
    Windsurf, plugin and project `mcp.json` discovery providers. On OMP
    19.2.1 a test with marker servers showed only the agent-directory
    `mcp.json` loading; without the settings, project `.mcp.json` and
    `.omp/mcp.json` also loaded. The probe fails on any `mcp__` tool that is
    not the caller's own server.
- Guards admit only tools of the approved servers. Claude/Codex names
  (`mcp__<server>__<tool>`) are matched on the exact server. OMP names
  (`mcp__<server>_<tool>`) cannot tell `mellanni_x` from `mellanni`, and its
  tool event has no server field, so pinning OMP discovery to the lease is
  what excludes a look-alike server.
- Callbacks never enter a queue, session, log or model prompt. Slack
  callbacks bypass raw-event staging, as before.
- With only Pi enabled, `mcp login` keeps Pi's native sign-in and store
  ([employee-pi.md](employee-pi.md)). With a non-Pi engine enabled, plain
  `mcp login` uses the broker and `!pi mcp login …` reaches Pi's native
  sign-in. **Pi still uses its own native store**: moving Pi onto the broker
  handoff needs the pack's `native-mcp-compat.mjs` to accept a static
  bearer, a follow-up outside this repository.

## Model access: access-only leases

Employees never log in. Like employee Pi, each turn gets a disposable 0700
directory (`~/.heimdall/turn-agents/<engine>-*`, removed when the process
exits) holding an access-only lease of the owner's existing login, never a
refresh token:

| Engine | Source (root) | Lease |
|---|---|---|
| Claude | `/root/.claude/.credentials.json` | `CLAUDE_CONFIG_DIR` with `.credentials.json` (access token, empty refresh), `ENABLE_CLAUDEAI_MCP_SERVERS=false` |
| Codex | `/root/.codex/auth.json` | `CODEX_HOME` with `auth.json` (access/id token, empty refresh), `cli_auth_credentials_store="file"`, `apps`/`plugins`/`remote_plugin`/`tool_suggest` disabled |
| OMP | `/root/.pi/agent/auth.json` (as employee Pi) | `PI_CODING_AGENT_DIR`: OMP creates its `agent.db` as the employee, then Heimdall adds one access-only `openai-codex` login |

An owner token with less than 30 minutes left is never leased: the turn fails
with a line naming the owner runtime that must renew it. Claude transcripts
live in `~/.claude/heimdall-sessions/<session>` and Codex rollouts in
`~/.codex/sessions`, so sessions resume across turns.

**Residual risk.** The model in an employee turn runs as the account that
owns its lease, so a reader bug or a native tool that escapes the confinement
could read the lease: that is the owner's subscription access token, valid
for the lease window (up to the token's own expiry). Employee Pi has carried
this risk since its rollout. Readers stay confined to `/opt/assistant` and the
turn directory, and credential file names are denied.

## Runtime preflight

Before every switched employee launch Heimdall checks that the engine
binary (and for Pi `runtime/node` and the Pi package) is readable by others.
If not, the turn fails with the path and the fix, instead of a native crash
such as "Cannot find module 'jiti'". Heimdall never changes permissions at
runtime. The fix: as root, rerun `deploy/contabo/employee-layout.sh install`
after **every** harness upgrade. It applies `a+rX` to the node binary and the
Pi package only (`NODE_ROOT` must be a `/root/.nvm/versions/node/v*`
directory), copies the Claude and Codex ELF binaries to
`/opt/assistant/runtime/<engine>/` (Codex lives under `/root/.codex`, an owner
auth directory that is never bound), links `bin/omp` to the already managed
`runtime/omp/omp` (never copied: `/root/.bun/bin/omp` is the pack's owner-only
wrapper), and checks every runtime as the employee account. A copy source
that is not an ELF executable, or is the managed copy itself, is refused.

## Hub probe

`deploy/contabo/employee-probe.sh [engine …]`, as root from a reviewed work
clone, after `employee-layout.sh install`:

1. Static, as `heimdall-employee`: `/root` and owner auth dirs inaccessible;
   runtimes readable; no owner login, ambient config or Memento reference in
   the employee home; no stale leases.
2. Live, one real model turn per engine (`TestEmployeeEngineLive`), started
   exactly as Heimdall starts an employee turn: no owner, connector or shell
   tools listed; owner credential reads denied; `/opt/assistant/AGENTS.md`
   readable. Without `PROBE_CALLER` Mellanni tools must be absent (the
   synthetic caller never signed in). With `PROBE_CALLER=<telegram id>` and
   `PROBE_MCP_DATA=<data_dir>`, after that employee's own `mcp login
   mellanni`, their Mellanni call must succeed.

The live checks read the model's own report plus the native tool events; the
operator reviews the logged answer. Enable an engine only after it passes.

## What only the hub can prove

Fake-harness and parser tests do not prove a live employee turn. Still open
until the probe passes on the hub, per engine: real model access through the
leases; OMP and Claude loading only the approved MCP servers from the
disposable config; Claude `--resume` across turn directories; Codex
thread resume through the shared sessions directory and its bubblewrap
sandbox; the pack's privileged-tool guard under OMP admitting approved MCP
tools; and a real employee's first broker sign-in against Mellanni and
Linear.
