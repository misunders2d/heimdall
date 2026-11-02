# Contabo staged install — root, no start

Run these steps yourself on the production hub. This release has not been
run there. Existing Pi hub remains responsible for Slack, Telegram and scheduling.
Never touch `heimdall-live` or `heimdall-slack-test`.

## Install only

1. Transfer release binary, its `.sha256`, and `deploy/contabo/` to a private
   staging directory. Set `RELEASE` to the binary's absolute staging path.
   Verify `sha256sum -c "$RELEASE.sha256"` from that directory and run
   `"$RELEASE" version`; compare commit with handoff. No secrets in these files.
2. Confirm `systemctl is-active heimdall.service` reports inactive/unknown and
   `systemctl is-enabled heimdall.service` reports disabled/static/unknown. If
   already active or enabled, stop installation and investigate; do not stop it
   or replace a running binary. Check existing files before overwriting; retain
   owner-only backups outside the repo if replacing an earlier staged install.
3. Install binary: `install -m 0755 "$RELEASE" /usr/local/bin/heimdall`.
4. Create private directories:
   `install -d -m 0700 /etc/heimdall /var/lib/heimdall`.
5. From transferred `deploy/contabo/`, run
   `install -m 0600 config.example.json /etc/heimdall/config.json`.
   Replace `telegram.bot_username` with the hub bot's already verified username.
   Template identity/workspace values are deliberately this deployment's values.
   Both modes keep employee Memento off. No tokens in config.
6. Install wrapper and unit:
   `install -m 0755 heimdall-run.sh /usr/local/sbin/heimdall-run.sh` and
   `install -m 0644 heimdall.service /etc/systemd/system/heimdall.service`.
7. Run `systemctl daemon-reload`. **Do not enable or start Heimdall.** Unit has
   no install target; it is intentionally static/disabled. Logs use journal.
8. Run `/usr/local/bin/heimdall check-config -config /etc/heimdall/config.json`.
   No interfaces, stores or harnesses open. Missing employee account produces a
   warning and exit 0; malformed config or an employee aliasing root/owner fails.
   This does not validate tokens, logins, binaries or bot username ownership.
9. Recheck inactive and static/disabled state. Old `pi-hub.service` stays serving.

## Launch findings

Source inspected: Pi pack `origin/hub`, revision
`c915ccbff0cde9c154f1cd99fa4707db47b42a24`,
`extensions/.shared/external-pi-runner.ts`, `scripts/pi-vps-cli.sh`,
`scripts/pi-native-cli.mjs`. No server inspection performed. Pack scripts do
not contain the deployed `/usr/local/sbin/pi-hub-run.sh`; its deployed override
must be confirmed by installer before switch. Runner default is
`PI_CHAT_CHILD_BIN || /usr/bin/pi`. Runtime test expects `/usr/bin/pi`.
Use that Infisical/native CLI wrapper, never `/usr/bin/pi-raw`: native CLI loads
`native-mcp-compat.mjs` factories. Owner PATH starts `/usr/bin` so Heimdall's
fixed `pi` executable resolves there. Other harnesses must be found on this PATH;
check with `heimdall harnesses` after employee account exists.

Wrapper forces hub A2A signing identity after injection, matching pi-vps-cli;
original bot-token variables are unset, HEIMDALL token variables stripped from
harness launches. Caller identity/private native MCP scope remains supplied by
Heimdall's Part 0 launch code; do not replace it with ambient owner scope.
Employee environment never inherits root secrets. The current hub Pi wrapper
forces root's agent directory/key, so employee needs its own user-native Pi
installation/launcher first on that account's PATH, with its own home, login,
native MCP seam and authorized identity. Never grant access to root credentials
or reuse the root-forcing wrapper for employees. Root instruction-file sharing
below does not authorize sharing secrets.

Default order remains `omp → codex → claude → pi`. Missing OMP is detected in
account PATH preparation before any process/turn starts, then Codex is tried.
The missing OMP reason appears in reply notes on every such default turn.
Recommendation for this hub: after owner agrees, remove `omp` from
`harness_order` to avoid noise. Explicit `!omp` never falls back.

Telegram `api_base` is the origin **without `/bot`**; adapter appends `/botTOKEN`.
`file_base` includes `/file/bot`, adapter appends `TOKEN/path`. Unit HEIMDALL
variables override JSON; max bytes is 2000000000. Default without overrides is
public API and 20 MiB. Local API must return downloadable relative file paths;
absolute server-local paths are not consumed as filesystem reads. Confirm this
in approved cutover smoke test. Prompt guard only logs metadata; never blocks.

## Pre-switch gates — separate approval, no actions during install

1. Obtain Sergey's switch approval and coordinate stopping the hub Slack and
   Telegram adapters. Keep hub scheduler running: scheduler cutover is phase 3.
   Stopping all `pi-hub.service` also stops scheduling; use only an explicitly
   approved outage with a scheduler continuity plan. Never run competing pollers
   or Slack Socket Mode clients. Do not touch the two reserved test/live units.
2. Obtain approval to create `heimdall-employee`; create a separate non-root OS
   account/home. Heimdall runtime refuses startup until account exists and UID
   differs from root/owner. Do not create it as part of staging.
3. Sergey performs native harness logins manually as root and separately as employee,
   using his own subscriptions for both.
   OMP absent; Codex, Claude, Pi required. Confirm per-user executables, homes,
   native automatic modes, MCP setup and restricted caller scope. No login copy.
4. Expose the ONE hub policy and skills outside `/root`, without copying:
   use administrator-owned `/opt/assistant` (0755) with read-only bind mounts
   of the canonical policy file at `/opt/assistant/AGENTS.md` (0644) and the
   ONE consolidated hub skills directory at `/opt/assistant/skills` (0755
   directories, 0644 text). This preserves the original inodes and pack hub
   ownership without making `/root` traversable. Resolve skill symlinks: any
   target still under `/root` is a failed gate. Do not mount credentials or
   all of the pack. Consolidating shared hub and Pi-specific skills must be
   completed first; no laptop skills or second discovery directory.
   Make both accounts' `~/.pi/agent/AGENTS.md`, `~/.codex/AGENTS.md`,
   `~/.claude/CLAUDE.md`, `~/.gemini/GEMINI.md` link to
   `/opt/assistant/AGENTS.md`; `~/.agents/skills`, `~/.claude/skills`, and
   `~/.gemini/config/skills` link to `/opt/assistant/skills`.
   Preserve existing files before replacing entries. Verify device/inode
   equality (`stat -L`) from source and every entry and read access as the
   employee. A symlink directly into `/root` fails. Bind mounts must be
   re-established after atomic source-file replacements; re-check inode
   equality on every policy update and after reboot. Install mounts only
   during an approved deployment, never from development tests.
   Install `deploy/employee/{employee-guard.ts,empty-mcp.json}` root-owned,
   employee-readable into `/opt/assistant/heimdall/`. Retain Pi's existing
   privileged-tool guard; employee extensions must contain no memory or
   owner business connectors. Employee native config and hook setup must
   be administrator-controlled. The Claude hook invokes the installed
   employee-readable Heimdall binary; a binary under `/root` cannot work.
5. Complete [owner Slack setup](../../docs/owner-slack-read.md) before enabling
   `owner_slack`; it stays false in staged config. Separate approval and native
   auth required; never connect live Slack during development.
6. Re-run offline check and harness discovery; confirm bot username, local API,
   scoped Pi launch, account separation, missing-token stripping and logins.
   Only after gates pass, approved operator may manually start `heimdall.service`.
   Run synthetic smoke tests without memory (`MEMENTO_PROCESSING=1`), then remove
   test state under controlled shutdown. Enable-on-boot is a separate unit change.
7. Rollback: stop only `heimdall.service`, wait for its cgroup to exit, confirm
   inactive, then restore hub chat adapters under approved procedure. Scheduler
   remains hub-owned. Preserve Heimdall state for diagnosis; do not replay turns
   interrupted by restart. Outbound delivery is at-least-once and may duplicate
   one ambiguous send. Never run both sets of chat adapters simultaneously.

## ONE-assistant launch gates (2026-11-02)

Default order is now `codex → claude → pi`. The example uses Pi-file admission
and `employee_harnesses: []`. Existing `/etc/heimdall/config.json` is NOT edited
by this change: migrate it deliberately at approved deployment, remove the old
OMP-first override, and retain Pi's access files as the only admission source.
`standalone_access: true` is a non-hub compatibility option, never for Contabo.

Before adding a harness to `employee_harnesses`, validate its installed version
in an isolated native test: skill discovery, SKILL.md body and reference reads;
denial of `/root`, employee provider auth, MCP auth, transcripts, other callers'
turns, shell mutation/network, owner MCP and credential/login flows. Verify
Memento recall/capture/hooks are absent. Keep validation evidence with release
notes. Fake tests alone do not pass these gates. The router refuses unvalidated
employee harnesses; AGY is always refused for employees, even when selected
explicitly. The AGY owner adapter now uses `-p --mode auto` for headless JSON.

Pi keeps `read` and a symlink-aware guard, excludes all other built-ins/MCP,
and uses a dedicated empty MCP config. Claude keeps `Skill,Read,WebSearch`,
uses strict empty MCP, denies mutation/delegation tools, disables inline skill
shell execution and auto-memory, and runs the fail-closed reader guard.
Employee Claude native memory hooks must be removed before validation (the
Memento suppression environment is defense in depth). Codex retains shell for
skill reads, requests a named filesystem profile limited to shared root, turn
cwd and system runtime files, denies home/auth/proc/sys, disables network,
auto-approval, hooks, memory, plugins/apps, browser/computer use and subagents,
and replaces the MCP map. Its app-server thread policy is also `never`.
Codex read-only execution cannot generate attachments; employee output files
are unsupported until a separately validated confinement design permits them.
The named profile implements read-only confinement; do not also pass the legacy
`sandbox_mode` override (official Codex config rejects that combination).
Version-dependent profile support must be verified; never fall back to an
unrestricted launch when unsupported. Employee warm processes are discarded
when turn directory or capability scope changes.

### Employee business MCP: intentionally excluded

`mcp.linear` and `scheduler` grants are evaluated by root and carried in launch
planning, never re-read using the employee UID. No business MCP is currently
exposed to employees, even with a grant. This is a remaining integration gate,
not a claim of employee tool parity.

Investigated pack `extensions/.shared/mcp-oauth-scope.ts`: per-caller stores
have `sha256-<scope>` directories, but the reviewed native Pi seam validates
against fixed `/root/.pi/agent/mcp-oauth-users`. Merely setting agentDir to the
employee home does not change that root. Native Claude/Codex OAuth caches are
account-level, not per-chat-caller. Sharing one employee UID cache would mix
employees' authority. Candidate storage is a root-owned broker tree outside
`/root`, e.g. `/var/lib/heimdall/employee-oauth/<principal-digest>` (0700), with
only a caller-bound connector's own subtree readable to its worker. Do NOT
make the entire tree readable to the shared employee UID. No credentials have
been moved or read by this implementation.

Linear requires `mcp.linear` AND a connector that selects the caller's OAuth
without exposing another caller's token. Mellanni additionally requires the
signed per-turn caller credential from MEL-724; model-controlled identity env
is insufficient. Both stay excluded until those connectors are implemented
and validated. Never substitute owner OAuth or company/service identity.
