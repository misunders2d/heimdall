> Employee Pi update: see [employee-pi.md](../../docs/employee-pi.md). Caller-owned Mellanni/Linear
> OAuth is the supported employee route; the older signed-token prerequisite
> below is superseded for that route. Enable only Pi after its native probes.
> Employees never sign into harnesses; business MCP uses their own sign-in.

# Contabo staged install — root, no start

Employee turns run on Sergey's existing harness installs and logins. Employees
never log in anywhere; there is no second login. Restriction and isolation are
implementation work, not a user setup task. Employee identity reaches tools via
Heimdall's verified platform identity and the Mellanni signed per-turn caller
token (MEL-724, not built yet). Keep `employee_harnesses: []` until native
isolation, skill reading, credential denial and caller-bound tools are validated.

Run these steps yourself on the production hub. Since cutover Heimdall serves
production there; this section records the first staged install. For later
releases use "Release, upgrade and rollback" below.
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
   `install -m 0755 heimdall-run.sh /usr/local/sbin/heimdall-run.sh`,
   `install -m 0755 heimdall-infisical-run.sh /usr/local/sbin/heimdall-infisical-run.sh` and
   `install -m 0644 heimdall.service /etc/systemd/system/heimdall.service`.
7. Run `systemctl daemon-reload`. **Do not enable or start Heimdall.** Unit has
   no install target; it is intentionally static/disabled. Logs use journal.
8. Run `/usr/local/bin/heimdall check-config -config /etc/heimdall/config.json`.
   No interfaces, stores or harnesses open. Missing employee account produces a
   warning and exit 0; malformed config or an employee aliasing root/owner fails.
   This does not validate tokens, logins, binaries or bot username ownership.
9. Recheck inactive and static/disabled state. Old `pi-hub.service` stays serving.

## Release, upgrade and rollback (2026-11-10)

Owner rule, 2026-11-10: only the main branch may run in production. A release is a
`main` commit whose CI run (`.github/workflows/ci.yml`: vet, race tests,
static build) passed, and which passed the real-engine check on the host,
as the owner OS user, with the candidate binary:
`deploy/contabo/engine-check.sh "$RELEASE"`. It sends one trivial prompt per
installed engine (`!codex`, `!claude`, `!pi`) through `heimdall local` with
a scratch config and data dir and `MEMENTO_PROCESSING=1`, and prints only
`pass`, `fail` or `skipped` per engine. CI uses fake harnesses only. The
check does not cover owner_slack launches.

Only `heimdall run` upgrades the database schema. `heimdall sched` and
`heimdall drain` never migrate. They serve schemas from
`store.AdminMinSchema` up to their own, and refuse any other schema with a
message that names both versions. `heimdall version` prints the schema the
binary applies.

Upgrade (root, on the host):

1. Verify the new binary in staging (`sha256sum -c`, `"$RELEASE" version`).
2. Drain with the **installed** binary:
   `/usr/local/bin/heimdall drain -config /etc/heimdall/config.json`.
   Exit 0 means idle. Exit 75 means timeout: do not stop, try again later.
3. `systemctl stop heimdall.service`, then confirm it is inactive and its
   cgroup has exited.
4. Back up the state while the service is stopped: copy
   `/var/lib/heimdall/heimdall.db` and `/var/lib/heimdall/durable.db` with
   their `-wal`/`-shm` files, together, and the old binary, into a new 0700
   directory outside the data dir. Do this before every release: it is the
   only way back if the release adds a schema step (`heimdall version`
   prints the schema of builds from 2026-11-10 on) or changes the DBOS
   version. Backups hold message text, so delete them once the release is
   accepted.
5. `install -m 0755 "$RELEASE" /usr/local/bin/heimdall`, then start the
   service. The first `run` applies the new schema steps.

Rollback: stop the service and reinstall the old binary. If the new release
added schema steps, the old binary refuses to start ("database schema N is
newer than this Heimdall"). In that case restore `heimdall.db` and
`durable.db` from the same backup, together, before starting. Messages and
replies stored after the backup are lost with it. Turns that a stop cut off
are reported, never re-run.

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
forces root's agent directory/key; implementing a restricted Pi launcher on
the existing installation/login is required before employee enablement. Never grant access to root credentials
or reuse the root-forcing wrapper for employees. Root instruction-file sharing
below does not authorize sharing secrets.

Default order is `codex → claude → pi`; preserve this in deployed config.

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
2. Keep `employee_harnesses: []` for this owner-only cutover. A separate non-root
   execution context and restricted native launches remain implementation work.
   Employee native validation is not a blocker for the owner-only switch, but
   current runtime still requires `run_as.employee` to exist before startup.
   In the approved live window, use `getent passwd <configured-user>`; if absent,
   create a system account with a private home and nologin shell, e.g.
   `useradd --system --create-home --home-dir /var/lib/heimdall-employee --shell /usr/sbin/nologin heimdall-employee`
   when that is the configured name. Verify `id -u <configured-user>` is neither
   root nor the owner's UID. No employee login, harness install or credentials.
   Do not create the account during work-clone preparation.
3. Verify the owner's existing Codex, Claude and Pi installations and logins.
   Employees never sign in; no employee login or credentials are required.
4. Before future employee enablement (not needed for this owner-only switch),
   expose the ONE hub policy and skills outside `/root`, without copying:
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
   employee-readable into `/opt/assistant/heimdall/`. Expose the administrator-owned Pi pack extensions subtree read-only at
   `/opt/assistant/pi/extensions`, including `.shared` access/identity helpers
   and its required read-only policy references. Employee Pi explicitly loads
   `privileged-tool-guard.ts` and `admin-lifecycle-guard.ts` from there;
   ambient extension discovery and native MCP are disabled. Retain the
   privileged-tool guard; employee extensions must contain no memory or
   owner business connectors. Employee native config and hook setup must
   be administrator-controlled. The Claude hook invokes the installed
   employee-readable Heimdall binary; a binary under `/root` cannot work.
5. Complete [owner Slack setup](../../docs/owner-slack-read.md) before enabling
   `owner_slack`; it stays false in staged config. Separate approval and native
   auth required; never connect live Slack during development.
6. Re-run offline check and harness discovery; confirm bot username, local API,
   scoped Pi launch, disabled employees and missing-token stripping.
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

## Dedicated Heimdall Infisical project (install before start)

Heimdall uses its own machine identity, created inside the Heimdall project
with the Viewer role (`prod`, path `/`), and loads ONLY that project, never Pi's.
(Infisical cannot assign the hub's project-scoped identity to another project.) Required keys: `TELEGRAM_BOT_TOKEN`,
`SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `ADMIN_USER_IDS`. Optional Pi-tool secrets
belong in this project only when required by the owner's Pi harness/tools.
Pi retains its own project and bot tokens for scheduler Web API delivery.

Heimdall has its OWN Infisical machine identity (Universal Auth, Viewer on the
Heimdall project only). It never reads the Pi hub's `/etc/pi-infisical*.env`.
Write `/etc/heimdall/infisical.env` without displaying the client secret: the
project ID and client ID are not secret; read the client secret from a private
file (e.g. a secret-request file), never from chat or shell history:

```bash
install -d -o root -g root -m 0700 /etc/heimdall
(umask 077; {
  printf 'INFISICAL_PROJECT_ID=%s\n' 'REPLACE_WITH_HEIMDALL_PROJECT_ID'
  printf 'INFISICAL_UNIVERSAL_AUTH_CLIENT_ID=%s\n' 'REPLACE_WITH_CLIENT_ID'
  printf 'INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET=%s\n' "$(cat /path/to/private-secret-file)"
} > /etc/heimdall/infisical.env)
chown root:root /etc/heimdall/infisical.env
chmod 0600 /etc/heimdall/infisical.env
install -o root -g root -m 0755 deploy/contabo/heimdall-infisical-run.sh /usr/local/sbin/heimdall-infisical-run.sh
install -o root -g root -m 0755 deploy/contabo/heimdall-run.sh /usr/local/sbin/heimdall-run.sh
install -o root -g root -m 0644 deploy/contabo/heimdall.service /etc/systemd/system/heimdall.service
stat -c '%U %a %n' /etc/heimdall/infisical.env
bash -n /usr/local/sbin/heimdall-infisical-run.sh
systemctl daemon-reload
```

The file holds exactly `INFISICAL_PROJECT_ID`, `INFISICAL_UNIVERSAL_AUTH_CLIENT_ID`
and `INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET` (unquoted, each once), root-owned
mode 0600. The wrapper parses it (never sources it), clears inherited Infisical
variables, and execs `/usr/local/lib/pi-infisical-exec.js` without printing values.
No authentication files are copied. Do not run the wrapper during preparation.
Employee business tools cannot use this machine identity as caller authority;
MEL-724 signed caller tokens remain unimplemented, so employees stay disabled.
