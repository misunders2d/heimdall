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
   Corporate mode matches shared workspace memory; obtain owner decision before
   changing to personal. No tokens in config.
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
3. Perform native harness logins manually as root and separately as employee.
   OMP absent; Codex, Claude, Pi required. Confirm per-user executables, homes,
   native automatic modes, MCP setup and restricted caller scope. No login copy.
4. With Sergey's go, make Pi AGENTS the sole instruction source for both users:
   root: `/root/.claude/CLAUDE.md`, `/root/.codex/AGENTS.md`, and
   `/root/.gemini/AGENTS.md` (AGY) symlink to
   `/root/.pi/agent/AGENTS.md`; employee: `~/.pi/agent/AGENTS.md` points to an
   approved readable instruction-only projection of that same canonical file,
   and `~/.claude/CLAUDE.md`, `~/.codex/AGENTS.md`, `~/.gemini/AGENTS.md`
   point to that employee Pi path.
   Preserve existing instruction files before replacement. Do not make `/root`
   traversable just to share instructions. AGY is outside launch order. Its
   [official global rule path](https://antigravity.google/docs/rules/) was
   checked 2026-11-01; verify installed version before enabling. Example commands
   (after backing up existing files), run as each target user:
   `mkdir -p ~/.claude ~/.codex ~/.gemini`;
   Use absolute symlink targets:
   `ln -s "$HOME/.pi/agent/AGENTS.md" "$HOME/.claude/CLAUDE.md"`,
   `ln -s "$HOME/.pi/agent/AGENTS.md" "$HOME/.codex/AGENTS.md"`,
   `ln -s "$HOME/.pi/agent/AGENTS.md" "$HOME/.gemini/AGENTS.md"`.
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
