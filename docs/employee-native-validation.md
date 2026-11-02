# Employee launch validation, 2026-11-02

Offline checks only. No model, authentication, chat connection, services or
production configuration changes. Temporary native homes contained no auth.

## Claude Code 2.1.316

Installed `claude --version` and `--help` confirm `--restricted`,
`--setting-sources`, `--permission-mode dontAsk`, `--tools`, `--add-dir`,
`--strict-mcp-config`, `--mcp-config`, `--disallowedTools` and `--settings`.
Help states restricted mode skips user/project/local settings (managed and
explicit settings still apply), and confines file tools to working directories.
The employee launch removes the owner's base `--permission-mode auto`.

Installed binary embedded schema (`strings`, command-hook schema containing
`onFailure:Za()`) explicitly describes `onFailure: "block"`: missing script or
plugin directory, timeout, exit other than 0/2, invalid JSON or schema failure
counts as exit 2 and blocks the guarded action. Contrary to review finding 4d,
this field is supported in this installed version. Retained, with a regression
assertion. Embedded schema also includes `disableSkillShellExecution`,
`autoMemoryEnabled` and `permissionDecision`. Its absolute-path handling contains
`a.startsWith("//")?a.slice(1):a`; deny paths now use `Read(//root/**)` and
`Read(//home/.../.claude/**)` etc. Reader unit tests cover symlink escapes and
malformed inputs. The installed hook executable must be administrator-owned,
0755, accessible outside the owner's home; failure blocks through `onFailure`.
No authenticated Claude turn or native hook execution was attempted. Real
employee skill-read/auth-denial probes remain required before enablement.

## Pi 1.4.0

Installed CLI help: `--no-extensions` disables discovery and built-ins, but
explicit `-e` paths still work. `--no-mcp` disables built-in MCP connections.
Source reference: installed `@earendil-works/pi-coding-agent/dist/bundle/cli.js`
and its CLI help. No MCP adapter is needed while employee MCP is disabled;
removed adapter-only `--mcp-config`. Explicitly retain the Pi pack privileged
and admin lifecycle guards (which import access-control and external identity
helpers), plus Heimdall's confined reader. No memory extension is allowlisted.

`TestNativePiExplicitExtensionsOnly` uses an isolated agent dir with a synthetic
ambient memory extension and explicit guard fixture. With `--offline
--no-extensions --no-mcp -e ... --help`, the explicit guard loads and ambient
memory extension does not. Generated launch flags have separate assertions.
Production pack guards and deployment mounts still require validation as the
employee account; fixture loading does not establish that gate.

## Codex 0.166.0

Installed `features list` recognizes every disabled feature in the employee
launch. `app-server --strict-config --print-startup-config` accepts the exact
generated employee policy, including disabled ambient MCP entries, using an
empty temporary `CODEX_HOME`. A deliberate unknown-key control is rejected.
`TestNativeEmployeeCodexConfig` reproduces the parser and feature-name checks.

Original per-path `-c permissions.employee.filesystem."/root"="deny"` was
rejected: `filesystem path '\"/root\"' must be absolute, use '~/...', or start
with ':'`. Corrected to one TOML `permissions.employee={...}` table with quoted
path keys inside the table. `default_permissions`, the named profile,
`filesystem` read/deny and `network.enabled=false` all parse. Keep this narrow
read allowlist rather than replace it with broad legacy read-only access.
The same dotted-key quoting bug also affected named MCP disable overrides.
These now use one TOML table; a synthetic ambient server with a dotted name
is verified disabled by the real `mcp list --json`, not just parser acceptance.
`--strict-config` now fails launch on unknown configuration. Owner flags stay
unchanged. No keys/features were silently ignored in the strict parser check.

Auth-free `codex sandbox -- /bin/echo probe` failed before executing:
`error building bubblewrap command: app-server socket directory must be a user-owned directory with mode 0700`.
Read/write/network enforcement and child-read-over-parent-deny behavior are
therefore **unverified** in this host environment. Do not enable employee Codex
until actual non-root probes pass; no credentials copied or account changed.

## Deferred review nits

10: retain deterministic sorted alias expansion; disjoint identity principals
are required. Changing chained-resolution semantics needs a separate contract.
15: AGY absent on this host; pinned laptop binary unavailable to this task.
No invented native assertion; employee AGY remains refused. Its owner launch
contract/version comment needs pinned laptop verification before changing.
