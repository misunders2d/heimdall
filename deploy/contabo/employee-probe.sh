#!/bin/bash
# MEL-846 employee boundary probe, per engine, on the hub layout.
# Run as root from a reviewed work clone (never the production checkout's
# live state); it starts no Heimdall service, chat adapter or sign-in.
#
#   deploy/contabo/employee-probe.sh [engine ...]          # default: omp codex claude pi
#   PROBE_CALLER=<telegram id> PROBE_MCP_DATA=<data_dir> deploy/contabo/employee-probe.sh claude
#
# Part 1 (static, as heimdall-employee): runtimes load, owner homes and auth
# stores are unreadable, the employee home holds no owner login or memory hook.
# Part 2 (live, one real model turn per engine, started exactly as Heimdall
# starts an employee turn): no owner/connector/shell tools, owner credential
# reads denied, shared instructions readable, Mellanni only through the
# caller's own sign-in. Without PROBE_CALLER the synthetic caller has never
# signed in, so Mellanni tools must be absent. With PROBE_CALLER (an employee
# who sent `mcp login mellanni`) their own Mellanni must work.
# Exit status is non-zero on the first failed check. List an engine in
# employee_harnesses only after its probe passes (owner decision).
set -euo pipefail
emp=heimdall-employee
root=/opt/assistant
home=$(getent passwd "$emp" | cut -d: -f6)
repo=$(cd -- "$(dirname -- "$0")/../.." && pwd)
engines=("$@")
[[ ${#engines[@]} -gt 0 ]] || engines=(omp codex claude pi)
[[ $(id -u) == 0 && -n $home && $home != /root ]] || { echo "run as root with the $emp account present" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
as_emp() { runuser -u "$emp" -- "$@"; }

echo "== static checks as $emp"
for path in /root /root/.claude /root/.codex /root/.pi /root/.pi/agent /root/.omp /root/.local/share/memento; do
  [[ -e $path ]] || continue
  if as_emp test -r "$path" || as_emp test -x "$path"; then fail "$path is accessible to $emp"; fi
done
for path in runtime/node runtime/pi/package.json runtime/pi/dist/main.js runtime/pi/node_modules/jiti/package.json bin/pi; do
  as_emp test -r "$root/$path" || fail "$root/$path unreadable: rerun deploy/contabo/employee-layout.sh install"
done
for engine in "${engines[@]}"; do
  [[ $engine == pi ]] && continue
  as_emp test -x "$root/bin/$engine" || fail "$root/bin/$engine missing or not executable: rerun deploy/contabo/employee-layout.sh install"
done
# Owner logins and memory hooks never live in the employee home (leases are
# per-turn under ~/.heimdall/turn-agents and removed at process exit).
for path in .claude/.credentials.json .codex/auth.json .pi/agent/auth.json .omp/agent/agent.db .claude/settings.json .codex/config.toml .claude.json; do
  [[ -e $home/$path ]] && fail "$home/$path exists (owner login or ambient config in the employee home)"
done
# Config only: session stores may quote shared instructions that name Memento.
if grep -rqsi --exclude-dir=sessions --exclude-dir=heimdall-sessions --exclude-dir=projects --exclude='*.jsonl' memento "$home/.claude" "$home/.codex" "$home/.pi/agent" "$home/.omp" 2>/dev/null; then fail "memento referenced in employee native config"; fi
leftover=$(find "$home/.heimdall/turn-agents" "$home/.pi/turn-agents" -mindepth 1 -maxdepth 1 -mmin +30 2>/dev/null | wc -l)
[[ $leftover == 0 ]] || fail "$leftover stale employee lease directories older than 30 min (crash leftovers; inspect and remove)"
echo "static checks passed"

echo "== live checks"
cd "$repo"
for engine in "${engines[@]}"; do
  echo "-- $engine"
  HEIMDALL_EMPLOYEE_LIVE=1 HEIMDALL_EMPLOYEE_ENGINE=$engine MEMENTO_PROCESSING=1 \
    HEIMDALL_EMPLOYEE_PROBE_CALLER=${PROBE_CALLER:-} HEIMDALL_EMPLOYEE_MCP_DATA=${PROBE_MCP_DATA:-} \
    go test ./internal/harness -run '^TestEmployeeEngineLive$' -count=1 -v || fail "$engine live probe"
done
echo "all requested engines passed: ${engines[*]}"
