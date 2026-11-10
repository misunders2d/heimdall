#!/usr/bin/env bash
# Real-engine release check (not CI). Sends one trivial prompt through each
# installed adapter (codex, claude, pi) with `heimdall local` and an explicit
# `!<engine>` (so fallback cannot hide a failure), and reports pass, fail or
# skipped per engine. It prints no message content: only whether the reply
# carried a random token. Run as the owner OS user of the release host, with
# the candidate binary:
#
#   deploy/contabo/engine-check.sh /path/to/heimdall [engine ...]
#
# ENGINE_TIMEOUT (seconds, default 300) bounds each engine. Scratch config
# and data live in a temporary directory removed on exit; each harness may
# still keep its own native session record in the user's home.
# MEMENTO_PROCESSING=1 keeps these turns out of memory capture.
# Not covered: owner_slack (Slack MCP) launches and their stderr checks.
# Exit status: 0 when every installed engine passed, 1 otherwise.
set -uo pipefail

bin=${1:-heimdall}
shift || true
engines=("$@")
[ ${#engines[@]} -eq 0 ] && engines=(codex claude pi)
limit=${ENGINE_TIMEOUT:-300}

if ! command -v "$bin" >/dev/null 2>&1; then
	echo "engine-check: heimdall binary not found: $bin" >&2
	exit 2
fi
employee=nobody
if ! id -u "$employee" >/dev/null 2>&1 || [ "$(id -u "$employee")" = "$(id -u)" ]; then
	echo "engine-check: needs an OS account '$employee' distinct from $(id -un) (config requires an employee account; it is never used)" >&2
	exit 2
fi

scratch=$(mktemp -d "${TMPDIR:-/tmp}/heimdall-engine-check.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
chmod 0700 "$scratch"
export MEMENTO_PROCESSING=1
unset ADMIN_USER_IDS

failed=0
echo "heimdall: $("$bin" version 2>/dev/null | head -1)"
for engine in "${engines[@]}"; do
	case $engine in codex | claude | pi) ;; *)
		echo "$engine: skipped (not a checked engine)"
		continue
		;;
	esac
	if ! command -v "$engine" >/dev/null 2>&1; then
		echo "$engine: skipped (not installed)"
		continue
	fi
	dir="$scratch/$engine"
	mkdir -m 0700 "$dir"
	cat >"$dir/config.json" <<EOF
{"data_dir": "$dir/data", "mode": "corporate", "standalone_access": true,
 "owners": ["local:owner"], "harness_order": ["$engine"], "max_parallel": 1,
 "run_as": {"owner": {"user": "$(id -un)", "dir": "$dir"}, "employee": {"user": "$employee"}}}
EOF
	token="HEIMDALL-CHECK-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
	start=$(date +%s)
	printf '!%s Reply with exactly this token and nothing else: %s\n' "$engine" "$token" |
		timeout --kill-after=30 "$limit" "$bin" local -config "$dir/config.json" -as owner \
			>"$dir/out" 2>"$dir/err"
	status=$?
	took=$(($(date +%s) - start))
	if grep -qF "$token" "$dir/out"; then
		echo "$engine: pass (${took}s)"
	elif [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
		echo "$engine: fail (no reply within ${limit}s)"
		failed=1
	else
		echo "$engine: fail (exit $status, reply without the token, ${took}s)"
		failed=1
	fi
done
exit $failed
