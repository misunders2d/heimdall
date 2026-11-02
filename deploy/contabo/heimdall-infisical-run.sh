#!/usr/bin/env bash
set -euo pipefail
# Never enable tracing or print credentials.
# Heimdall uses its OWN Infisical machine identity and project; it never reads
# the Pi hub's /etc/pi-infisical*.env files.
unset INFISICAL_PROJECT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET
conf=/etc/heimdall/infisical.env
[[ -f "$conf" && ! -L "$conf" && "$(stat -c '%u:%a' "$conf")" == '0:600' ]] || {
  echo 'Heimdall Infisical file must be a root-owned regular file, mode 0600' >&2; exit 2;
}
# Parse, never source: exactly these three keys, each once.
declare -A seen=()
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" == \#* ]] && continue
  if [[ "$line" =~ ^(INFISICAL_PROJECT_ID|INFISICAL_UNIVERSAL_AUTH_CLIENT_ID)=([[:alnum:]-]+)$ ]] ||
     [[ "$line" =~ ^(INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET)=([^[:space:]]+)$ ]]; then
    [[ -z "${seen[${BASH_REMATCH[1]}]:-}" ]] || { echo "Duplicate key in $conf" >&2; exit 2; }
    seen[${BASH_REMATCH[1]}]=1
    printf -v "${BASH_REMATCH[1]}" '%s' "${BASH_REMATCH[2]}"
  else
    echo "Unexpected line in $conf (allowed: INFISICAL_PROJECT_ID, INFISICAL_UNIVERSAL_AUTH_CLIENT_ID, INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET)" >&2; exit 2
  fi
done < "$conf"
: "${INFISICAL_PROJECT_ID:?missing INFISICAL_PROJECT_ID in /etc/heimdall/infisical.env}"
: "${INFISICAL_UNIVERSAL_AUTH_CLIENT_ID:?missing client ID in /etc/heimdall/infisical.env}"
: "${INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET:?missing client secret in /etc/heimdall/infisical.env}"
export INFISICAL_PROJECT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET
exec /usr/local/lib/pi-infisical-exec.js -- "$@"
