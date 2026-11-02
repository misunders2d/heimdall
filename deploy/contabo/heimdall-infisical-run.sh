#!/usr/bin/env bash
set -euo pipefail
# Never enable tracing or print credentials. Reuse hub machine identity only.
[[ -f /etc/pi-infisical-machine.env ]] || { echo 'Missing hub machine identity file' >&2; exit 2; }
[[ ! -f /etc/pi-infisical.env ]] || . /etc/pi-infisical.env
# shellcheck disable=SC1091
. /etc/pi-infisical-machine.env
# Fail closed: never fall back to the Pi project from either sourced file.
unset INFISICAL_PROJECT_ID
selector=/etc/heimdall/infisical.env
[[ -f "$selector" && ! -L "$selector" && "$(stat -c '%u:%a' "$selector")" == '0:600' ]] || {
  echo 'Heimdall project selector must be a root-owned regular file, mode 0600' >&2; exit 2;
}
mapfile -t project_lines < "$selector"
[[ ${#project_lines[@]} == 1 && "${project_lines[0]}" =~ ^INFISICAL_PROJECT_ID=([[:alnum:]-]+)$ ]] || {
  echo 'Heimdall project selector must contain only INFISICAL_PROJECT_ID' >&2; exit 2;
}
INFISICAL_PROJECT_ID=${BASH_REMATCH[1]}
: "${INFISICAL_UNIVERSAL_AUTH_CLIENT_ID:?missing machine identity client ID}"
: "${INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET:?missing machine identity client secret}"
export INFISICAL_PROJECT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET
exec /usr/local/lib/pi-infisical-exec.js -- "$@"
