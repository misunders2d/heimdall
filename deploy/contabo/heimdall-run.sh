#!/usr/bin/env bash
set -euo pipefail
# Invoked only after Infisical injection. Never enable shell tracing.
export HEIMDALL_SLACK_BOT_TOKEN="${SLACK_BOT_TOKEN:?SLACK_BOT_TOKEN missing}"
export HEIMDALL_SLACK_APP_TOKEN="${SLACK_APP_TOKEN:?SLACK_APP_TOKEN missing}"
export HEIMDALL_TELEGRAM_TOKEN="${TELEGRAM_BOT_TOKEN:?TELEGRAM_BOT_TOKEN missing}"
unset SLACK_BOT_TOKEN SLACK_APP_TOKEN TELEGRAM_BOT_TOKEN

# Match hub pi-vps-cli.sh: shared Infisical laptop defaults must not win.
export A2A_AGENT_ID=pi-remote-hub
export A2A_AGENT_NAME="Remote Pi Hub"
export A2A_HUB_URL=https://agents.bezosapp.uk
export A2A_AGENT_CAPABILITIES=remote-pi,hub,amazon,sp-api,ads,keepa,bigquery,google,scheduler,slack,telegram,a2a
export LOCAL_A2A_PRIVATE_KEY_FILE=/root/.config/pi-a2a/pi-remote-hub-ed25519-20260604.key
export LOCAL_A2A_KEY_ID=pi-remote-hub-ed25519-20260604
export LOCAL_A2A_PRINCIPAL=sergey@mellanni.com
export A2A_SIGNATURE_VERSION=2
export PI_CODING_AGENT_DIR=/root/.pi/agent
export PI_MEMORY_VECTOR_MODE=search
# Harness launch removes HEIMDALL_*; employee environment rebuilt separately.
exec /usr/local/bin/heimdall run -config /etc/heimdall/config.json
