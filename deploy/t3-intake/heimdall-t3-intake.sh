#!/bin/sh
# Heimdall T3 intake hook, for Codex and Claude Code sessions run by T3.
#
#   heimdall-t3-intake.sh /ABSOLUTE/PATH/TO/heimdall /ABSOLUTE/PATH/TO/control.sock prompt|stop
#
# On prompt submit it calls Heimdall's scheduler control with task_intake,
# on stop with task_intake_done, passing only the T3 thread id (a pointer)
# and the hook event. It prints nothing, always exits 0 and gives up within
# 3 seconds. It sends no prompt text and no identity: Heimdall binds the
# owner from the trusted T3 process lineage and reads the request itself
# through the T3 CLI (createdBy=user). See README.md.
heimdall=$1
socket=$2
event=$3

# Drain the hook's JSON input; nothing in it is used.
timeout 0.3 cat >/dev/null 2>&1

case $event in
prompt) action=task_intake rid=prompt_submit ;;
stop) action=task_intake_done rid=stop ;;
*) exit 0 ;;
esac
thread=${T3CODE_THREAD_ID:-}
case $thread in
'' | mcp:launch:*) exit 0 ;; # no T3 thread, or an agent-delegated one
esac
# A pointer only: anything that would need JSON escaping is skipped.
printf '%s' "$thread" | grep -Eq '^[A-Za-z0-9:._-]{1,128}$' || exit 0
[ -n "$heimdall" ] && [ -n "$socket" ] || exit 0

printf '{"action":"%s","id":"%s","request_id":"%s"}\n' "$action" "$thread" "$rid" |
	timeout -k 0.3 2.2 "$heimdall" sched control -socket "$socket" >/dev/null 2>&1
exit 0
