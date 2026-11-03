#!/bin/bash
# Inert employee dependencies only. Never starts/reloads Heimdall or chat adapters.
set -euo pipefail
mode=${1:-install}
repo=$(cd -- "$(dirname -- "$0")/../.." && pwd)
pack=/root/.pi/agent/git/github.com/misunders2d/misunderstood-pi-pack
node_root=/root/.nvm/versions/node/v22.23.4
root=/opt/assistant
emp_home=/var/lib/heimdall-employee
mounts=(AGENTS.md skills pi/extensions pi/extensions/.secrets pi/extensions/.shared/helium10-policy.ts pi/scripts/native-mcp-compat.mjs runtime/node runtime/pi)
if [[ $mode == undo ]]; then
  for ((i=${#mounts[@]}-1;i>=0;i--)); do
    target=$root/${mounts[i]}
    unit=$(systemd-escape --path --suffix=mount "$target")
    mountpoint -q "$target" && umount "$target"
    rm -f "/etc/systemd/system/local-fs.target.wants/$unit" "/etc/systemd/system/$unit"
  done
  # Preserve employee OAuth/transcripts and shared files for audit/rollback.
  echo 'Unmounted employee dependencies; files and employee state preserved.'
  exit
fi
[[ $mode == install && $(id -u) == 0 && $(id -u heimdall-employee) == 997 ]]
[[ ! -L $root ]]
install -d -m 0755 "$root" "$root/pi" "$root/pi/scripts" "$root/runtime" "$root/heimdall" "$root/bin"
bind_ro() {
 local source=$1 target=$root/$2 unit
 if [[ -d $source ]]; then [[ -d $target ]] || install -d -m 0755 "$target"; else [[ -e $target ]] || install -m 0644 /dev/null "$target"; fi
 unit=$(systemd-escape --path --suffix=mount "$target")
 if mountpoint -q "$target" && [[ $(stat -Lc '%d:%i' "$source") != $(stat -Lc '%d:%i' "$target") ]]; then umount "$target"; fi
 if ! mountpoint -q "$target"; then mount --bind "$source" "$target"; mount --make-private "$target"; mount -o remount,bind,ro,nosuid,nodev "$target"; fi
 mount --make-private "$target"
 [[ $(stat -Lc '%d:%i' "$source") == $(stat -Lc '%d:%i' "$target") ]]
 [[ $(findmnt -n -o VFS-OPTIONS --target "$target") == *ro* ]]
 cat > "/etc/systemd/system/$unit" <<UNIT
[Unit]
Description=Heimdall employee read-only dependency ($2)
Before=heimdall.service
[Mount]
What=$source
Where=$target
Type=none
Options=bind,ro,nosuid,nodev,private
[Install]
WantedBy=local-fs.target
UNIT
 install -d -m 0755 /etc/systemd/system/local-fs.target.wants
 ln -sfn "../$unit" "/etc/systemd/system/local-fs.target.wants/$unit"
}
bind_ro "$(readlink -f /root/.pi/agent/AGENTS.md)" AGENTS.md
bind_ro "$(readlink -f /root/.agents/skills)" skills
bind_ro "$pack/extensions" pi/extensions
install -d -m 0755 "$root/heimdall/empty"
bind_ro "$root/heimdall/empty" pi/extensions/.secrets
# Public guard code only; keep live 0600 source permissions unchanged.
install -m 0644 "$pack/extensions/.shared/helium10-policy.ts" "$root/heimdall/helium10-policy.ts"
bind_ro "$root/heimdall/helium10-policy.ts" pi/extensions/.shared/helium10-policy.ts
bind_ro "$pack/scripts/native-mcp-compat.mjs" pi/scripts/native-mcp-compat.mjs
bind_ro "$node_root/bin/node" runtime/node
bind_ro "$node_root/lib/node_modules/@earendil-works/pi-coding-agent" runtime/pi
# One guard dependency is mode 0600 in the live pack. Expose only that reviewed
# source file; do not chmod the live checkout or expose the .secrets subtree.
install -m 0644 "$repo/deploy/employee/employee-guard.ts" "$repo/deploy/employee/pi-employee-cli.mjs" "$root/heimdall/"
install -m 0755 "$repo/deploy/employee/pi" "$root/bin/pi"
# Existing account was provisioned with nobody ownership; account stays uid 997.
chown heimdall-employee:heimdall-employee "$emp_home"
chmod 0700 "$emp_home"
for path in .pi .pi/agent .agents .codex .claude .gemini .gemini/config; do
 install -d -o heimdall-employee -g heimdall-employee -m 0700 "$emp_home/$path"
done
link_new() {
 if [[ -e $2 || -L $2 ]]; then [[ $(readlink "$2") == "$1" ]]; else ln -s "$1" "$2"; chown -h heimdall-employee:heimdall-employee "$2"; fi
}
for path in .pi/agent/AGENTS.md .codex/AGENTS.md .claude/CLAUDE.md .gemini/GEMINI.md; do link_new "$root/AGENTS.md" "$emp_home/$path"; done
for path in .agents/skills .claude/skills .gemini/config/skills; do link_new "$root/skills" "$emp_home/$path"; done
echo 'Employee layout installed. No services reloaded/started. Mount units take effect on reboot.'
