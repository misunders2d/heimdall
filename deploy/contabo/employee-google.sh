#!/bin/bash
# Deploy ONLY the reviewed employee launcher/helper/runtime payload. No services,
# config, credentials, mount layout or secret provisioning are changed here.
set -euo pipefail
mode=${1:?usage: employee-google.sh install RELEASE_DIR | undo BACKUP_DIR}
source_dir=${2:?release or backup directory required}
root=${HEIMDALL_EMPLOYEE_LAYOUT_ROOT:-/opt/assistant}
die() { echo "employee-google.sh: $*" >&2; exit 1; }
[[ $(id -u) == 0 ]] || die 'must run as root'
[[ $root == /* && ! -L $root && ! -L $source_dir ]] || die 'layout root or source dir is not a plain absolute path'
if pgrep -u heimdall-employee >/dev/null; then
 echo 'Employee processes are still running; drain them before installing or undoing.' >&2
 exit 1
fi
files=(pi-employee-cli.mjs employee-guard.ts google-runtime.mjs google-tools.mjs google-workspace.py)
for dir in "$root" "$root/heimdall" "$root/runtime"; do
 [[ -d $dir && ! -L $dir && $(stat -c %u "$dir") == 0 ]] || die "$dir must be a root-owned directory"
done
[[ ! -L $root/runtime/google ]] || die "$root/runtime/google must not be a symlink"
if [[ $mode == undo ]]; then
 [[ -f $source_dir/manifest && $(stat -c '%u:%a' "$source_dir") == 0:700 ]] || die 'backup dir must be root 0700 with a manifest'
 [[ ! -e $source_dir/scratch-test-only.txt ]] || die 'refusing a scratch/test rollback dir'
 grep -Fxq "root:$root" "$source_dir/manifest" || die "manifest was not recorded for layout root $root"
 for file in "${files[@]}"; do
  if [[ -e $source_dir/files/$file ]]; then
   install -m 0644 "$source_dir/files/$file" "$root/heimdall/$file"
  elif grep -Fxq "new-file:$file" "$source_dir/manifest"; then
   # Only remove files this exact install introduced, after no employee process remains.
   rm -f -- "$root/heimdall/$file"
  else echo 'Rollback manifest is incomplete' >&2; exit 1; fi
 done
 if [[ -d $source_dir/google ]]; then
  [[ ! -e $root/runtime/google ]] || mv -- "$root/runtime/google" "$source_dir/replaced-google-$(date -u +%Y%m%dT%H%M%S%N)"
  cp -a -- "$source_dir/google" "$root/runtime/google"
 elif grep -Fxq 'new-runtime:google' "$source_dir/manifest"; then
  # Preserve the replaced runtime for audit, rather than deleting it.
  [[ ! -e $root/runtime/google ]] || mv -- "$root/runtime/google" "$source_dir/replaced-google-$(date -u +%Y%m%dT%H%M%S%N)"
 else echo 'Rollback runtime manifest is incomplete' >&2; exit 1; fi
 echo 'Employee Google payload restored; no service/config/credentials changed.'
 exit
fi
[[ $mode == install ]] || die "unknown mode $mode"
(cd -- "$source_dir" && sha256sum --check payload.sha256)
backup=$source_dir/rollback-$(date -u +%Y%m%dT%H%M%S%N)
install -d -m 0700 "$backup" "$backup/files"
echo "root:$root" > "$backup/manifest"
for file in "${files[@]}"; do
 [[ -f $source_dir/payload/$file && ! -L $root/heimdall/$file ]] || die "payload $file missing or target is a symlink"
 if [[ -e $root/heimdall/$file ]]; then cp -a -- "$root/heimdall/$file" "$backup/files/$file"; else echo "new-file:$file" >> "$backup/manifest"; fi
done
if [[ -e $root/runtime/google ]]; then
 mv -- "$root/runtime/google" "$backup/google"
else echo 'new-runtime:google' >> "$backup/manifest"; fi
# Checksummed archive was created from a root-owned, access-only dependency venv.
# No credentials or employee state are included.
tar -xzf "$source_dir/google-runtime.tar.gz" -C "$root/runtime"
for file in "${files[@]}"; do install -m 0644 "$source_dir/payload/$file" "$root/heimdall/$file"; done
"$root/runtime/google/bin/python" -I -c 'import googleapiclient, google.oauth2.credentials; print("Google runtime import OK")'
echo "Employee Google payload installed; rollback directory: $backup"
echo 'No service/config/credentials changed.'
