# T3 intake hooks

Every owner request typed into a T3-run Codex or Claude Code session becomes
a tracked Heimdall task (`T3-<n>`), without any change to T3. Nothing here is
installed by Heimdall; the owner installs it by hand.

## What happens

- **Prompt submit** → `heimdall-t3-intake.sh … prompt` sends
  `{"action":"task_intake","id":"<T3CODE_THREAD_ID>","request_id":"prompt_submit"}`
  to `heimdall sched control`.
- **Stop** → `… stop` sends `task_intake_done` for the same thread.
- The script prints nothing, always exits 0 and gives up within 3 s. It sends
  only the thread id, never prompt text or identity. Threads named
  `mcp:launch:*` (delegated by an agent) are skipped.

Heimdall then:

- accepts the call only from the configured `scheduler.control.trusted_t3`
  launcher lineage, bound to its owner (see `docs/external-origin.md`);
- reads the thread itself with the configured `durable.t3_command`
  (`thread show <id> --json --messages 50`) and takes the latest message with
  `createdBy: "user"`. When no message carries a mark (t3 v0.1.5), the latest
  user message counts if the thread itself is `createdBy: "user"` (weaker; the
  task's brief says so). If the CLI is missing or fails, or neither mark
  applies, the request is refused with one metadata-only log line; prompt
  text and environment are never used instead. One read may take up to 90 s
  (in the durable probe, not the hook): a large thread (about 9,000 items)
  takes about 20 s to show, whatever `--messages` asks;
- stores one `T3-<n>` per (thread, message, run) in `durable.db`; a repeat is
  ignored, a newer request in the same thread supersedes the older one;
- tracks it silently: it closes quietly when the run completes or is
  cancelled or rolled back, and becomes an ordinary assistant task (with a "check what was
  already done" brief) when the run ends interrupted or failed;
- `!work list` hides closed T3 rows; `!work show T3-<n>` shows the thread and run.

## Install (by hand)

1. Copy `heimdall-t3-intake.sh` somewhere the T3 user can execute.
2. Replace `/ABS/PATH/…` in the snippets with the script, the `heimdall`
   binary and the scheduler control socket.
3. **Claude Code**: merge `claude-settings.json` into that user's
   `~/.claude/settings.json`.
4. **Codex**: save `codex-hooks.json` as `~/.codex/hooks.json`, merge
   `codex-config.toml` into `~/.codex/config.toml`, then trust the hooks once.

## Known gaps

- t3 v0.1.5 `thread show --json` marks only the thread with `createdBy`, not
  each message, so intake there relies on the thread mark (an agent message in
  an owner thread counts as the owner's) until T3 exposes a per-message mark
  (and ideally a message id; without one dedupe is per thread + run).
- Not checked live: that Codex runs these hooks under T3's app-server mode,
  and that `T3CODE_THREAD_ID` reaches the hook environment of either harness.
- A hook fired while Heimdall is down is lost; nothing replays it.
- T3 sessions local to a laptop (not under the trusted T3 launcher) are out of
  scope.
