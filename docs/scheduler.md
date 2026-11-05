# Scheduler and watches

Design of record for phase 3. Approved by the owner 2026-11-03, decisions in §12.
Build: phase 3a (scheduler, natural-language drafts, Pi scheduler-tool switch), then phase 3b (watches).
Code references in Heimdall are to `main` 4f3f7e8, the phase 3a build base (§13); §17 records how 3a was built. Pi references are to the hub pack, `origin/hub` 956955e.

## 1. What it does

Heimdall runs jobs on a schedule, as the person who created them. It delivers each result where the job says:
- a Telegram chat;
- a Slack channel or thread;
- for owner jobs, an AgentNet address that the agent sends to itself.

Watches check a source cheaply and wake an agent only when something changed.

Principles:
- **Thin.**
  - A job run is an ordinary router turn with `Origin=scheduled` (already modeled: `internal/core/core.go:164-168`, `internal/router/router.go:713-720,1543`).
  - The router keeps doing identity, harness choice, sessions, delivery and crash recovery.
  - The scheduler adds a store, a due loop, commands and notices.
- **Runs as its creator.** The creator's role, capabilities and mode are evaluated at every fire, never only at creation.
- **Never twice.** Enqueue is exactly-once per scheduled slot. Delivery is at-least-once: one duplicate message is possible after a crash, and Heimdall never claims exactly-once.
- **The model proposes only when asked.** Direct owner person turns apply new jobs and edits with the reply in one transaction. Non-owners, removals and jobs with `rerun_on_timeout` enabled still require confirmation. Scheduled turns never apply blocks; MCP controls never auto-apply.
- **No new service.** One new dependency, `adhocore/gronx`, for cron parsing; the IANA zone data is stdlib `time/tzdata`.
- **Deterministic detection.** Watches use scripts and cursors; the LLM runs only on a match.
- **Long work goes elsewhere.** Multi-hour work is delegated to laptops through T3. A job only starts it, and a watch tracks it.

## 2. Jobs

### 2.1 Kinds

| Kind | Runs | Delivers |
|---|---|---|
| `agent` | a harness turn with the job's prompt | the answer, plus files in `out/` |
| `notify` | nothing; fixed text | the text |
| `watch` (3b) | a checker script or an inbound-event rule | on a match: a fixed notice, or an agent turn |

### 2.2 Definition

One JSON object, used by `!job add`, `!job edit` and model drafts (§5). The same validator checks all three.

```json
{
  "name": "Weekly summary",
  "kind": "agent",
  "schedule": {"cron": ["0 9 * * MON"]},
  "tz": "Area/City",
  "to": "here",
  "prompt": "…",
  "harness": "",
  "quiet": false,
  "grace": "2h",
  "until": "",
  "expect": {"tools": [], "files": 0, "prefix": ""},
  "overlap": "skip",
  "session": "fresh",
  "idempotent": false,
  "requires": [],
  "timeout": "20m"
}
```

| Field | Values and meaning |
|---|---|
| `schedule` | `{"cron": [...]}`: one or more 5-field specs, unioned. `{"every": "15m"}`: fixed duration from creation, UTC arithmetic; minimum 1 min owner, 1 h employee. `{"every": "2w", "at": "12:15", "from": "2026-11-14"}`: N days or weeks, wall-clock stable in `tz`. `{"at": "2026-12-05T08:00"}`: one-off, local time in `tz`. |
| `tz` | IANA zone name. Default `scheduler.default_tz`. |
| `to` | Delivery target (§2.3). Default `here`. |
| `harness` | Empty: the creator-bound source conversation's current `!use` selection at turn start, or the default order when unset; fallback only before the ready signal. A name: that harness only, no fallback (AGENTS.md "Harness choice"). |
| `quiet` | If the answer is exactly `NO_UPDATE`, nothing is sent and the run is `quiet`. |
| `grace` | How late a slot may start. Defaults: 2 h recurring, 24 h one-off. |
| `until` | Expiry date or duration. Optional for jobs, required for watches. |
| `expect` | Deterministic result checks (§3.8). |
| `overlap` | `skip` (default) or `queue_one` (§3.4). |
| `session` | `fresh` (default): a new native session each run. `job`: resume the job's stored native transcript, only if its last run ended cleanly. No process stays resident either way. |
| `idempotent` | Owner declares the job safe to repeat; an interrupted run may re-run once (§7.2). |
| `history_ctx` | Deferred (§17): the key is refused for now. Planned: the job's last 3 delivered chat replies (≤8 KB) as untrusted context, for deduplication. |
| `requires` | Hint only, e.g. `["mellanni"]`, `["google"]`. Picks the sign-in notice text (§4). Heimdall never reads credential stores. |
| `timeout` | Turn timeout. Default `harness.run_timeout_seconds`; owner maximum 2 h. |
| `paused` | Creation only (`!job add` or a draft): the job is created paused, by its creator (§7.5). Never stored; edits refuse it. |

### 2.3 Delivery targets

| `to` | Who may use it | Meaning |
|---|---|---|
| `here` | everyone | The conversation the job was created in (Slack: its root thread). |
| `tg:<chat>` | owner | A Telegram chat the bot is in. |
| `slack:<channel>`, `slack:<channel>/<thread_ts>` | owner | A Slack channel (each run a new root message) or a thread. The bot must be a member. |
| `agentnet:<address>` | owner | Heimdall never talks to AgentNet. Its run prompt says: "Deliver the result with the `agentnet` CLI to `<address>`. End your answer with `AGENTNET_SENT <message-id>`." The agent sends it; Heimdall checks only the marker. On success Heimdall posts nothing to chat (run `ok`, shown in `!job history`). If the marker is missing, the run is `unverified` and a notice goes to the creator. The marker is the agent's claim; AgentNet delivery proves storage only. Refused for employees, whose turns have no shell. |

The target is captured at creation. Edits may change it, within the same role rules.

**Notice chat.** Failure, missed, expiry and pause notices go to the target and to the creator's notice chat. The notice chat is:
1. the private chat the job was created in;
2. otherwise the creator's private chat with the bot, if Heimdall has one on record;
3. otherwise the conversation the job was created in (fixed text only).

### 2.4 Schedules, time zones, DST

- Cron is parsed by gronx.
  - Day-of-month and day-of-week combine with OR, as in Vixie cron.
  - Names, ranges, steps and lists work; so do `L`, `W` and `#`.
  - Next fire comes from `NextTickAfter`.
- Every job has an IANA zone. The binary embeds tzdata, so the host's zone files don't matter.
- **DST rule** (cronie cron(8)):
  - A job with a fixed minute and hour that falls in the skipped hour runs once, right after the change, at the first valid wall minute.
  - In the repeated hour, a fixed-time job runs once, at the first occurrence.
  - Frequent jobs (hour field `*` or a step) run normally. Minutes in the gap don't exist and are not made up; the repeated hour gives one run per local slot.
- **How**:
  - The run key is the local wall-clock minute in the job's zone (`2026-11-04T09:05`). The second pass through a repeated hour conflicts on `UNIQUE(job_id, slot)` and is dropped.
  - For the gap: when the UTC offset jumps between the previous slot and gronx's next time, the wrapper checks the gap's wall minutes with gronx `IsDue`. A fixed-time match gets one slot at the gap's end.
  - Neither cron library handles DST, and `time.Date` picks either zone during a transition, so the wrapper owns this.
- Creation warns when a fixed time falls inside that zone's DST transition window.
- Illustration: Pi is UTC-only, so some jobs today carry two UTC specs to hedge DST. Here one spec in the job's zone does the same.

### 2.5 Limits by role

| | Owner | Employee |
|---|---|---|
| Targets | `here`, or any chat the bot is in; `agentnet:` | `here`, or own private chat |
| Harness | any | `employee_harnesses` only |
| Minimum interval | 1 min | 1 h |
| Jobs | no cap | ≤10 active |
| Checkers (3b) | yes | no (v1) |
| `agentnet:` target | yes | no |
| `timeout` | ≤2 h | default only |

Memory is the same for every role: scheduled runs never capture memory (§3.7).

## 3. Runs

### 3.1 Due loop

One goroutine. It sleeps until the earliest `next_due`, or at most 60 s, and wakes early on any job change. Each wake writes `tick_at`.

### 3.2 Creating a run

One transaction:
1. Insert `job_runs(job_id, slot)`. A conflict ends the transaction with no effect.
2. Advance `jobs.next_due` to the spec's next slot after this one. Computing from the spec, not from now, avoids drift.
3. Insert the inbound row: `Origin=scheduled`, `From` = creator, conversation `sched:<job_id>`, `MsgID=run:<run_id>`. Inbound `UNIQUE(iface, chat, msg_id)` is a second guard.

### 3.3 Missed slots

- Lateness up to `grace`: the run starts once, late.
- Lateness beyond `grace`: the run is `missed`, with one combined notice per job (for example "3 runs missed since <time>").
- After downtime, only the newest missed slot is considered.

### 3.4 Overlap

Each job has its own conversation `sched:<job_id>`, and the router already runs one turn at a time per conversation.
- `skip` (default): if a run is pending or running, the slot becomes `skipped`. History shows it; no notice unless it happens 3 times in a row.
- `queue_one`: at most one run waits.

### 3.5 Concurrency and jitter

- Concurrency:
  - Scheduled turns use the router's `max_parallel` slots, capped by `scheduler.max_concurrent` (default 2). Live chat therefore always has at least `max_parallel − 2` slots.
  - One running run per creator. Waiting runs start in `due_at` order.
  - Checkers (3b) have their own cap (2) and take no turn slot.
- Jitter: a fixed offset per job, hash(job_id) mod `jitter_s`, always before the next slot.
  - Default 0 for agent and notify jobs.
  - Default 10% of the interval, at most 5 min, for watches.

### 3.6 Execution

The router handles the inbound row like any other message, with these differences for scheduled turns:
- **Identity and access.**
  - Access is `Access.Decide(creator, target conversation)`.
  - Slack creators are re-verified live each time the job fires (§4).
- **Delivery target.** The turn carries the job's target. Replies and `out/` files are enqueued to the target, not to `sched:<job_id>`.
- **No progress output.** No typing, status or progress messages, so quiet jobs stay silent.
- **Session and process.**
  - `session=fresh`: a new native session each run.
  - The warm worker is discarded when the run ends, so scheduled runs never take `max_warm` from chats.
- **Private chat flag.** `Private=false`: no MCP sign-in, and never `owner_slack` (AGENTS.md).
- **Environment.** The normal identity mapping (`internal/harness/identity.go`), plus:
  - `PI_EXTERNAL_DEFAULT_DELIVERY` = the target, in hub format;
  - `MEMENTO_PROCESSING=1`;
  - `PI_SCHEDULER_TOOLS=0` on Pi and OMP turns while the scheduler is enabled (§11).
- **Frame.** "scheduled job <name> created by this caller" (`router.go:1543`), and the AgentNet instruction when the target is `agentnet:`.

### 3.7 Memory

Scheduled runs never capture memory (`MEMENTO_PROCESSING=1`). There is no per-job opt-in.

### 3.8 Retries and result checks

- **Retries.** Only failures before the turn's ready signal are retried. That is the same no-side-effects boundary as harness fallback.
  - Backoff 1 min, 5 min, 15 min (±20%), at most 3 attempts, all within `grace`.
  - Timeout and worker lifetime limits fail without retry, even before ready, unless the explicit repeat-safe timeout option below applies. A turn may have produced effects without acknowledging readiness.
  - Owner agent jobs may set `"rerun_on_timeout":true` together with `"idempotent":true`, explicit `timeout`, `session:"fresh"`, and a platform target (never AgentNet). This asserts that repeating the entire prompt and its effects is safe. It is a rerun, not checkpoint continuation or exactly-once execution.
  - At most one timeout rerun uses the existing run/slot and durable replay flag. The first execution keeps its configured timeout within ordinary grace, even if queued. A rerun must finish within twice `timeout` from the slot due time, also constrained by grace; native interruption/termination can add shutdown grace. The old native process exits before retry is persisted. No native interrupted session is resumed.
  - Explicit stop, pause and deletion fence pending retries transactionally. Confirmed removal of the opt-in cancels pending timeout retries. Restart discards timeout retries with a notice; ordinary pre-ready recovery of a first execution stays unchanged. Creator admission and role are checked again at dispatch and turn start. Auth/approval failures and lifetime expiry never trigger a timeout rerun. A repeated hang exhausts the single retry; no semantic progress is inferred.
  - After the ready signal, nothing is replayed except that explicit repeat-safe timeout option.
- **Always checked:** the answer is not empty (unless it is the quiet token), and the turn ended at the harness's native result event.
- **Optional `expect` checks:**
  - `tools`: name patterns matched against native tool telemetry (`internal/harness/progress.go`). Telemetry carries names only, so a CLI run from the shell shows up only as the shell tool; use `files` or a checker for those.
  - `files`: at least N files in `out/`.
  - `prefix`: the answer starts with this text.
- **A failed check** makes the run `unverified`, and it is not retried.
  - Private target: the answer is delivered under a fixed first line, `⚠ unverified: <check>`.
  - Shared target: the answer is withheld; the target gets the fixed failure notice; the answer goes to the creator's notice chat.

### 3.9 States and end of run

States: `pending`, `running`, `ok`, `quiet`, `failed`, `unverified`, `skipped`, `missed`, `interrupted`, `cancelled`.

- A one-off ends as `done`.
- An answer containing the line `[job: done]` ends that job after this run. It works only for the job's own future runs, and is parsed like the existing `[react: …]` lines.
- `pause_after` (3) runs in a row that end `failed` or `unverified` pause the job automatically, with a notice.

### 3.10 Notices

- Fixed templates only:
  - job name and id;
  - slot, in local time and UTC;
  - phase and cause category;
  - attempts;
  - next run;
  - `!job show <id>`.
- They end with `— heimdall`.
- Model text never reaches a shared channel in a notice.
- Failure, interrupted and missed notices go to the target and to the creator's notice chat. Pause and expiry notices go to the creator's notice chat.

## 4. Identity and privileges

- A job stores its creator's principal key (`platform:id`), plus the Slack user ID for Slack creators.
- At every fire:
  - **Slack creators are re-verified live** with users.info: the live email must equal the stored one, and the account must not be deleted, not be a bot, and be in the bot's team. Remembered emails never count.
    - A temporary error is retried before the ready signal.
    - A permanent failure fails closed.
  - `Access.Decide` gives role, capabilities and admission. A creator who has been removed, blacklisted or can't be verified gets the run rejected, the job paused, and an owner notice (the creator too, if reachable). No other identity is ever used.
- **Owner jobs** run with the owner's native harness installs and automatic modes.
- **Employee jobs** run as normal employee turns:
  - the employee OS account;
  - the per-turn access-only harness token;
  - the employee's own sign-in stores;
  - `employee_harnesses` only;
  - Memento always off, no `owner_slack`.
- **Missing or expired sign-in.**
  - The turn runs and the service call fails, so the turn ends `failed` or `needs_human` before any side effect. A tool that reports the missing sign-in as a structured `auth_needed` result fails the run too, whatever the model then answers (§17).
  - The creator's notice: "send `google login` (Google) or `mcp login` (other services) in your private chat, then `!job resume <id>` — heimdall". The structured result names the service; otherwise `requires` only picks this text.
  - At most `pause_after` turns are spent before the job pauses itself.
  - Heimdall never inspects credential stores and never falls back to another identity.
- **Visibility.**
  - The owner sees all jobs (`!jobs all`); everyone else sees their own.
  - In shared chats, lists show names only. Prompts are shown only in private chats; there only a draft's card previews one (§5.3).
- **`PI_RESTRICTED_EXTERNAL_USER`** stays `0` for owner turns and `1` for every other turn (`docs/hub-parity.md`). Its only scheduler effect was in Pi's scheduler tools, which §11 switches off for Heimdall turns.

## 5. Natural-language drafts

People ask in plain language ("remind me every Monday at 9 to …"). The harness proposes a block only when asked. Direct owner requests for new jobs or edits apply immediately; other requests use a draft and confirmation.

1. **The block.** The model ends its answer with a fenced block:
   ```` ```heimdall-job ```` then a job definition (§2.2), or `{"edit": "<job id>", …changed fields}`, or `{"remove":"<job id>"}`, then ```` ``` ````.
   At most one block per answer. This is a directive with no file path, like the `[react: …]` lines, so the outbox rule that Heimdall never sends a path the model names still holds.
2. **Strip and validate.** Heimdall removes the block from the delivered answer and validates it with the same validator as `!job add`, under the caller's role rules. Removal checks the same job authority as `!job rm`; unknown or unauthorized IDs produce an error and change nothing.
3. **Apply or ask.** On direct owner person turns (not scheduled, MCP control or ambient), valid new jobs and edits without `rerun_on_timeout` apply through the same confirmation checks and `confirmDraft`, atomically with the reply. No draft remains. A signed one-line notice gives the job ID, active/paused state, name, schedule and zone, target when elsewhere, next run and `!job rm` command; edits name changed fields. The running scheduler is woken immediately. Existing cap rules remain: owners uncapped, employees at most ten active/paused jobs.

   For non-owners, removal, or any resulting definition with `rerun_on_timeout` enabled, Heimdall stores a `draft` that expires after 24 h, and posts a card showing:
   - the schedule in words (intervals such as `every 5m`, cron expressions retained, plain dates for one-offs), its timezone, and only the next run in local time and UTC;
   - the target, and the conversation the draft was made in;
   - the harness and kind;
   - the prompt or notification text: at most a 200-character preview everywhere, followed by `Full prompt: !job show <id>`; private `!job show` keeps showing the full content, shared-chat privacy is unchanged;
   - for an edit, a field-by-field diff;
   - `!job confirm <id>` and `!job discard <id>`.
   The short card fits one Slack message and ends `— heimdall`.
4. **Confirm.** Only the same verified caller can confirm, in the same conversation, within 24 h; confirm from anyone or anywhere else is rejected. Confirming makes a new job `active`, applies an edit, or removes the named job. Removal cards show the target job ID, name, schedule and next run. Confirmation rechecks current removal authority and uses the same running-run stop and retry fencing as `!job rm`. Discard leaves the target untouched. Removal drafts use the existing 24-hour expiry and caller/conversation binding.
   - A whole reply of `yes`, `ok`, `confirm`, `да` or `так` (surrounding whitespace and trailing Unicode punctuation ignored, any case; e.g. `yes.`, `Yes!`, `ok.`, `да.`, `no.`) from that caller in that conversation confirms when exactly one of their drafts waits there; `no`, `cancel`, `нет` or `ні` discards it. Longer replies such as "yes, but maybe in a separate folder" never confirm. With two or more, Heimdall asks for the ID. Heimdall matches these words itself; the model never confirms. In shared chats the reply needs addressing like any message.
5. **Invalid block, multiple blocks or cap exceeded.** One line names the error; nothing is stored.
6. **Blocks from scheduled turns are ignored**, so scheduled jobs never propose creating, editing or removing jobs. `[job: done]` still works.
7. **Telling the model.** A role-specific note goes into the frame of the first message of each native session (not into scheduled turns). It describes the block format. Owners are told to propose only when asked, that new jobs and edits apply at once, and that removal and `rerun_on_timeout` still ask. Non-owners retain the confirmation wording.

This replaces Pi's model-called `schedule_*` tools. With those tools, a prompt-injected model could schedule directly (`extensions/scheduler.ts:1128-1170`).

## 6. Watches (phase 3b)

There is exactly one Slack Socket Mode consumer and one Telegram poller: Heimdall's. A second consumer would steal events. Slack may send any event to any of up to 10 connections, and on Telegram a higher getUpdates offset confirms and forgets earlier updates.

### 6.1 Event watch

- **Rule:** `{"on": {"iface": "slack", "chats": ["<channel>"], "thread": "", "from": ["slack:<email>"], "match": ["…"]}}`. `match` is a list of substrings, or one bounded RE2 pattern.
- **Where it is evaluated:** after identity verification, against the messages Heimdall stores, addressed or ambient. Dedupe key: `iface/chat/msg_id`.
- **What it can see:** only what Heimdall admits — verified same-workspace humans, and allowlisted message subtypes.
  - Bot and integration posts are invisible, and so are channels the bot is not in.
  - Invite the bot, or use a poll watch.

### 6.2 Poll watch

- **Checker.** `{"check": {"checker": "<name>", "params": {…}}}`. `<name>` must appear in config `scheduler.checkers`, as an owner-installed executable with its argv and timeout. It can never come from chat.
- **Runtime.**
  - Runs as the creator's OS user, with a cleaned environment plus the identity variables, never Heimdall's own `HEIMDALL_*` variables (§17).
  - Timeout 60 s by default; stdout capped at 256 KiB.
  - Owner jobs only in v1.
- **Contract.**
  - stdin: `{"cursor": <opaque|null>, "params": {…}}`.
  - stdout: `{"cursor": <opaque>, "events": [{"key", "title", "body" (≤4 KB), "url"?}]}`, exit 0.
  - A non-zero exit, bad JSON or a timeout is a failed check, and the cursor is kept. 3 failures in a row send a creator notice.
- **State.**
  - The new cursor and the seen keys are committed in the same transaction that creates the wake run.
  - The first run only records a baseline and raises no events.
  - Events are deduped by key against `watch_seen` (1000 keys or 30 days).

### 6.3 Actions and lifecycle

- **`notify`:** a fixed template listing the event titles and URLs. No LLM.
- **`agent`:** one turn as the creator, with the events framed as untrusted data plus the watch's prompt.
  - Events arriving within 2 min are batched into one turn.
  - At most 4 turns per hour per watch; anything beyond that is reported as a count.
- **Expiry.**
  - `until` is required: default 7 days, at most 90 days.
  - A warning goes out 24 h before expiry; `!job renew <id> [duration]` extends it.
- **Stops:** `until`, `max_events`, `!job stop` / `!job rm`, or a `[job: done]` line in an agent answer.
- **Health:** each watch has an expected check period. Missing it shows in `heimdall sched health` and sends one notice.

### 6.4 Source notes

- **Gmail.**
  - The checker calls `users.history.list` from the stored `historyId`.
  - An HTTP 404 means a full resync: a new baseline plus one "gap" event. History IDs are typically valid for at least a week, but sometimes only hours.
  - About 6 s per call through the owner's CLI is fine at intervals of 5 min or more.
  - Push through Pub/Sub is deferred. It needs a topic, publish rights for Gmail's service account, and renewal at least every 7 days.
- **Slack channels without the bot:** prefer inviting the bot. Internal customer-built apps keep their existing rate limits; the 2025 limits apply only to apps distributed outside the Marketplace.
- **Long work:** an agent job delegates to a laptop through T3 and returns a task id. A poll watch with a status checker reports completion.

### 6.5 Illustration: monitor shapes

Illustration only. Existing Pi jobs are recreated by hand after the scheduler ships; nothing here decides how.

| Monitor shape | Capability it maps to |
|---|---|
| Product price monitor, every 2 h (Pi: an LLM turn every fire, 84 a week) | poll watch; an LLM turn only on a change |
| A weekly "is this monitor still needed" reminder | the watch expiry warning |
| An employee's recurring account check | quiet employee job: runs as the employee's own turn, sends only when the answer is not `NO_UPDATE`, notifies and auto-pauses on missing sign-in |

## 7. Reliability

### 7.1 Never twice

- One Heimdall process, holding the data-dir lock. No leader election.
- `UNIQUE(job_id, slot)` plus inbound `UNIQUE(msg_id)`, written in one transaction with `next_due`. A clock step backwards hits the same slot.
- Manual runs use the slot `manual:<unix-ms>`.

### 7.2 Restart and crash

- **Interrupted mid-turn.** Startup `Recover` (`internal/store/store.go:674`) marks the turn `interrupted`.
  - The run becomes `interrupted` and a notice goes out. It is never re-run.
  - Exceptions:
    - Interrupted before the ready signal: queued again.
    - `idempotent: true`: one re-run within `grace`.
- **Not yet started:** a run that never started goes ahead after the restart if it is still within `grace`; otherwise it is `missed`.
- **Persisted reply:** an outbound row persisted before the crash is still delivered (at-least-once).
- No lease timeouts: the process owns its runs, and startup recovery covers process death.

### 7.3 Drain and health

- **`heimdall drain [--timeout 30m]`**
  - Sets `hold_until`, so no new scheduled run starts; live chat is unaffected. The hold also applies where a run's turn starts: a run handed to the router earlier waits again (§17).
  - Waits until no turn is running, no run handed to the router is waiting for its turn, and the outbox is empty (`Router.Idle` semantics, read from the database).
  - Exits 0 when idle, 75 on timeout.
  - The hold clears at startup; slots missed during the hold follow `grace`.
  - Production restarts run `drain` first. It replaces Pi's restart guard.
- **`heimdall sched health`** prints JSON and exits 0 or 1. It reports:
  - a scheduler tick older than 3 min;
  - a run past its timeout;
  - an active job overdue beyond `grace`;
  - paused jobs and their reasons;
  - failing checkers and stale watches;
  - failures open longer than 24 h.

### 7.4 Coexistence with the Pi hub during migration

The Pi hub runs with its chat adapters off, so its own scheduler only sends: Slack `chat.postMessage` and Telegram `sendMessage`. Heimdall owns inbound. Two senders on the same bot tokens are safe.

### 7.5 Per-job cutover from Pi (never both firing)

1. Back up the Pi job's JSON. Pi has no pause, and deleting a job removes its file.
2. Create the Heimdall job **paused** (`"paused": true` in its definition, one step), from the identity whose tools it needs. Check its next 3 fires.
3. Optionally run it once by hand (`!job run`), only if the job is private and free of side effects.
4. Between two Pi fires, delete the Pi job and confirm it is gone.
5. Then `!job resume`.
6. Check the first fire; keep the backup for 14 days.
7. Rollback: `!job pause`, then restore the Pi JSON.

## 8. Storage

New tables in the existing database, as append-only schema steps (3a: step 14, tables; step 15, `job_runs.replayed`; 3b: two more tables):

- **`jobs`**
  - id, name, kind;
  - creator key and Slack user ID;
  - `created_in` (iface, chat, thread) and target;
  - spec JSON, tz, prompt or text, options JSON (§2.2);
  - state: `draft`, `active`, `paused`, `done` or `deleted`, with a reason;
  - `draft_expires_at`, `next_due` (UTC), version, created and updated times.
- **`job_runs`**
  - id, job_id, job version;
  - `slot`, with **UNIQUE(job_id, slot)**;
  - `due_at`, trigger (`schedule`, `manual` or `watch`);
  - state, attempt, inbound_id, turn_id, cause category;
  - `replayed`: the idempotent re-run after a restart was used (§7.2);
  - start and end times.
- **`sched_meta`**: `tick_at`, `hold_until`.
- **(3b) `watch_state`**: job_id, cursor (≤64 KiB), last check time, consecutive failures.
- **(3b) `watch_seen`**: (job_id, key) primary key, plus a time.

Run rows older than 90 days are pruned; the last 100 per job are always kept.

Logs carry metadata only: job, run, slot, state, lateness, harness, attempt, cause. Never prompt or answer text.

## 9. Commands, CLI, observability

**Chat commands** (`!` prefix, every interface, shared-chat addressing per `commands.unmentioned`):

| Command | Does |
|---|---|
| `!jobs [all]` | List your jobs (owner: `all`). |
| `!job show <id>` | Spec, zone, next 3 fires (local and UTC), target, options, last 5 runs. |
| `!job add` + JSON | Create a job (same validator as drafts); created active, or paused with `"paused": true`. |
| `!job edit <id>` + JSON of changed fields | Edit; applies from the next run. |
| `!job confirm <id>` / `!job discard <id>` | Accept or drop a draft. |
| `!job pause <id>` / `!job resume <id>` | Pause or resume. |
| `!job run <id>` | Run now (slot `manual:…`). |
| `!job stop <id>` | Stop the running run (native interrupt). |
| `!job rm <id>` | Delete: tombstone (purged after 30 days) and stop the running run. |
| `!job history <id>` | Last 20 runs: state, lateness, attempts, cause. |
| `!job renew <id> [duration]` | Extend `until`. |

- `!help` lists these. Unknown `!job` subcommands get help.
- `!status` gains one scheduler line: tick age, running, pending, next due.

**CLI** (read-only except `drain`):
- `heimdall sched list|show|health|export`. `export` writes job definitions as JSON: no secrets, for backup and rollback.
- `heimdall drain`.

## 10. Configuration

```json
"scheduler": {
  "enabled": true,
  "default_tz": "Area/City",
  "max_concurrent": 2,
  "per_creator": 1,
  "grace_recurring_seconds": 7200,
  "grace_oneoff_seconds": 86400,
  "pause_after": 3,
  "checkers": {
    "example-checker": {"argv": ["/path/to/checker"], "timeout_seconds": 60}
  }
}
```

- `default_tz` is required when `enabled` is true.
- `max_concurrent` must be at most `max_parallel − 1`.
- Config reload means a restart, as today.

## 11. Pi scheduler tools (D3)

- **Problem.** Pi's hub pack registers `schedule_one_off_task`, `schedule_recurring_task`, `list_scheduled_tasks`, `edit_scheduled_task`, `delete_scheduled_task` and `get_scheduled_task_logs`, even in child turns (`extensions/scheduler.ts:1128-1241`, before the `PI_TRANSPORT_CHILD` return at `:1243`; same in `omp-extensions/scheduler.ts`). Jobs written by those tools land in the Pi hub's scheduler directory, which the Pi hub still fires.
- **Pack change** (separate commit on the pack's `hub` branch, with an offline test): both scheduler extensions skip registering the six tools when `PI_SCHEDULER_TOOLS=0`.
- **Heimdall** sets `PI_SCHEDULER_TOOLS=0` on Pi and OMP turns only while `scheduler.enabled` is true.
  - Turning the scheduler off brings Pi's tools back, so both ship together and roll back together.
  - Employee Pi loads no pack extensions and is not affected.
  - The variable is not `HEIMDALL_*`, so it may reach harness environments.

## 12. Decisions (2026-11-03)

| # | Decision | Outcome |
|---|---|---|
| D1 | Cron library | gronx (fallback robfig/cron v3). Technical call. |
| D2 | Default time zone | Owner's zone, set in config; each job may override. |
| D3 | Pi scheduler tools in Heimdall turns | Switched off with `PI_SCHEDULER_TOOLS=0`, shipped with the scheduler (§11). Technical call. |
| D4 | Natural-language creation in v1 | Yes: immediate owner new/edit; otherwise draft and confirm (§5). |
| D5 | AgentNet delivery | A job may carry `to: agentnet:<address>`. The agent delivers with the CLI; Heimdall posts nothing to chat on success and posts failure or missing-marker notices to the creator. Owner jobs only. No AgentNet interface. |
| D6 | AgentNet peers creating jobs | No. Technical call. |
| D8 | Memory for scheduled runs | Never captured (`MEMENTO_PROCESSING=1`); no opt-in. |
| D9 | Interrupted runs | Never re-run, except before the ready signal or with `idempotent: true` (one re-run within grace). Technical call. |

Defaults:
- `max_concurrent` 2, `per_creator` 1;
- overlap `skip`, session `fresh`;
- grace 2 h recurring / 24 h one-off;
- 3 retries before the ready signal;
- `pause_after` 3;
- watch `until` 7 days (maximum 90);
- quiet token `NO_UPDATE`;
- `rm` stops a running run.

## 13. Build plan

**Base:** the hub head, with 104046d, 34a8c5d, 4f3f7e8 and the employee Google sign-in work. Before coding:
- re-read the employee launch path, the per-caller sign-in stores and the identity mapping;
- update the references in this document.

### Phase 3a: scheduler, drafts, D3

1. **`internal/schedule`**: gronx wrapper, zone loading (`time/tzdata`), DST gap and slot rules, `every` and `at` forms, jitter, next-N fires, creation warnings.
2. **Store**: schema step for `jobs`, `job_runs`, `sched_meta`; queries; pruning.
3. **`internal/scheduler`**: due loop, run creation in one transaction, grace and missed handling, overlap, caps, pre-ready retry, pause-after, hold, startup recovery hooks.
4. **Router glue**:
   - a delivery target separate from the conversation key;
   - the scheduled-turn flags of §3.6;
   - the Slack live re-verify hook;
   - quiet handling, `expect` checks, the AgentNet instruction and marker check;
   - notices.
5. **Commands**: `!jobs`, `!job …`, help text; one JSON validator shared with drafts.
6. **Drafts**: block extraction and stripping, draft rows and expiry, card, confirm and discard, edit diff, first-message note.
7. **CLI**: `heimdall sched list|show|health|export` and `heimdall drain`.
8. **Config**: the `scheduler` section and its validation.
9. **D3**:
   - Heimdall sets `PI_SCHEDULER_TOOLS=0`;
   - the pack commit with its offline test (pack repo; deployed by the orchestrator).
10. **Docs**: this file; AGENTS.md rules (§16); README; HANDOFF; `docs/hub-parity.md` (`PI_EXTERNAL_DEFAULT_DELIVERY`, `PI_SCHEDULER_TOOLS`).

### Phase 3b: watches

1. Checker config and runner: OS user, cleaned environment, timeout, output cap, contract parsing.
2. `watch_state` and `watch_seen`; baseline, dedupe, cursor committed in the same transaction as the wake run.
3. Event rules over stored inbound and ambient messages.
4. Actions, debounce, rate cap, expiry warning, renew, `max_events`, stale-watch health.
5. One example checker documented (inputs, outputs). Real checkers live outside the repo.

### Size estimate

| Part | New Go lines (non-test) |
|---|---|
| 3a scheduler core | ~1,300 |
| 3a drafts | ~170 |
| 3a AgentNet target + D3 | ~50 |
| **3a total** | **~1,520** |
| 3b watches | ~360 |
| **Total** | **~1,900** |

- Tests: about 2,400 lines.
- Pack change: about 20 lines plus a test.
- New dependencies: one (gronx, which has none of its own).

## 14. Tests

All run with `go vet ./...` and `go test -race ./...`, using fake clocks and fake harnesses. The binary must build with `CGO_ENABLED=0`.

**Schedules**
- Parse errors; DOM/DOW OR; `L`, `W` and `#`; names.
- Zone required and valid.
- Next-3 fires.
- America/Los_Angeles 2027-03-14: a fixed 02:30 job runs once at 03:00. 2027-11-07: a fixed 01:30 job runs once.
- `*/15` across both transitions.
- The configured default zone's transitions, found from tzdata offsets, not hardcoded.
- `every 2w at 12:15` keeps its wall time across DST; `every 15m` keeps its UTC anchor.
- Jitter is deterministic and stays before the next slot.
- A one-off in the past is rejected.

**Store and loop**
- Slot uniqueness.
- The run-creation transaction is atomic.
- The loop sleeps until due and wakes on change.
- Grace: on time, late within grace, and beyond grace (missed, with one combined notice).
- Downtime with several misses: only the newest slot runs.
- `skip` and `queue_one`.
- `max_concurrent` and per-creator caps; live chat keeps its slots.
- A hold blocks new runs.
- A clock step backwards creates no duplicate.
- Pruning keeps the last 100 runs per job.

**Recovery**
- Crash after a run is created but before it starts: it runs if within grace, else is missed.
- Crash mid-turn: interrupted, notice, no re-run.
- `idempotent`: exactly one re-run.
- Interrupted before the ready signal: queued again.
- A persisted outbound row is delivered after restart.

**Retries and harness**
- A pre-ready failure is retried 3 times with backoff, within grace, except timeout and worker lifetime limits (failed without retry).
- A post-ready failure is never retried except the explicit owner repeat-safe timeout option above.
- An explicit harness never falls back; the default order falls back only before the ready signal.

**Identity**
- A removed or blacklisted creator: run rejected, job paused, owner notice.
- A Slack creator whose email no longer matches, who was deleted, or who is in another team: fail closed. A temporary error is retried.
- An employee job uses the employee account and harness, never the owner's.
- An employee target outside the limits is rejected at creation.
- Missing sign-in: failed run, notice, pause after 3. No other identity is ever used.

**Environment**
- Scheduled turns get: `MEMENTO_PROCESSING=1`; `PI_EXTERNAL_DEFAULT_DELIVERY` = target; `Private=false`; no `owner_slack`.
- None of Heimdall's own `HEIMDALL_*` variables in the harness environment (only the non-secret ones Heimdall sets, §17).
- `PI_SCHEDULER_TOOLS=0` on Pi and OMP only while the scheduler is enabled.

**Delivery**
- The reply goes to the target, not to the conversation the job was created in.
- `NO_UPDATE` sends nothing.
- `out/` files are sent.
- No progress messages.
- Fixed failure text to the target and the notice chat; no model text in shared channels on failure.
- Every notice ends `— heimdall`.

**Result checks**
- An empty answer fails the run.
- A missing `expect.tools` match: unverified. Private target: delivered under the warning line. Shared target: withheld.
- `files` and `prefix` checks.
- 3 unverified runs in a row pause the job.

**AgentNet**
- The prompt carries the instruction.
- Success posts nothing to chat.
- A missing marker: unverified, notice to the creator.
- An employee job with `agentnet:` is rejected.

**Commands**
- Every command.
- Visibility by role; names only in shared chats.
- An edit applies from the next run.
- `rm` stops a running run.
- `renew`.
- An unknown subcommand gets help.

**Drafts**
- The block is parsed and stripped.
- An invalid or multiple block, or an exceeded cap, gets one error line and no stored proposal.
- Confirm works only for the same caller, in the same conversation, within 24 h; others are rejected.
- An expired draft cannot be confirmed.
- An edit shows its diff.
- Blocks from scheduled turns are ignored; at most one block per answer.
- Employee role limits apply.
- The first-message note appears only in the first message of a fresh session.
- `[job: done]` ends the job.

**CLI**
- `drain` exits 0 when idle and 75 on timeout.
- Each health issue is reported.
- `export` contains no secrets.

**Pack (D3)**
- With `PI_SCHEDULER_TOOLS=0`, the six tools are absent from both scheduler extensions.
- Without it, they are present.

**3b**
- Checker contract: valid, bad JSON, non-zero exit, timeout, oversize output.
- The first run is a baseline.
- Dedupe.
- The cursor is unchanged on failure.
- 3 failures in a row send a notice.
- The cursor and the run are committed in one transaction.
- Event rules match addressed and ambient messages, and never unverified callers.
- Debounce and rate cap.
- Expiry warning, renew, `max_events`, stale-watch alert.
- The checker runs as the creator's OS user with a cleaned environment.
- Employees cannot create checker watches.

## 15. Acceptance checks before a deploy request

Use a scratch config and data directory outside the repo, a separate test bot (never the production bot), synthetic prompts, and `MEMENTO_PROCESSING=1`.

1. **Build and tests.**
   - `CGO_ENABLED=0` build.
   - `go vet ./...` and `go test -race ./...` pass.
   - Every test group in §14 exists.
   - The pack test passes.
2. **Review.** One independent review of the full diff against this document.
3. **Create, two ways.** With `!job add`, and with a natural-language owner request → immediate activation (employee or removal/repeat-safe request → draft card → `!job confirm`). `!job show` gives the right next-3 fires in the default zone.
4. **Fire.** A one-minute owner job fires and delivers to its target; `!job history` shows `ok`. A quiet job sends nothing. A `notify` job sends its text.
5. **Target separate from creation.** A job created in a private chat with a group target delivers to the group, and its notices go to the private chat.
6. **Crash.** `kill -9` during a run, then restart: interrupted notice, no re-run. A run created but not started, still within grace, runs after the restart.
7. **Drain and health.** `heimdall drain` exits 0 when idle. `heimdall sched health` is clean, then flags a deliberately overdue job.
8. **Employee.** A test employee job runs on employee Pi as the employee account. Without sign-in: failed, sign-in notice, paused after 3.
9. **D3.** With the scheduler enabled, an owner Pi turn lists no `schedule_*` tools. With it disabled, they are back.
10. **AgentNet.** An owner job with an `agentnet:` target to a test address: the message arrives, nothing is posted to chat, and `!job history` shows `ok`. Without the marker, the run is unverified and a notice is sent.
11. **Data safety.**
    - Logs contain no prompt or answer text (check with a planted synthetic marker).
    - The harness environment contains none of Heimdall's own `HEIMDALL_*` variables (only the non-secret ones Heimdall sets, §17).
    - `export` contains no secrets.
12. **Rollback.** `scheduler.enabled=false`, then restart: no runs, Pi tools back, jobs kept. The previous binary only with the pre-deploy database backup (HANDOFF, rollback B).
13. **Clean-up.** Scratch state removed; the repo is clean.

For 3b, add:
- a poll watch with a stub checker: baseline, a change, a notice;
- an event watch in a test Telegram group;
- Slack only with fakes, unless the orchestrator arranges a live window.

## 16. Rules for AGENTS.md (add in 3a)

- Jobs run as their creator; role and admission are evaluated at every fire. Slack creators are re-verified live. Never another identity.
- One run per job and slot: `UNIQUE(job_id, slot)` in the same transaction that creates the run. Delivery is at-least-once.
- Scheduled turns:
  - their own conversation `sched:<job_id>`, delivering to the captured target;
  - `Private=false`; no `owner_slack`; `MEMENTO_PROCESSING=1`;
  - a fresh session by default;
  - no progress messages.
- Retry only before the ready signal. Interrupted runs are never re-run, except `idempotent` jobs (once).
- Models propose jobs only when asked, through the answer block. Direct owner new jobs and edits apply atomically with the reply; non-owners, removals and `rerun_on_timeout` jobs require confirmation in the same conversation within 24 h. Blocks from scheduled turns are ignored; MCP controls never auto-apply.
- Notices are fixed templates ending `— heimdall`; no model text in shared channels.
- AgentNet: owner jobs may name an address; the agent delivers; Heimdall never talks to AgentNet.
- Watches: deterministic checkers from config only, running as the creator, with an expiry. One Slack consumer and one Telegram poller.
- `PI_SCHEDULER_TOOLS=0` on Pi and OMP turns while the scheduler is enabled.

## 17. Phase 3a as built

Built on `main` 4f3f7e8, rebased onto `main` fe58149 (employee Google sign-in, `!which` model). Where this section and the sections above differ, this section describes the code.

**Layout**
- `internal/schedule`: specs to slots (gronx for cron fields, wall clock evaluated as UTC, the cronie DST rule of §3.4, `time/tzdata` embedded).
- `internal/store/jobs.go`: `jobs`, `job_runs` (`UNIQUE(job_id, slot)`), `sched_meta` (tick, drain hold); `turns.ready`.
- `internal/scheduler`: the due loop, definitions and role rules, commands, drafts, outcomes and notices, `heimdall sched` and `heimdall drain`.
- Router glue (`internal/router/router.go`): a run's message lives in `sched:<job>` on the target's interface; `Begin` before the turn, `End` decides the reply, files and notices, stored with the turn in one transaction. Slack re-verification: `internal/iface/slack/reverify.go`.

**Interpretations and deviations**
- Retries: a pre-ready failure is retried 3 times (1, 5, 15 min ±20%), within grace, as §14 says; so at most 4 attempts in all.
- After downtime one run row is kept for the newest slot; the older slots are counted in the missed notice, not stored one by one.
- The empty-answer check is a backstop: the native adapters already fail a blank answer, so such a run fails with the harness failure category.
- Scheduled turns never use the warm pool (`harness.Spec.OneShot`): one process per run, the run timeout kept, never retained.
- Identity variables: `PI_EXTERNAL_CHAT_ID` and `PI_EXTERNAL_DEFAULT_DELIVERY` follow the conversation's interface (the job's target), so a Telegram owner's job into a Slack channel gets the Slack chat. For live turns nothing changes. An `agentnet:` job runs with the conversation it was created in.
- `notify` jobs cannot target `agentnet:` (only an agent delivers there).
- `heimdall sched health` lists every paused job; only those the scheduler paused count as issues.
- The first-message note goes into the turn context for Codex and Claude, after the prompt in the message for Pi, and at the top of the prompt for OMP and AGY. As turn context live Pi ignored it and never proposed a job (orchestrator decision, 2026-11-03: about 300 characters in the first message of a new native session only, never in scheduled turns).
- A harness that asks for interactive input in a scheduled run (Pi's pack `ask_user_question` tool reaches native UI because RPC mode has a UI; an approval; a sign-in, since scheduled turns relay none) ends the turn at once with nothing answered. The run is `failed` with the cause `needs_human: …` in `!job history` and the failure notice; it counts towards `pause_after` and is not retried.
- A scheduled run's own notes (the quiet instruction, the AgentNet delivery instruction) follow its prompt in the native message on every harness. As turn context, live Pi ignored the quiet instruction.
- Heimdall's own notice follows the answer; an answer that was only the block is replaced by the immediate status line or, where confirmation is required, the draft card.
- `scheduler.checkers` is not accepted until 3b.
- Size (before the review fixes below): about 2,800 non-comment Go lines (3,300 with comments) against the ~1,520 estimate; the operator CLI, the DST evaluation and the role checks of every command are the difference. No dependency beyond gronx.

**Review fixes (independent review, 2026-11-03)**
- Job commands change nothing themselves: each returns its change (`store.JobChange`), and `AnswerCommand` stores it with the command's completion and its reply in one transaction. A crash in between replays the command once, never its change twice (two jobs from one `!job add`, two manual runs, a double renew).
- The creator is checked where the turn starts (`Scheduler.Admit`, called by the router before any harness): live Slack re-verification, `Access.Decide` against the target, and the job's role rules, evaluated then. A run that waited for a router slot, or whose message is replayed after a restart, never runs on the due loop's earlier check; that check stays as an early filter. A temporary Slack error retries the run (before ready); a refusal pauses the job with the identity notice.
- Fire-time rules hold at every fire: the future-run and future-`until` checks belong to creating and editing only (`validate`), so a due one-off is not refused.
- Drain's hold applies where a turn starts (`Begin`): a run handed to the router before the hold goes back to `pending` with the same attempt, its never-started message dropped (the next start stores `run:<id>:<attempt>` again), and starts once the hold clears at startup. `drain` counts runs handed over but not ended as busy.
- `"paused": true` creates a job paused in one step (§7.5); a draft may carry it. The job's reason is `paused by <creator> at creation`, so `sched health` does not count it.
- A tool result with `details.status` `auth_needed` (the employee Google runtime returns one, as an ordinary result, `isError` false, so the model explains it in a normal answer) fails a scheduled run: cause `sign-in needed: <service>` (`details.service`, else `google` for `google_` tools). It is not retried and counts towards `pause_after`. Failure and pause notices name the sign-in: `google login` for Google, `mcp login` for anything else; `requires` picks it when the cause is a sign-in category or `needs_human`. Read from Pi's `tool_execution_end` (`result.details`, Pi 1.5.0 RPC; the hub's employee Pi version is unchecked). Live turns are unchanged.
- The idempotent re-run is recorded in `job_runs.replayed` (schema step 15), not in the cause, so a crash before ready in between allows no second post-ready re-run.
- In shared chats `!job show`, `!job add` and `!job edit` carry no prompt or text (an edit's diff says only that it changed); only a draft's card previews 200 characters.
- `history_ctx` is deferred: the key is refused as unknown, the history block and its query are gone.
- Environment rule, as built: Heimdall's own `HEIMDALL_*` variables (bot tokens, the Google client ID and secret) are stripped from every harness environment (`harness/launch.go`), and employee turns also drop `GOOGLE_*`, `PI_GOOGLE_*`, `HEIMDALL_GOOGLE_*` and `INFISICAL_*`. Heimdall then sets only non-secret `HEIMDALL_*` values: `HEIMDALL_TURN_DIR` (employee turns), `HEIMDALL_CALLER_CAPABILITIES` (employee turns), and for employee Pi the paths `HEIMDALL_GOOGLE_ACCESS_FILE` (the per-turn access-only token file) and `HEIMDALL_MCP_SCOPE_ROOT`. No Heimdall secret or Infisical credential is among them.
- Rollback (HANDOFF): A, `scheduler.enabled=false` on this binary; B, the previous binary only with the database backup taken right before the deploy (the schema-13 binary refuses schema 15) and the previous config (it refuses the `scheduler` section).

## Appendix A. Pi hub scheduler: kept, changed, dropped

References are to `extensions/scheduler.ts` on `origin/hub` 956955e.

| Pi | Ref | Heimdall |
|---|---|---|
| One-off and recurring jobs, owner, delivery, misfire grace | `:57-88` | Kept. |
| UTC-only cron; `timezone` stored, unused | `:438-450`, `:1153-1164` | IANA zone per job plus the DST rule. |
| DOM/DOW combined with AND | `:446` | Fixed: OR. |
| JSON file per job; a finishing run overwrote edits and deletes; half-written files | `:142-167` | SQLite rows; run state lives only in `job_runs`. |
| External restart guard script | `:40-43`, `:290-327` | Transactional run creation plus `heimdall drain`. |
| 30 s polling | `:31` | Sleep until due; wake on change. |
| One job at a time globally | `:32` | `max_concurrent` 2 inside `max_parallel`. |
| Grace 24 h / 2 h; skip beyond grace with a notice | `:33-34`, `:858-877` | Kept. |
| Stale "running" at startup: notify, never re-run | `:333-393` | Kept, through `Recover`; plus `idempotent`. |
| Delivery receipt; never replay | `:186-199`, `:758` | `UNIQUE(job_id, slot)` plus outbound reconciliation. |
| Fresh session per fire | `:559-562` | Kept (default). |
| Retries with an LLM recovery prompt and read-only replay | `:499-530`, `:912-968` | Dropped; retry only before the ready signal. |
| Keyword-regex tool validation | `:452-497` | Declared `expect` checks. |
| Regex classes: blocked, partial, unsafe | `:531-557`, `:936-957` | Dropped. |
| Prompt sentinel strings | `:37-39`, `:125-135` | Structured `quiet` and `expect.prefix`. |
| Daily-intel-specific code and state | `:25-26`, `:185-287` | Dropped; a generic `history_ctx` is deferred (§17). |
| Open failures injected into chat turns | `:721-743`, `:1114-1126` | Dropped; notices plus `!jobs`. |
| Fixed failure envelope; failures reach the target | `:675-710` | Kept. |
| Non-admins: current chat only, own scope only | `:1082-1100` | Kept for employees. |
| Scoped MCP origin for jobs | `:45-56`, `:1017-1080` | Dropped; scheduled turns never get `owner_slack` or sign-in. |
| `allow_self_cancel` | `:1214-1224` | `[job: done]`. |
| Edits refused while running | `:1182` | Dropped; edits apply to the next run. |
| Delivered output fed back as trusted context | (pack fix #4) | Outbound rows come back as untrusted context. |
| Heartbeat plus health script | `:1272`, `scripts/scheduler-health.mjs` | `heimdall sched health`. |
| A2A peers may create jobs | `:1095` | Dropped. |
| Separate OMP copy | `omp-extensions/scheduler.ts` | One implementation. |
| Model-called scheduling tools | `:1128-1241` | Drafts plus human confirm; tools switched off (§11). |

## Appendix B. Build vs reuse

| Component | Candidate | Last release | License | Weight | Verdict |
|---|---|---|---|---|---|
| Cron | adhocore/gronx | v1.20.5, 2026-09-27 | MIT | no deps | **Adopt** |
| Cron | robfig/cron v3 | v3.0.1, 2020-01-04 (last commit 2021-01-06) | MIT | no deps | Fallback; frozen; documents skipping spring-forward jobs |
| Scheduler | go-co-op/gocron v2 | v2.23.0, 2026-10-26 | MIT | uuid, clockwork, robfig/cron | Reject: in-memory, a second loop |
| Zones | Go `time/tzdata` | stdlib | BSD | none | **Adopt** |
| Queue | River | active 2026-11; SQLite driver preview | MPL-2.0 | heavy | Reject: OSS periodic jobs not durable |
| Queue | maragudk/goqite | v0.4.0 (GitHub 2026-02-09; pkg.go.dev 2025-11-20) | MIT | none | Reject: duplicates Heimdall's queue |
| Queue | hibiken/asynq | v0.26.0, 2026-02-03 | MIT | Redis | Reject: extra service |
| Workflow | Temporal | n/a | MIT | server and database | Reject: extra service; concepts adopted |
| Store, delivery | Heimdall's own | in repo | n/a | none | **Reuse** |

Dropped as scale-only:
- distributed locks and leader election;
- lease and visibility-timeout reclaim;
- backfill;
- overlap modes AllowAll, Cancel and Terminate;
- per-harness caps and priority queues;
- job dependency graphs.

## Appendix C. Sources (accessed 2026-11-03)

| Source | URL | Used for |
|---|---|---|
| cronie cron(8) | https://raw.githubusercontent.com/cronie-crond/cronie/master/man/cron.8 | DST rule |
| robfig/cron | https://pkg.go.dev/github.com/robfig/cron/v3 · https://github.com/robfig/cron · https://raw.githubusercontent.com/robfig/cron/master/spec.go | CRON_TZ; spring-forward skip; OR semantics; status |
| gronx | https://pkg.go.dev/github.com/adhocore/gronx · https://api.github.com/repos/adhocore/gronx/releases/latest · https://raw.githubusercontent.com/adhocore/gronx/main/next.go · https://raw.githubusercontent.com/adhocore/gronx/main/gronx.go | chosen parser; OR semantics; no DST code |
| gocron v2 | https://pkg.go.dev/github.com/go-co-op/gocron/v2 · https://raw.githubusercontent.com/go-co-op/gocron/v2/go.mod | skip/wait modes; DST run-after-transition idea |
| Temporal Schedules | https://docs.temporal.io/schedule | overlap Skip/BufferOne; catch-up window; pause-on-failure; jitter cap |
| Kubernetes CronJob | https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ | starting deadline; Forbid; more than 100 missed; idempotency; time zone field |
| Quartz CronTrigger | https://www.quartz-scheduler.org/api/2.3.0/org/quartz/CronTrigger.html | fire once now vs do nothing |
| systemd.timer (v262) | https://www.freedesktop.org/software/systemd/man/latest/systemd.timer.html | Persistent=; fixed random delay |
| River | https://riverqueue.com/docs/periodic-jobs · https://riverqueue.com/docs/unique-jobs · https://riverqueue.com/docs/job-retries · https://riverqueue.com/docs/stuck-jobs | unique index; non-durable OSS periodic jobs |
| goqite | https://pkg.go.dev/maragu.dev/goqite | visibility timeout (not needed) |
| Asynq | https://pkg.go.dev/github.com/hibiken/asynq | needs Redis |
| SQLite WAL | https://www.sqlite.org/wal.html | one writer; concurrent readers; same host |
| Go time | https://pkg.go.dev/time | `time.Date` in transitions; tzdata fallback |
| Transactional outbox | https://microservices.io/patterns/data/transactional-outbox.html | at-least-once delivery |
| Slack chat.postMessage | https://docs.slack.dev/reference/methods/chat.postMessage | about 1 msg/s per channel; no idempotency key |
| Slack rate limits | https://docs.slack.dev/apis/web-api/rate-limits · https://docs.slack.dev/changelog/2025/06/03/rate-limits-clarity/ | Retry-After; internal apps keep their limits |
| Slack Events API, Socket Mode | https://docs.slack.dev/apis/events-api/ · https://docs.slack.dev/apis/events-api/using-socket-mode | retries; event_id; one consumer |
| Telegram Bot API, FAQ | https://core.telegram.org/bots/api · https://core.telegram.org/bots/faq | offset confirmation; retry_after; per-chat limits |
| Gmail history and sync | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list · https://developers.google.com/workspace/gmail/api/guides/sync | historyId cursor; 404 full sync |
| Gmail push | https://developers.google.com/workspace/gmail/api/guides/push | Pub/Sub; 7-day renewal (deferred) |
| Zapier dedupe | https://docs.zapier.com/platform/build/deduplication | id dedupe; first poll is a baseline |
| Huginn | https://github.com/huginn/huginn | watch, then act; on_change |
| Claude Code scheduling | https://code.claude.com/docs/en/scheduled-tasks · https://code.claude.com/docs/en/desktop-scheduled-tasks · https://code.claude.com/docs/en/routines | 7-day expiry; one catch-up run; missing connection → skip, then off |
| ChatGPT scheduled tasks | https://help.openai.com/en/articles/10291617-scheduled-tasks-in-chatgpt | active-task caps; monitors notify only on change. Page returned 403; from a search summary only. |

Unverified, to confirm during the build:
- How gronx and robfig behave across DST; DST table tests decide.
- Our Slack app's internal-app status.
- Bot access to channels it is not in.
- Tool-name telemetry on scheduled turns for each harness.
- The T3 CLI on the hub.
- The ChatGPT Tasks page (fetch returned 403).

## Owner control and checkpoint workflows

The optional local control socket, shared stdio tool adapter, atomic workflow
checkpoints, delivery windows and prepared owner Slack monitor are documented in
[scheduler-control.md](scheduler-control.md). They extend the existing scheduler;
native connector capability and production activation require the bounded live
probe described there.
