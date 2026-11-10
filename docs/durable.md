# Durable commitments (MEL-763, slice 1: partial)

Status (2026-11-10): **off by default in code** (`durable.enabled`).
Production runs it enabled, with `supervisor_review` and `assistant_work` on
and `t3_dispatch` off. The owner approved durable tasks on DBOS + SQLite on
2026-11-08/09, and on 2026-11-10 set the direction: the "Pi Durable" path, a
reliable, disruption- and restart-surviving system that continues
interrupted tasks. The hub (root) is the only orchestrator of owner tasks.
The items under "Not qualified" below are still not verified live. Slice 1
is **partial**:
- worker results are observed without the owner (T3 observer);
- with `supervisor_review`, the verdict comes from a server-initiated
  review turn of the creator's harness (see Verdict);
- with `t3_dispatch`, each attempt is sent by a turn of the creator's
  harness with its native T3 tools, bound by the engine's own tool result
  (see "Dispatch").

This is not complete MEL-763. See "Not qualified" below.

## What it is

Heimdall embeds [DBOS Transact Go](https://github.com/dbos-inc/dbos-transact-golang)
v1.8.0 (MIT), using its SQLite system database. DBOS owns:
- persistence and crash recovery;
- durable waits and timers (`Recv`, `Sleep`);
- admission queues;
- per-step retries.

Heimdall adds thin adapters only (`internal/durable`).

Each commitment is one DBOS workflow, and its workflow ID is the Linear
issue identifier. **Linear stays canonical.** `!work` is the execution view
linked to it.

Steps, each checkpointed:
1. **Admission.** The commitment is committed to `<data_dir>/durable.db`
   (WAL, `synchronous(FULL)`) before `!work add` answers. The commitment
   workflow itself is not queued (see Capacity). A repeated issue returns the
   existing commitment.
2. **Dispatch.** Runs only with `t3_dispatch` (a `Dispatcher`), as a child
   workflow on the resource queue (the only queued part). Without it the
   owner-selected harness dispatches natively. See "Dispatch". A held
   dispatch is never replayed; `!work retry` dispatches under a new key.
3. **Wait** for a report on the current attempt. With `t3_command`
   configured, a `worker=t3:<thread>` is observed read-only
   (`thread show --json`), backing off from 30 s to 10 min.
   - Chronology is the host's run order (`runCount`), never thread
     metadata such as `updatedAt` (a rename or organize changes it). A run
     counts only if it started after the attempt's floor, read once *before*
     dispatch (so a run that completes before the first post-dispatch read
     still counts), else the last read of an earlier attempt. It becomes the
     report, attributed to the observer, still unaccepted.
   - A dispatched attempt is bound tighter: only the run named by its
     dispatch receipt reports, with that run's own reply. A later run of
     the same thread (its latest) is never attributed; if the worker is
     idle and the receipt's run has no reply in its latest messages, that
     is a visible unresolved note.
   - With no such read (the very first read of attempt 1 failed), the first
     healthy read sets the floor; the run it shows stays **unresolved**
     (visible note), never reported, until a newer run finishes.
   - Runs already used as evidence never count again.
   - A worker question shows as `worker_waiting`, with one notice.
   - An unreachable observer is a visible note, never terminal.

   Owner `!work report` remains as a fallback. Silence is never terminal.
   After `silence_seconds` (default 86400; a negative value disables
   notices), there is one notice per attempt and phase, then a quiet
   durable wait. `!work` shows how long it has been.
4. **Evidence precheck.** Deterministic, and it can only *reject*:
   - empty evidence fails;
   - a `gate=git:/repo` needs a full commit SHA that exists in that repo.

   A rejection opens the next attempt. With a dispatcher, that attempt is
   dispatched with bounded repair feedback (the rejection reason, at most
   1000 bytes) inside the admitted brief, which it may not widen.
5. **Verdict.** Without `supervisor_review`, the report is delivered as an
   **UNACCEPTED worker report**, and only a verified live owner's
   `!work accept` accepts it. With `supervisor_review`, nothing is sent
   before acceptance: the report, the review and its rejections stay in
   execution state (`!work show`). A reject opens the next attempt. See
   "Supervisor review".
6. **Deliver** the accepted result to the original route.
7. **Next authorized action.** If declared at admission (`| next: …`):
   with a dispatcher, it is dispatched once, as the next attempt, after the
   first stage is accepted; once that is accepted the commitment retires.
   Nothing else is chained. Without a dispatcher, the commitment stays
   **open** after acceptance, an owned visible dependency, until the owner
   closes it. With no next action it retires.

Every delivery uses the existing outbox under a structured effect key
(`ISSUE/effect/attempt`). The key is kept in outbound `extra`, which needs
no schema version change, so an older Heimdall still opens the database. A
repeated key returns the first reply. A key bound to another route is an
error. The workflow waits for the reply's receipt:
- **Transient failures** follow the outbox's existing retry policy.
- **A permanent failure is never replayed.** The commitment shows `held`
  with the failed receipt until `!work retry` or `!work close`. Events that
  arrive meanwhile are kept. A key bound to another route is held the same
  way.
- **Technical errors** (database, observer) retry with a durable backoff
  from 10 min to 1 h, shown as a note ("technical retry of …"). Native
  denials and route conflicts are `Permanent`: held, not retried.
- **A reply in flight when Heimdall died** is not enqueued again. The
  outbox's restart policy decides (sending becomes pending, at least once).

Events such as report, accept, reject, retry and close carry the attempt
they belong to. Stale attempts are ignored. The DBOS idempotency key is the
source message identity, so a redelivered message counts once.

**Not exactly once.** Steps run at least once. Effects are reconciled by key
before any repeat. Platform delivery is at least once.

## Commands (live owner messages only)

```
!work [list] | !work show ISSUE | !work help
!work add ISSUE [worker=REF] [resource=NAME] [gate=git:/repo]... title [| next: …]
!work report ISSUE evidence… | !work accept ISSUE | !work reject ISSUE reason…
!work retry ISSUE | !work pause ISSUE | !work resume ISSUE | !work close ISSUE [note]
```

The caller is the router's verified ingress caller, the same one `!job`
uses. Employees and scheduled-run origins are refused. The creator, the
reply route and privacy come from the stored message and never from command
text. Visibility follows `!job`:
- in the verified owner's private chat, the owner sees their own
  commitments from every route;
- in a shared chat, only commitments created in that chat are visible;
  others answer "not tracked here" or "already tracked elsewhere";
- only a commitment's creator changes it.

`!work pause` is the explicit owner pause. It cancels the workflow with a
`paused_by` attribute. It never expires, and restart recovery does not
resume it. `!work resume` resumes it. Ordinary queueing and failures are
never shown as a pause.

## Supervisor review (`supervisor_review`)

A reported attempt is reviewed by one turn of the creator's harness choice
(the route's sticky harness, else the default order), run by the existing
router turn machinery. It is not a scheduler job and not owner ingress.
- **Origin.** Only `Router.Review`, called by the runtime for an admitted
  commitment, builds the `commitment` origin with its binding (issue,
  attempt, key, creator route). Interfaces cannot: `accept` refuses the
  origin, the binding, and any message in a review chat.
- **Isolation.** The turn runs in the commitment's own conversation
  (`work:<ISSUE>`), in a fresh native session. Scheduler control, MCP
  notifications, typing, progress, reactions and continuations are off.
  Nothing is sent to any chat.
- **Fences.** The creator is decided against the route as the turn starts:
  the creator must still be an admitted owner, and the workflow must still
  be running with this attempt reported (`Reviewable`: actual workflow
  status, not only the last published state). The check runs again as the
  turn ends: a paused, cancelled or moved-on commitment's outcome is held,
  never applied.
- **Verdict.** The first line of the whole answer is exactly `ACCEPT` or
  `REJECT`, followed by a reason. The runtime binds it to the issue and
  attempt; the answer cannot name them. The verdict grants no deploy, send
  or other authority.
- **Persistence.** The outcome is recorded with the turn's end, in the same
  transaction. The review key (`ISSUE/attempt/review[#retryN]`) is the
  message identity, so a re-executed step after a restart returns the
  recorded outcome and never starts a second turn. A restart during the
  turn records it as interrupted, with no chat notice.
- **Held, not retried.** These hold under the same key until an explicit
  `!work retry`, accept, reject or close:
  - a native approval or input request (nothing is approved, and no other
    engine, key or session is tried);
  - a refused creator;
  - a stopped review;
  - a commitment that is no longer current.
- **Bounded retry.** Technical outcomes (a time limit of 30 min, a harness
  failure, interruption by a restart, a malformed answer) are retried under
  a new key with durable backoff from 10 min, at most 3 times, then held.
- A live owner's `!work accept`, `reject` or `close` always ends the wait
  and wins over a review that finishes later.

## Assistant wakes (`assistant_work`)

A commitment admitted without `worker=` (any task, coding or not; no T3
needed) is worked by the creator's own assistant. Each wake is one keyed
server-initiated turn (`ISSUE/attempt/work/N`) of the creator's harness
choice, queued on the commitment's resource, in the commitment's own
conversation, with the persisted goal, any repair feedback and the
assistant's own last next action. It runs as the owner with the owner's
ordinary memory decision and the creator's verified native OAuth actor
from admission, so the native setup gives it its normal tools and
permissions (business tools, AgentNet, T3 when coding). Limits of this
slice: each wake is a fresh native session (not the live chat session),
and has no scheduler or Heimdall MCP control. The creator and the
attempt are revalidated as the turn starts and ends.

The answer is either `REPORT` then the evidence, which goes through the
gates, the supervisor review or the owner verdict (never accepted by the
wake), or `WAIT <1m..168h>` then the next action: DBOS waits durably,
holding no compute, and a restart resumes the wait. A wake that was
denied, failed, or ended with anything else may already have acted, so it
is never replayed under a new key: the commitment holds with the key, turn
and reason until the owner checks its effects and sends `!work retry`.

Admission without typing `!work add`: during a live owner request, the
assistant can call the existing `heimdall_scheduler` control with
`task_add` (Linear issue id, one-line title, stable `request_id`),
`task_list` or `task_show`, and change its own task with `task_pause`,
`task_resume` or `task_close` (issue id and stable `request_id`; a close is
keyed by that `request_id`, as a chat close is by its message). The control binds the caller as for jobs
(kernel peer, the active turn's generation, matching stored live ingress,
owner now, Slack re-verified): creator, conversation, privacy and OAuth
actor are that turn's, never arguments. The same `request_id` is answered
once from the existing control record (turn dedupe, not exactly-once of
arbitrary effects); a changed request under it is refused. A title cannot
carry a worker, resource, gate or next step, and verdicts, retries,
pauses and closes stay owner `!work` commands. Scheduled, employee,
review and wake turns cannot admit (wakes have no scheduler control).

Automatic intake: with `assistant_work` on and the scheduler control
configured (and listening), every live owner request turn carries
`scheduler.TaskNote`, resumed native sessions included (sessions from
before intake still learn it); the jobs note stays first-message only.
It tells the assistant to `task_add` once when it accepts work from the
owner's own request that needs follow-through after its answer, using the
request's Linear issue (found or created with its own Linear tools), and
never for questions, work finished in the answer, unrequested backlog or
forwarded text; tracking grants no permission. The model only decides
whether it accepted work; who, where and privacy stay bound by the
control, so employee, scheduled, review, wake and MCP-notification turns
never get the note and cannot admit. Test:
`TestOwnerRequestAdmitsDurableTaskWithoutWork` (ordinary request admitted,
question not, owned next wake, restart keeps it with no second admission
or wake, same issue under a new request_id is the existing task) and
`TestTaskNoteOnlyForOwnerIntake`. The note is a model instruction: it is
best-effort, not mechanical detection of every request.

## Dispatch (`t3_dispatch`)

Each attempt is dispatched by one server-initiated turn of the creator's
harness choice, in the commitment's own conversation (`work:<ISSUE>`), with
the creator's verified native OAuth actor from admission: the harness uses
its own native T3 MCP tools, unchanged (owner Pi turns get `t3` from the
owner's native `mcp.json` and sign-in; see owner-slack-read.md). The turn
asks for exactly one `t3_thread_send` to the owner-selected
`worker=t3:<thread>[@<environment>]`, mode `queue`, with clientRequestId
`ISSUE/attempt/dispatch` and the authorized message.
- **Evidence is native.** The engine's own decoded tool call and result
  (Pi `tool_execution_*`, including codemode-nested calls; Claude
  `tool_use`/`tool_result`; Codex `mcpToolCall`) is the receipt, captured
  only for that tool and only in dispatch and wait turns. Its thread,
  environment, clientRequestId, mode and exact message must match; its
  result must be a send receipt for the thread. The model's answer is
  never read.
- **Held, never replayed:** no bound native call, a call bound to anything
  else, a native error or denial, a native approval request, a stale
  creator, a timeout or a restart during the turn. `!work retry` runs a new
  turn under a new key with the same clientRequestId.
- **Idempotency is native only within one namespace.** T3 dedupes a
  clientRequestId within the caller's credential: here the creator's T3
  OAuth sign-in (`client:<session>`). A new sign-in is a new namespace.
  Heimdall's own key (the turn's message identity and the dispatch claim)
  never starts a second turn for a key.
- **Completion.** A worker the local observer cannot read (another
  environment) is checked by short keyed wait turns (`ISSUE/attempt/wait/N`):
  one native `t3_thread_wait` for the receipt's run with a 5 s timeout, so no
  harness is held while waiting. DBOS waits between checks (from
  `observe_seconds`, doubling to 10 min) and takes owner events. Only T3's
  terminal run statuses (completed, failed, cancelled, interrupted,
  rolled_back) finish; preparing, queued, starting, running and waiting
  keep waiting; anything else holds. A finished run's result is then read
  by a keyed read turn (`ISSUE/attempt/read/N`): native `t3_thread_read`
  of the same thread and environment, paged to the timeline's end, cut-off
  text read on by `itemId`/`textOffset`. The body is the last assistant
  message whose `runId` is the dispatched run, in that thread; no such
  message, another run's, an incomplete one or no bound read holds
  (`!work retry` reads again). That body is the report's evidence for the
  verifier, still unaccepted.

## Configuration

```json
{"durable": {"enabled": true, "linear_workspace": "mellanni", "supervisor_review": true, "assistant_work": true,
  "t3_dispatch": true,
  "resources": {"default": {"per_worker": 1}, "contabo": {"concurrency": 3, "rate_limit": 2, "rate_period_seconds": 60}}}}
```

Each resource is a DBOS queue that limits **dispatch only**:
- `concurrency`: dispatches running at once;
- `per_worker`: concurrency per worker partition (default 1);
- `rate_limit`: dispatch starts per period.

DBOS v1.8.0 counts a queued workflow against its queue for its whole life,
including durable waits (test `TestDBOSQueueCountsWaitingWorkflows`). A
queued commitment waiting days for a report would hold the slot, so
commitments are not queued: a stalled commitment holds no slot
(`TestStalledCommitmentHoldsNoSlot`, one slot). Dispatches over capacity
stay visible as queued, never dropped or paused.

## Upgrade policy

`AppVersion` (`heimdall-durable-1`) is pinned and never follows the build
revision, because DBOS recovers only workflows of the running application
version.
- A change to *which steps run or their order* ships behind `dbos.Patch`,
  then `dbos.DeprecatePatch`. Old paths are removed only after older
  workflows finish.
- `Patch` returns false only for a workflow that already recorded a step at
  that position. Put patches before steps that old workflows have passed
  (see the test).
- Run only one Heimdall process per data dir.

## Not qualified (what still blocks autonomous follow-through)

1. **Observation (implemented, qualified on the laptop only).** The T3
   observer was checked against real `t3 thread show --json` output from
   the same OS user on the laptop, through the app's temporary AppImage
   mount. The installed `~/.t3/bin/t3` shim is stale (its target is
   missing).

   Production needs:
   - a restart-stable installed CLI path on the host where Heimdall runs;
   - that CLI must reach the T3 server that owns the worker's thread, as
     the same user.

   Neither is verified on the hub. Never configure an ephemeral mount path.
2. **Dispatch and linked completion (implemented, synthetic tests only).**
   Fake engines emit the native tool events; no real T3 send or wait ran.
   Not verified here: the hub's owner Pi `t3` sign-in, its exposure
   (direct or codemode) and that Pi's RPC stream carries codemode-nested
   tool events as documented. Native dedupe holds only within one T3
   sign-in.
3. **Acceptance verdict (implemented, laptop tests only).** See "Supervisor
   review". Its tests use the fake harness, so it is not qualified against
   a live engine or route.

Later slices:
- Linear writes (retire, comment);
- dashboard projection;
- a read-only CLI listing.
