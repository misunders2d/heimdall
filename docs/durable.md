# Durable commitments (MEL-763, slice 1: partial)

Status: opt-in and **disabled by default**. Laptop-tested only; not deployed
or live-accepted. Slice 1 is **partial**:
- worker results are observed without the owner (T3 observer);
- with `supervisor_review`, the verdict comes from a server-initiated
  review turn of the creator's harness (see Verdict);
- with `t3_dispatch`, each attempt's brief is sent to its
  `worker=t3:<thread>` through the T3 host CLI, claimed before sending and
  reconciled, never resent (see "Dispatch").

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

## Dispatch (`t3_dispatch`)

The route is the existing T3 host CLI, the same one the observer reads
(`t3_command`): `thread send <thread> - --json`, the brief on stdin. It
queues behind any active work (the CLI's only mode) and prints T3's own
receipt: thread, message id, run id, delivery.

T3 dedupes nothing on this route (T3 server source, app 0.1.7): each CLI
call signs in as a fresh short-lived MCP client (`client:<session>`
namespace) and calls `t3_thread_send` with no `clientRequestId`. MCP
`clientRequestId` dedupes only within one credential's namespace, a T3
provider session or an outside OAuth client. Heimdall has neither: its
harness turns are not T3-launched sessions, and no daemon sign-in is added.
So Heimdall owns the at-most-once send:
- **Bound.** The runtime builds the request from the admitted commitment
  only: issue, attempt, key `ISSUE/attempt/dispatch[#retryN]`, the
  owner-selected worker, and the brief. The message's first line is the
  marker `[heimdall KEY]`.
- **Fences.** Immediately before sending, the creator must still be an
  admitted owner on the original route (`Router.OwnsRoute`), and the
  workflow must still be running and dispatching this attempt (actual
  workflow status). Otherwise it is held, and nothing is sent.
- **Claim before the effect.** The key is claimed in
  `heimdall_dispatch_claims` (in `durable.db`, `synchronous(FULL)`) before
  the CLI runs; a DBOS step is checkpointed only when it returns. The
  receipt is stored with the claim.
- **Outcomes.** A receipt binds the attempt to its run. A server that is
  not running is certainly unsent: the claim is released and retried. Any
  other failure (timeout, kill, lost response, an error text) is an
  **unknown outcome**: it is never sent again. The next try reads the
  thread's latest 50 messages for the user message starting with the
  marker and adopts the run T3 recorded it in. Not found within 2 minutes:
  held ("outcome unknown … not resent") until `!work retry`, which sends
  under a new key after the owner checked the worker.
- A restart re-executes an unfinished step; the claim makes that the
  reconcile path, never a second send (`TestCrashAtEverySeam/dispatch`,
  with a worker host that dedupes nothing).

The CLI reaches only its own host's T3 environment: a worker in a linked
environment cannot be dispatched this way.

## Configuration

```json
{"durable": {"enabled": true, "linear_workspace": "mellanni", "supervisor_review": true,
  "t3_command": ["/absolute/path/to/t3"], "t3_dispatch": true,
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
2. **Dispatch (implemented, synthetic tests only).** The CLI `send` path
   was read from T3's bundled source, not exercised: no real message was
   sent. It shares the observer's host requirements (item 1). Native
   boundaries, not Heimdall workarounds:
   - no `clientRequestId` on the host CLI `send`, so T3 dedupes nothing;
     at-most-once sending rests on Heimdall's claim and reconcile;
   - no `environmentId`: only the local environment's threads;
   - reconcile reads only the latest 50 messages, and a server error
     text (for example a missing thread) is an unknown outcome, so both
     hold rather than resend.

   The smallest native integration that would remove the first two is a
   `--client-request-id` (and environment) flag on `t3 thread send` that
   uses one stable namespace for the caller, upstream in T3.
3. **Acceptance verdict (implemented, laptop tests only).** See "Supervisor
   review". Its tests use the fake harness, so it is not qualified against
   a live engine or route.

Later slices:
- automatic admission of accepted natural-language requests;
- Linear writes (retire, comment);
- dashboard projection;
- a read-only CLI listing.
