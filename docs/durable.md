# Durable commitments (MEL-763, slice 1: manual scaffold)

Status: opt-in and **disabled by default**. Laptop-tested only; not deployed
or live-accepted. Slice 1 is a **manual scaffold**, not autonomous Bezos
supervision. See "Not qualified" below.

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
   (WAL, `synchronous(FULL)`) and queued on its resource before `!work add`
   answers. A repeated issue returns the existing commitment.
2. **Dispatch.** Runs only when a `Dispatcher` is configured. None is
   qualified yet (see below), so every commitment today is dispatched by the
   owner.
3. **Wait** for a report on the current attempt. Silence is never terminal.
   After `silence_seconds` (default 86400; a negative value disables
   notices), there is one notice per attempt and phase, then a quiet
   durable wait. `!work` shows how long it has been.
4. **Evidence precheck.** Deterministic, and it can only *reject*:
   - empty evidence fails;
   - a `gate=git:/repo` needs a full commit SHA that exists in that repo.

   A rejection opens the next attempt.
5. **Verdict.** A worker report is delivered as an **UNACCEPTED worker
   report**. Only an accept from a verified live owner (`!work accept`)
   accepts it. A reject opens the next attempt.
6. **Deliver** the accepted result to the original route.
7. **Next authorized action.** If declared (`| next: …`), the commitment
   stays **open** after acceptance until the owner closes it. With no next
   action it retires.

Every delivery uses the existing outbox under a structured effect key
(`ISSUE/effect/attempt`). The key is kept in outbound `extra`, which needs
no schema version change, so an older Heimdall still opens the database. A
repeated key returns the first reply. A key bound to another route is an
error. The workflow waits for the reply's receipt:
- **Transient failures** follow the outbox's existing retry policy.
- **A permanent failure is never replayed.** The commitment shows
  `delivery_blocked` with the failed receipt until `!work retry` or
  `!work close`. Events that arrive meanwhile are kept.
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
uses. Employees and scheduled-run origins are refused. The creator and the
reply route come from the stored message and never from command text.

`!work pause` is the explicit owner pause. It cancels the workflow with a
`paused_by` attribute. It never expires, and restart recovery does not
resume it. `!work resume` puts the commitment back on its own resource
queue. Ordinary queueing and failures are never shown as a pause.

## Configuration

```json
{"durable": {"enabled": true, "linear_workspace": "mellanni",
  "resources": {"default": {"per_worker": 1}, "contabo": {"concurrency": 3, "rate_limit": 2, "rate_period_seconds": 60}}}}
```

Each resource is a DBOS queue:
- `concurrency`: the number of active commitments;
- `per_worker`: concurrency per worker partition (default 1);
- `rate_limit`: starts per period.

Commitments over capacity stay visible as "queued for capacity", never
dropped or paused.

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

## Not qualified (blockers for autonomous follow-through)

Slice 1 needs the owner for report, verdict, retry and close. That is a
scaffold, not the target. Each blocker is a genuine interface or authority
boundary:

1. **Dispatch.** `t3 thread send` (T3 CLI 0.1.7) has no client request id,
   so a sent brief cannot be reconciled exactly; it would mean text
   matching. MCP `delegate_task` and `t3_thread_send` have request ids, but
   they are bound to a T3 provider session that the Heimdall daemon does
   not have. There is no supported, authenticated hub-to-laptop context.
2. **Observation.** `t3 thread show --json` worked only from the same OS
   user on the laptop, through a temporary AppImage mount. The installed
   `t3` shim is stale. There is no restart-stable executable or API, and
   status is thread-level, not task-level.
3. **Reports and verdicts from Bezos.** The supported caller-bound path is
   the scheduler control socket. It admits mutations only from a live
   person turn. Continuation turns (worker callbacks between turns) and
   scheduled runs fail closed, by design. Letting Bezos progress a
   commitment on its own needs a commitment-scoped capability for those
   origins. That is an authority decision for main and the owner, not
   something to work around.
4. **Verifier.** No deterministic acceptance verifier exists beyond the
   prechecks. A harness acceptance turn needs the structured verdict
   channel from item 3.

Later slices:
- automatic admission of accepted natural-language requests;
- Linear writes (retire, comment);
- dashboard projection;
- a read-only CLI listing.
