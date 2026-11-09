# External verified origins (scheduler control)

Owner rule (2026-11-09): Heimdall is not coupled to AgentNet, but it must
receive identity from AgentNet and pass it to the hub/scheduler. One command
from the owner in AgentNet creates a schedule, with no relay through Slack or
Telegram.

Today a scheduler control call is admitted only when kernel process ancestry
leads to a harness run **Heimdall** started for a live person turn
([scheduler-control.md](scheduler-control.md)). A harness started by another
trusted local program (the AgentNet daemon) is unknown to Heimdall and was
refused with `trusted active request origin unavailable`.

This adds one generic input: a **trusted local launcher** that vouches, live,
for the run it started. Heimdall has no AgentNet code or import; AgentNet is
one producer of this contract. Any launcher that implements it the same way can
be configured.

## Mechanism

Live query over a local Unix socket the launcher owns, bound to kernel process
lineage. Chosen over a signed short-lived assertion because:

- It is the same shape as today's check (kernel credentials, pidfd lifetime,
  process ancestry, live re-check before commit), so it is not weaker.
- No key to pin, rotate or leak; no TTL or clock; no bearer value. An
  assertion handed to the run (environment or file) is readable by every
  descendant of that run and replayable until it expires. The socket answer is
  about one live process and is asked again before commit.
- It answers "is that run still running, for whom" now, so a cancelled or
  finished run, or a person the launcher no longer verifies, fails at once.

Heimdall never reads launcher environment variables (`AGENTNET_REQUESTER`,
`AGENTNET_REQUEST_ID` or any other) as identity, not even as a hint.

### What Heimdall checks, per control call

1. Control peer P: `SO_PEERCRED` UID is root or `run_as.owner`, and
   `SO_PEERPIDFD` pins P's lifetime (unchanged).
2. If a Heimdall-started live person turn owns P, that is the origin
   (unchanged; Heimdall-started turns stay authoritative). Otherwise:
3. For each configured launcher, Heimdall **connects** to its configured
   socket path. The socket must be owned by the configured launcher user and
   its directory owned by that user and not group/other writable. The
   server's `SO_PEERCRED` must show that user; its PID is S.
4. P's kernel ancestry (`/proc/<pid>/stat`, ordered, bounded) must contain a
   process C whose parent is S: the launcher's own direct child. Wrappers and
   execs below C do not matter.
5. Heimdall sends `{"v":1,"pid":C,"start":<C's start time>}` and reads one
   strict JSON answer (unknown fields refused, 4 KiB, 2 s deadline).
6. The answer must name a run, a person and `author: "human"`. The person
   must be in this launcher's `people` map; the mapped principal must have a
   real registered private conversation with Heimdall (the same verified
   ingress binding used today), is re-verified live on Slack, and must be a
   current owner (`access`, evaluated now). Role is never configured.
7. After the answer, C must still have parent S and the same start time.
8. Before commit (the existing points where the turn generation is
   re-checked), Heimdall walks P's ancestry and asks the launcher again on a
   new connection; run, person and author must be identical and P alive.

### What the origin may do

- `create` and `edit`: applied at once (as direct owner chat requests are since
  f967daa). A job with `rerun_on_timeout` still becomes a draft that asks.
- `remove`: a draft; the card goes to the owner's private Heimdall chat and
  only a real human reply there confirms (unchanged removal rule).
- Reads, `run`, `pause`, `resume`, `stop`, `acknowledge`: as for a live owner
  turn.
- `task_add`, `task_list`, `task_show`: as for a live owner turn. The task's
  creator, route and privacy are the mapped principal's registered private
  conversation. A Slack owner's native OAuth actor and installation come only
  from the live Slack verification of that same principal and team; without
  them the task is refused. A Telegram owner's task carries no actor, as live
  Telegram turns do. No chat notice is posted; the control reply is the
  stored receipt. Employee, agent, scheduled and unknown origins stay
  refused.

  Producer gap (current AgentNet `--run-origin`): only a new question or task
  from the person's verified human device is `author: human`; that person's
  later replies in the same conversation are `agent` and are refused here.
  Heimdall does not relax the author check to compensate.
- The job's creator is the mapped principal; it is created in, and `here`
  delivers to, that principal's private Heimdall conversation, where the
  create/edit notice is also posted as the audit trail. A definition may
  still say `"to":"agentnet:<address>"` to have the job's agent deliver there.

Refusals name what is missing, for example: no Heimdall turn and no
configured launcher started this process; the launcher does not vouch for this
process; person `X` has no Heimdall identity mapping; the request was not
authored by the person themself; the mapped identity is not a current owner.

## Contract a launcher must provide (AgentNet side)

1. **Socket.** A Unix stream socket at a fixed path the launcher owns (for
   AgentNet, for example `<agentnet home>/run-origin.sock`; on the server the
   daemon home is `/root/.config/agentnet`, root, 0700). Directory owned by
   the launcher user and not group/other writable (0700 recommended); socket
   0600. Listened on by the long-running launcher process itself (the process
   whose direct children are the runs), never by a helper or child. Remove a
   stale socket only after proving it stale.
2. **Who may ask.** Answer only clients whose `SO_PEERCRED` UID is root or the
   launcher's own UID. Refuse others without detail.
3. **Request.** One line of JSON: `{"v":1,"pid":<int>,"start":<uint64>}`.
   `start` is field 22 of `/proc/<pid>/stat` (clock ticks since boot).
4. **Answer.** One line of JSON, then close:
   - `{"v":1,"run":"<run id>","person":"<person id>","author":"human"|"agent"}`
   - or `{"v":1,"error":"<one sentence>"}`
   No other fields. Each string at most 128 bytes, no control characters.
5. **When to vouch.** Only when `pid` is a process the launcher itself started
   for a request (the PID it spawned, not a descendant), that process is still
   running for that request (not exited, cancelled, timed out or finished),
   and its current start time equals `start`. Otherwise answer `error`.
6. **`person`.** The stable identifier of who made the request, as
   established by the launcher's own signature verification against that
   person's current, unrevoked keys. Never a display name, handle the sender
   can choose, or message text. Heimdall compares it as an exact string, so it
   must not change for the same person. Document which identifier it is.
7. **Lineage.** Each run must stay a direct child of the process that serves
   the socket for its whole lifetime. If the daemon restarts or re-execs (for
   example on auto-update) with a new PID, runs it started earlier are no
   longer vouched for and their control calls are refused; keep the serving
   PID stable across updates or accept that.
8. **`author`.** `human` only when the request was authored directly by that
   person on one of their own verified human devices. A request from an agent
   (including the person's own agents), a relay, a guest, a continuation,
   a scheduled or automatic run is `agent`. Heimdall acts only on `human`.
   This is AgentNet's existing distinction between a person's own request
   from their verified human device (`OwnHumanRequest`, roster human check)
   and everything else; no new concept is needed.
9. **Stability.** The answer for a run must not change during its lifetime;
   Heimdall asks again before commit and refuses any difference.
10. **No identity in the environment.** Nothing the launcher passes to the run
   is used by Heimdall. The run reaches Heimdall's control the usual way (its
   MCP/CLI adapter is a descendant of the run).

## Configuration

```json
{"scheduler":{"control":{
  "owner":"slack:OWNER_EMAIL",
  "external":[{
    "name":"agentnet",
    "socket":"/root/.config/agentnet/run-origin.sock",
    "user":"root",
    "people":{"<AgentNet person id>":{"principal":"slack:OWNER_EMAIL"}}
  }]
}}}
```

`people` maps a launcher person to an existing Heimdall principal. An
optional `source` (`{"interface","chat","thread"}`) selects which of that
principal's registered private conversations jobs are created in; without it
the most recent one is used. Unmapped people are refused. Config is read at
startup: removing an entry revokes it when Heimdall restarts. Revoking the
person's owner role or keys in AgentNet takes effect on the next call.

## Threats

- **Forgery by another local process.** Non-root, non-owner UIDs are refused
  by peer credentials (employee turns run under another UID). A same-UID
  process that does not descend from a launcher child has no C and is
  refused. Heimdall initiates the launcher connection to a configured path
  and checks the server's kernel credentials and PID against the lineage, so
  a process cannot pose as the launcher by binding its own socket elsewhere,
  and a socket at the path served by another PID does not match S. Root (or
  the owner UID) can already ptrace any run; that is the same trust boundary
  as today and out of scope.
- **Replay.** There is no token. Every decision is a live query about a live
  process; `request_id` replays return the recorded answer for the same
  principal only.
- **Stale or finished run.** The peer pidfd, C's start time (PID reuse) and
  the launcher's "still running for that request" rule; re-asked before
  commit.
- **Other person's run.** The launcher names the requester; another person is
  unmapped or not a current owner and is refused. A run of an agent's request
  (`author: agent`) is refused even for the owner.
- **Revoked identity.** Mapping is explicit config; owner role is evaluated
  live; Slack identity is re-verified live; the launcher vouches only against
  current keys.
