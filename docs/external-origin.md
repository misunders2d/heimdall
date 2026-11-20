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
6. The answer must name a run, a person and `author: "human"` (or, with
   the launcher's `agent_proposals` opt-in, `"agent"`; see below). The
   person must be in this launcher's `people` map; the mapped principal must have a
   real registered private conversation with Heimdall (the same verified
   ingress binding used today), is re-verified live on Slack, and must be a
   current owner (`access`, evaluated now). Role is never configured.
7. After the answer, C must still have parent S and the same start time.
8. Before commit (the existing points where the turn generation is
   re-checked), Heimdall walks P's ancestry and asks the launcher again on a
   new connection; run, person and author must be identical and P alive.

### What the origin may do

Owner, 2026-11-20: "my agents can assign, other people's agents can't";
the report goes to the "same channel where it was started of a direct DM
in telegram/slack"; "card is good option, yes"; "just make sure it's clear
what I'm being asked of. clearly, concisely and without tons of technical
babble."

- `create`, `edit`, `remove` and `task_add`: **a card** in the mapped
  principal's registered private Heimdall chat. Nothing is created or
  changed until that owner's own Yes there (a reply `Yes`, or
  `!job confirm <id>`); `No`, `!job discard <id>` or 24 hours without an
  answer drop it. The cards have no buttons (none of Heimdall's do): the
  owner answers by replying. The draft row (the existing scheduler drafts table, with
  the launcher's verified answer in its `origin` column) holds everything
  the Yes needs, so the Yes admits from it and **never asks the launcher
  again**: the run has usually ended by then. The control reply to the run
  is a plain receipt ("sent to the owner's private chat for approval").
  - A draft ends with exactly one line: the answer to Yes or No, or one
    fixed expiry line in the same private chat (stored with the delete in
    one transaction; a restart neither resends the card nor loses the
    draft).
  - **Bare words.** The card arrives unasked in the chat where the owner
    talks to his assistant, so a bare word answers it only when it is
    exactly `Yes`, `No` or `Details` (any case, trailing punctuation
    ignored; not `ok`, `confirm`, `cancel` or the Russian/Ukrainian words
    other drafts take) **and** the card is still the latest thing in that
    chat: no turn is running there, none ended after the card, and no
    unprompted delivery (scheduled output, a task notice, another card)
    followed it. Replies to the owner's own commands (`Details`, `!jobs`)
    do not count. Otherwise the word is an ordinary message (a steer, an
    answer to the assistant, the `cancel` control), and the owner answers
    with `!jobs`, which heads its list with each waiting request and its
    copyable `!job confirm <id>` / `!job discard <id>` line. The card says
    so in one line. This narrows the 2026-11-11 "a plain yes answers all"
    rule for launcher drafts only; the owner's own drafts are unchanged.
  - With several drafts waiting in that chat and one of them from a
    launcher, a bare yes or no answers none; Heimdall lists each (title
    quoted) with its copyable commands, so a request that arrives while
    the owner answers another is never swept up.
  - **The title is exactly what the card shows.** A `task_add` title is
    one line of visible text of at most 160 characters; a title with
    control, invisible or bidi format, tag, private-use or unassigned
    characters, or a longer one, is refused (`task_add not sent: …`), never
    trimmed, so the Yes admits exactly the text on the card. The assistant
    then works from the Linear issue's own text; the card does not show
    the issue (owner: no ids on its face), and `Details` says so.
  - **Revoked launcher.** A Yes checks again that the draft's launcher is
    still configured and, for an agent's draft, that `agent_proposals` is
    still on; if not, the Yes answers that it was withdrawn and nothing is
    started. Config changes take effect on restart, and the scheduler's
    next tick then withdraws such drafts with one line each.
  - A run proposes an issue once (its record outlives the draft, so a No
    is final for that run); an issue already waiting is not sent twice;
    at most 5 launcher proposals wait per person. Launcher request ids
    have their own namespace (`ext:<launcher>:`), separate from live
    turns'.
  - **Card face** (Telegram/Slack Markdown, fixed template): who asks
    (`🧑 You asked through <label> to start a job` for `author: human`,
    `🤖 One of your agents on <label> wants to start a job for you` for
    `author: agent`; `label` is launcher config, never request text), the
    job in one line (the request's title, quoted and escaped as untrusted
    text), where the report goes, and Yes/No (on Slack "Reply Yes in this
    thread"). The Linear issue, run, person and draft ids are only in
    `Details`. A job card shows the usual scheduler card below
    `🧑 You asked through <label>:`. The v1 answer names no agent or
    device, so the card cannot yet say which agent ("Laptop Claude"); that
    needs a name field in the launcher's answer.
  - Answer lines (plain text): Yes `✅ Started “<title>” (<issue>). I'll
    tell you here when it's done.` (the issue matches the later done and
    needs-you lines); No `Cancelled: “<title>”. Nothing was started.`;
    expiry `⌛ Your agent's request expired after 24 hours with no answer:
    “<title>”. Nothing was started.` ("Your request" for `author: human`).
    A refused admission answers a fixed `Couldn't start “<title>”. It still
    waits: …`, never durable's own text.
  - The same run asking again for a proposal that no longer waits is told
    it is no longer waiting (answered or expired) and that this run cannot
    send it again.
  - **Report destination:** the channel where the work started; when that
    is not reachable, the owner's private Heimdall chat. The v1 answer
    names no channel, so today every report goes to that private chat and
    the card says so ("this private chat"): the rule is **partly met**
    (the fallback half). The origin channel needs the launcher's v2
    contract.
  - The Yes runs under the answering message's own identity: a live owner
    message in that private chat (Slack: its verified OAuth actor, which
    the task keeps; without it nothing starts). The task's wakes say
    "Registered through the owner's Yes to an agent proposal from
    <label>" (or "… to their own request through <label>").
    Yes from a group, or from anyone but the mapped principal, answers
    nothing. The card is sent to the exact registered conversation (for a
    Slack owner, its DM thread); a Yes elsewhere, even in the same DM
    outside that thread, does not match it. The answer and expiry lines
    are plain text; only the card uses Markdown.
- `rerun_on_timeout` and removal keep their confirmation as before (now
  the same card).
- Reads, `run`, `pause`, `resume`, `stop`, `acknowledge`: as for a live owner
  turn (`author: human` only).
- `task_list`, `task_show`, `task_wake`, and the creator's own
  `task_pause`, `task_resume`, `task_close` (id and `request_id` only; a
  close is one event per `request_id`): as for a live owner turn
  (`task_wake`: [durable.md](durable.md), "Wake now"). The task's
  creator, route and privacy are the mapped principal's registered private
  conversation. A Slack owner's native OAuth actor and installation come only
  from the live Slack verification of that same principal and team; without
  them the task is refused. A Telegram owner's task carries no actor, as live
  Telegram turns do.
- **Agent authors** (`"agent_proposals": true` on the launcher, owner
  opt-in, default off): an `author: agent` run of a mapped current owner
  may only `task_add` (always a card) and `task_wake` (no text). Every
  other action is refused. Without the opt-in, an agent answer is refused
  as before. Employee, scheduled and unknown origins, and other people's
  agents (their person is unmapped or not a current owner), stay refused.

  **What "own person" means until the v2 contract.** AgentNet's v1
  `person` is the verified roster person of the exact key that signed the
  request, re-checked on every ask. A mapped current owner therefore proves
  the owner's own key signed it: his device or one of his own agents. It
  does not prove who started the chain: an own agent may relay text it got
  from someone else. That is why the title is shown as quoted untrusted
  text and only the owner's Yes admits it. Set `agent_proposals` only for a
  launcher whose `person` is bound to the signing key like this; a launcher
  that cannot bind it must leave it off (Heimdall cannot tell, so it trusts
  this configuration). AgentNet's planned v2 chain field (`chain: own`)
  will let Heimdall check the chain itself.

  Producer gap (current AgentNet `--run-origin`): only a new question or task
  from the person's verified human device is `author: human`; that person's
  later replies in the same conversation are `agent`. Without the opt-in
  they are refused here; with it they may only propose and wake.
- The job's creator is the mapped principal; it is created in, and `here`
  delivers to, that principal's private Heimdall conversation, where the
  card is also the audit trail. A definition may still say
  `"to":"agentnet:<address>"` to have the job's agent deliver there.

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
   a scheduled or automatic run is `agent`. Heimdall acts on `human`, and
   on `agent` only with the launcher's `agent_proposals` opt-in, for
   proposals the owner approves and `task_wake`.
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
    "label":"AgentNet",
    "agent_proposals":false,
    "people":{"<AgentNet person id>":{"principal":"slack:OWNER_EMAIL"}}
  }]
}}}
```

`people` maps a launcher person to an existing Heimdall principal. An
optional `source` (`{"interface","chat","thread"}`) selects which of that
principal's registered private conversations jobs are created in; without it
the most recent one is used. Unmapped people are refused. Config is read at
startup: removing an entry revokes it when Heimdall restarts, and its
waiting cards are withdrawn. `label` (up to 40 bytes; default `name`;
required with `agent_proposals`) is the launcher's name on cards.
`agent_proposals` (default false) admits the mapped owner's own agents to
propose tasks and wake them, as above. Revoking the
person's owner role or keys in AgentNet takes effect on the next call.

### Trusted T3 task registration (owner, 2026-11-09)

`"trusted_t3":{"launcher":"/ABSOLUTE/PATH/TO/t3","user":"T3_OS_USER"}` (absent = off)
lets a peer that no Heimdall turn or configured launcher owns, and that
descends from the running process of the executable `launcher` resolves to
(resolved per request, so no version pin), run `task_add`, `task_list`,
`task_show` and `task_wake` only, as `owner` in its registered private
conversation. Every
path component must be owned by root or `user` and not group/other writable;
below such a directory that others cannot search, another owner is allowed.
The process must run as `user`, and the ancestry must be unchanged at commit.
The owner trusts T3 as a registration source; this is not a human
attestation. Wakes say "Registered through trusted T3". A Heimdall process
tree, a stale turn or a launcher that refused never falls through to this.

`"manage":true` in `trusted_t3` (owner opt-in, default off) also lets that
same bound owner `create` jobs and `edit`, `list`, `show`, `history`, `run`,
`pause`, `resume` and `stop` their own jobs (exact creator) and
`task_pause`, `task_resume`, `task_close` their own tasks, with the same
binding, lineage recheck and `request_id` record. Owner, 2026-11-11: "T3 is
a trusted source of scheduling". Create and edit apply at once (unchanged
by the 2026-11-20 card rule, which covers launcher-vouched runs only),
through the same validation
and activation checks; the conversation and `"to":"here"` are the owner's
registered private chat, and an explicit target such as `slack:C…` works as
for chat-created jobs. A `rerun_on_timeout` job still waits as a draft with a
card in that chat. It never removes, checkpoints, acknowledges, confirms or
reroutes, and is still a trusted agent source, not human attestation.

The same trusted T3 caller may also send the intake hooks' `task_intake`
(`request_id` `prompt_submit`) and `task_intake_done` (`stop`), with only a
thread id; `manage` is not needed, they are not in discovery or the MCP
enum, and `mcp:launch:` threads are skipped. Heimdall reads the thread
itself to find the owner's request ([durable.md](durable.md), T3 intake).

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
  unmapped or not a current owner and is refused, agent or not. A run of the
  owner's own agent (`author: agent`) is refused unless the launcher opts in,
  and then may only propose (a card) or wake.
- **An agent posing as the owner.** Before 2026-11-20 a create from an
  `author: human` answer applied at once, so an agent that made its request
  look human-authored could create jobs. Every create, edit and `task_add`
  is now a card the owner answers in chat, whatever the author.
- **Revoked identity.** Mapping is explicit config; owner role is evaluated
  live; Slack identity is re-verified live; the launcher vouches only against
  current keys.
