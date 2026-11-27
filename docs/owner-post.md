# Posts to Slack as the owner

Owner decision, 2026-11-27: any of the owner's agents, from any interface,
may ask Heimdall to post to any Slack channel, private channel, group DM or
DM **as the owner**, with files. Posts use the owner's own Slack user token
(`HEIMDALL_SLACK_WORK_USER_TOKEN`), so Slack shows them under the owner's
name, sent with the app. The bot token is never used for these posts. Only
the owner may use this; employees never can.

Off by default. Nothing here is deployed by this change.

## Configuration

```json
{"slack":{"team_id":"TEAM_ID","work_owner_id":"UOWNER",
  "owner_post":{"enabled":true,"approval":"confirm"}},
 "scheduler":{"enabled":true,"default_tz":"Europe/Kyiv","control":{"owner":"slack:OWNER_EMAIL"}}}
```

- `approval` is the only switch between the two modes. Changing it is a
  config change and a restart, no code change:
  - `"confirm"` (**mode B, the default**): Heimdall shows the owner the exact
    post in the owner's private chat with the bot (Bezos): destination, who
    asked, the text as it will be posted and each file's name and size. It
    is sent only after the owner's own **Yes** there. **No** cancels it. It
    expires after 24 hours. Same Yes/No rules as job drafts: a whole reply,
    from the same person in that same chat, live human input only; one Yes
    answers every waiting draft and post there; **Details** shows the cards
    again. A Yes approves a post only when its card was confirmed
    delivered before that reply, by the platform's own message order
    (Telegram `message_id`, Slack `ts` of the card's last part) and by
    Heimdall's receipt order; a post whose card was still queued, failed or
    arrived after the reply keeps waiting, and the reply names it. A No
    cancels every waiting post, its card seen or not.
  - Off (`enabled` false or absent): nothing is sent as the owner. Rows
    queued earlier fail with the private notice, any Yes or No to a waiting
    card cancels it, and no Slack event is filtered as a post's echo.
  - `"immediate"` (**mode A**): the post is queued at once. A one-line audit
    note goes to the owner's private chat.
- Requires `slack.work_owner_id` and a verified owner user token (the
  existing conversation-work token; startup checks it with `auth.test`), and
  `scheduler.control`, the only entry point.
- The owner's Slack member and team verified when a post is requested are
  stored with it. Delivery refuses it when the configured owner is someone
  else by then, so a queued post never switches sender.
- Owner token scopes this uses: `chat:write` (text), `files:write` (files),
  `im:write` (open a DM by member ID). Checking Slack before a resend reads
  history with the same token: `channels:history`, `groups:history`,
  `mpim:history` or `im:history` for the conversation type (the watch scopes
  in [README.md](../README.md#slack-watches)). Without them a retry cannot
  check and is never resent (see Delivery).

## Who may ask

The same admission as scheduler control
([scheduler-control.md](scheduler-control.md),
[external-origin.md](external-origin.md)); no new trust path:

| Origin | Allowed |
|---|---|
| A Heimdall live owner turn (Slack or Telegram), via its `heimdall sched mcp` / `sched control` adapter | yes |
| An AgentNet-launched run on the hub, vouched for by the trusted launcher socket, `author` `human` | yes |
| The same, `author` `agent` (the owner's own verified agents) | yes (owner, 2026-11-27: an exception to "agents may only add tasks") |
| Trusted T3 with `trusted_t3.manage` | yes |
| Trusted T3 without `manage` | no |
| A scheduled run | no |
| A standalone terminal harness or CLI call (no Heimdall turn, launcher or T3 lineage) | no (owner, 2026-11-27: terminal agents go through AgentNet or T3) |
| Employees, unknown callers | no |

On top of that, the bound person must be the person whose Slack user token
posts (the configured `work_owner_id`, verified live, or the same person's
linked identity). Another owner never posts under this owner's name.

## The request

One control action, `post_as_owner`, on the existing `heimdall_scheduler`
tool (MCP) or `heimdall sched control` (one JSON object):

```json
{"action":"post_as_owner","request_id":"post-2026-11-27-q3",
 "target":"slack:C0123456789/1732700000.123456",
 "text":"Q3 numbers are in; summary attached.",
 "files":["/root/agent-work/q3-summary.pdf"]}
```

- `target`: `slack:<channel>` (`C…`, `G…`, `D…`), optionally
  `/<thread ts>` to reply in a thread, or a Slack member ID (`U…`, with or
  without `slack:`) for a DM, opened with the owner's token. The owner's own
  chat with the bot and DMs to the bot are refused: a post there would read
  as the owner's live message to Heimdall. A DM channel is refused while
  that chat is not verified, since it cannot be told apart.
- `text`: the exact message, Markdown as in an answer, at most 12000 bytes.
  Secrets are scrubbed before the card shows it, as in every reply. A
  mention of the bot is refused: it would start a turn as the owner.
- `files`: at most 5 absolute paths **on the hub**, each at most 50 MB,
  checked like outbox files: a regular file with one link, not empty, not a
  symlink or FIFO, owned by the control peer's kernel UID, and, by the
  opened file's resolved path, outside Heimdall's data directory and the
  config file's directory. The peer is root or the owner's UID, who can
  already read these files; with the default approval the owner sees each
  file's name and size before anything is sent. Heimdall copies them into its own 0700 outbox
  directory at once, recording each copy's size and SHA-256; the copies are
  what the card describes and what is sent. Before anything of a post is
  sent every copy is verified, and each upload sends the exact bytes it
  verified, so a copy changed after the owner saw it is refused, never
  uploaded. A symlinked config file denies its real directory too. This is the one
  place a caller names a path; the model never does, and identity is still
  kernel credentials, never the request.
- `request_id`: required. Reuse the same complete request after a lost
  reply; a changed request under the same ID is refused.

The reply carries `post_id` and says whether it waits for the owner or was
queued. Text or files alone are fine; at least one is required.

## Delivery

- One post: the text (tagged messages, chunked like a reply), then all
  files completed as one message without a comment. The text is never an
  upload's untagged comment, so its echo is always recognizable.
- The owner's user token posts and uploads (`chat.postMessage`,
  `files.getUploadURLExternal`, `files.completeUploadExternal`). Normal bot
  replies and their uploads are unchanged.
- **Check before any resend.** Each text part carries metadata
  (`heimdall_owner_post`, the row ID) and is looked up in Slack before a
  later attempt or after an ambiguous answer, as replies are. The uploaded
  file IDs are recorded before `files.completeUploadExternal`; a later
  attempt first looks for an owner message carrying them and completes
  again only when none is found. If the check itself fails, nothing is
  resent; the router retries the check later. Neither call is ever retried
  blindly. The check covers the whole thread (from its own ts, Slack's
  clock) or, outside a thread, from a day before the row's local creation:
  it tolerates a hub clock up to 24 h ahead of Slack's (replies keep their
  30 s margin). Beyond that skew a landed post could be sent again.
- Delivery is still at-least-once, never exactly-once: Slack can make a
  completed post visible to history after the check looked. Say so.
- Nothing about a post is ever sent to its target but the post. A post that
  fails for good (or after the router's attempts) gets one fixed notice in
  the owner's private chat with the bot, naming the destination. The file
  copies are removed when the row finishes, otherwise with the outbox's
  7-day retention.
- A post Heimdall made comes back from Slack as the owner's message. Events
  from the owner carrying the post metadata are dropped before persistence,
  so they never become the owner's live input. The files' message cannot
  carry metadata: an owner `file_share` without a comment, made at upload
  time (`upload` true), in the post's own channel and thread, whose files
  are all recorded uploads of that one approved post, is dropped the same
  way (the IDs are recorded before completing). Never in the owner's chat
  with the bot; a later re-share of the same files is the owner's own.
  Unverified live: that Slack includes the metadata, the file IDs,
  `subtype` `file_share`, `upload` and `thread_ts` in those events.
  Slack's `file_share` reference documents `upload`, but its example shows
  no `subtype` or `thread_ts`, and the page both calls the event "no longer
  served" and says Events API deliveries continue. If a field is missing
  live, Heimdall's own file post would be read as the owner's input. The
  target and mention refusals above do not depend on it.

## How an agent calls it, per interface

1. **Slack** (owner DM or thread with Bezos). The turn's agent calls the
   tool; the card arrives in that same DM:
   `heimdall_scheduler {"action":"post_as_owner","request_id":"slack-1","target":"slack:C0123456789","text":"Standup moved to 11:00."}`
2. **Telegram** (owner's private chat with the bot). Same call; the card
   arrives in that Telegram chat. Files the agent made on the hub:
   `{"action":"post_as_owner","request_id":"tg-1","target":"U0COLLEAGUE","text":"Here is the draft.","files":["/root/agent-work/draft.docx"]}`
3. **AgentNet, an agent on the hub.** An AgentNet-launched run calls
   `heimdall sched control -socket "$CONTROL_SOCKET"` (or the MCP adapter);
   the launcher socket vouches for it; the card goes to the owner's
   registered private Heimdall chat:
   `echo '{"action":"post_as_owner","request_id":"an-1","target":"slack:C0123456789","text":"Deploy finished."}' | heimdall sched control -socket "$CONTROL_SOCKET"`
4. **AgentNet, a laptop agent sending a file.** The file must be on the
   hub, so the laptop agent sends it to the owner's hub agent as a task
   attachment:
   `agentnet task --file ./q3-summary.pdf <owner>/<hub agent> "Post this to slack:C0123456789 as me with the text: Q3 numbers are in."`
   The hub agent's run saves the attachment
   (`agentnet download --dir /root/agent-work/in <message id>`) and calls
   `post_as_owner` with that path, as in 3. It is an `agent` origin, allowed
   for this action only. (CLI syntax from the installed `agentnet` help,
   2026-11-27; check the hub's version.)
5. **T3** (with `trusted_t3.manage`). A T3 thread's agent calls the same MCP
   tool; the card goes to the bound owner's private Heimdall chat.
6. **Terminal harnesses** (standalone Claude, Codex, Pi, OMP on the hub or a
   laptop) do not call Heimdall directly. They go through AgentNet (4) or T3
   (5). A standalone control call is refused.

## Tests

Fakes only (`internal/scheduler/owner_post_test.go`,
`internal/iface/slack/owner_post_test.go`,
`internal/router/owner_post_test.go`, `internal/config`): admission per
origin, owner-person binding, file checks and copies, card content and
escaping, Yes/No in the card's chat only, expiry, replay, immediate mode,
the owner token on every post and upload with zero bot posts (also after a
store round trip), recorded file IDs, check-before-resend for text and
files, no resend when the check fails, the private failure notice, the
echo filter, off sending and filtering nothing, Yes bound to cards
delivered before it (both orders), the recorded Slack account, changed
file copies refused, a symlinked config's real directory, and resend
checks with the hub clock ahead. They do not prove live Slack behavior: metadata in events and
history visibility timing are unverified until a live window. The owner
token's scopes, including the history and read scopes, were checked live
on 2026-11-27.
