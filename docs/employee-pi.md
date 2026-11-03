# Employee Pi turns

Employee Pi uses a separate non-root OS account on the existing Pi installation.
Heimdall checks that account against root and the owner on every turn. Native
Codex, Claude and AGY remain outside this rollout; enable only `pi` in
`employee_harnesses`. Owner launches and OAuth stores are unchanged.

The root broker reads only the selected model/provider/thinking settings and
hands over the current OpenAI Codex access token, account ID and an empty refresh
field in a disposable 0700 agent directory (files 0600). It never refreshes or
writes the owner login. The owner token must have at least 30 minutes remaining;
otherwise the turn fails closed until the existing owner Pi runtime renews it.
Each employee process has a 25-minute limit, a 30-minute local credential lease,
and no warm reuse. Normal exit and failed starts remove the disposable agent.
This local lease does not shorten the provider token's cryptographic lifetime.

`deploy/contabo/employee-layout.sh install` exposes the shared policy, skills,
Pi extensions and runtime through private read-only bind mounts. Mount units
persist them across reboot; it does not reload/start any service. Nested mounts
mask the pack's `.secrets` directory and expose a public copy of the guard's
otherwise unreadable `helium10-policy.ts` dependency. No live source permissions
change. Rerun after source upgrades or atomic policy replacement; inode checks
fail closed on drift. `undo` unmounts in reverse order and removes its mount units
and enablement links, preserving files, OAuth stores and transcripts.

The employee launcher bypasses the root-forcing system Pi wrapper. It explicitly
loads the caller-scoped native MCP factory, privileged-tool guard, lifecycle
guard and confined reader. Ambient extensions, packages, prompt templates and
shell/mutation tools are disabled. `read` resolves symlinks and permits only
`/opt/assistant` and the current turn directory, excluding credential/config
filenames. Memento is absent and `MEMENTO_PROCESSING=1` is forced.

Mellanni is always approved for a verified Slack/Telegram identity. Linear also
requires `mcp.linear` from Pi access files, evaluated by the router. OAuth stores
use the old Pi caller-scope digest contract under the employee home, never root's
store. The approved server list/config is generated outside model control. Native
MCP uses employee OAuth and the server's entitlements, not an owner token or
model-supplied identity. MEL-724 signed caller tokens are not a prerequisite for
this established OAuth route; they remain separate work for other connectors.

Employees send `mcp login mellanni` (or approved `linear`) in a private DM. Slack
also verifies the live DM peer. Shared channels receive DM guidance. Callbacks
stay ephemeral, tied to the same verified caller/chat and OAuth state; they never
enter a model prompt, session or durable message queue. `employee_mcp_status`
returns deterministic private sign-in guidance when business tools are absent.
MCP discovery and calls on approved native namespaces pass through both guards;
all unrelated privileged operations remain denied. No employee OAuth store is
seeded from owner credentials.

## Validation

Run `gofmt`, `go vet ./...`, `go test ./...`, and `go test -race ./...`.
Tests cover access-only filtering/expiry, per-caller scopes, approved servers,
private employee login ingress, callback isolation and unchanged owner behavior.

The opt-in real model probe is `HEIMDALL_EMPLOYEE_LIVE=1 go test
./internal/harness -run '^TestEmployeePiLive$' -v -count=1`. It runs only the
restricted Pi child; it starts no Slack/Telegram transport. With
`HEIMDALL_EMPLOYEE_BOUNDARY=1` and a prompt requesting the four paths in the test
plus `employee_mcp_status`, it asserts the actual native session tool results
before removing the synthetic session. Run only in an explicitly authorized
production preparation window. Never sign in as the owner in employee stores.

Remaining limits: one Unix account contains all employee stores; model tool
confinement supplies caller separation, not adversarial same-UID OS isolation.
A broker/host crash can leave an access-only temporary directory; inspect stale
turn agents before rollout. Provider bearer expiry remains provider-controlled.
Authenticated employee OAuth completion and entitlements require an employee's
own first sign-in after approved rollout; synthetic probes do not establish
that end-to-end acceptance step.

## Employee Google access

Optional `employee_google` enables a root-side OAuth broker for **employees only**.
It ports the existing Pi Google API helper and tool schemas, avoiding another
server or the owner's Workspace CLI. Owner Google routing stays separate.

```json
"employee_google": {
  "enabled": true,
  "redirect_uri": "https://oauth.example.test/oauth/google/callback"
}
```

Use the **existing Pi client's registered redirect**, not the owner's localhost
Workspace client. Root Heimdall receives that client's values through
`HEIMDALL_GOOGLE_CLIENT_ID` and `HEIMDALL_GOOGLE_CLIENT_SECRET` from its own
Infisical project. No automatic fallback to legacy Google environment names or
another account exists. The config contains no secret. `check-config` validates
the redirect without opening credential stores or making network requests.

In a live private DM, an enabled Pi employee sends `google login`, opens the
consent link with their own Google account, approves every requested permission,
and pastes the **entire final address** back into the same DM within ten minutes.
Slack validates `conversations.info`'s IM flag and peer, just as for `mcp login`.
The callback may be in a new thread of that same DM. Shared chats, ambient input,
scheduled jobs, owners and other interfaces cannot initiate/complete this flow.
`google status` shows the stored connected email. `google logout` deletes only
that caller's local Google connection and pending login. Already issued bearer
tokens remain valid until provider expiry; stopping an active turn removes its
local handoff sooner. Logout does not revoke all grants of the shared OAuth app.

No new listener or endpoint is installed. Heimdall generates its own 256-bit
state and PKCE verifier. The existing Pi callback first loads **Pi's own** pending
file before any token exchange; Heimdall state has no such file, so that page
shows an expected error and leaves the code unused. Heimdall validates the pasted
origin/path, state, caller, DM, expiry, duplicate parameters and one-time use
before exchange. Unknown/embedded callbacks are scrubbed before raw Slack event
staging, historical context, router queues, logs and model input. The existing
MCP callback flow and ordinary URL handling remain unchanged.

Credentials live under `<data_dir>/google-employees/<caller-digest>/` as root-owned
0700 directories and 0600 files. Slack's digest includes its verified principal,
workspace, actor and installation; Telegram uses its verified principal. Tokens
are also bound to Google's verified userinfo email and stable Google subject.
Slack consent must match the independently verified Slack email. Cross-platform
accounts sign in independently; no old Pi token database is read or imported.

Only root refreshes. Each employee Pi turn gets `google-access.json` in its
existing disposable agent directory: email, access token, expiry and scopes only.
No refresh token, client ID or client secret exists in that schema. The path,
not its contents, is in the turn environment. Google/Infisical inherited env
variables are stripped; the Python helper has an additional minimal environment
and isolated interpreter mode. Root store components reject symlinks, non-root
ownership and unsafe permissions. Renewal rechecks userinfo identity and scopes.
A token needs 26 minutes remaining for the existing 25-minute process limit.
Google renewal failure yields the one-line sign-in guidance while other employee
business tools remain available.

The root-owned employee launcher registers exactly 19 Google tools:

- Drive: list, metadata, download/export (20 MiB limit), upload text/file, create
  folder and update content. Local file inputs/outputs resolve symlinks and stay
  inside the current turn. Updates require the app-created file marker and
  Google's app-authorization flag; no general Drive delete/share tool exists.
- Sheets: list tabs, read, write, append and create tab.
- Calendar: list calendars/events, create and patch events, including shared
  calendars the connected employee can edit. No event-delete tool is registered.
- Gmail: search, read and create an actual draft. No send tool is registered.

Scopes match old Pi exactly: `drive.file`, `drive.readonly`, `spreadsheets`,
`calendar.readonly`, `calendar.events`, `gmail.readonly`, `gmail.compose` and
`userinfo.email` (all under Google's auth scope prefix). These remain subject to
Google's consent/app/org policy and the employee's actual resource permissions.
Every write schema and dispatcher requires `confirm_write=true`, asserting the
employee explicitly requested that specific write. Tool availability never
supplies that request. The helper cannot select another account or delegated
subject, renew, enroll, inspect a token DB or fall back to a service account.

Both launcher and helper enforce the shared-channel boundary. Gmail and Calendar
operations require a DM, except shared Slack may create a Gmail draft from the
explicit request with **status-only** output: no mailbox data, draft ID, link,
recipient, subject or body. Telegram groups cannot create Gmail drafts. Drive and
Sheets remain available within caller/channel authority. Disconnected tools
return exactly: `Send \`google login\` in a DM.`

The approved Google tool names join the existing narrow launcher guard exception;
MCP provenance checks, Memento disabling, lifecycle guard, owner-tool exclusion,
confined reader and lack of shell/edit/write tools remain. The reader also denies
`google-access.json`. One shared Unix uid still means access-token isolation
against a hostile same-uid local process is outside this model-tool boundary;
refresh tokens and client secrets are protected by root ownership independently.

Install the reviewed helper/runtime payload with
`deploy/contabo/employee-google.sh install RELEASE_DIR` after employee processes
are drained. The checksummed payload includes a dedicated Python venv built from
`google-requirements.txt` (hash-pinned, copied packages, root-owned readable code).
It never includes credentials or the owner's Workspace runtime. The installer
preserves previous payload files/runtime and prints its rollback directory.
`employee-google.sh undo BACKUP_DIR` restores them, retaining rollback material.
Neither operation changes config, Infisical, mount units or services. The existing
layout installer also installs the Google helper source files on fresh layouts;
provision the dedicated runtime separately. Production activation/restart remains
a separately approved operation with scheduler clearance.

Google verification, in addition to the Go commands above:

```bash
node deploy/employee/google-runtime.test.mjs
RELEASE_DIR/runtime/google/bin/python -I -B deploy/employee/test_google_workspace.py
```

The Go tests cover caller/installation/chat/state isolation, replay and expiry,
identity/scope changes, root storage permissions, access-only files/environment,
private command ingress and callback redaction. Node tests exercise all 19 tool
registrations, shared guards, missing-login guidance, write assertions and draft
redaction. Python tests verify caller-only credentials, file confinement and
Google API request contracts with fakes. Real employee consent and a real shared
calendar mutation still require that employee's first authorized use after rollout.
