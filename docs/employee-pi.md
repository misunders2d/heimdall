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
