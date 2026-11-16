# OpenCode 2.2.7 credential-free API contract

`openapi.json`, `probe-result.json`, and `probe.py` are unmodified copies of
the driver-supplied `.factory/chainlink-2125/contracts/` evidence captured on
2026-11-12. The probe is retained for inspection, not executed by these tests.

The driver ran plain `opencode serve --hostname 127.0.0.1 --port <unused-port>`
from @opencode/cli 2.2.7 with scratch HOME/XDG directories, an empty plugin
configuration, the seven headless-disable switches and a synthetic Basic API
password. No provider login, GitHub credentials or tracker access was used.
The server was terminated and awaited (exit 143).

Observed: authenticated info/config/session listing and session creation,
checkout location and permission preservation, server.connected SSE, and 401
for unauthenticated session/event requests. `/api/health` is 404 and `/health`
is HTML, not readiness. Scratch paths, IDs and process metadata are retained.

The shared in-process fake's prompt/command execution, durable log, messages,
inbox, cancellation and failure scenarios are **synthetic**, derived from this
OpenAPI and the retained session-contract supplement. They are not captured
provider executions. No live permission-evaluator proof is claimed: the finite
test evaluator models the inspected native core's agent-then-session last-match
semantics, including final deny before persisted project grants.

## Credential contract (#2131)

`probe-auth.py` was run against npm `@opencode/cli@2.2.7` on 2026-11-14 in
an isolated temporary HOME/XDG tree, with plugins disabled, a synthetic Basic
password, and no provider login or model request. `auth import --help` confirms
an optional file argument, stdin when omitted, and the global `--server` flag.
`auth-export.json` is the captured export after API credential creation, with
only the generated credential ID normalized. Importing these same bytes via
stdin exited 0 with empty stdout; listing returned the identical entry.
`auth-import-result.json` records the observed status/CLI results. Creating a
new active credential then deleting the old ID returned 200/204. Both
`chatgpt-browser` and `chatgpt-headless` OAuth values were accepted, including
refresh/access/expires/accountID. Unauthenticated credential export returned 401.
The fixture tokens are deliberately synthetic strings, not provider credentials;
refresh with a provider is not claimed. Re-run:

```
npm exec --yes --package=@opencode/cli@2.2.7 -- uv run --locked --all-packages --extra dev python tests/fixtures/worklink/opencode_2_2_7/probe-auth.py
```
