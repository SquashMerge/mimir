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

The shared in-process fake's prompt/command execution, messages, inbox,
cancellation and failure scenarios are **synthetic**, derived from this OpenAPI
and the retained session-contract supplement, and shaped to match the recorded
session lifecycle below. Its session log serves only the `log.synced`
watermark, as the real daemon does. No live permission-evaluator proof is claimed: the finite
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

## Session lifecycle (recorded)

`session-lifecycle.json` was captured on 2026-11-16 from the live Worklink
OpenCode 2.2.7 daemon (`opencode serve` on 127.0.0.1) with one real provider
session in a scratch git checkout: prompt 1 ("Reply with the single word: ok")
ran to `idle` / `succeeded`; prompt 2 started `sleep 60` and was interrupted
(`interrupted: true`, then `false` on a repeat). It records the full `/message`
pages and `/inbox` at each step, the session's `/api/event` items, and the
session log read live (`after=0&follow=true` across the whole lifecycle) and
replayed afterwards (`after=0&follow=false`).

Observed: the session log carried only `log.synced` (seq 0 live, seq 23 on
replay) and never a `session.execution.*` event, which appeared only on
`/api/event`. The admitted input is the `user` history message with the
prompt's ID, and each execution ends with an `idle` message whose `outcome` is
the result. Worklink derives session state from that history. No credentials
are in the file; `tokens` fields are usage counts.
