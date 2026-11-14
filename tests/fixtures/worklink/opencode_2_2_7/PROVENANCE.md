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
