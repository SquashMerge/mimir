# OpenCode 2.3.0 credential-free contract

Captured afresh on 2026-11-17 using npm `@opencode/cli@2.3.0` and its native
`@opencode/cli-linux-arm64@2.3.0` binary. No provider login, deployment access
or Docker was used. Factory remains pinned at 0.14.1. These results were
regenerated in this checkout, not copied from a prior attempt.

## Reproduction

From the repository root (the npm installation is ignored scratch data):

```sh
npm install --prefix .factory/2151-capture @opencode/cli@2.3.0 opencode2-feature-factory@0.14.1 feature-factory@0.14.1 --no-audit --no-fund
uv run --locked --all-packages --extra dev python tests/fixtures/worklink/opencode_2_3_0/probe.py --binary .factory/2151-capture/node_modules/@opencode/cli-linux-arm64/bin/opencode --factory-package .factory/2151-capture/node_modules/opencode2-feature-factory
uv run python tests/fixtures/worklink/opencode_2_3_0/inspect-source.py .factory/2151-capture/node_modules/@opencode/cli-linux-arm64/bin/opencode
uv run python tests/fixtures/worklink/opencode_2_3_0/compare-contract.py
```

Binary SHA-256: `5c7d89bc2d5fa5d83cc7768b813ca671368e70b8b9eda9d1ca9e6ec6b66d8ce0`.
Extraction markers are specific to this artifact, not other architectures.
The npm lock reports native tarball integrity
`sha512-1IF1NQFD5mdZ7NcU81UifdYAVKEDAP/J07ylbgE4ZATminS7pOn5SOXVzyOkuN1WLcDzXunkju0hw8hug48wTg==`
and wrapper integrity
`sha512-Q5RV8jLJ4O8bY2O8X2hkBOkgPf6+nDB9MEkz/Q/Cc4Ojd2SwUFFzEd1LzgNGKiXiS7g/9+/X4eqhrxY60fLMOQ==`.

## API and credentials

`probe.py` launches plain `opencode serve --hostname 127.0.0.1 --port <unused>`
with scratch HOME and four XDG roots, an empty plugin configuration, the seven
headless-disable switches and a synthetic Basic password. Its environment is
constructed from scratch rather than inheriting provider credentials. Each server
is terminated and awaited (exit 143), and scratch state is removed.

`openapi.json` is the complete document from `/openapi.json`, formatted as JSON.
`probe-result.json` retains response bytes as strings: info/config, session list
and creation, location/permission preservation, `server.connected` SSE, 401 without
Basic authentication, shell listing, environment replacement, queued prompt,
inbox/messages, interrupt, watermark-only log and inbox deletion. IDs, timestamps
and paths are not normalized. `/api/health` remains 404; `/health` is HTML, not
readiness. The retained prompt has `resume: false` in the body and never executes;
its message list stays empty and its inbox retains the admitted input. An initial
discarded probe used a query parameter instead, hit its inbox assertion and was
terminated/awaited; the probe was corrected against the real schema before capture.
No provider credentials were present in either run.

`auth-export.json` is the real CLI export after `POST /api/credential`, with only
the generated ID normalized. Tokens/account strings are deliberately synthetic.
`expires=0` survives export/import. Import via stdin exits zero with empty stdout;
API listing returns identical entries. `auth-import-result.json` records 401
without Basic authentication, 200 creation/replacement (`chatgpt-headless`) and
204 deletion. A read-only scratch DB query confirms the imported credential's
`method_id` is NULL: supported import stores the method in `value.methodID`.
Actual provider refresh, revocation and subscription validity are not claimed.

## Source inspection and permissions

`source-inspection.json` contains byte-exact excerpts and offsets extracted by
`inspect-source.py` from the hash-identified binary. The OAuth staleness predicate
is `s.expires!==0&&s.expires<=g+We(m)`; resolution returns the existing value unless
that predicate is true and a refresh implementation exists. Zero therefore still
disables on-use refresh.

The permission resolver concatenates `x?.permissions??BS` (agent or fallback)
then `P.permissions??[]` (session). `UN` uses `findLast`, defaulting to ask. It
rejects a final deny before appending project grants. The agent editor calls the
plugin's update callback directly on the draft; assigning `permissions` replaces
rather than merges the default list. A second scratch serve, with factory and a
tiny replacement probe enabled, confirms replacement: `replacement-probe` has
shell deny and browser-related denies but no broad allow; built-in `build` has
the broad allow. Worklink permission policy has not changed.

`factory-result.json` retains active plugin registrations, agent IDs/permissions,
skill IDs, command names and the published adapter's package requirements. It
omits large system/skill text. The installed npm package, not a workspace-modified
adapter, registers both drivers, eleven specialists and the `feature` skill and
command on 2.3.0. Its peers are `solid-js ^1.9.0` and `@opentui/solid >=0.5.17`;
there is no incompatible CLI/SDK peer requirement. npm warns about the separately
resolved latest `@opentui/core` Node engine; native server registration succeeds.
The TUI is advertised but not executed. No factory workflow or model turn runs.

## OpenAPI differences from 2.2.7

`contract-diff.json` compares all 18 Worklink operations and 252 transitively
referenced schemas, including session list and shell list/delete:

- Session creation adds a description for `parentID`, without a shape change.
- Assistant messages add optional numeric `time.firstToken`.
- Events add `session.step.first-token` and `experimental.mcp.event` to the type
  filter and `V2Event` union. First-token also joins the durable/session-log union.
- The new first-token event contains session and assistant-message IDs. MCP data
  contains id/server/name/eventId/timestamp/data/cursor and optional truncated.
- Generated version aliases shift `_37` to `_38` (constant 2) and `_47` to `_48`
  (constant 1), with corresponding references. Wire values are unchanged.

No other differences on session list/create/get/prompt/command/messages/inbox/
delete/interrupt/log, credentials, environment, info or shell list/delete. The
client preserves additive message fields and accepts generic events; regression
tests cover the additions, so no client adaptation is necessary.

## Synthetic versus observed

The offline daemon validates against this 2.3.0 schema and serves this info and
session-creation capture. Execution outcomes, commands, background shells, fault/
race controls and finite permission evaluation remain explicitly **synthetic**.
The 2.3.0 queued-input probe observes only `log.synced`; the historical live-provider
lifecycle/follow evidence remains under `opencode_2_2_7/session-lifecycle.json`, not
relabelled as a 2.3.0 provider run. Deployment/provider verification remains the
operator's post-merge gate, not evidence claimed by this capture.
