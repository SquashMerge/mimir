# OpenCode 2.3.0 credential-free contract

Captured afresh on 2026-11-17 using npm `@opencode/cli@2.3.0` and its native
`@opencode/cli-linux-arm64@2.3.0` binary. No provider login, deployment access
or Docker was used. Factory was pinned at 0.14.1 for this historical capture. These results were
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

## #2164: question tool and child permission inheritance

Additional byte-exact excerpts were extracted on 2026-11-17 from a fresh download
of the **same hash-identified** arm64 npm artifact above, without executing it,
starting a daemon, Docker, provider access, or touching a live deployment:

```sh
uv run python tests/fixtures/worklink/opencode_2_3_0/inspect-source.py .factory/2164-capture/opencode
```

The extractor and `source-inspection.json` now also retain:

- `question_permission_and_form` (offset 177072912): the tool calls
  `o.assert({action:"question",resources:["*"],...$u(y)})` **before** `T.ask`,
  which creates a form titled `Questions`, with `metadata.kind: "question"`.
- `question_field_text` (177073636): fields use `key: qN`, `title: e.header`,
  `description: e.question`, type string/multiselect and option value/label pairs.
- `session_parent_lookup` (177168432): `pe` is the session looked up by
  `H.parentID`; the parent also supplies the location.
- `session_permission_inheritance` (177168938): creation publishes
  `permissions:H.permissions??pe?.permissions`. This is a **copy at creation**,
  not dynamic ancestor evaluation; explicit child permissions replace inheritance.
- `subagent_session_create` (177969895) and
  `command_subagent_session_create` (177824374): native tool/command child creation
  supplies `parentID`, agent and model, but **no permissions override**.

Together with the already captured agent-then-session concatenation, last-match
evaluation and final-deny-before-project-grants, this establishes that a Worklink
session's question deny follows native subagents at every depth, even if a factory
profile agent allows questions. No `daemon_config.py` or factory definitions need
changing. The fake models this exact nullish inheritance, and tests apply it to
every agent from `factory-result.json`, including `backend-builder`.

The existing full OpenAPI document supplies `Form.Info`, `Form.Reply`, pending-only
`GET /api/session/{sessionID}/form` and paginated session listing with `parentID`.
Form injection/replies and child trees in tests are **synthetic**, schema-validated
scenarios, not claims of an executed provider or factory run. No new live probe
was run for #2164. Worklink exposes only read methods for forms; a fixture-only
manual reply verifies that its reply recorder would catch auto-answer regressions.

## #2181: published factory 0.14.2 metadata refresh

Only `factory-result.json`'s `package` version and exact core dependency were
refreshed from the published `opencode2-feature-factory@0.14.2/package.json`.
Its engines and peer dependencies are unchanged. The agent/permission/registration
observations above remain the historical 0.14.1 capture, not a new daemon run.
Comparing the published adapter tarballs confirms that `plugin/`, `observe/`,
`tui/dist/` and `skills/feature/SKILL.md` are byte-identical; only the package
manifest and staged `WORKFLOW.md` changed. No provider, Docker or deployment ran.

Reproduce the package-level comparison with `npm pack feature-factory@0.14.2
opencode2-feature-factory@0.14.2 feature-factory@0.14.1
opencode2-feature-factory@0.14.1`, then extract each into separate scratch roots.
Tarball SHA-256 values:

| Package | Version | SHA-256 |
| --- | --- | --- |
| feature-factory | 0.14.1 | `eb6419b5929eb5ac4de50d212c07807f54f9a31acbf8adb1a439ab8182bbb846` |
| feature-factory | 0.14.2 | `941b516adbf383c40b466efffa6c90e139307d690859e0cc2f7dee8a23ba15dc` |
| opencode2-feature-factory | 0.14.1 | `1753d5c0301a341a1be3a723fc73907a8fd673c9959eac26d0c2869e83ff6489` |
| opencode2-feature-factory | 0.14.2 | `1d331778da20af163f5e2db5c7a1ab6c1baf27c1e69c634771aba7023d79a0ec` |
