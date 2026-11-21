# Offline vendored feature-factory 0.14.1 capture

Captured 2026-11-18 using the inspected `claude-code-feature-factory@0.14.1`
package's `vendor/feature-factory/bin/factory.js` (vendor version 0.14.1).
Entrypoint SHA-256:
`bc5bded59ec5d335cc702eddf83218e7042d22ffcfa52a93650a845c24288340`.
The package is supplied through `MIMIR_TEST_CLAUDE_FACTORY_ROOT` in verification.

`test_worklink_claude_factory_host.local_factory_capture` is the executable
capture procedure. It creates a new local repository and HOME in a temporary
directory, commits `.gitignore` and `.factory.json`, and invokes only this
vendored CLI with Node. Config is `resolve=true`, `verify=true`, and the
synthetic MCP name `fixture-mcp`; no MCP process is started.

Commands (each prefixed by `node <inspected-plugin>/vendor/feature-factory/bin/factory.js`):

1. `init chainlink-2126 --repo <local-repo> --pr-base main --mode autonomous --max-retries 7 --publishing-identity fixture-bot --json` — exit 0.
2. `lock chainlink-2126 claim --repo <local-sandbox> --session claude-code:11111111-1111-4111-8111-111111111111` — exit 0.
3. `status chainlink-2126 --repo <local-sandbox> --json` — exit 0.
4. `lock chainlink-2126 claim --repo <local-sandbox> --session claude-code:22222222-2222-4222-8222-222222222222` — nonzero.

Environment is constructed from PATH, isolated HOME, locale, disabled system/
global Git configuration, and synthetic Git author/committer identity. No
GitHub token, provider credential, tracker, network service, or live Claude
session is used. No active factory state is read or controlled.

`status.json` is command 3 stdout, unmodified, including its temporary path.
SHA-256: `d2cb066fcccf5db7e1dacb1d2afd701627c8a5294b5760b075358338148d909e`.
`lock-refusal.stderr.txt` is command 4 stderr, unmodified, including timestamp.
SHA-256: `5632d857ee856542777756f4471b3b3ea00ebf236b5a3268d6df7aeb3d54235c`.

These are local CLI contract captures, not evidence of a subscription or
publishing identity being authenticated.

## #2181: claude-code-feature-factory 0.14.2 (no re-capture needed)

The Claude Code host now pins `claude-code-feature-factory@0.14.2` and its
vendored `feature-factory` 0.14.2. These 0.14.1 captures remain the contract
fixtures because the commands they exercise are unchanged:
- The vendored `feature-factory` in `claude-code-feature-factory@0.14.2` is
  byte-identical to the published `feature-factory@0.14.2` (recursive diff of
  the extracted tarballs). The Claude plugin's `launcher/` is byte-identical
  between 0.14.1 and 0.14.2.
- `vendor/feature-factory/bin/factory.js` changes only in the evidence path:
  `writeObservedEvidence` gains `testIsVerify`, a `PLAIN_COMMAND` pattern, and
  slice evidence may share one execution between the ratified test and the
  repository verify. `core/executable.js` adds `shellRunsSameExecutable` and
  an argv timeout for that shared run. `observe/index.js` threads the flag.
  `init`, `lock claim`, `status --json` and the lock-refusal path are untouched,
  so `status.json` and `lock-refusal.stderr.txt` are what 0.14.2 emits too.
- The other changes are agent prompts, `WORKFLOW.md`, and the manifests.

Reproduce with `npm pack claude-code-feature-factory@0.14.1
claude-code-feature-factory@0.14.2 feature-factory@0.14.2`, extracting each
into separate scratch roots.

| Package | Version | Tarball SHA-256 |
| --- | --- | --- |
| claude-code-feature-factory | 0.14.1 | `1a40d0c49971c38c9a988ad079070c0aedc3e782dd5b5c2ea479bc4d8ae108cd` |
| claude-code-feature-factory | 0.14.2 | `3838ee94fbf74af6527837e5e12887f1ac8139f183e410879cde3a75a8a25d56` |

Vendored entrypoint SHA-256: 0.14.1
`bc5bded59ec5d335cc702eddf83218e7042d22ffcfa52a93650a845c24288340` (as above);
0.14.2 `2f043b2d9300fca5a67ef41a7122ae1b0e2916ff0e2ff4e7876662a82dfdd681`.
