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
