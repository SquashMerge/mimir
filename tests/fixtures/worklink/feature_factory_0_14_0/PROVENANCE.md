# feature-factory 0.14.0 status fixture (independently captured)

- **Package:** feature-factory@0.14.0 from the public npm registry.
  - dist.integrity = 'sha512-8tXEi0WRQ2AVsieXg+hy7SXWlhFp255/i0Gbar3ZqL7Hdz35CwB6TA/XYp+hCB5yOvrUSB0xIwVRm1ZkzqgaCQ=='
  - dist.shasum = '45b877ab587a50b24c465cbdb089d96c6b8f9aa8'
  - Installed with `npm install --prefix <scratch>/pkg --no-save feature-factory@0.14.0`; the installed `package.json` reports 0.14.0.
- **CLI:** `<scratch>/pkg/node_modules/feature-factory/bin/factory.js`, run with node v24.22.1 on macOS (arm64).
- **Scratch repository:** a fresh `git init`, independent of chainlink-2125. It has three commits: a README, then `.factory.json`, then a `.gitignore` of `.factory/` and `/.factory-sandboxes/`. The base is `57da8181c3305e8c6c2dcee298db178a6112d3de`.
  - `.factory.json`: `{"resolve": "true", "verify": "true", "mcp_servers": ["fixture-mcp"]}`. `fixture-mcp` is a synthetic name; no MCP server exists or was contacted.
- **Environment:** every command ran under `env -i`, with `HOME=<scratch>/home`, `PATH=/usr/bin:/bin:<node dir>`, `LANG=C.UTF-8`, `GIT_CONFIG_GLOBAL=/dev/null`, and synthetic git author/committer `fixture <fixture@example.invalid>`.
  - No `GH_TOKEN`, `GITHUB_TOKEN`, provider key, subscription login, OpenCode or Codex auth, or tracker access was present or used.
  - The publishing identity was the synthetic account name `fixture-bot`, which is not a credential.
- **Commands and exit status:**
  1. `factory.js init 57da8181c3305e8c6c2dcee298db178a6112d3de --branch feature/fixture-mcp --publishing-identity fixture-bot --repo <REPO> --json` → exit 0.
  2. `factory.js status 57da8181c3305e8c6c2dcee298db178a6112d3de --repo <SANDBOX> --json` → exit 0. Its stdout is `status.raw.stdout.json`, kept byte-for-byte unmodified: sha256 `0267426670d2649d857f2312ccc37d7871de4b0e1872ddf31cbededb2a6755a8`, 990 bytes.
- **Normalization:** `status.normalized.json` is the same JSON with the scratch sandbox path → `<SANDBOX>`, the scratch repo path → `<REPO>`, and the run id (equal to the scratch base SHA) → `<RUN_ID>`, pretty-printed. Nothing else changed.
- **Result:** `valid: true`, `status: "running"`, top-level `mcp_servers: ["fixture-mcp"]`.
- **Isolation:** the capture ran on the operator's host, outside mimirbot. It never touched chainlink-2125's run, sandbox, lock or session. Captured by the operator's assistant on 2026-11-12.
