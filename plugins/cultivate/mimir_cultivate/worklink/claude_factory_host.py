"""Foreground adapter to the installed Claude factory launcher's public exports."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import UUID

DEFAULT_ENTRYPOINT = "/opt/mimir-claude/lib/node_modules/claude-code-feature-factory/vendor/feature-factory/bin/factory.js"
CLAUDE_EXECUTABLE = "/opt/mimir-claude/bin/claude"
FACTORY_VERSION = "0.14.2"
_LIMIT = 1024 * 1024
_EXPORT_ARGS = r"""
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
const input = JSON.parse(readFileSync(0, "utf8"));
const { claudeArgs, projectMcpServers } = await import(pathToFileURL(input.launcher));
const { servers, warnings } = projectMcpServers(input.cwd);
const args = claudeArgs({request: input.request, model: input.model, mcpServers: servers},
                        input.session, input.root);
const output = JSON.stringify({args, warnings});
if (Buffer.byteLength(output) > 1048576) throw new Error("factory arguments exceed bounds");
process.stdout.write(output);
"""


def plugin_root(entrypoint: str | Path) -> Path:
    raw = Path(entrypoint)
    if not raw.is_absolute():
        raise ValueError("Claude factory entrypoint must be absolute")
    resolved = raw.resolve(strict=True)
    vendor = resolved.parent.parent
    root = vendor.parent.parent
    if (resolved != raw or resolved.name != "factory.js" or resolved.parent.name != "bin"
            or vendor.name != "feature-factory" or vendor.parent.name != "vendor"
            or not resolved.is_file()):
        raise ValueError("Claude factory entrypoint must name the stable vendored bin/factory.js")
    for directory, name in ((root, "claude-code-feature-factory"), (vendor, "feature-factory")):
        content = (directory / "package.json").read_bytes()
        if len(content) > _LIMIT:
            raise ValueError("factory package manifest exceeds bounds")
        manifest = json.loads(content)
        if not isinstance(manifest, dict) or (manifest.get("name"), manifest.get("version")) != (name, FACTORY_VERSION):
            raise ValueError(f"requires {name}@{FACTORY_VERSION}")
    launcher = root / "launcher" / "launch.js"
    if not launcher.is_file() or launcher.resolve() != launcher:
        raise ValueError("Claude factory launcher is unavailable")
    return root


def exported_arguments(entrypoint: str | Path, *, cwd: Path, request: str,
                       model: str, session: str) -> tuple[list[str], list[str]]:
    root = plugin_root(entrypoint)
    if str(UUID(session, version=4)) != session:
        raise ValueError("invalid Claude factory session UUID")
    cwd = cwd.resolve(strict=True)
    for name in (".factory.json", ".mcp.json"):
        path = cwd / name
        if path.exists() and (not path.is_file() or path.stat().st_size > _LIMIT):
            raise ValueError("factory project configuration exceeds bounds")
    payload = {"launcher": str(root / "launcher/launch.js"), "root": str(root),
               "cwd": str(cwd), "request": request, "model": model, "session": session}
    result = subprocess.run(
        ["node", "--input-type=module", "-e", _EXPORT_ARGS],
        input=json.dumps(payload).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=cwd, timeout=30, check=False,
    )
    if result.returncode or len(result.stdout) > _LIMIT:
        raise ValueError("installed Claude factory exports failed")
    output = json.loads(result.stdout)
    if not isinstance(output, dict) or set(output) != {"args", "warnings"}:
        raise ValueError("invalid Claude factory export result")
    for name in ("args", "warnings"):
        values = output[name]
        if (not isinstance(values, list) or len(values) > 4096
                or any(not isinstance(value, str) or "\x00" in value for value in values)):
            raise ValueError("invalid Claude factory export result")
    if not output["args"]:
        raise ValueError("empty Claude factory arguments")
    return output["args"], output["warnings"]


def main() -> None:
    try:
        payload = json.loads(sys.argv[1])
        cwd = Path.cwd().resolve(strict=True)
        os.environ["PWD"] = str(cwd)
        home = Path(os.environ["HOME"])
        if os.environ.get("CLAUDE_CONFIG_DIR") != str(home / ".claude"):
            raise ValueError("Claude factory requires private configuration")
        args, warnings = exported_arguments(payload["entrypoint"], cwd=cwd,
            request=payload["request"], model=payload["model"], session=payload["session"])
        secrets = [os.environ.get(name, "") for name in
                   ("CLAUDE_CODE_OAUTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY")]
        for warning in warnings:
            for secret in secrets:
                if secret:
                    warning = warning.replace(secret, "[REDACTED]")
            print(f"warning: {warning}", file=sys.stderr, flush=True)
        os.execve(CLAUDE_EXECUTABLE, [CLAUDE_EXECUTABLE, *args], dict(os.environ))
    except (KeyError, ValueError, OSError, subprocess.SubprocessError):
        sys.exit("Claude factory foreground launch failed")


if __name__ == "__main__":
    main()
