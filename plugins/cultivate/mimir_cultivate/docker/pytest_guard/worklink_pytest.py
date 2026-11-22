"""Small stdlib-only launcher installed read-only outside builder checkouts.

This enforces ordinary pytest entry points, not a sandbox against a builder
deliberately removing its environment or using Python's -I/-S startup bypasses.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import sys


def single_node(arguments: list[str]) -> bool:
    # Unknown options (including -n, --pyargs, response files, repeats, option
    # values and extra paths) conservatively take the lease. PYTEST_ADDOPTS can
    # expand even a single node into distributed/repeated work.
    if os.environ.get("PYTEST_ADDOPTS"):
        return False
    nodes = [arg for arg in arguments if arg not in {"-q", "-v", "-s", "-x"}]
    return (len(nodes) == 1 and not nodes[0].startswith("-")
            and ".py::" in nodes[0] and nodes[0].split("::")[-1].startswith("test_"))


def enforce(command: list[str], arguments: list[str]) -> None:
    if (not os.environ.get("MIMIR_HEAVY_LOCK_FILE")
            or os.environ.get("MIMIR_HEAVY_LOCK_HELD") or single_node(arguments)):
        return
    helper = Path(__file__).resolve().parents[1] / "heavy_run.sh"
    environment = {**os.environ, "MIMIR_HEAVY_TIER": "medium"}
    # exec retains the caller's streams and signal path. The existing helper
    # uses timeout's 124 for queue/run budget exhaustion, not pytest's 1..5.
    print("worklink pytest: taking medium heavy-run lease "
          "(queue/run timeout exits 124)", file=sys.stderr, flush=True)
    os.execve("/bin/sh", ["sh", str(helper), *command], environment)


def startup() -> None:
    if Path(sys.argv[0]).resolve() == Path(__file__).resolve().parents[1] / "bin/pytest":
        # This shim must resolve the project's executable/interpreter first.
        return
    arguments = None
    if Path(sys.argv[0]).name in {"pytest", "py.test"}:
        arguments = sys.argv[1:]
    elif sys.argv[0] == "-m" and "-m" in sys.orig_argv:
        index = sys.orig_argv.index("-m")
        if sys.orig_argv[index + 1:index + 2] == ["pytest"]:
            arguments = sys.orig_argv[index + 2:]
    if arguments is not None:
        enforce([sys.executable, "-m", "pytest", *arguments], arguments)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    path = os.pathsep.join(
        entry for entry in os.environ.get("PATH", "").split(os.pathsep)
        if Path(entry).resolve() != root / "bin"
    )
    # A bare pytest in a uv-managed checkout should use that checkout's tests,
    # never the executor's dependency set.
    local = Path.cwd() / ".venv/bin/pytest"
    target = str(local) if local.is_file() else shutil.which("pytest", path=path)
    if target is None:
        print("worklink pytest: no project pytest executable found", file=sys.stderr)
        sys.exit(127)
    command = [target, *sys.argv[1:]]
    enforce(command, sys.argv[1:])
    os.execv(target, command)
