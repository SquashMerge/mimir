"""Small stdlib-only launcher installed read-only outside builder checkouts.

This enforces ordinary pytest entry points, not a sandbox against a builder
deliberately removing its environment or using Python's -I/-S startup bypasses.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import shlex
import sys

from worklink_pytest_args import pytest_arguments


def focused(arguments: list[str]) -> bool:
    """A few files or a -k selection, never xdist, repeats or timing runs."""
    try:
        arguments = [*shlex.split(os.environ.get("PYTEST_ADDOPTS", "")), *arguments]
    except ValueError:
        return False
    paths = []
    selection = False
    iterator = iter(arguments)
    for arg in iterator:
        if arg in {"-q", "-v", "-s", "-x", "--disable-warnings"}:
            continue
        if arg == "-k":
            if not next(iterator, ""):
                return False
            selection = True
        elif arg == "-o":
            if next(iterator, None) != "addopts=":
                return False
        elif arg.startswith("-k") and len(arg) > 2:
            selection = True
        elif arg.startswith("-"):
            return False
        else:
            paths.append(arg)
    return (len(paths) <= 5 and
            (selection or bool(paths) and all(p.split("::")[0].endswith(".py") for p in paths)))


def enforce(command: list[str], arguments: list[str]) -> None:
    if (not os.environ.get("MIMIR_HEAVY_LOCK_FILE")
            or os.environ.get("MIMIR_HEAVY_LOCK_HELD") or focused(arguments)):
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
    arguments = pytest_arguments(getattr(sys, "argv", None),
                                 getattr(sys, "orig_argv", None))
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
