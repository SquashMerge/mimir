"""Small stdlib-only launcher installed read-only outside builder checkouts.

This enforces ordinary pytest entry points, not a sandbox against a builder
deliberately removing its environment or using Python's -I/-S startup bypasses.
"""
from __future__ import annotations

import os
import configparser
from pathlib import Path
import shlex
import shutil
import sys
import tomllib

from worklink_pytest_args import pytest_arguments


def configured_options(arguments: list[str]) -> list[str]:
    """Account for xdist/repeat options inherited from repository configuration."""
    override = None
    for index, arg in enumerate(arguments[:-1]):
        if arg in {"-o", "--override-ini"} and arguments[index + 1].startswith("addopts="):
            override = arguments[index + 1].split("=", 1)[1]
    if override is not None:
        return shlex.split(override)
    for directory in (Path.cwd(), *Path.cwd().parents):
        for name, section in (("pytest.ini", "pytest"), (".pytest.ini", "pytest"),
                              ("pyproject.toml", ""), ("tox.ini", "pytest"), ("setup.cfg", "tool:pytest")):
            path = directory / name
            if not path.is_file():
                continue
            if not section:
                options = tomllib.loads(path.read_text()).get("tool", {}).get("pytest", {}).get("ini_options")
                if options is not None:
                    return shlex.split(options.get("addopts", ""))
            else:
                config = configparser.ConfigParser(interpolation=None)
                config.read(path)
                if config.has_section(section) or name in {"pytest.ini", ".pytest.ini"}:
                    return shlex.split(config.get(section, "addopts", fallback=""))
    return []


def focused_run(arguments: list[str]) -> bool:
    """One/few files or a -k selection, never xdist/repeats/unknown options."""
    try:
        options = configured_options(arguments) + shlex.split(os.environ.get("PYTEST_ADDOPTS", "")) + arguments
    except (OSError, ValueError, configparser.Error):
        return False
    files = 0
    directories = 0
    selection = False
    index = 0
    while index < len(options):
        arg = options[index]
        index += 1
        if arg in {"-q", "-v", "-vv", "-s", "-x", "-ra", "--disable-warnings", "--no-header", "--no-summary"}:
            continue
        if arg in {"-k", "-o", "--override-ini", "--junitxml", "--junit-xml", "--basetemp", "--tb", "--capture"}:
            if index == len(options):
                return False
            value = options[index]
            index += 1
            if arg == "-k":
                if not value.strip():
                    return False
                selection = True
            elif arg in {"-o", "--override-ini"} and not value.startswith(("addopts=", "junit_family=")):
                return False
            continue
        if arg.startswith(("--junitxml=", "--junit-xml=", "--basetemp=", "--tb=", "--capture=")):
            continue
        if arg.startswith("-") or arg.startswith("@"):
            return False
        if arg.split("::")[0].endswith(".py"):
            files += 1
        else:
            directories += 1
    return files <= 5 and (selection or (files > 0 and directories == 0))


def enforce(command: list[str], arguments: list[str]) -> None:
    if (not os.environ.get("MIMIR_HEAVY_LOCK_FILE")
            or os.environ.get("MIMIR_HEAVY_LOCK_HELD") or focused_run(arguments)):
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
