"""Catch venv console scripts and python -m pytest before test imports."""
from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import sys


def _is_pytest_invocation() -> bool:
    # This emergency detector must import NEITHER guard module: either one
    # may be absent or syntax-broken. Keep CPython option scanning in sync with
    # worklink_pytest_args, with subprocess coverage for both failure paths.
    argv = getattr(sys, "argv", None)
    original = getattr(sys, "orig_argv", None)
    if not isinstance(argv, (list, tuple)) or not argv or not isinstance(argv[0], str):
        return False
    if os.path.basename(argv[0]) in {"pytest", "py.test"}:
        return True
    if argv[0] != "-m" or not isinstance(original, (list, tuple)):
        return False
    index = 1
    while index < len(original):
        value = original[index]
        if not isinstance(value, str):
            return False
        if value == "--check-hash-based-pycs":
            index += 2
            continue
        if value == "-" or not value.startswith("-") or value.startswith("--"):
            return False
        for position, flag in enumerate(value[1:], start=1):
            if flag not in {"c", "m", "W", "X"}:
                continue
            operand = value[position + 1:]
            if not operand:
                index += 1
                if index >= len(original) or not isinstance(original[index], str):
                    return False
                operand = original[index]
            if flag == "m":
                return operand == "pytest"
            if flag == "c":
                return False
            break
        index += 1
    return False


def _chain_sitecustomize() -> None:
    own_directory = os.path.dirname(os.path.realpath(__file__))
    original_path = sys.path[:]
    try:
        sys.path[:] = [entry for entry in original_path
                       if os.path.realpath(entry) != own_directory]
        spec = importlib.machinery.PathFinder.find_spec("sitecustomize", sys.path)
        if spec is not None and spec.loader is not None:
            module = importlib.util.module_from_spec(spec)
            sys.modules["sitecustomize"] = module
            spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_path


try:
    from worklink_pytest import startup

    startup()
except BaseException as exc:
    # A broken lease installation must stop pytest, not unrelated Python tools.
    print(f"worklink pytest: startup guard failed: {exc}", file=sys.stderr, flush=True)
    if _is_pytest_invocation():
        os._exit(125)

# Preserve the venv/project hook which our PYTHONPATH entry otherwise shadows.
# Errors here retain Python's normal sitecustomize error handling.
_chain_sitecustomize()
