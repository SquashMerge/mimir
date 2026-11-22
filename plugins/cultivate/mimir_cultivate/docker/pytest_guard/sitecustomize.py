"""Catch venv console scripts and python -m pytest before test imports."""
from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import sys


def _is_pytest_invocation() -> bool:
    # Keep this fallback independent of worklink_pytest: its import can fail.
    # Embedded interpreters need not expose normal argv/orig_argv sequences.
    argv = getattr(sys, "argv", None)
    if isinstance(argv, (list, tuple)) and argv and isinstance(argv[0], str):
        if os.path.basename(argv[0]) in {"pytest", "py.test"}:
            return True
    original = getattr(sys, "orig_argv", None)
    if isinstance(original, (list, tuple)):
        for index, value in enumerate(original[:-1]):
            if value == "-m":
                return original[index + 1] == "pytest"
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
