"""Operator cleanup of retired, attempt-local runtime directories only.

The shared daemon owns its database. Per-run journal maintenance is refused,
including for historical records; retention of that database is not a run effect.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import stat
import sys
import re

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .factory_state import FactoryRunRecord


def _store_path(record: FactoryRunRecord) -> Path | None:
    sandbox = Path(record.sandbox)
    checkout = sandbox.parent.parent
    if checkout.name != "checkout":
        # Pre-worker factory runs have no sibling attempt-scoped OpenCode store.
        return None
    if (
        sandbox.name != record.run_id
        or sandbox.parent.name != ".factory-sandboxes"
        or checkout.parent.name != f"{record.issue_id}-{record.attempt}"
    ):
        raise RuntimeError("factory OpenCode store is outside the attempt boundary")
    return checkout.parent / ".factory-runtime" / "data" / "opencode" / "opencode.db"


def _prune_db(path: Path) -> dict[str, int] | None:
    raise RuntimeError("per-run OpenCode database maintenance is not supported")


def _clear_runtime(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise RuntimeError("factory runtime is not a directory")
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def clear_prunable_runtime(attempt: Path) -> None:
    """Delete worker-owned contents at TTL, leaving the outer attempt to the reaper."""
    runtime = attempt / ".factory-runtime"
    if re.fullmatch(r"[1-9][0-9]*-[1-9][0-9]*", attempt.name) is None:
        raise RuntimeError("factory runtime is outside a checkout attempt")
    from .worker_client import factory_checkout_for_path, run_factory_control

    checkout = attempt / "checkout"
    if not checkout.exists() and not checkout.is_symlink():
        # Interrupted outer deletion may already have removed checkout/. There
        # is then no safe worker cwd to reopen. Clear with existing controller
        # permissions only; permission failures retain the attempt for recovery.
        _clear_runtime(runtime)
    elif factory_checkout_for_path(checkout) is None:
        _clear_runtime(runtime)
    else:
        result = run_factory_control(
            checkout, [sys.executable, "-I", str(Path(__file__).resolve()),
                       "--clear-runtime", str(runtime)], env={}, timeout=3600, output_limit=4096,
        )
        if result.returncode:
            raise RuntimeError("factory runtime cleanup failed: " + result.stderr.decode(errors="replace")[:300])


def prune_factory_store(record: FactoryRunRecord) -> dict[str, int] | None:
    """Compatibility refusal for operators of retained process-era records."""
    raise RuntimeError("per-run OpenCode database maintenance is not supported")


if __name__ == "__main__":
    # -I script execution in the worklink worker; the controller chooses the
    # exact path from the trusted attempt layout, not from a builder prompt.
    import sys

    if len(sys.argv) != 3:
        raise SystemExit(2)
    if sys.argv[1] == "--prune":
        print(json.dumps(_prune_db(Path(sys.argv[2]))))
    elif sys.argv[1] == "--clear-runtime":
        _clear_runtime(Path(sys.argv[2]))
    else:
        raise SystemExit(2)
