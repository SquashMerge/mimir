"""Test-only POSIX timeout: the ``-k <grace> <seconds> <command...>`` subset."""
from __future__ import annotations

import os
import signal
import subprocess
import sys


def signal_group(pgid: int, signum: int) -> None:
    """Signal a command group, ignoring one that has already gone.

    The group holds only this helper's own same-uid descendants, so no member can
    refuse the signal for permission. macOS nevertheless reports EPERM, not ESRCH,
    for a group whose remaining members are all unreaped zombies -- the state a
    SIGKILLed, reparented grandchild sits in until launchd reaps it.
    """
    try:
        os.killpg(pgid, signum)
    except (ProcessLookupError, PermissionError):
        pass


def main() -> int:
    flag, grace, duration, *command = sys.argv[1:]
    assert flag == "-k" and command
    child: subprocess.Popen | None = None
    pending: int | None = None

    def forward(signum: int, _frame: object) -> None:
        nonlocal pending
        pending = signum
        if child is not None:
            signal_group(child.pid, signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, forward)
    child = subprocess.Popen(command, start_new_session=True)
    try:
        if pending is not None:
            signal_group(child.pid, pending)
        try:
            status = child.wait(timeout=float(duration.removesuffix("s")))
            return status if status >= 0 else 128 - status
        except subprocess.TimeoutExpired:
            signal_group(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=float(grace.removesuffix("s")))
            except subprocess.TimeoutExpired:
                signal_group(child.pid, signal.SIGKILL)
                child.wait()
                return 137
            return 124
    finally:
        # Also reap descendants that keep inherited stdout/stderr open.
        signal_group(child.pid, signal.SIGKILL)
        child.wait()


if __name__ == "__main__":
    sys.exit(main())
