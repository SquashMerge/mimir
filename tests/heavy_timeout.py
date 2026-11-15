"""Test-only POSIX timeout: the ``-k <grace> <seconds> <command...>`` subset."""
from __future__ import annotations

import os
import signal
import subprocess
import sys


def main() -> int:
    flag, grace, duration, *command = sys.argv[1:]
    assert flag == "-k" and command
    child: subprocess.Popen | None = None
    pending: int | None = None

    def forward(signum: int, _frame: object) -> None:
        nonlocal pending
        pending = signum
        if child is not None:
            send(signum)

    def send(signum: int) -> None:
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, forward)
    child = subprocess.Popen(command, start_new_session=True)
    try:
        if pending is not None:
            send(pending)
        try:
            status = child.wait(timeout=float(duration.removesuffix("s")))
            return status if status >= 0 else 128 - status
        except subprocess.TimeoutExpired:
            send(signal.SIGTERM)
            try:
                child.wait(timeout=float(grace.removesuffix("s")))
            except subprocess.TimeoutExpired:
                send(signal.SIGKILL)
                child.wait()
                return 137
            return 124
    finally:
        # Also reap descendants that keep inherited stdout/stderr open.
        send(signal.SIGKILL)
        child.wait()


if __name__ == "__main__":
    sys.exit(main())
