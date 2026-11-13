"""Credential-free workload gate. Refusal never stops the daemon service."""
from __future__ import annotations

import os
import sys
import time

from .daemon_health import daemon_health
from .daemon_service import RuntimePaths


def wait_for_daemon(environ: dict[str, str], *, paths: RuntimePaths = RuntimePaths(),
                    timeout: float = 60) -> dict[str, object]:
    if not 0 <= timeout <= 60:
        raise ValueError("invalid readiness timeout")
    deadline = time.monotonic() + timeout
    while True:
        result = daemon_health(environ, paths=paths)
        if not result.get("daemon_enabled") or result.get("ok"):
            return result
        # Login needs operator intervention, not a daemon restart. Wait only
        # while readiness itself may still be converging.
        if result.get("daemon_state") == "ready" or time.monotonic() >= deadline:
            return result
        time.sleep(min(1, max(0, deadline - time.monotonic())))


def main() -> int:
    result = wait_for_daemon(dict(os.environ))
    if result.get("daemon_enabled") and not result.get("ok"):
        print("Worklink daemon is not ready for workloads; inspect daemon health and native login", file=sys.stderr)
        return 1
    if result.get("daemon_warning"):
        print(result["daemon_warning"], file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
