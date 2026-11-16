"""Controller credential reconciliation and sanitized workload readiness gate."""
from __future__ import annotations

import os
import sys
import time

from .daemon_service import RuntimePaths


def prepare_daemon_health(environ: dict[str, str], *, paths: RuntimePaths | None = None,
                          status_timeout: float = 6) -> dict[str, object]:
    """Controller preflight: reconcile before admitting any new workload."""
    from mimir.worklink_daemon_activation import resolve_activation
    from . import daemon_health as health_module
    from ..opencode import CodexReconcileError, reconcile_codex_auth

    def inspect():
        return (health_module.daemon_health(environ) if paths is None
                else health_module.daemon_health(environ, paths=paths))

    health = inspect()
    if not resolve_activation(environ).enabled or health.get("daemon_state") != "ready":
        return health
    try:
        outcome = reconcile_codex_auth(environ, paths=paths)
    except (CodexReconcileError, OSError, ValueError, RuntimeError):
        return {**health, "ok": False, "login_state": "missing" if health.get("ok") else health.get("login_state"),
                "codex_error": "Agent Codex login unavailable; run codex login --device-auth and retry."}
    # The supervisor publishes the local login projection every five seconds.
    # Never admit against stale pre-seed status or manufacture a healthy status.
    deadline = time.monotonic() + status_timeout
    while True:
        health = inspect()
        if (health.get("ok") or health.get("daemon_error") or outcome not in {"seeded", "agent_newer"}
                or time.monotonic() >= deadline):
            return health
        time.sleep(.2)


def workload_refusal(health: dict[str, object]) -> str | None:
    """Project existing health into a bounded workload refusal reason."""
    if health.get("ok") is True:
        return None
    if health.get("daemon_error") == "bot_token_stale":
        return "bot_token_stale"
    if health.get("daemon_state") != "ready":
        return "daemon_down"
    if health.get("login_state") == "expired_no_refresh":
        return "login_expired"
    return "login_missing"


def wait_for_daemon(environ: dict[str, str], *, paths: RuntimePaths = RuntimePaths(),
                    timeout: float = 60) -> dict[str, object]:
    if not 0 <= timeout <= 60:
        raise ValueError("invalid readiness timeout")
    deadline = time.monotonic() + timeout
    while True:
        result = prepare_daemon_health(environ, paths=paths, status_timeout=min(6, max(0, deadline - time.monotonic())))
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
        print("Worklink daemon is not ready for workloads; inspect daemon health and agent Codex login (codex login --device-auth)", file=sys.stderr)
        return 1
    if result.get("daemon_warning"):
        print(result["daemon_warning"], file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
