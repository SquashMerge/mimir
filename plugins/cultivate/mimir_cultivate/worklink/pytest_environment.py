"""Executor-owned pytest entry points, independent of the writable checkout."""
from __future__ import annotations

PYTEST_GUARD_ROOT = "/opt/mimir-worklink/pytest-guard"


def pytest_session_environment(path: str) -> dict[str, str]:
    return {
        "PATH": f"{PYTEST_GUARD_ROOT}/bin:{path}",
        # uv prepends .venv/bin and python -m never searches PATH. Python's
        # startup hook covers both before pytest imports/collects any tests.
        "PYTHONPATH": f"{PYTEST_GUARD_ROOT}/python",
    }
