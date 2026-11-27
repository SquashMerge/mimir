"""Persistent browser downloads, without sharing workers' other home state."""
from __future__ import annotations

import os
from typing import Mapping

PLAYWRIGHT_CACHE_DIR = "/var/cache/mimir-playwright"
PLAYWRIGHT_ENV = "PLAYWRIGHT_BROWSERS_PATH"


def browser_environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Use the operator's cache override, or the setup-provisioned private volume."""
    from .backends.opencode import validate_test_env
    from .compute import _validate_worker_value

    environ = os.environ if environ is None else environ
    result = {PLAYWRIGHT_ENV: environ.get(PLAYWRIGHT_ENV, PLAYWRIGHT_CACHE_DIR)}
    validate_test_env(result)
    _validate_worker_value(PLAYWRIGHT_ENV, result[PLAYWRIGHT_ENV])
    return result
