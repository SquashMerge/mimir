"""Persistent browser downloads, without sharing workers' other home state."""
from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import Mapping

from .identities import get_identities

PLAYWRIGHT_CACHE_DIR = "/var/cache/mimir-playwright"
PLAYWRIGHT_ENV = "PLAYWRIGHT_BROWSERS_PATH"


def browser_environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Use the operator's cache override, or the setup-provisioned private volume."""
    from .backends.opencode import validate_test_env
    from .compute import _validate_worker_value

    environ = os.environ if environ is None else environ
    if PLAYWRIGHT_ENV in environ:
        value = environ[PLAYWRIGHT_ENV]
    else:
        # The controller may run as a different uid from the worker. Check the
        # provisioned worker identity, not the caller's uid or os.access().
        # lstat deliberately rejects symlinks, including dangling ones.
        try:
            info = Path(PLAYWRIGHT_CACHE_DIR).lstat()
            worker_uid = get_identities().worklink_uid
        except (OSError, RuntimeError):
            return {}
        mode = stat.S_IMODE(info.st_mode)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != worker_uid
                or mode & 0o077 or mode & 0o700 != 0o700):
            return {}
        value = PLAYWRIGHT_CACHE_DIR
    result = {PLAYWRIGHT_ENV: value}
    validate_test_env(result)
    _validate_worker_value(PLAYWRIGHT_ENV, result[PLAYWRIGHT_ENV])
    return result
