"""Catch venv console scripts and python -m pytest before test imports."""
from __future__ import annotations

import os
import sys

try:
    from worklink_pytest import startup

    startup()
except BaseException as exc:
    # Python normally logs sitecustomize errors and continues: that would turn
    # a broken lease installation into an unleased suite. Fail closed instead.
    print(f"worklink pytest: startup guard failed: {exc}", file=sys.stderr, flush=True)
    os._exit(125)
