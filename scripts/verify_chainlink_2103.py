"""Run both lock-consistent core and Cultivate acceptance suites without deselection.

The historical filename is retained for existing automation; the checks contain
no epic-specific paths, environment overrides, or test deselection.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SUITES = (
    ("core", ["env", "-u", "MIMIR_PLUGINS", "uv", "run", "--locked", "--extra", "dev",
              "--extra", "bench", "pytest", "-q"]),
    ("cultivate", ["env", "-u", "MIMIR_ACCESS_CONTROL_ENFORCED", "MIMIR_PLUGINS=cultivate",
                   "uv", "run", "--locked", "--all-packages", "--extra", "dev", "--extra",
                   "bench", "pytest", "-q", "tests", "plugins/cultivate/tests"]),
)


def main() -> int:
    environment = os.environ.copy()
    failed = False
    for name, command in SUITES:
        print(f"Running {name}: {' '.join(command)}", flush=True)
        result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
        if result.returncode:
            failed = True
            print(f"{name} failed (exit {result.returncode})", file=sys.stderr, flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
