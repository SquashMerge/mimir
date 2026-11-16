#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

# The sandbox uid cannot traverse the operator's private PATH entries. Tests
# supply their own CLIs; keep only the seed's public interpreter/tool paths.
PATH=/usr/local/bin:/usr/bin:/bin
export PATH
unset MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED
uv sync --locked --all-packages --extra dev --extra bench
uv run python -c 'from pathlib import Path; paths = ["plugins/cultivate/mimir_cultivate/worklink/daemon_client.py", "tests/worklink_daemon_fixtures.py", "tests/test_worklink_daemon_contracts.py"]; [compile(Path(p).read_bytes(), p, "exec") for p in paths]'
sh scripts/heavy_run.sh uv run pytest -q tests/test_worklink_daemon_contracts.py
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
