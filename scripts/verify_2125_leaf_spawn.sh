#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PATH=/usr/local/bin:/usr/bin:/bin
export PATH
unset MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED
uv sync --locked --all-packages --extra dev --extra bench
UV_NO_SYNC=1
export UV_NO_SYNC
uv run python -m py_compile plugins/cultivate/mimir_cultivate/worklink/worker_client.py plugins/cultivate/mimir_cultivate/worklink/compute.py plugins/cultivate/mimir_cultivate/contained_execution.py
uv run pytest -q tests/test_worklink_worker_exec.py
uv run pytest -q tests/test_worklink_compute.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_opencode.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_setup.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_contained_dispatch.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_worklink_daemon_leaf_spawn.py
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
