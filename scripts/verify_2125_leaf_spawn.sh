#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PATH=/usr/local/bin:/usr/bin:/bin
export PATH
unset MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED
uv sync --locked --all-packages --extra dev --extra bench
UV_NO_SYNC=1
export UV_NO_SYNC
uv run python -m compileall -q plugins/cultivate/mimir_cultivate scripts/worklink_image_identity.py
uv run pytest -q tests/test_worklink_worker_exec.py
uv run pytest -q tests/test_worklink_compute.py
uv run pytest -q tests/test_worklink_backends.py
uv run pytest -q tests/test_worklink_orchestrator.py
uv run pytest -q tests/test_worklink_worker_projection.py
uv run pytest -q tests/test_worklink_evidence.py
uv run pytest -q tests/test_worklink_reattach.py
uv run pytest -q tests/test_worklink_stop.py
uv run pytest -q tests/test_spawn_opencode.py
uv run pytest -q tests/test_spawn_caps.py
uv run pytest -q tests/test_pollers.py
uv run pytest -q tests/test_opencode_config.py
uv run pytest -q tests/test_worklink_image_identity.py
uv run pytest -q tests/test_worklink_factory_endings.py
uv run pytest -q tests/test_worklink_factory_stop_notices.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_opencode.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_setup.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_worklink_executor.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_worklink_image_identity.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_policy_consumers.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_contained_dispatch.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_worklink_daemon_leaf_spawn.py
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
# Verify optional-provider guards independently of the installed workspace matrix.
unset UV_NO_SYNC
uv sync --locked
UV_NO_SYNC=1
export UV_NO_SYNC
sh scripts/heavy_run.sh uv run pytest -q --tb=short
