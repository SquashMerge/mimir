#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PATH=/usr/local/bin:/usr/bin:/bin
export PATH
unset MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED
uv sync --locked --all-packages --extra dev --extra bench --reinstall-package mimir-agent --reinstall-package mimir-cultivate
UV_NO_SYNC=1
PYTEST_XDIST_AUTO_NUM_WORKERS=6
export UV_NO_SYNC PYTEST_XDIST_AUTO_NUM_WORKERS
uv run python -m compileall -q plugins/cultivate/mimir_cultivate mimir/scaffold_docker.py scripts/factory_park.py scripts/factory_resume.py
for file in \
    tests/test_worklink_feature_factory.py \
    tests/test_worklink_backends.py \
    tests/test_worklink_orchestrator.py \
    tests/test_worklink_factory_supervisor.py \
    tests/test_worklink_factory_state.py \
    tests/test_worklink_factory_endings.py \
    tests/test_worklink_factory_stop_notices.py \
    tests/test_factory_execution_config.py \
    tests/test_opencode_config.py \
    tests/test_worklink_worker_exec.py \
    tests/test_worklink_worker_parity.py \
    tests/test_worklink_worker_identity.py \
    tests/test_worklink_compute.py \
    tests/test_worklink_evidence.py \
    tests/test_worklink_stop.py \
    tests/test_worklink_reattach.py \
    tests/test_worklink_cli.py \
    tests/test_worklink_retained_scope.py \
    tests/test_worklink_remediation_e2e.py \
    tests/test_worklink_autonomy.py \
    tests/test_worklink_tool_pins.py \
    tests/test_worklink_factory_docs.py \
    tests/test_root_dockerfile.py \
    tests/test_scaffold_docker.py \
    tests/test_factory_park_scripts.py
do
    env MIMIR_PLUGINS=cultivate uv run pytest -q "$file"
done
for file in \
    plugins/cultivate/tests/test_worklink_factory.py \
    plugins/cultivate/tests/test_worklink_factory_store.py \
    plugins/cultivate/tests/test_setup.py \
    plugins/cultivate/tests/test_worklink_records.py \
    plugins/cultivate/tests/test_worklink_executor.py \
    plugins/cultivate/tests/test_contained_dispatch.py \
    plugins/cultivate/tests/test_opencode.py \
    plugins/cultivate/tests/test_worklink_daemon_epics.py
do
    env MIMIR_PLUGINS=cultivate uv run pytest -q "$file"
done
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
unset UV_NO_SYNC
uv sync --locked
UV_NO_SYNC=1
export UV_NO_SYNC
sh scripts/heavy_run.sh uv run pytest -q --tb=short
