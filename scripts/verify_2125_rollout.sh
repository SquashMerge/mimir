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
uv run python -m compileall -q mimir/scaffold_docker.py mimir/plugin_dockerfile.py plugins/cultivate/mimir_cultivate/setup.py plugins/cultivate/mimir_cultivate/commands.py
for file in \
    tests/test_root_dockerfile.py \
    tests/test_scaffold_docker.py \
    tests/test_worklink_tool_pins.py \
    tests/test_core_without_worklink.py \
    tests/test_plugin_skills_cli.py \
    tests/test_worklink_feature_factory.py \
    tests/test_factory_execution_config.py \
    tests/test_worklink_factory_docs.py \
    tests/test_worklink_pipeline_docs.py \
    tests/test_worklink_worker_identity.py \
    tests/test_worklink_image_identity.py \
    tests/test_worklink_daemon_rollout.py \
    plugins/cultivate/tests/test_docker_resources.py \
    plugins/cultivate/tests/test_setup.py \
    plugins/cultivate/tests/test_worklink_image_identity.py
do
    sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate uv run pytest -q "$file"
done
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
