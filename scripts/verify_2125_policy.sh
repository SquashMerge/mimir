#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

# The sandbox's inherited PATH can contain inaccessible deployment homes.
PATH=/usr/local/bin:/usr/bin:/bin
export PATH
unset MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED
uv run python -m py_compile mimir/worklink_daemon_activation.py plugins/cultivate/mimir_cultivate/worklink/daemon_config.py
uv run pytest -q tests/test_worklink_daemon_activation.py
env MIMIR_PLUGINS=cultivate uv run pytest -q plugins/cultivate/tests/test_worklink_daemon_config.py
uv run pytest -q tests/test_otel_credentials.py
uv run pytest -q tests/test_otel_redaction.py
sh scripts/heavy_run.sh uv run pytest -q --tb=short
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --extra dev --extra bench pytest -q -n 6
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --extra dev --extra bench pytest -q -n 6
