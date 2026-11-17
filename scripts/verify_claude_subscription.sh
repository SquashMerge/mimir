#!/bin/sh
# Offline subscription-route evidence. Never invoke a live coding CLI here.
set -eu

if [ "$#" -ne 2 ] || [ "$1" != leaf ]; then
    echo "usage: sh scripts/verify_claude_subscription.sh leaf <inspected-plugin-package>" >&2
    exit 64
fi
case "$2" in
    /*) ;;
    *) echo "inspected package root must be absolute" >&2; exit 64 ;;
esac
test -r "$2/package.json"
export MIMIR_TEST_CLAUDE_FACTORY_ROOT="$2"
cd "$(dirname "$0")/.."
# Secure output readers open ancestors for reading, not merely traversal. The
# daemon's private TMPDIR has execute-only ancestors for the worker identity.
export TMPDIR=/tmp
uv sync --locked --all-packages --extra dev --extra bench --extra claude-code
uv run --locked --extra dev --extra bench --extra claude-code python -m compileall -q \
    plugins/cultivate/mimir_cultivate/claude_code.py \
    plugins/cultivate/mimir_cultivate/contained_execution.py \
    plugins/cultivate/mimir_cultivate/opencode.py \
    plugins/cultivate/mimir_cultivate/server.py \
    plugins/cultivate/mimir_cultivate/worklink

for module in \
    tests/test_claude_code.py \
    tests/test_worklink_claude_code.py \
    tests/test_worklink_backends.py \
    tests/test_worklink_compute.py \
    tests/test_worklink_worker_exec.py \
    tests/test_worklink_orchestrator.py \
    tests/test_worklink_reattach.py \
    tests/test_worklink_evidence.py \
    tests/test_worklink_checkout.py \
    tests/test_worklink_factory_state.py \
    tests/test_worklink_feature_factory.py \
    plugins/cultivate/tests/test_worklink_daemon_epics.py \
    plugins/cultivate/tests/test_worklink_executor.py \
    plugins/cultivate/tests/test_worklink_factory.py \
    plugins/cultivate/tests/test_worklink_records.py \
    plugins/cultivate/tests/test_worklink_gate_byproducts.py \
    plugins/cultivate/tests/test_worklink_factory_store.py \
    plugins/cultivate/mimir_cultivate/defaults/skills/worklink-tool-pins/tests/test_worklink_tool_pins_poller.py
do
    sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED \
        uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 "$module"
done

sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests plugins/cultivate/tests
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests plugins/cultivate/tests
