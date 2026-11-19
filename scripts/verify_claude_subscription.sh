#!/bin/sh
# Offline subscription-route evidence. Never invoke a live coding CLI here.
set -eu

# This wrapper is also usable around the separate repository test command.
# Never use it around publishing, factory controls or deployment commands.
if [ "${1:-}" = --test-env ]; then
    shift
    exec python3 - "$@" <<'PY'
import os
import shlex
import sys

env = dict(os.environ)
identity_keys = {"user.name", "user.email"}
try:
    count = int(env.get("GIT_CONFIG_COUNT", "0"))
    if count < 0:
        raise ValueError
    retained = []
    for index in range(count):
        key = env.pop(f"GIT_CONFIG_KEY_{index}")
        value = env.pop(f"GIT_CONFIG_VALUE_{index}")
        if key.lower() not in identity_keys:
            retained.append((key, value))
    if "GIT_CONFIG_COUNT" in env:
        env["GIT_CONFIG_COUNT"] = str(len(retained))
    for index, (key, value) in enumerate(retained):
        env[f"GIT_CONFIG_KEY_{index}"] = key
        env[f"GIT_CONFIG_VALUE_{index}"] = value
    if "GIT_CONFIG_PARAMETERS" in env:
        raw = env["GIT_CONFIG_PARAMETERS"]
        parameters = shlex.shlex(raw, posix=True)
        parameters.whitespace_split = True
        parameters.commenters = ""
        retained_parameters = []
        while True:
            start = parameters.instream.tell()
            item = parameters.get_token()
            if item is None:
                break
            end = parameters.instream.tell()
            if item.partition("=")[0].lower() not in identity_keys:
                # Preserve Git's original key/value quoting, including '=' in
                # credential URL subsections and empty reset entries.
                retained_parameters.append(raw[start:end])
        env["GIT_CONFIG_PARAMETERS"] = "".join(retained_parameters)
except (KeyError, ValueError):
    sys.exit("invalid inherited Git test configuration")
if len(sys.argv) < 2:
    sys.exit("test environment wrapper requires a command")
os.execvpe(sys.argv[1], sys.argv[1:], env)
PY
fi

isolated=0
if [ "${1:-}" = --isolated-tests ]; then
    isolated=1
    shift
fi
if [ "$#" -ne 2 ]; then
    echo "usage: sh scripts/verify_claude_subscription.sh <leaf|factory|setup|spawn> <inspected-plugin-package>" >&2
    exit 64
fi
case "$1" in
    leaf|factory|setup|spawn) ;;
    *) echo "unknown subscription verification phase: $1" >&2; exit 64 ;;
esac
case "$2" in
    /*) ;;
    *) echo "inspected package root must be absolute" >&2; exit 64 ;;
esac
test -r "$2/package.json"
if [ "$isolated" = 0 ]; then
    exec sh "$0" --test-env sh "$0" --isolated-tests "$@"
fi
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

if [ "$1" = factory ] || [ "$1" = setup ] || [ "$1" = spawn ]; then
    for module in \
        tests/test_worklink_claude_factory_host.py \
        tests/test_worklink_factory_supervisor.py \
        tests/test_worklink_stop.py \
        tests/test_worklink_cli.py \
        tests/test_worklink_continuation.py \
        tests/test_web_ui.py \
        tests/test_spawn_opencode.py
    do
        sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED \
            uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 "$module"
    done
    sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate \
        uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests/test_worklink_autonomy.py
fi

if [ "$1" = setup ] || [ "$1" = spawn ]; then
    uv run --locked --extra dev --extra bench --extra claude-code python -m compileall -q \
        mimir/scaffold_docker.py mimir/plugin_dockerfile.py plugins/cultivate/mimir_cultivate/setup.py
    sh -n plugins/cultivate/mimir_cultivate/docker/base.sh
    sh -n plugins/cultivate/mimir_cultivate/docker/service.sh
    for module in \
        tests/test_root_dockerfile.py \
        tests/test_scaffold_docker.py \
        tests/test_saga_llm.py \
        tests/test_langchain_claude_code_patches.py \
        tests/test_worklink_tool_pins.py \
        tests/test_tool_pin_drift_skill.py \
        plugins/cultivate/tests/test_setup.py \
        plugins/cultivate/tests/test_docker_resources.py \
        plugins/cultivate/mimir_cultivate/defaults/skills/worklink-tool-pins/tests/test_worklink_tool_pins_poller.py
    do
        sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED \
            uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 "$module"
    done
fi

if [ "$1" = spawn ]; then
    uv run --locked --extra dev --extra bench --extra claude-code python -m compileall -q \
        plugins/cultivate/mimir_cultivate/policy.py mimir/tools/budget_gate.py
    for module in \
        tests/test_spawn_claude_code.py \
        tests/test_spawn_caps.py \
        tests/test_budget_gate_and_alias.py \
        tests/test_tool_registry.py \
        tests/test_service_tool_surface.py \
        tests/test_access_control.py \
        tests/test_information_flow.py \
        tests/test_plugins_core_policy.py \
        tests/test_tool_descriptors.py \
        tests/test_plugin_policy_parity.py \
        plugins/cultivate/tests/test_policy_tools.py \
        plugins/cultivate/tests/test_package.py
    do
        sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED \
            uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 "$module"
    done
fi

sh scripts/heavy_run.sh env -u MIMIR_PLUGINS -u MIMIR_ACCESS_CONTROL_ENFORCED uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests
sh scripts/heavy_run.sh env -u MIMIR_ACCESS_CONTROL_ENFORCED MIMIR_PLUGINS=cultivate uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests plugins/cultivate/tests
sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate MIMIR_ACCESS_CONTROL_ENFORCED=1 uv run --locked --extra dev --extra bench --extra claude-code pytest -q -n 6 tests plugins/cultivate/tests
