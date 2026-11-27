"""Retry policy reaches rendered orders, builder sessions, and gate processes."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink import compute, evidence
from mimir_cultivate.worklink.daemon_sessions import session_environment
from mimir_cultivate.worklink.orchestrator import (
    IssueContext,
    _template_path,
    render_work_order,
)


ROOT = Path(__file__).resolve().parents[1] / "mimir_cultivate"
IDENTIFIER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DEFAULTS = {"CARGO_NET_RETRY": "5", "CARGO_HTTP_TIMEOUT": "30"}


@pytest.fixture(autouse=True)
def clean_retry_settings(monkeypatch):
    for name in DEFAULTS:
        monkeypatch.delenv(name, raising=False)


def test_order_prompt_retries_transient_network_failures_before_blocking(tmp_path):
    prompt = render_work_order(
        IssueContext(1, "Build", "Acceptance criteria", {"worklink:ready"}),
        template_path=_template_path(tmp_path),
        backend_name="opencode",
        test_command="pytest",
    )
    for rule in (
        "DNS-resolution, connection-refused/reset and timeout errors",
        "package registries, git remotes and download hosts",
        "transient network failures, not immediately missing prerequisites",
        "Retry with backoff for up to 5 minutes total from the first failure",
        "including command runtimes and waits",
        "bound individual command timeouts by the remaining budget",
        "do not restart the budget for each retry",
        "Continue normally if a retry succeeds",
        "Only a failure that persists through the 5-minute bound",
        "reason must say the network failure persisted",
        "actual elapsed retry duration",
        "Genuinely impossible or contradictory specs still block immediately",
        "WORKLINK_BLOCKED: <one-line reason>",
        "does not spend a retry attempt on it",
        "routes the issue to `worklink:blocked`",
    ):
        assert rule in prompt


def test_worklink_order_packaged_release_hash():
    releases = json.loads((ROOT / "defaults/skill_releases.json").read_text())
    expected = releases["2242-transient-network-retries"]["prompt_templates"]["worklink-order.md"]
    assert hashlib.sha256((ROOT / "prompt_templates/worklink-order.md").read_bytes()).hexdigest() == expected


def test_order_prompt_home_override_still_wins(tmp_path):
    custom = tmp_path / "prompts/worklink-order.md"
    custom.parent.mkdir()
    custom.write_text("Operator order: {title}")
    assert _template_path(tmp_path) == custom
    assert render_work_order(
        IssueContext(1, "Custom", "", set()), template_path=_template_path(tmp_path),
        backend_name="opencode", test_command="pytest",
    ) == "Operator order: Custom"


@pytest.mark.parametrize("kind,source", [
    (kind, source)
    for kind in ("session", "contained_gate", "local_worker", "shell_gate", "evidence")
    for source in ("defaults", "operator", "pass_env")
    if source != "pass_env" or kind in {"session", "contained_gate"}
])
def test_session_and_gate_environments_carry_cargo_retries(tmp_path, monkeypatch, source, kind):
    expected = DEFAULTS if source == "defaults" else {
        "CARGO_NET_RETRY": "7", "CARGO_HTTP_TIMEOUT": "20",
    }
    if source != "defaults":
        # Explicit pass_env must win even when the process has another value.
        for name in DEFAULTS:
            monkeypatch.setenv(name, "2" if source == "pass_env" else expected[name])
    passed = expected if source == "pass_env" else {}
    if kind == "session":
        env = session_environment(passed, home=tmp_path, git_settings=(), credentials={})
    elif kind == "contained_gate":
        spec = compute.WorkSpec(
            1, 1, "url", "main", "branch", "prompt", None, "pytest", "opencode", 30,
            env=passed, backend_config={"pass_env": tuple(passed)},
        )
        env = compute._enabled_child_env(spec, IDENTIFIER)
    elif kind == "local_worker":
        env = compute._local_child_env()
    elif kind == "shell_gate":
        with evidence.shell_gate_environment() as env:
            assert {name: env[name] for name in DEFAULTS} == expected
        return
    else:
        captured = {}

        def run(args, **kwargs):
            captured.update(kwargs["env"])
            return subprocess.CompletedProcess(args, 0, "", "")

        monkeypatch.setattr(evidence.subprocess, "run", run)
        evidence._run(["git", "status"])
        env = captured
    assert {name: env[name] for name in DEFAULTS} == expected


@pytest.mark.parametrize("operator", [False, True])
def test_session_helper_environment_forwards_cargo_retries(tmp_path, monkeypatch, operator):
    from mimir_cultivate.worklink import daemon_config, daemon_service, worker_exec

    expected = DEFAULTS
    if operator:
        expected = {"CARGO_NET_RETRY": "6", "CARGO_HTTP_TIMEOUT": "15"}
        for name, value in expected.items():
            monkeypatch.setenv(name, value)
    monkeypatch.setenv("UNRELATED_SECRET", "not-forwarded")
    # Replace the executor's identity/snapshot infrastructure, not environment
    # assembly. A real directory FD still exercises the helper's launch path.
    monkeypatch.setattr(worker_exec, "_validate_executor_identity", lambda request: None)
    monkeypatch.setattr(worker_exec, "_validate_checkout", lambda fd, request: None)
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(mimir_uid=os.getuid()))
    monkeypatch.setattr(daemon_service, "read_json", lambda *args, **kwargs: {})
    monkeypatch.setattr(daemon_config, "parse_launch_snapshot", lambda value: SimpleNamespace(
        bot_token="fixture-token", publishing_identity="fixture-bot",
    ))
    monkeypatch.setattr(worker_exec, "_send", lambda *args: None)
    captured = {}

    def run(args, **kwargs):
        captured.update(kwargs["env"])
        return subprocess.CompletedProcess(args, 0, b"{}", b"")

    monkeypatch.setattr(worker_exec.subprocess, "run", run)
    request = dict.fromkeys((
        "version", "op", "executor_identity", "issue", "attempt", "device", "inode",
        "path", "generation", "opencode_session_id",
    ), "fixture")
    request.update(op="session_release", path=str(tmp_path.resolve()))
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        worker_exec._handle_session(None, request, [fd])
    finally:
        os.close(fd)
    assert {name: captured[name] for name in DEFAULTS} == expected
    assert "UNRELATED_SECRET" not in captured


@pytest.mark.parametrize("operator", [False, True])
async def test_rust_gate_environment_keeps_retry_settings(tmp_path, monkeypatch, operator):
    expected = DEFAULTS
    if operator:
        expected = {"CARGO_NET_RETRY": "4", "CARGO_HTTP_TIMEOUT": "25"}
        for name, value in expected.items():
            monkeypatch.setenv(name, value)
    seen = []

    class Compute:
        async def _launch_contained(self, spec, argv):
            seen.append(compute._enabled_child_env(spec, IDENTIFIER))
            assert argv == ("/bin/sh", "-c", "cargo test")
            return compute.LaunchHandle("local_subprocess", "retry-gate")

        async def wait(self, handle, timeout):
            return compute.ComputeResult(0, "passed", "")

        async def cleanup(self, handle):
            pass

    spec = compute.WorkSpec(
        1, 1, "url", "main", "branch", "prompt", None, "cargo test", "opencode", 30,
        backend_config={"pass_env": ()},
    )
    await evidence._run_compute_gate_command(
        "cargo test", checkout=tmp_path, work_spec=spec, compute=Compute(),
    )
    assert len(seen) == 1
    assert {name: seen[0][name] for name in DEFAULTS} == expected
