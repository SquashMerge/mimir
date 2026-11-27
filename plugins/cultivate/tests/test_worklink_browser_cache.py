"""Browser cache wiring through the real builder and evidence env producers."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from mimir.scaffold_docker import render_compose_yml
from mimir_cultivate import setup
from mimir_cultivate.worklink import evidence
from mimir_cultivate.worklink.backends import opencode
from mimir_cultivate.worklink.backends.base import ComputeResult, LaunchHandle, WorkOrder, WorkSpec
from mimir_cultivate.worklink.browser_environment import browser_environment
from mimir_cultivate.worklink.compute import _enabled_child_env
from mimir_cultivate.worklink.daemon_sessions import session_environment


@pytest.fixture(autouse=True)
def owned_environment(monkeypatch):
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE", raising=False)


def leaf_spec(tmp_path, monkeypatch, test_env=None):
    monkeypatch.setattr(opencode, "model_spec_at_call_time", lambda home: "openai:test")
    monkeypatch.setattr(opencode, "resolve_daemon_model",
                        lambda values: {"providerID": "openai", "id": "test"})
    return opencode.OpenCodeBackend(test_env=test_env or {}).work_spec(
        WorkOrder(1, tmp_path, "prompt", None, 30),
        attempt=1, repo_url="url", base_ref="main", branch="branch", test_command="tools/verify",
    )


@pytest.mark.parametrize("override", [None, "/worker/browser-override"])
async def test_browser_cache_builder_and_gate_env_match(tmp_path, monkeypatch, override):
    if override:
        monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", override)
    spec = leaf_spec(tmp_path, monkeypatch, {"RUSTUP_HOME": "/opt/rustup"})
    expected = override or "/var/cache/mimir-playwright"
    assert spec.env["PLAYWRIGHT_BROWSERS_PATH"] == expected
    assert spec.backend_config["test_env"]["PLAYWRIGHT_BROWSERS_PATH"] == expected
    assert "PLAYWRIGHT_BROWSERS_PATH" in spec.backend_config["pass_env"]
    # The privileged session helper has a closed ambient env. The override must
    # reach the daemon via pass_env, not depend on helper env inheritance.
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)
    home = tmp_path / "builder-home"
    builder = session_environment(dict(spec.env), home=home, git_settings=(), credentials={})
    assert builder["PLAYWRIGHT_BROWSERS_PATH"] == expected
    assert builder["HOME"] == str(home)
    assert builder["TMPDIR"] == str(home / "tmp")
    seen = []

    class Compute:
        async def _launch_contained(self, gate_spec, argv):
            seen.append(_enabled_child_env(gate_spec, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))
            return LaunchHandle("local_subprocess", "browser-test")

        async def wait(self, handle, timeout):
            return ComputeResult(0, "", "")

        async def cleanup(self, handle):
            pass

    # Exercise both the retained test_env and older/minimal specs that have no
    # browser entry yet. The gate must supply a default independently.
    await evidence._run_compute_gate_command(
        "tools/verify", checkout=tmp_path, work_spec=spec, compute=Compute(),
    )
    gate = seen.pop()
    assert gate["PLAYWRIGHT_BROWSERS_PATH"] == builder["PLAYWRIGHT_BROWSERS_PATH"]
    assert gate["RUSTUP_HOME"] == builder["RUSTUP_HOME"]
    assert gate["HOME"] != builder["HOME"]
    await evidence._run_compute_gate_command(
        "tools/verify", checkout=tmp_path,
        work_spec=replace(spec, env={}, backend_config={"pass_env": ()}), compute=Compute(),
    )
    assert seen.pop()["PLAYWRIGHT_BROWSERS_PATH"] == "/var/cache/mimir-playwright"


@pytest.mark.parametrize("override", [None, "/worker/explicit-cache"])
def test_session_environment_and_shell_gate_browser_defaults(tmp_path, monkeypatch, override):
    if override:
        monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", override)
    expected = override or "/var/cache/mimir-playwright"
    builder = session_environment({}, home=tmp_path, git_settings=(), credentials={})
    assert builder["PLAYWRIGHT_BROWSERS_PATH"] == expected
    with evidence.shell_gate_environment() as env:
        assert env["PLAYWRIGHT_BROWSERS_PATH"] == expected
        assert Path(env["HOME"]) != tmp_path
        assert env["XDG_CACHE_HOME"] == str(Path(env["HOME"]) / ".cache")


def test_browser_cache_explicit_test_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", "/worker/ambient")
    spec = leaf_spec(tmp_path, monkeypatch, {"PLAYWRIGHT_BROWSERS_PATH": "/worker/configured"})
    builder = session_environment(dict(spec.env), home=tmp_path, git_settings=(), credentials={})
    gate = _enabled_child_env(spec, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert builder["PLAYWRIGHT_BROWSERS_PATH"] == gate["PLAYWRIGHT_BROWSERS_PATH"] == "/worker/configured"


def test_browser_cache_survives_new_session_home(tmp_path, monkeypatch):
    cache = tmp_path / "persistent-cache"
    cache.mkdir(mode=0o700)
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(cache))
    # Model an already-installed browser without network/Docker. Real Chromium
    # download reuse is an operator post-deployment check.
    marker = cache / "chromium-installed"
    marker.write_text("installed")
    for name in ("attempt-one", "attempt-two"):
        home = tmp_path / name
        home.mkdir(mode=0o700)
        env = session_environment({}, home=home, git_settings=(), credentials={})
        result = subprocess.run(
            [sys.executable, "-c",
             "import os; from pathlib import Path; "
             "assert (Path(os.environ['PLAYWRIGHT_BROWSERS_PATH']) / 'chromium-installed').read_text() == 'installed'"],
            env=env, capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, result.stderr
        home.rmdir()
    assert marker.read_text() == "installed"


@pytest.mark.parametrize("kind", ["rust", "node", "python"])
@pytest.mark.parametrize("host", ["opencode", "claude_code"])
def test_browser_cache_setup_volume_is_worker_private(kind, host):
    template = render_compose_yml(service_name="agent", web_port=8123, mode="pypi")
    doc = yaml.safe_load(setup.render_compose(template, "image:sha", kind, "repo", worklink_host=host))
    assert "playwright" in doc["volumes"]
    for service in ("agent", "init"):
        assert "playwright:/var/cache/mimir-playwright" in doc["services"][service]["volumes"]
    agent = doc["services"]["agent"]
    assert agent["depends_on"]["init"]["condition"] == "service_completed_successfully"
    assert agent["environment"]["PLAYWRIGHT_BROWSERS_PATH"] == "/var/cache/mimir-playwright"
    assert "PLAYWRIGHT_BROWSERS_PATH" in agent["environment"]["MIMIR_SHELL_PASS_ENV"].split(",")
    init = doc["services"]["init"]
    assert init["user"] == "root"
    assert " && install -d -o worklink -g worklink -m 0700 /var/cache/mimir-playwright" in init["command"][0]
    assert "chown -R 1001:1001 /var/cache/mimir-playwright" not in init["command"][0]


def test_browser_cache_setup_preserves_operator_override():
    template = yaml.safe_load("services:\n  agent:\n    build: .\n    environment: {}\n")
    template["services"]["agent"]["environment"]["PLAYWRIGHT_BROWSERS_PATH"] = "/worker/custom"
    doc = yaml.safe_load(setup.render_compose(yaml.safe_dump(template), "image:sha", "node", "repo"))
    assert doc["services"]["agent"]["environment"]["PLAYWRIGHT_BROWSERS_PATH"] == "/worker/custom"


@pytest.mark.parametrize("value", [
    "", "relative", "/worker/../cache", "/home/mimir/.cache/ms-playwright",
    "/cache\n", "/cache\t",
])
def test_browser_cache_refuses_unsafe_paths(value, tmp_path, monkeypatch):
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", value)
    with pytest.raises((ValueError, RuntimeError)):
        browser_environment()
    with pytest.raises((ValueError, RuntimeError)):
        session_environment({}, home=tmp_path, git_settings=(), credentials={})
    spec = WorkSpec(1, 1, "url", "main", "branch", "p", None, "true", "opencode", 30,
                    backend_config={"pass_env": ()})
    with pytest.raises((ValueError, RuntimeError)):
        _enabled_child_env(spec, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    with pytest.raises((ValueError, RuntimeError)):
        with evidence.shell_gate_environment():
            pass


def test_browser_cache_refuses_nul_in_requested_environment(tmp_path):
    with pytest.raises(ValueError):
        browser_environment({"PLAYWRIGHT_BROWSERS_PATH": "/cache\x00"})
    with pytest.raises((ValueError, RuntimeError)):
        session_environment({"PLAYWRIGHT_BROWSERS_PATH": "/cache\x00"},
                            home=tmp_path, git_settings=(), credentials={})
