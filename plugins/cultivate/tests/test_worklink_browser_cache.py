"""Browser cache wiring through the real builder and evidence env producers."""
from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys

import pytest
import yaml

from mimir.scaffold_docker import render_compose_yml
from mimir_cultivate import setup
from mimir_cultivate.worklink import browser_environment as browser_module
from mimir_cultivate.worklink import evidence
from mimir_cultivate.worklink.backends import opencode
from mimir_cultivate.worklink.backends.base import ComputeResult, LaunchHandle, WorkOrder, WorkSpec
from mimir_cultivate.worklink.browser_environment import browser_environment
from mimir_cultivate.worklink.compute import _enabled_child_env
from mimir_cultivate.worklink.daemon_sessions import session_environment


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE", raising=False)


@pytest.fixture
def owned_environment(monkeypatch, tmp_path):
    cache = tmp_path / "provisioned-browser-cache"
    cache.mkdir(mode=0o700)
    cache.chmod(0o700)
    monkeypatch.setattr(browser_module, "PLAYWRIGHT_CACHE_DIR", str(cache))
    monkeypatch.setattr(browser_module, "get_identities",
                        lambda: SimpleNamespace(worklink_uid=os.getuid()))
    return cache


def configure_cache(cache, state, monkeypatch):
    if state == "missing":
        cache.rmdir()
    elif state == "wrong-owner":
        monkeypatch.setattr(browser_module, "get_identities",
                            lambda: SimpleNamespace(worklink_uid=os.getuid() + 1))
    elif state.startswith("mode-"):
        cache.chmod(int(state.removeprefix("mode-"), 8))
    elif state == "symlink":
        target = cache.with_name("symlink-target")
        cache.rename(target)
        cache.symlink_to(target, target_is_directory=True)
    elif state == "file":
        cache.rmdir()
        cache.write_text("not a directory")
    elif state == "identity-unavailable":
        def unavailable():
            raise RuntimeError("required account 'worklink' is missing")
        monkeypatch.setattr(browser_module, "get_identities", unavailable)


def leaf_spec(tmp_path, monkeypatch, test_env=None):
    monkeypatch.setattr(opencode, "model_spec_at_call_time", lambda home: "openai:test")
    monkeypatch.setattr(opencode, "resolve_daemon_model",
                        lambda values: {"providerID": "openai", "id": "test"})
    return opencode.OpenCodeBackend(test_env=test_env or {}).work_spec(
        WorkOrder(1, tmp_path, "prompt", None, 30),
        attempt=1, repo_url="url", base_ref="main", branch="branch", test_command="tools/verify",
    )


@pytest.mark.parametrize("state", [
    "private", "missing", "wrong-owner", "mode-755", "mode-710", "mode-701",
    "mode-600", "symlink", "file", "identity-unavailable",
])
@pytest.mark.parametrize("override", [None, "/worker/browser-override"])
async def test_browser_cache_builder_and_gate_env_match(
    tmp_path, monkeypatch, owned_environment, state, override,
):
    configure_cache(owned_environment, state, monkeypatch)
    if override:
        monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", override)
    spec = leaf_spec(tmp_path, monkeypatch, {"RUSTUP_HOME": "/opt/rustup"})
    expected = override or (str(owned_environment) if state == "private" else None)
    assert spec.env.get("PLAYWRIGHT_BROWSERS_PATH") == expected
    assert spec.backend_config["test_env"].get("PLAYWRIGHT_BROWSERS_PATH") == expected
    assert ("PLAYWRIGHT_BROWSERS_PATH" in spec.backend_config["pass_env"]) == (expected is not None)
    if expected is None:
        assert "PLAYWRIGHT_BROWSERS_PATH" not in spec.env
    # The privileged session helper has a closed ambient env. The override must
    # reach the daemon via pass_env, not depend on helper env inheritance.
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)
    home = tmp_path / "builder-home"
    builder = session_environment(dict(spec.env), home=home, git_settings=(), credentials={})
    assert builder.get("PLAYWRIGHT_BROWSERS_PATH") == expected
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
    assert gate.get("PLAYWRIGHT_BROWSERS_PATH") == builder.get("PLAYWRIGHT_BROWSERS_PATH")
    if expected is None:
        assert "PLAYWRIGHT_BROWSERS_PATH" not in builder
        assert "PLAYWRIGHT_BROWSERS_PATH" not in gate
    assert gate["RUSTUP_HOME"] == builder["RUSTUP_HOME"]
    assert gate["HOME"] != builder["HOME"]
    await evidence._run_compute_gate_command(
        "tools/verify", checkout=tmp_path,
        work_spec=replace(spec, env={}, backend_config={"pass_env": ()}), compute=Compute(),
    )
    minimal_gate = seen.pop()
    if state == "private":
        assert minimal_gate["PLAYWRIGHT_BROWSERS_PATH"] == str(owned_environment)
    else:
        assert "PLAYWRIGHT_BROWSERS_PATH" not in minimal_gate


@pytest.mark.parametrize("state", [
    "private", "missing", "wrong-owner", "mode-755", "mode-710", "mode-701",
    "mode-600", "symlink", "file", "identity-unavailable",
])
@pytest.mark.parametrize("override", [None, "/worker/explicit-cache"])
def test_session_environment_and_shell_gate_browser_defaults(
    tmp_path, monkeypatch, owned_environment, state, override,
):
    configure_cache(owned_environment, state, monkeypatch)
    if override:
        monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", override)
    expected = override or (str(owned_environment) if state == "private" else None)
    builder = session_environment({}, home=tmp_path, git_settings=(), credentials={})
    assert builder.get("PLAYWRIGHT_BROWSERS_PATH") == expected
    with evidence.shell_gate_environment() as env:
        assert env.get("PLAYWRIGHT_BROWSERS_PATH") == expected
        if expected is None:
            assert "PLAYWRIGHT_BROWSERS_PATH" not in builder
            assert "PLAYWRIGHT_BROWSERS_PATH" not in env
        assert Path(env["HOME"]) != tmp_path
        assert env["XDG_CACHE_HOME"] == str(Path(env["HOME"]) / ".cache")


@pytest.mark.parametrize("state", ["private", "missing", "wrong-owner", "mode-755", "symlink"])
def test_browser_cache_explicit_test_env_override(tmp_path, monkeypatch, owned_environment, state):
    configure_cache(owned_environment, state, monkeypatch)
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
    # Only runtime can verify the mounted volume. Setup must not turn the
    # implicit default into an explicit override which bypasses that check.
    assert "PLAYWRIGHT_BROWSERS_PATH" not in agent["environment"]
    assert "PLAYWRIGHT_BROWSERS_PATH" in agent["environment"]["MIMIR_SHELL_PASS_ENV"].split(",")
    init = doc["services"]["init"]
    assert init["user"] == "root"
    assert " && install -d -o worklink -g worklink -m 0700 /var/cache/mimir-playwright" in init["command"][0]
    assert "chown -R 1001:1001 /var/cache/mimir-playwright" not in init["command"][0]
    if kind == "rust":
        # Main's cargo cache must coexist with the all-toolchain browser cache
        # for both worker hosts, including both private ownership init steps.
        assert "cargo" in doc["volumes"]
        for service in ("agent", "init"):
            assert "cargo:/var/cache/mimir-cargo" in doc["services"][service]["volumes"]
        assert " && install -d -o worklink -g worklink -m 0700 /var/cache/mimir-cargo" in init["command"][0]


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
