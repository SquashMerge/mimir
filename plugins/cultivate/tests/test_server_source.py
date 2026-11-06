"""Mode-specific controller and executor health identity."""

from __future__ import annotations

import pytest

from mimir_cultivate import server, install_identity
from mimir_cultivate.worklink import worker_client


@pytest.mark.asyncio
@pytest.mark.parametrize("commit,worker,status", [
    ("a" * 40, "a" * 40, "matched"),
    ("a" * 40, "b" * 40, "mismatched"),
    (None, "a" * 40, "unknown"),
])
async def test_source_identity_fails_closed_without_target_head(
    tmp_path, monkeypatch, commit, worker, status,
) -> None:
    monkeypatch.setattr(server.sys, "prefix", str(tmp_path))
    monkeypatch.setenv("WORKLINK_REPO", str(tmp_path / "target"))
    if commit is not None:
        (tmp_path / "controller-source-commit").write_text(commit)
    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)

    async def identity():
        return {"mode": "source", "source_commit": worker}

    monkeypatch.setattr(worker_client, "verify_executor_identity", identity)
    app = {"check_worker_executor_health": True}
    await server.capture_controller_source_commit(app)
    health = await server.executor_health(app)
    assert health["worklink_executor_source_status"] == status
    assert health["ok"] is False if status != "matched" else "ok" not in health
    assert health["worklink_controller_source_commit"] == commit


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [None, "source", "pypi"])
async def test_legacy_workspace_fallback_never_applies_to_pypi(tmp_path, monkeypatch, mode):
    import subprocess
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture", "-c",
                    "user.email=fixture@example.com", "commit", "--allow-empty", "-m", "fixture"],
                   check=True, capture_output=True)
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    monkeypatch.setattr(server.sys, "prefix", str(tmp_path))
    monkeypatch.setenv("WORKLINK_REPO", str(repo))
    if mode is None:
        monkeypatch.delenv("MIMIR_INSTALL_MODE", raising=False)
    else:
        monkeypatch.setenv("MIMIR_INSTALL_MODE", mode)
    app = {"check_worker_executor_health": True}
    await server.capture_controller_source_commit(app)
    assert app["worklink_controller_source_commit"] == (commit if mode != "pypi" else None)


@pytest.mark.asyncio
async def test_pypi_never_honours_source_marker(tmp_path, monkeypatch):
    monkeypatch.setattr(server.sys, "prefix", str(tmp_path))
    monkeypatch.setenv("MIMIR_INSTALL_MODE", "pypi")
    monkeypatch.delenv("WORKLINK_REPO", raising=False)
    (tmp_path / "controller-source-commit").write_text("a" * 40)
    app = {"check_worker_executor_health": True}
    await server.capture_controller_source_commit(app)
    assert app["worklink_controller_identity"] is None


@pytest.mark.asyncio
async def test_verification_is_off_event_loop_at_startup_and_health(tmp_path, monkeypatch):
    import threading
    loop_thread = threading.get_ident()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", tmp_path / "absent.sock")
    monkeypatch.setattr(server.sys, "prefix", str(tmp_path))
    (tmp_path / install_identity.CONTROLLER_MARKER).touch()
    calls = []
    def verify():
        assert threading.get_ident() != loop_thread
        calls.append(True)
        return {"mode": "pypi", "distributions": {}}
    monkeypatch.setattr(install_identity, "controller_identity", verify)
    app = {"check_worker_executor_health": True}
    await server.capture_controller_source_commit(app)
    await server.executor_health(app)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_pypi_health_compares_verified_pairs(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(server.sys, "prefix", str(tmp_path))
    (tmp_path / install_identity.CONTROLLER_MARKER).write_text("{}")
    pair = {name: {"version": "1.2", "wheel": f"{name.replace('-', '_')}-1.2-py3-none-any.whl", "sha256": "a" * 64}
            for name in install_identity.PROJECTS}
    controller = {"mode": "pypi", "distributions": pair}
    monkeypatch.setattr(install_identity, "controller_identity", lambda: controller)
    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)
    executor = {"mode": "pypi", "distributions": pair}

    async def identity():
        return executor

    monkeypatch.setattr(worker_client, "verify_executor_identity", identity)
    app = {"check_worker_executor_health": True}
    await server.capture_controller_source_commit(app)
    assert (await server.executor_health(app))["worklink_install_status"] == "matched"
    executor = {"mode": "pypi", "distributions": {**pair, "mimir-agent": {**pair["mimir-agent"], "sha256": "b" * 64}}}
    health = await server.executor_health(app)
    assert health["ok"] is False
    assert health["worklink_install_status"] == "mismatched"
    assert health["worklink_executor_source_status"] == "not_applicable"
