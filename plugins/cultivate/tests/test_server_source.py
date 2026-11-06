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
