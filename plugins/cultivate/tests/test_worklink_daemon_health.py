from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest

from mimir_cultivate.worklink import daemon_health as health
from mimir_cultivate.worklink import daemon_gate as gate
from mimir_cultivate.worklink.daemon_service import RuntimePaths
from mimir_cultivate.worklink.identities import WorklinkIdentities

NOW = 1800000000000


def native(**updates):
    return {"type": "oauth", "methodID": "chatgpt-headless", "access": "synthetic-access",
            "refresh": "synthetic-refresh", "expires": NOW + 60000, "metadata": {}, **updates}


def database(path, rows):
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE credential (id TEXT PRIMARY KEY, integration_id TEXT, label TEXT NOT NULL, value TEXT NOT NULL, connector_id TEXT, method_id TEXT, active INTEGER, time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL)")
        for index, (value, method, active) in enumerate(rows):
            connection.execute("INSERT INTO credential VALUES (?, 'openai', 'native', ?, NULL, ?, ?, 1, 1)",
                               (str(index), value, method, active))
    path.chmod(0o600)


@pytest.mark.parametrize(("updates", "expected"), [
    ({}, "locally_unexpired"), ({"refresh": None}, "locally_unexpired"),
    ({"expires": NOW + 5001}, "locally_unexpired"),
    ({"expires": NOW + 5000}, "refreshable_unverified"),
    ({"expires": NOW - 1}, "refreshable_unverified"),
    ({"expires": NOW - 1, "refresh": ""}, "expired_no_refresh"),
    ({"expires": 0}, "zero_expiry"), ({"expires": True}, "malformed"),
    ({"expires": -1}, "malformed"), ({"expires": "123"}, "malformed"),
    ({"expires": 1.2}, "malformed"), ({"access": ""}, "malformed"),
    ({"access": None}, "malformed"), ({"type": "api"}, "malformed"),
    ({"methodID": "browser"}, "malformed"), ({"refresh": {}}, "malformed"),
])
def test_native_local_state_matrix_read_only(tmp_path, updates, expected):
    path = tmp_path / "opencode.db"
    database(path, [(json.dumps(native(**updates)), "chatgpt-headless", 1)])
    before = path.read_bytes()
    result = health.inspect_native_login(path, uid=os.getuid(), now=NOW)
    assert result.state == expected
    assert path.read_bytes() == before
    assert "synthetic" not in repr(result)


@pytest.mark.parametrize(("rows", "expected"), [
    ([], "missing"), ([("{}", "chatgpt-headless", 0)], "inactive"),
    ([("{}", "browser", 1)], "malformed"),
    ([("not-json", "chatgpt-headless", 1)], "malformed"),
    ([("x" * 65537, "chatgpt-headless", 1)], "malformed"),
    ([(json.dumps(native()), "chatgpt-headless", 1)] * 2, "malformed"),
])
def test_native_row_bounds_and_inactive(tmp_path, rows, expected):
    path = tmp_path / "opencode.db"
    database(path, rows)
    assert health.inspect_native_login(path, uid=os.getuid(), now=NOW).state == expected


def test_native_missing_table_unavailable_and_no_follow(tmp_path):
    path = tmp_path / "opencode.db"
    assert health.inspect_native_login(path, uid=os.getuid()).state == "missing"
    assert not path.exists()
    with sqlite3.connect(path):
        pass
    path.chmod(0o600)
    assert health.inspect_native_login(path, uid=os.getuid()).state == "missing"
    path.write_bytes(b"not a database")
    assert health.inspect_native_login(path, uid=os.getuid()).state == "unavailable"
    path.rename(tmp_path / "real")
    path.symlink_to(tmp_path / "real")
    assert health.inspect_native_login(path, uid=os.getuid()).state == "unavailable"


def status(**updates):
    return {"version": 1, "launch_generation": str(uuid4()), "checked_at_ms": NOW,
            "daemon_state": "ready", "daemon_version": "2.2.7", "login_state": "locally_unexpired",
            "login_expires_at_ms": NOW + 60000, "telemetry_state": "off", **updates}


@pytest.mark.parametrize("updates", [
    {"version": 2}, {"version": True}, {"extra": "refused"}, {"launch_generation": "invalid"},
    {"checked_at_ms": NOW - 15001}, {"checked_at_ms": NOW + 5001}, {"checked_at_ms": True},
    {"daemon_state": "unknown"}, {"login_state": "valid"}, {"telemetry_state": "on"},
    {"daemon_version": "x" * 129}, {"daemon_version": "2.2.6"}, {"login_expires_at_ms": False},
])
def test_status_rejects_invalid_stale_future_and_version(updates):
    with pytest.raises(ValueError):
        health.parse_status(status(**updates), now=NOW)


@pytest.mark.parametrize("age", [-5000, 0, 15000])
def test_status_freshness_edges(age):
    payload = status(checked_at_ms=NOW - age)
    assert health.parse_status(payload, now=NOW) == payload


def fixture_status(tmp_path, monkeypatch, *, login="locally_unexpired"):
    paths = RuntimePaths(tmp_path / "daemon", tmp_path / "run")
    paths.status.parent.mkdir(parents=True, mode=0o755)
    paths.status.parent.chmod(0o755)
    paths.snapshot.parent.mkdir(parents=True, mode=0o700)
    payload = status(login_state=login)
    paths.status.write_text(json.dumps(payload))
    paths.status.chmod(0o644)
    token = "bot-" + "private-fragment"
    snapshot = {"version": 1, "launch_generation": payload["launch_generation"],
                "bot_token": token, "publishing_identity": "publisher"}
    paths.snapshot.write_text(json.dumps(snapshot))
    paths.snapshot.chmod(0o600)
    monkeypatch.setattr(health, "now_ms", lambda: NOW)
    monkeypatch.setattr(health, "resolve_activation", lambda env: SimpleNamespace(enabled=True))
    ids = WorklinkIdentities(os.getuid(), os.getuid(), os.getgid())
    env = {"GITHUB_TOKEN": token, "MIMIR_FACTORY_PUBLISHING_IDENTITY": "publisher"}
    return paths, ids, env


@pytest.mark.parametrize("login", sorted(health.LOGIN_STATES))
def test_gate_local_login_policy(tmp_path, monkeypatch, login):
    paths, ids, env = fixture_status(tmp_path, monkeypatch, login=login)
    result = health.daemon_health(env, paths=paths, identities=ids)
    assert result["ok"] == (login in health.ALLOWED_LOGIN)
    assert result["login_state"] == login
    assert ("daemon_warning" in result) == (login == "refreshable_unverified")
    assert env["GITHUB_TOKEN"] not in json.dumps(result)
    monkeypatch.setattr(gate, "daemon_health", lambda *args, **kwargs: result)
    assert gate.wait_for_daemon(env, paths=paths, timeout=0) == result


@pytest.mark.parametrize("change", ["token", "identity", "generation", "invalid"])
def test_effective_configuration_private_stale_comparison(tmp_path, monkeypatch, change):
    paths, ids, env = fixture_status(tmp_path, monkeypatch)
    if change == "token":
        env["GITHUB_TOKEN"] = "new-private-value"
    elif change == "identity":
        env["MIMIR_FACTORY_PUBLISHING_IDENTITY"] = "new-publisher"
    elif change == "invalid":
        env["GH_TOKEN"] = "conflicting-private-value"
    else:
        payload = json.loads(paths.status.read_text())
        payload["launch_generation"] = str(uuid4())
        paths.status.write_text(json.dumps(payload))
    result = health.daemon_health(env, paths=paths, identities=ids)
    assert result["ok"] is False
    assert result["daemon_error"] in {"bot_token_stale", "daemon_status_unavailable"}
    assert not any(value in json.dumps(result) for value in env.values())


def test_host_file_edit_not_watched(tmp_path, monkeypatch):
    paths, ids, env = fixture_status(tmp_path, monkeypatch)
    (tmp_path / "compose.env").write_text("GITHUB_TOKEN=host-only-edit")
    assert health.daemon_health(env, paths=paths, identities=ids)["ok"] is True


def test_disabled_health_and_gate_never_read_credentials(monkeypatch):
    monkeypatch.setattr(health, "resolve_activation", lambda env: SimpleNamespace(enabled=False, reason="not_installed"))
    monkeypatch.setattr(health, "read_json", lambda *args, **kwargs: pytest.fail("disabled credential access"))
    assert health.daemon_health({}) == {"daemon_enabled": False, "daemon_reason": "not_installed"}
    assert gate.wait_for_daemon({}, timeout=0)["daemon_enabled"] is False


def test_status_size_bound_and_sanitized_exception(tmp_path, monkeypatch):
    paths, ids, env = fixture_status(tmp_path, monkeypatch)
    paths.status.write_text(" " * 16385)
    result = health.daemon_health(env, paths=paths, identities=ids)
    assert result == {"daemon_enabled": True, "ok": False, "daemon_error": "daemon_status_unavailable"}


@pytest.mark.asyncio
async def test_health_hook_preserves_executor_and_sanitizes_errors(tmp_path, monkeypatch):
    from mimir_cultivate import server
    from mimir_cultivate.worklink import worker_client

    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)
    async def unavailable():
        raise RuntimeError("must-not-export-private-value")
    monkeypatch.setattr(worker_client, "verify_executor_identity", unavailable)
    monkeypatch.setattr(health, "daemon_health", lambda env: {"daemon_enabled": False, "daemon_reason": "not_installed"})
    result = await server.executor_health({"check_worker_executor_health": True})
    assert result["ok"] is False
    assert result["worklink_install_status"] == "mismatched"
    assert result["daemon_enabled"] is False
    assert "must-not-export" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(("version", "expected"), [("2.2.7", "ready"), ("2.2.6", "version_mismatch"),
                                                    ("not-a-version-private", "version_mismatch")])
async def test_authenticated_info_status_publication(tmp_path, monkeypatch, version, expected):
    from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault
    from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient

    paths = RuntimePaths(tmp_path / "daemon", tmp_path / "run")
    paths.daemon.mkdir(mode=0o700)
    paths.status.parent.mkdir(parents=True, mode=0o755)
    generation = str(uuid4())
    launch = paths.daemon / "launch.json"
    launch.write_text(json.dumps({"version": 1, "launch_generation": generation, "telemetry_state": "off"}))
    launch.chmod(0o600)
    ids = WorklinkIdentities(os.getuid(), os.getuid(), os.getgid())
    # No privileged filesystem operations: fixture ownership remains unchanged.
    monkeypatch.setattr(os, "fchown", lambda *args: None)
    async with FakeOpenCodeDaemon() as fake:
        fake.faults[("GET", "/api/info")].append(SyntheticFault(body=json.dumps({"version": version}).encode()))
        paths.api.write_text(json.dumps({"version": 1, "url": "http://127.0.0.1:4096", "username": "opencode",
                                        "password": fake.password}))
        paths.api.chmod(0o600)
        def fixture_client(**kwargs):
            assert kwargs["base_url"] == "http://127.0.0.1:4096"
            return OpenCodeDaemonClient(**{**kwargs, "base_url": fake.base_url})
        monkeypatch.setattr(health, "OpenCodeDaemonClient", fixture_client)
        await health.inspect_and_publish(paths, ids)
        result = health.parse_status(json.loads(paths.status.read_text()))
        assert result["daemon_state"] == expected
        assert result["daemon_version"] == (None if version == "not-a-version-private" else version)
        assert result["login_state"] == "missing"
        assert fake.password not in paths.status.read_text()
        assert "not-a-version-private" not in paths.status.read_text()
        assert [request[1] for request in fake.requests] == ["/api/info"]
        await health.inspect_and_publish(paths, ids, down=True)
        assert json.loads(paths.status.read_text())["daemon_state"] == "down"
        assert len(fake.requests) == 1


def test_gate_readiness_wait_is_bounded_without_stopping_daemon(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(gate.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(gate.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    count = []
    def down(env, **kwargs):
        count.append(True)
        return {"daemon_enabled": True, "ok": False, "daemon_state": "down"}
    monkeypatch.setattr(gate, "daemon_health", down)
    assert gate.wait_for_daemon({})["ok"] is False
    assert clock[0] == 60 and len(count) == 61
