"""Offline credential reconciliation against the pinned credential wire contract."""
from __future__ import annotations

import asyncio
import base64
import copy
import fcntl
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace
import uuid

from aiohttp import web
import pytest

from mimir_cultivate import opencode
from mimir_cultivate.worklink import daemon_health, daemon_gate
from mimir_cultivate.worklink.daemon_service import RuntimePaths
from mimir_cultivate.worklink.identities import WorklinkIdentities
from tests.worklink_daemon_fixtures import FIXTURES, OpenCodeSchema


def jwt(exp):
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return "synthetic." + payload + "." + uuid.uuid4().hex


@pytest.fixture
async def stores(tmp_path, monkeypatch, aiohttp_server):
    root = FIXTURES / "opencode_2_2_7"
    template = json.loads((root / "auth-export.json").read_text())[0]
    schema = OpenCodeSchema()
    fixture = SimpleNamespace(entries=[], writes=0, reads=0, locked=[], corrupt=None, validate_export=True)
    fixture.path = tmp_path / ".codex/auth.json"
    fixture.path.parent.mkdir(mode=0o700)
    fixture.lock = fixture.path.with_name("auth.json.lock")
    fixture.store = tmp_path / "daemon-store.json"
    fixture.password = "synthetic-" + "api-password"
    fixture.secret = "runtime-" + uuid.uuid4().hex + "-refresh"
    fixture.env = {"HOME": str(tmp_path)}
    fixture.document = {"auth_mode": "chatgpt", "OPENAI_API_KEY": None, "unknown": {"keep": True},
        "tokens": {"access_token": jwt(1900000000), "refresh_token": fixture.secret,
                   "id_token": "retain-id", "account_id": "synthetic-account",
                   "oauth_provenance": {"issuer": "https://auth.openai.com", "client_id": "retained"},
                   "login_owner_id": "retained-owner", "future": [1, 2]}, "last_refresh": "old"}

    def save(document=None):
        fixture.path.write_text(json.dumps(document or fixture.document))
        fixture.path.chmod(0o600)

    def credential(exp, **updates):
        entry = copy.deepcopy(template)
        entry["id"] = "fixture-" + uuid.uuid4().hex
        entry["value"].update(access=jwt(exp), refresh="rotated-" + fixture.secret, expires=exp * 1000)
        entry["value"].update(updates)
        return entry

    def lock_held():
        with fixture.lock.open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
            return False

    async def handle(request):
        expected = "Basic " + base64.b64encode(("opencode:" + fixture.password).encode()).decode()
        if request.headers.get("Authorization") != expected:
            return web.Response(status=401, text=fixture.secret)
        fixture.locked.append(lock_held())
        if request.method == "GET":
            fixture.reads += 1
            if fixture.validate_export:
                for entry in fixture.entries:
                    schema.named("Credential.Entry", entry)
            return web.json_response({"data": fixture.entries})
        fixture.writes += 1
        if request.method == "DELETE":
            fixture.entries[:] = [e for e in fixture.entries if e["id"] != request.match_info["id"]]
            fixture.store.write_text(json.dumps(fixture.entries))
            return web.Response(status=204)
        payload = await request.json()
        schema.named("Credential.CreateInput", payload)
        for entry in fixture.entries:
            if entry["integrationID"] == payload["integrationID"]:
                entry["active"] = False
        entry = {"id": "created-" + uuid.uuid4().hex, "integrationID": payload["integrationID"],
                 "label": payload["label"], "active": payload["activate"], "value": payload["value"]}
        fixture.entries.append(entry)
        fixture.store.write_text(json.dumps(fixture.entries))
        returned = copy.deepcopy(entry)
        if fixture.corrupt:
            fixture.corrupt(returned)
        return web.json_response({"data": returned})

    app = web.Application()
    app.router.add_route("*", "/api/credential", handle)
    app.router.add_delete("/api/credential/{id}", handle)
    server = await aiohttp_server(app)
    paths = RuntimePaths(tmp_path / "daemon", tmp_path / "run")
    paths.controller_api.parent.mkdir(parents=True, mode=0o700)
    paths.controller_api.write_text(json.dumps({"version": 1, "url": str(server.make_url("/")).rstrip("/"),
        "username": "opencode", "password": fixture.password}))
    paths.controller_api.chmod(0o600)
    monkeypatch.setattr(daemon_health, "DAEMON_URL", str(server.make_url("/")).rstrip("/"))
    ids = WorklinkIdentities(os.getuid(), os.getuid() + 1, os.getgid())
    fixture.paths, fixture.ids = paths, ids
    fixture.save, fixture.credential, fixture.lock_held = save, credential, lock_held

    async def reconcile():
        return await asyncio.to_thread(opencode.reconcile_codex_auth, fixture.env, paths=paths, identities=ids)

    fixture.reconcile = reconcile
    save()
    return fixture


async def test_seed_supported_api_only_and_secret_nowhere_else(stores, tmp_path, caplog, capsys):
    s = stores
    assert await s.reconcile() == "seeded"
    assert len(s.entries) == 1 and s.entries[0]["active"]
    value = s.entries[0]["value"]
    assert value == {"type": "oauth", "methodID": "chatgpt-browser", "access": s.document["tokens"]["access_token"],
                     "refresh": s.secret, "expires": 1900000000000, "metadata": {"accountID": "synthetic-account"}}
    assert all(s.locked)
    assert s.secret not in caplog.text + str(capsys.readouterr())
    for path in tmp_path.rglob("*"):
        if path.is_file() and path not in {s.path, s.store}:
            assert s.secret.encode() not in path.read_bytes()
    assert not list(tmp_path.rglob("*.db"))
    assert not list(s.path.parent.glob(".auth-*"))


async def test_daemon_newer_preserves_provenance_owner_and_every_extra_key_under_lock(stores, monkeypatch):
    s = stores
    s.entries = [s.credential(1900000100)]
    before = s.path.read_bytes()
    inode = s.path.stat().st_ino
    atomic = opencode._atomic_auth_write

    def observed(path, document):
        assert s.lock_held(), "write-back must hold codex-plus's flock"
        assert path.read_bytes() == before
        atomic(path, document)

    monkeypatch.setattr(opencode, "_atomic_auth_write", observed)
    assert await s.reconcile() == "daemon_newer"
    after = json.loads(s.path.read_text())
    expected = copy.deepcopy(s.document)
    expected["tokens"].update(access_token=s.entries[0]["value"]["access"],
                              refresh_token=s.entries[0]["value"]["refresh"])
    expected["last_refresh"] = after["last_refresh"]
    assert after == expected
    assert after["last_refresh"] != "old"
    assert s.path.stat().st_mode & 0o777 == 0o600
    assert s.path.stat().st_ino != inode
    assert s.writes == 0 and all(s.locked)


async def test_agent_newer_import_and_equal_no_write(stores):
    s = stores
    s.entries = [s.credential(1899999900)]
    original = s.path.read_bytes()
    old_id = s.entries[0]["id"]
    assert await s.reconcile() == "agent_newer"
    assert len(s.entries) == 1 and s.entries[0]["id"] != old_id
    assert s.entries[0]["value"]["access"] == s.document["tokens"]["access_token"]
    writes = s.writes
    assert await s.reconcile() == "equal"
    assert s.writes == writes
    assert s.path.read_bytes() == original


async def test_equal_expiry_different_token_bytes_does_not_write(stores):
    stores.entries = [stores.credential(1900000000)]
    before = copy.deepcopy(stores.entries)
    raw = stores.path.read_bytes()
    assert await stores.reconcile() == "equal"
    assert stores.entries == before and stores.path.read_bytes() == raw
    assert stores.writes == 0


@pytest.mark.parametrize("change", [
    lambda d: d.update(auth_mode="apikey"), lambda d: d.pop("auth_mode"),
    lambda d: d["tokens"].update(refresh_token=""),
    lambda d: d["tokens"].update(access_token="not-a-jwt"),
    lambda d: d["tokens"].update(account_id=None), lambda d: d.update(tokens=[]),
])
async def test_invalid_or_apikey_agent_never_seeds(stores, change):
    s = stores
    change(s.document)
    s.save()
    with pytest.raises(opencode.CodexReconcileError, match="codex login --device-auth"):
        await s.reconcile()
    assert s.reads == s.writes == 0


@pytest.mark.parametrize("exp", [0, -1, True, "1900000000"])
async def test_invalid_jwt_expiry_never_seeds(stores, exp):
    stores.document["tokens"]["access_token"] = jwt(exp)
    stores.save()
    with pytest.raises(opencode.CodexReconcileError, match="codex login"):
        await stores.reconcile()
    assert stores.writes == 0


@pytest.mark.parametrize("updates", [
    {"type": "key"}, {"methodID": "unrecognized"}, {"refresh": ""},
    {"access": "bad-jwt"}, {"metadata": {}}, {"expires": 0}, {"expires": True},
])
async def test_invalid_daemon_credential_is_seeded(stores, updates):
    stores.entries = [stores.credential(1900000100, **updates)]
    if updates.get("type") == "key":
        stores.entries[0]["value"] = {"type": "key", "key": "synthetic-key"}
    if updates.get("expires") is True:
        # Explicit corrupt-response fault, outside the pinned server schema.
        stores.validate_export = False
    assert await stores.reconcile() == "seeded"
    assert len(stores.entries) == 1
    assert stores.entries[0]["value"]["access"] == stores.document["tokens"]["access_token"]


async def test_inactive_daemon_not_used_for_writeback_and_ambiguous_active_refused(stores):
    s = stores
    inactive = s.credential(1900000100)
    inactive["active"] = False
    s.entries = [inactive]
    assert await s.reconcile() == "seeded"
    assert json.loads(s.path.read_text()) == s.document
    s.entries.append(s.credential(1900000200))
    before = s.path.read_bytes()
    with pytest.raises(opencode.CodexReconcileError, match="ambiguous"):
        await s.reconcile()
    assert s.path.read_bytes() == before


async def test_other_integration_cannot_supply_codex_tokens(stores):
    entry = stores.credential(1900000100)
    entry["integrationID"] = "other"
    stores.entries = [entry]
    assert await stores.reconcile() == "seeded"
    assert stores.entries[0] == entry and entry["active"]
    assert json.loads(stores.path.read_text()) == stores.document


async def test_oversize_valid_agent_document_is_refused(stores):
    stores.document["padding"] = "x" * 65536
    stores.save()
    with pytest.raises(opencode.CodexReconcileError, match="codex login"):
        await stores.reconcile()
    assert stores.reads == stores.writes == 0


async def test_symlink_lock_is_refused_without_touching_target(stores, tmp_path):
    target = tmp_path / "foreign-lock"
    target.write_text("unchanged")
    stores.lock.symlink_to(target)
    with pytest.raises(opencode.CodexReconcileError):
        await stores.reconcile()
    assert target.read_text() == "unchanged" and stores.reads == 0


@pytest.mark.parametrize("raw", [None, "broken json", "[]", "x" * 65537])
async def test_missing_or_malformed_agent_login(stores, raw):
    if raw is None:
        stores.path.unlink()
    else:
        stores.path.write_text(raw)
    with pytest.raises(opencode.CodexReconcileError, match="codex login"):
        await stores.reconcile()
    assert stores.writes == 0


async def test_concurrent_codex_plus_refresh_waits_and_rereads(stores, monkeypatch):
    from langchain_codex_plus.codex_auth import _auth_file_lock

    s = stores
    s.entries = [s.credential(1900000100)]
    attempted = threading.Event()
    flock = fcntl.flock

    def observed(fd, operation):
        if operation == fcntl.LOCK_EX:
            attempted.set()
        return flock(fd, operation)

    with _auth_file_lock(s.path):
        monkeypatch.setattr(opencode.fcntl, "flock", observed)
        task = asyncio.create_task(s.reconcile())
        assert await asyncio.to_thread(attempted.wait, 5), "reconciler never attempted the flock"
        assert not task.done() and s.reads == 0
        # Codex-plus replaces the file while it owns the same flock. The
        # previously newer daemon must now lose to this fresh rotation.
        rotated = copy.deepcopy(s.document)
        rotated["tokens"].update(access_token=jwt(1900000200), refresh_token="codex-plus-" + s.secret)
        opencode._atomic_auth_write(s.path, rotated)
    assert await asyncio.wait_for(task, 10) == "agent_newer"
    assert s.entries[0]["value"]["access"] == rotated["tokens"]["access_token"]
    assert json.loads(s.path.read_text()) == rotated


@pytest.mark.parametrize("uid", ["getuid", "geteuid"])
async def test_only_mimir_uid_may_read_agent_login(stores, monkeypatch, uid):
    monkeypatch.setattr(opencode.os, uid, lambda: stores.ids.worklink_uid)
    with pytest.raises(opencode.CodexReconcileError, match="controller uid"):
        await stores.reconcile()
    assert not stores.reads and not stores.writes


async def test_different_account_is_not_written_back(stores):
    stores.entries = [stores.credential(1900000100, metadata={"accountID": "different"})]
    before = stores.path.read_bytes()
    with pytest.raises(opencode.CodexReconcileError, match="accounts differ"):
        await stores.reconcile()
    assert stores.path.read_bytes() == before and stores.writes == 0


@pytest.mark.parametrize("change", [
    lambda e: e.update(integrationID="other"), lambda e: e.update(active=False),
    lambda e: e.update(id=None), lambda e: e["value"].update(refresh="omitted"),
])
async def test_import_must_echo_complete_active_credential_before_removal(stores, change):
    stores.entries = [stores.credential(1899999900)]
    old_id = stores.entries[0]["id"]
    stores.corrupt = change
    with pytest.raises(opencode.CodexReconcileError, match="import verification"):
        await stores.reconcile()
    assert any(e["id"] == old_id for e in stores.entries)


async def test_wrong_password_has_no_payload_in_error(stores):
    payload = json.loads(stores.paths.controller_api.read_text())
    payload["password"] = "wrong"
    stores.paths.controller_api.write_text(json.dumps(payload))
    with pytest.raises(opencode.CodexReconcileError) as failure:
        await stores.reconcile()
    assert stores.secret not in str(failure.value)
    assert stores.writes == 0


def test_pinned_auth_export_import_contract():
    root = FIXTURES / "opencode_2_2_7"
    exported = json.loads((root / "auth-export.json").read_text())
    result = json.loads((root / "auth-import-result.json").read_text())
    assert result == {"version": "2.2.7", "unauthenticated": 401, "import_stdout": "",
                      "create_status": 200, "replacement_method": "chatgpt-headless",
                      "replacement_status": 200, "remove_status": 204}
    for entry in exported:
        OpenCodeSchema().named("Credential.Entry", entry)
    assert exported[0]["value"]["methodID"] == "chatgpt-browser"


async def test_preflight_and_periodic_job_reconcile_on_controller(stores, monkeypatch):
    from mimir import worklink_daemon_activation as activation
    from mimir_cultivate import scheduler

    s = stores
    monkeypatch.setattr(activation, "resolve_activation", lambda env: SimpleNamespace(enabled=True))
    monkeypatch.setattr(daemon_health, "get_identities", lambda: s.ids)
    health = {"daemon_enabled": True, "daemon_state": "ready", "ok": False, "login_state": "missing"}

    def published(*args, **kwargs):
        if s.entries:
            return {**health, "ok": True, "login_state": "locally_unexpired"}
        return health

    monkeypatch.setattr(daemon_health, "daemon_health", published)
    from mimir_cultivate.worklink import identities
    monkeypatch.setattr(identities, "get_identities", lambda: s.ids)
    assert (await asyncio.to_thread(daemon_gate.prepare_daemon_health, s.env, paths=s.paths))["ok"]
    assert s.entries
    registered = {}
    fake_scheduler = SimpleNamespace(register_callable=lambda **kw: registered.update(kw) or True)
    monkeypatch.setenv("HOME", s.env["HOME"])
    original = daemon_gate.prepare_daemon_health
    monkeypatch.setattr(daemon_gate, "prepare_daemon_health", lambda env: original(env, paths=s.paths))
    assert scheduler.add_codex_reconcile_job(fake_scheduler)
    assert registered["default_cron"] == "*/15 * * * *"
    await registered["fn"]()
    assert s.reads == 2
    assert scheduler.add_codex_reconcile_job(fake_scheduler, "*/5 * * * *")
    assert registered["default_cron"] == "*/5 * * * *"


@pytest.mark.parametrize("valid_agent", [True, False])
async def test_setup_actual_probe_seeds_or_returns_existing_codex_login_guidance(stores, monkeypatch, valid_agent):
    from contextlib import redirect_stdout
    import io
    from mimir import worklink_daemon_activation as activation
    from mimir_cultivate import setup
    from mimir_cultivate.worklink import identities

    s = stores
    if not valid_agent:
        s.document["auth_mode"] = "apikey"
        s.save()
    monkeypatch.setattr(activation, "resolve_activation", lambda env: SimpleNamespace(enabled=True))
    monkeypatch.setattr(identities, "get_identities", lambda: s.ids)
    monkeypatch.setenv("HOME", s.env["HOME"])
    monkeypatch.setenv("MIMIR_MODEL_SPEC", "codex-plus:fixture")
    monkeypatch.delenv("CODEX_HOME", raising=False)
    monkeypatch.setattr(daemon_health, "daemon_health", lambda *a, **kw: {
        "daemon_enabled": True, "daemon_state": "ready", "ok": bool(s.entries),
        "login_state": "locally_unexpired" if s.entries else "missing"})
    wait = daemon_gate.wait_for_daemon
    monkeypatch.setattr(daemon_gate, "wait_for_daemon", lambda env: wait(env, paths=s.paths, timeout=0))

    def agent_exec(deploy, *argv):
        assert argv == ("python", "-c", setup._OPENCODE_DAEMON_PROBE)
        output = io.StringIO()
        with redirect_stdout(output):
            exec(compile(argv[-1], "<setup-probe>", "exec"), {})
        return output.getvalue()

    monkeypatch.setattr(setup, "_agent_exec", agent_exec)
    if valid_agent:
        await asyncio.to_thread(setup._verify_opencode_daemon, s.path.parent)
        assert s.entries and s.writes == 1
    else:
        with pytest.raises(setup.SetupError, match="codex agent login --device-auth"):
            await asyncio.to_thread(setup._verify_opencode_daemon, s.path.parent)
        assert not s.entries and s.writes == 0
