"""Offline wire contracts; execution outcomes here are explicitly synthetic."""
from __future__ import annotations

import asyncio
import hashlib
import json

import aiohttp
import pytest
from jsonschema import ValidationError

from tests.worklink_daemon_fixtures import fake_opencode_daemon  # noqa: F401
from tests.worklink_daemon_fixtures import (
    FIXTURES, OpenCodeSchema, SyntheticFault, effective_permission,
)

pytest.importorskip("mimir_cultivate", reason="Daemon transport belongs to Cultivate")
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient


async def create(client):
    return (await client.create_session(directory="/synthetic/checkout", model={
        "providerID": "openai", "id": "synthetic-model",
    }, permissions=[]))["data"]["id"]


async def test_interrupt_never_resumes_queued_input(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        assert await client.interrupt(session_id) == {"interrupted": False}
    assert daemon.requests[-1][2] == {"resume": "false"}


def test_recorded_factory_bytes_and_normalization():
    root = FIXTURES / "feature_factory_0_14_0"
    raw = (root / "status.raw.stdout.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "0267426670d2649d857f2312ccc37d7871de4b0e1872ddf31cbededb2a6755a8"
    original = json.loads(raw)
    normalized = json.loads((root / "status.normalized.json").read_bytes())
    original.update(run_id="<RUN_ID>", sandbox_path="<SANDBOX>")
    assert original == normalized
    assert normalized["valid"] is True
    assert normalized["mcp_servers"] == ["fixture-mcp"]
    assert hashlib.sha256(raw).hexdigest() in (root / "PROVENANCE.md").read_text()


def test_recorded_api_evidence_and_synthetic_labels():
    root = FIXTURES / "opencode_2_2_7"
    record = json.loads((root / "probe-result.json").read_bytes())
    assert record["binary_version"] == "2.2.7"
    assert record["server_awaited"] is True
    schema = OpenCodeSchema()
    for path in ("/api/session", "/api/event"):
        response = record["responses"]["unauthenticated " + path]
        assert response["status"] == 401
        schema.named("UnauthorizedErrorEncoded", json.loads(response["body"]))
    for key in ("POST /api/session empty", "POST /api/session location"):
        schema.named("Session.Info", json.loads(record["responses"][key]["body"])["data"])
    assert record["responses"]["/api/event"]["body"].startswith("data: ")
    assert record["responses"]["/api/health"]["status"] == 404
    assert record["responses"]["/health"]["content_type"] == "text/html"
    assert "synthetic" in (root / "PROVENANCE.md").read_text()
    probe = (root / "probe.py").read_text()
    assert '"serve", "--hostname", "127.0.0.1", "--port"' in probe
    assert '"--standalone"' not in probe


async def test_basic_auth_session_and_event(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with aiohttp.ClientSession() as connection:
        for path in ("/api/session", "/api/event"):
            async with connection.get(daemon.base_url + path) as response:
                assert response.status == 401
                assert await response.json() == {"_tag": "UnauthorizedError", "message": "Authentication required"}
    async with OpenCodeDaemonClient(**{**daemon.connection, "password": "wrong"}) as client:
        with pytest.raises(DaemonClientError) as failure:
            await create(client)
        assert failure.value.status == 401
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        assert (await client.info())["version"] == "2.2.7"
        events = [event async for event in client.events()]
        assert events[0].data["type"] == "server.connected"


async def test_request_shapes_environment_replacement_and_command(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    model = {"providerID": "openai", "id": "synthetic-model", "variant": "high"}
    rules = [{"action": "external_directory", "resource": "*", "effect": "deny"}]
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session = (await client.create_session(directory="/synthetic/checkout", model=model, permissions=rules))["data"]
        session_id = session["id"]
        assert session["model"] == model
        assert session["permissions"] == rules
        assert daemon.requests[-1][3] == {"location": {"directory": "/synthetic/checkout"}, "model": model, "permissions": rules}
        await client.set_environment(session_id, {"OLD": "gone"})
        await client.set_environment(session_id, {"NEW": "present"})
        assert daemon.sessions[session_id].environment == {"NEW": "present"}
        await client.command(session_id, name="feature", text="--autonomous --max-retries 3 chainlink-2125")
        assert daemon.requests[-1][3] == {"name": "feature", "text": "--autonomous --max-retries 3 chainlink-2125"}
        assert daemon.sessions[session_id].active
        assert "synthetic" in daemon.scenario_provenance


async def test_durable_replay_watermark_history_and_opaque_cursors(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        baseline = [e.data async for e in client.log(session_id, follow=False)]
        assert baseline == [{"type": "log.synced", "aggregateID": session_id, "seq": 0}]
        admitted = (await client.prompt(session_id, "instructions"))["data"]
        assert admitted["sessionID"] == session_id
        daemon.finish(session_id)
        events = [e.data async for e in client.log(session_id, after=0, follow=False)]
        assert [e["type"] for e in events][-3:] == ["session.execution.started", "session.execution.succeeded", "log.synced"]
        seq = events[-2]["durable"]["seq"]
        assert [e.data async for e in client.log(session_id, after=seq, follow=False)] == [events[-1]]
        first = await client.messages(session_id, order="asc", limit=1)
        assert first["data"][0]["id"] == admitted["id"]
        second = await client.messages(session_id, cursor=first["cursor"]["next"], limit=1)
        assert second["data"][0]["type"] == "idle"
        assert second["data"][0]["outcome"] == "succeeded"
        assert second["cursor"]["next"] is None
        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.messages(session_id, cursor="opaque", order="asc")


async def test_follow_delivers_new_durable_events(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        stream = client.log(session_id)
        try:
            marker = await anext(stream)
            assert marker.data["type"] == "log.synced"
            await client.prompt(session_id, "instructions")
            event = await asyncio.wait_for(anext(stream), 5)
            assert event.data["type"] == "session.inbox.enqueued"
        finally:
            await stream.aclose()


@pytest.mark.parametrize("race", [False, True])
async def test_synthetic_queued_delete_and_delivery_race(fake_opencode_daemon, race):
    daemon = fake_opencode_daemon
    daemon.auto_deliver = False
    daemon.delete_race = race
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        admitted = (await client.prompt(session_id, "queued"))["data"]
        assert (await client.inbox(session_id))["data"] == [admitted]
        assert await client.interrupt(session_id) == {"interrupted": False}
        assert len((await client.inbox(session_id))["data"]) == 1
        await client.delete_inbox(session_id, admitted["id"])
        await client.delete_inbox(session_id, admitted["id"])
        assert (await client.inbox(session_id))["data"] == []
        assert daemon.sessions[session_id].active is race
        assert await client.interrupt(session_id) == {"interrupted": race}
        assert await client.interrupt(session_id) == {"interrupted": False}


async def test_lost_post_ack_does_not_retry(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        path = f"/api/session/{session_id}/prompt"
        daemon.faults[("POST", path)].append(SyntheticFault(disconnect=True, after=True))
        with pytest.raises(DaemonClientError, match="transport failure"):
            await client.prompt(session_id, "admitted despite lost ACK")
        assert len([r for r in daemon.requests if r[1] == path]) == 1
        assert (await client.messages(session_id))["data"][0]["text"] == "admitted despite lost ACK"


@pytest.mark.parametrize("error_type", ["provider.auth", "provider.quota", "provider.timeout", "future.error"])
async def test_synthetic_typed_failure_is_not_http_failure(fake_opencode_daemon, error_type):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        await client.prompt(session_id, "instructions")
        error = {"type": error_type, "message": "synthetic failure"}
        daemon.finish(session_id, outcome="failed", error=error)
        events = [e.data async for e in client.log(session_id, follow=False)]
        assert events[-2]["data"]["error"] == error
        assert (await client.messages(session_id))["data"][0]["error"] == error


@pytest.mark.parametrize("body,content_type,reason", [
    (b"{", "application/json", "invalid JSON"),
    (b"[]", "application/json", "invalid response"),
    (b'{"version":NaN}', "application/json", "invalid JSON"),
    (b"{}", "text/html", "invalid content type"),
    (b"x" * (1024 * 1024 + 1), "application/json", "item too large"),
])
async def test_bounded_malformed_http(fake_opencode_daemon, body, content_type, reason):
    daemon = fake_opencode_daemon
    daemon.faults[("GET", "/api/info")].append(SyntheticFault(body=body, content_type=content_type))
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        with pytest.raises(DaemonClientError, match=reason):
            await client.info()


@pytest.mark.parametrize("body,reason", [
    (b"data: {\n\n", "invalid JSON"),
    (b"data: {}", "truncated stream item"),
    (b"data: \xff\n\n", "invalid stream encoding"),
    (b"event: effect/http-api/stream/failure\ndata: []\n\n", "remote stream failure"),
    (b"data: " + b"x" * (1024 * 1024) + b"\n\n", "item too large"),
])
async def test_bounded_malformed_sse(fake_opencode_daemon, body, reason):
    daemon = fake_opencode_daemon
    daemon.faults[("GET", "/api/event")].append(SyntheticFault(body=body, content_type="text/event-stream"))
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        with pytest.raises(DaemonClientError, match=reason):
            _ = [e async for e in client.events()]


async def test_sse_multiline_comments_and_eof_are_not_success(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    daemon.faults[("GET", "/api/event")].append(SyntheticFault(
        body=b': heartbeat\r\nevent: diagnostic\r\nid: evt_synthetic\r\ndata: {"type":\r\ndata: "server.connected"}\r\n\r\n',
        content_type="text/event-stream"))
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        events = [e async for e in client.events()]
        assert len(events) == 1
        assert events[0].event == "diagnostic"
        assert events[0].id == "evt_synthetic"
        assert events[0].data == {"type": "server.connected"}


async def test_errors_do_not_export_remote_credentials_or_follow_redirects(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    secret = "Bearer " + "runtime-" + "fragment"
    daemon.faults[("GET", "/api/info")].append(SyntheticFault(body=secret.encode(), status=302))
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        with pytest.raises(DaemonClientError) as failure:
            await client.info()
        assert failure.value.status == 302
        assert secret not in str(failure.value)
        assert daemon.password not in repr(client)
        assert len(daemon.requests) == 1


async def test_absent_cursor_directions_and_missing_session(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        path = f"/api/session/{session_id}/message"
        daemon.faults[("GET", path)].append(SyntheticFault(body=b'{"data":[],"cursor":{}}'))
        assert await client.messages(session_id) == {"data": [], "cursor": {}}
        with pytest.raises(DaemonClientError) as failure:
            await client.inbox("ses_missing")
        assert failure.value.status == 404


async def test_configured_request_timeout(fake_opencode_daemon, monkeypatch):
    daemon = fake_opencode_daemon
    release = asyncio.Event()
    entered = asyncio.Event()

    async def stalled(request, payload):
        entered.set()
        await release.wait()
        return daemon._response(request, json.loads(daemon.recorded["responses"]["/api/info"]["body"]))

    monkeypatch.setattr(daemon, "_dispatch", stalled)
    try:
        async with OpenCodeDaemonClient(**daemon.connection, timeout=0.05) as client:
            with pytest.raises(DaemonClientError, match="transport failure"):
                await asyncio.wait_for(client.info(), 2)
        assert entered.is_set()
    finally:
        release.set()


def test_schema_backing_rejects_invented_execution_fields():
    with pytest.raises(ValidationError):
        OpenCodeSchema().named("Session.Message.Idle", {
            "id": "msg_synthetic", "time": {"created": 1}, "type": "idle", "outcome": "success",
        })


def test_effective_permission_order_and_final_deny_before_project_grants():
    allow = {"action": "shell", "resource": "*", "effect": "allow"}
    deny = {**allow, "effect": "deny"}
    rules = [deny, {**allow, "resource": "git *"}, {**allow, "resource": "uv *"}]
    for command, expected in (("git status", "allow"), ("uv run pytest", "allow"), ("gh pr create", "deny"), ("node factory.js", "deny")):
        assert effective_permission("shell", command, agent=[allow], session=rules, project=[allow]) == expected
    assert effective_permission("shell", "anything", agent=[deny], session=[allow]) == "allow"
    assert effective_permission("read", "file", agent=[], session=[]) == "ask"
    assert effective_permission("read", "file", agent=[], session=[], project=[{"action": "read", "resource": "*", "effect": "allow"}]) == "allow"
    epic = [{"action": "external_directory", "resource": "*", "effect": "deny"},
            *[{"action": "read", "resource": path, "effect": "deny"} for path in (".env", "**/.env", "**/.env.*")]]
    for path in (".env", "nested/.env", "nested/.env.local"):
        assert effective_permission("read", path, agent=[], session=epic, project=[{"action": "read", "resource": "*", "effect": "allow"}]) == "deny"
    assert effective_permission("external_directory", "/outside", agent=[], session=epic) == "deny"
    assert effective_permission("shell", "node factory.js", agent=[allow], session=epic) == "allow"
