"""Offline wire contracts; execution outcomes here are explicitly synthetic."""
from __future__ import annotations

import asyncio
import hashlib
import json
import runpy

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


def test_question_action_and_child_inheritance_are_pinned_to_native_source():
    root = FIXTURES / "opencode_2_3_0"
    record = json.loads((root / "source-inspection.json").read_bytes())
    assert record["sha256"] == "5c7d89bc2d5fa5d83cc7768b813ca671368e70b8b9eda9d1ca9e6ec6b66d8ce0"
    source = {k: v["source"] for k, v in record["excerpts"].items()}
    assert 'o.assert({action:"question",resources:["*"]' in source["question_permission_and_form"]
    assert 'metadata:{kind:"question"' in source["question_permission_and_form"]
    assert 'title:e.header,description:e.question' in source["question_field_text"]
    assert 'let pe=H.parentID?yield*C.get(H.parentID)' in source["session_parent_lookup"]
    assert 'permissions:H.permissions??pe?.permissions' in source["session_permission_inheritance"]
    for name in ("subagent_session_create", "command_subagent_session_create"):
        assert "parentID:" in source[name] and "permissions:" not in source[name]
    assert 'cZ(x?.permissions??BS,P.permissions??[])' in source["permission_agent_then_session"]
    assert 'findLast' in source["permission_last_match"]
    # Reproducible extractor pins offsets and lengths, not hand-written claims.
    markers = runpy.run_path(str(root / "inspect-source.py"))["MARKERS"]
    for name, (marker, length) in markers.items():
        assert source[name].encode().startswith(marker)
        assert len(source[name].encode()) == length


async def test_children_listing_pages_beyond_default_fifty_and_preserves_parent_filter(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        root = await create(client)
        other = await create(client)
        children = [daemon.new_session({"parentID": root})["id"] for _ in range(101)]
        daemon.new_session({"parentID": other})
        assert [c["id"] for c in await client.children(root)] == children
        requests = [r for r in daemon.requests if r[0] == "GET" and r[1] == "/api/session"]
        assert requests[0][2] == {"parentID": root, "order": "asc", "limit": "100"}
        assert len(requests) == 2 and "cursor" in requests[1][2]


async def test_fake_form_info_and_reply_match_captured_schema(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    schema = daemon.schema
    assert schema.operation("/api/session/{sessionID}/form", "GET")["description"] == "Retrieve pending forms for a session."
    reply_op = schema.operation("/api/session/{sessionID}/form/{formID}/reply", "POST")
    assert reply_op["requestBody"]["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/Form.Reply"}
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        sid = await create(client)
        form = daemon.question(sid)
        schema.named("Form.Info", form)
        assert await client.forms(sid) == [form]
        # Fixture-only manual reply proves that the fake would record a regression.
        answer = {"answer": {"q0": "Keep current paths"}}
        schema.named("Form.Reply", answer)
        with pytest.raises(ValidationError):
            schema.named("Form.Reply", {"answers": [["Keep current paths"]]})
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(daemon.username, daemon.password)) as connection:
            async with connection.post(f"{daemon.base_url}/api/session/{sid}/form/{form['id']}/reply", json=answer) as response:
                assert response.status == 204
        assert daemon.sessions[sid].replies == [answer]
        assert await client.forms(sid) == []


async def test_children_listing_keeps_last_same_page_occurrence(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        root = await create(client)
        child = daemon.new_session({"parentID": root})
        newer = {**child, "title": "Latest child state"}
        daemon.faults[("GET", "/api/session")].append(SyntheticFault(body=json.dumps({
            "data": [child, newer], "cursor": {"previous": None, "next": None},
        }).encode()))
        assert await client.children(root) == [newer]


@pytest.mark.parametrize("case", ["foreign_parent", "foreign_repeat", "bad_child_id", "bad_cursor", "repeated_cursor"])
async def test_children_listing_fails_closed_on_invalid_binding_or_pagination(fake_opencode_daemon, case):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        root = await create(client)
        child = daemon.new_session({"parentID": root})
        data = [dict(child)]
        cursor = {"previous": None, "next": None}
        if case == "foreign_parent":
            data[0]["parentID"] = "ses_other"
        elif case == "foreign_repeat":
            data.append({**child, "parentID": "ses_other"})
        elif case == "bad_child_id":
            data[0]["id"] = "../foreign"
        elif case == "bad_cursor":
            cursor = {}
        else:
            data = []
            cursor["next"] = "same"
        body = json.dumps({"data": data, "cursor": cursor}).encode()
        for _ in range(2):
            daemon.faults[("GET", "/api/session")].append(SyntheticFault(body=body))
        reason = {"foreign_parent": "foreign child session", "foreign_repeat": "foreign child session",
                  "bad_child_id": "invalid child session identity", "bad_cursor": "invalid session page",
                  "repeated_cursor": "repeated session cursor"}[case]
        with pytest.raises(DaemonClientError, match=reason):
            await client.children(root)


@pytest.mark.parametrize("case", ["foreign_session", "duplicate", "bad_id", "bad_title", "bad_metadata", "bad_fields", "bad_field_text"])
async def test_forms_fail_closed_on_invalid_identity_or_shape(fake_opencode_daemon, case):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        root = await create(client)
        form = daemon.question(root)
        data = [form]
        if case == "foreign_session":
            form["sessionID"] = "ses_other"
        elif case == "duplicate":
            data *= 2
        elif case == "bad_id":
            form["id"] = "../foreign"
        elif case == "bad_title":
            form["title"] = 42
        elif case == "bad_metadata":
            form["metadata"] = []
        elif case == "bad_fields":
            form["fields"] = []
        else:
            form["fields"][0]["description"] = 42
        daemon.faults[("GET", f"/api/session/{root}/form")].append(SyntheticFault(body=json.dumps({"data": data}).encode()))
        with pytest.raises(DaemonClientError):
            await client.forms(root)


@pytest.mark.parametrize("bound", ["count", "bytes"])
async def test_children_listing_has_aggregate_bounds(fake_opencode_daemon, monkeypatch, bound):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        root = await create(client)
        calls = []
        async def pages(*args, **kwargs):
            calls.append(kwargs)
            assert len(calls) < 20, "aggregate bound must fire before this synthetic sentinel"
            data = ([{"id": f"ses_{i}", "parentID": root} for i in range(4097)] if bound == "count" else [
                {"id": f"ses_{len(calls)}", "parentID": root, "title": "x" * (1024 * 1024 - 1024)},
            ])
            return {"data": data, "cursor": {"previous": None, "next": str(len(calls))}}
        monkeypatch.setattr(client, "_request", pages)
        with pytest.raises(DaemonClientError, match="session children exceed bound"):
            await client.children(root)


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
    root = FIXTURES / "opencode_2_3_0"
    record = json.loads((root / "probe-result.json").read_bytes())
    assert record["binary_version"] == "2.3.0"
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
        assert (await client.info())["version"] == "2.3.0"
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


async def test_watermark_only_log_history_and_opaque_cursors(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        baseline = [e.data async for e in client.log(session_id, follow=False)]
        assert baseline == [{"type": "log.synced", "aggregateID": session_id, "seq": 0}]
        admitted = (await client.prompt(session_id, "instructions"))["data"]
        assert admitted["sessionID"] == session_id
        daemon.finish(session_id)
        # Historical live-provider behaviour; 2.3.0 queued capture also has only a watermark.
        events = [e.data async for e in client.log(session_id, after=0, follow=False)]
        assert events == [{"type": "log.synced", "aggregateID": session_id, "seq": len(daemon.sessions[session_id].log)}]
        first = await client.messages(session_id, order="asc", limit=1)
        assert first["data"][0]["id"] == admitted["id"]
        second = await client.messages(session_id, cursor=first["cursor"]["next"], limit=1)
        assert second["data"][0]["type"] == "idle"
        assert second["data"][0]["outcome"] == "succeeded"
        assert second["cursor"]["next"] is None
        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.messages(session_id, cursor="opaque", order="asc")


async def test_follow_holds_open_after_the_watermark(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        stream = client.log(session_id)
        try:
            marker = await anext(stream)
            assert marker.data["type"] == "log.synced"
            await client.prompt(session_id, "instructions")
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(anext(stream), 0.2)
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
        assert daemon.sessions[session_id].log[-1]["data"]["error"] == error
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


@pytest.mark.parametrize("model", [None, {"providerID": "openai", "id": "gpt-6.1-sol", "variant": "high"},
                                   {"providerID": "openai", "id": "gpt-6.1-sol"}])
def test_generated_daemon_config_matches_the_pinned_config_schema(model):
    """A config 2.3.0 rejects would stop the daemon from starting at all."""
    from mimir_cultivate.worklink.daemon_config import daemon_configuration, resolve_daemon_credentials
    config = daemon_configuration({}, resolve_daemon_credentials({}), model=model).config
    OpenCodeSchema().named("Config.InfoEncoded", config)


def test_opencode_2_3_0_contract_comparison_is_reproducible():
    root = FIXTURES / "opencode_2_3_0"
    compare = runpy.run_path(str(root / "compare-contract.py"))
    old = compare["contract"](json.loads((FIXTURES / "opencode_2_2_7/openapi.json").read_text()))
    new = compare["contract"](OpenCodeSchema().document)
    result = json.loads((root / "contract-diff.json").read_text())
    assert result == {"operations_checked": sorted(new["operations"]), "schemas_checked": len(new["schemas"]),
                      "differences": compare["differences"](old, new)}
    assert len(new["operations"]) == 18 and len(new["schemas"]) == 252


def test_opencode_2_3_0_source_and_factory_permission_claims():
    root = FIXTURES / "opencode_2_3_0"
    source = json.loads((root / "source-inspection.json").read_text())
    excerpts = {key: value["source"] for key, value in source["excerpts"].items()}
    assert "s.expires!==0&&s.expires<=g+We(m)" in excerpts["oauth_stale"]
    assert "if(!x||!uZ(P.value,yield*Tt))return P.value" in excerpts["oauth_resolve"]
    assert "findLast" in excerpts["permission_last_match"]
    assert "cZ(x?.permissions??BS,P.permissions??[])" in excerpts["permission_agent_then_session"]
    assert 'if(J(Y,C))return{effect:"deny",rules:C};let P=[...C,...yield*D()]' in excerpts["permission_final_deny"]
    assert "A(y),y.id=g" in excerpts["agent_editor_replacement"]
    factory = json.loads((root / "factory-result.json").read_text())
    assert factory["binary_version"] == "2.3.0" and factory["server_awaited"] is True
    assert factory["package"]["version"] == "0.14.2"
    assert factory["package"]["dependencies"]["feature-factory"] == "0.14.2"
    assert factory["package"]["peerDependencies"] == {"solid-js": "^1.9.0", "@opentui/solid": ">=0.5.17"}
    assert "feature" in factory["skills"] and "feature" in factory["commands"]
    agents = {agent["id"]: agent["permissions"] for agent in factory["agents"]}
    for action in ("read", "skill"):
        assert effective_permission(action, "feature", agent=agents["replacement-probe"], session=[]) == "ask"
        assert effective_permission(action, "feature", agent=agents["feature-factory"], session=[]) == "allow"
        assert effective_permission(action, "feature", agent=agents["backend-builder"], session=[]) == "allow"


async def test_opencode_2_3_0_additive_assistant_first_token_is_preserved(fake_opencode_daemon):
    daemon = fake_opencode_daemon
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        session_id = await create(client)
        message = {"id": "msg_synthetic", "time": {"created": 1, "firstToken": 2}, "type": "assistant",
                   "agent": "build", "model": {"providerID": "openai", "id": "synthetic-model"}, "content": []}
        daemon.schema.named("Session.Message.Assistant", message)
        daemon.sessions[session_id].messages.append(message)
        assert (await client.messages(session_id))["data"] == [message]


@pytest.mark.parametrize("event_type", ["session.step.first-token", "experimental.mcp.event"])
async def test_opencode_2_3_0_additive_events_are_preserved(fake_opencode_daemon, event_type):
    daemon = fake_opencode_daemon
    schema = daemon.schema.document["components"]["schemas"][event_type]
    # Explicit synthetic instances of the two newly captured event shapes.
    data = ({"sessionID": "ses_synthetic", "assistantMessageID": "msg_synthetic"}
            if event_type == "session.step.first-token" else
            {"id": "synthetic", "server": "synthetic", "name": "synthetic", "eventId": "synthetic",
             "timestamp": "synthetic", "data": {}, "cursor": None})
    event = {"id": "evt_synthetic", "created": 1, "type": event_type, "data": data}
    if "durable" in schema["properties"]:
        event["durable"] = {"aggregateID": "ses_synthetic", "seq": 1, "version": 1}
    daemon.schema.named(event_type, event)
    daemon.faults[("GET", "/api/event")].append(SyntheticFault(
        body=("data: " + json.dumps(event) + "\n\n").encode(), content_type="text/event-stream"))
    async with OpenCodeDaemonClient(**daemon.connection) as client:
        assert [e.data async for e in client.events()] == [event]
