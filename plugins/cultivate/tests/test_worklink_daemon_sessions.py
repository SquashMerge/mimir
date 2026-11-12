"""Synthetic pinned-protocol lifecycle tests; no provider requests."""
from __future__ import annotations

import json
import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink.backends.base import SessionRequest
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient, ServerEvent
from mimir_cultivate.worklink.daemon_sessions import SessionController, session_environment
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault, effective_permission


async def create(client, tmp_path, kind="leaf"):
    return await SessionController.create(
        client, tmp_path / "controller.json", request=SessionRequest(
            kind, {"providerID": "openai", "id": "gpt-5", "variant": "high"},
            "--autonomous --max-retries 2 chainlink-2125" if kind == "epic" else "build",
            ("git *", "pytest *") if kind == "leaf" else (),
        ), issue_id=2125, attempt=1, generation="generation-1", checkout=tmp_path,
        environment={"PATH": "/usr/bin:/bin"}, timeout_s=60,
    )


@pytest.mark.parametrize("kind", ["leaf", "spawn", "epic"])
async def test_authoritative_completion_and_restart(tmp_path, kind):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, kind)
        sid = controller.record.opencode_session_id
        assert daemon.sessions[sid].environment["MIMIR_MODEL_SPEC"] == "openai/gpt-5#high"
        assert daemon.sessions[sid].environment["MIMIR_WORKLINK_GATE"] == "1"
        assert (await controller.observe()).execution_state == "started"
        daemon.finish(sid)
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        assert (await recovered.observe()).execution_state == "succeeded"
        assert len([r for r in daemon.requests if r[0] == "POST" and r[1].endswith(("/prompt", "/command"))]) == 1
        assert json.loads(controller.path.read_bytes())["version"] == 1
        assert b"build" not in controller.path.read_bytes()
        await recovered.release()
        assert not controller.path.exists()


async def test_lost_ack_is_reconciled_without_repost(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("POST", "/api/session/ses_000000000000000000000001/prompt")].append(
            SyntheticFault(disconnect=True, after=True))
        controller = await create(client, tmp_path)
        assert controller.record.admission_state == "uncertain"
        assert (await controller.observe()).execution_state == "started"
        daemon.finish(controller.record.opencode_session_id)
        assert (await controller.observe()).execution_state == "succeeded"
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_initial_idle_is_not_success(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        daemon.sessions[controller.record.opencode_session_id].messages.append({
            "id": "msg_initial", "type": "idle", "time": {"created": 1}, "outcome": "succeeded",
        })
        assert (await controller.observe()).execution_state == "pending"


@pytest.mark.parametrize("race", [False, True])
async def test_queued_cancel_and_delivery_race(tmp_path, race):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        daemon.delete_race = race
        controller = await create(client, tmp_path)
        assert await controller.cancel(timeout_s=5)
        state = daemon.sessions[controller.record.opencode_session_id]
        assert state.inbox == []
        assert not state.active
        assert controller.record.cancellation_state == "verified"


async def test_interrupt_false_retains_controller(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.interrupt_noop = True
        controller = await create(client, tmp_path)
        assert not await controller.cancel(timeout_s=0.1)
        assert controller.path.exists()
        assert controller.record.cancellation_state == "unverified"


async def test_lost_interrupt_ack_reconciles_without_reprompt(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.faults[("POST", f"/api/session/{sid}/interrupt")].append(
            SyntheticFault(disconnect=True, after=True))
        assert await controller.cancel(timeout_s=5)
        assert not daemon.sessions[sid].active
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_setup_http_401_keeps_created_identity_and_is_not_provider_auth(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("PUT", "/api/session/ses_000000000000000000000001/environment")].append(
            SyntheticFault(status=401))
        controller = await create(client, tmp_path)
        outcome = await controller.observe()
        assert outcome.opencode_session_id == controller.record.opencode_session_id
        assert outcome.execution_state == "incomplete"
        assert outcome.error.type == "backend_error"
        assert not any(r[1].endswith("/prompt") for r in daemon.requests)
        assert await controller.cancel(timeout_s=5)


async def test_generation_and_inode_binding(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        with pytest.raises(ValueError, match="generation mismatch"):
            SessionController.recover(client, controller.path, generation="other", checkout=tmp_path)
        other = tmp_path / "other"
        other.mkdir()
        with pytest.raises(ValueError, match="generation mismatch"):
            controller.check_binding("generation-1", other)


async def test_foreign_success_is_rejected(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)

        async def foreign(*args, **kwargs):
            yield ServerEvent("message", {"type": "session.execution.succeeded", "data": {"sessionID": "ses_foreign"},
                                         "durable": {"seq": 99, "aggregateID": "ses_foreign"}})

        monkeypatch.setattr(client, "log", foreign)
        with pytest.raises(DaemonClientError, match="foreign session event"):
            await controller.observe()


async def test_success_requires_matching_user_history(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        daemon.sessions[sid].messages[:] = [m for m in daemon.sessions[sid].messages if m["type"] != "user"]
        assert (await controller.observe()).execution_state == "incomplete"


async def test_eof_reconnect_never_readmits(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        original = client.log

        async def eof(*args, **kwargs):
            for frame in ():
                yield frame

        monkeypatch.setattr(client, "log", eof)
        with pytest.raises(DaemonClientError, match="missing log watermark"):
            await controller.observe()
        monkeypatch.setattr(client, "log", original)
        daemon.finish(controller.record.opencode_session_id)
        assert (await controller.observe()).execution_state == "succeeded"
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


@pytest.mark.parametrize("name,value", [
    ("OPENAI_API_KEY", "fake"), ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=fake"),
    ("HOME", "/tmp"), ("home", "/tmp"), ("XDG_CONFIG_HOME", "/tmp"),
    ("API_URL", "http://localhost"), ("OPENCODE_SERVER_PASSWORD", "fake"),
    ("CFLAGS", "-I/home/mimir/private"), ("GIT_CONFIG_COUNT", "1"),
])
def test_session_environment_refuses_untrusted_overrides(tmp_path, name, value):
    with pytest.raises((ValueError, RuntimeError)):
        session_environment({name: value}, home=tmp_path, git_settings=(), credentials={})


def test_session_environment_is_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "fragment-" + "not-in-session")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "99")
    env = session_environment({"CFLAGS": "-O2"}, home=tmp_path, git_settings=(), credentials={})
    assert env == {
        "PATH": "/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin", "USER": "worklink",
        "LOGNAME": "worklink", "SHELL": "/bin/sh", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": str(tmp_path), "TMPDIR": str(tmp_path / "tmp"), "MIMIR_WORKLINK_GATE": "1",
        "MIMIR_SPAWN_DEPTH": "1", "CFLAGS": "-O2", "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory", "GIT_CONFIG_VALUE_0": str(tmp_path),
    }


@pytest.mark.parametrize("error_type", ["provider.auth", "provider.quota", "provider.timeout", "future.error"])
async def test_structured_errors_are_bounded_redacted_and_not_reclassified(tmp_path, error_type):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        secret = "fragment-" + "synthetic-private-value"
        controller.sensitive_values = (secret,)
        daemon.finish(controller.record.opencode_session_id, outcome="failed", error={
            "type": error_type, "message": secret + " auth quota succeeded " + "é" * 3000,
            "response": {"body": secret + "x" * 10000}, "retryAfterMs": 999999999999,
        })
        outcome = await controller.observe()
        assert outcome.execution_state == "failed"
        assert outcome.error.type == error_type
        assert secret not in repr(outcome.error)
        assert len(outcome.error.message.encode()) <= 2048
        assert len(outcome.error.body.encode()) <= 8192
        assert outcome.error.retry_after_ms is None
        assert secret.encode() not in controller.path.read_bytes()


async def test_output_bounds_and_final_text_redaction(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        secret = "runtime-" + "fragment-secret"
        controller.sensitive_values = (secret,)
        daemon.sessions[sid].messages.append({
            "id": "msg_assistant", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": secret + "\nWORKLINK_BLOCKED: needs design"}],
        })
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.stdout == "<redacted>\nWORKLINK_BLOCKED: needs design"
        assert not outcome.output_overflow
        limited = await controller.observe(stdout_limit=1)
        assert limited.output_overflow
        assert len(limited.stdout.encode()) <= 1


async def test_record_unknown_fields_and_oversize_are_rejected(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        payload = json.loads(controller.path.read_bytes())
        payload["prompt"] = "must never be persisted"
        controller.path.write_text(json.dumps(payload))
        with pytest.raises(TypeError):
            SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        controller.path.write_text(" " * 16385)
        with pytest.raises(ValueError):
            SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)


async def test_multipart_output_counts_separator_bytes(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.sessions[sid].messages.append({
            "id": "msg_output", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}],
        })
        daemon.finish(sid)
        outcome = await controller.observe(stdout_limit=2)
        assert outcome.output_overflow
        assert len(outcome.stdout.encode()) <= 2


@pytest.mark.parametrize("kind", ["leaf", "spawn", "epic"])
@pytest.mark.parametrize("variant", [None, "high"])
async def test_captured_per_kind_wire_contract_and_effective_permissions(tmp_path, kind, variant):
    model = {"providerID": "openai", "id": "synthetic"}
    if variant is not None:
        model["variant"] = variant
    text = "--autonomous --max-retries 2 chainlink-2125" if kind == "epic" else "rules\nrendered prompt"
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        await SessionController.create(client, tmp_path / "controller.json", request=SessionRequest(
            kind, model, text, ("git *", "pytest *") if kind == "leaf" else (),
        ), issue_id=2125, attempt=1, generation="generation-1", checkout=tmp_path,
            environment={}, timeout_s=60)
        actual = next(r[3] for r in daemon.requests if r[:2] == ("POST", "/api/session"))
        rules = [{"action": "external_directory", "resource": "*", "effect": "deny"}]
        if kind == "leaf":
            rules += [{"action": "shell", "resource": "*", "effect": "deny"},
                      {"action": "shell", "resource": "git *", "effect": "allow"},
                      {"action": "shell", "resource": "pytest *", "effect": "allow"}]
        elif kind == "spawn":
            rules += [{"action": "shell", "resource": "*", "effect": "allow"}]
        else:
            rules += [{"action": "read", "resource": p, "effect": "deny"} for p in (".env", "**/.env", "**/.env.*")]
        assert actual == {"location": {"directory": str(tmp_path)}, "model": model, "permissions": rules}
        inputs = [r for r in daemon.requests if r[0] == "POST" and r[1].endswith(("/prompt", "/command"))]
        assert len(inputs) == 1
        assert inputs[0][1].endswith("/command" if kind == "epic" else "/prompt")
        assert inputs[0][3] == ({"name": "feature", "text": text} if kind == "epic" else {"text": text, "resume": True})
        agent = [{"action": "*", "resource": "*", "effect": "allow"}]
        def effective(action, resource):
            return effective_permission(action, resource, agent=agent, session=actual["permissions"], project=agent)
        assert effective("external_directory", "/outside") == "deny"
        assert effective("shell", "git status") == "allow"
        assert effective("shell", "pytest -q") == "allow"
        assert effective("shell", "node factory.js status") == ("deny" if kind == "leaf" else "allow")
        if kind == "epic":
            for resource in (".env", "nested/.env", "nested/.env.secret"):
                assert effective("read", resource) == "deny"


@pytest.mark.parametrize("defect", ["bare_started", "bare_succeeded", "id", "created", "version", "seq",
                                    "sessionID", "aggregateID", "data", "durable", "watermark", "error"])
async def test_malformed_durable_witnesses_cannot_succeed(tmp_path, monkeypatch, defect):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        events = deepcopy(daemon.sessions[sid].log)
        started = next(e for e in events if e["type"] == "session.execution.started")
        terminal = events[-1]
        if defect.startswith("bare_"):
            next(e for e in events if e["type"] == "session.execution." + defect[5:])["type"] = defect[5:]
        elif defect in {"id", "created"}:
            started.pop(defect)
        elif defect in {"version", "seq"}:
            started["durable"][defect] = True
        elif defect == "sessionID":
            started["data"].pop("sessionID")
        elif defect == "aggregateID":
            started["durable"].pop("aggregateID")
        elif defect in {"data", "durable"}:
            started.pop(defect)
        elif defect == "error":
            terminal["type"] = "session.execution.failed"
            terminal["data"]["error"] = {"message": "missing type"}
        watermark = {"type": "log.synced", "aggregateID": sid, "seq": len(events)}
        if defect == "watermark":
            watermark.pop("aggregateID")
        async def malformed(*args, **kwargs):
            for event in [*events, watermark]:
                yield ServerEvent("message", event)
        monkeypatch.setattr(client, "log", malformed)
        with pytest.raises(DaemonClientError):
            await controller.observe()


@pytest.mark.parametrize("defect", ["user_text", "user_time", "idle_outcome", "inbox_payload", "inbox_session", "inbox_delivery"])
async def test_malformed_history_and_inbox_refused(tmp_path, monkeypatch, defect):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = not defect.startswith("inbox")
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        if defect.startswith("inbox"):
            original = client.inbox
            async def malformed(*args, **kwargs):
                page = deepcopy(await original(*args, **kwargs))
                item = page["data"][0]
                item.pop({"inbox_payload": "payload", "inbox_session": "sessionID", "inbox_delivery": "delivery"}[defect])
                return page
            monkeypatch.setattr(client, "inbox", malformed)
        else:
            daemon.finish(sid)
            original = client.messages
            async def malformed(*args, **kwargs):
                page = deepcopy(await original(*args, **kwargs))
                item = next(m for m in page["data"] if m["type"] == ("idle" if defect == "idle_outcome" else "user"))
                item.pop({"user_text": "text", "user_time": "time", "idle_outcome": "outcome"}[defect])
                return page
            monkeypatch.setattr(client, "messages", malformed)
        with pytest.raises(DaemonClientError):
            await controller.observe()


@pytest.mark.parametrize("error_type", ["provider.auth", "provider.quota", "future.error"])
async def test_event_only_error_survives_cursor_and_transport_loss(tmp_path, monkeypatch, error_type):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        secret = "runtime-" + "error-fragment"
        daemon.finish(sid, outcome="failed", error={"type": error_type, "message": secret + "é" * 3000,
                      "response": {"body": secret + "x" * 9000}, "retryAfterMs": 12345})
        daemon.sessions[sid].messages[-1].pop("error")
        original = client.messages
        async def disconnected(*args, **kwargs):
            raise DaemonClientError("transport failure")
        monkeypatch.setattr(client, "messages", disconnected)
        with pytest.raises(DaemonClientError, match="transport failure"):
            await controller.observe()
        assert controller.record.execution_state == "failed"
        assert controller.record.last_seq == len(daemon.sessions[sid].log)
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        recovered.sensitive_values = (secret,)
        monkeypatch.setattr(client, "messages", original)
        result = await recovered.observe()
        assert result.error.type == error_type
        assert result.error.retry_after_ms == 12345
        assert secret not in repr(result.error)
        assert len(result.error.message.encode()) <= 2048 and len(result.error.body.encode()) <= 8192
        assert secret.encode() not in controller.path.read_bytes()
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_setup_cannot_post_after_persisted_deadline(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        original = client.set_environment
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)
        monkeypatch.setattr(client, "set_environment", slow)
        task = asyncio.create_task(create(client, tmp_path))
        await asyncio.wait_for(entered.wait(), 10)
        clock[0] += 61
        release.set()
        controller = await asyncio.wait_for(task, 10)
        assert not any(r[1].endswith(("/prompt", "/command")) for r in daemon.requests)
        assert (await controller.observe()).error.type == "timeout"


async def test_read_crossing_deadline_cannot_return_success(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        daemon.finish(controller.record.opencode_session_id)
        original = client.inbox
        async def crossing(*args, **kwargs):
            result = await original(*args, **kwargs)
            clock[0] += 61
            return result
        monkeypatch.setattr(client, "inbox", crossing)
        outcome = await controller.observe()
        assert outcome.execution_state != "succeeded"
        assert outcome.error.type == "timeout"
        assert controller.record.stop_requested


async def test_expired_recovery_retains_unverified_work(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        clock[0] += 61
        daemon.faults[("GET", f"/api/experimental/session/{controller.record.opencode_session_id}/log")].append(SyntheticFault(status=401))
        outcome = await recovered.observe()
        assert outcome.error.type == "timeout" and outcome.cancellation_state == "unverified"
        assert recovered.path.exists()
        assert daemon.sessions[controller.record.opencode_session_id].active


async def test_failed_stop_checkpoint_does_not_disable_deadline(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        original = controller._persist
        def failed_checkpoint(**changes):
            raise OSError("fixture checkpoint failure")
        monkeypatch.setattr(controller, "_persist", failed_checkpoint)
        with pytest.raises(OSError):
            await controller.cancel()
        assert not controller._cancelling
        monkeypatch.setattr(controller, "_persist", original)
        daemon.finish(controller.record.opencode_session_id)
        clock[0] += 61
        assert (await controller.observe()).error.type == "timeout"


async def test_blocked_read_uses_remaining_budget_not_http_timeout(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    # Freeze the owned wall clock while fsync/HTTP setup runs. The remaining
    # 50ms is then enforced by asyncio's monotonic timeout at the blocked read,
    # not consumed by fixture setup or checkpoint I/O under xdist contention.
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: 1000.0))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        entered = asyncio.Event()
        original_log = controller._read_log
        async def arm_read_budget(**kwargs):
            await original_log(**kwargs)
            if not controller._cancelling:
                controller._persist(deadline_at_ms=int(daemon_sessions.time.time() * 1000) + 50)
        monkeypatch.setattr(controller, "_read_log", arm_read_budget)
        original = client.messages
        async def blocked(*args, **kwargs):
            if not controller._cancelling:
                entered.set()
                await asyncio.Event().wait()
            return await original(*args, **kwargs)
        monkeypatch.setattr(client, "messages", blocked)
        task = asyncio.create_task(controller.observe())
        await asyncio.wait_for(entered.wait(), 2)
        outcome = await asyncio.wait_for(task, 4)
        assert outcome.error.type == "timeout"
        assert controller.record.cancellation_state == "verified"


@pytest.mark.parametrize("parts,limit,overflow", [(["a", "b"], 3, False), (["é", "é"], 4, True), (["é", "é"], 5, False)])
async def test_utf8_multipart_output_boundaries(tmp_path, parts, limit, overflow):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.sessions[sid].messages.append({"id": "msg_parts", "type": "assistant", "time": {"created": 2},
            "agent": "build", "model": {"providerID": "openai", "id": "synthetic"},
            "content": [{"type": "text", "text": text} for text in parts]})
        daemon.finish(sid)
        outcome = await controller.observe(stdout_limit=limit)
        assert outcome.output_overflow is overflow
        assert len(outcome.stdout.encode()) <= limit
        if not overflow:
            assert outcome.stdout == "\n".join(parts)
