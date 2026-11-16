"""Synthetic pinned-protocol lifecycle tests; no provider requests."""
from __future__ import annotations

import json
import asyncio
import os
import time
from copy import deepcopy
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink.backends.base import SessionRequest
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient, ServerEvent
from mimir_cultivate.worklink.daemon_sessions import ControllerRecord, SessionController, session_environment
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


@pytest.mark.parametrize("defect", ["aggregateID", "foreign", "seq_type", "regressed", "extra"])
async def test_malformed_watermark_is_refused(tmp_path, monkeypatch, defect):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        await controller.observe()
        watermark = {"type": "log.synced", "aggregateID": sid, "seq": controller.record.last_seq}
        if defect == "aggregateID":
            watermark.pop("aggregateID")
        elif defect == "foreign":
            watermark["aggregateID"] = "ses_foreign"
        elif defect == "seq_type":
            watermark["seq"] = True
        elif defect == "regressed":
            watermark["seq"] = controller.record.last_seq - 1
        else:
            watermark["extra"] = 1
        async def malformed(*args, **kwargs):
            yield ServerEvent("message", watermark)
        monkeypatch.setattr(client, "log", malformed)
        with pytest.raises(DaemonClientError, match="invalid log watermark"):
            await controller.observe()


async def test_log_events_cannot_forge_an_outcome(tmp_path, monkeypatch):
    """Only the message history decides execution state; log events are inert."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        forged = [{"id": "evt_forged", "created": 1, "type": "session.execution.succeeded",
                   "durable": {"aggregateID": sid, "seq": 90, "version": 1}, "data": {"sessionID": sid}}]
        async def forged_log(*args, **kwargs):
            for event in [*forged, {"type": "log.synced", "aggregateID": sid, "seq": 91}]:
                yield ServerEvent("message", event)
        monkeypatch.setattr(client, "log", forged_log)
        outcome = await controller.observe()
        assert outcome.execution_state == "started"
        assert controller.record.last_seq == 91


async def test_watermark_advances_last_seq_from_the_daemon(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        baseline = controller.record.baseline_seq
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"
        assert controller.record.baseline_seq == baseline
        assert controller.record.last_seq == len(daemon.sessions[sid].log) > baseline


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
async def test_idle_error_survives_transport_loss_and_recovery(tmp_path, monkeypatch, error_type):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        assert (await controller.observe()).execution_state == "started"
        secret = "runtime-" + "error-fragment"
        daemon.finish(sid, outcome="failed", error={"type": error_type, "message": secret + "é" * 3000,
                      "response": {"body": secret + "x" * 9000}, "retryAfterMs": 12345})
        original = client.messages
        async def disconnected(*args, **kwargs):
            raise DaemonClientError("transport failure")
        monkeypatch.setattr(client, "messages", disconnected)
        with pytest.raises(DaemonClientError, match="transport failure"):
            await controller.observe()
        assert controller.record.execution_state == "started"
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        recovered.sensitive_values = (secret,)
        monkeypatch.setattr(client, "messages", original)
        result = await recovered.observe()
        assert result.execution_state == "failed"
        assert result.error.type == error_type
        assert result.error.retry_after_ms == 12345
        assert secret not in repr(result.error)
        assert len(result.error.message.encode()) <= 2048 and len(result.error.body.encode()) <= 8192
        assert secret.encode() not in controller.path.read_bytes()
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_failed_idle_without_error_still_fails_with_backend_error(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid, outcome="failed", error={"type": "provider.auth", "message": "x"})
        daemon.sessions[sid].messages[-1].pop("error")
        outcome = await controller.observe()
        assert outcome.execution_state == "failed"
        assert outcome.error.type == "backend_error"


async def test_new_execution_after_idle_reads_as_started(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"
        daemon.sessions[sid].messages.append({
            "id": "msg_resumed", "type": "assistant", "time": {"created": 3}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"}, "content": []})
        assert (await controller.observe()).execution_state == "started"


async def test_vanished_input_without_stop_is_incomplete_and_with_stop_interrupted(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        assert (await controller.observe()).execution_state == "pending"
        daemon.sessions[sid].inbox.clear()
        assert (await controller.observe()).execution_state == "incomplete"
        controller._persist(stop_requested=True, cancellation_state="requested")
        assert (await controller.observe()).execution_state == "interrupted"


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
        # Hang guard, not a latency budget.
        await asyncio.wait_for(entered.wait(), 60)
        clock[0] += 61
        release.set()
        # Hang guard, not a latency budget; the owned clock enforces the deadline.
        controller = await asyncio.wait_for(task, 60)
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
        # Hang guard, not a latency budget.
        await asyncio.wait_for(entered.wait(), 60)
        # Behavioural bound: stay below the client's 30-second HTTP timeout so
        # completion proves the remaining 50ms session budget interrupted the read.
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


@pytest.mark.parametrize("stop_requested", [False, True])
async def test_delivery_between_inbox_and_history_reads_is_not_removal(tmp_path, monkeypatch, stop_requested):
    """Delivery at the boundary of the two reads must read as in flight, never vanished."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        if stop_requested:
            controller._persist(stop_requested=True, cancellation_state="requested")
        input_id = controller.record.input_id
        delivered = []

        def deliver_once():
            if not delivered:
                delivered.append(True)
                daemon.deliver(sid, input_id)

        for name in ("inbox", "messages"):
            original = getattr(client, name)
            async def first_read_then_deliver(*args, _original=original, **kwargs):
                result = await _original(*args, **kwargs)
                deliver_once()
                return result
            monkeypatch.setattr(client, name, first_read_then_deliver)
        assert (await controller.observe()).execution_state == "started"
        assert delivered


def _bulky(sid_messages, count, size):
    for index in range(count):
        sid_messages.append({"id": f"msg_bulk{index:04d}", "type": "assistant", "time": {"created": 2},
            "agent": "build", "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": f"{index}:" + "x" * size}]})


async def test_history_larger_than_one_response_is_read_in_narrower_pages(tmp_path):
    """Captured 2026-11-16: 98 messages / 1.46 MB broke a 100-message page against the 1 MiB bound."""
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        _bulky(daemon.sessions[sid].messages, 40, 6 * 1024)
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.execution_state == "succeeded"
        assert outcome.stdout.count("\n") == 39 and outcome.stdout.startswith("0:")
        assert await controller.cancel(timeout_s=5)
        assert controller.record.cancellation_state == "verified"
        limits = [r[2].get("limit") for r in daemon.requests if r[1].endswith("/message")]
        assert "100" in limits and any(int(value) < 100 for value in limits if value)


async def test_narrowing_after_an_advanced_cursor_keeps_the_exact_ordered_history(tmp_path):
    """A first page fits; a later page must narrow from its non-null cursor without skipping or reordering."""
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        messages = daemon.sessions[sid].messages
        texts = [f"small-{index:03d}" for index in range(98)] + [f"large-{index:02d}:" + "y" * 6 * 1024 for index in range(20)]
        for index, text in enumerate(texts):
            messages.append({"id": f"msg_order{index:04d}", "type": "assistant", "time": {"created": 2},
                "agent": "build", "model": {"providerID": "openai", "id": "gpt-5"},
                "content": [{"type": "text", "text": text}]})
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.execution_state == "succeeded"
        assert outcome.stdout == "\n".join(texts)
        pages = [(r[2].get("cursor") is not None, r[2].get("limit")) for r in daemon.requests if r[1].endswith("/message")]
        # The first page (no cursor) fits at 100; narrowing happens only on cursor-bearing pages.
        assert pages[0] == (False, "100") and any(cursor and int(limit) < 100 for cursor, limit in pages)


async def test_single_message_over_the_response_bound_still_fails_closed(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        _bulky(daemon.sessions[sid].messages, 1, 80 * 1024)
        daemon.finish(sid)
        with pytest.raises(DaemonClientError, match="item too large"):
            await controller.observe()
        assert controller.record.execution_state != "succeeded"


class _RecordedClient:
    """Serves one captured 2.2.7 snapshot at a time; no daemon, no provider."""

    def __init__(self, watermark: dict) -> None:
        self.watermark = watermark
        self.snapshot: dict = {}

    async def log(self, session_id, *, after=None, follow=True):
        yield ServerEvent("message", self.watermark)

    async def messages(self, session_id, *, cursor=None, order=None, limit=None, message_type=None):
        data = [item for page in self.snapshot["pages"] for item in page["data"]]
        return {"data": deepcopy(data), "cursor": {"previous": None, "next": None}}

    async def inbox(self, session_id):
        return deepcopy(self.snapshot["inbox"])


def _captured_lifecycle():
    from tests.worklink_daemon_fixtures import FIXTURES
    return json.loads((FIXTURES / "opencode_2_2_7/session-lifecycle.json").read_bytes())


def test_captured_log_endpoint_carries_only_the_watermark():
    """The recorded defect: execution events reach /api/event, never the session log."""
    capture = _captured_lifecycle()
    sid = capture["session"]["data"]["id"]
    live = [frame["data"] for _, frame in capture["log_live"]]
    replay = [frame["data"] for frame in capture["log_replay"]]
    assert live == [{"type": "log.synced", "aggregateID": sid, "seq": 0}]
    assert [item["type"] for item in replay] == ["log.synced"] and replay[0]["seq"] > 0
    published = [frame["data"]["type"] for _, frame in capture["events"]]
    assert {"session.execution.started", "session.execution.succeeded",
            "session.execution.interrupted"} <= set(published)


@pytest.mark.parametrize("scenario,expected", [
    ("prompt1", {"after_prompt1": "started", "idle1": "succeeded"}),
    ("prompt2", {"busy2": "started", "after_interrupt": "interrupted"}),
])
async def test_captured_history_drives_observed_state(tmp_path, scenario, expected):
    capture = _captured_lifecycle()
    sid = capture["session"]["data"]["id"]
    snapshots = {snap["label"]: snap for snap in capture["snapshots"]}
    replay = capture["log_replay"][0]["data"]
    baseline = ([] if scenario == "prompt1" else
                [item["id"] for page in snapshots["idle1"]["pages"] for item in page["data"]])
    client = _RecordedClient(replay)
    info = tmp_path.stat()
    record = ControllerRecord(1, "00000000-0000-0000-0000-000000000000", 2125, 1, "leaf", sid,
                              "generation-1", str(tmp_path), info.st_dev, info.st_ino, 0, 0, baseline,
                              capture[scenario]["data"]["id"], "admitted", "pending", False, "none",
                              int(time.time() * 1000) + 600_000)
    controller = SessionController(client, tmp_path / "controller.json", record)
    for label, state in expected.items():
        client.snapshot = snapshots[label]
        outcome = await controller.observe()
        assert (label, outcome.execution_state) == (label, state)
        assert outcome.error is None
    assert controller.record.last_seq == replay["seq"]
    if scenario == "prompt1":
        assert outcome.stdout.strip() == "ok"


async def test_session_exists_is_true_for_a_known_session_and_false_only_on_404(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        assert await client.session_exists(controller.record.opencode_session_id)
        assert not await client.session_exists("ses_gone")
        daemon.faults[("GET", "/api/session/ses_broken")].append(SyntheticFault(status=500))
        with pytest.raises(DaemonClientError):
            await client.session_exists("ses_broken")


@pytest.mark.parametrize("case,expected", [
    ("orphaned", True), ("same_generation", False), ("session_present", False),
    ("record_present", False), ("retired_present", False), ("no_current_generation", False),
])
async def test_ended_with_previous_daemon_requires_every_proof(tmp_path, case, expected):
    from mimir_cultivate.worklink.daemon_sessions import _ended_with_previous_daemon

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        path = tmp_path / "controller.json"
        sid = "ses_previous"
        if case == "session_present":
            sid = (await client.create_session(directory=str(tmp_path), model={"providerID": "openai", "id": "x"},
                                               permissions=[]))["data"]["id"]
        if case == "record_present":
            path.write_text("{}")
        if case == "retired_present":
            path.with_name(f"{path.stem}.{sid}.retired.json").write_text("{}")
        current = {"same_generation": "generation-old", "no_current_generation": None}.get(case, "generation-new")
        assert await _ended_with_previous_daemon(
            client, path, session_id=sid, request_generation="generation-old",
            current_generation=current) is expected


async def test_ended_with_previous_daemon_fails_closed_on_daemon_error(tmp_path):
    from mimir_cultivate.worklink.daemon_sessions import _ended_with_previous_daemon

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("GET", "/api/session/ses_previous")].append(SyntheticFault(status=500))
        with pytest.raises(DaemonClientError):
            await _ended_with_previous_daemon(client, tmp_path / "controller.json", session_id="ses_previous",
                                              request_generation="generation-old", current_generation="generation-new")


@pytest.mark.parametrize("op", ["session_observe", "session_cancel", "session_release"])
@pytest.mark.parametrize("restarted", [True, False])
async def test_executor_reports_a_session_that_ended_with_its_daemon(tmp_path, monkeypatch, op, restarted):
    """2026-11-16: every retained leaf failed closed forever after a container restart wiped its record."""
    from mimir_cultivate.worklink import daemon_config, daemon_health, daemon_service, daemon_sessions, identities

    async with FakeOpenCodeDaemon() as daemon:
        root = tmp_path / "daemon"
        (root / "sessions").mkdir(parents=True)
        checkout = (tmp_path / "checkout").resolve()
        checkout.mkdir()
        info = checkout.stat()
        launch = {"launch_generation": "generation-new" if restarted else "generation-old"}
        monkeypatch.setattr(daemon_config, "DAEMON_ROOT", str(root))
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        monkeypatch.setattr(identities, "assert_worklink_identity", lambda ids: None)
        monkeypatch.setattr(daemon_health, "load_api", lambda paths, uid: {
            "url": daemon.base_url, "username": daemon.username, "password": daemon.password})
        monkeypatch.setattr(daemon_service, "RuntimePaths", lambda: SimpleNamespace(daemon=root))
        def read_json(path, uid, mode):
            if path.name == "launch.json":
                return launch
            raise FileNotFoundError(path)
        monkeypatch.setattr(daemon_service, "read_json", read_json)
        import hashlib
        home = root / "sessions" / (hashlib.sha256(f"{checkout}:2151:2".encode()).hexdigest() + ".home")
        (home / "tmp").mkdir(parents=True)
        (home / "tmp" / "sentinel").write_text("attempt scratch")
        (checkout / "work.txt").write_text("retained work")
        output = tmp_path / "stdout"
        output.write_bytes(b"stale")
        fd = os.open(output, os.O_RDWR)
        monkeypatch.setenv("WORKLINK_SESSION_OUTPUT_FD", str(fd))
        request = {"op": op, "issue": 2151, "attempt": 2, "device": info.st_dev, "inode": info.st_ino,
                   "path": str(checkout), "generation": "generation-old", "opencode_session_id": "ses_gone",
                   **({"stdout_limit": 1024} if op == "session_observe" else {})}
        try:
            if not restarted:
                with pytest.raises(FileNotFoundError):
                    await daemon_sessions._executor_operation(request)
                return
            response = await daemon_sessions._executor_operation(request)
        finally:
            os.close(fd)
        if op == "session_observe":
            assert response["execution_state"] == "interrupted"
            assert response["cancellation_state"] == "verified"
            assert response["error_detail"]["type"] == "daemon_restarted"
            assert output.read_bytes() == b""
        elif op == "session_cancel":
            assert response == {"cancellation_state": "verified"}
        else:
            assert response == {"released": True}
            assert not home.exists()
        assert (checkout / "work.txt").read_text() == "retained work"
        if op != "session_release":
            assert (home / "tmp" / "sentinel").exists()
