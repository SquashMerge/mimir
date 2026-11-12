"""Synthetic pinned-protocol lifecycle tests; no provider requests."""
from __future__ import annotations

import json

import pytest

from mimir_cultivate.worklink.backends.base import SessionRequest
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient, ServerEvent
from mimir_cultivate.worklink.daemon_sessions import SessionController, session_environment
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault


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
