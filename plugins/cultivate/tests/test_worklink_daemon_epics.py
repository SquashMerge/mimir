"""Epic consumers against the schema-backed synthetic 2.2.7 daemon."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionTransportLostError, WorkOrder
from mimir_cultivate.worklink.backends.feature_factory import FeatureFactoryBackend, FactoryStatus
from mimir_cultivate.worklink.claims import ClaimRecord
from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend, ComputeCleanupError
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient
from mimir_cultivate.worklink.daemon_config import resolve_daemon_credentials
from mimir_cultivate.worklink.daemon_sessions import SessionController, session_environment
from mimir_cultivate.worklink.factory_state import FactoryRunRecord, load_factory_record, save_factory_record
from mimir_cultivate.worklink.orchestrator import IssueContext, WorklinkError, WorklinkRunner, WorklinkRunResult
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault, effective_permission


@pytest.fixture(autouse=True)
def epic_retry_budget(monkeypatch):
    monkeypatch.setenv("MIMIR_FACTORY_MAX_RETRIES", "5")
    from mimir_cultivate.worklink import daemon_health
    monkeypatch.setattr(daemon_health, "daemon_health", lambda env: {"ok": True, "launch_generation": "epic-generation"})


class EpicBoundary:
    """Substitute only executor transport; use the real controller and HTTP API."""

    def __init__(self, daemon, client, checkout):
        self.daemon, self.client, self.checkout = daemon, client, checkout
        self.controller = None
        self.outcome = "succeeded"
        self.error = None
        self.finish = True
        self.cancel_verified = True
        self.create_calls = 0

    async def session_create(self, request, *, timeout_s, pass_env=None, git_settings=()):
        self.create_calls += 1
        private = self.checkout.parent / "daemon-private"
        private.mkdir(mode=0o700, exist_ok=True)
        credentials = resolve_daemon_credentials({"GITHUB_TOKEN": "synthetic-bot-token",
            "MIMIR_FACTORY_PUBLISHING_IDENTITY": "fixture-bot"})
        environment = session_environment(dict(pass_env or {}), home=private / "home",
            git_settings=git_settings, credentials=credentials.environment())
        self.controller = await SessionController.create(self.client, private / "controller.json",
            request=request, issue_id=2125, attempt=1, generation="epic-generation",
            checkout=self.checkout, environment=environment, timeout_s=timeout_s)
        sid = self.controller.record.opencode_session_id
        return LaunchHandle("opencode_daemon", sid, opencode_session_id=sid, launch_generation="epic-generation")

    async def session_observe(self, handle, **kwargs):
        if self.finish:
            self.daemon.finish(handle.identifier, outcome=self.outcome, error=self.error)
            self.finish = False
        try:
            return await self.controller.observe(stdout_limit=kwargs.get("stdout_limit", 64 * 1024 * 1024))
        except DaemonClientError as exc:
            if exc.reason in {"transport failure", "missing log watermark"}:
                raise SessionTransportLostError("synthetic executor transport loss") from exc
            raise

    async def session_cancel(self, handle):
        if not self.cancel_verified or not await self.controller.cancel():
            raise RuntimeError("unverified owned quiescence")

    async def session_release(self, handle):
        await self.controller.release()


async def admit(tmp_path, daemon, client):
    sandbox = tmp_path / "checkout/.factory-sandboxes/chainlink-2125"
    sandbox.mkdir(parents=True)
    order = WorkOrder(2125, sandbox, "not a second prompt", None, 30,
        {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test#high",
         "MIMIR_WORK_ITEM_JSON": json.dumps({"run_id": "chainlink-2125", "title": "epic", "body": "build"}),
         "FACTORY_PUBLISHING_IDENTITY": "fixture-bot"}, tmp_path / "transcripts")
    spec = FeatureFactoryBackend().work_spec(order, attempt=1, repo_url="repo", base_ref="main",
        branch="feature/chainlink-2125", test_command="uv run pytest")
    boundary = EpicBoundary(daemon, client, sandbox)
    compute = OpenCodeDaemonComputeBackend(client=boundary)
    handle = await compute.launch(spec)
    status = FactoryStatus(run_id="chainlink-2125", valid=True, sandbox_path=str(sandbox),
        status="running", mode="autonomous", branch="feature/chainlink-2125", pr_base="main",
        pr_draft=False, lock="fresh", dead_lock=False, lock_session=handle.identifier,
        next="gate:story", next_present=True, mcp_servers=("fixture-mcp",))
    record = FactoryRunRecord(run_id=status.run_id, issue_id=2125, attempt=1, repository="owner/repo",
        base_ref="main", branch=status.branch, launcher="/opt/factory/bin/factory.js", sandbox=str(sandbox),
        session=handle.identifier, opencode_session_id=handle.identifier, handle=handle, status=status,
        mcp_servers=status.mcp_servers, observed_at=datetime.now(UTC).isoformat(), controller_phase="running")
    save_factory_record(tmp_path, record)
    return spec, boundary, compute, handle, record


async def test_epic_production_command_checkout_model_permissions_and_environment(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        create = next(body for method, route, _, body in daemon.requests if method == "POST" and route == "/api/session")
        assert create == {"location": {"directory": record.sandbox},
            "model": {"providerID": "openai", "id": "gpt-test", "variant": "high"},
            "permissions": [{"action": "external_directory", "resource": "*", "effect": "deny"},
                *[{"action": "read", "resource": pattern, "effect": "deny"}
                  for pattern in (".env", "**/.env", "**/.env.*")]]}
        commands = [body for _, route, _, body in daemon.requests if route.endswith("/command")]
        assert commands == [{"name": "feature", "text": "--autonomous --max-retries 5 chainlink-2125"}]
        assert not any(route.endswith("/prompt") for _, route, _, _ in daemon.requests)
        assert spec.local_argv is None
        environment = daemon.sessions[handle.identifier].environment
        assert environment["GH_TOKEN"] == environment["GITHUB_TOKEN"] == "synthetic-bot-token"
        assert environment["FACTORY_PUBLISHING_IDENTITY"] == "fixture-bot"
        assert "FACTORY_SESSION_ID" not in environment
        assert "OPENCODE_PERMISSION" not in environment and "OPENCODE_CONFIG" not in environment
        assert record.session == record.opencode_session_id == handle.identifier
        result = await compute.wait(handle, 30)
        assert result.session_outcome.execution_state == "succeeded"
        await compute.cancel(handle)
        await compute.cleanup(handle)


async def check_permissions(tmp_path, mutation):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, _ = await admit(tmp_path, daemon, client)
        rules = next(body["permissions"] for method, route, _, body in daemon.requests
                     if method == "POST" and route == "/api/session")
        agent = [{"action": "shell", "resource": "*", "effect": "allow"},
                 {"action": "read", "resource": "*", "effect": "allow"},
                 {"action": "external_directory", "resource": "*", "effect": "allow"}]
        for command in ("node /opt/factory/bin/factory.js status", "gh pr view", "uv run verify"):
            assert effective_permission("shell", command, agent=agent, session=rules) == "allow"
        assert effective_permission("read", "secrets/.env.production", agent=agent, session=rules,
                                    project=[{"action": "read", "resource": "*", "effect": "allow"}]) == "deny"
        assert effective_permission("external_directory", "/outside", agent=agent, session=rules) == "deny"
        if mutation == "read":
            changed = [rule for rule in rules if rule["action"] != "read"]
            assert effective_permission("read", ".env", agent=agent, session=changed) != "deny"
        elif mutation == "shell":
            changed = [*rules, {"action": "shell", "resource": "*", "effect": "deny"}]
            assert effective_permission("shell", "node factory.js", agent=agent, session=changed) == "deny"
        else:
            changed = [rule for rule in rules if rule["action"] != "external_directory"]
            assert effective_permission("external_directory", "/outside", agent=agent, session=changed) != "deny"
        await compute.cancel(handle)
        await compute.cleanup(handle)


async def test_mutation_epic_read_deny_removed(tmp_path):
    await check_permissions(tmp_path, "read")


async def test_mutation_epic_shell_deny_added(tmp_path):
    await check_permissions(tmp_path, "shell")


async def test_mutation_external_directory_deny_removed(tmp_path):
    await check_permissions(tmp_path, "external")


@pytest.mark.parametrize("host_outcome", ["succeeded", "failed", "interrupted", "missing"])
async def test_factory_authority_and_host_outcomes(tmp_path, monkeypatch, host_outcome):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.outcome = host_outcome if host_outcome != "missing" else "failed"
        if host_outcome == "failed":
            boundary.error = {"type": "provider.error", "message": "synthetic host failure"}
        if host_outcome == "missing":
            daemon.sessions.pop(handle.identifier)
            boundary.finish = False
        calls = []
        class Factory:
            poll_interval_s = 0.001
            def status(self, *args, **kwargs):
                calls.append("status")
                return replace(record.status, status="completed") if host_outcome == "succeeded" and len(calls) > 1 else record.status
            def heartbeat(self, *args, **kwargs):
                assert kwargs["session"] == handle.identifier
            def terminal(self, *args, **kwargs):
                pytest.fail("host outcome must not rewrite factory state")
        async def completed(self, **kwargs):
            assert kwargs["factory_record"].status.status == "completed"
            return WorklinkRunResult(2125, 1, "review_ready")
        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", completed)
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        claims = SimpleNamespace(heartbeat_issue=lambda *a, **kw: None,
                                 transition_issue=lambda *a, **kw: pytest.fail("unexpected tracker transition"))
        invocation = runner._supervise_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)), claims=claims,
            backend=Factory(), compute=compute, factory_record=record, test_cmd="pytest",
            runner=lambda *a, **kw: pytest.fail("no completion command expected"), started_at=datetime.now(UTC))
        if host_outcome == "succeeded":
            result = await invocation
            assert result.status == "review_ready" and len(calls) >= 2
        else:
            with pytest.raises((WorklinkError, ComputeCleanupError)):
                await invocation
            retained = load_factory_record(tmp_path, record.run_id)
            assert retained.opencode_session_id == handle.identifier
            assert retained.status.status == "running"
        assert boundary.create_calls == 1
        assert len([route for _, route, _, _ in daemon.requests if route.endswith("/command")]) == 1


async def test_unverified_epic_cancel_retains_controller_and_checkout(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.cancel_verified = False
        with pytest.raises(ComputeCleanupError):
            await compute.cancel(handle)
        assert Path(record.sandbox).is_dir()
        assert boundary.controller.path.exists()
        assert load_factory_record(tmp_path, record.run_id).children_reaped is False
        boundary.cancel_verified = True
        await compute.cancel(handle)
        await compute.cleanup(handle)


async def test_epic_lost_command_ack_and_log_eof_restart_never_replay(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        command = client.command
        async def lose_ack(session_id, **kwargs):
            daemon.faults[("POST", f"/api/session/{session_id}/command")].append(
                SyntheticFault(after=True, disconnect=True))
            return await command(session_id, **kwargs)
        monkeypatch.setattr(client, "command", lose_ack)
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        assert boundary.controller.record.admission_state == "uncertain"
        daemon.faults[("GET", f"/api/experimental/session/{handle.identifier}/log")].append(
            SyntheticFault(body=b"", content_type="text/event-stream"))
        boundary.controller = SessionController.recover(client, boundary.controller.path,
            generation=handle.launch_generation, checkout=Path(record.sandbox))
        reattached = OpenCodeDaemonComputeBackend(client=boundary)
        result = await reattached.wait(handle, 30)
        assert result.session_outcome.execution_state == "succeeded"
        assert boundary.controller.record.input_id is not None
        assert boundary.create_calls == 1
        assert len([route for _, route, _, _ in daemon.requests if route.endswith("/command")]) == 1
        await reattached.cancel(handle)
        await reattached.cleanup(handle)


@pytest.mark.parametrize("mode", ["queued", "active", "idle", "noop", "delete_race"])
async def test_epic_cancellation_reconciles_owned_delivery(tmp_path, monkeypatch, mode):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = mode not in {"queued", "delete_race"}
        daemon.delete_race = mode == "delete_race"
        daemon.interrupt_noop = mode == "noop"
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        if mode == "idle":
            daemon.finish(handle.identifier)
        if mode == "noop":
            cancel = boundary.controller.cancel
            async def bounded_cancel():
                return await cancel(timeout_s=0.05)
            monkeypatch.setattr(boundary.controller, "cancel", bounded_cancel)
            with pytest.raises(ComputeCleanupError):
                await compute.cancel(handle)
            assert boundary.controller.record.cancellation_state == "unverified"
            assert boundary.controller.path.exists() and Path(record.sandbox).is_dir()
            daemon.interrupt_noop = False
            monkeypatch.setattr(boundary.controller, "cancel", cancel)
        await compute.cancel(handle)
        assert boundary.controller.record.cancellation_state == "verified"
        assert daemon.sessions[handle.identifier].inbox == []
        assert not daemon.sessions[handle.identifier].active
        await compute.cleanup(handle)


async def test_factory_foreign_lock_is_refused_before_heartbeat(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        class Foreign:
            poll_interval_s = 0.001
            def status(self, *args, **kwargs):
                return replace(record.status, lock_session="ses_foreign")
            def heartbeat(self, *args, **kwargs):
                pytest.fail("foreign lock must never be heartbeated")
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        with pytest.raises(WorklinkError, match="lock session"):
            await runner._supervise_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
                claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
                claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None), backend=Foreign(), compute=compute,
                factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None,
                started_at=datetime.now(UTC))
        retained = load_factory_record(tmp_path, record.run_id)
        assert retained.session == retained.opencode_session_id == handle.identifier
        assert retained.status.lock_session == handle.identifier


async def test_epic_runtime_carriers_are_absent_from_environment_errors_transcript_and_evidence(tmp_path, monkeypatch):
    header = "opaque-" + "epic-header-" + "runtime"
    agent_key = "agent-only-" + "model-key-" + "runtime"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", header)
    monkeypatch.setenv("OPENAI_API_KEY", agent_key)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        record = replace(record, mcp_servers=("fixture-mcp", header, "fixture-mcp"),
                         status=replace(record.status, mcp_servers=("fixture-mcp", header, "fixture-mcp")))
        boundary.outcome = "failed"
        boundary.error = {"type": "future." + header, "message": header, "response": {"body": header}}
        result = await compute.wait(handle, 30)
        assert header not in json.dumps(asdict(result), default=str)
        raw = await FeatureFactoryBackend().interpret(WorkOrder(2125, Path(record.sandbox), "", None, 30), result)
        assert header not in json.dumps(asdict(raw), default=str)
        class Factory:
            poll_interval_s = 0.001
            def status(self, *args, **kwargs):
                return record.status
            def heartbeat(self, *args, **kwargs):
                assert kwargs["session"] == handle.identifier
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        with pytest.raises(WorklinkError) as error:
            await runner._supervise_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
                claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
                claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None), backend=Factory(), compute=compute,
                factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None, started_at=datetime.now(UTC))
        assert header not in str(error.value)
        retained = load_factory_record(tmp_path, record.run_id)
        evidence = (tmp_path / "state/worklink/evidence/2125-1.json").read_text()
        transcript = Path(retained.transcript).read_text()
        for secret in (header, agent_key):
            assert secret not in evidence and secret not in transcript
            assert secret not in json.dumps(daemon.sessions[handle.identifier].environment)
        assert json.loads(evidence)["opencode_session_id"] == handle.identifier
        assert json.loads(evidence)["mcp_servers"] == ["fixture-mcp", "<redacted>", "fixture-mcp"]


async def test_running_factory_reattaches_same_host_without_input(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import orchestrator
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        monkeypatch.setattr(orchestrator, "_verify_factory_recovery_binding", lambda **kwargs: Path(record.sandbox))
        calls = []
        class Factory:
            poll_interval_s = 0.001
            def status(self, *args, **kwargs):
                calls.append("status")
                return replace(record.status, status="completed") if len(calls) > 1 else record.status
            def heartbeat(self, *args, **kwargs):
                assert kwargs["session"] == handle.identifier
            def work_spec(self, *args, **kwargs):
                pytest.fail("reattachment must not prepare another command")
        async def finished(self, **kwargs):
            assert kwargs["factory_record"].opencode_session_id == handle.identifier
            return WorklinkRunResult(2125, 1, "review_ready")
        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finished)
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        result = await runner._recover_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None), backend=Factory(), compute=compute,
            retained=record, launcher=Path(record.launcher), repo_slug=record.repository, base="main",
            test_cmd="pytest", runner=lambda *a, **kw: None, work_item_json="{}")
        assert result.status == "review_ready" and boundary.create_calls == 1
        assert len([route for _, route, _, _ in daemon.requests if route.endswith("/command")]) == 1


@pytest.mark.parametrize("verified,release_fault", [(True, None), (False, None),
    *[(True, fault) for fault in ("failed", "held", "foreign", "history", "next", "status")]])
async def test_fresh_park_recovery_uses_actual_new_context_after_old_quiescence(tmp_path, monkeypatch, verified, release_fault):
    from mimir_cultivate.worklink import orchestrator
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, boundary, compute, old_handle, record = await admit(tmp_path, daemon, client)
        snapshot = tmp_path / "checkout/.factory/.parked/chainlink-2125"
        snapshot.mkdir(parents=True)
        record = replace(record, status=replace(record.status, status="needs-human", park_snapshot=str(snapshot),
                                               terminal_result={"status": "needs-human", "reason": "prior park"}),
                         controller_phase="parked")
        save_factory_record(tmp_path, record)
        boundary.cancel_verified = verified
        monkeypatch.setattr(orchestrator, "_verify_factory_recovery_binding", lambda **kwargs: Path(record.sandbox))
        monkeypatch.setattr(orchestrator, "_git_credential_settings", lambda *a: ())
        def order(self, **kwargs):
            return WorkOrder(2125, Path(record.sandbox), "not a resume prompt", None, 30,
                {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test#high", "MIMIR_WORK_ITEM_JSON": spec.env["MIMIR_WORK_ITEM_JSON"],
                 "FACTORY_PUBLISHING_IDENTITY": "fixture-bot", "GIT_AUTHOR_NAME": "Fixture Bot",
                 "GIT_AUTHOR_EMAIL": "fixture@example.invalid"}), "fixture-bot"
        monkeypatch.setattr(WorklinkRunner, "_factory_launch_order", order)
        release_calls = []
        current = record.status
        post_resume_reads = 0
        class Factory(FeatureFactoryBackend):
            poll_interval_s = 0.001
            def status(self, *args, **kwargs):
                nonlocal post_resume_reads
                if boundary.create_calls == 1:
                    return current
                post_resume_reads += 1
                sid = boundary.controller.record.opencode_session_id
                return replace(current, status="running" if post_resume_reads == 1 else "completed",
                               lock="fresh", dead_lock=False, lock_session=sid)
            def lock(self, *args, **kwargs):
                nonlocal current
                assert args[1] == "release" and kwargs["session"] == old_handle.identifier
                assert boundary.controller.record.cancellation_state == "verified"
                release_calls.append("release")
                if release_fault == "failed":
                    raise RuntimeError("synthetic release refusal")
                if release_fault == "held":
                    return
                current = replace(current, lock="absent", lock_session=None)
                if release_fault == "foreign":
                    current = replace(current, lock="fresh", lock_session="ses_foreign")
                elif release_fault == "history":
                    current = replace(current, terminal_result={"reason": "changed history"})
                elif release_fault == "next":
                    current = replace(current, next="unexpected next")
                elif release_fault == "status":
                    current = replace(current, status="running")
            def resume(self, *args, **kwargs):
                pytest.fail("the controller must not resume using an invented or old context")
            def heartbeat(self, *args, **kwargs):
                assert kwargs["session"] == boundary.controller.record.opencode_session_id
        async def finished(self, **kwargs):
            fresh = kwargs["factory_record"]
            assert fresh.session == fresh.opencode_session_id == boundary.controller.record.opencode_session_id
            assert fresh.session != old_handle.identifier
            assert fresh.status.terminal_result == record.status.terminal_result
            return WorklinkRunResult(2125, 1, "review_ready")
        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finished)
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        invocation = runner._recover_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 2, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None, _issue_comments=lambda *a, **kw: []),
            backend=Factory(poll_interval_s=0.001), compute=compute, retained=record, launcher=Path(record.launcher),
            repo_slug=record.repository, base="main", test_cmd="pytest", work_item_json=spec.env["MIMIR_WORK_ITEM_JSON"],
            runner=lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "https://github.com/owner/repo.git\n", ""))
        if verified and release_fault is None:
            result = await invocation
            assert result.status == "review_ready"
            assert release_calls == ["release"] and boundary.create_calls == 2
            assert len([route for _, route, _, _ in daemon.requests if route.endswith("/command")]) == 2
        elif not verified:
            with pytest.raises(ComputeCleanupError):
                await invocation
            assert release_calls == [] and boundary.create_calls == 1
            assert load_factory_record(tmp_path, record.run_id).opencode_session_id == old_handle.identifier
            boundary.cancel_verified = True
            await compute.cancel(old_handle)
            await compute.cleanup(old_handle)
        else:
            with pytest.raises(RuntimeError):
                await invocation
            assert release_calls == ["release"] and boundary.create_calls == 1
            assert load_factory_record(tmp_path, record.run_id).opencode_session_id == old_handle.identifier
            assert boundary.controller.record.cancellation_state == "verified"
            assert len([route for _, route, _, _ in daemon.requests if route.endswith("/command")]) == 1


@pytest.mark.parametrize("requested", [True, False])
async def test_explicit_epic_stop_is_bound_to_the_actual_host(tmp_path, requested):
    from mimir_cultivate.worklink.control import _mark_operator_stop
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.outcome = "interrupted"
        _mark_operator_stop(tmp_path, 2125, 1, opencode_session_id=handle.identifier if requested else "ses_old_host")
        class Factory:
            poll_interval_s = 0.001
            def status(self, *a, **kw):
                return record.status
            def heartbeat(self, *a, **kw):
                assert kw["session"] == handle.identifier
            def terminal(self, *a, **kw):
                pytest.fail("a host interruption cannot invent a factory terminal state")
        runner = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        invocation = runner._supervise_factory_070(issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None), backend=Factory(), compute=compute,
            factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None, started_at=datetime.now(UTC))
        if requested:
            assert (await invocation).status == "interrupted"
            assert load_factory_record(tmp_path, record.run_id).controller_phase == "stopped"
        else:
            with pytest.raises(WorklinkError, match="host stopped"):
                await invocation
        evidence = json.loads((tmp_path / "state/worklink/evidence/2125-1.json").read_text())
        assert evidence["opencode_session_id"] == handle.identifier
        assert evidence["status"] == ("interrupted" if requested else "failed")


@pytest.mark.parametrize("ending", ["running", "completed", "foreign", "unverified"])
@pytest.mark.parametrize("verified_evidence", [False, True])
async def test_operator_stop_consumer_retains_or_releases_only_after_owned_quiescence(tmp_path, monkeypatch, ending, verified_evidence):
    from mimir_cultivate.worklink import control, identities
    from mimir_cultivate.worklink.worker_client import WorkerClient
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, _, handle, record = await admit(tmp_path, daemon, client)
        loop = asyncio.get_running_loop()
        lookups = 0
        def status(self, *a, **kw):
            nonlocal lookups
            lookups += 1
            if ending == "foreign":
                return replace(record.status, lock_session="ses_foreign")
            return replace(record.status, status="completed") if ending == "completed" and lookups > 1 else record.status
        monkeypatch.setattr(FeatureFactoryBackend, "status", status)
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        class Peer:
            async def session_cancel(self, requested):
                assert requested == handle
                future = asyncio.run_coroutine_threadsafe(
                    boundary.controller.cancel(timeout_s=0.05 if ending == "unverified" else 25), loop)
                if not await asyncio.wrap_future(future):
                    raise RuntimeError("owned input still active")
        monkeypatch.setattr(WorkerClient, "for_path_checkout", lambda *a, **kw: Peer())
        daemon.interrupt_noop = ending == "unverified"
        proof = tmp_path / "state/worklink/evidence/2125-1.json"
        if verified_evidence:
            proof.parent.mkdir(parents=True)
            proof.write_text(json.dumps({"status": "completed", "head_sha": "verified-head",
                "review_ready": True, "issue": record.issue_id, "attempt": record.attempt,
                "backend": "feature_factory", "checkout": record.sandbox, "branch": record.branch,
                "opencode_session_id": handle.identifier}))
        prior = proof.read_bytes() if proof.exists() else None
        calls = []
        def command(argv):
            calls.append(list(argv))
            assert boundary.controller.record.cancellation_state == "verified"
            return subprocess.CompletedProcess(argv, 0, "", "")
        try:
            result = await asyncio.to_thread(control.stop_worklink, tmp_path, 2125, runner=command)
            retained = load_factory_record(tmp_path, record.run_id)
            assert retained.opencode_session_id == handle.identifier
            if ending in {"foreign", "unverified"}:
                assert not result.stopped and not result.claim_released
                assert calls == [] and not retained.children_reaped
                assert boundary.controller.path.exists()
            else:
                assert result.stopped and result.claim_released and retained.children_reaped
                assert retained.controller_phase == ("terminal" if ending == "completed" else "stopped")
                if verified_evidence:
                    assert proof.read_bytes() == prior
                else:
                    assert json.loads(proof.read_text())["status"] == "interrupted"
                    assert json.loads(proof.read_text())["opencode_session_id"] == handle.identifier
                    assert json.loads(proof.read_text())["mcp_servers"] == ["fixture-mcp"]
        finally:
            daemon.interrupt_noop = False
            await boundary.controller.release()


@pytest.mark.parametrize("outcome", ["succeeded", "interrupted"])
@pytest.mark.parametrize("missed_poll", [False, True])
async def test_live_supervisor_observes_operator_stop_after_idle_turn(tmp_path, monkeypatch, outcome, missed_poll):
    import threading
    from mimir_cultivate.worklink import control, identities
    from mimir_cultivate.worklink.worker_client import WorkerClient
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.outcome = outcome
        loop = asyncio.get_running_loop()
        finished, polling = asyncio.Event(), asyncio.Event()
        release_poll = threading.Event()
        stopping = False
        released = False
        real_wait = compute.wait
        async def wait(*args, **kwargs):
            result = await real_wait(*args, **kwargs)
            finished.set()
            return result
        monkeypatch.setattr(compute, "wait", wait)
        def status(*args, **kwargs):
            if finished.is_set() and not stopping:
                loop.call_soon_threadsafe(polling.set)
                assert release_poll.wait(3), "operator stop did not finish"
                if missed_poll:
                    raise RuntimeError("synthetic missed poll after stop")
            return record.status
        def heartbeat(*args, **kwargs):
            assert not released, "supervisor heartbeated a released claim"
        monkeypatch.setattr(FeatureFactoryBackend, "status", status)
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        class Peer:
            async def session_cancel(self, requested):
                assert requested == handle
                future = asyncio.run_coroutine_threadsafe(boundary.controller.cancel(), loop)
                assert await asyncio.wrap_future(future)
        monkeypatch.setattr(WorkerClient, "for_path_checkout", lambda *a, **kw: Peer())
        task = asyncio.create_task(WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=heartbeat),
            backend=SimpleNamespace(status=status, heartbeat=heartbeat, poll_interval_s=0.001),
            compute=compute, factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None,
            started_at=datetime.now(UTC)))
        try:
            await asyncio.wait_for(polling.wait(), 3)
            stopping = True
            stop = await asyncio.to_thread(control.stop_worklink, tmp_path, 2125,
                runner=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""))
            assert stop.stopped and stop.claim_released
            released = True
            release_poll.set()
            result = await asyncio.wait_for(task, 3)
            assert result.status == "interrupted"
            saved = load_factory_record(tmp_path, record.run_id)
            assert saved.controller_phase == "stopped" and saved.status.status == "running"
            assert saved.opencode_session_id == handle.identifier
            assert sum(route.endswith("/command") for _, route, _, _ in daemon.requests) == 1
        finally:
            release_poll.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("missed_poll", [False, True])
async def test_successful_host_lock_refusal_precedes_rewait(tmp_path, missed_poll):
    from mimir_cultivate.worklink.control import _mark_operator_stop
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        _mark_operator_stop(tmp_path, 2125, 1, opencode_session_id="ses_other_host")
        daemon.sessions[handle.identifier].messages.append({
            "id": "msg_refusal", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "synthetic"},
            "content": [{"type": "text", "text": "Another live session holds the run lock; did not acquire the lock"}],
        })
        def status(*args, **kwargs):
            if missed_poll:
                raise RuntimeError("synthetic unavailable status")
            return replace(record.status, lock="absent", lock_session=None)
        invocation = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None, transition_issue=lambda *a, **kw: None),
            backend=SimpleNamespace(status=status, poll_interval_s=0.001), compute=compute,
            factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None, started_at=datetime.now(UTC))
        if missed_poll:
            with pytest.raises(WorklinkError, match="did not acquire the retained run lock.*Another live session"):
                await asyncio.wait_for(invocation, 3)
        else:
            result = await asyncio.wait_for(invocation, 3)
            assert result.status == "needs-human" and "Another live session holds" in result.reason
        assert sum(route.endswith("/command") for _, route, _, _ in daemon.requests) == 1
        assert load_factory_record(tmp_path, record.run_id).opencode_session_id == handle.identifier


@pytest.mark.parametrize("failure", ["cancel", "release"])
async def test_completed_observation_cleanup_failure_has_actual_session_evidence(tmp_path, monkeypatch, failure):
    from mimir_cultivate.opencode import redact_daemon_output
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        names = ("first", "fixture-mcp", "fixture-mcp", "token=" + "ghp_" + "A" * 36)
        status = replace(record.status, status="completed", mcp_servers=names)
        record = replace(record, mcp_servers=names)
        save_factory_record(tmp_path, record)
        proof = tmp_path / "state/worklink/evidence/2125-1.json"
        assert not proof.exists()
        if failure == "cancel":
            boundary.cancel_verified = False
        else:
            async def fail_release(handle):
                raise RuntimeError("synthetic release failure")
            monkeypatch.setattr(boundary, "session_release", fail_release)
        invocation = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)), claims=object(),
            backend=SimpleNamespace(status=lambda *a, **kw: status, heartbeat=lambda *a, **kw: None, poll_interval_s=0.001),
            compute=compute, factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None,
            started_at=datetime.now(UTC))
        with pytest.raises(ComputeCleanupError):
            await invocation
        payload = json.loads(proof.read_text())
        assert payload["status"] == "failed" and payload["failure_reason"]
        assert payload["opencode_session_id"] == handle.identifier
        assert payload["mcp_servers"] == [redact_daemon_output(name) for name in names]
        assert not load_factory_record(tmp_path, record.run_id).children_reaped
        assert boundary.controller.path.exists() and Path(record.sandbox).is_dir()
        await boundary.controller.release()


@pytest.mark.parametrize("ending", ["completed", "needs-human", "foreign"])
async def test_budget_park_requalifies_factory_after_session_cancellation(tmp_path, monkeypatch, supervisor_clock, ending):
    from mimir_cultivate.worklink import orchestrator
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.finish = False
        cancelled = False
        real_cancel = compute.cancel
        async def cancel(selected):
            nonlocal cancelled
            await real_cancel(selected)
            cancelled = True
        monkeypatch.setattr(compute, "cancel", cancel)
        winning = replace(record.status, status=ending if ending != "foreign" else "running",
            lock_session="ses_foreign" if ending == "foreign" else handle.identifier)
        def status(*args, **kwargs):
            if cancelled:
                return winning
            # Cross the 100-second supervisor budget's park boundary only once
            # its owned host lock has been observed. HTTP and cleanup keep real
            # clocks; their scheduling must not decide when the budget fires.
            supervisor_clock.now = 60.0
            return record.status
        async def verify(**kwargs):
            assert kwargs["record"].status == winning
            return tmp_path / "verified.json", "https://github.com/owner/repo/pull/1"
        monkeypatch.setattr(orchestrator, "_verify_factory_completion", verify)
        monkeypatch.setattr(orchestrator, "_enforce_factory_pr_publication", lambda *a, **kw: None)
        def terminal(*args, **kwargs):
            pytest.fail("a post-cancellation outcome or foreign lock must not be terminalized")
        invocation = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None, transition_issue=lambda *a, **kw: None),
            backend=SimpleNamespace(status=status,
                heartbeat=lambda *a, **kw: None, terminal=terminal, poll_interval_s=0.001),
            compute=compute, factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None,
            started_at=datetime.now(UTC))
        if ending == "foreign":
            with pytest.raises(WorklinkError, match="owner changed during cancellation"):
                await asyncio.wait_for(invocation, 10)
        else:
            # Hang guard remains below the session's 30-second work deadline;
            # crossing the budget is proved by the controlled clock above.
            result = await asyncio.wait_for(invocation, 10)
            assert result.status == ("review_ready" if ending == "completed" else "needs-human")
            assert load_factory_record(tmp_path, record.run_id).status == winning
        assert cancelled
        assert sum(route.endswith("/command") for _, route, _, _ in daemon.requests) == 1


@pytest.fixture
def supervisor_clock(monkeypatch):
    from mimir_cultivate.worklink import orchestrator
    clock = SimpleNamespace(now=0.0)
    # Only the supervisor sees this clock; HTTP, asyncio timers and the session
    # controller retain their real clocks and bounded cancellation deadlines.
    monkeypatch.setattr(orchestrator, "asyncio", SimpleNamespace(**{
        **vars(asyncio), "get_running_loop": lambda: SimpleNamespace(time=lambda: clock.now),
    }))
    monkeypatch.setattr(orchestrator, "_epic_run_timeout_s", lambda: 100.0)
    return clock


@pytest.mark.parametrize("deadline_phase", ["before-verification", "during-snapshot"])
@pytest.mark.parametrize("failure", [None, "missing-ack", "unverified-cancel"])
@pytest.mark.parametrize("entrypoint", ["supervisor", "outer"])
async def test_budget_park_verifies_after_exhausted_work_deadline(
    tmp_path, monkeypatch, supervisor_clock, deadline_phase, failure, entrypoint,
):
    from mimir_cultivate.worklink import orchestrator
    from mimir_cultivate.worklink.factory_state import factory_checkout_interlock, factory_issue_resource_lock
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.finish = False
        boundary.cancel_verified = failure != "unverified-cancel"
        calls = []
        current = record.status
        snapshot = Path(record.sandbox).parent.parent / ".factory/.parked" / record.run_id
        real_cancel = compute.cancel
        async def cancel(selected):
            assert selected == handle
            await real_cancel(selected)
            calls.append("cancel")
            if deadline_phase == "before-verification":
                supervisor_clock.now = 101.0
        monkeypatch.setattr(compute, "cancel", cancel)
        class Factory(FeatureFactoryBackend):
            poll_interval_s = 0.001
            def admit(self):
                return Path(record.launcher)
            def status(self, *args, **kwargs):
                calls.append("ack" if current.park_snapshot else "status")
                if "snapshot" in calls and failure == "missing-ack":
                    # Exhaust the finite acknowledgment window without waiting
                    # on a real clock or accepting the writer's exit as proof.
                    supervisor_clock.now += 301.0
                return current
            def heartbeat(self, *args, **kwargs):
                assert kwargs["session"] == handle.identifier
                supervisor_clock.now = 50.0
            def terminal(self, *args, **kwargs):
                nonlocal current
                assert boundary.controller.record.cancellation_state == "verified"
                assert supervisor_clock.now == (101.0 if deadline_phase == "before-verification" else 50.0)
                assert args == (record.run_id, "needs-human")
                calls.append("terminal")
                current = replace(current, status="needs-human")
            def snapshot(self, *args, **kwargs):
                nonlocal current
                calls.append("snapshot")
                snapshot.mkdir(parents=True)
                if deadline_phase == "during-snapshot":
                    # Cross both the original work deadline and the first
                    # verification window while the actual writer is running.
                    supervisor_clock.now = 1000.0
                if failure != "missing-ack":
                    current = replace(current, park_snapshot=str(snapshot))
        transitions = []
        releases = []
        claim_record = ClaimRecord(2125, 1, "fixture", datetime.now(UTC))
        def release_issue(issue_id):
            assert issue_id == 2125
            saved = load_factory_record(tmp_path, record.run_id)
            assert saved.controller_phase == "parked" and saved.children_reaped
            assert boundary.controller.record.cancellation_state == "verified"
            assert not boundary.controller.path.exists() and not compute.job_alive(handle)
            assert saved.session == saved.opencode_session_id == handle.identifier
            releases.append(issue_id)
            return True
        claims = SimpleNamespace(heartbeat_issue=lambda *a, **kw: None,
            transition_issue=lambda *a, **kw: transitions.append((a, kw)), release_issue=release_issue)
        issue = IssueContext(2125, "epic", "build", {"worklink:epic"})
        factory = Factory(poll_interval_s=0.001)
        controller = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        if entrypoint == "outer":
            # Replace external admission/tracker transport, not recovery,
            # supervision, finalization, resource guards or the release decision.
            def claim_issue(issue_id, comments, **kwargs):
                assert issue_id == 2125
                kwargs["before_claim"]()
                return SimpleNamespace(claimed=True, record=claim_record, attempts_exhausted=False)
            claims.claim_issue = claim_issue
            monkeypatch.setattr(orchestrator, "ChainlinkClaims", lambda **kw: claims)
            monkeypatch.setattr(orchestrator.ChainlinkIssueReader, "read", lambda self, selected: issue)
            monkeypatch.setattr(orchestrator, "_repo_remote_url", lambda *a, **kw: "https://github.com/owner/repo.git")
            monkeypatch.setattr(orchestrator, "_verify_factory_recovery_target", lambda **kw: Path(record.sandbox))
            monkeypatch.setattr(orchestrator, "_verify_factory_recovery_binding", lambda **kw: Path(record.sandbox))
            monkeypatch.setattr(orchestrator, "_request_tracker_transition", lambda *a, **kw: None)
            controller = replace(controller,
                registry=SimpleNamespace(get=lambda name: factory, select_compute=lambda **kw: compute),
                runner=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""))
            results = await asyncio.gather(controller._run_factory_070(2125, autonomous=False), return_exceptions=True)
        else:
            with factory_checkout_interlock(tmp_path) as outer, factory_issue_resource_lock(tmp_path, 2125) as inner:
                assert outer and inner
                results = await asyncio.gather(controller._supervise_factory_070(
                    issue=issue, claim_record=claim_record, claims=claims,
                    backend=factory, compute=compute, factory_record=record, test_cmd="pytest",
                    runner=lambda *a, **kw: None, started_at=datetime.now(UTC)), return_exceptions=True)
        result = results[0]
        def resources_available():
            with factory_checkout_interlock(tmp_path, pruning=True) as outer, factory_issue_resource_lock(tmp_path, 2125) as inner:
                return outer, inner
        assert await asyncio.to_thread(resources_available) == (True, True)
        if failure == "unverified-cancel":
            if entrypoint == "outer":
                assert isinstance(result, WorklinkRunResult) and result.status == "failed"
                assert "daemon session quiescence unverified" in result.reason
                assert len(transitions) == 1 and transitions[0][1]["status"] == "failed"
            else:
                assert isinstance(result, ComputeCleanupError)
                assert transitions == []
            assert releases == []
            assert "terminal" not in calls and "snapshot" not in calls
            saved = load_factory_record(tmp_path, record.run_id)
            assert saved.status.status == "running" and not saved.children_reaped
            assert saved.session == saved.opencode_session_id == handle.identifier
            assert boundary.controller.path.exists() and Path(record.sandbox).is_dir()
            assert not (tmp_path / "state/worklink/clean-parks/2125-1.json").exists()
            boundary.cancel_verified = True
            await compute.cancel(handle)
            await compute.cleanup(handle)
            return
        assert isinstance(result, WorklinkRunResult), f"park finalization failed: {result}"
        assert result.status == "needs-human" and not result.review_ready
        assert releases == ([2125] if entrypoint == "outer" else [])
        assert calls[calls.index("cancel"):] == ["cancel", "status", "terminal", "status", "snapshot",
            "status" if failure == "missing-ack" else "ack"]
        saved = load_factory_record(tmp_path, record.run_id)
        assert saved.controller_phase == "parked" and saved.children_reaped
        assert saved.session == saved.opencode_session_id == handle.identifier
        assert saved.status.park_snapshot == (None if failure == "missing-ack" else str(snapshot))
        assert saved.mcp_servers == ("fixture-mcp",)
        assert not boundary.controller.path.exists() and Path(record.sandbox).is_dir()
        assert not compute.job_alive(handle)
        assert (tmp_path / "state/worklink/clean-parks/2125-1.json").is_file() == (failure is None)
        if failure == "missing-ack":
            assert "snapshot publication failed" in saved.controller_error
            assert "snapshot publication failed" in result.reason
        assert len(transitions) == 1 and transitions[0][1]["status"] == "blocked"
        assert transitions[0][1]["review_ready"] is False
        assert sum(route.endswith("/command") for _, route, _, _ in daemon.requests) == 1
        with factory_checkout_interlock(tmp_path, pruning=True) as outer, factory_issue_resource_lock(tmp_path, 2125) as inner:
            assert outer and inner


@pytest.mark.parametrize("writer", ["release", "terminal", "snapshot", "heartbeat"])
@pytest.mark.parametrize("cancellation", ["repeated", "timeout"])
async def test_bounded_node_writer_drains_at_real_lifecycle_call_sites(
    tmp_path, monkeypatch, supervisor_clock, writer, cancellation,
):
    import threading
    from mimir_cultivate.worklink import orchestrator
    from mimir_cultivate.worklink.factory_state import factory_checkout_interlock, factory_issue_resource_lock
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        entered = asyncio.Event()
        finish_writer = threading.Event()
        settled = threading.Event()
        loop = asyncio.get_running_loop()
        effects = []
        def write(name):
            if name != writer:
                return
            loop.call_soon_threadsafe(entered.set)
            assert finish_writer.wait(3), "test did not release bounded writer"
            effects.append(name)
            settled.set()
        parked = False
        class Factory(FeatureFactoryBackend):
            def status(self, *args, **kwargs):
                return replace(record.status, status="needs-human" if parked or writer == "release" else "running",
                    park_snapshot=str(tmp_path / "snapshot") if writer == "release" else None)
            def heartbeat(self, *args, **kwargs):
                write("heartbeat")
                supervisor_clock.now = 50.0
            def lock(self, *args, **kwargs):
                assert args[1] == "release"
                write("release")
            def terminal(self, *args, **kwargs):
                nonlocal parked
                write("terminal")
                parked = True
            def snapshot(self, *args, **kwargs):
                write("snapshot")
        factory = Factory(entrypoint=record.launcher, poll_interval_s=0.001)
        monkeypatch.setattr(orchestrator, "_verify_factory_recovery_binding", lambda **kw: Path(record.sandbox))
        monkeypatch.setattr(orchestrator, "_repo_remote_url", lambda *a, **kw: "https://github.com/owner/repo.git")
        monkeypatch.setattr(WorklinkRunner, "_factory_launch_order", lambda self, **kw: (
            WorkOrder(2125, Path(record.sandbox), "", None, 30,
                {"MIMIR_WORK_ITEM_JSON": spec.env["MIMIR_WORK_ITEM_JSON"], "MIMIR_MODEL_SPEC": "codex-plus:test",
                 "FACTORY_PUBLISHING_IDENTITY": "fixture-bot", "GIT_AUTHOR_NAME": "Fixture",
                 "GIT_AUTHOR_EMAIL": "fixture@example.invalid"}, tmp_path / "transcripts"), "fixture-bot"))
        controller = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))
        async def invoke():
            # The actual controller resource guards surround the actual recovery
            # or supervision method, not a direct call to the drain helper.
            with factory_checkout_interlock(tmp_path) as outer:
                assert outer
                with factory_issue_resource_lock(tmp_path, 2125) as inner:
                    assert inner
                    if writer == "release":
                        return await controller._recover_factory_070(
                            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
                            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)), claims=object(),
                            backend=factory, compute=compute, retained=record, launcher=Path(record.launcher),
                            repo_slug=record.repository, base="main", test_cmd="pytest", runner=lambda *a, **kw: None,
                            work_item_json=spec.env["MIMIR_WORK_ITEM_JSON"])
                    return await controller._supervise_factory_070(
                        issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
                        claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)), claims=SimpleNamespace(
                            heartbeat_issue=lambda *a, **kw: None, transition_issue=lambda *a, **kw: None),
                        backend=factory, compute=compute, factory_record=record, test_cmd="pytest",
                        runner=lambda *a, **kw: None, started_at=datetime.now(UTC))
        task = asyncio.create_task(invoke())
        timeout_task = None
        waiting = None
        def resources_available():
            with factory_checkout_interlock(tmp_path, pruning=True) as outer:
                with factory_issue_resource_lock(tmp_path, 2125) as inner:
                    return outer, inner
        try:
            waiting = asyncio.create_task(entered.wait())
            done, _ = await asyncio.wait((task, waiting), timeout=3, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                await task  # Surface a pre-writer production-path refusal.
            assert waiting in done, "production path never entered its writer"
            if cancellation == "timeout":
                timeout_task = asyncio.create_task(asyncio.wait_for(task, 0.01))
                async def cancellation_arrived():
                    while not task.cancelling():
                        await asyncio.sleep(0)
                await asyncio.wait_for(cancellation_arrived(), 1)
            else:
                task.cancel()
            for _ in range(2):
                await asyncio.sleep(0)
                task.cancel()
            assert not task.done() and not settled.is_set() and effects == []
            assert await asyncio.to_thread(resources_available) == (False, False)
            finish_writer.set()
            if timeout_task is not None:
                with pytest.raises(TimeoutError):
                    await timeout_task
            else:
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert effects == [writer] and settled.is_set()
            assert await asyncio.to_thread(resources_available) == (True, True)
        finally:
            finish_writer.set()
            if waiting is not None and not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
            await asyncio.gather(task, *( [timeout_task] if timeout_task is not None else []), return_exceptions=True)
            await boundary.controller.release()
