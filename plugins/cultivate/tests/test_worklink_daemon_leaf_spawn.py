"""Production consumers against the synthetic pinned daemon, without provider login."""
from __future__ import annotations

import json
import asyncio
from dataclasses import replace

import pytest

from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionError, SessionRequest, WorkOrder
from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend
from mimir_cultivate.worklink.compute import ComputeResult, OpenCodeDaemonComputeBackend
from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
from mimir_cultivate.worklink.daemon_sessions import SessionController
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault, effective_permission


class SessionBoundary:
    """Replace only the native executor transport; retain controller and HTTP."""

    def __init__(self, daemon, client, checkout):
        self.daemon, self.client, self.checkout_path = daemon, client, checkout
        self.controller = None
        self.error = None
        self.text = "done"
        self.finish = True

    async def session_create(self, request, *, timeout_s, pass_env=None, git_settings=()):
        self.controller = await SessionController.create(
            self.client, self.checkout_path / "controller.json", request=request,
            issue_id=2125, attempt=1, generation="generation", checkout=self.checkout_path,
            environment={"PATH": "/usr/bin:/bin", **(pass_env or {})}, timeout_s=timeout_s)
        sid = self.controller.record.opencode_session_id
        return LaunchHandle("opencode_daemon", sid, opencode_session_id=sid, launch_generation="generation")

    async def session_observe(self, handle, *, stdout_limit=64 * 1024 * 1024):
        if self.finish:
            self.daemon.sessions[handle.identifier].messages.append({
                "id": "msg_output", "type": "assistant", "time": {"created": 2}, "agent": "build",
                "model": {"providerID": "openai", "id": "gpt-test"},
                "content": [{"type": "text", "text": self.text}]})
            self.daemon.finish(handle.identifier, outcome="failed" if self.error else "succeeded", error=self.error)
            self.finish = False
        return await self.controller.observe(stdout_limit=stdout_limit)

    async def session_cancel(self, handle):
        if not await self.controller.cancel(timeout_s=1):
            raise RuntimeError("unverified cancellation")

    async def session_release(self, handle):
        await self.controller.release()


async def leaf(tmp_path, daemon, client, *, text="done", error=None):
    order = WorkOrder(2125, tmp_path, "build", "rules first", 30,
                      {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test#high"}, tmp_path / "transcripts")
    backend = OpenCodeBackend(bash_allowlist=("git *", "uv *"))
    spec = backend.work_spec(order, attempt=1, repo_url="repo", base_ref="main",
                             branch="issue/2125-a1", test_command="uv run pytest")
    boundary = SessionBoundary(daemon, client, tmp_path)
    boundary.text, boundary.error = text, error
    compute = OpenCodeDaemonComputeBackend(client=boundary)
    handle = await compute.launch(spec)
    result = await compute.wait(handle, 30)
    raw = await backend.interpret(order, result)
    await compute.cancel(handle)
    await compute.cleanup(handle)
    return spec, result, raw


async def test_leaf_production_create_prompt_model_and_permissions(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, result, raw = await leaf(tmp_path, daemon, client)
        create = next(body for method, route, _, body in daemon.requests if method == "POST" and route == "/api/session")
        assert create == {
            "location": {"directory": str(tmp_path)},
            "model": {"providerID": "openai", "id": "gpt-test", "variant": "high"},
            "permissions": [
                {"action": "external_directory", "resource": "*", "effect": "deny"},
                {"action": "shell", "resource": "*", "effect": "deny"},
                {"action": "shell", "resource": "git *", "effect": "allow"},
                {"action": "shell", "resource": "uv *", "effect": "allow"}],
        }
        prompt = next(body for _, route, _, body in daemon.requests if route.endswith("/prompt"))
        assert prompt == {"text": "rules first\n\nbuild", "resume": True}
        assert spec.local_argv is None and spec.env == {}
        assert "worker_projections" not in spec.backend_config
        assert raw.backend_status == "success"
        assert raw.opencode_session_id == result.handle.identifier


async def assert_leaf_permission_mutation(tmp_path, mutation):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        await leaf(tmp_path, daemon, client)
        rules = next(body["permissions"] for method, route, _, body in daemon.requests if method == "POST" and route == "/api/session")
        agent = [{"action": "shell", "resource": "*", "effect": "allow"},
                 {"action": "external_directory", "resource": "*", "effect": "allow"}]
        assert effective_permission("shell", "git status", agent=agent, session=rules) == "allow"
        assert effective_permission("shell", "uv run pytest", agent=agent, session=rules) == "allow"
        assert effective_permission("shell", "node unknown.js", agent=agent, session=rules) == "deny"
        assert effective_permission("external_directory", "/outside", agent=agent, session=rules) == "deny"
        if mutation == "leaf_shell_deny_removed":
            changed = [rule for rule in rules if not (rule["action"] == "shell" and rule["effect"] == "deny")]
            assert effective_permission("shell", "node unknown.js", agent=agent, session=changed) != "deny"
        elif mutation == "leaf_allowlist_broadened":
            changed = [*rules, {"action": "shell", "resource": "node *", "effect": "allow"}]
            assert effective_permission("shell", "node unknown.js", agent=agent, session=changed) != "deny"
        else:
            changed = [rule for rule in rules if rule["action"] != "external_directory"]
            assert effective_permission("external_directory", "/outside", agent=agent, session=changed) != "deny"


async def test_mutation_leaf_shell_deny_removed(tmp_path):
    await assert_leaf_permission_mutation(tmp_path, "leaf_shell_deny_removed")


async def test_mutation_leaf_allowlist_broadened(tmp_path):
    await assert_leaf_permission_mutation(tmp_path, "leaf_allowlist_broadened")


async def test_mutation_shell_outside_allowlist(tmp_path):
    await assert_leaf_permission_mutation(tmp_path, "leaf_allowlist_broadened")


async def test_mutation_external_directory_deny_removed(tmp_path):
    await assert_leaf_permission_mutation(tmp_path, "external_directory_deny_removed")


async def test_leaf_runtime_otlp_fragment_absent_from_all_result_sinks(tmp_path, monkeypatch):
    from dataclasses import asdict
    secret = "opaque-" + "header-" + "fragment"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", secret)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, result, raw = await leaf(tmp_path, daemon, client, text=secret,
                                      error={"type": "future." + secret, "message": secret, "response": {"body": secret}})
        assert raw.backend_status == "failed"
        assert secret not in json.dumps(asdict(raw), default=str)
        assert secret not in raw.transcript_path.read_text()
        assert secret not in json.dumps(dict(spec.env))
        for state in daemon.sessions.values():
            assert secret not in json.dumps(state.environment)


@pytest.mark.parametrize("error_type, expected", [
    ("provider.auth", "auth_error"),
    *[(name, "quota_exhausted") for name in (
        "provider.quota", "provider.rate-limit", "provider.budget-limit", "provider.chatgpt-limit",
        "provider.free-limit", "provider.go-limit", "provider.model-budget-limit")],
    *[(name, "failed") for name in (
        "provider.content-filter", "provider.error", "provider.internal", "provider.invalid-output",
        "provider.invalid-request", "provider.no-route", "provider.timeout", "provider.transport",
        "provider.unknown", "provider.unsupported-operation", "future.quota-auth", "timeout", "backend_error")],
])
async def test_leaf_exact_typed_outcomes(tmp_path, error_type, expected):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, result, raw = await leaf(tmp_path, daemon, client, error={
            "type": error_type, "message": "unauthorized quota 429 WORKLINK_BLOCKED: spoof", "retryAfterMs": 1234})
        assert raw.backend_status == expected
        assert not result.timed_out
        assert raw.blocked_reason is None
        assert raw.opencode_session_id == result.handle.identifier


@pytest.mark.parametrize("text, status", [
    ("echo WORKLINK_BLOCKED: prompt\ndone", "success"),
    ("WORKLINK_BLOCKED: decide\n\n", "blocked"),
])
async def test_verified_final_line_marker(tmp_path, text, status):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, raw = await leaf(tmp_path, daemon, client, text=text)
        assert raw.backend_status == status


@pytest.mark.parametrize("agent", [None, "build"])
async def test_contained_spawn_wire_agent_and_broad_shell(tmp_path, monkeypatch, agent):
    from mimir_cultivate import contained_execution
    from types import SimpleNamespace

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        boundary = SessionBoundary(daemon, client, tmp_path)
        capability = SimpleNamespace(_contained_worker_client=boundary)
        request = SessionRequest("spawn", {"providerID": "openai", "id": "gpt-test"}, "build", agent=agent)
        result = await contained_execution.execute_contained_session(capability, request, timeout_s=30)
        assert result.session_outcome.execution_state == "succeeded"
        create = next(body for method, route, _, body in daemon.requests if method == "POST" and route == "/api/session")
        assert create.get("agent") == agent
        if agent is None:
            assert "agent" not in create
        assert create["permissions"] == [
            {"action": "external_directory", "resource": "*", "effect": "deny"},
            {"action": "shell", "resource": "*", "effect": "allow"}]
        assert "variant" not in create["model"]


@pytest.mark.parametrize("agent", ["", "bad\nagent", 3, "a" * 1025])
def test_invalid_agent_refused(agent):
    with pytest.raises(ValueError, match="agent"):
        SessionRequest("spawn", {"providerID": "openai", "id": "gpt-test"}, "build", agent=agent)


@pytest.mark.parametrize("retry, expected", [(1, 1), (691200000, 691200000), (0, None),
                                            (-1, None), (691200001, None), (True, None), ("1000", None)])
async def test_leaf_retry_after_is_bounded_and_not_coerced(tmp_path, retry, expected):
    from mimir_cultivate.worklink.backends.base import SessionOutcome
    error = SessionError("provider.quota", "synthetic quota", retry_after_ms=retry)
    result = ComputeResult(1, "", "", session_error=error,
                           session_outcome=SessionOutcome("ses_quota", "failed", error=error))
    raw = await OpenCodeBackend().interpret(WorkOrder(2125, tmp_path, "build", None, 30,
                                                    transcript_root=tmp_path / "transcripts"), result)
    assert raw.backend_status == "quota_exhausted"
    assert raw.retry_after_ms == expected
    assert raw.opencode_session_id == "ses_quota"


async def test_lost_prompt_ack_recovers_same_leaf_without_replay(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("POST", "/api/session/ses_000000000000000000000001/prompt")].append(
            SyntheticFault(disconnect=True, after=True))
        _, _, raw = await leaf(tmp_path, daemon, client)
        assert raw.backend_status == "success"
        assert sum(route.endswith("/prompt") for _, route, _, _ in daemon.requests) == 1


class AdmissionBoundary(SessionBoundary):
    """Lose only the local executor acknowledgement, after real HTTP admission."""

    def __init__(self, *args, lost_ack=False):
        super().__init__(*args)
        self.admitted = asyncio.Event()
        self.acknowledge = asyncio.Event()
        self.lost_ack = lost_ack
        self.recoveries = 0
        self.creates = 0
        self.pause_after = 1

    async def session_create(self, request, **kwargs):
        from mimir_cultivate.worklink.worker_client import WorkerClient
        return await WorkerClient.session_create(self, request, **kwargs)

    def _admission_handle(self, response, generation):
        from mimir_cultivate.worklink.worker_client import WorkerClient
        return WorkerClient._admission_handle(response, generation)

    async def session_recover_admission(self, generation):
        from mimir_cultivate.worklink.worker_client import WorkerClient
        return await WorkerClient.session_recover_admission(self, generation)

    async def _session_rpc(self, operation, payload):
        if operation == "session_create":
            request = SessionRequest(**payload["request"])
            handle = await super().session_create(request, timeout_s=payload["timeout_s"])
            self.creates += 1
            self.finish = True
            if self.creates != self.pause_after:
                return {"opencode_session_id": handle.identifier}
            self.admitted.set()
            await self.acknowledge.wait()
            if self.lost_ack:
                raise RuntimeError("local acknowledgement lost")
            return {"opencode_session_id": handle.identifier}
        assert operation == "session_observe" and payload["opencode_session_id"] is None
        self.recoveries += 1
        return {"admission_state": "retained", "opencode_session_id": self.controller.record.opencode_session_id}


@pytest.mark.parametrize("consumer", ["leaf", "repair", "spawn"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_admission_ack_window_keeps_owned_identity(tmp_path, monkeypatch, consumer, cancelled):
    from types import SimpleNamespace
    from mimir_cultivate import contained_execution
    from mimir_cultivate.worklink.backends.base import SessionAdmissionCancelled

    monkeypatch.setattr("mimir_cultivate.worklink.daemon_health.daemon_health",
                        lambda env: {"ok": True, "launch_generation": "generation"})
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        boundary = AdmissionBoundary(daemon, client, tmp_path, lost_ack=True)
        compute = OpenCodeDaemonComputeBackend(client=boundary)
        order = WorkOrder(2125, tmp_path, "repair" if consumer == "repair" else "build", None, 30,
                          transcript_root=tmp_path / "transcripts")
        spec = OpenCodeBackend(bash_allowlist=("git *", "uv *")).work_spec(
            order, attempt=1, repo_url="repo", base_ref="main", branch="issue/2125-a1", test_command="uv run pytest")
        observed = []
        if consumer == "spawn":
            invocation = contained_execution.execute_contained_session(
                SimpleNamespace(_contained_worker_client=boundary),
                replace(spec.session_request, kind="spawn", shell_allowlist=()), timeout_s=30,
                on_session=observed.append)
        else:
            invocation = compute.launch(spec)
        task = asyncio.create_task(invocation)
        await asyncio.wait_for(boundary.admitted.wait(), 5)
        sid = boundary.controller.record.opencode_session_id
        if cancelled:
            task.cancel()
        boundary.acknowledge.set()
        if cancelled:
            expected = contained_execution.SessionCancelledError if consumer == "spawn" else SessionAdmissionCancelled
            with pytest.raises(expected) as caught:
                await task
            assert caught.value.handle.opencode_session_id == sid
            assert boundary.controller.record.cancellation_state == "verified"
            assert not boundary.controller.path.exists()
        else:
            result = await task
            handle = result.handle if consumer == "spawn" else result
            assert handle.opencode_session_id == sid
            if consumer != "spawn":
                await compute.cancel(handle)
                await compute.cleanup(handle)
        assert boundary.recoveries == 1
        assert sum(route.endswith("/prompt") for _, route, _, _ in daemon.requests) == 1
        assert len(daemon.sessions) == 1


async def test_controller_redacts_current_api_and_publishing_credentials(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        boundary = SessionBoundary(daemon, client, tmp_path)
        request = SessionRequest("spawn", {"providerID": "openai", "id": "gpt-test"}, "build")
        handle = await boundary.session_create(request, timeout_s=30)
        api_secret = "current-" + "api-credential"
        bot_secret = "current-" + "publishing-credential"
        boundary.controller.sensitive_values = (api_secret, bot_secret)
        boundary.text = api_secret + " " + bot_secret
        boundary.error = {"type": "future." + api_secret, "message": bot_secret,
                          "response": {"body": boundary.text}}
        outcome = await boundary.session_observe(handle)
        from dataclasses import asdict
        for secret in (api_secret, bot_secret):
            assert secret not in json.dumps(asdict(outcome))
            assert secret not in boundary.controller.path.read_text()


@pytest.mark.parametrize("phase, failure", [("initial", "cancel"), ("repair", "cancel"),
                                            ("repair", "lost_ack"), ("repair", "exception")])
async def test_runner_admission_and_repair_evidence_use_current_session(tmp_path, monkeypatch, phase, failure):
    from types import SimpleNamespace
    from mimir_cultivate.worklink import orchestrator as orch
    from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig
    from mimir_cultivate.worklink.checkout import CheckoutLease
    from mimir_cultivate.worklink.evidence import EvidenceValidation, WorklinkEvidence
    from mimir_cultivate.worklink.run_state import load_run_state
    from tests.test_worklink_orchestrator import _orchestrator_runner

    repo, checkout = tmp_path / "repo", tmp_path / "checkout"
    checkout.mkdir()
    _, runner = _orchestrator_runner(repo, checkout)
    monkeypatch.setattr(orch, "_cultivate_selected", lambda: False)
    monkeypatch.setattr(orch, "_create_backend_checkout", lambda *args, **kwargs:
                        CheckoutLease(441, 1, repo, checkout, "issue/441-a1", "main", isolated_checkout=True))
    monkeypatch.setattr(orch.WorklinkRunner, "_preserve_unpublished_attempt", lambda *args: None)
    monkeypatch.setattr("mimir_cultivate.worklink.daemon_health.daemon_health",
                        lambda env: {"ok": True, "launch_generation": "generation"})
    monkeypatch.setattr(orch.RepositoryInventory, "load", lambda *args: SimpleNamespace(
        declared=True, repository=lambda slug: SimpleNamespace(worklink_gate="ci", quick_gate_command="uv run pytest",
                                                               base_branch="main")))
    monkeypatch.setattr(orch, "_gate_checkout_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(orch, "_with_outside_checkout_detection", lambda validation, **kwargs: validation)
    for name in ("_commit_checkout_changes", "_ensure_clean_checkout", "_git_push"):
        monkeypatch.setattr(orch, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(orch, "_open_pr", lambda *args, **kwargs: "https://github.com/owner/repo/pull/1")
    monkeypatch.setattr(orch, "_repo_slug", lambda *args, **kwargs: "owner/repo")
    monkeypatch.setattr(orch, "_with_head_sha", lambda validation, *args, **kwargs: validation)

    async def observe(**kwargs):
        return EvidenceValidation("completed", True, (), WorklinkEvidence(
            issue=441, attempt=1, backend="opencode", branch="issue/441-a1", checkout=str(checkout),
            started_at="2026-11-12T00:00:00+00:00", finished_at="2026-11-12T00:00:01+00:00",
            files_changed=["changed.py"], diff_stat="changed", commands=[], tests=None,
            pr_url=None, status="completed", head_sha="abc123"))

    async def red_ci(*args, **kwargs):
        return SimpleNamespace(outcome="red", reason="test failed", excerpt="bounded log")

    monkeypatch.setattr(orch, "observe_evidence", observe)
    monkeypatch.setattr(orch, "wait_for_ci", red_ci)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        boundary = AdmissionBoundary(daemon, client, checkout, lost_ack=failure == "lost_ack")
        boundary.pause_after = 1 if phase == "initial" else 2
        compute = OpenCodeDaemonComputeBackend(client=boundary)
        monkeypatch.setattr("mimir_cultivate.worklink.compute.OpenCodeDaemonComputeBackend", lambda: compute)
        backend = OpenCodeBackend(bash_allowlist=("git *", "uv *"))
        original_interpret = backend.interpret

        async def interpret(self, order, result):
            if boundary.creates == 2 and failure == "exception":
                raise RuntimeError("ordinary finalization failure")
            return await original_interpret(order, result)

        monkeypatch.setattr(OpenCodeBackend, "interpret", interpret)
        registry = BackendRegistry(WorklinkConfig())
        registry.register(backend)
        task = asyncio.create_task(orch.WorklinkRunner(home=tmp_path, repo=repo, runner=runner,
                                                       registry=registry).run(441, backend_name="opencode"))
        admitted = asyncio.create_task(boundary.admitted.wait())
        done, _ = await asyncio.wait((task, admitted), timeout=10, return_when=asyncio.FIRST_COMPLETED)
        assert admitted in done, task.result() if task.done() else "admission did not complete"
        sid = boundary.controller.record.opencode_session_id
        assert boundary.creates == (1 if phase == "initial" else 2)
        if failure == "cancel":
            task.cancel()
        boundary.acknowledge.set()
        if failure == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            result = await task
            assert result.status == "failed"
        evidence_files = list((tmp_path / "state/worklink/evidence").rglob("*.json"))
        assert evidence_files
        evidence = json.loads(evidence_files[-1].read_text())
        assert evidence["opencode_session_id"] == sid
        state = load_run_state(tmp_path, 441)
        if state is not None:
            assert state.opencode_session_id == sid
        assert sum(route.endswith("/prompt") for _, route, _, _ in daemon.requests) == boundary.creates
