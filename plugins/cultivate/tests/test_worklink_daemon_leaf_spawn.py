"""Production consumers against the synthetic pinned daemon, without provider login."""
from __future__ import annotations

import json
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


@pytest.mark.parametrize("mutation", ["leaf_shell_deny_removed", "leaf_allowlist_broadened", "external_directory_deny_removed"])
async def test_named_leaf_permission_mutations(tmp_path, mutation):
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


@pytest.mark.parametrize("error_type, expected", [
    ("provider.auth", "auth_error"),
    *[(name, "quota_exhausted") for name in (
        "provider.quota", "provider.rate-limit", "provider.budget-limit", "provider.chatgpt-limit",
        "provider.free-limit", "provider.go-limit", "provider.model-budget-limit")],
    *[(name, "failed") for name in (
        "provider.content-filter", "provider.error", "provider.internal", "provider.invalid-output",
        "provider.invalid-request", "provider.no-route", "provider.timeout", "provider.transport",
        "provider.unknown", "provider.unsupported-operation", "future.quota-auth")],
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
