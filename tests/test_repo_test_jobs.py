"""Repository test continuations outlive turns but never resurrect authority."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mimir import agent as agent_module, mid_turn_injection as mti
from mimir.agent import Agent, _shell_continuation_auth_context
from mimir.models import AgentEvent, AuthContext, InformationFlowLabels, SourceLabel
from mimir.repo_test_jobs import RepoTestJobRegistry


def auth_context(channel_id="repo-test-channel"):
    return AuthContext(
        channel_id=channel_id, principal="alice", canonical_principal="alice",
        roles=("admin",), event_ingress=None, trigger="user_message", interactivity=None,
    )


def enqueue(registry, run, **overrides):
    values = dict(
        repository="o/r", pull_request=9, head_sha="a" * 40, suite="unit",
        selectors=(), channel_id="repo-test-channel", starting_turn_id="turn-1",
        auth=auth_context(),
        labels=InformationFlowLabels().with_source(SourceLabel(
            principal="alice", domain="repository", resource_id="o/r#pull/9",
            bridge_instance="forge", sensitivity="internal", integrity="untrusted",
        )),
        run=run,
    )
    values.update(overrides)
    return registry.enqueue(**values)


@pytest.mark.asyncio
async def test_repo_test_job_deduplication_and_durable_private_result(tmp_path):
    delivered = AsyncMock(return_value=True)
    registry = RepoTestJobRegistry(tmp_path / "jobs", delivered)
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def run(job):
        calls.append(job.job_id)
        started.set()
        await release.wait()
        return {"ok": True, "legs": [{"returncode": 0, "pytest_summary": "1 passed"}]}

    queued = enqueue(registry, run)
    assert queued == dict(status="queued", job_id=queued["job_id"], suite="unit", head_sha="a" * 40)
    await started.wait()
    assert enqueue(registry, run) == queued
    assert len(registry.jobs) == 1
    release.set()
    job = registry.jobs[queued["job_id"]]
    await job.task
    [event] = [call.args[0] for call in delivered.await_args_list]
    assert event.trigger == "repo_test_complete"
    assert event.channel_id == "repo-test-channel"
    assert event.continuation_auth_context is job.auth
    assert event.ifc_labels == job.labels
    assert json.loads(event.content)["result"]["legs"][0]["returncode"] == 0
    assert calls == [queued["job_id"]]
    assert registry.directory.stat().st_mode & 0o777 == 0o700
    path = registry.directory / f"{job.job_id}.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert "auth" not in json.loads(path.read_text())


@pytest.mark.asyncio
async def test_repo_test_turn_cancel_does_not_cancel_job_but_operator_does(tmp_path):
    complete = AsyncMock(return_value=True)
    registry = RepoTestJobRegistry(tmp_path, complete)
    started, reaped, cancel_turn = asyncio.Event(), asyncio.Event(), asyncio.Event()
    result = {}

    async def run(job):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            reaped.set()

    async def turn():
        result.update(enqueue(registry, run))
        await cancel_turn.wait()

    task = asyncio.create_task(turn())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    job = registry.jobs[result["job_id"]]
    assert not job.task.done()
    assert not reaped.is_set()
    await registry.cancel(job.job_id)
    assert reaped.is_set()
    assert job.record["status"] == "cancelled"
    assert "No result exists" in complete.call_args.args[0].content


@pytest.mark.asyncio
async def test_repo_test_cancel_before_worker_start(tmp_path):
    complete = AsyncMock(return_value=True)
    registry = RepoTestJobRegistry(tmp_path, complete)
    run = AsyncMock()
    queued = enqueue(registry, run)
    await registry.cancel(queued["job_id"])
    run.assert_not_called()
    assert registry.jobs[queued["job_id"]].record["status"] == "cancelled"
    assert complete.await_count == 1


@pytest.mark.asyncio
async def test_repo_test_boot_interruption_retires_workers_and_has_no_authority(tmp_path):
    registry = RepoTestJobRegistry(tmp_path, AsyncMock())
    queued = enqueue(registry, AsyncMock())
    job = registry.jobs[queued["job_id"]]
    # Model abrupt process death without cancelling the durable record.
    job.task.cancel()
    await asyncio.gather(job.task, return_exceptions=True)
    registry.save(job, workers=["worker-identity"])
    recovered = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=True))
    cleanup = AsyncMock()
    await recovered.recover(cleanup)
    cleanup.assert_awaited_once()
    assert cleanup.call_args.args[0]["workers"] == ["worker-identity"]
    event = recovered.on_complete.call_args.args[0]
    assert event.trigger == "repo_test_complete"
    assert event.continuation_auth_context is None
    assert event.ifc_labels.labels >= {"unknown", "internal"}
    assert json.loads(event.content)["status"] == "interrupted"
    assert "No result exists" in event.content
    await recovered.recover(cleanup)
    assert recovered.on_complete.await_count == 1


@pytest.mark.asyncio
async def test_repo_test_runtime_shutdown_reaps_before_boot_interruption(tmp_path):
    registry = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=True))
    started, reaped = asyncio.Event(), asyncio.Event()

    async def run(job):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            reaped.set()

    queued = enqueue(registry, run)
    await started.wait()
    await registry.close()
    assert reaped.is_set()
    assert registry.jobs[queued["job_id"]].record["status"] == "interrupted"
    registry.on_complete.assert_not_called()
    recovered = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=True))
    await recovered.recover(AsyncMock())
    assert "No result exists" in recovered.on_complete.call_args.args[0].content


@pytest.mark.asyncio
@pytest.mark.parametrize("inflight", [False, True])
async def test_repo_test_completion_routes_or_folds_with_scope_and_labels(monkeypatch, inflight):
    auth = auth_context()
    labels = InformationFlowLabels(labels=frozenset({"internal"}))
    event = AgentEvent(
        trigger="repo_test_complete", channel_id=auth.channel_id, content='{"ok":true}',
        extra={"job_id": "rt_one", "starting_turn_id": "turn-1", "status": "completed"},
        continuation_auth_context=auth, ifc_labels=labels,
    )
    owner = SimpleNamespace(
        _shell_jobs=SimpleNamespace(get=lambda _: None),
        _dispatcher=SimpleNamespace(enqueue=AsyncMock(return_value=True)),
        _shell_completion_batch_seconds=0,
    )
    monkeypatch.setattr(agent_module, "log_event", AsyncMock())
    if inflight:
        mti.register_inflight(auth.channel_id, turn_id="turn-1")
    try:
        await Agent._on_repo_test_complete(owner, event)
        if inflight:
            [folded] = mti.deactivate(auth.channel_id)[0]
            assert folded is event
            owner._dispatcher.enqueue.assert_not_called()
        else:
            queued = owner._dispatcher.enqueue.call_args.args[0]
            assert queued.trigger == "repo_test_complete"
            assert queued.continuation_auth_context is auth
            assert queued.ifc_labels == labels
            assert event.content in queued.content
        assert _shell_continuation_auth_context(event) is auth
        assert _shell_continuation_auth_context(replace(event, channel_id="other")) is None
    finally:
        mti.deactivate(auth.channel_id)
