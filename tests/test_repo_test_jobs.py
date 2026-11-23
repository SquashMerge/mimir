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
@pytest.mark.parametrize("outcome", ["success", "test_failure", "exception", "cancel"])
async def test_repo_test_reserves_bound_scope_until_takeover(tmp_path, outcome):
    registry = RepoTestJobRegistry(tmp_path / "jobs", AsyncMock(return_value=False))
    started, release = asyncio.Event(), asyncio.Event()
    binding = SimpleNamespace(lease_identity=("checkout", "identity"))

    async def run(job):
        started.set()
        await release.wait()
        if outcome == "exception":
            raise RuntimeError("execution refused")
        return {"ok": outcome == "success"}

    queued = enqueue(registry, run, grant_binding=binding)
    job = registry.jobs[queued["job_id"]]
    await started.wait()
    assert registry.pending_for_scope("O/R", 9) is job
    assert registry.pending_for_scope("o/r", 10) is None
    assert registry.pending_for_scope("other/repo", 9) is None
    if outcome == "cancel":
        await registry.cancel(job.job_id)
    else:
        release.set()
        await job.task
    assert not job.record["delivered"]
    assert registry.pending_for_scope("o/r", 9) is job
    assert registry.on_complete.call_args.args[0].extra["_repo_test_completion"].binding is binding

    recovered = RepoTestJobRegistry(registry.directory, AsyncMock(return_value=True))
    assert recovered.pending_for_scope("o/r", 9) is None
    await recovered.recover(AsyncMock())
    assert recovered.on_complete.call_args.args[0].extra["_repo_test_completion"].binding is None

    registry.on_complete = AsyncMock(return_value=True)
    await registry.deliver(job)
    assert job.record["delivered"] and job.grant_binding is binding
    assert registry.pending_for_scope("o/r", 9) is job
    registry.release_binding(SimpleNamespace(**vars(binding)))
    assert registry.pending_for_scope("o/r", 9) is job
    registry.release_binding(binding)
    assert job.grant_binding is None
    assert registry.pending_for_scope("o/r", 9) is None


@pytest.mark.asyncio
async def test_repo_test_prune_preserves_delivered_reservation(tmp_path, monkeypatch):
    from mimir import repo_test_jobs

    monkeypatch.setattr(repo_test_jobs, "_MAX_DELIVERED_RECORDS", 0)
    registry = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=True))
    binding = SimpleNamespace(lease_identity=("checkout", "identity"))
    queued = enqueue(registry, AsyncMock(return_value={"ok": True}), grant_binding=binding)
    job = registry.jobs[queued["job_id"]]
    await job.task
    registry.prune()
    assert job.record["delivered"]
    assert registry.pending_for_scope("o/r", 9) is job
    assert (tmp_path / f"{job.job_id}.json").exists()
    registry.release_completion(job.job_id)
    registry.prune()
    assert job.job_id not in registry.jobs


@pytest.mark.asyncio
async def test_repo_test_shutdown_and_reclamation_release_only_bound_identity(tmp_path):
    registry = RepoTestJobRegistry(tmp_path / "jobs", AsyncMock(return_value=False))
    started = asyncio.Event()

    async def run(job):
        started.set()
        await asyncio.Event().wait()

    binding = SimpleNamespace(lease_identity=("checkout", "identity"))
    queued = enqueue(registry, run, grant_binding=binding)
    job = registry.jobs[queued["job_id"]]
    await started.wait()
    registry.release_lease(("checkout", "other-identity"))
    assert registry.pending_for_scope("o/r", 9) is job
    registry.release_lease(binding.lease_identity)
    assert registry.pending_for_scope("o/r", 9) is None
    assert job.grant_binding is None
    job.grant_binding = binding
    await registry.close()
    assert job.record["status"] == "interrupted"
    assert registry.pending_for_scope("o/r", 9) is None
    assert job.grant_binding is None


@pytest.mark.asyncio
async def test_repo_test_without_grant_binding_does_not_reserve_scope(tmp_path):
    registry = RepoTestJobRegistry(tmp_path / "jobs", AsyncMock(return_value=False))
    queued = enqueue(registry, AsyncMock(return_value={"ok": True}))
    assert registry.pending_for_scope("o/r", 9) is None
    await registry.jobs[queued["job_id"]].task
    assert registry.pending_for_scope("o/r", 9) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["{partial", "[]", '{"job_id":"rt_broken"}', "invalid_types", "invalid_utf8"])
async def test_repo_test_registry_quarantines_corruption_and_recovers_healthy(tmp_path, monkeypatch, bad):
    from mimir import repo_test_jobs
    warnings = []
    monkeypatch.setattr(repo_test_jobs, "log_durable_event_sync",
                        lambda kind, **fields: warnings.append((kind, fields)))
    registry = RepoTestJobRegistry(tmp_path, AsyncMock())
    queued = enqueue(registry, AsyncMock())
    job = registry.jobs[queued["job_id"]]
    job.task.cancel()
    await asyncio.gather(job.task, return_exceptions=True)
    path = tmp_path / "rt_broken.json"
    if bad == "invalid_types":
        path.write_text(json.dumps({**job.record, "job_id": "rt_broken", "status": []}))
    elif bad == "invalid_utf8":
        path.write_bytes(b"\xff")
    else:
        path.write_text(bad)
    recovered = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=True))
    assert set(recovered.jobs) == {job.job_id}
    assert not path.exists()
    [quarantined] = (tmp_path / "quarantine").iterdir()
    assert quarantined.stat().st_mode & 0o777 == 0o600
    assert quarantined.parent.stat().st_mode & 0o777 == 0o700
    assert warnings[0][0] == "repo_test_record_quarantined"
    assert bad not in str(warnings)
    await recovered.recover(AsyncMock())
    recovered.on_complete.assert_awaited_once()
    assert recovered.on_complete.call_args.args[0].continuation_auth_context is None
    # Quarantine is not re-discovered or repeatedly warned about on next boot.
    RepoTestJobRegistry(tmp_path, AsyncMock())
    assert len(warnings) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [2, 4])
async def test_repo_test_registry_retention_preserves_pending_records(tmp_path, monkeypatch, limit):
    import os
    from mimir import repo_test_jobs
    monkeypatch.setattr(repo_test_jobs.time, "time", lambda: 2_000_000_000)
    monkeypatch.setattr(repo_test_jobs, "_MAX_DELIVERED_RECORDS", limit)
    registry = RepoTestJobRegistry(tmp_path, AsyncMock(return_value=False))
    jobs = []
    for index in range(7):
        queued = enqueue(registry, AsyncMock(return_value={"ok": True}), head_sha=str(index) * 40)
        job = registry.jobs[queued["job_id"]]
        await job.task
        jobs.append(job)
    for index, job in enumerate(jobs):
        # Three recent delivered, one expired delivered, one recent and one
        # expired undelivered, and one old queued job all share the directory.
        registry.save(job, delivered=index < 4, status="queued" if index == 6 else "completed")
        modified = 2_000_000_000 - (8 * 86400 if index >= 3 else index)
        os.utime(tmp_path / f"{job.job_id}.json", (modified, modified))
    recovered = RepoTestJobRegistry(tmp_path, AsyncMock())
    # The larger cap isolates age expiry: without the age guard, index 3
    # would survive. The smaller cap independently isolates count retention.
    survivors = (0, 1, 4, 5, 6) if limit == 2 else (0, 1, 2, 4, 5, 6)
    expected = {jobs[index].job_id for index in survivors}
    assert set(recovered.jobs) == expected
    assert {path.stem for path in tmp_path.glob("rt_*.json")} == expected
    # Enforce retention during a long-lived runtime too, not only at startup.
    monkeypatch.setattr(repo_test_jobs, "_MAX_DELIVERED_RECORDS", 1)
    await recovered.deliver(recovered.jobs[jobs[4].job_id])
    # AsyncMock's default is accepted, so newly delivered history is bounded.
    assert sum(job.record["delivered"] for job in recovered.jobs.values()) == 1
    assert jobs[5].job_id in recovered.jobs and jobs[6].job_id in recovered.jobs


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


@pytest.mark.parametrize("case", ["trusted", "tainted_start", "no_continuation", "shell", "http"])
def test_repo_test_completion_inherits_queueing_turn_integrity(case):
    """A completion turn is as trusted as the turn that queued its tests.

    Without this, the completion's channel source was always untrusted active
    ingest, so the repo_test_complete turn could never check out, comment or
    push, however clean the queueing turn and the attested lease were.
    """
    from mimir.access_control import HTTP_EVENT_INGRESS_EXTRA_KEY
    from mimir.agent import _initialize_ifc_labels

    auth = auth_context()
    labels = InformationFlowLabels().with_source(SourceLabel(
        principal="alice", domain="repository", resource_id="o/r#pull/9@" + "a" * 40,
        bridge_instance="forge", sensitivity="internal", integrity="trusted",
        integrity_effect="active_ingest",
    ))
    if case == "tainted_start":
        labels = labels.with_source(SourceLabel(
            principal="outside", domain="web", resource_id="untrusted-instruction",
            bridge_instance="web", sensitivity="public", integrity="untrusted",
            integrity_effect="active_ingest",
        ))
    extra = {"job_id": "rt_one", "starting_turn_id": "turn-1", "status": "completed"}
    if case == "http":
        extra[HTTP_EVENT_INGRESS_EXTRA_KEY] = {"route": "/events"}
    event = AgentEvent(
        trigger="shell_job_complete" if case == "shell" else "repo_test_complete",
        channel_id=auth.channel_id, source="system", content='{"ok": true}', extra=extra,
        continuation_auth_context=None if case == "no_continuation" else auth,
        ifc_labels=labels,
    )
    result = _initialize_ifc_labels(event)
    tainted = any(source.has_untrusted_active_ingest for source in result.sources)
    assert tainted is (case != "trusted"), [
        (s.domain, s.resource_id, s.integrity, s.integrity_effect) for s in result.sources
    ]
    # The completion's own ingress source, separate from the inherited labels:
    # it is vouched for only when nothing the queueing turn carried was tainted.
    [ingress] = [source for source in result.sources if source not in labels.sources]
    assert ingress.integrity == ("trusted" if case == "trusted" else "untrusted")
