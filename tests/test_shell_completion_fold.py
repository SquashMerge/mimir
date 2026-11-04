"""Job continuations are bound to the spawning turn, including at the last boundary."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mimir import agent as agent_module
from mimir import mid_turn_injection as mti
from mimir._context import reset_current_turn, set_current_turn
from mimir.agent import Agent
from mimir.models import (
    AuthContext, InformationFlowLabels, Integrity, IntegrityEffect,
    SourceLabel, TurnContext, TurnInteractivity,
)


def _job_source(labels, job_id):
    return next((
        source for source in labels.sources
        if source.resource_id == job_id and source.principal == "external"
        and source.sensitivity == "private"
        and source.authorized_principals == frozenset({"operator"})
        and source.integrity == Integrity.UNTRUSTED
    ), None)


@pytest.fixture(autouse=True)
def isolated_registry():
    yield
    mti.deactivate("scheduler:briefing")


@pytest.fixture
def harness(monkeypatch):
    queued = []
    logged = []
    notices = []
    jobs = {}

    async def enqueue(event):
        queued.append(event)
        return True

    async def notice(channel, text):
        notices.append((channel, text))

    async def log_event(kind, **fields):
        logged.append((kind, fields))

    monkeypatch.setattr(agent_module, "log_event", log_event)
    monkeypatch.setattr(mti, "get_config", lambda: {
        "configurable": {"channel_id": "scheduler:briefing"},
    })
    owner = SimpleNamespace(
        _shell_jobs=SimpleNamespace(
            get=jobs.get,
            read_job_output=lambda job, **kw: {
                "stdout_tail": "untrusted output", "stderr_tail": "warning",
            },
        ),
        _dispatcher=SimpleNamespace(enqueue=enqueue, _send_approval_notice=notice),
        _config=SimpleNamespace(operator_alert_channel="operator"),
        _shell_completion_batch_seconds=0,
        _bg_tasks=set(),
    )

    def make_job(job_id="job-1", turn_id="turn-1", chain=None):
        job = SimpleNamespace(
            job_id=job_id, channel_id="scheduler:briefing", starting_turn_id=turn_id,
            completion_chain_id=chain or turn_id, exit_code=0, status="exited_ok",
            elapsed_seconds=1.5, command="echo secret", auth_context=None,
            ifc_labels=InformationFlowLabels().with_source(SourceLabel(
                principal="external", domain="shell_jobs", resource_id=job_id,
                bridge_instance="shell", sensitivity="private",
                authorized_principals=frozenset({"operator"}),
                integrity=Integrity.UNTRUSTED,
                integrity_effect=IntegrityEffect.ACTIVE_INGEST,
            )),
            output_observed_by_turn=None,
        )
        job.final_output_observed = lambda: job.output_observed_by_turn == job.starting_turn_id
        jobs[job_id] = job
        return job

    return owner, make_job, queued, logged, notices


@pytest.mark.asyncio
async def test_completion_folds_at_boundary_with_notice_record_and_taint(harness):
    owner, job, queued, _, _ = harness
    auth = AuthContext(
        principal="operator", canonical_principal="operator", roles=("admin",),
        event_ingress=None, trigger="scheduled_tick", channel_id="scheduler:briefing",
        interactivity=TurnInteractivity.NON_INTERACTIVE,
    )
    ctx = TurnContext(
        turn_id="turn-1", session_id="scheduler:briefing", trigger="scheduled_tick",
        channel_id="scheduler:briefing", started_at=0, auth_context=auth,
    )
    mti.register_inflight(ctx.channel_id, turn_id=ctx.turn_id)
    token = set_current_turn(ctx)
    try:
        await Agent._on_shell_job_complete(owner, job())
        assert queued == []
        hook = mti.MidTurnInjectionMiddleware().before_model({}, None)
        [message] = hook["messages"]
        assert "[background job job-1 finished: exited_ok, exit code 0, elapsed 1.5s]" in message.content
        assert "Command: echo secret" in message.content
        assert "untrusted output" in message.content
        assert "[mid-turn message" not in message.content
        assert ctx.ifc_labels.persisted_integrity == Integrity.UNTRUSTED
        assert ctx.auth_context.ifc_state.has_untrusted_active_ingest(ctx.ifc_labels)
        # The job's own source, not just default ingress taint: its sensitivity
        # and audience decide where the folded output may be sent.
        assert _job_source(ctx.ifc_labels, "job-1") is not None
        leftovers, folded, _ = mti.deactivate(ctx.channel_id)
        assert leftovers == []
        assert len(folded) == 1
        assert mti.render_injected_message(folded[0][0]) == message.content
        assert queued == []
    finally:
        reset_current_turn(token)


def test_missing_job_output_labels_keep_unknown_output_source(harness):
    _, make_job, _, _, _ = harness
    job = make_job()
    job.ifc_labels = None
    labels = agent_module._shell_completion_labels(job)
    output_sources = [s for s in labels.sources if s.resource_id == "job:job-1:output"]
    assert len(output_sources) == 1
    assert output_sources[0].integrity == Integrity.UNTRUSTED


@pytest.mark.asyncio
async def test_other_turn_job_is_not_folded(harness):
    owner, job, queued, _, _ = harness
    mti.register_inflight("scheduler:briefing", turn_id="unrelated-turn")
    await Agent._on_shell_job_complete(owner, job())
    assert mti.folded_records("scheduler:briefing") == []
    assert mti.deactivate("scheduler:briefing") == ([], [], [])
    assert len(queued) == 1
    assert queued[0].extra["starting_turn_id"] == "turn-1"
    assert "scheduled task already ran" in queued[0].content


@pytest.mark.asyncio
async def test_final_output_already_read_suppresses_completion(harness):
    owner, job, queued, logged, _ = harness
    mti.register_inflight("scheduler:briefing", turn_id="turn-1")
    finished = job()
    finished.output_observed_by_turn = "turn-1"
    await Agent._on_shell_job_complete(owner, finished)
    assert mti.deactivate("scheduler:briefing") == ([], [], [])
    assert queued == []
    assert any(k == "shell_job_complete_suppressed" and fields["reason"] == "output_observed"
               and fields["turn_id"] == "turn-1" for k, fields in logged)


@pytest.mark.asyncio
async def test_output_read_after_completion_arrives_before_boundary(harness, monkeypatch):
    from mimir import event_logger

    owner, job, queued, _, _ = harness
    suppressed = []
    monkeypatch.setattr(event_logger, "log_event_sync", lambda kind, **fields: suppressed.append((kind, fields)))
    mti.register_inflight("scheduler:briefing", turn_id="turn-1")
    finished = job()
    await Agent._on_shell_job_complete(owner, finished)
    finished.output_observed_by_turn = "turn-1"
    assert mti.MidTurnInjectionMiddleware().before_model({}, None) is None
    assert mti.deactivate("scheduler:briefing") == ([], [], [])
    assert queued == []
    assert suppressed[0][1]["reason"] == "output_observed"


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [False, True])
async def test_last_boundary_leftover(harness, read):
    owner, job, queued, logged, _ = harness
    mti.register_inflight("scheduler:briefing", turn_id="turn-1")
    finished = job()
    await Agent._on_shell_job_complete(owner, finished)
    if read:
        finished.output_observed_by_turn = "turn-1"
    leftovers, folded, _ = mti.deactivate("scheduler:briefing")
    assert len(leftovers) == 1 and folded == []
    for event in leftovers:
        await Agent._queue_shell_followup(owner, event)
    assert len(queued) == (0 if read else 1)
    if read:
        assert any(k == "shell_job_complete_suppressed" and v["reason"] == "output_observed"
                   for k, v in logged)
    else:
        assert "end quietly if nothing remains to do" in queued[0].content


@pytest.mark.asyncio
async def test_five_completions_coalesce_into_one_turn(harness):
    owner, job, queued, _, _ = harness
    owner._shell_completion_batch_seconds = 0.02
    done = asyncio.Event()
    original = owner._dispatcher.enqueue

    async def observe(event):
        await original(event)
        done.set()
        return True

    owner._dispatcher.enqueue = observe
    mti.register_inflight("scheduler:briefing", turn_id="turn-1")
    for i in range(5):
        await Agent._on_shell_job_complete(owner, job(f"job-{i}"))
    leftovers, _, _ = mti.deactivate("scheduler:briefing")
    for event in leftovers:
        await Agent._queue_shell_followup(owner, event)
    await asyncio.wait_for(done.wait(), 2)
    assert len(queued) == 1
    assert len(queued[0].extra["completion_job_ids"]) == 5
    assert all(f"job-{i}" in queued[0].content for i in range(5))
    # Every merged job's own source rides on the follow-up event.
    assert all(_job_source(queued[0].ifc_labels, f"job-{i}") is not None for i in range(5))


@pytest.mark.asyncio
async def test_windowed_flush_task_is_retained_and_failures_are_logged(harness, monkeypatch):
    owner, job, queued, logged, _ = harness
    owner._shell_completion_batch_seconds = 0.02
    mti.register_inflight("scheduler:briefing", turn_id="turn-1")
    await Agent._on_shell_job_complete(owner, job("job-a"))
    [leftover], _, _ = mti.deactivate("scheduler:briefing")
    await Agent._queue_shell_followup(owner, leftover)
    [task] = owner._bg_tasks  # held strongly until it finishes
    await asyncio.wait_for(task, 2)
    assert owner._bg_tasks == set()
    assert len(queued) == 1

    calls = []

    def observed(owner_, event):
        calls.append(event)
        if len(calls) > 1:  # the in-flush recheck fails
            raise RuntimeError("registry gone")
        return False

    monkeypatch.setattr(Agent, "_completion_output_observed", staticmethod(observed))
    mti.register_inflight("scheduler:briefing", turn_id="turn-2")
    await Agent._on_shell_job_complete(owner, job("job-b", turn_id="turn-2"))
    [leftover], _, _ = mti.deactivate("scheduler:briefing")
    calls.clear()
    await Agent._queue_shell_followup(owner, leftover)
    [task] = owner._bg_tasks
    assert await asyncio.wait_for(task, 2) is False
    assert ("shell_job_complete_enqueue_failed", {
        "job_id": "job-b", "error": "registry gone",
    }) in logged
    assert len(queued) == 1


@pytest.mark.asyncio
async def test_fourth_chain_followup_is_capped_with_one_notice(harness):
    owner, job, queued, logged, notices = harness
    for i in range(5):
        await Agent._on_shell_job_complete(owner, job(f"job-{i}", turn_id=f"turn-{i}", chain="root"))
    assert len(queued) == 3
    assert len(notices) == 1
    assert notices[0][0] == "operator"
    assert sum(k == "shell_job_complete_suppressed" and v.get("reason") == "chain_cap"
               for k, v in logged) == 2


def test_job_fold_is_independent_of_user_opt_in(harness):
    from mimir.config import Config
    from mimir.dispatcher import Dispatcher

    config = SimpleNamespace(midturn_injection_channels=())
    assert not Dispatcher._injection_enabled(SimpleNamespace(_config=config), "scheduler:briefing")
    mti.register_inflight("scheduler:briefing", turn_id="turn-1", allow_user_messages=False)
    from mimir.models import AgentEvent
    user = AgentEvent(trigger="user_message", channel_id="scheduler:briefing")
    completion = AgentEvent(trigger="shell_job_complete", channel_id="scheduler:briefing")
    assert mti.inject_message("scheduler:briefing", user) == "no_active_turn"
    assert mti.inject_job_completion("scheduler:briefing", "turn-1", completion) == "injected"
    assert [e.trigger for e in mti.deactivate("scheduler:briefing")[0]] == ["shell_job_complete"]
