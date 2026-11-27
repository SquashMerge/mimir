"""Real lease contention, deadline mutations, and untrusted relay inputs."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("mimir_cultivate")

from mimir.heavy_run import helper_path
from mimir_cultivate.worklink.lease_wait import LeaseRelay, BuilderLeaseCompute
from mimir_cultivate.worklink.compute import ComputeResult, WorkSpec, LaunchHandle
from mimir_cultivate.worklink.claims import ChainlinkClaims, ClaimRecord, ShutdownAbortRecord, LEASE_WAIT_PREFIX, SHUTDOWN_ABORT_PREFIX
from mimir_cultivate.worklink.dispatch_failures import (
    record_lease_wait_backoff, pending_failure_alerts, autonomous_dispatch_block_reason,
)


async def eventually(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


async def holder(env):
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-I", str(helper_path()), sys.executable, "-I", "-c",
        "import sys; sys.stdin.buffer.read()",
        env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
    )
    assert await asyncio.wait_for(process.stdout.readline(), 5) == b"queued\n"
    assert await asyncio.wait_for(process.stdout.readline(), 5) == b"acquired\n"
    return process


@pytest.fixture
def lease_environment(tmp_path):
    return {
        "MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy.lock"),
        "MIMIR_HEAVY_LOCK_TIMEOUT": "10",
        "MIMIR_HEAVY_HEARTBEAT_SECONDS": "0.05",
        "MIMIR_HEAVY_REPORT_FD": "1",
    }


class RealBuilder(BuilderLeaseCompute):
    """Exercises the common launch/wait boundary with an actual heavy helper."""
    async def _launch(self, spec):
        self.spec = spec
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, "-I", str(helper_path()), sys.executable, "-I", "-c",
            getattr(self, "payload", "print('done')"),
            env={**self.environment, **spec.env, "MIMIR_HEAVY_REPORT_FD": ""},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        return LaunchHandle("test", "builder")

    async def _wait(self, handle, timeout_s):
        stdout, stderr = await self.process.communicate()
        return ComputeResult(self.process.returncode, stdout.decode(), stderr.decode())

    async def cancel(self, handle):
        if self.process.returncode is None:
            self.process.terminate()
        await self.process.wait()

    async def _cleanup(self, handle):
        await self.cancel(handle)


def spec(tmp_path):
    return WorkSpec(1, 1, "", "main", "test", "", None, "", "test", 1,
                    local_checkout=tmp_path, builder_lease_credit=True)


@pytest.mark.asyncio
async def test_wait_beyond_original_deadline_then_brief_command_completes(
    tmp_path, monkeypatch, lease_environment,
):
    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment",
                        lambda: {k: v for k, v in lease_environment.items() if k != "MIMIR_HEAVY_REPORT_FD"})
    occupying = await holder(lease_environment)
    builder = RealBuilder()
    builder.environment = lease_environment
    handle = await builder.launch(spec(tmp_path))
    relay, _ = builder._lease_relays[handle.identifier]
    waiting = asyncio.create_task(builder.wait(handle, 1))
    try:
        await eventually(lambda: relay.queued_since is not None)
        # Lease deliberately stays held past the original one-second deadline.
        await asyncio.sleep(1.2)
        assert not waiting.done()
        occupying.stdin.close()
        await occupying.wait()
        result = await asyncio.wait_for(waiting, 5)
        assert result.exit_code == 0 and not result.timed_out, result.stderr
        assert result.stdout.strip() == "done"
        assert result.lease_wait_s >= 1.2
        assert result.lease_extension_s == result.lease_wait_s
        assert builder.spec.timeout_s == 11  # Inner deadline cannot preempt credit.
    finally:
        if occupying.returncode is None:
            occupying.terminate()
            await occupying.wait()
        await builder.cleanup(handle)
        await asyncio.gather(waiting, return_exceptions=True)
    assert not Path(lease_environment["MIMIR_HEAVY_LOCK_FILE"] + ".lease").exists()


@pytest.mark.asyncio
async def test_extension_cap_stops_a_real_queued_builder(tmp_path, monkeypatch, lease_environment):
    environment = dict(lease_environment)
    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment",
                        lambda: {k: v for k, v in environment.items() if k != "MIMIR_HEAVY_REPORT_FD"})
    occupying = await holder(lease_environment)
    builder = RealBuilder()
    # The wrapper's own timeout is not used here: the controller must stop it.
    builder.environment = environment
    handle = await builder.launch(spec(tmp_path))
    # Shorten the credit cap, not the helper's independent lease timeout, so
    # removal of the deadline cap cannot pass via an early helper failure.
    builder._lease_relays[handle.identifier][0].cap = 0.2
    try:
        result = await asyncio.wait_for(builder.wait(handle, 1), 4)
        assert result.timed_out
        assert 0 < result.lease_extension_s <= 0.2
        assert result.lease_wait_s <= result.run_duration_s
    finally:
        await builder.cleanup(handle)
        occupying.terminate()
        await occupying.wait()


@pytest.mark.asyncio
async def test_running_builder_without_queue_still_times_out(tmp_path, monkeypatch, lease_environment):
    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment",
                        lambda: {k: v for k, v in lease_environment.items() if k != "MIMIR_HEAVY_REPORT_FD"})
    builder = RealBuilder()
    builder.environment = lease_environment
    builder.payload = "import signal; signal.pause()"
    handle = await builder.launch(spec(tmp_path))
    try:
        result = await asyncio.wait_for(builder.wait(handle, 1), 3)
        assert result.timed_out
        assert result.lease_wait_s < result.run_duration_s * 0.5
    finally:
        await builder.cleanup(handle)


def test_credit_cap_is_minimum_of_configured_timeout_and_four_hours(lease_environment, monkeypatch):
    for timeout, expected in (("3", 3), ("99999", 14400)):
        relay = LeaseRelay({**lease_environment, "MIMIR_HEAVY_LOCK_TIMEOUT": timeout})
        relay.total = 20000
        assert relay.credit_s == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["wrong_auth", "reported_wait", "reported_acquired", "bad_tier"])
async def test_forged_builder_wait_reports_grant_no_extension(lease_environment, forgery):
    relay = LeaseRelay(lease_environment)
    await relay.start()
    try:
        request = {"auth": relay.token, "tier": "large"}
        if forgery == "wrong_auth":
            request["auth"] = "another-run"
        elif forgery == "bad_tier":
            request["tier"] = "unknown"
        else:
            request[forgery] = 14400
        reader, writer = await asyncio.open_unix_connection(relay.path)
        writer.write(json.dumps(request).encode() + b"\n")
        await writer.drain()
        # Valid requests queue; forged ones must be refused, not merely ignored
        # until some later timeout.
        assert await asyncio.wait_for(reader.readline(), 2) == b""
        assert relay.wait_s == 0 and relay.credit_s == 0
        writer.close()
        await writer.wait_closed()
    finally:
        await relay.close()


def test_lease_release_budget_is_bounded_and_claim_bound(tmp_path):
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path)
    comments = []
    for attempt in range(1, 5):
        claim = ClaimRecord(1, attempt, "agent", datetime.now(UTC))
        comments.append(claim.to_comment())
        hold = ShutdownAbortRecord(1, attempt, "agent", claim.claimed_at, datetime.now(UTC))
        forged = replace(hold, agent_id="other")
        marker = lambda h: LEASE_WAIT_PREFIX + h.to_comment().removeprefix(SHUTDOWN_ABORT_PREFIX)
        charged = max(1, attempt - 2)
        assert claims.attempts_used(comments) == charged
        assert claims.attempts_used([*comments, marker(forged)]) == charged
        comments.append(marker(hold))
        assert claims.attempts_used(comments) == max(0, attempt - 2)
        # Duplicate markers cannot grant a second credit.
        comments.append(marker(hold))
        assert claims.attempts_used(comments) == max(0, attempt - 2)
    assert claims.next_attempt(comments) == 5


def test_lease_release_cannot_credit_a_later_reset_generation(tmp_path):
    from mimir_cultivate.worklink.claims import CLAIM_RESET_PREFIX
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path)
    claim = ClaimRecord(1, 1, "agent", datetime.now(UTC))
    hold = ShutdownAbortRecord(1, 1, "agent", claim.claimed_at, datetime.now(UTC))
    marker = LEASE_WAIT_PREFIX + hold.to_comment().removeprefix(SHUTDOWN_ABORT_PREFIX)
    # A replayed heartbeat in the new generation cannot reuse old queue credit.
    history = [claim.to_comment(), marker, CLAIM_RESET_PREFIX + "{}", claim.to_comment()]
    assert claims.attempts_used(history) == 1


def test_old_claim_markers_cannot_saturate_current_lease_allowance(tmp_path):
    from mimir_cultivate.worklink.claims import CLAIM_RESET_PREFIX
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path)
    old = [ClaimRecord(1, attempt, "agent", datetime.now(UTC)) for attempt in (1, 2)]
    history = [claim.to_comment() for claim in old] + [CLAIM_RESET_PREFIX + "{}"]
    # Delayed old-generation markers arrive after the reset, before current work.
    for claim in [*old, ClaimRecord(1, 3, "agent", datetime.now(UTC))]:
        if claim.attempt == 3:
            history.append(claim.to_comment())
        hold = ShutdownAbortRecord(1, claim.attempt, claim.agent_id, claim.claimed_at, datetime.now(UTC))
        history.append(LEASE_WAIT_PREFIX + hold.to_comment().removeprefix(SHUTDOWN_ABORT_PREFIX))
    assert claims.attempts_used(history) == 0


def test_lease_release_has_backoff_but_no_incident_or_notice(tmp_path):
    record_lease_wait_backoff(tmp_path, 1, 1)
    backed_off, alerts = pending_failure_alerts(tmp_path)
    assert backed_off == {1} and alerts == []
    assert autonomous_dispatch_block_reason(tmp_path, 1)
    later = datetime.now(UTC) + timedelta(hours=1)
    assert pending_failure_alerts(tmp_path, now=later) == (set(), [])
    assert autonomous_dispatch_block_reason(tmp_path, 1, now=later) is None
    state = json.loads((tmp_path / "dispatch_failures.json").read_text())
    assert state["issues"] == {}
    entry = state["lease_wait_backoffs"]["1"]
    assert entry["active"] is False and entry["attempt_consumed"] is False


def test_lease_backoff_does_not_replace_an_existing_incident(tmp_path):
    from mimir_cultivate.worklink.dispatch_failures import record_failure, load_failure_state
    incident = record_failure(tmp_path, issue_id=1, attempt=1, exit_status=1,
                              error="running timeout", log_path=None)
    record_lease_wait_backoff(tmp_path, 1, 2)
    assert load_failure_state(tmp_path)["issues"]["1"] == incident
    assert autonomous_dispatch_block_reason(tmp_path, 1, now=datetime.now(UTC) + timedelta(days=1))


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_daemon_inner_budget_and_environment_wiring(tmp_path, monkeypatch, lease_environment, enabled):
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
    from mimir_cultivate.worklink.backends.base import SessionRequest
    from mimir_cultivate.worklink.daemon_sessions import session_environment

    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment",
                        lambda: {k: v for k, v in lease_environment.items() if k != "MIMIR_HEAVY_REPORT_FD"})
    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned")
    client = SimpleNamespace(session_create=AsyncMock(return_value=handle), session_release=AsyncMock())
    backend = OpenCodeDaemonComputeBackend(client=client)
    work = replace(spec(tmp_path), builder_lease_credit=enabled,
                   backend_config={"pass_env": ()},
                   session_request=SessionRequest("leaf", {"providerID": "unit", "id": "unit"}, "build"))
    assert await backend.launch(work) == handle
    try:
        arguments = client.session_create.call_args.kwargs
        assert arguments["timeout_s"] == (11 if enabled else 1)
        passed = arguments["pass_env"]
        assert ("WORKLINK_LEASE_SOCKET" in passed) is enabled
        assert ("WORKLINK_LEASE_AUTH" in passed) is enabled
        environment = session_environment(passed, home=Path("/var/lib/mimir-worklink/test-home"),
                                           git_settings=(), credentials={})
        if enabled:
            assert environment["WORKLINK_LEASE_SOCKET"] == passed["WORKLINK_LEASE_SOCKET"]
    finally:
        await backend.cleanup(handle)


@pytest.mark.asyncio
async def test_relay_is_removed_when_worker_environment_is_refused(tmp_path, monkeypatch, lease_environment):
    from mimir_cultivate.worklink.compute import ComputeLaunchError
    captured = []
    start = LeaseRelay.start

    async def capture(relay):
        await start(relay)
        captured.append(relay)

    monkeypatch.setattr(LeaseRelay, "start", capture)
    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment",
                        lambda: lease_environment)
    with pytest.raises(ComputeLaunchError):
        await RealBuilder().launch(replace(spec(tmp_path), backend_config={"pass_env": "invalid"}))
    assert len(captured) == 1
    assert not Path(captured[0].path).exists()
    assert not captured[0].server.is_serving()


@pytest.mark.asyncio
async def test_relay_start_failure_removes_its_private_directory(monkeypatch, lease_environment):
    monkeypatch.setattr(asyncio, "start_unix_server", AsyncMock(side_effect=OSError("unavailable")))
    relay = LeaseRelay(lease_environment)
    with pytest.raises(OSError):
        await relay.start()
    assert not Path(relay.directory.name).exists()


@pytest.mark.asyncio
async def test_relay_ignores_out_of_order_helper_events(tmp_path, monkeypatch, lease_environment):
    script = tmp_path / "broken-helper.py"
    script.write_text("print('acquired', flush=True)\n")
    monkeypatch.setattr("mimir_cultivate.worklink.lease_wait.helper_path", lambda: script)
    relay = LeaseRelay(lease_environment)
    await relay.start()
    try:
        reader, writer = await asyncio.open_unix_connection(relay.path)
        writer.write(json.dumps({"auth": relay.token, "tier": "large"}).encode() + b"\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.readline(), 2) == b""
        assert relay.credit_s == 0
        writer.close()
        await writer.wait_closed()
    finally:
        await relay.close()


@pytest.mark.parametrize("gate_mode", ["ci", "local"])
def test_packaged_work_order_matches_gate_mode(gate_mode):
    from mimir_cultivate.worklink.orchestrator import render_work_order, IssueContext
    template = Path(__file__).parents[1] / "plugins/cultivate/mimir_cultivate/prompt_templates/worklink-order.md"
    rendered = render_work_order(IssueContext(1, "Build", "Criteria", set()),
                                 template_path=template, backend_name="opencode",
                                 test_command="uv run pytest -q", gate_mode=gate_mode)
    if gate_mode == "ci":
        assert "Run focused tests" in rendered
        assert "Do NOT run the whole suite locally" in rendered
        assert "NOT done until" not in rendered
        assert "both legs and all checks" in rendered
    else:
        assert "NOT done until the gate command below passes when YOU run it" in rendered
        assert "\n  uv run pytest -q" in rendered


def test_agent_evidence_instructions_are_identical():
    root = Path(__file__).parents[1]
    sections = [
        (root / name).read_text().split("## Evidence\n", 1)[1].split("## Reading outside", 1)[0]
        for name in ("AGENTS.md", "CLAUDE.md")
    ]
    assert sections[0] == sections[1]
    assert "without `-n`" in sections[0]
    assert "Do not run the whole suite locally" in sections[0]


def test_scheduled_lease_release_does_not_poison_later_publication(tmp_path):
    from mimir_cultivate.worklink.orchestrator import _leaf_publication
    directory = tmp_path / "state/worklink/evidence"
    directory.mkdir(parents=True)
    (directory / "1-1.json").write_text(json.dumps({
        "issue": 1, "attempt": 1, "status": "lease_wait", "pr_url": None,
        "review_ready": False,
    }))
    with _leaf_publication(tmp_path, 1, 2):
        pass


def test_scheduled_lease_release_wrapper_does_not_record_incident(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import orchestrator
    result = orchestrator.WorklinkRunResult(1, 1, "lease_wait")

    async def run(*args, **kwargs):
        return result

    def unexpected(**kwargs):
        pytest.fail("scheduled release recorded an operator incident")

    monkeypatch.setattr(orchestrator.WorklinkRunner, "run", run)
    monkeypatch.setattr(orchestrator, "_record_run_failure", unexpected)
    assert orchestrator.run_worklink(home=tmp_path, repo=tmp_path, issue_id=1, autonomous=True) == result
