"""Real per-attempt helper locks and supervisor retries, without Docker."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionRequest, SessionTransportLostError
from mimir_cultivate.worklink.backends.feature_factory import FactoryStatus
from mimir_cultivate.worklink.claims import ClaimRecord
from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
from mimir_cultivate.worklink.daemon_sessions import SessionController
from mimir_cultivate.worklink.factory_state import FactoryRunRecord, load_factory_record, save_factory_record
from mimir_cultivate.worklink.orchestrator import IssueContext, WorklinkRunner, WorklinkRunResult
from mimir_cultivate.worklink.worker_client import WorkerClient
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon


@pytest.fixture
async def locked_helper(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_config, daemon_health, daemon_service, daemon_sessions, identities

    root = tmp_path / "daemon"
    sessions = root / "sessions"
    sessions.mkdir(parents=True, mode=0o700)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    key = hashlib.sha256(f"{checkout}:2172:1".encode()).hexdigest()
    loop = asyncio.get_running_loop()
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await SessionController.create(
            client, sessions / f"{key}.json",
            request=SessionRequest("epic", {"providerID": "openai", "id": "test"},
                                   "--autonomous --max-retries 2 chainlink-2172"),
            issue_id=2172, attempt=1, generation="generation", checkout=checkout,
            environment={"PATH": "/usr/bin:/bin"}, timeout_s=60)
        sid = controller.record.opencode_session_id
        handle = LaunchHandle("opencode_daemon", sid, opencode_session_id=sid, launch_generation="generation")
        monkeypatch.setattr(daemon_config, "DAEMON_ROOT", str(root))
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        monkeypatch.setattr(identities, "assert_worklink_identity", lambda _: None)
        monkeypatch.setattr(daemon_health, "load_api", lambda paths, uid: {
            "url": daemon.base_url, "username": daemon.username, "password": daemon.password})
        monkeypatch.setattr(daemon_service, "RuntimePaths", lambda: SimpleNamespace(daemon=root))
        read_json = daemon_service.read_json
        monkeypatch.setattr(daemon_service, "read_json", lambda path, **kw: (
            {"launch_generation": "generation"} if path.name == "launch.json" else read_json(path, **kw)))
        monkeypatch.setattr(daemon_sessions, "CONTROLLER_LOCK_TIMEOUT_S", 0.05)
        monkeypatch.setattr(daemon_sessions, "CONTROLLER_LOCK_RETRY_S", 0.005)
        state = SimpleNamespace(
            daemon=daemon, checkout=checkout, handle=handle, lock=sessions / f"{key}.lock",
            contended=asyncio.Event(), busy=asyncio.Event(), observed=asyncio.Event(),
            collisions=0, override=None)

        def flock(fd, flags):
            try:
                return fcntl.flock(fd, flags)
            except BlockingIOError:
                state.collisions += 1
                state.contended.set()
                raise

        # Instrument, but do not replace, the real kernel lock.
        monkeypatch.setattr(daemon_sessions, "fcntl", SimpleNamespace(
            flock=flock, LOCK_EX=fcntl.LOCK_EX, LOCK_NB=fcntl.LOCK_NB))

        async def dispatch(request, output_fd):
            if state.override is not None:
                return state.override
            if output_fd is not None:
                monkeypatch.setenv("WORKLINK_SESSION_OUTPUT_FD", str(output_fd))
            try:
                response = await daemon_sessions._executor_operation(request)
            except Exception as exc:
                return {"error": type(exc).__name__}
            if response == {"controller_busy": True}:
                state.busy.set()
            elif request["op"] == "session_observe" and request["opencode_session_id"] is not None:
                state.observed.set()
            return response

        class Socket:
            # Only substitute the executor transport. Keep WorkerClient's request,
            # response and output-FD handling, and the actual helper lock/controller.
            def sendmsg(self, buffers, ancillary):
                self.request = json.loads(b"".join(buffers))
                self.output_fd = ancillary[0][2][-1] if ancillary else None

            def recv(self, size):
                result = asyncio.run_coroutine_threadsafe(
                    dispatch(self.request, self.output_fd), loop).result(timeout=5)
                return json.dumps(result).encode()

            def close(self):
                pass

        state.worker = WorkerClient(None, path_checkout=checkout, issue_id=2172, attempt=1)
        monkeypatch.setattr(state.worker, "_connect", lambda timeout: Socket())
        state.compute = OpenCodeDaemonComputeBackend(client=state.worker)
        info = checkout.stat()
        state.request = {
            "op": "session_observe", "path": str(checkout), "issue": 2172, "attempt": 1,
            "device": info.st_dev, "inode": info.st_ino, "generation": "generation",
            "opencode_session_id": sid, "stdout_limit": 1024}
        yield state


@pytest.mark.parametrize("first,second", [
    ("tree_activity", "observe"), ("observe", "cancel"), ("cancel", "observe"),
    ("tree_activity", "cancel"), ("tree_activity", "release"), ("release", "observe"),
])
async def test_overlapping_session_operations_wait_and_both_complete(locked_helper, monkeypatch, first, second):
    from mimir_cultivate.worklink import daemon_sessions

    state = locked_helper
    # The first operation is deliberately held until the second hits the real lock.
    monkeypatch.setattr(daemon_sessions, "CONTROLLER_LOCK_TIMEOUT_S", 5)
    entered, release = asyncio.Event(), asyncio.Event()
    operation = getattr(SessionController, first)

    async def held(self, *args, **kwargs):
        entered.set()
        await release.wait()
        return await operation(self, *args, **kwargs)

    monkeypatch.setattr(SessionController, first, held)
    tasks = []
    try:
        tasks.append(asyncio.create_task(getattr(state.worker, f"session_{first}")(state.handle)))
        await asyncio.wait_for(entered.wait(), 2)
        tasks.append(asyncio.create_task(getattr(state.worker, f"session_{second}")(state.handle)))
        await asyncio.wait_for(state.contended.wait(), 2)
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), 5)
        assert len(results) == 2
        assert not state.busy.is_set()
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    with state.lock.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_default_controller_lock_deadline_is_below_helper_timeout():
    from mimir_cultivate.worklink.daemon_config import SESSION_HELPER_TIMEOUT_S
    from mimir_cultivate.worklink.daemon_sessions import CONTROLLER_LOCK_TIMEOUT_S, CONTROLLER_LOCK_RETRY_S

    assert 0 < CONTROLLER_LOCK_RETRY_S < CONTROLLER_LOCK_TIMEOUT_S < SESSION_HELPER_TIMEOUT_S


async def test_controller_lock_deadline_below_helper_timeout_is_transient(locked_helper):
    from mimir_cultivate.worklink.daemon_config import SESSION_HELPER_TIMEOUT_S

    state = locked_helper
    assert 0.5 < SESSION_HELPER_TIMEOUT_S
    before = len(state.daemon.requests)
    with state.lock.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        # This guard discriminates an unbounded wait, far below the helper timeout.
        async with asyncio.timeout(0.5):
            with pytest.raises(SessionTransportLostError, match="^controller_busy$"):
                await state.worker.session_observe(state.handle)
        assert state.collisions > 1  # short non-busy retries, not an immediate failure
        assert len(state.daemon.requests) == before
    state.daemon.finish(state.handle.identifier)
    result = await state.compute.wait(state.handle, 5)
    assert result.exit_code == 0 and result.session_error is None


async def test_other_attempt_lock_is_not_waited_on(locked_helper):
    state = locked_helper
    other = hashlib.sha256(f"{state.checkout}:2172:2".encode()).hexdigest()
    with (state.lock.parent / f"{other}.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        await asyncio.wait_for(state.worker.session_tree_activity(state.handle), 2)
    assert state.collisions == 0


async def test_recovery_admission_still_returns_busy_without_waiting(locked_helper, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions

    state = locked_helper
    monkeypatch.setattr(daemon_sessions, "CONTROLLER_LOCK_TIMEOUT_S", 5)
    with state.lock.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        response = await asyncio.wait_for(daemon_sessions._executor_operation({
            **state.request, "opencode_session_id": None}), 0.5)
    assert response == {"admission_state": "busy"}
    assert state.collisions == 1


@pytest.mark.parametrize("exit_path", ["cancelled", "exception"])
async def test_helper_releases_controller_lock_on_every_exit(locked_helper, monkeypatch, exit_path):
    from mimir_cultivate.worklink import daemon_sessions

    state = locked_helper
    entered = asyncio.Event()

    async def fail(self):
        entered.set()
        if exit_path == "exception":
            raise ValueError("synthetic controller failure")
        await asyncio.Event().wait()

    monkeypatch.setattr(SessionController, "tree_activity", fail)
    task = asyncio.create_task(daemon_sessions._executor_operation({
        **state.request, "op": "session_tree_activity"}))
    await asyncio.wait_for(entered.wait(), 2)
    if exit_path == "cancelled":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(ValueError, match="synthetic controller failure"):
            await task
    with state.lock.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("response", [
    {"error": "session operation failed (BlockingIOError)"},
    {"controller_busy": False}, {"controller_busy": 1},
    {"controller_busy": True, "unexpected": True}, {"transport_lost": False},
])
async def test_only_exact_controller_busy_response_is_transient(locked_helper, response):
    state = locked_helper
    state.override = response
    result = await state.compute.wait(state.handle, 2)
    assert result.session_error.type == "backend_error"
    assert not result.timed_out


async def test_controller_busy_is_a_missed_poll_not_factory_epic_failure(locked_helper, tmp_path, monkeypatch):
    state = locked_helper
    sandbox = state.checkout / ".factory-sandboxes/chainlink-2172"
    sandbox.mkdir(parents=True)
    status = FactoryStatus(
        run_id="chainlink-2172", valid=True, sandbox_path=str(sandbox), status="running",
        mode="autonomous", branch="feature/chainlink-2172", pr_base="main", pr_draft=False,
        lock="fresh", dead_lock=False, lock_session=state.handle.identifier, next="gate:story", next_present=True)
    record = FactoryRunRecord(
        run_id=status.run_id, issue_id=2172, attempt=1, repository="owner/repo", base_ref="main",
        branch=status.branch, launcher="/opt/factory/bin/factory.js", sandbox=str(sandbox),
        session=state.handle.identifier, opencode_session_id=state.handle.identifier,
        handle=state.handle, status=status, observed_at=datetime.now(UTC).isoformat(),
        controller_phase="running", session_checkout=str(state.checkout))
    save_factory_record(tmp_path, record)

    class Factory:
        poll_interval_s = 0.01

        def status(self, *args, **kwargs):
            return replace(status, status="completed") if state.observed.is_set() else status

        def heartbeat(self, *args, **kwargs):
            pass

    async def finished(self, **kwargs):
        assert kwargs["factory_record"].status.status == "completed"
        return WorklinkRunResult(2172, 1, "review_ready")

    monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finished)
    runner = WorklinkRunner(home=tmp_path, repo=sandbox)
    task = None
    try:
        with state.lock.open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            task = asyncio.create_task(runner._supervise_factory_070(
                issue=IssueContext(2172, "epic", "build", {"worklink:epic"}),
                claim_record=ClaimRecord(2172, 1, "fixture", datetime.now(UTC)),
                claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None),
                backend=Factory(), compute=state.compute, factory_record=record,
                test_cmd="pytest", runner=lambda *a, **kw: None, started_at=datetime.now(UTC)))
            await asyncio.wait_for(state.busy.wait(), 2)
            assert not task.done()
            assert not state.observed.is_set()
            assert not any(method == "POST" and route.endswith("/interrupt")
                           for method, route, _, _ in state.daemon.requests)
        state.daemon.finish(state.handle.identifier)
        result = await asyncio.wait_for(task, 5)
        assert result.status == "review_ready"
        assert state.observed.is_set()
        retained = load_factory_record(tmp_path, record.run_id)
        assert retained.controller_phase == "terminal"
        assert retained.controller_error is None
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
