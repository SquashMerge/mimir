"""Epic consumers against the schema-backed synthetic 2.3.0 daemon."""
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
            raise RuntimeError(self.controller.record.cleanup_error or "unverified owned quiescence")

    async def session_release(self, handle):
        await self.controller.release()


async def admit(tmp_path, daemon, client):
    sandbox = tmp_path / "checkout/.factory-sandboxes/chainlink-2125"
    sandbox.mkdir(parents=True)
    checkout = sandbox.parent.parent
    order = WorkOrder(2125, checkout, "not a second prompt", None, 30,
        {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test#high",
         "MIMIR_WORK_ITEM_JSON": json.dumps({"run_id": "chainlink-2125", "title": "epic", "body": "build"}),
         "FACTORY_PUBLISHING_IDENTITY": "fixture-bot"}, tmp_path / "transcripts")
    spec = FeatureFactoryBackend().work_spec(order, attempt=1, repo_url="repo", base_ref="main",
        branch="feature/chainlink-2125", test_command="uv run pytest")
    spec = replace(spec, factory_credential_settings=(("credential.helper", ""),
        ("credential.helper", "!fixture-credential-helper"),
        ("user.name", "Fixture Bot"), ("user.email", "fixture@example.invalid")))
    boundary = EpicBoundary(daemon, client, checkout)
    compute = OpenCodeDaemonComputeBackend(client=boundary)
    handle = await compute.launch(spec)
    status = FactoryStatus(run_id="chainlink-2125", valid=True, sandbox_path=str(sandbox),
        status="running", mode="autonomous", branch="feature/chainlink-2125", pr_base="main",
        pr_draft=False, lock="fresh", dead_lock=False, lock_session=handle.identifier,
        next="gate:story", next_present=True, mcp_servers=("fixture-mcp",))
    record = FactoryRunRecord(run_id=status.run_id, issue_id=2125, attempt=1, repository="owner/repo",
        base_ref="main", branch=status.branch, launcher="/opt/factory/bin/factory.js", sandbox=str(sandbox),
        session=handle.identifier, opencode_session_id=handle.identifier, handle=handle, status=status,
        mcp_servers=status.mcp_servers, observed_at=datetime.now(UTC).isoformat(), controller_phase="running",
        session_checkout=str(checkout))
    save_factory_record(tmp_path, record)
    return spec, boundary, compute, handle, record


def specialist_shells(daemon, handle, record):
    """Two direct children plus a nested specialist that has moved locations."""
    sandbox = record.sandbox
    slice_path = str(Path(sandbox) / "worktrees/be-leaf")
    first = daemon.child_session(handle.identifier, sandbox)
    second = daemon.child_session(handle.identifier, slice_path)
    nested = daemon.child_session(first, slice_path + "/old")
    owned = [daemon.start_shell(first, sandbox), daemon.start_shell(second, slice_path),
             daemon.start_shell(nested, slice_path + "/old"),
             daemon.start_shell(handle.identifier, record.session_checkout)]
    daemon.move_session(nested, slice_path)
    return owned


@pytest.mark.parametrize("anonymous", [False, True])
async def test_epic_cancel_never_touches_unrelated_session_shells_at_same_location(tmp_path, anonymous):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        owned = specialist_shells(daemon, handle, record)
        foreign = [daemon.start_shell("ses_unrelated_leaf", daemon.shells[shell]["cwd"]) for shell in owned]
        # Include missing ownership metadata: location alone must never authorize deletion.
        if anonymous:
            shell = daemon.start_shell("ses_unknown", record.sandbox)
            daemon.shells[shell]["metadata"] = {}
            foreign.append(shell)
        await compute.cancel(handle)
        assert boundary.controller.record.cancellation_state == "verified"
        assert all(daemon.shells[s]["status"] == "killed" for s in owned)
        assert all(daemon.shells[s]["status"] == "running" for s in foreign)
        deleted = {route.rsplit("/", 1)[-1] for method, route, _, _ in daemon.requests
                   if method == "DELETE" and route.startswith("/api/shell/")}
        assert deleted == set(owned)


@pytest.mark.parametrize("fault", ["delete-failure", "lying-ack", "listing-failure"])
async def test_epic_shell_cleanup_incomplete_names_remaining_shell_and_session(tmp_path, monkeypatch, fault):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        owned = specialist_shells(daemon, handle, record)
        retained = owned[1]
        delete = client.delete_shell
        async def failing_delete(shell_id, directory):
            if shell_id == retained:
                if fault == "delete-failure":
                    daemon.faults[("DELETE", f"/api/shell/{shell_id}")].append(SyntheticFault(status=500))
                elif fault == "lying-ack":
                    daemon.faults[("DELETE", f"/api/shell/{shell_id}")].append(SyntheticFault(status=204))
                else:
                    daemon.faults[("GET", "/api/shell")].append(SyntheticFault(status=500))
                    return
            await delete(shell_id, directory)
        monkeypatch.setattr(client, "delete_shell", failing_delete)
        cancel = boundary.controller.cancel
        async def bounded_cancel():
            return await cancel(timeout_s=0.3)
        monkeypatch.setattr(boundary.controller, "cancel", bounded_cancel)
        with pytest.raises(ComputeCleanupError, match=retained):
            await compute.cancel(handle)
        recovered = SessionController.recover(client, boundary.controller.path,
            generation=handle.launch_generation, checkout=Path(record.session_checkout))
        assert recovered.record.cancellation_state == "unverified"
        assert retained in recovered.record.cleanup_error
        assert daemon.shells[retained]["metadata"]["sessionID"] in recovered.record.cleanup_error
        assert daemon.shells[retained]["status"] == "running"
        assert not load_factory_record(tmp_path, record.run_id).children_reaped


@pytest.mark.parametrize("reused", [False, True])
async def test_epic_cancel_verifies_orphan_pid_start_time_not_only_shell_listing(tmp_path, monkeypatch, reused):
    from mimir_cultivate.worklink import daemon_processes as processes
    import signal

    # A faithful /proc tree: child escaped into its own process group, then
    # DELETE reparents it to init and removes all daemon shell visibility.
    table = {200: (1, 100, "S"), 201: (200, 101, "S"), 202: (201, 102, "S"),
             999: (1, 999, "S")}
    monkeypatch.setattr(processes, "_process", lambda pid: table.get(pid))
    monkeypatch.setattr(processes, "Path", lambda _: SimpleNamespace(
        iterdir=lambda: [SimpleNamespace(name=str(pid)) for pid in table]))
    sent = []
    handles = {}
    def open_pid(pid):
        handles[pid + 1000] = pid
        return pid + 1000
    def send(fd, sig):
        pid = handles[fd]
        sent.append((pid, sig))
        if pid == 202 and sig == signal.SIGKILL:
            table.pop(pid, None)
        elif pid == 201:
            table.pop(pid, None)
    # Patch only this module's os/signal facades, not process-global stdlib.
    monkeypatch.setattr(processes, "os", SimpleNamespace(pidfd_open=open_pid, close=lambda fd: None))
    monkeypatch.setattr(processes, "signal", SimpleNamespace(
        SIGTERM=signal.SIGTERM, SIGKILL=signal.SIGKILL, pidfd_send_signal=send))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        child = daemon.child_session(handle.identifier, record.sandbox)
        shell = daemon.start_shell(child, record.sandbox)
        daemon.shells[shell]["pid"] = 200
        delete = client.delete_shell
        async def delete_and_orphan(sid, directory):
            # Witnesses must already be durable before lineage disappears.
            assert boundary.controller.record.cleanup_processes == [[201, 101], [202, 102]]
            await delete(sid, directory)
            table.pop(200)
            table[201] = (1, 101, "S")
            table[202] = (1, 777 if reused else 102, "S")
        monkeypatch.setattr(client, "delete_shell", delete_and_orphan)
        await compute.cancel(handle)
        assert boundary.controller.record.cancellation_state == "verified"
        assert boundary.controller.record.cleanup_processes == []
        assert (201, signal.SIGTERM) in sent
        assert 999 in table and not any(pid == 999 for pid, _ in sent)
        if reused:
            assert table[202][1] == 777 and not any(pid == 202 for pid, _ in sent)
        else:
            assert 202 not in table
            assert (202, signal.SIGKILL) in sent


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="Linux process identity witness")
async def test_epic_cancel_reaps_real_setsid_child_after_daemon_forgets_shell(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_processes as processes
    import signal
    import sys

    # The child acknowledges setsid and TERM handling before exposing its PID.
    child_code = "import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print(os.getpid(),flush=True); time.sleep(60)"
    parent_code = ("import subprocess,sys,time; "
                   "p=subprocess.Popen([sys.executable,'-u','-c',sys.argv[1]],start_new_session=True,stdout=subprocess.PIPE,text=True); "
                   "print(p.stdout.readline(),end='',flush=True); time.sleep(60)")
    parent = subprocess.Popen([sys.executable, "-u", "-c", parent_code, child_code],
                              stdout=subprocess.PIPE, text=True)
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    child_pid = None
    try:
        child_pid = int(await asyncio.wait_for(asyncio.to_thread(parent.stdout.readline), 5))
        assert os.getpgid(child_pid) == child_pid != os.getpgid(parent.pid)
        async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
            _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
            shell = daemon.start_shell(handle.identifier, record.session_checkout)
            daemon.shells[shell]["pid"] = parent.pid
            delete = client.delete_shell
            async def delete_parent(sid, directory):
                assert any(w[0] == child_pid for w in boundary.controller.record.cleanup_processes)
                parent.terminate()
                await asyncio.to_thread(parent.wait, 5)
                await delete(sid, directory)
                assert processes._process(child_pid)[0] != parent.pid
            monkeypatch.setattr(client, "delete_shell", delete_parent)
            await compute.cancel(handle)
            assert boundary.controller.record.cancellation_state == "verified"
            assert not await client.running_shells(record.session_checkout)
            state = processes._process(child_pid)
            assert state is None or state[2] == "Z"
            assert unrelated.poll() is None
    finally:
        # Test-owned interlocks: even a cleanup-removal mutation cannot leak
        # its spawned processes, and no production process is selected.
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=5)
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        unrelated.kill()
        unrelated.wait(timeout=5)
        parent.stdout.close()


async def test_epic_cancel_reports_surviving_orphan_pids_and_retains_recovery(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_processes as processes

    table = {200: (1, 100, "S"), 201: (200, 101, "S")}
    monkeypatch.setattr(processes, "_process", lambda pid: table.get(pid))
    monkeypatch.setattr(processes, "Path", lambda _: SimpleNamespace(
        iterdir=lambda: [SimpleNamespace(name=str(pid)) for pid in table]))
    monkeypatch.setattr(processes, "_signal", lambda *args: None)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, _, handle, record = await admit(tmp_path, daemon, client)
        shell = daemon.start_shell(handle.identifier, record.session_checkout)
        daemon.shells[shell]["pid"] = 200
        delete = client.delete_shell
        async def orphan(sid, directory):
            await delete(sid, directory)
            table.pop(200)
            table[201] = (1, 101, "S")
        monkeypatch.setattr(client, "delete_shell", orphan)
        assert not await boundary.controller.cancel(timeout_s=0.2)
        recovered = SessionController.recover(client, boundary.controller.path,
            generation=handle.launch_generation, checkout=Path(record.session_checkout))
        assert recovered.record.cleanup_processes == [[201, 101]]
        assert "PID 201" in recovered.record.cleanup_error
        assert recovered.record.cancellation_state == "unverified"
        assert not await client.running_shells(record.session_checkout)


async def test_epic_late_root_shell_waits_for_process_snapshot_before_delete(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_processes as processes

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        refresh = boundary.controller._refresh
        spawned = []
        witnessed = []
        monkeypatch.setattr(processes, "snapshot_descendants", lambda pid: witnessed.append(pid) or [])
        async def late_refresh():
            if not spawned:
                spawned.append(daemon.start_shell(handle.identifier, record.session_checkout))
            return await refresh()
        monkeypatch.setattr(boundary.controller, "_refresh", late_refresh)
        delete = client.delete_shell
        async def checked_delete(sid, directory):
            assert daemon.shells[sid]["pid"] in witnessed
            await delete(sid, directory)
        monkeypatch.setattr(client, "delete_shell", checked_delete)
        await compute.cancel(handle)
        assert spawned and daemon.shells[spawned[0]]["status"] == "killed"


async def test_epic_session_tree_follows_every_page_and_archived_descendants(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        for _ in range(101):
            last = daemon.child_session(handle.identifier, record.sandbox)
        daemon.sessions[last].info["time"]["archived"] = 3
        nested = daemon.child_session(last, str(Path(record.sandbox) / "worktrees/last"))
        shell = daemon.start_shell(nested, daemon.sessions[nested].info["location"]["directory"])
        await compute.cancel(handle)
        assert daemon.shells[shell]["status"] == "killed"
        assert any(query.get("cursor") == "100" for _, route, query, _ in daemon.requests if route == "/api/session")


async def test_epic_cancel_drains_child_resume_queued_by_shell_deletion(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.delete_race = True
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        child = daemon.child_session(handle.identifier, record.sandbox)
        shell = daemon.start_shell(child, record.sandbox)
        delete = client.delete_shell
        async def delete_and_resume(shell_id, directory):
            await delete(shell_id, directory)
            daemon.enqueue_synthetic(child, shell_id)
            daemon.deliver(child, daemon.sessions[child].inbox[0]["id"])
            # Model both an admitted and a still-queued completion delivery.
            daemon.enqueue_synthetic(child, shell_id)
        monkeypatch.setattr(client, "delete_shell", delete_and_resume)
        await compute.cancel(handle)
        assert boundary.controller.record.cancellation_state == "verified"
        assert daemon.shells[shell]["status"] == "killed"
        assert not daemon.sessions[child].active
        assert not daemon.sessions[child].inbox


async def test_epic_cancel_rediscovers_child_spawned_during_final_interrupt(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        interrupt = client.interrupt
        root_interrupts = 0
        spawned = []
        async def spawning_interrupt(session_id):
            nonlocal root_interrupts
            result = await interrupt(session_id)
            if session_id == handle.identifier:
                root_interrupts += 1
                if root_interrupts == 2:
                    child = daemon.child_session(session_id, str(Path(record.sandbox) / "worktrees/late"))
                    spawned.append(daemon.start_shell(child, daemon.sessions[child].info["location"]["directory"]))
            return result
        monkeypatch.setattr(client, "interrupt", spawning_interrupt)
        await compute.cancel(handle)
        assert spawned and daemon.shells[spawned[0]]["status"] == "killed"
        assert boundary.controller.record.cancellation_state == "verified"


async def test_epic_session_tree_rejects_foreign_parent_link_before_deletion(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, _, handle, record = await admit(tmp_path, daemon, client)
        child = daemon.child_session(handle.identifier, record.sandbox)
        foreign = daemon.sessions[child].info
        foreign["parentID"] = "ses_another_run"
        shell = daemon.start_shell(child, record.sandbox)
        daemon.faults[("GET", "/api/session")].append(SyntheticFault(body=json.dumps({
            "data": [foreign], "cursor": {"previous": None, "next": None}}).encode()))
        assert not await boundary.controller.cancel()
        assert boundary.controller.record.cancellation_state == "unverified"
        assert daemon.shells[shell]["status"] == "running"
        assert not any(method == "DELETE" and route.startswith("/api/shell/")
                       for method, route, _, _ in daemon.requests)


async def test_epic_session_tree_rejects_foreign_root_identity_before_deletion(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, _, handle, record = await admit(tmp_path, daemon, client)
        foreign = daemon.child_session(handle.identifier, record.sandbox)
        shell = daemon.start_shell(foreign, record.sandbox)
        daemon.faults[("GET", f"/api/session/{handle.identifier}")].append(SyntheticFault(
            body=json.dumps({"data": daemon.sessions[foreign].info}).encode()))
        assert not await boundary.controller.cancel()
        assert boundary.controller.record.cancellation_state == "unverified"
        assert daemon.shells[shell]["status"] == "running"
        assert not any(method == "DELETE" and route.startswith("/api/shell/")
                       for method, route, _, _ in daemon.requests)


async def test_epic_terminal_recovery_stops_descendant_shells_before_finalization(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import orchestrator

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        owned = specialist_shells(daemon, handle, record)
        completed = replace(record.status, status="completed")
        monkeypatch.setattr(orchestrator, "_verify_factory_recovery_binding", lambda **kw: Path(record.sandbox))
        async def finished(self, **kwargs):
            assert kwargs["factory_record"].children_reaped
            assert all(daemon.shells[s]["status"] == "killed" for s in owned)
            return WorklinkRunResult(2125, 1, "review_ready")
        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finished)
        result = await WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._recover_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)), claims=object(),
            backend=SimpleNamespace(status=lambda *a, **kw: completed), compute=compute,
            retained=record, launcher=Path(record.launcher), repo_slug=record.repository,
            base="main", test_cmd="pytest", runner=lambda *a, **kw: None, work_item_json="{}")
        assert result.status == "review_ready"


async def test_session_cancel_executor_diagnostic_reaches_caller(tmp_path, monkeypatch):
    from mimir_cultivate.worklink.worker_client import WorkerClient

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, _, handle, _ = await admit(tmp_path, daemon, client)
        peer = WorkerClient(None)
        async def rpc(*args, **kwargs):
            return {"cancellation_state": "unverified", "cleanup_error":
                    "session tree cleanup incomplete: sh_retained (ses_specialist)"}
        monkeypatch.setattr(peer, "_session_handle_rpc", rpc)
        with pytest.raises(RuntimeError, match=r"sh_retained \(ses_specialist\)"):
            await peer.session_cancel(handle)


@pytest.mark.parametrize("cause", ["budget", "idle", "startup", "already-parked", "terminal", "delete-failure"])
async def test_epic_park_stops_descendant_shells_at_sandbox_and_slice_locations(
    tmp_path, monkeypatch, supervisor_clock, cause,
):
    from mimir_cultivate.worklink import orchestrator

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        owned = specialist_shells(daemon, handle, record)
        foreign = daemon.start_shell("ses_unrelated", record.sandbox)
        boundary.finish = False
        current = record.status
        polls = 0
        terminal_calls = []
        if cause == "idle":
            monkeypatch.setattr(orchestrator, "_epic_run_timeout_s", lambda: 20000.0)
        if cause == "startup":
            record = replace(record, status=None, observed_at=None)
            monkeypatch.setattr(orchestrator, "_factory_startup_status_timeout_s", lambda: 10.0)
        if cause == "delete-failure":
            delete = client.delete_shell
            async def refuse(shell_id, directory):
                if shell_id == owned[1]:
                    daemon.faults[("DELETE", f"/api/shell/{shell_id}")].append(SyntheticFault(status=500))
                await delete(shell_id, directory)
            monkeypatch.setattr(client, "delete_shell", refuse)
            cancel = boundary.controller.cancel
            async def bounded_cancel():
                return await cancel(timeout_s=0.3)
            monkeypatch.setattr(boundary.controller, "cancel", bounded_cancel)

        def status(*args, **kwargs):
            nonlocal polls
            polls += 1
            if cause == "startup":
                supervisor_clock.now += 6
                return FactoryStatus(record.run_id, False, record.sandbox)
            if cause in {"already-parked", "terminal"}:
                return replace(current, status="needs-human" if cause == "already-parked" else "completed",
                               park_snapshot=str(tmp_path / "snapshot") if cause == "already-parked" else None)
            if boundary.controller.record.cancellation_state == "none":
                if cause == "idle":
                    # First observe progress, then cross the idle threshold without
                    # advancing the work budget or changing the snapshot.
                    supervisor_clock.now = 1 if polls == 1 else 8000
                else:
                    supervisor_clock.now = 60
            return current

        def terminal(*args, **kwargs):
            nonlocal current
            assert all(daemon.shells[s]["status"] == "killed" for s in owned)
            terminal_calls.append(kwargs["reason"])
            current = replace(current, status="needs-human", park_snapshot=str(tmp_path / "snapshot"))

        async def finished(self, **kwargs):
            assert kwargs["factory_record"].children_reaped
            assert all(daemon.shells[s]["status"] == "killed" for s in owned)
            return WorklinkRunResult(2125, 1, "needs-human")

        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finished)
        invocation = WorklinkRunner(home=tmp_path, repo=Path(record.sandbox))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None),
            backend=SimpleNamespace(status=status, heartbeat=lambda *a, **kw: None,
                                    terminal=terminal, poll_interval_s=0.001),
            compute=compute, factory_record=record, test_cmd="pytest", runner=lambda *a, **kw: None,
            started_at=datetime.now(UTC))
        if cause == "delete-failure":
            with pytest.raises(ComputeCleanupError, match=owned[1]):
                await asyncio.wait_for(invocation, 60)
            saved = load_factory_record(tmp_path, record.run_id)
            assert not saved.children_reaped and owned[1] in saved.controller_error
            assert not terminal_calls
        elif cause == "startup":
            with pytest.raises(WorklinkError, match="startup deadline"):
                await asyncio.wait_for(invocation, 60)
            assert all(daemon.shells[s]["status"] == "killed" for s in owned)
        else:
            assert (await asyncio.wait_for(invocation, 60)).status == "needs-human"
            if cause in {"budget", "idle"}:
                assert len(terminal_calls) == 1
                if cause == "idle":
                    assert "driver idle" in terminal_calls[0]
        assert daemon.shells[foreign]["status"] == "running"


async def test_epic_production_command_checkout_model_permissions_and_environment(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        spec, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        create = next(body for method, route, _, body in daemon.requests if method == "POST" and route == "/api/session")
        assert create == {"location": {"directory": record.session_checkout},
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
        settings = [(environment[f"GIT_CONFIG_KEY_{i}"], environment[f"GIT_CONFIG_VALUE_{i}"])
                    for i in range(int(environment["GIT_CONFIG_COUNT"]))]
        assert settings[1:] == list(spec.factory_credential_settings)
        assert environment["GIT_CONFIG_NOSYSTEM"] == "1"
        assert environment["GIT_CONFIG_GLOBAL"] == "/dev/null"
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
            generation=handle.launch_generation, checkout=Path(record.session_checkout))
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
            assert kwargs["checkout"] == Path(record.session_checkout)
            return WorkOrder(2125, kwargs["checkout"], "not a resume prompt", None, 30,
                {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test#high", "MIMIR_WORK_ITEM_JSON": spec.env["MIMIR_WORK_ITEM_JSON"],
                 "FACTORY_PUBLISHING_IDENTITY": "fixture-bot", "GIT_AUTHOR_NAME": "Fixture Bot",
                 "GIT_AUTHOR_EMAIL": "fixture@example.invalid"}), "fixture-bot"
        monkeypatch.setattr(WorklinkRunner, "_factory_launch_order", order)
        release_calls = []
        current = record.status
        post_resume_reads = 0
        class Factory(FeatureFactoryBackend):
            poll_interval_s = 0.001
            def initialize(self, *args, **kwargs):
                pytest.fail("recovery must not initialize a retained run")
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
            assert fresh.session_checkout == record.session_checkout
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
        owned = specialist_shells(daemon, handle, record)
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
        assert all(daemon.shells[s]["status"] == "killed" for s in owned)


@pytest.mark.parametrize("ending", ["running", "completed", "foreign", "unverified"])
@pytest.mark.parametrize("verified_evidence", [False, True])
async def test_operator_stop_consumer_retains_or_releases_only_after_owned_quiescence(tmp_path, monkeypatch, ending, verified_evidence):
    from mimir_cultivate.worklink import control, identities
    from mimir_cultivate.worklink.worker_client import WorkerClient
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, _, handle, record = await admit(tmp_path, daemon, client)
        owned = specialist_shells(daemon, handle, record)
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
                assert all(daemon.shells[s]["status"] == "killed" for s in owned)
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
        finished, polling = threading.Event(), asyncio.Event()
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
            # This scenario stops BETWEEN an idle result and its next status
            # read. Own that ordering: otherwise the interrupted result can
            # finish after status returned and end supervision before polling.
            assert finished.wait(3), "owned session result did not finish"
            if not stopping:
                loop.call_soon_threadsafe(polling.set)
                # Hang guard, not a latency budget.
                assert release_poll.wait(60), "operator stop did not finish"
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
            # Hang guard, not a latency budget.
            await asyncio.wait_for(polling.wait(), 60)
            stopping = True
            stop = await asyncio.to_thread(control.stop_worklink, tmp_path, 2125,
                runner=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""))
            assert stop.stopped and stop.claim_released
            released = True
            release_poll.set()
            # Hang guard, not a latency budget.
            result = await asyncio.wait_for(task, 60)
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
async def test_successful_host_lock_refusal_precedes_rewait(tmp_path, monkeypatch, missed_poll):
    import threading
    from mimir_cultivate.worklink.control import _mark_operator_stop
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        _mark_operator_stop(tmp_path, 2125, 1, opencode_session_id="ses_other_host")
        daemon.sessions[handle.identifier].messages.append({
            "id": "msg_refusal", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "synthetic"},
            "content": [{"type": "text", "text": "Another live session holds the run lock; did not acquire the lock"}],
        })
        observed = threading.Event()
        real_wait = compute.wait
        async def wait(*args, **kwargs):
            result = await real_wait(*args, **kwargs)
            observed.set()
            return result
        monkeypatch.setattr(compute, "wait", wait)
        def status(*args, **kwargs):
            # Refusal precedence is tested after its owned result is available,
            # not against whichever HTTP/status thread the scheduler runs first.
            assert observed.wait(3), "owned refusal result was not observed"
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
                # Hang guard, not a latency budget.
                await asyncio.wait_for(invocation, 60)
        else:
            # Hang guard, not a latency budget.
            result = await asyncio.wait_for(invocation, 60)
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
                # Hang guard, not a latency budget; the controlled clock proves budget crossing.
                await asyncio.wait_for(invocation, 60)
        else:
            # Hang guard, not a latency budget; the controlled clock proves budget crossing.
            result = await asyncio.wait_for(invocation, 60)
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


def test_epic_launch_at_operator_checkout_without_controller_init(tmp_path, monkeypatch):
    from tests.test_worklink_orchestrator import _run_factory_preflight_case

    async def launch(compute, spec):
        checkout = tmp_path / "factory-checkout"
        assert spec.local_checkout == checkout
        record = load_factory_record(tmp_path, "chainlink-700")
        assert record.status is None and record.observed_at is None
        assert record.sandbox == str(checkout / ".factory-sandboxes/chainlink-700")
        assert record.session_checkout == str(checkout)
        assert not Path(record.sandbox).exists()
        raise RuntimeError("driver owns init")

    # The helper's initialize stub fails the test if any host calls it.
    result, launched, _, _ = _run_factory_preflight_case(tmp_path, monkeypatch,
        credentials={"GITHUB_TOKEN": "fixture-token"}, compute_launch=launch)
    assert result.reason == "driver owns init" and len(launched) == 1


@pytest.mark.parametrize("missing", ["absent-directory", "uninitialized-status"])
@pytest.mark.parametrize("initializes", [True, False])
async def test_absent_factory_run_waits_for_driver_init_until_startup_deadline(
    tmp_path, monkeypatch, supervisor_clock, missing, initializes,
):
    from mimir_cultivate.worklink import orchestrator

    monkeypatch.setattr(orchestrator, "_factory_startup_status_timeout_s", lambda: 10.0)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, boundary, compute, handle, record = await admit(tmp_path, daemon, client)
        boundary.finish = False
        pending = replace(record, status=None, observed_at=None)
        save_factory_record(tmp_path, pending)
        reads = []

        def status(*args, **kwargs):
            reads.append(supervisor_clock.now)
            if initializes and len(reads) == 3:
                return replace(record.status, status="completed")
            supervisor_clock.now += 2
            if missing == "absent-directory":
                raise FileNotFoundError("driver has not created its sandbox")
            return FactoryStatus(record.run_id, False, record.sandbox)

        async def finish(self, **kwargs):
            assert kwargs["factory_record"].status.status == "completed"
            return WorklinkRunResult(2125, 1, "review_ready")

        monkeypatch.setattr(WorklinkRunner, "_finish_factory_070", finish)
        invocation = WorklinkRunner(home=tmp_path, repo=Path(record.session_checkout))._supervise_factory_070(
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}),
            claim_record=ClaimRecord(2125, 1, "fixture", datetime.now(UTC)),
            claims=SimpleNamespace(heartbeat_issue=lambda *a, **kw: None),
            backend=SimpleNamespace(status=status, heartbeat=lambda *a, **kw: None, poll_interval_s=0.001),
            compute=compute, factory_record=pending, test_cmd="pytest", runner=lambda *a, **kw: None,
            started_at=datetime.now(UTC))
        if initializes:
            assert (await asyncio.wait_for(invocation, 60)).status == "review_ready"
            assert reads == [0, 2, 4]
        else:
            with pytest.raises(WorklinkError, match="factory driver did not acquire the run lock before startup deadline"):
                await asyncio.wait_for(invocation, 60)
            assert reads == [0, 2, 4, 6, 8]


async def test_driver_initialized_sandbox_must_equal_deterministic_path(tmp_path):
    from mimir_cultivate.worklink.orchestrator import _require_factory_status

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        with pytest.raises(WorklinkError, match="sandbox mismatch"):
            _require_factory_status(replace(record.status, sandbox_path=str(tmp_path / "foreign")),
                                    replace(record, status=None))
        await compute.cancel(handle)
        await compute.cleanup(handle)


async def test_recorded_epic_location_cannot_escape_operator_checkout(tmp_path):
    from mimir_cultivate.worklink.factory_state import FactoryRecordError

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        assert FactoryRunRecord.from_json(record.to_json()).session_checkout == record.session_checkout
        legacy = record.to_json()
        legacy.pop("session_checkout")
        assert FactoryRunRecord.from_json(legacy).session_checkout is None
        with pytest.raises(FactoryRecordError, match="session checkout"):
            FactoryRunRecord.from_json({**record.to_json(), "session_checkout": str(tmp_path / "outside")})
        await compute.cancel(handle)
        await compute.cleanup(handle)


@pytest.mark.parametrize("entrypoint", ["target", "binding"])
@pytest.mark.parametrize("location", ["missing-boundary", "outside-root", "wrong-attempt", "nested-boundary"])
async def test_factory_recovery_refuses_sandbox_outside_attempt_boundary(tmp_path, monkeypatch, entrypoint, location):
    from mimir_cultivate.worklink import orchestrator

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        root = tmp_path / "attempts"
        sandbox = {
            "missing-boundary": root / "checkout/.factory-sandboxes/chainlink-2125",
            "outside-root": Path(record.sandbox),
            "wrong-attempt": root / "repo/2125-2/checkout/.factory-sandboxes/chainlink-2125",
            "nested-boundary": root / "repo/2125-1/checkout/nested/.factory-sandboxes/chainlink-2125",
        }[location]
        sandbox.mkdir(parents=True, exist_ok=True)
        record = replace(record, sandbox=str(sandbox), session_checkout=str(sandbox.parent.parent),
                         status=replace(record.status, sandbox_path=str(sandbox)))
        monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(root))
        monkeypatch.setattr(orchestrator, "_repo_remote_url", lambda *a, **kw: "https://github.com/owner/repo.git")
        monkeypatch.setattr(orchestrator, "_verify_factory_checkout", lambda *a, **kw: "verified-head")
        kwargs = dict(runner=WorklinkRunner(home=tmp_path, repo=tmp_path, agent_id="mimir"),
            issue=IssueContext(2125, "epic", "build", {"worklink:epic"}), retained=record,
            launcher=Path(record.launcher), repo_slug=record.repository, base="main", command_runner=lambda *a, **kw: None)
        if entrypoint == "binding":
            kwargs.update(claim_record=ClaimRecord(2125, 1, "mimir", datetime.now(UTC)),
                claims=SimpleNamespace(agent_id="mimir", _lock_still_held_by=lambda claim: True))
        reason = ("no private ownership-transfer boundary" if location in {"missing-boundary", "outside-root"}
                  else "outside the recorded worker checkout")
        with pytest.raises(WorklinkError, match=reason):
            getattr(orchestrator, f"_verify_factory_recovery_{entrypoint}")(**kwargs)
        await compute.cancel(handle)
        await compute.cleanup(handle)


@pytest.mark.parametrize("location", ["outside-root", "wrong-attempt", "nested-boundary"])
async def test_factory_completion_refuses_sandbox_outside_recorded_attempt(tmp_path, monkeypatch, location):
    from mimir_cultivate.worklink import orchestrator

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        _, _, compute, handle, record = await admit(tmp_path, daemon, client)
        root = tmp_path / "attempts"
        sandbox = {"outside-root": Path(record.sandbox),
            "wrong-attempt": root / "repo/2125-2/checkout/.factory-sandboxes/chainlink-2125",
            "nested-boundary": root / "repo/2125-1/checkout/nested/.factory-sandboxes/chainlink-2125"}[location]
        record = replace(record, sandbox=str(sandbox), session_checkout=str(sandbox.parent.parent),
                         status=replace(record.status, sandbox_path=str(sandbox)))
        status = replace(record.status, status="completed", pr_url="https://github.com/owner/repo/pull/1",
                         slices=({"id": "one", "status": "merged", "attempts": 1,
                                  "extra_attempts": 0, "retry_limit": 5},))
        record = replace(record, status=status)
        monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(root))
        with pytest.raises(WorklinkError, match="outside the recorded worker checkout"):
            await orchestrator._verify_factory_completion(home=tmp_path,
                issue=IssueContext(2125, "epic", "build", {"worklink:epic"}), record=record,
                test_command="pytest", runner=lambda *a, **kw: pytest.fail("unbound sandbox reached Git"),
                started_at=datetime.now(UTC))
        await compute.cancel(handle)
        await compute.cleanup(handle)


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
            preflight = []
            def claim_issue(issue_id, comments, **kwargs):
                assert issue_id == 2125
                # The base-permission refusal must already have run, before claim.
                assert preflight == [Path(record.sandbox)]
                kwargs["before_claim"]()
                return SimpleNamespace(claimed=True, record=claim_record, attempts_exhausted=False)
            claims.claim_issue = claim_issue
            # The fixture base is a bare sandbox directory, not a Git repository;
            # the permission scan itself is covered by test_worklink_checkout.
            monkeypatch.setattr(orchestrator, "assert_base_repo_not_worker_writable",
                                lambda repo, **kw: preflight.append(repo))
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
            # Hang guard only; asserts no latency.
            assert finish_writer.wait(30), "test did not release bounded writer"
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
            # Hang guard only; asserts no latency.
            done, _ = await asyncio.wait((task, waiting), timeout=30, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                await task  # Surface a pre-writer production-path refusal.
            assert waiting in done, "production path never entered its writer"
            if cancellation == "timeout":
                timeout_task = asyncio.create_task(asyncio.wait_for(task, 0.01))
                async def cancellation_arrived():
                    while not task.cancelling():
                        await asyncio.sleep(0)
                # Hang guard, not a latency budget.
                await asyncio.wait_for(cancellation_arrived(), 60)
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
