from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path
import socket
import subprocess
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import uuid

import pytest

pytest.importorskip("mimir_cultivate")

from mimir_cultivate.worklink import control, orchestrator, worker_client, worker_exec
from mimir_cultivate.worklink.compute import ComputeResult, LaunchHandle
from mimir_cultivate.worklink.run_state import WorklinkRunState, load_run_state, save_run_state


@pytest.fixture
def leaf(tmp_path):
    state = WorklinkRunState(
        issue_id=700, attempt=3, backend="claude_code", compute_name="local_subprocess",
        handle_substrate="local_subprocess", handle_identifier=str(uuid.uuid4()),
        shim_pid=4321, process_start_ticks=99, executor_instance_id=str(uuid.uuid4()),
        executor_process_epoch=f"{uuid.uuid4()}:1",
        branch="issue/700-a3", base_ref="main", local_base="main", repo=str(tmp_path),
        repo_url="", test_command=None, started_at=datetime.now(UTC).isoformat(),
    )
    save_run_state(tmp_path, state)
    return state


def _proc(root, pid, *, uid=1001, ticks=99, state="S"):
    entry = root / str(pid)
    entry.mkdir(parents=True, exist_ok=True)
    (entry / "stat").write_text(f"{pid} (worker (name)) {state} " + "0 " * 18 + str(ticks))
    (entry / "status").write_text(f"Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n")
    return entry


@pytest.fixture
def restarted_executor(tmp_path, monkeypatch, leaf):
    """Real client/dispatch/RPC with new instance, no job or exit tombstones."""
    root = tmp_path / "proc"
    root.mkdir()
    original = worker_exec._leaf_tree_verified_dead
    monkeypatch.setattr(worker_exec, "_jobs", {})
    monkeypatch.setattr(worker_exec, "_exited", set())
    monkeypatch.setattr(worker_exec, "_launching", set())
    monkeypatch.setattr(worker_exec, "EXECUTOR_INSTANCE_ID", str(uuid.uuid4()))
    monkeypatch.setattr(worker_exec, "_executor_process_epoch", lambda: leaf.executor_process_epoch)
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(worklink_uid=1001))
    monkeypatch.setattr(worker_exec, "_leaf_tree_verified_dead",
                        lambda pid, ticks, epoch=None: original(pid, ticks, epoch, proc_root=root))
    threads, requests = [], []

    def connect(self, timeout_s=None):
        client, server = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        client.settimeout(5)

        def serve():
            worker_exec.handle_connection(server)

        thread = threading.Thread(target=serve)
        threads.append(thread)
        thread.start()
        return client

    original_send = worker_exec._send

    def send(connection, response, **kwargs):
        requests.append(response)
        return original_send(connection, response, **kwargs)

    monkeypatch.setattr(worker_client.WorkerClient, "_connect", connect)
    monkeypatch.setattr(worker_exec, "_send", send)
    yield root, requests
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()


@pytest.mark.parametrize("tree", ["dead", "live", "escaped", "unreadable", "reused-pid"])
def test_restart_reconcile_requires_dead_tree(tmp_path, monkeypatch, leaf, restarted_executor, tree):
    root, responses = restarted_executor
    if tree == "live":
        _proc(root, leaf.shim_pid)
    elif tree == "escaped":
        _proc(root, 5555)  # Supervisor is gone; a setsid/reparented descendant remains.
    elif tree == "unreadable":
        (_proc(root, 5555) / "status").write_text("no UID data\n")
    elif tree == "reused-pid":
        _proc(root, leaf.shim_pid, uid=2000, ticks=100)
    # The controller's historical probe isn't trusted for descendant retirement.
    monkeypatch.setattr(control, "process_is_alive", lambda _: False)
    calls, events = [], []

    def run(args):
        calls.append(list(args))
        return subprocess.CompletedProcess(args, 0, json.dumps({"labels": []}), "")

    remaining = control.reconcile_run_states(
        tmp_path, runner=run, event_logger=lambda event, **fields: events.append((event, fields)))
    if tree in {"dead", "reused-pid"}:
        assert remaining == []
        assert load_run_state(tmp_path, 700) is None
        assert ["chainlink", "issue", "label", "700", "worklink:ready"] in calls
        assert any(response.get("status") == "retired" for response in responses)
    else:
        assert remaining == [leaf]
        assert load_run_state(tmp_path, 700) == leaf
        assert not calls
        assert events[-1][0] == "worklink_run_state_reconcile_failed"


@pytest.mark.parametrize("tree", ["dead", "live", "escaped"])
def test_operator_clear_verifies_tree_without_cancelling(tmp_path, leaf, restarted_executor, tree):
    root, responses = restarted_executor
    if tree != "dead":
        _proc(root, leaf.shim_pid if tree == "live" else 5555)
    events = []
    logger = lambda event, **fields: events.append((event, fields))
    if tree == "dead":
        control.clear_worklink_leaf_state(tmp_path, 700, worker_id=leaf.handle_identifier, event_logger=logger)
        assert load_run_state(tmp_path, 700) is None
        assert events == [("worklink_leaf_state_cleared", {
            "issue_id": 700, "attempt": 3, "worker_id": leaf.handle_identifier,
            "executor_instance_id": leaf.executor_instance_id,
        })]
    else:
        with pytest.raises(RuntimeError, match="unverified"):
            control.clear_worklink_leaf_state(tmp_path, 700, worker_id=leaf.handle_identifier, event_logger=logger)
        assert load_run_state(tmp_path, 700) == leaf
        assert not events
    assert not any(response.get("status") == "cancelled" for response in responses)


@pytest.mark.parametrize("defect", ["worker-id", "missing-instance", "same-instance", "active", "launching",
                                   "factory", "compute", "no-state", "direct", "gate", "audit-failure"])
def test_operator_clear_fails_closed(tmp_path, monkeypatch, leaf, restarted_executor, defect):
    worker_id = leaf.handle_identifier
    if defect == "worker-id":
        worker_id = str(uuid.uuid4())
    elif defect == "missing-instance":
        leaf = replace(leaf, executor_instance_id=None, executor_process_epoch=None)
    elif defect == "same-instance":
        monkeypatch.setattr(worker_exec, "EXECUTOR_INSTANCE_ID", leaf.executor_instance_id)
    elif defect == "active":
        worker_exec._jobs[worker_id] = Mock()
    elif defect == "launching":
        worker_exec._launching.add(str(uuid.uuid4()))
    elif defect == "factory":
        leaf = replace(leaf, backend="feature_factory")
    elif defect == "compute":
        leaf = replace(leaf, compute_name="remote")
    elif defect == "direct":
        leaf = replace(leaf, shim_pid=None, executor_instance_id=None, executor_process_epoch=None,
                       handle_identifier="4321")
        worker_id = leaf.handle_identifier
    elif defect == "gate":
        leaf = replace(leaf, opencode_session_id="ses_gate", launch_generation="generation")
    save_run_state(tmp_path, leaf)
    if defect == "no-state":
        from mimir_cultivate.worklink.run_state import run_state_path

        run_state_path(tmp_path, 700).unlink()
    logger = Mock(side_effect=OSError("audit failed") if defect == "audit-failure" else None)
    with pytest.raises((RuntimeError, OSError)):
        control.clear_worklink_leaf_state(tmp_path, 700, worker_id=worker_id, event_logger=logger)
    assert load_run_state(tmp_path, 700) == (None if defect == "no-state" else leaf)


@pytest.mark.parametrize("action", ["stop", "reconcile"])
@pytest.mark.parametrize("alive", [False, True])
def test_durable_proof_skips_recancellation(tmp_path, monkeypatch, leaf, action, alive):
    leaf = replace(leaf, children_reaped=True)
    save_run_state(tmp_path, leaf)
    monkeypatch.setattr(control, "process_is_alive", lambda _: alive)
    monkeypatch.setattr(control, "process_identity_verified", lambda _: True)
    cancel = Mock(side_effect=AssertionError("must not contact restarted executor"))
    monkeypatch.setattr(worker_client.WorkerClient, "cancel", cancel)
    local_cancel = AsyncMock()
    monkeypatch.setattr(control.LocalSubprocessComputeBackend, "cancel", local_cancel)
    run = lambda args: subprocess.CompletedProcess(args, 0, json.dumps({"labels": []}), "")
    if action == "stop":
        assert control.stop_worklink(tmp_path, 700, runner=run).stopped is not alive
    else:
        assert control.reconcile_run_states(tmp_path, runner=run) == ([leaf] if alive else [])
    assert load_run_state(tmp_path, 700) == (leaf if alive else None)
    cancel.assert_not_called()
    local_cancel.assert_not_called()


def test_restart_stop_and_partial_reconcile_keep_durable_proof(tmp_path, monkeypatch, leaf, restarted_executor):
    monkeypatch.setattr(control, "process_is_alive", lambda _: False)
    failing_run = lambda args: subprocess.CompletedProcess(args, 1, "", "offline")
    control.reconcile_run_states(tmp_path, runner=failing_run)
    retired = load_run_state(tmp_path, 700)
    assert retired == replace(leaf, children_reaped=True)
    monkeypatch.setattr(worker_client.WorkerClient, "cancel", Mock(side_effect=AssertionError("re-cancel")))
    run = lambda args: subprocess.CompletedProcess(args, 0, "", "")
    assert control.stop_worklink(tmp_path, 700, runner=run).state_cleared


@pytest.mark.parametrize("defect", ["none", "no-proof", "identifier", "pid", "birth"])
def test_terminal_proof_is_bound_to_exact_worker(tmp_path, leaf, defect):
    handle = LaunchHandle("local_subprocess", leaf.handle_identifier, leaf.process_start_ticks, leaf.shim_pid)
    if defect == "identifier":
        handle = replace(handle, identifier=str(uuid.uuid4()))
    elif defect == "pid":
        handle = replace(handle, shim_pid=leaf.shim_pid + 1)
    elif defect == "birth":
        handle = replace(handle, process_start_ticks=100)
    orchestrator._persist_leaf_retirement(
        tmp_path, 700, handle, ComputeResult(0, "", "", children_reaped=defect != "no-proof"))
    assert load_run_state(tmp_path, 700).children_reaped is (defect == "none")


def test_clear_leaf_cli_requires_exact_identifier_and_logs(tmp_path, monkeypatch, leaf, restarted_executor, capsys):
    from mimir_cultivate import commands

    parser = argparse.ArgumentParser()
    commands.add_argparse(parser.add_subparsers(dest="command"))
    with pytest.raises(SystemExit):
        parser.parse_args(["worklink", "clear-leaf-state", "700"])
    args = parser.parse_args(["worklink", "clear-leaf-state", "700",
                              "--worker-id", leaf.handle_identifier, "--home", str(tmp_path)])
    assert commands.dispatch(args, parser) == 0
    assert "leaf state cleared" in capsys.readouterr().out
    events = [json.loads(line) for line in (tmp_path / "logs/events.jsonl").read_text().splitlines()]
    assert any(event.get("type") == "worklink_leaf_state_cleared" for event in events)


@pytest.mark.parametrize("defect", ["missing-proc", "malformed-stat", "invalid-uid", "vanished-entry", "other-uid-live",
                                   "worker-zombie", "recorded-live-other-uid"])
def test_proc_proof_is_conservative(tmp_path, monkeypatch, defect):
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(worklink_uid=1001))
    root = tmp_path / "proc"
    if defect != "missing-proc":
        root.mkdir()
    if defect == "malformed-stat":
        (_proc(root, 4321) / "stat").write_text("invalid")
    elif defect == "vanished-entry":
        (root / "5555").mkdir()
    elif defect == "invalid-uid":
        (_proc(root, 5555) / "status").write_text("Uid: 2000 2000\n")
    elif defect == "other-uid-live":
        _proc(root, 5555, uid=2000)
    elif defect == "worker-zombie":
        _proc(root, 4321, state="Z")
    elif defect == "recorded-live-other-uid":
        _proc(root, 4321, uid=2000)
    assert worker_exec._leaf_tree_verified_dead(4321, 99, proc_root=root) is (
        defect in {"other-uid-live", "worker-zombie"})


@pytest.mark.parametrize("defect", ["fds", "extra-field", "protocol", "id", "instance", "epoch", "pid", "ticks"])
def test_retirement_rpc_validates_exact_contract(leaf, monkeypatch, defect):
    monkeypatch.setattr(worker_exec, "_jobs", {})
    monkeypatch.setattr(worker_exec, "_launching", set())
    monkeypatch.setattr(worker_exec, "_leaf_tree_verified_dead", lambda *args: True)
    request = {
        "version": 1, "op": "verify_leaf_retired", "id": leaf.handle_identifier,
        "executor_identity": worker_exec.EXECUTOR_PROTOCOL_IDENTITY,
        "executor_instance_id": leaf.executor_instance_id,
        "executor_process_epoch": leaf.executor_process_epoch,
        "shim_pid": leaf.shim_pid, "process_start_ticks": leaf.process_start_ticks,
    }
    fds = []
    if defect == "fds":
        fds = [1]
    elif defect == "extra-field":
        request["trust_me"] = True
    elif defect == "protocol":
        request["executor_identity"] = "obsolete"
    elif defect == "id":
        request["id"] = "bad-id"
    elif defect == "instance":
        request["executor_instance_id"] = "bad-instance"
    elif defect == "epoch":
        request["executor_process_epoch"] = "bad-epoch"
    elif defect == "pid":
        request["shim_pid"] = True
    elif defect == "ticks":
        request["process_start_ticks"] = -1
    connection = Mock()
    with pytest.raises((RuntimeError, ValueError)):
        worker_exec._handle_verify_leaf_retired(connection, request, fds)
    connection.send.assert_not_called()


@pytest.mark.parametrize("response", [
    {"status": "retired", "id": "foreign"},
    {"status": "cancelled"},
    {"error": "unknown worker"},
])
def test_retirement_client_requires_bound_proof(monkeypatch, leaf, response):
    connection = Mock()
    connection.recv.return_value = json.dumps(response).encode()
    monkeypatch.setattr(worker_client.WorkerClient, "_connect", lambda *args: connection)
    with pytest.raises(RuntimeError, match="unverified"):
        asyncio.run(worker_client.WorkerClient(None).verify_leaf_retired(
            leaf.handle_identifier, leaf.executor_instance_id, leaf.shim_pid, leaf.process_start_ticks))
    connection.close.assert_called_once()


@pytest.mark.parametrize("defect", ["instance", "epoch", "orphan-epoch", "proof-type", "no-worker", "negative-birth"])
def test_run_state_does_not_trust_malformed_retirement_metadata(leaf, defect):
    data = leaf.to_json()
    if defect == "instance":
        data["executor_instance_id"] = "bad-instance"
    elif defect == "epoch":
        data["executor_process_epoch"] = "bad-epoch"
    elif defect == "orphan-epoch":
        data["executor_instance_id"] = None
    elif defect == "proof-type":
        data["children_reaped"] = "true"
    elif defect == "no-worker":
        data.update(shim_pid=None, handle_identifier="4321")
    elif defect == "negative-birth":
        data["process_start_ticks"] = -1
    with pytest.raises(ValueError):
        WorklinkRunState.from_json(data)


def test_legacy_leaf_remains_readable_but_unproven(leaf):
    data = leaf.to_json()
    del data["executor_instance_id"]
    del data["executor_process_epoch"]
    del data["children_reaped"]
    restored = WorklinkRunState.from_json(data)
    assert restored.executor_instance_id is None
    assert restored.children_reaped is False


def test_reconcile_does_not_clear_concurrently_replaced_worker(tmp_path, monkeypatch, leaf):
    newer = replace(leaf, handle_identifier=str(uuid.uuid4()), attempt=4)
    monkeypatch.setattr(control, "process_is_alive", lambda _: False)
    monkeypatch.setattr(control, "load_run_state", lambda *args: newer)
    cancel = Mock(side_effect=AssertionError("stale cancellation"))
    monkeypatch.setattr(worker_client.WorkerClient, "cancel", cancel)
    assert control.reconcile_run_states(tmp_path) == [newer]
    cancel.assert_not_called()


def test_terminal_state_persists_worker_instance(tmp_path, leaf):
    from mimir_cultivate.worklink.checkout import CheckoutLease

    handle = LaunchHandle("local_subprocess", leaf.handle_identifier, 99, 4321)
    compute = SimpleNamespace(name="local_subprocess", worker_executor_instance_id=lambda _: leaf.executor_instance_id,
                              worker_executor_process_epoch=lambda _: leaf.executor_process_epoch)
    orchestrator._persist_run_state(
        tmp_path, issue=orchestrator.IssueContext(700, "title", "body", set()),
        attempt=3, backend_name="claude_code", compute=compute, handle=handle,
        lease=CheckoutLease(700, 3, tmp_path, tmp_path, leaf.branch, "main"),
        repo=tmp_path, repo_url="", test_command=None, started_at=datetime.now(UTC))
    assert load_run_state(tmp_path, 700).executor_instance_id == leaf.executor_instance_id
    assert load_run_state(tmp_path, 700).executor_process_epoch == leaf.executor_process_epoch


def test_proc_proof_refuses_unsupported_platform(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_exec.sys, "platform", "darwin")
    assert not worker_exec._leaf_tree_verified_dead(4321, 99, proc_root=tmp_path)


def test_proc_proof_refuses_permission_errors(tmp_path, monkeypatch):
    entry = _proc(tmp_path, 4321)
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path == entry / "stat":
            raise PermissionError("unreadable birth identity")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    assert not worker_exec._leaf_tree_verified_dead(4321, 99, proc_root=tmp_path)


@pytest.mark.parametrize("epoch", ["new-namespace", "new-boot", "same", "unreadable", "live-recorded",
                                  "missing-recorded"])
def test_redeploy_proof_does_not_confuse_new_daemon_with_old_leaf(
    tmp_path, monkeypatch, leaf, restarted_executor, epoch,
):
    root, responses = restarted_executor
    boot, _ = leaf.executor_process_epoch.split(":")
    current = {"new-namespace": f"{boot}:1000", "new-boot": f"{uuid.uuid4()}:1",
               "same": leaf.executor_process_epoch, "unreadable": None,
               "live-recorded": f"{boot}:1000", "missing-recorded": leaf.executor_process_epoch}[epoch]
    if epoch == "missing-recorded":
        leaf = replace(leaf, executor_process_epoch=None)
        save_run_state(tmp_path, leaf)
    monkeypatch.setattr(worker_exec, "_executor_process_epoch", lambda: current)
    _proc(root, 5555, ticks=1001)  # Persistent daemon started in the new deployment.
    if epoch == "live-recorded":
        _proc(root, leaf.shim_pid)
    monkeypatch.setattr(control, "process_is_alive", lambda _: False)
    run = lambda args: subprocess.CompletedProcess(args, 0, json.dumps({"labels": []}), "")
    remaining = control.reconcile_run_states(tmp_path, runner=run)
    if epoch in {"new-namespace", "new-boot"}:
        assert remaining == []
        assert load_run_state(tmp_path, 700) is None
    else:
        assert remaining == [leaf]
        assert load_run_state(tmp_path, 700) == leaf


@pytest.mark.parametrize("defect", ["none", "foreign-namespace", "unreadable", "bad-boot", "missing-init-birth"])
def test_executor_process_epoch_requires_own_proc_namespace(monkeypatch, defect):
    boot = str(uuid.uuid4())
    monkeypatch.setattr(worker_exec.os, "readlink", lambda path: (
        "pid:[2]" if defect == "foreign-namespace" and path == "/proc/1/ns/pid" else "pid:[1]"))
    original = Path.read_text

    def read(path, *args, **kwargs):
        if str(path) == "/proc/sys/kernel/random/boot_id":
            if defect == "unreadable":
                raise PermissionError("boot identity unavailable")
            return "invalid" if defect == "bad-boot" else boot
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(worker_exec, "process_start_ticks", lambda _: None if defect == "missing-init-birth" else 42)
    assert worker_exec._executor_process_epoch() == (f"{boot}:42" if defect == "none" else None)


@pytest.mark.parametrize("defect", ["instance", "epoch", "orphan-epoch"])
def test_launch_client_refuses_invalid_restart_metadata(tmp_path, monkeypatch, defect):
    identifier = str(uuid.uuid4())
    response = {
        "id": identifier, "status": "started", "pid": 4321, "process_start_ticks": 99,
        "executor_instance_id": str(uuid.uuid4()), "executor_process_epoch": f"{uuid.uuid4()}:1",
    }
    if defect == "instance":
        response["executor_instance_id"] = "broken-instance"
    elif defect == "epoch":
        response["executor_process_epoch"] = "broken-epoch"
    else:
        response["executor_instance_id"] = None
    connection = Mock()
    connection.recv.return_value = json.dumps(response).encode()
    monkeypatch.setattr(worker_client.WorkerClient, "_connect", lambda *args: connection)
    client = worker_client.WorkerClient.for_path_checkout(tmp_path, issue_id=700, attempt=3, run_uid=1001)
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(client.launch(
            local_checkout=tmp_path, argv=["true"], env={}, identifier=identifier, timeout_s=30))
    connection.close.assert_called_once()
