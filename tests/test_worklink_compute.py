from __future__ import annotations

import asyncio
import errno
import os
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

pytest.importorskip("mimir_cultivate", reason="Worklink compute belongs to Cultivate")

from mimir_cultivate.worklink import compute, run_state


@pytest.mark.asyncio
@pytest.mark.parametrize("state,exit_code", [("succeeded", 0), ("failed", 1), ("interrupted", 1), ("incomplete", 1)])
async def test_daemon_compute_uses_authoritative_session_outcome(state, exit_code):
    from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionOutcome

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    outcome = SessionOutcome("ses_owned", state, stdout="bounded output")
    client = SimpleNamespace(session_observe=AsyncMock(return_value=outcome),
                             session_cancel=AsyncMock(), session_release=AsyncMock())
    backend = compute.OpenCodeDaemonComputeBackend(client=client)
    result = await backend.wait(handle, 1)
    assert result.exit_code == exit_code
    assert result.session_outcome == outcome
    assert result.stdout == "bounded output"
    assert handle.shim_pid is None
    assert backend.capabilities().persistent_after_disconnect
    await backend.cancel(handle)
    await backend.cleanup(handle)
    client.session_cancel.assert_awaited_once_with(handle)
    client.session_release.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_daemon_compute_overflow_cancels_owned_session():
    from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionOutcome

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    client = SimpleNamespace(session_observe=AsyncMock(return_value=SessionOutcome("ses_owned", "started", output_overflow=True)),
                             session_cancel=AsyncMock())
    result = await compute.OpenCodeDaemonComputeBackend(client=client).wait(handle, 1)
    assert result.output_overflow
    assert result.exit_code != 0
    client.session_cancel.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_daemon_compute_reconnects_without_readmission():
    from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionOutcome, SessionTransportLostError

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    client = SimpleNamespace(session_observe=AsyncMock(side_effect=[
        SessionTransportLostError("lost"), SessionOutcome("ses_owned", "succeeded"),
    ]), session_create=AsyncMock())
    result = await compute.OpenCodeDaemonComputeBackend(client=client).wait(handle, 2)
    assert result.exit_code == 0
    assert client.session_observe.await_count == 2
    client.session_create.assert_not_awaited()


@pytest.mark.asyncio
async def test_daemon_wait_does_not_refresh_expired_launch_budget():
    from mimir_cultivate.worklink.backends.base import LaunchHandle

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    client = SimpleNamespace(session_observe=AsyncMock(), session_cancel=AsyncMock(side_effect=RuntimeError("unverified")))
    backend = compute.OpenCodeDaemonComputeBackend(client=client)
    backend._deadlines[handle.identifier] = asyncio.get_running_loop().time() - 1
    result = await backend.wait(handle, 60)
    assert result.timed_out and result.session_error.type == "timeout"
    assert result.session_outcome.cancellation_state == "unverified"
    client.session_observe.assert_not_awaited()
    client.session_cancel.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_daemon_observation_backs_off_to_a_bounded_interval(monkeypatch):
    """Each observation spawns an executor helper; a long build must not poll every 100 ms."""
    from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionOutcome, SessionTransportLostError

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    running = SessionOutcome("ses_owned", "started")
    client = SimpleNamespace(session_observe=AsyncMock(side_effect=[
        running, SessionTransportLostError("lost"), *([running] * 6), SessionOutcome("ses_owned", "succeeded"),
    ]))
    sleeps = []
    async def no_wait(seconds):
        sleeps.append(seconds)
    monkeypatch.setattr(compute.asyncio, "sleep", no_wait)
    result = await compute.OpenCodeDaemonComputeBackend(client=client).wait(handle, 60)
    assert result.exit_code == 0
    assert sleeps == pytest.approx([0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0, 2.0])
    assert max(sleeps) == compute._OBSERVE_INTERVAL_CAP_S


def test_session_helper_timeout_outlasts_its_bounded_work():
    """The executor's helper backstop is the shared constant and exceeds the 25 s cancel window."""
    import inspect
    from mimir_cultivate.worklink import daemon_config, worker_exec

    assert daemon_config.SESSION_HELPER_TIMEOUT_S >= 120
    assert "timeout=SESSION_HELPER_TIMEOUT_S" in inspect.getsource(worker_exec._handle_session)


@pytest.mark.asyncio
async def test_session_rpc_waits_longer_than_the_helper_allowance(monkeypatch, tmp_path):
    """Review P1 on #103: a 35 s client socket abandoned a helper allowed 120 s."""
    import json as _json
    import socket as _socket
    import threading
    from mimir_cultivate.worklink import daemon_config
    from mimir_cultivate.worklink.worker_client import WorkerClient

    client_end, executor_end = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    requested = []

    def connect(self, timeout_s=None):
        requested.append(timeout_s)
        client_end.settimeout(timeout_s)
        return client_end

    def executor():
        _json.loads(executor_end.recv(1 << 20))
        executor_end.sendall(_json.dumps({"cancellation_state": "verified"}).encode())

    thread = threading.Thread(target=executor)
    thread.start()
    monkeypatch.setattr(WorkerClient, "_connect", connect)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    client = WorkerClient.for_path_checkout(checkout, issue_id=1, attempt=1, run_uid=os.geteuid())
    response = await client._session_rpc("session_cancel", {"generation": "g", "opencode_session_id": "ses_x"})
    thread.join(5)
    executor_end.close()
    assert response == {"cancellation_state": "verified"}
    assert requested == [daemon_config.SESSION_RPC_TIMEOUT_S]
    assert daemon_config.SESSION_RPC_TIMEOUT_S > daemon_config.SESSION_HELPER_TIMEOUT_S


@pytest.mark.parametrize("stdout,label", [
    (b'{"failure": {"class": "DaemonClientError", "reason": "invalid shell entry"}}', " (DaemonClientError invalid shell entry)"),
    (b'{"failure": {"class": "TimeoutError"}}', " (TimeoutError)"),
    (b'{"failure": {"class": "X", "reason": "<script>"}}', " (X)"),
    (b"not json", ""), (b'{"other": 1}', ""),
])
def test_executor_labels_a_failed_session_helper(stdout, label):
    from mimir_cultivate.worklink.worker_exec import _helper_failure

    assert _helper_failure(stdout) == label


@pytest.mark.asyncio
async def test_session_rpc_keeps_the_executor_failure_reason(monkeypatch, tmp_path, caplog):
    import json as _json
    import logging
    import socket as _socket
    import threading
    from mimir_cultivate.worklink.worker_client import WorkerClient

    client_end, executor_end = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    reason = "session operation failed (DaemonClientError invalid shell entry); retain controller and checkout"

    def executor():
        _json.loads(executor_end.recv(1 << 20))
        executor_end.sendall(_json.dumps({"id": "x", "error": reason + " Bearer " + "fragment-secret"}).encode())

    thread = threading.Thread(target=executor)
    thread.start()
    monkeypatch.setattr(WorkerClient, "_connect", lambda self, timeout_s=None: client_end)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    client = WorkerClient.for_path_checkout(checkout, issue_id=1, attempt=1, run_uid=os.geteuid())
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError) as raised:
        await client._session_rpc("session_observe" if False else "session_cancel",
                                  {"generation": "g", "opencode_session_id": "ses_x"})
    thread.join(5)
    executor_end.close()
    assert "invalid shell entry" in str(raised.value)
    assert "fragment-secret" not in str(raised.value) and "fragment-secret" not in caplog.text
    assert "worklink session session_cancel failed" in caplog.text


def test_authorized_checkout_binds_configured_worker_uid(monkeypatch, tmp_path) -> None:
    from mimir_cultivate.worklink import identities
    from mimir_cultivate.worklink.checkout import _mint_checkout_authorization

    monkeypatch.setattr(
        identities, "get_identities", lambda: SimpleNamespace(worklink_uid=4242),
    )
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    with _mint_checkout_authorization(tmp_path, 17, 1, fd) as authorization:
        worker = compute.LocalSubprocessComputeBackend.for_authorized_checkout(
            authorization, worker_uid=4242,
        )
        assert worker._authorized_checkout is authorization
        assert worker._worker_uid == 4242
        authorization.verify(tmp_path)
        for wrong_uid in (4243, -1, True, "4242"):
            with pytest.raises(ValueError, match="configured worker identity"):
                compute.LocalSubprocessComputeBackend.for_authorized_checkout(
                    authorization, worker_uid=wrong_uid,
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [errno.EPERM, errno.ESRCH, errno.EINVAL], ids=["EPERM", "ESRCH", "EINVAL"])
async def test_external_process_wait_kill_guard(monkeypatch, error) -> None:
    failure = OSError(error, os.strerror(error))
    kill = Mock(side_effect=failure)
    sleep = AsyncMock(side_effect=AssertionError("wait must stop after signal error"))
    monkeypatch.setattr(compute.os, "kill", kill)
    monkeypatch.setattr(run_state, "process_is_zombie", Mock(return_value=False))
    monkeypatch.setattr(compute.asyncio, "sleep", sleep)

    process = compute._ExternalProcess(4321)
    if error != errno.ESRCH:
        # EPERM must fail loudly, not return a successful wait for a live PID.
        with pytest.raises(PermissionError if error == errno.EPERM else OSError) as raised:
            await process.wait()
        assert raised.value is failure
    else:
        assert await process.wait() is None
    kill.assert_called_once_with(4321, 0)
    sleep.assert_not_called()


@pytest.mark.asyncio
async def test_external_process_cancel_propagates_probe_permission_error(monkeypatch) -> None:
    from mimir_cultivate.worklink import worker_exec

    failure = PermissionError(errno.EPERM, "not our process")
    kill = Mock(side_effect=failure)
    terminate = Mock()
    monkeypatch.setattr(compute.os, "getpgid", Mock(return_value=4321))
    monkeypatch.setattr(compute.os, "kill", kill)
    monkeypatch.setattr(run_state, "process_is_zombie", Mock(return_value=False))
    monkeypatch.setattr(worker_exec, "_terminate_process_group_pid", terminate)

    with pytest.raises(PermissionError) as raised:
        await compute._kill_process_group(compute._ExternalProcess(4321))

    assert raised.value is failure
    terminate.assert_called_once_with(4321)
    kill.assert_called_once_with(4321, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("check_raises", [False, True], ids=["inflight", "inflight-error"])
async def test_cleanup_after_wait_error_drains_live_monitor(
    monkeypatch, tmp_path, check_raises
) -> None:
    backend = compute.LocalSubprocessComputeBackend()
    failure = RuntimeError("process wait failed")
    proc = Mock(pid=None, wait=AsyncMock(side_effect=failure))
    monkeypatch.setattr(compute.asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))
    spec = compute.WorkSpec(
        issue_id=1, attempt=1, repo_url="", base_ref="main", branch="test",
        prompt="", rules=None, test_command="", backend="test", timeout_s=10,
        local_checkout=tmp_path, local_argv=("unused",),
    )
    handle = await backend.launch(spec)
    capture = backend._direct_captures[handle.identifier]
    loop = asyncio.get_running_loop()
    checking = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    check_errors = []
    original_overflowed = capture.stdout_sink.overflowed

    def blocked_check():
        loop.call_soon_threadsafe(checking.set)
        try:
            if not release.wait(5):
                raise AssertionError("test did not release output check")
            original_overflowed()
            if check_raises:
                raise RuntimeError("output check failed")
            return False
        except OSError as exc:
            check_errors.append(exc)
            raise
        finally:
            finished.set()

    monkeypatch.setattr(capture.stdout_sink, "overflowed", blocked_check)
    close_observations = []
    for sink in (capture.stdout_sink, capture.stderr_sink):
        original_close = sink.close

        def close(original_close=original_close):
            close_observations.append((
                capture.monitor_task.done(), capture.collect_task.done(), finished.is_set(),
            ))
            original_close()

        monkeypatch.setattr(sink, "close", close)

    cleanup_task = None
    try:
        await asyncio.wait_for(checking.wait(), 5)
        with pytest.raises(RuntimeError, match="process wait failed"):
            await backend.wait(handle, 10)
        assert not capture.monitor_task.done()
        cleanup_task = asyncio.create_task(backend.cleanup(handle))
        await asyncio.sleep(0)
        assert not cleanup_task.done()
        assert capture.stdout_sink.fd >= 0
        release.set()
        await asyncio.wait_for(cleanup_task, 5)
        assert close_observations == [(True, True, True), (True, True, True)]
        assert capture.monitor_task.cancelled()
        assert not capture.collect_task._log_traceback
        assert not check_errors
        assert handle.identifier not in backend._direct_captures
    finally:
        release.set()
        tasks = [capture.monitor_task, capture.collect_task]
        if cleanup_task is not None:
            tasks.append(cleanup_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        capture.stdout_sink.close()
        capture.stderr_sink.close()


@pytest.mark.asyncio
async def test_cleanup_drains_kill_task_published_during_monitor_teardown() -> None:
    backend = compute.LocalSubprocessComputeBackend()
    stdout, stderr = compute.open_output_pair(None, 1024, None, 1024)
    capture = compute._DirectCapture(stdout, stderr)
    handle = compute.LaunchHandle(backend.name, "test")
    backend._handles[handle.identifier] = handle
    backend._direct_captures[handle.identifier] = capture
    started = asyncio.Event()
    stopped = []

    async def pending(name):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            assert stdout.fd >= 0 and stderr.fd >= 0
            stopped.append(name)
            raise RuntimeError(f"{name} teardown failed")

    async def monitor():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            capture.kill_task = asyncio.create_task(pending("kill"))
            await asyncio.sleep(0)

    capture.collect_task = asyncio.create_task(pending("collect"))
    capture.monitor_task = asyncio.create_task(monitor())
    try:
        await started.wait()
        await backend.cleanup(handle)
        assert sorted(stopped) == ["collect", "kill"]
        assert capture.monitor_task.cancelled()
        # A task with an unretrieved exception still has _log_traceback set.
        for task in (capture.collect_task, capture.kill_task):
            assert task.done()
            assert not task._log_traceback
        assert stdout.fd == stderr.fd == -1
    finally:
        tasks = [capture.monitor_task, capture.collect_task]
        if capture.kill_task is not None:
            tasks.append(capture.kill_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        stdout.close()
        stderr.close()
