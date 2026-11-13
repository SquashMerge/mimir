"""In-process transport for real factory-control capture/supervision tests."""
from __future__ import annotations

import json
import os
from types import SimpleNamespace


def install_local_control_executor(monkeypatch, tmp_path):
    from mimir_cultivate.worklink import worker_client, worker_exec

    homes = tmp_path / "executor-homes"
    homes.mkdir()
    monkeypatch.setattr(worker_exec, "HOME_ROOT", homes)
    # These tests own the checkout and run as the test uid. Identity/FD boundary
    # guards are exercised separately; retain real launch, capture and subreaping.
    monkeypatch.setattr(worker_exec, "_open_factory_checkout", lambda request, **kw: os.open(request["path"], os.O_RDONLY | os.O_DIRECTORY))
    monkeypatch.setattr(worker_exec, "_drop_worker", os.fchdir)
    monkeypatch.setattr(worker_exec.os, "chown", lambda *args: None)
    calls = []

    async def launch(self, *, local_checkout, argv, env, identifier, timeout_s, stdout_sink, stderr_sink):
        assert self._launch_op == "launch_factory_control"
        responses = []
        connection = SimpleNamespace(send=lambda payload, *args: responses.append(json.loads(payload)))
        request = {
            "version": 1, "op": self._launch_op, "executor_identity": worker_client.EXECUTOR_PROTOCOL_IDENTITY,
            "id": identifier, "issue": self.issue_id, "attempt": self.attempt,
            "path": str(local_checkout), "run_uid": self.run_uid,
            "argv": list(argv), "env": dict(env), "projections": [], "timeout_s": timeout_s,
            "stdout_limit": stdout_sink.limit, "stderr_limit": stderr_sink.limit,
        }
        fds = [os.dup(stdout_sink.fd), os.dup(stderr_sink.fd)]
        try:
            worker_exec._handle_launch(connection, request, fds)
        finally:
            for fd in fds:
                os.close(fd)
        terminal = responses[-1]
        assert terminal["status"] == "terminal"
        calls.append(terminal)

        async def wait():
            return terminal["exit_code"]

        return SimpleNamespace(wait=wait, **{name: terminal[name] for name in (
            "timed_out", "output_overflow", "stdout_bytes", "stderr_bytes",
        )})

    monkeypatch.setattr(worker_client.WorkerClient, "launch", launch)
    return calls
