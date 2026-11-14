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
    monkeypatch.setattr(worker_exec, "_drop_factory", lambda fd, home: os.fchdir(fd))
    monkeypatch.setattr(worker_exec.os, "chown", lambda *args: None)
    calls = []

    class Socket:
        def __init__(self):
            self.responses = []

        def sendmsg(self, buffers, ancillary):
            # Keep real client request serialization and terminal decoding; only
            # replace the socket transport and privileged checkout setup.
            request = json.loads(b"".join(buffers))
            assert request["op"] in {"launch_factory_control", "launch_factory"}
            connection = SimpleNamespace(send=lambda payload, *args: self.responses.append(payload))
            fds = [os.dup(fd) for fd in ancillary[0][2]]
            try:
                worker_exec._handle_launch(connection, request, fds)
            finally:
                for fd in fds:
                    os.close(fd)
            terminal = json.loads(self.responses[-1])
            assert terminal["status"] == "terminal"
            calls.append(terminal)

        def recv(self, size):
            return self.responses.pop(0)

        def close(self):
            pass

    monkeypatch.setattr(worker_client.WorkerClient, "_connect", lambda self: Socket())
    return calls
