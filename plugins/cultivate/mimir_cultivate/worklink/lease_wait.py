"""Controller-owned queue accounting, never builder-supplied duration claims.

The relay acquires the *existing* heavy lease on behalf of a leaf. The command
still runs as the builder; the relay accepts no commands, lease paths or duration
claims, only an authenticated request for a valid tier. Only time actually spent
inside acquisition earns deadline credit.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import select
import socket
import socketserver
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Mapping

from mimir.heavy_run import heavy_run

if TYPE_CHECKING:
    from .backends.base import LaunchHandle
    from .compute import ComputeBackend, ComputeResult

SOCKET_ENV = "WORKLINK_LEASE_SOCKET"
TOKEN_ENV = "WORKLINK_LEASE_AUTH"
POLL_S = 0.1


def extension_cap(environment: Mapping[str, str]) -> int:
    if not environment.get("MIMIR_HEAVY_LOCK_FILE"):
        return 0
    value = float(environment.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200"))
    if not math.isfinite(value) or value <= 0:
        raise ValueError("invalid heavy lease timeout")
    return min(int(value), 14400)


async def wait_for_builder(
    compute: ComputeBackend, handle: LaunchHandle, timeout_s: float,
    relay: LeaseWaitRelay | None, *, started_at: float | None = None,
) -> ComputeResult:
    """Keep the executor's hard ceiling, enforcing the credited deadline here."""
    if relay is None:
        return await compute.wait(handle, timeout_s)
    started = time.monotonic() if started_at is None else started_at
    task = asyncio.create_task(compute.wait(handle, timeout_s + relay.cap))
    timed_out = False
    try:
        while not task.done():
            _, extension = relay.snapshot()
            remaining = started + timeout_s + extension - time.monotonic()
            if remaining <= 0:
                timed_out = True
                await compute.cancel(handle)
                break
            await asyncio.wait({task}, timeout=min(POLL_S, remaining))
        result = await task
        total, extension = relay.snapshot()
        return replace(result, timed_out=timed_out or result.timed_out,
                       lease_wait_s=total, deadline_extension_s=extension,
                       builder_elapsed_s=time.monotonic() - started)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class LeaseWaitRelay:
    def __init__(self, environment: Mapping[str, str]) -> None:
        self.base = environment["MIMIR_HEAVY_LOCK_FILE"]
        self.cap = extension_cap(environment)
        self.token = secrets.token_hex(32)
        self._lock = threading.Lock()
        self._waiting = 0
        self._since = 0.0
        self._total = 0.0
        self._stopped = threading.Event()
        # Only the controller can replace the socket. Workers may traverse and
        # connect, but cannot unlink it or substitute a fake server.
        # AF_UNIX has a 108-byte path bound. Session TMPDIRs can already exceed
        # that; /tmp is short, traversable by the dropped uid, and sticky.
        self._directory = tempfile.TemporaryDirectory(prefix="worklink-lease-", dir="/tmp")
        os.chmod(self._directory.name, 0o711)
        self.path = Path(self._directory.name) / "relay.sock"
        relay = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                relay._handle(self.request)

        class Server(socketserver.ThreadingUnixStreamServer):
            daemon_threads = False

        self._server = Server(str(self.path), Handler)
        os.chmod(self.path, 0o666)
        self._thread = threading.Thread(
            target=lambda: self._server.serve_forever(poll_interval=POLL_S), daemon=True,
        )
        self._thread.start()

    @property
    def environment(self) -> dict[str, str]:
        return {SOCKET_ENV: str(self.path), TOKEN_ENV: self.token}

    def snapshot(self) -> tuple[float, float]:
        with self._lock:
            total = self._total
            if self._waiting:
                total += time.monotonic() - self._since
            return total, min(total, self.cap)

    def _queue(self, entering: bool) -> None:
        with self._lock:
            if entering:
                if not self._waiting:
                    self._since = time.monotonic()
                self._waiting += 1
            else:
                self._waiting -= 1
                if not self._waiting:
                    self._total += time.monotonic() - self._since

    def _handle(self, connection: socket.socket) -> None:
        connection.settimeout(2)
        try:
            raw = bytearray()
            while not raw.endswith(b"\n") and len(raw) <= 512:
                part = connection.recv(513 - len(raw))
                if not part:
                    return
                raw.extend(part)
            request = json.loads(raw)
            if (not isinstance(request, dict) or set(request) != {"token", "tier"}
                    or not isinstance(request["token"], str)
                    or not hmac.compare_digest(request["token"], self.token)
                    or request["tier"] not in heavy_run.TIERS):
                return
        except (OSError, ValueError):
            return
        hb = heavy_run.Heartbeat()
        queued = False

        def check() -> None:
            if self._stopped.is_set() or select.select([connection], [], [], 0)[0]:
                raise ConnectionAbortedError("lease client disconnected")

        def entered() -> None:
            nonlocal queued
            self._queue(True)
            queued = True

        try:
            heavy_run._acquire(self.base, request["tier"], 600, 1, hb,
                               check=check, queued=entered)
            self._queue(False)
            queued = False
            connection.sendall(b"acquired\n")
            next_beat = time.monotonic() + 1
            while not self._stopped.wait(POLL_S):
                check()
                if time.monotonic() >= next_beat:
                    hb.beat()
                    next_beat = time.monotonic() + 1
        except OSError:
            pass
        finally:
            if queued:
                self._queue(False)
            for path in sorted(hb.held, key=lambda p: p.name, reverse=True):
                fd = hb.held[path]
                try:
                    heavy_run._release(path, fd)
                finally:
                    os.close(fd)

    def close(self) -> None:
        self._stopped.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
        self._directory.cleanup()
