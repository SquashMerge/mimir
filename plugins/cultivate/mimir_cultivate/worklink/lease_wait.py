"""Controller-owned lease relay and bounded builder deadline credit.

The worker may request a tier, never report a duration or a state transition.
Only our installed helper, spawned with fixed argv and private pipes, can enter
and leave the credited queue interval. Neither checkout code nor lease owner
files (which are writable by participants) are trusted as timing evidence.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hmac
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import time
from typing import TYPE_CHECKING

from mimir.heavy_run import helper_path

SOCKET_ENV = "WORKLINK_LEASE_SOCKET"
AUTH_ENV = "WORKLINK_LEASE_AUTH"

if TYPE_CHECKING:
    from .compute import WorkSpec, ComputeResult
    from .backends.base import LaunchHandle


class LeaseRelay:
    def __init__(self, environment: dict[str, str]) -> None:
        self.environment = environment
        self.cap = min(float(environment.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200")), 14400)
        self.token = secrets.token_hex(32)
        self.started = time.monotonic()
        self.total = 0.0
        self.queued_since: float | None = None
        self.tasks: set[asyncio.Task] = set()
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        # A controller-owned directory: workers can traverse/connect, not replace
        # the socket. The random per-run credential fences other worker sessions.
        # Ignore inherited TMPDIR: session-private roots are not traversable by
        # other uids and routinely exceed sockaddr_un's 108-byte path bound.
        # /tmp's sticky bit protects this controller-owned random directory.
        self.directory = tempfile.TemporaryDirectory(prefix="wl-lease-", dir="/tmp")
        try:
            os.chmod(self.directory.name, 0o711)
            self.path = str(Path(self.directory.name) / "relay.sock")
            self.server = await asyncio.start_unix_server(self._accept, self.path, limit=1024)
            os.chmod(self.path, 0o666)
        except BaseException:
            if hasattr(self, "server"):
                self.server.close()
                await self.server.wait_closed()
            self.directory.cleanup()
            raise

    @property
    def env(self) -> dict[str, str]:
        return {SOCKET_ENV: self.path, AUTH_ENV: self.token}

    @property
    def wait_s(self) -> float:
        return self.total + (time.monotonic() - self.queued_since if self.queued_since is not None else 0)

    @property
    def credit_s(self) -> float:
        return min(self.wait_s, self.cap)

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            request = json.loads(await asyncio.wait_for(reader.readline(), 5))
            if (not isinstance(request, dict) or set(request) != {"auth", "tier"}
                    or not isinstance(request["auth"], str)
                    or not hmac.compare_digest(request["auth"], self.token)
                    or not isinstance(request["tier"], str)
                    or request["tier"] not in {"medium", "large"}):
                return
            # Serialize this run's requests; do not double-credit overlapping waits.
            async with self.lock:
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    await self._hold(request["tier"], writer, disconnected)
                finally:
                    disconnected.cancel()
                    await asyncio.gather(disconnected, return_exceptions=True)
        except (OSError, ValueError, TimeoutError):
            pass  # Malformed/forged reports fail closed, granting no time.
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            finally:
                self.tasks.discard(task)

    async def _hold(self, tier: str, writer: asyncio.StreamWriter, disconnected: asyncio.Task) -> None:
        env = {**self.environment, "MIMIR_HEAVY_TIER": tier, "MIMIR_HEAVY_REPORT_FD": "1"}
        # Not worker argv, PATH, Python settings, or checkout helper code.
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-I", str(helper_path()), sys.executable, "-I", "-c",
            "import sys; sys.stdin.buffer.read()",
            env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            async with asyncio.timeout(float(self.environment.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200"))):
                for expected in (b"queued\n", b"acquired\n"):
                    event = asyncio.create_task(process.stdout.readline())
                    try:
                        done, _ = await asyncio.wait((event, disconnected), return_when=asyncio.FIRST_COMPLETED)
                        if disconnected in done or await event != expected:
                            return
                    finally:
                        event.cancel()
                        await asyncio.gather(event, return_exceptions=True)
                    if expected == b"queued\n":
                        self.queued_since = time.monotonic()
                    else:
                        self._stop_wait()
                writer.write(b"acquired\n")
                await writer.drain()
                await disconnected
        finally:
            self._stop_wait()

            async def retire() -> None:
                if process.returncode is None:
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except TimeoutError:
                    process.kill()
                    await process.wait()

            # close() can cancel a handler already retiring after disconnect.
            # Never let that second cancellation detach the holder's cleanup.
            retirement = asyncio.create_task(retire())
            while True:
                try:
                    await asyncio.shield(retirement)
                    break
                except asyncio.CancelledError:
                    continue

    def _stop_wait(self) -> None:
        if self.queued_since is not None:
            self.total += time.monotonic() - self.queued_since
            self.queued_since = None

    async def close(self) -> None:
        self.server.close()
        await self.server.wait_closed()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.directory.cleanup()

    async def wait(self, task: asyncio.Task, timeout_s: float, *, started: float) -> object:
        """Only actual queue time moves the deadline, and never beyond the cap."""
        while True:
            remaining = started + timeout_s + self.credit_s - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            done, _ = await asyncio.wait((task,), timeout=min(remaining, 0.05))
            if done:
                return task.result()


class BuilderLeaseCompute:
    """Shared deadline boundary; gates and non-builder work remain unchanged."""

    async def launch(self, spec: WorkSpec) -> LaunchHandle:
        from .compute import ComputeLaunchError, _heavy_lock_environment, with_worker_environment
        relay = None
        original_timeout = spec.timeout_s
        try:
            if spec.builder_lease_credit:
                environment = _heavy_lock_environment()
                if environment:
                    candidate = LeaseRelay(environment)
                    await candidate.start()
                    relay = candidate
                    # Inner executors get the maximum budget, not authority to grant
                    # it. This controller enforces the dynamic, credited deadline.
                    spec = with_worker_environment(spec, relay.env)
                    spec = replace(spec, timeout_s=spec.timeout_s + int(relay.cap))
            if relay is None:
                handle = await self._launch(spec)
            else:
                launch = asyncio.create_task(self._launch(spec))
                try:
                    handle = await relay.wait(launch, original_timeout, started=relay.started)
                except TimeoutError as exc:
                    raise ComputeLaunchError("builder launch deadline exceeded") from exc
                finally:
                    if not launch.done():
                        launch.cancel()
                        await asyncio.gather(launch, return_exceptions=True)
        except BaseException:
            if relay is not None:
                await relay.close()
            raise
        if relay is not None:
            if not hasattr(self, "_lease_relays"):
                self._lease_relays = {}
            self._lease_relays[handle.identifier] = (relay, original_timeout)
        return handle

    async def wait(self, handle: LaunchHandle, timeout_s: float) -> ComputeResult:
        entry = getattr(self, "_lease_relays", {}).get(handle.identifier)
        if entry is None:
            return await self._wait(handle, timeout_s)
        relay, original_timeout = entry
        task = asyncio.create_task(self._wait(handle, original_timeout + relay.cap))
        try:
            try:
                result = await relay.wait(task, min(timeout_s, original_timeout), started=relay.started)
            except TimeoutError:
                wait_s, credit_s = relay.wait_s, relay.credit_s
                elapsed_s = time.monotonic() - relay.started
                await self.cancel(handle)
                # Preserve the worker's terminal/quiescence witness and bounded
                # transcript. A deadline is still authoritative over a zero exit.
                result = await asyncio.wait_for(asyncio.shield(task), 10)
                result = replace(result, timed_out=True)
            else:
                wait_s, credit_s = relay.wait_s, relay.credit_s
                elapsed_s = time.monotonic() - relay.started
            return replace(result, lease_wait_s=wait_s,
                           lease_extension_s=credit_s, run_duration_s=elapsed_s)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def cleanup(self, handle: LaunchHandle) -> None:
        # Keep the relay/lease alive until worker quiescence is verified.
        await self._cleanup(handle)
        entry = getattr(self, "_lease_relays", {}).pop(handle.identifier, None)
        if entry is not None:
            await entry[0].close()
