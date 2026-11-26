from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import socket
import sys

import pytest

from mimir_cultivate.worklink.compute import ComputeResult
from mimir_cultivate.worklink.lease_wait import (
    LeaseWaitRelay, extension_cap, wait_for_builder,
)
from mimir.heavy_run import helper_path


async def eventually(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


class ProcessCompute:
    def __init__(self, proc):
        self.proc = proc
        self.cancelled = False

    async def wait(self, handle, timeout_s):
        await self.proc.communicate()
        return ComputeResult(self.proc.returncode, "", "")

    async def cancel(self, handle):
        self.cancelled = True
        self.proc.terminate()


@pytest.mark.parametrize("capped", [False, True])
def test_real_lease_wait_extends_builder_deadline_but_is_capped(tmp_path, capped):
    """No extension kills the uncapped command; no cap hangs the capped case."""
    async def scenario():
        base = str(tmp_path / "heavy")
        # Simulate another deployment holding both slots. These generations are
        # fixture-owned; no shared/ambient lease is inspected or modified.
        for suffix in (".lease", ".lease2"):
            slot = Path(base + suffix)
            slot.mkdir()
            (slot / "owner").write_text('{"tier": "large"}')
            (slot / "heartbeat").write_text("0")
        env = {"MIMIR_HEAVY_LOCK_FILE": base}
        relay = LeaseWaitRelay(env)
        relay.cap = 0.3 if capped else 3
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(helper_path()), sys.executable, "-c", "pass",
                env={**os.environ, **env, **relay.environment},
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            compute = ProcessCompute(proc)
            await eventually(lambda: relay.snapshot()[0] > 0)
            waiter = asyncio.create_task(wait_for_builder(compute, None, 0.2, relay))
            if capped:
                # A loose hang guard is still far below the simulated uncapped
                # wait. This test must distinguish removal of the cap.
                result = await asyncio.wait_for(waiter, 2)
                assert result.timed_out
                assert compute.cancelled
                assert result.deadline_extension_s == relay.cap
            else:
                await eventually(lambda: relay.snapshot()[0] > 0.5 or waiter.done())
                assert not waiter.done(), "original deadline killed a queued builder"
                for suffix in (".lease", ".lease2"):
                    slot = Path(base + suffix)
                    (slot / "owner").unlink()
                    (slot / "heartbeat").unlink()
                    slot.rmdir()
                result = await asyncio.wait_for(waiter, 3)
                assert not result.timed_out
                assert not compute.cancelled
                assert result.exit_code == 0
                assert result.lease_wait_s >= 0.5
                assert result.deadline_extension_s >= 0.5
        finally:
            if proc.returncode is None:
                proc.terminate()
                await asyncio.wait_for(proc.communicate(), 3)
            await asyncio.to_thread(relay.close)
    asyncio.run(scenario())


@pytest.mark.parametrize("report", [
    {"token": "forged", "tier": "large"}, {"token": None, "tier": "large"}, {},
    {"token": "valid", "tier": "large", "wait_s": 999999},
    {"token": "valid", "tier": "forged"},
])
def test_forged_wait_reports_grant_no_credit(tmp_path, report):
    relay = LeaseWaitRelay({"MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy")})
    try:
        if report.get("token") == "valid":
            report = {**report, "token": relay.token}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(relay.path))
            client.sendall(json.dumps(report).encode() + b"\n")
            assert client.recv(16) == b""
        assert relay.snapshot() == (0, 0)
        assert not Path(relay.base + ".queue").exists()
    finally:
        relay.close()


def test_credit_counts_union_of_waits_not_parallel_sum(tmp_path):
    relay = LeaseWaitRelay({"MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy")})
    try:
        relay._queue(True)
        relay._queue(True)
        relay._queue(False)
        assert relay._waiting == 1
        relay._queue(False)
        assert relay._waiting == 0
        frozen = relay.snapshot()
        assert relay.snapshot() == frozen
    finally:
        relay.close()


@pytest.mark.parametrize("configured,expected", [("60", 60), ("14400", 14400), ("86400", 14400)])
def test_extension_cap_never_exceeds_lock_timeout(configured, expected):
    assert extension_cap({"MIMIR_HEAVY_LOCK_FILE": "/lease", "MIMIR_HEAVY_LOCK_TIMEOUT": configured}) == expected
    assert extension_cap({}) == 0


def test_relay_environment_survives_closed_builder_environment(tmp_path):
    from mimir_cultivate.worklink.daemon_sessions import session_environment
    relay = LeaseWaitRelay({"MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy")})
    try:
        env = session_environment(relay.environment, home=tmp_path, git_settings=(), credentials={})
        assert env["WORKLINK_LEASE_SOCKET"] == str(relay.path)
        assert env["WORKLINK_LEASE_AUTH"] == relay.token
    finally:
        relay.close()


def test_lost_relay_kills_the_leased_command(tmp_path):
    async def scenario():
        env = {"MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy")}
        relay = LeaseWaitRelay(env)
        marker = tmp_path / "started"
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(helper_path()), sys.executable, "-c",
            f"from pathlib import Path; import signal; Path({str(marker)!r}).touch(); signal.pause()",
            env={**os.environ, **env, **relay.environment},
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await eventually(marker.exists)
            await asyncio.to_thread(relay.close)
            await asyncio.wait_for(proc.communicate(), 3)
            assert proc.returncode != 0
        finally:
            if proc.returncode is None:
                proc.terminate()
                await asyncio.wait_for(proc.communicate(), 3)
    asyncio.run(scenario())


def test_running_builder_gets_no_unearned_deadline_extension(tmp_path):
    async def scenario():
        env = {"MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy")}
        relay = LeaseWaitRelay(env)
        marker = tmp_path / "running"
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(helper_path()), sys.executable, "-c",
            f"from pathlib import Path; import signal; Path({str(marker)!r}).touch(); signal.pause()",
            env={**os.environ, **env, **relay.environment},
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await eventually(marker.exists)
            compute = ProcessCompute(proc)
            result = await asyncio.wait_for(wait_for_builder(compute, None, 0.3, relay), 3)
            assert result.timed_out and compute.cancelled
            assert result.lease_wait_s < result.builder_elapsed_s * 0.5
            assert result.deadline_extension_s < 0.3
        finally:
            if proc.returncode is None:
                proc.terminate()
                await asyncio.wait_for(proc.communicate(), 3)
            await asyncio.to_thread(relay.close)
    asyncio.run(scenario())
