"""Installed async heavy jobs, exercised without a coding CLI or deployment."""
from __future__ import annotations

import asyncio
import inspect
import json
import os
from pathlib import Path
import shlex
import sys
import textwrap
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mimir.heavy_run import helper_path
from mimir_cultivate.worklink import jobs
from mimir_cultivate.worklink.backends.base import SessionRequest
from mimir_cultivate.worklink.compute import ComputeResult, LaunchHandle, WorkSpec
from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
from mimir_cultivate.worklink.daemon_processes import _alive, _signal
from mimir_cultivate.worklink.daemon_service import read_json
from mimir_cultivate.worklink.daemon_sessions import SessionController, session_permissions
from mimir_cultivate.worklink.lease_wait import BuilderLeaseCompute, LeaseRelay
from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux Worklink executor jobs")


async def until(predicate):
    async with asyncio.timeout(8):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest.fixture
def lease_env(tmp_path):
    return {
        "MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy"),
        "MIMIR_HEAVY_LOCK_TIMEOUT": "10",
        "MIMIR_HEAVY_HEARTBEAT_SECONDS": "0.02",
    }


async def holder(environment):
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-I", str(helper_path()), sys.executable, "-I", "-c",
        "import sys; sys.stdin.read()", env={**environment, "MIMIR_HEAVY_REPORT_FD": "1"},
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
    )
    assert await asyncio.wait_for(process.stdout.readline(), 8) == b"queued\n"
    assert await asyncio.wait_for(process.stdout.readline(), 8) == b"acquired\n"
    return process


@pytest.fixture
async def service(tmp_path):
    """Only this fixture's own service/processes, never ambient process state."""
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    references = []

    async def start(*, policy=None, environment=None, deadline_s=60, session=None):
        sid = session or f"ses_test{len(references)}"
        env = {"PATH": os.environ["PATH"], "HOME": str(home), **(environment or {})}
        values = await jobs.start_service(
            tmp_path / "controller.json", sid, policy=policy or [f"{sys.executable} *"],
            checkout=tmp_path, environment=env, deadline_ms=int((time.time() + deadline_s) * 1000),
        )
        reference = read_json(jobs.reference_path(tmp_path / "controller.json", sid),
                              uid=os.geteuid(), mode=0o600)
        references.append((sid, reference))
        return sid, reference, values

    yield start
    for sid, reference in references:
        witness = [reference["pid"], reference["ticks"]]
        if _alive(witness):
            try:
                await jobs.session_jobs(tmp_path / "controller.json", sid, close=True)
            finally:
                if _alive(witness):
                    _signal(witness, 15)
                    await until(lambda: not _alive(witness))


async def start_job(reference, tmp_path, code="print('complete')", **extra):
    return await jobs.rpc(reference, "start", argv=[sys.executable, "-c", code],
                          cwd=str(tmp_path), tier="medium", **extra)


async def done(reference, identifier):
    async with asyncio.timeout(8):
        while True:
            result = await jobs.rpc(reference, "wait", id=identifier, timeout=0.05)
            if result["state"] == "done":
                return result


async def test_worklink_job_refuses_disallowed_payload(service, tmp_path):
    _, reference, _ = await service(policy=["git *", "worklink-job *"])
    with pytest.raises(ValueError, match="refused"):
        await start_job(reference, tmp_path)
    with pytest.raises(ValueError, match="refused"):
        await jobs.rpc(reference, "start", argv=["worklink-job", "start", "--", "arbitrary"],
                       cwd=str(tmp_path), tier="medium")
    assert list((tmp_path / "home/ses_test0.jobs").glob("*.log")) == []
    assert (await jobs.rpc(reference, "active")) == {"active": False}


async def test_worklink_job_original_policy_not_client_environment(service, tmp_path):
    _, reference, values = await service(policy=["git status"])
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-I", "-m", "mimir_cultivate.worklink.jobs", "start", "--",
        sys.executable, "-c", "print('not allowed')", cwd=tmp_path,
        env={**os.environ, **values, "WORKLINK_JOB_ALLOWLIST": "*"},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 1
    assert b"refused" in stderr and not stdout
    assert not (await jobs.rpc(reference, "active"))["active"]


async def test_worklink_job_cli_has_no_service_or_policy_entrypoint(service, tmp_path):
    _, _, values = await service()
    # A valid private configuration, not empty stdin: exposing the internal
    # service entry point must fail this test rather than fail by JSON accident.
    config = dict(policy=["*"], checkout=str(tmp_path), logs=str(tmp_path), environment={},
                  reference={"auth": "forged", "socket": "@wl-forged-" + os.urandom(8).hex(),
                             "finished": str(tmp_path / "forged-closed.json")}, deadline_ms=0)
    for arguments in (["--serve"], ["start", "--policy", "*", "--", "bad"]):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-I", "-m", "mimir_cultivate.worklink.jobs", *arguments,
            env={**os.environ, **values}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await process.communicate(json.dumps(config).encode() + b"\n")
        assert process.returncode != 0


async def test_worklink_job_cli_detaches_and_supports_wait_log_cancel(service, tmp_path):
    _, _, values = await service()
    async def cli(*arguments):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-I", "-m", "mimir_cultivate.worklink.jobs", *arguments,
            cwd=tmp_path, env={**os.environ, **values},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 4)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)
    marker = tmp_path / "started"
    job = await cli("start", "--tier", "medium", "--", sys.executable, "-c",
                    f"import time; from pathlib import Path; print('started', flush=True); "
                    f"Path({str(marker)!r}).touch(); time.sleep(60)")
    assert job["state"] == "queued" and job["id"]
    await until(marker.exists)
    assert (await cli("wait", job["id"], "--timeout", "0.01"))["state"] == "running"
    assert (await cli("log", job["id"], "--tail", "1"))["log"] == "started"
    assert (await cli("cancel", job["id"]))["state"] == "done"
    assert (await cli("wait", job["id"], "--timeout", "0"))["exit_code"] == 130


async def test_worklink_job_command_is_argv_not_shell(service, tmp_path):
    _, reference, _ = await service()
    command = [sys.executable, "-c", "import sys; print(sys.argv[1])", "$(touch injected); echo pwned"]
    result = await jobs.rpc(reference, "start", argv=command, cwd=str(tmp_path), tier="medium")
    result = await done(reference, result["id"])
    assert result["exit_code"] == 0
    assert "$(touch injected); echo pwned" in result["log"]
    assert not (tmp_path / "injected").exists()


async def test_worklink_job_foreign_session_is_invisible(service, tmp_path):
    _, first, _ = await service()
    _, second, _ = await service()
    job = await start_job(first, tmp_path)
    for operation in ("wait", "log", "cancel"):
        with pytest.raises(ValueError, match="refused"):
            await jobs.rpc(second, operation, id=job["id"], timeout=0)
        with pytest.raises(ValueError, match="refused"):
            await jobs.rpc({**first, "auth": second["auth"]}, operation, id=job["id"], timeout=0)
    assert (await done(first, job["id"]))["exit_code"] == 0


async def test_worklink_job_tier_directory_and_argument_guards(service, tmp_path):
    _, reference, _ = await service()
    base = dict(argv=[sys.executable, "-c", "print('valid')"], cwd=str(tmp_path), tier="medium")
    for changes in ({"tier": "invalid"}, {"cwd": str(tmp_path.parent)},
                    {"argv": []}, {"argv": ["bad\0command"]}, {"argv": "python -c bad"},
                    {"argv": [True]}, {"argv": ["x" * 32769]}):
        with pytest.raises(ValueError, match="refused"):
            await jobs.rpc(reference, "start", **{**base, **changes})
    assert not (await jobs.rpc(reference, "active"))["active"]


async def test_worklink_job_queued_wait_is_bounded_then_credit_in_evidence(
    service, tmp_path, lease_env, monkeypatch,
):
    """The actual BuilderLeaseCompute result is the evidence source, not a fake timer."""
    monkeypatch.setattr("mimir_cultivate.worklink.compute._heavy_lock_environment", lambda: lease_env)
    occupying = await holder(lease_env)

    class Builder(BuilderLeaseCompute):
        async def _launch(self, spec):
            self.sid, self.reference, _ = await service(environment={**lease_env, **spec.env})
            self.job = await start_job(self.reference, tmp_path)
            return LaunchHandle("test", "builder")

        async def _wait(self, handle, timeout_s):
            result = await done(self.reference, self.job["id"])
            return ComputeResult(result["exit_code"], result["log"], "")

        async def cancel(self, handle):
            await jobs.session_jobs(tmp_path / "controller.json", self.sid, close=True)

        async def _cleanup(self, handle):
            await self.cancel(handle)

    builder = Builder()
    spec = WorkSpec(2243, 1, "", "main", "test", "", None, "", "test", 3,
                    local_checkout=tmp_path, builder_lease_credit=True)
    handle = await builder.launch(spec)
    relay, _ = builder._lease_relays[handle.identifier]
    waiting = asyncio.create_task(builder.wait(handle, 3))
    try:
        await until(lambda: relay.queued_since is not None)
        result = await jobs.rpc(builder.reference, "wait", id=builder.job["id"], timeout=0.03)
        assert result["state"] == "queued" and result["exit_code"] is None
        # Stay queued past the uncredited builder deadline.
        await asyncio.sleep(3.1)
        assert not waiting.done()
        occupying.stdin.close()
        await occupying.wait()
        evidence = await asyncio.wait_for(waiting, 8)
        assert evidence.exit_code == 0 and not evidence.timed_out
        assert "complete" in evidence.stdout
        assert evidence.lease_wait_s >= 3.1
        assert evidence.lease_extension_s == evidence.lease_wait_s
    finally:
        if occupying.returncode is None:
            occupying.terminate()
            await occupying.wait()
        await builder.cleanup(handle)
        await asyncio.gather(waiting, return_exceptions=True)


async def test_worklink_job_cancel_queued_never_executes(service, tmp_path, lease_env):
    relay = LeaseRelay(lease_env)
    await relay.start()
    occupying = await holder(lease_env)
    try:
        _, reference, _ = await service(environment={**lease_env, **relay.env})
        marker = tmp_path / "must-not-exist"
        job = await start_job(reference, tmp_path, f"from pathlib import Path; Path({str(marker)!r}).touch()")
        await until(lambda: relay.queued_since is not None)
        assert (await jobs.rpc(reference, "wait", id=job["id"], timeout=0.01))["state"] == "queued"
        assert (await jobs.rpc(reference, "cancel", id=job["id"]))["state"] == "done"
        await until(lambda: relay.queued_since is None)
        occupying.stdin.close()
        await occupying.wait()
        assert (await done(reference, job["id"]))["exit_code"] == 130
        assert not marker.exists()
    finally:
        if occupying.returncode is None:
            occupying.terminate()
            await occupying.wait()
        await relay.close()


async def test_worklink_job_wait_never_exceeds_server_bound(tmp_path, monkeypatch):
    # Exercise the wait branch directly: changing bounded_timeout's cap makes
    # this wait exceed the loose outer hang guard, not just a numeric assertion.
    monkeypatch.setattr(jobs, "MAX_WAIT_S", 0.03)
    service = jobs.JobService(policy=[], checkout=tmp_path, logs=tmp_path, environment={}, token="own")
    job = jobs.Job("known", [], tmp_path, "medium", state="running")
    service.jobs[job.identifier] = job
    result = await asyncio.wait_for(service.dispatch(
        {"auth": "own", "op": "wait", "id": "known", "timeout": 999999}), 0.5)
    assert result["state"] == "running" and not job.done.is_set()
    assert jobs.MAX_WAIT_S < 1200
    for invalid in (-1, float("inf"), float("nan"), "1", True):
        with pytest.raises(ValueError):
            await service.dispatch({"auth": "own", "op": "wait", "id": "known", "timeout": invalid})


async def check_admission_guard(tmp_path, monkeypatch, guard):
    service = jobs.JobService(policy=[f"{sys.executable} *"], checkout=tmp_path, logs=tmp_path,
                              environment={}, token="own")
    monkeypatch.setattr(service, "run", AsyncMock())
    argv, cwd, tier = [sys.executable, "-c", "print('ok')"], str(tmp_path), "medium"
    if guard == "allowlist":
        service.policy = ["git *"]
    elif guard == "recursive":
        service.policy, argv = ["worklink-job *"], ["worklink-job", "start", "--", "bad"]
    elif guard == "tier":
        tier = "unknown"
    elif guard == "directory":
        cwd = str(tmp_path.parent)
    elif guard == "arguments":
        argv = [sys.executable, "x" * 32769]
    elif guard == "closed":
        service.stopping = True
    elif guard == "cleanup":
        service.cleanup_error = True
    elif guard == "capacity":
        service.jobs = {str(i): None for i in range(jobs.MAX_JOBS)}
    elif guard == "auth":
        with pytest.raises(ValueError):
            await service.dispatch({"auth": "foreign", "op": "active"})
        return
    try:
        with pytest.raises(ValueError):
            service.start(argv, cwd, tier)
    finally:
        # Mutants that admit a command use the harmless coroutine above.
        tasks = [job.task for job in service.jobs.values() if job is not None]
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("guard", [
    "allowlist", "recursive", "tier", "directory", "arguments", "closed", "cleanup", "capacity", "auth",
])
async def test_worklink_job_admission_security_guards(tmp_path, monkeypatch, guard):
    await check_admission_guard(tmp_path, monkeypatch, guard)


def mutant(function, before, after):
    """Compile one changed production guard without writing another checkout.

    The exact same acceptance assertion must fail under the permissive mutation.
    monkeypatch restores the original function after each test.
    """
    source = textwrap.dedent(inspect.getsource(function))
    assert source.count(before) == 1, before
    namespace = dict(function.__globals__)
    exec(compile(source.replace(before, after), "<worklink-job-security-mutant>", "exec"), namespace)
    return namespace[function.__name__]


@pytest.mark.parametrize("guard,target,before,after", [
    ("allowlist", "start", "if not command_allowed(self.policy, argv):", "if False:"),
    ("recursive", "command_allowed", 'if rule.startswith("worklink-job"):', "if False:"),
    ("tier", "start", 'if tier not in {"medium", "large"}:', "if False:"),
    ("directory", "start",
     "if not isinstance(cwd, str) or not Path(cwd).resolve().is_relative_to(self.checkout):", "if False:"),
    ("arguments", "start", "or sum(len(arg.encode()) for arg in argv) > 32768", ""),
    ("closed", "start", "self.stopping or self.cleanup_error", "self.cleanup_error"),
    ("cleanup", "start", "or self.cleanup_error", ""),
    ("capacity", "start", "or len(self.jobs) >= MAX_JOBS", ""),
    ("auth", "dispatch", 'or not hmac.compare_digest(request["auth"], self.token)', ""),
])
async def test_worklink_job_admission_mutations_killed(tmp_path, monkeypatch, guard, target, before, after):
    owner = jobs if target == "command_allowed" else jobs.JobService
    monkeypatch.setattr(owner, target, mutant(getattr(owner, target), before, after))
    # pytest's "DID NOT RAISE" failure is the refusal acceptance test going red.
    with pytest.raises(pytest.fail.Exception):
        await check_admission_guard(tmp_path, monkeypatch, guard)


async def check_cleanup_guard(tmp_path, monkeypatch):
    service = jobs.JobService(policy=[], checkout=tmp_path, logs=tmp_path,
                              environment={"WORKLINK_LEASE_SOCKET": "test", "WORKLINK_LEASE_AUTH": "own"},
                              token="own")
    reader = asyncio.StreamReader()
    reader.feed_data(b"acquired\n")
    reader.feed_eof()
    writer = SimpleNamespace(write=lambda _: None, drain=AsyncMock(), close=lambda: None,
                             wait_closed=AsyncMock())
    output = asyncio.StreamReader()
    output.feed_eof()
    process = SimpleNamespace(returncode=0, stdout=output, wait=AsyncMock(return_value=0))
    monkeypatch.setattr(asyncio, "open_unix_connection", AsyncMock(return_value=(reader, writer)))
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    monkeypatch.setattr(jobs, "snapshot_descendants", lambda _: [[123, 456]])
    monkeypatch.setattr(jobs, "terminate_descendants", AsyncMock(return_value=[[123, 456]]))
    monkeypatch.setattr(os, "waitpid", lambda *_: (0, 0))
    job = jobs.Job("unit", [sys.executable], tmp_path, "medium")
    await service.run(job)
    assert service.cleanup_error
    assert writer in service.holders
    writer.wait_closed.assert_not_awaited()  # Never release on an unverified tree.
    assert (await service.dispatch({"auth": "own", "op": "active"}))["active"]
    with pytest.raises(RuntimeError, match="unverified"):
        await service.close_jobs()


async def test_worklink_job_unverified_cleanup_retains_lease(tmp_path, monkeypatch):
    await check_cleanup_guard(tmp_path, monkeypatch)


async def test_worklink_job_already_queued_after_cleanup_failure_and_mutation(tmp_path, monkeypatch):
    service = jobs.JobService(policy=[], checkout=tmp_path, logs=tmp_path, environment={}, token="own")
    service.cleanup_error = True
    output = asyncio.StreamReader()
    output.feed_eof()
    spawn = AsyncMock(return_value=SimpleNamespace(returncode=0, stdout=output, wait=AsyncMock(return_value=0)))
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(jobs, "snapshot_descendants", lambda _: [])
    monkeypatch.setattr(jobs, "terminate_descendants", AsyncMock(return_value=[]))
    monkeypatch.setattr(os, "waitpid", lambda *_: (0, 0))
    async def check():
        job = jobs.Job("unit", [sys.executable], tmp_path, "medium")
        await service.run(job)
        assert job.exit_code == 1
        spawn.assert_not_awaited()
    await check()
    monkeypatch.setattr(jobs.JobService, "run", mutant(
        jobs.JobService.run, "if self.cleanup_error:", "if False:"))
    with pytest.raises(AssertionError):
        await check()


@pytest.mark.parametrize("method", ["run", "close_jobs"])
async def test_worklink_job_cleanup_mutations_killed(tmp_path, monkeypatch, method):
    original = getattr(jobs.JobService, method)
    monkeypatch.setattr(jobs.JobService, method, mutant(original, "if survivors:", "if False:"))
    with pytest.raises((AssertionError, pytest.fail.Exception)):
        await check_cleanup_guard(tmp_path, monkeypatch)


@pytest.mark.parametrize("target,before,after", [
    ("append", "del job.log[:-LOG_BYTES]", "pass"),
    ("result", "job.log[-TAIL_BYTES:]", "job.log"),
])
def test_worklink_job_log_bound_mutations_killed(tmp_path, monkeypatch, target, before, after):
    def check():
        service = jobs.JobService(policy=[], checkout=tmp_path, logs=tmp_path, environment={}, token="own")
        job = jobs.Job("unit", [], tmp_path, "medium")
        service.append(job, b"x" * (jobs.LOG_BYTES * 2))
        assert len(job.log) <= jobs.LOG_BYTES
        assert (tmp_path / "unit.log").stat().st_size <= jobs.LOG_BYTES
        assert len(service.result(job)["log"].encode()) <= jobs.TAIL_BYTES
    check()
    monkeypatch.setattr(jobs.JobService, target, mutant(getattr(jobs.JobService, target), before, after))
    with pytest.raises(AssertionError):
        check()


async def test_worklink_job_wait_bound_mutation_killed(tmp_path, monkeypatch):
    await test_worklink_job_wait_never_exceeds_server_bound(tmp_path, monkeypatch)
    monkeypatch.setattr(jobs, "bounded_timeout", mutant(
        jobs.bounded_timeout, "min(float(value), MAX_WAIT_S)", "float(value)"))
    with pytest.raises(TimeoutError):
        await test_worklink_job_wait_never_exceeds_server_bound(tmp_path, monkeypatch)


def test_worklink_job_log_symlink_refused_and_mutation_killed(tmp_path, monkeypatch):
    outside = tmp_path / "private-output"
    outside.write_text("preserve")
    (tmp_path / "unit.log").symlink_to(outside)
    service = jobs.JobService(policy=[], checkout=tmp_path, logs=tmp_path, environment={}, token="own")
    job = jobs.Job("unit", [], tmp_path, "medium")
    def check():
        with pytest.raises(OSError):
            service.append(job, b"overwrite")
        assert outside.read_text() == "preserve"
    check()
    monkeypatch.setattr(jobs.JobService, "append", mutant(
        jobs.JobService.append, " | os.O_NOFOLLOW", ""))
    with pytest.raises(pytest.fail.Exception):
        check()


async def test_worklink_job_subreaper_failure_and_mutation_killed(tmp_path, monkeypatch):
    import ctypes
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **kw: SimpleNamespace(prctl=lambda *a: -1))
    config = dict(policy=[], checkout=str(tmp_path), logs=str(tmp_path), environment={},
                  reference={"auth": "own", "socket": "@unit", "finished": str(tmp_path / "closed.json")},
                  deadline_ms=0)
    async def check():
        with pytest.raises(RuntimeError, match="subreaper"):
            await jobs.serve(config)
    await check()
    # The mutant must not alter the test process's signal handlers or bind a
    # real socket; it runs no jobs and reaches its immediate expired deadline.
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", lambda *a: None)
    monkeypatch.setattr(asyncio, "start_unix_server", AsyncMock(return_value=SimpleNamespace(
        close=lambda: None, wait_closed=AsyncMock())))
    monkeypatch.setattr(jobs, "serve", mutant(
        jobs.serve, "if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:", "if False:"))
    with pytest.raises(pytest.fail.Exception):
        await check()


@pytest.mark.parametrize("witness", ["missing", "malformed"])
async def test_worklink_job_dead_service_requires_cleanup_witness_and_mutation(tmp_path, monkeypatch, witness):
    from mimir_cultivate.worklink import daemon_service
    controller = tmp_path / "controller.json"
    reference_path = jobs.reference_path(controller, "ses_unit")
    reference = {"pid": 123, "ticks": 456, "finished": str(tmp_path / "closed.json")}
    def read(path, **kwargs):
        if path == reference_path:
            return reference
        if witness == "missing":
            raise FileNotFoundError
        return {"closed": False}
    monkeypatch.setattr(daemon_service, "read_json", read)
    monkeypatch.setattr(jobs, "_alive", lambda _: False)
    async def check():
        with pytest.raises(RuntimeError, match="cleanup unverified"):
            await jobs.session_jobs(controller, "ses_unit")
    await check()
    before = ('raise RuntimeError("session job cleanup unverified") from None' if witness == "missing"
              else 'if ended != {"closed": True}:')
    after = "return False" if witness == "missing" else "if False:"
    monkeypatch.setattr(jobs, "session_jobs", mutant(jobs.session_jobs, before, after))
    with pytest.raises(pytest.fail.Exception):
        await check()


async def test_worklink_job_log_is_private_bounded_and_tail_limited(service, tmp_path):
    sid, reference, _ = await service()
    result = await done(reference, (await start_job(reference, tmp_path,
        "print('x' * 600000); print('last line')"))["id"])
    assert result["exit_code"] == 0
    assert len(result["log"].encode()) <= jobs.TAIL_BYTES
    log = await jobs.rpc(reference, "log", id=result["id"], tail=1)
    assert log["log"] == "last line"
    assert (await jobs.rpc(reference, "log", id=result["id"], tail=0))["log"] == ""
    path = tmp_path / "home" / (sid + ".jobs") / (result["id"] + ".log")
    assert path.stat().st_size <= jobs.LOG_BYTES
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    for invalid in (-1, 1001, True, "10"):
        with pytest.raises(ValueError):
            await jobs.rpc(reference, "log", id=result["id"], tail=invalid)


async def test_worklink_job_nested_installed_heavy_helper_does_not_deadlock(
    service, tmp_path, lease_env,
):
    relay = LeaseRelay(lease_env)
    await relay.start()
    # This is the exact .sh asset installed by executor.sh, not modified checkout
    # code or a helper invented by the test.
    nested = Path(__file__).resolve().parents[3] / "scripts/heavy_run.sh"
    try:
        _, reference, _ = await service(policy=["sh *"], environment={**lease_env, **relay.env})
        job = await jobs.rpc(reference, "start", argv=[
            "sh", str(nested), sys.executable, "-c",
            "import os; print(os.environ['MIMIR_HEAVY_LOCK_HELD'])",
        ], cwd=str(tmp_path), tier="large")
        result = await done(reference, job["id"])
        assert result["exit_code"] == 0 and result["log"] == "1"
    finally:
        await relay.close()


@pytest.mark.parametrize("ending", ["cancel", "session", "deadline"])
async def test_worklink_job_ending_kills_escaped_descendant_and_releases_lease(
    service, tmp_path, lease_env, ending,
):
    relay = LeaseRelay(lease_env)
    await relay.start()
    child = tmp_path / "child.json"
    sid, reference, _ = await service(environment={**lease_env, **relay.env},
                                      deadline_s=2 if ending == "deadline" else 60)
    # A double-forked setsid child exercises the service's subreaper, not just
    # the command's process group.
    code = (
        "import os,time,json; "
        f"p={str(child)!r}; "
        "\nif os.fork() == 0:\n"
        " os.setsid()\n"
        " if os.fork() == 0:\n"
        "  fields=open('/proc/self/stat').read().split(') ')[1].split()\n"
        "  open(p,'w').write(json.dumps([os.getpid(),int(fields[19])]))\n"
        "  time.sleep(60)\n"
        " os._exit(0)\n"
        "time.sleep(60)\n"
    )
    witness = None
    try:
        job = await start_job(reference, tmp_path, code)
        await until(lambda: child.exists() and child.stat().st_size > 0)
        witness = json.loads(child.read_text())
        assert _alive(witness)
        if ending == "cancel":
            result = await jobs.rpc(reference, "cancel", id=job["id"])
            assert result["state"] == "done" and result["exit_code"] == 130
        elif ending == "session":
            await jobs.session_jobs(tmp_path / "controller.json", sid, close=True)
        else:
            await until(lambda: not _alive([reference["pid"], reference["ticks"]]))
        assert not _alive(witness)
        # A fresh large holder proves BOTH slots were freed, not just telemetry.
        next_holder = await asyncio.wait_for(holder(lease_env), 8)
        next_holder.stdin.close()
        await next_holder.wait()
    finally:
        if witness is not None and _alive(witness):
            _signal(witness, 9)
        await relay.close()


async def test_worklink_job_missing_or_refused_relay_never_executes(service, tmp_path, lease_env):
    for environment in (lease_env, {**lease_env, "WORKLINK_LEASE_SOCKET": str(tmp_path / "absent"),
                                    "WORKLINK_LEASE_AUTH": "bad"}):
        _, reference, _ = await service(environment=environment)
        result = await done(reference, (await start_job(
            reference, tmp_path, "print('must not run')"))["id"])
        assert result["exit_code"] == 1
        assert "must not run" not in result["log"]
    relay = LeaseRelay(lease_env)
    await relay.start()
    try:
        _, reference, _ = await service(environment={**lease_env, **relay.env, "WORKLINK_LEASE_AUTH": "bad"})
        result = await done(reference, (await start_job(reference, tmp_path, "print('must not run')"))["id"])
        assert result["exit_code"] == 1 and "must not run" not in result["log"]
        assert relay.wait_s == 0
    finally:
        await relay.close()


async def test_worklink_job_controller_quiescence_and_cleanup(tmp_path):
    home = tmp_path / "private"
    home.mkdir(mode=0o700)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await SessionController.create(
            client, tmp_path / "controller.json",
            request=SessionRequest("leaf", {"providerID": "unit", "id": "unit"}, "build",
                                   (f"{sys.executable} *",)),
            issue_id=2243, attempt=1, generation="unit", checkout=tmp_path,
            environment={"PATH": os.environ["PATH"], "HOME": str(home)}, timeout_s=60,
        )
        sid = controller.record.opencode_session_id
        reference = read_json(jobs.reference_path(controller.path, sid), uid=os.geteuid(), mode=0o600)
        marker = tmp_path / "started"
        try:
            job = await start_job(reference, tmp_path, f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(60)")
            await until(marker.exists)
            daemon.finish(sid)
            assert (await controller.observe()).execution_state == "started"
            recovered = SessionController.recover(client, controller.path, generation="unit", checkout=tmp_path)
            with pytest.raises(RuntimeError, match="retained_running_jobs"):
                await recovered.quiescence()
            assert await recovered.cancel(timeout_s=2)
            assert not _alive([reference["pid"], reference["ticks"]])
            assert (await recovered.observe()).execution_state == "succeeded"
            await recovered.release()
            assert not jobs.reference_path(controller.path, sid).exists()
            assert (await recovered.observe()).execution_state == "succeeded"
        finally:
            await jobs.session_jobs(controller.path, sid, close=True)


async def test_worklink_job_retained_quiescence_mutation_killed(tmp_path, monkeypatch):
    monkeypatch.setattr(SessionController, "quiescence", mutant(
        SessionController.quiescence,
        "if await session_jobs(self.path, self.record.opencode_session_id):", "if False:"))
    with pytest.raises(pytest.fail.Exception):
        await test_worklink_job_controller_quiescence_and_cleanup(tmp_path)


def test_worklink_job_builtin_permission_does_not_mutate_payload_policy():
    request = SessionRequest("leaf", {"providerID": "unit", "id": "unit"}, "build", ("git *",))
    assert {"action": "shell", "resource": "worklink-job *", "effect": "allow"} in session_permissions(
        request, jobs_enabled=True)
    assert request.shell_allowlist == ("git *",)


@pytest.mark.parametrize("pattern", ["git *", "./tools/verify --lane ratchet", "uv run *", "x?z *"])
@pytest.mark.parametrize("argv", [
    ["git"], ["git", "status"], ["git", "log", "quoted text"],
    ["./tools/verify", "--lane", "ratchet"], ["uv", "run", "pytest", "-k", "a or b"], ["xyz", "foo"],
])
def test_worklink_job_matcher_agrees_with_backend(pattern, argv):
    from mimir_cultivate.worklink.backends.registry import _opencode_pattern_matches
    assert jobs.command_allowed([pattern], argv) == _opencode_pattern_matches(pattern, shlex.join(argv))


def test_worklink_job_install_and_order_guidance():
    root = Path(__file__).resolve().parents[1] / "mimir_cultivate"
    wrapper = (root / "docker/pytest_guard/worklink-job").read_text()
    assert "python -I -m mimir_cultivate.worklink.jobs" in wrapper
    install = (root / "docker/executor.sh").read_text()
    assert '"worklink-job"' in install
    assert install.count("install_pytest_guard\n") == 2  # wheel and source installs
    assert "chown -R root:root /opt/mimir-worklink" in install
    template = (root / "prompt_templates/worklink-order.md").read_text()
    for text in ("worklink-job start", "worklink-job wait", "queued", "not a failure",
                 "MIMIR_HEAVY_LOCK_HELD", "Focused local tests still run directly"):
        assert text in template
