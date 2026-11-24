"""Controller-observed gate lease budgeting, using real protocol participants."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from pathlib import Path
import shlex
import signal
import sys

import pytest

pytest.importorskip("mimir_cultivate")
from mimir_cultivate.worklink import compute as cp, evidence as ev
from mimir_cultivate.worklink.compute import ComputeResult, LaunchHandle, WorkSpec


class ProcessCompute:
    """Real commands and deadlines; no privileged executor needed."""

    def __init__(self, checkout):
        self.checkout = checkout
        self.processes = []
        self.specs = []

    async def _launch_contained(self, spec, command):
        self.specs.append(spec)
        process = await asyncio.create_subprocess_exec(
            *command, cwd=self.checkout, env={"PATH": os.defpath},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self.processes.append(process)
        return LaunchHandle("local_subprocess", str(len(self.processes) - 1))

    async def wait(self, handle, timeout_s):
        process = self.processes[int(handle.identifier)]
        task = asyncio.create_task(process.communicate())
        timed_out = False
        try:
            output = await asyncio.wait_for(asyncio.shield(task), timeout_s)
        except TimeoutError:
            timed_out = True
            await self.cancel(handle)
            output = await task
        return ComputeResult(
            process.returncode, output[0].decode(), output[1].decode(), timed_out=timed_out,
        )

    async def cancel(self, handle):
        process = self.processes[int(handle.identifier)]
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
        await process.wait()

    async def cleanup(self, handle):
        await self.cancel(handle)


@pytest.fixture
def lease(tmp_path, monkeypatch):
    # Never participate in (or assert on) the host's ambient heavy-run lease.
    environment = {
        "MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "heavy"),
        "MIMIR_HEAVY_LOCK_TIMEOUT": "5",
    }
    monkeypatch.setattr(cp, "_heavy_lock_environment", lambda: dict(environment))
    return environment


def spec(tmp_path, budget=0.4):
    return WorkSpec(
        2202, 1, "repo", "main", "branch", "", None, "", "opencode", budget,
        local_checkout=tmp_path,
    )


def command(source):
    return shlex.join(["sh", "scripts/heavy_run.sh", sys.executable, "-c", source])


async def queued(lease):
    queue = Path(lease["MIMIR_HEAVY_LOCK_FILE"] + ".queue")
    async with asyncio.timeout(5):
        while not queue.exists() or not any(p.name.isdigit() for p in queue.iterdir()):
            await asyncio.sleep(0.01)


def assert_released(lease):
    base = lease["MIMIR_HEAVY_LOCK_FILE"]
    assert not Path(base + ".lease").exists()
    assert not Path(base + ".lease2").exists()
    queue = Path(base + ".queue")
    assert not queue.exists() or not list(queue.iterdir())


@pytest.mark.asyncio
async def test_gate_uses_installed_core_helper_outside_checkout(tmp_path, monkeypatch, lease):
    from mimir import heavy_run

    installed = tmp_path / "venv/lib/python/site-packages/mimir/heavy_run"
    installed.mkdir(parents=True)
    helper = installed / "heavy_run.py"
    helper.write_bytes(heavy_run.helper_path().read_bytes())
    anchors = []

    def package_files(anchor):
        anchors.append(anchor)
        return installed

    monkeypatch.setattr(heavy_run.resources, "files", package_files)
    monkeypatch.setattr(ev, "__file__", str(tmp_path / "unrelated/plugin/evidence.py"))
    original = asyncio.create_subprocess_exec
    launched = []

    async def spawn(*args, **kwargs):
        launched.append(args)
        return await original(*args, **kwargs)

    monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", spawn)
    compute = ProcessCompute(tmp_path)
    result = await ev._run_compute_gate(
        command("import os; assert os.environ['MIMIR_HEAVY_LOCK_HELD'] == '1'; print('installed')"),
        checkout=tmp_path, work_spec=spec(tmp_path, 5), compute=compute,
    )
    assert result.exit_code == 0
    assert result.stdout == "installed\n"
    assert anchors == ["mimir.heavy_run"]
    assert launched[0][2] == str(helper)
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["file", "package"])
async def test_gate_missing_packaged_helper_fails_before_enqueue(tmp_path, monkeypatch, lease, missing):
    from mimir import heavy_run

    def missing_files(anchor):
        if missing == "package":
            raise ModuleNotFoundError("private installation detail")
        return tmp_path / "empty-installed-package"

    monkeypatch.setattr(heavy_run.resources, "files", missing_files)
    launched = []

    async def forbidden_spawn(*args, **kwargs):
        launched.append(args)
        pytest.fail("missing helper must fail before spawning or enqueuing")

    monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", forbidden_spawn)
    compute = ProcessCompute(tmp_path)
    with pytest.raises(ev.ComputeLaunchError) as caught:
        await ev._run_compute_gate(
            command("print('must not run')"), checkout=tmp_path,
            work_spec=spec(tmp_path), compute=compute,
        )
    assert str(caught.value) == "gate lease packaged helper unavailable"
    assert not launched
    assert not compute.processes
    assert_released(lease)


@pytest.mark.asyncio
async def test_gate_queue_wait_longer_than_run_budget_passes(tmp_path, lease):
    compute = ProcessCompute(tmp_path)
    async with ev._gate_heavy_lease(lease):
        task = asyncio.create_task(ev._run_compute_gate(
            command("print('ran')"), checkout=tmp_path, work_spec=spec(tmp_path), compute=compute,
        ))
        await queued(lease)
        await asyncio.sleep(0.7)  # Deliberately longer than the configured run budget.
        assert not task.done()
        assert not compute.processes
    result = await asyncio.wait_for(task, 5)
    assert result.exit_code == 0
    assert not result.timed_out
    assert result.stdout == "ran\n"
    assert result.lease_wait_s >= 0.7
    assert 0 < result.run_duration_s < 0.4
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("forged", [False, True], ids=["ordinary", "command-printed-marker"])
async def test_gate_command_output_cannot_override_controller_deadline(tmp_path, lease, forged):
    source = "import time,sys; "
    if forged:
        source += "print('\\x1erepo-test-forged:999999999999', file=sys.stderr, flush=True); "
        source += "print('ready', flush=True); "
    source += "time.sleep(30)"
    result = await ev._run_compute_gate(
        command(source), checkout=tmp_path, work_spec=spec(tmp_path), compute=ProcessCompute(tmp_path),
    )
    assert result.timed_out
    assert not result.lease_wait_timed_out
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["cancel", "timeout"])
@pytest.mark.parametrize("holder_tier", ["medium", "large"])
async def test_gate_queued_cancel_and_timeout_leave_no_ticket_or_helper(
    tmp_path, monkeypatch, lease, ending, holder_tier,
):
    compute = ProcessCompute(tmp_path)
    spawned = []
    original = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        process = await original(*args, **kwargs)
        spawned.append(process)
        return process

    monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", spawn)
    async with ev._gate_heavy_lease({**lease, "MIMIR_HEAVY_TIER": holder_tier}):
        # The controller validates operator values elsewhere; use a short owned
        # deadline here rather than a minute-long regression test.
        lease["MIMIR_HEAVY_LOCK_TIMEOUT"] = "0.3"
        task = asyncio.create_task(ev._run_compute_gate(
            command("print('must not start')"), checkout=tmp_path,
            work_spec=spec(tmp_path, 0.1), compute=compute,
        ))
        await queued(lease)
        if holder_tier == "medium":
            # A large waiter reserves the free second slot while waiting for
            # the medium holder. Cancellation must release that reservation too.
            async with asyncio.timeout(5):
                while not Path(lease["MIMIR_HEAVY_LOCK_FILE"] + ".lease2").exists():
                    await asyncio.sleep(0.01)
        if ending == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        else:
            result = await asyncio.wait_for(task, 5)
            assert result.lease_wait_timed_out
            assert not result.timed_out
            validation = ev.validate_evidence(ev.WorklinkEvidence(
                issue=2202, attempt=1, backend="opencode", branch="branch", checkout=str(tmp_path),
                started_at="", finished_at="", files_changed=["fix.py"], diff_stat="",
                commands=[], tests=ev.TestResult(
                    "gate", result.exit_code, lease_wait_timed_out=result.lease_wait_timed_out,
                ), pr_url=None, status="completed",
            ))
            assert validation.reasons == ("gate_lease_wait_timeout",)
            assert "infrastructure" in validation.evidence.failure_reason
            assert not validation.review_ready
        assert not compute.processes
        assert spawned[1].returncode is not None
        assert not list(Path(lease["MIMIR_HEAVY_LOCK_FILE"] + ".queue").iterdir())
    assert all(process.returncode is not None for process in spawned)
    assert_released(lease)


@pytest.mark.asyncio
async def test_gate_two_legs_exclude_second_queue_wait(tmp_path, lease):
    compute = ProcessCompute(tmp_path)
    holder_entered = asyncio.Event()
    holder_release = asyncio.Event()
    holder = None

    async def contend():
        async with ev._gate_heavy_lease(lease):
            holder_entered.set()
            await holder_release.wait()

    original = compute.wait

    async def wait(handle, timeout_s):
        nonlocal holder
        result = await original(handle, timeout_s)
        if len(compute.processes) == 1:
            holder = asyncio.create_task(contend())
            # Enqueue the contender BEFORE leg one's controller releases.
            await queued(lease)
        return result

    compute.wait = wait
    task = asyncio.create_task(ev._run_compute_gate(
        command("print('first')") + " && " + command("print('second')"),
        checkout=tmp_path, work_spec=spec(tmp_path), compute=compute,
    ))
    try:
        await asyncio.wait_for(holder_entered.wait(), 5)
        await queued(lease)
        await asyncio.sleep(0.7)
        assert len(compute.processes) == 1
        holder_release.set()
        result = await asyncio.wait_for(task, 5)
        assert result.exit_code == 0
        assert result.stdout == "first\nsecond\n"
        assert not result.timed_out
        assert result.lease_wait_s >= 0.7
        assert compute.specs[1].timeout_s < compute.specs[0].timeout_s
    finally:
        holder_release.set()
        if holder is not None:
            await holder
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert_released(lease)


@pytest.mark.asyncio
async def test_gate_two_legs_share_one_run_budget(tmp_path, lease):
    compute = ProcessCompute(tmp_path)
    result = await ev._run_compute_gate(
        command("import time; time.sleep(0.25)") + " && " +
        command("import time; time.sleep(0.25)"),
        checkout=tmp_path, work_spec=spec(tmp_path), compute=compute,
    )
    assert len(compute.processes) == 2
    assert result.timed_out
    assert compute.specs[1].timeout_s < 0.2
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("durations", [(1.25,), (0.4, 0.85)], ids=["only-leg", "cumulative-legs"])
@pytest.mark.parametrize("later_leg", [False, True], ids=["last-leg", "later-leg-not-launched"])
async def test_gate_successful_overrun_exhausts_cumulative_run_budget(
    monkeypatch, lease, durations, later_leg,
):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    # Drive the production accounting loop without real sleeps or a runner
    # timeout flag: a successful leg can return just after its allotted time.
    now = 1000.0
    launched = []

    @asynccontextmanager
    async def acquired(environment, *, on_launch=None):
        nonlocal now
        assert environment == lease
        now += 10.0  # Queue time must not consume the one-second run budget.
        yield

    async def run_command(leg, budget):
        nonlocal now
        launched.append((leg, budget))
        index = len(launched) - 1
        # Let an incorrectly launched later leg succeed too, so the failure
        # diagnoses the missing budget guard rather than an exhausted fixture.
        now += durations[index] if index < len(durations) else 0.0
        return ComputeResult(0, f"leg-{index}\n", "")

    monkeypatch.setattr(ev, "_gate_heavy_lease", acquired)
    # Replace only evidence's asyncio binding, not the real event-loop clock
    # used by pytest, cancellation, and unrelated runtime machinery.
    monkeypatch.setattr(ev, "asyncio", SimpleNamespace(
        get_running_loop=lambda: SimpleNamespace(time=lambda: now),
    ))
    legs = [command(f"print({index})") for index in range(len(durations) + later_leg)]
    result = await ev._run_gate_with_lease(" && ".join(legs), 1.0, run_command)

    assert result.exit_code == 0
    assert result.timed_out is True
    assert not result.lease_wait_timed_out
    assert len(launched) == len(durations)
    assert [budget for _, budget in launched] == pytest.approx(
        [1.0] if len(durations) == 1 else [1.0, 0.6],
    )
    assert result.run_duration_s == pytest.approx(sum(durations))
    assert result.lease_wait_s == pytest.approx(10.0 * len(durations))
    assert result.stdout == "".join(f"leg-{index}\n" for index in range(len(durations)))
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("first_code,timed_out", [(3, False), (0, True)])
async def test_gate_failed_or_timed_out_leg_cannot_be_hidden_by_later_leg(
    tmp_path, lease, first_code, timed_out,
):
    compute = ProcessCompute(tmp_path)
    original = compute.wait

    async def wait(handle, timeout_s):
        result = await original(handle, timeout_s)
        # Collection can time out after a command writes reports and exits zero.
        return replace(result, timed_out=timed_out)

    compute.wait = wait
    result = await ev._run_compute_gate(
        command(f"raise SystemExit({first_code})") + " && " + command("print('must not run')"),
        checkout=tmp_path, work_spec=spec(tmp_path), compute=compute,
    )
    assert len(compute.processes) == 1
    assert result.exit_code == first_code
    assert result.timed_out is timed_out
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["operator", "persistence-failure"])
async def test_gate_queued_helper_publishes_stoppable_identity(tmp_path, lease, stop):
    handles = []
    compute = ProcessCompute(tmp_path)

    def launched(handle):
        handles.append(handle)
        if stop == "persistence-failure":
            raise RuntimeError("cannot persist")

    async with ev._gate_heavy_lease(lease):
        task = asyncio.create_task(ev._run_compute_gate(
            command("print('must not run')"), checkout=tmp_path,
            work_spec=spec(tmp_path), compute=compute, on_launch=launched,
        ))
        if stop == "operator":
            await queued(lease)
            assert handles[0].process_start_ticks is not None
            # A fresh controller can stop this exact helper from the persisted
            # handle without access to the gate task or its compute instance.
            await cp.LocalSubprocessComputeBackend().cancel(handles[0])
            with pytest.raises(ev.ComputeLaunchError, match="lease helper failed"):
                await asyncio.wait_for(task, 5)
        else:
            with pytest.raises(RuntimeError, match="cannot persist"):
                await asyncio.wait_for(task, 5)
        assert not compute.processes
        assert len(handles) == 1
        queue = Path(lease["MIMIR_HEAVY_LOCK_FILE"] + ".queue")
        assert not queue.exists() or not list(queue.iterdir())
    assert_released(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True], ids=["no-lease", "never-takes-lease"])
async def test_gate_without_lease_keeps_single_deadline(tmp_path, monkeypatch, lease, configured):
    if not configured:
        monkeypatch.setattr(cp, "_heavy_lock_environment", lambda: {})
    result = await ev._run_compute_gate(
        shlex.join([sys.executable, "-c", "import time; print('ready'); time.sleep(30)"]),
        checkout=tmp_path, work_spec=spec(tmp_path, 0.1), compute=ProcessCompute(tmp_path),
    )
    assert result.timed_out
    assert not result.lease_wait_timed_out
    assert result.lease_wait_s == 0
    assert_released(lease)


def test_gate_lease_parser_preserves_literal_arguments():
    assert ev._gate_lease_legs("sh scripts/heavy_run.sh echo '&&' sh scripts/heavy_run.sh echo") == (
        "env MIMIR_HEAVY_LOCK_HELD=1 echo '&&' sh scripts/heavy_run.sh echo",
    )
    assert ev._gate_lease_legs('sh scripts/heavy_run.sh echo "a"b') == (
        "env MIMIR_HEAVY_LOCK_HELD=1 echo ab",
    )
    for unsafe in (
        "python scripts/heavy_run.sh echo hi", "sh attacker.sh echo hi",
        "sh scripts/heavy_run.sh", "sh scripts/heavy_run.sh echo hi &&",
        "sh scripts/heavy_run.sh echo $PATH", "sh scripts/heavy_run.sh echo *.py",
        "sh scripts/heavy_run.sh echo hi; true", "sh scripts/heavy_run.sh echo hi > file",
        "sh scripts/heavy_run.sh echo hi || true", "sh scripts/heavy_run.sh echo 'unterminated",
    ):
        assert not ev._gate_lease_legs(unsafe)


@pytest.mark.asyncio
async def test_gate_untrusted_helper_readiness_fails_closed(tmp_path, monkeypatch, lease):
    original = asyncio.create_subprocess_exec

    async def wrong_helper(*args, **kwargs):
        return await original(
            sys.executable, "-c", "print('not ready')",
            **kwargs,
        )

    monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", wrong_helper)
    compute = ProcessCompute(tmp_path)
    with pytest.raises(ev.ComputeLaunchError, match="lease helper failed"):
        await ev._run_compute_gate(
            command("print('must not start')"), checkout=tmp_path,
            work_spec=spec(tmp_path), compute=compute,
        )
    assert not compute.processes


@pytest.mark.asyncio
@pytest.mark.parametrize("queued_helper", [False, True], ids=["held", "queued"])
@pytest.mark.parametrize("cancel_retirement", [False, True], ids=["ordinary", "repeated-cancel"])
async def test_gate_term_ignoring_helper_is_killed_and_stale_state_reclaimed(
    tmp_path, monkeypatch, lease, queued_helper, cancel_retirement,
):
    from contextlib import AsyncExitStack
    from mimir.heavy_run import helper_path

    # Use the actual queue/slot protocol. On TERM, deliberately freeze inside
    # the signal handler: no heartbeat or finally cleanup can make progress.
    marker = tmp_path / "term-observed"
    wrapper = tmp_path / "stuck-helper.py"
    wrapper.write_text(
        "import importlib.util, pathlib, signal, sys, time\n"
        f"spec = importlib.util.spec_from_file_location('protocol', {str(helper_path())!r})\n"
        "protocol = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(protocol)\n"
        "def ignore_term(signum, frame):\n"
        f"    pathlib.Path({str(marker)!r}).write_text('TERM')\n"
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "    while True: time.sleep(0.01)\n"
        "protocol._interrupt = ignore_term\n"
        "sys.exit(protocol.run(sys.argv[1:]))\n"
    )
    monkeypatch.setattr(ev, "_GATE_LEASE_TERM_GRACE_S", 0.3)
    monkeypatch.setattr(ev, "_GATE_LEASE_KILL_GRACE_S", 0.5)
    lease.update(MIMIR_HEAVY_HEARTBEAT_SECONDS="0.03", MIMIR_HEAVY_STALE_SECONDS="0.2")
    original = asyncio.create_subprocess_exec
    processes = []
    signals = []
    original_killpg = os.killpg

    def observed_signal(pid, signum):
        if processes and pid == processes[0].pid:
            signals.append(signum)
        return original_killpg(pid, signum)

    async def spawn(*args, **kwargs):
        args = (*args[:2], str(wrapper), *args[3:])
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    entered = asyncio.Event()
    leave = asyncio.Event()

    async def participant():
        async with ev._gate_heavy_lease(lease):
            entered.set()
            await leave.wait()

    task = canceller = None
    async with AsyncExitStack() as holders:
        if queued_helper:
            await holders.enter_async_context(ev._gate_heavy_lease(lease))
        monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", spawn)
        monkeypatch.setattr(ev.os, "killpg", observed_signal)
        task = asyncio.create_task(participant())
        try:
            if queued_helper:
                await queued(lease)
                # Wait until this ticket has a handler and a live heartbeat.
                queue = Path(lease["MIMIR_HEAVY_LOCK_FILE"] + ".queue")
                async with asyncio.timeout(5):
                    while not any((p / "heartbeat").exists() for p in queue.iterdir()):
                        await asyncio.sleep(0.01)
                task.cancel()  # Enter retirement while still queued.
            else:
                await asyncio.wait_for(entered.wait(), 5)
                leave.set()

            async def interrupt_retirement():
                async with asyncio.timeout(5):
                    while not marker.exists():
                        await asyncio.sleep(0.005)
                if cancel_retirement:
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(0.02)

            canceller = asyncio.create_task(interrupt_retirement())
            done, _ = await asyncio.wait({task}, timeout=5)
            assert task in done, "retirement exceeded its bound"
            await canceller
            if queued_helper or cancel_retirement:
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                await task
            assert signals == [signal.SIGTERM, signal.SIGKILL]
            assert processes[0].returncode == -signal.SIGKILL
            base = lease["MIMIR_HEAVY_LOCK_FILE"]
            if queued_helper:
                assert list(Path(base + ".queue").iterdir()), "KILL must leave a stale ticket"
            else:
                assert Path(base + ".lease").exists(), "KILL must leave stale slots"
        finally:
            # Test-owned interlock: deleting production escalation must fail the
            # assertion, not leak a participant or hang pytest's teardown.
            for process in processes:
                if process.returncode is None:
                    original_killpg(process.pid, signal.SIGKILL)
                await asyncio.wait_for(process.wait(), 2)
            if task is not None and not task.done():
                task.cancel()
            if canceller is not None:
                canceller.cancel()
            await asyncio.gather(*[t for t in (task, canceller) if t is not None], return_exceptions=True)
        monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", original)
    # A fresh real participant must reclaim the frozen generation/ticket,
    # not merely observe that the killed PID disappeared.
    async with asyncio.timeout(5):
        async with ev._gate_heavy_lease(lease):
            pass
    assert_released(lease)


@pytest.mark.asyncio
async def test_gate_post_kill_reap_is_bounded(tmp_path, monkeypatch, lease):
    class Unreapable:
        pid = 123456789
        returncode = None
        stdin = None

        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data(b"ready\n")

        async def wait(self):
            await asyncio.Event().wait()

    process = Unreapable()
    signals = []

    async def spawn(*args, **kwargs):
        return process

    monkeypatch.setattr(ev.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(ev.os, "killpg", lambda pid, signum: signals.append(signum))
    monkeypatch.setattr(ev, "_GATE_LEASE_TERM_GRACE_S", 0.01)
    monkeypatch.setattr(ev, "_GATE_LEASE_KILL_GRACE_S", 0.01)
    async with asyncio.timeout(1):
        with pytest.raises(ev.ComputeLaunchError, match="retirement timed out"):
            async with ev._gate_heavy_lease(lease):
                pass
    assert signals == [signal.SIGTERM, signal.SIGKILL]


@pytest.mark.asyncio
async def test_factory_gate_lease_budget_is_retained_in_evidence(tmp_path, monkeypatch, lease):
    import subprocess
    from datetime import UTC, datetime
    from mimir_cultivate.worklink import worker_client
    from mimir_cultivate.worklink.execution_policy import OPENCODE_FACTORY
    from tests.worklink_control_helpers import install_local_control_executor

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.name=test", "-c", "user.email=test@example.com",
                    "commit", "--allow-empty", "-qm", "seed"], cwd=repo, check=True)
    (repo / "change.txt").write_text("builder work\n")
    calls = install_local_control_executor(monkeypatch, tmp_path)
    monkeypatch.setattr(worker_client, "factory_checkout_for_path", lambda path: (repo, 2202, 1))
    gate = command("import os; assert os.environ['MIMIR_HEAVY_LOCK_HELD'] == '1'; print('ran')")
    work_spec = replace(spec(repo, 10), backend="feature_factory", execution=OPENCODE_FACTORY)
    async with ev._gate_heavy_lease(lease):
        task = asyncio.create_task(ev.observe_evidence(
            issue=2202, attempt=1, backend="feature_factory", branch="branch", checkout=repo,
            started_at=datetime.now(UTC), base_ref="main", backend_status="completed",
            test_command=gate, compute=object(), work_spec=work_spec,
        ))
        await queued(lease)
        assert not calls
    validation = await asyncio.wait_for(task, 10)
    assert validation.review_ready
    assert validation.evidence.tests.lease_wait_s > 0
    assert validation.evidence.tests.run_duration_s > 0
    assert calls[0]["children_reaped"]
    assert_released(lease)
