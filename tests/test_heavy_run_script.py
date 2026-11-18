"""Real local processes exercise the cross-host directory-lease protocol."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import errno
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time

import pytest

from tests.timing import SUBPROCESS_HANG_GUARD_SECONDS

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "heavy_run.sh"
TIMEOUT_HELPER = Path(__file__).with_name("heavy_timeout.py")


@pytest.fixture(autouse=True)
def _hermetic_tools(tmp_path: Path, monkeypatch) -> None:
    # Exercise every lease test as on a macOS runner without coreutils. Only
    # ordinary POSIX utilities needed by the wrapper/payloads are available.
    bindir = tmp_path / "posix-bin"
    bindir.mkdir()
    for name in ("sh", "dirname", "uname", "touch", "sleep", "true"):
        tool = shutil.which(name)
        assert tool is not None, f"missing POSIX utility: {name}"
        (bindir / name).symlink_to(tool)
    monkeypatch.setenv("PATH", str(bindir))
    assert shutil.which("timeout") is None and shutil.which("gtimeout") is None
    timeout = tmp_path / "test-timeout"
    timeout.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} "
                       f'{shlex.quote(str(TIMEOUT_HELPER))} "$@"\n')
    timeout.chmod(0o755)


def _env(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIMIR_HEAVY_")}
    env.update(MIMIR_HEAVY_LOCK_FILE=str(tmp_path / "heavy.lock"),
               MIMIR_HEAVY_TIMEOUT_TOOL=str(tmp_path / "test-timeout"),
               MIMIR_HEAVY_PYTHON=sys.executable, MIMIR_HEAVY_LOCK_TIMEOUT="60",
               MIMIR_HEAVY_HEARTBEAT_SECONDS="0.03")
    env.update(extra)
    return env


def _lease(tmp_path: Path) -> Path:
    return tmp_path / "heavy.lock.lease"


def _run(tmp_path: Path, *args: str, **extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([shutil.which("sh"), str(SCRIPT), *args], env=_env(tmp_path, **extra),
                          capture_output=True, text=True, timeout=90)


def _wait(predicate, seconds: float = 15) -> None:
    end = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < end, "condition never became true"
        time.sleep(0.01)


@pytest.fixture
def start(tmp_path: Path):
    children = []

    def launch(*args: str, **extra: str):
        child = subprocess.Popen(["sh", str(SCRIPT), *args], env=_env(tmp_path, **extra),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children.append(child)
        return child

    yield launch
    for child in children:
        if child.poll() is None:
            child.terminate()
        child.communicate(timeout=15)


def _blocking_payload(release: Path) -> str:
    return ("from pathlib import Path; import time; "
            f"p=Path({str(release)!r})\nwhile not p.exists(): time.sleep(.01)")


@pytest.mark.parametrize("status", [0, 3])
def test_release_on_exit_and_owner_metadata(tmp_path: Path, start, status: int) -> None:
    release = tmp_path / "release"
    child = start(sys.executable, "-c",
                  "import os,sys; print(os.environ['MIMIR_HEAVY_LOCK_HELD'], flush=True); "
                  + _blocking_payload(release) + f"\nsys.exit({status})")
    assert child.stdout.readline().strip() == "1"
    owner = json.loads((_lease(tmp_path) / "owner").read_text())
    assert owner["host"] and owner["pid"] > 0 and owner["start_time"] > 0
    assert owner["timeout"] == "60"
    release.touch()
    assert child.wait(timeout=SUBPROCESS_HANG_GUARD_SECONDS) == status
    assert not _lease(tmp_path).exists()
    assert not list(tmp_path.glob("*.retired-*"))


def test_two_wrappers_serialize(tmp_path: Path, start) -> None:
    release = tmp_path / "release"
    first = start(sys.executable, "-c", _blocking_payload(release))
    heartbeat = _lease(tmp_path) / "heartbeat"
    _wait(heartbeat.exists)
    second = start("touch", str(tmp_path / "second"))
    # Observe the holder's advancing counter, rather than a process-start sleep.
    _wait(lambda: int(heartbeat.read_text()) >= 8)
    assert second.poll() is None and not (tmp_path / "second").exists()
    release.touch()
    assert (
        first.wait(timeout=SUBPROCESS_HANG_GUARD_SECONDS)
        == second.wait(timeout=SUBPROCESS_HANG_GUARD_SECONDS) == 0
    )
    assert (tmp_path / "second").exists()


def test_hundreds_of_raced_acquisitions_have_one_holder(tmp_path: Path) -> None:
    # Independent payload mkdir detects overlap, rather than trusting the lease's
    # own assertion of ownership.
    payload = ("from pathlib import Path; import time; "
               f"p=Path({str(tmp_path / 'payload-active')!r}); p.mkdir(); "
               f"log=Path({str(tmp_path / 'completed')!r}); time.sleep(.002); "
               "f=log.open('a'); f.write('done\\n'); f.close(); p.rmdir()")
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: _run(tmp_path, sys.executable, "-c", payload), range(240)))
    assert all(p.returncode == 0 for p in results), [p.stderr for p in results if p.returncode]
    assert len((tmp_path / "completed").read_text().splitlines()) == 240
    assert not _lease(tmp_path).exists()


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_signal_kills_command_group_before_release(tmp_path: Path, start, sig: int) -> None:
    holder = start(sys.executable, "-c",
                   "import subprocess,sys,time; "
                   "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
                   "print('ready', flush=True); time.sleep(60)")
    assert holder.stdout.readline().strip() == "ready"
    holder.send_signal(sig)
    # EOF is held by the grandchild too: communicate proves the whole group died.
    holder.communicate(timeout=15)
    assert holder.returncode == 128 + sig
    assert not _lease(tmp_path).exists()
    assert _run(tmp_path, "true").returncode == 0


def test_overall_timeout_covers_queue_and_execution(tmp_path: Path, start) -> None:
    holder = start("sleep", "60", MIMIR_HEAVY_LOCK_TIMEOUT="3")
    _wait(lambda: (_lease(tmp_path) / "owner").exists())
    queued = start("touch", str(tmp_path / "unexpected"), MIMIR_HEAVY_LOCK_TIMEOUT="0.3")
    queued.communicate(timeout=5)  # Far below the payload's unbounded 60-second run.
    assert queued.returncode == 124 and not (tmp_path / "unexpected").exists()
    assert holder.wait(timeout=15) == 124
    assert not _lease(tmp_path).exists()


def test_timeout_helper_allows_term_cleanup_during_kill_grace(tmp_path: Path) -> None:
    release = tmp_path / "release"
    payload = ("import signal,time; "
               "signal.signal(signal.SIGTERM, lambda *_: print('term', flush=True)); "
               + _blocking_payload(release))
    child = subprocess.Popen([sys.executable, str(TIMEOUT_HELPER), "-k", "5s", "0.5s",
                              sys.executable, "-c", payload],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "term"
        assert child.poll() is None, "SIGTERM must allow cleanup before SIGKILL"
        release.touch()
        child.communicate(timeout=3)
        assert child.returncode == 124
    finally:
        release.touch()
        child.terminate()
        child.communicate(timeout=10)


def test_timeout_helper_kills_stubborn_command_group_after_grace(tmp_path: Path) -> None:
    payload = ("import signal,subprocess,sys,time; "
               "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
               "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
               "print('ready', flush=True); time.sleep(60)")
    result = subprocess.run([sys.executable, str(TIMEOUT_HELPER), "-k", "0.2s", "0.5s",
                             sys.executable, "-c", payload],
                            capture_output=True, text=True, timeout=5)
    assert result.stdout.strip() == "ready"
    assert result.returncode == 137, result.stderr


def _is_zombie(pid: int) -> bool:
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():  # Linux; slim images ship no ps.
        return stat.read_text().rpartition(")")[2].split()[0] == "Z"
    return subprocess.run(["/bin/ps", "-o", "stat=", "-p", str(pid)],
                          capture_output=True, text=True).stdout.strip().startswith("Z")


def test_timeout_helper_tolerates_a_zombie_only_command_group() -> None:
    """macOS answers killpg on an all-zombie group with EPERM (chainlink #2146 fallout)."""
    spec = importlib.util.spec_from_file_location("heavy_timeout_under_test", TIMEOUT_HELPER)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    leader = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    try:
        deadline = time.monotonic() + 10
        while not _is_zombie(leader.pid):
            assert time.monotonic() < deadline, "leader never became a zombie"
            time.sleep(0.01)
        helper.signal_group(leader.pid, signal.SIGKILL)  # Zombie-only group.
    finally:
        leader.wait(timeout=10)
    helper.signal_group(leader.pid, signal.SIGKILL)  # Reaped group.


def test_heartbeat_stops_when_child_exits(tmp_path: Path, start) -> None:
    release = tmp_path / "release"
    holder = start(sys.executable, "-c", _blocking_payload(release))
    heartbeat = _lease(tmp_path) / "heartbeat"
    _wait(lambda: heartbeat.exists() and int(heartbeat.read_text()) >= 3)
    # Retain the generation fd to detect writes after detachment as well.
    fd = os.open(_lease(tmp_path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        release.touch()
        assert holder.wait(timeout=SUBPROCESS_HANG_GUARD_SECONDS) == 0
        assert os.listdir(fd) == []
        time.sleep(0.12)
        assert os.listdir(fd) == []
    finally:
        os.close(fd)
    assert not _lease(tmp_path).exists()


def _frozen(tmp_path: Path, timestamp: float = 0) -> Path:
    lease = _lease(tmp_path)
    lease.mkdir()
    (lease / "owner").write_text(json.dumps({"host": "other-host", "pid": 123,
                                            "start_time": timestamp, "timeout": 7200}))
    (lease / "heartbeat").write_text("7\n")
    return lease


def _helper():
    spec = importlib.util.spec_from_file_location("heavy_run_under_test", SCRIPT.with_suffix(".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stale_lease_has_single_winner_among_process_waiters(tmp_path: Path, start) -> None:
    _frozen(tmp_path, timestamp=10**12)  # future owner time must not prevent recovery
    payload = ("from pathlib import Path; import time; "
               f"p=Path({str(tmp_path / 'active')!r}); p.mkdir(); time.sleep(.05); p.rmdir()")
    waiters = [start(sys.executable, "-c", payload, MIMIR_HEAVY_STALE_SECONDS="1.2") for _ in range(8)]
    outputs = [p.communicate(timeout=SUBPROCESS_HANG_GUARD_SECONDS) for p in waiters]
    assert all(p.returncode == 0 for p in waiters), outputs
    assert sum(err.count("retired stale lease") for _, err in outputs) == 1
    assert not _lease(tmp_path).exists()


def test_retired_cleanup_races_losing_waiter_without_failing_run(tmp_path: Path, monkeypatch, capsys) -> None:
    module = _helper()
    lease = _frozen(tmp_path)
    loser_fd = os.open(lease, os.O_RDONLY | os.O_DIRECTORY)
    claimed, resume = threading.Event(), threading.Event()
    real_current, real_rmdir = module._is_current, module.os.rmdir
    retired = []
    loser = None

    def pause_loser(path, fd):
        if fd == loser_fd:
            claimed.set()  # _break_stale has created breaking through its fd.
            assert resume.wait(10)
        return real_current(path, fd)

    with ThreadPoolExecutor(max_workers=1) as pool:
        def race_final_rmdir(path, *, dir_fd=None):
            nonlocal loser
            if dir_fd is None and Path(path).name.startswith(lease.name + ".retired-") and not retired:
                retired.append(Path(path))
                assert not (Path(path) / "breaking").exists()
                loser = pool.submit(module._break_stale, lease, loser_fd, "7\n")
                assert claimed.wait(10)
                assert (Path(path) / "breaking").is_dir()
                # Real rmdir now raises ENOTEMPTY, with the loser still paused.
            return real_rmdir(path, dir_fd=dir_fd)

        monkeypatch.setattr(module, "_is_current", pause_loser)
        monkeypatch.setattr(module.os, "rmdir", race_final_rmdir)
        # run() owns signal handlers in production; keep this in-process test
        # from changing the test runner's handlers.
        monkeypatch.setattr(module.signal, "signal", lambda *args: None)
        for name, value in _env(tmp_path, MIMIR_HEAVY_STALE_SECONDS="0.06").items():
            monkeypatch.setenv(name, value)
        marker = tmp_path / "ran-with-leftover"
        payload = ("from pathlib import Path; "
                   f"p=Path({str(tmp_path)!r}); "
                   "assert len(list(p.glob('heavy.lock.lease.retired-*'))) == 1; "
                   f"Path({str(marker)!r}).touch()")
        try:
            assert module.run([sys.executable, "-c", payload]) == 0
            assert marker.exists(), "winner's command must run despite ENOTEMPTY"
            assert len(retired) == 1 and not retired[0].exists(), "release must sweep the leftover"
        finally:
            resume.set()
            try:
                if loser is not None:
                    assert loser.result(timeout=10) is False
            finally:
                os.close(loser_fd)
    warnings = [line for line in capsys.readouterr().err.splitlines() if "cleanup deferred" in line]
    assert len(warnings) == 1
    assert f"errno={errno.ENOTEMPTY}" in warnings[0]


@pytest.mark.parametrize("operation", ["acquire", "release"])
def test_sweep_only_retired_directories_for_this_lease(tmp_path: Path, capsys, operation: str) -> None:
    module = _helper()
    lease = tmp_path / "heavy[1].lock.lease"
    fd = module._acquire(lease, stale=600, interval=1) if operation == "release" else None
    retired = lease.with_name(lease.name + ".retired-old")
    retired.mkdir()
    (retired / "breaking").mkdir()
    other = tmp_path / "heavy1.lock.lease.retired-other"
    other.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    link = lease.with_name(lease.name + ".retired-link")
    link.symlink_to(target, target_is_directory=True)
    ordinary_file = lease.with_name(lease.name + ".retired-file")
    ordinary_file.touch()
    try:
        if operation == "acquire":
            fd = module._acquire(lease, stale=600, interval=1)
            assert lease.is_dir()
        else:
            module._release(lease, fd)
        assert not retired.exists()
        assert other.is_dir() and target.is_dir() and link.is_symlink() and ordinary_file.is_file()
        assert capsys.readouterr().err == ""
    finally:
        if fd is not None:
            module._release(lease, fd)
            os.close(fd)


@pytest.mark.parametrize("operation", ["acquire", "release"])
@pytest.mark.parametrize("fault", ["delete", "scan", "already-removed"])
def test_retired_sweep_errors_are_nonfatal_and_sanitized(tmp_path: Path, monkeypatch, capsys,
                                                       operation: str, fault: str) -> None:
    module = _helper()
    parent = tmp_path / "parent\n\x1b[31m"
    parent.mkdir()
    lease = parent / "heavy\n\x1b[31m.lock.lease"
    fd = module._acquire(lease, stale=600, interval=1)
    retired = lease.with_name(lease.name + ".retired-old")
    retired.mkdir()

    def fail(*args, **kwargs):
        error = FileNotFoundError if fault == "already-removed" else PermissionError
        raise error(errno.ENOENT if fault == "already-removed" else errno.EACCES, "unsafe\n\x1btext")

    monkeypatch.setattr(module.os if fault == "scan" else module.shutil,
                        "scandir" if fault == "scan" else "rmtree", fail)
    try:
        if operation == "acquire":
            # Keep the live generation as a replacement, but make a fresh
            # acquisition possible without invoking release's sweep.
            lease.rename(tmp_path / "displaced")
            acquired = module._acquire(lease, stale=600, interval=1)
            os.close(acquired)
        else:
            module._release(lease, fd)
    finally:
        os.close(fd)
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == (0 if fault == "already-removed" else 1 if operation == "acquire" else 2)
    for line in lines:
        assert "cleanup deferred" in line and "errno=13" in line
        assert "\x1b" not in line and "unsafe" not in line
        assert "\\n" in line and "\\x1b" in line


def test_stale_break_single_winner_preserves_replacement(tmp_path: Path, monkeypatch) -> None:
    # Every waiter observed the old generation. Insert a replacement immediately
    # after the winner's rename, before any delayed loser resumes.
    module = _helper()
    lease = _frozen(tmp_path)
    fds = [os.open(lease, os.O_RDONLY | os.O_DIRECTORY) for _ in range(8)]
    real_delete = module.shutil.rmtree

    def delete_detached(path):
        assert path != lease, "must rename before deleting: direct deletion races new holders"
        lease.mkdir()
        (lease / "heartbeat").write_text("new-holder\n")
        real_delete(path)

    monkeypatch.setattr(module.shutil, "rmtree", delete_detached)
    try:
        winners = [module._break_stale(lease, fd, "7\n") for fd in fds]
        assert winners == [True] + [False] * 7
        assert (lease / "heartbeat").read_text() == "new-holder\n"
    finally:
        for fd in fds:
            os.close(fd)


def test_failed_stale_rename_does_not_delete(tmp_path: Path, monkeypatch) -> None:
    module = _helper()
    lease = _frozen(tmp_path)
    fd = os.open(lease, os.O_RDONLY | os.O_DIRECTORY)

    def lost_rename(*args):
        raise FileNotFoundError("another waiter won")

    def unexpected_delete(*args):
        pytest.fail("a failed rename must never delete a lease")

    monkeypatch.setattr(Path, "rename", lost_rename)
    monkeypatch.setattr(module.shutil, "rmtree", unexpected_delete)
    try:
        assert module._break_stale(lease, fd, "7\n") is False
        assert (lease / "heartbeat").read_text() == "7\n"
    finally:
        os.close(fd)


def test_abandoned_empty_lease_gets_full_monotonic_grace(tmp_path: Path, monkeypatch) -> None:
    module = _helper()
    lease = _lease(tmp_path)
    lease.mkdir()  # creator died between mkdir and its first heartbeat
    now = [100.0]
    sleeps = []
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])

    def advance(seconds):
        sleeps.append(seconds)
        now[0] += 100

    monkeypatch.setattr(module.time, "sleep", advance)
    fd = module._acquire(lease, stale=600, interval=1)
    try:
        assert len(sleeps) == 6
    finally:
        module._release(lease, fd)
        os.close(fd)


@pytest.mark.parametrize("change", ["counter", "generation"])
def test_break_revalidates_observed_generation_and_counter(tmp_path: Path, change: str) -> None:
    module = _helper()
    lease = _frozen(tmp_path)
    fd = os.open(lease, os.O_RDONLY | os.O_DIRECTORY)
    try:
        if change == "generation":
            lease.rename(tmp_path / "old-generation")
            _frozen(tmp_path)  # same counter, different generation
        else:
            (lease / "heartbeat").write_text("8\n")
        assert module._break_stale(lease, fd, "7\n") is False
        assert lease.is_dir()
        assert not (lease / "breaking").exists()
    finally:
        os.close(fd)


def test_normal_release_coordinates_with_inflight_stale_break(tmp_path: Path, monkeypatch) -> None:
    module = _helper()
    lease = _frozen(tmp_path)
    fd = os.open(lease, os.O_RDONLY | os.O_DIRECTORY)
    renaming, resume, release_waiting = threading.Event(), threading.Event(), threading.Event()
    real_retire, real_sleep = module._retire, module.time.sleep
    retired_by = []

    def pause_retire(path):
        retired_by.append(threading.current_thread().name)
        renaming.set()
        assert resume.wait(10)
        real_retire(path)
        lease.mkdir()
        (lease / "heartbeat").write_text("replacement\n")

    def observe_wait(seconds):
        if threading.current_thread().name == "releasing-holder":
            release_waiting.set()
        real_sleep(seconds)

    monkeypatch.setattr(module, "_retire", pause_retire)
    monkeypatch.setattr(module.time, "sleep", observe_wait)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            breaker = pool.submit(module._break_stale, lease, fd, "7\n")
            assert renaming.wait(10)

            def release():
                threading.current_thread().name = "releasing-holder"
                module._release(lease, fd)

            holder = pool.submit(release)
            try:
                assert release_waiting.wait(10), "holder did not wait for stale-break claim"
            finally:
                resume.set()
            assert breaker.result(timeout=10) is True
            holder.result(timeout=10)
        assert len(retired_by) == 1
        assert (lease / "heartbeat").read_text() == "replacement\n"
    finally:
        resume.set()
        os.close(fd)


def test_displaced_holder_never_updates_or_releases_replacement(tmp_path: Path, start) -> None:
    release = tmp_path / "release"
    holder = start(sys.executable, "-c", _blocking_payload(release))
    lease = _lease(tmp_path)
    _wait(lambda: (lease / "heartbeat").exists() and int((lease / "heartbeat").read_text()) >= 3)
    displaced = tmp_path / "displaced"
    lease.rename(displaced)
    _frozen(tmp_path)
    initial = int((displaced / "heartbeat").read_text())
    _wait(lambda: int((displaced / "heartbeat").read_text()) > initial)
    assert (lease / "heartbeat").read_text() == "7\n"
    release.touch()
    assert holder.wait(timeout=SUBPROCESS_HANG_GUARD_SECONDS) == 0
    assert (lease / "heartbeat").read_text() == "7\n"


@pytest.mark.parametrize("timestamp", [0, 10**12])
def test_changing_counter_with_old_or_future_timestamp_is_never_broken(tmp_path: Path, start, timestamp: float) -> None:
    lease = _frozen(tmp_path, timestamp)
    stop = threading.Event()

    def update():
        counter = 7
        while not stop.wait(.02):
            counter += 1
            temporary = lease / "update"
            temporary.write_text(f"{counter}\n")
            temporary.replace(lease / "heartbeat")

    updater = threading.Thread(target=update)
    updater.start()
    try:
        waiter = start("touch", str(tmp_path / "entered"), MIMIR_HEAVY_STALE_SECONDS="1.2")
        # Multiple complete stale windows while the counter keeps changing.
        end = time.monotonic() + 2.5
        _wait(lambda: time.monotonic() >= end)
        assert waiter.poll() is None
        assert not (tmp_path / "entered").exists()
        assert json.loads((lease / "owner").read_text())["start_time"] == timestamp
    finally:
        stop.set()
        updater.join(timeout=10)


@pytest.mark.parametrize("fault", ["missing-parent", "not-directory", "symlink", "unwritable", "timeout", "no-timeout", "python", "invalid-stale", "invalid-interval"])
def test_unusable_configuration_fails_closed(tmp_path: Path, fault: str) -> None:
    extra = {}
    if fault == "missing-parent":
        extra["MIMIR_HEAVY_LOCK_FILE"] = str(tmp_path / "absent" / "heavy.lock")
    elif fault == "not-directory":
        _lease(tmp_path).touch()
    elif fault == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        _lease(tmp_path).symlink_to(target, target_is_directory=True)
    elif fault == "unwritable":
        # /sys is read-only even in root sandboxes. Non-root/macOS uses EACCES.
        if sys.platform == "linux" and os.geteuid() == 0:
            extra["MIMIR_HEAVY_LOCK_FILE"] = "/sys/mimir-heavy-test.lock"
        else:
            parent = tmp_path / "readonly"
            parent.mkdir(mode=0o555)
            extra["MIMIR_HEAVY_LOCK_FILE"] = str(parent / "heavy.lock")
    elif fault == "timeout":
        extra["MIMIR_HEAVY_TIMEOUT_TOOL"] = str(tmp_path / "absent-tool")
    elif fault == "no-timeout":
        bindir = tmp_path / "bin"
        bindir.mkdir()
        (bindir / "python3").symlink_to(sys.executable)
        extra["PATH"] = str(bindir)
        extra["MIMIR_HEAVY_TIMEOUT_TOOL"] = ""
    elif fault == "python":
        extra["MIMIR_HEAVY_PYTHON"] = str(tmp_path / "absent-python")
    elif fault == "invalid-interval":
        extra["MIMIR_HEAVY_HEARTBEAT_SECONDS"] = "600"
    else:
        extra["MIMIR_HEAVY_STALE_SECONDS"] = "nan"
    result = _run(tmp_path, sys.executable, "-c", "print('RAN')", **extra)
    assert result.returncode != 0, result
    assert "RAN" not in result.stdout
    assert "heavy_run:" in result.stderr
    if fault in {"timeout", "no-timeout"}:
        assert "requires gtimeout or timeout" in result.stderr


@pytest.mark.parametrize("setting", ["unset", "empty", "held"])
def test_unconfigured_or_ancestor_held_runs_directly(tmp_path: Path, setting: str) -> None:
    env = _env(tmp_path, MIMIR_HEAVY_TIMEOUT_TOOL="/absent")
    if setting == "unset":
        env.pop("MIMIR_HEAVY_LOCK_FILE")
    elif setting == "empty":
        env["MIMIR_HEAVY_LOCK_FILE"] = ""
    else:
        env["MIMIR_HEAVY_LOCK_HELD"] = "1"
    result = subprocess.run(["sh", str(SCRIPT), "sh", "-c", "echo ran"], env=env,
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0 and result.stdout.strip() == "ran"
    assert not _lease(tmp_path).exists()


def test_nested_wrapper_inherits_held_marker(tmp_path: Path) -> None:
    result = _run(tmp_path, "sh", str(SCRIPT), "sh", "-c", "echo held=$MIMIR_HEAVY_LOCK_HELD")
    assert result.returncode == 0 and result.stdout.strip() == "held=1"
    assert not _lease(tmp_path).exists()


@pytest.mark.parametrize("override", ["MIMIR_HEAVY_TIMEOUT_TOOL", "MIMIR_HEAVY_GTIMEOUT"])
def test_timeout_tool_override_and_arguments(tmp_path: Path, override: str) -> None:
    timeout = tmp_path / "custom-timeout"
    log = tmp_path / "timeout-args"
    timeout.write_text(f'#!/bin/sh\nprintf "%s\\n" "$1" "$2" "$3" > "{log}"\nshift 3\nexec "$@"\n')
    timeout.chmod(0o755)
    extra = {"MIMIR_HEAVY_TIMEOUT_TOOL": "", override: str(timeout),
             "MIMIR_HEAVY_LOCK_TIMEOUT": "123"}
    result = _run(tmp_path, "true", **extra)
    assert result.returncode == 0, result.stderr
    assert log.read_text().splitlines() == ["-k", "10s", "123s"]


@pytest.mark.parametrize("platform,expected", [("Darwin", "gtimeout"), ("Linux", "timeout")])
def test_timeout_tool_is_selected_for_platform(tmp_path: Path, platform: str, expected: str) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "selected"
    for name in ("timeout", "gtimeout"):
        tool = bindir / name
        tool.write_text(f'#!/bin/sh\nprintf "%s" "{name}" > "{log}"\nshift 3\nexec "$@"\n')
        tool.chmod(0o755)
    uname = bindir / "uname"
    uname.write_text(f'#!/bin/sh\nprintf "%s" "{platform}"\n')
    uname.chmod(0o755)
    (bindir / "dirname").symlink_to(shutil.which("dirname"))
    result = _run(tmp_path, sys.executable, "-c", "pass", PATH=str(bindir),
                  MIMIR_HEAVY_TIMEOUT_TOOL="")
    assert result.returncode == 0, result.stderr
    assert log.read_text() == expected


def test_requires_a_command(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 64 and "usage" in result.stderr


def test_lease_generation_is_shared_across_identities_whatever_the_umask(tmp_path: Path) -> None:
    # Participants are different uids without a shared group (host user, container
    # mimir and worklink). A stale waiter must create its breaker claim inside the
    # holder's generation and read its heartbeat, so a strict holder umask must not
    # leak into the lease. The payload observes the live generation.
    probe = tmp_path / "modes.json"
    payload = ("import json, os, stat; from pathlib import Path; "
               f"lease=Path({str(_lease(tmp_path))!r}); "
               "mode=lambda p: stat.S_IMODE(os.stat(p).st_mode); "
               f"Path({str(probe)!r}).write_text(json.dumps("
               "{'lease': mode(lease), 'owner': mode(lease/'owner'), 'heartbeat': mode(lease/'heartbeat')}))")
    child = subprocess.Popen(["sh", str(SCRIPT), sys.executable, "-c", payload], env=_env(tmp_path),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             umask=0o077)
    out, err = child.communicate(timeout=60)
    assert child.returncode == 0, (out, err)
    assert json.loads(probe.read_text()) == {"lease": 0o777, "owner": 0o644, "heartbeat": 0o644}


def test_generation_is_published_open_and_the_umask_is_restored(tmp_path: Path, monkeypatch) -> None:
    # Observe the generation at the instant mkdir publishes it: a concurrent
    # waiter of another identity must never see a umask-restricted directory.
    module = _helper()
    real_mkdir, modes = module.os.mkdir, []

    def observed(path, mode=0o777, **kwargs):
        real_mkdir(path, mode, **kwargs)
        if Path(path) == _lease(tmp_path):
            modes.append((stat.S_IMODE(os.stat(path).st_mode), os.umask(os.umask(0))))

    monkeypatch.setattr(module.os, "mkdir", observed)
    previous = os.umask(0o077)
    try:
        fd = module._acquire(_lease(tmp_path), 600, 1)
        after = os.umask(0o077)
    finally:
        os.umask(previous)
    os.close(fd)
    assert modes == [(0o777, 0)]
    assert after == 0o077


@pytest.mark.skipif(sys.platform != "linux" or os.geteuid() != 0,
                    reason="two real identities need Linux root")
@pytest.mark.parametrize("holder_uid, waiter_uid", [(41001, 41002), (41002, 41001)])
def test_stale_lease_recovers_across_identities_without_a_shared_group(
        tmp_path: Path, holder_uid: int, waiter_uid: int) -> None:
    # Real wrappers as two uids with distinct primary groups, no supplementary
    # groups and umask 022: the holder dies by SIGKILL, the other identity must
    # retire its lease. Everything both identities touch lives in one shared tree.
    import tempfile
    shared = Path(tempfile.mkdtemp(prefix="heavy-xuid-", dir="/tmp"))
    try:
        shared.chmod(0o755)
        locks = shared / "locks"
        locks.mkdir()
        locks.chmod(0o777)  # shared parent: writable by all, not sticky
        tools = shared / "bin"
        tools.mkdir()
        for name in ("sh", "dirname", "uname", "sleep"):
            (tools / name).symlink_to(shutil.which(name, path=os.defpath) or f"/bin/{name}")
        timeout = shared / "test-timeout"
        timeout.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} "
                           f'{shlex.quote(str(TIMEOUT_HELPER))} "$@"\n')
        timeout.chmod(0o755)
        env = _env(locks, PATH=str(tools), MIMIR_HEAVY_TIMEOUT_TOOL=str(timeout),
                   MIMIR_HEAVY_STALE_SECONDS="1")
        probe = subprocess.run([sys.executable, "-c", "pass"], user=holder_uid, group=holder_uid,
                               extra_groups=[], capture_output=True)
        if probe.returncode:
            pytest.skip("interpreter is not readable by an unprivileged identity here")

        def wrapper(uid: int, log: Path, *command: str) -> subprocess.Popen[bytes]:
            # Files, not pipes: the holder's orphaned command group would keep a pipe open.
            with log.open("wb") as output:
                return subprocess.Popen(["sh", str(SCRIPT), *command], env=env, user=uid, group=uid,
                                        extra_groups=[], umask=0o022, start_new_session=True,
                                        stdout=output, stderr=subprocess.STDOUT)

        scratch = shared / "scratch"
        scratch.mkdir()
        scratch.chmod(0o777)
        pid_file = scratch / "holder.pid"
        holder_log = shared / "holder.log"
        holder = wrapper(holder_uid, holder_log, sys.executable, "-c",
                         f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(300)")
        try:
            _wait(lambda: pid_file.exists() and (_lease(locks) / "heartbeat").exists())
        except AssertionError:
            os.killpg(holder.pid, signal.SIGKILL)
            holder.wait(timeout=15)
            raise AssertionError(holder_log.read_text()) from None
        # Kill the lease helper itself (the timeout tool runs it in its own group)
        # before its command, so it can never release: a dead holder, not an exit.
        helper_pid = json.loads((_lease(locks) / "owner").read_text())["pid"]
        os.kill(helper_pid, signal.SIGKILL)
        os.killpg(holder.pid, signal.SIGKILL)
        holder.wait(timeout=15)
        os.kill(int(pid_file.read_text()), signal.SIGKILL)  # the orphaned command group
        assert _lease(locks).stat().st_uid == holder_uid

        done = scratch / "waiter-ran"
        waiter_log = shared / "waiter.log"
        waiter = wrapper(waiter_uid, waiter_log, sys.executable, "-c", f"open({str(done)!r}, 'w').close()")
        waiter.wait(timeout=60)
        err = waiter_log.read_text()
        assert waiter.returncode == 0, err
        assert done.exists()
        assert err.count("retired stale lease") == 1, err
        assert not _lease(locks).exists()
    finally:
        shutil.rmtree(shared, ignore_errors=True)


@pytest.mark.skipif(sys.platform != "linux" or os.geteuid() != 0,
                    reason="two real identities need Linux root")
def test_waiter_of_another_identity_waits_through_generation_initialization(tmp_path: Path) -> None:
    # A umask-077 holder of one uid is paused between publishing its generation
    # and taking ownership; a real waiter of another uid (no shared group) must
    # keep waiting, then run once the holder releases.
    import tempfile
    shared = Path(tempfile.mkdtemp(prefix="heavy-xinit-", dir="/tmp"))
    try:
        shared.chmod(0o755)
        locks, scratch = shared / "locks", shared / "scratch"
        for directory in (locks, scratch):
            directory.mkdir()
            directory.chmod(0o777)
        tools = shared / "bin"
        tools.mkdir()
        for name in ("sh", "dirname", "uname", "sleep"):
            (tools / name).symlink_to(shutil.which(name, path=os.defpath) or f"/bin/{name}")
        timeout = shared / "test-timeout"
        timeout.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} "
                           f'{shlex.quote(str(TIMEOUT_HELPER))} "$@"\n')
        timeout.chmod(0o755)
        lease = _lease(locks)
        published, go, done = scratch / "published", scratch / "go", scratch / "waiter-ran"
        harness = shared / "holder.py"
        harness.write_text(f"""
import importlib.util, os, time
from pathlib import Path
spec = importlib.util.spec_from_file_location("heavy_run_holder", {str(SCRIPT.with_suffix('.py'))!r})
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
real_mkdir = module.os.mkdir
def paused(path, mode=0o777, **kwargs):
    real_mkdir(path, mode, **kwargs)
    Path({str(published)!r}).touch()
    while not Path({str(go)!r}).exists():
        time.sleep(.01)
module.os.mkdir = paused
fd = module._acquire(Path({str(lease)!r}), 600, 1)
module._release(Path({str(lease)!r}), fd)
os.close(fd)
""")
        harness.chmod(0o644)
        probe = subprocess.run([sys.executable, "-c", "pass"], user=41001, group=41001,
                               extra_groups=[], capture_output=True)
        if probe.returncode:
            pytest.skip("interpreter is not readable by an unprivileged identity here")
        holder = subprocess.Popen([sys.executable, str(harness)], user=41001, group=41001,
                                  extra_groups=[], umask=0o077,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        _wait(published.exists)
        waiter_log = shared / "waiter.log"
        with waiter_log.open("wb") as output:
            waiter = subprocess.Popen(
                ["sh", str(SCRIPT), sys.executable, "-c", f"open({str(done)!r}, 'w').close()"],
                env=_env(locks, PATH=str(tools), MIMIR_HEAVY_TIMEOUT_TOOL=str(timeout)),
                user=41002, group=41002, extra_groups=[], umask=0o022,
                stdout=output, stderr=subprocess.STDOUT)
        time.sleep(1.5)  # the waiter observes the initializing generation repeatedly
        assert waiter.poll() is None, waiter_log.read_text()
        assert not done.exists()
        go.touch()
        assert holder.wait(timeout=30) == 0, holder.stderr.read()
        assert waiter.wait(timeout=60) == 0, waiter_log.read_text()
        assert done.exists()
    finally:
        shutil.rmtree(shared, ignore_errors=True)
