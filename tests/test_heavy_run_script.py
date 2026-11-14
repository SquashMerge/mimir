"""Real local processes exercise the cross-host directory-lease protocol."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "heavy_run.sh"


def _env(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIMIR_HEAVY_")}
    env.update(MIMIR_HEAVY_LOCK_FILE=str(tmp_path / "heavy.lock"),
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
    assert child.wait(timeout=15) == status
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
    assert first.wait(timeout=15) == second.wait(timeout=15) == 0
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
    queued = _run(tmp_path, "touch", str(tmp_path / "unexpected"), MIMIR_HEAVY_LOCK_TIMEOUT="0.3")
    assert queued.returncode == 124 and not (tmp_path / "unexpected").exists()
    assert holder.wait(timeout=15) == 124
    assert not _lease(tmp_path).exists()


def test_heartbeat_stops_when_child_exits(tmp_path: Path, start) -> None:
    release = tmp_path / "release"
    holder = start(sys.executable, "-c", _blocking_payload(release))
    heartbeat = _lease(tmp_path) / "heartbeat"
    _wait(lambda: heartbeat.exists() and int(heartbeat.read_text()) >= 3)
    # Retain the generation fd to detect writes after detachment as well.
    fd = os.open(_lease(tmp_path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        release.touch()
        assert holder.wait(timeout=15) == 0
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
    outputs = [p.communicate(timeout=30) for p in waiters]
    assert all(p.returncode == 0 for p in waiters), outputs
    assert sum(err.count("retired stale lease") for _, err in outputs) == 1
    assert not _lease(tmp_path).exists()


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
    assert holder.wait(timeout=15) == 0
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
    result = _run(tmp_path, "true", **{override: str(timeout), "MIMIR_HEAVY_LOCK_TIMEOUT": "123"})
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
    result = _run(tmp_path, sys.executable, "-c", "pass", PATH=str(bindir))
    assert result.returncode == 0, result.stderr
    assert log.read_text() == expected


def test_requires_a_command(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 64 and "usage" in result.stderr
