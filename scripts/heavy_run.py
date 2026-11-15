"""Portable directory lease implementation; public entry point is heavy_run.sh."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid


_pending_signal: int | None = None


class Interrupted(Exception):
    def __init__(self, signum: int):
        self.signum = signum


def _interrupt(signum: int, _frame: object) -> None:
    # Defer until ownership bookkeeping is complete: never interrupt
    # mkdir->open or Popen->child assignment.
    global _pending_signal
    _pending_signal = signum


def _check_signal() -> None:
    if _pending_signal is not None:
        raise Interrupted(_pending_signal)


def _seconds(name: str, default: str) -> float:
    value = float(os.environ.get(name, default))
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return value


def _identity(fd: int) -> tuple[int, int]:
    stat = os.fstat(fd)
    return stat.st_dev, stat.st_ino


def _is_current(path: Path, fd: int) -> bool:
    try:
        stat = path.stat(follow_symlinks=False)
        return (stat.st_dev, stat.st_ino) == _identity(fd)
    except FileNotFoundError:
        return False


def _write(fd: int, name: str, value: str) -> None:
    # Relative to the held generation, never accidentally update a newer lease.
    with open(os.open(name + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                      0o644, dir_fd=fd), "w") as out:
        out.write(value)
    os.rename(name + ".tmp", name, src_dir_fd=fd, dst_dir_fd=fd)


def _counter(fd: int) -> str | None:
    try:
        with open(os.open("heartbeat", os.O_RDONLY, dir_fd=fd)) as source:
            return source.read()
    except FileNotFoundError:
        # A creator may not yet have written its initial counter. Give it the
        # same full monotonic grace period as a frozen heartbeat.
        return None


def _retire(path: Path) -> None:
    retired = path.with_name(path.name + ".retired-" + uuid.uuid4().hex)
    path.rename(retired)
    shutil.rmtree(retired)


def _break_stale(path: Path, fd: int, observed: str | None) -> bool:
    # Claim in the observed generation. Rename alone permits a delayed second
    # waiter to steal a NEW holder after the first rename and re-acquisition.
    try:
        os.mkdir("breaking", dir_fd=fd)
    except (FileExistsError, FileNotFoundError):
        return False
    if not _is_current(path, fd) or _counter(fd) != observed:
        os.rmdir("breaking", dir_fd=fd)
        return False
    try:
        _retire(path)
    except FileNotFoundError:
        return False
    print(f"heavy_run: retired stale lease {path}", file=sys.stderr, flush=True)
    return True


def _release(path: Path, fd: int) -> None:
    # Release and stale breaking arbitrate on the SAME generation. Otherwise a
    # resuming holder can release/reacquire between a breaker's check and rename.
    while _is_current(path, fd):
        try:
            os.mkdir("breaking", dir_fd=fd)
        except FileExistsError:
            time.sleep(0.01)
            continue
        except FileNotFoundError:
            return
        if _is_current(path, fd):
            _retire(path)
        return


def _acquire(path: Path, stale: float, interval: float) -> int:
    previous: tuple[tuple[int, int], str | None] | None = None
    unchanged_since = time.monotonic()
    while True:
        _check_signal()
        try:
            path.mkdir()
        except FileExistsError:
            try:
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            except FileNotFoundError:
                continue
            try:
                observation = (_identity(fd), _counter(fd))
                now = time.monotonic()
                if observation != previous:
                    previous, unchanged_since = observation, now
                elif now - unchanged_since >= stale:
                    if _break_stale(path, fd, observation[1]):
                        previous = None
                        continue
            finally:
                os.close(fd)
            time.sleep(min(interval, 0.1))
        else:
            return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)


def run(command: list[str]) -> int:
    path = Path(os.environ["MIMIR_HEAVY_LOCK_FILE"] + ".lease").absolute()
    stale = _seconds("MIMIR_HEAVY_STALE_SECONDS", "600")
    interval = _seconds("MIMIR_HEAVY_HEARTBEAT_SECONDS", "1")
    if interval >= stale:
        raise ValueError("heartbeat interval must be less than stale threshold")
    # Probe actual mkdir permission even when an existing lease means waiting.
    with tempfile.TemporaryDirectory(prefix=".heavy-probe-", dir=path.parent):
        pass

    fd: int | None = None
    child: subprocess.Popen | None = None
    handled = {signal.SIGINT, signal.SIGTERM}
    for sig in handled:
        signal.signal(sig, _interrupt)
    try:
        fd = _acquire(path, stale, interval)
        _write(fd, "owner", json.dumps({
            "host": socket.gethostname(), "pid": os.getpid(),
            "start_time": time.time(), "timeout": os.environ.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200"),
        }) + "\n")
        _write(fd, "heartbeat", "0\n")
        _check_signal()
        child = subprocess.Popen(command, start_new_session=True,
                                 env={**os.environ, "MIMIR_HEAVY_LOCK_HELD": "1"})
        counter = 0
        next_heartbeat = time.monotonic() + interval
        while True:
            _check_signal()
            try:
                status = child.wait(timeout=min(interval, 0.1))
                return status if status >= 0 else 128 - status
            except subprocess.TimeoutExpired:
                if time.monotonic() >= next_heartbeat and child.poll() is None:
                    counter += 1
                    _write(fd, "heartbeat", f"{counter}\n")
                    next_heartbeat = time.monotonic() + interval
    except Interrupted as exc:
        return 128 + exc.signum
    finally:
        # Repeated termination must not interrupt kill/wait/release ordering.
        for sig in handled:
            signal.signal(sig, signal.SIG_IGN)
        if child is not None:
            # Also clean up descendants left behind by an exited group leader.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
        if fd is not None:
            try:
                _release(path, fd)
            finally:
                os.close(fd)


if __name__ == "__main__":
    try:
        sys.exit(run(sys.argv[1:]))
    except (OSError, ValueError) as exc:
        print(f"heavy_run: cannot use configured lease: {exc}", file=sys.stderr)
        sys.exit(1)
