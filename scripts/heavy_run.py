"""Portable directory lease implementation; public entry point is heavy_run.sh.

v2 (2026-11-21): two slots and two tiers.

* ``MIMIR_HEAVY_TIER=medium`` holds ONE slot. Two medium jobs can run at once.
  Use it for verification, builds and tests whose timing is not being measured.
* ``MIMIR_HEAVY_TIER=large`` (the default) holds BOTH slots, so it runs alone.
  Use it for measurements and anything that needs a quiet machine.

Slots are the lease directories ``<file>.lease`` (slot 1, the v1 name) and
``<file>.lease2`` (slot 2). Every lease directory uses the v1 protocol: atomic
mkdir, a heartbeat counter for staleness, and per-generation breaker claims.

Compatibility: a v1 helper knows only slot 1 and writes no ``tier`` in its owner
file. A slot-1 holder whose tier is missing or unreadable counts as large, and a
medium job may take slot 2 only while slot 1 is held by a medium job, so a v2
medium never starts beside a v1 holder. The reverse is NOT guaranteed: when the
slot-1 medium of a medium pair exits first, a waiting v1 helper (which never
looks at slot 2 or the queue) can take slot 1 and run beside the slot-2
medium. v2 large jobs are not affected; they wait for both slots. Use the
medium tier only once every participant on the lease runs this helper.

Jobs start in arrival order (owner design, 2026-11-21). Each waiter takes a
FIFO ticket in ``<file>.queue/``; only the oldest live ticket may take slots. A
large job at the head takes whichever slot is free and holds it while waiting for
the other, so nothing behind it overtakes and nothing is starved. The cost is
that a slot can sit idle while a large job waits for the second one.
"""
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

TIERS = ("medium", "large")


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
    # Waiters running as other identities read the counter: 0644 whatever the umask.
    with open(os.open(name + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                      0o644, dir_fd=fd), "w") as out:
        os.fchmod(out.fileno(), 0o644)
        out.write(value)
    os.rename(name + ".tmp", name, src_dir_fd=fd, dst_dir_fd=fd)


def _read(fd: int, name: str) -> str | None:
    try:
        with open(os.open(name, os.O_RDONLY, dir_fd=fd)) as source:
            return source.read()
    except FileNotFoundError:
        return None


def _counter(fd: int) -> str | None:
    # A creator may not yet have written its initial counter. Give it the
    # same full monotonic grace period as a frozen heartbeat.
    return _read(fd, "heartbeat")


def _cleanup_warning(path: Path, exc: OSError) -> None:
    # Repr escapes control characters in configured paths; don't print arbitrary
    # exception text (which can also contain paths or embedded newlines).
    print(f"heavy_run: retired lease cleanup deferred for {str(path)!r} "
          f"(errno={exc.errno})", file=sys.stderr, flush=True)


def _remove_retired(retired: Path) -> None:
    try:
        shutil.rmtree(retired)
    except FileNotFoundError:
        pass  # Another sweeper already removed it.
    except OSError as exc:
        # A losing waiter can still mkdir through its old generation fd while
        # rmtree runs. The rename already settled ownership; deletion is only
        # housekeeping. Try once, then leave it for a later acquisition/release.
        _cleanup_warning(retired, exc)


def _sweep_retired(path: Path) -> None:
    prefix = path.name + ".retired-"
    try:
        with os.scandir(path.parent) as entries:
            for entry in entries:
                # Literal prefix, not a glob: configured names can contain glob
                # metacharacters. Never sweep the live lease or follow symlinks.
                if entry.name.startswith(prefix) and entry.is_dir(follow_symlinks=False):
                    _remove_retired(Path(entry.path))
    except OSError as exc:
        _cleanup_warning(path.parent, exc)


def _retire(path: Path) -> None:
    retired = path.with_name(path.name + ".retired-" + uuid.uuid4().hex)
    path.rename(retired)
    _remove_retired(retired)


def _break_stale(path: Path, fd: int, observed: str | None) -> bool:
    # Claim in the observed generation. Rename alone permits a delayed second
    # waiter to steal a NEW holder after the first rename and re-acquisition.
    try:
        os.mkdir("breaking", dir_fd=fd)
    except (FileExistsError, FileNotFoundError):
        return False
    if not _is_current(path, fd) or _counter(fd) != observed:
        try:
            os.rmdir("breaking", dir_fd=fd)
        except FileNotFoundError:
            pass  # A retired-generation cleanup removed our claim already.
        return False
    try:
        _retire(path)
    except FileNotFoundError:
        return False
    print(f"heavy_run: retired stale lease {path}", file=sys.stderr, flush=True)
    return True


def _release(path: Path, fd: int) -> None:
    _sweep_retired(path)
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


def _publish_generation(path: Path) -> None:
    """Create the generation already open to every participant identity.

    Participants are different uids (host user, mimir, worklink) without a
    shared group: a waiter must open the generation and a stale waiter must
    create its breaker claim inside it. A mode fixed after mkdir would expose a
    umask-restricted directory to concurrent waiters, so create it 0777 under a
    zero umask. This helper is single-threaded and its command has not started,
    so the temporary umask is observable by nothing else.
    """
    previous = os.umask(0)
    try:
        os.mkdir(path, 0o777)
    finally:
        os.umask(previous)


class Lease:
    """One lease directory: non-blocking take, stale tracking, holder inspection."""

    def __init__(self, path: Path, stale: float):
        self.path = path
        self.stale = stale
        self._previous: tuple[tuple[int, int], str | None] | None = None
        self._unchanged_since = time.monotonic()

    def try_take(self) -> int | None:
        """Return an fd for a freshly created generation, or None if held."""
        try:
            _publish_generation(self.path)
        except FileExistsError:
            pass
        else:
            return os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None  # Released between mkdir and open: retry next round.
        try:
            observation = (_identity(fd), _counter(fd))
            now = time.monotonic()
            if observation != self._previous:
                self._previous, self._unchanged_since = observation, now
            elif now - self._unchanged_since >= self.stale:
                if _break_stale(self.path, fd, observation[1]):
                    self._previous = None
        finally:
            os.close(fd)
        return None

    def holder_tier(self) -> str | None:
        """None if free; the holder's tier if held. Unknown counts as large."""
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        try:
            owner = _read(fd, "owner")
        finally:
            os.close(fd)
        try:
            tier = json.loads(owner).get("tier") if owner else None
        except (ValueError, AttributeError):
            tier = None
        return tier if tier in TIERS else "large"


def _hold(fd: int, tier: str, role: str) -> None:
    _write(fd, "owner", json.dumps({
        "host": socket.gethostname(), "pid": os.getpid(),
        "start_time": time.time(),
        "timeout": os.environ.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200"),
        "tier": tier, "role": role,
    }) + "\n")
    _write(fd, "heartbeat", "0\n")


class Heartbeat:
    def __init__(self) -> None:
        self.held: dict[Path, int] = {}
        self.counter = 0

    def beat(self) -> None:
        self.counter += 1
        for fd in self.held.values():
            _write(fd, "heartbeat", f"{self.counter}\n")


class Queue:
    """FIFO tickets (owner design, 2026-11-21).

    Each waiter creates a ticket directory ``<file>.queue/<n>`` (atomic mkdir,
    n = highest waiting ticket + 1) and heartbeats it like a lease. Only the
    lowest live ticket may take slots; once a job holds what it needs it drops
    its ticket. So jobs start in arrival order: a large job at the head takes
    one free slot and holds it while it waits for the other, and nobody behind
    it can overtake. A ticket whose heartbeat stops goes stale and is broken
    exactly like a lease, so a crashed waiter cannot block the queue.

    Numbering comes from a directory snapshot, so it is best-effort order, not
    a reservation: an enqueuer delayed between the snapshot and its mkdir can
    publish a number below a job that is already waiting. Safety does not rest
    on the order. A job that is not the head never keeps a reserved slot, so
    two waiting large jobs can never hold one slot each.
    """

    def __init__(self, base: str, stale: float):
        self.dir = Path(base + ".queue").absolute()
        self.stale = stale
        self.leases: dict[str, Lease] = {}

    def _ensure_dir(self) -> None:
        try:
            _publish_generation(self.dir)
        except FileExistsError:
            pass

    def _waiting(self) -> list[str]:
        try:
            return sorted(n for n in os.listdir(self.dir) if n.isdigit())
        except FileNotFoundError:
            return []

    def enqueue(self) -> tuple[Path, int]:
        self._ensure_dir()
        try:  # Housekeeping: remove broken (retired) tickets.
            for name in os.listdir(self.dir):
                if ".retired-" in name:
                    _remove_retired(self.dir / name)
        except OSError as exc:
            _cleanup_warning(self.dir, exc)
        while True:
            _check_signal()
            waiting = self._waiting()
            n = (int(waiting[-1]) + 1) if waiting else 1
            path = self.dir / f"{n:012d}"
            try:
                _publish_generation(path)
            except FileExistsError:
                continue  # Lost the race for that number: take the next one.
            except FileNotFoundError:
                self._ensure_dir()
                continue
            return path, os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

    def at_head(self, mine: Path) -> bool:
        """True when no live ticket is ahead of ours. Breaks stale ones."""
        for name in self._waiting():
            if name >= mine.name:
                return True
            lease = self.leases.setdefault(name, Lease(self.dir / name, self.stale))
            _probe_stale(lease)
            if lease.path.exists():
                return False
            self.leases.pop(name, None)
        return True


def _acquire(base: str, tier: str, stale: float, interval: float, hb: Heartbeat) -> None:
    slot1 = Lease(Path(base + ".lease").absolute(), stale)
    slot2 = Lease(Path(base + ".lease2").absolute(), stale)
    for lease in (slot1, slot2):
        _sweep_retired(lease.path)
    # Transition: helpers started before the FIFO queue (2026-11-21) mark a
    # waiting large job with ``<file>.intent``. Honour it so they are not skipped.
    legacy_intent = Lease(Path(base + ".intent").absolute(), stale)
    queue = Queue(base, stale)
    ticket, tfd = queue.enqueue()
    hb.held[ticket] = tfd
    _hold(tfd, tier, "ticket")
    next_beat = time.monotonic() + interval

    def take(lease: Lease, role: str) -> bool:
        fd = lease.try_take()
        if fd is None:
            return False
        hb.held[lease.path] = fd
        _hold(fd, tier, role)
        return True

    def done() -> None:
        fd = hb.held.pop(ticket)
        try:
            _release(ticket, fd)
        finally:
            os.close(fd)

    def give_back() -> None:
        # Only a large job keeps a slot before its command starts, and only as
        # the head of the queue. Its command has not started, so this is safe.
        for path in (slot2.path, slot1.path):
            fd = hb.held.pop(path, None)
            if fd is not None:
                try:
                    _release(path, fd)
                finally:
                    os.close(fd)

    while True:
        _check_signal()
        if time.monotonic() >= next_beat:
            hb.beat()  # Keep our ticket and any slot we already hold alive.
            next_beat = time.monotonic() + interval
        if not queue.at_head(ticket):
            # Ticket numbers come from a directory snapshot, so a delayed
            # enqueuer can publish a lower number ahead of this job. Holding a
            # slot while another job is head could form a cycle with it that
            # heartbeats keep alive, so give the reservation back.
            give_back()
        elif legacy_intent.path.exists():
            # A pre-queue large helper is waiting: it holds the intent and
            # takes slot 1 then slot 2. Holding either slot here could form a
            # cycle with it, so stand aside until it runs. A dead waiter's
            # intent expires through the stale probe.
            give_back()
            _probe_stale(legacy_intent)
        elif tier == "large":
            if slot1.path not in hb.held:
                take(slot1, "slot1")
            if slot2.path not in hb.held:
                take(slot2, "slot2")
            if slot1.path in hb.held and slot2.path in hb.held:
                done()
                return
        else:
            if take(slot1, "slot1"):
                done()
                return
            # Slot 2 only beside a medium: a v1 or unknown slot-1 holder
            # counts as large and must keep the machine to itself.
            if slot1.holder_tier() == "medium" and take(slot2, "slot2"):
                done()
                return
            _probe_stale(slot2)
        time.sleep(min(interval, 0.1))


def _probe_stale(lease: Lease) -> None:
    """Track a lease we are not trying to take, so a dead holder is broken.

    Only reached when slot 1 is held by a large or unknown job, so slot 2 must
    not be taken; but a SIGKILLed medium on slot 2 must still go stale.
    """
    try:
        fd = os.open(lease.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return
    try:
        observation = (_identity(fd), _counter(fd))
        now = time.monotonic()
        if observation != lease._previous:
            lease._previous, lease._unchanged_since = observation, now
        elif now - lease._unchanged_since >= lease.stale:
            if _break_stale(lease.path, fd, observation[1]):
                lease._previous = None
    finally:
        os.close(fd)


def run(command: list[str]) -> int:
    base = os.environ["MIMIR_HEAVY_LOCK_FILE"]
    tier = os.environ.get("MIMIR_HEAVY_TIER", "large") or "large"
    if tier not in TIERS:
        raise ValueError("MIMIR_HEAVY_TIER must be medium or large")
    stale = _seconds("MIMIR_HEAVY_STALE_SECONDS", "600")
    interval = _seconds("MIMIR_HEAVY_HEARTBEAT_SECONDS", "1")
    if interval >= stale:
        raise ValueError("heartbeat interval must be less than stale threshold")
    # Probe actual mkdir permission even when an existing lease means waiting.
    with tempfile.TemporaryDirectory(prefix=".heavy-probe-",
                                     dir=Path(base + ".lease").absolute().parent):
        pass

    hb = Heartbeat()
    child: subprocess.Popen | None = None
    handled = {signal.SIGINT, signal.SIGTERM}
    for sig in handled:
        signal.signal(sig, _interrupt)
    try:
        _acquire(base, tier, stale, interval, hb)
        _check_signal()
        child = subprocess.Popen(command, start_new_session=True,
                                 env={**os.environ, "MIMIR_HEAVY_LOCK_HELD": "1"})
        next_heartbeat = time.monotonic() + interval
        while True:
            _check_signal()
            try:
                status = child.wait(timeout=min(interval, 0.1))
                return status if status >= 0 else 128 - status
            except subprocess.TimeoutExpired:
                if time.monotonic() >= next_heartbeat and child.poll() is None:
                    hb.beat()
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
        # Release in reverse acquisition order: slot 2, then slot 1.
        for path in sorted(hb.held, key=lambda p: p.name, reverse=True):
            fd = hb.held[path]
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
