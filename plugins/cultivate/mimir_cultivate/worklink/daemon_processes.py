"""Process witnesses used only inside the worklink-identity session executor.

Never accept PID lists from the controller RPC. Roots come from owned daemon
shells; retained witnesses live in the executor's private controller record.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import signal

from .daemon_client import DaemonClientError


def _process(pid: int) -> tuple[int, int, str] | None:
    """Return parent, kernel start ticks, state; disappearance is the only absence."""
    try:
        path = Path('/proc') / str(pid)
        raw = (path / 'stat').read_text()
        fields = raw[raw.rindex(')') + 2:].split()
        return int(fields[1]), int(fields[19]), fields[0]
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, ValueError, IndexError):
        raise DaemonClientError('process identity unreadable') from None


def snapshot_descendants(pid: int) -> list[list[int]]:
    if type(pid) is not int or pid <= 1:
        raise DaemonClientError('invalid shell process identity')
    root = _process(pid)
    if root is None:
        return []
    # Read once before following links so reparented children cannot be silently
    # assigned by process group or by their worktree location.
    processes = {}
    try:
        for entry in Path('/proc').iterdir():
            if entry.name.isdecimal():
                if len(processes) >= 65536:
                    raise DaemonClientError('process table exceeds bound')
                info = _process(int(entry.name))
                if info is not None:
                    processes[int(entry.name)] = info
    except OSError:
        raise DaemonClientError('process table unreadable') from None
    fresh = _process(pid)
    if fresh is None or fresh[:2] != root[:2] or processes.get(pid, ())[:2] != root[:2]:
        raise DaemonClientError('shell process identity changed')
    owned = {pid}
    for _ in range(len(processes) + 1):
        children = {child for child, (parent, _, _) in processes.items() if parent in owned}
        expanded = owned | children
        if expanded == owned:
            break
        owned = expanded
    return [[child, processes[child][1]] for child in sorted(owned - {pid})]


def _alive(witness: list[int]) -> bool:
    info = _process(witness[0])
    return info is not None and info[1] == witness[1] and info[2] not in {'Z', 'X'}


def _signal(witness: list[int], sig: int) -> None:
    """Pin the task before checking start ticks, eliminating kill(PID)'s reuse race."""
    try:
        fd = os.pidfd_open(witness[0])
    except ProcessLookupError:
        return
    except (OSError, AttributeError):
        raise DaemonClientError('process handle unavailable') from None
    try:
        if _alive(witness):
            signal.pidfd_send_signal(fd, sig)
    except ProcessLookupError:
        pass
    except (OSError, AttributeError):
        raise DaemonClientError('process signal refused') from None
    finally:
        os.close(fd)


async def terminate_descendants(witnesses: list[list[int]], *, wait_s: float = 0.5) -> list[list[int]]:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        remaining = [w for w in witnesses if _alive(w)]
        for witness in remaining:
            _signal(witness, sig)
        deadline = asyncio.get_running_loop().time() + wait_s
        while any(_alive(w) for w in remaining) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.02)
    return [w for w in witnesses if _alive(w)]
