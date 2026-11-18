"""Process witnesses used only inside the worklink-identity session executor.

Never accept PID lists from the controller RPC. Roots come from owned daemon
shells; retained witnesses live in the executor's private controller record.
"""
from __future__ import annotations

import asyncio
import errno
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


def _fallback_signal(witness: list[int], sig: int, cause: str) -> None:
    """Same-uid fallback when pidfds are unavailable.

    Unlike pidfds, kill(PID) has a reuse window between identity read and signal.
    Constrain it to same-uid tasks and check start ticks immediately before and
    after signalling. A changed identity is gone for this witness, never followed.
    """
    try:
        if not _alive(witness):
            return
        if os.stat(f'/proc/{witness[0]}').st_uid != os.geteuid():
            raise DaemonClientError(f'{cause}; fallback process owner mismatch')
        if not _alive(witness):
            return
        os.kill(witness[0], sig)
        _alive(witness)  # Revalidate after kill; subsequent waits use this same witness.
    except ProcessLookupError:
        return
    except FileNotFoundError:
        return
    except OSError as exc:
        name = errno.errorcode.get(exc.errno, 'unknown')
        raise DaemonClientError(f'{cause}; fallback signal refused: errno {name}') from None


def _signal(witness: list[int], sig: int) -> None:
    """Prefer a pinned pidfd; otherwise use the documented same-uid fallback."""
    try:
        fd = os.pidfd_open(witness[0])
    except ProcessLookupError:
        return
    except AttributeError:
        return _fallback_signal(witness, sig, 'process handle unavailable: Python API missing')
    except OSError as exc:
        # Only a stdlib errno name is safe diagnostic data; never render the
        # exception's free-form message, filename, or an arbitrary errno value.
        name = errno.errorcode.get(exc.errno, 'unknown')
        return _fallback_signal(witness, sig, f'process handle unavailable: errno {name}')
    try:
        if _alive(witness):
            signal.pidfd_send_signal(fd, sig)
    except ProcessLookupError:
        pass
    except AttributeError:
        _fallback_signal(witness, sig, 'process signal refused: Python API missing')
    except OSError as exc:
        name = errno.errorcode.get(exc.errno, 'unknown')
        raise DaemonClientError(f'process signal refused: errno {name}') from None
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
