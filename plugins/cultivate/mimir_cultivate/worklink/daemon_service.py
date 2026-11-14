"""Privileged preparation followed by a clean native exec into Worklink.

The supervisor owns only its serve child. Session cancellation must never use
this service's shutdown path. Same-uid build access to daemon credentials is an
intentional boundary; agent credentials are not part of this projection.
"""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import signal
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from mimir.worklink_daemon_activation import resolve_activation
from .daemon_config import (
    DAEMON_ROOT, DAEMON_URL, daemon_configuration, daemon_environment,
    make_launch_snapshot, resolve_daemon_credentials,
)
from .identities import WorklinkIdentities, assert_worklink_identity, get_identities

PYTHON = "/opt/mimir-worklink/venv/bin/python"
# Session RPC helpers still use this transport; daemon startup must not.
UID_HELPER = "/command/s6-setuidgid"
SERVE_ARGV = ("/opt/mimir-opencode/bin/opencode", "serve", "--hostname", "127.0.0.1", "--port", "4096")
SUPERVISOR_ARGV = (PYTHON, "-m", "mimir_cultivate.worklink.daemon_service", "--supervise")


def exec_worklink(argv: tuple[str, ...], environment: dict[str, str], *, cwd: str) -> None:
    """Root-only launch shared by the service and the real-image wire fixture.

    Drop before exec (not a preexec_fn in the threaded supervisor). The fresh
    supervisor and all daemon tool children inherit the legacy worker boundary.
    """
    from .worker_exec import _drop_worker_privileges

    if os.getuid() != 0 or os.geteuid() != 0:
        raise PrivateStateError("privilege preparation requires root")
    os.chdir(cwd)
    _drop_worker_privileges()
    os.umask(0o002)  # Worker output must remain removable by the controller group.
    os.execve(argv[0], argv, environment)


@dataclass(frozen=True)
class RuntimePaths:
    daemon: Path = Path(DAEMON_ROOT)
    run: Path = Path("/run/mimir-worklink")

    @property
    def api(self) -> Path:
        return self.daemon / "api.json"

    @property
    def status(self) -> Path:
        return self.run / "status/status.json"

    @property
    def snapshot(self) -> Path:
        return self.run / "agent/launch.json"


class PrivateStateError(ValueError):
    """Invalid local state; never include file contents in diagnostics."""


@contextmanager
def directory_fd(path: Path):
    """Walk every component without following links, including parents."""
    if not path.is_absolute() or ".." in path.parts:
        raise PrivateStateError("invalid state path")
    fd = os.open("/", (os.O_RDONLY if path == Path("/") else os.O_PATH) | os.O_DIRECTORY)
    try:
        for index, component in enumerate(path.parts[1:], start=1):
            # Parents may be search-only (the image's root-owned 0711 state
            # root). Keep the final fd readable for fchmod/fchown and fsync.
            access = os.O_RDONLY if index == len(path.parts) - 1 else os.O_PATH
            next_fd = os.open(component, access | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        yield fd
    finally:
        os.close(fd)


def read_json(path: Path, *, uid: int, mode: int, bound: int = 16384) -> object:
    with directory_fd(path.parent) as parent:
        parent_info = os.fstat(parent)
        if (parent_info.st_uid != uid
                or stat.S_IMODE(parent_info.st_mode) != (0o755 if mode == 0o644 else 0o700)):
            raise PrivateStateError("invalid state directory")
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid
                    or stat.S_IMODE(info.st_mode) != mode or info.st_nlink != 1
                    or info.st_size > bound):
                raise PrivateStateError("invalid state file")
            raw = os.read(fd, bound + 1)
            if len(raw) > bound:
                raise PrivateStateError("invalid state file")
            return json.loads(raw)
        finally:
            os.close(fd)


def atomic_json(path: Path, payload: object, *, uid: int, gid: int, mode: int) -> None:
    raw = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
    if len(raw) > 16384:
        raise PrivateStateError("state file too large")
    with directory_fd(path.parent) as parent:
        name = ".worklink-" + secrets.token_hex(16)
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        try:
            os.fchown(fd, uid, gid)
            os.fchmod(fd, mode)
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(fd)
            os.rename(name, path.name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(fd)
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                pass


def prepare_directory(path: Path, *, uid: int, gid: int, mode: int) -> None:
    with directory_fd(path.parent) as parent:
        try:
            os.mkdir(path.name, mode, dir_fd=parent)
        except FileExistsError:
            pass
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            os.fchown(fd, uid, gid)
            os.fchmod(fd, mode)
        finally:
            os.close(fd)


def protect_agent_paths(environ: dict[str, str], identities: WorklinkIdentities) -> None:
    """Restrict only named secret roots, never recursively chown a checkout."""
    homes = {Path("/home/mimir"), Path(environ.get("MIMIR_HOME", "/home/mimir"))}
    for path in homes | {Path(environ.get("CODEX_HOME", "/home/mimir/.codex"))}:
        try:
            with directory_fd(path) as fd:
                os.fchown(fd, identities.mimir_uid, -1)
                os.fchmod(fd, 0o700)
        except FileNotFoundError:
            if path in homes:
                raise PrivateStateError("agent home unavailable") from None
    # A compose file is not normally mounted into the container. When present
    # below an agent home it must retain the same owner-only boundary.
    for home in homes:
        with directory_fd(home) as parent:
            try:
                fd = os.open("compose.env", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            except FileNotFoundError:
                continue
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise PrivateStateError("invalid agent secret file")
                os.fchown(fd, identities.mimir_uid, -1)
                os.fchmod(fd, 0o600)
            finally:
                os.close(fd)


def prepare(environ: dict[str, str], identities: WorklinkIdentities,
            paths: RuntimePaths = RuntimePaths()) -> dict[str, str]:
    credentials = resolve_daemon_credentials(environ)
    configuration = daemon_configuration(environ, credentials)
    generation = str(uuid4())
    password = secrets.token_urlsafe(48)
    protect_agent_paths(environ, identities)
    prepare_directory(paths.daemon, uid=identities.worklink_uid, gid=identities.worklink_gid, mode=0o700)
    for relative in ("home", "tmp", "config", "config/opencode", "data", "state", "cache", "sessions"):
        prepare_directory(paths.daemon / relative, uid=identities.worklink_uid,
                          gid=identities.worklink_gid, mode=0o700)
    prepare_directory(paths.run, uid=0, gid=0, mode=0o711)
    # Never modify the executor's sibling socket directory.
    prepare_directory(paths.run / "status", uid=identities.worklink_uid,
                      gid=identities.worklink_gid, mode=0o755)
    prepare_directory(paths.run / "agent", uid=identities.mimir_uid, gid=-1, mode=0o700)
    atomic_json(paths.api, {"version": 1, "url": DAEMON_URL, "username": "opencode", "password": password},
                uid=identities.worklink_uid, gid=identities.worklink_gid, mode=0o600)
    atomic_json(paths.daemon / "config/opencode/opencode.json", configuration.config,
                uid=identities.worklink_uid, gid=identities.worklink_gid, mode=0o600)
    atomic_json(paths.daemon / "launch.json", {"version": 1, "launch_generation": generation,
                "telemetry_state": configuration.telemetry_state}, uid=identities.worklink_uid,
                gid=identities.worklink_gid, mode=0o600)
    atomic_json(paths.snapshot, make_launch_snapshot(credentials, generation).to_dict(),
                uid=identities.mimir_uid, gid=-1, mode=0o600)
    return daemon_environment(credentials, api_password=password)


def root_start(environ: dict[str, str]) -> None:
    if os.getuid() != 0 or os.geteuid() != 0:
        raise PrivateStateError("root preparation requires root")
    activation = resolve_activation(environ)
    if not activation.enabled:
        os.execve("/command/s6-pause", ("/command/s6-pause",), {"PATH": "/usr/bin:/bin"})
        return
    clean_env = prepare(environ, get_identities())
    exec_worklink(SUPERVISOR_ARGV, clean_env, cwd="/")


async def supervise(paths: RuntimePaths = RuntimePaths(), *, stop: asyncio.Event | None = None) -> int:
    from .daemon_health import inspect_and_publish

    identities = get_identities()
    assert_worklink_identity(identities)
    from .worker_exec import _verify_worker_identity
    _verify_worker_identity()
    os.umask(0o002)  # Worker output must remain removable by the controller group.
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    child = None
    waiter = None
    stopped = asyncio.create_task(stop.wait())
    try:
        child = await asyncio.create_subprocess_exec(*SERVE_ARGV, env=dict(os.environ), cwd=str(paths.daemon / "home"))
        waiter = asyncio.create_task(child.wait())
        while not stop.is_set() and child.returncode is None:
            await inspect_and_publish(paths, identities)
            await asyncio.wait((stopped, waiter), timeout=5, return_when=asyncio.FIRST_COMPLETED)
        return 0 if stop.is_set() else (child.returncode or 1)
    finally:
        if child is not None:
            if child.returncode is None:
                child.terminate()
                try:
                    await asyncio.wait_for(asyncio.shield(waiter), timeout=25)
                except TimeoutError:
                    child.kill()
            await child.wait()
            await inspect_and_publish(paths, identities, down=True)
        stopped.cancel()
        await asyncio.gather(stopped, return_exceptions=True)
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


def main() -> int:
    try:
        if sys.argv[1:] == ["--supervise"]:
            return asyncio.run(supervise())
        if sys.argv[1:]:
            raise PrivateStateError("invalid service mode")
        root_start(dict(os.environ))
        return 0
    except (OSError, ValueError, RuntimeError):
        print("Worklink daemon service unavailable", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
