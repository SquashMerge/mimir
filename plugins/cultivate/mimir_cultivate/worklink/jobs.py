"""Session-owned heavy jobs; the CLI has no authority to choose its shell policy.

The session executor starts the service from installed code with policy on stdin.
Clients can supply argv, never a shell program, environment, policy, or duration
report. The controller's existing lease relay remains the only source of credit.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import sys
import time

from .daemon_processes import _alive, snapshot_descendants, terminate_descendants

SOCKET_ENV = "WORKLINK_JOB_SOCKET"
AUTH_ENV = "WORKLINK_JOB_AUTH"
MAX_WAIT_S = 60.0  # Well below even the shortest supported backend shell cap.
LOG_BYTES = 256 * 1024
TAIL_BYTES = 16 * 1024
MAX_JOBS = 32  # A lifetime bound, including completed jobs and their logs.


def command_allowed(policy: list[str], argv: list[str]) -> bool:
    # Same anchored * / ? and trailing " *" semantics as OpenCode's permission
    # matcher. Keep the service independent of the registry's provider imports.
    command = shlex.join(argv)
    for rule in policy:
        if rule.startswith("worklink-job"):
            continue
        expression = re.escape(rule).replace(r"\*", ".*").replace(r"\?", ".")
        if rule.endswith(" *"):
            expression = expression[:-4] + r"(?: .*)?"
        if re.fullmatch(expression, command, flags=re.DOTALL) is not None:
            return True
    return False


def address(value: str) -> str:
    # Linux abstract sockets avoid sockaddr_un's path limit on private homes.
    return "\0" + value.removeprefix("@")


def bounded_timeout(value: object) -> float:
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise ValueError("invalid wait timeout")
    return min(float(value), MAX_WAIT_S)


@dataclass
class Job:
    identifier: str
    argv: list[str]
    cwd: Path
    tier: str
    state: str = "queued"
    exit_code: int | None = None
    log: bytearray = field(default_factory=bytearray)
    task: asyncio.Task | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


class JobService:
    def __init__(self, *, policy: list[str], checkout: Path, logs: Path,
                 environment: dict[str, str], token: str) -> None:
        self.policy = policy
        self.checkout = checkout.resolve()
        self.logs = logs
        self.environment = environment
        self.token = token
        self.jobs: dict[str, Job] = {}
        # Only one command tree runs at once. This also lets the subreaper
        # attribute double-forked descendants without trusting process groups.
        self.execution = asyncio.Lock()
        self.stopping = False
        self.stop = asyncio.Event()
        self.handlers: set[asyncio.Task] = set()
        self.cleanup_error = False
        self.holders: list[asyncio.StreamWriter] = []

    def append(self, job: Job, data: bytes) -> None:
        job.log.extend(data)
        del job.log[:-LOG_BYTES]
        path = self.logs / (job.identifier + ".log")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(job.log)

    def start(self, argv: object, cwd: object, tier: object) -> Job:
        if (not isinstance(argv, list) or not argv or len(argv) > 1024
                or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
                or sum(len(arg.encode()) for arg in argv) > 32768):
            raise ValueError("invalid command argv")
        # Helper permission is NOT permission to recursively execute arbitrary
        # payloads. Match only the original backend rules, not the helper rule.
        if not command_allowed(self.policy, argv):
            raise ValueError("command outside session shell allowlist")
        if tier not in {"medium", "large"}:
            raise ValueError("invalid heavy tier")
        if not isinstance(cwd, str) or not Path(cwd).resolve().is_relative_to(self.checkout):
            raise ValueError("job directory outside session checkout")
        if self.stopping or self.cleanup_error or len(self.jobs) >= MAX_JOBS:
            raise ValueError("session jobs closed or full")
        job = Job(secrets.token_hex(16), argv, Path(cwd).resolve(), tier)
        self.jobs[job.identifier] = job
        job.task = asyncio.create_task(self.run(job))
        return job

    async def run(self, job: Job) -> None:
        connection = None
        process = None
        pump = None
        try:
            async with self.execution:
                # Previously admitted jobs may have been waiting when another
                # job's retirement failed. They must fail closed as well.
                if self.cleanup_error:
                    raise RuntimeError("job process cleanup unverified")
                try:
                    environment = dict(self.environment)
                    if environment.get("WORKLINK_LEASE_SOCKET"):
                        reader, connection = await asyncio.open_unix_connection(
                            environment["WORKLINK_LEASE_SOCKET"], limit=1024)
                        connection.write((json.dumps({
                            "auth": environment["WORKLINK_LEASE_AUTH"], "tier": job.tier,
                        }) + "\n").encode())
                        await connection.drain()
                        if await reader.readline() != b"acquired\n":
                            raise ValueError("controller refused heavy lease")
                    elif environment.get("MIMIR_HEAVY_LOCK_FILE"):
                        raise ValueError("heavy jobs require the controller lease relay")
                    # Nested helpers MUST honor this signal before trying either
                    # the shared lock or the relay. Cultivate's installed .sh does.
                    environment["MIMIR_HEAVY_LOCK_HELD"] = "1"
                    environment["MIMIR_HEAVY_TIER"] = job.tier
                    launch = asyncio.create_task(asyncio.create_subprocess_exec(
                        *job.argv, cwd=job.cwd, env=environment,
                        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT, start_new_session=True,
                    ))
                    try:
                        process = await asyncio.shield(launch)
                    except asyncio.CancelledError:
                        # Cancellation must not lose a just-spawned process.
                        process = await launch
                        raise
                    job.state = "running"

                    async def output() -> None:
                        while data := await process.stdout.read(65536):
                            self.append(job, data)

                    pump = asyncio.create_task(output())
                    # Process.wait() can wait for inherited pipes after the
                    # leader exits. Notice that exit before cleaning adoptees.
                    while process.returncode is None:
                        await asyncio.sleep(0.02)
                    job.exit_code = process.returncode
                finally:
                    async def retire() -> None:
                        if process is None:
                            if connection is not None:
                                connection.close()
                                await connection.wait_closed()
                            return
                        # Include escaped process groups and reparented children.
                        # The installed service is a Linux subreaper.
                        try:
                            survivors = await terminate_descendants(snapshot_descendants(os.getpid()))
                            if survivors:
                                raise RuntimeError("job process cleanup unverified")
                            await process.wait()
                            if pump is not None:
                                await asyncio.wait_for(pump, 2)
                            # Reap adopted grandchildren after the direct child's
                            # asyncio watcher has consumed its status.
                            while True:
                                try:
                                    if os.waitpid(-1, os.WNOHANG)[0] == 0:
                                        break
                                except ChildProcessError:
                                    break
                        except BaseException:
                            self.cleanup_error = True
                            if connection is not None:
                                self.holders.append(connection)
                            raise
                        if connection is not None:
                            connection.close()
                            await connection.wait_closed()

                    retirement = asyncio.create_task(retire())
                    while True:
                        try:
                            await asyncio.shield(retirement)
                            break
                        except asyncio.CancelledError:
                            continue
        except asyncio.CancelledError:
            job.exit_code = 130
        except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
            self.append(job, f"worklink-job: {type(exc).__name__}\n".encode())
            job.exit_code = 1
        finally:
            if pump is not None and not pump.done():
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
            job.state = "done"
            job.done.set()

    def result(self, job: Job, tail: int = 80) -> dict:
        text = bytes(job.log[-TAIL_BYTES:]).decode("utf-8", "replace")
        return {"id": job.identifier, "state": job.state, "exit_code": job.exit_code,
                "log": "\n".join(text.splitlines()[-tail:]) if tail else ""}

    async def dispatch(self, request: dict) -> dict:
        if (not isinstance(request, dict) or not isinstance(request.get("auth"), str)
                or not hmac.compare_digest(request["auth"], self.token)):
            raise ValueError("unknown session job")
        op = request.get("op")
        if op == "start":
            return self.result(self.start(request.get("argv"), request.get("cwd"), request.get("tier")))
        if op == "active":
            return {"active": self.cleanup_error or any(not job.done.is_set() for job in self.jobs.values())}
        if op == "close":
            self.stopping = True  # Fence admission before cancelling anything.
            await self.close_jobs()
            return {"closed": True}
        job = self.jobs.get(request.get("id"))
        if job is None:
            raise ValueError("unknown session job")
        if op == "wait":
            timeout = bounded_timeout(request.get("timeout", MAX_WAIT_S))
            try:
                await asyncio.wait_for(job.done.wait(), timeout)
            except TimeoutError:
                pass
        elif op == "cancel":
            if not job.done.is_set():
                if not job.task.cancelling():
                    job.task.cancel()
                await asyncio.gather(job.task, return_exceptions=True)
                job.state, job.exit_code = "done", 130
                job.done.set()
            if self.cleanup_error:
                raise RuntimeError("job process cleanup unverified")
        elif op == "log":
            tail = request.get("tail", 80)
            if type(tail) is not int or not 0 <= tail <= 1000:
                raise ValueError("invalid log tail")
            return self.result(job, tail)
        else:
            raise ValueError("unknown job operation")
        return self.result(job)

    async def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            request = json.loads(await asyncio.wait_for(reader.readline(), 5))
            response = await self.dispatch(request)
        except (OSError, ValueError, TypeError, RuntimeError, TimeoutError):
            response = {"error": "job request refused"}
        try:
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
            if response.get("closed"):
                self.stop.set()
        except OSError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            self.handlers.discard(task)

    async def close_jobs(self) -> None:
        self.stopping = True
        for job in self.jobs.values():
            if not job.done.is_set() and not job.task.cancelling():
                job.task.cancel()
        await asyncio.gather(*(job.task for job in self.jobs.values()), return_exceptions=True)
        for job in self.jobs.values():
            if not job.done.is_set():
                job.state, job.exit_code = "done", 130
                job.done.set()
        if self.cleanup_error:
            survivors = await terminate_descendants(snapshot_descendants(os.getpid()))
            if survivors:
                raise RuntimeError("job process cleanup unverified")
            self.cleanup_error = False
            for connection in self.holders:
                connection.close()
                await connection.wait_closed()
            self.holders.clear()


async def rpc(reference: dict, op: str, **arguments: object) -> dict:
    reader, writer = await asyncio.open_unix_connection(address(reference["socket"]), limit=128 * 1024)
    try:
        writer.write((json.dumps({"auth": reference["auth"], "op": op, **arguments}) + "\n").encode())
        await writer.drain()
        response = json.loads(await asyncio.wait_for(reader.readline(), MAX_WAIT_S + 5))
        if "error" in response:
            raise ValueError(response["error"])
        return response
    finally:
        writer.close()
        await writer.wait_closed()


def reference_path(controller: Path, session: str) -> Path:
    return controller.with_name(f"{controller.stem}.{session}.jobs.json")


async def start_service(controller: Path, session: str, *, policy: list[str], checkout: Path,
                        environment: dict[str, str], deadline_ms: int) -> dict[str, str]:
    from .daemon_service import atomic_json

    logs = Path(environment["HOME"]) / (session + ".jobs")
    logs.mkdir(mode=0o700)
    reference = {"socket": "@wl-job-" + secrets.token_hex(16), "auth": secrets.token_hex(32),
                 "finished": str(logs / "closed.json")}
    config = {"policy": policy, "checkout": str(checkout), "logs": str(logs),
              "environment": environment, "reference": reference, "deadline_ms": deadline_ms}
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-I", "-c",
        "from mimir_cultivate.worklink.jobs import service_main; service_main()",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, start_new_session=True,
        # Never import from checkout/PYTHONPATH, or inherit builder secrets here.
        env={"PATH": "/usr/bin:/bin"}, cwd="/",
    )
    try:
        from .daemon_processes import _process
        identity = _process(process.pid)
        reference.update(pid=process.pid, ticks=identity[1])
        # Persist the process witness BEFORE sending configuration/admitting jobs.
        atomic_json(reference_path(controller, session), reference,
                    uid=os.geteuid(), gid=os.getegid(), mode=0o600)
        process.stdin.write(json.dumps(config).encode() + b"\n")
        await process.stdin.drain()
        process.stdin.close()
        if await asyncio.wait_for(process.stdout.readline(), 10) != b"ready\n":
            raise RuntimeError("session job service unavailable")
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        # Credentials have not been handed to the session, so no command could
        # be admitted during this failed startup.
        atomic_json(Path(reference["finished"]), {"closed": True},
                    uid=os.geteuid(), gid=os.getegid(), mode=0o600)
        raise
    return {SOCKET_ENV: reference["socket"], AUTH_ENV: reference["auth"]}


async def session_jobs(controller: Path, session: str, *, close: bool = False) -> bool:
    from .daemon_service import read_json

    try:
        reference = read_json(reference_path(controller, session), uid=os.geteuid(), mode=0o600)
    except FileNotFoundError:
        return False
    witness = [reference["pid"], reference["ticks"]]
    if not _alive(witness):
        # A vanished server alone is not proof that its children were reaped.
        try:
            ended = read_json(Path(reference["finished"]), uid=os.geteuid(), mode=0o600)
        except FileNotFoundError:
            raise RuntimeError("session job cleanup unverified") from None
        if ended != {"closed": True}:
            raise RuntimeError("session job cleanup unverified")
        return False
    if not close:
        return (await rpc(reference, "active"))["active"]
    await rpc(reference, "close")
    # Acknowledgment alone isn't quiescence. Verify the exact service exited.
    async with asyncio.timeout(5):
        while _alive(witness):
            await asyncio.sleep(0.02)
    return False


async def serve(config: dict) -> None:
    import ctypes

    # Reparent double forks here rather than to PID 1, preserving cleanup scope.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError("job service requires Linux child subreaper")
    service = JobService(policy=config["policy"], checkout=Path(config["checkout"]),
                         logs=Path(config["logs"]), environment=config["environment"],
                         token=config["reference"]["auth"])
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, service.stop.set)
    server = await asyncio.start_unix_server(service.accept, address(config["reference"]["socket"]), limit=65536)
    print("ready", flush=True)
    try:
        await asyncio.wait_for(service.stop.wait(), max(0, config["deadline_ms"] / 1000 - time.time()))
    except TimeoutError:
        pass
    finally:
        server.close()
        await server.wait_closed()
        await service.close_jobs()
        for task in list(service.handlers):
            task.cancel()
        await asyncio.gather(*service.handlers, return_exceptions=True)
        from .daemon_service import atomic_json
        atomic_json(Path(config["reference"]["finished"]), {"closed": True},
                    uid=os.geteuid(), gid=os.getegid(), mode=0o600)


def main() -> int:
    parser = argparse.ArgumentParser(prog="worklink-job")
    sub = parser.add_subparsers(dest="op", required=True)
    start = sub.add_parser("start")
    start.add_argument("--tier", choices=("medium", "large"), default="medium")
    start.add_argument("argv", nargs=argparse.REMAINDER)
    for op in ("wait", "log", "cancel"):
        item = sub.add_parser(op)
        item.add_argument("id")
        if op == "wait":
            item.add_argument("--timeout", type=float, default=MAX_WAIT_S)
        if op == "log":
            item.add_argument("--tail", type=int, default=80)
    args = vars(parser.parse_args())
    op = args.pop("op")
    if op == "start":
        args["argv"] = args["argv"][1:] if args["argv"][:1] == ["--"] else args["argv"]
        args["cwd"] = os.getcwd()
    try:
        reference = {"socket": os.environ[SOCKET_ENV], "auth": os.environ[AUTH_ENV]}
        print(json.dumps(asyncio.run(rpc(reference, op, **args))))
        return 0
    except (KeyError, OSError, ValueError, TimeoutError):
        print("worklink-job: session job request refused", file=sys.stderr)
        return 1


def service_main() -> None:
    """Private installed Python entry point, deliberately absent from the CLI."""
    asyncio.run(serve(json.loads(sys.stdin.buffer.readline(262144))))


if __name__ == "__main__":
    raise SystemExit(main())
