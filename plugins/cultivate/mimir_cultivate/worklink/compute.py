"""Compute substrates for Worklink tool backends.

Tool backends decide *what* backend should build a Worklink issue. Compute
backends decide *where* that work unit runs.  After the #832 substrate
cleanup, ``local_subprocess`` is the sole Worklink compute substrate: the
backend runs as a local subprocess in the per-issue checkout, with the
shared-filesystem capabilities and (for autonomous dispatch) the explicit
opt-in gate that the rest of the executor already enforces.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
import json
import os
import posixpath
import re
from pathlib import Path
import sys
import uuid
from typing import TYPE_CHECKING, Any, Callable, Mapping, Protocol, Sequence

from mimir.output_capture import OutputSink, open_output_pair
from mimir.otel_credentials import filter_otlp_env, is_sensitive_otlp_env
from ..contained_execution import (
    CollectedExecutionResult,
    base_worker_environment,
    execute_contained,
    opencode_worker_environment,
)
from .backends.base import LaunchHandle, SessionRequest, SessionOutcome, SessionError, SessionTransportLostError
from .execution_policy import ExecutionPolicy, home_projection_documents

if TYPE_CHECKING:
    from .worker_client import CheckoutCapability, WorkerClient


@dataclass(frozen=True)
class ComputeCaps:
    """Capabilities of a Worklink execution substrate."""

    shared_filesystem: bool
    network_isolated: bool
    handle_cancel: bool
    persistent_after_disconnect: bool


@dataclass(frozen=True)
class WorkSpec:
    """Portable work unit handed to a Worklink compute substrate.

    The durable handoff is git-shaped: a worker can clone ``repo_url``, check out
    ``base_ref``/``branch``, run ``backend`` with ``prompt``/``rules``, execute
    ``test_command``, and push evidence. ``local_checkout`` and ``local_argv``
    are compatibility pointers used only by the ``local_subprocess`` substrate
    for today's manual in-container runs.
    """

    issue_id: int
    attempt: int
    repo_url: str
    base_ref: str
    branch: str
    prompt: str
    rules: str | None
    test_command: str
    backend: str
    timeout_s: int
    creds_ref: Mapping[str, str] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict, repr=False)
    backend_config: Mapping[str, Any] = field(default_factory=dict)
    local_checkout: Path | None = None
    local_argv: Sequence[str] | None = None
    output_root: Path | None = None
    factory_credential_settings: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    session_request: SessionRequest | None = None
    execution: ExecutionPolicy = field(default_factory=ExecutionPolicy)


def with_worker_environment(
    spec: WorkSpec, additions: Mapping[str, str]
) -> WorkSpec:
    """Supply injected environment and request it from a closed worker together.

    Specs without ``pass_env`` use the legacy direct environment unchanged. For a
    closed worker, deriving requested names here means every future injection made
    through this boundary automatically participates in the worker contract.
    """
    if not additions:
        return spec
    merged = {**spec.env, **additions}
    env = filter_otlp_env(merged)
    if "pass_env" not in spec.backend_config:
        return replace(spec, env=env)
    requested = spec.backend_config["pass_env"]
    if isinstance(requested, (str, bytes)) or not isinstance(requested, Sequence):
        raise ComputeLaunchError("worker pass_env must be a sequence")
    pass_env = tuple(dict.fromkeys(
        name for name in (*requested, *additions)
        if not is_sensitive_otlp_env(name, merged.get(name))
    ))
    return replace(
        spec,
        env=env,
        backend_config={**spec.backend_config, "pass_env": pass_env},
    )


@dataclass(frozen=True)
class ComputeResult:
    """Observed result from a launched compute job."""

    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False
    output_overflow: bool = False
    launch_error: str | None = None
    handle: LaunchHandle | None = None
    command: tuple[str, ...] = ()
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    children_reaped: bool = False
    session_outcome: SessionOutcome | None = None
    session_error: SessionError | None = None
    output_truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    lease_wait_timed_out: bool = False
    lease_wait_s: float = 0.0
    run_duration_s: float = 0.0


class ComputeLaunchError(RuntimeError):
    """Raised when a compute substrate cannot launch a work unit."""


class ComputeBackend(Protocol):
    name: str

    def capabilities(self) -> ComputeCaps: ...

    async def launch(self, spec: WorkSpec) -> LaunchHandle: ...

    async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult: ...

    async def logs(self, handle: LaunchHandle) -> str: ...

    async def cancel(self, handle: LaunchHandle) -> None: ...

    async def cleanup(self, handle: LaunchHandle) -> None: ...


class ComputeCleanupError(RuntimeError):
    def __init__(self, handle: LaunchHandle, detail: str = "") -> None:
        from mimir_cultivate.opencode import redact_daemon_output

        worker = "daemon session" if handle.substrate == "opencode_daemon" else "contained worker"
        super().__init__(f"{worker} quiescence unverified; retain checkout and recovery state" +
                         (": " + redact_daemon_output(detail)[:8192] if detail else ""))
        self.handle = handle


async def admit_session(client: WorkerClient, request: SessionRequest, **kwargs: Any) -> LaunchHandle:
    """Settle the one admission request before cancellation can lose its identity."""
    from .backends.base import SessionAdmissionCancelled, SessionAdmissionError
    pending = asyncio.create_task(client.session_create(request, **kwargs))
    cancelled = False
    while True:
        try:
            handle = await asyncio.shield(pending)
        except asyncio.CancelledError:
            if pending.cancelled():
                raise SessionAdmissionCancelled(None, uncertain=True) from None
            cancelled = True
            continue
        except SessionAdmissionError as exc:
            if cancelled:
                raise SessionAdmissionCancelled(exc.handle, uncertain=exc.uncertain,
                                                generation=exc.launch_generation) from None
            raise
        if cancelled:
            raise SessionAdmissionCancelled(handle) from None
        return handle


_OBSERVE_INTERVAL_CAP_S = 2.0


class OpenCodeDaemonComputeBackend:
    """Session identity and authoritative outcomes, not helper PIDs or exits."""

    name = "opencode_daemon"

    def __init__(self, checkout: CheckoutCapability | None = None, *, client: WorkerClient | None = None,
                 stdout_limit: int | None = None) -> None:
        if stdout_limit is not None and (type(stdout_limit) is not int or not 0 < stdout_limit <= 64 * 1024 * 1024):
            raise ValueError("invalid session stdout limit")
        self._stdout_limit = stdout_limit
        self._bind_on_launch = client is None and checkout is None
        if client is None:
            from .worker_client import WorkerClient
            client = WorkerClient(checkout)
        self._client = client
        self._deadlines: dict[str, float] = {}
        self._quiescent: set[str] = set()

    def capabilities(self) -> ComputeCaps:
        return ComputeCaps(True, False, True, True)

    async def recover_admission(self, generation: str) -> LaunchHandle | None:
        return await self._client.session_recover_admission(generation)

    async def rearm_session(self, handle: LaunchHandle) -> str:
        return await self._client.session_rearm(handle)

    def bind_retained_checkout(self, checkout: Path, *, issue_id: int, attempt: int) -> None:
        """Bind a new controller to retained correlation without admitting input."""
        from .worker_client import WorkerClient
        from .identities import get_identities

        if self._bind_on_launch:
            self._client = WorkerClient.for_path_checkout(checkout, issue_id=issue_id,
                attempt=attempt, run_uid=get_identities().worklink_uid)
            self._bind_on_launch = False
        elif isinstance(self._client, WorkerClient) and self._client.path_checkout is not None:
            if (self._client.path_checkout, self._client.issue_id, self._client.attempt) != (checkout, issue_id, attempt):
                raise ComputeLaunchError("daemon compute retained checkout binding mismatch")

    def prepare_fresh_checkout(self) -> None:
        """A parked host was cancelled and released; its replacement binds anew."""
        from .worker_client import WorkerClient

        if isinstance(self._client, WorkerClient) and self._client.path_checkout is not None:
            self._bind_on_launch = True

    async def launch(self, spec: WorkSpec) -> LaunchHandle:
        from .backends.base import SessionAdmissionCancelled, SessionAdmissionError
        if spec.session_request is None:
            raise ComputeLaunchError("daemon compute requires a typed session request")
        from .worker_client import WorkerClient
        if self._bind_on_launch:
            if spec.local_checkout is None:
                raise ComputeLaunchError("daemon compute requires a checkout")
            from .identities import get_identities
            self._client = WorkerClient.for_path_checkout(
                spec.local_checkout, issue_id=spec.issue_id, attempt=spec.attempt,
                run_uid=get_identities().worklink_uid)
            self._bind_on_launch = False
        if isinstance(self._client, WorkerClient):
            if self._client.path_checkout is not None and (
                self._client.path_checkout != spec.local_checkout or self._client.issue_id != spec.issue_id
                or self._client.attempt != spec.attempt
            ):
                raise ComputeLaunchError("daemon compute checkout binding mismatch")
            if self._client.checkout is not None:
                authorization = self._client.checkout
                authorization.verify(spec.local_checkout)
                if (authorization.issue_id, authorization.attempt) != (spec.issue_id, spec.attempt):
                    raise ComputeLaunchError("daemon compute attempt binding mismatch")
        requested = spec.backend_config.get("pass_env", ())
        if isinstance(requested, (str, bytes)) or not isinstance(requested, Sequence):
            raise ComputeLaunchError("session pass_env must be a sequence")
        if (any(not isinstance(name, str) for name in requested) or len(set(requested)) != len(requested)
                or any(name not in spec.env for name in requested)):
            raise ComputeLaunchError("session pass_env requires unique present environment names")
        pass_env = {name: spec.env[name] for name in requested}
        from .daemon_sessions import session_environment
        try:
            session_environment(pass_env, home=Path("/var/lib/mimir-worklink/session-validation"),
                                git_settings=spec.factory_credential_settings, credentials={})
        except (ValueError, ComputeLaunchError) as exc:
            raise ComputeLaunchError("session environment refused") from exc
        deadline = asyncio.get_running_loop().time() + spec.timeout_s
        try:
            handle = await admit_session(self._client,
                spec.session_request, timeout_s=spec.timeout_s,
                pass_env=pass_env,
                git_settings=spec.factory_credential_settings,
            )
        except (SessionAdmissionCancelled, SessionAdmissionError) as exc:
            if exc.handle is None and not exc.uncertain:
                exc.quiescent = True
            if exc.handle is not None:
                try:
                    await asyncio.shield(self.cancel(exc.handle))
                    await asyncio.shield(self.cleanup(exc.handle))
                    exc.quiescent = True
                except (OSError, RuntimeError, ValueError):
                    pass
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            raise ComputeLaunchError("OpenCode daemon session admission unavailable") from exc
        self._deadlines[handle.identifier] = deadline
        return handle

    async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
        deadline = self._deadlines.get(handle.identifier, float("inf"))
        remaining = deadline - asyncio.get_running_loop().time()
        try:
            if remaining <= 0:
                raise TimeoutError
            async with asyncio.timeout(min(timeout_s, max(0, remaining))):
                # Every observation runs a fresh executor helper process. Back off
                # from 100 ms to a 2 s cap so a long build does not keep a core busy
                # spawning helpers, which on a loaded host pushed helpers past their
                # executor deadline (2026-11-16, #2151-#2154).
                interval = 0.1
                while True:
                    try:
                        outcome = await self._observe(handle)
                    except SessionTransportLostError:
                        await asyncio.sleep(interval)
                        interval = min(interval * 2, _OBSERVE_INTERVAL_CAP_S)
                        continue
                    if asyncio.get_running_loop().time() >= deadline:
                        raise TimeoutError
                    if outcome.output_overflow:
                        try:
                            await self.cancel(handle)
                        except (OSError, RuntimeError, ValueError):
                            outcome = replace(outcome, cancellation_state="unverified")
                        return ComputeResult(1, outcome.stdout, "", output_overflow=True,
                                             handle=handle, session_outcome=outcome)
                    if outcome.execution_state in {"succeeded", "failed", "interrupted", "incomplete"}:
                        return ComputeResult(
                            0 if outcome.execution_state == "succeeded" else 1, outcome.stdout, "",
                            handle=handle, session_outcome=outcome, session_error=outcome.error,
                            timed_out=(outcome.execution_state == "incomplete" and outcome.error is not None
                                       and outcome.error.type == "timeout"),
                        )
                    await asyncio.sleep(interval)
                    interval = min(interval * 2, _OBSERVE_INTERVAL_CAP_S)
        except TimeoutError:
            cancellation = "verified"
            try:
                await self.cancel(handle)
            except RuntimeError:
                cancellation = "unverified"
            error = SessionError("timeout", "Worklink session deadline exceeded")
            outcome = SessionOutcome(handle.identifier, "incomplete", cancellation, error)
            return ComputeResult(1, "", "", timed_out=True, handle=handle,
                                 session_outcome=outcome, session_error=error)
        except (OSError, RuntimeError, ValueError):
            cancellation = "verified"
            try:
                await self.cancel(handle)
            except (OSError, RuntimeError, ValueError):
                cancellation = "unverified"
            error = SessionError("backend_error", "OpenCode session observation failed")
            outcome = SessionOutcome(handle.identifier, "incomplete", cancellation, error)
            return ComputeResult(1, "", "", handle=handle, session_outcome=outcome, session_error=error)

    async def logs(self, handle: LaunchHandle) -> str:
        outcome = await self._observe(handle)
        return outcome.stdout

    async def session_tree_activity(self, handle: LaunchHandle) -> dict[str, Any]:
        return await self._client.session_tree_activity(handle)

    def job_alive(self, handle: LaunchHandle) -> bool:
        """Conservative supervision hint; only observation proves a host outcome."""
        return handle.substrate == self.name and handle.identifier not in self._quiescent

    async def _observe(self, handle: LaunchHandle) -> SessionOutcome:
        if self._stdout_limit is None:
            outcome = await self._client.session_observe(handle)
        else:
            outcome = await self._client.session_observe(handle, stdout_limit=self._stdout_limit)
        from mimir_cultivate.opencode import redact_daemon_output

        error = outcome.error
        if error is not None:
            def bounded(value: str, limit: int) -> str:
                return redact_daemon_output(value).encode("utf-8")[:limit].decode("utf-8", "ignore")
            error = replace(error, type=bounded(error.type, 128), message=bounded(error.message, 2048),
                            body=bounded(error.body, 8192))
        return replace(outcome, error=error, stdout=redact_daemon_output(outcome.stdout))

    async def cancel(self, handle: LaunchHandle) -> None:
        try:
            await self._client.session_cancel(handle)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ComputeCleanupError(handle, str(exc)) from exc
        self._quiescent.add(handle.identifier)

    async def cleanup(self, handle: LaunchHandle) -> None:
        try:
            await self._client.session_release(handle)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ComputeCleanupError(handle, str(exc)) from exc
        self._deadlines.pop(handle.identifier, None)


_LOCAL_ENV_INFRA = (
    "PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "TERM",
    "TMPDIR", "TMP", "TEMP", "TZ", "NODE_EXTRA_CA_CERTS",
    "SSL_CERT_FILE", "SSL_CERT_DIR",
)
_LOCAL_ENV_INFRA_PREFIXES = ("LC_", "XDG_")
# Provider-credential families a routed coding CLI (codex / claude / opencode,
# which is provider-agnostic) may legitimately need. Union kept broad on
# purpose — opencode routes to whichever provider its config selects.
_LOCAL_ENV_CRED_PREFIXES = (
    "OPENAI_", "CODEX_", "ANTHROPIC_", "CLAUDE_", "OPENCODE_",
    "MINIMAX_", "OPENROUTER_", "GROQ_", "GEMINI_", "GOOGLE_",
    "VOYAGE_", "GITHUB_TOKEN", "GH_TOKEN",
)

DEFAULT_WORKLINK_STDOUT_BYTES = 64 * 1024 * 1024
DEFAULT_WORKLINK_STDERR_BYTES = 16 * 1024 * 1024
WORKLINK_GATE_ENV = "MIMIR_WORKLINK_GATE"
_TERMINATED_DRAIN_TIMEOUT_S = 5.0
_WORKER_CANCEL_TIMEOUT_S = 25.0
_WORKER_COLLECTION_TIMEOUT_S = 10.0
_DIRECT_PROCESS_WAIT_TIMEOUT_S = 5.0


async def _cancel_worker(client: object, identifier: str) -> None:
    try:
        await asyncio.wait_for(
            getattr(client, "cancel")(identifier), _WORKER_CANCEL_TIMEOUT_S
        )
    except TimeoutError as exc:
        raise RuntimeError(
            f"worker cancellation exceeded {_WORKER_CANCEL_TIMEOUT_S:g}s"
        ) from exc


def _output_limit(env_name: str, default: int) -> int:
    """Return a positive output cap, falling back on invalid overrides."""
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return min(value, default) if value > 0 else default


def _worklink_output_limits() -> tuple[int, int]:
    return (
        _output_limit("MIMIR_WORKLINK_MAX_STDOUT_BYTES", DEFAULT_WORKLINK_STDOUT_BYTES),
        _output_limit("MIMIR_WORKLINK_MAX_STDERR_BYTES", DEFAULT_WORKLINK_STDERR_BYTES),
    )


_OUTPUT_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def _output_paths(spec: WorkSpec, identifier: str) -> tuple[Path | None, Path | None]:
    root = spec.output_root
    if root is None:
        return None, None
    if _OUTPUT_COMPONENT.fullmatch(spec.backend) is None or spec.backend in {".", ".."}:
        raise ComputeLaunchError("backend name is not a safe output path component")
    stem = f"{spec.backend}-{spec.issue_id}-a{spec.attempt}-{identifier}-{uuid.uuid4()}"
    return root / f"{stem}.stdout.log", root / f"{stem}.stderr.log"


@dataclass
class _DirectCapture:
    stdout_sink: OutputSink
    stderr_sink: OutputSink
    collect_task: asyncio.Task[None] | None = None
    monitor_task: asyncio.Task[None] | None = None
    kill_task: asyncio.Task[None] | None = None
    output_overflow: bool = False


def _heavy_lock_environment() -> dict[str, str]:
    from ..contained_execution import heavy_lock_environment

    try:
        return heavy_lock_environment()
    except ValueError as exc:
        raise ComputeLaunchError(str(exc)) from None


def _local_child_env() -> dict[str, str]:
    """Allowlisted env for an autonomous local_subprocess worker (#830).

    Infra vars + provider-credential families from the parent process; nothing
    else (no bridge/operator secrets). Mirrors ``tools.registry`` spawn-env
    philosophy on the worklink compute path."""
    env: dict[str, str] = _heavy_lock_environment()
    for key in _LOCAL_ENV_INFRA:
        val = os.environ.get(key)
        if val is not None:
            env[key] = val
    for key, val in os.environ.items():
        if key.startswith(_LOCAL_ENV_INFRA_PREFIXES) or key.startswith(_LOCAL_ENV_CRED_PREFIXES):
            env[key] = val
    return env


_ENABLED_SYNTHESIZED_NAMES = frozenset({
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_CACHE_HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "PATH",
    "LANG",
    "LC_ALL",
    "OPENCODE_CONFIG",
    "OPENCODE_PERMISSION",
    "FACTORY_SESSION_ID",
    "FACTORY_PUBLISHING_IDENTITY",
    "CLAUDE_CONFIG_DIR",
})
_ENABLED_DENIED_PREFIXES = (
    "MIMIR_",
    "GIT_",
    "GH_",
    "GITHUB_",
    "XDG_",
    "LD_",
    "DYLD_",
    "PYTHON",
    "BASH_",
)
_ENABLED_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_ENABLED_VALUE_BYTES = 64 * 1024


def _refers_to_controller_home(value: str) -> bool:
    for component in re.split(r"[=,;\s:]", value):
        if not component.startswith("/"):
            continue
        normalized = posixpath.normpath(component)
        if normalized == "/home/mimir" or normalized.startswith("/home/mimir/"):
            return True
    if re.search(r"(?:^|[=,;\s])/?home/mimir(?:/|$)", value):
        return True
    return bool(
        re.search(r"(?:^|[=,;\s])(?:\.\./)+home/mimir(?:/|$)", value)
    )


def _validate_worker_value(name: str, value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ComputeLaunchError(f"worker environment {name} must be a non-empty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ComputeLaunchError(f"worker environment {name} is not UTF-8") from exc
    if len(encoded) > _ENABLED_VALUE_BYTES:
        raise ComputeLaunchError(f"worker environment {name} exceeds size limit")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ComputeLaunchError(f"worker environment {name} contains a forbidden character")
    if name == "MIMIR_WORK_ITEM_JSON":
        import json

        try:
            item = json.loads(value)
        except ValueError as exc:
            raise ComputeLaunchError("factory work item is not JSON") from exc
        if (not isinstance(item, dict) or not isinstance(item.get("run_id"), str)
                or re.fullmatch(r"(?:chainlink-)?[1-9][0-9]*", item["run_id"]) is None):
            raise ComputeLaunchError("factory work item identity is invalid")
    if _refers_to_controller_home(value):
        raise ComputeLaunchError(f"worker environment {name} refers to controller home")
    return value


def _validate_dynamic_worker_name(name: object) -> str:
    if not isinstance(name, str) or _ENABLED_NAME.fullmatch(name) is None:
        raise ComputeLaunchError("worker environment name is invalid")
    upper = name.upper()
    if (
        name in _ENABLED_SYNTHESIZED_NAMES
        or (upper.startswith(_ENABLED_DENIED_PREFIXES) and name != "MIMIR_WORK_ITEM_JSON")
        or "GITHUB" in upper
    ):
        raise ComputeLaunchError(f"worker environment {name} is denied")
    return name


def _enabled_child_env(spec: WorkSpec, identifier: str) -> dict[str, str]:
    home = f"/var/lib/mimir-worklink/homes/{identifier}"
    additions: dict[str, str] = {}
    permission = spec.env.get("OPENCODE_PERMISSION")
    if permission is not None:
        additions["OPENCODE_PERMISSION"] = _validate_worker_value(
            "OPENCODE_PERMISSION", permission
        )
    claude = spec.execution.home_projection_profile != "none"
    env = (base_worker_environment(identifier) if claude else
           opencode_worker_environment(base_worker_environment(identifier), additions))
    if claude and permission is not None:
        raise ComputeLaunchError("Claude worker refuses OpenCode configuration")
    env["HOME"] = home
    env[WORKLINK_GATE_ENV] = "1"
    if permission is not None:
        try:
            parsed = json.loads(additions["OPENCODE_PERMISSION"])
        except (TypeError, ValueError) as exc:
            raise ComputeLaunchError("invalid OPENCODE_PERMISSION") from exc
        if not isinstance(parsed, dict):
            raise ComputeLaunchError("invalid OPENCODE_PERMISSION")
    permitted = spec.backend_config.get("pass_env", ())
    if isinstance(permitted, (str, bytes)) or not isinstance(permitted, Sequence):
        raise ComputeLaunchError("worker pass_env must be a sequence")
    seen: set[str] = set()
    for raw_name in permitted:
        if isinstance(raw_name, str) and is_sensitive_otlp_env(raw_name, spec.env.get(raw_name)):
            seen.add(raw_name)
            continue
        name = _validate_dynamic_worker_name(raw_name)
        if claude:
            from ..claude_code import OAUTH_ENV, _PROVIDER_ENV_PREFIXES
            if name.startswith(_PROVIDER_ENV_PREFIXES) and name != OAUTH_ENV:
                raise ComputeLaunchError("Claude worker refuses caller authentication or configuration")
        if name in seen:
            raise ComputeLaunchError(f"worker environment {name} is duplicated")
        seen.add(name)
        if name not in spec.env:
            raise ComputeLaunchError(f"worker environment {name} is missing")
        if not is_sensitive_otlp_env(name, spec.env[name]):
            env[name] = _validate_worker_value(name, spec.env[name])
    unknown = set(spec.env) - seen - {"OPENCODE_PERMISSION"}
    unknown = {name for name in unknown if not is_sensitive_otlp_env(name, spec.env[name])}
    if unknown:
        name = sorted(str(item) for item in unknown)[0]
        raise ComputeLaunchError(f"worker environment {name} was not requested")
    return filter_otlp_env(env)


def _fd_anchored_opencode_argv(
    command: tuple[str, ...], checkout: Path | None
) -> tuple[str, ...]:
    """Keep enabled OpenCode inside the checkout reached through its issued FD."""
    if checkout is None:
        raise ComputeLaunchError("local_subprocess requires spec.local_checkout")
    for index, arg in enumerate(command[:-1]):
        if arg != "--dir":
            continue
        if command[index + 1] == ".":
            return command
        if command[index + 1] != str(checkout):
            raise ComputeLaunchError("enabled OpenCode --dir must name the issued checkout")
        return (*command[: index + 1], ".", *command[index + 2 :])
    return command


class _ClientBoundCapability:
    def __init__(
        self,
        capability: object,
        client: object,
        started: asyncio.Future[object],
    ) -> None:
        self._capability = capability
        self._contained_worker_client = client
        self._started = started

    def _contained_started(self, process: object) -> None:
        if not self._started.done():
            self._started.set_result(process)

    def __getattr__(self, name: str) -> object:
        return getattr(self._capability, name)


@dataclass(frozen=True)
class _PathCheckoutCapability:
    path: Path


@dataclass
class LocalSubprocessComputeBackend:
    """Run a WorkSpec as a local subprocess in the current container."""

    name: str = "local_subprocess"
    _authorized_checkout: object | None = field(default=None, repr=False)
    _worker_uid: int | None = field(default=None, repr=False)
    _worker_client: object | None = field(default=None, repr=False)

    @classmethod
    def for_authorized_checkout(
        cls,
        authorization: object,
        *,
        worker_uid: int | None = None,
        worker_client: object | None = None,
    ) -> LocalSubprocessComputeBackend:
        if not all(
            hasattr(authorization, member) for member in ("verify", "duplicate_fd", "path")
        ):
            raise TypeError("authorization is not a checkout capability")
        if worker_uid is not None:
            from .identities import get_identities

            if type(worker_uid) is not int or worker_uid != get_identities().worklink_uid:
                raise ValueError("worker uid must match the configured worker identity")
        return cls(
            _authorized_checkout=authorization, _worker_uid=worker_uid,
            _worker_client=worker_client,
        )

    @classmethod
    def for_path_checkout(
        cls,
        worker_uid: int,
        *,
        worker_client: object | None = None,
    ) -> LocalSubprocessComputeBackend:
        if type(worker_uid) is not int or worker_uid < 0:
            raise ValueError("worker uid must be a non-negative integer")
        return cls(_worker_uid=worker_uid, _worker_client=worker_client)

    def __post_init__(self) -> None:
        self._jobs: dict[str, tuple[object, WorkSpec, tuple[str, ...]]] = {}
        self._handles: dict[str, LaunchHandle] = {}
        self._worker_clients: dict[str, object] = {}
        self._executor_instances: dict[str, str | None] = {}
        self._executor_process_epochs: dict[str, str | None] = {}
        self._output_files: dict[str, tuple[Path | None, Path | None]] = {}
        self._direct_captures: dict[str, _DirectCapture] = {}

    def capabilities(self) -> ComputeCaps:
        return ComputeCaps(
            shared_filesystem=True,
            network_isolated=False,
            handle_cancel=True,
            persistent_after_disconnect=False,
        )

    def worker_executor_instance_id(self, handle: LaunchHandle) -> str | None:
        return self._executor_instances.get(handle.identifier)

    def worker_executor_process_epoch(self, handle: LaunchHandle) -> str | None:
        return self._executor_process_epochs.get(handle.identifier)

    async def launch(self, spec: WorkSpec) -> LaunchHandle:
        if spec.local_checkout is None:
            raise ComputeLaunchError("local_subprocess requires spec.local_checkout")
        if spec.local_argv is None:
            raise ComputeLaunchError("local_subprocess requires spec.local_argv")
        if isinstance(spec.local_argv, (str, bytes)):
            raise ComputeLaunchError("local_subprocess spec.local_argv must be a sequence")
        command = tuple(str(arg) for arg in spec.local_argv)
        if not command:
            raise ComputeLaunchError("local_subprocess spec.local_argv must not be empty")
        from .checkout import _cultivate_selected
        if spec.execution.checkout_protocol == "factory_worker":
            return await self._launch_contained(spec, command)
        if spec.execution.home_projection_profile != "none":
            return await self._launch_contained(spec, command)
        if _cultivate_selected():
            return await self._launch_enabled(spec, command)
        # chainlink #830: autonomous local_subprocess builds an allowlisted env
        # from the parent process — infra vars (HOME so a coding CLI finds its
        # config/plugins + provider auth files; locale/cert vars) plus provider
        # credential families. Bridge/operator secrets (DISCORD_/SLACK_/
        # MIMIR_API_KEY, ...) are NEVER passed. This was inert while docker was
        # the only autonomous substrate (creds arrived via broker policy) and
        # local_subprocess was operator-CLI-only (full env inherited); the
        # opencode-on-checkouts pivot makes it the live path. ``spec.env`` (the
        # orchestrator's per-run vars, e.g. MIMIR_HOME) wins over the passthrough.
        env = _local_child_env()
        env.update(spec.env)
        env = filter_otlp_env(env)
        output_identifier = str(uuid.uuid4())
        stdout_path, stderr_path = _output_paths(spec, output_identifier)
        stdout_limit, stderr_limit = _worklink_output_limits()
        try:
            stdout_sink, stderr_sink = open_output_pair(
                stdout_path,
                stdout_limit,
                stderr_path,
                stderr_limit,
            )
        except OSError as exc:
            raise ComputeLaunchError(str(exc)) from exc
        try:
            from .worker_exec import _arm_parent_death_signal

            launch_options: dict[str, Any] = {}
            if sys.platform.startswith("linux"):
                parent_pid = os.getpid()
                launch_options["preexec_fn"] = (
                    lambda: _arm_parent_death_signal(parent_pid)
                )
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=stdout_sink.file,
                stderr=stderr_sink.file,
                cwd=str(spec.local_checkout),
                env=env,
                start_new_session=True,
                **launch_options,
            )
        except BaseException as exc:
            stdout_sink.close()
            stderr_sink.close()
            for path in (stdout_path, stderr_path):
                if path is not None:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise ComputeLaunchError(str(exc)) from exc
        pid = getattr(proc, "pid", None)
        start_ticks = None
        if isinstance(pid, int):
            from .run_state import process_start_ticks

            start_ticks = process_start_ticks(pid)
        handle = LaunchHandle(self.name, str(pid if pid is not None else "unknown"), start_ticks)
        self._jobs[handle.identifier] = (proc, spec, command)
        self._handles[handle.identifier] = handle
        self._output_files[handle.identifier] = (stdout_path, stderr_path)
        capture = _DirectCapture(stdout_sink, stderr_sink)

        async def monitor_output() -> None:
            while True:
                check_task = asyncio.create_task(asyncio.to_thread(
                    lambda: stdout_sink.overflowed() or stderr_sink.overflowed()
                ))
                try:
                    overflow = await asyncio.shield(check_task)
                finally:
                    # Cancellation cannot stop a thread that still uses the sinks.
                    await asyncio.gather(check_task, return_exceptions=True)
                if overflow and not capture.output_overflow:
                    capture.output_overflow = True
                    capture.kill_task = asyncio.create_task(self.cancel(handle))
                if overflow:
                    stdout_sink.truncate_to_limit()
                    stderr_sink.truncate_to_limit()
                await asyncio.sleep(0.01)

        async def collect() -> None:
            await getattr(proc, "wait")()

        capture.collect_task = asyncio.create_task(collect())
        capture.monitor_task = asyncio.create_task(monitor_output())
        self._direct_captures[handle.identifier] = capture
        return handle

    async def _launch_enabled(self, spec: WorkSpec, command: tuple[str, ...]) -> LaunchHandle:
        command = _fd_anchored_opencode_argv(command, spec.local_checkout)
        return await self._launch_contained(spec, command)

    async def _launch_contained(self, spec: WorkSpec, command: tuple[str, ...]) -> LaunchHandle:
        from .worker_client import WorkerClient, WorkerProjection

        authorization = self._authorized_checkout
        factory = spec.execution.checkout_protocol == "factory_worker"
        if factory:
            capability = _PathCheckoutCapability(spec.local_checkout)
            client = self._worker_client or WorkerClient.for_factory_checkout(
                spec.local_checkout, issue_id=spec.issue_id, attempt=spec.attempt
            )
        elif authorization is not None and all(
            hasattr(authorization, member) for member in ("verify", "duplicate_fd", "path")
        ):
            capability: object = authorization
            client = self._worker_client or WorkerClient(authorization)
        elif self._worker_uid is not None and spec.local_checkout is not None:
            capability = _PathCheckoutCapability(spec.local_checkout)
            client = self._worker_client or WorkerClient.for_path_checkout(
                spec.local_checkout,
                issue_id=spec.issue_id,
                attempt=spec.attempt,
                run_uid=self._worker_uid,
            )
        else:
            raise ComputeLaunchError(
                "enabled local_subprocess requires an authorized or path-addressed worker checkout"
            )
        profile = spec.execution.home_projection_profile
        if profile != "none" and "worker_projections" in spec.backend_config:
            raise ComputeLaunchError("Claude home projections are server-generated")
        projections_raw = spec.backend_config.get("worker_projections", ())
        if profile != "none":
            projections_raw = tuple(WorkerProjection(path, document) for path, document in home_projection_documents(profile))
        if isinstance(projections_raw, (str, bytes)) or not isinstance(
            projections_raw, Sequence
        ):
            raise ComputeLaunchError("worker projections must be a sequence")
        projections: list[WorkerProjection] = []
        try:
            for item in projections_raw:
                projections.append(WorkerProjection(path=item.path, document=item.document))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ComputeLaunchError("worker projection is invalid") from exc
        identifier = spec.backend_config.get("worker_identifier") or str(uuid.uuid4())
        from .worker_client import _validate_identifier
        _validate_identifier(identifier)
        started: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        capability = _ClientBoundCapability(capability, client, started)
        stdout_limit, stderr_limit = _worklink_output_limits()
        stdout_path, stderr_path = _output_paths(spec, identifier)
        if profile != "none":
            # Raw credential-bearing output is captured in anonymous bounded files;
            # only the backend's redacted transcript is made durable.
            stdout_path, stderr_path = None, None
        try:
            stdout_sink, stderr_sink = open_output_pair(
                stdout_path,
                stdout_limit,
                stderr_path,
                stderr_limit,
            )
        except OSError as exc:
            raise ComputeLaunchError(str(exc)) from exc

        async def collect_contained() -> CollectedExecutionResult:
            try:
                if factory:
                    # Factory publishing/work-item inputs are not the leaf-build
                    # environment contract. Keep the installed CLI/plugin PATH.
                    if profile == "claude_factory":
                        from ..claude_code import EPIC_CREDENTIALS, OAUTH_ENV
                        allowed = {OAUTH_ENV, *EPIC_CREDENTIALS, "MIMIR_WORK_ITEM_JSON",
                                   "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL",
                                   "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL"}
                        if set(spec.env) - allowed or not spec.env.get(OAUTH_ENV, "").strip():
                            raise ComputeLaunchError("invalid Claude factory environment")
                        environment = {"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
                                       **spec.env}
                    else:
                        environment = {**_local_child_env(), **spec.env}
                    runtime_path = environment.get("PATH")
                    environment.update(base_worker_environment(identifier))
                    # Captured by the controller from the trusted repository, never
                    # from the worker checkout. Preserve scoped keys and reset entries.
                    if spec.factory_credential_settings:
                        environment["GIT_CONFIG_COUNT"] = str(len(spec.factory_credential_settings))
                        for index, (key, value) in enumerate(spec.factory_credential_settings):
                            environment[f"GIT_CONFIG_KEY_{index}"] = key
                            environment[f"GIT_CONFIG_VALUE_{index}"] = value
                    if runtime_path:
                        environment["PATH"] = runtime_path
                    if Path(command[0]).name == "opencode":
                        raise ComputeLaunchError("OpenCode workloads require daemon sessions")
                else:
                    environment = _enabled_child_env(spec, identifier)
                scrubber = None
                if profile != "none":
                    from ..claude_code import secret_scrubber
                    scrubber = secret_scrubber(spec.env)
                return await execute_contained(
                        command,
                        capability,
                        filter_otlp_env({
                            key: value
                            for key, value in environment.items()
                            if key != "HOME"
                        }),
                        projections,
                        identifier=identifier,
                        home_projection_profile=profile,
                        retirement=("contained_process_tree"
                                    if spec.execution.retirement == "contained_process_tree" else "process"),
                        timeout_s=spec.timeout_s,
                        stdout_limit=stdout_limit,
                        stderr_limit=stderr_limit,
                        stdout_sink=stdout_sink,
                        stderr_sink=stderr_sink,
                        scrubber=scrubber,
                    )
            except asyncio.CancelledError:
                if not started.done():
                    started.cancel()
                raise
            except BaseException as exc:
                if not started.done():
                    started.set_exception(exc)
                raise
            finally:
                stdout_sink.close()
                stderr_sink.close()

        task = asyncio.create_task(collect_contained())
        interrupted = False
        while True:
            try:
                process = await asyncio.shield(started)
                break
            except asyncio.CancelledError:
                if started.cancelled():
                    raise
                interrupted = True
            except (OSError, RuntimeError, ValueError) as exc:
                await asyncio.gather(task, return_exceptions=True)
                raise ComputeLaunchError(str(exc)) from exc
        pid = getattr(process, "pid", None)
        ticks = getattr(process, "process_start_ticks", None)
        if ticks is None and isinstance(pid, int):
            from .run_state import process_start_ticks

            ticks = process_start_ticks(pid)
        handle = LaunchHandle(
            substrate=self.name,
            identifier=identifier,
            process_start_ticks=ticks,
            shim_pid=pid,
        )
        self._jobs[identifier] = (task, spec, command)
        self._handles[identifier] = handle
        self._worker_clients[identifier] = client
        self._executor_instances[identifier] = getattr(process, "executor_instance_id", None)
        self._executor_process_epochs[identifier] = getattr(process, "executor_process_epoch", None)
        self._output_files[identifier] = (stdout_path, stderr_path)
        if interrupted:
            await self.cleanup(handle)
            raise asyncio.CancelledError
        return handle

    async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
        job, _spec, command = self._job(handle)
        stdout_path, stderr_path = self._output_files.get(handle.identifier, (None, None))
        if isinstance(job, asyncio.Task):
            timed_out = False
            try:
                collected = await asyncio.wait_for(
                    asyncio.shield(job), timeout=timeout_s
                )
            except TimeoutError:
                timed_out = True
                client = self._worker_clients[handle.identifier]
                await _cancel_worker(client, handle.identifier)
                try:
                    collected = await asyncio.wait_for(
                        job, _WORKER_COLLECTION_TIMEOUT_S
                    )
                except TimeoutError as exc:
                    raise RuntimeError(
                        "worker did not report a terminal result within "
                        f"{_WORKER_COLLECTION_TIMEOUT_S:g}s of cancellation"
                    ) from exc
            if not isinstance(collected, CollectedExecutionResult):
                raise RuntimeError("contained execution returned an invalid result")
            launch_error = None
            try:
                errors = "strict" if _spec.execution.home_projection_profile != "none" else "replace"
                stdout = collected.stdout.decode(errors=errors)
                stderr = collected.stderr.decode(errors=errors)
            except UnicodeDecodeError:
                stdout, stderr = "", ""
                launch_error = "Claude output is not UTF-8"
            return ComputeResult(
                exit_code=collected.exit_code if collected.exit_code is not None else -1,
                stdout=stdout,
                stderr=stderr,
                launch_error=launch_error,
                timed_out=timed_out or collected.timed_out,
                output_overflow=collected.output_overflow,
                handle=handle,
                command=command,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                children_reaped=collected.children_reaped,
            )

        proc = job
        timed_out = False
        capture = self._direct_captures[handle.identifier]
        collect_task = capture.collect_task
        if collect_task is None:
            raise RuntimeError("direct output capture did not start")

        async def collect_after_termination() -> None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(collect_task), _TERMINATED_DRAIN_TIMEOUT_S
                )
            except TimeoutError:
                collect_task.cancel()
                await asyncio.gather(collect_task, return_exceptions=True)

        try:
            await asyncio.wait_for(asyncio.shield(collect_task), timeout=timeout_s)
        except TimeoutError:
            timed_out = True
            await self.cancel(handle)
            await collect_after_termination()
        if capture.kill_task is not None:
            await capture.kill_task
            if not collect_task.done():
                await collect_after_termination()
        if capture.monitor_task is not None:
            capture.monitor_task.cancel()
            await asyncio.gather(capture.monitor_task, return_exceptions=True)

        if capture.stdout_sink.overflowed() or capture.stderr_sink.overflowed():
            capture.output_overflow = True
            if getattr(proc, "returncode", None) is None:
                await self.cancel(handle)
                await collect_after_termination()
        stdout_b, _stdout_dropped = capture.stdout_sink.read_bounded()
        stderr_b, _stderr_dropped = capture.stderr_sink.read_bounded()
        capture.output_overflow = (
            capture.output_overflow
            or capture.stdout_sink.did_overflow
            or capture.stderr_sink.did_overflow
        )
        capture.stdout_sink.truncate_to_limit()
        capture.stderr_sink.truncate_to_limit()
        stdout = stdout_b.decode(errors="replace")
        stderr = stderr_b.decode(errors="replace")
        exit_code = getattr(proc, "returncode", None)
        return ComputeResult(
            exit_code=exit_code if exit_code is not None else -1,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
            output_overflow=capture.output_overflow,
            handle=handle,
            command=command,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

    async def logs(self, handle: LaunchHandle) -> str:
        self._job(handle)
        return ""

    async def admit_factory_handle(self, handle: LaunchHandle) -> None:
        self._job(handle)
        await self._worker_clients[handle.identifier].admit_factory_handle(handle)

    def job_alive(self, handle: LaunchHandle) -> bool:
        """Whether the launched subprocess is still running (liveness probe for
        the feature-factory observe loop). Unknown/gone handles read as dead."""
        try:
            job, _spec, _command = self._job(handle)
        except KeyError:
            return False
        if isinstance(job, asyncio.Task):
            return not job.done()
        return getattr(job, "returncode", 0) is None

    async def cancel(self, handle: LaunchHandle) -> None:
        try:
            proc, _spec, _command = self._job(handle)
        except KeyError:
            proc = _verified_external_process(handle, self.name)
        client = self._worker_clients.get(handle.identifier)
        if client is not None:
            await _cancel_worker(client, handle.identifier)
            return
        if handle.shim_pid is not None:
            from .worker_client import WorkerClient

            await _cancel_worker(WorkerClient(None), handle.identifier)  # type: ignore[arg-type]
            return
        await _kill_process_group(proc)

    async def cleanup(self, handle: LaunchHandle) -> None:
        known = self._handles.get(handle.identifier)
        if known is not None and known != handle:
            raise KeyError(f"unknown {self.name} handle: {handle.identifier}")
        if known is None:
            return
        interrupted = False
        entry = self._jobs.get(handle.identifier)
        job = entry[0] if entry is not None else None
        if isinstance(job, asyncio.Task):
            async def retire() -> None:
                try:
                    if not job.done() or job.cancelled() or job.exception() is not None:
                        await _cancel_worker(self._worker_clients[handle.identifier], handle.identifier)
                    collected = await asyncio.wait_for(
                        asyncio.shield(job), _WORKER_COLLECTION_TIMEOUT_S
                    )
                    if not isinstance(collected, CollectedExecutionResult):
                        raise RuntimeError("contained execution returned an invalid result")
                except (Exception, asyncio.CancelledError) as exc:
                    raise ComputeCleanupError(handle) from exc

            # Caller cancellation must not detach retirement. Keep the exact
            # identity and collector until its terminal report proves quiescence.
            retirement = asyncio.create_task(retire())
            while True:
                try:
                    await asyncio.shield(retirement)
                    break
                except asyncio.CancelledError:
                    interrupted = True
        capture = self._direct_captures.get(handle.identifier)
        if capture is not None:
            if capture.monitor_task is not None:
                capture.monitor_task.cancel()
                await asyncio.gather(capture.monitor_task, return_exceptions=True)
            # The monitor can publish kill_task until it has finished.
            tasks = [
                task for task in (capture.collect_task, capture.kill_task)
                if task is not None
            ]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self._jobs.pop(handle.identifier, None)
        self._handles.pop(handle.identifier)
        self._worker_clients.pop(handle.identifier, None)
        self._executor_instances.pop(handle.identifier, None)
        self._executor_process_epochs.pop(handle.identifier, None)
        self._output_files.pop(handle.identifier, None)
        capture = self._direct_captures.pop(handle.identifier, None)
        if capture is not None:
            capture.stdout_sink.close()
            capture.stderr_sink.close()
        if interrupted:
            raise asyncio.CancelledError

    def _job(self, handle: LaunchHandle) -> tuple[object, WorkSpec, tuple[str, ...]]:
        if (
            handle.substrate != self.name
            or handle.identifier not in self._jobs
            or self._handles.get(handle.identifier) != handle
        ):
            raise KeyError(f"unknown {self.name} handle: {handle.identifier}")
        return self._jobs[handle.identifier]


class _ExternalProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    async def wait(self) -> None:
        while True:
            from .run_state import process_is_zombie

            if process_is_zombie(self.pid):
                return
            try:
                # Authoritative liveness probe: EPERM is not proof of exit.
                os.kill(self.pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.05)


def _verified_external_process(handle: LaunchHandle, substrate: str) -> _ExternalProcess:
    """Reconstruct a local handle only when its PID birth marker still matches."""
    if handle.substrate != substrate or handle.process_start_ticks is None:
        raise KeyError(f"unknown {substrate} handle: {handle.identifier}")
    if handle.shim_pid is None:
        try:
            pid = int(handle.identifier)
        except ValueError as exc:
            raise KeyError(f"invalid {substrate} pid: {handle.identifier}") from exc
    else:
        try:
            parsed = uuid.UUID(handle.identifier, version=4)
        except ValueError as exc:
            raise KeyError(f"invalid {substrate} UUID: {handle.identifier}") from exc
        if str(parsed) != handle.identifier:
            raise KeyError(f"invalid {substrate} UUID: {handle.identifier}")
        pid = handle.shim_pid
    from .run_state import process_start_ticks

    if process_start_ticks(pid) != handle.process_start_ticks:
        raise RuntimeError("refusing to cancel: recorded process identity no longer matches")
    return _ExternalProcess(pid)


async def _kill_process_group(proc: object) -> None:
    pid = getattr(proc, "pid", None)
    if pid is None:
        kill = getattr(proc, "kill", None)
        if kill:
            kill()
        return
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return
    except OSError:
        kill = getattr(proc, "kill", None)
        if kill:
            kill()
        return
    from .worker_exec import _terminate_process_group_pid

    await asyncio.to_thread(_terminate_process_group_pid, pgid)
    wait = getattr(proc, "wait", None)
    if wait is not None:
        try:
            await asyncio.wait_for(wait(), _DIRECT_PROCESS_WAIT_TIMEOUT_S)
        except TimeoutError as exc:
            raise RuntimeError(
                f"process group {pgid} leader was not reaped after SIGKILL"
            ) from exc
