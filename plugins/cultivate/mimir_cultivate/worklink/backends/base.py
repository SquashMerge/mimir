"""Protocol types for Worklink tool backends.

Backends own CLI session semantics only: rendering the tool-specific work spec,
capturing transcripts, and mapping tool-specific failures into common status
strings. Claiming, compute launch/wait/cancel/cleanup, checkout lifecycle,
evidence validation, and state transitions stay in shared Worklink plumbing.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class SessionTransportLostError(RuntimeError):
    """Observation can reconnect to the durable session, without readmission."""


class SessionAdmissionError(RuntimeError):
    def __init__(self, *, uncertain: bool, generation: str | None = None,
                 handle: LaunchHandle | None = None) -> None:
        super().__init__("OpenCode admission uncertain; retain recovery state" if uncertain
                         else "OpenCode daemon admission unavailable")
        self.uncertain = uncertain
        self.launch_generation = generation
        self.handle = handle
        self.quiescent = False


class SessionAdmissionCancelled(asyncio.CancelledError):
    def __init__(self, handle: LaunchHandle | None, *, uncertain: bool = False,
                 generation: str | None = None) -> None:
        super().__init__("OpenCode admission cancelled")
        self.handle = handle
        self.uncertain = uncertain
        self.launch_generation = handle.launch_generation if handle else generation
        self.quiescent = False


@dataclass(frozen=True)
class SessionRequest:
    kind: str
    model: Mapping[str, str]
    text: str
    shell_allowlist: tuple[str, ...] = ()
    spawn_depth: int = 1
    agent: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"leaf", "spawn", "epic"}:
            raise ValueError("invalid session kind")
        if self.agent is not None and (not isinstance(self.agent, str) or not self.agent
                                      or len(self.agent.encode()) > 1024
                                      or any(not char.isprintable() for char in self.agent)):
            raise ValueError("invalid session agent")
        if type(self.spawn_depth) is not int or not 0 <= self.spawn_depth <= 1000:
            raise ValueError("invalid server-generated spawn depth")
        if (not isinstance(self.model, Mapping) or not {"providerID", "id"} <= self.model.keys()
                or self.model.keys() - {"providerID", "id", "variant"}
                or any(not isinstance(v, str) or not v or len(v.encode()) > 1024 or "\x00" in v
                       for v in self.model.values())):
            raise ValueError("invalid session model reference")
        if not isinstance(self.text, str) or "\x00" in self.text or len(self.text.encode()) > 1024 * 1024:
            raise ValueError("invalid session input")
        if (not isinstance(self.shell_allowlist, (tuple, list)) or len(self.shell_allowlist) > 128
                or any(not isinstance(rule, str) or not rule or len(rule.encode()) > 1024
                       or any(char in rule for char in "\x00\r\n") for rule in self.shell_allowlist)):
            raise ValueError("invalid session shell allowlist")


@dataclass(frozen=True)
class SessionError:
    type: str
    message: str
    body: str = ""
    retry_after_ms: int | None = None


@dataclass(frozen=True)
class SessionOutcome:
    opencode_session_id: str
    execution_state: str
    cancellation_state: str = "none"
    error: SessionError | None = None
    stdout: str = ""
    output_overflow: bool = False


@dataclass(frozen=True)
class WorkSpec:
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
    env: Mapping[str, str] = field(default_factory=dict)
    backend_config: Mapping[str, Any] = field(default_factory=dict)
    local_checkout: Path | None = None
    local_argv: Sequence[str] | None = None
    output_root: Path | None = None
    factory_credential_settings: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    session_request: SessionRequest | None = None


@dataclass(frozen=True)
class LaunchHandle:
    substrate: str
    identifier: str
    process_start_ticks: int | None = None
    shim_pid: int | None = None
    opencode_session_id: str | None = None
    launch_generation: str | None = None


@dataclass(frozen=True)
class ComputeResult:
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


class ComputeLaunchError(RuntimeError):
    pass


class ComputeBackend(Protocol):
    name: str

    async def launch(self, spec: WorkSpec) -> LaunchHandle: ...

    async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult: ...

    async def cancel(self, handle: LaunchHandle) -> None: ...

    async def cleanup(self, handle: LaunchHandle) -> None: ...


def with_worker_environment(spec: WorkSpec, additions: Mapping[str, str]) -> WorkSpec:
    """Add gate settings to both the environment and the worker pass list."""
    from dataclasses import replace

    if not additions:
        return spec
    env = {**spec.env, **additions}
    if "pass_env" not in spec.backend_config:
        return replace(spec, env=env)
    requested = spec.backend_config["pass_env"]
    if isinstance(requested, (str, bytes)) or not isinstance(requested, Sequence):
        raise ComputeLaunchError("worker pass_env must be a sequence")
    return replace(spec, env=env, backend_config={
        **spec.backend_config, "pass_env": tuple(dict.fromkeys((*requested, *additions))),
    })


@dataclass(frozen=True)
class Caps:
    tool_category: str
    persistent_sessions: bool
    json_output: bool
    native_pr_creation: bool
    quota_pool: str | None


class CheckoutShape(StrEnum):
    """Server-owned containment shape for a backend attempt checkout."""

    ISOLATED_CLONE = "isolated_clone"


@dataclass(frozen=True)
class WorkOrder:
    issue_id: int
    checkout: Path
    prompt: str
    rules: str | None
    timeout_s: int
    env: dict[str, str] = field(default_factory=dict)
    transcript_root: Path | None = None


BLOCKED_MARKER = "WORKLINK_BLOCKED:"


def blocked_reason_from_output(stdout: str, stderr: str) -> str | None:
    """Extract a backend-requested Worklink blocked reason from output.

    Backend CLIs are model-driven and may discover a planner/design flaw that
    deterministic Worklink cannot repair.  They can route that case back to the
    planner by emitting a line like ``WORKLINK_BLOCKED: <reason>`` to stdout or
    stderr.  The reason is intentionally plain text and bounded to one line so it
    can be copied into Chainlink evidence comments safely.

    The marker is honored only when it is the **final non-empty line** of stdout
    or stderr — exactly the convention the work-order prompt enforces ("emit it
    as the final line and stop"). Enforcing the final-line rule (not merely
    last-match-anywhere) means a backend that echoes the prompt's marker
    instruction near the top and then *completes normally* is not mislabeled
    blocked: the real final line is its success output, not the echoed marker.
    """
    for stream in (stdout, stderr):
        last = last_nonempty_line(stream)
        if last is None:
            continue
        stripped = last.strip()
        if stripped.startswith(BLOCKED_MARKER):
            reason = _clean_blocked_reason(stripped[len(BLOCKED_MARKER) :].strip())
            if reason:
                return reason
    return None


def last_nonempty_line(text: str) -> str | None:
    """Return the last line with non-whitespace content, or None.

    Shared by the backend signal readers so "the executor reported this" is
    distinguished from "this text appeared somewhere in the output" the same way
    for every marker — see ``blocked_reason_from_output`` and OpenCode's
    permission-refusal reader.
    """
    for line in reversed(text.splitlines()):
        if line.strip():
            return line
    return None


def _clean_blocked_reason(reason: str) -> str | None:
    # Keep the Chainlink label/comment payload bounded and one-line even if a
    # backend emits a paragraph after the marker.
    reason = reason.strip().replace("\x00", "")
    if not reason:
        return None
    return reason[:500]


@dataclass(frozen=True)
class RawResult:
    exit_code: int
    transcript_path: Path | None
    backend_status: str
    error: str | None
    blocked_reason: str | None = None
    output_overflow: bool = False
    opencode_session_id: str | None = None
    retry_after_ms: int | None = None


class ToolBackend(Protocol):
    name: str
    checkout_shape: CheckoutShape

    def capabilities(self) -> Caps: ...

    def work_spec(
        self,
        order: WorkOrder,
        *,
        attempt: int,
        repo_url: str,
        base_ref: str,
        branch: str,
        test_command: str,
    ) -> WorkSpec: ...

    async def interpret(self, order: WorkOrder, result: ComputeResult) -> RawResult: ...


def checkout_shape_for_backend(backend: ToolBackend) -> CheckoutShape:
    """Return a backend's shape, failing safely for older or third-party backends."""
    return getattr(backend, "checkout_shape", CheckoutShape.ISOLATED_CLONE)
