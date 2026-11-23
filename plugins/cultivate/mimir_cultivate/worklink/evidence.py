"""Worklink evidence schema, observation, and validation."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import pwd
import re
import shlex
import shutil
import signal
import stat as stat_module
import subprocess
import sys
import tempfile
from typing import Awaitable, Callable, Iterator, Protocol, Sequence
import uuid
import xml.etree.ElementTree as ET

from mimir.redaction import redact_text
from mimir.otel_credentials import filter_otlp_env
from .backends.base import (
    ComputeBackend,
    ComputeLaunchError,
    ComputeResult,
    LaunchHandle,
    WorkSpec,
    with_worker_environment,
)


log = logging.getLogger(__name__)


def terminal_error(value: BaseException | str) -> str:
    """Return a bounded, redacted final line for durable failure records."""
    text = f"{type(value).__name__}: {value}" if isinstance(value, BaseException) else value
    lines = [line.strip() for line in str(text).splitlines() if line.strip()]
    return redact_text(lines[-1] if lines else "Worklink run failed")[:1000]


@dataclass(frozen=True)
class CommandResult:
    cmd: str
    exit_code: int
    summary: str | None = None
    observed: bool = True


@dataclass(frozen=True)
class TestCounts:
    total: int
    passed: int
    failed: int
    errors: int
    skipped: int


@dataclass(frozen=True)
class TestResult:
    __test__ = False

    cmd: str | None
    exit_code: int | None = None
    summary: str | None = None
    skipped_reason: str | None = None
    observed: bool = True
    counts: TestCounts | None = None
    failed_tests: tuple[str, ...] = ()
    report_error: str | None = None
    # Failed in the gate but passed in isolation; diagnostic, not proof of flakiness.
    flaky_tests: tuple[str, ...] = ()
    initial_run: TestResult | None = None
    rerun: TestResult | None = None
    previous_observation: TestResult | None = None
    timed_out: bool = False
    retained_artifacts: tuple[str, ...] = ()
    retention_manifest: str | None = None
    retention_truncated: bool = False
    retention_error: str | None = None
    gate_run_id: str | None = None
    gate_phase: str | None = None
    output_truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    environment_failure: str | None = None
    lease_wait_timed_out: bool = False
    lease_wait_s: float = 0.0
    run_duration_s: float = 0.0


@dataclass(frozen=True)
class WorklinkEvidence:
    issue: int
    attempt: int
    backend: str
    branch: str
    checkout: str
    started_at: str
    finished_at: str
    files_changed: list[str]
    diff_stat: str
    commands: list[CommandResult]
    tests: TestResult | None
    pr_url: str | None
    status: str
    gate_byproducts_count: int = 0
    gate_byproducts_top_level: list[str] = field(default_factory=list)
    base_ref: str = "main"
    model: str | None = None
    failure_reason: str | None = None
    blocked_reason: str | None = None
    transcript: str | None = None
    diff_observed: bool = True
    executor_tests: TestResult | None = None
    gate_result_diverged: bool | None = None
    # chainlink #817: in-attempt gate-repair rounds this evidence reflects
    # (0 = the gate passed/failed without repair).
    repair_rounds: int = 0
    gate_mode: str = "local"
    ci_repair_rounds: int = 0
    ci_checks: tuple[tuple[str, str], ...] = ()
    # Commit pushed for this completed attempt. Recovery uses it to detect PR
    # branch updates made after Worklink finished.
    head_sha: str | None = None
    # PR head observed after publication; may advance beyond the verified head_sha.
    observed_pr_head_sha: str | None = None
    test_env: dict[str, str] = field(default_factory=dict)
    opencode_session_id: str | None = None
    mcp_servers: tuple[str, ...] = ()


@dataclass(frozen=True)
class EvidenceValidation:
    status: str
    review_ready: bool
    reasons: tuple[str, ...]
    evidence: WorklinkEvidence


Run = Callable[..., subprocess.CompletedProcess[str]]


class EvidenceGit(Protocol):
    def run(self, *args: str, check: bool = False) -> subprocess.CompletedProcess[str]: ...


EVIDENCE_SAFE_GIT_OPERATIONS = frozenset({
    "diff_name_only",
    "diff_stat",
    "status",
    "checkout_detach",
})

def validate_evidence(evidence: WorklinkEvidence) -> EvidenceValidation:
    """Validate and normalize backend-independent evidence.

    The review gate is intentionally based on observed diff/test data. A backend
    transcript saying "tests passed" is not enough: callers must provide a
    ``TestResult`` produced by the executor's own command run.
    """
    reasons: list[str] = []
    status = evidence.status

    if evidence.failure_reason:
        evidence = replace(
            evidence,
            failure_reason=terminal_error(evidence.failure_reason),
        )

    if status not in {"completed", "blocked", "failed"}:
        reasons.append("invalid_status")
        status = "failed"

    if status == "blocked" and not evidence.blocked_reason:
        reasons.append("blocked_missing_reason")
        status = "failed"

    if status == "blocked":
        return EvidenceValidation(status="blocked", review_ready=False, reasons=tuple(reasons), evidence=evidence)

    if status == "failed":
        if evidence.failure_reason:
            reasons.append(evidence.failure_reason)
        else:
            # A backend that reports "failed" without supplying text used to
            # produce a record with status=failed, failure_reason=null and an
            # EMPTY reasons list: every other transition below names itself,
            # but this one only did so when the backend happened to provide a
            # message. Chainlink #1108 was diagnosed from three such records —
            # each had committed work and a passing gate, and nothing said why
            # the run failed. `blocked` already has `blocked_missing_reason`
            # for exactly this; `failed` now has its counterpart.
            reasons.append("failed_missing_reason")
            evidence = replace(
                evidence,
                failure_reason=(
                    f"{evidence.backend} reported failure without a reason"
                ),
            )

    if status == "completed" and not evidence.files_changed:
        reasons.append("completed_empty_diff")
        status = "failed"

    if not evidence.diff_observed:
        reasons.append("diff_not_observed")
        status = "failed"

    tests_ok = False
    if evidence.tests is None:
        if status == "completed" and evidence.gate_mode == "ci":
            tests_ok = True  # CI is the authoritative gate after draft publication.
        elif status == "completed":
            reasons.append("tests_missing")
            status = "failed"
    elif not evidence.tests.observed:
        reasons.append("tests_not_observed")
        status = "failed"
    elif evidence.tests.skipped_reason is not None:
        # A recorded skip explains the missing gate; it does not pass it.
        tests_ok = False
    elif evidence.tests.lease_wait_timed_out:
        reasons.append("gate_lease_wait_timeout")
        if status == "completed":
            status = "failed"
        if not evidence.failure_reason:
            evidence = replace(evidence, failure_reason="test gate heavy lease wait timed out (infrastructure)")
    elif evidence.tests.timed_out:
        # A gate budget/configuration fault, not a failing assertion to repair.
        reasons.append("gate_timed_out")
        if status == "completed":
            status = "failed"
        if not evidence.failure_reason:
            evidence = replace(evidence, failure_reason="test gate timed out; check gate timeout configuration")
    elif evidence.tests.exit_code == 0:
        tests_ok = True
    elif evidence.tests.exit_code == 127:
        # chainlink #820: `sh -c` 127 means the gate COMMAND was not found — an
        # environment/config error no code change can fix. Distinct reason so
        # retries and #817 repair rounds are not spent on it.
        reasons.append("gate_command_not_found")
        if status == "completed":
            status = "failed"
        if not evidence.failure_reason:
            evidence = replace(
                evidence,
                failure_reason="test gate command was not found (exit 127)",
            )
    else:
        reasons.append("gate_environment_failure" if evidence.tests.environment_failure else "tests_failed")
        if status == "completed":
            status = "failed"
        if not evidence.failure_reason:
            evidence = replace(evidence, failure_reason=_gate_failure_reason(evidence.tests))

    review_ready = status == "completed" and bool(evidence.files_changed) and tests_ok and evidence.diff_observed
    if status != evidence.status:
        evidence = replace(evidence, status=status)
    return EvidenceValidation(status=status, review_ready=review_ready, reasons=tuple(reasons), evidence=evidence)


async def observe_evidence(
    *,
    issue: int,
    attempt: int,
    backend: str,
    branch: str,
    checkout: Path,
    started_at: datetime,
    base_ref: str,
    backend_status: str,
    test_command: str | None,
    gate_mode: str = "local",
    safe_git: EvidenceGit | None = None,
    head_ref: str = "HEAD",
    checkout_ref: str | None = None,
    work_spec: WorkSpec | None = None,
    compute: ComputeBackend | None = None,
    on_gate_launch: Callable[[LaunchHandle], None] | None = None,
    transcript: str | None = None,
    pr_url: str | None = None,
    blocked_reason: str | None = None,
    model: str | None = None,
    failure_reason: str | None = None,
    executor_tests: TestResult | None = None,
    skip_test_reason: str | None = None,
    runner: Run | None = None,
    gate_rerun_max_failures: int = 10,
    factory_run_id: str | None = None,
) -> EvidenceValidation:
    """Build evidence by observing a normalized checkout after a backend run."""
    return await _observe_evidence_from_ref(
        issue=issue,
        attempt=attempt,
        backend=backend,
        branch=branch,
        checkout=checkout,
        started_at=started_at,
        base_ref=base_ref,
        head_ref=head_ref,
        backend_status=backend_status,
        test_command=test_command,
        gate_mode=gate_mode,
        safe_git=safe_git,
        work_spec=work_spec,
        compute=compute,
        on_gate_launch=on_gate_launch,
        transcript=transcript,
        pr_url=pr_url,
        blocked_reason=blocked_reason,
        model=model,
        failure_reason=failure_reason,
        executor_tests=executor_tests,
        skip_test_reason=skip_test_reason,
        runner=runner,
        gate_rerun_max_failures=gate_rerun_max_failures,
        factory_run_id=factory_run_id,
        include_checkout_status=True,
        checkout_ref=checkout_ref,
    )


@contextlib.contextmanager
def _gate_report_directory(
    checkout: Path, worker_uid_drop: bool, *, cleanup_errors: list[str] | None = None,
) -> Iterator[Path]:
    """Yield a report directory the process that runs the gate can write.

    ``tempfile.TemporaryDirectory`` creates 0700 owned by the CONTROLLER. When the
    gate is dropped to the worker uid that directory is unwritable, and pytest
    raises ``PermissionError`` from ``pytest_sessionfinish`` while writing
    ``--junitxml`` -- AFTER every test has already passed. The gate then exits 1 on
    a green run, and ``read_pytest_result`` finds no junit.xml, so the failure is
    reported as ``exit 1, structured counts unavailable`` and names nothing.

    On the worker path the directory is created inside the attempt checkout, which
    is group-owned by the worker and setgid (2770), so the new directory inherits
    that group and both identities can use it: the worker writes the report, the
    controller reads it back. This is narrower than widening a temp directory to
    0777, and needs no shared group membership between the two identities.
    """
    if worker_uid_drop:
        checkout.mkdir(parents=True, exist_ok=True)
    temporary = tempfile.TemporaryDirectory(
        prefix=".worklink-gate-" if worker_uid_drop else "worklink-gate-",
        dir=checkout if worker_uid_drop else None,
    )
    try:
        report_dir = Path(temporary.name).resolve(strict=True)
        if worker_uid_drop:
            # setgid on the checkout supplies the group; make it group-usable.
            report_dir.chmod(0o770)
        yield report_dir
    finally:
        # Only cleanup is caught here, never an exception from the gate body.
        try:
            temporary.cleanup()
        except Exception as exc:
            if cleanup_errors is not None:
                cleanup_errors.append(f"report_cleanup_failed:{type(exc).__name__}")
            with contextlib.suppress(Exception):
                log.warning("Worklink gate report cleanup incomplete (%s)", type(exc).__name__)


_GATE_RETENTION_BYTES = 32 * 1024 * 1024
_GATE_RETENTION_ENTRIES = 2048
_GATE_RETENTION_DEPTH = 32


def _configured_gate_tmp_root() -> Path | None:
    """Validate operator-provisioned scratch without creating or repairing it."""
    value = os.environ.get("MIMIR_WORKLINK_GATE_TMPDIR")
    if value is None:
        return None
    from .identities import get_identities

    reason = "MIMIR_WORKLINK_GATE_TMPDIR"
    root = Path(value)
    if not root.is_absolute():
        raise ValueError(f"{reason} must be an absolute directory path")
    try:
        root = root.resolve(strict=True)
        metadata = root.stat()
    except OSError as exc:
        raise ValueError(f"{reason} directory unavailable: {exc}") from exc
    if not stat_module.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{reason} must be a directory")
    if metadata.st_uid != get_identities().worklink_uid:
        raise ValueError(f"{reason} must be owned by the worker uid")
    if stat_module.S_IMODE(metadata.st_mode) != 0o700:
        raise ValueError(f"{reason} must have mode 0700")
    return root


def _gate_tmp_directory(report_dir: Path) -> Path:
    # Pytest makes basetemp private. It must never be a child of the controller's
    # TemporaryDirectory: that identity cannot chmod/remove a worker's 0700 tree.
    # Nor may it live in the group-writable checkout: output_capture rejects that
    # ancestor. By default use shared host /tmp (sticky), not inherited TMPDIR
    # or per-launch HOME. Configured scratch has a separate worker-private parent.
    # Resolve only the trusted parent: macOS /tmp -> /private/tmp would otherwise
    # fail _gate_open's O_NOFOLLOW walk. Never resolve the worker-controlled leaf.
    # Hash the controller's unique report path into a single confined leaf.
    identity = hashlib.sha256(os.fsencode(report_dir.absolute())).hexdigest()
    parent = _configured_gate_tmp_root()
    return (parent if parent is not None else Path("/tmp").resolve()) / f"worklink-gate-{identity}-tmp"


def _export_gate_tmp(root: str, max_bytes: int, max_entries: int, max_depth: int) -> dict:
    """Export regular whole files, also executable under the worker identity.

    Keep imports local: the worker runs this source with isolated Python, without
    importing code from the untrusted checkout or the controller's environment.
    """
    import base64
    import os
    import stat

    result = {"files": [], "reasons": [], "entries": 0}
    try:
        metadata = os.stat(root, follow_symlinks=False)
    except FileNotFoundError:
        return result
    except OSError:
        result["reasons"].append("tmp_unavailable")
        return result
    if not stat.S_ISDIR(metadata.st_mode):
        result["reasons"].append("tmp_not_directory")
        return result

    remaining = max_bytes

    def omit(reason):
        if reason not in result["reasons"]:
            result["reasons"].append(reason)

    def visit(fd, prefix, depth):
        nonlocal remaining
        with os.scandir(fd) as entries:
            for entry in entries:
                if result["entries"] >= max_entries:
                    omit("entry_limit")
                    break
                result["entries"] += 1
                name = prefix + entry.name
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(metadata.st_mode):
                        if depth >= max_depth:
                            omit("depth_limit")
                            continue
                        child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                        try:
                            visit(child, name + "/", depth + 1)
                        finally:
                            os.close(child)
                    elif stat.S_ISREG(metadata.st_mode):
                        child = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                        with os.fdopen(child, "rb") as source:
                            metadata = os.fstat(source.fileno())
                            if not stat.S_ISREG(metadata.st_mode):
                                omit("unsafe_entry")
                                continue
                            if metadata.st_size > remaining:
                                omit("byte_limit")
                                continue
                            data = source.read(remaining + 1)
                            if len(data) > remaining or len(data) != metadata.st_size or os.fstat(source.fileno()).st_size != len(data):
                                omit("file_changed_or_oversize")
                                continue
                        remaining -= len(data)
                        result["files"].append({"name": name, "data": base64.b64encode(data).decode("ascii")})
                    else:
                        omit("unsafe_entry")
                except OSError:
                    omit("tmp_read_error")

    # Anchor every component, including ancestors of the export root.
    absolute = os.path.isabs(root)
    components = root.split("/")[1:] if absolute else root.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError("invalid export root")
    fd = os.open("/" if absolute else ".", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in components:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        visit(fd, "", 1)
    finally:
        os.close(fd)
    return result


async def _retain_gate_failure(
    observation: TestResult,
    output: subprocess.CompletedProcess[str],
    *,
    report_dir: Path,
    issue: int,
    attempt: int,
    run_id: str,
    phase: str,
    checkout: Path,
    work_spec: WorkSpec | None,
    compute: ComputeBackend | None,
    compute_result: ComputeResult | None,
    worker_uid_drop: bool,
) -> TestResult:
    """Best-effort diagnostics only. Gate execution is outside this boundary."""
    artifacts: list[str] = []
    records: list[dict] = []
    reasons: list[str] = []
    manifest_path = None
    remaining = _GATE_RETENTION_BYTES
    entries = 1  # Reserve an entry for the manifest in the shared budget.
    try:
        import base64
        import inspect
        from mimir.output_capture import open_output_sink

        root = (work_spec.output_root if work_spec else None) or (
            Path(os.environ.get("MIMIR_HOME", ".")).resolve() / "state/worklink/transcripts"
        )
        if not root.is_absolute() or ".." in root.parts:
            raise ValueError("retention destination must be an absolute normalized path")
        if any(root.is_relative_to(directory) for directory in (report_dir, _gate_tmp_directory(report_dir))):
            raise ValueError("retention destination is inside a temporary gate directory")
        stem = f"gate-{issue}-a{attempt}-{run_id}-{phase}"

        def write(name: str, data: bytes) -> None:
            nonlocal remaining, entries
            if entries >= _GATE_RETENTION_ENTRIES:
                reasons.append("entry_limit")
                return
            entries += 1
            try:
                data = redact_text(data.decode("utf-8")).encode("utf-8")
            except UnicodeDecodeError:
                pass
            if len(data) > remaining:
                reasons.append("byte_limit")
                return
            destination = root / f"{stem}-{len(artifacts):04d}.artifact"
            sink = open_output_sink(destination, max(1, len(data)))
            try:
                sink.file.write(data)
            except Exception:
                destination.unlink(missing_ok=True)
                raise
            finally:
                sink.close()
            remaining -= len(data)
            artifacts.append(str(destination))
            records.append({"source": redact_text(name), "path": str(destination), "bytes": len(data)})

        def copy(name: str, source: Path) -> None:
            try:
                if name == "junit.xml":
                    # Leave room for output and the manifest. A large repeated
                    # setup traceback is useful as a prefix, not a retention error.
                    data, truncated = _gate_read_prefix(
                        source, min(_PYTEST_REPORT_MAX_BYTES, remaining - min(4096, remaining // 10)),
                    )
                    if truncated:
                        reasons.append("junit_truncated")
                else:
                    data = _gate_read(source, remaining)
            except (OSError, ValueError) as exc:
                reasons.append(f"{name}:{type(exc).__name__}")
                return
            write(name, data)

        if _is_pytest_command(observation.cmd or ""):
            copy("junit.xml", report_dir / "junit.xml")
        for stream in ("stdout", "stderr"):
            path = getattr(compute_result, f"{stream}_path", None)
            if path is not None:
                copy(stream, path)
            else:
                write(stream, (getattr(output, stream) or "").encode("utf-8"))
        if compute_result is not None and compute_result.output_overflow:
            reasons.append("output_overflow")

        tmp = _gate_tmp_directory(report_dir)
        # A missing basetemp means pytest never used tmp_path. Permission/I/O
        # failures instead mean its contents are unknown, not known empty.
        if worker_uid_drop and _configured_gate_tmp_root() is not None:
            # The controller cannot traverse the worker's 0700 scratch parent.
            # The exporter handles missing trees under the owning identity.
            tmp_available = True
        else:
            try:
                tmp.lstat()
            except FileNotFoundError:
                tmp_available = False
            except OSError:
                tmp_available = False
                reasons.append("tmp_unavailable")
            else:
                tmp_available = True
        exported_tmp = {"files": [], "reasons": [], "entries": 0}
        if tmp_available and worker_uid_drop:
            # Execute only the exporter in this guard, never the test command.
            script = (
                inspect.getsource(_export_gate_tmp)
                + "\nimport json, shutil\n"
                + f"root = {str(tmp)!r}\n"
                + "try:\n"
                + f" print(json.dumps(_export_gate_tmp(root, {remaining}, {_GATE_RETENTION_ENTRIES - entries}, {_GATE_RETENTION_DEPTH})))\n"
                + "finally:\n shutil.rmtree(root, ignore_errors=True)\n"
            )
            # ComputeResult.stdout may be an excerpt. The controller owns these
            # sinks; consume the full bounded export before deleting them.
            with tempfile.TemporaryDirectory(prefix="worklink-gate-export-") as text:
                export_root = Path(text).resolve(strict=True)
                exported = await _run_compute_gate(
                    shlex.join(["python3", "-I", "-c", script]),
                    checkout=checkout, work_spec=replace(work_spec, output_root=export_root), compute=compute,
                    diagnostic=True,
                )
                if exported.exit_code or exported.timed_out or exported.output_overflow:
                    raise ValueError("tmp_export_failed")
                if exported.stdout_path is None or exported.stdout_path.parent != export_root:
                    raise ValueError("tmp_export_output_unavailable")
                document = _gate_read(exported.stdout_path, 2 * _GATE_RETENTION_BYTES)
                exported_tmp = json.loads(document)
        elif tmp_available:
            exported_tmp = _export_gate_tmp(
                str(tmp), remaining, _GATE_RETENTION_ENTRIES - entries, _GATE_RETENTION_DEPTH,
            )
        reasons.extend(exported_tmp["reasons"])
        for item in exported_tmp["files"]:
            if entries >= _GATE_RETENTION_ENTRIES:
                reasons.append("entry_limit")
                break
            # Names are metadata only, never destination paths.
            name = item["name"]
            if len(Path(name).parts) > _GATE_RETENTION_DEPTH:
                reasons.append("depth_limit")
                continue
            write("tmp/" + name, base64.b64decode(item["data"], validate=True))
        document = json.dumps({
            "issue": issue, "attempt": attempt, "run_id": run_id, "phase": phase,
            "exit_code": observation.exit_code, "timed_out": observation.timed_out,
            "artifacts": records, "truncated": bool(reasons),
            "reasons": sorted(set(reasons)),
        }, ensure_ascii=True).encode("utf-8")
        destination = root / f"{stem}.json"
        if len(document) > remaining:
            reasons.append("manifest_byte_limit")
            raise ValueError("manifest_byte_limit")
        if entries > _GATE_RETENTION_ENTRIES:
            reasons.append("manifest_entry_limit")
            raise ValueError("manifest_entry_limit")
        sink = open_output_sink(destination, max(1, len(document)))
        try:
            sink.file.write(document)
        except Exception:
            destination.unlink(missing_ok=True)
            raise
        finally:
            sink.close()
        manifest_path = str(destination)
    except Exception as exc:
        reasons.append(f"retention_failed:{type(exc).__name__}")
    if reasons:
        with contextlib.suppress(Exception):
            log.warning("Worklink gate retention incomplete issue=%s attempt=%s run=%s phase=%s (%s)",
                        issue, attempt, run_id, phase, ", ".join(sorted(set(reasons))))
    return replace(
        observation, retained_artifacts=tuple(artifacts), retention_manifest=manifest_path,
        retention_truncated=bool(reasons), retention_error=", ".join(sorted(set(reasons))) or None,
        gate_run_id=run_id, gate_phase=phase,
    )


async def _observe_evidence_from_ref(
    *,
    issue: int,
    attempt: int,
    backend: str,
    branch: str,
    checkout: Path,
    started_at: datetime,
    base_ref: str,
    head_ref: str,
    backend_status: str,
    test_command: str | None,
    gate_mode: str = "local",
    safe_git: EvidenceGit | None,
    work_spec: WorkSpec | None,
    compute: ComputeBackend | None,
    on_gate_launch: Callable[[LaunchHandle], None] | None,
    transcript: str | None,
    pr_url: str | None,
    blocked_reason: str | None,
    model: str | None,
    failure_reason: str | None,
    executor_tests: TestResult | None,
    skip_test_reason: str | None,
    runner: Run | None,
    include_checkout_status: bool,
    checkout_ref: str | None = None,
    pre_commands: list[CommandResult] | None = None,
    pre_observed: bool = True,
    gate_rerun_max_failures: int = 10,
    factory_run_id: str | None = None,
) -> EvidenceValidation:
    runner = runner or _run
    if work_spec is not None:
        execution = work_spec.execution
    else:
        # Historical read-only evidence calls have no launch spec. Resolve their
        # registered tool's capabilities, never a compute-name containment gate.
        from .backends.registry import BackendRegistry
        from .execution_policy import ExecutionPolicy, execution_for_backend
        try:
            execution = execution_for_backend(BackendRegistry().get(backend))
        except KeyError:
            execution = ExecutionPolicy()
    factory = execution.checkout_protocol == "factory_worker"
    worker_uid_drop = not factory or compute is not None
    if factory and factory_run_id is not None and test_command and not skip_test_reason:
        from .worker_client import factory_checkout_for_path

        binding = factory_checkout_for_path(checkout)
        if (
            binding is None
            or checkout != binding[0] / ".factory-sandboxes" / factory_run_id
            or binding[1:] != (issue, attempt)
            or not callable(getattr(compute, "_launch_contained", None))
            or work_spec is None
            or work_spec.local_checkout != checkout
            or work_spec.issue_id != issue
            or work_spec.attempt != attempt
            or work_spec.branch != branch
            or work_spec.backend != backend
            or work_spec.test_command != test_command
        ):
            raise ComputeLaunchError("Cultivate factory evidence gate requires the recorded worker sandbox")
    if not factory and worker_uid_drop and test_command and not skip_test_reason:
        if (work_spec is not None and work_spec.execution.substrate == "opencode_daemon"
                and not callable(getattr(compute, "_launch_contained", None))):
            from .compute import LocalSubprocessComputeBackend
            from .identities import get_identities
            if work_spec is None or work_spec.session_request is None or work_spec.session_request.kind != "leaf":
                raise ComputeLaunchError("daemon evidence gate requires the leaf session work spec")
            compute = LocalSubprocessComputeBackend.for_path_checkout(get_identities().worklink_uid)
        authorization = getattr(compute, "_authorized_checkout", None)
        worker_bound = (
            not callable(getattr(compute, "_launch_contained", None))
            or work_spec is None
            or work_spec.local_checkout != checkout
            or work_spec.issue_id != issue
            or work_spec.attempt != attempt
            or work_spec.branch != branch
            or work_spec.backend != backend
            or work_spec.test_command != test_command
        )
        if authorization is None:
            # Leaf builds (base's model): a path-addressed checkout that the
            # root executor opens for the configured worklink uid. Never the
            # controller uid, and never launch()'s controller fallback.
            from .identities import get_identities

            worker_bound = worker_bound or (
                getattr(compute, "_worker_uid", None) != get_identities().worklink_uid
            )
        else:
            worker_bound = worker_bound or (
                not all(hasattr(authorization, member) for member in ("verify", "duplicate_fd", "path"))
                or authorization.path != checkout
                or authorization.issue_id != issue
                or authorization.attempt != attempt
            )
        if worker_bound:
            raise ComputeLaunchError("Cultivate evidence gate requires a worker-bound checkout")
        if authorization is not None:
            authorization.verify(checkout)
    range_ref = f"{base_ref}...{head_ref}"
    def git_run(*args: str) -> subprocess.CompletedProcess[str]:
        if safe_git is not None:
            return safe_git.run(*args)
        return runner(["git", "-C", str(checkout), *args])

    committed = git_run("diff", "--name-only", range_ref)
    stat = git_run("diff", "--stat", range_ref)
    status = None
    if include_checkout_status:
        status = git_run(
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
    path_groups = [[line for line in committed.stdout.splitlines() if line.strip()]]
    if status is not None:
        path_groups.append(_paths_from_status(status.stdout))
    files_changed = _merge_paths(*path_groups)
    commands: list[CommandResult] = list(pre_commands or [])
    commands.extend([
        CommandResult(f"git diff --name-only {range_ref}", committed.returncode, _summarize(committed)),
        CommandResult(f"git diff --stat {range_ref}", stat.returncode, stat.stdout.strip()),
    ])
    if status is not None:
        commands.append(
            CommandResult(
                "git status --porcelain=v1 --untracked-files=all",
                status.returncode,
                _summarize(status),
            )
        )

    tests: TestResult | None = None
    checkout_result = None
    if checkout_ref:
        checkout_result = git_run("checkout", "--detach", checkout_ref)
        commands.append(
            CommandResult(
                f"git checkout --detach {checkout_ref}",
                checkout_result.returncode,
                _summarize(checkout_result),
            )
        )
    if test_command and skip_test_reason:
        tests = TestResult(test_command, skipped_reason=skip_test_reason)
    elif test_command:
        if checkout_result is not None and checkout_result.returncode != 0:
            tests = TestResult(test_command, None, "checkout failed before test", observed=False)
        else:
            run_id = uuid.uuid4().hex

            async def run_gate(command: str, report_dir: Path, phase: str) -> TestResult:
                timed_out = False
                result = None
                if worker_uid_drop:
                    if compute is None:
                        raise ValueError("enabled worker evidence requires a compute backend")
                    if work_spec is None:
                        raise ValueError("worker evidence requires the originating WorkSpec")
                    result = await _run_compute_gate(
                        command,
                        checkout=checkout,
                        work_spec=work_spec,
                        compute=compute,
                        on_launch=on_gate_launch,
                        report_dir=report_dir,
                    )
                    timed_out = result.timed_out
                    test = subprocess.CompletedProcess(
                        ["/bin/sh", "-c", command],
                        result.exit_code,
                        stdout=result.stdout,
                        stderr=result.stderr,
                    )
                else:
                    timeout_s = work_spec.timeout_s if work_spec is not None else 1800
                    async def run_local(leg: str, budget: float) -> ComputeResult:
                        observed_command = _command_with_pytest_report(leg, report_dir)
                        try:
                            observed = await asyncio.to_thread(
                                runner, observed_command, cwd=checkout, timeout=budget,
                            )
                            return ComputeResult(observed.returncode, observed.stdout, observed.stderr)
                        except subprocess.TimeoutExpired as exc:
                            # TimeoutExpired may carry bytes even with text=True.
                            stdout, stderr = exc.stdout or "", exc.stderr or ""
                            return ComputeResult(
                                124,
                                stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout,
                                stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr,
                                timed_out=True,
                            )
                    result = await _run_gate_with_lease(command, timeout_s, run_local)
                    timed_out = result.timed_out
                    test = subprocess.CompletedProcess(
                        command, result.exit_code, result.stdout, result.stderr,
                    )
                structured = read_pytest_result(command, report_dir)
                commands.append(CommandResult(redact_text(command), test.returncode, redact_text(_summarize(test))))
                observation = replace(
                    structured or TestResult(redact_text(command)),
                    cmd=redact_text(command),
                    exit_code=test.returncode,
                    summary=redact_text(_summarize_test_output(test)),
                    timed_out=timed_out,
                    gate_run_id=run_id,
                    gate_phase=phase,
                    output_truncated=result.output_truncated if result is not None else False,
                    stdout_bytes=result.stdout_bytes if result is not None else 0,
                    stderr_bytes=result.stderr_bytes if result is not None else 0,
                    lease_wait_timed_out=result.lease_wait_timed_out if result is not None else False,
                    lease_wait_s=result.lease_wait_s if result is not None else 0.0,
                    run_duration_s=result.run_duration_s if result is not None else 0.0,
                )
                if test.returncode != 0 or timed_out or phase == "rerun":
                    try:
                        observation = await _retain_gate_failure(
                            observation, test, report_dir=report_dir, issue=issue,
                            attempt=attempt, run_id=run_id, phase=phase, checkout=checkout,
                            work_spec=work_spec, compute=compute, compute_result=result,
                            worker_uid_drop=worker_uid_drop,
                        )
                    except Exception as exc:
                        with contextlib.suppress(Exception):
                            log.warning("Worklink gate retention failed (%s)", type(exc).__name__)
                        observation = replace(
                            observation, retention_truncated=True,
                            retention_error=f"retention_failed:{type(exc).__name__}",
                        )
                # Cleanup is also diagnostic-only and must not mask the verdict.
                try:
                    tmp = _gate_tmp_directory(report_dir)
                    if not worker_uid_drop:
                        try:
                            shutil.rmtree(tmp)
                        except FileNotFoundError:
                            pass
                    else:
                        tmp_available = _configured_gate_tmp_root() is not None
                        if not tmp_available:
                            try:
                                tmp.lstat()
                            except FileNotFoundError:
                                pass
                            else:
                                tmp_available = True
                        if tmp_available:
                            code = (
                                "import shutil\n"
                                "try:\n"
                                f" shutil.rmtree({str(tmp)!r})\n"
                                "except FileNotFoundError:\n pass\n"
                            )
                            cleanup = await _run_compute_gate(
                                shlex.join(["python3", "-I", "-c", code]),
                                checkout=checkout, work_spec=replace(work_spec, output_root=None), compute=compute,
                                diagnostic=True,
                            )
                            if cleanup.exit_code != 0:
                                raise OSError("tmp_cleanup_failed")
                except Exception as exc:
                    with contextlib.suppress(Exception):
                        log.warning("Worklink gate tmp cleanup incomplete (%s)", type(exc).__name__)
                    observation = replace(
                        observation, retention_truncated=True,
                        retention_error=", ".join(filter(None, (observation.retention_error, "tmp_cleanup_failed"))),
                    )
                return observation

            cleanup_errors: list[str] = []
            with _gate_report_directory(checkout, worker_uid_drop, cleanup_errors=cleanup_errors) as report_dir:
                tests = await run_gate(test_command, report_dir, "initial")
                failed_ids = _pytest_cache_ids(report_dir, "lastfailed")
                rerun_command = _pytest_rerun_command(test_command, failed_ids)
                eligible = (
                    tests.exit_code == 1
                    and not tests.timed_out
                    and tests.report_error is None
                    and tests.counts is not None
                    and tests.counts.errors == 0
                    and tests.counts.failed == len(failed_ids)
                    and 0 < len(failed_ids) <= gate_rerun_max_failures
                    and rerun_command is not None
                )
            if cleanup_errors:
                tests = replace(tests, retention_truncated=True, retention_error=", ".join(
                    filter(None, (tests.retention_error, *cleanup_errors)),
                ))
            if eligible:
                cleanup_errors = []
                with _gate_report_directory(checkout, worker_uid_drop, cleanup_errors=cleanup_errors) as rerun_dir:
                    rerun = await run_gate(rerun_command, rerun_dir, "rerun")
                    remaining = _pytest_cache_ids(rerun_dir, "lastfailed")
                    collected = _pytest_cache_ids(rerun_dir, "nodeids")
                    counts = rerun.counts
                    # An exit-zero rerun alone is not proof: selection, skips,
                    # missing reports or a different collected set must fail closed.
                    complete = (
                        rerun.exit_code in {0, 1}
                        and not rerun.timed_out
                        and rerun.report_error is None
                        and counts is not None
                        and counts.total == len(failed_ids)
                        and counts.errors == 0
                        and counts.skipped == 0
                        and counts.failed == len(remaining)
                        and counts.passed + counts.failed == counts.total
                        and set(collected) == set(failed_ids)
                        and set(remaining) <= set(failed_ids)
                        and (rerun.exit_code == 0) == (counts.failed == 0)
                    )
                if cleanup_errors:
                    rerun = replace(rerun, retention_truncated=True, retention_error=", ".join(
                        filter(None, (rerun.retention_error, *cleanup_errors)),
                    ))
                initial = tests
                tests = replace(tests, initial_run=initial, rerun=rerun, timed_out=rerun.timed_out)
                if complete:
                    # Deliberately supersedes Chainlink #1557's verdict-flip:
                    # parallel-only failures (xdist ordering/shared state) are real
                    # defects; passing in isolation must not hide them. Preserve
                    # the gate's verdict, counts and failures; rerun is evidence only.
                    tests = replace(
                        tests,
                        flaky_tests=tuple(
                            redact_text(node)[:1000] for node in failed_ids if node not in remaining
                        ),
                    )

    evidence = WorklinkEvidence(
        issue=issue,
        attempt=attempt,
        backend=backend,
        branch=branch,
        checkout=str(checkout),
        started_at=started_at.astimezone(UTC).isoformat(),
        finished_at=datetime.now(UTC).isoformat(),
        files_changed=files_changed,
        diff_stat=stat.stdout.strip(),
        commands=commands,
        tests=tests,
        pr_url=pr_url,
        status=_common_status(backend_status),
        gate_mode=gate_mode,
        base_ref=work_spec.base_ref if work_spec is not None else base_ref,
        model=model,
        failure_reason=failure_reason,
        blocked_reason=blocked_reason,
        transcript=transcript,
        test_env=dict(work_spec.backend_config.get("test_env", {})) if work_spec else {},
        executor_tests=executor_tests,
        gate_result_diverged=_gate_results_diverge(executor_tests, tests),
        diff_observed=pre_observed
        and committed.returncode == 0
        and stat.returncode == 0
        and (status is None or status.returncode == 0),
    )
    return validate_evidence(evidence)


def _gate_lease_legs(command: str) -> tuple[str, ...]:
    """Recognize standalone heavy-run legs, not arbitrary shell programs.

    Only a literal argv AND-list can be moved across the containment boundary:
    shell expansion, redirections and stateful shell commands must retain their
    original single-shell semantics and deadline. Quoting is preserved by shlex.
    """
    legs: list[list[str]] = [[]]
    # Keep quotes until AFTER recognizing operators. A quoted '&&' is an
    # argument, not permission to split a command into independent workers.
    word = re.compile(r"""[ \t]+|&&|(?:[^\s;&|<>()'"\\$`*?\[#~]+|'[^']*'|"[^"$`\\]*")+""")
    offset = 0
    while offset < len(command):
        match = word.match(command, offset)
        if match is None:
            return ()
        raw = match[0]
        offset = match.end()
        if raw.isspace():
            continue
        if raw == "&&":
            legs.append([])
        else:
            legs[-1].append(shlex.split(raw)[0])
    if not all(
        len(leg) > 2 and leg[0] in {"sh", "/bin/sh"}
        and leg[1] in {"scripts/heavy_run.sh", "./scripts/heavy_run.sh"}
        for leg in legs
    ):
        return ()
    return tuple(shlex.join(["env", "MIMIR_HEAVY_LOCK_HELD=1", *leg[2:]]) for leg in legs)


# This child is NOT the gate. Only the controller-owned helper can write its
# stdout; the worker never inherits this pipe or supplies timing telemetry.
_GATE_LEASE_READY = "import sys; sys.stdout.write('ready\\n'); sys.stdout.flush(); sys.stdin.buffer.read()"


class _GateLeaseWaitTimeout(TimeoutError):
    pass


@contextlib.asynccontextmanager
async def _gate_heavy_lease(
    environment: dict[str, str],
    on_launch: Callable[[LaunchHandle], None] | None = None,
):
    helper = Path(__file__).resolve().parents[4] / "scripts" / "heavy_run.py"
    # Use the unchanged protocol helper directly. Its shell wrapper's timeout
    # covers queue + run; here the controller owns the two independent budgets.
    launch = asyncio.create_task(asyncio.create_subprocess_exec(
        sys.executable, "-I", str(helper), sys.executable, "-I", "-c", _GATE_LEASE_READY,
        env={"PATH": os.defpath, **environment},
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    ))
    interrupted = False
    while True:
        try:
            process = await asyncio.shield(launch)
            break
        except asyncio.CancelledError:
            interrupted = True
    try:
        if interrupted:
            raise asyncio.CancelledError
        if on_launch is not None:
            from .run_state import process_start_ticks

            # Queued helpers must be stoppable through the same durable handle
            # path as executing gates. The actual worker supersedes this handle
            # when launched; the next leg publishes its own queued helper.
            on_launch(LaunchHandle(
                "local_subprocess", str(process.pid), process_start_ticks(process.pid),
            ))
        assert process.stdout is not None
        try:
            ready = await asyncio.wait_for(
                process.stdout.readline(), float(environment.get("MIMIR_HEAVY_LOCK_TIMEOUT", "7200")),
            )
        except TimeoutError as exc:
            raise _GateLeaseWaitTimeout from exc
        if ready != b"ready\n":
            code = await process.wait()
            raise ComputeLaunchError(f"gate lease helper failed (exit {code})")
        yield
    finally:
        # TERM, not KILL: the helper releases its ticket and any reserved slot.
        # Do not abandon this reap when cancellation arrives during cleanup.
        async def retire() -> None:
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            await process.wait()
        retired = asyncio.create_task(retire())
        while True:
            try:
                await asyncio.shield(retired)
                break
            except asyncio.CancelledError:
                interrupted = True
        if process.stdin is not None:
            process.stdin.close()
        if interrupted:
            raise asyncio.CancelledError


async def _run_compute_gate(
    command: str,
    *,
    checkout: Path,
    work_spec: WorkSpec,
    compute: ComputeBackend,
    on_launch: Callable[[LaunchHandle], None] | None = None,
    report_dir: Path | None = None,
    diagnostic: bool = False,
) -> ComputeResult:
    async def run_command(leg: str, budget: float) -> ComputeResult:
        return await _run_compute_gate_command(
            leg, checkout=checkout, work_spec=replace(work_spec, timeout_s=budget),
            compute=compute, on_launch=on_launch, report_dir=report_dir, diagnostic=diagnostic,
        )

    if diagnostic:
        return await run_command(command, work_spec.timeout_s)
    return await _run_gate_with_lease(command, work_spec.timeout_s, run_command, on_launch=on_launch)


async def _run_gate_with_lease(
    command: str, timeout_s: float,
    run_command: Callable[[str, float], Awaitable[ComputeResult]],
    *,
    on_launch: Callable[[LaunchHandle], None] | None = None,
) -> ComputeResult:
    from .compute import _heavy_lock_environment

    environment = _heavy_lock_environment()
    legs = _gate_lease_legs(command) if environment else ()
    if not legs:
        return await run_command(command, timeout_s)
    loop = asyncio.get_running_loop()
    queue_s = run_s = 0.0
    stdout: list[str] = []
    stderr: list[str] = []
    stdout_bytes = stderr_bytes = 0
    truncated = False
    result = ComputeResult(0, "", "")
    for leg in legs:
        queued = loop.time()
        try:
            async with _gate_heavy_lease(environment, on_launch=on_launch):
                queue_s += loop.time() - queued
                started = loop.time()
                result = await run_command(leg, max(0.001, timeout_s - run_s))
                run_s += loop.time() - started
        except _GateLeaseWaitTimeout:
            queue_s += loop.time() - queued
            result = ComputeResult(124, "", "gate heavy lease wait timed out", lease_wait_timed_out=True)
        stdout.append(result.stdout)
        stderr.append(result.stderr)
        stdout_bytes += result.stdout_bytes
        stderr_bytes += result.stderr_bytes
        truncated |= result.output_truncated
        if result.exit_code != 0 or result.timed_out or result.lease_wait_timed_out:
            break
        if run_s >= timeout_s:
            result = replace(result, timed_out=True)
            break
    return replace(
        result, stdout="".join(stdout), stderr="".join(stderr),
        stdout_bytes=stdout_bytes, stderr_bytes=stderr_bytes, output_truncated=truncated,
        lease_wait_s=queue_s, run_duration_s=run_s,
    )


async def _run_compute_gate_command(
    command: str,
    *,
    checkout: Path,
    work_spec: WorkSpec,
    compute: ComputeBackend,
    on_launch: Callable[[LaunchHandle], None] | None = None,
    report_dir: Path | None = None,
    diagnostic: bool = False,
) -> ComputeResult:
    # Diagnostics get their own job budget, never the test's full allowance.
    # Keep deadline enforcement and bounded process teardown in compute.wait;
    # the worker-side timeout must agree with that supervisor too.
    gate_spec = replace(
        work_spec,
        execution=replace(work_spec.execution, substrate="contained_subprocess",
                          retirement="contained_process_tree", home_projection_profile="none"),
        backend_config={key: value for key, value in work_spec.backend_config.items()
                        if key not in {"worker_identifier", "worker_projections"}},
        session_request=None,
        local_checkout=checkout,
        local_argv=("/bin/sh", "-c", command),
        timeout_s=min(work_spec.timeout_s, 60) if diagnostic else work_spec.timeout_s,
    )
    if work_spec.execution.home_projection_profile == "claude_leaf":
        # Deterministic gates need the test environment, not the model's OAuth
        # credential. Their ordinary retained output must not acquire it.
        from ..claude_code import OAUTH_ENV
        gate_spec = replace(
            gate_spec,
            env={key: value for key, value in gate_spec.env.items() if key != OAUTH_ENV},
            backend_config={**gate_spec.backend_config, "pass_env": tuple(
                name for name in gate_spec.backend_config.get("pass_env", ()) if name != OAUTH_ENV
            )},
        )
    if work_spec.backend == "feature_factory":
        # Completion tests use the leaf's closed environment contract, not the
        # factory control/CLI environment. Report injections must be requested too.
        gate_spec = replace(gate_spec, backend_config={
            **gate_spec.backend_config,
            "pass_env": gate_spec.backend_config.get("pass_env", tuple(filter_otlp_env(gate_spec.env))),
        })
    if report_dir is not None:
        report_option_dir = report_dir
        try:
            # Contained workers enter the checkout through an authorized fd and
            # may not be able to traverse the controller's absolute parent path.
            report_option_dir = report_dir.relative_to(checkout)
        except ValueError:
            pass
        gate_spec = with_worker_environment(
            gate_spec,
            pytest_report_environment(
                command,
                report_option_dir,
                existing=gate_spec.env.get("PYTEST_ADDOPTS"),
                create_directory=False,
                basetemp=_gate_tmp_directory(report_dir),
            ),
        )
    gate_spec = replace(gate_spec, env=filter_otlp_env(gate_spec.env))
    if (work_spec.execution.substrate == "opencode_daemon"
            and not callable(getattr(compute, "_launch_contained", None))):
        from .compute import LocalSubprocessComputeBackend
        from .identities import get_identities
        compute = LocalSubprocessComputeBackend.for_path_checkout(get_identities().worklink_uid)
    if work_spec.backend == "feature_factory" or work_spec.execution.checkout_protocol == "factory_worker":
        from .compute import _enabled_child_env
        from .worker_client import run_factory_control

        # The executor authenticates the controller-owned checkout by FD; the
        # sandbox is the factory's nested clone, not a separate issued checkout.
        command_in_sandbox = f"cd {shlex.quote(str(checkout))} && {command}"
        identifier = str(uuid.uuid4())
        try:
            observed = await asyncio.to_thread(
                run_factory_control, checkout, ("/bin/sh", "-c", command_in_sandbox),
                env=_enabled_child_env(gate_spec, identifier),
                identifier=identifier,
                timeout=gate_spec.timeout_s, output_limit=20_000_000,
                allow_output_truncation=True,
            )
        except subprocess.TimeoutExpired:
            return ComputeResult(124, "", "factory gate timed out", timed_out=True)
        return ComputeResult(
            observed.returncode,
            observed.stdout.decode("utf-8", errors="replace"),
            observed.stderr.decode("utf-8", errors="replace"),
            output_truncated=getattr(observed, "output_truncated", False),
            stdout_bytes=getattr(observed, "stdout_bytes", len(observed.stdout)),
            stderr_bytes=getattr(observed, "stderr_bytes", len(observed.stderr)),
        )
    # Enter the contained worker path explicitly; never let a provider gate
    # choose launch()'s controller fallback.
    handle = await compute._launch_contained(gate_spec, tuple(gate_spec.local_argv))
    try:
        if on_launch is not None:
            try:
                on_launch(handle)
            except BaseException:
                await compute.cancel(handle)
                raise
        return await compute.wait(handle, gate_spec.timeout_s)
    except asyncio.CancelledError:
        await compute.cancel(handle)
        raise
    finally:
        await compute.cleanup(handle)


def _common_status(status: str) -> str:
    normalized = status.lower().strip()
    if normalized in {"completed", "success", "succeeded", "ok"}:
        return "completed"
    if normalized in {"blocked", "needs_human"}:
        return "blocked"
    return "failed"


_PYTEST_REPORT_MAX_BYTES = 20_000_000


@contextlib.contextmanager
def _gate_open(path: Path):
    """Open a fully resolved absolute path without following any symlinks."""
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("gate path must be absolute and resolved")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        child = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(child, "rb") as source:
            if not stat_module.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise OSError("gate report is not a regular file")
            yield source
    finally:
        os.close(fd)


def _gate_read(path: Path, max_bytes: int) -> bytes:
    with _gate_open(path) as source:
        before = os.fstat(source.fileno())
        if before.st_size > max_bytes:
            raise ValueError("gate report exceeds byte limit")
        data = source.read(max_bytes + 1)
        after = os.fstat(source.fileno())
        if len(data) > max_bytes or len(data) != before.st_size or after.st_size != len(data):
            raise ValueError("gate report changed or exceeds byte limit")
        return data


def _gate_read_prefix(path: Path, max_bytes: int) -> tuple[bytes, bool]:
    """Read a bounded diagnostic prefix, retaining the report's safety checks."""
    with _gate_open(path) as source:
        before = os.fstat(source.fileno())
        data = source.read(max_bytes)
        after = os.fstat(source.fileno())
        if after.st_size != before.st_size or len(data) != min(before.st_size, max_bytes):
            raise ValueError("gate report changed while reading prefix")
        return data, before.st_size > max_bytes


def _pytest_cache_ids(report_dir: Path, name: str) -> tuple[str, ...]:
    """Keep executable selectors private; evidence IDs are separately scrubbed."""
    try:
        path = report_dir / "cache" / "v" / "cache" / name
        payload = json.loads(_gate_read(path, _PYTEST_REPORT_MAX_BYTES))
        if name == "lastfailed":
            if not isinstance(payload, dict) or any(value is not True for value in payload.values()):
                return ()
            payload = list(payload)
        if not isinstance(payload, list) or not all(isinstance(node, str) for node in payload):
            return ()
        return tuple(payload)
    except (OSError, ValueError):
        return ()


def _pytest_rerun_command(command: str, node_ids: tuple[str, ...]) -> str | None:
    """Rewrite only simple pytest invocations, preserving their launcher/extras.

    Unknown options and shell programs are not safe to reinterpret as selectors.
    Refusing them leaves the original gate failure intact.
    """
    if not node_ids or any(not node or "::" not in node or node.startswith("-") or "\x00" in node for node in node_ids):
        return None
    if any(char in command for char in "$`\n\r"):
        return None
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>()")
        lexer.whitespace_split = True
        tokens = list(lexer)
        if any(token in {";", "&&", "||", "|", "&", "<", ">", "(", ")"} for token in tokens):
            return None
        args = shlex.split(command)
        index = next(i for i, token in enumerate(args) if Path(token).name in {"pytest", "py.test"})
    except (ValueError, StopIteration):
        return None
    prefix = args[:index + 1]
    launcher_args = args[:index]
    if launcher_args and Path(launcher_args[0]).name == "uv":
        if launcher_args[1:2] != ["run"]:
            return None
        launcher_args = launcher_args[2:]
        while launcher_args and launcher_args[0].startswith("-"):
            option = launcher_args.pop(0)
            if option in {"--extra", "--group", "--python"} and launcher_args:
                launcher_args.pop(0)
            elif option not in {"--no-sync", "--locked", "--frozen", "--all-extras"}:
                return None
    if launcher_args and not (
        len(launcher_args) == 2
        and Path(launcher_args[0]).name in {"python", "python3", "python3.11", "python3.12", "python3.13", "python3.14"}
        and launcher_args[1] == "-m"
    ):
        return None
    # Drop original paths, selection and parallelism; preserve execution options
    # only when their argument shape is known. Never carry --lf/--ff or -x.
    retained: list[str] = []
    index += 1
    while index < len(args):
        token = args[index]
        if token in {"-n", "--numprocesses", "--dist", "-k", "-m", "--maxfail"}:
            index += 2
            continue
        if token.startswith(("--numprocesses=", "--dist=", "--maxfail=", "-n=")) or (token.startswith("-n") and token[2:].isdigit()):
            index += 1
            continue
        if token in {"-q", "-v", "-vv", "-s", "--disable-warnings", "--strict-markers", "--strict-config"}:
            retained.append(token)
        elif token in {"-x", "--exitfirst", "--lf", "--last-failed", "--ff", "--failed-first"}:
            pass
        elif token.startswith("-"):
            return None
        index += 1
    return shlex.join([*prefix, *retained, "-n", "0", "-k", "", "-m", "", "--", *node_ids])


def pytest_report_environment(
    command: str,
    report_dir: Path,
    *,
    existing: str | None = None,
    create_directory: bool = True,
    basetemp: Path | None = None,
) -> dict[str, str]:
    """Configure gate scratch and pytest reports without moving retained output."""
    configured = _configured_gate_tmp_root() is not None
    if not _is_pytest_command(command) and not configured:
        return {}
    scratch = basetemp if basetemp is not None else _gate_tmp_directory(report_dir)
    environment: dict[str, str] = {}
    if configured:
        environment.update(TMPDIR=str(scratch), WORKLINK_GATE_SCRATCH=str(scratch))
    if not _is_pytest_command(command):
        return environment
    pytest_tmp = scratch / "pytest" if configured else scratch
    if create_directory:
        report_dir.mkdir(parents=True, exist_ok=True)
    options = (
        f"--junitxml={shlex.quote(str(report_dir / 'junit.xml'))} "
        f"--basetemp={shlex.quote(str(pytest_tmp))} "
        "-o tmp_path_retention_policy=all "
        f"-o cache_dir={shlex.quote(str(report_dir / 'cache'))}"
    )
    # Keep tempfile scratch outside pytest's destructive basetemp reset. Both
    # remain in the same per-run tree for bounded retention and cleanup.
    environment["PYTEST_ADDOPTS"] = " ".join(part for part in (existing, options) if part)
    return environment


def read_pytest_result(command: str, report_dir: Path) -> TestResult | None:
    """Read counts and exact failed node IDs from pytest-owned machine files."""
    if not _is_pytest_command(command):
        return None
    junit_path = report_dir / "junit.xml"
    lastfailed_path = report_dir / "cache" / "v" / "cache" / "lastfailed"
    report_error = None
    try:
        try:
            document, truncated = _gate_read_prefix(junit_path, _PYTEST_REPORT_MAX_BYTES)
        except ValueError:
            return TestResult(command, report_error="junit_oversize")
        if truncated:
            report_error = "junit_oversize"
            # Suite counts and the first setup error precede the repeated
            # tracebacks. Recover diagnostics without trusting a partial report
            # as a passing gate or reading the entire oversized file.
            parser = ET.XMLPullParser(events=("start",))
            parser.feed(document)
            elements = [element for _, element in parser.read_events()]
            if not elements:
                return TestResult(command, report_error=report_error)
            root = elements[0]
        else:
            root = ET.fromstring(document)
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        total = sum(_xml_count(suite, "tests") for suite in suites)
        failed = sum(_xml_count(suite, "failures") for suite in suites)
        errors = sum(_xml_count(suite, "errors") for suite in suites)
        skipped = sum(_xml_count(suite, "skipped") for suite in suites)
    except FileNotFoundError:
        return TestResult(command, report_error="junit_missing")
    except ET.ParseError:
        return TestResult(command, report_error=report_error or "junit_parse_error")
    except ValueError:
        return TestResult(command, report_error="junit_invalid_counts")
    except OSError:
        return TestResult(command, report_error="junit_read_error")

    failed_tests: tuple[str, ...] = ()
    try:
        payload = json.loads(_gate_read(lastfailed_path, _PYTEST_REPORT_MAX_BYTES))
        if isinstance(payload, dict):
            failed_tests = tuple(
                redact_text(node_id)[:1000]
                for node_id, is_failed in payload.items()
                if isinstance(node_id, str) and is_failed is True
            )
    except (OSError, ValueError):
        pass

    counts = TestCounts(
        total=total,
        passed=max(0, total - failed - errors - skipped),
        failed=failed,
        errors=errors,
        skipped=skipped,
    )
    return TestResult(
        command,
        exit_code=0 if failed == 0 and errors == 0 else 1,
        counts=counts,
        failed_tests=failed_tests,
        report_error=report_error,
        environment_failure=_setup_environment_failure(root, counts),
    )


def _setup_environment_failure(root: ET.Element, counts: TestCounts) -> str | None:
    """A session-wide setup fault is an environment failure, not an assertion."""
    errors = list(root.iter("error"))
    setup = [
        error.attrib.get("message", "")
        for error in errors
        if error.attrib.get("message", "").startswith("failed on setup with ")
    ]
    # For oversized reports, the bounded prefix is a sample of error phases;
    # the suite header still supplies whole-run counts. Ordinary assertion or
    # teardown failures must not be relabelled as environment failures.
    if (counts.total > 0 and counts.errors * 100 >= counts.total * 95
            and setup and len(setup) * 100 >= len(errors) * 95):
        first = setup[0].removeprefix("failed on setup with ").strip('"')
        return terminal_error(f"test gate environment failure: {first}")
    return None


def _xml_count(suite: ET.Element, name: str) -> int:
    return int(suite.attrib.get(name, "0"))


def _is_pytest_command(command: str) -> bool:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return False
    return any(Path(token).name in {"pytest", "py.test"} for token in tokens)


def _command_with_pytest_report(command: str, report_dir: Path) -> str:
    environment = pytest_report_environment(command, report_dir, existing=os.environ.get("PYTEST_ADDOPTS"))
    if not environment:
        return command
    if "TMPDIR" in environment:
        assignments = " ".join(f"{key}={shlex.quote(value)}" for key, value in environment.items())
        return f"export {assignments}; mkdir -p -m 700 -- {shlex.quote(environment['TMPDIR'])} && {command}"
    return f"PYTEST_ADDOPTS={shlex.quote(environment['PYTEST_ADDOPTS'])} {command}"


def _gate_failure_reason(tests: TestResult) -> str:
    if tests.environment_failure:
        return tests.environment_failure
    counts = tests.counts
    if counts is None:
        detail = terminal_error(tests.summary) if tests.summary else None
        return terminal_error(
            f"test gate failed (exit {tests.exit_code}); structured counts unavailable"
            + (f" ({tests.report_error})" if tests.report_error else "")
            + (f"; output: {detail}" if detail else "")
        )
    count_text = (
        f"{counts.failed} failed, {counts.errors} errors, {counts.passed} passed, "
        f"{counts.skipped} skipped, {counts.total} total"
    )
    failures = ", ".join(tests.failed_tests) or "no failing node IDs reported"
    return terminal_error(f"test gate failed; counts: {count_text}; failures: {failures}")


def _gate_results_diverge(
    executor: TestResult | None,
    measured: TestResult | None,
) -> bool | None:
    if executor is None or measured is None:
        return None
    return (
        executor.exit_code,
        executor.counts,
        executor.failed_tests,
        executor.report_error,
        executor.timed_out,
    ) != (
        measured.exit_code,
        measured.counts,
        measured.failed_tests,
        measured.report_error,
        measured.timed_out,
    )


@contextlib.contextmanager
def shell_gate_environment() -> Iterator[dict[str, str]]:
    """Provision a private home owned by the uid executing a local shell gate.

    Unlike contained workers, these runners have no executor-provisioned UUID
    home. Keep their caches/config outside the checkout and never fall back to
    the controller's home. This bounds environment inheritance, not filesystem
    access: the local gate still executes repository code as the current uid.
    """
    with tempfile.TemporaryDirectory(prefix="worklink-gate-home-") as text:
        home = Path(text)
        uid = os.getuid()
        try:
            username = pwd.getpwuid(uid).pw_name
        except KeyError:
            username = f"uid-{uid}"
        paths = {
            "XDG_CONFIG_HOME": home / ".config",
            "XDG_DATA_HOME": home / ".local" / "share",
            "XDG_CACHE_HOME": home / ".cache",
        }
        for path in paths.values():
            path.mkdir(parents=True, mode=0o700)
        yield {
            "USER": username,
            "LOGNAME": username,
            "SHELL": "/bin/sh",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": str(home),
            **{name: str(path) for name, path in paths.items()},
        }


def _run(
    args: Sequence[str] | str, *, cwd: Path | None = None, timeout: float = 1800,
    text: bool = True,
) -> subprocess.CompletedProcess[str]:
    env = filter_otlp_env(os.environ)
    env.pop("MIMIR_MODEL_SPEC", None)
    if isinstance(args, str):
        # Shell syntax supports configured commands and the report env prefix.
        # Configuration must be trusted; checkout code still gets a bounded env.
        with shell_gate_environment() as gate_env:
            with subprocess.Popen(
                args, shell=True, cwd=cwd, env=gate_env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text,
                start_new_session=True,
            ) as process:
                try:
                    stdout, stderr = process.communicate(timeout=timeout)
                except subprocess.TimeoutExpired:
                    # Kill the whole gate, not just the shell that launched it.
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise
                return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
    return subprocess.run(
        list(args), cwd=cwd, env=env, capture_output=True, text=True, check=False
    )


def _merge_paths(*groups: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for path in group:
            if path and path not in seen:
                seen.add(path)
                merged.append(path)
    return merged


def _paths_from_status(output: str) -> list[str]:
    paths: list[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        path = line[3:] if len(line) > 3 else ""
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[1]
        if path:
            paths.append(path.strip())
    return paths


def _summarize(result: subprocess.CompletedProcess[str]) -> str:
    text = (result.stdout or result.stderr or "").strip()
    if len(text) > 500:
        return text[:497] + "..."
    return text


_TEST_OUTPUT_TAIL_LINES = 60
_TEST_OUTPUT_TAIL_CHARS = 6000


def _summarize_test_output(result: subprocess.CompletedProcess[str]) -> str:
    """Tail-based summary for the gate test run (chainlink #815). Test runners
    print the failure list LAST — a head-truncated summary loses exactly the
    detail a retry needs to act on."""
    parts = [part for part in (result.stdout, result.stderr) if part and part.strip()]
    text = "\n".join(part.strip() for part in parts)
    if not text:
        return ""
    clipped = "\n".join(text.splitlines()[-_TEST_OUTPUT_TAIL_LINES:])
    if len(clipped) > _TEST_OUTPUT_TAIL_CHARS:
        clipped = clipped[-_TEST_OUTPUT_TAIL_CHARS:]
    return clipped
