"""Preserve unpublished Worklink attempts as reference for the next attempt (#2114).

An attempt that ends blocked, failed or interrupted without a published PR is
kept as ``refs/worklink/<issue>/<attempt>`` in the operator repository plus a
bounded patch under worklink state. The next attempt always starts fresh from
the current base; it only *reads* the most recent preserved attempt, the reason
it stopped, and how the spec changed since, as untrusted reference.

Preserved material confers no authority: refs are never pushed to the forge,
never seed a checkout, and prior test results never count as verification.

The checkout is worker-controlled, so it is only ever read through
``ControllerGitPublication`` (controller-owned Git dir and index, hooks off,
controller config). Base, file and patch computation run in the trusted
operator repository after the snapshot commit reaches it.
"""

from __future__ import annotations

import difflib
import fcntl
import hashlib
import json
import os
import subprocess
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mimir._atomic import atomic_write_json
from mimir.redaction import redact_text

from .safe_git import ControllerGitPublication, SafeGitError

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]

MAX_SPEC_TEXT = 65_536
MAX_PATCH_BYTES = 1_048_576
MAX_PROMPT_DIFF = 60_000
MAX_PROMPT_SPEC_DIFF = 20_000
MAX_FILES = 200
MAX_REASON = 2_000
GIT_TIMEOUT_S = 120
MAX_PRESERVATION_ATTEMPTS = 3
RETRY_BACKOFF_S = 30 * 60
_IDENTITY = ("-c", "user.name=Worklink", "-c", "user.email=worklink@localhost")


@dataclass(frozen=True)
class PreservedAttempt:
    issue: int
    attempt: int
    ref: str | None
    base_sha: str | None
    head_sha: str | None
    files: list[str] = field(default_factory=list)
    gate: str | None = None
    reason: str | None = None
    spec_sha256: str | None = None
    created_at: str = ""
    patch_truncated: bool = False
    error: str | None = None
    status: str = "preserved"
    failures: int = 0
    retry_after: float | None = None


def _positive(value: object, name: str) -> int:
    # Refs and paths are built from these: integers only, never issue text.
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def preserved_ref(issue_id: int, attempt: int) -> str:
    return f"refs/worklink/{_positive(issue_id, 'issue')}/{_positive(attempt, 'attempt')}"


def _state_dir(home: Path, issue_id: int) -> Path:
    return home / "state" / "worklink" / "preserved" / str(_positive(issue_id, "issue"))


def _lifecycle_dir(home: Path, issue_id: int) -> Path:
    # Controller-owned tombstones outlive retention of refs, records and patches.
    return home / "state" / "worklink" / "preservation-state" / str(_positive(issue_id, "issue"))


@contextmanager
def _preservation_lock(home: Path, issue_id: int) -> Iterator[None]:
    directory = _lifecycle_dir(home, issue_id)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _load_lifecycle(home: Path, issue_id: int, attempt: int) -> dict[str, Any]:
    for directory in (_lifecycle_dir(home, issue_id), _state_dir(home, issue_id)):
        try:
            data = json.loads((directory / f"{attempt}.json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        # An unreadable marker must not authorize another snapshot.
        if not isinstance(data, dict):
            raise ValueError("invalid preservation state")
        return data
    return {}


def _unrecoverable(exc: Exception) -> bool:
    if isinstance(exc, (FileNotFoundError, NotADirectoryError)):
        return True
    if isinstance(exc, subprocess.TimeoutExpired):
        return False
    message = str(exc).lower()
    # SafeGit rejects invalid/replaced checkout metadata. Lock contention
    # during private-index initialization, however, can heal on retry.
    if isinstance(exc, SafeGitError):
        return not any(text in message for text in ("index.lock", "cannot lock", "unable to lock"))
    return any(text in message for text in (
        "failed to unpack tree", "unable to read tree", "bad tree object",
        "bad object", "invalid object", "missing blob", "missing tree",
        "missing commit", "object file", "corrupt", "not a git repository",
    ))


def spec_text(title: str, description: str) -> str:
    return f"{title}\n\n{description}"


def _bounded(text: str, limit: int) -> tuple[str, bool]:
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return text, False
    return data[:limit].decode("utf-8", "ignore"), True


def record_dispatch_spec(home: Path, issue_id: int, attempt: int, title: str, description: str) -> dict[str, Any]:
    """Record the spec an attempt was dispatched with (sha256 + bounded text)."""
    text = spec_text(title, description)
    bounded, truncated = _bounded(text, MAX_SPEC_TEXT)
    payload = {
        "sha256": hashlib.sha256(text.encode("utf-8", "replace")).hexdigest(),
        "text": bounded,
        "truncated": truncated,
    }
    path = home / "state" / "worklink" / "dispatch-spec" / (
        f"{_positive(issue_id, 'issue')}-{_positive(attempt, 'attempt')}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, payload)
    return payload


def load_dispatch_spec(home: Path, issue_id: int, attempt: int) -> dict[str, Any] | None:
    path = home / "state" / "worklink" / "dispatch-spec" / f"{issue_id}-{attempt}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("sha256"), str) or not isinstance(data.get("text"), str):
        return None
    return data


def _open_checkout(checkout: Path) -> int:
    return os.open(
        checkout, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )


@contextmanager
def _checkout_git(
    checkout: Path, trusted_repo: Path, branch: str, metadata_root: Path,
    publication: ControllerGitPublication | None,
) -> Iterator[ControllerGitPublication]:
    if publication is not None:
        yield publication
        return
    fd = _open_checkout(checkout)
    try:
        captured = ControllerGitPublication.capture(fd, trusted_repo, branch, metadata_root)
    finally:
        os.close(fd)
    with captured:
        yield captured


def _git_ok(result: subprocess.CompletedProcess[Any], what: str) -> str:
    if result.returncode != 0:
        raise RuntimeError(f"{what} failed: {(result.stderr or result.stdout or '').strip()[:500]}")
    return (result.stdout or "").strip()


def _resolve_base(trusted_repo: Path, head: str, candidates: Sequence[str], runner: Runner) -> str | None:
    for candidate in candidates:
        if not candidate or candidate.startswith("-"):
            continue
        result = runner(["git", "-C", str(trusted_repo), "merge-base", head, candidate])
        if result.returncode == 0 and (result.stdout or "").strip():
            return result.stdout.strip()
    return None


def _gate_summary(home: Path, issue_id: int, attempt: int) -> str | None:
    path = home / "state" / "worklink" / "evidence" / f"{issue_id}-{attempt}.json"
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    tests = evidence.get("tests") if isinstance(evidence, dict) else None
    if not isinstance(tests, dict):
        return f"status={evidence.get('status')} (no gate result)" if isinstance(evidence, dict) else None
    failed = tests.get("failed_tests") or []
    summary = (f"status={evidence.get('status')} exit_code={tests.get('exit_code')} "
               f"passed={tests.get('passed')} failed={tests.get('failed')}")
    if failed:
        summary += " failing=" + ", ".join(str(node) for node in failed[:20])
    if tests.get("skipped_reason"):
        summary += f" skipped_reason={tests['skipped_reason']}"
    return redact_text(summary)[:MAX_REASON]


def preserve_attempt(
    *,
    home: Path,
    trusted_repo: Path,
    checkout: Path,
    branch: str,
    issue_id: int,
    attempt: int,
    base_candidates: Sequence[str],
    reason: str | None,
    runner: Runner,
    publication: ControllerGitPublication | None = None,
) -> PreservedAttempt | None:
    """Keep an unpublished attempt's work; return only durable state changes.

    All callers (factory, dispatch and periodic pruning) share the same durable
    terminal markers and bounded retry budget. Reporting happens only after a
    state transition is persisted, never once per retry/cycle.
    """
    preserved_ref(issue_id, attempt)
    try:
        with _preservation_lock(home, issue_id):
            return _preserve_attempt(
                home=home, trusted_repo=trusted_repo, checkout=checkout, branch=branch,
                issue_id=issue_id, attempt=attempt, base_candidates=base_candidates,
                reason=reason, runner=runner, publication=publication,
            )
    except Exception:  # noqa: BLE001 - no durable transition, so nothing to report
        return None


def _preserve_attempt(
    *, home: Path, trusted_repo: Path, checkout: Path, branch: str,
    issue_id: int, attempt: int, base_candidates: Sequence[str], reason: str | None,
    runner: Runner, publication: ControllerGitPublication | None,
) -> PreservedAttempt | None:
    previous = _load_lifecycle(home, issue_id, attempt)
    # Legacy successful records are terminal too; legacy errors get one bounded
    # migration into the new failure state machine.
    previous_status = previous.get("status") or ("preserved" if previous and not previous.get("error") else None)
    if previous_status in ("preserved", "unpreservable", "pruned", "empty"):
        return None
    if previous.get("retry_after", 0) and time.time() < previous["retry_after"]:
        return None
    ref = preserved_ref(issue_id, attempt)
    created = datetime.now(UTC).isoformat()
    spec = load_dispatch_spec(home, issue_id, attempt)
    gate = _gate_summary(home, issue_id, attempt)
    bounded_reason = redact_text(reason)[:MAX_REASON] if reason else None
    base = head = None
    try:
        with _checkout_git(checkout, trusted_repo, branch,
                           home / "state" / "worklink" / "publication", publication) as git:
            # Snapshot tracked and untracked work in the controller-owned index;
            # the worker's own index and config never take part.
            _git_ok(git.run("read-tree", "HEAD", timeout=GIT_TIMEOUT_S), "read-tree")
            _git_ok(git.run("add", "-A", timeout=GIT_TIMEOUT_S), "add")
            tree = _git_ok(git.run("write-tree", timeout=GIT_TIMEOUT_S), "write-tree")
            parent = _git_ok(git.run("rev-parse", "--verify", "HEAD^{commit}", timeout=GIT_TIMEOUT_S), "rev-parse")
            head = _git_ok(git.run(
                *_IDENTITY, "commit-tree", tree, "-p", parent,
                "-m", f"worklink: preserved attempt {issue_id}-{attempt}", timeout=GIT_TIMEOUT_S,
            ), "commit-tree")
            # Only ever to the local operator repository, never a forge remote.
            _git_ok(git.run("push", "--no-verify", str(trusted_repo), f"+{head}:{ref}",
                            timeout=GIT_TIMEOUT_S), "push to operator repository")
        base = _resolve_base(trusted_repo, head, base_candidates, runner)
        floor = base or f"{head}^"
        names = _git_ok(runner(["git", "-C", str(trusted_repo), "diff", "--name-only", floor, head]),
                        "diff --name-only")
        files = [name for name in names.splitlines() if name]
        if not files:
            runner(["git", "-C", str(trusted_repo), "update-ref", "-d", ref])
            atomic_write_json(_lifecycle_dir(home, issue_id) / f"{attempt}.json", {
                "issue": issue_id, "attempt": attempt, "status": "empty",
            })
            return None
        patch = runner(["git", "-C", str(trusted_repo), "diff", "--binary", "--no-color",
                        "--no-ext-diff", "--no-textconv", floor, head])
        _git_ok(patch, "diff")
        text, truncated = _bounded(patch.stdout or "", MAX_PATCH_BYTES)
        record = PreservedAttempt(
            issue=issue_id, attempt=attempt, ref=ref, base_sha=base, head_sha=head,
            files=files[:MAX_FILES], gate=gate, reason=bounded_reason,
            spec_sha256=spec["sha256"] if spec else None, created_at=created,
            patch_truncated=truncated,
        )
        state = _state_dir(home, issue_id)
        state.mkdir(parents=True, exist_ok=True)
        (state / f"{attempt}.patch").write_text(text, encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - preservation never blocks the transition
        failures = int(previous.get("failures", 0)) + 1
        terminal = _unrecoverable(exc) or failures >= MAX_PRESERVATION_ATTEMPTS
        record = PreservedAttempt(
            issue=issue_id, attempt=attempt, ref=None, base_sha=base, head_sha=head,
            gate=gate, reason=bounded_reason, spec_sha256=spec["sha256"] if spec else None,
            created_at=created, error=redact_text(f"{type(exc).__name__}: {exc}")[:MAX_REASON],
            status="unpreservable" if terminal else "retry_pending", failures=failures,
            retry_after=None if terminal else time.time() + RETRY_BACKOFF_S * 2 ** (failures - 1),
        )
    state = _state_dir(home, issue_id)
    state.mkdir(parents=True, exist_ok=True)
    payload = asdict(record)
    if spec is not None:
        payload["spec_text"] = spec["text"]
    atomic_write_json(_lifecycle_dir(home, issue_id) / f"{attempt}.json", payload)
    atomic_write_json(state / f"{attempt}.json", payload)
    return record if record.status != previous_status else None


def attempt_numbers(name: str) -> tuple[int, int] | None:
    left, sep, right = name.partition("-")
    if not (sep and left.isdigit() and right.isdigit()) or int(left) < 1 or int(right) < 1:
        return None
    return int(left), int(right)


def _factory_sandbox_of(attempt_dir: Path, sandbox: Any) -> bool:
    """Whether ``sandbox`` is this attempt's factory sandbox, in either layout.

    A worker checkout nests the factory's sandboxes in ``<attempt>/checkout``;
    a controller checkout is the attempt directory itself. The implementation
    lives in the sandbox, so matching neither would snapshot the intake
    checkout and lose the factory's work.
    """
    try:
        relative = Path(sandbox).resolve().relative_to(attempt_dir.resolve())
    except (TypeError, ValueError, OSError):
        return False
    return relative.parts[:-1] in ((".factory-sandboxes",), ("checkout", ".factory-sandboxes"))


def preserve_retained_attempt(
    *,
    home: Path,
    trusted_repo: Path,
    attempt_dir: Path,
    factory_records: Sequence[Any],
    default_base: str,
    runner: Runner,
    reason_if_unknown: str,
) -> PreservedAttempt | None:
    """Preserve a retained directory, respecting terminal state and retry budget.

    ``None`` means no reportable state change (including a deferred retry).
    Missing checkouts are recorded as terminal failures, not silently skipped.
    """
    numbers = attempt_numbers(attempt_dir.name)
    if numbers is None:
        return None
    issue_id, attempt = numbers
    try:
        evidence = json.loads((home / "state" / "worklink" / "evidence"
                               / f"{issue_id}-{attempt}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        evidence = {}
    if not isinstance(evidence, dict):
        evidence = {}
    if evidence.get("pr_url"):
        return None  # Published attempts keep today's behaviour.
    matching = [record for record in factory_records
                if _factory_sandbox_of(attempt_dir, record.sandbox)]
    if matching:
        checkout, branch = Path(matching[0].sandbox), matching[0].branch
    else:
        checkout = attempt_dir / "checkout" if (attempt_dir / "checkout" / ".git").is_dir() else attempt_dir
        branch = f"issue/{issue_id}-a{attempt}"
    base = str(evidence.get("base_ref") or default_base)
    reason = (evidence.get("blocked_reason") or evidence.get("failure_reason")
              or (f"status={evidence['status']}" if evidence.get("status") else None)
              or reason_if_unknown)
    return preserve_attempt(
        home=home, trusted_repo=trusted_repo, checkout=checkout, branch=branch,
        issue_id=issue_id, attempt=attempt, base_candidates=(f"origin/{base}", base),
        reason=str(reason), runner=runner,
    )


def preserved_line(record: PreservedAttempt) -> str:
    fields = {
        "ref": record.ref, "base": record.base_sha, "head": record.head_sha,
        "files": record.files, "gate": record.gate, "reason": record.reason,
        "patch_truncated": record.patch_truncated,
        "status": record.status,
    }
    if record.error is not None:
        fields["error"] = record.error
    return (f"WORKLINK_PRESERVED issue={record.issue} attempt={record.attempt} "
            + " ".join(f"{key}={json.dumps(value)}" for key, value in fields.items()))


def list_preserved(home: Path, issue_id: int) -> list[dict[str, Any]]:
    directory = _state_dir(home, issue_id)
    records = []
    try:
        entries = list(directory.iterdir())
    except OSError:
        return []
    for path in entries:
        if path.suffix != ".json" or not path.stem.isdigit():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if (isinstance(data, dict) and data.get("attempt") == int(path.stem)
                and _load_lifecycle(home, issue_id, int(path.stem)).get("status") != "pruned"):
            records.append(data)
    return sorted(records, key=lambda item: item["attempt"])


def is_preserved(home: Path, issue_id: int, attempt: int) -> bool:
    return (_state_dir(home, issue_id) / f"{attempt}.json").exists()


def prune_preserved(
    home: Path, trusted_repo: Path, issue_id: int, *, keep: int, runner: Runner,
) -> list[int]:
    """Drop retained material, leaving tombstones so it cannot be re-selected."""
    with _preservation_lock(home, issue_id):
        return _prune_preserved(home, trusted_repo, issue_id, keep=keep, runner=runner)


def _prune_preserved(
    home: Path, trusted_repo: Path, issue_id: int, *, keep: int, runner: Runner,
) -> list[int]:
    records = [record for record in list_preserved(home, issue_id)
               if keep == 0 or record.get("status") != "retry_pending"]
    doomed = records[:-keep] if keep > 0 else records
    pruned = []
    for record in doomed:
        attempt = record["attempt"]
        _git_ok(runner(["git", "-C", str(trusted_repo), "update-ref", "-d", preserved_ref(issue_id, attempt)]),
                "prune preserved ref")
        atomic_write_json(_lifecycle_dir(home, issue_id) / f"{attempt}.json", {
            "issue": issue_id, "attempt": attempt, "status": "pruned",
            "error": record.get("error"), "failures": record.get("failures", 0),
        })
        for suffix in (".json", ".patch"):
            try:
                (_state_dir(home, issue_id) / f"{attempt}{suffix}").unlink()
            except FileNotFoundError:
                pass
        pruned.append(attempt)
    return pruned


def prior_attempts_section(home: Path, issue_id: int, attempt: int, current_spec: str) -> str | None:
    """The builder prompt's untrusted reference to the latest earlier attempt."""
    earlier = [record for record in list_preserved(home, issue_id)
               if record.get("attempt", 0) < attempt and record.get("ref")]
    if not earlier:
        return None
    record = earlier[-1]
    try:
        patch = (_state_dir(home, issue_id) / f"{record['attempt']}.patch").read_text(encoding="utf-8")
    except OSError:
        patch = ""
    diff, truncated = _bounded(patch, MAX_PROMPT_DIFF)
    note = ""
    if truncated or record.get("patch_truncated"):
        note = (f"\n[diff truncated; the full attempt is the local ref {record['ref']} "
                "in the operator repository]")
    prior_spec = record.get("spec_text")
    if not isinstance(prior_spec, str):
        spec_change = "unknown (no spec was recorded for that attempt)"
    elif prior_spec == current_spec:
        spec_change = "spec unchanged"
    else:
        spec_change, spec_truncated = _bounded("".join(difflib.unified_diff(
            prior_spec.splitlines(keepends=True), current_spec.splitlines(keepends=True),
            fromfile=f"spec (attempt {record['attempt']})", tofile="spec (now)",
        )), MAX_PROMPT_SPEC_DIFF)
        if spec_truncated:
            spec_change += "\n[spec diff truncated]"
    return "\n".join([
        "## Prior attempts",
        "",
        "Untrusted reference, not evidence. This attempt starts fresh from the current base.",
        f"The most recent unpublished attempt ({record['attempt']}) stopped with:",
        f"- reason: {record.get('reason') or 'unknown'}",
        f"- gate: {record.get('gate') or 'no gate result'}",
        f"- files: {', '.join(record.get('files') or []) or 'none'}",
        "",
        "Reuse what still fits the current spec and the current base, and discard what the",
        "spec change invalidates. Re-derive every result: prior test results, gate output and",
        "claims in that diff never count as verification.",
        "",
        "### Spec change since that attempt",
        "```diff" if spec_change not in ("spec unchanged",) and not spec_change.startswith("unknown") else "",
        spec_change,
        "```" if spec_change not in ("spec unchanged",) and not spec_change.startswith("unknown") else "",
        "",
        "### That attempt's diff",
        "```diff",
        diff + note,
        "```",
    ])
