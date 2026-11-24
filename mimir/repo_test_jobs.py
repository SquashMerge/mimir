"""Durable, turn-independent repository test jobs.

Only metadata and already bounded/scrubbed results are persisted. Authority stays
in memory: recovery never resurrects a grant from disk.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
from typing import Any, Awaitable, Callable
import uuid

from .event_logger import log_durable_event_sync
from .models import AgentEvent, AuthContext, InformationFlowLabels

_TERMINAL = frozenset({"completed", "failed", "cancelled", "interrupted"})
_RETENTION_SECONDS = 7 * 24 * 60 * 60
_MAX_DELIVERED_RECORDS = 1000
_REQUIRED_FIELDS = {
    "job_id", "key", "status", "repository", "pull_request", "head_sha", "suite",
    "selectors", "channel_id", "starting_turn_id", "workers", "snapshot", "delivered",
}


@dataclass
class RepoTestJob:
    record: dict[str, Any]
    auth: AuthContext | None = None
    labels: InformationFlowLabels = field(default_factory=InformationFlowLabels)
    task: asyncio.Task | None = None
    grant_binding: Any = None

    @property
    def job_id(self) -> str:
        return self.record["job_id"]


@dataclass(frozen=True)
class RepoTestCompletion:
    """Host-owned result metadata; never reconstructed from persisted authority."""

    binding: Any
    repository: str
    pull_request: int
    head: str
    status: str
    evidence: bool


class RepoTestJobRegistry:
    def __init__(
        self, directory: Path, on_complete: Callable[[AgentEvent], Awaitable[Any]],
    ) -> None:
        self.directory = directory
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        directory.chmod(0o700)
        self.on_complete = on_complete
        self.jobs: dict[str, RepoTestJob] = {}
        for path in directory.glob("rt_*.json"):
            try:
                record = json.loads(path.read_text())
                self._validate_record(record, path)
            except (OSError, UnicodeError, ValueError, TypeError) as exc:
                self._quarantine(path, type(exc).__name__)
                continue
            self.jobs[record["job_id"]] = RepoTestJob(record)
        self.prune()

    @staticmethod
    def _validate_record(record: Any, path: Path) -> None:
        if not isinstance(record, dict) or not _REQUIRED_FIELDS <= record.keys():
            raise ValueError("incomplete repository test record")
        if (
            record["job_id"] != path.stem
            or record["status"] not in _TERMINAL | {"queued"}
            or not isinstance(record["delivered"], bool)
            or not isinstance(record["key"], list)
            or not isinstance(record["workers"], list)
            or not all(isinstance(worker, str) for worker in record["workers"])
            or not isinstance(record["selectors"], list)
            or not all(isinstance(record[name], str) for name in (
                "repository", "head_sha", "suite", "channel_id",
            ))
            or not isinstance(record["pull_request"], int)
            or (record["starting_turn_id"] is not None
                and not isinstance(record["starting_turn_id"], str))
            or (record["snapshot"] is not None and not isinstance(record["snapshot"], dict))
            or (record["status"] in _TERMINAL and not isinstance(record.get("result"), dict))
        ):
            raise ValueError("invalid repository test record")

    def _quarantine(self, path: Path, error_type: str) -> None:
        quarantine = self.directory / "quarantine"
        destination = quarantine / (uuid.uuid4().hex + ".json")
        try:
            quarantine.mkdir(mode=0o700, exist_ok=True)
            quarantine.chmod(0o700)
            path.replace(destination)
            destination.chmod(0o600)
        except OSError:
            # A read-only or disappearing record must not prevent healthy job
            # recovery. Keep a durable warning without rendering file contents.
            log_durable_event_sync("repo_test_record_quarantine_failed", error_type=error_type)
        else:
            log_durable_event_sync("repo_test_record_quarantined", error_type=error_type)

    def prune(self) -> None:
        """Bound delivered terminal history; never discard pending recovery."""
        now = time.time()
        delivered = []
        for job in self.jobs.values():
            if (job.grant_binding is not None or job.record["status"] not in _TERMINAL
                    or not job.record["delivered"]):
                continue
            path = self.directory / f"{job.job_id}.json"
            try:
                # mtime also supports records written before retention existed.
                delivered.append((path.stat().st_mtime, job.job_id, path))
            except OSError:
                continue
        for index, (modified, job_id, path) in enumerate(sorted(delivered, reverse=True)):
            if now - modified <= _RETENTION_SECONDS and index < _MAX_DELIVERED_RECORDS:
                continue
            try:
                path.unlink()
            except OSError:
                log_durable_event_sync("repo_test_record_prune_failed")
            else:
                self.jobs.pop(job_id, None)

    def save(self, job: RepoTestJob, **updates: Any) -> None:
        job.record.update(updates)
        path = self.directory / f"{job.job_id}.json"
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(job.record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)

    def has_pending_checkout(self, lease: Any) -> bool:
        """Protect only an in-process binding until its checkout handoff.

        Delivery means queued to the agent, not resumed. Persisted records have
        no binding and cannot keep a checkout alive after a restart.
        """
        from .access_control import RepoTestGrantBinding

        for job in tuple(self.jobs.values()):
            binding = job.grant_binding
            if (
                isinstance(binding, RepoTestGrantBinding)
                and binding.lease_identity == (str(lease.path), lease.recovery_id)
                and binding.scope.scope_id == lease.scope_id
                and binding.starting_turn_id == lease.last_turn_id
            ):
                return True
        return False

    def enqueue(
        self, *, repository: str, pull_request: int, head_sha: str, suite: str,
        selectors: tuple[str, ...], channel_id: str, starting_turn_id: str | None,
        auth: AuthContext, labels: InformationFlowLabels,
        run: Callable[[RepoTestJob], Awaitable[dict[str, Any]]],
        grant_binding: Any = None,
    ) -> dict[str, Any]:
        key = [repository.casefold(), pull_request, head_sha, suite, list(selectors)]
        # No await between lookup, publication and task creation: atomic on the
        # owning event loop, even for simultaneous tool calls.
        for job in self.jobs.values():
            if job.record["key"] == key and job.record["status"] == "queued":
                return self.queued(job)
        job = RepoTestJob({
            "job_id": "rt_" + uuid.uuid4().hex, "key": key, "status": "queued",
            "repository": repository, "pull_request": pull_request,
            "head_sha": head_sha, "suite": suite, "selectors": list(selectors),
            "channel_id": channel_id, "starting_turn_id": starting_turn_id,
            "workers": [], "snapshot": None, "delivered": False,
        }, auth, labels, grant_binding=grant_binding)
        self.save(job)
        self.jobs[job.job_id] = job
        job.task = asyncio.create_task(self._run(job, run), name=job.job_id)
        return self.queued(job)

    @staticmethod
    def queued(job: RepoTestJob) -> dict[str, Any]:
        return {name: job.record[name] for name in ("status", "job_id", "suite", "head_sha")}

    def pending_for_scope(self, repository: str, pull_request: int) -> RepoTestJob | None:
        """Find an in-process lease reservation, including undelivered results.

        Repository/PR, not scope_id or head, is the exclusion key: a later
        poller event can mint a different scope for the very same checkout.
        Recovered records have no binding and therefore reserve nothing.
        """
        return next((
            job for job in tuple(self.jobs.values())
            if job.grant_binding is not None
            and job.record["repository"].casefold() == repository.casefold()
            and job.record["pull_request"] == pull_request
        ), None)

    def release_binding(self, binding: Any) -> None:
        """Successful takeover retires only the identical host binding."""
        if binding is not None:
            for job in tuple(self.jobs.values()):
                if job.grant_binding is binding:
                    job.grant_binding = None

    def release_completion(self, job_id: str | None) -> None:
        """Turn-end abandonment is keyed by the delivered completion's job ID."""
        job = self.jobs.get(job_id)
        if job is not None:
            job.grant_binding = None

    def release_lease(self, identity: tuple[str, str]) -> None:
        """Retire only the binding belonging to a reclaimed checkout identity."""
        for job in tuple(self.jobs.values()):
            if job.grant_binding is not None and job.grant_binding.lease_identity == identity:
                job.grant_binding = None

    async def _run(self, job: RepoTestJob, run: Callable) -> None:
        try:
            result = await run(job)
            status = "completed"
        except asyncio.CancelledError:
            status = "interrupted" if job.record.get("interrupting") else "cancelled"
            result = {"ok": False, "code": status, "message": f"No result exists: job {status}."}
        except Exception as exc:
            # Exceptions may include raw repository output or controller paths.
            result = {"ok": False, "code": getattr(exc, "code", "project_test_failed"),
                      "message": "No result exists: project test execution was refused."}
            status = "failed"
        self.save(job, status=status, result=result)
        if not job.record.get("interrupting"):
            await self.deliver(job)

    async def deliver(self, job: RepoTestJob) -> None:
        record = job.record
        event = AgentEvent(
            trigger="repo_test_complete", channel_id=record["channel_id"],
            source="system", source_id=f"repo_test:{job.job_id}",
            content=json.dumps({
                "job_id": job.job_id, "status": record["status"],
                "head_sha": record["head_sha"], "suite": record["suite"],
                "snapshot": ({key: value for key, value in record["snapshot"].items()
                              if key != "path"} if record["snapshot"] else None),
                "result": record["result"],
            }),
            extra={"job_id": job.job_id, "status": record["status"],
                   "starting_turn_id": record["starting_turn_id"],
                   "_repo_test_completion": RepoTestCompletion(
                       job.grant_binding, record["repository"], record["pull_request"],
                       record["head_sha"], record["status"],
                       record["result"].get("evidence") is True
                       and not record["result"].get("stale", False),
                   )},
            ifc_labels=job.labels, continuation_auth_context=job.auth,
        )
        accepted = await self.on_complete(event)
        if accepted is not False:
            self.save(job, delivered=True)
            # Enqueue acceptance is not completion-turn takeover. The binding
            # stays reserved until successful resume or explicit abandonment.
            self.prune()

    async def recover(self, cleanup: Callable[[dict], Awaitable[None]]) -> None:
        for job in tuple(self.jobs.values()):
            if job.task is not None:
                continue
            if job.record["status"] == "queued" or not job.record["delivered"]:
                # Confirm retirement before reporting interruption. Cleanup
                # failure leaves the durable record pending for the next boot.
                await cleanup(job.record)
            if job.record["status"] == "queued":
                self.save(job, status="interrupted", result={
                    "ok": False, "code": "interrupted",
                    "message": "No result exists: the repository test worker was interrupted.",
                })
            if not job.record["delivered"]:
                # No persisted credentials, grants, or successful results are
                # replayed under fresh authority after restart.
                job.labels = InformationFlowLabels(labels=frozenset({"internal", "unknown"}))
                self.save(job, result={
                    "ok": False, "code": "interrupted",
                    "message": "No result exists after restart; rerun tests for evidence.",
                })
                await self.deliver(job)

    async def cancel(self, job_id: str) -> None:
        """Trusted operator API, not a model-callable cross-channel tool."""
        job = self.jobs[job_id]
        if job.task is not None and not job.task.done():
            job.task.cancel()
            try:
                await job.task
            except asyncio.CancelledError:
                # Cancellation before the coroutine's first instruction.
                self.save(job, status="cancelled", result={
                    "ok": False, "code": "cancelled", "message": "No result exists: job cancelled.",
                })
                await self.deliver(job)

    async def close(self) -> None:
        """Runtime shutdown interrupts workers, not the originating turn cancel."""
        for job in tuple(self.jobs.values()):
            if job.task is not None and not job.task.done():
                self.save(job, interrupting=True)
                job.task.cancel()
                await asyncio.gather(job.task, return_exceptions=True)
                # Also covers cancellation before the task's first instruction.
                if job.record["status"] == "queued":
                    self.save(job, status="interrupted", result={
                        "ok": False, "code": "interrupted", "message": "No result exists after shutdown.",
                    })
            job.grant_binding = None


_REGISTRY: RepoTestJobRegistry | None = None


def set_registry(registry: RepoTestJobRegistry | None) -> None:
    global _REGISTRY
    _REGISTRY = registry


def get_registry() -> RepoTestJobRegistry | None:
    return _REGISTRY
