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
from typing import Any, Awaitable, Callable
import uuid

from .models import AgentEvent, AuthContext, InformationFlowLabels


@dataclass
class RepoTestJob:
    record: dict[str, Any]
    auth: AuthContext | None = None
    labels: InformationFlowLabels = field(default_factory=InformationFlowLabels)
    task: asyncio.Task | None = None

    @property
    def job_id(self) -> str:
        return self.record["job_id"]


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
            record = json.loads(path.read_text())
            self.jobs[record["job_id"]] = RepoTestJob(record)

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

    def enqueue(
        self, *, repository: str, pull_request: int, head_sha: str, suite: str,
        selectors: tuple[str, ...], channel_id: str, starting_turn_id: str | None,
        auth: AuthContext, labels: InformationFlowLabels,
        run: Callable[[RepoTestJob], Awaitable[dict[str, Any]]],
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
        }, auth, labels)
        self.save(job)
        self.jobs[job.job_id] = job
        job.task = asyncio.create_task(self._run(job, run), name=job.job_id)
        return self.queued(job)

    @staticmethod
    def queued(job: RepoTestJob) -> dict[str, Any]:
        return {name: job.record[name] for name in ("status", "job_id", "suite", "head_sha")}

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
                   "starting_turn_id": record["starting_turn_id"]},
            ifc_labels=job.labels, continuation_auth_context=job.auth,
        )
        accepted = await self.on_complete(event)
        if accepted is not False:
            self.save(job, delivered=True)

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


_REGISTRY: RepoTestJobRegistry | None = None


def set_registry(registry: RepoTestJobRegistry | None) -> None:
    global _REGISTRY
    _REGISTRY = registry


def get_registry() -> RepoTestJobRegistry | None:
    return _REGISTRY
