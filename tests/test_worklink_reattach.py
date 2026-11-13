"""Tests for Worklink controller resume-after-restart (#561)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import subprocess
import sys
import pytest
from pathlib import Path
from typing import Sequence

pytestmark = pytest.mark.usefixtures("authorized_fake_backend_gate")


@pytest.fixture(autouse=True)
def attempt_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path / ".worklink"))

pytest.importorskip("mimir_cultivate", reason="Worklink reattachment belongs to Cultivate")

from mimir_cultivate.worklink.backends.base import Caps, RawResult, WorkOrder
from mimir_cultivate.worklink.compute import ComputeCaps, ComputeResult, LaunchHandle, WorkSpec
from mimir_cultivate.worklink.backends.feature_factory import parse_factory_status
from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig, WorklinkDefaults
from mimir_cultivate.worklink.factory_state import FactoryRunRecord, save_factory_record
from mimir_cultivate.worklink.orchestrator import (
    WorklinkRunResult,
    WorklinkRunner,
    run_worklink,
    run_worklink_reattach,
)
from mimir_cultivate.worklink.run_state import (
    WorklinkRunState,
    clear_run_state,
    list_run_states,
    load_run_state,
    reattach_dispatch_argv,
    save_run_state,
)


def cp(
    args: Sequence[str] | str,
    returncode: int = 0,
    stdout: str | bytes = "",
    stderr: str | bytes = "",
) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr=stderr)


ISSUE_JSON_TEMPLATE = '''{{
  "id": {id},
  "title": "worklink slice",
  "description": "Acceptance criteria:\\n- [ ] do it\\n- [ ] echo ok\\n\\nReview criteria:\\n- reviewer checks it\\n\\nWorklink notes:\\n- Scope: test fixture\\n- Out of scope: unrelated work\\n- Suggested test command: echo ok",
  "labels": {labels},
  "parent_id": 380,
  "comments": []
}}'''


def _issue_json(issue_id: int, labels: list[str]) -> str:
    return ISSUE_JSON_TEMPLATE.format(
        id=issue_id, labels=str(labels).replace("'", '"')
    )


class FakeRemoteCompute:
    """A persistent (non-shared-filesystem) compute: survives a controller
    disconnect, so the controller would reattach to it.

    After the #832 substrate cleanup local_subprocess is the only built-in
    Worklink compute substrate and it is non-persistent, so this fake no
    longer models a real production path. It is kept to validate the
    WorklinkRunner.reattach() plumbing (handle/claim/wait transitions) so
    the entry point and the server-side reattach_inflight_worklink_runs
    stay testable should a future persistent compute be added."""

    def __init__(
        self,
        *,
        persistent: bool = True,
        wait_result: ComputeResult | None = None,
        on_wait=None,
    ) -> None:
        self.name = "fake_remote"
        self.persistent = persistent
        self.wait_result = wait_result or ComputeResult(exit_code=0, stdout="ok", stderr="")
        self.on_wait = on_wait
        self.launched: list[tuple[WorkSpec, LaunchHandle]] = []
        self.waited: list[LaunchHandle] = []
        self.cleaned: list[LaunchHandle] = []
        self._n = 0

    def capabilities(self) -> ComputeCaps:
        return ComputeCaps(
            shared_filesystem=False,
            network_isolated=True,
            handle_cancel=True,
            persistent_after_disconnect=self.persistent,
        )

    async def launch(self, spec: WorkSpec) -> LaunchHandle:
        self._n += 1
        handle = LaunchHandle(self.name, f"job-{self._n}")
        self.launched.append((spec, handle))
        return handle

    async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
        self.waited.append(handle)
        if self.on_wait is not None:
            self.on_wait()
        return self.wait_result

    async def logs(self, handle: LaunchHandle) -> str:
        return ""

    async def cancel(self, handle: LaunchHandle) -> None:
        return None

    async def cleanup(self, handle: LaunchHandle) -> None:
        self.cleaned.append(handle)


class FakeLocalCompute(FakeRemoteCompute):
    """Shared-filesystem, non-persistent (local_subprocess-shaped): nothing to
    reattach to, so its runs are never persisted."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.name = "fake_local"

    def capabilities(self) -> ComputeCaps:
        return ComputeCaps(
            shared_filesystem=True,
            network_isolated=False,
            handle_cancel=True,
            persistent_after_disconnect=False,
        )


class FakeBackend:
    name = "fake"

    def capabilities(self) -> Caps:
        return Caps("fake", False, False, False, True, None)

    def work_spec(
        self,
        order: WorkOrder,
        *,
        attempt: int,
        repo_url: str,
        base_ref: str,
        branch: str,
        test_command: str,
    ) -> WorkSpec:
        return WorkSpec(
            issue_id=order.issue_id,
            attempt=attempt,
            repo_url=repo_url,
            base_ref=base_ref,
            branch=branch,
            prompt=order.prompt,
            rules=order.rules,
            test_command=test_command,
            backend=self.name,
            timeout_s=order.timeout_s,
            env=order.env,
            local_checkout=order.checkout,
        )

    async def interpret(self, order: WorkOrder, result: object) -> RawResult:
        assert isinstance(result, ComputeResult)
        return RawResult(result.exit_code, order.transcript_root / "fake.json", "success", None)


def _remote_runner(repo: Path, calls: list, *, issue_id: int, labels: list[str]):
    """Fake runner: chainlink JSON + all git/gh routed through here (no real git)."""

    checkout = repo.parent / ".worklink" / repo.name / f"{issue_id}-1"

    def runner(
        args: Sequence[str] | str,
        *,
        cwd: Path | None = None,
        text: bool = True,
        timeout: float = 1800,
    ) -> subprocess.CompletedProcess:
        calls.append(args)
        if isinstance(args, str):
            return cp(args)
        if args[:4] == ["git", "clone", "--local", "--quiet"]:
            checkout.mkdir(parents=True, exist_ok=True)
            (checkout / ".git" / "objects" / "info").mkdir(parents=True)
            return cp(args)
        if args[:5] == ["git", "-C", str(repo), "rev-parse", "--verify"]:
            return cp(args, stdout="abc123\n")
        if args in (
            ["git", "-C", str(repo), "remote", "get-url", "--push", "origin"],
            ["git", "-C", str(checkout), "remote", "get-url", "--push", "origin"],
        ):
            return cp(args, stdout="git@github.com:jasoncarreira/mimir.git\n")
        if args == ["git", "-C", str(checkout), "rev-parse", "--show-toplevel"]:
            return cp(args, stdout=f"{checkout}\n")
        if args == ["git", "-C", str(checkout), "rev-parse", "--absolute-git-dir"]:
            return cp(args, stdout=f"{checkout / '.git'}\n")
        if args[:1] == ["chainlink"]:
            if args[1:3] == ["issue", "show"]:
                return cp(args, stdout=_issue_json(issue_id, labels))
            return cp(args)  # locks claim/release, issue label/unlabel/comment
        if args[:1] == ["git"]:
            if "config" in args:
                return cp(args, stdout="git@github.com:jasoncarreira/mimir.git\n")
            if args[2] != str(repo) and "status" in args and "-z" in args:
                return cp(args, stdout="?? changed.txt\0" if not any(
                    isinstance(call, list) and "commit" in call for call in calls
                ) else "")
            if "diff" in args and "--cached" in args and "--quiet" in args:
                # chainlink #832: reattach finalize path now commits locally.
                # exit 1 = "yes there are staged changes" (matches the contract
                # WorklinkRunner's _commit_checkout_changes expects).
                return cp(args, returncode=1)
            if "diff" in args and "--cached" in args and "-z" in args:
                return cp(args, stdout=b"changed.txt\0")
            if "cat-file" in args and "blob" in args:
                return cp(args, stdout=b"clean content\n")
            if "diff" in args and "--name-only" in args:
                return cp(args, stdout="changed.txt\n")
            if "diff" in args and "--stat" in args:
                return cp(args, stdout=" changed.txt | 1 +\n")
            if len(args) >= 4 and args[3] == "add":
                return cp(args)
            if len(args) >= 4 and args[3] == "commit":
                return cp(args, stdout=f"[issue/{issue_id}-a1 abc123] worklink\n")
            if len(args) >= 4 and args[3] == "push":
                return cp(args)
            return cp(args)  # fetch, rev-parse, worktree add/remove, status
        if args[:3] == ["gh", "pr", "create"]:
            return cp(args, stdout="https://github.com/jasoncarreira/mimir/pull/777\n")
        return cp(args)

    return runner


# --------------------------------------------------------------------------
# run_state module
# --------------------------------------------------------------------------


def test_run_state_roundtrip(tmp_path: Path) -> None:
    state = WorklinkRunState(
        issue_id=561,
        attempt=2,
        backend="codex",
        compute_name="fake_persistent",
        handle_substrate="fake_persistent",
        handle_identifier="worklink-abc123",
        branch="issue/561-a2",
        base_ref="main",
        local_base="origin/main",
        repo="/workspace/mimir",
        repo_url="git@github.com:jasoncarreira/mimir.git",
        test_command="echo ok",
        started_at="2026-06-18T16:00:00+00:00",
    )
    path = save_run_state(tmp_path, state)
    assert path.exists()
    assert load_run_state(tmp_path, 561) == state
    assert [s.issue_id for s in list_run_states(tmp_path)] == [561]
    clear_run_state(tmp_path, 561)
    assert load_run_state(tmp_path, 561) is None
    assert list_run_states(tmp_path) == []
    # Clearing a missing state is a no-op, not an error.
    clear_run_state(tmp_path, 561)


async def _cancel_with_trace(controller, daemon, traces, *, timeout_s=None):
    """Observe only this controller's swallowed errors and bounded wire metadata."""
    from mimir_cultivate.worklink.daemon_client import DaemonClientError
    from mimir_cultivate.worklink.daemon_sessions import SessionController

    trace = {"checkpoints": [], "exceptions": [], "result": None}
    request_start = len(daemon.requests)
    code = SessionController.cancel.__code__

    def observe(frame, event, arg):
        if frame.f_code is not code or frame.f_locals.get("self") is not controller:
            return None
        if event == "exception" and isinstance(arg[1], (TimeoutError, DaemonClientError)):
            trace["exceptions"].append(type(arg[1]).__name__)
            del trace["exceptions"][:-16]
        return observe

    def checkpoint(name, stage, error=None):
        record = controller.record
        trace["checkpoints"].append({"name": name, "stage": stage, "error": error,
            "execution": record.execution_state, "cancellation": record.cancellation_state,
            "cursor": record.last_seq})
        del trace["checkpoints"][:-16]

    previous = sys.gettrace()
    with pytest.MonkeyPatch.context() as patch:
        for name in ("_read_log", "_messages", "_inbox"):
            original = getattr(controller, name)

            async def wrapped(*args, _name=name, _original=original, **kwargs):
                checkpoint(_name, "entered")
                try:
                    result = await _original(*args, **kwargs)
                except BaseException as exc:
                    checkpoint(_name, "raised", type(exc).__name__)
                    raise
                checkpoint(_name, "returned")
                return result

            patch.setattr(controller, name, wrapped)
        sys.settrace(observe)
        try:
            trace["result"] = await controller.cancel(**({"timeout_s": timeout_s} if timeout_s is not None else {}))
            return trace["result"]
        finally:
            sys.settrace(previous)
            record = controller.record
            session = daemon.sessions[record.opencode_session_id]
            trace.update(execution=record.execution_state, cancellation=record.cancellation_state,
                         cursor=record.last_seq, active=session.active,
                         pending=[item["id"] for item in session.inbox][:16],
                         events=[(event["durable"]["seq"], event["type"]) for event in session.log[-16:]],
                         requests=[(method, route) for method, route, _, _ in
                                   daemon.requests[max(request_start, len(daemon.requests) - 32):]])
            traces.append(trace)
            del traces[:-8]


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["terminal", "interrupted", "failed"])
async def test_failed_leaf_reattach_retains_until_real_session_quiescence(tmp_path, monkeypatch, recovery):
    from types import SimpleNamespace
    from mimir_cultivate.worklink.backends.base import SessionRequest
    from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
    from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
    from mimir_cultivate.worklink.daemon_sessions import SessionController
    from mimir_cultivate.worklink.worker_client import WorkerClient
    from mimir_cultivate.worklink.claims import ChainlinkClaims
    from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon, SyntheticFault

    repo = tmp_path / "repo"
    repo.mkdir()
    checkout = tmp_path / ".worklink/repo/561-1"
    checkout.mkdir(parents=True)
    calls = []
    labels = ["worklink:in-progress"]
    runner = _remote_runner(repo, calls, issue_id=561, labels=labels)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await SessionController.create(
            client, tmp_path / "controller.json",
            request=SessionRequest("leaf", {"providerID": "openai", "id": "gpt-test"}, "build", ("git *",)),
            issue_id=561, attempt=1, generation="generation", checkout=checkout,
            environment={"PATH": "/usr/bin:/bin"}, timeout_s=30)
        sid = controller.record.opencode_session_id
        traces = []

        class Boundary:
            async def session_observe(self, handle):
                if recovery == "interrupted":
                    raise asyncio.CancelledError("reattach interrupted")
                raise AssertionError("reattach observation failed")

            async def session_cancel(self, handle):
                assert (handle.identifier, handle.launch_generation) == (sid, "generation")
                if not await _cancel_with_trace(controller, daemon, traces):
                    raise RuntimeError("unverified cancellation: " + json.dumps(traces[-1]))

            async def session_release(self, handle):
                await controller.release()

        boundary = Boundary()
        monkeypatch.setattr(WorkerClient, "for_path_checkout", lambda *args, **kwargs: boundary)
        registry = BackendRegistry(WorklinkConfig())
        registry.register(OpenCodeBackend(bash_allowlist=("git *",)))
        registry.register_compute(OpenCodeDaemonComputeBackend(client=boundary))
        state = WorklinkRunState(
            issue_id=561, attempt=1, backend="opencode", compute_name="opencode_daemon",
            handle_substrate="opencode_daemon", handle_identifier=sid, branch="issue/561-a1",
            base_ref="main", local_base="main", repo=str(repo), repo_url="repo", test_command=None,
            started_at="2026-06-18T16:00:00+00:00", checkout=str(checkout),
            opencode_session_id=sid, launch_generation="generation")
        save_run_state(tmp_path, state)
        claims = ChainlinkClaims(chainlink_bin="chainlink", agent_id="test", runner=lambda args: runner(args))
        claims.transition_issue(561, status="failed", review_ready=False, attempt=1, reason="cleanup unverified")
        labels[:] = ["worklink:failed"]
        daemon.faults[("GET", f"/api/experimental/session/{sid}/log")].append(SyntheticFault(status=401))
        result = await WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(561)
        assert result.reason == "session quiescence unverified; recovery retained"
        assert load_run_state(tmp_path, 561) == state
        assert checkout.exists() and controller.path.exists()
        assert ["chainlink", "locks", "release", "561"] not in calls
        assert json.loads(result.evidence_path.read_text())["opencode_session_id"] == sid
        assert traces[0]["exceptions"] == ["DaemonClientError"], traces
        assert traces[0]["result"] is False and traces[0]["active"], traces
        assert traces[0]["cancellation"] == "unverified" and traces[0]["cursor"] == 0, traces

        daemon.finish(sid, outcome="interrupted")
        if recovery != "terminal":
            labels[:] = ["worklink:in-progress"]
            from mimir_cultivate.worklink import orchestrator
            monkeypatch.setattr(orchestrator.ControllerGitPublication, "capture",
                                lambda *args: SimpleNamespace(close=lambda: None))
            if recovery == "interrupted":
                with pytest.raises(asyncio.CancelledError):
                    await WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(561)
                evidence = json.loads(result.evidence_path.read_text())
                assert evidence["status"] == "interrupted"
            else:
                result = await WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(561)
                evidence = json.loads(result.evidence_path.read_text())
                assert result.status == "failed"
            assert evidence["opencode_session_id"] == sid
            assert not controller.path.exists()
            assert traces[-1]["result"] is True and traces[-1]["exceptions"] == [], traces
            assert traces[-1]["execution"] == "interrupted" and not traces[-1]["pending"], traces
            assert traces[-1]["cancellation"] == "verified" and not traces[-1]["active"], traces
            assert sum(route.endswith("/prompt") for _, route, _, _ in daemon.requests) == 1
            return
        result = await WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(561)
        assert result.reason == "retained session cancellation verified"
        assert traces[-1]["result"] is True and traces[-1]["exceptions"] == [], traces
        assert traces[-1]["execution"] == "interrupted" and not traces[-1]["pending"], traces
        assert traces[-1]["cancellation"] == "verified" and not traces[-1]["active"], traces
        assert load_run_state(tmp_path, 561) is None
        assert not controller.path.exists()
        assert ["chainlink", "locks", "release", "561"] in calls
        assert sum(route.endswith("/prompt") for _, route, _, _ in daemon.requests) == 1


def test_list_run_states_skips_unparseable(tmp_path: Path) -> None:
    good = WorklinkRunState(
        issue_id=1, attempt=1, backend="codex", compute_name="fake_persistent",
        handle_substrate="fake_persistent", handle_identifier="j1", branch="issue/1-a1",
        base_ref="main", local_base="main", repo="/r", repo_url="", test_command=None,
        started_at="2026-06-18T16:00:00+00:00",
    )
    save_run_state(tmp_path, good)
    runs = tmp_path / "state" / "worklink" / "runs"
    (runs / "bad.json").write_text("{not json", encoding="utf-8")
    (runs / "incomplete.json").write_text('{"issue_id": 9}', encoding="utf-8")
    assert [s.issue_id for s in list_run_states(tmp_path)] == [1]


def test_reattach_dispatch_argv() -> None:
    argv = reattach_dispatch_argv(["mimir"], Path("/home"), "/repo", 561)
    assert argv == [
        "mimir", "worklink", "run", "561", "--reattach", "--autonomous",
        "--home", "/home", "--repo", "/repo",
    ]


# --------------------------------------------------------------------------
# orchestrator.reattach
# --------------------------------------------------------------------------


def _save_inflight_state(home: Path, repo: Path, *, issue_id: int, job: str) -> None:
    save_run_state(
        home,
        WorklinkRunState(
            issue_id=issue_id,
            attempt=1,
            backend="fake",
            compute_name="fake_remote",
            handle_substrate="fake_remote",
            handle_identifier=job,
            branch=f"issue/{issue_id}-a1",
            base_ref="main",
            local_base="origin/main",
            repo=str(repo),
            repo_url="git@github.com:jasoncarreira/mimir.git",
            test_command="echo ok",
            started_at="2026-06-18T16:00:00+00:00",
        ),
    )


def _write_completed_evidence(
    home: Path,
    *,
    issue_id: int,
    head_sha: str | None = "abc123",
    archived: bool = False,
) -> Path:
    evidence_dir = home / "state" / "worklink" / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".json.closed-unmerged" if archived else ".json"
    path = evidence_dir / f"{issue_id}-1{suffix}"
    path.write_text(
        json.dumps(
            {
                "issue": issue_id,
                "attempt": 1,
                "status": "completed",
                "branch": f"issue/{issue_id}-a1",
                "pr_url": "https://github.com/jasoncarreira/mimir/pull/777",
                "head_sha": head_sha,
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
async def daemon_recovery(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from mimir_cultivate.worklink.backends.base import SessionRequest
    from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
    from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
    from mimir_cultivate.worklink.daemon_sessions import SessionController
    from mimir_cultivate.worklink.worker_client import WorkerClient
    from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon

    repo = tmp_path / "repo"
    repo.mkdir()
    checkout = tmp_path / ".worklink/repo/561-1"
    checkout.mkdir(parents=True)
    path = tmp_path / "controller.json"
    labels, calls, operations, traces = ["worklink:failed"], [], [], []
    runner = _remote_runner(repo, calls, issue_id=561, labels=labels)
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        async def create(text):
            return await SessionController.create(
                client, path, request=SessionRequest("leaf", {"providerID": "openai", "id": "gpt-test"}, text, ("git *",)),
                issue_id=561, attempt=1, generation="generation", checkout=checkout,
                environment={"PATH": "/usr/bin:/bin"}, timeout_s=30)

        class Boundary:
            def recover(self, handle, operation):
                operations.append((operation, handle.opencode_session_id, handle.launch_generation))
                assert handle.identifier == handle.opencode_session_id
                return SessionController.recover_owned(
                    client, path, session_id=handle.opencode_session_id,
                    generation=handle.launch_generation, checkout=checkout)

            async def session_observe(self, handle):
                return await self.recover(handle, "observe").observe()

            async def session_cancel(self, handle):
                controller = self.recover(handle, "cancel")
                if not await _cancel_with_trace(controller, daemon, traces):
                    raise RuntimeError("session cancellation unverified: " + json.dumps(traces[-1]))

            async def session_release(self, handle):
                await self.recover(handle, "release").release()

        boundary = Boundary()
        monkeypatch.setattr(WorkerClient, "for_path_checkout", lambda *args, **kwargs: boundary)
        registry = BackendRegistry(WorklinkConfig())
        registry.register(OpenCodeBackend(bash_allowlist=("git *",)))
        registry.register_compute(OpenCodeDaemonComputeBackend(client=boundary))

        def save(controller):
            sid = controller.record.opencode_session_id
            state = WorklinkRunState(
                issue_id=561, attempt=1, backend="opencode", compute_name="opencode_daemon",
                handle_substrate="opencode_daemon", handle_identifier=sid, branch="issue/561-a1",
                base_ref="main", local_base="main", repo=str(repo), repo_url="repo", test_command=None,
                started_at="2026-06-18T16:00:00+00:00", checkout=str(checkout),
                opencode_session_id=sid, launch_generation="generation")
            save_run_state(tmp_path, state)
            return state

        yield SimpleNamespace(daemon=daemon, client=client, create=create, save=save, boundary=boundary,
                              repo=repo, checkout=checkout, path=path, labels=labels, calls=calls,
                              operations=operations, traces=traces, registry=registry,
                              runner=WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry))


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["deadline", "protocol"])
async def test_cancel_trace_distinguishes_budget_exhaustion_from_protocol_refusal(monkeypatch, daemon_recovery, cause):
    from tests.worklink_daemon_fixtures import SyntheticFault

    fixture = daemon_recovery
    controller = await fixture.create("diagnose cancellation")
    sid = controller.record.opencode_session_id
    traces = []
    original_read = controller._read_log
    fixture.daemon.faults[("GET", f"/api/experimental/session/{sid}/log")].append(SyntheticFault(status=401))
    assert not await _cancel_with_trace(controller, fixture.daemon, traces)
    assert traces[0]["exceptions"] == ["DaemonClientError"], traces
    assert traces[0]["active"] and traces[0]["cancellation"] == "unverified", traces
    fixture.daemon.finish(sid, outcome="interrupted")
    if cause == "deadline":
        entered, proceed = asyncio.Event(), asyncio.Event()

        async def held_checkpoint(*args, **kwargs):
            entered.set()
            await proceed.wait()
            return await original_read(*args, **kwargs)

        monkeypatch.setattr(controller, "_read_log", held_checkpoint)
        # Reproduce the old helper's budget exhaustion with a terminal witness
        # already present. The owned checkpoint, not machine load, holds it.
        assert not await asyncio.wait_for(
            _cancel_with_trace(controller, fixture.daemon, traces, timeout_s=0.5), 3)
        assert entered.is_set()
        assert traces[1]["exceptions"] == ["TimeoutError"], traces
        assert any(item["name"] == "_read_log" and item["error"] == "CancelledError"
                   for item in traces[1]["checkpoints"]), traces
        assert not traces[1]["active"] and not traces[1]["pending"], traces
        assert traces[1]["cancellation"] == "unverified", traces
        monkeypatch.setattr(controller, "_read_log", original_read)
    assert await _cancel_with_trace(controller, fixture.daemon, traces)
    assert traces[-1]["exceptions"] == [] and traces[-1]["execution"] == "interrupted", traces
    assert traces[-1]["cursor"] > traces[0]["cursor"], traces
    assert not traces[-1]["active"] and not traces[-1]["pending"], traces
    await controller.release()
    assert not controller.path.exists()
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == 1
    print(json.dumps({"cause": cause, "cancellation_trace": traces}))


@pytest.mark.asyncio
async def test_completed_initial_evidence_cannot_release_active_repair(tmp_path, daemon_recovery):
    from tests.worklink_daemon_fixtures import SyntheticFault

    fixture = daemon_recovery
    initial = await fixture.create("initial build")
    initial_id = initial.record.opencode_session_id
    fixture.daemon.finish(initial_id, outcome="succeeded")
    await initial.release()
    repair = await fixture.create("repair CI")
    state = fixture.save(repair)
    evidence_path = _write_completed_evidence(tmp_path, issue_id=561)
    evidence = json.loads(evidence_path.read_text())
    evidence["opencode_session_id"] = initial_id
    evidence_path.write_text(json.dumps(evidence))

    fixture.daemon.faults[("GET", f"/api/experimental/session/{state.opencode_session_id}/log")].append(
        SyntheticFault(status=401))
    result = await fixture.runner.reattach(561)
    assert result.status == "failed" and not result.review_ready
    assert result.reason == "session quiescence unverified; recovery retained"
    assert load_run_state(tmp_path, 561) == state
    assert fixture.checkout.exists() and fixture.path.exists()
    assert ["chainlink", "locks", "release", "561"] not in fixture.calls
    assert fixture.operations == [("cancel", state.opencode_session_id, "generation")]
    assert fixture.traces[0]["exceptions"] == ["DaemonClientError"], fixture.traces
    assert fixture.traces[0]["active"] and fixture.traces[0]["result"] is False, fixture.traces
    assert fixture.traces[0]["cancellation"] == "unverified" and fixture.traces[0]["cursor"] == 0, fixture.traces

    fixture.daemon.finish(state.opencode_session_id, outcome="interrupted")
    result = await fixture.runner.reattach(561)
    assert result.reason == "retained session cancellation verified"
    assert load_run_state(tmp_path, 561) is None
    assert ["chainlink", "locks", "release", "561"] in fixture.calls
    assert fixture.traces[-1]["exceptions"] == [] and fixture.traces[-1]["result"] is True, fixture.traces
    assert fixture.traces[-1]["execution"] == "interrupted" and not fixture.traces[-1]["pending"], fixture.traces
    assert fixture.traces[-1]["cancellation"] == "verified" and not fixture.traces[-1]["active"], fixture.traces
    assert all(sid == state.opencode_session_id and generation == "generation"
               for _, sid, generation in fixture.operations)
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence_is_current", [False, True])
async def test_completed_evidence_requires_current_session_and_verified_cleanup(tmp_path, daemon_recovery, evidence_is_current):
    fixture = daemon_recovery
    initial = await fixture.create("initial build")
    initial_id = initial.record.opencode_session_id
    fixture.daemon.finish(initial_id, outcome="succeeded")
    await initial.release()
    repair = await fixture.create("repair CI")
    state = fixture.save(repair)
    if evidence_is_current:
        fixture.daemon.finish(state.opencode_session_id, outcome="succeeded")
    evidence_path = _write_completed_evidence(tmp_path, issue_id=561)
    evidence = json.loads(evidence_path.read_text())
    evidence["opencode_session_id"] = state.opencode_session_id if evidence_is_current else initial_id
    evidence_path.write_text(json.dumps(evidence))

    result = await fixture.runner.reattach(561)
    assert result.status == ("completed" if evidence_is_current else "failed")
    assert load_run_state(tmp_path, 561) is None
    assert ["chainlink", "locks", "release", "561"] in fixture.calls
    assert fixture.operations == [(operation, state.opencode_session_id, "generation")
                                  for operation in ("cancel", "release")]
    assert json.loads(result.evidence_path.read_text())["opencode_session_id"] == state.opencode_session_id
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == 2


@pytest.mark.asyncio
async def test_retained_session_reconciliation_interruption_writes_current_identity(tmp_path, monkeypatch, daemon_recovery):
    fixture = daemon_recovery
    controller = await fixture.create("repair CI")
    state = fixture.save(controller)
    entered = asyncio.Event()
    proceed = asyncio.Event()
    original_cancel = fixture.boundary.session_cancel

    async def paused_cancel(handle):
        assert (handle.identifier, handle.launch_generation) == (state.opencode_session_id, "generation")
        entered.set()
        await proceed.wait()
        await original_cancel(handle)

    monkeypatch.setattr(fixture.boundary, "session_cancel", paused_cancel)
    canary = "opaque-" + "retained-interruption"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", canary)
    task = asyncio.create_task(fixture.runner.reattach(561))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel(canary + "x" * 3000)
    with pytest.raises(asyncio.CancelledError):
        await task
    evidence = json.loads((tmp_path / "state/worklink/evidence/561-1.json").read_text())
    assert evidence["status"] == "interrupted"
    assert evidence["opencode_session_id"] == state.opencode_session_id
    assert canary not in evidence["failure_reason"]
    assert len(evidence["failure_reason"]) <= 2048
    assert load_run_state(tmp_path, 561) == state
    assert fixture.path.exists() and fixture.checkout.exists()
    assert ["chainlink", "locks", "release", "561"] not in fixture.calls

    monkeypatch.setattr(fixture.boundary, "session_cancel", original_cancel)
    result = await fixture.runner.reattach(561)
    assert result.reason == "retained session cancellation verified"
    assert load_run_state(tmp_path, 561) is None
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == 1


@pytest.mark.asyncio
async def test_retirement_witness_is_scoped_to_session_and_generation(daemon_recovery):
    from mimir_cultivate.worklink.daemon_sessions import SessionController

    fixture = daemon_recovery
    initial = await fixture.create("initial")
    initial_id = initial.record.opencode_session_id
    fixture.daemon.finish(initial_id, outcome="succeeded")
    await initial.release()
    repair = await fixture.create("repair")
    repair_id = repair.record.opencode_session_id
    requests_before = len(fixture.daemon.requests)
    retired = SessionController.recover_owned(fixture.client, fixture.path, session_id=initial_id,
                                              generation="generation", checkout=fixture.checkout)
    assert await retired.cancel()
    await retired.release()
    assert len(fixture.daemon.requests) == requests_before
    assert fixture.daemon.sessions[repair_id].active
    assert fixture.path.exists()
    assert SessionController.recover(fixture.client, fixture.path, generation="generation",
                                     checkout=fixture.checkout).record.opencode_session_id == repair_id
    with pytest.raises(ValueError, match="generation mismatch"):
        SessionController.recover_owned(fixture.client, fixture.path, session_id=initial_id,
                                        generation="foreign-generation", checkout=fixture.checkout)


@pytest.mark.parametrize("sid, generation", [(None, "generation"), ("ses_current", None),
                                            ("not-a-session", "generation")])
def test_process_gate_state_rejects_invalid_session_correlation(sid, generation):
    import uuid

    state = WorklinkRunState(
        issue_id=561, attempt=1, backend="opencode", compute_name="local_subprocess",
        handle_substrate="local_subprocess", handle_identifier=str(uuid.uuid4()), branch="issue/561-a1",
        base_ref="main", local_base="main", repo="/repo", repo_url="repo", test_command="pytest",
        started_at="2026-06-18T16:00:00+00:00", process_start_ticks=1, shim_pid=12345,
        opencode_session_id=sid, launch_generation=generation)
    with pytest.raises(ValueError, match="gate session identity"):
        WorklinkRunState.from_json(state.to_json())


@pytest.mark.asyncio
@pytest.mark.parametrize("request_stop", [False, True], ids=["startup", "stop-startup"])
@pytest.mark.parametrize("kind", ["gate", "ci_wait"])
@pytest.mark.parametrize("repair", [False, True], ids=["initial", "repair"])
@pytest.mark.parametrize("live", [False, True], ids=["dead", "live"])
async def test_stop_and_startup_dispatch_correlated_terminal_recovery(tmp_path, monkeypatch, daemon_recovery,
                                                                    kind, repair, live, request_stop):
    import uuid
    from types import SimpleNamespace
    from mimir_cultivate import server
    from mimir_cultivate.worklink import control, orchestrator
    from mimir_cultivate.worklink.daemon_sessions import SessionController

    fixture = daemon_recovery
    controller = await fixture.create("initial build")
    fixture.daemon.finish(controller.record.opencode_session_id, outcome="succeeded")
    await controller.release()
    initial_id = controller.record.opencode_session_id
    if repair:
        controller = await fixture.create("repair CI")
        fixture.daemon.finish(controller.record.opencode_session_id, outcome="succeeded")
        await controller.release()
    state = fixture.save(controller)
    sid = state.opencode_session_id
    state = replace(state, compute_name="local_subprocess",
                    handle_substrate="local_subprocess" if kind == "gate" else "controller",
                    handle_identifier=str(uuid.uuid4()) if kind == "gate" else "12345",
                    shim_pid=12345 if kind == "gate" else None, process_start_ticks=42,
                    phase="spawned" if kind == "gate" else "ci_wait")
    save_run_state(tmp_path, state)
    fixture.labels[:] = ["worklink:in-progress"]
    process_live = live
    monkeypatch.setattr(control, "process_is_alive", lambda current: process_live)
    monkeypatch.setattr(control, "process_identity_verified", lambda current: process_live)
    monkeypatch.setattr(orchestrator, "process_is_alive", lambda current: process_live)
    monkeypatch.setenv("WORKLINK_REPO", str(fixture.repo))
    monkeypatch.setenv("WORKLINK_RUN_BIN", "fixture-mimir")
    cancellations = []

    async def cancel_process(self, handle):
        nonlocal process_live
        assert handle.identifier == state.handle_identifier
        assert handle.opencode_session_id is None and handle.launch_generation is None
        assert handle.shim_pid == state.shim_pid
        cancellations.append(handle)
        process_live = False

    monkeypatch.setattr(control.LocalSubprocessComputeBackend, "cancel", cancel_process)
    evidence_path = tmp_path / "state/worklink/evidence/561-1.json"

    def checked_runner(args, **kwargs):
        if isinstance(args, list) and args[:3] == ["chainlink", "locks", "release"]:
            evidence = json.loads(evidence_path.read_text())
            assert evidence["opencode_session_id"] == sid
            assert evidence["status"] == "failed"
            assert load_run_state(tmp_path, 561).opencode_session_id == sid
        return fixture.runner.runner(args, **kwargs)

    if request_stop:
        result = await asyncio.to_thread(control.stop_worklink, tmp_path, 561, runner=checked_runner)
        assert result.stopped and not result.state_cleared and not result.claim_released
    assert load_run_state(tmp_path, 561) == state
    assert control.operator_stop_requested(tmp_path, 561, 1) is request_stop
    assert ["chainlink", "locks", "release", "561"] not in fixture.calls
    assert len(cancellations) == int(request_stop and live and kind == "gate")

    recovery = WorklinkRunner(home=tmp_path, repo=fixture.repo, runner=checked_runner, registry=fixture.registry)
    children, argv_seen, events = [], [], []

    def spawn(argv, **kwargs):
        argv_seen.append(argv)
        assert argv == reattach_dispatch_argv(["fixture-mimir"], tmp_path, str(fixture.repo), 561)
        assert kwargs["start_new_session"] is True
        children.append(asyncio.create_task(recovery.reattach(561)))
        return SimpleNamespace(pid=43210)

    requests_before = len(fixture.daemon.requests)
    dispatched = server.reattach_inflight_worklink_runs(
        tmp_path, popen=spawn, event_logger=lambda event, **fields: events.append((event, fields)))
    assert dispatched == [561], events
    outcome = await children[-1]
    assert json.loads(outcome.evidence_path.read_text())["opencode_session_id"] == sid
    if live and kind == "ci_wait":
        assert "still live" in outcome.reason
        assert load_run_state(tmp_path, 561) == state
        assert ["chainlink", "locks", "release", "561"] not in fixture.calls
        process_live = False
        assert server.reattach_inflight_worklink_runs(tmp_path, popen=spawn) == [561]
        outcome = await children[-1]
    assert outcome.status == "failed"
    assert load_run_state(tmp_path, 561) is None
    assert ["chainlink", "locks", "release", "561"] in fixture.calls
    assert fixture.checkout.exists()
    assert len(cancellations) == int(live and kind == "gate")
    evidence = json.loads(evidence_path.read_text())
    assert evidence["opencode_session_id"] == sid
    assert (sid != initial_id) is repair
    retired = SessionController.recover_owned(fixture.client, fixture.path, session_id=sid,
                                              generation=state.launch_generation, checkout=fixture.checkout)
    assert retired.record.cancellation_state == "verified"
    assert all(current_id == sid and generation == state.launch_generation
               for _, current_id, generation in fixture.operations)
    assert len(fixture.daemon.requests) == requests_before
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == (2 if repair else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["gate", "ci_wait"])
async def test_startup_terminal_recovery_keeps_pointer_until_evidence_is_written(tmp_path, monkeypatch, daemon_recovery, kind):
    import uuid
    from types import SimpleNamespace
    from mimir_cultivate import server
    from mimir_cultivate.worklink import control, orchestrator

    fixture = daemon_recovery
    controller = await fixture.create("initial build")
    fixture.daemon.finish(controller.record.opencode_session_id, outcome="succeeded")
    await controller.release()
    state = replace(fixture.save(controller), compute_name="local_subprocess",
                    handle_substrate="local_subprocess" if kind == "gate" else "controller",
                    handle_identifier=str(uuid.uuid4()) if kind == "gate" else "12345",
                    shim_pid=12345 if kind == "gate" else None, process_start_ticks=42,
                    phase="spawned" if kind == "gate" else "ci_wait")
    save_run_state(tmp_path, state)
    monkeypatch.setattr(control, "process_is_alive", lambda state: False)
    monkeypatch.setattr(orchestrator, "process_is_alive", lambda state: False)
    monkeypatch.setenv("WORKLINK_REPO", str(fixture.repo))
    write_evidence = orchestrator._write_session_failure
    monkeypatch.setattr(orchestrator, "_write_session_failure", lambda *args, **kwargs: None)
    children = []

    def spawn(argv, **kwargs):
        children.append(asyncio.create_task(fixture.runner.reattach(561)))
        return SimpleNamespace(pid=43210)

    assert server.reattach_inflight_worklink_runs(tmp_path, popen=spawn) == [561]
    result = await children[-1]
    assert result.reason == "terminal evidence unavailable; session recovery retained"
    assert load_run_state(tmp_path, 561) == state
    assert ["chainlink", "locks", "release", "561"] not in fixture.calls
    monkeypatch.setattr(orchestrator, "_write_session_failure", write_evidence)
    assert server.reattach_inflight_worklink_runs(tmp_path, popen=spawn) == [561]
    result = await children[-1]
    assert json.loads(result.evidence_path.read_text())["opencode_session_id"] == state.opencode_session_id
    assert load_run_state(tmp_path, 561) is None
    assert ["chainlink", "locks", "release", "561"] in fixture.calls
    assert sum(route.endswith("/prompt") for _, route, _, _ in fixture.daemon.requests) == 1


@pytest.mark.parametrize("release_ok", [True, False])
def test_reattach_completed_evidence_reconciles_without_worker_or_push(
    tmp_path: Path, monkeypatch, release_ok: bool,
) -> None:
    pytest.importorskip("mimir_cultivate")
    from mimir_cultivate.worklink import orchestrator as provider_orchestrator
    from mimir_cultivate.worklink.run_state import load_run_state

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 564
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-stale")
    evidence_path = _write_completed_evidence(tmp_path, issue_id=issue_id)
    calls: list = []
    base_runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])

    def runner(args, *, cwd=None):
        if isinstance(args, list) and args[:3] == ["gh", "pr", "view"]:
            calls.append(args)
            return cp(args, stdout='{"state":"OPEN","headRefOid":"abc123"}')
        if not release_ok and isinstance(args, list) and args[:3] == ["chainlink", "locks", "release"]:
            calls.append(args)
            return cp(args, returncode=1, stderr="release denied")
        return base_runner(args, cwd=cwd)

    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "mimir_cultivate.worklink.orchestrator._log_event",
        lambda event_type, **payload: events.append((event_type, payload)),
    )

    async def unexpected_wait(*args: object, **kwargs: object) -> None:
        pytest.fail("completed evidence must not restart the worker")

    monkeypatch.setattr(provider_orchestrator.LocalSubprocessComputeBackend, "wait", unexpected_wait)

    result = asyncio.run(
        provider_orchestrator.WorklinkRunner(home=tmp_path, repo=repo, runner=runner).reattach(issue_id)
    )

    assert result.status == ("completed" if release_ok else "failed")
    assert result.review_ready is release_ok
    assert result.evidence_path == evidence_path
    assert not any(isinstance(a, list) and "push" in a for a in calls)
    assert (["chainlink", "issue", "label", str(issue_id), "worklink:review"] in calls) is release_ok
    assert ["chainlink", "locks", "release", str(issue_id)] in calls
    assert any(event == "worklink_reattach_reconciled" for event, _ in events) is release_ok
    assert not any(event == "worklink_reattach_branch_mismatch" for event, _ in events)
    assert (load_run_state(tmp_path, issue_id) is None) is release_ok
    if not release_ok:
        assert result.reason == "terminal recovery incomplete: Chainlink lock release failed"


def test_reattach_completed_evidence_remote_head_mismatch_warns_without_push(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 565
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-stale")
    _write_completed_evidence(tmp_path, issue_id=issue_id, head_sha="attempt-final")
    calls: list = []
    base_runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])

    def runner(args, *, cwd=None):
        if isinstance(args, list) and args[:3] == ["gh", "pr", "view"]:
            calls.append(args)
            return cp(args, stdout='{"state":"OPEN","headRefOid":"reviewer-fix"}')
        return base_runner(args, cwd=cwd)

    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "mimir_cultivate.worklink.orchestrator._log_event",
        lambda event_type, **payload: events.append((event_type, payload)),
    )
    compute = FakeRemoteCompute()
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(issue_id)
    )

    assert result.status == "completed"
    assert compute.waited == []
    assert not any(isinstance(a, list) and "push" in a for a in calls)
    warning = next(payload for event, payload in events if event == "worklink_reattach_branch_mismatch")
    assert warning["level"] == "warning"
    assert warning["expected_head"] == "attempt-final"
    assert warning["remote_head"] == "reviewer-fix"
    assert ["chainlink", "locks", "release", str(issue_id)] in calls


def test_reattach_ignores_archived_completed_evidence(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 566
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-live")
    _write_completed_evidence(tmp_path, issue_id=issue_id, archived=True)
    calls: list = []
    runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])
    compute = FakeRemoteCompute(
        wait_result=ComputeResult(
            exit_code=-1,
            stdout="",
            stderr="broker gone",
            launch_error="broker wait failed",
        )
    )
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(issue_id)
    )

    assert result.reason == "reattach: worker lost"
    assert compute.waited == [LaunchHandle("fake_remote", "job-live")]


@pytest.mark.parametrize("clear_fails", [False, True], ids=["clear", "clear-write-failure"])
def test_reattach_waits_on_original_handle_and_resolves_startup_incident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clear_fails: bool,
) -> None:
    """After the #832 substrate cleanup local_subprocess is the only Worklink
    compute substrate and never persists run state, so a *fresh* controller
    will never have something to reattach to. These tests pre-date #832 and
    simulate a persistent remote compute by saving a fake run state by hand
    and registering a fake persistent compute in the registry. They validate
    the WorklinkRunner.reattach() plumbing so the API surface stays testable
    should a future persistent substrate be added; they no longer reflect a
    production-reachable path."""
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 561
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-orig")
    calls: list = []
    runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])
    compute = FakeRemoteCompute(wait_result=ComputeResult(exit_code=0, stdout="ok", stderr=""))

    class ReattachWriteBackend(FakeBackend):
        async def interpret(self, order: WorkOrder, result: object) -> RawResult:
            # Mimic a resumable compute having pushed changes by writing into the
            # reattach worktree. The local-only finalize path then commits +
            # pushes from there.
            order.checkout.mkdir(parents=True, exist_ok=True)
            (order.checkout / "changed.txt").write_text("hello\n", encoding="utf-8")
            return await super().interpret(order, result)

    registry = BackendRegistry(WorklinkConfig())
    registry.register(ReattachWriteBackend())
    registry.register_compute(compute)
    from mimir_cultivate.worklink import orchestrator
    from mimir_cultivate.worklink.dispatch_failures import (
        dispatch_failure_state_dir,
        load_failure_state,
        record_failure,
    )

    state_dir = dispatch_failure_state_dir(tmp_path)
    startup_incident = record_failure(
        state_dir,
        issue_id=issue_id,
        attempt=1,
        exit_status=1,
        error="startup reattach spawn failed",
        log_path="reattach-561.log",
    )
    monkeypatch.setattr(orchestrator, "BackendRegistry", lambda *_: registry)
    monkeypatch.setattr(orchestrator, "_runner_for_home", lambda *_: runner)
    if clear_fails:
        import mimir_cultivate.worklink.dispatch_failures as dispatch_failures

        monkeypatch.setattr(
            dispatch_failures,
            "resolve_failure_if_current",
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError("disk full")),
        )

    if clear_fails:
        with pytest.raises(OSError, match="disk full"):
            run_worklink_reattach(
                home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
            )
        result = None
    else:
        result = run_worklink_reattach(
            home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
        )

    if result is not None:
        assert result.status == "completed", (result.reason, calls)
        assert result.review_ready is True
        assert result.pr_url == "https://github.com/jasoncarreira/mimir/pull/777"
    # It harvested the SURVIVING worker (reconstructed handle), not a new launch.
    assert compute.waited[0] == LaunchHandle("fake_remote", "job-orig")
    # No re-claim / attempt bump on resume.
    assert not any(isinstance(a, list) and a[:3] == ["chainlink", "locks", "claim"] for a in calls)
    # Transitioned to review + released the lock.
    assert ["chainlink", "issue", "label", str(issue_id), "worklink:review"] in calls
    assert ["chainlink", "locks", "release", str(issue_id)] in calls
    # Run state cleared on terminal completion.
    assert load_run_state(tmp_path, issue_id) is None
    assert load_failure_state(state_dir)["issues"][str(issue_id)]["active"] is clear_fails
    if not clear_fails:
        from mimir.models import AgentEvent
        from mimir_cultivate.worklink.poller_hooks import recovery_relevance_check

        delivery_key = (
            f"worklink-run-failure:{issue_id}:{startup_incident['signature']}:"
            f"{startup_incident['occurrence_id']}"
        )
        queued_wake = AgentEvent(
            trigger="poller",
            channel_id="poller:worklink-ready-queue",
            content="queued startup recovery wake",
            source="poller",
            source_id=delivery_key,
            extra={
                "poller_name": "worklink-ready-queue",
                "items": [{
                    "issue_id": issue_id,
                    "error_signature": startup_incident["signature"],
                    "failure_occurrence_id": startup_incident["occurrence_id"],
                    "delivery_key": delivery_key,
                }],
            },
        )
        assert asyncio.run(
            recovery_relevance_check(state_dir)(queued_wake)
        ) is False


def test_reattach_worker_lost_is_durable_before_release_and_blocks_fresh_core(
    tmp_path: Path,
) -> None:
    from mimir_cultivate.worklink.dispatch_failures import dispatch_failure_state_dir, load_failure_state

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 562
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-gone")
    calls: list = []
    base_runner = _remote_runner(
        repo, calls, issue_id=issue_id, labels=["worklink:in-progress"]
    )

    def runner(args: Sequence[str] | str, **kwargs: object) -> subprocess.CompletedProcess:
        if isinstance(args, list) and args[1:3] == ["locks", "release"]:
            incident = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
                str(issue_id)
            ]
            assert incident["active"] is True
            concurrent: list[WorklinkRunResult] = []
            import threading

            thread = threading.Thread(
                target=lambda: concurrent.append(
                    run_worklink(
                        home=tmp_path,
                        repo=repo,
                        issue_id=issue_id,
                        autonomous=True,
                    )
                )
            )
            thread.start()
            thread.join(timeout=5)
            assert not thread.is_alive()
            assert concurrent[0].status == "refused"
            assert concurrent[0].attempt is None
        return base_runner(args, **kwargs)
    # Broker can no longer produce the result -> launch_error set.
    lost = ComputeResult(
        exit_code=-1, stdout="", stderr="broker gone", launch_error="broker wait failed"
    )
    compute = FakeRemoteCompute(wait_result=lost)
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(
            issue_id, autonomous=True
        )
    )

    assert result.status == "failed"
    assert result.reason == "reattach: worker lost"
    assert compute.waited  # it tried to harvest
    # Transitioned back to ready for redispatch (attempt 1 < max), no PR opened.
    assert ["chainlink", "issue", "label", str(issue_id), "worklink:ready"] in calls
    assert not any(isinstance(a, list) and a[:3] == ["gh", "pr", "create"] for a in calls)
    assert load_run_state(tmp_path, issue_id) is None


def test_sync_reattach_failed_result_keeps_primary_incident_when_release_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mimir.poller_triggers as poller_triggers
    import mimir_cultivate.worklink.orchestrator as orchestrator
    from mimir_cultivate.worklink.dispatch_failures import dispatch_failure_state_dir, load_failure_state

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 563
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-original")
    retained = load_run_state(tmp_path, issue_id)
    assert retained is not None
    retained = replace(retained, checkout=str(tmp_path / "retained-checkout"))
    save_run_state(tmp_path, retained)
    calls: list = []
    base_runner = _remote_runner(
        repo, calls, issue_id=issue_id, labels=["worklink:in-progress"]
    )

    def runner(
        args: Sequence[str] | str, **kwargs: object,
    ) -> subprocess.CompletedProcess:
        if isinstance(args, list) and args[1:3] == ["locks", "release"]:
            calls.append(args)
            return cp(args, returncode=1, stderr="release failed")
        return base_runner(args, **kwargs)

    class FailureBackend(FakeBackend):
        async def interpret(self, order: WorkOrder, result: object) -> RawResult:
            order.checkout.mkdir(parents=True, exist_ok=True)
            (order.checkout / "changed.txt").write_text("failed\n", encoding="utf-8")
            return RawResult(
                1,
                order.transcript_root / "fake.json",
                "backend_error",
                "retained backend failed",
            )

    compute = FakeRemoteCompute(
        wait_result=ComputeResult(exit_code=0, stdout="ok", stderr="")
    )
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FailureBackend())
    registry.register_compute(compute)
    writes: list[dict[str, object]] = []
    ready_signals: list[str] = []
    real_record = orchestrator._record_run_failure

    def record_once(**fields: object) -> dict[str, object] | None:
        incident = real_record(**fields)
        assert incident is not None
        writes.append(dict(incident))
        return incident

    monkeypatch.setattr(orchestrator, "BackendRegistry", lambda *_: registry)
    monkeypatch.setattr(orchestrator, "_runner_for_home", lambda *_: runner)
    monkeypatch.setattr(orchestrator, "_record_run_failure", record_once)
    monkeypatch.setattr(
        poller_triggers,
        "notify_poller",
        lambda home, poller, *, reason: ready_signals.append(reason) or True,
    )

    result = run_worklink_reattach(
        home=tmp_path,
        repo=repo,
        issue_id=issue_id,
        autonomous=True,
    )

    assert result.status == "failed"
    assert result.incident_recorded is True
    assert result.reason == "terminal recovery incomplete: Chainlink lock release failed"
    assert len(writes) == 1
    [primary] = writes
    assert load_run_state(tmp_path, issue_id) == retained
    current = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
        str(issue_id)
    ]
    assert current["signature"] == primary["signature"]
    assert current["occurrence_id"] == primary["occurrence_id"]
    assert current["terminal_error"] == primary["terminal_error"]
    assert current["terminal_error"] == "retained backend failed"
    assert current["preserved_ref"] == retained.branch
    assert current["work_path"] == primary["work_path"]
    assert ready_signals == []
    assert compute.launched == []
    assert compute.waited == [LaunchHandle("fake_remote", "job-original")]


@pytest.mark.parametrize(
    "branch",
    [
        "shim",
        "shim-cleanup",
        "lost-in-progress",
        "lookup",
        "nonresumable",
        "worker-lost",
        "exception",
        "base-exception",
    ],
)
@pytest.mark.parametrize("write_fails", [False, True], ids=["durable", "write-failure"])
def test_autonomous_reattach_failure_branch_records_original_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, branch: str, write_fails: bool,
) -> None:
    import mimir_cultivate.worklink.orchestrator as orchestrator
    from mimir_cultivate.worklink.dispatch_failures import dispatch_failure_state_dir, load_failure_state

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 580
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="original-job")
    retained = load_run_state(tmp_path, issue_id)
    assert retained is not None
    retained = replace(
        retained,
        checkout=str(tmp_path / "retained-checkout"),
        **(
            {
                "handle_substrate": "local_subprocess",
                "handle_identifier": "123e4567-e89b-42d3-a456-426614174000",
                "shim_pid": 999_999_999,
                "process_start_ticks": 1,
            }
            if branch in {"shim", "shim-cleanup"}
            else {}
        ),
        **({"compute_name": "fake_local"} if branch == "nonresumable" else {}),
    )
    save_run_state(tmp_path, retained)
    labels = [] if branch == "lost-in-progress" else ["worklink:in-progress"]
    calls: list = []
    base_runner = _remote_runner(repo, calls, issue_id=issue_id, labels=labels)

    def runner(
        args: Sequence[str] | str, **kwargs: object,
    ) -> subprocess.CompletedProcess:
        if isinstance(args, list) and tuple(args[1:3]) in {
            ("issue", "label"),
            ("issue", "unlabel"),
            ("locks", "release"),
        }:
            incident = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
                str(issue_id)
            ]
            assert incident["active"] is True
        return base_runner(args, **kwargs)

    registry = BackendRegistry(WorklinkConfig())
    compute: FakeRemoteCompute | None = None

    if branch != "lookup":
        registry.register(FakeBackend())
    if branch == "nonresumable":
        registry.register_compute(FakeLocalCompute())
    elif branch in {"worker-lost", "exception", "base-exception"}:
        if branch == "worker-lost":
            compute = FakeRemoteCompute(wait_result=ComputeResult(
                exit_code=-1,
                stdout="",
                stderr="broker gone",
                launch_error="broker wait failed",
            ))
        elif branch == "exception":
            class ExceptionCompute(FakeRemoteCompute):
                async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
                    raise RuntimeError("remote wait failed")

            compute = ExceptionCompute()
        else:
            class InterruptCompute(FakeRemoteCompute):
                async def wait(self, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
                    raise asyncio.CancelledError

            compute = InterruptCompute()
        registry.register_compute(compute)
    elif branch not in {"shim", "shim-cleanup", "lost-in-progress"}:
        compute = FakeRemoteCompute()
        registry.register_compute(compute)

    if branch == "shim":
        monkeypatch.setattr(orchestrator, "process_is_alive", lambda state: False)
    elif branch == "shim-cleanup":
        monkeypatch.setattr(orchestrator, "process_is_alive", lambda state: True)

        async def cleanup_failure(*args: object, **kwargs: object) -> None:
            raise OSError("cleanup failed")

        monkeypatch.setattr(
            orchestrator.LocalSubprocessComputeBackend,
            "cancel",
            cleanup_failure,
        )

    real_record = orchestrator._record_run_failure
    write_attempts = 0

    def record_failure(**kwargs: object) -> dict[str, object] | None:
        nonlocal write_attempts
        write_attempts += 1
        if write_fails:
            raise OSError("disk full")
        return real_record(**kwargs)

    real_clear = orchestrator.clear_run_state
    clears: list[int] = []

    def clear_after_incident(home: Path, selected_issue: int) -> None:
        incident = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
            str(issue_id)
        ]
        assert incident["active"] is True
        clears.append(selected_issue)
        real_clear(home, selected_issue)

    monkeypatch.setattr(orchestrator, "_record_run_failure", record_failure)
    monkeypatch.setattr(orchestrator, "clear_run_state", clear_after_incident)
    monkeypatch.setattr(orchestrator, "BackendRegistry", lambda *_: registry)
    monkeypatch.setattr(orchestrator, "_runner_for_home", lambda *_: runner)

    if branch == "base-exception":
        with pytest.raises(asyncio.CancelledError):
            run_worklink_reattach(
                home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
            )
    elif write_fails:
        with pytest.raises(OSError, match="disk full"):
            run_worklink_reattach(
                home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
            )
    else:
        result = run_worklink_reattach(
            home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
        )
        assert result.status == "failed"

    assert write_attempts == 1

    assert not any(
        isinstance(call, list) and call[1:3] == ["locks", "claim"]
        for call in calls
    )
    if compute is not None:
        assert compute.launched == []
    if write_fails:
        assert load_run_state(tmp_path, issue_id) == retained
        assert clears == []
        assert not any(
            isinstance(call, list)
            and tuple(call[1:3]) in {
                ("issue", "label"),
                ("issue", "unlabel"),
                ("locks", "release"),
            }
            for call in calls
        )
        return

    incident = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
        str(issue_id)
    ]
    assert incident["active"] is True
    assert incident["attempt"] == retained.attempt
    assert incident["preserved_ref"] == retained.branch
    assert incident["work_path"] == retained.checkout
    assert retained.handle_identifier == (
        "123e4567-e89b-42d3-a456-426614174000"
        if branch in {"shim", "shim-cleanup"}
        else "original-job"
    )


def test_reattach_incident_write_failure_preserves_claim_and_retained_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mimir_cultivate.worklink.orchestrator as orchestrator

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 562
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-gone")
    calls: list = []
    runner = _remote_runner(
        repo, calls, issue_id=issue_id, labels=["worklink:in-progress"]
    )
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(FakeRemoteCompute(wait_result=ComputeResult(
        exit_code=-1,
        stdout="",
        stderr="broker gone",
        launch_error="broker wait failed",
    )))
    monkeypatch.setattr(
        orchestrator,
        "_record_run_failure",
        lambda **kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(OSError, match="disk full"):
        asyncio.run(
            WorklinkRunner(
                home=tmp_path, repo=repo, runner=runner, registry=registry
            ).reattach(issue_id, autonomous=True)
        )

    assert load_run_state(tmp_path, issue_id) is not None
    assert not any(
        isinstance(call, list) and call[1:3] == ["locks", "release"]
        for call in calls
    )
    assert not any(
        isinstance(call, list) and call[1:3] in (["issue", "label"], ["issue", "unlabel"])
        for call in calls
    )


def test_reattach_primary_incident_survives_secondary_transition_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir_cultivate.worklink import orchestrator
    from mimir_cultivate.worklink.dispatch_failures import dispatch_failure_state_dir, load_failure_state

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 562
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="original-job")
    calls: list = []
    runner = _remote_runner(
        repo, calls, issue_id=issue_id, labels=["worklink:in-progress"]
    )
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(FakeRemoteCompute(wait_result=ComputeResult(
        exit_code=-1,
        stdout="",
        stderr="broker gone",
        launch_error="broker wait failed",
    )))
    monkeypatch.setattr(orchestrator, "BackendRegistry", lambda *_: registry)
    monkeypatch.setattr(orchestrator, "_runner_for_home", lambda *_: runner)
    monkeypatch.setattr(
        orchestrator.ChainlinkClaims,
        "transition_issue",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("secondary transition failed")
        ),
    )

    with pytest.raises(RuntimeError, match="secondary transition failed"):
        run_worklink_reattach(
            home=tmp_path, repo=repo, issue_id=issue_id, autonomous=True
        )

    incident = load_failure_state(dispatch_failure_state_dir(tmp_path))["issues"][
        str(issue_id)
    ]
    assert incident["active"] is True
    assert incident["terminal_error"] == "reattach: worker lost after controller restart"
    assert "secondary transition failed" not in incident["terminal_error"]


def test_successful_reattach_cannot_resolve_newer_concurrent_incident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir_cultivate.worklink import orchestrator
    from mimir_cultivate.worklink.dispatch_failures import (
        dispatch_failure_state_dir,
        load_failure_state,
        record_failure,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 562
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="original-job")
    calls: list = []
    runner = _remote_runner(
        repo, calls, issue_id=issue_id, labels=["worklink:in-progress"]
    )
    state_dir = dispatch_failure_state_dir(tmp_path)
    first = record_failure(
        state_dir,
        issue_id=issue_id,
        attempt=1,
        exit_status=1,
        error="startup reattach failed",
        log_path="reattach-562.log",
    )

    def create_newer_incident() -> None:
        record_failure(
            state_dir,
            issue_id=issue_id,
            attempt=1,
            exit_status=1,
            error="newer retained failure",
            log_path="newer.log",
        )

    class ReattachWriteBackend(FakeBackend):
        async def interpret(self, order: WorkOrder, result: object) -> RawResult:
            order.checkout.mkdir(parents=True, exist_ok=True)
            (order.checkout / "changed.txt").write_text("hello\n", encoding="utf-8")
            return await super().interpret(order, result)

    compute = FakeRemoteCompute(on_wait=create_newer_incident)
    registry = BackendRegistry(WorklinkConfig())
    registry.register(ReattachWriteBackend())
    registry.register_compute(compute)
    monkeypatch.setattr(orchestrator, "BackendRegistry", lambda *_: registry)
    monkeypatch.setattr(orchestrator, "_runner_for_home", lambda *_: runner)

    result = run_worklink_reattach(
        home=tmp_path,
        repo=repo,
        issue_id=issue_id,
        autonomous=True,
    )

    current = load_failure_state(state_dir)["issues"]["562"]
    assert result.status == "completed"
    assert current["active"] is True
    assert current["occurrence_id"] != first["occurrence_id"]
    assert current["terminal_error"] == "newer retained failure"
    assert compute.waited == [LaunchHandle("fake_remote", "original-job")]


def test_reattach_skips_when_leaf_no_longer_in_progress(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 563
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-x")
    calls: list = []
    # Reaper already moved it back to ready (no in-progress label).
    runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:ready"])
    compute = FakeRemoteCompute()
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(issue_id)
    )

    assert result.status == "failed"
    assert "no longer in-progress" in (result.reason or "")
    assert compute.waited == []  # never touched the worker
    assert ["chainlink", "locks", "release", str(issue_id)] in calls
    assert load_run_state(tmp_path, issue_id) is None


def test_reattach_retains_state_when_transitioned_lock_release_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 567
    _save_inflight_state(tmp_path, repo, issue_id=issue_id, job="job-x")
    calls: list = []
    base_runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:ready"])

    def runner(args, *, cwd=None):
        result = base_runner(args, cwd=cwd)
        if isinstance(args, list) and args[:3] == ["chainlink", "locks", "release"]:
            return cp(args, returncode=1, stderr="release denied")
        return result

    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    registry.register_compute(FakeRemoteCompute())

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).reattach(issue_id)
    )

    assert "no longer in-progress" in (result.reason or "")
    assert load_run_state(tmp_path, issue_id) is not None


def test_reattach_no_state_is_noop(tmp_path: Path) -> None:
    pytest.importorskip("mimir_cultivate")
    from mimir_cultivate.worklink.orchestrator import WorklinkRunner

    repo = tmp_path / "repo"
    repo.mkdir()
    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=lambda *a, **k: cp(a)).reattach(999)
    )
    assert result.status == "failed"
    assert result.reason == "reattach: no run state"


# --------------------------------------------------------------------------
# run() persistence: persisted for persistent substrates, cleared on completion
# --------------------------------------------------------------------------


def test_run_persists_state_for_persistent_compute_then_clears(tmp_path: Path) -> None:
    """After #832 the only built-in compute substrate is local_subprocess
    (persistent_after_disconnect=False), so a fresh run never persists state.
    This test pre-dates #832 and exercises the persistence plumbing against
    a fake persistent compute; it remains useful as a guard for the
    WorklinkRunner.run() persistence path should a future persistent substrate
    be added."""
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 441
    calls: list = []
    runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])

    seen: dict[str, object] = {}

    def on_wait() -> None:
        # While the worker runs, the handle is durably persisted so a restart can
        # find it.
        st = load_run_state(tmp_path, issue_id)
        seen["during"] = st

    compute = FakeRemoteCompute(on_wait=on_wait)

    class PersistentWriteBackend(FakeBackend):
        async def interpret(self, order: WorkOrder, result: object) -> RawResult:
            # Mimic a persistent resumable compute having pushed changes.
            order.checkout.mkdir(parents=True, exist_ok=True)
            (order.checkout / "changed.txt").write_text("hello\n", encoding="utf-8")
            return await super().interpret(order, result)

    registry = BackendRegistry(
        WorklinkConfig(defaults=WorklinkDefaults(compute_backend="fake_remote"))
    )
    registry.register(PersistentWriteBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry).run(
            issue_id, backend_name="fake"
        )
    )

    assert result.status == "completed", (result.reason, calls)
    during = seen["during"]
    assert isinstance(during, WorklinkRunState)
    assert during.compute_name == "fake_remote"
    assert during.handle_identifier == "job-1"
    assert during.branch == "issue/441-a1"
    # Cleared on terminal completion.
    assert load_run_state(tmp_path, issue_id) is None


def test_run_persists_state_for_local_compute_then_clears(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue_id = 442
    calls: list = []

    seen: dict[str, object] = {}

    def on_wait() -> None:
        seen["during"] = load_run_state(tmp_path, issue_id)

    # Local/shared-filesystem path: build an isolated checkout the backend writes into.
    worktree = repo.parent / ".worklink" / repo.name / f"{issue_id}-1"

    class LocalBackend(FakeBackend):
        async def interpret(self, order: WorkOrder, result: object) -> RawResult:
            order.checkout.mkdir(parents=True, exist_ok=True)
            (order.checkout / "changed.txt").write_text("hi\n", encoding="utf-8")
            return RawResult(0, order.transcript_root / "fake.json", "success", None)

    def local_runner(args, *, cwd=None, text=True, timeout=1800):
        calls.append(args)
        if isinstance(args, str):
            return cp(args, stdout="ok\n")
        if args[:4] == ["git", "clone", "--local", "--quiet"]:
            worktree.mkdir(parents=True, exist_ok=True)
            (worktree / ".git" / "objects" / "info").mkdir(parents=True)
            return cp(args)
        if args[:5] == ["git", "-C", str(repo), "rev-parse", "--verify"]:
            return cp(args, stdout="abc123\n")
        if args in (
            ["git", "-C", str(repo), "remote", "get-url", "--push", "origin"],
            ["git", "-C", str(worktree), "remote", "get-url", "--push", "origin"],
        ):
            return cp(args, stdout="git@github.com:jasoncarreira/mimir.git\n")
        if args == ["git", "-C", str(worktree), "rev-parse", "--show-toplevel"]:
            return cp(args, stdout=f"{worktree}\n")
        if args == ["git", "-C", str(worktree), "rev-parse", "--absolute-git-dir"]:
            return cp(args, stdout=f"{worktree / '.git'}\n")
        if args[:1] == ["chainlink"]:
            if args[1:3] == ["issue", "show"]:
                return cp(args, stdout=_issue_json(issue_id, ["worklink:in-progress"]))
            return cp(args)
        if args[:1] == ["git"]:
            if "config" in args:
                return cp(args, stdout="git@github.com:jasoncarreira/mimir.git\n")
            if args[2] == str(worktree) and "status" in args and "-z" in args:
                return cp(args, stdout="?? changed.txt\0" if not any(
                    isinstance(call, list) and "commit" in call for call in calls
                ) else "")
            if "diff" in args and "--cached" in args and "-z" in args:
                return cp(args, stdout=b"changed.txt\0")
            if "diff" in args and "--name-only" in args:
                return cp(args, stdout="changed.txt\n")
            if "diff" in args and "--stat" in args:
                return cp(args, stdout=" changed.txt | 1 +\n")
            if "diff" in args and "--cached" in args and "--quiet" in args:
                return cp(args, returncode=1)
            if "cat-file" in args and "blob" in args:
                return cp(args, stdout=b"clean content\n")
            return cp(args)
        if args[:3] == ["gh", "pr", "create"]:
            return cp(args, stdout="https://github.com/x/y/pull/1\n")
        return cp(args)

    compute = FakeLocalCompute(on_wait=on_wait)
    registry = BackendRegistry(
        WorklinkConfig(defaults=WorklinkDefaults(compute_backend="fake_local"))
    )
    registry.register(LocalBackend())
    registry.register_compute(compute)

    result = asyncio.run(
        WorklinkRunner(home=tmp_path, repo=repo, runner=local_runner, registry=registry).run(
            issue_id, backend_name="fake"
        )
    )

    assert result.status == "completed", (result.reason, calls)
    during = seen["during"]
    assert isinstance(during, WorklinkRunState)
    assert during.compute_name == "fake_local"
    assert during.checkout == str(worktree)
    assert load_run_state(tmp_path, issue_id) is None


# --------------------------------------------------------------------------
# server startup reconcile spawner
# --------------------------------------------------------------------------


def test_reattach_inflight_spawns_detached(tmp_path: Path, monkeypatch) -> None:
    server = pytest.importorskip("mimir_cultivate.server")

    _save_inflight_state(tmp_path, tmp_path / "repo", issue_id=601, job="j1")
    _save_inflight_state(tmp_path, tmp_path / "repo", issue_id=602, job="j2")
    monkeypatch.setenv("WORKLINK_REPO", "/workspace/mimir")
    monkeypatch.delenv("WORKLINK_RUN_BIN", raising=False)

    spawned: list[dict] = []

    def fake_popen(argv, **kwargs):
        spawned.append({"argv": argv, "kwargs": kwargs})
        return object()

    dispatched = server.reattach_inflight_worklink_runs(tmp_path, popen=fake_popen)

    assert sorted(dispatched) == [601, 602]
    assert len(spawned) == 2
    argvs = [s["argv"] for s in spawned]
    assert ["mimir", "worklink", "run", "601", "--reattach", "--autonomous",
            "--home", str(tmp_path), "--repo", "/workspace/mimir"] in argvs
    # Detached so a long worker wait never blocks startup.
    assert all(s["kwargs"].get("start_new_session") is True for s in spawned)


def test_reattach_inflight_noop_without_repo(tmp_path: Path, monkeypatch) -> None:
    server = pytest.importorskip("mimir_cultivate.server")

    _save_inflight_state(tmp_path, tmp_path / "repo", issue_id=601, job="j1")
    monkeypatch.delenv("WORKLINK_REPO", raising=False)
    spawned: list = []
    dispatched = server.reattach_inflight_worklink_runs(
        tmp_path, popen=lambda *a, **k: spawned.append(a)
    )
    assert dispatched == []
    assert spawned == []


def _factory_restart_record(
    home: Path,
    *,
    status: str,
    ticks: int | None,
    controller_phase: str | None = None,
) -> FactoryRunRecord:
    sandbox = home / "sandbox"
    sandbox.mkdir(exist_ok=True)
    factory_status = parse_factory_status(
        {
            "run_id": "700",
            "issue_key": "700",
            "valid": True,
            "sandbox_path": str(sandbox),
            "status": status,
            "mode": "autonomous",
            "branch": "epic/700",
            "pr_base": "main",
            "pr_draft": False,
            "lock": "stale",
            "dead_lock": True,
            "lock_session": "session-1",
            "gates": {},
            "steps": [{"agent": "implementation", "status": "running", "attempts": 1}],
            "slices": [{
                "id": "factory-070-migration", "status": "ready", "attempts": 0,
                "extra_attempts": 0, "retry_limit": 5,
            }],
            "validator": None,
            "pr_url": None,
            "terminal_result": {"reason": "opaque"},
        }
    )
    record = FactoryRunRecord(
        run_id="700",
        issue_id=700,
        attempt=1,
        repository="owner/repo",
        base_ref="main",
        branch="epic/700",
        launcher="/opt/factory/bin/factory.js",
        sandbox=str(sandbox),
        session="session-1",
        handle=LaunchHandle("local_subprocess", "999999999", ticks),
        status=factory_status,
        observed_at="2026-08-18T12:00:00+00:00",
        controller_phase=(
            controller_phase
            if controller_phase is not None
            else "parked" if status == "needs-human" else "running"
        ),
    )
    save_factory_record(home, record)
    return record


@pytest.mark.parametrize(
    ("status", "ticks", "expected"),
    [
        ("running", 1, [700]),
        ("needs-human", 1, []),
        ("running", None, []),
    ],
)
def test_factory_startup_recovery_routes_only_verified_dead_runs_through_run_epic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    ticks: int | None,
    expected: list[int],
) -> None:
    # Pin run_bin: server.py reads WORKLINK_RUN_BIN and the deployment sets it to
    # "uv run mimir", which would prepend ["uv", "run"] to the asserted argv.
    monkeypatch.setenv("WORKLINK_RUN_BIN", "mimir")
    server = pytest.importorskip("mimir_cultivate.server")

    _factory_restart_record(tmp_path, status=status, ticks=ticks)
    monkeypatch.setenv("WORKLINK_REPO", "/workspace/mimir")
    spawned: list[list[str]] = []

    dispatched = server.reattach_inflight_worklink_runs(
        tmp_path,
        popen=lambda argv, **kwargs: spawned.append(list(argv)) or object(),
    )

    assert dispatched == expected
    if expected:
        assert spawned == [[
            "mimir",
            "worklink",
            "run-epic",
            "700",
            "--autonomous",
            "--home",
            str(tmp_path),
            "--repo",
            "/workspace/mimir",
        ]]
    else:
        assert spawned == []


def test_factory_startup_recovery_does_not_spawn_run_epic_for_failed_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = pytest.importorskip("mimir_cultivate.server")

    _factory_restart_record(
        tmp_path,
        status="running",
        ticks=1,
        controller_phase="failed",
    )
    monkeypatch.setenv("WORKLINK_REPO", "/workspace/mimir")
    spawned: list[list[str]] = []

    dispatched = server.reattach_inflight_worklink_runs(
        tmp_path,
        popen=lambda argv, **kwargs: spawned.append(list(argv)) or object(),
        event_logger=lambda *args, **kwargs: None,
    )

    assert dispatched == []
    assert spawned == []


def test_factory_startup_recovery_isolates_unparseable_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = pytest.importorskip("mimir_cultivate.server")

    valid = _factory_restart_record(tmp_path, status="running", ticks=1)
    valid_path = tmp_path / "state/worklink/factory-runs/700.json"
    payload = json.loads(valid_path.read_text(encoding="utf-8"))
    bad_sandbox = tmp_path / "chainlink-701"
    payload.update({
        "run_id": "chainlink-701",
        "issue_id": 701,
        "branch": "feature/chainlink-701",
        "sandbox": str(bad_sandbox),
    })
    payload["status"]["run_id"] = "chainlink-701"
    payload["status"]["sandbox_path"] = str(bad_sandbox)
    payload["status"]["slices"] = [
        {"id": "factory-070-migration", "status": "ready", "attempts": 0}
    ]
    bad_path = valid_path.with_name("chainlink-701.json")
    bad_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv("WORKLINK_REPO", "/workspace/mimir")
    monkeypatch.setenv("WORKLINK_RUN_BIN", "mimir")
    events: list[tuple[str, dict[str, object]]] = []
    spawned: list[list[str]] = []

    dispatched = server.reattach_inflight_worklink_runs(
        tmp_path,
        popen=lambda argv, **kwargs: spawned.append(list(argv)) or object(),
        event_logger=lambda event, **fields: events.append((event, fields)),
    )

    assert dispatched == [valid.issue_id]
    assert len(spawned) == 1
    failure = next(
        fields for event, fields in events
        if event == "worklink_reattach_dispatch_failed"
        and fields["reason"] == "factory_record_load_failed"
    )
    assert failure["path"] == str(bad_path)
    assert "slices.extra_attempts missing" in str(failure["error"])
    attempted = next(fields for event, fields in events if event == "worklink_reattach_attempted")
    assert attempted["examined"] == 1


def test_part_b_startup_recovery_emits_for_all_five_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = pytest.importorskip("mimir_cultivate.server")
    import mimir_cultivate.worklink.factory_state as factory_state

    monkeypatch.setenv("WORKLINK_REPO", "/workspace/mimir")
    monkeypatch.setenv("WORKLINK_RUN_BIN", "mimir")

    def capture() -> tuple[list[tuple[str, dict[str, object]]], object]:
        events: list[tuple[str, dict[str, object]]] = []
        return events, lambda event, **payload: events.append((event, payload))

    state_home = tmp_path / "state-arms"
    _save_inflight_state(state_home, tmp_path / "repo", issue_id=4242, job="j1")
    for spawn in (
        lambda *args, **kwargs: object(),
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("spawn failed")),
    ):
        events, logger = capture()
        server.reattach_inflight_worklink_runs(
            state_home,
            popen=spawn,
            event_logger=logger,
        )
        assert events

    factory_home = tmp_path / "factory-arms"
    factory_home.mkdir()
    _factory_restart_record(factory_home, status="running", ticks=1)
    for spawn in (
        lambda *args, **kwargs: object(),
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("spawn failed")),
    ):
        events, logger = capture()
        server.reattach_inflight_worklink_runs(
            factory_home,
            popen=spawn,
            event_logger=logger,
        )
        assert events

    enumeration_home = tmp_path / "enumeration-arm"
    monkeypatch.setattr(
        factory_state,
        "list_factory_records",
        lambda home: (_ for _ in ()).throw(OSError("records unavailable")),
    )
    events, logger = capture()
    server.reattach_inflight_worklink_runs(
        enumeration_home,
        popen=lambda *args, **kwargs: object(),
        event_logger=logger,
    )
    assert any(
        event == "worklink_reattach_dispatch_failed"
        and payload["reason"] == "factory_record_enumeration_failed"
        for event, payload in events
    )


@pytest.mark.parametrize(
    ("scenario", "alive", "cancel_error", "expected_cancel"),
    [
        ("cancel_success", True, None, True),
        ("term_kill_group_reap", True, None, True),
        ("shim_identity_mismatch", False, None, False),
        ("executor_error", True, RuntimeError("executor unavailable"), True),
    ],
)
def test_worker_restart_is_cleanup_only_and_retains_checkout(
    tmp_path: Path,
    monkeypatch,
    scenario: str,
    alive: bool,
    cancel_error: Exception | None,
    expected_cancel: bool,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    checkout = tmp_path / "authorized-checkout"
    checkout.mkdir()
    (checkout / "worker-output.txt").write_text("retain me\n")
    issue_id = 1410
    state = WorklinkRunState(
        issue_id=issue_id,
        attempt=1,
        backend="opencode",
        compute_name="local_subprocess",
        handle_substrate="local_subprocess",
        handle_identifier="123e4567-e89b-42d3-a456-426614174000",
        branch="issue/1410-a1",
        base_ref="main",
        local_base="abc123",
        repo=str(repo),
        repo_url="git@github.com:jasoncarreira/mimir.git",
        test_command="pytest -q",
        started_at="2026-08-10T00:00:00+00:00",
        checkout=str(checkout),
        process_start_ticks=10,
        shim_pid=4321,
    )
    save_run_state(tmp_path, state)
    calls = []
    runner = _remote_runner(repo, calls, issue_id=issue_id, labels=["worklink:in-progress"])
    cancel_events = []
    escalation_signals = []
    escalation_waits = []
    process_waited = []

    async def cancel(self, handle):
        cancel_events.append(("cancel", handle))
        if scenario == "term_kill_group_reap":
            import mimir_cultivate.worklink.worker_exec as worker_exec

            class Process:
                pid = 4321

                def wait(self, timeout=None):
                    process_waited.append(timeout)

            monkeypatch.setattr(
                worker_exec.os,
                "killpg",
                lambda pid, sig: escalation_signals.append((pid, sig)),
            )
            monkeypatch.setattr(
                worker_exec,
                "_wait_process_group",
                lambda pgid, deadline: (
                    escalation_waits.append((pgid, deadline)) or True
                ),
            )
            monkeypatch.setattr(
                worker_exec, "_process_group_has_live_members", lambda pgid: True
            )
            worker_exec._terminate_process_group(Process(), timeout_s=0.01)
        if cancel_error is not None:
            raise cancel_error

    monkeypatch.setattr("mimir_cultivate.worklink.orchestrator.process_is_alive", lambda _state: alive)
    monkeypatch.setattr(
        "mimir_cultivate.worklink.orchestrator.LocalSubprocessComputeBackend.cancel",
        cancel,
    )
    monkeypatch.setattr(
        "mimir_cultivate.worklink.orchestrator.observe_evidence",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("evidence must not run")),
    )
    monkeypatch.setattr(
        "mimir_cultivate.worklink.orchestrator.ControllerGitPublication.capture",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("publication must not run")),
    )

    async def run_cleanup():
        return await asyncio.wait_for(
            WorklinkRunner(home=tmp_path, repo=repo, runner=runner).reattach(issue_id),
            timeout=1,
        )

    result = asyncio.run(run_cleanup())

    assert result.status == "failed"
    assert result.review_ready is False
    assert checkout.is_dir()
    assert (checkout / "worker-output.txt").read_text() == "retain me\n"
    assert load_run_state(tmp_path, issue_id) is None
    assert bool(cancel_events) is expected_cancel
    assert ["chainlink", "locks", "release", str(issue_id)] in calls
    assert not any(
        isinstance(call, list)
        and call[:3] == ["chainlink", "issue", "label"]
        and "worklink:review" in call
        for call in calls
    )
    if scenario == "term_kill_group_reap":
        import signal

        assert escalation_signals == [(4321, signal.SIGTERM), (4321, signal.SIGKILL)]
        assert len(escalation_waits) == 2
        assert escalation_waits[0][1] is not None
        assert escalation_waits[1][0] == 4321
        assert escalation_waits[1][1] is not None
        assert process_waited == [5.0]
