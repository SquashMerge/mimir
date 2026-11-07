"""Provider-owned factory completion and evidence boundaries."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink import autonomy, evidence, factory_store, orchestrator, worker_client, worker_exec
from mimir_cultivate.worklink.backends.base import ComputeLaunchError
from mimir_cultivate.worklink.backends.feature_factory import FeatureFactoryBackend, parse_factory_status
from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig, WorklinkDefaults
from mimir_cultivate.worklink.checkout import CheckoutLease
from mimir_cultivate.worklink.claims import ChainlinkClaims, ClaimRecord, ClaimResult
from mimir_cultivate.worklink.compute import ComputeResult, LaunchHandle, LocalSubprocessComputeBackend, WorkSpec
from mimir_cultivate.worklink.factory_state import load_factory_record
from mimir_cultivate.worklink.run_state import WorklinkRunState, load_run_state, save_run_state
from tests.opencode_fixtures import seed_codex_subscription


@contextmanager
def _executor_in_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, root: Path, socket_dir: Path,
):
    """Run the provider executor protocol without deployment-only root credentials."""
    identities = SimpleNamespace(
        mimir_uid=os.getuid(), worklink_uid=os.getuid(), worklink_gid=os.getgid(),
    )
    socket_path = socket_dir / "executor.sock"
    homes = tmp_path / "homes"
    homes.mkdir()
    monkeypatch.setattr(worker_client.identities, "get_identities", lambda: identities)
    monkeypatch.setattr(worker_exec, "get_identities", lambda: identities)
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(root))
    monkeypatch.setattr(worker_exec, "HOME_ROOT", homes)
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket_path)

    # The test has no root-owned socket or uid-changing privileges. Keep the
    # real request, FD transfer, checkout validation, process launch and reply.
    def connect(self: worker_client.WorkerClient, timeout_s: float | None = None) -> socket.socket:
        peer = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        peer.connect(str(self.socket_path))
        return peer

    def drop(checkout_fd: int) -> None:
        os.fchdir(checkout_fd)

    monkeypatch.setattr(worker_client.WorkerClient, "_connect", connect)
    monkeypatch.setattr(worker_exec, "_drop_worker", drop)
    errors: list[BaseException] = []
    requests: list[dict[str, object]] = []
    finished = threading.Event()

    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as listener:
        listener.bind(str(socket_path))
        listener.listen(8)
        listener.settimeout(0.1)

        def serve() -> None:
            while not finished.is_set():
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError as exc:
                    if not finished.is_set():
                        errors.append(exc)
                    break
                try:
                    requests.append(json.loads(connection.recv(65536, socket.MSG_PEEK)))
                    worker_exec.handle_connection(connection)
                except BaseException as exc:
                    errors.append(exc)
                    connection.close()

        thread = threading.Thread(target=serve, name="factory-test-executor")
        thread.start()
        try:
            yield requests
        finally:
            finished.set()
            thread.join(timeout=10)
            assert not thread.is_alive()
            assert not errors


def test_provider_renders_default_work_order_without_override(tmp_path: Path) -> None:
    issue = orchestrator.IssueContext(1949, "Move coding", "Implement the factory", {"worklink:ready"})
    rendered = orchestrator.render_work_order(
        issue,
        template_path=orchestrator._template_path(tmp_path),
        backend_name="feature_factory",
        test_command="uv run pytest -q",
    )
    assert rendered.startswith("You are executing this Worklink issue.\n")
    assert "Title: Move coding" in rendered
    assert "#1949" not in rendered and "chainlink 1949" not in rendered.lower()
    assert "Parent:" not in rendered
    assert "Implement the factory" in rendered
    assert "uv run pytest -q" in rendered
    _assert_publication_rule(rendered)


def _assert_publication_rule(order: str) -> None:
    rule = next((line for line in order.splitlines() if line.startswith("- Worklink commits,")), "")
    for phrase in (
        "pushes the branch and opens the pull request after its gate passes",
        "builder never pushes, calls `gh` or opens a PR itself",
        "opening, titling or describing the PR",
        "`.worklink-pr-body.md` / `.worklink-commit-subject`",
        "PR publication criteria are never a reason for `WORKLINK_BLOCKED`",
    ):
        assert phrase in rule


def test_default_work_order_publication_rule_mutation_is_detected(tmp_path: Path) -> None:
    issue = orchestrator.IssueContext(1949, "Move coding", "Open a PR for review", {"worklink:ready"})
    template = orchestrator._template_path(tmp_path)
    original = template.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    removed = [line for line in lines if line.startswith("- Worklink commits,")]
    assert len(removed) == 1
    mutated = tmp_path / "mutated-order.md"
    mutated.write_text("".join(line for line in lines if line not in removed), encoding="utf-8")
    rendered = orchestrator.render_work_order(
        issue, template_path=mutated, backend_name="opencode", test_command="uv run pytest -q",
    )
    with pytest.raises(AssertionError):
        _assert_publication_rule(rendered)


def test_provider_runner_dry_run_uses_default_work_order(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    issue = json.dumps({
        "id": 1949, "title": "Move coding",
        "description": "Acceptance criteria:\n- [ ] Move the factory\n- [ ] Test it\n\n"
        "Review criteria:\n- Check the gate\n\nWorklink notes:\n- Scope: factory\n"
        "- Out of scope: UI\n- Suggested test command: pytest -q",
        "labels": ["worklink:ready"], "comments": [],
    })

    def runner(args: list[str] | str, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if isinstance(args, list) and args[:4] == ["chainlink", "issue", "show", "1949"]:
            return subprocess.CompletedProcess(args, 0, issue, "")
        if isinstance(args, list) and args[:4] == ["git", "-C", str(repo), "config"]:
            return subprocess.CompletedProcess(args, 0, "git@github.com:owner/repository.git\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    result = asyncio.run(orchestrator.WorklinkRunner(home=tmp_path, repo=repo, runner=runner).run(
        1949, backend_name="feature_factory", dry_run=True,
    ))
    assert result.dry_run
    output = capsys.readouterr().out
    assert "You are executing this Worklink issue." in output
    assert "Move the factory" in output
    assert "WORKLINK_BLOCKED:" in output


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform != "linux", reason="factory executor requires Linux")
@pytest.mark.parametrize("reaped", [False, True])
@pytest.mark.parametrize("pr_head", [
    "equal", "advanced", "unrelated", "compare_error", "ancestry_timeout",
    "invalid_json", "invalid_data", "behind", "diverged", "wrong_merge_base",
    "missing_behind", "bool_behind", "invalid_sha", "control_overflow",
])
async def test_provider_completion_uses_emitted_record_and_worker_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, short_socket_dir: Path, reaped: bool, pr_head: str,
) -> None:
    root = tmp_path / "worklink"
    checkout = root / "repository" / "1949-1" / "checkout"
    checkout.mkdir(parents=True)

    def git_at(path: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(path), *args], capture_output=True, text=True, check=True,
        ).stdout.strip()

    # Match factory init: forge -> local lease -> clone --local sandbox.
    # Collaborator commits and refs/pull/* exist only at the forge, never in
    # the lease used as the sandbox's origin.
    forge = tmp_path / "forge"
    forge.mkdir()
    git_at(forge, "init", "-q", "-b", "main")
    git_at(forge, "config", "user.email", "factory@example.org")
    git_at(forge, "config", "user.name", "Factory")
    (forge / "base.py").write_text("base\n")
    git_at(forge, "add", "base.py")
    git_at(forge, "commit", "-qm", "base")
    git_at(forge, "remote", "add", "origin", "git@github.com:owner/repository.git")
    git_at(tmp_path, "clone", "--local", "--", str(forge), str(checkout))
    root.chmod(0o755)
    checkout.parent.parent.chmod(0o755)
    checkout.parent.chmod(0o2750)
    checkout.chmod(0o2770)
    db_path = checkout.parent / ".factory-runtime" / "data" / "opencode" / "opencode.db"
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE event (type TEXT)")
        db.execute("INSERT INTO event VALUES ('message.part.updated')")
    # The controller must not open the worker-owned store itself. The executor
    # invokes the same script under the checkout owner, outside this process.
    monkeypatch.setattr(factory_store, "_prune_db", lambda path: pytest.fail("controller opened worker store"))
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".factory.json").write_text(json.dumps({
        "publishing_identity": "factory-owner", "bootstrap": "uv sync",
    }))
    gate_command = "uv run python -c 'print(1949)'"
    # Pin the opencode model/auth inputs work_spec resolves, as the core factory
    # tests do; otherwise the launch reads the host's ~/.local/share/opencode and
    # fails with feature_factory_opencode_resolution_failed:auth_missing anywhere
    # that file is absent (CI, containers).
    operator_home = tmp_path / "operator-home"
    monkeypatch.setenv("HOME", str(operator_home))
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setenv("MIMIR_MODEL_SPEC", "codex-plus:gpt-5.6-luna")
    auth = operator_home / ".local" / "share" / "opencode" / "auth.json"
    auth.parent.mkdir(parents=True)
    auth.write_text(json.dumps({"openai": {"type": "oauth", "refresh": "subscription"}}))
    seed_codex_subscription(operator_home)
    (tmp_path / "worklink.yaml").write_text(f"defaults:\n  test_command: {json.dumps(gate_command)}\n")
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(root))
    monkeypatch.setattr(orchestrator, "get_identities", lambda: SimpleNamespace(
        mimir_uid=os.getuid(), worklink_uid=os.getuid(), worklink_gid=os.getgid(),
    ))
    monkeypatch.setattr(orchestrator, "_read_checkout_git_identity", lambda *a: ("Factory", "factory@example.org"))
    monkeypatch.setattr(orchestrator, "_read_factory_publishing_identity", lambda *a: ("factory-owner", ".factory.json"))
    monkeypatch.setattr(orchestrator, "_resolve_factory_github_credential", lambda *a: ("token", {"GH_TOKEN": "token"}))
    monkeypatch.setattr(orchestrator, "GitHubForgeClient", lambda **kw: SimpleNamespace(verify_identity=lambda login: login))
    monkeypatch.setattr(FeatureFactoryBackend, "admit", lambda self: Path(self.entrypoint))
    monkeypatch.setattr(orchestrator, "_create_backend_checkout", lambda *a, **kw: CheckoutLease(
        issue_id=1949, attempt=1, repo=repo, path=checkout, branch="main",
        base_ref="main", isolated_checkout=True,
    ))
    claim = ClaimRecord(1949, 1, "agent", datetime.now(UTC))
    monkeypatch.setattr(orchestrator.ChainlinkClaims, "claim_issue", lambda self, *a, **kw: ClaimResult(True, claim))
    transitions: list[dict[str, object]] = []
    labels: list[tuple[str, ...]] = []
    transition_issue = ChainlinkClaims.transition_issue

    def transition(self: ChainlinkClaims, issue_id: int, **kwargs: object) -> None:
        transitions.append(kwargs)
        transition_issue(self, issue_id, **kwargs)

    monkeypatch.setattr(ChainlinkClaims, "transition_issue", transition)
    monkeypatch.setattr(ChainlinkClaims, "_run", lambda self, *args, **kw: (
        labels.append(args) or subprocess.CompletedProcess(args, 0, "", "")
    ))
    monkeypatch.setattr(orchestrator.ChainlinkClaims, "release_issue", lambda *a, **kw: True)
    dead = subprocess.Popen(["true"])
    dead.wait()
    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "launch", lambda self, spec: asyncio.sleep(
        0, result=LaunchHandle("local_subprocess", str(dead.pid), 1, dead.pid),
    ))
    cancelled = asyncio.Event()
    waits: list[LaunchHandle] = []
    cancels: list[LaunchHandle] = []
    cleanups: list[LaunchHandle] = []

    supervision_order = []
    real_prune = factory_store.prune_factory_store

    def checked_prune(record):
        # Exercise the production observe -> cancel -> drain -> persist -> prune
        # wiring, not just a record with an acknowledgement injected in isolation.
        assert supervision_order == ["cancel", "drained"]
        assert record.children_reaped is reaped
        persisted = load_factory_record(tmp_path, record.run_id)
        assert persisted is not None and persisted.children_reaped is reaped
        supervision_order.append("prune")
        return real_prune(record)

    monkeypatch.setattr(factory_store, "prune_factory_store", checked_prune)

    async def wait(self: object, handle: LaunchHandle, timeout_s: int) -> ComputeResult:
        waits.append(handle)
        await cancelled.wait()
        supervision_order.append("drained")
        return ComputeResult(0, "", "", handle=handle, children_reaped=reaped)

    async def cancel(self: object, handle: LaunchHandle) -> None:
        cancels.append(handle)
        supervision_order.append("cancel")
        cancelled.set()

    async def cleanup(self: object, handle: LaunchHandle) -> None:
        cleanups.append(handle)

    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "wait", wait)
    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "cancel", cancel)
    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "cleanup", cleanup)
    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "job_alive", lambda self, handle: not cancelled.is_set())
    overflow_events: list[dict[str, object]] = []
    if pr_head == "control_overflow":
        from mimir import event_logger

        original_log = event_logger.log_event_sync

        def capture_event(name: str, **fields: object) -> None:
            if name == "worklink_factory_control_overflow":
                overflow_events.append(fields)
            else:
                try:
                    original_log(name, **fields)
                except RuntimeError:
                    pass

        monkeypatch.setattr(event_logger, "log_event_sync", capture_event)
        real_launch = worker_client.WorkerClient.launch

        async def oversized_launch(self: worker_client.WorkerClient, **kwargs: object):
            if kwargs["argv"][0] == "/bin/sh" and "1048576" in kwargs["argv"][-1]:
                sink = kwargs["stdout_sink"]
                os.write(sink.fd, b"X" * (1048576 + 8))
                return SimpleNamespace(
                    wait=lambda: asyncio.sleep(0, result=0), timed_out=False,
                    output_overflow=True, stdout_bytes=1048584, stderr_bytes=0,
                )
            return await real_launch(self, **kwargs)

        monkeypatch.setattr(worker_client.WorkerClient, "launch", oversized_launch)

        async def overflow_gate(*args: object, **kwargs: object) -> ComputeResult:
            await asyncio.to_thread(
                worker_client.run_factory_control, checkout,
                ("/bin/sh", "-c", "python3 -c 'print(\"X\" * 1048576)'"),
                env={}, output_limit=1024 * 1024,
            )
            pytest.fail("oversized factory control unexpectedly returned")

        monkeypatch.setattr(evidence, "_run_compute_gate", overflow_gate)

    issue = json.dumps({
        "id": 1949, "title": "Factory", "description": "Build factory", "labels": ["worklink:epic"],
        "comments": [],
    })
    branches: list[str] = []
    heads: list[str] = []
    observed_heads: list[str] = []
    comparisons: list[list[str]] = []

    def publish_collaborator_head() -> None:
        if observed_heads:
            return
        # Publish the verified work to the forge, not back into the lease.
        git_at(forge, "fetch", str(sandbox), branches[0])
        git_at(forge, "checkout", "--detach", "FETCH_HEAD")
        if pr_head == "equal":
            observed_heads.append(heads[0])
        else:
            if pr_head == "unrelated":
                git_at(forge, "checkout", "--detach", "main")
            git_at(forge, "commit", "--allow-empty", "-qm", "collaborator")
            observed_heads.append(git_at(forge, "rev-parse", "HEAD"))
        git_at(forge, "update-ref", "refs/pull/42/head", observed_heads[0])

    def runner(args: list[str] | str, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if isinstance(args, list) and args[:4] == ["chainlink", "issue", "show", "1949"]:
            return subprocess.CompletedProcess(args, 0, issue, "")
        if isinstance(args, list) and args[:2] == ["gh", "api"]:
            if "/compare/" in args[2]:
                assert args == [
                    "gh", "api", f"repos/owner/repository/compare/{heads[0]}...{observed_heads[0]}",
                ]
                comparisons.append(args)
                if pr_head == "compare_error":
                    return subprocess.CompletedProcess(args, 1, "", "compare unavailable")
                if pr_head == "ancestry_timeout":
                    raise subprocess.TimeoutExpired(args, 1)
                if pr_head == "invalid_json":
                    return subprocess.CompletedProcess(args, 0, "{", "")
                if pr_head == "invalid_data":
                    return subprocess.CompletedProcess(args, 0, "[]", "")
                ancestor = subprocess.run([
                    "git", "-C", str(forge), "merge-base", "--is-ancestor", heads[0], observed_heads[0],
                ]).returncode == 0
                comparison = {
                    "status": "ahead" if ancestor else "diverged",
                    "behind_by": 0 if ancestor else 1,
                    "merge_base_commit": {"sha": git_at(forge, "merge-base", heads[0], observed_heads[0])},
                }
                if pr_head == "behind":
                    comparison["behind_by"] = 1
                elif pr_head == "diverged":
                    comparison["status"] = "diverged"
                elif pr_head == "wrong_merge_base":
                    comparison["merge_base_commit"] = {"sha": observed_heads[0]}
                elif pr_head == "missing_behind":
                    del comparison["behind_by"]
                elif pr_head == "bool_behind":
                    comparison["behind_by"] = False
                return subprocess.CompletedProcess(args, 0, json.dumps(comparison), "")
            assert args == ["gh", "api", "repos/owner/repository/pulls/42"]
            publish_collaborator_head()
            return subprocess.CompletedProcess(args, 0, json.dumps({
                "html_url": "https://github.com/owner/repository/pull/42", "state": "open", "draft": False,
                "base": {"repo": {"full_name": "owner/repository"}, "ref": "main"},
                "head": {"repo": {"full_name": "owner/repository"}, "ref": branches[0],
                         "sha": "invalid" if pr_head == "invalid_sha" else observed_heads[0]},
            }), "")
        if isinstance(args, list) and args[:3] == ["gh", "pr", "view"]:
            assert args[3:] == ["https://github.com/owner/repository/pull/42", "--json", "headRefOid"]
            return subprocess.CompletedProcess(args, 0, json.dumps({"headRefOid": heads[0]}), "")
        if isinstance(args, list) and args[:3] == ["git", "-C", str(repo)]:
            return subprocess.CompletedProcess(args, 0, "git@github.com:owner/repository.git\n", "")
        if isinstance(args, list) and args[:3] == ["git", "-C", str(checkout)]:
            if args[3:5] == ["config", "--get"]:
                return subprocess.run(args, capture_output=True, text=True)
            return subprocess.CompletedProcess(args, 0, "", "")
        if isinstance(args, list) and args[:2] == ["git", "-C"]:
            return subprocess.run(args, capture_output=True, text=True)
        return subprocess.CompletedProcess(args, 0, "", "")

    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(sandbox), *args], capture_output=True, text=True, check=True).stdout.strip()

    def status(self: FeatureFactoryBackend, run_id: str, *, sandbox: Path, launcher: str):
        assert sandbox == checkout / ".factory-sandboxes" / run_id
        if not branches:
            branches.append(f"feature/{run_id}")
            git_at(tmp_path, "clone", "--local", "--", str(checkout), str(sandbox))
            git("config", "user.email", "factory@example.org")
            git("config", "user.name", "Factory")
            git("checkout", "-qb", branches[0])
            (sandbox / "changed.py").write_text("changed\n")
            git("add", "changed.py")
            git("commit", "-qm", "change")
            heads.append(git("rev-parse", "HEAD"))
        return parse_factory_status({
            "run_id": run_id, "issue_key": "1949", "valid": True,
            "sandbox_path": str(sandbox), "status": "completed", "mode": "autonomous",
            "branch": branches[0], "pr_base": "main", "pr_draft": False,
            "lock": "absent", "dead_lock": False, "lock_session": None,
            "slices": [{"id": "implementation", "status": "merged", "attempts": 1,
                         "extra_attempts": 0, "retry_limit": 5}],
            "pr_url": "https://github.com/owner/repository/pull/42",
        })

    monkeypatch.setattr(FeatureFactoryBackend, "status", status)
    sandbox = checkout / ".factory-sandboxes" / "chainlink-1949"
    with _executor_in_process(tmp_path, monkeypatch, root, short_socket_dir) as requests:
        result = await orchestrator.WorklinkRunner(home=tmp_path, repo=repo, runner=runner).run_epic(1949)
        record = load_factory_record(tmp_path, "chainlink-1949")
        assert record is not None and record.sandbox == str(sandbox)
        spec = WorkSpec(
            issue_id=record.issue_id, attempt=record.attempt, repo_url="", base_ref=record.base_ref,
            branch=record.branch, prompt="", rules=None, test_command=gate_command,
            backend="feature_factory", timeout_s=30, local_checkout=sandbox,
        )
        for invalid_checkout, compute in (
            (sandbox / ".factory" / "worktrees" / "leaf", LocalSubprocessComputeBackend()),
            (sandbox, None),
        ):
            before = len(requests)
            with pytest.raises(ComputeLaunchError, match="recorded worker sandbox"):
                await evidence.observe_evidence(
                    issue=record.issue_id, attempt=record.attempt, backend="feature_factory",
                    branch=record.branch, checkout=invalid_checkout, started_at=datetime.now(UTC),
                    base_ref=record.base_ref, backend_status="completed", test_command=gate_command,
                    work_spec=spec, compute=compute, factory_run_id=record.run_id,
                )
            assert len(requests) == before
    assert len(waits) == len(cancels) == len(cleanups) == 1
    assert result.status == (
        "review_ready" if pr_head in {"equal", "advanced"} else "published-with-controller-error"
    ), result.reason
    assert transitions[-1]["status"] == ("review" if result.review_ready else "blocked")
    assert transitions[-1]["review_ready"] is result.review_ready
    applied = [args for args in labels if args[:2] == ("issue", "label")]
    assert applied == [("issue", "label", "1949", "worklink:review" if result.review_ready else "worklink:blocked")]
    assert all("worklink:ready" not in args for args in applied)
    if not result.review_ready:
        if pr_head == "control_overflow":
            assert result.pr_url == "https://github.com/owner/repository/pull/42"
            assert result.checkout.is_dir() and (result.checkout / "changed.py").exists()
            assert "factory control gate stdout exceeded 1048576 bytes" in result.reason
            assert "observed " in result.reason and "bytes" in result.reason
            assert overflow_events and overflow_events[0]["command"] == "gate"
            assert overflow_events[0]["observed_bytes"] > 1048576
            assert str(overflow_events[0]["observed_bytes"]) in result.reason
            assert f"head: {heads[0]}" in result.reason
            assert any(args[:3] == ("issue", "comment", "1949") and
                       args[3].startswith("WORKLINK_BLOCKED published-with-controller-error:") and
                       result.pr_url in args[3] for args in labels)
            assert not any(args[:2] == ("issue", "label") and "worklink:ready" in args for args in labels)
            assert record.controller_phase == "published-with-controller-error"
            assert autonomy._attempt_is_active(checkout.parent, [record],
                                               now=datetime(2100, 1, 1, tzinfo=UTC))
        else:
            assert "GitHub PR head SHA mismatch" in result.reason
            assert any(args[:3] == ("issue", "comment", "1949") and
                       args[3].startswith("WORKLINK_BLOCKED published-with-controller-error: GitHub PR head SHA mismatch")
                       and result.pr_url in args[3] for args in labels)
    assert bool(comparisons) is (pr_head not in {"equal", "invalid_sha", "control_overflow"})
    assert git("config", "--get", "remote.origin.url") == str(checkout)
    assert git_at(checkout, "config", "--get", "remote.origin.url") == str(forge)
    if pr_head != "control_overflow":
        assert git_at(forge, "rev-parse", "refs/pull/42/head") == observed_heads[0]
    assert not git_at(checkout, "for-each-ref", "refs/pull/")
    assert not git("for-each-ref", "refs/pull/")
    if pr_head == "advanced":
        # The old ancestry implementation cannot fetch a PR ref from the
        # local lease. This positive case must still finish review-ready.
        legacy_fetch = subprocess.run([
            "git", "-C", str(sandbox), "fetch", "--no-tags", "origin", "refs/pull/42/head",
        ], capture_output=True, text=True)
        assert legacy_fetch.returncode != 0
        assert "couldn't find remote ref" in legacy_fetch.stderr
        assert subprocess.run([
            "git", "-C", str(sandbox), "cat-file", "-e", observed_heads[0],
        ], capture_output=True).returncode != 0
    assert result.checkout == sandbox
    assert requests and all(request["op"] == "launch_factory_control" for request in requests)
    assert all(request["path"] == str(checkout) for request in requests)
    if pr_head == "control_overflow":
        assert not any(request["argv"] == [
            "/bin/sh", "-c", "python3 -c 'print(\"X\" * 1048576)'",
        ] for request in requests)
    else:
        assert any(request["argv"] == [
            "/bin/sh", "-c", f"cd {result.checkout} && {gate_command}",
        ] for request in requests)
    evidence_path = result.evidence_path or next((tmp_path / "state" / "worklink" / "evidence").rglob("*.json"))
    payload = json.loads(evidence_path.read_text())
    assert payload["checkout"] == str(result.checkout)
    if pr_head != "control_overflow":
        assert payload["head_sha"] == heads[0]
        assert payload["observed_pr_head_sha"] == ("invalid" if pr_head == "invalid_sha" else observed_heads[0])
    if pr_head != "control_overflow":
        assert payload["tests"]["exit_code"] == 0
        assert "1949" in payload["tests"]["summary"]
    assert supervision_order == ["cancel", "drained", "prune"]
    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT count(*) FROM event").fetchone() == (0 if reaped else 1,)


@pytest.mark.asyncio
@pytest.mark.parametrize("alive", [False, True])
async def test_provider_reattach_cleans_only_verified_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, alive: bool,
) -> None:
    checkout = tmp_path / "retained"
    checkout.mkdir()
    (checkout / "output.txt").write_text("keep")
    state = WorklinkRunState(
        issue_id=1949, attempt=1, backend="opencode", compute_name="local_subprocess",
        handle_substrate="local_subprocess", handle_identifier="123e4567-e89b-42d3-a456-426614174000",
        branch="issue/1949-a1", base_ref="main", local_base="abc123",
        repo=str(tmp_path), repo_url="git@github.com:owner/repository.git",
        test_command="uv run pytest -q", started_at=datetime.now(UTC).isoformat(),
        checkout=str(checkout), process_start_ticks=10, shim_pid=4321,
    )
    save_run_state(tmp_path, state)
    cancellations: list[LaunchHandle] = []
    transitions: list[str] = []

    async def cancel(self: object, handle: LaunchHandle) -> None:
        cancellations.append(handle)

    monkeypatch.setattr(orchestrator, "process_is_alive", lambda current: alive)
    monkeypatch.setattr(orchestrator.LocalSubprocessComputeBackend, "cancel", cancel)
    monkeypatch.setattr(orchestrator.ChainlinkClaims, "transition_issue", lambda self, *a, **kw: transitions.append(kw["status"]))
    monkeypatch.setattr(orchestrator.ChainlinkClaims, "release_issue", lambda *a, **kw: True)
    result = await orchestrator.WorklinkRunner(home=tmp_path, repo=tmp_path).reattach(1949)
    assert result.status == "failed"
    assert cancellations == ([LaunchHandle("local_subprocess", state.handle_identifier, 10, 4321)] if alive else [])
    assert transitions == ["failed"]
    assert (checkout / "output.txt").read_text() == "keep"
    assert load_run_state(tmp_path, 1949) is None


def test_provider_registry_refuses_unknown_backend_and_selects_factory() -> None:
    registry = BackendRegistry(WorklinkConfig(defaults=WorklinkDefaults(backend="feature_factory")))
    assert registry.get("feature_factory").name == "feature_factory"
    assert registry.select(labels={"worklink:epic"}, repo="owner/repository").name == "feature_factory"
    with pytest.raises((KeyError, ValueError)):
        registry.get("unregistered")


def test_published_head_lookup_refuses_noncanonical_url() -> None:
    def forbidden(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        pytest.fail("unverified PR URL reached the GitHub CLI")

    assert orchestrator._published_factory_head(
        "https://github.com/owner/repository/pull/42/extra", runner=forbidden,
    ) is None


def _large_status(sandbox: Path) -> dict[str, object]:
    return {
        "run_id": "chainlink-2109", "valid": True, "sandbox_path": str(sandbox),
        "next": "step current-agent",
        "slices": [
            {"id": f"slice-{index}",
             "status": "merged" if index < 23 else "in_review" if index < 28 else "pending",
             "attempts": 2, "extra_attempts": 1, "retry_limit": 5,
             "history": "x" * 40_000}
            for index in range(40)
        ],
        "steps": [
            {"agent": f"agent-{index}", "status": "running" if index == 29 else "completed",
             "attempts": 1, "history": "x" * 40_000}
            for index in range(30)
        ],
        "gates": {
            f"gate-{index}": {"status": "failed" if index == 19 else "passed",
                                "at": "2026-11-06T00:00:00Z", "artifact": "gate.log",
                                "reviewed_head": "a" * 40, "history": "x" * 40_000}
            for index in range(20)
        },
    }


def _assert_large_projection(projected: dict[str, object]) -> None:
    assert len(projected["slices"]) == 40
    assert len(projected["steps"]) == 30
    assert len(projected["gates"]) == 20
    assert all("history" not in row for row in projected["slices"] + projected["steps"])
    assert all("history" not in row for row in projected["gates"].values())
    assert projected["slices_total"] == 40
    assert projected["slices_counts"] == {"merged": 23, "in-review": 5, "pending": 12}
    parsed = parse_factory_status(projected)
    assert parsed.steps[-1] == {"agent": "agent-29", "status": "running", "attempts": 1}
    assert parsed.slices[-1]["retry_limit"] == 5
    assert parsed.gates["gate-19"]["artifact"] == "gate.log"
    assert parse_factory_status(parsed.to_json()) == parsed
    record = SimpleNamespace(session="session", run_id=parsed.run_id, sandbox=parsed.sandbox_path)
    reason = orchestrator._factory_budget_park_reason(parsed, record)
    assert "23 of 40 slices merged, 5 in review, 12 pending" in reason
    assert "gate-19=failed" in reason
    # Prove the alert uses worker counts rather than recounting a display list.
    display_only = replace(parsed, slices=parsed.slices[:16])
    assert "23 of 40 slices merged, 5 in review, 12 pending" in orchestrator._factory_budget_park_reason(
        display_only, record,
    )


def test_large_factory_status_projection_preserves_progress(tmp_path: Path) -> None:
    from mimir_cultivate.worklink.factory_control_output import _status_projection

    projected = _status_projection(_large_status(tmp_path))
    assert len(json.dumps(projected).encode()) < 256 * 1024
    _assert_large_projection(projected)


@pytest.mark.parametrize("original,mutant", [
    ("for row in rows]", "for row in rows[:16]]"),
    ("for name, row in gates.items()}", "for name, row in list(gates.items())[:16]}"),
])
def test_large_factory_status_regression_kills_row_cap(
    tmp_path: Path, original: str, mutant: str,
) -> None:
    import inspect
    from mimir_cultivate.worklink import factory_control_output

    source = inspect.getsource(factory_control_output._status_projection)
    assert source.count(original) == 1
    namespace = vars(factory_control_output).copy()
    exec(compile(source.replace(original, mutant), "<row-cap-mutation>", "exec"), namespace)
    with pytest.raises(AssertionError):
        _assert_large_projection(namespace["_status_projection"](_large_status(tmp_path)))


@pytest.mark.parametrize("summary", [
    {"slices_total": True, "slices_counts": {}},
    {"slices_total": 40, "slices_counts": {"merged": True, "pending": 39}},
    {"slices_total": 16, "slices_counts": {"merged": 16}},
])
def test_factory_status_refuses_invalid_worker_counts(tmp_path: Path, summary: dict[str, object]) -> None:
    from mimir_cultivate.worklink.factory_control_output import _status_projection
    from mimir_cultivate.worklink.backends.feature_factory import FactoryContractError

    projected = _status_projection(_large_status(tmp_path))
    projected.update(summary)
    with pytest.raises(FactoryContractError, match="slice summary"):
        parse_factory_status(projected)


@pytest.mark.skipif(sys.platform != "linux", reason="factory executor requires Linux")
def test_large_factory_control_projection_measured_through_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, short_socket_dir: Path,
) -> None:
    root = tmp_path / "worklink"
    checkout = root / "repository" / "2109-1" / "checkout"
    checkout.mkdir(parents=True)
    root.chmod(0o755)
    checkout.parent.parent.chmod(0o755)
    checkout.parent.chmod(0o2750)
    checkout.chmod(0o2770)
    sandbox = checkout / ".factory-sandboxes" / "chainlink-2109"
    sandbox.mkdir(parents=True)
    status = {
        "run_id": "chainlink-2109", "valid": True, "sandbox_path": str(sandbox),
        "issue_key": "2109", "status": "completed", "mode": "autonomous",
        "branch": "feature/chainlink-2109", "pr_base": "main", "pr_draft": False,
        "pr_url": "https://github.com/owner/repository/pull/42",
        "lock": "absent", "dead_lock": False, "lock_session": None,
        "slices": [{"id": f"slice-{index}", "status": "merged", "attempts": 2,
                    "extra_attempts": 1, "retry_limit": 5} for index in range(8)],
        "validator": {"verdict": "GO", "report": "remediation verified",
                      "reviewed_head": "a" * 40, "loops": 1},
        "terminal_result": {"result": "published", "remediation_log": "x" * 1_100_000},
    }
    status.update(_large_status(sandbox))
    artifact = tmp_path / "factory-status.json"
    artifact.write_text(json.dumps(status))
    cli = tmp_path / "control-stub.py"
    cli.write_text(
        "import pathlib,sys\n"
        "sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())\n"
    )
    wrapper = Path(__file__).resolve().parents[1] / "mimir_cultivate" / "worklink" / "factory_control_output.py"
    measurements: dict[str, tuple[int, int]] = {}
    with _executor_in_process(tmp_path, monkeypatch, root, short_socket_dir):
        for operation in ("status", "heartbeat", "resume", "lock", "terminal", "snapshot"):
            raw_command = [sys.executable, "-I", str(cli), str(artifact), operation]
            raw = worker_client.run_factory_control(
                sandbox, raw_command, env={}, output_limit=8 * 1024 * 1024,
            )
            projected = worker_client.run_factory_control(
                sandbox, [sys.executable, "-I", str(wrapper),
                          "status" if operation == "status" else "other", *raw_command],
                env={}, output_limit=1024 * 1024,
            )
            assert raw.returncode == projected.returncode == 0
            measurements[operation] = (len(raw.stdout), len(projected.stdout))
            assert len(projected.stdout) < 256 * 1024
            if operation == "status":
                parsed = parse_factory_status(projected.stdout)
                assert parsed.pr_url == status["pr_url"]
                assert parsed.validator["loops"] == 1
                _assert_large_projection(json.loads(projected.stdout))
    assert all(raw > 1024 * 1024 for raw, _ in measurements.values())
    # The other run-facing control sites use independent bounds. Exercise their
    # command shapes against the same eight-slice checkout, not only the CLI.
    def git(*arguments: str) -> None:
        subprocess.run(["git", "-C", str(sandbox), *arguments], check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.email", "worker@example.org")
    git("config", "user.name", "Worker")
    for index in range(8):
        (sandbox / f"slice-{index}.py").write_text("base\n")
    git("add", ".")
    git("commit", "-qm", "base")
    for index in range(8):
        (sandbox / f"slice-{index}.py").write_text("base\nremediation\n")
    other_measurements: dict[str, int] = {}
    second = tmp_path / "second"
    second.mkdir()
    second_socket = short_socket_dir / "second"
    second_socket.mkdir()
    with _executor_in_process(second, monkeypatch, root, second_socket):
        for name, args, limit in (
            ("git diff --name-only", ["git", "-C", str(sandbox), "diff", "--name-only", "HEAD"], 1024 * 1024),
            ("git diff --stat", ["git", "-C", str(sandbox), "diff", "--stat", "HEAD"], 1024 * 1024),
            ("git status --porcelain", ["git", "-C", str(sandbox), "status", "--porcelain=v1"], 1024 * 1024),
            ("git rev-parse HEAD", ["git", "-C", str(sandbox), "rev-parse", "HEAD"], 1024 * 1024),
            ("gate", ["/bin/sh", "-c", "python3 -c 'print(\"8 slices, remediation GO\")'"], 20_000_000),
        ):
            observed = worker_client.run_factory_control(sandbox, args, env={}, output_limit=limit)
            assert observed.returncode == 0
            other_measurements[name] = len(observed.stdout)
        runtime = checkout.parent / ".factory-runtime"
        runtime.mkdir()
        cleared = worker_client.run_factory_control(
            sandbox, [sys.executable, "-I", str(Path(factory_store.__file__).resolve()),
                      "--clear-runtime", str(runtime)], env={}, output_limit=4096,
        )
        assert cleared.returncode == 0
        other_measurements["store clear-runtime"] = len(cleared.stdout)
        auth = worker_client.run_factory_control(
            sandbox, [sys.executable, "-I", str(Path(worker_client.__file__).with_name("factory_auth.py"))],
            env={}, output_limit=64 * 1024 + 1,
        )
        other_measurements["auth read (missing)"] = len(auth.stdout)
        assert auth.returncode != 0
    print(f"synthetic factory control stdout bytes (raw, projected): {measurements}")
    print(f"synthetic other control stdout bytes: {other_measurements}")


@pytest.mark.parametrize("active,allowed", [(1, True), (2, False)])
def test_provider_autonomy_respects_claim_cap(tmp_path: Path, active: int, allowed: bool) -> None:
    (tmp_path / "worklink.yaml").write_text("defaults:\n  max_concurrent: 2\n")

    class Claims:
        def active_worklink_lock_count(self, *, exclude_label: str) -> int:
            assert exclude_label == "worklink:epic"
            return active

    check = autonomy.check_concurrency(tmp_path, claims=Claims())
    assert (check.active, check.cap, check.allowed) == (active, 2, allowed)
