"""Head-bound draft publication and repair, using fake forge and CI responses."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import stat
import subprocess

import pytest

from mimir_cultivate.repository import RepositoryInventory
from mimir_cultivate.worklink import ci_gate, control, orchestrator
from mimir_cultivate.worklink.backends.base import LaunchHandle, RawResult
from mimir_cultivate.worklink.backends.base import WorkOrder
from mimir_cultivate.worklink.backends.registry import WorklinkConfig, WorklinkDefaults
from mimir_cultivate.worklink.checkout import CheckoutLease
from mimir_cultivate.worklink.claims import ChainlinkClaims, ClaimRecord
from mimir_cultivate.worklink.compute import ComputeResult
from mimir_cultivate.worklink.evidence import EvidenceValidation, WorklinkEvidence, validate_evidence


A, B, PR = "a" * 40, "b" * 40, "https://github.com/acme/widget/pull/1"


class Forge:
    def __init__(self, heads: dict[str, list[dict]], *, change_head: bool = False, ready_at: int | None = None):
        self.heads = heads
        self.head = A
        self.current = {sha: 0 for sha in heads}
        self.body = "Closes chainlink-2050.\n\nWorklink evidence:\n- Gate mode: `ci`\n"
        self.ready = False
        self.ready_calls = 0
        self.reruns = 0
        self.change_head = change_head
        self.reads = 0
        self.ready_at = ready_at
        self.state = "OPEN"
        self.actor = "reviewer"

    def __call__(self, args, **kw):
        result: object = ""
        if args[:3] == ["gh", "pr", "view"]:
            if "headRefOid" in args[-1]:
                self.reads += 1
                if self.ready_at is not None and self.reads >= self.ready_at:
                    self.ready = True
                if self.change_head and self.reads >= 2:
                    self.head = B
                result = {"headRefOid": self.head, "state": self.state, "isDraft": not self.ready}
            else:
                result = {"body": self.body}
        elif args[:2] == ["gh", "api"]:
            path = args[-1]
            if "check-runs?" in path:
                sha = path.split("/commits/")[1].split("/")[0]
                rows = self.heads[sha]
                index = min(self.current[sha], len(rows) - 1)
                check = rows[index]
                # A pair of reads (check-runs and statuses) constitutes one poll.
                result = [{"total_count": 1, "check_runs": [check]}]
            elif "/timeline?" in path:
                result = [[{"event": "ready_for_review", "actor": {"login": self.actor}}]]
            elif "/statuses?" in path:
                sha = path.split("/commits/")[1].split("/")[0]
                self.current[sha] += 1
                result = [[]]
            elif path == "repos/acme/widget/actions/jobs/34":
                rows = self.heads[self.head]
                row = rows[min(max(self.current[self.head] - 1, 0), len(rows) - 1)]
                result = {"id": 34, "run_id": 56, "head_sha": self.head,
                          "status": "completed", "conclusion": row["conclusion"]}
            elif path == "repos/acme/widget/actions/runs/56":
                result = {"id": 56, "head_sha": self.head,
                          "repository": {"full_name": "acme/widget"},
                          "head_repository": {"full_name": "acme/widget"}}
            elif path == "repos/acme/widget/actions/jobs/34/rerun":
                assert args[:5] == ["gh", "api", "-X", "POST", path]
                self.reruns += 1
                assert self.reruns <= 1, "infrastructure rerun cap exceeded"
                result = {}
            else:
                raise AssertionError(f"Unexpected forge API request: {args}")
        elif args[:3] == ["gh", "pr", "edit"]:
            self.body = args[-1]
        elif args[:3] == ["gh", "pr", "ready"]:
            self.ready_calls += 1
            self.ready = "--undo" not in args
        elif args[:3] == ["gh", "auth", "token"]:
            result = "test-token"
        elif args[:2] == ["git", "-C"]:
            if "status" in args:
                result = ""
            elif "diff" in args:
                result = ""
            elif "remote.origin.url" in args:
                result = "https://github.com/acme/widget.git"
        return subprocess.CompletedProcess(args, 0, json.dumps(result) if isinstance(result, (dict, list)) else result, "")


def check(conclusion: str, *, output: str = "") -> dict:
    return {"id": 12, "name": "candidate (linux)", "status": "completed", "conclusion": conclusion,
            "output": {"summary": output}, "head_sha": A,
            "details_url": "https://github.com/acme/widget/actions/runs/56/job/34"}


@pytest.fixture(autouse=True)
def fake_job_logs(monkeypatch):
    # Never contact GitHub from a test. The check summary is deliberately unrelated.
    monkeypatch.setattr(ci_gate, "capture_job_log", lambda *a, **kw: (b"test failed in job log", ""))


@pytest.mark.parametrize("scenario", ["green", "repair", "exhausted", "infrastructure", "registry", "timeout", "moved", "stopped", "quick_failure", "local", "trusted_handoff", "trusted_repair", "untrusted_handoff", "closed", "merged"])
@pytest.mark.asyncio
async def test_ci_publication_scenarios(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str) -> None:
    started = datetime.now(UTC)
    if scenario == "infrastructure":
        heads = {A: [check("startup_failure"), check("startup_failure")]}
    elif scenario == "registry":
        heads = {A: [check("failure"), check("failure")]}
        monkeypatch.setattr(ci_gate, "capture_job_log", lambda *a, **kw: (b"toomanyrequests", ""))
    elif scenario in ("green", "trusted_handoff", "untrusted_handoff", "closed", "merged"):
        heads = {A: [check("success")]}
    elif scenario in ("repair", "exhausted", "trusted_repair"):
        heads = {A: [check("failure", output="test failed"), check("failure")], B: [check("success" if scenario != "exhausted" else "failure")]}
    elif scenario == "timeout":
        heads = {A: [{**check("failure"), "status": "in_progress", "conclusion": None}]}
    else:
        heads = {A: [check("success")], B: [check("success")]}
    forge = Forge(heads, change_head=scenario == "moved", ready_at=2 if scenario in ("trusted_handoff", "trusted_repair", "untrusted_handoff") else None)
    if scenario in ("closed", "merged"):
        forge.state = "CLOSED" if scenario == "closed" else "MERGED"
    if scenario in ("trusted_handoff", "trusted_repair", "untrusted_handoff"):
        monkeypatch.setattr(ci_gate, "_github_author_is_trusted", lambda repo, actor, token: scenario != "untrusted_handoff")
    claim = ClaimRecord(2050, 1, "agent", started)
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path, runner=forge)
    worker = orchestrator.WorklinkRunner(home=tmp_path, repo=tmp_path)
    issue = orchestrator.IssueContext(2050, "Fix", "body", set())
    lease = CheckoutLease(2050, 1, tmp_path, tmp_path, "issue/2050-a1", "main")
    opened: list[bool] = []
    pushed: list[str] = []
    repair_prompts: list[str] = []

    async def interpret_result(order, result):
        return RawResult(0, None, "success", None)

    @dataclass(frozen=True)
    class Spec:
        repo_url: str = "https://github.com/acme/widget.git"
        base_ref: str = "main"
        timeout_s: int = 5
        backend_config: dict | None = None
        factory_credential_settings: tuple[tuple[str, str], ...] = ()
        builder_lease_credit: bool = False

    class Backend:
        name = "opencode"
        interpret = staticmethod(interpret_result)

        def work_spec(self, order, **kwargs):
            assert order.checkout == lease.path
            assert kwargs["attempt"] == 1 and kwargs["branch"] == lease.branch
            repair_prompts.append(order.prompt)
            return Spec(backend_config={"test_env": {}})

    class Compute:
        async def launch(self, spec):
            # CI repair builders receive the same queue-time credit as initial builds.
            assert spec.builder_lease_credit is True
            return LaunchHandle("local_subprocess", "42")

        async def wait(self, handle, timeout):
            return ComputeResult(0, "", "")

        async def cleanup(self, handle):
            pass

    async def observe_evidence(**kwargs):
        evidence = WorklinkEvidence(
            issue=2050, attempt=1, backend="opencode", branch=lease.branch, checkout=str(tmp_path),
            started_at=started.isoformat(), finished_at=started.isoformat(), files_changed=["fix.py"],
            diff_stat="1 file", commands=[], tests=None, pr_url=None, status="completed",
            gate_mode=kwargs["gate_mode"], diff_observed=True,
        )
        if scenario == "quick_failure":
            from mimir_cultivate.worklink.evidence import TestResult
            evidence = replace(evidence, tests=TestResult("false", exit_code=1))
        elif scenario == "local":
            from mimir_cultivate.worklink.evidence import TestResult
            evidence = replace(evidence, tests=TestResult("true", exit_code=0))
        return validate_evidence(evidence)

    monkeypatch.setattr(orchestrator, "observe_evidence", observe_evidence)
    monkeypatch.setattr(orchestrator, "_with_outside_checkout_detection", lambda value, **kw: value)
    monkeypatch.setattr(orchestrator, "_commit_checkout_changes", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_ensure_clean_checkout", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_git_push", lambda *a, **kw: pushed.append(forge.head))
    monkeypatch.setattr(orchestrator, "_with_head_sha", lambda value, *a, **kw: replace(value, evidence=replace(value.evidence, head_sha=B if len(pushed) > 1 else A)))
    monkeypatch.setattr(orchestrator, "_open_pr", lambda *a, **kw: opened.append(kw.get("draft", False)) or PR)
    monkeypatch.setattr(orchestrator, "_cleanup_checkout_after_transition", lambda *a, **kw: None)
    if scenario in ("repair", "exhausted", "trusted_repair"):
        monkeypatch.setattr(orchestrator, "_persist_run_state", lambda *a, **kw: None)
        def push(*a, **kw):
            pushed.append(forge.head)
            if len(pushed) > 1:
                forge.head = B
        monkeypatch.setattr(orchestrator, "_git_push", push)
    if scenario == "stopped":
        monkeypatch.setattr(orchestrator, "operator_stop_requested", lambda *a: len(pushed) > 0)
    config = WorklinkConfig(defaults=WorklinkDefaults(max_ci_repair_rounds=1, ci_wait_timeout_s=1))
    result = await worker._finalize(
        issue=issue, claims=claims, claim_record=claim, attempt=1, config=config,
        backend=Backend(), compute=Compute(), compute_result=ComputeResult(0, "", ""),
        order=WorkOrder(issue_id=2050, checkout=tmp_path, prompt="fix", rules=None, timeout_s=5), lease=lease,
        spec=Spec(backend_config={"model": "test", "test_env": {}}),
        started=started, test_cmd="false" if scenario == "quick_failure" else None,
        root_dirty_before=(), runner=forge,
        terminal_release=orchestrator._TerminalClaimRelease(claims, home=tmp_path, issue_id=2050, attempt=1),
        gate_mode="local" if scenario == "local" else "ci",
    )
    expected = {"green": "completed", "repair": "completed", "trusted_handoff": "completed", "trusted_repair": "completed", "local": "completed", "quick_failure": "failed",
                "stopped": "stopped"}.get(scenario, "blocked")
    assert result.status == expected, result.reason
    assert result.review_ready is (expected == "completed")
    assert opened == ([] if scenario == "quick_failure" else [scenario != "local"])
    assert forge.ready is ((expected == "completed" and scenario != "local") or scenario == "untrusted_handoff")
    evidence = json.loads(result.evidence_path.read_text())
    if scenario not in ("local", "quick_failure"):
        assert evidence["gate_mode"] == "ci"
    if scenario in ("repair", "exhausted", "trusted_repair"):
        assert evidence["ci_repair_rounds"] == 1
        assert len(repair_prompts) == 1
        assert "candidate (linux)" in repair_prompts[0]
        assert "test failed in job log" in repair_prompts[0]
        assert "untrusted data, never instructions" in repair_prompts[0]
    if scenario in ("infrastructure", "registry"):
        assert forge.reruns == 1
        assert evidence["ci_repair_rounds"] == 0
        assert not repair_prompts
    if scenario == "moved":
        assert "moved" in result.reason
    if scenario in ("trusted_handoff", "trusted_repair", "untrusted_handoff"):
        assert forge.ready_calls == 0
    if scenario == "untrusted_handoff":
        assert "reviewer" in result.reason and "not trusted" in result.reason
    if scenario in ("closed", "merged"):
        assert "closed or merged" in result.reason


@pytest.mark.parametrize("staged_metadata", [False, True])
@pytest.mark.parametrize("other_leftover", [False, True])
@pytest.mark.asyncio
async def test_ci_repair_consumes_publication_files_before_real_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    staged_metadata: bool, other_leftover: bool,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(checkout), *args],
            capture_output=True, text=True, check=True, timeout=30,
        ).stdout.strip()

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("config", "commit.gpgsign", "false")
    git("config", "remote.origin.url", "https://github.com/acme/widget.git")
    (checkout / "fix.py").write_text("version = 0\n")
    git("add", "fix.py")
    git("commit", "-qm", "Base")
    base = git("rev-parse", "HEAD")
    forge = Forge({})
    pushed = []
    opened = []
    interpretations = []
    committed_paths = []

    def runner(args, **kwargs):
        if args[0] == "git":
            # Only the forge is fake: staging, commit and cleanliness use Git.
            if args[3] == "push":
                forge.head = git("rev-parse", "HEAD")
                pushed.append(forge.head)
                return subprocess.CompletedProcess(args, 0, "", "")
            result = subprocess.run(
                args, capture_output=True, text=kwargs.get("text", True), timeout=30,
            )
            if args[3] == "commit" and result.returncode == 0:
                committed_paths.append(git("show", "--format=", "--name-only", "HEAD").splitlines())
                if other_leftover and len(committed_paths) == 2:
                    # Model a post-commit hook/byproduct: not part of the
                    # selected repair diff, and never eligible for exclusion.
                    (checkout / "unexpected.txt").write_text("must block publication\n")
            return result
        if args[:3] == ["gh", "pr", "create"]:
            opened.append(args)
            forge.body = args[args.index("--body") + 1]
            return subprocess.CompletedProcess(args, 0, PR + "\n", "")
        return forge(args, **kwargs)

    @dataclass(frozen=True)
    class Spec:
        repo_url: str = "https://github.com/acme/widget.git"
        base_ref: str = "main"
        timeout_s: int = 5
        backend_config: dict | None = None
        factory_credential_settings: tuple[tuple[str, str], ...] = ()
        builder_lease_credit: bool = False

    class Backend:
        name = "opencode"

        def work_spec(self, order, **kwargs):
            return Spec(backend_config={"test_env": {}})

        async def interpret(self, order, result):
            interpretations.append(order)
            version = len(interpretations)
            (checkout / "fix.py").write_text(f"version = {version}\n")
            (checkout / ".worklink-commit-subject").write_text(
                "Original subject\n" if version == 1 else "Repair supplied subject\n"
            )
            (checkout / ".worklink-pr-body.md").write_text(
                "Original build summary" if version == 1 else "Repair supplied summary"
            )
            if staged_metadata:
                git("add", ".worklink-commit-subject", ".worklink-pr-body.md")
            return RawResult(0, None, "success", None)

    class Compute:
        async def launch(self, spec):
            return LaunchHandle("local_subprocess", "42")

        async def wait(self, handle, timeout):
            return ComputeResult(0, "", "")

        async def cleanup(self, handle):
            pass

    started = datetime.now(UTC)
    claim = ClaimRecord(2050, 1, "agent", started)
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path, runner=runner)
    lease = CheckoutLease(2050, 1, checkout, checkout, "issue/2050-a1", base)

    async def observe(**kwargs):
        return validate_evidence(WorklinkEvidence(
            issue=2050, attempt=1, backend="opencode", branch=lease.branch, checkout=str(checkout),
            started_at=started.isoformat(), finished_at=started.isoformat(), files_changed=["fix.py"],
            diff_stat="1 file", commands=[], tests=None, pr_url=None, status="completed",
            gate_mode="ci", diff_observed=True,
        ))

    async def wait_for_ci(*args, **kwargs):
        # Exercise consumption before *each* repair, not just the first one.
        return ci_gate.CIResult(
            "red" if len(pushed) < 3 else "green", "test failed",
            checks=(("tests", "failure" if len(pushed) < 3 else "success"),),
        )

    monkeypatch.setattr(orchestrator, "observe_evidence", observe)
    monkeypatch.setattr(orchestrator, "wait_for_ci", wait_for_ci)
    monkeypatch.setattr(orchestrator, "_with_outside_checkout_detection", lambda value, **kw: value)
    monkeypatch.setattr(orchestrator, "_persist_run_state", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_cleanup_checkout_after_transition", lambda *a, **kw: None)
    config = WorklinkConfig(defaults=WorklinkDefaults(max_ci_repair_rounds=2))
    result = await orchestrator.WorklinkRunner(home=tmp_path, repo=checkout)._finalize(
        issue=orchestrator.IssueContext(2050, "Fallback title", "body", set()),
        claims=claims, claim_record=claim, attempt=1, config=config,
        backend=Backend(), compute=Compute(), compute_result=ComputeResult(0, "", ""),
        order=WorkOrder(issue_id=2050, checkout=checkout, prompt="fix", rules=None, timeout_s=5),
        lease=lease, spec=Spec(backend_config={"test_env": {}}), started=started,
        test_cmd=None, root_dirty_before=(), runner=runner,
        terminal_release=orchestrator._TerminalClaimRelease(claims, home=tmp_path, issue_id=2050, attempt=1),
        gate_mode="ci",
    )
    assert result.status == ("blocked" if other_leftover else "completed"), result.reason
    assert result.review_ready is (not other_leftover)
    assert forge.ready is (not other_leftover)
    assert len(pushed) == (1 if other_leftover else 3)
    assert len(interpretations) == (2 if other_leftover else 3)
    assert committed_paths == [["fix.py"]] * len(interpretations)
    assert git("log", f"{base}..HEAD", "--format=%s").splitlines() == [
        "Original subject"
    ] * len(interpretations)
    for name in (".worklink-commit-subject", ".worklink-pr-body.md"):
        assert not (checkout / name).exists()
        assert not git("log", "--format=%H", "--all", "--", name)
    assert len(opened) == 1 and "--draft" in opened[0]
    assert opened[0][opened[0].index("--title") + 1] == "Original subject"
    assert "Build summary:\n\nOriginal build summary" in forge.body
    assert "Repair supplied" not in forge.body
    if other_leftover:
        assert result.reason == (
            "CI repair publication failed: checkout still dirty after Worklink commit"
        )
        assert git("status", "--porcelain=v1", "--untracked-files=all") == "?? unexpected.txt"
    else:
        assert not git("status", "--porcelain=v1", "--untracked-files=all")
        assert json.loads(result.evidence_path.read_text())["ci_repair_rounds"] == 2


@pytest.mark.parametrize("kind", ["symlink", "fifo", "oversized"])
@pytest.mark.parametrize("name", [".worklink-commit-subject", ".worklink-pr-body.md"])
def test_repair_publication_readers_are_bounded_regular_and_no_follow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, kind: str,
) -> None:
    is_subject = name == ".worklink-commit-subject"
    limit = (
        orchestrator._COMMIT_SUBJECT_MAX_BYTES if is_subject
        else orchestrator._PR_BODY_SECTION_MAX_BYTES
    )
    path = tmp_path / name
    target = tmp_path / "target"
    target.write_text("Must not follow this target")
    if kind == "symlink":
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(b"x" * (limit * 2))
    read = os.read
    reads = []

    def bounded_regular_read(fd, count):
        assert stat.S_ISREG(os.fstat(fd).st_mode), "non-regular publication file was read"
        assert count <= limit + 1, "publication read exceeded its bound"
        reads.append(count)
        return read(fd, count)

    monkeypatch.setattr(os, "read", bounded_regular_read)
    value = (
        orchestrator._read_commit_subject(tmp_path, "Original subject")
        if is_subject else orchestrator._read_pr_body_section(tmp_path)
    )
    if kind == "oversized":
        assert reads == [limit + 1]
        if is_subject:
            assert value == "Original subject"
        else:
            assert len(value.encode("utf-8")) <= limit
            assert value.endswith("[Build summary truncated by Worklink.]")
    else:
        assert reads == []
        assert value == ("Original subject" if is_subject else None)
    assert not path.exists() and not path.is_symlink()
    assert target.read_text() == "Must not follow this target"


def test_ci_gate_inventory_and_defaults(tmp_path: Path) -> None:
    inventory = tmp_path / "repositories.yaml"
    row = {"slug": "acme/widget", "root": str(tmp_path), "origin": "https://github.com/acme/widget.git",
           "base_branch": "main", "mode": "rw"}
    import yaml
    inventory.write_text(yaml.safe_dump({"repositories": [row]}))
    assert RepositoryInventory.load(inventory).repositories[0].worklink_gate == "local"
    for mode, command in (("ci", None), ("ci", "ruff check ."), ("local", None)):
        inventory.write_text(yaml.safe_dump({"repositories": [{**row, "worklink_gate": mode, "quick_gate_command": command}]}))
        assert RepositoryInventory.load(inventory).repositories[0].quick_gate_command == command
    for bad in (-1, True, 1.5, "3"):
        with pytest.raises(ValueError, match="max_ci_repair_rounds"):
            WorklinkDefaults(max_ci_repair_rounds=bad)
    config_path = tmp_path / "worklink.yaml"
    config_path.write_text("defaults:\n  max_ci_repair_rounds: 0\n  ci_wait_timeout_s: 90\n")
    defaults = WorklinkConfig.load(config_path).defaults
    assert (defaults.max_ci_repair_rounds, defaults.ci_wait_timeout_s) == (0, 90)


@pytest.mark.asyncio
async def test_head_bound_and_rerun_cap(tmp_path: Path) -> None:
    forge = Forge({A: [check("success")], B: [check("failure")]}, change_head=True)
    result = await ci_gate.wait_for_ci(forge, "acme/widget", PR, A, timeout_s=1, stopped=lambda: False, poll_s=0)
    assert result.outcome == "moved"


@pytest.mark.asyncio
async def test_first_ci_read_refuses_a_temporarily_moved_head() -> None:
    class RevertedForge(Forge):
        def __call__(self, args, **kwargs):
            if args[:2] == ["gh", "api"] and "/statuses?" in args[-1]:
                self.head = A
            return super().__call__(args, **kwargs)

    forge = RevertedForge({A: [check("success")], B: [check("failure")]})
    forge.head = B
    result = await ci_gate.wait_for_ci(forge, "acme/widget", PR, A, timeout_s=1, stopped=lambda: False)
    assert result.outcome == "moved"


@pytest.mark.asyncio
async def test_absent_ci_checks_never_count_as_green() -> None:
    forge = Forge({A: [check("success")]})

    def empty_checks(args, **kwargs):
        if args[:2] == ["gh", "api"] and "check-runs?" in args[-1]:
            return subprocess.CompletedProcess(args, 0, json.dumps([{"total_count": 0, "check_runs": []}]), "")
        return forge(args, **kwargs)

    result = await ci_gate.wait_for_ci(empty_checks, "acme/widget", PR, A,
        timeout_s=0.01, stopped=lambda: False, poll_s=0)
    assert result.outcome == "timeout"


@pytest.mark.asyncio
async def test_ready_refuses_moved_head() -> None:
    forge = Forge({A: [check("success")], B: [check("failure")]}, change_head=True)
    forge.reads = 1
    with pytest.raises(RuntimeError, match="head moved"):
        await ci_gate.ready_pr(forge, PR, A)
    assert not forge.ready
    assert forge.ready_calls == 0


@pytest.mark.asyncio
async def test_rerun_refuses_moved_head() -> None:
    forge = Forge({A: [check("startup_failure")], B: [check("failure")]}, change_head=True)
    forge.reads = 1
    assert not await ci_gate.rerun_infrastructure(forge, "acme/widget", PR, A, (34,))
    assert forge.reruns == 0


@pytest.mark.asyncio
async def test_ready_does_not_accept_changed_head_after_transition() -> None:
    forge = Forge({A: [check("success")], B: [check("failure")]}, change_head=True)
    with pytest.raises(RuntimeError, match="head moved"):
        await ci_gate.ready_pr(forge, PR, A)
    assert forge.ready  # Do not undo a ready transition on a head we no longer own.


@pytest.mark.parametrize(("events", "trust", "reason"), [
    ([{"event": "ready_for_review", "actor": {}}], True, "could not be established"),
    ([{"event": "ready_for_review", "actor": {"login": "reviewer"}},
                  {"event": "convert_to_draft", "actor": {"login": "reviewer"}}], True, "could not be established"),
    ([{"event": "ready_for_review", "actor": {"login": "reviewer"}}], None, "attestation unavailable"),
])
@pytest.mark.asyncio
async def test_ready_actor_requires_current_attested_transition(
    monkeypatch: pytest.MonkeyPatch, events: list[dict], trust: bool | None, reason: str,
) -> None:
    forge = Forge({A: [check("success")]}, ready_at=1)
    monkeypatch.setattr(ci_gate, "_github_author_is_trusted", lambda repo, login, token: trust)

    def run(args, **kwargs):
        if args[:2] == ["gh", "api"] and "/timeline?" in args[-1]:
            return subprocess.CompletedProcess(args, 0, json.dumps([events]), "")
        return forge(args, **kwargs)

    result = await ci_gate.wait_for_ci(run, "acme/widget", PR, A, timeout_s=1, stopped=lambda: False)
    assert result.outcome == "error" and reason in result.reason
    assert forge.ready_calls == 0


@pytest.mark.asyncio
async def test_ready_head_change_during_attestation_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    forge = Forge({A: [check("success")], B: [check("success")]}, ready_at=1)
    monkeypatch.setattr(ci_gate, "_github_author_is_trusted", lambda repo, login, token: True)

    def run(args, **kwargs):
        if args[:2] == ["gh", "api"] and "/timeline?" in args[-1]:
            forge.head = B
        return forge(args, **kwargs)

    result = await ci_gate.wait_for_ci(run, "acme/widget", PR, A, timeout_s=1, stopped=lambda: False)
    assert result.outcome == "error" and "head moved" in result.reason


@pytest.mark.asyncio
async def test_ci_watch_refuses_pr_from_other_repository() -> None:
    forge = Forge({A: [check("success")]})
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", "https://github.com/other/widget/pull/1", A,
        timeout_s=1, stopped=lambda: False,
    )
    assert result.outcome == "error" and "repository does not match" in result.reason
    assert forge.reads == 0


@pytest.mark.parametrize("verified", [True, False])
def test_stop_during_ci_wait(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, verified: bool) -> None:
    # Portable controller-flow coverage; do not weaken the production PID guard.
    monkeypatch.setattr(control, "process_identity_verified", lambda state: verified)
    from mimir_cultivate.worklink.run_state import WorklinkRunState, save_run_state
    import os
    from mimir_cultivate.worklink.run_state import process_start_ticks

    state = WorklinkRunState(2050, 1, "opencode", "local_subprocess", "controller", str(os.getpid()),
        "issue/2050-a1", "main", "main", str(tmp_path), "https://github.com/acme/widget.git",
        None, datetime.now(UTC).isoformat(), phase="ci_wait", process_start_ticks=process_start_ticks(os.getpid()))
    save_run_state(tmp_path, state)
    commands = []

    def run(args):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    result = control.stop_worklink(tmp_path, 2050, runner=run)
    assert result.stopped is verified
    assert control.operator_stop_requested(tmp_path, 2050, 1) is verified
    if not verified:
        assert result.reason == "CI waiter identity could not be verified"
    assert not commands  # controller owns the terminal transition and claim release


def test_stop_ci_wait_real_process_identity(tmp_path: Path) -> None:
    import os
    from mimir_cultivate.worklink.run_state import WorklinkRunState, save_run_state, process_start_ticks

    ticks = process_start_ticks(os.getpid())
    if ticks is None:
        pytest.skip("process start ticks unavailable on this platform")
    state = WorklinkRunState(2050, 1, "opencode", "local_subprocess", "controller", str(os.getpid()),
        "issue/2050-a1", "main", "main", str(tmp_path), "https://github.com/acme/widget.git",
        None, datetime.now(UTC).isoformat(), phase="ci_wait", process_start_ticks=ticks)
    save_run_state(tmp_path, state)
    result = control.stop_worklink(tmp_path, 2050, runner=lambda args: pytest.fail("controller must not be signalled"))
    assert result.stopped
    assert control.operator_stop_requested(tmp_path, 2050, 1)


def test_ci_body_update_removes_legacy_internal_references() -> None:
    forge = Forge({A: [check("success")]})
    forge.body += "- Branch: `issue/2050-a1`\n- Transcript run: `2050-1 (gate-77)`\n"
    checks = tuple((name, "success") for name in ("chainlink-cli-audit", "lint (no std)", "test #12"))
    orchestrator._update_ci_pr_body(forge, PR, checks, 2, A)
    assert "chainlink" not in forge.body.lower() and "2050-1" not in forge.body
    assert "issue/2050-a1" not in forge.body
    assert f"3/3 checks green on {A}" in forge.body
    assert "CI repair rounds: 2" in forge.body
    assert all(name not in forge.body for name, _ in checks)
    orchestrator._update_ci_pr_body(forge, PR, checks, 3, A)
    assert forge.body.count("- CI:") == 1


@pytest.mark.asyncio
async def test_registry_failure_in_real_log_uses_actions_rerun(monkeypatch) -> None:
    forge = Forge({A: [check("failure")]})
    calls = []

    def capture(repo, job_id, **kwargs):
        calls.append((repo, job_id, kwargs))
        return b"\x1b[31mtoomanyrequests: registry rate limit\x1b[0m", ""

    monkeypatch.setattr(ci_gate, "capture_job_log", capture)
    result = ci_gate._snapshot(forge, "acme/widget", PR, A)
    assert result.outcome == "infrastructure"
    assert result.rerun_ids == (34,)  # Actions job ID, NOT check-run ID 12
    assert "toomanyrequests" in result.excerpt
    assert "\\u001b" not in result.excerpt
    assert calls[0][:2] == ("acme/widget", 34)
    assert calls[0][2]["limit"] == 2048
    assert await ci_gate.rerun_infrastructure(forge, "acme/widget", PR, A, result.rerun_ids)
    assert forge.reruns == 1


@pytest.mark.parametrize("mismatch", ["url", "check_head", "job_head", "run_head", "repository", "head_repository", "job_id", "run_id"])
def test_log_capture_refuses_unbound_jobs(monkeypatch, mismatch) -> None:
    row = check("failure")
    if mismatch == "url":
        row["details_url"] = "https://github.com/outsider/repo/actions/runs/56/job/34"
    elif mismatch == "check_head":
        row["head_sha"] = B
    forge = Forge({A: [row]})
    captured = []
    monkeypatch.setattr(ci_gate, "capture_job_log", lambda *a, **kw: captured.append(a) or (b"toomanyrequests", ""))

    def run(args, **kwargs):
        result = forge(args, **kwargs)
        if args[-1] == "repos/acme/widget/actions/jobs/34":
            data = json.loads(result.stdout)
            if mismatch == "job_head": data["head_sha"] = B
            if mismatch == "job_id": data["id"] = 999
            if mismatch == "run_id": data["run_id"] = 999
            result.stdout = json.dumps(data)
        elif args[-1] == "repos/acme/widget/actions/runs/56":
            data = json.loads(result.stdout)
            if mismatch == "run_head": data["head_sha"] = B
            if mismatch in ("repository", "head_repository"): data[mismatch]["full_name"] = "outsider/repo"
            result.stdout = json.dumps(data)
        return result

    result = ci_gate._snapshot(run, "acme/widget", PR, A)
    assert result.outcome == "red"
    assert not captured
    assert "limitation" in result.excerpt


def test_log_failure_has_explicit_limitation(monkeypatch) -> None:
    monkeypatch.setattr(ci_gate, "capture_job_log", lambda *a, **kw: (b"", "HTTP 403"))
    result = ci_gate._snapshot(Forge({A: [check("failure")]}), "acme/widget", PR, A)
    assert result.outcome == "red"
    assert "HTTP 403" in result.excerpt


class ReadFailureForge(Forge):
    """Inject read failures, then recover, without counting partial reads as polls."""

    def __init__(self, failures, *, rows=None):
        super().__init__({A: rows or [check("success")]})
        self.failures = iter(failures)
        self.polls = 0
        self.comments = []

    def __call__(self, args, **kwargs):
        if args[:2] == ["gh", "api"] and "check-runs?" in args[-1]:
            self.polls += 1
            failure = next(self.failures, None)
            if isinstance(failure, Exception):
                raise failure
            if failure is not None:
                return subprocess.CompletedProcess(args, 1, "", failure)
        if args[1:3] == ["issue", "comment"]:
            self.comments.append(args[-1])
        return super().__call__(args, **kwargs)


@pytest.mark.parametrize("failure", [
    "error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com",
    "Could not resolve host: api.github.com",
    "read: connection reset by peer",
    "TLS handshake timed out",
    "gh: server error (HTTP 500)",
    "HTTP/2 599",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    subprocess.TimeoutExpired(["gh", "api"], 30),
])
@pytest.mark.asyncio
async def test_transient_forge_read_recovers_to_green(failure) -> None:
    forge = ReadFailureForge([failure])
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, A, timeout_s=60, stopped=lambda: False, poll_s=0,
    )
    assert result.outcome == "green"
    assert forge.polls == 2
    await ci_gate.ready_pr(forge, PR, A)
    assert forge.ready and forge.ready_calls == 1


@pytest.mark.parametrize("failure", [
    "gh: Resource not accessible by integration (HTTP 403)",
    "gh: Bad credentials (HTTP 401)",
    "gh: Not Found (HTTP 404)",
    "gh: Forbidden (HTTP 403); upstream connection timed out (503)",
    "gh: Not Found; error connecting to api.github.com",
    "gh: unknown API failure",
    RuntimeError("PR head moved; connection timed out"),
    ValueError("invalid JSON: 502"),
])
@pytest.mark.asyncio
async def test_permanent_forge_read_fails_immediately(failure) -> None:
    forge = ReadFailureForge([failure])
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, A, timeout_s=60, stopped=lambda: False, poll_s=0,
    )
    assert result.outcome == "error"
    assert forge.polls == 1
    assert "CI was not observed" in result.reason
    assert PR in result.reason and A in result.reason
    assert forge.ready_calls == 0


@pytest.mark.asyncio
async def test_invalid_forge_json_is_not_retried() -> None:
    reads = []

    def invalid_json(args, **kwargs):
        reads.append(args)
        return subprocess.CompletedProcess(args, 0, "502 Bad Gateway", "")

    result = await ci_gate.wait_for_ci(
        invalid_json, "acme/widget", PR, A, timeout_s=60, stopped=lambda: False, poll_s=0,
    )
    assert result.outcome == "error"
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_consecutive_transient_forge_failures_are_bounded() -> None:
    # Recover on poll 11: deleting the bound must fail, not hang this test.
    forge = ReadFailureForge(["Could not resolve host: api.github.com"] * 10)
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, A, timeout_s=60, stopped=lambda: False, poll_s=0,
    )
    assert result.outcome == "error"
    assert forge.polls == 10
    assert "10 consecutive transient forge read failures" in result.reason
    assert "CI was not observed" in result.reason
    assert PR in result.reason and A in result.reason
    assert forge.ready_calls == 0


@pytest.mark.asyncio
async def test_successful_pending_poll_resets_transient_failure_count() -> None:
    pending = {**check("success"), "status": "in_progress", "conclusion": None}
    forge = ReadFailureForge(
        ["HTTP 503"] * 9 + [None] + ["HTTP 503"] * 9 + [None],
        rows=[pending, check("success")],
    )
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, A, timeout_s=60, stopped=lambda: False, poll_s=0,
    )
    assert result.outcome == "green"
    assert forge.polls == 20


@pytest.mark.parametrize("stop", [False, True])
@pytest.mark.asyncio
async def test_transient_forge_failure_respects_deadline_and_stop(monkeypatch, stop) -> None:
    class Clock:
        now = 0

        def monotonic(self):
            return self.now

        async def sleep(self, delay):
            sleeps.append(delay)
            self.now += delay

    clock = Clock()
    sleeps = []
    monkeypatch.setattr(ci_gate, "time", clock)
    monkeypatch.setattr(ci_gate.asyncio, "sleep", clock.sleep)
    forge = ReadFailureForge(["HTTP 503"] * 10)
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, A, timeout_s=25,
        stopped=lambda: stop and bool(sleeps), poll_s=10,
    )
    assert result.outcome == ("stopped" if stop else "timeout")
    assert sleeps == ([10] if stop else [10, 10, 5])
    assert forge.polls < 10
    if not stop:
        assert "CI was not observed" in result.reason
        assert PR in result.reason and A in result.reason


@pytest.mark.parametrize("recover", [True, False])
@pytest.mark.asyncio
async def test_forge_read_failure_publication_and_comment(tmp_path, monkeypatch, recover) -> None:
    started = datetime.now(UTC)
    forge = ReadFailureForge(["Could not resolve host: api.github.com"] * (1 if recover else 10))
    claim = ClaimRecord(2050, 1, "agent", started)
    claims = ChainlinkClaims(agent_id="agent", home_path=tmp_path, runner=forge)
    lease = CheckoutLease(2050, 1, tmp_path, tmp_path, "issue/2050-a1", "main")
    worker = orchestrator.WorklinkRunner(home=tmp_path, repo=tmp_path)

    @dataclass(frozen=True)
    class Spec:
        repo_url: str = "https://github.com/acme/widget.git"
        base_ref: str = "main"
        timeout_s: int = 5
        backend_config: dict | None = None

    class Backend:
        name = "opencode"

        async def interpret(self, order, result):
            return RawResult(0, None, "success", None)

    async def observe_evidence(**kwargs):
        return validate_evidence(WorklinkEvidence(
            issue=2050, attempt=1, backend="opencode", branch=lease.branch, checkout=str(tmp_path),
            started_at=started.isoformat(), finished_at=started.isoformat(), files_changed=["fix.py"],
            diff_stat="1 file", commands=[], tests=None, pr_url=None, status="completed",
            gate_mode="ci", diff_observed=True,
        ))

    # Exercise the real wait and finalization, but neither wait ten seconds in
    # tests nor run an actual builder, git publication, or tracker CLI.
    async def wait_without_delay(*args, **kwargs):
        return await ci_gate.wait_for_ci(*args, **kwargs, poll_s=0)

    opened = []
    monkeypatch.setattr(orchestrator, "wait_for_ci", wait_without_delay)
    monkeypatch.setattr(orchestrator, "observe_evidence", observe_evidence)
    monkeypatch.setattr(orchestrator, "_with_outside_checkout_detection", lambda value, **kw: value)
    monkeypatch.setattr(orchestrator, "_commit_checkout_changes", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_ensure_clean_checkout", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_git_push", lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, "_with_head_sha", lambda value, *a, **kw:
        replace(value, evidence=replace(value.evidence, head_sha=A)))
    monkeypatch.setattr(orchestrator, "_open_pr", lambda *a, **kw: opened.append(kw["draft"]) or PR)
    monkeypatch.setattr(orchestrator, "_cleanup_checkout_after_transition", lambda *a, **kw: None)
    result = await worker._finalize(
        issue=orchestrator.IssueContext(2050, "Fix", "body", set()),
        claims=claims, claim_record=claim, attempt=1,
        config=WorklinkConfig(defaults=WorklinkDefaults(ci_wait_timeout_s=60)),
        backend=Backend(), compute=None, compute_result=ComputeResult(0, "", ""),
        order=WorkOrder(issue_id=2050, checkout=tmp_path, prompt="fix", rules=None, timeout_s=5),
        lease=lease, spec=Spec(backend_config={"test_env": {}}), started=started,
        test_cmd=None, root_dirty_before=(), runner=forge, gate_mode="ci",
        terminal_release=orchestrator._TerminalClaimRelease(
            claims, home=tmp_path, issue_id=2050, attempt=1,
        ),
    )
    assert result.status == ("completed" if recover else "blocked"), result.reason
    assert result.review_ready is recover
    assert opened == [True]
    assert forge.ready is recover
    assert forge.ready_calls == (1 if recover else 0)
    evidence = json.loads(result.evidence_path.read_text())
    assert evidence["pr_url"] == PR and evidence["head_sha"] == A
    if not recover:
        assert "CI was not observed" in evidence["blocked_reason"]
        assert PR in evidence["blocked_reason"] and A in evidence["blocked_reason"]
        comment = next(text for text in forge.comments if text.startswith("WORKLINK_EVIDENCE"))
        assert "CI was not observed" in comment
        assert PR in comment and A in comment


@pytest.fixture
def propagation_clock(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    now = [1000.0]

    async def sleep(delay):
        now[0] += delay

    # Replace only the CI gate's clock/sleep, not the event loop's clock.
    monkeypatch.setattr(ci_gate, "time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(ci_gate, "asyncio", SimpleNamespace(to_thread=asyncio.to_thread, sleep=sleep))
    return now


@pytest.mark.parametrize("infrastructure_rerun", [False, True])
@pytest.mark.asyncio
async def test_repair_push_propagates_before_green_and_ready(
    tmp_path, monkeypatch, infrastructure_rerun,
) -> None:
    forges = []

    class LaggingForge(Forge):
        lag_reads = 0

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            forges.append(self)

        def __call__(self, args, **kwargs):
            result = super().__call__(args, **kwargs)
            if args[:3] == ["gh", "pr", "view"] and "headRefOid" in args[-1] and self.head == B:
                if self.lag_reads == 0:
                    data = json.loads(result.stdout)
                    data["headRefOid"] = A
                    result.stdout = json.dumps(data)
                self.lag_reads += 1
            return result

    wait = ci_gate.wait_for_ci
    observed_previous = []

    async def fast_wait(*args, **kwargs):
        observed_previous.append(kwargs.get("previous_sha"))
        result = await wait(*args, **kwargs, poll_s=0)
        if infrastructure_rerun and args[3] == B and kwargs.get("previous_sha") is not None:
            assert result.outcome == "green"
            return ci_gate.CIResult("infrastructure", "registry unavailable", rerun_ids=(34,))
        return result

    monkeypatch.setattr(orchestrator, "wait_for_ci", fast_wait)
    monkeypatch.setitem(globals(), "Forge", LaggingForge)
    # Reuse the full publication/repair fixture and its completed/ready assertions.
    await test_ci_publication_scenarios(tmp_path, monkeypatch, "repair")
    assert observed_previous == ([None, A, None] if infrastructure_rerun else [None, A])
    assert forges[0].lag_reads >= 3
    assert forges[0].ready_calls == 1
    assert forges[0].current[B] == (2 if infrastructure_rerun else 1)
    assert forges[0].reruns == int(infrastructure_rerun)


@pytest.mark.parametrize("head_read", [1, 2, 3])
@pytest.mark.parametrize("observed", [A, "c" * 40])
def test_propagation_head_checks_remain_exact(
    monkeypatch, propagation_clock, head_read, observed,
) -> None:
    forge = Forge({B: [{**check("failure"), "head_sha": B}]})
    forge.head = B

    def run(args, **kwargs):
        result = forge(args, **kwargs)
        if args[:3] == ["gh", "pr", "view"] and forge.reads == head_read:
            data = json.loads(result.stdout)
            data["headRefOid"] = observed
            result.stdout = json.dumps(data)
        return result

    result = ci_gate._snapshot(
        run, "acme/widget", PR, B, previous_sha=A, settle_deadline=1120,
    )
    assert result.outcome == ("pending" if observed == A else "moved")
    assert forge.reads == head_read


@pytest.mark.asyncio
async def test_unknown_head_moves_immediately_during_propagation(propagation_clock) -> None:
    forge = Forge({B: [check("success")]})
    forge.head = "c" * 40
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, B, previous_sha=A,
        timeout_s=300, stopped=lambda: False,
    )
    assert result.outcome == "moved"
    assert forge.reads == 1
    assert propagation_clock[0] == 1000
    assert forge.current[B] == 0


@pytest.mark.asyncio
async def test_previous_head_moves_at_settle_window_bound(propagation_clock) -> None:
    forge = Forge({A: [check("success")], B: [check("success")]})
    result = await ci_gate.wait_for_ci(
        forge, "acme/widget", PR, B, previous_sha=A,
        timeout_s=300, stopped=lambda: False, poll_s=10,
    )
    assert result.outcome == "moved"
    assert propagation_clock[0] == 1120
    assert forge.reads == 13
    assert forge.current == {A: 0, B: 0}
    assert not forge.ready


@pytest.mark.parametrize("head_read", [1, 2, 3])
def test_settle_deadline_is_checked_on_every_head_read(propagation_clock, head_read) -> None:
    forge = Forge({B: [{**check("failure"), "head_sha": B}]})
    forge.head = B

    def run(args, **kwargs):
        result = forge(args, **kwargs)
        if args[:3] == ["gh", "pr", "view"] and forge.reads == head_read:
            propagation_clock[0] = 1120
            data = json.loads(result.stdout)
            data["headRefOid"] = A
            result.stdout = json.dumps(data)
        return result

    result = ci_gate._snapshot(run, "acme/widget", PR, B, previous_sha=A, settle_deadline=1120)
    assert result.outcome == "moved"
    assert forge.reads == head_read


@pytest.mark.asyncio
async def test_foreign_move_before_repair_push_is_not_overwritten(tmp_path, monkeypatch) -> None:
    forges = []

    class MovedBeforePushForge(Forge):
        repair_pushes = 0

        def __setattr__(self, name, value):
            if name == "head" and value == B:
                self.repair_pushes += 1
            super().__setattr__(name, value)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            forges.append(self)

        def __call__(self, args, **kwargs):
            # The initial red snapshot reads three times; the fourth is pre-push.
            if args[:3] == ["gh", "pr", "view"] and "headRefOid" in args[-1] and self.reads == 3:
                self.head = "c" * 40
            return super().__call__(args, **kwargs)

    monkeypatch.setitem(globals(), "Forge", MovedBeforePushForge)
    await test_ci_publication_scenarios(tmp_path, monkeypatch, "exhausted")
    assert forges[0].repair_pushes == 0
    assert forges[0].head == "c" * 40  # The repair push would have set B.
    assert forges[0].current[B] == 0
    assert forges[0].ready_calls == 0


class MixedChecksForge(Forge):
    """Keep macOS queued on the old head, alongside the primary Linux check."""

    def __call__(self, args, **kwargs):
        result = super().__call__(args, **kwargs)
        if args[:2] == ["gh", "api"] and f"/commits/{A}/check-runs?" in args[-1]:
            pages = json.loads(result.stdout)
            pages[0]["check_runs"].append({
                **check("success"), "id": 13, "name": "candidate (macos)",
                "status": "queued", "conclusion": None, "details_url": None,
            })
            pages[0]["total_count"] += 1
            result.stdout = json.dumps(pages)
        return result


@pytest.mark.parametrize("conclusion", ["failure", "timed_out", "startup_failure"])
def test_completed_failure_is_observed_while_other_checks_are_queued(monkeypatch, conclusion) -> None:
    captured = []
    monkeypatch.setattr(ci_gate, "capture_job_log", lambda repo, job_id, **kw:
        captured.append(job_id) or (b"test failed in job log", ""))
    forge = MixedChecksForge({A: [check(conclusion)]})
    result = ci_gate._snapshot(forge, "acme/widget", PR, A)
    assert result.outcome == ("infrastructure" if conclusion == "startup_failure" else "red")
    assert result.rerun_ids == ((34,) if conclusion == "startup_failure" else ())
    assert result.checks == (("candidate (linux)", conclusion), ("candidate (macos)", "queued"))
    assert captured == [34]
    assert "test failed in job log" in result.excerpt
    assert "limitation" not in result.excerpt  # No attempt to fetch the queued job's log.
    assert forge.current[A] == 1


@pytest.mark.parametrize("conclusion", ["success", "neutral", "skipped", "cancelled"])
def test_queued_checks_prevent_green_or_cancelled_repair(monkeypatch, conclusion) -> None:
    monkeypatch.setattr(ci_gate, "capture_job_log", lambda *a, **kw:
        pytest.fail("pending CI must not fetch failure logs"))
    result = ci_gate._snapshot(MixedChecksForge({A: [check(conclusion)]}), "acme/widget", PR, A)
    assert result.outcome == "pending"
    assert not result.excerpt and not result.rerun_ids


@pytest.mark.parametrize("status", ["queued", "in_progress"])
@pytest.mark.parametrize("conclusion", ["failure", "timed_out", "startup_failure"])
def test_unfinished_failure_conclusions_do_not_trigger_early_repair(status, conclusion) -> None:
    forge = MixedChecksForge({A: [{**check(conclusion), "status": status}]})
    result = ci_gate._snapshot(forge, "acme/widget", PR, A)
    assert result.outcome == "pending"
    assert not result.excerpt and not result.rerun_ids


def test_pending_commit_status_prevents_green() -> None:
    forge = Forge({A: [check("success")]})

    def run(args, **kwargs):
        result = forge(args, **kwargs)
        if args[:2] == ["gh", "api"] and "/statuses?" in args[-1]:
            result.stdout = json.dumps([[{"context": "external-ci", "state": "pending"}]])
        return result

    result = ci_gate._snapshot(run, "acme/widget", PR, A)
    assert result.outcome == "pending"


@pytest.mark.parametrize("conclusion", ["success", "neutral", "skipped"])
def test_all_completed_passing_checks_are_green(conclusion) -> None:
    forge = MixedChecksForge({A: [check(conclusion)]})

    def run(args, **kwargs):
        result = forge(args, **kwargs)
        if args[:2] == ["gh", "api"] and "check-runs?" in args[-1]:
            pages = json.loads(result.stdout)
            pages[0]["check_runs"][-1].update(status="completed", conclusion="success")
            result.stdout = json.dumps(pages)
        return result

    result = ci_gate._snapshot(run, "acme/widget", PR, A)
    assert result.outcome == "green"
    assert len(result.checks) == 2


@pytest.mark.parametrize("scenario", ["infrastructure", "registry"])
@pytest.mark.asyncio
async def test_queued_checks_preserve_infrastructure_rerun_once(tmp_path, monkeypatch, scenario) -> None:
    monkeypatch.setitem(globals(), "Forge", MixedChecksForge)
    await test_ci_publication_scenarios(tmp_path, monkeypatch, scenario)


@pytest.mark.asyncio
async def test_early_red_repairs_once_then_waits_for_new_head(tmp_path, monkeypatch) -> None:
    forges = []

    class RepairForge(MixedChecksForge):
        lag_reads = 0

        def __init__(self, heads, **kwargs):
            # The repaired head must also wait for its own pending checks.
            heads[B] = [
                {**check("success"), "head_sha": B, "status": "in_progress", "conclusion": None},
                {**check("success"), "head_sha": B},
            ]
            super().__init__(heads, **kwargs)
            forges.append(self)

        def __call__(self, args, **kwargs):
            result = super().__call__(args, **kwargs)
            if args[:3] == ["gh", "pr", "view"] and "headRefOid" in args[-1] and self.head == B:
                if self.lag_reads == 0:
                    data = json.loads(result.stdout)
                    data["headRefOid"] = A
                    result.stdout = json.dumps(data)
                self.lag_reads += 1
            return result

    observed = []

    async def fast_wait(*args, **kwargs):
        result = await ci_gate.wait_for_ci(*args, **kwargs, poll_s=0)
        observed.append((args[3], kwargs.get("previous_sha"), result.outcome))
        return result

    monkeypatch.setitem(globals(), "Forge", RepairForge)
    monkeypatch.setattr(orchestrator, "wait_for_ci", fast_wait)
    # Reuse the existing real finalization assertions: one repair, failed-job
    # excerpts in its prompt, one ready transition, and successful publication.
    await test_ci_publication_scenarios(tmp_path, monkeypatch, "repair")
    assert observed == [(A, None, "red"), (B, A, "green")]
    assert forges[0].current == {A: 1, B: 2}
    assert forges[0].lag_reads >= 5
