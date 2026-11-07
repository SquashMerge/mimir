"""Cultivate's repository operations must remain independent of core wrappers."""

from __future__ import annotations

import ast
import asyncio
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import subprocess
import threading
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage

from mimir_cultivate import repository
from mimir._context import reset_current_turn, set_current_turn
from mimir_cultivate.policy import RepoPRScopeRegistry, RepoReviewState
from mimir.plugins.manager import load_plugins
from mimir.tools.budget_gate import BudgetGateMiddleware
from tests.test_budget_gate_and_alias import (
    _ifc_turn, _make_request, _repo_granted_auth, _untainted_ifc_auth,
)
from tests.test_repo_tools import (
    _configure_test_suites, _configure_worklink_test, _fixture_git_env,
    _git, _lineage_turn, _repo_scope_and_state, _test_checkout_factory,
)
from tests.lease_attestation_helper import assert_lease_attested


@pytest.mark.parametrize("args, stderr, transport", [
    (["git", "-C", "/x", "fetch", "--no-tags", "origin", "main"],
     "fatal: unable to access 'https://github.com/o/r/': Could not resolve host: github.com", True),
    (["git", "-C", "/x", "fetch", "--no-tags", "origin", "main"],
     "error: RPC failed; HTTP 503 curl 22 The requested URL returned error: 503", True),
    (["git", "-C", "/x", "fetch", "--no-tags", "origin", "main"],
     "fatal: couldn't find remote ref refs/pull/9/head", False),
    (["git", "-C", "/x", "rev-parse", "--verify", "HEAD"],
     "fatal: Could not resolve host: github.com", False),
])
def test_git_fetch_transport_failures_are_typed(args, stderr, transport) -> None:
    from mimir._provider_errors import TransientTransportError

    def runner(_args):
        return subprocess.CompletedProcess(_args, 128, stdout="", stderr=stderr)

    with pytest.raises(RuntimeError) as raised:
        repository._run(runner, args, "fetch failed")
    assert isinstance(raised.value, TransientTransportError) is transport
    assert isinstance(raised.value, repository.GitTransportError) is transport


def test_repository_implementation_is_owned_by_provider() -> None:
    root = Path(__file__).resolve().parents[3] / "mimir"
    for retired in (
        "repository_config.py", "repo_tools.py", "project_tests.py",
        "tools/repo.py", "pr_checkout_lease.py",
    ):
        assert not (root / retired).exists()
    assert {tool.name for tool in repository.REPO_TOOLS} == {
        "repo_checkout", "repo_cleanup", "repo_fetch", "repo_status", "repo_test",
        "repo_diff", "repo_file_at_revision", "repo_restore_file", "repo_unmerged", "repo_stage", "repo_commit", "repo_merge",
        "repo_merge_abort", "repo_rebase", "repo_rebase_abort", "repo_revert",
        "repo_revert_abort", "repo_push",
    }

    tree = ast.parse(Path(repository.__file__).read_text(encoding="utf-8"))
    forbidden = {
        "mimir.repo_tools", "mimir.project_tests", "mimir.pr_checkout_lease",
        "mimir.repository_config", "mimir.repo_pr_parity", "mimir.tools.repo",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in forbidden:
            pytest.fail(f"provider imports retired core repository module: {node.module}")


def test_surviving_core_shell_does_not_import_provider() -> None:
    root = Path(__file__).resolve().parents[3]
    tree = ast.parse((root / "mimir/tools/_shell_env.py").read_text(encoding="utf-8"))
    modules = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    modules.extend(
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert all(not (module or "").startswith("mimir_cultivate") for module in modules)


def test_repository_config_and_parity_are_provider_owned(tmp_path: Path) -> None:
    assert not (Path(__file__).resolve().parents[3] / "mimir" / "repo_pr_parity.py").exists()
    assert repository.RepositoryInventory.load(tmp_path / "absent.yaml").declared is False
    report = repository.run_parity(repository.offline_canary_probes())
    assert report["cutover_blocked"] is False
    assert report["counts"]["operations_total"] > 0


def test_repository_git_path_validation_stays_closed() -> None:
    with pytest.raises(repository.GitRefusal, match="repository-relative"):
        repository._validated_paths(("../outside",), required=True)


def test_project_test_failure_summary_requires_source_inventory(tmp_path: Path) -> None:
    path = tmp_path / "test_sample.py"
    path.write_text("def test_known():\n    assert False\n", encoding="utf-8")
    inventory = repository.pytest_node_inventory(tmp_path)
    result = repository.pytest_failure_summary(
        b"= short test summary info =\nFAILED test_sample.py::test_known - failure\n"
        b"FAILED test_sample.py::test_forged - failure\n= 2 failed in 0.1s =\n",
        inventory,
    )
    assert result["failing"] == ["test_sample.py::test_known"]
    assert result["failing_dropped"] == 1


def test_provider_reacquires_and_inspects_an_exact_pr_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir.tools.budget_gate import _hold_scoped_pr_dispatch
    from mimir.plugins.core import policy_scope

    selected = load_plugins(enable="cultivate").policy
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    previous = state.checkout_lease
    assert previous is not None
    acquired = repository.recover_pr_checkout_lease(
        previous.path, scope, owner=scope.principal,
        lease_root=previous.lease_root, review_state=state,
        runner=lambda args: subprocess.run(args, capture_output=True, text=True, check=False),
    )
    assert type(acquired) is repository.PRCheckoutLease
    assert acquired.recovery_id != previous.recovery_id
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(acquired.lease_root))
    with pytest.raises(RuntimeError, match="no longer live"):
        with repository.hold_pr_checkout_lease(previous):
            pass
    with policy_scope(selected), repository.hold_pr_checkout_lease(acquired):
        result = repository.RepoGitTools(state).execute(repository.GitStatus())
    assert result.ok is True
    assert result.code == "ok"
    call = SimpleNamespace(
        tool_name="repo_status", policy=selected,
        authorization=SimpleNamespace(repo_pr_action_scope=scope),
        auth_context=SimpleNamespace(
            server_discovered_pr_states=None,
            repo_pr_scope_registry=SimpleNamespace(resolve=lambda *_: state),
        ),
        arguments={"repository": scope.canonical_repo, "pull_request": scope.pr_number},
    )
    with ExitStack() as stack:
        _hold_scoped_pr_dispatch(call, stack)
        assert repository.active_pr_checkout_lease_for_path(acquired.path) is not None
    object.__setattr__(acquired, "expires_at", datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(RuntimeError, match="no longer live"):
        repository.assert_live_pr_checkout_lease(acquired)


def test_provider_creates_and_cleans_up_checkout(tmp_path: Path) -> None:
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    fresh_root = tmp_path / "provider-leases"
    fresh_root.mkdir()
    def run(argv):
        return subprocess.run(
            argv, capture_output=True, text=True, check=False, env=_fixture_git_env(),
        )
    lease = repository.create_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=fresh_root, runner=run,
    )
    assert isinstance(lease, repository.PRCheckoutLease)
    assert (lease.path / ".git/mimir-pr-checkout-lease.json").is_file()
    assert repository.RepoGitTools(state).validated_checkout_root() == state.checkout_lease.path
    assert repository.cleanup_pr_checkout_lease(lease, runner=run) is True
    assert not os.path.lexists(lease.path)


def _rebased_divergent_lease(tmp_path: Path, *, production_clone: bool = False):
    origin, source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    _git(source, "checkout", "-q", "main")
    (source / "base-update.txt").write_text("advanced base\n")
    _git(source, "add", "base-update.txt")
    _git(source, "commit", "-qm", "advance base")
    advanced = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "-q", "origin", "HEAD:main")
    if production_clone:
        _git(source, "tag", "fixture-release")
        # Create the lease with the canonical source on main and tagged, using
        # the real clone path. Remove the unused fixture lease first.
        repository.cleanup_pr_checkout_lease(lease)
        lease = repository.create_pr_checkout_lease(
            scope, owner=scope.principal, lease_root=lease.lease_root,
        )
        assert _git(lease.path, "rev-parse", "refs/heads/main") == advanced
        assert _git(lease.path, "rev-parse", "refs/tags/fixture-release") == advanced
    _git(lease.path, "fetch", "origin", "main")
    _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "rebase", "origin/main")
    assert _git(lease.path, "rev-parse", "HEAD") != scope.observed_head_sha
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha
    return source, scope, replace(scope, observed_base_sha=advanced), lease


@pytest.mark.parametrize("production_clone", [False, True])
def test_divergent_patch_equivalent_lease_is_discarded_before_renewal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, production_clone: bool,
) -> None:
    source, original_scope, scope, lease = _rebased_divergent_lease(
        tmp_path, production_clone=production_clone,
    )
    metadata = lease.path / repository._METADATA
    original_expiry = json.loads(metadata.read_text())["expires_at"]
    with pytest.raises(RuntimeError, match="cannot be renewed"):
        repository.recover_pr_checkout_lease(
            lease.path, original_scope, owner=scope.principal, lease_root=lease.lease_root,
        )
    assert json.loads(metadata.read_text())["expires_at"] == original_expiry
    events = []
    monkeypatch.setattr(repository, "_report_retained_candidates",
                        lambda event, *_, **_details: events.append(event))
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    fresh_state = RepoReviewState(scope)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root,
        review_state=fresh_state,
    )
    assert "pr_checkout_lease_discarded_equivalent" in events
    assert not lease.path.exists()
    assert fresh.path != lease.path and candidates == ()
    assert _git(fresh.path, "rev-parse", "HEAD") == scope.observed_head_sha
    from mimir.plugins.core import policy_scope
    with policy_scope(load_plugins(enable="cultivate").policy):
        assert_lease_attested(_lineage_turn(scope, fresh_state), scope, fresh)


def test_divergent_unique_work_is_preserved_and_fresh_lease_acquired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    source, _original_scope, scope, lease = _rebased_divergent_lease(tmp_path)
    (lease.path / "unique.txt").write_text("unpublished work\n")
    _git(lease.path, "add", "unique.txt")
    _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "commit", "-qm", "unique fix")
    unique_head = _git(lease.path, "rev-parse", "HEAD")
    events = []
    monkeypatch.setattr(repository, "_report_retained_candidates",
                        lambda event, *_, **_details: events.append(event))
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root,
        review_state=RepoReviewState(scope),
    )
    ref = f"refs/worklink-lease/{scope.pr_number}/{lease.recovery_id}"
    patch = lease.lease_root / ".recovery" / f"{lease.path.name}-{lease.recovery_id}.patch"
    assert _git(source, "rev-parse", ref) == unique_head
    assert "unique.txt" in patch.read_text()
    assert "pr_checkout_lease_preserved_divergent" in events
    assert "pr_checkout_lease_discarded_equivalent" not in events
    assert ref in caplog.text and str(patch) in caplog.text
    assert not lease.path.exists()
    assert fresh.path != lease.path and candidates == ()
    assert _git(fresh.path, "rev-parse", "HEAD") == scope.observed_head_sha


def test_repo_rebase_records_unpublished_replay_and_next_turn_discards_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    origin, source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    _git(source, "checkout", "-q", "main")
    (source / "base-update.txt").write_text("advanced base\n")
    _git(source, "add", "base-update.txt")
    _git(source, "commit", "-qm", "advance base")
    advanced = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "-q", "origin", "HEAD:main")
    from mimir.plugins.core import policy_scope
    with policy_scope(load_plugins(enable="cultivate").policy):
        repository.RepoGitTools(
            state, auth_context=_lineage_turn(scope, state),
        ).execute(repository.GitRebase())
    rebased = _git(lease.path, "rev-parse", "HEAD")
    assert json.loads((lease.path / repository._METADATA).read_text())["lineage"][rebased] is True
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha
    next_scope = replace(scope, observed_base_sha=advanced, event_type="pr_review_requested")
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: next_scope.observed_head_sha)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        next_scope, owner=scope.principal, lease_root=lease.lease_root,
        review_state=RepoReviewState(next_scope),
    )
    assert fresh.path != lease.path and candidates == ()
    assert not lease.path.exists()


def test_dirty_equivalent_checkout_is_preserved_not_discarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    (lease.path / "draft.txt").write_text("working tree draft\n")
    events = []
    monkeypatch.setattr(repository, "_report_retained_candidates",
                        lambda event, *_, **_details: events.append(event))
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    fresh, _ = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root,
        review_state=RepoReviewState(scope),
    )
    archive = (lease.lease_root / ".recovery" /
               f"{lease.path.name}-{lease.recovery_id}.checkout.tar.gz")
    assert archive.is_file()
    assert "pr_checkout_lease_preserved_divergent" in events
    assert "pr_checkout_lease_discarded_equivalent" not in events
    assert fresh.path != lease.path


@pytest.mark.parametrize("content", ["pull\trequest\n", "    pull request\n", "pullrequest\n"])
def test_divergent_whitespace_is_unique_work(tmp_path: Path, content: str) -> None:
    source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    (lease.path / "tracked.txt").write_text(content)
    _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "commit", "--amend", "-qam", "whitespace resolution")
    head = _git(lease.path, "rev-parse", "HEAD")
    notices = []
    assert repository._discard_divergent_candidate(
        lease, scope, runner=repository._default_runner, notices=notices,
    )
    shown = subprocess.run(
        ["git", "-C", str(source), "show", f"{notices[0]['ref']}:tracked.txt"],
        capture_output=True, text=True, check=True, env=_fixture_git_env(),
    )
    assert shown.stdout == content
    assert _git(source, "rev-parse", notices[0]["ref"]) == head
    assert Path(notices[0]["bundle"]).is_file()
    assert Path(notices[0]["archive"]).is_file()


@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/tags/fixture-release"])
def test_modified_inherited_clone_ref_forces_preservation(tmp_path: Path, ref: str) -> None:
    source, _original, scope, lease = _rebased_divergent_lease(tmp_path, production_clone=True)
    head = _git(lease.path, "rev-parse", "HEAD")
    _git(lease.path, "update-ref", ref, head)
    assert _git(source, "rev-parse", ref) != head
    notices = []
    assert repository._discard_divergent_candidate(
        lease, scope, runner=repository._default_runner, notices=notices,
    )
    assert notices and Path(notices[0]["bundle"]).is_file()


@pytest.mark.parametrize("state_kind", ["stash", "ignored", "branch", "tag"])
def test_divergent_hidden_state_is_recoverable(tmp_path: Path, state_kind: str) -> None:
    import tarfile

    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    if state_kind == "ignored":
        (lease.path / ".git/info/exclude").write_text("secret-draft.txt\n")
        (lease.path / "secret-draft.txt").write_text("ignored work\n")
    else:
        (lease.path / "tracked.txt").write_text("hidden work\n")
        if state_kind == "stash":
            _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
                 "stash", "push", "-m", "saved draft")
        else:
            _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
                 "commit", "-qam", "hidden work")
            _git(lease.path, "branch" if state_kind == "branch" else "tag", "saved-draft")
            _git(lease.path, "reset", "--hard", "HEAD~1")
    assert not _git(lease.path, "status", "--porcelain")
    notices = []
    assert repository._discard_divergent_candidate(
        lease, scope, runner=repository._default_runner, notices=notices,
    )
    restored = tmp_path / "restored"
    restored.mkdir()
    with tarfile.open(notices[0]["archive"]) as archive:
        archive.extractall(restored, filter="data")
    tree = restored / "worktree"
    assert not (tree / ".git/objects").exists()
    # Seed objects through a separate origin clone: fetching directly into the
    # restored metadata would negotiate using refs whose objects are absent.
    import shutil
    seed = tmp_path / "published.git"
    subprocess.run(["git", "clone", "--bare", scope.canonical_origin, str(seed)],
                   check=True, capture_output=True, env=_fixture_git_env())
    # Incremental recovery must actually exclude published objects, not merely
    # pass the flag: its header advertises prerequisite commits from origin.
    header = Path(notices[0]["bundle"]).read_bytes().split(b"\n\n", 1)[0]
    assert any(line.startswith(b"-") for line in header.splitlines())
    imported = subprocess.run(
        ["git", "-C", str(seed), "bundle", "unbundle", notices[0]["bundle"]],
        capture_output=True, text=True, env=_fixture_git_env(),
    )
    assert imported.returncode == 0, imported.stderr
    shutil.copytree(seed / "objects", tree / ".git/objects")
    if state_kind == "ignored":
        assert (tree / "secret-draft.txt").read_text() == "ignored work\n"
    else:
        ref = "refs/stash" if state_kind == "stash" else (
            "refs/heads/saved-draft" if state_kind == "branch" else "refs/tags/saved-draft"
        )
        assert _git(tree, "show", f"{ref}:tracked.txt") == "hidden work"
        heads = _git(tree, "bundle", "list-heads", notices[0]["bundle"])
        assert ref in heads


@pytest.mark.parametrize("gate", ["scoped", "base", "substitution", "recovery_ref", "diff"])
def test_divergent_probe_errors_preserve_lease(tmp_path: Path, gate: str) -> None:
    _source, original, scope, lease = _rebased_divergent_lease(tmp_path)
    if gate == "recovery_ref":
        (lease.path / "draft.txt").write_text("draft")
    before = (lease.path / repository._METADATA).read_bytes()
    head = _git(lease.path, "rev-parse", "HEAD")
    reached = []
    base_probes = []

    def runner(argv):
        is_base_probe = argv[3:] == ["merge-base", "--is-ancestor", scope.observed_base_sha, head]
        if is_base_probe:
            base_probes.append(argv)
        fail = (
            (gate == "scoped" and argv[3:] == ["merge-base", "--is-ancestor", scope.observed_head_sha, head])
            or (gate == "base" and is_base_probe and len(base_probes) == 2)
            or (gate == "substitution" and argv[3:] == ["merge-base", "--is-ancestor", lease.base_sha, scope.observed_base_sha])
            or (gate == "recovery_ref" and argv[3:5] == ["rev-parse", "--verify"] and argv[-1].startswith("refs/worklink-lease/"))
            or (gate == "diff" and "diff-tree" in argv)
        )
        if fail:
            reached.append(argv)
            return subprocess.CompletedProcess(argv, 128, stdout="", stderr="injected probe failure")
        return repository._default_runner(argv)

    expected = "base ancestry inspection failed" if gate == "base" else None
    with pytest.raises(RuntimeError, match=expected):
        repository._discard_divergent_candidate(lease, scope, runner=runner)
    assert reached
    if gate == "base":
        assert len(base_probes) == 2  # substitution passed; final ancestry failed
    assert lease.path.is_dir()
    assert (lease.path / repository._METADATA).read_bytes() == before


@pytest.mark.parametrize("probe_error", [False, True])
def test_foreign_rebind_ancestry_gate_does_not_renew(tmp_path: Path, probe_error: bool) -> None:
    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    before = (lease.path / repository._METADATA).read_bytes()
    head = _git(lease.path, "rev-parse", "HEAD")
    reached = []
    def runner(argv):
        if argv[3:] == ["merge-base", "--is-ancestor", scope.observed_head_sha, head]:
            reached.append(argv)
            if probe_error:
                return subprocess.CompletedProcess(argv, 128, stdout="", stderr="probe failed")
        return repository._default_runner(argv)
    with pytest.raises(RuntimeError, match="cannot be rebound"):
        repository._rebind_foreign_candidate(
            lease, scope, head=head, ttl=timedelta(hours=2), review_state=None,
            runner=runner,
        )
    assert reached
    assert (lease.path / repository._METADATA).read_bytes() == before


@pytest.mark.parametrize("control", ["alternates", "cap", "merge", "old_base", "nonancestor_base"])
def test_divergent_equivalence_guards(tmp_path: Path, control: str) -> None:
    _source, original, scope, lease = _rebased_divergent_lease(tmp_path)
    if control == "alternates":
        (lease.path / ".git/objects/info/alternates").write_text("/missing/objects\n")
        with pytest.raises(RuntimeError):
            repository._discard_divergent_candidate(lease, scope, runner=repository._default_runner)
        assert lease.path.is_dir()
        return
    if control == "nonancestor_base":
        # The published PR head is not an ancestor of the rebased checkout.
        scope = replace(scope, observed_base_sha=original.observed_head_sha)
    if control == "old_base":
        scope = original
    if control == "merge":
        _git(lease.path, "checkout", "-b", "side", scope.observed_base_sha)
        (lease.path / "side.txt").write_text("merge resolution\n")
        _git(lease.path, "add", "side.txt")
        _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
             "commit", "-qm", "side")
        _git(lease.path, "checkout", scope.head_ref)
        _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
             "merge", "--no-ff", "side", "-m", "merge resolution")
        _git(lease.path, "branch", "-D", "side")
    reached = []
    def runner(argv):
        result = repository._default_runner(argv)
        if control == "cap" and argv[3:5] == ["rev-list", "--max-count=501"]:
            reached.append(argv)
            commit = result.stdout.splitlines()[0]
            return subprocess.CompletedProcess(argv, 0, stdout=(commit + "\n") * 501, stderr="")
        return result
    notices = []
    assert repository._discard_divergent_candidate(lease, scope, runner=runner, notices=notices)
    assert notices and Path(notices[0]["bundle"]).is_file()
    if control == "cap":
        assert reached


def test_divergent_live_recheck_prevents_retirement(tmp_path: Path, monkeypatch) -> None:
    from mimir_cultivate import scheduler

    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    calls = []
    def active():
        calls.append(True)
        return set() if len(calls) == 1 else {lease.path}
    monkeypatch.setattr(scheduler, "active_pr_checkout_lease_paths", active)
    with pytest.raises(RuntimeError, match="live PR checkout"):
        repository._discard_divergent_candidate(lease, scope, runner=repository._default_runner)
    assert len(calls) == 2 and lease.path.is_dir()


def test_preserved_notice_reaches_real_checkout_result(tmp_path: Path, monkeypatch) -> None:
    from mimir.plugins.core import policy_scope
    from langgraph.prebuilt import ToolRuntime

    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    (lease.path / "draft.txt").write_text("recover me\n")
    state = RepoReviewState(scope)
    monkeypatch.setattr(repository, "_observe_current_pr_head", lambda _scope: scope.observed_head_sha)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    auth = replace(
        _untainted_ifc_auth(), repo_review_state=state, repo_pr_action_scope=scope,
        repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
    )
    from mimir_cultivate import forge
    monkeypatch.setattr(forge, "_client", lambda _scope: SimpleNamespace())
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    with policy_scope(load_plugins(enable="cultivate").policy):
        result = repository.checkout_scoped_pr(
            scope.canonical_repo, scope.pr_number,
            ToolRuntime(state={}, context=auth, config={}, stream_writer=lambda _: None,
                        tool_call_id="checkout", store=None),
        )
    notice, = result["preserved_prior_work"]
    assert notice["ref"] == f"refs/worklink-lease/{scope.pr_number}/{lease.recovery_id}"
    assert Path(notice["bundle"]).is_file() and Path(notice["archive"]).is_file()
    assert result["path"] != str(lease.path)


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("operation", ["repo_checkout", "repo_status"])
@pytest.mark.parametrize("expiry_point", [None, "handler", "classification", "label_merge"])
def test_provider_checkout_handoff_and_live_result_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    async_mode: bool, operation: str, expiry_point: str | None,
) -> None:
    from mimir.tools import budget_gate

    selected = load_plugins(enable="cultivate").policy
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    root = state.checkout_lease.lease_root
    if operation == "repo_checkout":
        root = tmp_path / "provider-leases"
        root.mkdir()
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(root))
    auth = replace(
        _untainted_ifc_auth(), repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
        repo_review_state=state, repo_pr_action_scope=scope,
    )
    auth = _repo_granted_auth(
        auth, scope, state, follow_on=operation != "repo_checkout", policy=selected,
    )
    calls: list[str] = []
    completed = threading.Event()
    competitor_ready = threading.Event()
    release_competitor = threading.Event()
    rebound = []
    threads = []
    original_finish = budget_gate._finish_tool_result
    original_classify = budget_gate._result_labels_for_call
    original_merge = budget_gate._merge_result_labels_from_result
    clock: dict[str, datetime | None] = {"now": None}

    class LeaseClock:
        @staticmethod
        def now(tz):
            return clock["now"] or datetime.now(tz)

    def classify(*args, **kwargs):
        labels = original_classify(*args, **kwargs)
        if expiry_point == "classification":
            clock["now"] = state.checkout_lease.expires_at + timedelta(seconds=1)
        return labels

    def merge(*args, **kwargs):
        original_merge(*args, **kwargs)
        if expiry_point == "label_merge":
            clock["now"] = state.checkout_lease.expires_at + timedelta(seconds=1)

    monkeypatch.setattr(budget_gate, "_result_labels_for_call", classify)
    monkeypatch.setattr(budget_gate, "_merge_result_labels_from_result", merge)

    def run(argv):
        return subprocess.run(
            argv, capture_output=True, text=True, check=False, env=_fixture_git_env(),
        )

    def competitor(lease):
        assert lock_is_held()
        competitor_ready.set()
        assert release_competitor.wait(10)
        rebound.append(repository.recover_pr_checkout_lease(
            lease.path, scope, owner=scope.principal, lease_root=root, runner=run,
        ))
        completed.set()

    def lock_is_held():
        fd = os.open(root, os.O_RDONLY)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    def handler(req):
        if operation == "repo_checkout":
            lease, _ = repository.acquire_pr_checkout_lease(
                scope, owner=scope.principal, lease_root=root,
                review_state=state, runner=run,
            )
        else:
            lease = state.checkout_lease
            assert repository.RepoGitTools(state).execute(repository.GitStatus()).ok
        assert lock_is_held()
        if expiry_point is not None:
            monkeypatch.setattr(repository, "datetime", LeaseClock)
            clock["now"] = lease.expires_at + timedelta(
                seconds=(1 if expiry_point == "handler" else -1),
            )
        else:
            thread = threading.Thread(target=competitor, args=(lease,))
            threads.append(thread)
            thread.start()
            assert competitor_ready.wait(5)
        calls.append("handler")
        return ToolMessage(content='{"ok": true}', tool_call_id=req.tool_call["id"])

    def finish(call, capture, result, **kwargs):
        assert lock_is_held()
        assert not completed.is_set()
        calls.append("observed")
        return original_finish(call, capture, result, **kwargs)

    monkeypatch.setattr(budget_gate, "_finish_tool_result", finish)
    ctx = _ifc_turn(auth)
    token = set_current_turn(ctx)
    try:
        request = _make_request(
            operation, "provider", auth,
            {"repository": scope.canonical_repo, "pull_request": scope.pr_number},
        )
        if async_mode:
            async def async_handler(req):
                return handler(req)

            result = asyncio.run(BudgetGateMiddleware(selected).awrap_tool_call(request, async_handler))
        else:
            result = BudgetGateMiddleware(selected).wrap_tool_call(request, handler)
    finally:
        release_competitor.set()
        reset_current_turn(token)
    if expiry_point is not None:
        assert result.status == "error"
        assert "expired or changed" in str(result.content)
        assert calls == (["handler"] if expiry_point == "handler" else ["handler", "observed"])
        if expiry_point == "label_merge":
            assert ctx.ifc_labels.has_untrusted_active_ingest
    else:
        assert result.status != "error", result.content
        assert calls == ["handler", "observed"]
        assert completed.wait(10)
        assert rebound[0].recovery_id != state.checkout_lease.recovery_id
        threads[0].join(timeout=10)


@pytest.mark.asyncio
async def test_provider_project_tests_run_fixed_command_in_contained_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_worklink_test(home)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    calls = []

    async def run(argv, capability, environment, projections, **kwargs):
        calls.append((argv, capability, environment, projections, kwargs))
        return CollectedExecutionResult(0, b"ok", b"", False, False, 0, 0)

    def checkout(source, **kwargs):
        capability = SimpleNamespace(path=source)
        return SimpleNamespace(path=source, capability=capability, close=lambda: None)

    result = await repository.RepoProjectTests(
        state, runner=run, checkout_factory=checkout,
    ).execute()
    assert result.ok is True
    argv, capability, environment, projections, _kwargs = calls[0]
    assert argv == ("/usr/bin/true", "-q")
    assert capability.path == state.checkout_lease.path
    assert environment["GIT_CONFIG_KEY_0"] == "safe.directory"
    assert "GITHUB_TOKEN" not in environment
    assert projections == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("runner_kind", ["relative", "bare", "absolute"])
async def test_repo_test_runner_executes_from_validated_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner_kind: str,
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    root = state.checkout_lease.path
    tools = root / "tools"
    tools.mkdir()
    script = tmp_path / "absolute-verify" if runner_kind == "absolute" else tools / "verify"
    script.write_text('#!/bin/sh\npwd > "$1"\nprintf "%s\\n" "$@" >> "$1"\n')
    script.chmod(0o755)
    marker = tmp_path / "invoked"
    if runner_kind == "bare":
        # A bare name still uses the fixed system PATH, not tools/verify.
        command = "git --version"
    elif runner_kind == "absolute":
        command = str(script)
    else:
        command = "./tools/verify"
    home = tmp_path / "home"
    _configure_worklink_test(home, f"{command} {marker} argument" if runner_kind != "bare" else command)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.chdir(tmp_path)
    calls = []

    async def run(argv, capability, environment, projections, **kwargs):
        calls.append(argv)
        completed = subprocess.run(
            argv, cwd=capability.path, env=environment, capture_output=True, check=False,
        )
        return CollectedExecutionResult(
            completed.returncode, completed.stdout, completed.stderr, False, False, 0, 0,
        )

    def checkout(source, **kwargs):
        return SimpleNamespace(
            path=source, capability=SimpleNamespace(path=source), close=lambda: None,
        )

    result = await repository.RepoProjectTests(
        state, runner=run, checkout_factory=checkout,
    ).execute()
    assert result.ok, result
    assert len(calls) == 1
    if runner_kind == "bare":
        assert calls[0] == ("/usr/bin/git", "--version")
        assert not marker.exists()
    else:
        assert calls[0] == (("./tools/verify" if runner_kind == "relative" else str(script)),
                            str(marker), "argument")
        assert marker.read_text().splitlines() == [str(root), str(marker), "argument"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "parent", "relative_symlink", "absolute_symlink", "directory", "nonexecutable", "missing",
])
async def test_repo_test_relative_runner_refuses_unresolvable_or_escaping_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    root = state.checkout_lease.path
    tools = root / "tools"
    tools.mkdir()
    marker = tmp_path / "invoked"
    outside = root.parent / "outside-verify"
    outside.write_text(f'#!/bin/sh\n: > "{marker}"\n')
    outside.chmod(0o755)
    runner_path = tools / "verify"
    if case == "parent":
        command = "../outside-verify"
    else:
        command = "./tools/verify"
        if case == "relative_symlink":
            runner_path.symlink_to("../../outside-verify")
        elif case == "absolute_symlink":
            runner_path.symlink_to(outside)
        elif case == "directory":
            runner_path.mkdir()
        elif case == "nonexecutable":
            runner_path.write_text(f'#!/bin/sh\n: > "{marker}"\n')
            runner_path.chmod(0o644)
    home = tmp_path / "home"
    _configure_worklink_test(home, command)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.chdir(tmp_path)
    calls = []

    async def forbidden(*args, **kwargs):
        calls.append(args)
        pytest.fail("refused runner must not execute")

    def checkout(source, **kwargs):
        pytest.fail("refused runner must not create a snapshot")

    with pytest.raises(repository.ProjectTestRefusal) as error:
        await repository.RepoProjectTests(
            state, runner=forbidden, checkout_factory=checkout,
        ).execute()
    assert error.value.code == "test_command_unresolvable"
    assert not error.value.execution_started
    assert not calls and not marker.exists()


def _two_leg_suites(command: str) -> list[dict]:
    return [{
        "name": "python", "default": True, "selector_prefixes": ["tests/"],
        "legs": [
            {"name": "core", "command": command},
            {"name": "cultivate", "command": command,
             "env": {"MIMIR_PLUGINS": "cultivate"},
             "default_paths": ["tests", "plugins/cultivate/tests"]},
        ],
    }]


@pytest.mark.asyncio
@pytest.mark.parametrize("selectors", [(), ("tests/test_changed.py",)])
async def test_repo_test_legs_process_env_and_selectors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selectors: tuple[str, ...],
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    root = state.checkout_lease.path
    (root / "tools").mkdir()
    script = root / "tools" / "verify"
    script.write_text('#!/bin/sh\nprintf "%s|%s\\n" "${MIMIR_PLUGINS-unset}" "$*"\n')
    script.chmod(0o755)
    (root / "tests").mkdir()
    (root / "tests/test_changed.py").write_text("def test_changed(): pass\n")
    (root / "plugins/cultivate/tests").mkdir(parents=True)
    home = tmp_path / "home"
    _configure_test_suites(home, state, test_suites=_two_leg_suites("./tools/verify"))
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.setenv("MIMIR_PLUGINS", "ambient")
    calls = []

    async def runner(argv, capability, environment, projections, **kwargs):
        calls.append(argv)
        process = subprocess.run(argv, cwd=capability.path, env=environment, capture_output=True, check=False)
        return CollectedExecutionResult(process.returncode, process.stdout, process.stderr,
                                        False, False, 0, 0)

    result = await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory,
    ).execute(selectors)
    assert result.ok and result.failed_leg is None
    expected = selectors or ()
    assert calls[0] == ("./tools/verify", *expected)
    assert calls[1] == ("./tools/verify", *(selectors or ("tests", "plugins/cultivate/tests")))
    assert result.legs[0]["stdout"] == f"unset|{' '.join(expected)}\n"
    assert result.legs[1]["stdout"] == (
        f"cultivate|{' '.join(selectors or ('tests', 'plugins/cultivate/tests'))}\n"
    )
    assert state.full_tested_head == (state.git_expected_head if not selectors else None)


@pytest.mark.asyncio
async def test_repo_test_legs_stop_at_first_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_test_suites(home, state, test_suites=_two_leg_suites("/usr/bin/false"))
    monkeypatch.setenv("MIMIR_HOME", str(home))
    (state.checkout_lease.path / "tests").mkdir()
    (state.checkout_lease.path / "plugins/cultivate/tests").mkdir(parents=True)
    calls = []

    async def runner(argv, *args, **kwargs):
        calls.append(argv)
        return CollectedExecutionResult(1, b"failed", b"", False, False, 0, 0)

    result = await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory,
    ).execute()
    assert result.code == "tests_failed" and result.failed_leg == "core"
    assert [leg["name"] for leg in result.legs] == ["core"]
    assert len(calls) == 1 and state.full_tested_head is None


@pytest.mark.asyncio
@pytest.mark.parametrize("first_elapsed", [6.0, 10.0, 11.0])
async def test_repo_test_legs_share_timeout_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first_elapsed: float,
) -> None:
    from types import SimpleNamespace

    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_test_suites(home, state, test_suites=_two_leg_suites("/usr/bin/true"))
    monkeypatch.setenv("MIMIR_HOME", str(home))
    (state.checkout_lease.path / "tests").mkdir()
    (state.checkout_lease.path / "plugins/cultivate/tests").mkdir(parents=True)
    now = [100.0]
    # Replace only this module's clock, not asyncio's process-global clock.
    monkeypatch.setattr(repository, "time", SimpleNamespace(monotonic=lambda: now[0]))
    timeouts = []

    async def runner(argv, *args, timeout_s, **kwargs):
        timeouts.append(timeout_s)
        elapsed = first_elapsed if len(timeouts) == 1 else 6.0
        now[0] += min(elapsed, timeout_s)
        timed_out = len(timeouts) > 1 and elapsed > timeout_s
        return CollectedExecutionResult(
            None if timed_out else 0, b"", b"", timed_out, False, 0, 0,
        )

    result = await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory, timeout=10.0,
    ).execute()
    assert timeouts == ([10.0, 4.0] if first_elapsed == 6.0 else [10.0])
    assert not result.ok and result.code == "test_timeout"
    assert result.failed_leg == "cultivate"
    assert [leg["name"] for leg in result.legs] == ["core", "cultivate"]
    assert [leg["timed_out"] for leg in result.legs] == [False, True]
    assert state.full_tested_head is None


@pytest.mark.asyncio
@pytest.mark.parametrize("leg_count", [4, 5])
async def test_repo_test_legs_count_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, leg_count: int,
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    suites = [{"name": "python", "default": True, "legs": [
        {"name": f"leg-{index}", "command": "/usr/bin/true"}
        for index in range(leg_count)
    ]}]
    _configure_test_suites(home, state, test_suites=suites)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    calls = []

    async def runner(*args, **kwargs):
        calls.append(args)
        return CollectedExecutionResult(0, b"", b"", False, False, 0, 0)

    def checkout(*args, **kwargs):
        assert leg_count == 4, "over-limit suites must refuse before creating a checkout"
        return _test_checkout_factory(*args, **kwargs)

    executor = repository.RepoProjectTests(state, runner=runner, checkout_factory=checkout)
    if leg_count == 5:
        with pytest.raises(repository.ProjectTestRefusal) as error:
            await executor.execute()
        assert error.value.code == "test_config_invalid" and not error.value.execution_started
        assert calls == [] and state.full_tested_head is None
    else:
        result = await executor.execute()
        assert result.ok and len(result.legs) == len(calls) == 4
        assert result.failed_leg is None


@pytest.mark.asyncio
@pytest.mark.parametrize("env", [
    {"UNLISTED": "cultivate"}, {"PATH": "safe"},
    {"MIMIR_PLUGINS": "cultivate;id"}, {"MIMIR_PLUGINS": "a" * 65},
    {"MIMIR_ACCESS_CONTROL_ENFORCED": 1},
])
async def test_repo_test_legs_reject_env_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env: dict,
) -> None:
    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    suites = _two_leg_suites("/usr/bin/true")
    suites[0]["legs"][1]["env"] = env
    _configure_test_suites(home, state, test_suites=suites)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    (state.checkout_lease.path / "tests").mkdir()
    (state.checkout_lease.path / "plugins/cultivate/tests").mkdir(parents=True)

    def forbidden(*args, **kwargs):
        pytest.fail("invalid leg configuration must not create a checkout")

    with pytest.raises(repository.ProjectTestRefusal) as error:
        await repository.RepoProjectTests(state, checkout_factory=forbidden).execute()
    assert error.value.code == "test_config_invalid" and not error.value.execution_started


@pytest.mark.asyncio
@pytest.mark.parametrize("paths", [["../outside"], ["missing"]])
async def test_repo_test_legs_reject_bad_default_paths_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, paths: list[str],
) -> None:
    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    suites = _two_leg_suites("/usr/bin/true")
    suites[0]["legs"][1]["default_paths"] = paths
    _configure_test_suites(home, state, test_suites=suites)
    monkeypatch.setenv("MIMIR_HOME", str(home))

    def forbidden(*args, **kwargs):
        pytest.fail("invalid fallback must not create a checkout")

    with pytest.raises(repository.ProjectTestRefusal) as error:
        await repository.RepoProjectTests(state, checkout_factory=forbidden).execute()
    assert error.value.code in {"test_selector_outside_checkout", "test_selector_not_found"}
    assert not error.value.execution_started
