"""Merge-only conflict publication boundaries, using real fixture Git repositories."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from mimir.plugins.core import policy_scope
from mimir.plugins.manager import load_plugins
from mimir_cultivate import repository as repo
from tests.test_repo_tools import (
    _conflict_commit_evidence, _configure_conflict_suites, _configure_test_suites,
    _git, _local_contained_runner, _test_checkout_factory, _two_sided_conflict_tools,
)


@pytest.fixture(autouse=True)
def policy():
    with policy_scope(load_plugins(enable="cultivate").policy):
        yield


@pytest.fixture
def conflict(tmp_path, monkeypatch):
    origin, scope, state, tools = _two_sided_conflict_tools(tmp_path)
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitMerge())
    assert raised.value.code == "merge_conflict"
    assert state.conflict_merge.conflicted_paths == {"tracked.txt"}
    home = tmp_path / "home"
    _configure_conflict_suites(home, state)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    # The fixture's local runner has no host-wide lease wrapper.
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE", raising=False)
    return origin, scope, state, tools, home


def _resolve(conflict, *, evidence=None, extra_paths=()):
    _, _, state, tools, _ = conflict
    (state.checkout_lease.path / "tracked.txt").write_text("base property\nhead property\n")
    tools.execute(repo.GitStage(("tracked.txt",)))
    return tools.execute(repo.GitCommit(
        tuple(dict.fromkeys(("tracked.txt", "base-test", "head-test", "suite", *extra_paths))),
        "resolve base merge",
        _conflict_commit_evidence() if evidence is None else evidence,
    ))


def _test(state, **kwargs):
    return asyncio.run(repo.RepoProjectTests(
        state, runner=_local_contained_runner, checkout_factory=_test_checkout_factory,
    ).execute(**kwargs))


def test_conflict_merge_evidence_both_full_suites_and_leased_push(conflict):
    origin, scope, state, tools, _ = conflict
    observed = scope.observed_head_sha
    assert _resolve(conflict).ok
    head = state.git_expected_head
    assert state.conflict_evidence_head == head
    assert _git(state.checkout_lease.path, "show", "-s", "--format=%P", head).split() == [
        observed, scope.observed_base_sha,
    ]
    message = _git(state.checkout_lease.path, "log", "-1", "--format=%B")
    assert 'Path: "tracked.txt"' in message
    for text in ("Base property: base property", "Head property: head property",
                 "Base verification: base-test", "Head verification: head-test"):
        assert text in message
    tested = _test(state)
    assert tested.ok and [leg["name"] for leg in tested.legs] == ["core", "cultivate"]
    assert state.full_tested_head == head
    assert tools.execute(repo.GitPush()).ok
    assert _git(origin, "rev-parse", scope.destination_ref) == head
    assert _git(origin, "merge-base", observed, head) == observed


@pytest.mark.parametrize("invalid", [
    "missing", "wrong_path", "duplicate", "base_property", "base_verification",
    "head_property", "head_verification", "oversized", "nul",
])
def test_conflict_commit_requires_bounded_evidence_for_every_path(conflict, invalid):
    _, scope, state, _, _ = conflict
    [entry] = _conflict_commit_evidence()
    evidence = (entry,)
    if invalid == "missing":
        evidence = ()
    elif invalid == "wrong_path":
        evidence = (replace(entry, path="base-test"),)
    elif invalid == "duplicate":
        evidence = (entry, entry)
    elif invalid == "oversized":
        evidence = (replace(entry, base_property="x" * 2049),)
    elif invalid == "nul":
        evidence = (replace(entry, head_verification="bad\x00proof"),)
    else:
        evidence = (replace(entry, **{invalid: "  "}),)
    with pytest.raises(repo.GitRefusal) as raised:
        _resolve(conflict, evidence=evidence)
    assert raised.value.code == "conflict_evidence_required"
    assert state.conflict_evidence_head is None
    assert _git(state.checkout_lease.path, "rev-parse", "HEAD") == scope.observed_head_sha


def test_conflict_push_requires_evidence_at_exact_head(conflict):
    origin, scope, state, tools, _ = conflict
    _resolve(conflict)
    assert _test(state).ok
    object.__setattr__(state, "conflict_evidence_head", scope.observed_head_sha)
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitPush())
    assert raised.value.code == "conflict_evidence_required"
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha


def test_conflict_push_requires_full_tests_at_exact_head(conflict):
    origin, scope, state, tools, _ = conflict
    _resolve(conflict)
    assert _test(state).ok
    object.__setattr__(state, "full_tested_head", scope.observed_head_sha)
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitPush())
    assert raised.value.code == "full_test_required"
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha


@pytest.mark.parametrize("invalid", ["missing", "head", "base"])
def test_conflict_commit_requires_matching_typed_merge_proof(conflict, invalid):
    _, _, state, _, _ = conflict
    proof = state.conflict_merge
    object.__setattr__(state, "conflict_merge", (
        None if invalid == "missing" else replace(
            proof, **{"original_head" if invalid == "head" else "base": "a" * 40},
        )
    ))
    with pytest.raises(repo.GitRefusal) as raised:
        _resolve(conflict)
    assert raised.value.code == "conflict_evidence_required"


def test_conflict_push_cannot_replace_typed_merge_with_only_markers(conflict):
    _, _, state, tools, _ = conflict
    _resolve(conflict)
    assert _test(state).ok
    object.__setattr__(state, "conflict_merge", None)
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitPush())
    assert raised.value.code == "conflict_evidence_required"


@pytest.mark.parametrize("change", ["modify", "delete", "mode", "new_path"])
def test_conflict_resolution_outside_pr_paths_refuses_push(conflict, change):
    origin, scope, state, tools, _ = conflict
    root = state.checkout_lease.path
    name = "base-test"
    if change == "modify":
        (root / name).write_text("#!/bin/sh\nexit 0\n")
    elif change == "delete":
        (root / name).unlink()
    elif change == "mode":
        (root / name).chmod(0o644)
    else:
        name = "non-pr.txt"
        (root / name).write_text("unexpected change\n")
    _resolve(conflict, extra_paths=(name,))
    # A passing configured full suite must not waive the independent path gate.
    _configure_test_suites(conflict[4], state, test_suites=[{
        "name": "python", "default": True, "legs": [
            {"name": "core", "command": "/usr/bin/true"},
            {"name": "cultivate", "command": "/usr/bin/true", "env": {"MIMIR_PLUGINS": "cultivate"}},
        ],
    }])
    assert _test(state).ok
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitPush())
    assert raised.value.code == "conflict_resolution_outside_pr_paths"
    assert name in str(raised.value)
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha


@pytest.mark.parametrize("modes", [("",), ("cultivate",), ("", "other")])
def test_conflict_full_marker_accepts_every_configured_leg(conflict, modes):
    _, _, state, _, home = conflict
    _resolve(conflict)
    _configure_test_suites(home, state, test_suites=[{
        "name": "python", "default": True, "legs": [
            {"name": f"leg{n}", "command": "./suite", "env": {"MIMIR_PLUGINS": mode}}
            for n, mode in enumerate(modes)
        ],
    }])
    result = _test(state)
    assert result.ok and result.full_suite_evidence
    assert len(result.legs) == len(modes)
    assert state.full_tested_head == state.git_expected_head
    assert conflict[3].execute(repo.GitPush()).ok


@pytest.mark.parametrize("failure", ["core", "cultivate"])
def test_conflict_each_full_suite_must_pass(conflict, failure):
    _, _, state, _, home = conflict
    _resolve(conflict)
    _configure_test_suites(home, state, test_suites=[{
        "name": "python", "default": True, "legs": [
            {"name": mode, "command": "/usr/bin/false" if mode == failure else "./suite",
             "env": {"MIMIR_PLUGINS": "" if mode == "core" else mode}}
            for mode in ("core", "cultivate")
        ],
    }])
    assert not _test(state).ok
    assert state.full_tested_head is None


def test_conflict_targeted_test_does_not_mark_full_suites(conflict):
    _, _, state, _, _ = conflict
    _resolve(conflict)
    assert _test(state, selectors=("tracked.txt",), suite="python").ok
    assert state.full_tested_head is None


def test_conflict_nondefault_suite_does_not_mark_full_suites(conflict):
    _, _, state, _, home = conflict
    _resolve(conflict)
    _configure_test_suites(home, state, test_suites=[{
        "name": "extra", "legs": [
            {"name": "core", "command": "./suite"},
            {"name": "cultivate", "command": "./suite", "env": {"MIMIR_PLUGINS": "cultivate"}},
        ],
    }])
    assert _test(state, suite="extra").ok
    assert state.full_tested_head is None


def test_conflict_dirty_snapshot_is_not_commit_test_evidence(conflict):
    _, _, state, _, _ = conflict
    _resolve(conflict)
    (state.checkout_lease.path / "tracked.txt").write_text("base property\nhead property\nextra\n")
    assert _test(state).ok
    assert state.full_tested_head is None


@pytest.mark.parametrize("change", ["dirty_then_clean", "dirty_after", "git_head", "state_head"])
def test_conflict_full_tests_bind_snapshot_and_final_commit(conflict, change):
    _, scope, state, _, _ = conflict
    _resolve(conflict)
    path = state.checkout_lease.path / "tracked.txt"
    clean = path.read_text()
    if change == "dirty_then_clean":
        path.write_text(clean + "untested commit tree\n")

    async def runner(*args, **kwargs):
        result = await _local_contained_runner(*args, **kwargs)
        if change == "dirty_then_clean":
            path.write_text(clean)
        elif change == "dirty_after":
            path.write_text(clean + "new edit\n")
        elif change == "git_head":
            root = state.checkout_lease.path
            # A different commit with the identical tree stays clean. Thus the
            # actual-HEAD comparison, not the dirty-tree check, must reject it.
            tree = _git(root, "rev-parse", "HEAD^{tree}")
            advanced = _git(root, "-c", "user.name=test", "-c", "user.email=test@example.test",
                            "commit-tree", tree, "-p", "HEAD", "-m", "same-tree concurrent commit")
            _git(root, "update-ref", scope.destination_ref, advanced)
        else:
            object.__setattr__(state, "git_expected_head", scope.observed_head_sha)
        return result

    result = asyncio.run(repo.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory,
    ).execute())
    assert state.full_tested_head is None
    assert result.ok


@pytest.mark.parametrize("outside", [False, True])
def test_conflict_scope_clean_merge_records_empty_proof_and_keeps_gates(tmp_path, monkeypatch, outside):
    from tests.test_repo_tools import _repo_scope_and_state
    from mimir_cultivate.repository import create_pr_checkout_lease
    from mimir_cultivate.policy import RepoReviewState

    origin, source, old_scope, old_state = _repo_scope_and_state(tmp_path)
    _git(source, "checkout", "main")
    (source / "base-only.txt").write_text("base change\n")
    _git(source, "add", "base-only.txt")
    _git(source, "commit", "-m", "advance base without conflicts")
    base = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "origin", "HEAD:main")
    scope = replace(old_scope, event_type="pr_mergeability_conflicting", observed_base_sha=base)
    state = RepoReviewState(scope)
    create_pr_checkout_lease(scope, owner="mimir-bot",
                             lease_root=old_state.checkout_lease.lease_root, review_state=state)
    tools = repo.RepoGitTools(state)
    assert tools.execute(repo.GitMerge()).ok
    proof = state.conflict_merge
    assert proof.conflicted_paths == frozenset()
    assert proof.original_head == scope.observed_head_sha
    assert proof.base == scope.observed_base_sha
    assert state.conflict_evidence_head == state.git_expected_head
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitPush())
    assert raised.value.code == "full_test_required"
    if outside:
        (state.checkout_lease.path / "outside.txt").write_text("unrelated\n")
        _git(state.checkout_lease.path, "add", "outside.txt")
        _git(state.checkout_lease.path, "-c", "user.name=test", "-c", "user.email=test@example.test",
             "commit", "-m", "unrelated edit")
        tools._refresh_expected_head()
        # Isolate the independent path boundary from the exact-head marker.
        state.record_conflict_evidence(scope.scope_id, state.git_expected_head)
    home = tmp_path / "home"
    _configure_test_suites(home, state, test_suites=[{
        "name": "default", "default": True, "command": "/usr/bin/true",
    }])
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE", raising=False)
    assert _test(state).full_suite_evidence
    if outside:
        with pytest.raises(repo.GitRefusal) as raised:
            tools.execute(repo.GitPush())
        assert raised.value.code == "conflict_resolution_outside_pr_paths"
    else:
        assert tools.execute(repo.GitPush()).ok
        assert _git(origin, "rev-parse", scope.destination_ref) == state.git_expected_head


def test_conflict_rebase_is_refused_without_starting(conflict):
    _, _, state, tools, _ = conflict
    assert tools.execute(repo.GitMergeAbort()).ok
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitRebase())
    assert raised.value.code == "rebase_in_lease_refused"
    assert not (state.checkout_lease.path / ".git/rebase-merge").exists()


def test_conflict_push_lease_rejects_concurrent_remote_rewind(conflict):
    origin, scope, state, _, _ = conflict
    _resolve(conflict)
    assert _test(state).ok
    observed = scope.observed_head_sha
    seen = []

    def race(argv, **kwargs):
        if "push" in argv:
            seen.append(argv)
            ancestor = _git(origin, "merge-base", observed, scope.observed_base_sha)
            _git(origin, "update-ref", scope.destination_ref, ancestor)
        return repo._bounded_subprocess_runner(argv, **kwargs)

    tools = repo.RepoGitTools(state, runner=race)
    with pytest.raises(repo.GitRefusal):
        tools.execute(repo.GitPush())
    assert len(seen) == 1
    assert f"--force-with-lease={scope.destination_ref}:{observed}" in seen[0]
    assert scope.observed_head_sha == observed


@pytest.mark.parametrize("author,eligible", [
    ("mimir-bot", True), ("worklink-bot", True), ("collaborator", False),
])
def test_conflict_scope_author_must_be_deployment_owned(monkeypatch, author, eligible):
    from mimir import access_control

    monkeypatch.setenv("MIMIR_GITHUB_SELF_LOGIN", "mimir-bot")
    monkeypatch.setenv("MIMIR_FACTORY_PUBLISHING_IDENTITY", "worklink-bot")
    monkeypatch.setattr(access_control, "_canonical_repo_binding_resolution", lambda _: SimpleNamespace(
        binding=("/repo", "https://github.com/o/r.git"),
    ))
    scope = access_control._repo_pr_scope(
        provenance="poller_payload", repo="o/r", principal=author,
        event_type="pr_mergeability_conflicting", number=42,
        head_repo="o/r", head_remote="origin", head_ref="worklink/42",
        head_sha="a" * 40, base_ref="main", base_sha="b" * 40,
        pr_author_is_trusted=True,
    )
    assert (scope is not None) is eligible
    if eligible:
        assert scope.principal == "mimir-bot"
        assert scope.pull_request_author == author


def test_conflict_evidence_cannot_omit_one_of_multiple_conflicted_paths(tmp_path):
    _, scope, old_state, _ = _two_sided_conflict_tools(tmp_path)
    source = Path(scope.canonical_root)
    _git(source, "checkout", "-q", scope.head_ref)
    (source / "base-test").write_text("#!/bin/sh\n# head test intent\nexit 0\n")
    _git(source, "add", "base-test")
    _git(source, "commit", "-qm", "add a second conflicting PR path")
    _git(source, "push", "-q", "origin", f"HEAD:{scope.destination_ref}")
    scope = replace(scope, observed_head_sha=_git(source, "rev-parse", "HEAD"))
    from mimir_cultivate.policy import RepoReviewState

    state = RepoReviewState(scope)
    repo.create_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=old_state.checkout_lease.lease_root,
        review_state=state,
    )
    tools = repo.RepoGitTools(state)
    with pytest.raises(repo.GitRefusal, match="merge conflict"):
        tools.execute(repo.GitMerge())
    assert state.conflict_merge.conflicted_paths == {"tracked.txt", "base-test"}
    root = state.checkout_lease.path
    (root / "tracked.txt").write_text("base property\nhead property\n")
    (root / "base-test").write_text("#!/bin/sh\n# head test intent\ngrep -q 'base property' tracked.txt\n")
    tools.execute(repo.GitStage(("tracked.txt", "base-test")))
    paths = ("tracked.txt", "base-test", "head-test", "suite")
    with pytest.raises(repo.GitRefusal) as raised:
        tools.execute(repo.GitCommit(paths, "incomplete evidence", _conflict_commit_evidence()))
    assert raised.value.code == "conflict_evidence_required"
    evidence = (*_conflict_commit_evidence(), repo.ConflictResolutionEvidence(
        "base-test", "base assertion", "grep base property", "head test intent", "retained comment",
    ))
    assert tools.execute(repo.GitCommit(paths, "complete evidence", evidence)).ok
    assert state.conflict_evidence_head == state.git_expected_head


def test_conflict_evidence_tool_schema_parses_per_path_entries():
    from langchain_core.utils.function_calling import convert_to_openai_tool

    schema = convert_to_openai_tool(repo.repo_commit)["function"]["parameters"]
    assert "conflict_evidence" in schema["properties"]
    parsed = repo.repo_commit.args_schema.model_validate({
        "repository": "o/r", "pull_request": 42, "paths": ["tracked.txt"], "message": "merge",
        "conflict_evidence": [{
            "path": "tracked.txt", "base_property": "base", "base_verification": "base check",
            "head_property": "head", "head_verification": "head check",
        }],
    })
    assert isinstance(parsed.conflict_evidence[0], repo.ConflictResolutionEvidence)


@pytest.mark.parametrize("bound", ["paths", "message"])
def test_conflict_evidence_aggregate_limits(conflict, bound):
    _, _, state, tools, _ = conflict
    count = repo._MAX_PATHS + 1 if bound == "paths" else 9
    value = "v" if bound == "paths" else "v" * 2048
    evidence = tuple(
        repo.ConflictResolutionEvidence(f"file-{n}", value, value, value, value)
        for n in range(count)
    )
    # This is a host-captured multi-path proof; only the index capture is
    # substituted to keep a resource-limit test independent of 257 Git files.
    object.__setattr__(state, "conflict_merge", replace(
        state.conflict_merge, conflicted_paths=frozenset(entry.path for entry in evidence),
    ))
    operation = repo.GitCommit(("tracked.txt",), "resolve", evidence)
    with pytest.raises(repo.GitRefusal) as raised:
        tools._merge_evidence_message(operation)
    assert raised.value.code == ("conflict_evidence_required" if bound == "paths" else "invalid_message")


def test_conflict_merge_can_commit_when_resolved_tree_matches_head(tmp_path):
    from mimir_cultivate.policy import RepoReviewState
    from tests.test_repo_tools import _repo_scope_and_state

    _, source, scope, previous = _repo_scope_and_state(tmp_path)
    _git(source, "checkout", "-q", "main")
    (source / "tracked.txt").write_text("equivalent base intent\n")
    _git(source, "commit", "-qam", "update base intent")
    _git(source, "push", "-q", "origin", "HEAD:main")
    scope = replace(scope, event_type="pr_mergeability_conflicting",
                    observed_base_sha=_git(source, "rev-parse", "HEAD"))
    state = RepoReviewState(scope)
    repo.create_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=previous.checkout_lease.lease_root,
        review_state=state,
    )
    tools = repo.RepoGitTools(state)
    with pytest.raises(repo.GitRefusal, match="merge conflict"):
        tools.execute(repo.GitMerge())
    (state.checkout_lease.path / "tracked.txt").write_text("pull request\n")
    tools.execute(repo.GitStage(("tracked.txt",)))
    assert _git(state.checkout_lease.path, "diff", "--cached") == ""
    evidence = (repo.ConflictResolutionEvidence(
        "tracked.txt", "equivalent base intent", "semantic equivalence verified",
        "pull request intent", "head contents retained",
    ),)
    assert tools.execute(repo.GitCommit(("tracked.txt",), "equivalent resolution", evidence)).ok
    assert _git(state.checkout_lease.path, "rev-parse", "HEAD^{tree}") == _git(
        state.checkout_lease.path, "rev-parse", f"{scope.observed_head_sha}^{{tree}}",
    )
    assert len(_git(state.checkout_lease.path, "show", "-s", "--format=%P").split()) == 2
