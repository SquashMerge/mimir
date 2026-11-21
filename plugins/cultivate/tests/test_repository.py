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


def _suite_declarations(*names: str, default: bool = False) -> list[dict]:
    return [
        {"name": name, "command": "/usr/bin/true", "default": default and index == 0}
        for index, name in enumerate(names)
    ]


def test_repo_test_rendered_suite_guidance(tmp_path, monkeypatch) -> None:
    from langchain_core.utils.function_calling import convert_to_openai_tool

    _, _, scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_test_suites(
        home, state, test_suites=_suite_declarations("python", "cultivate", "frontend"),
    )
    monkeypatch.setenv("MIMIR_HOME", str(home))
    selected = load_plugins(enable="cultivate").policy
    factory = selected.tools["repo_test"].factory
    tool = factory()
    rendered = convert_to_openai_tool(tool)["function"]
    assert "more than one test suite, pass exactly one" in rendered["description"]
    assert f"{scope.canonical_repo}: python, cultivate, frontend" in rendered["description"]
    parameter = rendered["parameters"]["properties"]["suite"]
    assert "more than one test suite, pass exactly one" in parameter["description"]
    assert "$MIMIR_HOME/repositories.yaml" in parameter["description"]
    assert "test_suites" in parameter["description"]
    assert "suite" not in rendered["parameters"].get("required", [])
    assert "runtime" not in rendered["parameters"]["properties"]

    # Materialization reads current declarations, not an import-time snapshot,
    # and never leaks the first deployment's hints into the shared tool.
    _configure_test_suites(home, state, test_suites=_suite_declarations("unit", default=True))
    assert f"{scope.canonical_repo}: unit" in factory().description
    assert "python, cultivate, frontend" not in factory().description
    assert "Configured suites by repository" not in repository.repo_test.description


@pytest.mark.parametrize("configuration", ["missing", "invalid", "no_home"])
def test_repo_test_unknown_suites_keep_generic_guidance(tmp_path, monkeypatch, configuration):
    home = tmp_path / "home"
    home.mkdir()
    if configuration == "invalid":
        (home / "repositories.yaml").write_text("repositories: not-a-list\n")
    monkeypatch.setenv("MIMIR_HOME", str(home) if configuration != "no_home" else "")
    tool = repository.build_repo_test_tool()
    assert "more than one test suite, pass exactly one" in tool.description
    assert "$MIMIR_HOME/repositories.yaml" in tool.description
    assert "Configured suites by repository" not in tool.description


@pytest.mark.parametrize("legacy", [False, True])
def test_repo_test_single_suite_can_be_omitted(tmp_path, monkeypatch, legacy):
    _, _, scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_test_suites(
        home, state,
        test_suites=[] if legacy else _suite_declarations("unit", default=True),
    )
    monkeypatch.setenv("MIMIR_HOME", str(home))
    command, _, _, suite, _ = repository._configured_command(scope.canonical_repo)
    assert command[0] == "/usr/bin/true"
    assert suite == ("default" if legacy else "unit")
    assert f"{scope.canonical_repo}: {suite}" in repository.build_repo_test_tool().description


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [False, True])
async def test_repo_test_suite_refusal_logging_has_no_publication_verdict(
    tmp_path, monkeypatch, scoped,
):
    from mimir.tools import budget_gate
    from mimir.tools.refusals import ToolPolicyRefusal

    _, _, scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_test_suites(
        home, state, test_suites=_suite_declarations("python", "cultivate", "frontend"),
    )
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.setattr(repository, "_state", lambda *args: state)

    def forbidden(*args, **kwargs):
        pytest.fail("suite selection refusal must precede snapshot creation")

    real_runner = repository.RepoProjectTests
    monkeypatch.setattr(repository, "RepoProjectTests", lambda state: real_runner(
        state, checkout_factory=forbidden,
    ))
    events = []
    monkeypatch.setattr(
        budget_gate, "_emit_event_sync",
        lambda kind, **fields: events.append((kind, fields)),
    )
    selectors = ("unmatched/",) if scoped else ()
    with pytest.raises(ToolPolicyRefusal, match="test_suite_selection_refused") as raised:
        await repository.repo_test.coroutine(
            scope.canonical_repo, scope.pr_number, selectors,
        )
    assert raised.value.__cause__.execution_started is False
    assert "python, cultivate, frontend" in str(raised.value)
    assert "No tests ran. Retry with exactly one declared suite" in str(raised.value)
    assert "Hold publication" not in str(raised.value)
    assert state.full_tested_head is None
    # Exercise the real event formatter used by the tool exception middleware.
    budget_gate._record_tool_outcome("repo_test", refused_reason=str(raised.value))
    hard = next(fields for kind, fields in events if kind == "hard_boundary_denied")
    assert "test_suite_selection_refused" in hard["reason"]
    assert "No tests ran" in hard["reason"]
    assert "Hold publication" not in json.dumps(events)


def test_repository_git_path_validation_stays_closed() -> None:
    with pytest.raises(repository.GitRefusal, match="repository-relative"):
        repository._validated_paths(("../outside",), required=True)


@pytest.fixture
def repository_git_policy():
    from mimir.plugins.core import policy_scope

    with policy_scope(load_plugins(enable="cultivate").policy):
        yield


@pytest.mark.usefixtures("repository_git_policy")
@pytest.mark.parametrize("already_staged", [False, True])
@pytest.mark.parametrize("name", ["deleted.txt", "literal[1]*\nfile.txt"])
def test_stage_and_commit_deletion(tmp_path: Path, already_staged: bool, name: str) -> None:
    _, _, scope, state = _repo_scope_and_state(tmp_path, base_files={name: b"retired\n"})
    checkout = state.checkout_lease.path
    tools = repository.RepoGitTools(state)
    (checkout / name).unlink()
    if already_staged:
        _git(checkout, "rm", "--cached", "--quiet", "--", f":(literal){name}")
    else:
        assert _git(checkout, "ls-files", "-z", "--", f":(literal){name}") == name + "\x00"

    assert tools.execute(repository.GitStage((name,))).ok
    assert _git(checkout, "diff", "--cached", "--name-only", "-z") == name + "\x00"
    assert tools.execute(repository.GitCommit((name,), "remove retired file")).ok
    assert name not in _git(checkout, "ls-tree", "-r", "--name-only", "-z", "HEAD").split("\x00")
    assert _git(checkout, "rev-parse", "HEAD^") == scope.observed_head_sha


@pytest.mark.usefixtures("repository_git_policy")
@pytest.mark.parametrize("replacement", ["file", "dangling_symlink"])
def test_stage_recreated_file_after_staged_deletion(tmp_path: Path, replacement: str) -> None:
    name = "deleted.txt"
    _, _, _, state = _repo_scope_and_state(tmp_path, base_files={name: b"retired\n"})
    checkout = state.checkout_lease.path
    tools = repository.RepoGitTools(state)
    _git(checkout, "rm", "--quiet", "--", name)
    if replacement == "file":
        (checkout / name).write_text("replacement\n", encoding="utf-8")
    else:
        (checkout / name).symlink_to("missing-target")

    assert tools.execute(repository.GitStage((name,))).ok
    assert tools.execute(repository.GitCommit((name,), "replace retired file")).ok
    assert _git(checkout, "show", f"HEAD:{name}") == (
        "replacement" if replacement == "file" else "missing-target"
    )


@pytest.mark.usefixtures("repository_git_policy")
@pytest.mark.parametrize("path", ["tracked.txt", "./tracked.txt"])
def test_stage_already_staged_rename_deletion(tmp_path: Path, path: str) -> None:
    _, _, _, state = _repo_scope_and_state(tmp_path)
    checkout = state.checkout_lease.path
    _git(checkout, "mv", "tracked.txt", "renamed.txt")
    tools = repository.RepoGitTools(state)

    assert tools.execute(repository.GitStage((path,))).ok
    assert tools.execute(repository.GitCommit(("tracked.txt", "renamed.txt"), "rename file")).ok
    assert _git(checkout, "ls-tree", "HEAD", "--", "tracked.txt") == ""
    assert _git(checkout, "show", "HEAD:renamed.txt") == "pull request"


@pytest.mark.usefixtures("repository_git_policy")
@pytest.mark.parametrize("commit", [False, True])
@pytest.mark.parametrize("with_deletion", [False, True])
def test_stage_never_existing_path_is_refused(
    tmp_path: Path, commit: bool, with_deletion: bool,
) -> None:
    _, _, scope, state = _repo_scope_and_state(tmp_path)
    checkout = state.checkout_lease.path
    tools = repository.RepoGitTools(state)
    paths = ("never-existed.txt",)
    if with_deletion:
        # A real staged deletion cannot make an unrelated missing path acceptable.
        _git(checkout, "rm", "--quiet", "--", "tracked.txt")
        paths = ("tracked.txt", *paths)
    operation = repository.GitCommit(paths, "invalid commit") if commit else repository.GitStage(paths)
    with pytest.raises(repository.GitRefusal) as refusal:
        tools.execute(operation)
    assert refusal.value.code == "git_failed"
    assert "did not match any files" in str(refusal.value)
    assert _git(checkout, "rev-parse", "HEAD") == scope.observed_head_sha


@pytest.mark.usefixtures("repository_git_policy")
def test_merge_deletion_stages_and_commits_with_two_parents(tmp_path: Path) -> None:
    name = "retired.txt"
    origin, source, scope, old_state = _repo_scope_and_state(
        tmp_path, base_files={name: b"retired\n"},
    )
    _git(source, "checkout", "-q", "main")
    _git(source, "rm", "--quiet", "--", name)
    (source / "tracked.txt").write_text("advanced base\n", encoding="utf-8")
    _git(source, "commit", "-qam", "delete retired file and advance base")
    base = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "-q", "origin", "HEAD:main")
    scope = replace(scope, observed_base_sha=base)
    state = RepoReviewState(scope)
    repository.create_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=old_state.checkout_lease.lease_root,
        review_state=state,
    )
    checkout = state.checkout_lease.path
    tools = repository.RepoGitTools(state)
    with pytest.raises(repository.GitRefusal) as conflict:
        tools.execute(repository.GitMerge())
    assert conflict.value.code == "merge_conflict"
    assert not (checkout / name).exists()
    assert _git(checkout, "ls-files", "--", name) == ""
    assert _git(checkout, "diff", "--cached", "--diff-filter=D", "--name-only") == name

    # The auto-staged deletion cannot bypass the unmerged-index proof rule.
    with pytest.raises(repository.GitRefusal) as refusal:
        tools.execute(repository.GitStage((name,)))
    assert refusal.value.code == "unproven_conflict_path"
    (checkout / "tracked.txt").write_text("resolved base and head\n", encoding="utf-8")
    assert tools.execute(repository.GitStage(("tracked.txt",))).ok
    assert tools.execute(repository.GitStage((name,))).ok
    assert tools.execute(repository.GitCommit(("tracked.txt", name), "merge main deletion")).ok

    assert _git(checkout, "log", "-1", "--format=%P").split() == [scope.observed_head_sha, base]
    assert _git(checkout, "ls-tree", "HEAD", "--", name) == ""
    assert _git(checkout, "show", "HEAD:tracked.txt") == "resolved base and head"
    assert _git(checkout, "status", "--porcelain") == ""
    assert _git(origin, "rev-parse", scope.destination_ref) == scope.observed_head_sha


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


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("cleanup_by", ["parent", "turn_end"])
@pytest.mark.parametrize("acquire_by", ["parent", "delegate"])
def test_delegated_cleanup_preserves_parent_pr_grants_until_turn_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, async_mode: bool, cleanup_by: str,
    acquire_by: str,
) -> None:
    from deepagents.middleware.subagents import _build_task_tool
    from langchain_core.messages import AIMessage
    from langchain_core.runnables import Runnable
    from mimir._deepagents_subagent_auth import _AuthContextRunnable, _wrap_task_tool
    from mimir.access_control import ToolRegistry
    from mimir.plugins.core import policy_scope
    from mimir_cultivate import forge
    from mimir_cultivate.forge import ReviewProjection, ReviewVerdict
    from tests.test_structured_subagents import _task_runtime

    selected = load_plugins(enable="cultivate").policy
    _origin, _source, scope, state = _repo_scope_and_state(
        tmp_path, event_type="pr_review_requested",
        allowed_operations=frozenset({"repo.inspect", "repo.checkout", "pr.review"}),
    )
    lease = state.checkout_lease
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    auth = replace(
        _untainted_ifc_auth(), principal=scope.principal, canonical_principal=scope.principal,
        repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
        repo_review_state=state, repo_pr_action_scope=scope,
    )
    auth = _repo_granted_auth(auth, scope, state, follow_on=acquire_by == "parent", policy=selected)
    if acquire_by == "delegate":
        # Start with neither an attached nor an on-disk checkout for this PR.
        assert repository.cleanup_pr_checkout_lease(lease, review_state=state)
        assert state.checkout_lease is None and not lease.path.exists()
        acquire = repository.acquire_pr_checkout_lease

        def local_acquire(*args, **kwargs):
            return acquire(*args, **kwargs, runner=lambda argv: subprocess.run(
                argv, capture_output=True, text=True, check=False, env=_fixture_git_env(),
            ))

        monkeypatch.setattr(repository, "acquire_pr_checkout_lease", local_acquire)
        monkeypatch.setattr(forge, "pr_trust_verdict", lambda *_: True)
    submitted = []

    def submit_review(bound, verdict, body):
        submitted.append((bound, verdict, body))
        return ReviewProjection("review", "mimir-bot", "COMMENTED", body, "now", bound.observed_head_sha)

    client = SimpleNamespace(
        submit_review=submit_review,
        get_pull_request_snapshot=lambda *_: SimpleNamespace(
            repo=scope.canonical_repo, number=scope.pr_number, state="open",
            head_sha=scope.observed_head_sha,
        ),
    )
    monkeypatch.setattr(forge, "_client", lambda _: client)
    monkeypatch.setattr(forge, "_client_for_repository", lambda _: client)
    cleanup_results = []

    class Reviewer(Runnable):
        def invoke(self, input, config=None, *, context=None, **kwargs):
            nonlocal lease
            assert context.repo_review_state is state
            if acquire_by == "delegate":
                assert state.checkout_lease is None
                args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}

                def checkout_handler(request):
                    result = repository.repo_checkout.func(
                        **args, runtime=SimpleNamespace(context=context),
                    )
                    assert result["status"] == "checked_out"
                    # The fixture uses a local bare origin, not github.com.
                    # Supply its trusted transport provenance, as in the
                    # checkout-classification test below; grants and the real
                    # acquired lease still pass through BudgetGate unchanged.
                    from mimir.access_control import publish_protected_result
                    from mimir.models import SourceLabel

                    publish_protected_result((SourceLabel(
                        principal=context.canonical_principal, domain="repository",
                        resource_id=f"{scope.canonical_repo}#pull/{scope.pr_number}@{scope.observed_head_sha}",
                        bridge_instance="forge", sensitivity="internal",
                        authorized_principals=frozenset({context.canonical_principal}),
                        source_kind="protected_tool", integrity="trusted", integrity_effect="active_ingest",
                    ),))
                    return ToolMessage(content=json.dumps(result), tool_call_id=request.tool_call["id"])

                result = BudgetGateMiddleware(policy=selected).wrap_tool_call(
                    _make_request("repo_checkout", "delegate-checkout", context, args), checkout_handler,
                )
                assert result.status == "success", result.content
                lease = state.checkout_lease
                assert lease is not None and lease.is_active and lease.path.is_dir()
            cleanup_results.append(repository.repo_cleanup.func(
                scope.canonical_repo, scope.pr_number, runtime=SimpleNamespace(context=context),
            ))
            return {"messages": [AIMessage(content="Read-only review complete")]}

        async def ainvoke(self, input, config=None, *, context=None, **kwargs):
            # Match LangChain's thread-offloaded sync repository tools.
            return await asyncio.to_thread(self.invoke, input, config, context=context)

    task = _wrap_task_tool(_build_task_tool([{
        "name": "reviewer", "description": "Read-only reviewer",
        "runnable": _AuthContextRunnable(Reviewer()),
    }]))
    turn = _ifc_turn(auth)
    token = set_current_turn(turn)
    try:
        with policy_scope(selected):
            kwargs = dict(description="Review this PR", subagent_type="reviewer", runtime=_task_runtime(auth))
            if async_mode:
                asyncio.run(task.coroutine(**kwargs))
            else:
                task.func(**kwargs)

            # Check the real scoped-grant boundary first: restoring eager
            # revocation must fail with the incident's repo_pr_grant_denied,
            # not merely a different cleanup status.
            args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}
            for name in ("pr_metadata", "pr_inline_review_comment", "pr_submit_review"):
                decision = ToolRegistry().authorize_tool(name, auth, enforce=True, arguments=args, policy=selected)
                assert decision.allowed, decision.reason

            gate = BudgetGateMiddleware(policy=selected)

            def handler(request):
                result = forge.pr_submit_review.func(
                    **args, verdict=ReviewVerdict.COMMENT, body="Reviewed; no findings",
                    runtime=SimpleNamespace(context=auth),
                )
                return ToolMessage(content=json.dumps(result), tool_call_id=request.tool_call["id"])

            result = gate.wrap_tool_call(_make_request(
                "pr_submit_review", "parent-review", auth,
                {**args, "verdict": "comment", "body": "Reviewed; no findings"},
            ), handler)
            assert result.status == "success", result.content
            assert submitted == [(scope, ReviewVerdict.COMMENT, "Reviewed; no findings")]
            assert cleanup_results == [{"status": "deferred", "scope_id": scope.scope_id}]
            assert state.checkout_lease is lease and lease.is_active and lease.path.is_dir()
            if cleanup_by == "parent":
                assert repository.repo_cleanup.func(
                    **args, runtime=SimpleNamespace(context=auth),
                )["removed"] is True
            else:
                selected.turn_hooks[("cultivate", "pr_checkout_cleanup")](auth)
            assert state.checkout_lease is None and not lease.is_active
            assert not lease.path.exists()
            # Repeated turn-end cleanup is harmless.
            selected.turn_hooks[("cultivate", "pr_checkout_cleanup")](auth)
    finally:
        reset_current_turn(token)


def test_deferred_cleanup_only_drains_requested_states(tmp_path: Path) -> None:
    from mimir_cultivate.policy import ServerDiscoveredPRStates

    states = []
    for name in ("requested", "unrequested"):
        root = tmp_path / name
        root.mkdir()
        states.append(_repo_scope_and_state(root)[3])
    requested, unrequested = states
    leases = [state.checkout_lease for state in states]
    requested.defer_checkout_cleanup()
    discovered = ServerDiscoveredPRStates()
    discovered.remember(requested)
    # Deduplicate the requested state's aliases and keep an unrequested lease.
    auth = replace(
        _untainted_ifc_auth(), repo_review_state=requested,
        server_discovered_pr_states=discovered,
        repo_pr_scope_registry=RepoPRScopeRegistry((unrequested,)),
    )
    repository.cleanup_deferred_pr_checkouts(auth)
    assert requested.checkout_lease is None and not leases[0].path.exists()
    assert not requested.checkout_cleanup_deferred
    assert unrequested.checkout_lease is leases[1] and leases[1].is_active
    assert leases[1].path.exists()


def test_failed_deferred_cleanup_does_not_strand_other_requests(tmp_path: Path, monkeypatch) -> None:
    states = []
    for name in ("failed", "cleaned"):
        root = tmp_path / name
        root.mkdir()
        state = _repo_scope_and_state(root)[3]
        state.defer_checkout_cleanup()
        states.append(state)
    failed, cleaned = states
    failed_lease, cleaned_lease = (state.checkout_lease for state in states)
    cleanup = repository.cleanup_pr_checkout_lease

    def fail_one(lease, **kwargs):
        if lease is failed_lease:
            raise OSError("cleanup unavailable")
        return cleanup(lease, **kwargs)

    monkeypatch.setattr(repository, "cleanup_pr_checkout_lease", fail_one)
    # Separate carriers exercise all turn-end state sources, even though these
    # independent fixture checkouts deliberately share a repository/PR number.
    from mimir_cultivate.policy import ServerDiscoveredPRStates

    discovered = ServerDiscoveredPRStates()
    discovered.remember(cleaned)
    auth = replace(
        _untainted_ifc_auth(), repo_review_state=failed,
        server_discovered_pr_states=discovered,
    )
    repository.cleanup_deferred_pr_checkouts(auth)
    assert failed.checkout_lease is failed_lease and failed_lease.is_active
    assert failed.checkout_cleanup_deferred and failed_lease.path.exists()
    assert cleaned.checkout_lease is None and not cleaned_lease.path.exists()
    # A subsequent host cleanup can still reclaim the unchanged failed lease.
    monkeypatch.setattr(repository, "cleanup_pr_checkout_lease", cleanup)
    repository.cleanup_deferred_pr_checkouts(auth)
    assert failed.checkout_lease is None and not failed_lease.path.exists()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("carrier", ["direct", "registry", "discovered"])
def test_delegate_cannot_reacquire_and_revoke_parent_checkout(
    tmp_path: Path, monkeypatch, async_mode: bool, carrier: str,
) -> None:
    from mimir._deepagents_subagent_auth import _wrap_task_tool
    from mimir.tools.refusals import ToolPolicyRefusal
    from mimir_cultivate import forge

    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    from mimir_cultivate.policy import ServerDiscoveredPRStates

    discovered = ServerDiscoveredPRStates()
    if carrier == "discovered":
        discovered.remember(state)
    auth = replace(
        _untainted_ifc_auth(), repo_pr_action_scope=scope,
        repo_review_state=state if carrier == "direct" else None,
        repo_pr_scope_registry=RepoPRScopeRegistry((state,)) if carrier == "registry" else None,
        server_discovered_pr_states=discovered,
    )

    def preflight(*_):
        # This entry point can replace/revoke the shared lease.
        lease.revoke()
        state.revoke_checkout_lease(lease)
        pytest.fail("delegate entered lease-replacing checkout preflight")

    def checkout(**kwargs):
        return repository.checkout_scoped_pr(scope.canonical_repo, scope.pr_number, kwargs["runtime"])

    async def acheckout(**kwargs):
        return await asyncio.to_thread(checkout, **kwargs)

    monkeypatch.setattr(forge, "remediation_checkout_preflight", preflight)
    task = _wrap_task_tool(SimpleNamespace(func=checkout, coroutine=acheckout))
    with pytest.raises(ToolPolicyRefusal, match="checkout lifecycle belongs to the parent"):
        if async_mode:
            asyncio.run(task.coroutine(runtime=SimpleNamespace(context=auth)))
        else:
            task.func(runtime=SimpleNamespace(context=auth))
    assert state.checkout_lease is lease and lease.is_active


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["success", "error", "cancel"])
async def test_parent_turn_end_drains_delegated_cleanup(
    tmp_path: Path, monkeypatch, ending: str,
) -> None:
    from langchain_core.messages import AIMessage
    from mimir._context import get_current_turn
    from mimir._deepagents_subagent_auth import _wrap_task_tool
    from mimir.models import AgentEvent
    from tests.test_agent import _build_agent, _FakeAgent, _FakeSaga

    manager = load_plugins(enable="cultivate")
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))

    class Parent(_FakeAgent):
        async def astream(self, input, *, config, context=None, stream_mode="values"):
            turn = get_current_turn()
            turn.auth_context = replace(
                context, repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
                repo_review_state=state, repo_pr_action_scope=scope,
            )
            task = _wrap_task_tool(SimpleNamespace(
                func=lambda **kwargs: repository.repo_cleanup.func(
                    scope.canonical_repo, scope.pr_number, kwargs["runtime"],
                ), coroutine=None,
            ))
            assert task.func(runtime=SimpleNamespace(context=turn.auth_context))["status"] == "deferred"
            assert lease.is_active and lease.path.exists()
            if ending == "error":
                raise RuntimeError("parent failed after delegation")
            if ending == "cancel":
                raise asyncio.CancelledError
            async for item in super().astream(input, config=config, context=context, stream_mode=stream_mode):
                yield item

    agent = _build_agent(tmp_path, fake_agent=Parent([AIMessage(content="Done")]), fake_saga=_FakeSaga())
    agent._config = replace(agent._config, plugin_manager=manager)
    event = AgentEvent(trigger="user_message", channel_id="ch-1", content="Review this PR")
    if ending == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await agent.run_turn(event)
    else:
        await agent.run_turn(event)
    assert state.checkout_lease is None and not lease.is_active
    assert not lease.path.exists()


@pytest.mark.parametrize("async_mode", [False, True])
def test_trusted_pr_checkout_and_cleanup_do_not_taint_or_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, async_mode: bool,
) -> None:
    from mimir import trust_approval
    from mimir.access_control import SinkCategory, SinkGate, publish_protected_result
    from mimir.models import SourceLabel
    from mimir.plugins.core import policy_scope

    selected = load_plugins(enable="cultivate").policy
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    auth = replace(
        _untainted_ifc_auth(), repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
        repo_review_state=state, repo_pr_action_scope=scope,
    )
    auth = _repo_granted_auth(auth, scope, state, follow_on=False, policy=selected)
    auth.ifc_state.pr_checkout_author_trust[scope.scope_id] = True
    trusted_source = SourceLabel(
        principal=auth.canonical_principal, domain="repository",
        resource_id=f"{scope.canonical_repo}#pull/{scope.pr_number}@{scope.observed_head_sha}",
        bridge_instance="forge", sensitivity="internal",
        authorized_principals=frozenset({auth.canonical_principal}),
        source_kind="protected_tool", integrity="trusted", integrity_effect="active_ingest",
    )
    prompts = []
    monkeypatch.setattr(trust_approval, "register_chat_request", lambda *args: prompts.append(args))
    turn = _ifc_turn(auth)
    middleware = BudgetGateMiddleware(selected)
    calls = []

    def handler(request):
        nonlocal lease
        operation = request.tool_call["name"]
        calls.append(operation)
        if operation == "repo_checkout":
            # The local Git transport fixture supplies the trusted server scope;
            # production classification and the live checkout lease stay real.
            lease, _ = repository.acquire_pr_checkout_lease(
                scope, owner=scope.principal, lease_root=lease.lease_root, review_state=state,
                runner=lambda argv: subprocess.run(
                    argv, capture_output=True, text=True, check=False, env=_fixture_git_env(),
                ),
            )
            assert_lease_attested(auth, scope, lease)
            publish_protected_result((trusted_source,))
            content = {"status": "checked_out", "path": str(lease.path)}
        elif operation == "repo_cleanup":
            content = repository.repo_cleanup.func(
                scope.canonical_repo, scope.pr_number, runtime=SimpleNamespace(context=auth),
            )
        return ToolMessage(content=json.dumps(content), tool_call_id=request.tool_call["id"])

    async def async_handler(request):
        return handler(request)

    token = set_current_turn(turn)
    try:
        with policy_scope(selected):
            for operation in ("repo_checkout", "repo_cleanup"):
                request = _make_request(
                    operation, operation, auth,
                    {"repository": scope.canonical_repo, "pull_request": scope.pr_number},
                )
                result = (
                    asyncio.run(middleware.awrap_tool_call(request, async_handler)) if async_mode
                    else middleware.wrap_tool_call(request, handler)
                )
                assert result.status != "error", result.content
                if operation == "repo_checkout":
                    assert trusted_source in turn.ifc_labels.sources
                assert not turn.ifc_labels.has_untrusted_active_ingest
            forge_flow = SinkGate.check_sink_flow(
                "pr_comment", scope.canonical_repo, turn.ifc_labels, auth, enforce=True,
                sink_category=SinkCategory.FORGE, repo_pr_action_scope=scope,
            )
            assert forge_flow.allowed, forge_flow.reason
    finally:
        reset_current_turn(token)
    assert calls == ["repo_checkout", "repo_cleanup"]
    assert state.checkout_lease is None and not os.path.lexists(lease.path)
    assert not auth.ifc_state.current().has_untrusted_active_ingest
    assert prompts == []


@pytest.mark.parametrize("message", [False, True], ids=["raw", "tool_message"])
@pytest.mark.parametrize("invalid", [
    None, "extra_key", "missing_key", "removed_int", "scope_type", "wrong_scope_id",
    "wrong_status", "other_owner", "would_block", "disallowed", "wrong_decision",
    "wrong_repository", "wrong_pr", "missing_state", "failed", "refusal", "exception",
    "error_message", "malformed_message", "untrusted_provenance", "untrusted_artifact",
])
def test_repo_cleanup_exempts_only_exact_server_success(tmp_path, monkeypatch, message, invalid) -> None:
    from copy import copy
    from mimir.access_control import (
        OperationDecision, ProtectedResultProvenance, ToolAuthorization, classify_protected_result,
    )
    from mimir.models import SourceLabel
    from mimir.tools.refusals import ToolPolicyRefusal

    selected = load_plugins(enable="cultivate").policy
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(state.checkout_lease.lease_root))
    auth = replace(
        _untainted_ifc_auth(), repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
        repo_review_state=state, repo_pr_action_scope=scope,
    )
    auth.ifc_state.pr_checkout_author_trust[scope.scope_id] = True
    repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=state.checkout_lease.lease_root, review_state=state,
        runner=lambda argv: subprocess.run(
            argv, capture_output=True, text=True, check=False, env=_fixture_git_env(),
        ),
    )
    from mimir.plugins.core import policy_scope
    with policy_scope(selected):
        assert_lease_attested(auth, scope, state.checkout_lease)
    arguments = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}
    authorization = ToolAuthorization(
        "repo_cleanup", OperationDecision.RESOURCE_SCOPED, True,
        policy=selected, repo_pr_action_scope=scope,
    )
    payload = {"status": "cleaned", "removed": True, "scope_id": scope.scope_id}
    # A positive control with the same live state prevents earlier admission
    # failures from masking shape/owner regressions.
    assert classify_protected_result(
        "repo_cleanup", arguments, auth, authorization, result=payload,
    ) is None
    failed = False
    refusal = None
    provenance = None
    if invalid == "extra_key":
        payload["content"] = "untrusted content"
    elif invalid == "missing_key":
        payload.pop("removed")
    elif invalid == "removed_int":
        payload["removed"] = 1
    elif invalid == "scope_type":
        payload["scope_id"] = 7
    elif invalid == "wrong_scope_id":
        payload["scope_id"] = "another_scope"
    elif invalid == "wrong_status":
        payload["status"] = "refused"
    elif invalid == "other_owner":
        selected = copy(selected)
        selected._tools = dict(selected.tools)
        selected._tools["repo_cleanup"] = replace(selected.tools["repo_cleanup"], owner="other_plugin")
        authorization = replace(authorization, policy=selected)
    elif invalid == "would_block":
        authorization = replace(authorization, would_block=True)
    elif invalid == "disallowed":
        authorization = replace(authorization, allowed=False)
    elif invalid == "wrong_decision":
        authorization = replace(authorization, decision=OperationDecision.OPEN)
    elif invalid == "wrong_repository":
        arguments["repository"] = "other/repository"
    elif invalid == "wrong_pr":
        arguments["pull_request"] += 1
    elif invalid == "missing_state":
        auth = replace(auth, repo_review_state=None, repo_pr_scope_registry=None)
    elif invalid == "failed":
        failed = True
    elif invalid == "refusal":
        refusal = ToolPolicyRefusal("refused cleanup")
    if message:
        payload = ToolMessage(content=json.dumps(payload), tool_call_id="cleanup")
    if invalid == "exception":
        payload = RuntimeError("cleanup failed with untrusted content")
        failed = True
    elif invalid == "refusal":
        payload = ToolMessage(content=str(refusal), tool_call_id="cleanup", status="error")
    elif invalid == "error_message":
        payload = ToolMessage(content=json.dumps({"status": "cleaned", "removed": True,
                                                "scope_id": scope.scope_id}),
                              tool_call_id="cleanup", status="error")
    elif invalid == "malformed_message":
        payload = ToolMessage(content="untrusted text", tool_call_id="cleanup")
    elif invalid in {"untrusted_provenance", "untrusted_artifact"}:
        provenance = ProtectedResultProvenance((SourceLabel(
            principal=None, domain="web", resource_id="external", bridge_instance=None,
            sensitivity="public", source_kind="protected_tool", integrity="untrusted",
            integrity_effect="active_ingest",
        ),))
        if invalid == "untrusted_artifact":
            payload = ToolMessage(content=json.dumps({"status": "cleaned", "removed": True,
                                                    "scope_id": scope.scope_id}),
                                  tool_call_id="cleanup", artifact=provenance)
            provenance = None
    labels = classify_protected_result(
        "repo_cleanup", arguments, auth, authorization, result=payload,
        failed=failed, policy_refusal=refusal, provenance=provenance,
    )
    if invalid is None:
        assert labels is None
    else:
        assert labels is not None and labels.has_untrusted_active_ingest
        if invalid not in {"untrusted_provenance", "untrusted_artifact"}:
            assert any(source.resource_id == "<unresolved-resource:protected_tool:repo_cleanup>"
                       for source in labels.sources)


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


@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/tags/fixture-release"])
@pytest.mark.parametrize("reachable_from", ["source", "origin"])
def test_moved_source_ref_does_not_force_preservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ref: str, reachable_from: str,
) -> None:
    source, _original, scope, lease = _rebased_divergent_lease(tmp_path, production_clone=True)
    inherited = _git(lease.path, "rev-parse", ref)
    (source / "later.txt").write_text("source advanced again\n")
    _git(source, "add", "later.txt")
    _git(source, "commit", "-qm", "advance source refs")
    _git(source, "update-ref", ref, "HEAD")
    if reachable_from == "source":
        # Remove origin reachability to isolate the source-current-refs lane.
        _git(lease.path, "update-ref", "-d", "refs/remotes/origin/main")
    else:
        # Move source refs to unrelated history. The inherited tip is now
        # redundant solely through the lease's published origin tracking ref.
        _git(source, "update-ref", "refs/heads/main", scope.observed_head_sha)
        _git(source, "update-ref", "refs/tags/fixture-release", scope.observed_head_sha)
    assert _git(source, "rev-parse", ref) != inherited
    events = []
    monkeypatch.setattr(repository, "_report_retained_candidates",
                        lambda event, *_, **_details: events.append(event))
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root,
        review_state=RepoReviewState(scope),
    )
    assert events == ["pr_checkout_lease_discarded_equivalent"]
    assert not lease.path.exists() and fresh.path != lease.path and candidates == ()
    assert not (lease.lease_root / ".recovery").exists()


@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/tags/fixture-release"])
@pytest.mark.parametrize("control", ["missing_name", "unreachable_source_object"])
def test_inherited_ref_without_source_reachability_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ref: str, control: str,
) -> None:
    source, _original, scope, lease = _rebased_divergent_lease(tmp_path, production_clone=True)
    inherited = _git(lease.path, "rev-parse", ref)
    if control == "missing_name":
        _git(source, "update-ref", "-d", ref)
        # Published reachability alone must not exempt a name absent in source.
        assert not _git(lease.path, "rev-list", inherited, "--not", "--remotes=origin", "--")
    else:
        _git(lease.path, "update-ref", "-d", "refs/remotes/origin/main")
        _git(source, "update-ref", "refs/heads/main", scope.observed_head_sha)
        _git(source, "update-ref", "refs/tags/fixture-release", scope.observed_head_sha)
        _git(source, "update-ref", "-d", "refs/remotes/origin/main")
        assert _git(source, "cat-file", "-t", inherited) == "commit"
        assert _git(source, "rev-parse", ref) != inherited
        assert _git(source, "rev-list", inherited, "--not", "--branches", "--tags",
                    "--remotes=origin", "--")
        assert _git(lease.path, "rev-list", inherited, "--not", "--remotes=origin", "--")
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    events = []
    monkeypatch.setattr(repository, "_report_retained_candidates",
                        lambda event, *_, **_details: events.append(event))
    state = RepoReviewState(scope)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root, review_state=state,
    )
    notice, = state.checkout_recovery_notices
    assert events == ["pr_checkout_lease_preserved_divergent"]
    assert fresh.path != lease.path and not lease.path.exists() and candidates == ()
    assert _git(fresh.path, "rev-parse", "HEAD") == scope.observed_head_sha
    import shutil
    import tarfile

    restored = tmp_path / "restored"
    restored.mkdir()
    with tarfile.open(notice["archive"]) as archive:
        archive.extractall(restored, filter="data")
    tree = restored / "worktree"
    seed = tmp_path / "published.git"
    subprocess.run(["git", "clone", "--bare", scope.canonical_origin, str(seed)],
                   check=True, capture_output=True, env=_fixture_git_env())
    _git(seed, "bundle", "unbundle", notice["bundle"])
    shutil.copytree(seed / "objects", tree / ".git/objects")
    assert _git(tree, "rev-parse", ref) == inherited
    assert _git(tree, "show", f"{ref}:base-update.txt") == "advanced base"


@pytest.mark.parametrize("reflog_state", ["expired", "replay", "unique_commit"])
def test_behind_head_with_untracked_file_recovers_empty_incremental_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reflog_state: str,
) -> None:
    import shutil
    import tarfile

    source, original, scope, lease = _rebased_divergent_lease(tmp_path, production_clone=True)
    replay = _git(lease.path, "rev-parse", "HEAD")
    reflog_commits = [replay]
    if reflog_state == "unique_commit":
        (lease.path / "reflog-only.txt").write_text("unique reflog draft\n")
        _git(lease.path, "add", "reflog-only.txt")
        _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
             "commit", "-qm", "reflog-only work")
        reflog_commits.append(_git(lease.path, "rev-parse", "HEAD"))
    _git(lease.path, "reset", "--hard", original.observed_base_sha)
    if reflog_state == "expired":
        _git(lease.path, "reflog", "expire", "--expire=now", "--all")
        reflog_commits = []
    # Every advertised ref tip is published, regardless of reflog-only work.
    assert not _git(lease.path, "rev-list", "--all", "HEAD",
                    "--not", "--remotes=origin", "--")
    unpublished = _git(lease.path, "rev-list", "--all", "HEAD", "--reflog",
                       "--not", "--remotes=origin", "--").splitlines()
    assert set(reflog_commits) <= set(unpublished)
    if reflog_state == "expired":
        assert not unpublished
    (lease.path / "draft.txt").write_text("untracked draft survives\n")
    monkeypatch.setattr(repository, "_observe_current_pr_head",
                        lambda _scope: scope.observed_head_sha)
    state = RepoReviewState(scope)
    fresh, candidates = repository.acquire_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=lease.lease_root, review_state=state,
    )
    notice, = state.checkout_recovery_notices
    assert fresh.path != lease.path and not lease.path.exists() and candidates == ()
    assert _git(fresh.path, "rev-parse", "HEAD") == scope.observed_head_sha
    assert _git(source, "rev-parse", notice["ref"]) == original.observed_base_sha
    assert Path(notice["patch"]).is_file()
    bundle = Path(notice["bundle"])
    assert bundle.is_file()
    # Full fallback must be self-contained, not an empty placeholder.
    header = bundle.read_bytes().split(b"\n\n", 1)[0]
    assert not any(line.startswith(b"-") for line in header.splitlines())
    restored = tmp_path / "restored"
    restored.mkdir()
    with tarfile.open(notice["archive"]) as archive:
        archive.extractall(restored, filter="data")
    tree = restored / "worktree"
    seed = tmp_path / "empty.git"
    seed.mkdir()
    _git(seed, "init", "--bare")
    _git(seed, "bundle", "verify", str(bundle))
    _git(seed, "bundle", "unbundle", str(bundle))
    # Verify object presence in an empty object store, not the original lease
    # or an origin clone that could mask missing bundle contents.
    for commit in reflog_commits:
        assert _git(seed, "cat-file", "-t", commit) == "commit"
    shutil.copytree(seed / "objects", tree / ".git/objects")
    for commit in reflog_commits:
        assert commit in _git(tree, "reflog", "show", "--format=%H", "HEAD").splitlines()
        assert _git(tree, "show", f"{commit}:tracked.txt") == "pull request"
    if reflog_state == "unique_commit":
        assert _git(tree, "show", f"{reflog_commits[-1]}:reflog-only.txt") == "unique reflog draft"
    assert (tree / "draft.txt").read_text() == "untracked draft survives\n"
    assert _git(tree, "rev-parse", "HEAD") == original.observed_base_sha
    assert _git(tree, "show", "HEAD:tracked.txt")
    from mimir.plugins.core import policy_scope
    with policy_scope(load_plugins(enable="cultivate").policy):
        assert_lease_attested(_lineage_turn(scope, state), scope, fresh)


@pytest.mark.parametrize("state_kind", ["stash", "two_stashes", "ignored", "branch", "tag"])
def test_divergent_hidden_state_is_recoverable(tmp_path: Path, state_kind: str) -> None:
    import tarfile

    _source, _original, scope, lease = _rebased_divergent_lease(tmp_path)
    if state_kind == "ignored":
        (lease.path / ".git/info/exclude").write_text("secret-draft.txt\n")
        (lease.path / "secret-draft.txt").write_text("ignored work\n")
    else:
        (lease.path / "tracked.txt").write_text("hidden work\n")
        if state_kind in {"stash", "two_stashes"}:
            _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
                 "stash", "push", "-m", "saved draft")
            if state_kind == "two_stashes":
                (lease.path / "tracked.txt").write_text("newer hidden work\n")
                _git(lease.path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
                     "stash", "push", "-m", "second draft")
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
        ref = "refs/stash" if state_kind in {"stash", "two_stashes"} else (
            "refs/heads/saved-draft" if state_kind == "branch" else "refs/tags/saved-draft"
        )
        expected = "newer hidden work" if state_kind == "two_stashes" else "hidden work"
        assert _git(tree, "show", f"{ref}:tracked.txt") == expected
        if state_kind == "two_stashes":
            assert _git(tree, "show", "stash@{1}:tracked.txt") == "hidden work"
        heads = _git(tree, "bundle", "list-heads", notices[0]["bundle"])
        assert ref in heads


@pytest.mark.parametrize("gate", [
    "scoped", "base", "substitution", "recovery_ref", "diff",
    "recovery_revisions", "bundle_create", "bundle_verify",
])
def test_divergent_probe_errors_preserve_lease(tmp_path: Path, gate: str) -> None:
    _source, original, scope, lease = _rebased_divergent_lease(tmp_path)
    if gate in {"recovery_ref", "recovery_revisions", "bundle_create", "bundle_verify"}:
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
            or (gate == "recovery_revisions" and argv[3:] == [
                "rev-list", "--objects", "--no-object-names", "--max-count=1",
                "--all", "HEAD", "--not", "--remotes=origin", "--",
            ])
            or (gate == "bundle_create" and argv[3:5] == ["bundle", "create"])
            or (gate == "bundle_verify" and argv[3:5] == ["bundle", "verify"])
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
@pytest.mark.parametrize("stage", ["factory", "cleanup", "config_refusal_cleanup"])
async def test_repo_test_checkout_io_keeps_event_loop_progressing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str,
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    home = tmp_path / "home"
    command = "/usr/bin/true"
    if stage == "config_refusal_cleanup":
        command += f" {lease.path}"
    _configure_worklink_test(home, command)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    factory_arguments = []

    def blocking_io():
        loop.call_soon_threadsafe(entered.set)
        try:
            # A finite guard lets the synchronous mutation fail without hanging.
            assert release.wait(3), "checkout IO blocked the event-loop ticker"
        finally:
            finished.set()

    def checkout(source, **kwargs):
        factory_arguments.append((source, kwargs))
        if stage == "factory":
            blocking_io()
        return SimpleNamespace(
            path=source, capability=SimpleNamespace(path=source),
            close=blocking_io if stage != "factory" else lambda: None,
        )

    async def runner(*args, **kwargs):
        return CollectedExecutionResult(0, b"ok", b"", False, False, 0, 0)

    task = asyncio.create_task(repository.RepoProjectTests(
        state, runner=runner, checkout_factory=checkout,
    ).execute())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        # This coroutine is the ticker: it must run while IO is still blocked,
        # not merely after a queued callback finally runs on a stalled loop.
        assert not finished.is_set()
    finally:
        release.set()
        if stage == "config_refusal_cleanup":
            with pytest.raises(repository.ProjectTestRefusal) as refusal:
                await task
            assert refusal.value.code == "test_config_invalid"
        else:
            assert (await task).ok
    assert factory_arguments == [(lease.path, {
        "scope_id": scope.scope_id, "pr_number": scope.pr_number, "known_sensitive": (),
    })]
    assert state.checkout_lease is lease
    assert lease.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("during", ["factory", "runner", "cleanup"])
async def test_repo_test_cancellation_settles_checkout_and_removes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, during: str,
) -> None:
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    _origin, _source, _scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    home = tmp_path / "home"
    _configure_worklink_test(home)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    loop = asyncio.get_running_loop()
    factory_entered = asyncio.Event()
    runner_entered = asyncio.Event()
    cleanup_entered = asyncio.Event()
    release_factory = threading.Event()
    release_cleanup = threading.Event()
    path = tmp_path / "snapshots" / "attempt" / "checkout"
    closed = []

    def checkout(source, **kwargs):
        path.mkdir(parents=True)
        (path / "partial").write_text("half-created snapshot")
        loop.call_soon_threadsafe(factory_entered.set)
        assert release_factory.wait(5)
        contained = repository.ContainedCheckout(
            path, SimpleNamespace(close=lambda: closed.append(True)),
            SimpleNamespace(destination=path),
        )

        def close():
            loop.call_soon_threadsafe(cleanup_entered.set)
            assert release_cleanup.wait(5)
            contained.close()

        return SimpleNamespace(path=path, capability=contained.capability, close=close)

    async def runner(*args, **kwargs):
        assert during != "factory", "cancelled snapshot must not launch a worker"
        runner_entered.set()
        if during == "runner":
            await asyncio.Event().wait()
        return CollectedExecutionResult(0, b"ok", b"", False, False, 0, 0)

    task = asyncio.create_task(repository.RepoProjectTests(
        state, runner=runner, checkout_factory=checkout,
    ).execute())
    try:
        await asyncio.wait_for(factory_entered.wait(), 5)
        assert path.exists()
        if during == "factory":
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        release_factory.set()
        if during == "runner":
            await asyncio.wait_for(runner_entered.wait(), 5)
            task.cancel()
        await asyncio.wait_for(cleanup_entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "cancellation must wait for checkout removal"
        assert path.exists()
    finally:
        release_factory.set()
        release_cleanup.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert closed == [True]
    assert not path.parent.exists()
    assert state.checkout_lease is lease
    assert lease.path.exists()
    assert state.full_tested_head is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure, code, reason, message", [
    (repository.SnapshotCredentialsRefused(1), "test_snapshot_credentials_refused",
     "snapshot_credentials", "project test snapshot contains credential-like material"),
    (repository.SnapshotEmbeddedRepository(), "test_snapshot_embedded_repository",
     "snapshot_embedded_repository", "project test snapshot source contains an embedded Git repository"),
    (OSError("private path"), "test_snapshot_unavailable",
     "snapshot_unavailable", "project test snapshot is unavailable"),
])
async def test_repo_test_threaded_factory_preserves_refusals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    failure: Exception, code: str, reason: str, message: str,
) -> None:
    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    home = tmp_path / "home"
    _configure_worklink_test(home)
    monkeypatch.setenv("MIMIR_HOME", str(home))
    events = []

    async def log_event(name, **fields):
        events.append((name, fields))

    monkeypatch.setattr(repository, "safe_log_event", log_event)

    def refused(*args, **kwargs):
        raise failure

    async def runner(*args, **kwargs):
        pytest.fail("refused snapshot must never launch a worker")

    with pytest.raises(repository.ProjectTestRefusal) as refusal:
        await repository.RepoProjectTests(
            state, runner=runner, checkout_factory=refused,
        ).execute()
    assert refusal.value.code == code
    assert str(refusal.value) == message
    assert refusal.value.__cause__ is failure
    assert refusal.value.execution_started and refusal.value.fixed_message
    assert events == [("repo_test_containment_refused", {
        "reason_code": reason, "repository": scope.canonical_repo,
        "pull_request": scope.pr_number,
    })]


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


def _repo_test_lease_fixture(tmp_path, monkeypatch, *, command="/usr/bin/true", wrapper=True):
    _, _, _, state = _repo_scope_and_state(tmp_path)
    root = state.checkout_lease.path
    (root / "tests").mkdir()
    (root / "tests/test_changed.py").write_text("def test_changed(): pass\n")
    (root / "plugins/cultivate/tests").mkdir(parents=True)
    if wrapper:
        (root / "scripts").mkdir()
        source = Path(__file__).resolve().parents[3] / "scripts"
        for name in ("heavy_run.sh", "heavy_run.py"):
            (root / "scripts" / name).write_bytes((source / name).read_bytes())
    home = tmp_path / "home"
    _configure_test_suites(home, state, test_suites=_two_leg_suites(command))
    monkeypatch.setenv("MIMIR_HOME", str(home))
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", str(tmp_path / "heavy.lock"))
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_TIMEOUT", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_TIER", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_TIMEOUT_TOOL", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_GTIMEOUT", raising=False)
    return state


def _lease_start(identifier, started, stderr=b""):
    return f"\x1erepo-test-{identifier}:{started}\n".encode() + stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["medium", "large", "no_lease", "missing_wrapper"])
@pytest.mark.parametrize("selectors", [(), ("tests/test_changed.py",)])
async def test_repo_test_lease_wraps_every_leg(tmp_path, monkeypatch, mode, selectors):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch, wrapper=mode != "missing_wrapper")
    if mode == "large":
        monkeypatch.setenv("MIMIR_HEAVY_TIER", "large")
        monkeypatch.setenv("MIMIR_HEAVY_LOCK_TIMEOUT", "20000")
    if mode == "no_lease":
        monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE")
    calls, events, closed = [], [], []

    async def log_event(name, **fields):
        events.append((name, fields))

    monkeypatch.setattr(repository, "safe_log_event", log_event)

    async def runner(argv, capability, env, projections, **kwargs):
        calls.append((argv, env, kwargs))
        stderr = (
            _lease_start(kwargs["identifier"], repository.time.monotonic(), b"leg stderr")
            if argv[0] == "sh" else b"leg stderr"
        )
        return CollectedExecutionResult(0, b"ok", stderr, False, False, 0, 0)

    def checkout(source, **kwargs):
        return SimpleNamespace(path=source, capability=SimpleNamespace(path=source),
                               close=lambda: closed.append(True))

    result = await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=checkout,
    ).execute(selectors)
    assert result.ok and len(calls) == 2 and closed == [True]
    for index, (argv, env, kwargs) in enumerate(calls):
        paths = selectors or (() if index == 0 else ("tests", "plugins/cultivate/tests"))
        if mode in {"medium", "large"}:
            assert argv[:5] == ("sh", "./scripts/heavy_run.sh", "timeout", "-k", "10s")
            assert 0 < float(argv[5][:-1]) <= repository._TIMEOUT_SECONDS
            assert argv[6:9] == ("python3", "-c", repository._LEASE_RUN_START)
            assert argv[10:] == ("/usr/bin/true", *paths)
            assert env["MIMIR_HEAVY_TIER"] == mode
            lease_timeout = 20000 if mode == "large" else 14400
            assert env["MIMIR_HEAVY_LOCK_TIMEOUT"] == str(lease_timeout)
            assert kwargs["timeout_s"] == float(argv[5][:-1]) + lease_timeout + 20
        else:
            assert argv == ("/usr/bin/true", *paths)
            assert kwargs["timeout_s"] <= repository._TIMEOUT_SECONDS
        assert env.get("MIMIR_PLUGINS") == (None if index == 0 else "cultivate")
    assert result.stderr == "leg stderr"
    assert all(leg["stderr"] == "leg stderr" for leg in result.legs)
    assert bool(events) is (mode == "missing_wrapper")
    if events:
        assert events[0][0] == "repo_test_lease_warning"
        assert events[0][1]["reason_code"] == "wrapper_missing"
        assert events[0][1]["level"] == "warning"


@pytest.mark.asyncio
async def test_repo_test_wrapped_uv_keeps_config_and_cache_protections(tmp_path, monkeypatch):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    root = state.checkout_lease.path
    (root / "tools").mkdir()
    uv = root / "tools/uv"
    uv.write_text("#!/bin/sh\nexit 0\n")
    uv.chmod(0o755)
    _configure_test_suites(
        tmp_path / "home", state, test_suites=_two_leg_suites("env -u MIMIR_PLUGINS ./tools/uv run pytest -q"),
    )

    async def runner(argv, capability, env, projections, **kwargs):
        assert argv[0:2] == ("sh", "./scripts/heavy_run.sh")
        assert argv[10:14] == ("./tools/uv", "run", "pytest", "-q")
        assert env["UV_NO_CONFIG"] == "1"
        assert env["UV_CACHE_DIR"] == env["XDG_CACHE_HOME"] + "/uv"
        assert "HOME" not in env
        return CollectedExecutionResult(
            0, b"", _lease_start(kwargs["identifier"], repository.time.monotonic()), False, False, 0, 0,
        )

    assert (await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory,
    ).execute()).ok


@pytest.mark.asyncio
async def test_repo_test_lease_queue_does_not_spend_multileg_run_budget(tmp_path, monkeypatch):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    now, budgets = [100.0], []
    monkeypatch.setattr(repository, "time", SimpleNamespace(monotonic=lambda: now[0]))

    async def runner(argv, capability, env, projections, **kwargs):
        budgets.append(float(argv[5][:-1]))
        # Queue for longer than the entire repo_test run budget, on BOTH legs.
        now[0] += 5000
        started = now[0]
        now[0] += 6 if len(budgets) == 1 else 4
        return CollectedExecutionResult(
            0 if len(budgets) == 1 else 124, b"",
            _lease_start(kwargs["identifier"], started), False, False, 0, 0,
        )

    result = await repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory, timeout=10,
    ).execute()
    assert budgets == [10, 4]
    assert result.code == "test_timeout" and result.failed_leg == "cultivate"
    assert result.legs[-1]["timed_out"]
    assert state.full_tested_head is None


@pytest.mark.asyncio
@pytest.mark.parametrize("started", [False, True])
@pytest.mark.parametrize("outcome", ["exit124", "exit137", "controller_timeout"])
async def test_repo_test_lease_wait_timeout_is_not_test_failure(tmp_path, monkeypatch, started, outcome):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    closed = []

    async def runner(argv, capability, env, projections, **kwargs):
        return CollectedExecutionResult(
            {"exit124": 124, "exit137": 137, "controller_timeout": None}[outcome], b"",
            _lease_start(kwargs["identifier"], repository.time.monotonic()) if started else b"",
            outcome == "controller_timeout", False, 0, 0,
        )

    def checkout(source, **kwargs):
        return SimpleNamespace(path=source, capability=SimpleNamespace(path=source),
                               close=lambda: closed.append(True))

    executor = repository.RepoProjectTests(state, runner=runner, checkout_factory=checkout)
    if started:
        assert (await executor.execute()).code == "test_timeout"
    else:
        with pytest.raises(repository.ProjectTestRefusal) as error:
            await executor.execute()
        assert error.value.code == "repo_test_lease_wait_timeout"
        assert error.value.execution_started is False
    assert closed == [True] and state.full_tested_head is None


@pytest.mark.asyncio
async def test_repo_test_lease_wait_refusal_reaches_tool_without_failure_guidance(tmp_path, monkeypatch):
    from mimir.tools.refusals import ToolPolicyRefusal
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(repository, "_state", lambda *args: state)
    real_runner = repository.RepoProjectTests

    async def runner(*args, **kwargs):
        return CollectedExecutionResult(124, b"", b"", False, False, 0, 0)

    monkeypatch.setattr(repository, "RepoProjectTests", lambda state: real_runner(
        state, runner=runner, checkout_factory=_test_checkout_factory,
    ))
    scope = state.action_scope
    with pytest.raises(ToolPolicyRefusal, match="repo_test_lease_wait_timeout") as error:
        await repository.repo_test.coroutine(
            scope.canonical_repo, scope.pr_number, ("tests/test_changed.py",),
        )
    assert not error.value.__cause__.execution_started
    assert "No tests ran" in str(error.value)
    assert "Hold publication" not in str(error.value)
    assert state.full_tested_head is None


@pytest.mark.asyncio
async def test_repo_test_lease_timeout_override_cannot_forward_controller_path(tmp_path, monkeypatch):
    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    monkeypatch.setenv("MIMIR_HEAVY_TIMEOUT_TOOL", str(tmp_path / "home" / "timeout"))
    closed = []

    async def forbidden(*args, **kwargs):
        pytest.fail("controller path must not reach the worker")

    def checkout(source, **kwargs):
        return SimpleNamespace(path=source, capability=SimpleNamespace(path=source),
                               close=lambda: closed.append(True))

    with pytest.raises(repository.ProjectTestRefusal) as error:
        await repository.RepoProjectTests(state, runner=forbidden, checkout_factory=checkout).execute()
    assert error.value.code == "test_config_invalid"
    assert closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("telemetry", ["missing", "nan", "before_launch", "future", "no_newline"])
async def test_repo_test_lease_invalid_start_fails_closed(tmp_path, monkeypatch, telemetry):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(repository, "time", SimpleNamespace(monotonic=lambda: 100.0))
    closed = []

    async def runner(argv, capability, env, projections, **kwargs):
        stderr = b"" if telemetry == "missing" else _lease_start(
            kwargs["identifier"], {"nan": "nan", "before_launch": 99, "future": 101, "no_newline": 100}[telemetry],
        )
        if telemetry == "no_newline":
            stderr = stderr.rstrip(b"\n")
        return CollectedExecutionResult(0, b"", stderr, False, False, 0, 0)

    def checkout(source, **kwargs):
        return SimpleNamespace(path=source, capability=SimpleNamespace(path=source),
                               close=lambda: closed.append(True))

    with pytest.raises(repository.ProjectTestRefusal) as error:
        await repository.RepoProjectTests(state, runner=runner, checkout_factory=checkout).execute()
    assert error.value.code == "test_containment_unavailable"
    assert closed == [True] and state.full_tested_head is None


@pytest.mark.parametrize("platform,available,override,expected", [
    ("linux", {"timeout", "gtimeout"}, None, "timeout"),
    ("darwin", {"timeout", "gtimeout"}, None, "gtimeout"),
    ("linux", {"gtimeout"}, None, "gtimeout"),
    ("darwin", {"timeout"}, None, "timeout"),
    ("linux", set(), "/custom/timeout", "/custom/timeout"),
])
def test_repo_test_lease_timeout_tool_selection(monkeypatch, platform, available, override, expected):
    monkeypatch.setattr(repository, "sys", SimpleNamespace(platform=platform))
    monkeypatch.setattr(repository.shutil, "which", lambda name, **kwargs: name if name in available else None)
    env = {"PATH": "/fixed/path"}
    if override:
        env["MIMIR_HEAVY_TIMEOUT_TOOL"] = override
    assert repository._lease_test_command(("test",), env, "id", 12)[2:6] == (expected, "-k", "10s", "12s")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["pass", "run_timeout", "queue_timeout"])
async def test_repo_test_real_lease_and_run_timeout(tmp_path, monkeypatch, outcome):
    from mimir_cultivate.contained_execution import CollectedExecutionResult

    state = _repo_test_lease_fixture(tmp_path, monkeypatch)
    root = state.checkout_lease.path
    command = "/usr/bin/true" if outcome != "run_timeout" else "/bin/sleep 5"
    _configure_test_suites(tmp_path / "home", state, test_suites=[{
        "name": "unit", "default": True, "command": command,
    }])
    if outcome == "queue_timeout":
        # Scale only the fixture copy's outer timeout to keep the test fast.
        wrapper = root / "scripts/heavy_run.sh"
        wrapper.write_text(wrapper.read_text().replace(
            '${MIMIR_HEAVY_LOCK_TIMEOUT:-7200}', "0.3",
        ))
        intent = tmp_path / "heavy.lock.intent"
        intent.mkdir()
        (intent / "owner").write_text('{"tier":"large"}')
        (intent / "heartbeat").write_text("1\n")
    calls = []

    async def runner(argv, capability, env, projections, **kwargs):
        calls.append(argv)
        process = await asyncio.to_thread(
            subprocess.run, argv, cwd=capability.path, env=env, capture_output=True, check=False,
            timeout=5,
        )
        return CollectedExecutionResult(
            process.returncode, process.stdout, process.stderr, False, False, 0, 0,
        )

    executor = repository.RepoProjectTests(
        state, runner=runner, checkout_factory=_test_checkout_factory,
        timeout=0.3 if outcome == "run_timeout" else 10,
    )
    if outcome == "queue_timeout":
        with pytest.raises(repository.ProjectTestRefusal) as error:
            await executor.execute()
        assert error.value.code == "repo_test_lease_wait_timeout"
        assert not error.value.execution_started
    else:
        result = await executor.execute()
        assert result.code == ("tests_passed" if outcome == "pass" else "test_timeout")
        assert "\x1erepo-test-" not in result.stderr
    assert calls[0][:2] == ("sh", "./scripts/heavy_run.sh")
    assert not (tmp_path / "heavy.lock.lease").exists()
    assert not (tmp_path / "heavy.lock.lease2").exists()


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
