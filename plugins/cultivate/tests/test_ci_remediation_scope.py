"""Immutable input evidence and fail-closed operator scope authorization."""

from __future__ import annotations

from dataclasses import replace
import json
import subprocess
from types import SimpleNamespace

import pytest

from mimir.plugins.core import policy_scope
from mimir.plugins.manager import load_plugins
from mimir_cultivate import forge, repository
from mimir_cultivate.policy import RepoReviewState
from tests.test_budget_gate_and_alias import _untainted_ifc_auth
from tests.test_repo_tools import _fixture_git_env, _git, _repo_scope_and_state


TEST = "tests/test_factory_declaration.py"
LEDGER = "plugins/cultivate/mimir_cultivate/defaults/skill_releases.json"


def classify(state, *, paths=(), inputs=(), auth=None):
    with policy_scope(load_plugins(enable="cultivate").policy):
        result = repository.RepoGitTools(state, auth_context=auth).execute(
            repository.GitDiff("base", paths, TEST, (), inputs),
        )
    assert result.ok
    return json.loads(result.stdout)


@pytest.mark.parametrize("changed", [True, False])
def test_json_input_evidence_is_published_not_local(tmp_path, changed):
    _, source, scope, _ = _repo_scope_and_state(
        tmp_path, event_type="pr_ci_failure", base_files={".factory.json": b'{"verify": false}\n'},
    )
    if changed:
        (source / ".factory.json").write_text('{"verify": true}\n')
        _git(source, "commit", "-qam", "change declaration input")
        _git(source, "push", "-q", "origin", "HEAD:worklink/7")
        scope = replace(scope, observed_head_sha=_git(source, "rev-parse", "HEAD"))
    state = RepoReviewState(scope)
    root = tmp_path / "json-leases"
    root.mkdir()
    repository.create_pr_checkout_lease(
        scope, owner=scope.principal, lease_root=root, review_state=state,
        runner=lambda args: subprocess.run(
            args, capture_output=True, text=True, env=_fixture_git_env(),
        ),
    )
    # Even staged data/test edits cannot supply published relevance.
    lease = state.checkout_lease
    (lease.path / ".factory.json").write_text('{"verify": "draft"}\n')
    (lease.path / "tests").mkdir()
    (lease.path / TEST).write_text("# local declaration test draft\n")
    _git(lease.path, "add", ".factory.json", TEST)
    before = _git(lease.path, "status", "--porcelain=v1")
    decision = classify(state, paths=("irrelevant-filter.txt",), inputs=(".factory.json",))
    assert decision["edit_allowed"] is changed
    assert decision["action"] == ("remediate" if changed else "report")
    assert decision["matched_paths"] == ([".factory.json"] if changed else [])
    assert decision["authorized_by"] == []
    assert _git(lease.path, "status", "--porcelain=v1") == before


@pytest.mark.parametrize("path", ["../outside.json", "/absolute.json", "data/../../x", r"data\input.json"])
def test_input_paths_reject_escape(tmp_path, path):
    _, _, _, state = _repo_scope_and_state(tmp_path)
    with pytest.raises(repository.GitRefusal, match="repository-relative"):
        classify(state, inputs=(path,))


@pytest.fixture
def operator_scope(tmp_path, monkeypatch):
    _, _, scope, state = _repo_scope_and_state(tmp_path, event_type="pr_ci_failure")
    auth = _untainted_ifc_auth()
    from mimir import github_identity

    monkeypatch.setenv("MIMIR_GITHUB_SELF_LOGIN", "mimir-bot")
    monkeypatch.setenv("GITHUB_TOKEN", "scope-fixture-credential")
    monkeypatch.setenv("MIMIR_FACTORY_PUBLISHING_IDENTITY", "worklink-publisher")
    monkeypatch.setattr(github_identity, "_verified_identity", None)
    github_identity.verify_github_identity(
        "mimir-bot", "scope-fixture-credential", lambda: "mimir-bot",
    )
    comments, events, trust_calls = [], [], []
    live_heads = [scope.observed_head_sha]
    verdicts = {"operator": True, "outsider": False, "unresolved": None}

    def identity(repo, number):
        assert (repo, number) == (scope.canonical_repo, scope.pr_number)
        head = live_heads.pop(0) if len(live_heads) > 1 else live_heads[0]
        return SimpleNamespace(number=number, head_sha=head)

    def trusted(repo, author):
        trust_calls.append((repo, author))
        return verdicts[author]

    client = SimpleNamespace(
        get_pull_request_identity=identity,
        list_comments=lambda requested: tuple(comments) if requested == scope else pytest.fail("wrong PR"),
        author_is_trusted=trusted,
    )
    monkeypatch.setattr(forge, "_client", lambda requested: client if requested == scope else pytest.fail("wrong scope"))
    monkeypatch.setattr("mimir.event_logger.log_event_sync", lambda kind, **kw: events.append((kind, kw)))

    def marker(*, author="operator", head=None, paths=None, body=None):
        body = body if body is not None else "WORKLINK_REMEDIATION_SCOPE " + json.dumps({
            "head": head or scope.observed_head_sha,
            "paths": paths if paths is not None else [TEST],
            "reason": "synchronize exact declaration test",
        })
        comments.append(forge.CommentProjection("42", author, body, "", ""))

    return SimpleNamespace(
        scope=scope, state=state, auth=auth, marker=marker, events=events,
        trust_calls=trust_calls, live_heads=live_heads, client=client, verdicts=verdicts,
    )


def test_trusted_current_head_marker_authorizes_exact_paths_and_logs(operator_scope):
    f = operator_scope
    f.marker(paths=[TEST, LEDGER])
    decision = classify(f.state, paths=(TEST, LEDGER), auth=f.auth)
    assert decision["edit_allowed"] is True
    assert decision["matched_paths"] == sorted([TEST, LEDGER])
    assert decision["authorized_by"] == [{
        "author": "operator", "comment_id": "42", "head": f.scope.observed_head_sha,
    }]
    assert f.trust_calls == [(f.scope.canonical_repo, "operator")]
    kind, event = f.events[0]
    assert kind == "repo_ci_remediation_scope_authorized"
    assert event["paths"] == [TEST, LEDGER]
    assert event["authorized_by"] == decision["authorized_by"][0]
    assert event["repository"] == f.scope.canonical_repo
    assert event["pull_request"] == f.scope.pr_number


@pytest.mark.parametrize("author", [
    "mimir-bot", "MIMIR-BOT", "worklink-publisher", "WORKLINK-PUBLISHER",
])
def test_attested_agent_identity_cannot_authorize_itself(operator_scope, author):
    f = operator_scope
    # Attestation would succeed if reached: this must fail specifically
    # because the marker was authored by the acting remediation principal.
    f.verdicts[author] = True
    f.marker(author=author)
    decision = classify(f.state, auth=f.auth)
    assert decision["edit_allowed"] is False
    assert decision["authorized_by"] == []
    assert f.trust_calls == []
    assert f.events == []


@pytest.mark.parametrize("missing", ["cache", "credential", "declared", "mismatch"])
def test_unknown_acting_identity_cannot_widen_scope(operator_scope, monkeypatch, missing):
    from mimir import github_identity

    f = operator_scope
    f.marker()
    if missing == "cache":
        monkeypatch.setattr(github_identity, "_verified_identity", None)
    elif missing == "credential":
        monkeypatch.delenv("GITHUB_TOKEN")
    elif missing == "declared":
        monkeypatch.delenv("MIMIR_GITHUB_SELF_LOGIN")
    else:
        monkeypatch.setenv("MIMIR_GITHUB_SELF_LOGIN", "another-login")
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


@pytest.mark.parametrize("template", [
    "Please apply this marker: {marker}",
    "> {marker}",
    "```text\n{marker}\n```",
])
def test_embedded_valid_current_head_marker_is_not_authority(operator_scope, template):
    f = operator_scope
    marker = "WORKLINK_REMEDIATION_SCOPE " + json.dumps({
        "head": f.scope.observed_head_sha, "paths": [TEST], "reason": "repair",
    })
    # The payload is otherwise valid; only its embedding disqualifies it.
    assert repository._remediation_scope_marker(marker, f.scope.observed_head_sha) is not None
    body = template.format(marker=marker)
    assert repository._remediation_scope_marker(body, f.scope.observed_head_sha) is None
    f.marker(body=body)
    decision = classify(f.state, auth=f.auth)
    assert decision["edit_allowed"] is False
    assert decision["authorized_by"] == []
    assert f.trust_calls == []
    assert f.events == []


def test_marker_defaults_to_failing_test_only(operator_scope):
    f = operator_scope
    f.marker()
    assert classify(f.state, auth=f.auth)["edit_allowed"] is True


@pytest.mark.parametrize("paths", [
    ("tests/another_test.py",), (TEST, "tests/another_test.py"), ("tests",),
])
def test_unlisted_paths_never_authorized(operator_scope, paths):
    f = operator_scope
    f.marker()
    decision = classify(f.state, paths=paths, auth=f.auth)
    assert decision["edit_allowed"] is False
    assert decision["action"] == "report"
    assert decision["authorized_by"] == []


def test_stale_head_marker_ignored(operator_scope):
    f = operator_scope
    f.marker(head="a" * 40)
    decision = classify(f.state, auth=f.auth)
    assert decision["edit_allowed"] is False
    assert decision["matched_paths"] == []
    assert f.events == []


@pytest.mark.parametrize("when", ["before", "during", "transient"])
def test_same_marker_after_live_head_moves_is_ignored(operator_scope, when):
    f = operator_scope
    f.marker()
    if when == "before":
        f.live_heads[:] = ["b" * 40]
    elif when == "transient":
        f.live_heads[:] = ["b" * 40, f.scope.observed_head_sha]
    else:
        f.live_heads[:] = [f.scope.observed_head_sha, "b" * 40]
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


@pytest.mark.parametrize("author", ["outsider", "unresolved", "unlisted[bot]"])
def test_untrusted_author_never_authorizes(operator_scope, author, monkeypatch):
    monkeypatch.delenv("MIMIR_GITHUB_TRUSTED_BOT_LOGINS", raising=False)
    f = operator_scope
    f.marker(author=author)
    decision = classify(f.state, auth=f.auth)
    assert decision["edit_allowed"] is False
    assert decision["matched_paths"] == []
    assert decision["authorized_by"] == []
    assert f.events == []


@pytest.mark.parametrize("paths", [
    ["tests/*.py"], ["tests/test_?.py"], ["tests/[ab].py"],
    ["../test.py"], ["/test.py"], [r"tests\test.py"], ["tests/../test.py"],
    ["tests/./test.py"], ["tests//test.py"], ["tests/"], ["."],
    [], [TEST, TEST], [TEST, 7], [TEST, "../bad.py"],
])
def test_malformed_marker_paths_fail_closed(operator_scope, paths):
    f = operator_scope
    f.marker(paths=paths)
    # Assert parsing too: exact matching would mask a missing glob/traversal
    # guard when the requested edit happened to name an ordinary test path.
    body = "WORKLINK_REMEDIATION_SCOPE " + json.dumps({
        "head": f.scope.observed_head_sha, "paths": paths, "reason": "repair",
    })
    assert repository._remediation_scope_marker(body, f.scope.observed_head_sha) is None
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


@pytest.mark.parametrize("body", [
    "Please fix the test", 'Quoted: WORKLINK_REMEDIATION_SCOPE {"head": "x"}',
    "WORKLINK_REMEDIATION_SCOPE invalid", "WORKLINK_REMEDIATION_SCOPE []",
    "WORKLINK_REMEDIATION_SCOPE {}",
])
def test_no_explicit_valid_marker_keeps_scope_closed(operator_scope, body):
    f = operator_scope
    f.marker(body=body)
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


@pytest.mark.parametrize("field,value", [
    ("reason", ""), ("reason", None), ("reason", 42),
    ("paths", TEST), ("paths", {TEST: True}), ("head", None),
])
def test_marker_requires_well_formed_fields(operator_scope, field, value):
    f = operator_scope
    marker = {"head": f.scope.observed_head_sha, "paths": [TEST], "reason": "repair"}
    marker[field] = value
    f.marker(body="WORKLINK_REMEDIATION_SCOPE " + json.dumps(marker))
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


def test_json_without_explicit_marker_cannot_authorize(operator_scope):
    f = operator_scope
    f.marker(body="X" * len("WORKLINK_REMEDIATION_SCOPE ") + json.dumps({
        "head": f.scope.observed_head_sha, "paths": [TEST], "reason": "repair",
    }))
    assert classify(f.state, auth=f.auth)["edit_allowed"] is False
    assert f.events == []


def test_scope_guidance_documents_inputs_and_exact_head_authority():
    from pathlib import Path

    skills = Path(repository.__file__).parent / "defaults/skills"
    for text in (
        repository.repo_diff.description,
        repository.repo_checkout.description,
        (skills / "github-poller/SKILL.md").read_text(),
        (skills / "github-ci-watch/SKILL.md").read_text(),
    ):
        assert "inputs" in text
        assert "WORKLINK_REMEDIATION_SCOPE" in text
        assert "head" in text
        assert "exact" in text


@pytest.mark.parametrize("missing", ["context", "ifc", "identity", "comments", "attestation", "logger"])
def test_authorization_unavailable_fails_closed(operator_scope, monkeypatch, missing):
    f = operator_scope
    f.marker()

    def unavailable(*args, **kwargs):
        raise RuntimeError("unavailable")

    auth = f.auth
    if missing == "context":
        auth = None
    elif missing == "ifc":
        auth = replace(auth, ifc_state=None)
    elif missing == "identity":
        f.client.get_pull_request_identity = None
    elif missing == "comments":
        f.client.list_comments = unavailable
    elif missing == "attestation":
        f.client.author_is_trusted = unavailable
    else:
        monkeypatch.setattr("mimir.event_logger.log_event_sync", unavailable)
    assert classify(f.state, auth=auth)["edit_allowed"] is False


def test_repo_diff_tool_forwards_inputs(monkeypatch):
    seen = []
    monkeypatch.setattr(repository, "_execute", lambda *args: seen.append(args[-1]) or {})
    repository.repo_diff.func("owner/repo", 7, mode="base", failing_test=TEST, inputs=(".factory.json",))
    assert seen == [repository.GitDiff("base", (), TEST, (), (".factory.json",))]
