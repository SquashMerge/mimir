"""PR checkout trusts the last authenticated contributor, not commit text."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mimir.models import InformationFlowState
from mimir_cultivate.forge import GitHubForgeClient, pr_trust_verdict


@pytest.mark.parametrize(
    ("verified", "login", "owner", "collaborator", "expected"),
    [
        (False, "collaborator", "outsider", True, False),
        (True, "collaborator", "outsider", True, True),
        (False, "collaborator", "collaborator", True, True),
        (True, "outsider", "outsider", True, False),
        (True, "collaborator", "outsider", None, None),
        (False, "collaborator", "collaborator", None, None),
        (None, "collaborator", "collaborator", True, None),
    ],
)
def test_fork_head_identity(verified, login, owner, collaborator, expected):
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo=f"{owner}/repo",
        pr_number=7, observed_head_sha="a" * 40, pull_request_author="outsider",
    )
    context = SimpleNamespace(ifc_state=InformationFlowState())
    metadata = SimpleNamespace(number=7, head_sha="a" * 40, author="outsider",
                               repo=scope.canonical_repo, head_repo=scope.head_repo)
    client = SimpleNamespace(
        get_pull_request_identity=lambda *_: metadata,
        get_head_commit_identity=lambda _: (verified, login) if verified is not None else None,
        author_is_trusted=lambda _, author: collaborator if author == "collaborator" else False,
    )
    assert pr_trust_verdict(context, scope, client) is expected


def test_base_head_ignores_event_actor_and_unavailable_collaborator_api():
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo="acme/repo", pr_number=7,
        observed_head_sha="a" * 40, pull_request_author="dependabot[bot]",
    )
    metadata = SimpleNamespace(number=7, head_sha="a" * 40, author="dependabot[bot]",
                               repo=scope.canonical_repo, head_repo=scope.head_repo)
    context = SimpleNamespace(ifc_state=InformationFlowState(), principal="outsider")
    client = SimpleNamespace(get_pull_request_identity=lambda *_: metadata,
                             author_is_trusted=lambda *_: None)
    assert pr_trust_verdict(context, scope, client) is True
    metadata.author = "outsider"
    assert pr_trust_verdict(context, scope, client) is None


def test_native_identity_snapshot_must_match_scoped_head_repository():
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo="acme/repo", pr_number=7,
        observed_head_sha="a" * 40, pull_request_author="dependabot[bot]",
    )
    metadata = SimpleNamespace(
        number=7, head_sha="a" * 40, author="dependabot[bot]",
        repo="acme/repo", head_repo="elsewhere/repo",
    )
    context = SimpleNamespace(ifc_state=InformationFlowState())
    client = SimpleNamespace(
        get_pull_request_identity=lambda *_: metadata,
        get_pull_request=lambda *_: pytest.fail("checkout read PR text"),
    )
    assert pr_trust_verdict(context, scope, client) is None
    metadata.head_repo = "acme/repo"
    metadata.repo = "elsewhere/repo"
    assert pr_trust_verdict(context, scope, client) is None
    metadata.repo = "acme/repo"
    assert pr_trust_verdict(context, scope, client) is True


def test_fork_head_identity_lookup_unavailable():
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo="collaborator/repo", pr_number=7,
        observed_head_sha="a" * 40, pull_request_author="outsider",
    )
    context = SimpleNamespace(ifc_state=InformationFlowState())
    client = SimpleNamespace(
        get_pull_request_identity=lambda *_: SimpleNamespace(
            number=7, head_sha="a" * 40, author="outsider",
            repo=scope.canonical_repo, head_repo=scope.head_repo),
        get_head_commit_identity=lambda _: None,
        author_is_trusted=lambda *_: True,
    )
    assert pr_trust_verdict(context, scope, client) is None


def test_github_head_commit_identity_uses_verified_and_linked_login(monkeypatch):
    scope = SimpleNamespace(head_repo="external/repo", observed_head_sha="a" * 40)
    payload = {"sha": "a" * 40, "commit": {
        "verification": {"verified": True}, "author": {"email": "collaborator@example.com"},
    }, "author": {"login": "spoofed-author"}, "committer": {"login": "collaborator"}}
    calls = []

    def request(_self, method, path):
        calls.append((method, path))
        return payload

    monkeypatch.setattr(GitHubForgeClient, "_request", request)
    client = object.__new__(GitHubForgeClient)
    assert client.get_head_commit_identity(scope) == (True, "collaborator")
    assert calls == [("GET", f"/repos/external/repo/commits/{'a' * 40}")]
    payload["author"] = None
    assert client.get_head_commit_identity(scope) == (True, "collaborator")
    payload["committer"] = None
    assert client.get_head_commit_identity(scope) == (True, None)
    payload["commit"]["verification"] = {}
    assert client.get_head_commit_identity(scope) is None
    payload["sha"] = "b" * 40
    payload["commit"]["verification"] = {"verified": True}
    assert client.get_head_commit_identity(scope) is None


def _native_case():
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo="acme/repo", pr_number=7,
        observed_head_sha="a" * 40, pull_request_author="collaborator",
    )
    metadata = SimpleNamespace(
        number=7, head_sha="a" * 40, author="collaborator",
        repo=scope.canonical_repo, head_repo=scope.head_repo,
    )
    return scope, metadata, SimpleNamespace(ifc_state=InformationFlowState())


def test_no_ifc_state_refuses_before_identity_read():
    scope, _, context = _native_case()
    context.ifc_state = None
    client = SimpleNamespace(get_pull_request_identity=lambda *_: pytest.fail("identity read"))
    assert pr_trust_verdict(context, scope, client) is None


def test_legacy_projection_cannot_attest_payload_head_repo():
    scope, metadata, context = _native_case()
    client = SimpleNamespace(get_pull_request=lambda *_: pytest.fail("text read"))
    assert pr_trust_verdict(context, scope, client) is None
    client.get_pull_request_identity = lambda *_: metadata
    assert pr_trust_verdict(context, scope, client) is True


@pytest.mark.parametrize("stage", ["pr_identity", "head_identity", "contributor"])
@pytest.mark.parametrize("error_kind", ["generic", "rate_limit", "server", "transport", "identity"])
def test_identity_lookup_failures_are_unresolved_with_specific_causes(monkeypatch, stage, error_kind):
    from mimir import pollers
    from mimir_cultivate import forge

    scope, metadata, context = _native_case()
    if stage != "pr_identity":
        scope.head_repo = metadata.head_repo = "outsider/repo"
    errors = {
        "generic": forge.ForgeError("untrusted error text"),
        "rate_limit": forge._GitHubRequestError("untrusted", retryable=True, status_code=429),
        "server": forge._GitHubRequestError("untrusted", retryable=True, status_code=503),
        "transport": forge._GitHubRequestError("untrusted", retryable=True),
        "identity": forge.GitHubIdentityVerificationError("untrusted"),
    }
    error = errors[error_kind]
    latched = []
    monkeypatch.setattr(forge, "_latch_github_identity_degraded", latched.append)

    def fail(*_):
        raise error

    client = SimpleNamespace(
        get_pull_request_identity=fail if stage == "pr_identity" else lambda *_: metadata,
        get_head_commit_identity=fail if stage == "head_identity" else lambda *_: (True, "collaborator"),
        author_is_trusted=fail if stage == "contributor" else lambda *_: True,
    )
    token = pollers._github_author_trust_cause.set(None)
    try:
        assert pr_trust_verdict(context, scope, client) is None
        suffix = {"generic": "unavailable", "rate_limit": "rate_limited",
                  "server": "server_error", "transport": "transport_failed",
                  "identity": "github_identity_verification_failed"}[error_kind]
        assert pollers.github_author_trust_cause() == (
            suffix if error_kind == "identity" else
            f"{'contributor_trust' if stage == 'contributor' else stage}_{suffix}"
        )
        assert latched == ([error] if error_kind == "identity" else [])
    finally:
        pollers._github_author_trust_cause.reset(token)


@pytest.mark.parametrize("author,committer,owner,expected", [
    ("collaborator", "outsider", "outsider", False),
    ("outsider", "collaborator", "outsider", True),
    ("collaborator", "web-flow", "outsider", False),
    ("outsider", "WEB-FLOW", "collaborator", True),
    ("collaborator", None, "collaborator", None),
])
def test_verified_committer_not_spoofable_author(monkeypatch, author, committer, owner, expected):
    scope, metadata, context = _native_case()
    scope.head_repo = metadata.head_repo = f"{owner}/repo"
    payload = {
        "sha": scope.observed_head_sha, "commit": {"verification": {"verified": True}},
        "author": {"login": author}, "committer": {"login": committer},
    }
    monkeypatch.setattr(GitHubForgeClient, "_request", lambda *_: payload)
    monkeypatch.setattr(GitHubForgeClient, "get_pull_request_identity", lambda *_: metadata)
    calls = []

    def attest(_self, repo, login):
        calls.append(login)
        return login == "collaborator"

    monkeypatch.setattr(GitHubForgeClient, "author_is_trusted", attest)
    client = object.__new__(GitHubForgeClient)
    assert pr_trust_verdict(context, scope, client) is expected
    assert calls == ([] if committer is None else [owner if committer.casefold() == "web-flow" else committer])
