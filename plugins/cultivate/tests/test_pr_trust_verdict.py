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
    metadata = SimpleNamespace(number=7, head_sha="a" * 40, author="outsider")
    client = SimpleNamespace(
        get_pull_request=lambda _: metadata,
        get_head_commit_identity=lambda _: (verified, login) if verified is not None else None,
        author_is_trusted=lambda _, author: collaborator if author == "collaborator" else False,
    )
    assert pr_trust_verdict(context, scope, client) is expected


def test_base_head_ignores_event_actor_and_unavailable_collaborator_api():
    scope = SimpleNamespace(
        canonical_repo="acme/repo", head_repo="acme/repo", pr_number=7,
        observed_head_sha="a" * 40, pull_request_author="dependabot[bot]",
    )
    metadata = SimpleNamespace(number=7, head_sha="a" * 40, author="dependabot[bot]")
    context = SimpleNamespace(ifc_state=InformationFlowState(), principal="outsider")
    client = SimpleNamespace(get_pull_request=lambda _: metadata,
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
        get_pull_request=lambda _: SimpleNamespace(number=7, head_sha="a" * 40, author="outsider"),
        get_head_commit_identity=lambda _: None,
        author_is_trusted=lambda *_: True,
    )
    assert pr_trust_verdict(context, scope, client) is None


def test_github_head_commit_identity_uses_verified_and_linked_login(monkeypatch):
    scope = SimpleNamespace(head_repo="external/repo", observed_head_sha="a" * 40)
    payload = {"sha": "a" * 40, "commit": {
        "verification": {"verified": True}, "author": {"email": "collaborator@example.com"},
    }, "author": {"login": "collaborator"}}
    calls = []

    def request(_self, method, path):
        calls.append((method, path))
        return payload

    monkeypatch.setattr(GitHubForgeClient, "_request", request)
    client = object.__new__(GitHubForgeClient)
    assert client.get_head_commit_identity(scope) == (True, "collaborator")
    assert calls == [("GET", f"/repos/external/repo/commits/{'a' * 40}")]
    payload["author"] = None
    assert client.get_head_commit_identity(scope) == (True, None)
    payload["commit"]["verification"] = {}
    assert client.get_head_commit_identity(scope) is None
    payload["sha"] = "b" * 40
    payload["commit"]["verification"] = {"verified": True}
    assert client.get_head_commit_identity(scope) is None
