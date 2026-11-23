"""Queue membership gates automatic remediation, never requested reviews."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mimir_cultivate import forge


NOW = datetime(2026, 11, 22, 1, tzinfo=timezone.utc)
HEAD = "a" * 40
BASE = "b" * 40


def _queue_response(entry):
    return {"data": {"repository": {"pullRequest": {
        "number": 42, "headRefOid": HEAD, "mergeQueueEntry": entry,
    }}}}


@pytest.fixture
def poller(monkeypatch, tmp_path):
    path = Path(forge.__file__).parent / "defaults/skills/github-poller/scripts/poller.py"
    spec = importlib.util.spec_from_file_location("_merge_queue_poller_test", path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "STATE_DIR", tmp_path)
    monkeypatch.setattr(module, "_emit", Mock())
    monkeypatch.setattr(module, "_emit_signal", Mock())
    monkeypatch.setattr(module, "_github_author_is_trusted", lambda *a, **kw: True)
    monkeypatch.setattr(module, "_pr_author_trust", lambda *a, **kw: (True, None))
    monkeypatch.setattr(module, "_pr_author_is_trusted", lambda *a, **kw: True)
    return module


def _pr(mergeable=True, author="bot"):
    return {
        "number": 42, "title": "Ready", "state": "open", "merged": False,
        "user": {"login": author}, "html_url": "https://github.com/o/r/pull/42",
        "head": {"sha": HEAD, "ref": "fix", "repo": {"full_name": "o/r"}},
        "base": {"sha": BASE, "ref": "main"}, "mergeable": mergeable,
        "requested_reviewers": [{"login": "bot"}],
    }


def _patch_reads(monkeypatch, poller, response, *, mergeable=True, author="bot"):
    pr = _pr(mergeable, author)
    calls = []

    def api(endpoint, token):
        calls.append(endpoint)
        if "pulls?state=open" in endpoint:
            return [pr]
        if endpoint == "repos/o/r/pulls/42":
            return pr
        if "/compare/" in endpoint:
            return {"behind_by": 12}
        if endpoint.endswith("/reviews"):
            return []
        if endpoint == "repos/o/r/issues/42":
            return {"state": "open", "pull_request": {"url": pr["html_url"]}}
        if endpoint == "repos/o/r/issues/42/timeline?per_page=100":
            return []
        raise AssertionError(endpoint)

    request = Mock(return_value=response)
    monkeypatch.setattr(poller, "_gh_api", api)
    monkeypatch.setattr(forge.GitHubForgeClient, "_request", request)
    return calls, request


@pytest.mark.parametrize("mergeable", [True, False])
def test_queued_pr_emits_no_checkout_test_or_push_turn(
    monkeypatch, poller, capsys, mergeable,
):
    calls, request = _patch_reads(
        monkeypatch, poller, _queue_response({"id": "queue-entry"}), mergeable=mergeable,
    )
    prior = {"42": {"head_sha": HEAD, "reason": "behind_base", "attempts": 1}}
    budget = [1]

    count, cursor = poller._check_own_mergeability(
        "o/r", "token", "bot", prior, now=NOW, attempt_budget=budget,
    )

    assert count == 0
    assert cursor == prior
    assert budget == [1]
    # Checkout, tests and push run only in emitted turns; suppressing _emit
    # prevents all three, rather than merely editing their prompt.
    poller._emit.assert_not_called()
    poller._emit_signal.assert_not_called()
    assert not any("/compare/" in call or call.endswith("/reviews") for call in calls)
    request.assert_called_once()
    assert "PR #42: in merge queue" in capsys.readouterr().err


@pytest.mark.parametrize("mergeable,event", [
    (True, "pr_mergeability_rebase"), (False, "pr_mergeability_conflicting"),
])
def test_unqueued_pr_still_authorizes_remediation(monkeypatch, poller, mergeable, event):
    _patch_reads(monkeypatch, poller, _queue_response(None), mergeable=mergeable)
    count, cursor = poller._check_own_mergeability("o/r", "token", "bot", {}, now=NOW)
    assert count == 1
    assert cursor["42"]["attempts"] == 1
    assert poller._emit.call_args.kwargs["event_type"] == event
    assert "scoped PR checkout" in poller._emit.call_args.args[0]
    assert "repo_push" in poller._emit.call_args.args[0]


@pytest.mark.parametrize("response", [
    None, {}, {"data": None}, {"data": {"repository": None}},
    {"data": {"repository": {"pullRequest": None}}},
    {"data": {"repository": {"pullRequest": {"number": 42}}}},
    {"data": {"repository": {"pullRequest": {"number": 43, "mergeQueueEntry": None}}}},
    _queue_response(False), _queue_response({}), _queue_response({"id": ""}),
    {**_queue_response(None), "errors": [{"message": "unavailable"}]},
])
def test_unknown_queue_status_defers_without_losing_retry_state(
    monkeypatch, poller, capsys, response,
):
    _patch_reads(monkeypatch, poller, response)
    prior = {"42": {"head_sha": HEAD, "reason": "behind_base", "attempts": 2}}
    count, cursor = poller._check_own_mergeability("o/r", "token", "bot", prior, now=NOW)
    assert (count, cursor) == (0, prior)
    poller._emit.assert_not_called()
    assert "merge queue status unavailable" in capsys.readouterr().err


def test_queue_transport_failure_defers(monkeypatch, poller):
    _, request = _patch_reads(monkeypatch, poller, None)
    request.side_effect = forge.ForgeError("transport failed")
    assert poller._check_own_mergeability("o/r", "token", "bot", {}, now=NOW) == (0, {})
    poller._emit.assert_not_called()


def test_queue_read_respects_exhausted_tick_budget(monkeypatch, poller):
    _, request = _patch_reads(monkeypatch, poller, _queue_response(None))
    poller.set_active_tick_budget(poller.TickBudget(hard_deadline_seconds=0))
    try:
        assert poller._check_own_mergeability("o/r", "token", "bot", {}, now=NOW) == (0, {})
        request.assert_not_called()
        poller._emit.assert_not_called()
    finally:
        poller.set_active_tick_budget(None)


@pytest.mark.parametrize("activity", ["review_request", "operator_ask"])
def test_queued_pr_requested_work_is_unaffected(monkeypatch, poller, activity):
    _, request = _patch_reads(
        monkeypatch, poller, _queue_response({"id": "queue-entry"}), author="alice",
    )
    # Establish that this very PR is queued via the real typed read.
    assert poller._pr_merge_queue_status("o/r", 42, HEAD, "token") is True
    request.reset_mock()
    if activity == "review_request":
        count, _, _ = poller._check_pr_pushes(
            "o/r", "token", "bot", {"42": HEAD}, {}, auto_review="requested-only",
        )
        expected = "pr_review_requested"
    else:
        count = poller._check_issue_comments(
            "o/r", "2026-11-22T00:00:00Z", "token", "bot",
            comments=[{
                "id": 123, "user": {"login": "operator"}, "body": "@bot please review",
                "created_at": "2026-11-22T00:30:00Z",
                "html_url": "https://github.com/o/r/pull/42#issuecomment-123",
                "issue_url": "https://api.github.com/repos/o/r/issues/42",
            }],
            auto_review="requested-only",
        )
        expected = "issue_comment"
    assert count == 1
    assert poller._emit.call_args.kwargs["event_type"] == expected
    request.assert_not_called()


@pytest.mark.parametrize("mergeable", [True, False])
@pytest.mark.parametrize("observed_head", [BASE, None, "", 123])
def test_queue_read_head_mismatch_preserves_retry_state(
    monkeypatch, poller, mergeable, observed_head,
):
    response = _queue_response(None)
    response["data"]["repository"]["pullRequest"]["headRefOid"] = observed_head
    _, request = _patch_reads(monkeypatch, poller, response, mergeable=mergeable)
    prior = {"42": {"head_sha": HEAD, "reason": "conflicting", "attempts": 2}}
    budget = [1]
    assert poller._check_own_mergeability(
        "o/r", "token", "bot", prior, now=NOW, attempt_budget=budget,
    ) == (0, prior)
    assert budget == [1]
    request.assert_called_once()
    poller._emit.assert_not_called()
    poller._emit_signal.assert_not_called()


@pytest.mark.parametrize("entry,queued", [(None, False), ({"id": "queue-entry"}, True)])
def test_typed_queue_read_uses_bounded_graphql_request(entry, queued):
    calls = []

    class Session:
        def request(self, *args, **kwargs):
            calls.append((args, kwargs))
            return SimpleNamespace(
                status_code=200, headers={"Content-Type": "application/json"},
                iter_content=lambda **_: (json.dumps(_queue_response(entry)).encode(),),
                close=lambda: None,
            )

    client = forge.GitHubForgeClient(token="token", timeout=3, session=Session())
    assert client.is_pull_request_queued("o/r", 42, expected_head=HEAD) is queued
    [(args, kwargs)] = calls
    assert args == ("POST", "https://api.github.com/graphql")
    assert kwargs["json"]["variables"] == {"owner": "o", "name": "r", "number": 42}
    assert "mergeQueueEntry { id }" in kwargs["json"]["query"]
    assert "headRefOid" in kwargs["json"]["query"]
    assert kwargs["timeout"] == 3
    assert kwargs["stream"] is True


@pytest.mark.parametrize("repo,number", [("bad", 42), ("o/r", True), ("o/r", 0)])
def test_typed_queue_read_rejects_invalid_selector(monkeypatch, repo, number):
    request = Mock()
    monkeypatch.setattr(forge.GitHubForgeClient, "_request", request)
    with pytest.raises(forge.ForgeError, match="invalid pull-request selector"):
        forge.GitHubForgeClient(token="token").is_pull_request_queued(repo, number)
    request.assert_not_called()
