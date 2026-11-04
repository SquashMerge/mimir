"""Home state-repo review boundary and same-branch revisions."""
from __future__ import annotations

import asyncio
import json
import subprocess
from types import SimpleNamespace
from pathlib import Path

import pytest

import mimir.proposals as proposals
from mimir.proposals import ProposalRevision, PollerProposalScope
from mimir.access_control import CapabilityTier, build_trigger_service_principal
from mimir.plugins.core import core_policy_registry
from mimir.scheduler import Scheduler
from mimir.agent import _create_turn_auth_context
from mimir.models import InformationFlowLabels
from mimir import access_control
from mimir.prompts import build_turn_prompt
from mimir.models import AgentEvent

REPO = "owner/home"
HEAD = "a" * 40
REVIEW_TIME = "2026-11-02T01:00:00Z"
AFTER_REVIEW = "2026-11-02T02:00:00Z"


def forge(monkeypatch, *, branch="proposal/example", verdict="CHANGES_REQUESTED",
           commit=HEAD, permission="write", reviewer="operator", comments=None,
           extra_reviews=(), conversation=(), standalone=()):
    """Fake every gh request at the same subprocess boundary as production."""
    commands = []
    review = {"id": 42, "state": verdict, "commit_id": commit,
              "user": {"login": reviewer}, "body": "Please fix the wording",
              "submitted_at": REVIEW_TIME}
    pr = {"number": 7, "url": "https://github.com/owner/home/pull/7",
          "headRefName": branch, "headRefOid": HEAD, "state": "OPEN", "isCrossRepository": False,
          "files": [{"path": "prompts/example.md"}]}
    if branch.startswith("poller/"):
        pr["files"] = [{"path": "state/social-outbox/papers/outbox-1.yaml" if branch.endswith("social-outbox") else "state/wiki/example.md"}]

    def run(args, *, cwd, capture):
        assert args[0] == "gh"
        commands.append(args[1:])
        if args[1:3] == ["repo", "view"]:
            payload = {"nameWithOwner": REPO}
        elif args[1:3] == ["api", "user"]:
            payload = {"login": "agent"}
        elif args[1:3] == ["pr", "list"]:
            payload = [{k: v for k, v in pr.items() if k != "files"}]
        elif args[1:3] == ["pr", "view"]:
            payload = pr
        elif args[1:3] == ["api", f"repos/{REPO}/pulls/7/reviews"]:
            payload = [review, *({"submitted_at": REVIEW_TIME, **r} for r in extra_reviews)]
        elif args[1] == "api" and args[2].startswith(f"repos/{REPO}/pulls/7/reviews/") and args[2].endswith("/comments"):
            payload = comments if comments is not None else [{"id": 11, "user": {"login": "operator"},
                                                                "path": pr["files"][0]["path"], "line": 3, "body": "Inline fix"}]
        elif args[1:3] == ["api", f"repos/{REPO}/issues/7/comments"]:
            payload = conversation
        elif args[1:3] == ["api", f"repos/{REPO}/pulls/7/comments"]:
            payload = standalone
        elif len(args) > 2 and args[1] == "api" and "/collaborators/" in args[2]:
            payload = {"permission": (permission if args[2].endswith(f"/{reviewer}/permission")
                                      else "write" if args[2].endswith("/agent/permission") else "read")}
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")

    monkeypatch.setattr(proposals, "_run", run)
    monkeypatch.setattr(proposals, "_git", lambda args, **kw: subprocess.CompletedProcess(
        args, 0, f"https://github.com/{REPO}.git\n", "",
    ))
    return commands


def follow_up(cid, body, *, author="operator", created_at=AFTER_REVIEW, **fields):
    return {"id": cid, "user": {"login": author}, "created_at": created_at,
            "body": body, **fields}


def test_collaborator_conversation_follows_review(tmp_path, monkeypatch):
    commands = forge(monkeypatch, conversation=[follow_up(71, "Correction: keep the original claim")])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    assert body.index("Inline fix") < body.index("Follow-up comments from collaborators (newest last)")
    assert "operator at 2026-11-02T02:00:00+00:00: Correction: keep the original claim" in body
    assert ["api", f"repos/{REPO}/issues/7/comments", "--paginate"] in commands


def test_collaborator_standalone_inline_has_location(tmp_path, monkeypatch):
    commands = forge(monkeypatch, standalone=[follow_up(
        72, "Use the revised heading", path="prompts/example.md", line=8,
        pull_request_review_id=None,
    ), follow_up(73, "Already in review", path="prompts/example.md", line=9,
                 pull_request_review_id=42)])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    assert "operator at 2026-11-02T02:00:00+00:00 prompts/example.md:8: Use the revised heading" in body
    assert "Already in review" not in body
    assert ["api", f"repos/{REPO}/pulls/7/comments", "--paginate"] in commands


def test_before_review_comment_is_excluded(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(74, "Old instruction", created_at="2026-11-02T00:59:59Z")])
    assert "Old instruction" not in proposals.pending_proposal_revisions(tmp_path)[0][1]


def test_review_timestamp_is_inclusive_but_future_comment_is_excluded(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(74, "At review", created_at=REVIEW_TIME),
                                    follow_up(75, "Not yet sent", created_at="2099-01-01T00:00:00Z")])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    assert "At review" in body
    assert "Not yet sent" not in body


def test_agent_comment_is_excluded(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(75, "Agent's own note", author="agent")])
    assert "Agent's own note" not in proposals.pending_proposal_revisions(tmp_path)[0][1]


def test_noncollaborator_follow_up_noticed_once_across_sweeps(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(76, "UNTRUSTED_COMMENT", author="stranger")])
    notices, events = [], []
    async def notice(text):
        notices.append(text)
    async def enqueue(event):
        events.append(event)
        return False
    scheduler = Scheduler(tmp_path / "scheduler.yaml", enqueue, home=tmp_path,
                          policy=core_policy_registry(), operator_notice=notice)
    assert scheduler.add_proposal_revision_job(tmp_path)
    async def twice():
        await scheduler._callables["proposal-revisions"].fn()
        await scheduler._callables["proposal-revisions"].fn()
    asyncio.run(twice())
    assert len(events) == 2
    assert all("UNTRUSTED_COMMENT" not in event.content for event in events)
    assert notices == ["Ignored non-collaborator review/comment on home proposal PR #7 (id 76)."]
    assert proposals._revision_state(tmp_path)["7"]["untrusted"] == ["issue_comment:76"]


def test_grouped_cutoff_uses_oldest_review(tmp_path, monkeypatch):
    forge(monkeypatch, extra_reviews=[{
        "id": 43, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "Newer review",
        "submitted_at": "2026-11-02T03:00:00Z",
    }], conversation=[follow_up(77, "Between reviews")])
    revision, body, _ = proposals.pending_proposal_revisions(tmp_path)[0]
    assert revision.related_review_ids == (42,)
    assert body.index("Please fix the wording") < body.index("Between reviews")


def test_follow_up_limit_and_truncation(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(cid, f"comment {cid}" + ("x" * 4100 if cid == 104 else ""),
                                          created_at=f"2026-11-02T02:{cid - 80:02d}:00Z")
                                     for cid in range(80, 105)])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    section = body.split("Follow-up comments from collaborators (newest last):\n", 1)[1]
    assert section.count("- operator at ") == 20
    assert "comment 84" not in section and "comment 85" in section
    assert section.index("comment 85") < section.index("comment 104")
    assert "comment 104" + "x" * (4000 - len("comment 104")) + "\n[truncated]" in section
    assert "x" * 4001 not in section


def test_malformed_follow_up_fails_closed(tmp_path, monkeypatch):
    forge(monkeypatch, conversation=[follow_up(78, "bad", created_at="nonsense")])
    assert proposals.pending_proposal_revisions(tmp_path) == []


@pytest.mark.parametrize("created_at,included", [
    (AFTER_REVIEW, True), ("2026-11-02T00:59:59Z", False),
])
def test_file_level_follow_up(tmp_path, monkeypatch, created_at, included):
    forge(monkeypatch, standalone=[follow_up(
        80, "File-wide correction", created_at=created_at,
        subject_type="file", path="prompts/example.md", line=None,
        original_line=None, pull_request_review_id=None,
    )])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    assert ("File-wide correction" in body) is included
    if included:
        assert "prompts/example.md: File-wide correction" in body
        assert "prompts/example.md:None" not in body


@pytest.mark.parametrize("user", [None, {}, {"login": "ghost"}])
@pytest.mark.parametrize("created_at", ["2026-11-02T00:59:59Z", AFTER_REVIEW])
def test_deleted_author_does_not_suppress_revision(tmp_path, monkeypatch, user, created_at):
    forge(monkeypatch, conversation=[follow_up(
        81, "Deleted author's text", user=user, created_at=created_at,
    )])
    body = proposals.pending_proposal_revisions(tmp_path)[0][1]
    assert "Please fix the wording" in body
    assert "Deleted author's text" not in body


@pytest.mark.parametrize("excluded", ["old", "future", "own", "outsider", "included_review"])
def test_irrelevant_comments_do_not_require_renderable_shape(tmp_path, monkeypatch, excluded):
    fields = {"path": None, "line": None, "id": None, "body": None}
    if excluded == "old":
        fields["created_at"] = "2026-11-02T00:59:59Z"
    elif excluded == "future":
        fields["created_at"] = "2099-01-01T00:00:00Z"
    elif excluded == "own":
        fields.update(author="agent", created_at="invalid")
    elif excluded == "outsider":
        fields.update(author="stranger", created_at="invalid")
    else:
        fields["pull_request_review_id"] = 42
    comment = {**follow_up(82, "Unused"), **fields}
    if "author" in fields:
        comment["user"] = {"login": fields["author"]}
    forge(monkeypatch, standalone=[comment])
    assert len(proposals.pending_proposal_revisions(tmp_path)) == 1


@pytest.mark.parametrize("endpoint", ["issues", "pulls"])
@pytest.mark.parametrize("payload,reason", [
    ({"message": "unavailable"}, "invalid_comment_page"),
    (["UNUSABLE_PAYLOAD"], "invalid_comment_item"),
])
def test_unusable_follow_up_logs_one_content_free_event(tmp_path, monkeypatch, endpoint, payload, reason):
    forge(monkeypatch)
    original = proposals._forge_json
    def unusable(home, args):
        if args == ["api", f"repos/{REPO}/{endpoint}/7/comments", "--paginate"]:
            return payload
        return original(home, args)
    monkeypatch.setattr(proposals, "_forge_json", unusable)
    logged = []
    monkeypatch.setattr(proposals, "log_event_sync", lambda event, **fields: logged.append((event, fields)))
    assert proposals.pending_proposal_revisions(tmp_path) == []
    assert logged == [("proposal_revision_feedback_unavailable", {"pr_number": 7, "reason": reason})]
    assert proposals._revision_state(tmp_path) == {}


@pytest.mark.parametrize("fields", [
    {"id": None}, {"body": None}, {"pull_request_review_id": "bad"},
    {"path": None}, {"line": None}, {"line": True},
])
def test_included_malformed_follow_up_still_fails_closed(tmp_path, monkeypatch, fields):
    comment = follow_up(83, "Invalid collaborator comment", path="prompts/example.md", line=3)
    comment.update(fields)
    forge(monkeypatch, standalone=[comment])
    logged = []
    monkeypatch.setattr(proposals, "log_event_sync", lambda event, **kw: logged.append(event))
    assert proposals.pending_proposal_revisions(tmp_path) == []
    assert logged == ["proposal_revision_feedback_unavailable"]


def test_untrusted_notice_ids_are_namespaced_across_all_sources(tmp_path, monkeypatch):
    forge(monkeypatch, reviewer="stranger", permission="read", extra_reviews=[{
        "id": 43, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "Trusted review",
    }], comments=[follow_up(42, "UNTRUSTED_REVIEW_COMMENT", author="stranger")],
          conversation=[follow_up(42, "UNTRUSTED_ISSUE_COMMENT", author="stranger")],
          standalone=[follow_up(42, "UNTRUSTED_INLINE", author="stranger")])
    original = proposals._forge_json
    def permissions(home, args):
        if args == ["api", f"repos/{REPO}/collaborators/operator/permission"]:
            return {"permission": "write"}
        return original(home, args)
    monkeypatch.setattr(proposals, "_forge_json", permissions)
    # Ambiguous legacy IDs must not suppress notices in any of the three spaces.
    proposals._save_revision_state(tmp_path, {"7": {"untrusted": [42]}})
    notices = []
    for _ in range(2):
        found = proposals.pending_proposal_revisions(tmp_path, on_untrusted=lambda *args: notices.append(args))
        assert len(found) == 1 and "UNTRUSTED" not in found[0][1]
    assert notices == [(7, 42)] * 3
    assert set(proposals._revision_state(tmp_path)["7"]["untrusted"]) == {
        42, "review:42", "issue_comment:42", "review_comment:42",
    }


def test_comment_without_review_does_not_trigger(tmp_path, monkeypatch):
    forge(monkeypatch, verdict="COMMENTED", conversation=[follow_up(79, "Please revise")])
    assert proposals.pending_proposal_revisions(tmp_path) == []


@pytest.mark.parametrize("branch,lane,surface", [
    ("proposal/example", "agent", ""),
    ("upgrade/example", "upgrade", ""),
    ("poller/papers/turn-abcdef", "poller", "wiki"),
    ("poller/papers/social-outbox", "poller", "social-outbox"),
])
def test_detects_each_lane(tmp_path, monkeypatch, branch, lane, surface):
    forge(monkeypatch, branch=branch)
    found = proposals.pending_proposal_revisions(tmp_path)
    assert len(found) == 1
    revision, body, files = found[0]
    assert (revision.lane, revision.surface, revision.branch) == (lane, surface, branch)
    assert "Please fix" in body and "Inline fix" in body and files
    if lane == "poller":
        scope = PollerProposalScope("poller:papers", "revision", "review", "review:42", surface, branch)
        assert proposals.poller_branch_name(scope) == branch
        assert proposals.poller_worktree_path(tmp_path, scope).as_posix().endswith(branch)


@pytest.mark.parametrize("verdict,commit,permission,reviewer", [
    ("APPROVED", HEAD, "write", "operator"),
    ("CHANGES_REQUESTED", "b" * 40, "write", "operator"),
    ("CHANGES_REQUESTED", HEAD, "read", "operator"),
    ("CHANGES_REQUESTED", HEAD, "write", "agent"),
])
def test_denies_nonqualifying_review(tmp_path, monkeypatch, verdict, commit, permission, reviewer):
    forge(monkeypatch, verdict=verdict, commit=commit, permission=permission, reviewer=reviewer)
    assert proposals.pending_proposal_revisions(tmp_path) == []


def test_detection_rejects_stale_head_independently(tmp_path, monkeypatch):
    forge(monkeypatch, commit="b" * 40)
    calls = []
    def details(*args, **kwargs):
        calls.append(args)
        return "review", ["prompts/example.md"]
    # Isolate detection from the independent ingress recheck.
    monkeypatch.setattr(proposals, "_review_details", details)
    assert proposals.pending_proposal_revisions(tmp_path) == []
    assert calls == []


def test_details_rejects_stale_head_independently(tmp_path, monkeypatch):
    forge(monkeypatch, commit="b" * 40)
    revision = ProposalRevision(7, "https://github.com/owner/home/pull/7",
                                "proposal/example", "agent", 42, "operator", REPO)
    assert proposals._review_details(tmp_path, revision) is None


def test_same_head_reviews_share_one_event_and_cap_slot(tmp_path, monkeypatch):
    forge(monkeypatch, extra_reviews=[{
        "id": 43, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "Also fix the follow-up",
    }])
    events = []
    async def enqueue(event):
        events.append(event)
        return True
    scheduler = Scheduler(tmp_path / "scheduler.yaml", enqueue, home=tmp_path,
                          policy=core_policy_registry())
    assert scheduler.add_proposal_revision_job(tmp_path)
    async def twice():
        await scheduler._callables["proposal-revisions"].fn()
        await scheduler._callables["proposal-revisions"].fn()
    asyncio.run(twice())
    assert len(events) == 1
    event = events[0]
    assert event.extra["proposal_revision"].review_id == 43
    assert "Please fix the wording" in event.content
    assert "Also fix the follow-up" in event.content
    assert "Inline fix" in event.content
    record = proposals._revision_state(tmp_path)["7"]
    assert record["reviews"] == [43]
    assert set(record["handled_reviews"]) == {42, 43}
    # Simulated restart: no in-process scheduler state is needed for dedup.
    assert proposals.pending_proposal_revisions(tmp_path) == []
    # Even an isolated replay of an older included review is handled.
    forge(monkeypatch)
    assert proposals.pending_proposal_revisions(tmp_path) == []
    older = ProposalRevision(7, event.extra["proposal_revision"].url,
                             "proposal/example", "agent", 42, "operator", REPO)
    assert proposals.revision_dispatch_allowed(tmp_path, older) == (False, False)
    # A new review after resubmission still consumes only the next turn slot.
    forge(monkeypatch, extra_reviews=[{
        "id": 44, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "New review",
    }])
    revision = proposals.pending_proposal_revisions(tmp_path)[0][0]
    assert revision.review_id == 44
    assert proposals.record_revision_dispatch(tmp_path, revision)
    assert proposals._revision_state(tmp_path)["7"]["reviews"] == [43, 44]


def test_grouped_feedback_excludes_untrusted_older_review(tmp_path, monkeypatch):
    forge(monkeypatch, reviewer="stranger", permission="read", extra_reviews=[{
        "id": 43, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "Trusted follow-up",
    }])
    original = proposals._forge_json
    def permissions(home, args):
        if args == ["api", f"repos/{REPO}/collaborators/operator/permission"]:
            return {"permission": "write"}
        return original(home, args)
    monkeypatch.setattr(proposals, "_forge_json", permissions)
    notices = []
    found = proposals.pending_proposal_revisions(
        tmp_path, on_untrusted=lambda *args: notices.append(args),
    )
    assert len(found) == 1
    revision, body, _ = found[0]
    assert revision.review_id == 43 and revision.related_review_ids == ()
    assert "Trusted follow-up" in body
    assert "Please fix the wording" not in body
    assert notices == [(7, 42)]


def test_grouped_feedback_retries_rejected_enqueue(tmp_path, monkeypatch):
    forge(monkeypatch, extra_reviews=[{
        "id": 43, "state": "CHANGES_REQUESTED", "commit_id": HEAD,
        "user": {"login": "operator"}, "body": "Follow-up",
    }])
    accepted = False
    events = []
    async def enqueue(event):
        events.append(event)
        return accepted
    scheduler = Scheduler(tmp_path / "scheduler.yaml", enqueue, home=tmp_path,
                          policy=core_policy_registry())
    assert scheduler.add_proposal_revision_job(tmp_path)
    asyncio.run(scheduler._callables["proposal-revisions"].fn())
    assert proposals._revision_state(tmp_path) == {}
    accepted = True
    asyncio.run(scheduler._callables["proposal-revisions"].fn())
    assert len(events) == 2
    assert events[0].extra["proposal_revision"] == events[1].extra["proposal_revision"]
    record = proposals._revision_state(tmp_path)["7"]
    assert record["reviews"] == [43]
    assert set(record["handled_reviews"]) == {42, 43}


def test_untrusted_inline_text_never_enters_prompt(tmp_path, monkeypatch):
    forge(monkeypatch, comments=[{"id": 19, "user": {"login": "stranger"},
                                  "path": "prompts/example.md", "line": 3, "body": "SECRET_UNTRUSTED_TEXT"}])
    found = proposals.pending_proposal_revisions(tmp_path)
    assert len(found) == 1
    assert "SECRET_UNTRUSTED_TEXT" not in found[0][1]


def test_untrusted_review_produces_notice_once(tmp_path, monkeypatch):
    forge(monkeypatch, permission="read")
    notices = []
    assert proposals.pending_proposal_revisions(tmp_path, on_untrusted=lambda *args: notices.append(args)) == []
    assert notices == [(7, 42)]
    assert proposals.pending_proposal_revisions(tmp_path, on_untrusted=lambda *args: notices.append(args)) == []
    assert notices == [(7, 42)]


def test_later_approval_clears_revision(tmp_path, monkeypatch):
    forge(monkeypatch)
    original = proposals._forge_json
    def approved(home, args):
        result = original(home, args)
        if args[:2] == ["api", f"repos/{REPO}/pulls/7/reviews"]:
            return [*result, {"id": 43, "state": "APPROVED", "commit_id": HEAD,
                              "user": {"login": "operator"}}]
        return result
    monkeypatch.setattr(proposals, "_forge_json", approved)
    assert proposals.pending_proposal_revisions(tmp_path) == []


def test_http_review_claim_stays_untrusted_in_prompt():
    revision = ProposalRevision(7, "https://github.com/owner/home/pull/7", "proposal/example",
                                "agent", 42, "operator", REPO)
    event = AgentEvent(trigger="proposal_revision", channel_id="proposal-revision:agent",
                       source="http", content="Unverified instruction", extra={
                           "proposal_revision": revision,
                           access_control.HTTP_EVENT_INGRESS_EXTRA_KEY: "http",
                       })
    assert "| Unverified instruction" in build_turn_prompt(event)


def test_durable_dedup_and_three_turn_cap(tmp_path, monkeypatch):
    forge(monkeypatch)
    revision = proposals.pending_proposal_revisions(tmp_path)[0][0]
    assert proposals.record_revision_dispatch(tmp_path, revision)
    # No process-local cache: a second scan of the same home sees persisted state.
    assert proposals.revision_dispatch_allowed(tmp_path, revision) == (False, False)
    for rid in (43, 44):
        assert proposals.record_revision_dispatch(tmp_path, proposals.ProposalRevision(
            revision.number, revision.url, revision.branch, revision.lane, rid,
            revision.reviewer, revision.repo))
    fourth = proposals.ProposalRevision(revision.number, revision.url, revision.branch,
                                        revision.lane, 45, revision.reviewer, revision.repo)
    assert proposals.revision_dispatch_allowed(tmp_path, fourth) == (False, True)
    assert proposals.revision_dispatch_allowed(tmp_path, fourth) == (False, False)


def test_scheduler_alerts_once_at_limit(tmp_path, monkeypatch):
    forge(monkeypatch)
    revision = proposals.pending_proposal_revisions(tmp_path)[0][0]
    for rid in (39, 40, 41):
        assert proposals.record_revision_dispatch(tmp_path, ProposalRevision(
            revision.number, revision.url, revision.branch, revision.lane, rid,
            revision.reviewer, revision.repo))
    alerts, events = [], []
    async def alert(text):
        alerts.append(text)
    async def enqueue(event):
        events.append(event)
        return True
    scheduler = Scheduler(tmp_path / "scheduler.yaml", enqueue, home=tmp_path,
                          policy=core_policy_registry(), operator_notice=alert)
    assert scheduler.add_proposal_revision_job(tmp_path)
    async def twice():
        await scheduler._callables["proposal-revisions"].fn()
        await scheduler._callables["proposal-revisions"].fn()
    asyncio.run(twice())
    assert events == []
    assert alerts == [f"Proposal revision limit (3 turns) reached: {revision.url}"]


@pytest.fixture
def agent_revision_home(tmp_path, monkeypatch):
    """Real local Git remote, fake only forge-side review and publication."""
    home = tmp_path / "home"
    home.mkdir()
    remote = tmp_path / "remote.git"
    def git(*args, cwd):
        return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    git("init", "--bare", "-b", "main", str(remote), cwd=tmp_path)
    git("init", "-b", "main", cwd=home)
    git("config", "user.name", "test", cwd=home)
    git("config", "user.email", "test@example.test", cwd=home)
    (home / ".gitignore").write_text("scratch/\n")
    (home / "prompts").mkdir()
    (home / "prompts" / "example.md").write_text("original\n")
    (home / "memory" / "core").mkdir(parents=True)
    (home / "memory" / "core" / "identity.md").write_text("identity\n")
    git("add", ".", cwd=home)
    git("commit", "-m", "initial", cwd=home)
    git("remote", "add", "origin", str(remote), cwd=home)
    git("push", "origin", "main", cwd=home)
    git("branch", "proposal/example", cwd=home)
    git("push", "origin", "proposal/example", cwd=home)
    revision = ProposalRevision(7, "https://github.com/owner/home/pull/7", "proposal/example",
                                "agent", 42, "operator", REPO)
    monkeypatch.setattr(proposals, "_review_details", lambda *args: ("review", ["prompts/example.md"]))
    original_run = proposals._run
    calls, failures = [], set()
    def run(args, **kw):
        if args[0] == "gh":
            calls.append(args)
            operation = "comment" if args[1:3] == ["pr", "comment"] else "request" if args[1:4] == ["api", "-X", "POST"] else "close"
            return subprocess.CompletedProcess(args, int(operation in failures), "ok", "")
        return original_run(args, **kw)
    monkeypatch.setattr(proposals, "_run", run)
    opened = proposals.open_proposal(home, revision=revision)
    assert opened.ok
    return home, revision, opened.worktree, calls, failures, git


def assert_revision_notifications(calls, revision, rationale):
    assert calls == [
        ["gh", "pr", "comment", str(revision.number), "--body", rationale],
        ["gh", "api", "-X", "POST", f"repos/{revision.repo}/pulls/{revision.number}/requested_reviewers",
         "-f", f"reviewers[]={revision.reviewer}"],
    ]


@pytest.mark.parametrize("failure", [None, "comment", "request"])
def test_same_branch_resubmit(agent_revision_home, failure):
    home, revision, worktree, calls, failures, git = agent_revision_home
    if failure:
        failures.add(failure)
    (worktree / "prompts/example.md").write_text("revised\n")
    result = proposals.finalize_proposal(home, revision=revision, title="Fix", rationale="Fixed wording")
    assert result.ok is (failure is None)
    assert result.pushed and result.reused_pr and result.pr_url == revision.url
    if failure:
        assert result.reason == "error"
        assert result.detail == "revision pushed but comment or review request failed"
    assert_revision_notifications(calls, revision, "Fixed wording")
    git("fetch", "origin", revision.branch, cwd=home)
    assert git("show", "origin/proposal/example:prompts/example.md", cwd=home).stdout == "revised\n"


@pytest.mark.parametrize("case", ["success", "empty_reason", "stale_review", "close_failure"])
def test_abandon_revision(agent_revision_home, monkeypatch, case):
    home, revision, worktree, calls, failures, _ = agent_revision_home
    reason = "Superseded by another approach"
    if case == "empty_reason":
        reason = "  "
    if case == "stale_review":
        monkeypatch.setattr(proposals, "_review_details", lambda *args: None)
    if case == "close_failure":
        failures.add("close")
    if case == "success":
        assert proposals.abandon_proposal(home, revision=revision, reason=reason)
        assert not worktree.exists()
        assert proposals.list_open_proposals(home) == []
    else:
        message = {"empty_reason": "scope/reason required", "stale_review": "review no longer applies",
                   "close_failure": "could not close revision PR"}[case]
        with pytest.raises(ValueError, match=message):
            proposals.abandon_proposal(home, revision=revision, reason=reason)
        assert worktree.exists()
        assert proposals.list_open_proposals(home)
    assert calls == ([] if case in {"empty_reason", "stale_review"} else [
        ["gh", "pr", "close", str(revision.number), "--comment", reason],
    ])


@pytest.mark.parametrize("surface,branch,path,original,revised", [
    ("wiki", "poller/papers/turn-abcdef", "state/wiki/example.md", "# Initial\n", "# Revised\n"),
    ("social-outbox", "poller/papers/social-outbox", "state/social-outbox/papers/outbox-1.yaml",
     "dispatch:\n  - post: {platform: bsky, text: Initial}\n",
     "dispatch:\n  - post: {platform: bsky, text: Revised}\n"),
])
def test_poller_revision_reuses_branch(tmp_path, monkeypatch, surface, branch, path, original, revised):
    home = tmp_path / "home"
    home.mkdir()
    remote = tmp_path / "remote.git"
    def git(*args, cwd):
        return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    git("init", "--bare", "-b", "main", str(remote), cwd=tmp_path)
    git("init", "-b", "main", cwd=home)
    git("config", "user.name", "test", cwd=home)
    git("config", "user.email", "test@example.test", cwd=home)
    (home / ".gitignore").write_text("scratch/\n")
    target = home / path
    target.parent.mkdir(parents=True)
    target.write_text(original)
    git("add", ".", cwd=home)
    git("commit", "-m", "initial", cwd=home)
    git("remote", "add", "origin", str(remote), cwd=home)
    git("push", "origin", "main", cwd=home)
    git("branch", branch, cwd=home)
    git("push", "origin", branch, cwd=home)
    revision = ProposalRevision(7, "https://github.com/owner/home/pull/7", branch,
                                "poller", 42, "operator", REPO, surface)
    scope = PollerProposalScope("poller:papers", "review:42", "review", "review:42", surface, branch)
    monkeypatch.setattr(proposals, "_review_details", lambda *args: ("review", [path]))
    original_run = proposals._run
    calls = []
    def run(args, **kw):
        if args[0] == "gh":
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, "ok", "")
        return original_run(args, **kw)
    monkeypatch.setattr(proposals, "_run", run)
    opened = proposals.open_proposal(home, lane="poller", poller=scope, revision=revision)
    assert opened.ok
    (opened.worktree / path).write_text(revised)
    result = proposals.finalize_proposal(home, lane="poller", poller=scope,
                                         revision=revision, title="Fix", rationale="Addressed review point")
    assert result.ok and result.reused_pr and result.pr_url == revision.url
    expected_body = "Addressed review point\n\n"
    if surface == "social-outbox":
        expected_body += "Submission title: Fix\n\n"
    expected_body += (
        "Untrusted-ingest source (not verified): review\n"
        "Trusted origin_ref: review:42\nTurn: review:42\n\n"
        "Untrusted-origin proposal: requires operator review and must never auto-merge."
    )
    assert_revision_notifications(calls, revision, expected_body)
    git("fetch", "origin", branch, cwd=home)
    assert git("show", f"origin/{branch}:{path}", cwd=home).stdout == revised


@pytest.mark.parametrize("branch,lane", [
    ("proposal/example", "agent"),
    ("upgrade/example", "upgrade"),
    ("poller/papers/turn-abcdef", "poller"),
    ("poller/papers/social-outbox", "poller"),
])
def test_revision_dispatch_authority_and_dedup(tmp_path, monkeypatch, branch, lane):
    forge(monkeypatch, branch=branch)
    events = []
    async def enqueue(event):
        events.append(event)
        return True
    scheduler = Scheduler(tmp_path / "scheduler.yaml", enqueue, home=tmp_path,
                          policy=core_policy_registry())
    if lane == "poller":
        surface = "social-outbox" if branch.endswith("social-outbox") else "wiki"
        authority = build_trigger_service_principal(
            canonical="poller:papers", trigger="poller", profile="research",
            tier=CapabilityTier.SCOPE_CONTAINED,
            capabilities=("open_proposal", "submit_proposal", "abandon_proposal",
                          "read_file", "write_file", "edit_file"),
            roots=(tmp_path / "state/pollers/papers",), proposal_surface=surface,
            creation_path="test", policy=core_policy_registry(),
        )
        scheduler._pollers["papers"] = SimpleNamespace(resolved_authority=lambda: authority)
    assert scheduler.add_proposal_revision_job(tmp_path)
    async def run_twice():
        await scheduler._callables["proposal-revisions"].fn()
        await scheduler._callables["proposal-revisions"].fn()
    asyncio.run(run_twice())
    assert len(events) == 1
    event = events[0]
    assert event.extra["proposal_revision"].lane == lane
    assert event.service_authority.canonical == (
        "poller:papers" if lane == "poller" else "system" if lane == "upgrade" else "proposal-revision"
    )
    if lane == "poller":
        assert event.ifc_labels.sources[0].integrity == "untrusted"
        assert event.service_authority.proposal_surface == surface
    monkeypatch.setenv("MIMIR_HOME", str(tmp_path))
    context = _create_turn_auth_context(event, None, policy_version=None, enforce=True,
                                        ifc_labels=event.ifc_labels or InformationFlowLabels(),
                                        policy=core_policy_registry())
    assert context.proposal_revision == event.extra["proposal_revision"]
    assert context.service_authority == event.service_authority
    prompt = build_turn_prompt(event)
    if lane == "poller":
        assert "| Revise the existing" in prompt
    else:
        assert "\nRevise the existing" in prompt
        assert "| Revise the existing" not in prompt
    if lane == "poller":
        assert context.poller_proposal_state.scope.surface == surface
        assert context.poller_proposal_state.scope.revision_branch == branch
    else:
        worktree = proposals._worktree_dir(tmp_path, branch, lane=lane)
        permitted = worktree / "prompts/example.md"
        forbidden = worktree / "state/wiki/example.md"
        permitted.parent.mkdir(parents=True)
        assert access_control._target_within_revision_proposal(
            str(permitted), "MIMIR_HOME/scratch/proposals/agent", auth_context=context,
        )
        assert not access_control._target_within_revision_proposal(
            str(forbidden), "MIMIR_HOME/scratch/proposals/agent", auth_context=context,
        )
