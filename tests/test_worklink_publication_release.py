from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

import pytest

pytest.importorskip("mimir_cultivate", reason="Worklink publication belongs to Cultivate")

from mimir.cli import main
from mimir_cultivate.worklink.claims import ChainlinkClaims, ClaimRecord
from mimir_cultivate.worklink.orchestrator import WorklinkError, _leaf_publication
from mimir_cultivate.worklink.publication_release import publication_release_path
from mimir_cultivate.worklink.run_state import WorklinkRunState, save_run_state

PR_URL = "https://github.com/example/project/pull/214"


@pytest.fixture
def release(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import control, autonomy

    monkeypatch.setenv("MIMIR_PLUGINS", "cultivate")
    monkeypatch.setattr(autonomy, "chainlink_bin", lambda: "chainlink")
    evidence = tmp_path / "state/worklink/evidence/441-1.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(json.dumps({"status": "blocked", "pr_url": PR_URL}))
    forge = {
        "number": 214, "html_url": PR_URL, "state": "closed", "merged": False,
        "merged_at": None, "base": {"repo": {"full_name": "example/project"}, "ref": "main"},
    }
    calls = []
    controls = {"forge_error": None, "comment_error": False, "locks": []}

    def run(args):
        calls.append(list(args))
        if args == ["chainlink", "locks", "list", "--json"]:
            return subprocess.CompletedProcess(args, 0, json.dumps(controls["locks"]), "")
        if args == ["gh", "api", "repos/example/project/pulls/214"]:
            error = controls["forge_error"]
            if isinstance(error, Exception):
                raise error
            if error == "exit":
                return subprocess.CompletedProcess(args, 1, "", "forge unavailable")
            return subprocess.CompletedProcess(args, 0, error or json.dumps(forge), "")
        if args[:3] == ["chainlink", "issue", "comment"]:
            return subprocess.CompletedProcess(args, int(controls["comment_error"]), "", "")
        raise AssertionError(args)

    monkeypatch.setattr(control, "_runner", lambda *args: run)

    def invoke(*, issue=441, attempt=1, reason="Earlier PR conflicts with merged work"):
        with pytest.raises(SystemExit) as exc:
            main([
                "worklink", "release-publication", str(issue), "--attempt", str(attempt),
                "--reason", reason, "--home", str(tmp_path),
            ])
        return exc.value.code

    return invoke, evidence, forge, calls, controls


@pytest.mark.parametrize("status", ["blocked", "failed", "completed"])
def test_release_closed_unmerged_publication_is_audited(tmp_path, release, status):
    invoke, evidence, _, calls, _ = release
    evidence.write_text(json.dumps({"status": status, "pr_url": PR_URL}))
    original = evidence.read_bytes()
    with pytest.raises(WorklinkError, match="publication already recorded"):
        with _leaf_publication(tmp_path, 441, 2):
            pytest.fail("unreleased publication passed")
    assert invoke() == 0
    record = json.loads(publication_release_path(tmp_path, evidence).read_text())
    assert record["issue_id"] == 441
    assert record["attempt"] == 1
    assert record["pr_url"] == PR_URL
    assert record["reason"] == "Earlier PR conflicts with merged work"
    assert datetime.fromisoformat(record["released_at"]).tzinfo is not None
    events = [json.loads(line) for line in (tmp_path / "logs/events.jsonl").read_text().splitlines()]
    event = next(e for e in events if e["type"] == "worklink_publication_released")
    assert all(event[k] == v for k, v in record.items())
    comment = next(c[-1] for c in calls if c[:3] == ["chainlink", "issue", "comment"])
    assert json.loads(comment.removeprefix("WORKLINK_PUBLICATION_RELEASED ")) == record
    assert evidence.read_bytes() == original
    assert ChainlinkClaims(agent_id="test", home_path=tmp_path).review_ready_evidence(441) is None
    with _leaf_publication(tmp_path, 441, 2):
        pass


@pytest.mark.parametrize("state,merged", [("open", False), ("closed", True)], ids=["open", "merged"])
def test_release_publication_refuses_open_or_merged(tmp_path, release, state, merged):
    invoke, evidence, forge, calls, _ = release
    forge.update(state=state, merged=merged)
    if merged:
        forge.update(merged_at="2026-11-26T21:00:00Z", merge_commit_sha="a" * 40)
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()
    assert not any(c[:3] == ["chainlink", "issue", "comment"] for c in calls)
    with pytest.raises(WorklinkError, match="publication already recorded"):
        with _leaf_publication(tmp_path, 441, 2):
            pass


@pytest.mark.parametrize("error", [
    "exit", "invalid json", "[]", "{}", OSError("unreachable"),
    subprocess.TimeoutExpired("gh", 10),
])
def test_release_publication_forge_read_fails_closed(tmp_path, release, error):
    invoke, evidence, _, calls, controls = release
    controls["forge_error"] = error
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()
    assert not any(c[:3] == ["chainlink", "issue", "comment"] for c in calls)


@pytest.mark.parametrize("field,value", [
    ("state", None), ("merged", None), ("merged", "false"),
    ("html_url", "https://github.com/other/project/pull/214"), ("number", 215),
])
def test_release_publication_invalid_forge_state(tmp_path, release, field, value):
    invoke, evidence, forge, _, _ = release
    forge[field] = value
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()


def test_release_publication_refuses_running_attempt(tmp_path, release):
    invoke, evidence, _, calls, _ = release
    save_run_state(tmp_path, WorklinkRunState(
        issue_id=441, attempt=2, backend="fake", compute_name="local_subprocess",
        handle_substrate="local_subprocess", handle_identifier=str(os.getpid()),
        branch="worklink/441-2", base_ref="main", local_base="main",
        repo=str(tmp_path), repo_url="", test_command=None,
        started_at=datetime.now(UTC).isoformat(),
    ))
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()
    assert calls == []


@pytest.mark.parametrize("locks", [[{"issue_id": 441}], {"locks": None}])
def test_release_publication_refuses_active_or_unknown_claim(tmp_path, release, locks):
    invoke, evidence, _, _, controls = release
    controls["locks"] = locks
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()


@pytest.mark.parametrize("failure", ["event", "comment", "write"])
def test_release_publication_requires_audit_and_durable_record(tmp_path, monkeypatch, release, failure):
    import mimir._atomic as atomic
    import mimir_cultivate.commands as commands

    invoke, evidence, _, _, controls = release

    def fail(*args, **kwargs):
        raise OSError("audit unavailable")

    if failure == "event":
        monkeypatch.setattr(commands, "log_durable_event_sync", fail)
    elif failure == "comment":
        controls["comment_error"] = True
    else:
        monkeypatch.setattr(atomic, "atomic_write_json", fail)
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()
    with pytest.raises(WorklinkError, match="publication already recorded"):
        with _leaf_publication(tmp_path, 441, 2):
            pass


@pytest.mark.parametrize("payload", [
    {"status": "running", "pr_url": PR_URL}, {}, [], {"status": "blocked"},
    {"status": "blocked", "pr_url": "https://evil.invalid/pull/214"},
])
def test_release_publication_requires_terminal_pr_evidence(tmp_path, release, payload):
    invoke, evidence, _, _, _ = release
    evidence.write_text(json.dumps(payload))
    assert invoke() == 1
    assert not publication_release_path(tmp_path, evidence).exists()


@pytest.mark.parametrize("issue,attempt,reason", [(0, 1, "reason"), (441, 0, "reason"), (441, 1, " ")])
def test_release_publication_requires_explicit_identity_and_reason(release, issue, attempt, reason):
    invoke, _, _, calls, _ = release
    assert invoke(issue=issue, attempt=attempt, reason=reason) == 1
    assert calls == []


@pytest.mark.parametrize("marker", ["{", "[]", "{}", '{"pr_url":"https://github.com/example/project/pull/999"}'])
def test_release_publication_invalid_marker_does_not_release(tmp_path, release, marker):
    _, evidence, _, _, _ = release
    path = publication_release_path(tmp_path, evidence)
    path.parent.mkdir(parents=True)
    path.write_text(marker)
    with pytest.raises(WorklinkError, match="publication already recorded"):
        with _leaf_publication(tmp_path, 441, 2):
            pass


def test_release_publication_does_not_cover_other_attempt(tmp_path, release):
    invoke, evidence, _, _, _ = release
    assert invoke() == 0
    evidence.with_name("441-2.json").write_bytes(evidence.read_bytes())
    with pytest.raises(WorklinkError, match="publication already recorded"):
        with _leaf_publication(tmp_path, 441, 3):
            pass


def test_release_publication_retained_intent_is_unchanged(tmp_path, release):
    invoke, _, _, _, _ = release
    path = tmp_path / "state/worklink/publications/441.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"issue":441,"attempt":1}')
    original = path.read_bytes()
    assert invoke() == 0
    assert path.read_bytes() == original
    with pytest.raises(FileExistsError):
        with _leaf_publication(tmp_path, 441, 2):
            pass


@pytest.mark.usefixtures("authorized_fake_backend_gate")
def test_released_publication_next_attempt_publishes(tmp_path, monkeypatch, release):
    from tests.test_worklink_orchestrator import (
        ISSUE_JSON, FakeBackend, _orchestrator_runner,
    )
    from mimir_cultivate.worklink import daemon_health
    from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig
    from mimir_cultivate.worklink.orchestrator import WorklinkRunner

    invoke, _, _, _, _ = release
    assert invoke() == 0
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path / ".worklink"))
    monkeypatch.setattr(daemon_health, "daemon_health", lambda env: {"ok": True})
    repo = tmp_path / "repo"
    issue = json.loads(ISSUE_JSON)
    issue["comments"] = [ClaimRecord(
        441, 1, "earlier", datetime(2020, 1, 1, tzinfo=UTC),
    ).to_comment()]
    calls, runner = _orchestrator_runner(
        repo, tmp_path / ".worklink/repo/441-2", issue_json=json.dumps(issue),
    )
    registry = BackendRegistry(WorklinkConfig())
    registry.register(FakeBackend())
    result = asyncio.run(WorklinkRunner(
        home=tmp_path, repo=repo, runner=runner, registry=registry,
    ).run(441, backend_name="fake", test_command="echo ok"))
    assert result.status == "completed"
    assert result.attempt == 2
    assert sum(c[:3] == ["gh", "pr", "create"] for c in calls if isinstance(c, list)) == 1
