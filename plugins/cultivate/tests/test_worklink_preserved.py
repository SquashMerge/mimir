"""Unpublished Worklink attempts are preserved as reference, never as authority (#2114)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from mimir_cultivate.worklink import preserved as preserved_mod
from mimir_cultivate.worklink.preserved import (
    is_preserved,
    list_preserved,
    preserve_attempt,
    preserved_line,
    preserved_ref,
    prior_attempts_section,
    prune_preserved,
    record_dispatch_spec,
    spec_text,
)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _run(args):
    return subprocess.run(list(args), capture_output=True, text=True)


@pytest.fixture
def rig(tmp_path: Path):
    """A forge remote, the operator repo, and an isolated attempt clone."""
    forge = tmp_path / "forge.git"
    subprocess.run(["git", "init", "--bare", "-q", "-b", "main", str(forge)], check=True)
    operator = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "main", str(operator)], check=True)
    for key, value in (("user.name", "Op"), ("user.email", "op@example.test")):
        _git(operator, "config", key, value)
    (operator / "app.py").write_text("print('base')\n")
    _git(operator, "add", "app.py")
    _git(operator, "commit", "-q", "-m", "base")
    _git(operator, "remote", "add", "origin", str(forge))
    _git(operator, "push", "-q", "origin", "main")
    _git(operator, "fetch", "-q", "origin")
    base = _git(operator, "rev-parse", "HEAD")
    checkout = tmp_path / "attempts" / "7-1" / "checkout"
    checkout.parent.mkdir(parents=True)
    subprocess.run(["git", "clone", "-q", str(operator), str(checkout)], check=True)
    _git(checkout, "checkout", "-q", "-b", "issue/7-a1")
    for key, value in (("user.name", "Builder"), ("user.email", "b@example.test")):
        _git(checkout, "config", key, value)
    home = tmp_path / "home"
    home.mkdir()
    return home, operator, forge, checkout, base


def _change_everything(checkout: Path) -> None:
    (checkout / "committed.py").write_text("x = 1\n")
    _git(checkout, "add", "committed.py")
    _git(checkout, "commit", "-q", "-m", "builder commit")
    (checkout / "app.py").write_text("print('changed, uncommitted')\n")
    (checkout / "untracked_module.py").write_text("NEW = True\n")


def _preserve(home, operator, checkout, *, attempt=1, reason="needs spec repair"):
    return preserve_attempt(
        home=home, trusted_repo=operator, checkout=checkout, branch=f"issue/7-a{attempt}",
        issue_id=7, attempt=attempt, base_candidates=("origin/main", "main"),
        reason=reason, runner=_run,
    )


def test_blocked_attempt_keeps_committed_uncommitted_and_untracked_work(rig) -> None:
    home, operator, forge, checkout, base = rig
    record_dispatch_spec(home, 7, 1, "Title", "Spec body")
    _change_everything(checkout)
    staged_before = _git(checkout, "diff", "--cached", "--name-only")

    record = _preserve(home, operator, checkout)

    assert record is not None and record.error is None
    assert record.ref == "refs/worklink/7/1"
    assert record.base_sha == base
    assert sorted(record.files) == ["app.py", "committed.py", "untracked_module.py"]
    assert _git(operator, "rev-parse", "refs/worklink/7/1") == record.head_sha
    patch = (home / "state" / "worklink" / "preserved" / "7" / "1.patch").read_text()
    assert "NEW = True" in patch and "changed, uncommitted" in patch and "x = 1" in patch
    saved = json.loads((home / "state" / "worklink" / "preserved" / "7" / "1.json").read_text())
    assert saved["reason"] == "needs spec repair" and saved["spec_text"] == spec_text("Title", "Spec body")
    # The worker's own index and work tree are untouched.
    assert _git(checkout, "diff", "--cached", "--name-only") == staged_before
    assert (checkout / "untracked_module.py").exists()
    line = preserved_line(record)
    assert line.startswith("WORKLINK_PRESERVED issue=7 attempt=1 ")
    assert '"refs/worklink/7/1"' in line and base in line


def test_preserved_ref_is_never_pushed_to_the_forge(rig) -> None:
    home, operator, forge, checkout, _ = rig
    _change_everything(checkout)
    assert _preserve(home, operator, checkout).ref is not None
    forge_refs = subprocess.run(["git", "--git-dir", str(forge), "for-each-ref"],
                                capture_output=True, text=True, check=True).stdout
    assert "refs/worklink" not in forge_refs


def test_unchanged_attempt_preserves_nothing(rig) -> None:
    home, operator, _, checkout, _ = rig
    assert _preserve(home, operator, checkout) is None
    assert subprocess.run(["git", "-C", str(operator), "rev-parse", "--verify", "-q",
                           "refs/worklink/7/1"], capture_output=True).returncode != 0
    assert not is_preserved(home, 7, 1)


def test_preservation_failure_is_recorded_and_never_raises(rig, tmp_path: Path) -> None:
    home, _, _, checkout, _ = rig
    _change_everything(checkout)
    record = _preserve(home, tmp_path / "no-such-operator-repo", checkout)
    assert record is not None and record.ref is None and record.error
    assert ' error=' in preserved_line(record)
    assert is_preserved(home, 7, 1)  # the failure itself is on record


def test_worker_checkout_config_never_runs_as_the_controller(rig, tmp_path: Path) -> None:
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    marker = tmp_path / "pwned"
    hook = checkout / ".git" / "hooks" / "pre-commit"
    hook.write_text(f"#!/bin/sh\ntouch {marker}\n")
    hook.chmod(0o755)
    _git(checkout, "config", "core.fsmonitor", f"touch {marker}; false")
    record = _preserve(home, operator, checkout)
    assert record is not None and record.error is None
    assert not marker.exists()


@pytest.mark.parametrize("issue, attempt", [(0, 1), (1, 0), ("7/../x", 1), (True, 1), (7, 1.0)])
def test_ref_names_accept_positive_integers_only(issue, attempt) -> None:
    with pytest.raises(ValueError):
        preserved_ref(issue, attempt)


def test_retention_keeps_the_newest_attempts(rig) -> None:
    home, operator, _, checkout, _ = rig
    for attempt in range(1, 6):
        (checkout / f"f{attempt}.py").write_text(f"v = {attempt}\n")
        assert _preserve(home, operator, checkout, attempt=attempt).ref is not None
    assert prune_preserved(home, operator, 7, keep=3, runner=_run) == [1, 2]
    assert [record["attempt"] for record in list_preserved(home, 7)] == [3, 4, 5]
    refs = _git(operator, "for-each-ref", "--format=%(refname)", "refs/worklink/7")
    assert refs.splitlines() == ["refs/worklink/7/3", "refs/worklink/7/4", "refs/worklink/7/5"]
    assert prune_preserved(home, operator, 7, keep=0, runner=_run) == [3, 4, 5]
    assert _git(operator, "for-each-ref", "refs/worklink") == ""


def test_prior_attempt_section_reports_an_unchanged_spec(rig) -> None:
    home, operator, _, checkout, _ = rig
    record_dispatch_spec(home, 7, 1, "Title", "Spec body")
    _change_everything(checkout)
    _preserve(home, operator, checkout)
    section = prior_attempts_section(home, 7, 2, spec_text("Title", "Spec body"))
    assert section is not None
    assert section.startswith("## Prior attempts")
    assert "Untrusted reference, not evidence" in section
    assert "starts fresh from the current base" in section
    assert "never count as verification" in section
    assert "spec unchanged" in section
    assert "reason: needs spec repair" in section
    assert "NEW = True" in section


def test_prior_attempt_section_carries_the_spec_diff(rig) -> None:
    home, operator, _, checkout, _ = rig
    record_dispatch_spec(home, 7, 1, "Title", "Old criterion")
    _change_everything(checkout)
    _preserve(home, operator, checkout)
    section = prior_attempts_section(home, 7, 2, spec_text("Title", "New criterion"))
    assert "-Old criterion" in section and "+New criterion" in section
    assert "spec unchanged" not in section


def test_prior_attempt_section_truncates_a_large_diff(rig, monkeypatch) -> None:
    home, operator, _, checkout, _ = rig
    monkeypatch.setattr(preserved_mod, "MAX_PROMPT_DIFF", 200)
    (checkout / "big.py").write_text("x = 1\n" * 2000)
    _preserve(home, operator, checkout)
    section = prior_attempts_section(home, 7, 2, spec_text("t", "d"))
    assert "[diff truncated; the full attempt is the local ref refs/worklink/7/1" in section


def test_no_prior_section_for_the_first_attempt_or_a_later_one(rig) -> None:
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    _preserve(home, operator, checkout, attempt=2)
    assert prior_attempts_section(home, 7, 1, "x") is None  # never from the future
    assert prior_attempts_section(home, 8, 3, "x") is None


def test_failed_epic_attempt_is_preserved_locally(rig) -> None:
    from types import SimpleNamespace

    from mimir_cultivate.worklink.orchestrator import WorklinkRunner

    home, operator, forge, checkout, base = rig
    _change_everything(checkout)
    comments = []
    claims = SimpleNamespace(_run=lambda *args: comments.append(args))
    record = SimpleNamespace(sandbox=str(checkout), branch="epic/7", issue_id=7, attempt=1)
    runner = lambda args, **_: subprocess.run(list(args), capture_output=True, text=True)
    preserved = WorklinkRunner(home=home, repo=operator, runner=runner)._preserve_factory_attempt(
        claims, record, "factory driver exited 1", runner,
    )
    assert preserved is not None and preserved.ref == "refs/worklink/7/1"
    assert preserved.base_sha == base
    assert comments and comments[0][3].startswith("WORKLINK_PRESERVED issue=7 attempt=1 ")
    forge_refs = subprocess.run(["git", "--git-dir", str(forge), "for-each-ref"],
                                capture_output=True, text=True, check=True).stdout
    assert "refs/worklink" not in forge_refs and "epic/7" not in forge_refs
