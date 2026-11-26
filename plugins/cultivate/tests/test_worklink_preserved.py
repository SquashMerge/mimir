"""Unpublished Worklink attempts are preserved as reference, never as authority (#2114)."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

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


@pytest.mark.parametrize("layout", [("checkout",), ()], ids=["worker", "controller"])
def test_retained_epic_attempt_preserves_its_factory_sandbox(rig, tmp_path: Path, layout) -> None:
    from types import SimpleNamespace

    from mimir_cultivate.worklink.preserved import preserve_retained_attempt

    home, operator, _forge, _checkout, _base = rig
    attempt_dir = tmp_path / "attempts" / "9-2"
    intake = attempt_dir.joinpath(*layout)
    intake.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "-q", str(operator), str(intake)], check=True)
    sandbox = intake / ".factory-sandboxes" / "chainlink-9"
    subprocess.run(["git", "clone", "-q", str(operator), str(sandbox)], check=True)
    _git(sandbox, "checkout", "-q", "-b", "feature/chainlink-9")
    # Only the sandbox changed: the factory's implementation lives there.
    (sandbox / "factory_work.py").write_text("DONE = True\n")
    record = SimpleNamespace(sandbox=str(sandbox), branch="feature/chainlink-9")
    stranger = SimpleNamespace(sandbox=str(tmp_path / "attempts" / "9-1" / "checkout"
                                           / ".factory-sandboxes" / "chainlink-9"), branch="x")
    preserved = preserve_retained_attempt(
        home=home, trusted_repo=operator, attempt_dir=attempt_dir,
        factory_records=(stranger, record), default_base="main", runner=_run,
        reason_if_unknown="interrupted",
    )
    assert preserved is not None and preserved.error is None
    assert "factory_work.py" in preserved.files
    assert "factory_work.py" in _git(operator, "show", "--stat", "refs/worklink/9/2")


def _periodic_rig(rig, monkeypatch):
    from mimir_cultivate.worklink import autonomy

    home, operator, _, checkout, _ = rig
    events, comments = [], []
    monkeypatch.setattr(autonomy, "_log_best_effort", lambda event, **kw: events.append((event, kw)))

    def run(args):
        if args[1:3] == ["issue", "show"]:
            return subprocess.CompletedProcess(args, 0, '{"status":"closed"}', "")
        if args[1:3] == ["issue", "comment"]:
            comments.append(args[-1])
            return subprocess.CompletedProcess(args, 0, "", "")
        return _run(args)

    def cycle(*, closed=False):
        autonomy._preserve_before_prune(
            home, operator, checkout.parent, (), run=run,
            defaults=SimpleNamespace(base_branch="main", preserved_attempts_keep=3),
        )
        if closed:
            autonomy._prune_preserved_for_closed_issues(home, operator, run=run)

    return cycle, events, comments


def _lifecycle(home, attempt=1):
    return json.loads((home / "state/worklink/preservation-state/7" / f"{attempt}.json").read_text())


@pytest.mark.parametrize("object_type", ["commit", "tree"])
def test_corrupt_retained_attempt_is_terminal_across_periodic_cycles(rig, monkeypatch, object_type):
    home, _, _, checkout, _ = rig
    _change_everything(checkout)
    oid = _git(checkout, "rev-parse", f"HEAD^{{{object_type}}}")
    (checkout / ".git/objects" / oid[:2] / oid[2:]).unlink()
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    capture = preserved_mod.ControllerGitPublication.capture
    captures = []

    def counted_capture(*args, **kwargs):
        captures.append(1)
        return capture(*args, **kwargs)

    monkeypatch.setattr(preserved_mod.ControllerGitPublication, "capture", counted_capture)
    cycle()
    saved = _lifecycle(home)
    assert saved["status"] == "unpreservable" and saved["failures"] == 1
    assert saved["error"] and saved["ref"] is None
    for _ in range(10):
        cycle()
    assert len(captures) == 1  # proves Git wasn't retried, not just reporting suppressed
    assert _lifecycle(home) == saved
    assert [event for event, _ in events] == ["worklink_attempt_preserved"]
    assert len(comments) == 1 and 'status="unpreservable"' in comments[0]


@pytest.mark.parametrize("corrupt", [False, True])
def test_closed_issue_pruning_is_durable_and_second_cycle_is_noop(rig, monkeypatch, corrupt):
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    if corrupt:
        oid = _git(checkout, "rev-parse", "HEAD")
        (checkout / ".git/objects" / oid[:2] / oid[2:]).unlink()
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle(closed=True)
    assert _lifecycle(home)["status"] == "pruned"
    assert not list_preserved(home, 7)
    assert not (home / "state/worklink/preserved/7/1.patch").exists()
    assert not (home / "state/worklink/preserved/7/1.json").exists()
    assert _git(operator, "for-each-ref", "refs/worklink") == ""
    assert checkout.exists()  # no unsafe deletion of worker-owned checkouts
    assert [event for event, _ in events] == ["worklink_attempt_preserved", "worklink_preserved_pruned"]
    assert len(comments) == 2
    before = list(events), list(comments)
    for _ in range(10):
        cycle(closed=True)
    assert (events, comments) == before


@pytest.mark.parametrize("error", [
    subprocess.TimeoutExpired(["git"], 120),
    preserved_mod.SafeGitError("could not initialize private Git index: index.lock: File exists"),
    RuntimeError("temporarily unavailable"),
], ids=["timeout", "lock", "unknown"])
def test_transient_failure_retries_with_backoff_and_a_hard_bound(rig, monkeypatch, error):
    home, _, _, _, _ = rig
    clock = [1000.0]
    monkeypatch.setattr(preserved_mod, "time", SimpleNamespace(time=lambda: clock[0]))
    calls = []

    def failing_checkout(*args, **kwargs):
        calls.append(1)
        raise error

    monkeypatch.setattr(preserved_mod, "_checkout_git", failing_checkout)
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle()
    assert _lifecycle(home)["status"] == "retry_pending"
    for retry in range(1, preserved_mod.MAX_PRESERVATION_ATTEMPTS):
        due = clock[0] + preserved_mod.RETRY_BACKOFF_S * 2 ** (retry - 1)
        assert _lifecycle(home)["retry_after"] == due
        clock[0] = due - 1
        cycle()
        assert len(calls) == retry
        clock[0] = due
        cycle()
        assert len(calls) == retry + 1
    assert _lifecycle(home)["status"] == "unpreservable"
    assert _lifecycle(home)["failures"] == preserved_mod.MAX_PRESERVATION_ATTEMPTS
    assert len(comments) == 2  # pending -> exhausted, not one per retry
    assert len(events) == 2
    clock[0] += 10**9
    for _ in range(10):
        cycle()
    assert len(calls) == preserved_mod.MAX_PRESERVATION_ATTEMPTS
    assert len(comments) == len(events) == 2


def test_transient_failure_can_recover_and_comments_only_on_change(rig, monkeypatch):
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    clock = [1000.0]
    monkeypatch.setattr(preserved_mod, "time", SimpleNamespace(time=lambda: clock[0]))
    capture = preserved_mod._checkout_git

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(["git"], 120)

    monkeypatch.setattr(preserved_mod, "_checkout_git", timeout)
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle()
    assert prune_preserved(home, operator, 7, keep=3, runner=_run) == []
    monkeypatch.setattr(preserved_mod, "_checkout_git", capture)
    cycle()
    assert len(comments) == 1
    clock[0] += preserved_mod.RETRY_BACKOFF_S
    cycle()
    assert _lifecycle(home)["status"] == "preserved"
    assert len(comments) == len(events) == 2
    cycle()
    assert len(comments) == len(events) == 2


def test_factory_preservation_of_missing_checkout_reports_once(rig, monkeypatch):
    from mimir_cultivate.worklink import orchestrator

    home, operator, _, checkout, _ = rig
    events, comments = [], []
    monkeypatch.setattr(orchestrator, "_log_event", lambda event, **kw: events.append(event))
    record = SimpleNamespace(sandbox=str(checkout / "gone"), branch="epic/7", issue_id=7, attempt=1)
    claims = SimpleNamespace(_run=lambda *args: comments.append(args))
    runner = lambda args, **_: _run(args)
    worklink = orchestrator.WorklinkRunner(home=home, repo=operator, runner=runner)
    for _ in range(10):
        worklink._preserve_factory_attempt(claims, record, "failed", runner)
    assert _lifecycle(home)["status"] == "unpreservable"
    assert len(comments) == 1
    assert events == ["worklink_attempt_preserved"]


def test_legacy_failed_record_migrates_to_terminal_state(rig, monkeypatch):
    home, _, _, checkout, _ = rig
    directory = home / "state/worklink/preserved/7"
    directory.mkdir(parents=True)
    (directory / "1.json").write_text(json.dumps({"issue": 7, "attempt": 1, "error": "old failure"}))
    oid = _git(checkout, "rev-parse", "HEAD")
    (checkout / ".git/objects" / oid[:2] / oid[2:]).unlink()
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle()
    cycle()
    assert _lifecycle(home)["status"] == "unpreservable"
    assert len(events) == len(comments) == 1


def test_prune_failure_does_not_claim_a_state_change(rig):
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    _preserve(home, operator, checkout)

    def failing_run(args):
        return subprocess.CompletedProcess(args, 1, "", "cannot lock ref")

    with pytest.raises(RuntimeError, match="cannot lock ref"):
        prune_preserved(home, operator, 7, keep=0, runner=failing_run)
    assert _lifecycle(home)["status"] == "preserved"
    assert len(list_preserved(home, 7)) == 1
    assert prune_preserved(home, operator, 7, keep=0, runner=_run) == [1]
    assert prune_preserved(home, operator, 7, keep=0, runner=_run) == []


@pytest.mark.parametrize("payload", ["not json", "[]"])
def test_unreadable_lifecycle_never_authorizes_another_snapshot(rig, monkeypatch, payload):
    home, _, _, _, _ = rig
    directory = home / "state/worklink/preservation-state/7"
    directory.mkdir(parents=True)
    (directory / "1.json").write_text(payload)
    calls = []
    monkeypatch.setattr(preserved_mod, "_checkout_git", lambda *a, **kw: calls.append(1))
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle()
    assert not calls and not events and not comments


def test_dispatch_sweep_does_not_reselect_retention_pruned_attempt(rig, monkeypatch):
    from mimir_cultivate.worklink import checkout as checkout_mod, orchestrator

    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    monkeypatch.setattr(checkout_mod, "_attempt_roots", lambda *a: [(checkout.parent.parent, True)])
    events, comments = [], []
    monkeypatch.setattr(orchestrator, "_log_event", lambda event, **kw: events.append(event))
    runner = lambda args, **_: _run(args)
    comment = lambda *args: comments.append(args)
    orchestrator._sweep_retained_attempts(home, operator, 7, 2, runner=runner, comment=comment)
    assert len(comments) == 1
    assert prune_preserved(home, operator, 7, keep=0, runner=_run) == [1]
    for _ in range(10):
        orchestrator._sweep_retained_attempts(home, operator, 7, 2, runner=runner, comment=comment)
    assert len(comments) == 1 and events == ["worklink_attempt_preserved"]


@pytest.mark.parametrize("only_unreadable", [False, True])
def test_unreadable_untracked_is_omitted_and_reported_without_retries(rig, monkeypatch, only_unreadable):
    home, operator, _, checkout, _ = rig
    if not only_unreadable:
        _change_everything(checkout)
        # Worker filenames must never select files through pathspec magic.
        (checkout / ":(glob)**").write_text("literal filename\n")
    target = checkout / "target-ratchet" / "private.lock"
    target.parent.mkdir()
    target.write_bytes(b"private")
    target.chmod(0o600)
    original = Path.open

    def denied(path, *args, **kwargs):
        if path == target:
            raise PermissionError("worker-private lock")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied)
    cycle, events, comments = _periodic_rig(rig, monkeypatch)
    cycle()
    record = _lifecycle(home)
    assert record["status"] == "preserved" and record["failures"] == 0
    assert record["omitted_paths"] == ["target-ratchet/private.lock"]
    assert len(events) == 1 and events[0][0] == "worklink_attempt_preserved"
    assert events[0][1]["omitted_paths"] == ["target-ratchet/private.lock"]
    assert 'omitted_paths=["target-ratchet/private.lock"]' in comments[0]
    names = _git(operator, "ls-tree", "-r", "--name-only", preserved_ref(7, 1)).splitlines()
    assert "target-ratchet/private.lock" not in names
    if not only_unreadable:
        assert {"app.py", "committed.py", "untracked_module.py", ":(glob)**"} <= set(names)
        assert "changed, uncommitted" in _git(operator, "show", f"{preserved_ref(7, 1)}:app.py")
    for _ in range(10):
        cycle()
    assert len(events) == len(comments) == 1
    assert _lifecycle(home) == record
    assert target.exists()  # Omission never deletes worker work.


def test_unreadable_tracked_work_is_not_silently_omitted(rig, monkeypatch):
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    original = preserved_mod.ControllerGitPublication.run

    def denied(git, *args, **kwargs):
        if args[:2] == ("add", "-u"):
            return subprocess.CompletedProcess(args, 1, "", "app.py: Permission denied")
        return original(git, *args, **kwargs)

    monkeypatch.setattr(preserved_mod.ControllerGitPublication, "run", denied)
    record = _preserve(home, operator, checkout)
    assert record.status == "retry_pending"
    assert record.ref is None and "app.py: Permission denied" in record.error
    assert record.omitted_paths == []


@pytest.mark.parametrize("operation", ["list", "add"])
def test_untracked_git_errors_do_not_report_success(rig, monkeypatch, operation):
    home, operator, _, checkout, _ = rig
    _change_everything(checkout)
    original = preserved_mod.ControllerGitPublication.run

    def failed(git, *args, **kwargs):
        if (operation == "list" and args[0] == "ls-files") or (
            operation == "add" and args[:2] == ("--literal-pathspecs", "add")
        ):
            return subprocess.CompletedProcess(args, 1, "", "untracked work unavailable")
        return original(git, *args, **kwargs)

    monkeypatch.setattr(preserved_mod.ControllerGitPublication, "run", failed)
    record = _preserve(home, operator, checkout)
    assert record.status == "retry_pending"
    assert record.ref is None and "untracked work unavailable" in record.error


def test_staging_never_opens_special_untracked_files(tmp_path, monkeypatch):
    target = tmp_path / "fifo"
    os.mkfifo(target)
    calls = []

    def run(*args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "fifo\0" if args[0] == "ls-files" else "", "")

    def unexpected_open(*args, **kwargs):
        pytest.fail("opening a worker FIFO can block preservation indefinitely")

    monkeypatch.setattr(Path, "open", unexpected_open)
    omitted = []
    preserved_mod._stage_attempt(SimpleNamespace(run=run), tmp_path, omitted)
    assert omitted == ["fifo"]
    assert calls == [("add", "-u"), ("ls-files", "-z", "--others", "--exclude-standard")]
