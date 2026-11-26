"""Worker-private and oversized output cannot crash or exhaust gate snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
import errno
import hashlib
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mimir_cultivate.worklink import orchestrator as orch
from mimir_cultivate.worklink.backends.base import RawResult
from mimir_cultivate.worklink.backends.feature_factory import FeatureFactoryBackend
from mimir_cultivate.worklink.backends.registry import WorklinkConfig
from mimir_cultivate.worklink.checkout import CheckoutLease
from mimir_cultivate.worklink.claims import ClaimRecord
from mimir_cultivate.worklink.compute import ComputeResult, WorkSpec


def _runner(args, **kwargs):
    kwargs.setdefault("text", True)
    return subprocess.run(
        args, shell=isinstance(args, str), capture_output=True, check=False, **kwargs,
    )


def _git(repo, *args):
    result = _runner(["git", "-C", str(repo), *args])
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@pytest.fixture
def checkout(tmp_path):
    repo = tmp_path / "checkout"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.test")
    (repo / "app.py").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")
    (repo / "app.py").write_text("builder\n")
    (repo / "target-ratchet").mkdir()
    return repo


def _deny_open(monkeypatch, target, error=None):
    original = Path.open

    def open_path(path, *args, **kwargs):
        if path == target:
            raise error or PermissionError(errno.EACCES, "worker-private")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_path)


@pytest.mark.parametrize("failure", ["open", "read", "io"])
def test_snapshot_unreadable_untracked_uses_metadata(checkout, monkeypatch, failure):
    target = checkout / "target-ratchet/private.lock"
    target.write_bytes(b"private")
    target.chmod(0o600)
    if failure == "read":
        original = Path.open

        class Unreadable:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self, limit):
                raise PermissionError(errno.EACCES, "read denied")

        monkeypatch.setattr(
            Path, "open",
            lambda path, *a, **kw: Unreadable() if path == target else original(path, *a, **kw),
        )
    else:
        error = PermissionError(errno.EACCES, "denied") if failure == "open" else OSError(errno.EIO, "io")
        _deny_open(monkeypatch, target, error)
    snapshot = orch._gate_checkout_snapshot(checkout, runner=_runner)
    assert snapshot.untracked == {"target-ratchet/private.lock"}
    fingerprint = snapshot.fingerprints["target-ratchet/private.lock"]
    assert fingerprint[:4] == (
        "unreadable", target.lstat().st_mode, 7, target.lstat().st_mtime_ns,
    )
    assert snapshot.fingerprints["app.py"] == (
        (checkout / "app.py").stat().st_mode, hashlib.sha256(b"builder\n").hexdigest(),
    )


@pytest.mark.parametrize("cap", ["file", "total", "growth"])
def test_snapshot_reads_are_bounded(checkout, monkeypatch, cap):
    monkeypatch.setattr(orch, "_GATE_SNAPSHOT_FILE_BYTES", 32)
    monkeypatch.setattr(orch, "_GATE_SNAPSHOT_TOTAL_BYTES", 48)
    targets = [checkout / f"target-ratchet/{i}.bin" for i in range(4)]
    for target in targets:
        target.write_bytes(b"x" * (33 if cap == "file" else 20))
    original = Path.open
    reads = []

    class Spy:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def fileno(self):
            return self.stream.fileno()

        def read(self, size=-1):
            assert 0 <= size <= 32, "unbounded per-file read"
            data = self.stream.read(size)
            reads.append(len(data))
            assert sum(reads) <= 48, "unbounded total read"
            return data

    def spy_open(path, *args, **kwargs):
        if cap == "file" and path in targets:
            pytest.fail("oversized file must not be opened")
        if cap == "growth" and path in targets:
            # Grow after lstat so a size-only check cannot bound the read.
            with original(path, "ab") as stream:
                stream.write(b"y" * 100)
        return Spy(original(path, *args, **kwargs))

    monkeypatch.setattr(Path, "open", spy_open)
    snapshot = orch._gate_checkout_snapshot(checkout, runner=_runner)
    assert reads and sum(reads) <= 48
    assert any(fp[0] == "bounded" for fp in snapshot.fingerprints.values())
    if cap == "growth":
        assert any(fp[0] == "unstable" for fp in snapshot.fingerprints.values())


@pytest.mark.parametrize("change", ["same_size", "short_read"])
def test_snapshot_detects_change_during_read(checkout, monkeypatch, change):
    target = checkout / "target-ratchet/racing.bin"
    target.write_bytes(b"before")
    original = Path.open

    class RacingRead:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def fileno(self):
            return self.stream.fileno()

        def read(self, limit):
            data = self.stream.read(limit)
            if change == "short_read":
                return data[:3]
            metadata = target.stat()
            with original(target, "wb") as writer:
                writer.write(b"after!")
            os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000))
            return data

    monkeypatch.setattr(
        Path, "open",
        lambda path, *a, **kw: RacingRead(original(path, *a, **kw))
        if path == target else original(path, *a, **kw),
    )
    snapshot = orch._gate_checkout_snapshot(checkout, runner=_runner)
    assert snapshot.fingerprints["target-ratchet/racing.bin"][0] == "unstable"


@pytest.mark.parametrize("fallback", ["unreadable", "bounded"])
@pytest.mark.parametrize("change", ["content", "mode", "mtime", "inode"])
def test_protected_metadata_changes_fail_closed(checkout, monkeypatch, fallback, change):
    target = checkout / "target-ratchet/private.lock"
    target.write_bytes(b"before")
    if fallback == "unreadable":
        _deny_open(monkeypatch, target)
    else:
        monkeypatch.setattr(orch, "_GATE_SNAPSHOT_FILE_BYTES", 4)
    before = orch._gate_checkout_snapshot(checkout, runner=_runner)
    if change == "content":
        # Same size; metadata, not size alone, must discriminate.
        with open(target, "wb") as stream:
            stream.write(b"after!")
    elif change == "mode":
        target.chmod(0o600)
    elif change == "mtime":
        metadata = target.stat()
        os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000))
    elif change == "inode":
        replacement = target.with_suffix(".new")
        replacement.write_bytes(b"before")
        replacement.replace(target)
    after = orch._gate_checkout_snapshot(checkout, runner=_runner, protected=before.fingerprints)
    assert before.fingerprints[target.relative_to(checkout).as_posix()] != after.fingerprints[
        target.relative_to(checkout).as_posix()
    ]


@pytest.mark.parametrize("fallback", ["unreadable", "bounded"])
def test_protected_same_size_rewrite_with_restored_mtime_is_changed(checkout, monkeypatch, fallback):
    target = checkout / "target-ratchet/private.lock"
    target.write_bytes(b"before")
    if fallback == "unreadable":
        _deny_open(monkeypatch, target)
    else:
        monkeypatch.setattr(orch, "_GATE_SNAPSHOT_FILE_BYTES", 4)
    before = orch._gate_checkout_snapshot(checkout, runner=_runner)
    metadata = target.lstat()
    # Cross even a coarse filesystem timestamp tick without changing mode,
    # size, inode or device. Restore mtime after an in-place gate rewrite.
    time.sleep(1.05)
    with open(target, "r+b") as stream:
        stream.write(b"after!")
    os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    current = target.lstat()
    assert current.st_ctime_ns != metadata.st_ctime_ns
    for field in ("st_mode", "st_size", "st_mtime_ns", "st_ino", "st_dev"):
        assert getattr(current, field) == getattr(metadata, field), field
    after = orch._gate_checkout_snapshot(checkout, runner=_runner, protected=before.fingerprints)
    path = "target-ratchet/private.lock"
    assert before.fingerprints[path][0] == after.fingerprints[path][0] == fallback
    assert before.fingerprints[path] != after.fingerprints[path]


@pytest.mark.parametrize("change", ["becomes_unreadable", "stat_denied"])
def test_protected_unreadable_transition_fails_closed(checkout, monkeypatch, change):
    target = checkout / "target-ratchet/private.lock"
    target.write_bytes(b"before")
    if change == "stat_denied":
        original = Path.lstat

        def denied(path, *args, **kwargs):
            if path == target:
                raise PermissionError(errno.EACCES, "lstat denied")
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "lstat", denied)
    before = orch._gate_checkout_snapshot(checkout, runner=_runner)
    if change == "becomes_unreadable":
        _deny_open(monkeypatch, target)
    after = orch._gate_checkout_snapshot(checkout, runner=_runner, protected=before.fingerprints)
    assert before.fingerprints["target-ratchet/private.lock"] != after.fingerprints[
        "target-ratchet/private.lock"
    ]


def test_protected_budget_is_independent_of_new_gate_byproducts(checkout, monkeypatch):
    monkeypatch.setattr(orch, "_GATE_SNAPSHOT_FILE_BYTES", 16)
    monkeypatch.setattr(orch, "_GATE_SNAPSHOT_TOTAL_BYTES", 16)
    (checkout / "target-ratchet/output").write_bytes(b"x" * 12)
    before = orch._gate_checkout_snapshot(checkout, runner=_runner)
    (checkout / "000-gate-output").write_bytes(b"x" * 16)
    after = orch._gate_checkout_snapshot(checkout, runner=_runner, protected=before.fingerprints)
    assert after.fingerprints == before.fingerprints
    assert after.untracked - before.untracked == {"000-gate-output"}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["unreadable", "oversized", "protected_unreadable", "protected_bounded"])
async def test_finalize_reaches_gate_with_worker_output(checkout, tmp_path, monkeypatch, case):
    target = checkout / "target-ratchet/private.lock"
    target.write_bytes(b"private")
    if "unreadable" in case:
        _deny_open(monkeypatch, target)
    else:
        monkeypatch.setattr(orch, "_GATE_SNAPSHOT_FILE_BYTES", 4)
    monkeypatch.setattr(orch, "_with_outside_checkout_detection", lambda result, **kw: result)
    monkeypatch.setattr(orch, "_git_push", Mock())
    monkeypatch.setattr(orch, "_open_pr", lambda *a, **kw: "https://example.test/pr/2223")
    monkeypatch.setattr(orch, "_comment_evidence", lambda *a, **kw: None)
    monkeypatch.setattr(orch, "_cleanup_checkout_after_transition", lambda *a, **kw: None)
    gate = "printf ran > gate-ran"
    if case.startswith("protected"):
        gate += "; printf changed > target-ratchet/private.lock"
    home = tmp_path / "home"
    home.mkdir()
    started = datetime.now(UTC)
    backend = FeatureFactoryBackend()

    async def interpret(*args):
        return RawResult(0, None, "success", None)

    class Release:
        label_transition_applied = False
        retain_for_recovery = False

        def __call__(self):
            return True

    result = await orch.WorklinkRunner(home=home, repo=checkout)._finalize(
        issue=orch.IssueContext(2223, "title", "body", set()),
        claims=SimpleNamespace(transition_issue=Mock()),
        claim_record=ClaimRecord(2223, 1, "agent", started),
        attempt=1, config=WorklinkConfig(),
        backend=SimpleNamespace(name="feature_factory", interpret=interpret, capabilities=backend.capabilities),
        compute=None, compute_result=ComputeResult(0, "", ""), order=None,
        lease=CheckoutLease(2223, 1, checkout, checkout, "main", "main"),
        spec=WorkSpec(
            issue_id=2223, attempt=1, repo_url="repo", base_ref="main", branch="main",
            prompt="", rules=None, test_command=gate, backend="feature_factory", timeout_s=30,
            local_checkout=checkout, execution=backend.capabilities().execution,
        ),
        started=started, test_cmd=gate, root_dirty_before=(), runner=_runner,
        terminal_release=Release(),
    )
    assert (checkout / "gate-ran").read_text() == "ran"
    if case.startswith("protected"):
        assert result.status == "blocked"
        assert "gate_changed_builder_content: target-ratchet/private.lock" in result.reason
        orch._git_push.assert_not_called()
    else:
        assert result.status == "completed", result.reason
