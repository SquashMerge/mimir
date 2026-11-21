"""Bounded fake-CLI properties: persisted mutations with lost responses.

Reservations are not workers: failed cleanup may retain a lock, and only a
positive claim result is allowed to admit a worker in this caller model.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

pytest.importorskip("mimir_cultivate", reason="Worklink belongs to Cultivate")
from mimir_cultivate.worklink import claims as sut

CAMPAIGN = settings(
    max_examples=240, deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
BASE = datetime(2026, 11, 1, tzinfo=UTC)
ISSUES = (11, 29, 47, 83)
AGENTS = ("process-a", "process-b", "process-c")
COMMENT_FAULTS = ("after-rc", "after-exc")
RELEASE_FAULTS = ("ok", "before-rc", "after-rc", "before-exc", "after-exc")


@dataclass(frozen=True)
class Op:
    kind: str
    issue: int = 11
    agent: str = "process-a"
    seconds: int = 0
    comment_fault: str = "ok"
    release_fault: str = "ok"


OPS = st.builds(
    Op,
    kind=st.sampled_from(("claim", "claim", "finish", "reconcile", "tick")),
    issue=st.sampled_from(ISSUES),
    agent=st.sampled_from(AGENTS),
    seconds=st.integers(0, 5),
    comment_fault=st.sampled_from(("ok", "ok", "before-rc", *COMMENT_FAULTS)),
    release_fault=st.sampled_from(RELEASE_FAULTS),
)


@dataclass
class Ledger:
    """Independent semantic state, never populated from a production result."""

    reservations: dict[int, str] = field(default_factory=dict)
    attempts: dict[int, list[tuple[str, datetime]]] = field(
        default_factory=lambda: {issue: [] for issue in ISSUES}
    )
    workers: dict[int, str] = field(default_factory=dict)
    fences: set[int] = field(default_factory=set)


class FakeCLI:
    """Storage and transport only: deliberately contains no admission policy."""

    def __init__(self):
        self.locks: dict[int, str] = {}
        self.comments = {issue: [] for issue in ISSUES}
        self.labels = {issue: {"worklink:ready"} for issue in ISSUES}
        self.calls: list[tuple[str, ...]] = []
        self.comment_fault = "ok"
        self.release_fault = "ok"

    def __call__(self, argv):
        args = tuple(argv[1:])
        self.calls.append(args)
        command = args[:2]
        issue = int(args[2]) if len(args) > 2 and args[2].isdigit() else None
        out, rc, error = "", 0, ""
        if command == ("locks", "list"):
            out = json.dumps({"locks": {
                str(i): {"issue_id": i, "agent_id": owner}
                for i, owner in sorted(self.locks.items())
            }})
        elif command == ("issue", "show"):
            out = json.dumps({
                "labels": sorted(self.labels[issue]), "comments": self.comments[issue],
            })
        elif command == ("locks", "claim"):
            owner = self.locks.get(issue)
            if owner == "mimir-worklink":
                out = "You already hold this lock"
            elif owner is not None:
                rc, error = 1, "held by another agent"
            else:
                self.locks[issue] = "mimir-worklink"
        elif command == ("locks", "steal"):
            self.locks[issue] = "mimir-worklink"
        elif command in {("locks", "release"), ("issue", "comment")}:
            fault = self.release_fault if command[0] == "locks" else self.comment_fault
            if command[0] == "locks":
                self.release_fault = "ok"
            else:
                self.comment_fault = "ok"
            if not fault.startswith("before"):
                if command[0] == "locks":
                    self.locks.pop(issue, None)
                else:
                    self.comments[issue].append(args[3])
            if fault.endswith("exc"):
                raise OSError(f"injected {command[1]} response lost")
            if fault.endswith("rc"):
                rc, error = 1, f"injected {command[1]} response lost"
        elif command == ("issue", "label"):
            self.labels[issue].add(args[3])
        elif command == ("issue", "unlabel"):
            self.labels[issue].discard(args[3])
        else:
            raise AssertionError(f"unexpected fake CLI request: {args!r}")
        return subprocess.CompletedProcess(argv, rc, out, error)


class History:
    def __init__(self, home: Path, cap: int, *, record_events: bool = False):
        self.home, self.cap = home.resolve(), cap
        self.record_events = record_events
        self.now = BASE
        self.fake = FakeCLI()
        self.model = Ledger()
        self.trace: list[Op] = []
        self.sleeps: list[float] = []
        self.clients = {
            agent: sut.ChainlinkClaims(
                agent_id=agent, runner=self.fake, home_path=self.home,
                clock=lambda: self.now, max_attempts=1000,
                sleeper=self.sleeps.append,
            ) for agent in AGENTS
        }

    def _fence(self, issue: int, present: bool) -> None:
        path = self.home / f"state/worklink/runs/{issue}.json"
        if present:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Presence, including malformed data, is the documented fence.
            path.write_text("{", encoding="utf-8")
        else:
            path.unlink(missing_ok=True)

    def check(self) -> None:
        context = f"trace={self.trace!r}"
        assert self.fake.locks == self.model.reservations, f"reservation ownership mismatch; {context}"
        for issue in ISSUES:
            payloads = [
                json.loads(text.removeprefix("WORKLINK_CLAIM "))
                for text in self.fake.comments[issue]
            ]
            expected = [
                (issue, n, agent, at.isoformat(), n)
                for n, (agent, at) in enumerate(self.model.attempts[issue], 1)
            ]
            actual = [
                (p["issue_id"], p["attempt"], p["agent_id"], p["claimed_at"], p["budget_attempt"])
                for p in payloads
            ]
            assert actual == expected, f"durable attempt ledger mismatch; {context}"
        assert len(self.model.workers) <= self.cap, f"worker hard cap exceeded; {context}"
        assert set(self.model.workers) <= self.fake.locks.keys(), f"live worker lost reservation; {context}"
        assert not self.sleeps, "non-contention failures must not spend a retry budget"

    def step(self, op: Op) -> str:
        self.trace.append(op)
        model, fake = self.model, self.fake
        client = self.clients[op.agent]
        before_labels = {issue: set(labels) for issue, labels in fake.labels.items()}
        fake.comment_fault, fake.release_fault = op.comment_fault, op.release_fault
        if op.kind == "tick":
            self.now += timedelta(seconds=op.seconds)
            outcome = "tick"
        elif op.kind == "finish":
            model.workers.pop(op.issue, None)
            model.fences.discard(op.issue)
            self._fence(op.issue, False)
            if not op.release_fault.startswith("before"):
                model.reservations.pop(op.issue, None)
            if op.release_fault.endswith("exc"):
                with pytest.raises(OSError, match="injected release response lost"):
                    client.release_issue(op.issue)
            else:
                assert client.release_issue(op.issue) == (op.release_fault == "ok"), (
                    "release acknowledgement mismatch"
                )
            outcome = "finish"
        elif op.kind == "reconcile":
            # Only this operation's explicit fault is applied to the first
            # eligible release. Protected/foreign locks do not consume it.
            eligible = sorted(
                i for i, owner in model.reservations.items()
                if owner == "mimir-worklink" and i not in model.fences
            )
            acknowledged = []
            for n, issue in enumerate(eligible):
                fault = op.release_fault if n == 0 else "ok"
                if not fault.startswith("before"):
                    model.reservations.pop(issue)
                if fault == "ok":
                    acknowledged.append(issue)
            assert client.reconcile_orphan_locks() == acknowledged, "reconciliation acknowledgement mismatch"
            outcome = "reconcile"
        else:
            owner = model.reservations.get(op.issue)
            previous = model.attempts[op.issue]
            # The tracker claimant is shared across independent process IDs.
            live_comment = previous and (self.now - previous[-1][1]).total_seconds() < 600
            if owner not in (None, "mimir-worklink"):
                expected = "foreign"
            elif owner is not None and live_comment:
                expected = "duplicate"
            elif len(set(model.reservations) | {op.issue}) > self.cap:
                expected = "cap"
            else:
                expected = "error" if op.comment_fault != "ok" else "admitted"
            if expected in {"cap", "error", "admitted"}:
                model.reservations[op.issue] = "mimir-worklink"
                if expected != "cap" and not op.comment_fault.startswith("before"):
                    previous.append((op.agent, self.now))
                if expected in {"cap", "error"} and not op.release_fault.startswith("before"):
                    model.reservations.pop(op.issue)
            before_comments = sum(map(len, fake.comments.values()))
            if expected == "error" or (
                expected == "cap" and op.release_fault.endswith("exc")
            ):
                error = OSError if (
                    op.release_fault.endswith("exc") or op.comment_fault.endswith("exc")
                ) else RuntimeError
                with pytest.raises(error, match="injected (comment|release) response lost"):
                    client.claim_issue(op.issue, list(fake.comments[op.issue]), max_active_locks=self.cap)
            else:
                result = client.claim_issue(
                    op.issue, list(fake.comments[op.issue]), max_active_locks=self.cap,
                )
                assert result.claimed == (expected == "admitted"), (
                    f"admission acknowledgement mismatch; trace={self.trace!r}"
                )
                if expected == "admitted":
                    assert op.issue not in model.workers, "duplicate live worker admission"
                    model.workers[op.issue] = op.agent
                    model.fences.add(op.issue)
                    self._fence(op.issue, True)
                    assert result.record.attempt == len(previous)
                elif expected == "duplicate":
                    assert result.reason == "duplicate_run_live"
                elif expected == "cap":
                    assert result.reason.startswith("concurrency cap reached")
                else:
                    assert result.reason == "held by another agent"
            if expected in {"duplicate", "cap", "foreign"}:
                assert sum(map(len, fake.comments.values())) == before_comments
                assert fake.labels == before_labels, "denied admission mutated labels"
            outcome = expected
        if op.kind in {"tick", "finish", "reconcile"}:
            assert fake.labels == before_labels, "cleanup mutated labels"
        self.check()
        if self.record_events:
            event(f"operation={op.kind}:{outcome}")
            event(f"reservations-over-cap={len(model.reservations) > self.cap}")
            if op.kind in {"finish", "reconcile"}:
                event(f"cleanup-release={op.release_fault}")
        return outcome

    def drain(self) -> None:
        for issue in list(self.model.workers):
            self.step(Op("finish", issue))
        self.step(Op("reconcile"))


@CAMPAIGN
@given(
    cap=st.integers(1, 3),
    prefix=st.lists(OPS, max_size=12),
    suffix=st.lists(OPS, max_size=18),
    issue=st.sampled_from(ISSUES),
    agent=st.sampled_from(AGENTS),
    comment_fault=st.sampled_from(COMMENT_FAULTS),
    release_fault=st.sampled_from(RELEASE_FAULTS),
    elapsed=st.integers(0, 30),
)
def test_persisted_comment_lost_response_embedded_in_histories(
    tmp_path, cap, prefix, suffix, issue, agent, comment_fault, release_fault, elapsed,
):
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        history = History(Path(directory), cap, record_events=True)
        for op in prefix:
            history.step(op)
        history.drain()  # Constructively reach the fault, regardless of prefix.
        assert history.step(Op("claim", issue, agent, comment_fault=comment_fault,
                               release_fault=release_fault)) == "error"
        event(f"critical-comment={comment_fault}")
        event(f"critical-release={release_fault}")
        event(f"critical-fault-pair={comment_fault}/{release_fault}")
        history.step(Op("tick", seconds=elapsed))
        history.step(Op("claim", issue, AGENTS[(AGENTS.index(agent) + 1) % 3]))
        other = ISSUES[(ISSUES.index(issue) + 1) % len(ISSUES)]
        history.step(Op("claim", other))
        history.step(Op("reconcile"))
        history.step(Op("claim", issue, agent))
        for op in suffix:
            history.step(op)
        history.drain()
        assert not history.fake.locks


@CAMPAIGN
@given(
    issue=st.sampled_from(ISSUES), agent=st.sampled_from(AGENTS),
    comment_fault=st.sampled_from(COMMENT_FAULTS),
    release_fault=st.sampled_from(RELEASE_FAULTS[1:]),
    elapsed=st.sampled_from((0, 599, 600, 601, 900)),
    fence=st.booleans(), foreign=st.booleans(),
)
def test_uncertain_reservation_retry_age_and_reconciliation_ownership(
    tmp_path, issue, agent, comment_fault, release_fault, elapsed, fence, foreign,
):
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        h = History(Path(directory), 1, record_events=True)
        h.step(Op("claim", issue, agent, comment_fault=comment_fault, release_fault=release_fault))
        event(f"critical-comment={comment_fault}")
        event(f"critical-release={release_fault}")
        h.step(Op("tick", seconds=elapsed))
        event(f"age={elapsed}")
        event(f"retained-fence={fence}")
        event(f"foreign-owner={foreign}")
        if fence:
            h.model.fences.add(issue)
            h._fence(issue, True)
        if foreign and issue in h.model.reservations:
            h.model.reservations[issue] = h.fake.locks[issue] = "human"
        # Retry before reconciliation observes durable comments and ownership.
        h.step(Op("claim", issue, AGENTS[(AGENTS.index(agent) + 1) % 3]))
        h.step(Op("reconcile"))
        if issue in h.model.workers:
            h.step(Op("finish", issue))
        elif fence:
            h.model.fences.discard(issue)
            h._fence(issue, False)
        h.step(Op("reconcile"))


def _calibration(home: Path, mode: str) -> None:
    h = History(home, 1)
    if mode == "lost-comment":
        h.step(Op("claim", comment_fault="after-rc"))
    else:
        h.step(Op("claim"))
        h.step(Op("finish", release_fault="before-rc"))
        h.step(Op("reconcile"))


@pytest.mark.parametrize("mode", ("lost-comment", "failed-release", "orphan-release"))
def test_uncertainty_oracle_detects_owned_mutants(tmp_path, monkeypatch, mode):
    _calibration(tmp_path / "baseline", mode)
    if mode == "lost-comment":
        original = sut.ChainlinkClaims._run

        def ignore_lost_comment(self, *args, **kwargs):
            try:
                return original(self, *args, **kwargs)
            except RuntimeError as exc:
                if args[:2] != ("issue", "comment") or str(exc) != "injected comment response lost":
                    raise
                return subprocess.CompletedProcess(args, 0, "", "")

        monkeypatch.setattr(sut.ChainlinkClaims, "_run", ignore_lost_comment)
        with pytest.raises(pytest.fail.Exception, match="^DID NOT RAISE RuntimeError$"):
            _calibration(tmp_path / "mutant", mode)
    elif mode == "failed-release":
        original = sut.ChainlinkClaims.release_issue

        def acknowledge_failed_release(self, issue):
            original(self, issue)
            return True

        monkeypatch.setattr(sut.ChainlinkClaims, "release_issue", acknowledge_failed_release)
        with pytest.raises(AssertionError, match="release acknowledgement mismatch"):
            _calibration(tmp_path / "mutant", mode)
    else:
        monkeypatch.setattr(sut.ChainlinkClaims, "reconcile_orphan_locks", lambda self: [])
        with pytest.raises(AssertionError, match="reconciliation acknowledgement mismatch"):
            _calibration(tmp_path / "mutant", mode)


def _reconciliation_race(
    home: Path, issue: int, agent: str, comment_fault: str,
    release_fault: str, change: str, *, record_events: bool = False,
) -> None:
    """Change owned storage after discovery's snapshot, before verify's read."""
    h = History(home, 1, record_events=record_events)
    assert h.step(Op("claim", issue, agent, comment_fault=comment_fault,
                     release_fault=release_fault)) == "error"
    assert h.fake.locks == {issue: "mimir-worklink"}
    assert len(h.fake.comments[issue]) == 1
    before_labels = {i: set(labels) for i, labels in h.fake.labels.items()}
    before_comments = {i: list(comments) for i, comments in h.fake.comments.items()}
    before_calls = len(h.fake.calls)
    original = h.fake
    trace = ["persist-comment", f"lose-response:{comment_fault}",
             f"rollback:{release_fault}", "discover"]
    reads = 0
    hooks = 0

    def runner(argv):
        nonlocal reads, hooks
        # Capture stdout first: discovery must still observe the *old* owner,
        # making it a candidate rather than filtering it out before verify.
        result = original(argv)
        if tuple(argv[1:3]) == ("locks", "list"):
            reads += 1
            if reads == 1:
                observed = json.loads(result.stdout)["locks"][str(issue)]
                assert observed["agent_id"] == "mimir-worklink"
                hooks += 1
                trace.append(f"after-discovery-snapshot:{change}")
                if change == "foreign-owner":
                    h.model.reservations[issue] = h.fake.locks[issue] = "human"
                else:
                    h.model.fences.add(issue)
                    h._fence(issue, True)
            else:
                trace.append("verify-current-state")
        return result

    h.clients[agent].runner = runner
    released = h.clients[agent].reconcile_orphan_locks()
    later_calls = h.fake.calls[before_calls:]
    assert not any(args[:2] == ("locks", "release") for args in later_calls), (
        f"protected-reservation release command issued; trace={trace!r}"
    )
    assert released == [], f"protected-reservation cleanup acknowledged; trace={trace!r}"
    assert hooks == 1 and reads == 2, f"revalidation hook not reached exactly once; trace={trace!r}"
    assert h.fake.labels == before_labels, f"protected-reservation labels changed; trace={trace!r}"
    assert h.fake.comments == before_comments, f"protected-reservation comments changed; trace={trace!r}"
    expected_owner = "human" if change == "foreign-owner" else "mimir-worklink"
    assert h.fake.locks == {issue: expected_owner}, (
        f"protected-reservation ownership changed; trace={trace!r}"
    )
    h.check()
    if record_events:
        event(f"revalidation-change={change}")
        event("revalidation-hook=after-discovery-before-verify")
        event(f"critical-comment={comment_fault}")
        event(f"retaining-release={release_fault}")


@settings(
    max_examples=100, deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    issue=st.sampled_from(ISSUES), agent=st.sampled_from(AGENTS),
    comment_fault=st.sampled_from(COMMENT_FAULTS),
    release_fault=st.sampled_from(("before-rc", "before-exc")),
    change=st.sampled_from(("foreign-owner", "malformed-run-fence")),
)
def test_uncertain_reservation_revalidates_changes_after_discovery(
    tmp_path, issue, agent, comment_fault, release_fault, change,
):
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        _reconciliation_race(
            Path(directory), issue, agent, comment_fault, release_fault, change,
            record_events=True,
        )


@pytest.mark.parametrize("change", ("foreign-owner", "malformed-run-fence"))
def test_revalidation_oracle_detects_owned_bypass_mutant(tmp_path, monkeypatch, change):
    args = (11, "process-b", "after-exc", "before-rc", change)
    _reconciliation_race(tmp_path / "baseline", *args)
    original = sut.ChainlinkClaims._run_with_retry

    def bypass_verify(self, *args, **kwargs):
        if args[:2] == ("locks", "release") and kwargs.get("before_attempt") is not None:
            kwargs["before_attempt"] = None
        return original(self, *args, **kwargs)

    monkeypatch.setattr(sut.ChainlinkClaims, "_run_with_retry", bypass_verify)
    with pytest.raises(AssertionError, match="protected-reservation release command issued"):
        _reconciliation_race(tmp_path / "mutant", *args)
