"""Bounded, offline Worklink state-history and resource-coordination models."""

from __future__ import annotations

import fcntl
import json
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import HealthCheck, event, example, given, settings
from hypothesis import strategies as st

pytest.importorskip("mimir_cultivate", reason="Worklink belongs to the optional Cultivate plugin")
from mimir_cultivate.worklink import claims as sut

CAMPAIGN = settings(
    max_examples=300, deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
BASE = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class Entry:
    kind: str
    identity: int = 0
    heartbeat: int = 0


ENTRIES = st.builds(
    Entry,
    st.sampled_from(["claim", "claim", "abort", "stop", "quota", "reset", "noise"]),
    st.integers(0, 5),
    st.integers(0, 10),
)


def _payload(identity: int) -> dict:
    # Tiny identity domain deliberately produces duplicate comments/heartbeats.
    return {
        "issue_id": 7, "attempt": identity + 1, "agent_id": f"agent-{identity % 2}",
        "claimed_at": (BASE + timedelta(seconds=identity)).isoformat(),
    }


def _wire(entry: Entry) -> str:
    if entry.kind == "reset":
        return "WORKLINK_CLAIM_RESET payload-is-documentation"
    if entry.kind == "noise":
        return "irrelevant\nWORKLINK_CLAIM {broken json"
    payload = _payload(entry.identity)
    if entry.kind == "claim":
        payload["heartbeat_at"] = (BASE + timedelta(seconds=20 + entry.heartbeat)).isoformat()
        prefix = "WORKLINK_CLAIM "
    else:
        payload["aborted_at"] = (BASE + timedelta(seconds=40)).isoformat()
        prefix = {
            "abort": "WORKLINK_SHUTDOWN_ABORT ",
            "stop": "WORKLINK_STOPPED ",
            "quota": "WORKLINK_QUOTA_HOLD ",
        }[entry.kind]
    return prefix + json.dumps(payload)


def _history_oracle(trace: list[Entry]) -> tuple[int, int, list[tuple[int, int, int]]]:
    """Partition epochs, then derive credits by preceding event positions.

    No production parser, record identity helper, or accounting helper is used.
    Heartbeats are repeated observations, not newly charged attempts.
    """
    resets = [i for i, e in enumerate(trace) if e.kind == "reset"][:2]
    claims = [
        (i, e.identity, sum(r < i for r in resets), e.heartbeat)
        for i, e in enumerate(trace) if e.kind == "claim"
    ]
    valid_credits = [
        e for i, e in enumerate(trace)
        if e.kind in {"abort", "stop", "quota"}
        and any(pos < i and identity == e.identity for pos, identity, _, _ in claims)
    ]
    shutdown_order = list(dict.fromkeys(e.identity for e in valid_credits if e.kind == "abort"))
    credits = set(shutdown_order[:2]) | {
        e.identity for e in valid_credits if e.kind in {"stop", "quota"}
    }
    active = {identity for _, identity, gen, _ in claims if gen == len(resets)}
    return len(active - credits), len(resets), [
        (identity + 1, gen, hb) for _, identity, gen, hb in claims
    ]


def _check_history(trace: list[Entry], bundled: bool = False) -> None:
    lines = [_wire(e) for e in trace]
    comments = ["\n".join(lines)] if bundled else lines
    charged, generation, observations = _history_oracle(trace)
    client = sut.ChainlinkClaims(agent_id="next")
    budget = client.attempt_budget(iter(comments))
    assert (budget.charged, budget.clean_parks, budget.generation) == (charged, 0, generation)
    assert client.next_attempt(iter(comments)) == max((n for n, _, _ in observations), default=0) + 1
    parsed = sut.claim_records_from_comments(iter(comments))
    assert [(r.attempt, r.generation, int((r.heartbeat_at - BASE).total_seconds()) - 20)
            for r in parsed] == observations
    # Ordering is independently expressed as a lexicographic maximum.
    if parsed:
        expected = max(observations, key=lambda row: (row[1], row[0], row[2]))
        latest = parsed[0]
        for record in parsed[1:]:
            if sut._claim_is_newer(record, latest):
                latest = record
        assert (latest.attempt, latest.generation,
                int((latest.heartbeat_at - BASE).total_seconds()) - 20) == expected


@CAMPAIGN
@given(st.lists(ENTRIES, max_size=65), st.booleans())
@example([Entry("abort"), Entry("claim")], False)
@example([Entry("claim", 5), Entry("reset"), Entry("claim"), Entry("claim", heartbeat=9)], True)
@example([Entry("reset")] * 3 + [Entry("claim"), Entry("abort"), Entry("abort")], False)
@example([
    Entry("claim"), Entry("abort"), Entry("reset"),
    Entry("claim", 1), Entry("abort", 1), Entry("reset"),
    Entry("claim", 2), Entry("abort", 2),
    Entry("claim", 3), Entry("stop", 3), Entry("quota", 3),
], True)
def test_claim_history_matches_independent_event_ledger(trace, bundled):
    event(f"reset-count={min(sum(e.kind == 'reset' for e in trace), 3)}")
    event(f"duplicate-observations={len([e for e in trace if e.kind == 'claim']) > len({e.identity for e in trace if e.kind == 'claim'})}")
    _check_history(trace, bundled)


def _exercise_retry(home: Path | None, schedule: list[str], limit: int, abort_prepare: bool) -> None:
    observed: list[str] = []
    sleeps: list[float] = []
    lock_path = home / "state/worklink/chainlink-claim.lock" if home else None

    def assert_mutex(held: bool) -> None:
        if lock_path is None:
            return
        # Probe only this test's owned lock, never ambient process state.
        with lock_path.open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                assert held
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)
                assert not held

    def prepare():
        assert_mutex(True)
        observed.append("prepare")
        if abort_prepare:
            raise ValueError("preparation failed")

    def validate():
        assert_mutex(True)
        observed.append("validate")

    calls = 0

    def runner(argv):
        nonlocal calls
        assert_mutex(True)
        status = schedule[min(calls, len(schedule) - 1)]
        calls += 1
        observed.append(status)
        return subprocess.CompletedProcess(
            argv, {"busy": 128, "ok": 0, "deny": 1}[status], "",
            "Another git process seems to be running" if status == "busy" else status,
        )

    def finish(result):
        assert_mutex(True)
        observed.append("finish")
        return result

    def sleep(delay):
        assert_mutex(False)
        sleeps.append(delay)

    client = sut.ChainlinkClaims(
        agent_id="test", runner=runner, sleeper=sleep,
        contention_max_attempts=limit, contention_initial_backoff_s=0.125,
    )
    expected = ["prepare"]
    outcome = None
    if not abort_prepare:
        for i in range(limit):
            status = schedule[min(i, len(schedule) - 1)]
            expected += ["validate", status]
            if status != "busy":
                expected.append("finish")
                outcome = {"ok": 0, "deny": 1}[status]
                break
    if abort_prepare:
        with pytest.raises(ValueError, match="preparation failed"):
            client._run_with_retry(
                "locks", "claim", "7", home_path=home,
                before_claim=prepare, before_attempt=validate, after_claim=finish,
            )
    elif outcome is None:
        with pytest.raises(sut._ChainlinkContentionExhausted):
            client._run_with_retry(
                "locks", "claim", "7", home_path=home,
                before_claim=prepare, before_attempt=validate, after_claim=finish,
            )
    else:
        result = client._run_with_retry(
            "locks", "claim", "7", home_path=home,
            before_claim=prepare, before_attempt=validate, after_claim=finish,
        )
        assert result.returncode == outcome, (
            f"retry return code mismatch: expected={outcome} actual={result.returncode}"
        )
    assert observed == expected
    assert sleeps == [0.125 * 2**i for i in range(max(0, calls - 1))]
    assert_mutex(False)  # Exception and exhaustion paths must release the mutex too.


@CAMPAIGN
@given(
    st.lists(st.sampled_from(["busy", "ok", "deny"]), min_size=1, max_size=10),
    st.integers(1, 7), st.booleans(), st.booleans(),
)
@example(["busy", "busy", "ok"], 3, True, False)
@example(["busy"], 3, True, False)
def test_retry_history_callbacks_backoff_and_mutex(tmp_path, schedule, limit, with_home, abort_prepare):
    event(f"preparation-fails={abort_prepare}")
    event(f"first-result={schedule[0]}")
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        _exercise_retry(Path(directory) if with_home else None, schedule, limit, abort_prepare)


def test_retry_model_kills_no_contention_retry_mutation(tmp_path, monkeypatch):
    # Plausible regression: contention gets mistaken for a permanent denial.
    _exercise_retry(tmp_path, ["busy", "ok"], 3, False)
    monkeypatch.setattr(sut, "_is_git_contention", lambda result: False)
    with pytest.raises(AssertionError, match="retry return code mismatch: expected=0 actual=128"):
        _exercise_retry(tmp_path, ["busy", "ok"], 3, False)


@pytest.mark.parametrize("shape", ["mapping", "wrapped-list", "bare-list"])
@CAMPAIGN
@given(
    st.sets(st.integers(1, 8), max_size=8),
    st.sets(st.integers(1, 8), max_size=8),
    st.sampled_from(["all", "include", "exclude"]),
)
def test_resource_scope_matches_explicit_membership(shape, active, labelled, scope):
    records = [{"issue_id": issue, "agent_id": "mimir-worklink"} for issue in sorted(active)]
    wire = (
        {"locks": {str(r["issue_id"]): r for r in records}} if shape == "mapping"
        else {"locks": records} if shape == "wrapped-list"
        else records
    )

    def runner(argv):
        assert list(argv[1:]) == ["locks", "list", "--json"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(wire), "")

    client = sut.ChainlinkClaims(agent_id="test", runner=runner)
    client._list_issue_ids = lambda label: list(labelled)
    kwargs = {"label": "epic"} if scope == "include" else {"exclude_label": "epic"} if scope == "exclude" else {}
    expected = {
        issue for issue in active
        if scope == "all" or (scope == "include" and issue in labelled)
        or (scope == "exclude" and issue not in labelled)
    }
    assert client._active_worklink_lock_ids_for_scope(**kwargs) == expected


@CAMPAIGN
@given(st.integers(0, 5))
def test_scoped_lock_list_rejects_missing_identity(count):
    # Regression for #2175 (W2): a list offset is never an issue identity.
    client = sut.ChainlinkClaims(
        agent_id="test",
        runner=lambda argv: subprocess.CompletedProcess(argv, 0, json.dumps({"locks": [{}] * (count + 1)}), ""),
    )
    with pytest.raises(RuntimeError, match="omitted issue identity"):
        client._active_worklink_lock_ids(require_identity=True)


@CAMPAIGN
@given(
    st.integers(1, 3),
    st.lists(
        st.tuples(st.sampled_from(["claim", "claim", "publish-fails", "release"]), st.integers(1, 4)),
        min_size=1, max_size=45,
    ),
)
@example(1, [("claim", 1), ("claim", 1), ("claim", 2), ("release", 1), ("claim", 2)])
@example(2, [("publish-fails", 1), ("claim", 1), ("claim", 2), ("claim", 3)])
def test_claim_reservation_history_is_bounded_and_failure_releases(tmp_path, cap, trace):
    """Fake CLI is storage/transport only; expected admission uses a separate ledger."""
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        locks: set[int] = set()
        labels = {i: {"worklink:ready"} for i in range(1, 5)}
        comments = {i: [] for i in range(1, 5)}
        mutations: list[tuple] = []
        fail_publication = False

        def runner(argv):
            command = tuple(argv[1:3])
            issue = int(argv[3]) if argv[3].isdigit() else None
            stdout = ""
            rc = 0
            stderr = ""
            if command == ("issue", "show"):
                stdout = json.dumps({"labels": sorted(labels[issue]), "comments": comments[issue]})
            elif command == ("locks", "list"):
                stdout = json.dumps({"locks": {str(i): {"issue_id": i} for i in locks}})
            elif command == ("locks", "claim"):
                if issue in locks:
                    stdout = "You already hold this lock"
                else:
                    locks.add(issue)
            elif command == ("locks", "release"):
                locks.discard(issue)
            elif command == ("issue", "comment"):
                if fail_publication:
                    rc, stderr = 1, "injected publication failure"
                else:
                    comments[issue].append(argv[4])
                    mutations.append((issue, "comment"))
            elif command in {("issue", "label"), ("issue", "unlabel")}:
                if command[1] == "label":
                    labels[issue].add(argv[4])
                else:
                    labels[issue].discard(argv[4])
                mutations.append((issue, command[1], argv[4]))
            else:
                raise AssertionError(f"unexpected fake CLI request: {argv}")
            return subprocess.CompletedProcess(argv, rc, stdout, stderr)

        client = sut.ChainlinkClaims(
            agent_id="test", runner=runner, home_path=Path(directory),
            clock=lambda: BASE, max_attempts=100,
        )
        reserved: set[int] = set()
        issued = {i: 0 for i in range(1, 5)}
        for operation, issue in trace:
            before = len(mutations)
            if operation == "release":
                assert client.release_issue(issue)
                reserved.discard(issue)
            else:
                fail_publication = operation == "publish-fails"
                if issue in reserved:
                    event("admission=duplicate")
                    result = client.claim_issue(issue, comments[issue], max_active_locks=cap)
                    assert not result.claimed and result.reason == "duplicate_run_live"
                    assert len(mutations) == before
                elif len(reserved) == cap:
                    event("admission=cap")
                    result = client.claim_issue(issue, comments[issue], max_active_locks=cap)
                    assert not result.claimed and result.reason.startswith("concurrency cap reached")
                    assert len(mutations) == before
                elif fail_publication:
                    event("admission=publication-failure")
                    with pytest.raises(RuntimeError, match="injected publication failure"):
                        client.claim_issue(issue, comments[issue], max_active_locks=cap)
                else:
                    event("admission=success")
                    result = client.claim_issue(issue, comments[issue], max_active_locks=cap)
                    assert result.claimed
                    issued[issue] += 1
                    assert result.record.attempt == issued[issue]
                    assert result.record.budget_attempt == issued[issue]
                    reserved.add(issue)
            assert locks == reserved
            assert len(locks) <= cap
            assert {i: len(history) for i, history in comments.items()} == issued
