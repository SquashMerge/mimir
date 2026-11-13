#!/usr/bin/env python3
"""Park a live factory run by hand, so its control plane survives to be resumed.

A run that exhausts its budget is killed and swept, losing an approved spec, an
approved decomposition and every slice review. Parking preserves them, and
resume grants a fresh full budget because ``_supervise_factory_070`` recomputes
its deadline from the monotonic clock on entry.

Two facts drove this script's shape; both cost a real run to learn.

**The snapshot belongs to the checkout, not the sandbox.** ``observedParkSnapshot``
in the factory CLI computes ``operatorRoot = dirname(dirname(repo))``, so the
published path is ``<checkout>/.factory/.parked/<run>``. The sandbox has its own
``.factory`` holding the live plane, and publishing there produces a byte-perfect
snapshot the factory will never acknowledge.

**mimir's retained record must be reconciled, or the parked run is pruned.** The
retention guard reads mimir's last *observed* status, not the factory's plane
(``_attempt_is_active`` in ``mimir/worklink/autonomy.py``). A park that leaves
that record saying ``running`` is deleted by the next cleanup pass even though
the factory itself reports ``needs-human``. That is why the supervisor is stopped
before the driver here: if the driver dies under a live supervisor, the
supervisor records a terminal failure first and marks the tree prunable before
the park can land.

**The controller has to be stopped first, before anything it owns.** The
controller's own budget park (``park_for_budget`` in ``orchestrator.py``) is
race-free precisely because it cancels a handle it owns and therefore never
observes an unexpected exit. A script that kills the compute processes under a
live controller gives it exactly that unexpected exit: it runs its failure path,
writes ``failed`` to the retained record, and races this script's later
terminalize and reconcile — and whichever write lands second wins. There is no
ordering of the compute processes that avoids this, so the controller is stopped
first and its death is verified before any other process is signalled.

Stopping the controller leaves its Chainlink claim behind, and that claim is
what refuses the next dispatch, so the park releases it as its last step. The
refusal is indirect enough to be worth stating: the chainlink CLI treats a
same-agent re-claim as idempotent success and says "You already hold the lock",
and ``claim_issue`` uses exactly that string to decide whether to run its
duplicate-liveness guard -- which then finds the stopped controller's own
heartbeat comment still fresh and returns ``duplicate_run_live`` for the whole
``duplicate_freshness_s`` window. A released lock is claimed outright instead,
so the guard is never reached.

Which process to stop is not guessed from ``ps``. The retained record's
``handle`` names the driver, and ``factory_process_is_verified_dead`` is the same
predicate mimir's own recovery path applies before it will resume, so this script
asserts the state resume actually requires rather than a proxy for it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import select
import shlex
import stat
import subprocess
import sys
import time
from dataclasses import replace
from contextlib import contextmanager
from typing import Callable, NamedTuple
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mimir_cultivate.worklink.autonomy import DEFAULT_AGENT_ID  # noqa: E402
from mimir_cultivate.worklink.backends.feature_factory import FactoryContractError, parse_factory_status, resolve_factory_entrypoint  # noqa: E402
from mimir_cultivate.opencode import redact_daemon_output  # noqa: E402
from mimir_cultivate.worklink.claims import (  # noqa: E402
    ChainlinkClaims,
    _read_clean_park,
    claim_records_from_comments,
    record_clean_park,
)
from mimir_cultivate.worklink.factory_state import (  # noqa: E402
    FactoryRecordError,
    FactoryRunRecord,
    _require_safe_directory,
    factory_checkout_interlock,
    factory_issue_resource_lock,
    factory_process_is_alive,
    factory_process_is_verified_dead,
    load_factory_record,
    save_factory_record,
)

TERMINAL_PARKED = "needs-human"


class ParkError(RuntimeError):
    """A controlled park refusal or incomplete reconciliation."""


class ClaimAlreadyReconciled(ParkError):
    """Only these explicit no-work outcomes may suppress reconciliation."""


def _run(cmd: list[str], *, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    if cmd and cmd[0] == "node":
        from mimir_cultivate.worklink.backends.feature_factory import _run_bounded

        result = _run_bounded(cmd, cwd=None, env=os.environ, timeout=timeout,
                              output_limit=1024 * 1024)
        return subprocess.CompletedProcess(cmd, result.returncode,
            result.stdout.decode("utf-8", "replace"), result.stderr.decode("utf-8", "replace"))
    return subprocess.run(
        cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=timeout, check=False,
    )


def operator_root(sandbox: Path) -> Path:
    """Return the directory the factory publishes parked snapshots under.

    Mirrors ``observedParkSnapshot``: the sandbox must be
    ``<operatorRoot>/.factory-sandboxes/<runId>``, and the snapshot lives at
    ``<operatorRoot>/.factory/.parked/<runId>``. Refusing a sandbox of another
    shape is deliberate — a wrong root publishes a snapshot nothing reads.
    """
    sandbox = sandbox.resolve()
    container = sandbox.parent
    if container.name != ".factory-sandboxes":
        raise ParkError(
            f"sandbox is not under .factory-sandboxes: {sandbox}; refusing to guess a snapshot root"
        )
    return container.parent


def factory_status(launcher: Path, run_id: str, sandbox: Path) -> dict:
    launcher = resolve_factory_entrypoint(launcher)
    result = _run(["node", str(launcher), "status", run_id, "--repo", str(sandbox), "--json"])
    if result.returncode != 0:
        raise ParkError(f"factory status failed: {redact_daemon_output((result.stderr or result.stdout).strip())[:300]}")
    try:
        status = parse_factory_status(result.stdout)
        if not status.valid or status.run_id != run_id or status.sandbox_path != str(sandbox):
            raise ParkError("factory status identity could not be qualified")
        return status.to_json()
    except (FactoryContractError, ValueError) as exc:
        raise ParkError("factory status response could not be qualified") from exc


def plane_inventory(root: Path) -> list[tuple[str, str, int, str]]:
    """Relative path, type, mode, and content digest or link target, sorted.

    The plane-root ``factory.lock`` is excluded by the caller, not here: it is
    session liveness on a timer, so a heartbeat landing between the two reads
    would fail an otherwise byte-correct comparison. Any ``factory.lock`` below
    the plane root is run state and must match.
    """
    items: list[tuple[str, str, int, str]] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        filenames.sort()
        for name in dirnames + filenames:
            path = Path(dirpath) / name
            rel = path.relative_to(root).as_posix()
            st = path.lstat()
            mode = stat.S_IMODE(st.st_mode)
            if stat.S_ISLNK(st.st_mode):
                items.append((rel, "link", mode, os.readlink(path)))
            elif stat.S_ISDIR(st.st_mode):
                items.append((rel, "dir", mode, ""))
            elif stat.S_ISREG(st.st_mode):
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1 << 20), b""):
                        digest.update(chunk)
                items.append((rel, "file", mode, digest.hexdigest()))
            else:
                items.append((rel, "other", mode, ""))
    return sorted(items)


def publish_snapshot(plane: Path, parked_dir: Path, run_id: str, *, launcher: Path) -> Path:
    """Factory alone publishes; independently verify its acknowledged inventory."""
    launcher = resolve_factory_entrypoint(launcher)
    canonical = parked_dir / run_id
    sandbox = plane.parent.parent
    root = operator_root(sandbox)
    if plane != sandbox / ".factory" / run_id or parked_dir != root / ".factory" / ".parked":
        raise ParkError("snapshot paths do not match the run's operator repository")
    for parent in (parked_dir.parent, parked_dir):
        if parent.is_symlink():
            raise ParkError("snapshot has a symlinked parent; refusing publication")
        if parent.exists() and not parent.is_dir():
            raise ParkError("snapshot parent is not a directory")
    if canonical.is_symlink():
        raise ParkError("canonical snapshot is a symlink; refusing publication")
    source = [item for item in plane_inventory(plane) if item[0] != "factory.lock"]
    result = _run(["node", str(launcher), "snapshot", run_id, "--repo", str(root), "--json"])
    if result.returncode:
        raise ParkError("factory snapshot publication failed; retained state was not acknowledged")
    status = factory_status(launcher, run_id, sandbox)
    if status.get("park_snapshot") != str(canonical) or canonical.is_symlink() or not canonical.is_dir():
        raise ParkError("factory snapshot publication was not acknowledged at the canonical path")
    copied = [item for item in plane_inventory(canonical) if item[0] != "factory.lock"]
    if source != copied:
        raise ParkError("factory snapshot inventory mismatch; claim retained")
    return canonical


def process_argv(pid: int) -> str | None:
    """Return a live process's argv, or None if it is gone.

    ``ps`` rather than ``/proc`` so the same check works on the macOS host where
    these scripts are tested and in the Linux container where they are run.
    """
    result = _run(["ps", "-o", "args=", "-p", str(pid)])
    if result.returncode != 0:
        return None
    argv = result.stdout.strip()
    return argv or None


def verify_controller(pid: int, issue_id: int) -> str:
    """Confirm a pid really is the controller for this issue before signalling it.

    The tokens are derived from the retained record, not supplied by the
    operator, so a mistyped pid is refused rather than acted on.
    """
    argv = process_argv(pid)
    if argv is None:
        raise ParkError(
            f"--controller-pid {pid} is not running. If the controller is already gone, "
            "pass --controller-pid none, which verifies that rather than assuming it."
        )
    required = ["worklink", "run-epic", str(issue_id)]
    try:
        tokens = shlex.split(argv)
    except ValueError:
        tokens = []
    if not any(tokens[index:index + 3] == required for index in range(len(tokens))):
        raise ParkError(
            f"--controller-pid {pid} does not look like this run's controller "
            f"(argv lacks the exact issue command): {redact_daemon_output(argv)[:200]}"
        )
    return argv


def find_controllers(issue_id: int) -> list[int]:
    """Report controller candidates so a refusal can name them.

    This never selects one. Picking a process to kill by pattern match is how an
    operator kills the wrong run; the operator names the pid and this verifies it.
    """
    listing = _run(["ps", "-eo", "pid=,args="])
    self_pid = os.getpid()
    found: list[int] = []
    for line in listing.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        head, _, argv = line.partition(" ")
        try:
            pid = int(head)
        except ValueError:
            continue
        if pid == self_pid:
            continue
        # This script's own argv carries every token it searches for.
        if "factory_park.py" in argv:
            continue
        if all(token in argv for token in ("worklink", "run-epic", str(issue_id))):
            found.append(pid)
    return found


def stop_pid(pid: int, label: str, *, timeout: float = 20.0,
             verify: Callable[[int], object] | None = None) -> None:
    """Bind the process before revalidation, so PID reuse cannot signal a daemon."""
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal") or verify is None:
        raise ParkError("verified controller signalling requires Linux pidfds and identity revalidation")
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        verify(pid)
        watcher = select.poll()
        watcher.register(fd, select.POLLIN)
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                signal.pidfd_send_signal(fd, sig)
            except ProcessLookupError:
                return
            except PermissionError as exc:
                raise ParkError(f"not permitted to signal the verified {label}") from exc
            if watcher.poll(max(1, int(timeout * 1000))):
                return
        raise ParkError(f"{label} (pid {pid}) survived SIGKILL; refusing to continue the park")
    finally:
        os.close(fd)


def stop_residual_compute(run_id: str, *, timeout: float = 20.0) -> list[int]:
    """No argv sweep can prove session ownership; only recorded handles are stopped."""
    return []


@contextmanager
def _park_locks(home: Path, issue_id: int):
    with factory_checkout_interlock(home) as acquired:
        if not acquired:
            raise ParkError("could not acquire the factory checkout interlock; state and claim retained")
        with factory_issue_resource_lock(home, issue_id) as issue_acquired:
            if not issue_acquired:
                raise ParkError("could not acquire the factory issue resource lock; state and claim retained")
            yield


def _require_daemon_owner(record: FactoryRunRecord, status: dict) -> None:
    if record.handle is not None and record.handle.substrate == "opencode_daemon":
        actual = record.handle.opencode_session_id
        if (actual is None or record.opencode_session_id != actual or record.session != actual
                or status.get("lock_session") not in {None, actual}):
            raise ParkError("factory lock does not name the retained OpenCode host; state and claim retained")


def _quiesce_daemon_host(record: FactoryRunRecord, home: Path) -> FactoryRunRecord:
    import asyncio
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
    from mimir_cultivate.worklink.worker_client import WorkerClient
    from mimir_cultivate.worklink.identities import get_identities
    from mimir_cultivate.worklink.control import _mark_operator_stop

    assert record.handle is not None and record.handle.opencode_session_id is not None
    _mark_operator_stop(home, record.issue_id, record.attempt, opencode_session_id=record.handle.opencode_session_id)
    backend = OpenCodeDaemonComputeBackend(client=WorkerClient.for_path_checkout(
        Path(record.sandbox), issue_id=record.issue_id, attempt=record.attempt, run_uid=get_identities().worklink_uid))
    async def cancel_host() -> None:
        await backend.cancel(record.handle)
        await backend.cleanup(record.handle)
    try:
        asyncio.run(cancel_host())
    except (OSError, RuntimeError, ValueError) as exc:
        from mimir_cultivate.worklink.orchestrator import _write_factory_host_evidence
        _write_factory_host_evidence(home, record, "failed", "operator park: session quiescence unverified")
        raise ParkError("session quiescence unverified; checkout, record and claim retained") from exc
    current = load_factory_record(home, record.run_id)
    if current is None or current.handle != record.handle:
        raise ParkError("retained host changed during cancellation; claim retained")
    record = replace(current, children_reaped=True, observed_at=datetime.now(UTC).isoformat())
    save_factory_record(home, record)
    return record


def claims_client(chainlink_bin: str, agent_id: str, home: Path) -> ChainlinkClaims:
    """A claims client that shells through this script's own ``_run``."""
    return ChainlinkClaims(
        chainlink_bin=chainlink_bin,
        agent_id=agent_id,
        home_path=home,
        runner=lambda argv: _run(list(argv)),
    )


def issue_comments(claims: ChainlinkClaims, issue_id: int) -> list[str]:
    """Read an issue's comments through the reader the claim guard itself uses.

    Deliberately not a local parser. ``chainlink issue show --json`` may return
    comments as strings *or* as objects carrying the text under ``content``,
    ``text`` or ``body``, and a parser that only handles the string form finds no
    claim records on a real issue -- which here would mean refusing a valid park
    because its claim "does not exist". Reusing ``_issue_comments`` means this
    cannot disagree with the code whose decisions it is trying to anticipate;
    ``strict=True`` turns a shape surprise into a loud failure.
    """
    try:
        return claims._issue_comments(issue_id, strict=True)
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        raise ParkError(f"could not read the comments on issue {issue_id}: {exc}") from exc


def latest_claim(comments: list[str], issue_id: int):
    """The claim record that currently owns the issue, or None."""
    latest = None
    for record in claim_records_from_comments(comments):
        if record.issue_id != issue_id:
            continue
        if latest is None or (
            (record.generation, record.attempt, record.heartbeat_at or record.claimed_at)
            > (latest.generation, latest.attempt, latest.heartbeat_at or latest.claimed_at)
        ):
            latest = record
    return latest


class ClaimPlan(NamedTuple):
    """What releasing this run's claim would do, computed without writing."""

    claim: object
    comments: list[str]
    before: int
    after: int


def plan_claim_release(
    chainlink_bin: str, issue_id: int, agent_id: str, home: Path,
    *, expected_record=None,
) -> ClaimPlan:
    """Verify ownership before stopping the controller, writing nothing.

    This runs *before* the park stops anything. Every refusal below is
    unrecoverable once the run is terminalized: the park's own already-parked
    refusal stops the script being re-run to finish the job, so a late failure
    strands a parked run whose claim is still held and still charged. Deciding
    first means a park that starts is a park that can finish.
    """
    claims = claims_client(chainlink_bin, agent_id, home)
    comments = issue_comments(claims, issue_id)
    claim = latest_claim(comments, issue_id)
    if claim is None:
        raise ParkError(
            f"issue {issue_id} has no claim record, so there is nothing to release; "
            "refusing to credit a claim that was never recorded"
        )
    if claim.agent_id != agent_id:
        raise ParkError(
            f"the claim on issue {issue_id} is held by {claim.agent_id!r}, not {agent_id!r}. "
            "Releasing another agent's claim is exactly what the ownership check prevents; "
            "pass --agent-id if this run really was claimed under a different identity."
        )

    record = load_factory_record(home, f"chainlink-{issue_id}")
    if record is None or record.issue_id != issue_id:
        raise ParkError("no matching retained record for the claim")
    if expected_record is not None and record != expected_record:
        raise ParkError("retained record changed during park preflight")
    sandbox = Path(record.sandbox)
    if sandbox.parent.name != ".factory-sandboxes":
        raise ParkError("sandbox must be inside .factory-sandboxes")
    expected_snapshot = sandbox.parent.parent / ".factory" / ".parked" / record.run_id
    if (record.status is not None and record.status.park_snapshot is not None
            and Path(record.status.park_snapshot) != expected_snapshot):
        raise ParkError("park_snapshot differs from the expected canonical path")
    if record.attempt > claim.attempt:
        raise ParkError("retained attempt is newer than the claim")

    try:
        # Missing directories are safe to create at publication; unsafe existing
        # ancestors must refuse before the controller is stopped.
        _require_safe_directory(home / "state/worklink/clean-parks", create=False)
    except (FactoryRecordError, OSError) as exc:
        raise ParkError(f"clean-park archive is unsafe or unavailable: {exc}") from exc
    credited = _read_clean_park(home, claim)
    if credited:
        try:
            held = claims._lock_still_held_by(claim)
        except RuntimeError as exc:
            raise ParkError(f"could not verify claim lock: {exc}") from exc
        if not held:
            raise ClaimAlreadyReconciled("this claim is already credited and released; nothing to reconcile")
    before = claims.attempts_used(comments, home_path=home)
    after = claims.attempt_budget(comments, home_path=home, projected_clean_park=claim).charged
    if after >= before and not credited:
        try:
            held = claims._lock_still_held_by(claim)
        except RuntimeError as exc:
            raise ParkError(f"could not verify claim lock: {exc}") from exc
        if held:
            raise ParkError(
                "this claim is already discounted but its lock is still held; "
                "reconcile the interrupted shutdown release before resuming"
            )
        raise ClaimAlreadyReconciled(
            "this claim is already discounted; nothing to reconcile"
        )
    return ClaimPlan(claim=claim, comments=comments, before=before, after=after)


def release_parked_claim(
    chainlink_bin: str, issue_id: int, agent_id: str, home: Path,
) -> tuple[int, str]:
    """Release a parked run's claim after archiving its verified park.

    Evidence is published before releasing the lock, so a partial failure
    cannot leave an undispatchable, charged park. Claim ordinals still advance.

    One deliberate departure: the shutdown path also adds ``worklink:ready``,
    because it is handing work back to the queue. A park is not doing that --
    the run is resumed by an explicit dispatch -- and arming it here would
    auto-dispatch a parked epic.
    """
    plan = plan_claim_release(chainlink_bin, issue_id, agent_id, home)
    already_credited = _read_clean_park(home, plan.claim)
    record = load_factory_record(home, f"chainlink-{issue_id}")
    try:
        if not already_credited and (record is None or not record_clean_park(home, plan.claim, record)):
            raise ParkError(
                "could not verify and archive the published park; claim remains charged and held"
            )
    except (FactoryRecordError, OSError) as exc:
        raise ParkError(f"could not archive the published park; claim remains held: {exc}") from exc

    # Re-read the durable evidence before releasing the lock.
    claims = claims_client(chainlink_bin, agent_id, home)
    observed = claims.attempts_used(issue_comments(claims, issue_id), home_path=home)
    if observed >= plan.before and not already_credited:
        raise ParkError(
            f"the recorded clean park did not discount the claim (attempts_used stayed "
            f"{plan.before}). The claim is still charged; resolve it before resuming."
        )

    released = _run([chainlink_bin, "locks", "release", str(issue_id)])
    if released.returncode != 0:
        return released.returncode, (released.stderr or released.stdout).strip()[:200]
    _run([chainlink_bin, "issue", "unlabel", str(issue_id), "worklink:in-progress"])
    return 0, f"attempt {plan.claim.attempt} cleanly parked, attempts_used {plan.before} -> {observed}"


def claim_needs_reconciliation(
    chainlink_bin: str, issue_id: int, agent_id: str, home: Path,
) -> bool:
    """True when a parked run's claim is still charged."""
    try:
        plan_claim_release(chainlink_bin, issue_id, agent_id, home)
    except ClaimAlreadyReconciled:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ and __doc__.splitlines()[0])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--sandbox", required=True, type=Path)
    parser.add_argument("--home", required=True, type=Path, help="MIMIR_HOME holding the retained record")
    parser.add_argument("--launcher", required=True, type=Path, help="path to the factory CLI entrypoint")
    parser.add_argument("--reason", required=True, help="diagnosis that makes resume a one-liner")
    parser.add_argument(
        "--controller-pid", required=True,
        help=(
            "pid of the 'mimir worklink run-epic <issue>' controller that owns this run, "
            "or 'none' to assert it is already gone. The controller is stopped first: killing "
            "the compute processes underneath a live one makes it race this park's own writes."
        ),
    )
    parser.add_argument(
        "--agent-id",
        default=os.environ.get("MIMIR_WORKLINK_AGENT_ID") or DEFAULT_AGENT_ID,
        help=(
            "identity that must own the claim being released; resolved the same way the "
            "controller resolves it, so it matches by default"
        ),
    )
    parser.add_argument(
        "--chainlink-bin", default="chainlink",
        help="chainlink CLI used to release the claim the stopped controller held",
    )
    parser.add_argument("--dry-run", action="store_true", help="report the plan and refuse to change anything")
    args = parser.parse_args(argv)

    sandbox = args.sandbox.resolve()
    root = operator_root(sandbox)
    plane = sandbox / ".factory" / args.run_id
    parked_dir = root / ".factory" / ".parked"

    print(f"run        : {args.run_id}")
    print(f"sandbox    : {sandbox}")
    print(f"live plane : {plane}")
    print(f"snapshot   : {parked_dir / args.run_id}")

    if not plane.is_dir():
        raise ParkError(f"live plane missing: {plane}")
    args.launcher = resolve_factory_entrypoint(args.launcher)

    before = factory_status(args.launcher, args.run_id, sandbox)
    status = before.get("status")
    print(f"status     : {status}")
    if status == TERMINAL_PARKED:
        # Do not refuse outright. A park that failed after terminalizing leaves
        # the run parked with its claim still held and still charged, and a flat
        # refusal here is what makes that state unrecoverable. Finish the job.
        record = load_factory_record(args.home, args.run_id)
        if record is None:
            raise ParkError(f"no retained record for {args.run_id} under {args.home}")
        if Path(record.sandbox) != sandbox:
            raise ParkError("retained sandbox differs from the requested sandbox")
        if before.get("park_snapshot") != str(parked_dir / args.run_id):
            raise ParkError("park_snapshot is not acknowledged at the expected canonical path")
        _require_daemon_owner(record, before)
        if not claim_needs_reconciliation(
            args.chainlink_bin, record.issue_id, args.agent_id, args.home
        ):
            raise ParkError(
                "run is already parked and its claim needs no reconciliation; nothing to do"
            )
        print("already parked: completing the claim reconciliation a previous run left undone")
        if args.dry_run:
            print("dry-run: would verify owned quiescence and reconcile the parked claim")
            return 0
        with _park_locks(args.home, record.issue_id):
            observed = before
            if record.handle is not None and record.handle.substrate == "opencode_daemon":
                record = _quiesce_daemon_host(record, args.home)
                observed = factory_status(args.launcher, args.run_id, sandbox)
                _require_daemon_owner(record, observed)
                if observed.get("status") != TERMINAL_PARKED or observed.get("park_snapshot") != str(parked_dir / args.run_id):
                    raise ParkError("factory park changed during cancellation; claim retained")
            if not record.children_reaped or not factory_process_is_verified_dead(record):
                raise ParkError("parked host quiescence is unverified; claim retained")
            if record.controller_phase != "parked" or record.status is None or not record.status.is_parked:
                record = replace(record.observed(parse_factory_status(observed), datetime.now(UTC).isoformat()),
                                 controller_phase="parked")
                save_factory_record(args.home, record)
            from mimir_cultivate.worklink.orchestrator import _write_factory_host_evidence
            _write_factory_host_evidence(args.home, record, "needs-human", "operator park reconciled")
            code, detail = release_parked_claim(args.chainlink_bin, record.issue_id, args.agent_id, args.home)
        if code != 0:
            print(f"PARKED, BUT THE CLAIM IS STILL HELD: {detail}", file=sys.stderr)
            return 3
        print(f"released   : claim on issue {record.issue_id} ({detail})")
        return 0
    if status in {"completed", "partial", "blocked"}:
        raise ParkError(f"run is terminal ({status}); it cannot be parked")

    record = load_factory_record(args.home, args.run_id)
    if record is None:
        raise ParkError(f"no retained record for {args.run_id} under {args.home}")
    if Path(record.sandbox) != sandbox:
        raise ParkError("retained sandbox differs from the requested sandbox")
    _require_daemon_owner(record, before)
    expected_snapshot = parked_dir / args.run_id
    if before.get("park_snapshot") is not None and Path(before["park_snapshot"]) != expected_snapshot:
        raise ParkError("park_snapshot differs from the expected canonical path")

    # Identify the controller before anything is signalled, so a bad pid is a
    # refusal rather than a half-completed park.
    controller_pid: int | None = None
    if args.controller_pid.strip().lower() == "none":
        live = find_controllers(record.issue_id)
        if live:
            raise ParkError(
                f"--controller-pid none asserts the controller is gone, but these processes "
                f"still look like this run's controller: {live}. Pass the right pid."
            )
        print("controller : already gone (verified: no matching process)")
    else:
        try:
            controller_pid = int(args.controller_pid)
        except ValueError as exc:
            raise ParkError("--controller-pid must be a pid or the word 'none'") from exc
        argv = verify_controller(controller_pid, record.issue_id)
        print(f"controller : pid {controller_pid} verified -> {argv[:120]}")

    # Decide the claim outcome before stopping anything. Every refusal inside
    # plan_claim_release is unrecoverable once the run is terminalized, so it has
    # to happen while nothing has been changed yet.
    plan = plan_claim_release(
        args.chainlink_bin, record.issue_id, args.agent_id, args.home, expected_record=record,
    )
    print(
        f"claim      : attempt {plan.claim.attempt} owned by {plan.claim.agent_id}; "
        f"clean park would take attempts_used {plan.before} -> {plan.after}"
    )

    if args.dry_run:
        print(
            "dry-run: would stop the controller, then the recorded driver, then any residual "
            "supervisor/driver, then terminalize, publish and reconcile"
        )
        return 0

    # The controller goes first and its death is verified. Until it is gone, any
    # write this script makes to the retained record can be overwritten by the
    # controller's failure path reacting to the driver's exit.
    if controller_pid is not None:
        stop_pid(controller_pid, "controller", verify=lambda pid: verify_controller(pid, record.issue_id))
        print(f"stopped    : controller pid {controller_pid}")

    with _park_locks(args.home, record.issue_id):
        return _finish_operator_park(args, record, sandbox, plane, parked_dir)


def _finish_operator_park(args, record: FactoryRunRecord, sandbox: Path, plane: Path, parked_dir: Path) -> int:
    current = load_factory_record(args.home, args.run_id)
    if current is None or (current.handle, current.session, current.sandbox) != (record.handle, record.session, record.sandbox):
        raise ParkError("retained host changed while stopping the controller; claim retained")
    record = current
    already_parked = False
    # The record's handle is the authoritative driver, not a ps pattern match.
    if record.handle is not None and record.handle.substrate == "opencode_daemon":
        record = _quiesce_daemon_host(record, args.home)
        verified_status = factory_status(args.launcher, args.run_id, sandbox)
        _require_daemon_owner(record, verified_status)
        if verified_status.get("status") in {"completed", "partial", "blocked"}:
            raise ParkError("factory became terminal during cancellation; preserve it for completion recovery")
        already_parked = verified_status.get("status") == TERMINAL_PARKED
    elif factory_process_is_alive(record):
        import asyncio
        from mimir_cultivate.worklink.worker_client import WorkerClient
        if record.handle is None or record.handle.shim_pid is None:
            raise ParkError("historical host has no executor correlation; refusing to guess a PID")
        try:
            asyncio.run(WorkerClient(None).cancel_factory_handle(record.handle))
        except (OSError, RuntimeError, ValueError) as exc:
            raise ParkError("historical host quiescence is unverified; claim retained") from exc
        record = replace(record, children_reaped=True, observed_at=datetime.now(UTC).isoformat())
        save_factory_record(args.home, record)

    residual = stop_residual_compute(args.run_id)
    print(f"stopped    : residual {residual or 'none found'}")

    # This is the predicate mimir's recovery path applies before it will resume.
    # Asserting it here means a park that publishes is a park that can be resumed.
    if not factory_process_is_verified_dead(record) or not record.children_reaped:
        raise ParkError(
            "cannot verify the recorded factory process is dead. mimir's recovery path requires "
            "factory_process_is_verified_dead before it will resume, so publishing a park now "
            "would produce a run that cannot be resumed through the supported path."
        )
    print("verified   : recorded process is dead (the predicate resume will re-check)")

    if not already_parked:
        terminal = _run([
            "node", str(args.launcher), "terminal", args.run_id, TERMINAL_PARKED,
            "--reason", args.reason, "--repo", str(sandbox), "--json",
        ])
        if terminal.returncode != 0:
            raise ParkError(f"factory terminal failed: {redact_daemon_output((terminal.stderr or terminal.stdout).strip())[:300]}")
        print("terminalized: needs-human")

    published = publish_snapshot(plane, parked_dir, args.run_id, launcher=args.launcher)
    print(f"published  : {published}")

    after = factory_status(args.launcher, args.run_id, sandbox)
    if not after.get("park_snapshot"):
        raise ParkError(
            "factory status reports park_snapshot null: the snapshot is not acknowledged. "
            "The park is not complete; do not treat the run as resumable."
        )
    print(f"verified   : park_snapshot = {after['park_snapshot']}")

    # Reconcile mimir's view last, under the interlock the pruner also takes, so
    # cleanup cannot observe a stale 'running' record and delete a parked run.
    current = load_factory_record(args.home, args.run_id)
    if current is None:
        raise ParkError("retained record vanished while parking")
    reconciled = replace(current.observed(parse_factory_status(after), datetime.now(UTC).isoformat()),
                         controller_phase="parked")
    save_factory_record(args.home, reconciled)
    from mimir_cultivate.worklink.orchestrator import _write_factory_host_evidence
    _write_factory_host_evidence(args.home, reconciled, "needs-human", "operator park reconciled")
    print("reconciled : controller_phase=parked, observed status=needs-human")

    print()
    # Re-read rather than trust the write: if anything still held this record,
    # the operator needs to know now and not at resume time.
    settled = load_factory_record(args.home, args.run_id)
    if settled is None:
        raise ParkError("retained record vanished immediately after reconcile")
    if settled.controller_phase != "parked":
        raise ParkError(
            f"retained record reads controller_phase={settled.controller_phase!r} immediately "
            "after reconcile: something else is still writing it. The park is not safe to rely on."
        )
    if settled.status is None or settled.status.status != TERMINAL_PARKED:
        raise ParkError(
            "retained record does not read needs-human after reconcile; resume would refuse it"
        )
    print("settled    : record re-read and still parked")

    # Release the claim the stopped controller held after recording the clean park.
    # Two separate reasons, both of which bite:
    #   * unreleased, the next dispatch is refused as `duplicate_run_live`,
    #     because the chainlink CLI answers a same-agent re-claim with "You
    #     already hold the lock" and `claim_issue` reads that string as the
    #     trigger for its duplicate-liveness guard;
    #   * uncredited, the resume's own claim is charged a second attempt for
    #     one run, and a run parked on its last attempt returns
    #     `attempts_exhausted` rather than resuming.
    code, detail = release_parked_claim(
        args.chainlink_bin, record.issue_id, args.agent_id, args.home,
    )
    if code == 0:
        print(f"released   : claim on issue {record.issue_id} ({detail})")
    else:
        # The park itself is published and reconciled, so this is not a refusal
        # -- but the run is not dispatchable until the claim is released, which
        # is the one thing an operator must not have to discover at resume time.
        print()
        print(f"PARKED, BUT THE CLAIM IS STILL HELD: {detail}", file=sys.stderr)
        print(
            f"release it before resuming:  {args.chainlink_bin} locks release {record.issue_id}",
            file=sys.stderr,
        )
        return 3

    print()
    print("resume with:")
    print(f"  scripts/factory_resume.py --run-id {args.run_id} --sandbox {sandbox} \\")
    print(f"      --home {args.home} --launcher {args.launcher} --repo <CONTROLLER_REPO>")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ParkError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
