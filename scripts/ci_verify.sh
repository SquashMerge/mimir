#!/bin/sh
# Verify the captured commit on GitHub, never the latest run on a moving branch.
# Requires git, gh and Python 3 (available after factory bootstrap).
# Outage policy: exit 75 and pause. NEVER fall back to a local heavy/full run.
# 0 = every required check succeeded; 1 = test failure/timed_out; 75 = unavailable.
# CI_VERIFY_TIMEOUT_SECONDS defaults to 170 minutes, below factory's 180 minutes.
# CI_VERIFY_START_TIMEOUT_SECONDS (10 minutes) bounds waiting for any required
# check to appear; CI_VERIFY_POLL_SECONDS (30 seconds) controls polling frequency.
# Network commands retry up to 10 consecutive transient failures within the
# deadline (each attempt at most 60 seconds); success resets the count.
# Cleanup has a separate 15-second bound.

sha=$(git rev-parse HEAD) || exit 75
ref="refs/heads/qualify/factory-$sha"

cleanup() {
    result=$?
    trap - 0 HUP INT TERM
    python3 - "$ref" <<'PY_CLEANUP'
import subprocess
import sys

try:
    subprocess.run(
        ["git", "push", "origin", ":" + sys.argv[1]],
        timeout=15, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
except (OSError, subprocess.TimeoutExpired):
    pass
PY_CLEANUP
    exit "$result"
}
trap cleanup 0
trap 'exit 75' HUP INT TERM

python3 - "$sha" <<'PY_VERIFY'
import json
import math
import os
import re
import subprocess
import sys
import time

REQUIRED_CHECKS = (
    "pytest (3.11)",
    "pytest (3.12)",
    "pytest (3.13)",
    "pytest-macos (3.11)",
    "pytest-macos (3.12)",
    "pytest-macos (3.13)",
    "pytest-worker-uid",
    "pytest-enforced",
    "pytest-fresh-resolve",
    "skill-conformance",
    "frontend",
    "claude-code-extra-smoke",
    "package",
    "chainlink-cli-audit",
    "worklink-image-identity",
)


def unavailable(reason):
    print("CI verify unavailable: " + reason, file=sys.stderr)
    raise SystemExit(75)


def seconds(name, default):
    try:
        value = float(os.environ.get(name, default))
    except ValueError:
        unavailable(name + " must be a positive number")
    if not math.isfinite(value) or value <= 0:
        unavailable(name + " must be a positive number")
    return value


started = time.monotonic()
budget = seconds("CI_VERIFY_TIMEOUT_SECONDS", 170 * 60)
# Even an override cannot run into factory's 180-minute kill timeout.
if budget > 170 * 60:
    unavailable("deadline must not exceed 170 minutes")
deadline = started + budget
start_deadline = min(deadline, started + seconds("CI_VERIFY_START_TIMEOUT_SECONDS", 600))
poll_seconds = seconds("CI_VERIFY_POLL_SECONDS", 30)
sha = sys.argv[1]


def remaining():
    left = deadline - time.monotonic()
    if left <= 0:
        unavailable("deadline reached")
    return left


MAX_CONSECUTIVE_TRANSIENT_FAILURES = 10
TRANSIENT_ERRORS = re.compile(
    r"error connecting to api[.]github[.]com|could not resolve host|"
    r"connection reset|timed out|"
    r"(?:HTTP(?:/\d(?:[.]\d)?)?\s+|returned error:\s*)5\d\d\b",
    re.IGNORECASE,
)
PERMANENT_HTTP_ERRORS = re.compile(
    r"(?:HTTP(?:/\d(?:[.]\d)?)?\s+|returned error:\s*)4\d\d\b", re.IGNORECASE,
)


def command(*args, until=None):
    # This counter spans retries of one operation, not the lifetime of verify:
    # returning successfully resets it before the next push/poll operation.
    consecutive = 0
    command_deadline = min(deadline, until) if until is not None else deadline
    while True:
        left = min(remaining(), command_deadline - time.monotonic())
        if left <= 0:
            unavailable("no required checks started before command timeout")
        try:
            result = subprocess.run(
                args, check=True, capture_output=True, text=True,
                timeout=min(left, 60),
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GH_PROMPT_DISABLED": "1"},
            )
            remaining()
            return result.stdout
        except subprocess.TimeoutExpired:
            # A bounded network attempt timing out is transient, but cannot
            # extend either the overall deadline or the initial-start bound.
            pass
        except subprocess.CalledProcessError as exc:
            evidence = (exc.stderr or "") + "\n" + (exc.stdout or "")
            if PERMANENT_HTTP_ERRORS.search(evidence) or not TRANSIENT_ERRORS.search(evidence):
                unavailable(f"{args[0]} failed with a non-transient error")
        except (OSError, subprocess.SubprocessError) as exc:
            unavailable(f"{args[0]} could not run ({type(exc).__name__})")
        remaining()
        if time.monotonic() >= command_deadline:
            unavailable("no required checks started before command timeout")
        consecutive += 1
        if consecutive >= MAX_CONSECUTIVE_TRANSIENT_FAILURES:
            unavailable("consecutive transient failure limit reached")
        print(
            f"Retrying {args[0]} after transient failure {consecutive}/"
            f"{MAX_CONSECUTIVE_TRANSIENT_FAILURES}", file=sys.stderr, flush=True,
        )
        time.sleep(min(poll_seconds, remaining(), max(0, command_deadline - time.monotonic())))


# origin's fetch URL is a local sandbox path; only its push URL identifies GitHub.
push_url = command("git", "remote", "get-url", "--push", "origin").strip()
match = re.fullmatch(
    r"(?:https://github[.]com/|ssh://git@github[.]com/|git@github[.]com:)"
    r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:[.]git)?/?",
    push_url,
)
if match is None:
    unavailable("cannot derive owner/name from origin push URL")
REPO = match.group(1)
command("git", "push", "origin", f"{sha}:refs/heads/qualify/factory-{sha}")
seen_required = False

while True:
    remaining()
    if not seen_required and time.monotonic() >= start_deadline:
        unavailable("no required checks started")
    # Explicit all + pagination avoids dropping superseded or late-page checks.
    # Never consult branch runs, whose head could have changed since the push.
    raw = command(
        "gh", "api", f"repos/{REPO}/commits/{sha}/check-runs?per_page=100&filter=all",
        "--paginate", "--slurp",
        until=None if seen_required else start_deadline,
    )
    remaining()
    try:
        pages = json.loads(raw)
        if not isinstance(pages, list) or not pages:
            raise ValueError("expected paginated check-runs objects")
        newest = {}
        for page in pages:
            if not isinstance(page, dict) or not isinstance(page.get("check_runs"), list):
                raise ValueError("expected check_runs list")
            for check in page["check_runs"]:
                if check["head_sha"] != sha or check["name"] not in REQUIRED_CHECKS:
                    continue
                # GitHub check-run IDs increase on reruns. API ordering is not
                # authoritative, including when a rerun crosses a page boundary.
                name = check["name"]
                if name not in newest or check["id"] > newest[name]["id"]:
                    newest[name] = check
        completed = {
            name: check["conclusion"]
            for name, check in newest.items() if check["status"] == "completed"
        }
    except (ValueError, KeyError, TypeError):
        unavailable("invalid check-runs response")

    seen_required = seen_required or bool(newest)
    failures = [name for name, result in completed.items() if result in ("failure", "timed_out")]
    if failures:
        print("CI checks failed: " + ", ".join(failures), file=sys.stderr)
        raise SystemExit(1)
    if any(result != "success" for result in completed.values()):
        unavailable("required check cancelled, skipped, or otherwise inconclusive")
    if all(completed.get(name) == "success" for name in REQUIRED_CHECKS):
        print(f"All {len(REQUIRED_CHECKS)} required checks succeeded on {sha}")
        raise SystemExit(0)
    if not seen_required and time.monotonic() >= start_deadline:
        unavailable("no required checks started")
    pending = [name for name in REQUIRED_CHECKS if completed.get(name) != "success"]
    print("Waiting for: " + ", ".join(pending), file=sys.stderr, flush=True)
    wait = min(poll_seconds, remaining())
    if not seen_required:
        wait = min(wait, max(0, start_deadline - time.monotonic()))
    time.sleep(wait)
PY_VERIFY
result=$?
case "$result" in
    0|1) exit "$result" ;;
    *) exit 75 ;;
esac
