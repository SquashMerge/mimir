#!/bin/sh
# Verify this exact HEAD through GitHub CI, without taking the local heavy lease.
# Exit 0: every required check succeeded; 1: a test failed/timed out;
# 75: verification unavailable. Outage policy: exit 75 and pause, NEVER fall
# back to a local full-suite/heavy run.
#
# Requires git, gh and python3. CI_VERIFY_TIMEOUT_SECONDS defaults to 170 minutes
# (must be <179 minutes, leaving cleanup margin below factory's 180 minutes).
# CI_VERIFY_START_TIMEOUT_SECONDS (default 300) bounds waiting for any required
# check to appear; CI_VERIFY_POLL_SECONDS (default 30) controls polling.

sha=$(git rev-parse HEAD) || exit 75

cleanup() {
    result=$?
    trap - 0
    # Bound cleanup too; a failed delete must never replace the verify result.
    python3 - "$sha" <<'PY' || :
import subprocess
import sys

try:
    subprocess.run(
        ["git", "push", "origin", f":refs/heads/qualify/factory-{sys.argv[1]}"],
        timeout=30, check=False,
    )
except (OSError, subprocess.TimeoutExpired) as exc:
    print(f"ci_verify: branch cleanup unavailable: {exc}", file=sys.stderr)
PY
    exit "$result"
}
trap cleanup 0
trap 'exit 75' HUP INT TERM

# Python supplies monotonic deadlines, bounded subprocesses, and JSON parsing;
# the shell owns cleanup even when the verifier exits early.
python3 - "$sha" <<'PY'
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


class Unavailable(Exception):
    pass


def seconds(name, default):
    value = float(os.environ.get(name, default))
    if not math.isfinite(value) or value <= 0:
        raise Unavailable(f"{name} must be finite and positive")
    return value


def main():
    sha = sys.argv[1]
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", sha):
        raise Unavailable("HEAD did not resolve to a commit SHA")
    budget = seconds("CI_VERIFY_TIMEOUT_SECONDS", "10200")
    if budget >= 10740:
        raise Unavailable("deadline must be shorter than 179 minutes")
    start = time.monotonic()
    deadline = start + budget
    start_deadline = min(deadline, start + seconds("CI_VERIFY_START_TIMEOUT_SECONDS", "300"))
    poll = seconds("CI_VERIFY_POLL_SECONDS", "30")

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise Unavailable("verification deadline reached")
        return left

    def run(*command):
        result = subprocess.run(
            command, capture_output=True, text=True,
            timeout=min(60, remaining()), check=False,
        )
        remaining()
        if result.returncode:
            raise Unavailable(f"{command[0]} failed ({result.returncode}): {result.stderr.strip()}")
        return result.stdout.strip()

    # The sandbox's fetch URL can be a local path. Only the push URL identifies
    # GitHub; reject unknown hosts/paths rather than letting gh infer a repo.
    url = run("git", "remote", "get-url", "--push", "origin")
    match = re.fullmatch(
        r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)"
        r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?", url,
    )
    if not match or any(part in (".", "..") for part in match[1].split("/")):
        raise Unavailable("cannot derive GitHub owner/name from origin push URL")
    repo = match[1]
    run("git", "push", "origin", f"{sha}:refs/heads/qualify/factory-{sha}")
    started = False
    while True:
        remaining()
        # filter=all includes reruns; pagination prevents unrelated checks from
        # hiding a required name. Never query 'latest on branch'.
        pages = json.loads(run(
            "gh", "api", f"repos/{repo}/commits/{sha}/check-runs?filter=all&per_page=100",
            "--paginate", "--slurp",
        ))
        newest = {}
        for page in pages:
            for check in page["check_runs"]:
                name = check["name"]
                if name not in REQUIRED_CHECKS:
                    continue
                if check["head_sha"] != sha:
                    raise Unavailable("check run does not belong to the requested SHA")
                # GitHub assigns a new, increasing check-run id to each rerun.
                if name not in newest or check["id"] > newest[name]["id"]:
                    newest[name] = check
        started = started or bool(newest)
        failures = [
            name for name, check in newest.items()
            if check["status"] == "completed" and check["conclusion"] in ("failure", "timed_out")
        ]
        if failures:
            print("ci_verify: failed: " + ", ".join(failures), file=sys.stderr)
            return 1
        for name, check in newest.items():
            if check["status"] == "completed" and check["conclusion"] != "success":
                raise Unavailable(f"{name}: {check['conclusion']}")
        remaining()
        if all(
            name in newest and newest[name]["status"] == "completed"
            and newest[name]["conclusion"] == "success"
            for name in REQUIRED_CHECKS
        ):
            print(f"ci_verify: all {len(REQUIRED_CHECKS)} required checks succeeded on {sha}")
            return 0
        if not started and time.monotonic() >= start_deadline:
            raise Unavailable("no required check started within the startup wait")
        wait = remaining()
        if not started:
            wait = min(wait, max(0, start_deadline - time.monotonic()))
        time.sleep(min(poll, wait))


try:
    sys.exit(main())
except (Unavailable, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
    print(f"ci_verify: unavailable: {exc}", file=sys.stderr)
    sys.exit(75)
PY
result=$?
case "$result" in
    0|1) exit "$result" ;;
    *) exit 75 ;;
esac
