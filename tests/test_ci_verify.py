"""Exercise the real shell entry point with fixture-owned git/gh, never GitHub."""
from __future__ import annotations

import ast
import itertools
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/ci_verify.sh"
HANG_GUARD_SECONDS = 30
SHA = "1234567890abcdef1234567890abcdef12345678"
# Independent fixture, not imported from the implementation under test.
REQUIRED = [
    "pytest (3.11)", "pytest (3.12)", "pytest (3.13)",
    "pytest-macos (3.11)", "pytest-macos (3.12)", "pytest-macos (3.13)",
    "pytest-worker-uid", "pytest-enforced", "pytest-fresh-resolve",
    "skill-conformance", "frontend", "claude-code-extra-smoke", "package",
    "chainlink-cli-audit", "worklink-image-identity",
]


def green_checks() -> list[dict]:
    return [
        {"id": i + 1, "name": name, "head_sha": SHA,
         "status": "completed", "conclusion": "success"}
        for i, name in enumerate(REQUIRED)
    ]


class FakeCI:
    def __init__(self, tmp_path: Path):
        self.path = tmp_path
        self.log = tmp_path / "calls.jsonl"
        bindir = tmp_path / "bin"
        bindir.mkdir()
        # Both executables are strict: any branch-relative query, implicit repo,
        # fetch URL lookup, or push of HEAD fails rather than silently working.
        source = f"#!{sys.executable}\n" + '''
import json
import os
from pathlib import Path
import sys
import time

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
root = Path(os.environ["FAKE_CI_ROOT"])
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps([tool, *args]) + "\\n")
sha = os.environ["FAKE_CI_SHA"]
ref = "refs/heads/qualify/factory-" + sha


def sequence_error(kind):
    count_file = root / (kind + "-errors-count")
    count = int(count_file.read_text()) if count_file.exists() else 0
    count_file.write_text(str(count + 1))
    errors = json.loads(os.environ.get("FAKE_CI_" + kind.upper() + "_ERRORS", "[]"))
    if count < len(errors) and errors[count] is not None:
        print(errors[count], file=sys.stderr)
        sys.exit(1)


if tool == "git":
    if args == ["rev-parse", "HEAD"]:
        print(sha)
    elif args == ["remote", "get-url", "--push", "origin"]:
        print(os.environ["FAKE_CI_PUSH_URL"])
    elif args == ["remote", "get-url", "origin"]:
        print("/local/factory/source")  # deliberately not a GitHub fetch URL
    elif args == ["push", "origin", sha + ":" + ref]:
        sequence_error("push")
        sys.exit(int(os.environ.get("FAKE_CI_PUSH_RC", "0")))
    elif args == ["push", "origin", ":" + ref]:
        sys.exit(int(os.environ.get("FAKE_CI_CLEANUP_RC", "0")))
    else:
        sys.exit("unexpected git arguments: " + repr(args))
elif tool == "gh":
    expected = [
        "api", "repos/push-owner/push-repo/commits/" + sha
        + "/check-runs?per_page=100&filter=all", "--paginate", "--slurp",
    ]
    if args != expected:
        sys.exit("unexpected gh arguments: " + repr(args))
    sequence_error("gh")
    time.sleep(float(os.environ.get("FAKE_CI_GH_DELAY", "0")))
    if os.environ.get("FAKE_CI_GH_RC"):
        sys.exit(int(os.environ["FAKE_CI_GH_RC"]))
    count_file = root / "count"
    count = int(count_file.read_text()) if count_file.exists() else 0
    count_file.write_text(str(count + 1))
    responses = json.loads((root / "responses.json").read_text())
    print(json.dumps(responses[min(count, len(responses) - 1)]))
'''
        for name in ("git", "gh"):
            stub = bindir / name
            stub.write_text(source)
            stub.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_CI_ROOT": str(tmp_path),
            "FAKE_CI_SHA": SHA,
            "FAKE_CI_PUSH_URL": "git@github.com:push-owner/push-repo.git",
            "CI_VERIFY_TIMEOUT_SECONDS": "10",
            "CI_VERIFY_START_TIMEOUT_SECONDS": "5",
            "CI_VERIFY_POLL_SECONDS": "0.02",
            # A configured but unusable lease must not affect this entry point.
            "MIMIR_HEAVY_LOCK_FILE": str(tmp_path / "nonexistent" / "lease"),
        }

    def run(
        self, checks: list[dict] | None = None, *, responses: list | None = None,
        script: Path = SCRIPT,
    ) -> subprocess.CompletedProcess:
        if responses is None:
            responses = [[{"check_runs": green_checks() if checks is None else checks}]]
        (self.path / "responses.json").write_text(json.dumps(responses))
        proc = subprocess.Popen(
            ["sh", str(script)], env=self.env, cwd=ROOT,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            # Hang guard, not a latency assertion: loaded macOS runners may
            # need time to launch the many CLI stubs. Deadline tests assert
            # the script's own reported reason and exit status instead.
            stdout, stderr = proc.communicate(timeout=HANG_GUARD_SECONDS)
        except subprocess.TimeoutExpired:
            # A removed-deadline mutation must fail without leaving an orphaned
            # poller (killing only the shell would leave its Python child alive).
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                # The group may already be gone/reaping on macOS. Check the
                # direct child and kill it if needed; preserve the guard error.
                if proc.poll() is None:
                    proc.kill()
            proc.communicate()
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)

    def calls(self) -> list[list[str]]:
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def assert_cleanup(self) -> None:
        assert self.calls()[-1] == [
            "git", "push", "origin", f":refs/heads/qualify/factory-{SHA}",
        ]


@pytest.fixture
def ci(tmp_path: Path) -> FakeCI:
    return FakeCI(tmp_path)


@pytest.mark.parametrize("group_error", [ProcessLookupError, PermissionError])
@pytest.mark.parametrize("already_exited", [True, False])
def test_hang_guard_preserves_timeout_when_group_kill_races(
    ci: FakeCI, monkeypatch: pytest.MonkeyPatch,
    group_error: type[OSError], already_exited: bool,
) -> None:
    script = ci.path / "child.sh"
    # exec keeps the live fallback case to a single fixture-owned process.
    script.write_text(
        "exit 0\n" if already_exited
        else "exec python3 -c 'import time; time.sleep(300)'\n"
    )
    proc = subprocess.Popen(
        ["sh", str(script)], env=ci.env, cwd=ROOT,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    communicate = proc.communicate
    kill = proc.kill
    guard_timeout = subprocess.TimeoutExpired(proc.args, HANG_GUARD_SECONDS)
    calls = []

    def communicate_with_guard(timeout=None):
        calls.append(("communicate", timeout))
        if len(calls) == 1:
            assert timeout == HANG_GUARD_SECONDS
            if already_exited:
                proc.wait(timeout=HANG_GUARD_SECONDS)
                assert proc.returncode == 0
            raise guard_timeout
        return communicate(timeout=timeout)

    def group_gone(pid, sig):
        assert (pid, sig) == (proc.pid, signal.SIGKILL)
        calls.append(("killpg", sig))
        raise group_error("process group already gone")

    def kill_child():
        calls.append(("kill", None))
        kill()

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: proc)
    monkeypatch.setattr(proc, "communicate", communicate_with_guard)
    monkeypatch.setattr(proc, "kill", kill_child)
    monkeypatch.setattr(os, "killpg", group_gone)
    try:
        with pytest.raises(subprocess.TimeoutExpired) as exc:
            ci.run(script=script)
        assert exc.value is guard_timeout
        assert proc.returncode is not None
        assert (("kill", None) in calls) == (not already_exited)
        assert calls[-1] == ("communicate", None)
    finally:
        # Also reap the fixture if a cleanup regression makes the assertion fail.
        kill()
        communicate(timeout=HANG_GUARD_SECONDS)


def test_all_required_green_and_exact_sha_push_and_poll(ci: FakeCI) -> None:
    proc = ci.run()
    assert proc.returncode == 0, proc.stderr
    calls = ci.calls()
    assert calls[0] == ["git", "rev-parse", "HEAD"]
    assert calls[1] == ["git", "remote", "get-url", "--push", "origin"]
    assert calls[2] == [
        "git", "push", "origin", f"{SHA}:refs/heads/qualify/factory-{SHA}",
    ]
    assert calls[3] == [
        "gh", "api",
        f"repos/push-owner/push-repo/commits/{SHA}/check-runs?per_page=100&filter=all",
        "--paginate", "--slurp",
    ]
    ci.assert_cleanup()


@pytest.mark.parametrize("conclusion", ["failure", "timed_out"])
def test_failed_required_check(ci: FakeCI, conclusion: str) -> None:
    checks = green_checks()
    checks[0]["conclusion"] = conclusion
    assert ci.run(checks).returncode == 1
    ci.assert_cleanup()


@pytest.mark.parametrize("conclusion", ["cancelled", "skipped", "neutral", "stale", None])
def test_inconclusive_required_check_is_unavailable(ci: FakeCI, conclusion: str) -> None:
    checks = green_checks()
    checks[0]["conclusion"] = conclusion
    assert ci.run(checks).returncode == 75
    ci.assert_cleanup()


@pytest.mark.parametrize("variant", ["missing", "renamed", "wrong-sha", "queued", "in-progress"])
def test_missing_success_reaches_own_deadline(ci: FakeCI, variant: str) -> None:
    checks = green_checks()
    if variant == "missing":
        checks.pop()
    elif variant == "renamed":
        checks[-1]["name"] = "renamed-not-required"
    elif variant == "wrong-sha":
        checks[-1]["head_sha"] = "a" * 40
    else:
        checks[-1].update(status=variant.replace("-", "_"), conclusion="success")
    ci.env["CI_VERIFY_TIMEOUT_SECONDS"] = "0.6"
    proc = ci.run(checks)
    assert proc.returncode == 75, proc.stderr
    assert "deadline reached" in proc.stderr
    ci.assert_cleanup()


def test_no_required_check_starts_within_bounded_wait(ci: FakeCI) -> None:
    ci.env["CI_VERIFY_START_TIMEOUT_SECONDS"] = "0.6"
    proc = ci.run([])
    assert proc.returncode == 75
    assert "no required checks started" in proc.stderr
    ci.assert_cleanup()


@pytest.mark.parametrize("mode", ["error", "hung"])
def test_github_unreachable_or_hung_is_unavailable(ci: FakeCI, mode: str) -> None:
    ci.env["CI_VERIFY_TIMEOUT_SECONDS"] = "0.6"
    ci.env["FAKE_CI_GH_RC" if mode == "error" else "FAKE_CI_GH_DELAY"] = "15"
    assert ci.run().returncode == 75
    ci.assert_cleanup()


TRANSIENT_MESSAGES = [
    "error connecting to api.github.com",
    "fatal: Could not resolve host: github.com",
    "connection reset by peer",
    "request timed out",
    "gh: service unavailable (HTTP 503)",
    "fatal: unable to access remote: The requested URL returned error: 502",
    "HTTP/2 500 Internal Server Error",
]


def network_calls(ci: FakeCI, kind: str) -> int:
    if kind == "gh":
        return sum(call[0] == "gh" for call in ci.calls())
    return sum(call == [
        "git", "push", "origin", f"{SHA}:refs/heads/qualify/factory-{SHA}",
    ] for call in ci.calls())


@pytest.mark.parametrize("kind", ["gh", "push"])
@pytest.mark.parametrize("message", TRANSIENT_MESSAGES)
def test_transient_error_then_success_recovers(ci: FakeCI, kind: str, message: str) -> None:
    ci.env[f"FAKE_CI_{kind.upper()}_ERRORS"] = json.dumps([message])
    proc = ci.run()
    assert proc.returncode == 0, proc.stderr
    assert network_calls(ci, kind) == 2
    ci.assert_cleanup()


@pytest.mark.parametrize("kind", ["gh", "push"])
def test_ten_consecutive_transient_errors_hit_bound(ci: FakeCI, kind: str) -> None:
    # A green response is available on call 11: removing only the retry bound
    # would return 0, rather than failing for an unrelated process timeout.
    ci.env[f"FAKE_CI_{kind.upper()}_ERRORS"] = json.dumps([TRANSIENT_MESSAGES[0]] * 10)
    proc = ci.run()
    assert proc.returncode == 75, proc.stderr
    assert "consecutive transient failure limit reached" in proc.stderr
    assert network_calls(ci, kind) == 10
    if kind == "push":
        assert network_calls(ci, "gh") == 0
    ci.assert_cleanup()


def test_successful_poll_resets_consecutive_failure_count(ci: FakeCI) -> None:
    ci.env["FAKE_CI_GH_ERRORS"] = json.dumps(
        [TRANSIENT_MESSAGES[0]] * 9 + [None] + [TRANSIENT_MESSAGES[0]] * 9,
    )
    pending = green_checks()
    pending[0].update(status="in_progress", conclusion=None)
    proc = ci.run(responses=[[{"check_runs": pending}], [{"check_runs": green_checks()}]])
    assert proc.returncode == 0, proc.stderr
    assert network_calls(ci, "gh") == 20
    ci.assert_cleanup()


@pytest.mark.parametrize("kind", ["gh", "push"])
@pytest.mark.parametrize("message", [
    "HTTP 401 Bad credentials", "HTTP 403 Forbidden", "HTTP 404 Not Found",
    "HTTP 403 Forbidden: upstream timed out", "unrecognized error",
])
def test_non_transient_error_exits_immediately(ci: FakeCI, kind: str, message: str) -> None:
    ci.env[f"FAKE_CI_{kind.upper()}_ERRORS"] = json.dumps([message])
    proc = ci.run()
    assert proc.returncode == 75, proc.stderr
    assert network_calls(ci, kind) == 1
    ci.assert_cleanup()


@pytest.mark.parametrize("kind", ["gh", "push"])
def test_transient_retries_obey_overall_deadline(ci: FakeCI, kind: str) -> None:
    ci.env[f"FAKE_CI_{kind.upper()}_ERRORS"] = json.dumps([TRANSIENT_MESSAGES[0]] * 10)
    ci.env["CI_VERIFY_TIMEOUT_SECONDS"] = "0.6"
    ci.env["CI_VERIFY_POLL_SECONDS"] = "1"
    proc = ci.run()
    assert proc.returncode == 75
    assert "deadline reached" in proc.stderr
    assert network_calls(ci, kind) < 10
    ci.assert_cleanup()


def test_transient_polls_obey_start_deadline(ci: FakeCI) -> None:
    ci.env["FAKE_CI_GH_ERRORS"] = json.dumps([TRANSIENT_MESSAGES[0]] * 10)
    ci.env["CI_VERIFY_START_TIMEOUT_SECONDS"] = "0.6"
    ci.env["CI_VERIFY_POLL_SECONDS"] = "1"
    proc = ci.run()
    assert proc.returncode == 75
    assert "no required checks started" in proc.stderr
    assert network_calls(ci, "gh") < 10
    ci.assert_cleanup()


def test_recovered_network_does_not_hide_check_failure(ci: FakeCI) -> None:
    ci.env["FAKE_CI_GH_ERRORS"] = json.dumps([TRANSIENT_MESSAGES[0]])
    checks = green_checks()
    checks[0]["conclusion"] = "failure"
    proc = ci.run(checks)
    assert proc.returncode == 1, proc.stderr
    assert network_calls(ci, "gh") == 2
    ci.assert_cleanup()


@pytest.mark.parametrize("mutation", ["all-errors-fatal", "no-consecutive-bound"])
def test_retry_regressions_distinguish_targeted_mutations(ci: FakeCI, mutation: str) -> None:
    source = SCRIPT.read_text()
    if mutation == "all-errors-fatal":
        old = "if PERMANENT_HTTP_ERRORS.search(evidence) or not TRANSIENT_ERRORS.search(evidence):"
        new = "if True:"
        errors = [TRANSIENT_MESSAGES[0]]
        expected = 0
    else:
        old = "if consecutive >= MAX_CONSECUTIVE_TRANSIENT_FAILURES:"
        new = "if False:"
        errors = [TRANSIENT_MESSAGES[0]] * 10
        expected = 75
    assert source.count(old) == 1
    mutant = ci.path / "mutant.sh"
    mutant.write_text(source.replace(old, new))
    ci.env["FAKE_CI_GH_ERRORS"] = json.dumps(errors)
    proc = ci.run(script=mutant)
    assert proc.returncode != expected, proc.stderr
    # Both mutants terminate normally with the opposite outcome; timeout or a
    # syntax error is not accepted as a mutation kill.
    assert proc.returncode == (75 if mutation == "all-errors-fatal" else 0)
    ci.assert_cleanup()


@pytest.mark.parametrize("new_result,expected", [("success", 0), ("failure", 1)])
@pytest.mark.parametrize("new_first", [False, True])
def test_newest_run_wins_across_pages(
    ci: FakeCI, new_result: str, expected: int, new_first: bool,
) -> None:
    checks = green_checks()
    old = {**checks[0], "conclusion": "failure" if new_result == "success" else "success"}
    new = {**checks[0], "id": 1000, "conclusion": new_result}
    pages = [{"check_runs": [old, *checks[1:]]}, {"check_runs": [new]}]
    if new_first:
        pages.reverse()
    proc = ci.run(responses=[pages])
    assert proc.returncode == expected, proc.stderr
    ci.assert_cleanup()


def test_queued_newer_run_supersedes_older_success(ci: FakeCI) -> None:
    checks = green_checks()
    checks.append({**checks[0], "id": 1000, "status": "queued", "conclusion": None})
    ci.env["CI_VERIFY_TIMEOUT_SECONDS"] = "0.6"
    assert ci.run(checks).returncode == 75


def test_waits_for_pending_required_checks_then_passes(ci: FakeCI) -> None:
    pending = green_checks()
    pending[0].update(status="in_progress", conclusion=None)
    proc = ci.run(responses=[
        [{"check_runs": []}], [{"check_runs": pending}], [{"check_runs": green_checks()}],
    ])
    assert proc.returncode == 0, proc.stderr
    assert sum(call[0] == "gh" for call in ci.calls()) == 3


def test_unrelated_failure_is_not_a_required_check(ci: FakeCI) -> None:
    checks = green_checks()
    checks.append({**checks[0], "name": "optional-check", "id": 1000, "conclusion": "failure"})
    assert ci.run(checks).returncode == 0


@pytest.mark.parametrize("url", [
    "https://github.com/push-owner/push-repo.git",
    "https://github.com/push-owner/push-repo",
    "ssh://git@github.com/push-owner/push-repo.git",
    "git@github.com:push-owner/push-repo.git",
])
def test_repo_comes_from_push_url_not_local_fetch_url(ci: FakeCI, url: str) -> None:
    ci.env["FAKE_CI_PUSH_URL"] = url
    proc = ci.run()
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("url", [
    "/local/factory/source", "", "https://not-github.com/push-owner/push-repo",
    "https://github.com/owner/repo/extra", "https://github.com/owner",
])
def test_unrecognized_repo_fails_closed(ci: FakeCI, url: str) -> None:
    ci.env["FAKE_CI_PUSH_URL"] = url
    assert ci.run().returncode == 75
    assert not any(call[0] == "gh" for call in ci.calls())
    ci.assert_cleanup()


def test_push_failure_is_unavailable_and_still_cleans_up(ci: FakeCI) -> None:
    ci.env["FAKE_CI_PUSH_RC"] = "1"
    assert ci.run().returncode == 75
    assert not any(call[0] == "gh" for call in ci.calls())
    ci.assert_cleanup()


@pytest.mark.parametrize("expected", [0, 1, 75])
def test_cleanup_failure_preserves_result(ci: FakeCI, expected: int) -> None:
    ci.env["FAKE_CI_CLEANUP_RC"] = "1"
    checks = green_checks()
    if expected == 1:
        checks[0]["conclusion"] = "failure"
    elif expected == 75:
        checks[0]["conclusion"] = "cancelled"
    assert ci.run(checks).returncode == expected
    ci.assert_cleanup()


@pytest.mark.parametrize("response", [None, {}, [{"check_runs": None}], [{"check_runs": [{}]}]])
def test_malformed_api_response_never_passes(ci: FakeCI, response) -> None:
    assert ci.run(responses=[response]).returncode == 75
    ci.assert_cleanup()


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "invalid", "10800"])
def test_invalid_deadline_is_unavailable(ci: FakeCI, value: str) -> None:
    ci.env["CI_VERIFY_TIMEOUT_SECONDS"] = value
    assert ci.run().returncode == 75
    ci.assert_cleanup()


def test_fixed_required_names_match_workflow_check_runs() -> None:
    source = SCRIPT.read_text().split("<<'PY_VERIFY'\n", 1)[1].split("\nPY_VERIFY", 1)[0]
    assignment = next(
        node for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "REQUIRED_CHECKS" for t in node.targets)
    )
    required = ast.literal_eval(assignment.value)
    assert len(required) == len(set(required)) == 15
    assert set(required) == set(REQUIRED)
    workflow = yaml.safe_load((ROOT / ".github/workflows/tests.yml").read_text())
    names = []
    for job_id, job in workflow["jobs"].items():
        matrix = job.get("strategy", {}).get("matrix", {})
        assert not {"include", "exclude"} & matrix.keys(), "extend expansion for new matrix rules"
        for values in itertools.product(*matrix.values()):
            if "name" in job:
                name = job["name"]
                for key, value in zip(matrix, values):
                    name = re.sub(
                        r"\$\{\{\s*matrix[.]" + re.escape(key) + r"\s*\}\}",
                        str(value), name,
                    )
                assert "${{" not in name, "unresolved job name expression"
            else:
                name = job_id + (" (" + ", ".join(map(str, values)) + ")" if values else "")
            names.append(name)
    assert len(names) == len(set(names))
    assert set(required) == set(names)
    # PyYAML's YAML 1.1 loader calls unquoted `on` True.
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["push"]["branches"] == ["main", "qualify/**"]
    assert {"pull_request", "merge_group", "workflow_dispatch"} <= triggers.keys()


def test_factory_evidence_instructions_are_identical() -> None:
    agents = (ROOT / "AGENTS.md").read_text()
    claude = (ROOT / "CLAUDE.md").read_text()
    assert agents == claude
    assert "### Factory runs: targeted acceptance tests only" in agents
