"""Exercise the real shell entrypoint with owned git/gh executables, never CI."""
from __future__ import annotations

import ast
import itertools
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ci_verify.sh"
SHA = "1234567890abcdef" * 2 + "12345678"
REF = f"refs/heads/qualify/factory-{SHA}"
REQUIRED = (
    "pytest (3.11)", "pytest (3.12)", "pytest (3.13)",
    "pytest-macos (3.11)", "pytest-macos (3.12)", "pytest-macos (3.13)",
    "pytest-worker-uid", "pytest-enforced", "pytest-fresh-resolve",
    "skill-conformance", "frontend", "claude-code-extra-smoke", "package",
    "chainlink-cli-audit", "worklink-image-identity",
)

FAKE_CLI = r'''
import json
import os
from pathlib import Path
import sys
import time

root = Path(os.environ["FAKE_ROOT"])
config = json.loads((root / "config.json").read_text())
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as stream:
    stream.write(json.dumps([tool, *args]) + "\n")
if tool == "git":
    if args == ["rev-parse", "HEAD"]:
        print(config["sha"])
    elif args == ["remote", "get-url", "--push", "origin"]:
        if config.get("remote_error"):
            sys.exit(128)
        print(config["push_url"])
    elif args == ["remote", "get-url", "origin"]:
        print("/local/factory/base.git")
    elif args[:2] == ["push", "origin"]:
        if args[2].startswith(":"):
            if config.get("cleanup_error"):
                sys.exit(128)
        else:
            if config.get("push_error"):
                sys.exit(128)
            if config.get("push_hang"):
                time.sleep(60)
    else:
        sys.exit("unexpected git arguments")
elif tool == "gh":
    assert args == [
        "api", f"repos/PushOwner/push-repo/commits/{config['sha']}/check-runs?filter=all&per_page=100",
        "--paginate", "--slurp",
    ], args
    if config.get("gh_error"):
        sys.exit(1)
    if config.get("gh_hang"):
        time.sleep(60)
    counter = root / "poll-count"
    index = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(index + 1))
    responses = config["responses"]
    print(json.dumps(responses[min(index, len(responses) - 1)]))
else:
    sys.exit("unexpected executable")
'''


def green():
    return [
        {"id": index + 1, "name": name, "head_sha": SHA,
         "status": "completed", "conclusion": "success"}
        for index, name in enumerate(REQUIRED)
    ]


@pytest.fixture
def verify(tmp_path: Path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    # PATH contains only fixture-owned CLIs and this checkout's interpreter.
    for name in ("git", "gh"):
        executable = bindir / name
        executable.write_text(f"#!{sys.executable}\n" + FAKE_CLI)
        executable.chmod(0o755)
    (bindir / "python3").symlink_to(sys.executable)
    shell = shutil.which("sh")
    assert shell

    def run(checks=None, *, responses=None, timeout="3", startup="1",
            poll="0.01", script=SCRIPT, **config):
        (tmp_path / "calls.jsonl").write_text("")
        (tmp_path / "poll-count").unlink(missing_ok=True)
        (tmp_path / "config.json").write_text(json.dumps({
            "sha": SHA, "push_url": "git@github.com:PushOwner/push-repo.git",
            "responses": responses if responses is not None else [
                [{"check_runs": green() if checks is None else checks}],
            ],
            **config,
        }))
        env = dict(os.environ, PATH=str(bindir), FAKE_ROOT=str(tmp_path),
                   CI_VERIFY_TIMEOUT_SECONDS=timeout,
                   CI_VERIFY_START_TIMEOUT_SECONDS=startup,
                   CI_VERIFY_POLL_SECONDS=poll,
                   # Prove even an inherited shared lease is not acquired.
                   MIMIR_HEAVY_LOCK_FILE=str(tmp_path / "must-not-take-lease"))
        with subprocess.Popen(
            [shell, str(script)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        ) as child:
            try:
                stdout, stderr = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                # Deadline mutations must not leave a polling verifier behind.
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()
                pytest.fail("ci_verify did not honor its own short deadline")
            result = subprocess.CompletedProcess(child.args, child.returncode, stdout, stderr)
        calls = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text().splitlines()]
        assert calls[0] == ["git", "rev-parse", "HEAD"]
        assert calls[-1] == ["git", "push", "origin", f":{REF}"]
        assert not list(tmp_path.glob("must-not-take-lease*"))
        return result, calls

    return run


def test_all_required_green_and_exact_sha_refspec(verify):
    result, calls = verify()
    assert result.returncode == 0, result.stderr
    assert ["git", "push", "origin", f"{SHA}:{REF}"] in calls
    assert calls[1] == ["git", "remote", "get-url", "--push", "origin"]
    assert all(f"repos/PushOwner/push-repo/commits/{SHA}/check-runs?" in call[2]
               for call in calls if call[0] == "gh")


@pytest.mark.parametrize("conclusion", ["failure", "timed_out"])
def test_failed_or_test_timed_out_is_failure_and_cleans_up(verify, conclusion):
    checks = green()
    checks[7]["conclusion"] = conclusion
    result, _ = verify(checks)
    assert result.returncode == 1, result.stderr


@pytest.mark.parametrize("conclusion", ["cancelled", "neutral", "skipped", "action_required", "stale"])
def test_infrastructure_or_non_success_is_unavailable(verify, conclusion):
    checks = green()
    checks[0]["conclusion"] = conclusion
    result, _ = verify(checks)
    assert result.returncode == 75, result.stderr


@pytest.mark.parametrize("mode", ["missing", "renamed", "unrelated"])
def test_missing_required_name_never_passes_and_deadline_cleans_up(verify, mode):
    checks = green()
    if mode == "missing":
        checks.pop()
    elif mode == "renamed":
        checks[-1]["name"] = "renamed-image-check"
    else:
        for index, check in enumerate(checks):
            check["name"] = f"unrelated-{index}"
    result, calls = verify(checks, timeout="0.4", startup="2")
    assert result.returncode == 75, result.stderr
    assert "deadline reached" in result.stderr or "timed out" in result.stderr
    assert sum(call[0] == "gh" for call in calls) >= 2


def test_nothing_started_has_separate_bounded_wait(verify):
    result, _ = verify([], timeout="5", startup="0.3")
    assert result.returncode == 75
    assert "startup wait" in result.stderr


@pytest.mark.parametrize("config", [
    {"gh_error": True}, {"gh_hang": True}, {"push_error": True}, {"push_hang": True},
    {"remote_error": True},
])
def test_cli_errors_and_hangs_are_unavailable_and_cleaned_up(verify, config):
    result, _ = verify(timeout="0.4", **config)
    assert result.returncode == 75, result.stderr


@pytest.mark.parametrize("new_conclusion,expected", [("success", 0), ("failure", 1)])
@pytest.mark.parametrize("new_first", [True, False])
def test_only_newest_run_per_name_counts(verify, new_conclusion, expected, new_first):
    checks = green()
    older = dict(checks[0], conclusion="failure" if expected == 0 else "success")
    newer = dict(older, id=1000, conclusion=new_conclusion)
    # Also cross page boundaries so neither API order nor a single page suffices.
    pages = [{"check_runs": [newer if new_first else older, *checks[1:]]},
             {"check_runs": [older if new_first else newer]}]
    result, _ = verify(responses=[pages])
    assert result.returncode == expected, result.stderr


def test_newest_pending_run_does_not_inherit_older_success(verify):
    checks = green()
    checks.append(dict(checks[0], id=1000, status="in_progress", conclusion=None))
    result, _ = verify(checks, timeout="0.3")
    assert result.returncode == 75


def test_waits_for_required_checks_to_finish(verify):
    pending = [dict(check, status="queued", conclusion=None) for check in green()]
    result, calls = verify(responses=[
        [{"check_runs": []}], [{"check_runs": pending}], [{"check_runs": green()}],
    ])
    assert result.returncode == 0, result.stderr
    assert sum(call[0] == "gh" for call in calls) == 3


@pytest.mark.parametrize("push_url", [
    "https://github.com/PushOwner/push-repo.git",
    "https://github.com/PushOwner/push-repo",
    "ssh://git@github.com/PushOwner/push-repo.git",
    "git@github.com:PushOwner/push-repo.git",
])
def test_repo_is_derived_from_push_url_not_local_fetch_url(verify, push_url):
    result, calls = verify(push_url=push_url)
    assert result.returncode == 0, result.stderr
    assert ["git", "remote", "get-url", "origin"] not in calls


@pytest.mark.parametrize("push_url", [
    "/local/base.git", "https://other.example/PushOwner/push-repo.git",
    "https://github.com/only-owner", "https://github.com/../push-repo.git", "",
])
def test_unknown_push_repo_fails_closed(verify, push_url):
    result, calls = verify(push_url=push_url)
    assert result.returncode == 75, result.stderr
    assert not any(call[0] == "gh" for call in calls)
    assert not any(call[:3] == ["git", "push", "origin"] and not call[3].startswith(":")
                   for call in calls)


def test_checks_for_another_sha_are_not_evidence(verify):
    checks = [dict(check, head_sha="a" * 40) for check in green()]
    result, _ = verify(checks)
    assert result.returncode == 75, result.stderr


@pytest.mark.parametrize("pages", [None, [{}], [{"check_runs": [{"name": REQUIRED[0]}]}]])
def test_malformed_github_response_is_unavailable(verify, pages):
    result, _ = verify(responses=[pages])
    assert result.returncode == 75, result.stderr


@pytest.mark.parametrize("expected", [0, 1, 75])
def test_cleanup_failure_never_overwrites_result(verify, expected):
    checks = green()
    if expected == 1:
        checks[0]["conclusion"] = "failure"
    elif expected == 75:
        checks.pop()
    result, _ = verify(checks, cleanup_error=True, timeout="0.3")
    assert result.returncode == expected, result.stderr


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf", "10740", "10800", "invalid"])
def test_invalid_or_factory_length_deadline_is_unavailable(verify, timeout):
    result, _ = verify(timeout=timeout)
    assert result.returncode == 75


def script_required_names():
    match = re.search(r"^REQUIRED_CHECKS = (\([\s\S]*?\n\))", SCRIPT.read_text(), re.MULTILINE)
    assert match
    return ast.literal_eval(match[1])


def workflow_check_names(workflow):
    """GitHub defaults to job id + matrix values unless a job sets its name."""
    names = []
    for job_id, job in workflow["jobs"].items():
        matrix = job.get("strategy", {}).get("matrix", {})
        assert not ({"include", "exclude"} & matrix.keys()), "extend matrix expansion for new strategy"
        for values in itertools.product(*matrix.values()):
            if "name" in job:
                name = job["name"]
                for key, value in zip(matrix, values):
                    name = re.sub(r"\$\{\{\s*matrix\." + re.escape(key) + r"\s*\}\}", str(value), name)
                assert "${{" not in name, "unresolved job name expression"
            else:
                name = job_id + (" (" + ", ".join(map(str, values)) + ")" if values else "")
            names.append(name)
    return names


def test_required_names_equal_workflow_job_names_expanded_by_matrix():
    workflow = yaml.load((ROOT / ".github/workflows/tests.yml").read_text(), Loader=yaml.BaseLoader)
    names = workflow_check_names(workflow)
    assert len(names) == len(set(names)) == 15
    assert len(script_required_names()) == 15
    assert set(names) == set(script_required_names()) == set(REQUIRED)
    assert workflow["on"]["push"]["branches"] == ["main", "qualify/**"]
    assert set(workflow["on"]) == {"push", "pull_request", "merge_group", "workflow_dispatch"}


def test_factory_config_and_identical_targeted_test_instructions():
    config = json.loads((ROOT / ".factory.json").read_text())
    assert config["verify"] == "sh scripts/ci_verify.sh"
    assert config["verify_unavailable_exit"] == 75
    assert config["verify_scope"] == "integration"
    assert config["verify_timeout_ms"] == 10800000
    assert config["bootstrap"] == "uv sync --locked --extra dev --extra bench"
    assert config["max_retries"] == 5
    assert config["pr_draft"] is False
    instructions = (ROOT / "AGENTS.md").read_text()
    assert instructions == (ROOT / "CLAUDE.md").read_text()
    assert "### Factory runs: targeted acceptance tests only" in instructions
