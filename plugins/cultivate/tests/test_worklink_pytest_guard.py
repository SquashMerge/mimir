"""Exercise the image-resident guard with real child processes and real leases."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import selectors
import subprocess
import sys
import textwrap

import pytest

from mimir_cultivate.worklink import pytest_environment
from mimir_cultivate.worklink.daemon_sessions import session_environment

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "plugins/cultivate/mimir_cultivate/docker/pytest_guard"


@pytest.fixture
def session(tmp_path, monkeypatch):
    guard = tmp_path / "executor/pytest-guard"
    (guard / "bin").mkdir(parents=True)
    (guard / "python").mkdir()
    for name in ("worklink_pytest.py", "sitecustomize.py"):
        shutil.copyfile(SOURCE / name, guard / "python" / name)
    shutil.copyfile(SOURCE / "pytest", guard / "bin/pytest")
    (guard / "bin/pytest").chmod(0o755)
    for name in ("heavy_run.sh", "heavy_run.py"):
        shutil.copyfile(ROOT / "scripts" / name, guard / name)
    monkeypatch.setattr(pytest_environment, "PYTEST_GUARD_ROOT", str(guard))
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", str(tmp_path / "heavy.lock"))
    monkeypatch.delenv("MIMIR_HEAVY_TIER", raising=False)
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_TIMEOUT", raising=False)
    repo = tmp_path / "checkout"
    repo.mkdir()
    # A controlled pytest stand-in observes the lease at actual command entry.
    # It is deliberately also a module, exercising python -m's startup path.
    probe = repo / "pytest.py"
    probe.write_text(textwrap.dedent("""
        import json, os, signal, sys
        from pathlib import Path
        def main():
            base = os.environ.get("MIMIR_HEAVY_LOCK_FILE", "")
            owners = []
            for suffix in (".lease", ".lease2"):
                owner = Path(base + suffix) / "owner"
                if owner.is_file():
                    owners.append(json.loads(owner.read_text()))
            print(json.dumps({"held": os.environ.get("MIMIR_HEAVY_LOCK_HELD"),
                              "tier": os.environ.get("MIMIR_HEAVY_TIER"),
                              "owners": owners, "args": sys.argv[1:]}), flush=True)
            print("pytest stderr preserved", file=sys.stderr, flush=True)
            if os.environ.get("PROBE_WAIT"):
                signal.pause()
            if os.environ.get("PROBE_SIGNAL"):
                os.kill(os.getpid(), int(os.environ["PROBE_SIGNAL"]))
            sys.exit(int(os.environ.get("PROBE_EXIT", "0")))
        if __name__ == "__main__":
            main()
    """))
    (repo / ".venv/bin").mkdir(parents=True)
    entry = repo / ".venv/bin/pytest"
    entry.write_text(
        f"#!{sys.executable}\nimport runpy\nrunpy.run_path({str(probe)!r}, run_name='__main__')\n"
    )
    entry.chmod(0o755)
    env = session_environment({}, home=tmp_path, git_settings=(), credentials={})
    return repo, guard, env


def run(session, command, arguments=("-q", "tests"), **updates):
    repo, _, env = session
    return subprocess.run(
        [*command, *arguments], cwd=repo, env={**env, **updates},
        text=True, capture_output=True, timeout=20,
    )


def result(completed):
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert "pytest stderr preserved" in completed.stderr
    return json.loads(completed.stdout)


def test_builder_path_resolves_executor_shim(session):
    _, guard, env = session
    assert shutil.which("pytest", path=env["PATH"]) == str(guard / "bin/pytest")
    assert not guard.is_relative_to(session[0])


@pytest.mark.parametrize("entry", ["shim", "module", "venv"])
def test_multitest_requires_medium_lease(session, entry):
    """Mutation oracle: deleting enforce's exec must fail this test."""
    command = {
        "shim": ["pytest"],
        "module": [sys.executable, "-m", "pytest"],
        "venv": [str(session[0] / ".venv/bin/pytest")],
    }[entry]
    observed = result(run(session, command))
    assert observed["held"] == "1"
    assert observed["tier"] == "medium"
    assert len(observed["owners"]) == 1
    assert observed["owners"][0]["tier"] == "medium"
    assert observed["args"] == ["-q", "tests"]


@pytest.mark.parametrize("entry", [["pytest"], [sys.executable, "-m", "pytest"]])
def test_single_node_and_held_marker_pass_through(session, entry):
    observed = result(run(session, entry, arguments=("test_example.py::test_one",)))
    assert observed["held"] is None
    assert observed["owners"] == []
    observed = result(run(session, entry, MIMIR_HEAVY_LOCK_HELD="1"))
    assert observed["held"] == "1"
    assert observed["owners"] == []


def test_unconfigured_lease_passes_through(session):
    observed = result(run(session, ["pytest"], MIMIR_HEAVY_LOCK_FILE=""))
    assert observed["held"] is None
    assert observed["owners"] == []


def test_uv_run_venv_precedence_still_takes_lease(session):
    uv = shutil.which("uv")
    assert uv, "uv is required for this repository's acceptance tests"
    repo, _, env = session
    # uv requires a real venv, but no downloads/dependencies are needed: its
    # pytest entry point is the controlled probe installed by this fixture.
    subprocess.run([uv, "venv", "--offline", "--python", sys.executable, "--allow-existing", ".venv"],
                   cwd=repo, env=env, check=True, capture_output=True, timeout=20)
    (repo / "pyproject.toml").write_text(
        '[project]\nname="lease-probe"\nversion="0.0.0"\nrequires-python=">=3.11"\n'
    )
    observed = result(run(session, [uv, "run", "--offline", "--no-sync", "pytest"]))
    assert observed["held"] == "1"
    assert observed["owners"][0]["tier"] == "medium"


def test_real_pytest_collects_and_runs_only_under_lease(session):
    repo = session[0]
    (repo / "pytest.py").unlink()
    (repo / "test_lease.py").write_text(textwrap.dedent("""
        import os
        def test_first():
            assert os.environ["MIMIR_HEAVY_LOCK_HELD"] == "1"
            assert os.environ["MIMIR_HEAVY_TIER"] == "medium"
        def test_second():
            assert os.environ["MIMIR_HEAVY_LOCK_HELD"] == "1"
    """))
    completed = run(session, [sys.executable, "-m", "pytest"],
                    arguments=("-q", "test_lease.py"), PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert "2 passed" in completed.stdout


@pytest.mark.parametrize("status", [1, 5, 7])
def test_test_exit_and_streams_are_preserved(session, status):
    completed = run(session, ["pytest"], PROBE_EXIT=str(status))
    assert completed.returncode == status
    assert json.loads(completed.stdout)["held"] == "1"
    assert "pytest stderr preserved" in completed.stderr


def test_test_signal_status_is_preserved(session):
    completed = run(session, ["pytest"], PROBE_SIGNAL=str(signal.SIGTERM))
    assert completed.returncode == 128 + signal.SIGTERM
    assert json.loads(completed.stdout)["held"] == "1"
    assert not Path(session[2]["MIMIR_HEAVY_LOCK_FILE"] + ".lease").exists()


def test_termination_reaches_helper_and_releases_lease(session):
    repo, _, env = session
    child = subprocess.Popen(["pytest", "-q", "tests"], cwd=repo,
                             env={**env, "PROBE_WAIT": "1"}, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True)
    try:
        with selectors.DefaultSelector() as ready:
            ready.register(child.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=10), "leased test did not start"
        observed = json.loads(child.stdout.readline())
        assert observed["held"] == "1"
        child.send_signal(signal.SIGTERM)
        child.communicate(timeout=10)
        assert child.returncode == 128 + signal.SIGTERM
        assert not Path(env["MIMIR_HEAVY_LOCK_FILE"] + ".lease").exists()
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)


def test_queue_timeout_is_distinct_and_does_not_start_pytest(session):
    base = session[2]["MIMIR_HEAVY_LOCK_FILE"]
    # A live-looking large holder blocks both slots; no process needs sleeping.
    for suffix in (".lease", ".lease2"):
        slot = Path(base + suffix)
        slot.mkdir()
        (slot / "owner").write_text('{"tier": "large"}')
        (slot / "heartbeat").write_text("1")
    completed = run(session, ["pytest"], MIMIR_HEAVY_LOCK_TIMEOUT="0.3")
    assert completed.returncode == 124
    assert completed.stdout == ""
    assert "queue/run timeout exits 124" in completed.stderr
    assert not list(Path(base + ".queue").iterdir())


def test_broken_guard_fails_closed(session):
    (session[1] / "heavy_run.sh").unlink()
    completed = run(session, ["pytest"])
    assert completed.returncode != 0
    assert completed.stdout == ""


def test_startup_exception_cannot_continue_to_tests(session):
    (session[1] / "python/worklink_pytest.py").write_text("raise RuntimeError('broken guard')\n")
    completed = run(session, [sys.executable, "-m", "pytest"])
    assert completed.returncode == 125
    assert "startup guard failed" in completed.stderr
    assert completed.stdout == ""


def test_single_node_detection_is_conservative(monkeypatch):
    spec = importlib.util.spec_from_file_location("guard_under_test", SOURCE / "worklink_pytest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    assert module.single_node(["-q", "test_x.py::test_one"])
    for args in ([], ["tests"], ["test_x.py"], ["a.py::test_a", "b.py::test_b"],
                 ["-n", "6", "a.py::test_a"], ["--pyargs", "a.py::test_a"],
                 ["-k", "a.py::test_a"], ["a.py::"], ["a.py::TestClass"],
                 ["test_folder"], ["--option.py::test_a"]):
        assert not module.single_node(args)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n 6")
    assert not module.single_node(["a.py::test_a"])


def test_claude_leaf_environment_installs_guard(monkeypatch, tmp_path):
    from mimir_cultivate.worklink.backends.base import WorkOrder
    from mimir_cultivate.worklink.backends.claude_code import ClaudeCodeBackend
    from mimir_cultivate.worklink.compute import _enabled_child_env

    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", str(tmp_path / "heavy.lock"))
    order = WorkOrder(2197, tmp_path, "build", None, 30,
                      {"CLAUDE_CODE_OAUTH_TOKEN": "test-token"}, tmp_path)
    spec = ClaudeCodeBackend().work_spec(order, attempt=1, repo_url="repo",
                                        base_ref="main", branch="issue/2197",
                                        test_command="uv run pytest")
    env = _enabled_child_env(spec, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert env["PATH"].split(os.pathsep)[0] == "/opt/mimir-worklink/pytest-guard/bin"
    assert env["PYTHONPATH"] == "/opt/mimir-worklink/pytest-guard/python"
    assert env["MIMIR_HEAVY_LOCK_FILE"] == str(tmp_path / "heavy.lock")
    assert "MIMIR_HEAVY_LOCK_HELD" not in env
