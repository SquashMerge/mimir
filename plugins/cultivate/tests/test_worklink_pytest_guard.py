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
MODULE_ENTRIES = [
    [sys.executable, "-m", "pytest"],
    [sys.executable, "-mpytest"],
    [sys.executable, "-X", "dev", "-m", "pytest"],
    [sys.executable, "-X", "dev", "-mpytest"],
    [sys.executable, "-Bm", "pytest"],
    [sys.executable, "-Bmpytest"],
    [sys.executable, "-bbm", "pytest"],
    [sys.executable, "-OOm", "pytest"],
    [sys.executable, "-BWignore", "-m", "pytest"],
    [sys.executable, "-BX", "dev", "-Bmpytest"],
]


@pytest.fixture
def session(tmp_path, monkeypatch):
    guard = tmp_path / "executor/pytest-guard"
    (guard / "bin").mkdir(parents=True)
    (guard / "python").mkdir()
    for name in ("worklink_pytest.py", "worklink_pytest_args.py", "sitecustomize.py"):
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


@pytest.mark.parametrize("entry", ["shim", "venv", *MODULE_ENTRIES])
def test_multitest_requires_medium_lease(session, entry):
    """Mutation oracle: deleting enforce's exec must fail this test."""
    command = (["pytest"] if entry == "shim" else
               [str(session[0] / ".venv/bin/pytest")] if entry == "venv" else entry)
    observed = result(run(session, command))
    assert observed["held"] == "1"
    assert observed["tier"] == "medium"
    assert len(observed["owners"]) == 1
    assert observed["owners"][0]["tier"] == "medium"
    assert observed["args"] == ["-q", "tests"]


@pytest.mark.parametrize("entry", [["pytest"], *MODULE_ENTRIES])
def test_single_node_and_held_marker_pass_through(session, entry):
    observed = result(run(session, entry, arguments=("test_example.py::test_one",)))
    assert observed["held"] is None
    assert observed["owners"] == []
    observed = result(run(session, entry, MIMIR_HEAVY_LOCK_HELD="1"))
    assert observed["held"] == "1"
    assert observed["owners"] == []


@pytest.mark.parametrize("entry", MODULE_ENTRIES)
def test_multiple_module_nodes_run_without_lease(session, entry):
    arguments = ("a.py::test_a", "b.py::test_b")
    observed = result(run(session, entry, arguments=arguments))
    assert observed["held"] is None
    assert observed["owners"] == []
    assert observed["args"] == list(arguments)


@pytest.mark.parametrize("original, expected", [
    (["python", "-m", "pytest", "tests"], ["tests"]),
    (("python", "-mpytest", "tests"), ["tests"]),
    (["python", "-X", "dev", "-mpytest", "--version"], ["--version"]),
    (["python", "-W", "-mpytest", "-m", "other"], None),
    (["python", "-c", "-mpytest"], None),
    (["python", "-m", "other", "-mpytest"], None),
    (["python", "script.py", "-mpytest"], None),
    (["python", "-m"], None),
    (["python", "-mpytest_other"], None),
    (["python", "-Bm", "pytest", "tests"], ["tests"]),
    (["python", "-Bmpytest", "tests"], ["tests"]),
    (["python", "-bbm", "pytest", "tests"], ["tests"]),
    (["python", "-OOm", "pytest", "tests"], ["tests"]),
    (["python", "-BWignore", "-Bmpytest", "tests"], ["tests"]),
    (["python", "-BX", "dev", "-m", "pytest", "tests"], ["tests"]),
    (["python", "--check-hash-based-pycs", "always", "-Bmpytest", "tests"], ["tests"]),
    (["python", "-BW", "-mpytest", "-m", "other"], None),
    (["python", "-BX", "-mpytest", "-m", "other"], None),
    (["python", "-BWmpytest"], None),
    (["python", "-BXmpytest"], None),
    (["python", "-Bc", "print(1)", "-mpytest"], None),
    (["python", "-Bcprint(1)", "-mpytest"], None),
    (["python", "-Bm", "other", "-mpytest"], None),
    (["python", "-Bmpytest_other"], None),
    (["python", "-Bm"], None),
    (["python", "-BW"], None),
    (["python", "-BX"], None),
    (["python", "--", "-mpytest"], None),
    (["python", "-", "-mpytest"], None),
    (["python", "--check-hash-based-pycs", "-mpytest"], None),
])
def test_shared_module_parser(monkeypatch, original, expected):
    monkeypatch.syspath_prepend(str(SOURCE))
    from worklink_pytest_args import pytest_arguments

    assert pytest_arguments(["-m"], original) == expected


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


@pytest.mark.parametrize("entry", MODULE_ENTRIES)
def test_real_pytest_whole_suite_collects_and_runs_only_under_lease(session, entry):
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
    completed = run(session, entry,
                    arguments=("-q",), PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
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


@pytest.mark.parametrize("entry", MODULE_ENTRIES)
def test_startup_exception_cannot_continue_to_tests(session, entry):
    (session[1] / "python/worklink_pytest.py").write_text("raise RuntimeError('broken guard')\n")
    completed = run(session, entry)
    assert completed.returncode == 125
    assert "startup guard failed" in completed.stderr
    assert completed.stdout == ""


@pytest.mark.parametrize("option", ["-c", "-Bc"])
def test_startup_exception_allows_unrelated_python(session, option):
    """Mutation oracle: an unconditional os._exit(125) must fail this test."""
    (session[1] / "python/worklink_pytest.py").write_text("raise RuntimeError('broken guard')\n")
    completed = run(session, [sys.executable, option, "print('unrelated tool ran')"], arguments=())
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert completed.stdout == "unrelated tool ran\n"
    assert "startup guard failed: broken guard" in completed.stderr


def break_parser(session, failure):
    parser = session[1] / "python/worklink_pytest_args.py"
    if failure == "missing":
        parser.unlink()
    else:
        parser.write_text("def broken(:\n")


@pytest.mark.parametrize("failure", ["missing", "syntax"])
@pytest.mark.parametrize("entry", ["shim", "venv", *MODULE_ENTRIES])
def test_broken_parser_cannot_continue_to_tests(session, failure, entry):
    # Never import the parser into the parent: the real startup hook must
    # enforce before the child reaches even a controlled pytest stand-in.
    break_parser(session, failure)
    command = (["pytest"] if entry == "shim" else
               [str(session[0] / ".venv/bin/pytest")] if entry == "venv" else entry)
    completed = run(session, command)
    assert completed.returncode == 125, (completed.stdout, completed.stderr)
    assert "startup guard failed" in completed.stderr
    assert completed.stdout == ""


@pytest.mark.parametrize("failure", ["missing", "syntax"])
@pytest.mark.parametrize("option", ["-c", "-Bc"])
def test_broken_parser_allows_unrelated_python(session, failure, option):
    break_parser(session, failure)
    completed = run(session, [sys.executable, option, "print('unrelated tool ran')"], arguments=())
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert completed.stdout == "unrelated tool ran\n"
    assert "startup guard failed" in completed.stderr


@pytest.mark.parametrize("option", ["-c", "-Bc"])
def test_command_mode_does_not_take_lease(session, option):
    completed = run(session, [sys.executable, option,
                             "import os; print(os.environ.get('MIMIR_HEAVY_LOCK_HELD'))"],
                    arguments=("-mpytest",))
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert completed.stdout == "None\n"
    assert "taking medium heavy-run lease" not in completed.stderr


@pytest.mark.parametrize("argv, original", [
    (None, None), ([], 42), (["tool.py"], ["python", "-m"]),
    (["tool.py"], ["python", "-c", "pytest"]),
])
def test_unusual_interpreter_startup_does_not_kill_python(session, argv, original):
    hook = session[1] / "python/sitecustomize.py"
    # Start normally, then reproduce an embedded interpreter's missing/odd
    # attributes while executing the real hook in an isolated process.
    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('embedded_hook', {str(hook)!r})\n"
        "module = importlib.util.module_from_spec(spec)\n"
        + ("del sys.argv\n" if argv is None else f"sys.argv = {argv!r}\n")
        + ("del sys.orig_argv\n" if original is None else f"sys.orig_argv = {original!r}\n")
        + "spec.loader.exec_module(module)\nprint('embedded tool ran')\n"
    )
    completed = run(session, [sys.executable, "-c", code], arguments=())
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert completed.stdout == "embedded tool ran\n"


@pytest.mark.parametrize("entry", ["shim", "venv"])
def test_startup_exception_blocks_console_pytest(session, entry):
    (session[1] / "python/worklink_pytest.py").write_text("raise RuntimeError('broken guard')\n")
    command = ["pytest"] if entry == "shim" else [str(session[0] / ".venv/bin/pytest")]
    completed = run(session, command)
    assert completed.returncode == 125
    assert "startup guard failed" in completed.stderr
    assert completed.stdout == ""


@pytest.mark.parametrize("broken_guard", [False, True])
def test_venv_sitecustomize_is_chained(session, broken_guard):
    repo, guard, env = session
    uv = shutil.which("uv")
    assert uv, "uv is required for this repository's acceptance tests"
    subprocess.run([uv, "venv", "--offline", "--python", sys.executable,
                    "--allow-existing", ".venv"], cwd=repo, env=env,
                   check=True, capture_output=True, timeout=20)
    python = str(repo / ".venv/bin/python")
    paths = run(session, [python, "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
                arguments=())
    assert paths.returncode == 0, paths.stderr
    site_packages = Path(paths.stdout.strip())
    (site_packages / "sitecustomize.py").write_text(textwrap.dedent("""
        import os, sitecustomize, sys
        assert sitecustomize.__file__ == __file__
        assert os.environ['PYTHONPATH'] not in sys.path
        os.environ['PROJECT_STARTUP_RAN'] = 'yes'
    """))
    if broken_guard:
        (guard / "python/worklink_pytest.py").write_text("raise RuntimeError('broken guard')\n")
    completed = run(session, [python, "-c", textwrap.dedent("""
        import os, sys
        assert os.environ['PROJECT_STARTUP_RAN'] == 'yes'
        assert os.environ['PYTHONPATH'] in sys.path
        print('project hook ran')
    """)], arguments=())
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert completed.stdout == "project hook ran\n"
    assert ("startup guard failed" in completed.stderr) == broken_guard


def test_focused_detection_is_conservative(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(SOURCE))
    spec = importlib.util.spec_from_file_location("guard_under_test", SOURCE / "worklink_pytest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    monkeypatch.chdir(tmp_path)
    assert module.focused_run(["-q", "test_x.py::test_one"])
    assert module.focused_run(["test_x.py"])
    assert module.focused_run(["a.py::test_a", "b.py::test_b"])
    assert module.focused_run(["-k", "lease"])
    assert module.focused_run(["-q", "test_x.py", "--junitxml=report.xml", "-o", "junit_family=xunit2"])
    for args in ([], ["tests"], ["-k", ""], ["-k"], ["-o", "addopts=-n 6", "a.py"],
                  ["-n", "6", "a.py::test_a"], ["--pyargs", "a.py::test_a"],
                  ["--count=2", "a.py"], ["--repeat", "2", "a.py"],
                  ["test_folder"], ["--option.py::test_a"], ["a.py"] * 6):
        assert not module.focused_run(args)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n 6")
    assert not module.focused_run(["a.py::test_a"])


@pytest.mark.parametrize("entry", [["pytest"], *MODULE_ENTRIES])
def test_focused_files_and_k_run_directly_while_lease_is_occupied(session, entry):
    base = Path(session[2]["MIMIR_HEAVY_LOCK_FILE"])
    # A live-looking large holder must not delay focused serial iterations.
    for suffix in (".lease", ".lease2"):
        lease = Path(str(base) + suffix)
        lease.mkdir()
        (lease / "owner").write_text(json.dumps({"tier": "large"}))
        (lease / "heartbeat").write_text("0")
    for arguments in (("-q", "a.py", "b.py"), ("-k", "lease")):
        observed = result(run(session, entry, arguments=arguments))
        assert observed["held"] is None
        assert observed["args"] == list(arguments)


@pytest.mark.parametrize("config,content", [
    ("pyproject.toml", '[tool.pytest.ini_options]\naddopts = "-n 6"\n'),
    ("pytest.ini", "[pytest]\naddopts = -n 6\n"),
    ("setup.cfg", "[tool:pytest]\naddopts = --count=2\n"),
])
def test_configured_heavy_options_still_lease_focused_files(session, config, content):
    (session[0] / config).write_text(content)
    assert result(run(session, ["pytest"], arguments=("a.py",)))["held"] == "1"
    assert result(run(session, ["pytest"], arguments=("-o", "addopts=", "a.py")))["held"] is None


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
