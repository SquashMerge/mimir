"""The operator's shared heavy-run lease path reaches every Worklink command.

scripts/heavy_run.sh serializes heavy commands across the host and its
containers. Worklink constructs worker environments from scratch and denies
MIMIR_* names, so the controller's lease path and timeout are carried explicitly.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import shlex
import sys
from textwrap import dedent

import pytest
import yaml

from mimir_cultivate import setup as installer
from mimir_cultivate.contained_execution import base_worker_environment, heavy_lock_environment
from mimir_cultivate.worklink import compute
from mimir_cultivate.worklink import daemon_service as service
from mimir_cultivate.worklink.identities import WorklinkIdentities

LOCK = "/mimir-heavy-locks/heavy.lock"
TIMEOUT = "14400"
IDENTIFIER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
INVALID = (
    "heavy.lock",                       # relative
    "/locks/../heavy.lock",             # not normalized
    "/locks//heavy.lock",
    "/locks/heavy.lock/",
    "/home/mimir",                      # controller home
    "/home/mimir/agent/heavy.lock",
    "/locks/heavy lock",                # whitespace
    "/locks/heavy\nlock",
    "/locks/heavy:lock",
)


@pytest.mark.parametrize(
    ("lease", "invalid"),
    [
        ({}, None),
        ({"MIMIR_HEAVY_LOCK_FILE": LOCK}, None),
        ({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": TIMEOUT,
          "MIMIR_HEAVY_TIER": "medium"}, None),
        ({"MIMIR_HEAVY_LOCK_FILE": "relative.lock"}, "MIMIR_HEAVY_LOCK_FILE"),
        ({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": "0"},
         "MIMIR_HEAVY_LOCK_TIMEOUT"),
        ({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_TIER": "small"}, "MIMIR_HEAVY_TIER"),
    ],
    ids=["unset", "lock-only", "full-lease", "bad-path", "bad-timeout", "bad-tier"],
)
async def test_ready_queue_dispatch_carries_lease_to_gate_and_builder(
    tmp_path, monkeypatch, lease, invalid,
):
    """Real poller scrub -> shipped dispatch -> detached run -> worker envs.

    Substitute only issue selection and the CLI probe, not environment assembly
    or the detached launcher. No live queue, worker daemon or deployment needed.
    """
    from mimir.event_logger import init_logger
    from mimir.plugins.manager import load_plugins
    from mimir.pollers import discover_pollers, run_poller

    root = Path(__file__).resolve().parents[3]
    skill = root / "plugins/cultivate/mimir_cultivate/defaults/skills/chainlink-orchestrator"
    home = tmp_path / "home"
    home.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    result_path = tmp_path / "result.json"
    init_logger(home / "events.jsonl", session_id="heavy-lock-dispatch")
    monkeypatch.setattr(os, "environ", {
        "PATH": os.environ.get("PATH", ""), "HOME": str(home),
        "WORKLINK_REPO": str(repo), "MIMIR_PLUGINS": "cultivate",
        "MIMIR_API_KEY": "controller-secret", **lease,
    })
    [cfg] = discover_pollers(
        skill, state_root=home / "state" / "pollers",
        policy=load_plugins(enable="cultivate").policy, plugin_candidates=(),
    )
    driver = tmp_path / "dispatch.py"
    driver.write_text(dedent(f"""
        import importlib.util, json, os, subprocess, sys
        from pathlib import Path
        sys.path.insert(0, {str(root)!r})
        def lease_only(env):
            return {{k: v for k, v in env.items() if k.startswith("MIMIR_HEAVY_")}}
        if "--probe" in sys.argv:
            from mimir_cultivate.contained_execution import base_worker_environment
            from mimir_cultivate.worklink.compute import WorkSpec, _enabled_child_env
            from mimir_cultivate.worklink.daemon_sessions import session_environment
            assert sys.argv[2:5] == ["worklink", "run", "2196"]
            assert "MIMIR_API_KEY" not in os.environ
            spec = WorkSpec(2196, 1, "repo", "main", "issue/2196", "prompt",
                            None, "true", "opencode", 30)
            result = {{"run": lease_only(os.environ)}}
            for name, build in [
                ("base", lambda: base_worker_environment({IDENTIFIER!r})),
                ("gate", lambda: _enabled_child_env(spec, {IDENTIFIER!r})),
                ("builder", lambda: session_environment(
                    {{}}, home=Path({str(home)!r}), git_settings=(), credentials={{}})),
            ]:
                try:
                    result[name] = lease_only(build())
                except ValueError as exc:
                    result[name] = {{"error": str(exc)}}
            Path({str(result_path)!r}).write_text(json.dumps(result))
        else:
            loader = importlib.util.spec_from_file_location(
                "queue_under_test", {str(skill / "scripts/poller.py")!r})
            queue = importlib.util.module_from_spec(loader)
            sys.modules[loader.name] = queue
            loader.loader.exec_module(queue)
            children = []
            real_popen = subprocess.Popen
            def tracked_popen(*args, **kwargs):
                child = real_popen(*args, **kwargs)
                children.append(child)
                return child
            queue.subprocess.Popen = tracked_popen
            assert queue._dispatch(
                item=queue.DispatchItem(2196, "leaf"),
                home=Path(os.environ["MIMIR_HOME"]), repo=os.environ["WORKLINK_REPO"],
                state_dir=Path(os.environ["STATE_DIR"]),
                run_bin=[sys.executable, __file__, "--probe"],
                active=0, leaf_cap=2, factory_cap=1)
            assert len(children) == 1
            try:
                assert children[0].wait(timeout=15) == 0
            finally:
                if children[0].poll() is None:
                    children[0].kill()
                    children[0].wait()
    """), encoding="utf-8")
    cfg = replace(
        cfg, command=f"{shlex.quote(sys.executable)} {shlex.quote(str(driver))}", hooks=None,
    )

    async def enqueue(_event):
        pytest.fail("the dispatch probe must not enqueue agent turns")

    assert await run_poller(cfg, enqueue=enqueue, home=home, timeout=30) == 0
    dispatch_log = cfg.resolved_persist_dir() / "run-2196.log"
    assert result_path.exists(), (
        (home / "events.jsonl").read_text(),
        dispatch_log.read_text() if dispatch_log.exists() else "no dispatch log",
    )
    result = json.loads(result_path.read_text())
    assert result["run"] == lease
    for name in ("base", "gate", "builder"):
        if invalid is not None:
            assert invalid in result[name]["error"]
        else:
            assert result[name] == heavy_lock_environment(lease)


def test_unset_or_empty_means_no_lease(monkeypatch):
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE", raising=False)
    assert heavy_lock_environment() == {}
    assert "MIMIR_HEAVY_LOCK_FILE" not in base_worker_environment(IDENTIFIER)
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": ""}) == {}


def test_valid_path_is_the_only_mimir_name_a_worker_receives(monkeypatch):
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", LOCK)
    monkeypatch.setenv("MIMIR_API_KEY", "controller-secret")
    base = base_worker_environment(IDENTIFIER)
    assert base["MIMIR_HEAVY_LOCK_FILE"] == LOCK
    assert {name for name in base if name.startswith("MIMIR_")} == {"MIMIR_HEAVY_LOCK_FILE"}
    # The neighbouring home path is not the controller home.
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": "/home/mimirx/heavy.lock"})


@pytest.mark.parametrize("value", ["60", "86400", TIMEOUT, "00060", "0" * 5000 + "60"],
                         ids=["minimum", "maximum", "setup-default", "leading-zeros", "many-leading-zeros"])
def test_heavy_lock_environment_forwards_valid_timeout(value):
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": value}) == {
        "MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": value,
    }


@pytest.mark.parametrize("value", ["0", "59", "86401", "999999", "9" * 5000, "", "-60", "+60", "60.0",
                                  " 60", "60 ", "60\n", "٦٠", "６０", "6０"],
                         ids=["zero", "below-minimum", "above-maximum", "six-digits", "huge", "empty", "negative",
                              "positive-sign", "decimal", "leading-space", "trailing-space", "newline",
                              "arabic-digits", "fullwidth-digits", "mixed-digits"])
def test_heavy_lock_timeout_fails_closed(value):
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_TIMEOUT"):
        heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": value})


@pytest.mark.parametrize("lock", [None, ""])
@pytest.mark.parametrize("timeout", [TIMEOUT, "0", ""])
def test_timeout_without_lock_file_is_not_forwarded(lock, timeout):
    env = {"MIMIR_HEAVY_LOCK_TIMEOUT": timeout}
    if lock is not None:
        env["MIMIR_HEAVY_LOCK_FILE"] = lock
    assert heavy_lock_environment(env) == {}


def test_unset_timeout_uses_heavy_run_default():
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": LOCK}) == {"MIMIR_HEAVY_LOCK_FILE": LOCK}


def test_contained_base_worker_environment_carries_timeout(monkeypatch):
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", LOCK)
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_TIMEOUT", TIMEOUT)
    monkeypatch.setenv("MIMIR_API_KEY", "controller-secret")
    env = base_worker_environment(IDENTIFIER)
    assert {name: value for name, value in env.items() if name.startswith("MIMIR_")} == {
        "MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_LOCK_TIMEOUT": TIMEOUT,
    }


@pytest.mark.parametrize("value", INVALID)
def test_malformed_or_controller_home_path_fails_closed(monkeypatch, value):
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_FILE"):
        heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": value})
    monkeypatch.setitem(os.environ, "MIMIR_HEAVY_LOCK_FILE", value)
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_FILE"):
        base_worker_environment(IDENTIFIER)


@pytest.mark.parametrize("tier", ["medium", "large"])
def test_heavy_lock_environment_forwards_a_valid_tier(tier):
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_TIER": tier}) == {
        "MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_TIER": tier,
    }


@pytest.mark.parametrize("tier", ["", "MEDIUM", "small", " medium", "medium\n"])
def test_heavy_lock_tier_fails_closed(tier):
    with pytest.raises(ValueError, match="MIMIR_HEAVY_TIER"):
        heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_TIER": tier})


def test_tier_without_lock_file_is_not_forwarded():
    assert heavy_lock_environment({"MIMIR_HEAVY_TIER": "medium"}) == {}
    assert heavy_lock_environment({"MIMIR_HEAVY_LOCK_FILE": "", "MIMIR_HEAVY_TIER": "bogus"}) == {}


def test_factory_and_local_children_carry_the_lease(monkeypatch):
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", LOCK)
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_TIMEOUT", TIMEOUT)
    monkeypatch.setenv("MIMIR_HEAVY_TIER", "medium")
    assert compute._local_child_env()["MIMIR_HEAVY_LOCK_FILE"] == LOCK
    assert compute._local_child_env()["MIMIR_HEAVY_LOCK_TIMEOUT"] == TIMEOUT
    assert compute._local_child_env()["MIMIR_HEAVY_TIER"] == "medium"
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE")
    assert "MIMIR_HEAVY_LOCK_FILE" not in compute._local_child_env()
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in compute._local_child_env()
    assert "MIMIR_HEAVY_TIER" not in compute._local_child_env()


def test_local_child_refuses_a_malformed_lease_as_a_launch_error(monkeypatch):
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", "relative.lock")
    with pytest.raises(compute.ComputeLaunchError, match="MIMIR_HEAVY_LOCK_FILE"):
        compute._local_child_env()


async def test_contained_launch_refuses_a_malformed_lease_before_the_worker_starts(monkeypatch, tmp_path):
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", "/home/mimir/heavy.lock")

    class Client:
        def __getattr__(self, name):
            pytest.fail(f"worker client used: {name}")

    backend = compute.LocalSubprocessComputeBackend.for_path_checkout(1002, worker_client=Client())
    spec = compute.WorkSpec(issue_id=1, attempt=1, repo_url="", base_ref="main", branch="b",
                            prompt="", rules=None, test_command="true", backend="opencode",
                            timeout_s=5, local_checkout=tmp_path, local_argv=("true",))
    with pytest.raises(compute.ComputeLaunchError, match="MIMIR_HEAVY_LOCK_FILE"):
        await backend._launch_contained(spec, ("true",))
    assert list(tmp_path.iterdir()) == []


def _prepare(tmp_path, monkeypatch, environ):
    paths = service.RuntimePaths(tmp_path / "daemon", tmp_path / "run")
    paths.run.mkdir(parents=True)
    monkeypatch.setattr(service.os, "fchown", lambda fd, uid, gid: None)
    monkeypatch.setattr(service, "protect_agent_paths", lambda env, identities: None)
    return paths, service.prepare({"GH_TOKEN": "bot-value", **environ},
                                  WorklinkIdentities(42001, 42002, 42003), paths)


@pytest.mark.skipif(not hasattr(os, "O_PATH"), reason="daemon directory preparation is Linux-only")
def test_daemon_environment_carries_the_lease_to_builder_shells(tmp_path, monkeypatch):
    _, env = _prepare(tmp_path, monkeypatch, {"MIMIR_HEAVY_LOCK_FILE": LOCK, "MIMIR_HEAVY_TIER": "medium",
                                           "MIMIR_HEAVY_LOCK_TIMEOUT": TIMEOUT, "MIMIR_API_KEY": "secret"})
    assert env["MIMIR_HEAVY_LOCK_FILE"] == LOCK
    assert env["MIMIR_HEAVY_LOCK_TIMEOUT"] == TIMEOUT
    assert env["MIMIR_HEAVY_TIER"] == "medium"
    assert "MIMIR_API_KEY" not in env
    _, env = _prepare(tmp_path / "unset", monkeypatch, {})
    assert "MIMIR_HEAVY_LOCK_FILE" not in env
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in env


def test_daemon_refuses_a_malformed_lease_before_writing_state(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_FILE"):
        _prepare(tmp_path, monkeypatch, {"MIMIR_HEAVY_LOCK_FILE": "/locks/../x"})
    assert not (tmp_path / "daemon").exists()


def _compose(**kwargs):
    from mimir.scaffold_docker import render_compose_yml
    template = render_compose_yml(service_name="agent", web_port=8123, mode="pypi")
    return yaml.safe_load(installer.render_compose(template, "image:sha", "rust", "repo", **kwargs))


def test_compose_mounts_the_shared_directory_into_the_agent_only(tmp_path):
    result = _compose(heavy_lock_dir=str(tmp_path))
    agent, init = result["services"]["agent"], result["services"]["init"]
    assert f"{tmp_path}:/mimir-heavy-locks" in agent["volumes"]
    assert agent["environment"]["MIMIR_HEAVY_LOCK_FILE"] == LOCK
    assert agent["environment"]["MIMIR_HEAVY_LOCK_TIMEOUT"] == TIMEOUT
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in init.get("environment", {})
    assert not any("mimir-heavy-locks" in volume for volume in init["volumes"])
    plain = _compose()
    assert not any("mimir-heavy-locks" in v for v in plain["services"]["agent"]["volumes"])
    assert "MIMIR_HEAVY_LOCK_FILE" not in plain["services"]["agent"]["environment"]
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in plain["services"]["agent"]["environment"]
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in _compose(heavy_lock_timeout="60")["services"]["agent"]["environment"]
    custom = _compose(heavy_lock_dir=str(tmp_path), heavy_lock_timeout="60")
    assert custom["services"]["agent"]["environment"]["MIMIR_HEAVY_LOCK_TIMEOUT"] == "60"


def test_compose_refuses_invalid_timeout(tmp_path):
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_TIMEOUT"):
        _compose(heavy_lock_dir=str(tmp_path), heavy_lock_timeout="0")


def test_setup_option_validates_preserves_and_clears(tmp_path):
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    args = lambda value: argparse.Namespace(heavy_lock_dir=value)
    assert installer._heavy_lock_dir(args(str(lock_dir)), {}) == str(lock_dir)
    assert installer._heavy_lock_dir(args(None), {"heavy_lock_dir": str(lock_dir)}) == str(lock_dir)
    assert installer._heavy_lock_dir(args("none"), {"heavy_lock_dir": str(lock_dir)}) is None
    assert installer._heavy_lock_dir(args(None), {}) is None
    link = tmp_path / "link"
    link.symlink_to(lock_dir)
    for bad in ("relative", str(tmp_path / "missing"), str(link), str(lock_dir) + "/",
                str(lock_dir) + "/../locks", str(lock_dir) + ":/etc"):
        with pytest.raises(installer.SetupError, match="--heavy-lock-dir"):
            installer._heavy_lock_dir(args(bad), {})
    with pytest.raises(installer.SetupError, match="manifest"):
        installer._heavy_lock_dir(args(None), {"heavy_lock_dir": 7})


def test_daemon_session_environment_carries_the_lease(monkeypatch, tmp_path):
    # Builder sessions get a constructed environment (set_environment), not the
    # daemon's own: the lease must be carried here too.
    from mimir_cultivate.worklink.daemon_sessions import session_environment

    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", LOCK)
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_TIMEOUT", TIMEOUT)
    monkeypatch.setenv("MIMIR_HEAVY_TIER", "medium")
    env = session_environment({}, home=tmp_path, git_settings=(), credentials={})
    assert env["MIMIR_HEAVY_LOCK_FILE"] == LOCK
    assert env["MIMIR_HEAVY_LOCK_TIMEOUT"] == TIMEOUT
    assert env["MIMIR_HEAVY_TIER"] == "medium"
    # pass_env can neither supply nor override it.
    with pytest.raises(compute.ComputeLaunchError, match="MIMIR_HEAVY_LOCK_FILE.*denied"):
        session_environment({"MIMIR_HEAVY_LOCK_FILE": "/elsewhere/heavy.lock"}, home=tmp_path,
                            git_settings=(), credentials={})
    with pytest.raises(compute.ComputeLaunchError, match="MIMIR_HEAVY_LOCK_TIMEOUT.*denied"):
        session_environment({"MIMIR_HEAVY_LOCK_TIMEOUT": "60"}, home=tmp_path,
                            git_settings=(), credentials={})
    with pytest.raises(compute.ComputeLaunchError, match="MIMIR_HEAVY_TIER.*denied"):
        session_environment({"MIMIR_HEAVY_TIER": "large"}, home=tmp_path,
                            git_settings=(), credentials={})
    monkeypatch.delenv("MIMIR_HEAVY_LOCK_FILE")
    assert "MIMIR_HEAVY_LOCK_FILE" not in session_environment({}, home=tmp_path, git_settings=(), credentials={})
    assert "MIMIR_HEAVY_LOCK_TIMEOUT" not in session_environment({}, home=tmp_path, git_settings=(), credentials={})
    monkeypatch.setenv("MIMIR_HEAVY_LOCK_FILE", "/home/mimir/heavy.lock")
    with pytest.raises(ValueError, match="MIMIR_HEAVY_LOCK_FILE"):
        session_environment({}, home=tmp_path, git_settings=(), credentials={})
