"""Closed epic environment and retained-owner node-control contracts."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import struct
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

if "cultivate" not in os.environ.get("MIMIR_PLUGINS", "").split(","):
    pytest.skip("factory execution requires selected Cultivate", allow_module_level=True)

from mimir_cultivate.worklink import compute, worker_exec, worker_client
from mimir_cultivate.worklink.backends.base import WorkOrder
from mimir_cultivate.worklink.backends.feature_factory import FeatureFactoryBackend
from mimir_cultivate.worklink.daemon_sessions import session_environment
from mimir_cultivate.worklink.safe_git import _git_credential_settings


@pytest.mark.parametrize("kind", ["missing", "regular", "oversize", "symlink", "hardlink", "fifo"])
def test_epic_does_not_read_or_reconcile_any_legacy_auth_shape(tmp_path, monkeypatch, kind):
    controller = tmp_path / "controller"
    controller.mkdir()
    native = controller / ".codex/auth.json"
    native.parent.mkdir()
    native.write_text('{"tokens":{"refresh_token":"agent-owned"}}')
    before = native.read_bytes()
    runtime = tmp_path / "41-2/.factory-runtime/data/opencode/auth.json"
    runtime.parent.mkdir(parents=True)
    if kind == "regular":
        runtime.write_bytes(b"{}")
    elif kind == "oversize":
        runtime.write_bytes(b"x" * (1024 * 1024 + 1))
    elif kind == "symlink":
        runtime.symlink_to(native)
    elif kind == "hardlink":
        os.link(native, runtime)
    elif kind == "fifo":
        os.mkfifo(runtime)
    original = Path.open
    def guarded(path, *args, **kwargs):
        if path in {native, runtime}:
            pytest.fail("epic admission accessed independent agent/retired runtime auth")
        return original(path, *args, **kwargs)
    with monkeypatch.context() as guard:
        guard.setattr(Path, "open", guarded)
        spec = FeatureFactoryBackend().work_spec(WorkOrder(41, tmp_path, "build", None, 30,
            {"MIMIR_MODEL_SPEC": "codex-plus:gpt-test", "HOME": str(controller)}),
            attempt=2, repo_url="repo", base_ref="main", branch="feature/chainlink-41", test_command="pytest")
        assert spec.session_request.kind == "epic" and spec.local_argv is None
        assert "worker_projections" not in spec.backend_config
        assert "HOME" not in spec.env and "OPENCODE_CONFIG" not in spec.env
    assert native.read_bytes() == before
    assert not Path(worker_client.__file__).with_name("factory_auth.py").exists()
    assert not hasattr(worker_exec, "_prepare_factory_runtime")


@pytest.mark.parametrize("helper", [None, "!gh auth git-credential", "!other-forge-helper"])
def test_factory_launch_uses_captured_trusted_scoped_helpers(tmp_path, monkeypatch, helper):
    from mimir_cultivate.worklink import safe_git

    trusted, checkout = tmp_path / "trusted", tmp_path / "checkout"
    for repo in (trusted, checkout):
        subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    capture = safe_git._capture_environment
    monkeypatch.setattr(safe_git, "_capture_environment", lambda: {
        **capture(), "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull})
    settings = () if helper is None else (("credential.https://github.com.helper", ""),
        ("credential.https://github.com.helper", helper),
        ("credential.https://other.example.helper", "!separate-host-helper"))
    for key, value in settings:
        subprocess.run(["git", "-C", str(trusted), "config", "--add", key, value], check=True)
    subprocess.run(["git", "-C", str(checkout), "config", "credential.helper", "!untrusted"], check=True)
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "credential.helper")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "!ambient-untrusted")
    captured = _git_credential_settings(trusted)
    assert captured == settings
    home = tmp_path / "worker-home"
    home.mkdir(mode=0o700)
    environment = session_environment({}, home=home, git_settings=captured, credentials={"GH_TOKEN": "fixture-token"})
    result = subprocess.run(["git", "config", "--get-regexp", r"^credential\.(.+\.)?helper$"],
        cwd=home, env=environment, capture_output=True, text=True)
    assert result.returncode == (0 if settings else 1)
    assert result.stdout.splitlines() == [f"{key} {value}" for key, value in settings]
    assert "!untrusted" not in result.stdout and "!ambient-untrusted" not in result.stdout
    assert "fixture-token" not in repr(captured)
    if helper is not None:
        matched = subprocess.run(["git", "config", "--get-urlmatch", "credential.helper", "https://github.com/owner/repo"],
            cwd=home, env=environment, check=True, capture_output=True, text=True)
        assert matched.stdout.strip() == helper


@pytest.mark.parametrize("name", ["OPENAI_API_KEY", "CODEX_HOME", "OPENCODE_CONFIG", "OPENCODE_PERMISSION",
    "HOME", "XDG_DATA_HOME", "GH_TOKEN", "GITHUB_TOKEN", "FACTORY_SESSION_ID", "OTEL_EXPORTER_OTLP_HEADERS"])
def test_epic_pass_env_cannot_replace_native_or_agent_credentials(tmp_path, name):
    with pytest.raises((RuntimeError, ValueError)):
        session_environment({name: "fixture-value"}, home=tmp_path, git_settings=(), credentials={})


@pytest.fixture
def factory_control_identity(monkeypatch):
    identity = SimpleNamespace(worklink_uid=42424)
    monkeypatch.setattr(worker_client.identities, "get_identities", lambda: identity)
    return identity


@pytest.mark.asyncio
async def test_retained_factory_control_uses_worker_without_runtime_refresh(tmp_path, monkeypatch, factory_control_identity):
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    checkout = tmp_path / "repo/41-2/checkout"
    calls = []
    async def launch(client, **kwargs):
        assert client._socket_timeout_s == 30 + worker_client.CANCEL_SOCKET_TIMEOUT_S
        assert client.run_uid == factory_control_identity.worklink_uid
        calls.append((client._launch_op, client.path_checkout, kwargs))
        os.write(kwargs["stdout_sink"].fd, b"retained-status")
        async def wait():
            return 0
        return SimpleNamespace(wait=wait, timed_out=False, output_overflow=False)
    monkeypatch.setattr(worker_client.WorkerClient, "launch", launch)
    result = worker_client.run_factory_control(checkout / ".factory-sandboxes/run", ["node", "factory.js", "status"],
        env={"HOME": "/controller", "PATH": "/usr/bin:/bin"})
    assert result.stdout == b"retained-status" and result.returncode == 0
    operation, path, kwargs = calls[0]
    assert operation == "launch_factory_control" and path == checkout
    assert kwargs["local_checkout"] == checkout and kwargs["env"] == {"PATH": "/usr/bin:/bin"}
    assert "projections" not in kwargs


@pytest.mark.parametrize("failure", ["timed_out", "output_overflow"])
def test_factory_control_rejects_incomplete_results(tmp_path, monkeypatch, failure, factory_control_identity):
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    async def launch(client, **kwargs):
        assert client.run_uid == factory_control_identity.worklink_uid
        async def wait():
            return 0
        return SimpleNamespace(wait=wait, timed_out=failure == "timed_out", output_overflow=failure == "output_overflow",
                               stdout_bytes=1024 * 1024 + 1, stderr_bytes=0)
    monkeypatch.setattr(worker_client.WorkerClient, "launch", launch)
    expected = subprocess.TimeoutExpired if failure == "timed_out" else RuntimeError
    with pytest.raises(expected):
        worker_client.run_factory_control(tmp_path / "repo/41-2/checkout", ["node", "status"], env={})


def test_factory_control_socket_uses_its_bound(monkeypatch):
    sock = Mock()
    sock.getsockopt.return_value = struct.pack("3i", 123, 0, 0)
    monkeypatch.setattr(worker_client.socket, "SO_PEERCRED", 17, raising=False)
    monkeypatch.setattr(worker_client.socket, "socket", lambda *args: sock)
    client = worker_client.WorkerClient(None)
    client._socket_timeout_s = 7
    assert client._connect() is sock
    sock.settimeout.assert_called_once_with(7)


@pytest.mark.parametrize("side", ["client", "executor"])
def test_executor_requires_peer_credentials_before_side_effects(tmp_path, monkeypatch, side):
    monkeypatch.delattr(worker_client.socket, "SO_PEERCRED", raising=False)
    create_socket = Mock(side_effect=AssertionError("unauthenticated socket created"))
    monkeypatch.setattr(worker_client.socket, "socket", create_socket)
    path = tmp_path / "not-created/executor.sock"
    with pytest.raises(RuntimeError, match="requires Linux SO_PEERCRED peer authentication"):
        if side == "client":
            worker_client.WorkerClient(None, socket_path=path)._connect()
        else:
            worker_exec.serve(path)
    create_socket.assert_not_called()
    assert not path.parent.exists()


@pytest.mark.parametrize("relative,result", [("repo/41-2/checkout/nested", "valid"), ("repo/41-2", "none"),
    ("repo/41-2/project", "none"), ("../41-2/checkout", "error"), ("repo/0-2/checkout", "error"),
    ("repo/41-0/checkout", "error"), ("repo/41-2/checkout/../elsewhere", "error")])
def test_factory_control_path_classification(tmp_path, monkeypatch, relative, result):
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    path = tmp_path / relative
    if result == "error":
        with pytest.raises(ValueError):
            worker_client.factory_checkout_for_path(path)
    elif result == "none":
        assert worker_client.factory_checkout_for_path(path) is None
    else:
        assert worker_client.factory_checkout_for_path(path) == (tmp_path / "repo/41-2/checkout", 41, 2)
    assert worker_client.factory_checkout_for_path(tmp_path.parent / "outside/repo/41-2/checkout") is None


@pytest.mark.parametrize("guard", ["owner", "directory", "nofollow"])
def test_factory_controller_boundary_guard(tmp_path, monkeypatch, guard):
    from mimir_cultivate.worklink import orchestrator
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    monkeypatch.setattr(orchestrator, "get_identities", lambda: SimpleNamespace(mimir_uid=os.getuid()))
    path = tmp_path / "repo/41-2/checkout"
    path.mkdir(parents=True)
    path.parent.chmod(0o2700)
    real_stat = Path.stat
    if guard == "nofollow":
        moved = path.parent.with_name("saved")
        path.parent.rename(moved)
        path.parent.symlink_to(moved, target_is_directory=True)
    else:
        def observed(target, **kwargs):
            value = real_stat(target, **kwargs)
            if target == path.parent:
                return SimpleNamespace(st_uid=os.getuid() + (guard == "owner"),
                    st_mode=(stat.S_IFREG if guard == "directory" else stat.S_IFDIR) | 0o2700)
            return value
        monkeypatch.setattr(Path, "stat", observed)
    controller, worker = Mock(), Mock()
    monkeypatch.setattr(worker_client, "run_factory_control", worker)
    with pytest.raises(orchestrator.WorklinkError, match="boundary is not controller-owned"):
        orchestrator._factory_git_runner(controller)(["git", "-C", str(path), "status"])
    controller.assert_not_called()
    worker.assert_not_called()


@pytest.mark.parametrize("text", [True, False])
def test_factory_git_recovery_uses_owner_not_safe_directory(tmp_path, monkeypatch, text):
    from mimir_cultivate.worklink import orchestrator
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    monkeypatch.setattr(orchestrator, "get_identities", lambda: SimpleNamespace(mimir_uid=os.getuid()))
    checkout = tmp_path / "repo/41-2/checkout"
    checkout.mkdir(parents=True)
    calls = []
    def controller(args, cwd=None, *, text=True):
        assert cwd is None
        calls.append("controller")
        return subprocess.CompletedProcess(args, 0, "initial" if text else b"initial", "" if text else b"")
    def worker(root, args, **kwargs):
        assert root == checkout and "GIT_CONFIG_COUNT" not in kwargs["env"]
        assert not any("safe.directory" in arg for arg in args)
        calls.append("worker")
        return subprocess.CompletedProcess(args, 0, b"retained", b"")
    monkeypatch.setattr(worker_client, "run_factory_control", worker)
    run = orchestrator._factory_git_runner(controller)
    args = ["git", "-C", str(checkout / ".factory-sandboxes/run"), "rev-parse", "HEAD"]
    os.chown(checkout.parent, -1, os.getgid())
    checkout.parent.chmod(0o2700)
    assert run(args, text=text).stdout == ("initial" if text else b"initial")
    checkout.parent.chmod(0o2750)
    assert run(args, text=text).stdout == ("retained" if text else b"retained")
    assert calls == ["controller", "worker"]


@pytest.mark.parametrize("command", ["status", "resume", "heartbeat", "lock"])
def test_factory_backend_controls_use_retained_owner(tmp_path, monkeypatch, command):
    from mimir_cultivate.worklink.backends import feature_factory
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(tmp_path))
    entrypoint = tmp_path / "installed/bin/factory.js"
    monkeypatch.setattr(feature_factory, "resolve_factory_entrypoint", lambda path: entrypoint)
    sandbox = tmp_path / "repo/41-2/checkout/.factory-sandboxes/run"
    calls = []
    def control(path, argv, **kwargs):
        calls.append((path, argv))
        return subprocess.CompletedProcess(argv, 0, b"{}", b"")
    monkeypatch.setattr(worker_client, "run_factory_control", control)
    backend = feature_factory.FeatureFactoryBackend(runner=Mock(side_effect=AssertionError("controller ran worker control")))
    backend._control(entrypoint, [command, "run", "--repo", str(sandbox)], sandbox=sandbox)
    assert calls == [(sandbox, [sys.executable, "-I",
        str(Path(feature_factory.__file__).resolve().parent.parent / "factory_control_output.py"),
        "status" if command == "status" else "other", "node", str(entrypoint), command, "run", "--repo", str(sandbox)])]


def test_initial_factory_control_selects_private_transfer_only_for_bound_init(tmp_path, monkeypatch, factory_control_identity):
    from tests.opencode_fixtures import seed_factory_install

    root = tmp_path / "checkouts"
    outer = root / "repo/41-2/checkout"
    launcher = seed_factory_install(tmp_path / "installed")
    monkeypatch.setenv("WORKLINK_CHECKOUT_ROOT", str(root))
    seen = []
    async def launch(client, **kwargs):
        seen.append(client._launch_op)
        assert kwargs["argv"] == ["node", str(launcher), "init", "chainlink-41", "--repo", str(outer)]
        async def wait():
            return 0
        return SimpleNamespace(wait=wait, timed_out=False, output_overflow=False)
    monkeypatch.setattr(worker_client.WorkerClient, "launch", launch)
    argv = ["node", str(launcher), "init", "chainlink-41", "--repo", str(outer)]
    worker_client.run_factory_control(outer, argv, env={}, initialize=True)
    assert seen == ["launch_factory"]
    for unsafe in (["sh", "-c", "arbitrary"], ["node", str(launcher), "status", "chainlink-41", "--repo", str(outer)],
                   ["node", str(launcher), "init", "chainlink-42", "--repo", str(outer)],
                   ["node", str(launcher), "init", "chainlink-41", "--repo", str(tmp_path)]):
        with pytest.raises(ValueError):
            worker_client.run_factory_control(outer, unsafe, env={}, initialize=True)
    assert seen == ["launch_factory"]
