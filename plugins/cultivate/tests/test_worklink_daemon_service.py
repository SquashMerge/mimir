from __future__ import annotations

from types import SimpleNamespace
import asyncio
import ast
import json
import os
from pathlib import Path
import sys

import pytest

from mimir_cultivate.worklink import daemon_service as service
from mimir_cultivate.worklink import daemon_login
from mimir_cultivate.worklink.identities import WorklinkIdentities


def test_root_start_execs_only_closed_environment(monkeypatch):
    clean = {"PATH": "/usr/bin:/bin", "OPENCODE_SERVER_PASSWORD": "synthetic-private"}
    inherited = {"OTHER_AGENT_KEY": "never-project"}
    calls = []
    monkeypatch.setattr(service.os, "getuid", lambda: 0)
    monkeypatch.setattr(service.os, "geteuid", lambda: 0)
    monkeypatch.setattr(service, "resolve_activation", lambda env: SimpleNamespace(enabled=True))
    monkeypatch.setattr(service, "get_identities", lambda: object())
    monkeypatch.setattr(service, "prepare", lambda env, ids: clean)
    monkeypatch.setattr(service.os, "chdir", lambda path: None)
    monkeypatch.setattr(service.os, "umask", lambda mode: None)
    monkeypatch.setattr(service.os, "execve", lambda *args: calls.append(args))
    service.root_start(inherited)
    assert calls == [(service.UID_HELPER, service.SUPERVISOR_ARGV, clean)]
    assert "OTHER_AGENT_KEY" not in calls[0][2]


def test_disabled_service_execs_pause_without_preparation(monkeypatch):
    calls = []
    monkeypatch.setattr(service.os, "getuid", lambda: 0)
    monkeypatch.setattr(service.os, "geteuid", lambda: 0)
    monkeypatch.setattr(service, "resolve_activation", lambda env: SimpleNamespace(enabled=False))
    monkeypatch.setattr(service, "prepare", lambda *args: pytest.fail("disabled credential access"))
    monkeypatch.setattr(service.os, "execve", lambda *args: calls.append(args))
    service.root_start({"AGENT_SECRET": "excluded"})
    assert calls == [("/command/s6-pause", ("/command/s6-pause",), {"PATH": "/usr/bin:/bin"})]


def test_enabled_missing_binary_is_not_pause(monkeypatch):
    from mimir.worklink_daemon_activation import ActivationError

    monkeypatch.setattr(service.os, "getuid", lambda: 0)
    monkeypatch.setattr(service.os, "geteuid", lambda: 0)
    def unavailable(env):
        raise ActivationError("enabled OpenCode binary is unavailable")
    monkeypatch.setattr(service, "resolve_activation", unavailable)
    monkeypatch.setattr(service.os, "execve", lambda *args: pytest.fail("must refuse"))
    with pytest.raises(ActivationError):
        service.root_start({})


def test_preparation_private_layout_and_socket_preservation(tmp_path, monkeypatch):
    paths = service.RuntimePaths(tmp_path / "daemon", tmp_path / "run")
    paths.run.mkdir()
    socket = paths.run / "socket"
    socket.mkdir(mode=0o710)
    before = socket.stat()
    ids = WorklinkIdentities(42001, 42002, 42003)
    owners = []
    monkeypatch.setattr(service.os, "fchown", lambda fd, uid, gid: owners.append((os.fstat(fd).st_ino, uid, gid)))
    monkeypatch.setattr(service, "protect_agent_paths", lambda env, identities: None)
    secret = "agent-" + "secret-only"
    env = service.prepare({"GH_TOKEN": "bot-value", "OTHER_KEY": secret,
                           "MIMIR_FACTORY_PUBLISHING_IDENTITY": "bot"}, ids, paths)
    assert env["GH_TOKEN"] == env["GITHUB_TOKEN"] == "bot-value"
    assert env["FACTORY_PUBLISHING_IDENTITY"] == "bot"
    assert "OTHER_KEY" not in env
    assert socket.stat() == before
    assert paths.run.stat().st_mode & 0o777 == 0o711
    for relative in ("home", "tmp", "sessions", "config/opencode", "data", "state", "cache"):
        assert (paths.daemon / relative).stat().st_mode & 0o777 == 0o700
    assert paths.api.stat().st_mode & 0o777 == 0o600
    api = json.loads(paths.api.read_text())
    assert api["password"] == env["OPENCODE_SERVER_PASSWORD"]
    assert api["url"] == "http://127.0.0.1:4096"
    assert paths.snapshot.stat().st_mode & 0o777 == 0o600
    assert paths.snapshot.parent.stat().st_mode & 0o777 == 0o700
    assert paths.status.parent.stat().st_mode & 0o777 == 0o755
    assert (paths.snapshot.stat().st_ino, ids.mimir_uid, -1) in owners
    assert (paths.api.stat().st_ino, ids.worklink_uid, ids.worklink_gid) in owners
    contents = "".join(p.read_text() for p in tmp_path.rglob("*.json"))
    assert secret not in contents


@pytest.mark.parametrize("kind", ["leaf", "parent", "oversize", "mode", "hardlink"])
def test_private_reader_rejects_substitution(tmp_path, kind):
    directory = tmp_path / "state"
    directory.mkdir()
    target = directory / "api.json"
    target.write_text("{}")
    target.chmod(0o600)
    if kind == "leaf":
        target.rename(directory / "real")
        target.symlink_to(directory / "real")
    elif kind == "parent":
        directory.rename(tmp_path / "real")
        directory.symlink_to(tmp_path / "real", target_is_directory=True)
    elif kind == "oversize":
        target.write_bytes(b" " * 16385)
    elif kind == "mode":
        target.chmod(0o644)
    else:
        os.link(target, directory / "alias")
    with pytest.raises((OSError, ValueError)):
        service.read_json(target, uid=os.getuid(), mode=0o600)


def test_atomic_write_does_not_follow_destination_link(tmp_path, monkeypatch):
    monkeypatch.setattr(service.os, "fchown", lambda *args: None)
    other = tmp_path / "other"
    other.write_text("unchanged")
    target = tmp_path / "api.json"
    target.symlink_to(other)
    service.atomic_json(target, {"safe": True}, uid=os.getuid(), gid=os.getgid(), mode=0o600)
    assert not target.is_symlink()
    assert json.loads(target.read_text()) == {"safe": True}
    assert other.read_text() == "unchanged"


def test_private_state_through_search_only_parent(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("requires an unprivileged permission check")
    parent = tmp_path / "search-only"
    parent.mkdir()
    directory = parent / "private"
    directory.mkdir(mode=0o700)
    target = directory / "api.json"
    target.write_text('{"version":1}')
    target.chmod(0o600)
    parent.chmod(0o111)
    try:
        with pytest.raises(PermissionError):
            os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        assert service.read_json(target, uid=os.getuid(), mode=0o600) == {"version": 1}
        service.atomic_json(target, {"version": 2}, uid=os.getuid(), gid=os.getgid(), mode=0o600)
        assert service.read_json(target, uid=os.getuid(), mode=0o600) == {"version": 2}
        # The final descriptor still supports preparation and durable writes.
        with service.directory_fd(directory) as fd:
            os.fchmod(fd, 0o700)
            os.fsync(fd)
        directory.chmod(0o755)
        with pytest.raises(service.PrivateStateError, match="invalid state directory"):
            service.read_json(target, uid=os.getuid(), mode=0o600)
    finally:
        parent.chmod(0o700)


def test_no_same_interpreter_uid_drop_or_secret_argv():
    tree = ast.parse(Path(service.__file__).read_text())
    calls = [node.func.attr for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)]
    assert not {"setuid", "setgid", "setresuid", "setresgid", "setgroups", "clear"} & set(calls)
    assert service.SUPERVISOR_ARGV == (
        "/command/s6-setuidgid", "worklink", "/opt/mimir-worklink/venv/bin/python", "-m",
        "mimir_cultivate.worklink.daemon_service", "--supervise")
    assert service.SERVE_ARGV == (
        "/opt/mimir-opencode/bin/opencode", "serve", "--hostname", "127.0.0.1", "--port", "4096")


@pytest.mark.asyncio
async def test_supervisor_awaits_only_its_serve_child(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_health

    home = tmp_path / "home"
    home.mkdir()
    executable = tmp_path / "fixture-serve"
    executable.write_text(f"#!{sys.executable}\nimport signal, time\nsignal.signal(signal.SIGTERM, lambda *args: exit(0))\nwhile True: time.sleep(1)\n")
    executable.chmod(0o700)
    monkeypatch.setattr(service, "SERVE_ARGV", (str(executable),))
    monkeypatch.setattr(service, "assert_worklink_identity", lambda ids: None)
    monkeypatch.setattr(service, "get_identities", lambda: WorklinkIdentities(1, 2, 3))
    monkeypatch.setattr(service.os, "umask", lambda mode: None)
    stop = asyncio.Event()
    states = []
    async def publish(paths, ids, *, down=False):
        states.append(down)
        stop.set()
    monkeypatch.setattr(daemon_health, "inspect_and_publish", publish)
    create = asyncio.create_subprocess_exec
    owned = []
    async def spawn(*args, **kwargs):
        assert args == (str(executable),)
        process = await create(*args, **kwargs)
        owned.append(process)
        return process
    monkeypatch.setattr(service.asyncio, "create_subprocess_exec", spawn)
    assert await service.supervise(service.RuntimePaths(tmp_path, tmp_path / "run"), stop=stop) == 0
    assert len(owned) == 1 and owned[0].returncode is not None
    assert states == [False, True]


def test_login_exec_exact_native_command_private_password(tmp_path, monkeypatch, capsys):
    paths = service.RuntimePaths(tmp_path, tmp_path / "run")
    (tmp_path / "home").mkdir()
    password = "private-" + "api-test-value"
    paths.api.write_text(json.dumps({"version": 1, "url": "http://127.0.0.1:4096",
                                    "username": "opencode", "password": password}))
    paths.api.chmod(0o600)
    monkeypatch.setattr(daemon_login, "get_identities", lambda: WorklinkIdentities(1, os.getuid(), os.getgid()))
    monkeypatch.setattr(daemon_login, "assert_worklink_identity", lambda ids: None)
    calls = []
    monkeypatch.setattr(daemon_login.os, "execve", lambda *args: calls.append(args))
    monkeypatch.setattr(daemon_login.os, "chdir", lambda path: None)
    monkeypatch.setenv("AGENT_SECRET", "excluded")
    daemon_login.login(paths)
    binary, argv, env = calls[0]
    assert argv == ("/opt/mimir-opencode/bin/opencode", "auth", "login", "--server",
                    "http://127.0.0.1:4096", "--method", "chatgpt-headless", "openai")
    assert binary == argv[0] and password not in argv
    assert env["OPENCODE_PASSWORD"] == password
    assert "OPENCODE_SERVER_PASSWORD" not in env
    assert "AGENT_SECRET" not in env and "GH_TOKEN" not in env
    assert password not in str(capsys.readouterr())


def test_login_identity_refusal_precedes_private_file_access(monkeypatch):
    monkeypatch.setattr(daemon_login, "get_identities", lambda: object())
    def refuse(ids):
        raise RuntimeError("identity")
    monkeypatch.setattr(daemon_login, "assert_worklink_identity", refuse)
    monkeypatch.setattr(daemon_login, "load_api", lambda *args, **kwargs: pytest.fail("credential read"))
    with pytest.raises(RuntimeError):
        daemon_login.login()


def test_agent_roots_are_mimir_private_without_recursive_changes(tmp_path, monkeypatch):
    from contextlib import contextmanager

    mimir_home = tmp_path / "mimir"
    volume = tmp_path / "volume"
    for home in (mimir_home, volume):
        home.mkdir(mode=0o755)
        (home / "compose.env").write_text("OTHER_AGENT_SECRET=fixture-only")
    codex = mimir_home / ".codex"
    codex.mkdir(mode=0o755)
    (codex / "auth.json").write_text("agent-owned")
    owners = []
    monkeypatch.setattr(service.os, "fchown", lambda fd, uid, gid: owners.append((os.fstat(fd).st_ino, uid)))
    walk = service.directory_fd
    @contextmanager
    def fixture_walk(path):
        if path == Path("/home/mimir"):
            path = mimir_home
        with walk(path) as fd:
            yield fd
    monkeypatch.setattr(service, "directory_fd", fixture_walk)
    ids = WorklinkIdentities(42001, 42002, 42003)
    service.protect_agent_paths({"MIMIR_HOME": str(volume), "CODEX_HOME": str(codex)}, ids)
    for path in (mimir_home, volume, codex):
        assert path.stat().st_mode & 0o777 == 0o700
        assert (path.stat().st_ino, ids.mimir_uid) in owners
    for home in (mimir_home, volume):
        path = home / "compose.env"
        assert path.stat().st_mode & 0o777 == 0o600
        assert (path.stat().st_ino, ids.mimir_uid) in owners
    assert (codex / "auth.json").read_text() == "agent-owned"


def test_login_with_fixture_executable_never_initiates_oauth(tmp_path, monkeypatch):
    """Exercise exec/environment plumbing, substituting only the CLI binary."""
    import subprocess

    fixture = tmp_path / "fixture-opencode"
    fixture.write_text(f"#!{sys.executable}\nimport os, sys\nassert sys.argv[1:] == ['auth', 'login', '--server', 'http://127.0.0.1:4096', '--method', 'chatgpt-headless', 'openai']\nassert os.environ['OPENCODE_PASSWORD'] == 'fixture-private-password'\nassert 'OTHER_AGENT_SECRET' not in os.environ\nprint('fixture argv and environment accepted')\n")
    fixture.chmod(0o700)
    paths = service.RuntimePaths(tmp_path, tmp_path / "run")
    (tmp_path / "home").mkdir()
    paths.api.write_text(json.dumps({"version": 1, "url": "http://127.0.0.1:4096", "username": "opencode",
                                    "password": "fixture-private-password"}))
    paths.api.chmod(0o600)
    script = f"""
from pathlib import Path
from mimir_cultivate.worklink import daemon_login as login
from mimir_cultivate.worklink.daemon_service import RuntimePaths
from mimir_cultivate.worklink.identities import WorklinkIdentities
import os
login.get_identities = lambda: WorklinkIdentities(1, os.getuid(), os.getgid())
login.assert_worklink_identity = lambda ids: None
login.LOGIN_ARGV = ({str(fixture)!r}, *login.LOGIN_ARGV[1:])
login.login(RuntimePaths(Path({str(tmp_path)!r}), Path({str(paths.run)!r})))
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                            env={**os.environ, "OTHER_AGENT_SECRET": "never-inherit"}, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "fixture argv and environment accepted"
    assert "fixture-private-password" not in result.stdout + result.stderr
