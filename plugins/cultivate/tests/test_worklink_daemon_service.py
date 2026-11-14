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
    monkeypatch.setattr(service, "exec_worklink", lambda *args, **kw: calls.append((args, kw)))
    service.root_start(inherited)
    assert calls == [((service.SUPERVISOR_ARGV, clean), {"cwd": "/"})]
    assert "OTHER_AGENT_KEY" not in calls[0][0][1]


def test_daemon_exec_drops_before_closed_exec(monkeypatch):
    from mimir_cultivate.worklink import worker_exec

    events = []
    monkeypatch.setattr(service.os, "getuid", lambda: 0)
    monkeypatch.setattr(service.os, "geteuid", lambda: 0)
    monkeypatch.setattr(service.os, "chdir", lambda path: events.append(("cwd", path)))
    monkeypatch.setattr(worker_exec, "_drop_worker_privileges", lambda: events.append(("drop",)))
    monkeypatch.setattr(service.os, "umask", lambda mask: events.append(("umask", mask)))
    monkeypatch.setattr(service.os, "execve", lambda *args: events.append(("exec", *args)))
    service.exec_worklink(service.SUPERVISOR_ARGV, {"PATH": "/usr/bin:/bin"}, cwd="/")
    assert events == [("cwd", "/"), ("drop",), ("umask", 0o002),
                      ("exec", service.PYTHON, service.SUPERVISOR_ARGV, {"PATH": "/usr/bin:/bin"})]


@pytest.mark.real_worklink_identities
def test_daemon_descendant_has_worker_privileges_and_no_drop_mutation_fails(
    synthetic_worklink_identities,
):
    """Real kernel/exec inheritance; required (not skipped) in the root CI leg."""
    import shutil
    import subprocess
    from mimir_cultivate.worklink import worker_exec

    # Check the isolation opt-out even where the real UID execution must skip.
    assert synthetic_worklink_identities is None
    if sys.platform != "linux" or os.geteuid() != 0:
        pytest.skip("requires Linux root; covered by required worker-uid root CI leg")
    ids = worker_exec.get_identities()
    python = shutil.which("python3", path="/usr/bin:/bin")
    assert python is not None
    child = f'''
import os
from pathlib import Path
status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
assert os.getresuid() == ({ids.worklink_uid},) * 3
assert os.getresgid() == ({ids.worklink_gid},) * 3
assert os.getgroups() == [], os.getgroups()
for key in ('CapInh', 'CapPrm', 'CapEff', 'CapAmb', 'CapBnd'):
    assert int(status[key], 16) == 0, (key, status[key])
assert status['NoNewPrivs'].strip() == '1', status['NoNewPrivs']
print('daemon child privileges verified')
'''
    daemon = f"import subprocess, sys; sys.exit(subprocess.run([sys.executable, '-c', {child!r}], timeout=10).returncode)"
    script = f'''
from types import SimpleNamespace
from mimir_cultivate.worklink import daemon_service as service, worker_exec
import sys
service.resolve_activation = lambda env: SimpleNamespace(enabled=True)
service.prepare = lambda env, ids: {{'PATH': '/usr/bin:/bin'}}
service.SUPERVISOR_ARGV = ({python!r}, '-c', {daemon!r})
if sys.argv[1] == 'mutant':
    worker_exec._drop_worker_privileges = lambda: None
service.root_start({{}})
'''
    baseline = subprocess.run([sys.executable, "-c", script, "baseline"],
                              capture_output=True, text=True, timeout=30)
    assert baseline.returncode == 0, baseline.stderr
    assert baseline.stdout.strip() == "daemon child privileges verified"
    mutant = subprocess.run([sys.executable, "-c", script, "mutant"],
                            capture_output=True, text=True, timeout=30)
    assert mutant.returncode != 0, "no-drop mutation survived the real descendant assertion"
    assert "AssertionError" in mutant.stderr
    assert "daemon child privileges verified" not in mutant.stdout


def _daemon_cleanup_base_setup(repo: Path) -> str:
    """Shared payload: keep fixture provisioning executable without root."""
    return f'''
from pathlib import Path
import subprocess
p = Path({str(repo)!r})
p.mkdir(mode=0o755)
subprocess.run(['git', 'init', '-q', '-b', 'main', str(p)], check=True)
# Checkout creation resolves the base's persistent identity, not commit-only -c flags.
subprocess.run(['git', '-C', str(p), 'config', '--local', 'user.name', 'daemon-cleanup-test'], check=True)
subprocess.run(['git', '-C', str(p), 'config', '--local', 'user.email', 'daemon-cleanup@example.invalid'], check=True)
subprocess.run(['git', '-C', str(p), 'commit', '-q', '--allow-empty', '-m', 'base'], check=True)
subprocess.run(['git', '-C', str(p), 'remote', 'add', 'origin', str(p)], check=True)
'''


def test_daemon_cleanup_base_lends_identity_without_ambient_git_config(tmp_path):
    import subprocess

    env = {key: value for key, value in os.environ.items()
           if key not in {"GH_USER_NAME", "GH_USER_EMAIL"}}
    env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                "WORKLINK_CHECKOUT_ROOT": str(tmp_path / "checkouts")})
    repo = tmp_path / "base"
    script = _daemon_cleanup_base_setup(repo) + f'''
from mimir_cultivate.worklink.checkout import create_isolated_checkout
lease = create_isolated_checkout(Path({str(repo)!r}), issue_id=2138, attempt=1,
                                base='main', worker_eligible=False)
for key, expected in (('user.name', 'daemon-cleanup-test'),
                      ('user.email', 'daemon-cleanup@example.invalid')):
    result = subprocess.run(['git', '-C', str(lease.path), 'config', '--local', '--get', key],
                            check=True, capture_output=True, text=True)
    assert result.stdout.strip() == expected, (key, result.stdout)
'''
    result = subprocess.run([sys.executable, "-c", script], env=env,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.real_worklink_identities
def test_daemon_child_output_cleanup_and_private_umask_mutation(
    tmp_path, synthetic_worklink_identities,
):
    """Distinct real UIDs: 002 output cleans up; restoring 077 must refuse."""
    import pwd
    import shutil
    import subprocess
    import tempfile

    assert synthetic_worklink_identities is None
    if sys.platform != "linux" or os.geteuid() != 0:
        pytest.skip("requires Linux root; mandatory in the distinct-identity CI leg")
    from mimir_cultivate.worklink.identities import get_identities
    ids = get_identities()
    controller_gid = pwd.getpwuid(ids.mimir_uid).pw_gid
    assert ids.mimir_uid not in (0, ids.worklink_uid)
    python = shutil.which("python3", path="/usr/bin:/bin")
    assert python is not None
    # The daemon uid must traverse this test-owned workspace, independently of
    # pytest's root-only basetemp. Never chmod ancestors owned by another test.
    with tempfile.TemporaryDirectory(prefix="daemon-cleanup-") as directory:
        workspace = Path(directory)
        workspace.chmod(0o755)
        os.chown(workspace, ids.mimir_uid, controller_gid)
        env = {key: value for key, value in os.environ.items()
               if key not in {"GH_USER_NAME", "GH_USER_EMAIL"}}
        env.update({"MIMIR_PLUGINS": "cultivate",
                    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                    "WORKLINK_CHECKOUT_ROOT": str(workspace / "checkouts")})
        controller = dict(user=ids.mimir_uid, group=controller_gid,
                          extra_groups=[ids.worklink_gid], env=env,
                          capture_output=True, text=True, timeout=30)
        repo = workspace / "base"
        setup = _daemon_cleanup_base_setup(repo)
        result = subprocess.run([sys.executable, "-c", setup], **controller)
        assert result.returncode == 0, result.stderr
        for attempt, mutant in ((1, False), (2, True)):
            checkout = workspace / "checkouts" / repo.name / f"2138-{attempt}"
            create = f'''
from pathlib import Path
from mimir_cultivate.worklink.checkout import create_isolated_checkout
lease = create_isolated_checkout(Path({str(repo)!r}), issue_id=2138, attempt={attempt}, base='main', worker_eligible=True)
assert str(lease.path) == {str(checkout)!r}
'''
            result = subprocess.run([sys.executable, "-c", create], **controller)
            assert result.returncode == 0, result.stderr
            child = ("from pathlib import Path; "
                     f"p=Path({str(checkout)!r}) / 'worker-hooks'; p.mkdir(); "
                     "(p / 'pre-commit').write_text('worker output'); "
                     "import time; time.sleep(60)")
            supervise = f'''
import asyncio
from pathlib import Path
from mimir_cultivate.worklink import daemon_service as service, daemon_health
service.SERVE_ARGV = ({python!r}, '-c', {child!r})
stop = asyncio.Event()
async def publish(*args, **kwargs):
    if (Path({str(checkout)!r}) / 'worker-hooks/pre-commit').exists():
        stop.set()
daemon_health.inspect_and_publish = publish
{'service.os.umask = lambda mode: None' if mutant else ''}
asyncio.run(service.supervise(service.RuntimePaths(Path('/'), Path('/')), stop=stop))
'''
            mutation = "service.os.umask(0o077); service.os.umask = lambda mode: None" if mutant else ""
            launch = f'''
from mimir_cultivate.worklink import daemon_service as service
{mutation}
service.exec_worklink(({sys.executable!r}, '-c', {supervise!r}), dict(__import__('os').environ), cwd='/')
'''
            result = subprocess.run([sys.executable, "-c", launch], env=env,
                                    capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, result.stderr
            hooks = checkout / "worker-hooks"
            assert hooks.stat().st_uid == ids.worklink_uid
            assert hooks.stat().st_mode & 0o777 == (0o700 if mutant else 0o775)
            assert (hooks / "pre-commit").stat().st_mode & 0o777 == (0o600 if mutant else 0o664)
            cleanup = f'''
from pathlib import Path
from mimir_cultivate.worklink.checkout import CheckoutLease, cleanup_checkout
lease = CheckoutLease(2138, {attempt}, Path({str(repo)!r}), Path({str(checkout)!r}), 'issue/2138-a{attempt}', 'main', isolated_checkout=True)
assert cleanup_checkout(lease, outcome='completed')
assert not lease.path.exists()
print('controller cleanup succeeded')
'''
            result = subprocess.run([sys.executable, "-c", cleanup], **controller)
            if mutant:
                assert result.returncode != 0, "umask-077 mutation survived controller cleanup"
                assert "PermissionError" in result.stderr, result.stderr
                assert hooks.exists()
            else:
                assert result.returncode == 0, result.stderr
                assert result.stdout.strip() == "controller cleanup succeeded"


@pytest.mark.real_worklink_identities
async def test_spawn_checkout_real_session_path_and_scope_mutation(
    synthetic_worklink_identities, monkeypatch,
):
    """Real factory, FD executor, session helper, HTTP and distinct-UID cleanup."""
    import pwd
    import subprocess
    import tempfile
    import uuid
    from mimir_cultivate.worklink import worker_exec
    from tests.test_worklink_remediation_e2e import _ExecutorServer
    from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon

    assert synthetic_worklink_identities is None
    if sys.platform != "linux" or os.geteuid() != 0:
        pytest.skip("requires Linux root; mandatory in the distinct-identity CI leg")
    ids = worker_exec.get_identities()
    controller_gid = pwd.getpwuid(ids.mimir_uid).pw_gid
    with tempfile.TemporaryDirectory(prefix="spawn-session-") as directory:
        workspace = Path(directory)
        workspace.chmod(0o755)
        os.chown(workspace, ids.mimir_uid, controller_gid)
        root = workspace / "opencode-checkouts"
        root.mkdir(mode=0o771)
        root.chmod(0o771)
        os.chown(root, ids.mimir_uid, controller_gid)
        repo = workspace / "base"
        env = {key: value for key, value in os.environ.items()
               if key not in {"GH_USER_NAME", "GH_USER_EMAIL"}}
        env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        controller = dict(user=ids.mimir_uid, group=controller_gid,
                          extra_groups=[ids.worklink_gid], env=env,
                          capture_output=True, text=True, timeout=60)
        setup = subprocess.run([sys.executable, "-c", _daemon_cleanup_base_setup(repo)], **controller)
        assert setup.returncode == 0, setup.stderr
        daemon_root, run = workspace / "daemon", workspace / "run"
        for path in (daemon_root, daemon_root / "sessions", run, run / "agent"):
            path.mkdir(mode=0o700)
            os.chown(path, ids.worklink_uid, ids.worklink_gid)
        run.chmod(0o711)
        paths = service.RuntimePaths(daemon_root, run)
        generation = str(uuid.uuid4())
        for path, value, uid, gid in (
            (paths.snapshot, {"version": 1, "launch_generation": generation,
                              "bot_token": None, "publishing_identity": None}, ids.mimir_uid, controller_gid),
            (daemon_root / "launch.json", {"version": 1, "launch_generation": generation,
                                          "telemetry_state": "off"}, ids.worklink_uid, ids.worklink_gid),
        ):
            path.write_text(json.dumps(value))
            path.chmod(0o600)
            os.chown(path, uid, gid)
        # The controller snapshot is private to mimir, as in the shipped layout.
        os.chown(run / "agent", ids.mimir_uid, controller_gid)
        monkeypatch.setattr(service, "RuntimePaths", lambda: paths)
        monkeypatch.setattr(worker_exec, "OPENCODE_CHECKOUT_ROOT", root)
        socket_path = workspace / "executor.sock"

        class PayloadDaemon(FakeOpenCodeDaemon):
            async def _dispatch(self, request, payload):
                response = await super()._dispatch(request, payload)
                if request.path.endswith("/prompt"):
                    sid = request.match_info["sessionID"]
                    checkout = Path(self.sessions[sid].info["location"]["directory"])
                    # Synthetic provider only: actual pathname access and kernel
                    # ownership/umask are exercised by a real worker descendant.
                    child = subprocess.run(
                        ["/usr/bin/python3", "-c",
                         "import os; from pathlib import Path; os.umask(0o002); "
                         f"p=Path({str(checkout)!r})/'worker-hooks'; p.mkdir(); "
                         "(p/'pre-commit').write_text('spawn worker output')"],
                        user=ids.worklink_uid, group=ids.worklink_gid, extra_groups=[],
                        capture_output=True, text=True, timeout=10,
                    )
                    assert child.returncode == 0, child.stderr
                    self.finish(sid)
                return response

        async with PayloadDaemon() as daemon:
            paths.api.write_text(json.dumps({"version": 1, "url": daemon.base_url,
                                             "username": daemon.username, "password": daemon.password}))
            paths.api.chmod(0o600)
            os.chown(paths.api, ids.worklink_uid, ids.worklink_gid)
            shim = workspace / "session-helper"
            helper = f'''
from pathlib import Path
from mimir_cultivate.worklink import daemon_service, daemon_sessions, daemon_config, daemon_health
paths = daemon_service.RuntimePaths(Path({str(daemon_root)!r}), Path({str(run)!r}))
daemon_service.RuntimePaths = lambda: paths
daemon_config.DAEMON_ROOT = {str(daemon_root)!r}
daemon_health.DAEMON_URL = {daemon.base_url!r}
raise SystemExit(daemon_sessions.main())
'''
            # Replace only the installed native-helper path, retaining the real
            # privilege drop, session controller and wire operations.
            shim.write_text(f"#!{sys.executable}\nimport sys\nfrom mimir_cultivate.worklink.daemon_service import exec_worklink\n"
                            f"exec_worklink(({sys.executable!r}, '-c', {helper!r}), dict(__import__('os').environ), cwd='/')\n")
            shim.chmod(0o755)
            monkeypatch.setattr(service, "UID_HELPER", str(shim))
            script = f'''
import asyncio, json, os, sys
from dataclasses import asdict
from pathlib import Path
from mimir_cultivate import contained_checkout
from mimir_cultivate.worklink import checkout, worker_client
from mimir_cultivate.worklink.backends.base import SessionRequest
root = Path({str(root)!r})
contained_checkout.OPENCODE_CHECKOUT_ROOT = root
checkout._OPENCODE_CHECKOUT_ROOT = root
issued = contained_checkout.create_opencode_checkout(Path({str(repo)!r}), default_cwd=Path({str(repo)!r}))
scope = issued.path.parent.parent
assert scope.stat().st_uid == {ids.mimir_uid}
assert scope.stat().st_gid == {ids.worklink_gid}
assert scope.stat().st_mode & 0o777 == 0o710
if sys.argv[1] == 'mutant':
    scope.chmod(0o700)
async def main():
    client = worker_client.WorkerClient(issued.capability, socket_path=Path({str(socket_path)!r}))
    response = await client._session_rpc('session_create', {{
        'generation': {generation!r}, 'request': asdict(SessionRequest('spawn', {{'providerID': 'proof', 'id': 'model'}}, 'build')),
        'timeout_s': 30, 'pass_env': {{}}, 'git_settings': [],
    }})
    handle = client._admission_handle(response, {generation!r})
    outcome = await client.session_observe(handle)
    assert outcome.execution_state == 'succeeded', outcome
    await client.session_cancel(handle)
    await client.session_release(handle)
    hooks = issued.path / 'worker-hooks'
    assert hooks.stat().st_uid == {ids.worklink_uid}
    assert hooks.stat().st_mode & 0o777 == 0o775
    assert (hooks / 'pre-commit').read_text() == 'spawn worker output'
    issued.close()
    assert not issued.path.parent.exists()
    print('spawn session and controller cleanup passed')
asyncio.run(main())
'''
            with _ExecutorServer(socket_path, controller_uid=ids.mimir_uid):
                baseline = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, "baseline"], **controller)
                assert baseline.returncode == 0, baseline.stderr
                assert baseline.stdout.strip() == "spawn session and controller cleanup passed"
                assert len(daemon.sessions) == 1
                mutant = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, "mutant"], **controller)
                assert mutant.returncode != 0, "controller-only scope mutation survived"
                assert "session executor operation failed" in mutant.stderr, mutant.stderr
                assert len(daemon.sessions) == 1, "mutation unexpectedly reached session creation"


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
    previous_umask = os.umask(0o002)
    try:
        env = service.prepare({"GH_TOKEN": "bot-value", "OTHER_KEY": secret,
                               "MIMIR_FACTORY_PUBLISHING_IDENTITY": "bot"}, ids, paths)
    finally:
        os.umask(previous_umask)
    assert env["GH_TOKEN"] == env["GITHUB_TOKEN"] == "bot-value"
    assert env["FACTORY_PUBLISHING_IDENTITY"] == "bot"
    assert "OTHER_KEY" not in env
    assert socket.stat() == before
    assert paths.run.stat().st_mode & 0o777 == 0o711
    for relative in ("home", "tmp", "sessions", "config/opencode", "data", "state", "cache"):
        assert (paths.daemon / relative).stat().st_mode & 0o777 == 0o700
    assert paths.daemon.stat().st_mode & 0o777 == 0o700
    assert (paths.daemon / "config").stat().st_mode & 0o777 == 0o700
    for path in (paths.api, paths.daemon / "launch.json",
                 paths.daemon / "config/opencode/opencode.json", paths.snapshot):
        assert path.stat().st_mode & 0o777 == 0o600, path
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


def test_uid_drop_is_factored_and_service_argv_has_no_secrets():
    tree = ast.parse(Path(service.__file__).read_text())
    calls = [node.func.attr for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)]
    assert not {"setuid", "setgid", "setresuid", "setresgid", "setgroups", "clear"} & set(calls)
    assert service.SUPERVISOR_ARGV == (
        "/opt/mimir-worklink/venv/bin/python", "-m",
        "mimir_cultivate.worklink.daemon_service", "--supervise")
    assert service.SERVE_ARGV == (
        "/opt/mimir-opencode/bin/opencode", "serve", "--hostname", "127.0.0.1", "--port", "4096")


@pytest.mark.asyncio
async def test_supervisor_awaits_only_its_serve_child(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_health, worker_exec

    monkeypatch.setattr(worker_exec, "_verify_worker_identity", lambda: None)
    home = tmp_path / "home"
    home.mkdir()
    executable = tmp_path / "fixture-serve"
    executable.write_text(f"#!{sys.executable}\nimport signal, time\nsignal.signal(signal.SIGTERM, lambda *args: exit(0))\nwhile True: time.sleep(1)\n")
    executable.chmod(0o700)
    monkeypatch.setattr(service, "SERVE_ARGV", (str(executable),))
    monkeypatch.setattr(service, "assert_worklink_identity", lambda ids: None)
    monkeypatch.setattr(service, "get_identities", lambda: WorklinkIdentities(1, 2, 3))
    masks = []
    monkeypatch.setattr(service.os, "umask", lambda mode: masks.append(mode))
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
    assert masks == [0o002]
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
