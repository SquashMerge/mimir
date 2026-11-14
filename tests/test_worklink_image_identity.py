from __future__ import annotations

import hashlib
from pathlib import Path
import runpy
import subprocess
import zipfile

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
PROOF = ROOT / "scripts/worklink_image_identity.py"


def test_image_job_builds_and_proves_both_modes() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/tests.yml").read_text())
    steps = workflow["jobs"]["worklink-image-identity"]["steps"]
    commands = [step.get("run", "") for step in steps]
    source = next(i for i, command in enumerate(commands) if "--mode source" in command)
    assets = next(i for i, command in enumerate(commands) if "npm run build" in command)
    wheels = next(i for i, command in enumerate(commands) if "uv build --wheel" in command)
    pypi = next(i for i, command in enumerate(commands) if "--mode pypi" in command)
    assert source < assets < wheels < pypi
    assert "uv build --wheel --out-dir dist/agent" in commands[wheels]
    assert "uv build --wheel --out-dir dist/cultivate plugins/cultivate" in commands[wheels]
    assert "--agent-wheel dist/agent/*.whl --cultivate-wheel dist/cultivate/*.whl" in commands[pypi]
    assert workflow["jobs"]["worklink-image-identity"]["env"]["WORKLINK_REPO"] == "/workspace/worklink-base"


@pytest.mark.parametrize("mode", ["source", "pypi"])
def test_live_proofs_initialize_lease_root_before_services(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = runpy.run_path(str(PROOF))
    proof = namespace["prove_" + mode]
    globals_ = proof.__globals__
    calls = []

    class Started(Exception):
        pass

    def record(args, **kwargs):
        calls.append(args)
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            output = ""
        elif args[:3] == ["git", "rev-parse", "HEAD"]:
            output = "a" * 40
        else:
            output = ""
        return subprocess.CompletedProcess(args, 0, stdout=output)

    def stop_after_launch():
        raise Started

    deploy = tmp_path / "deployment"
    deploy.mkdir()
    (deploy / "manifest.json").write_text('{"layer_tag":"proof-layer"}')
    monkeypatch.setenv("WORKLINK_REPO", "/workspace/worklink-base")
    monkeypatch.setitem(globals_, "run", record)
    monkeypatch.setitem(globals_, "source_ref", lambda: "refs/pull/30/merge")
    monkeypatch.setitem(globals_, "source_url", lambda: None)
    monkeypatch.setitem(globals_, "require_rust_native_toolchain", lambda _: None)
    monkeypatch.setitem(globals_, "prepare_pypi_deployment", lambda *args: deploy)
    monkeypatch.setitem(globals_, "wheel_identity", lambda *args: ("1.0", "0" * 64))
    monkeypatch.setitem(globals_, "wait_for_runtime", stop_after_launch)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: None)  # Docker cleanup only.
    args = () if mode == "source" else (tmp_path / "agent.whl", tmp_path / "cultivate.whl")
    with pytest.raises(Started):
        proof(*args)
    launch, = [call for call in calls if call[:3] == ["docker", "run", "--detach"]]
    environments = [launch[i + 1] for i, arg in enumerate(launch) if arg == "--env"]
    assert "MIMIR_PLUGINS=cultivate" in environments
    assert "MIMIR_PR_CHECKOUT_LEASE_ROOT=/workspace/pr-leases" in environments
    if mode == "source":
        assert "WORKLINK_REPO=/workspace/worklink-base" in environments
    assert launch[launch.index("--entrypoint") + 1:] == [
        "/bin/sh", globals_["IMAGE"], "-ceu",
        "install -d -o mimir -g mimir -m 0755 /workspace/pr-leases; exec /init",
    ]


def test_source_mode_keeps_explicit_build_arg_and_live_proof() -> None:
    script = PROOF.read_text()
    assert '"MIMIR_INSTALL_MODE=source"' in script
    assert 'f"MIMIR_GIT_REF={git_ref}"' in script
    assert 'f"MIMIR_CONTROLLER_COMMIT={source_commit}"' in script
    assert 'f"MIMIR_EXECUTOR_COMMIT={source_commit}"' in script
    for invariant in ("stat -c %U:%G /opt/mimir-worklink/uv-cache",
                      "sibling-access negative control did not detect a cross-write",
                      "ControllerGitPublication.capture", "SPAWN_PROOF", "TOOLCHAIN_PROOF"):
        assert invariant in script


def test_pypi_proof_builds_local_context_and_checks_runtime() -> None:
    script = PROOF.read_text()
    for invariant in ("prepare_pypi_deployment", 'installer.setup(args,',
                      'run(["sh", str(deploy / "build.sh")]',
                      '"--platform", "linux/amd64"', '"--embedding", "local"'):
        assert invariant in script
    namespace = runpy.run_path(str(PROOF))
    compile(namespace["PYPI_RUNTIME_PROOF"], "PYPI_RUNTIME_PROOF", "exec")
    for invariant in ("controller_identity() == expected", "executor_identity() == expected",
                      "verify_executor_identity()", "status == 503", "status == 200",
                      "worklink_install_status"):
        assert invariant in namespace["PYPI_RUNTIME_PROOF"]
    assert "# altered installed content" in script
    assert "executor-install-identity.json" in script


def test_pypi_protocol_proof_uses_controller_uid_and_root_only_for_mutations(monkeypatch):
    import base64

    namespace = runpy.run_path(str(PROOF))
    prove = namespace['prove_pypi_runtime']
    calls = []
    def execute(*args, user='root', **kwargs):
        calls.append((args, user))
        if 'print(next(Path' in args[2]:
            return args[3] + '/lib/python3.11/site-packages/mimir_cultivate/install_identity.py\n'
        if 'tempfile.mkstemp()' in args[2]:
            return '/tmp/private-root-backup\n'
        return ''

    monkeypatch.setitem(prove.__globals__, 'docker_exec', execute)
    prove({'distributions': {'agent': {'wheel': 'agent.whl'}, 'cultivate': {'wheel': 'cultivate.whl'}}})
    checks = [(args, user) for args, user in calls if args[2] == namespace['PYPI_RUNTIME_PROOF']]
    assert len(checks) == 13  # initial plus failure/recovery for all six mutations
    assert all(user == 'mimir' for _, user in checks)
    assert [args[4] for args, _ in checks] == ['', 'controller', '', 'worker', '', 'controller', '', 'worker', '', 'both', '', 'both', '']
    imports = [(args, user) for args, user in calls if args[2] == namespace['PYPI_READONLY_IMPORT_PROOF']]
    assert len(imports) == 1 and imports[0][1] == 'mimir'
    writes = [(args, user) for args, user in calls if 'write_bytes(' in args[2]]
    assert len(writes) == 6
    assert all(user == 'root' for _, user in writes)
    restores = [(args, user) for args, user in calls if 'shutil.copyfile(sys.argv[2]' in args[2]]
    assert len(restores) == 6
    assert all(user == 'root' and args[4] == '/tmp/private-root-backup' for args, user in restores)
    assert [args[3] for args, _ in writes] == [args[3] for args, _ in restores]
    for args, _ in writes:
        if args[3].endswith('.py'):
            assert base64.b64decode(args[4]) == b'# altered installed content\n'
            assert 'path.read_bytes() + data' in args[2]


def test_pypi_runtime_proof_refuses_root_before_protocol(monkeypatch):
    import os
    import sys

    namespace = runpy.run_path(str(PROOF))
    monkeypatch.setattr(os, 'geteuid', lambda: 0)
    monkeypatch.setattr(sys, 'argv', ['proof', '{}'])
    with pytest.raises(AssertionError, match='must run as mimir'):
        exec(namespace['PYPI_RUNTIME_PROOF'], {})


@pytest.mark.parametrize(
    ("state", "error"),
    [
        ({"ok": False, "daemon_enabled": True}, None),
        ({"ok": True, "daemon_enabled": True}, "unexpectedly passed"),
        ({"ok": True, "daemon_enabled": False}, "unexpectedly passed"),
    ],
)
def test_setup_daemon_image_proof_requires_refusal(state, error, monkeypatch) -> None:
    import json
    setup = pytest.importorskip("mimir_cultivate.setup", reason="daemon setup belongs to Cultivate")

    payload = runpy.run_path(str(PROOF))["SETUP_DAEMON_PROOF"]
    calls = []

    def execute(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(state), stderr="")

    monkeypatch.setattr(subprocess, "run", execute)
    # The embedded payload replaces _run as it does in the image. Restore it
    # after the test, without substituting the real verification function.
    monkeypatch.setattr(setup, "_run", setup._run)
    if error:
        with pytest.raises(AssertionError, match=error):
            exec(payload, {})
    else:
        exec(payload, {})
    assert calls == [(
        ["/command/s6-setuidgid", "mimir", "python", "-c", setup._OPENCODE_DAEMON_PROBE],
        {"cwd": "/tmp", "capture_output": True, "text": True, "timeout": 30},
    )]


def test_setup_daemon_image_proof_rejects_unrelated_setup_error(monkeypatch) -> None:
    setup = pytest.importorskip("mimir_cultivate.setup", reason="daemon setup belongs to Cultivate")

    def fail(*args, **kwargs):
        raise setup.SetupError("unrelated setup failure")

    monkeypatch.setattr(setup, "_verify_opencode_daemon", fail)
    monkeypatch.setattr(setup, "_run", setup._run)
    with pytest.raises(AssertionError, match="unrelated setup failure"):
        exec(runpy.run_path(str(PROOF))["SETUP_DAEMON_PROOF"], {})


def test_setup_daemon_image_proof_rejects_wrong_user_before_execution(monkeypatch) -> None:
    setup = pytest.importorskip("mimir_cultivate.setup", reason="daemon setup belongs to Cultivate")

    def wrong_user(deploy, *argv):
        return setup._run(["docker", "compose", "exec", "-T", "-u", "worklink", "agent", *argv], cwd=deploy)

    def forbidden(*args, **kwargs):
        pytest.fail("wrong-user probe reached subprocess execution")

    monkeypatch.setattr(setup, "_agent_exec", wrong_user)
    monkeypatch.setattr(setup, "_run", setup._run)
    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(AssertionError, match="worklink"):
        exec(runpy.run_path(str(PROOF))["SETUP_DAEMON_PROOF"], {})


@pytest.mark.parametrize("kind", ["leaf", "spawn"])
async def test_image_daemon_fixture_uses_production_session_controller(kind, tmp_path, monkeypatch):
    pytest.importorskip("mimir_cultivate", reason="daemon sessions belong to Cultivate")
    import asyncio
    from http.server import ThreadingHTTPServer
    from threading import Thread
    from types import SimpleNamespace
    from mimir_cultivate.worklink.backends.base import SessionRequest
    from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient, DaemonClientError
    from mimir_cultivate.worklink.daemon_sessions import SessionController

    namespace = {"__name__": "image_fixture"}
    exec(runpy.run_path(str(PROOF))["DAEMON_FIXTURE"], namespace)
    namespace["api"] = {"username": "fixture", "password": "fixture-password"}
    calls = []

    def payload(argv, **kwargs):
        calls.append((argv, kwargs))
        assert kwargs["cwd"] == str(tmp_path)
        assert kwargs["env"]["MIMIR_WORKLINK_GATE"] == "1"
        assert kwargs["env"]["MIMIR_MODEL_SPEC"] == "proof/model"
        assert kwargs["timeout"] == 25
        return subprocess.CompletedProcess(argv, 0, stdout="fixed payload ran", stderr="")

    namespace["subprocess"] = SimpleNamespace(run=payload)
    server = ThreadingHTTPServer(("127.0.0.1", 0), namespace["Handler"])
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = {"base_url": f"http://127.0.0.1:{server.server_port}", **namespace["api"]}
    try:
        async with OpenCodeDaemonClient(**{**connection, "password": "wrong"}) as bad:
            with pytest.raises(DaemonClientError, match="HTTP 401"):
                await bad.info()
        async with OpenCodeDaemonClient(**connection) as client:
            controller = await SessionController.create(
                client, tmp_path / "record.json", request=SessionRequest(
                    kind, {"providerID": "proof", "id": "model"}, "fixed input", ("git *",)),
                issue_id=1, attempt=1, generation="fixture", checkout=tmp_path,
                environment={"PATH": "/usr/bin:/bin"}, timeout_s=30)
            result = await controller.observe()
            assert result.execution_state == "succeeded"
            assert result.stdout == "fixed payload ran"
            assert controller.record.admission_state == "admitted"
            await controller.release()
            assert controller.record.cancellation_state == "verified"
        assert len(namespace["sessions"]) == 1
        assert calls[0][0] == ["/tmp/opencode-proof" if kind == "leaf" else "/tmp/opencode-spawn-proof"]
        assert len(calls) == 1
    finally:
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(timeout=5)


async def test_missing_login_image_payload_uses_real_health_and_refuses_before_session(tmp_path, monkeypatch, capsys):
    pytest.importorskip("mimir_cultivate", reason="daemon health belongs to Cultivate")
    import json
    import os
    import time
    from types import SimpleNamespace
    from mimir.tools.registry import _STATE
    from mimir.worklink_daemon_activation import DaemonActivation
    from mimir_cultivate.worklink import daemon_health as health_module
    from mimir_cultivate.worklink.daemon_config import make_launch_snapshot, resolve_daemon_credentials

    namespace = {"__name__": "image_refusal"}
    exec(runpy.run_path(str(PROOF))["MISSING_LOGIN_PROOF"], namespace)
    monkeypatch.setenv("WORKLINK_REPO", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    generation = "12345678-1234-4234-8234-123456789abc"
    status = {"version": 1, "launch_generation": generation, "checked_at_ms": time.time_ns() // 1_000_000,
              "daemon_state": "ready", "daemon_version": "2.2.7", "login_state": "missing",
              "login_expires_at_ms": None, "telemetry_state": "off"}
    snapshot = make_launch_snapshot(resolve_daemon_credentials(dict(os.environ)), generation).to_dict()
    monkeypatch.setattr(health_module, "resolve_activation", lambda env: DaemonActivation(True, "enabled"))
    monkeypatch.setattr(health_module, "get_identities", lambda: SimpleNamespace(worklink_uid=1002, mimir_uid=1001))
    monkeypatch.setattr(health_module, "read_json", lambda path, **kw: status if path.name == "status.json" else snapshot)
    monkeypatch.setitem(_STATE, "spawn_config", _STATE["spawn_config"])
    namespace["os"] = SimpleNamespace(environ=os.environ, geteuid=lambda: 1001)
    namespace["Path"] = lambda path: tmp_path / "artifacts" if path == "/home/mimir/worklink-refusal-artifacts" else Path(path)
    await namespace["main"]()
    result = json.loads(capsys.readouterr().out)
    assert result == {"proof": "missing_login", "reason_code": "login_missing",
                      "sessions_started": 0, "native_login_proven": False}


def test_positive_image_proofs_keep_session_fixture_boundaries_and_evidence():
    namespace = runpy.run_path(str(PROOF))
    for name in ("CONTROLLER_PROOF", "SPAWN_PROOF"):
        payload = namespace[name]
        compile(payload, name, "exec")
        assert 'patch("mimir_cultivate.worklink.daemon_health.daemon_health"' in payload
        assert '"native_login_proven": False' in payload
    assert "ControllerGitPublication.capture" in namespace["CONTROLLER_PROOF"]
    assert '"gate-euid=1002"' in namespace["CONTROLLER_PROOF"]
    assert '"spawn-euid=1002"' in namespace["SPAWN_PROOF"]
    assert 'proposal["sha256"]' in namespace["SPAWN_PROOF"] or 'proposal.get("sha256")' in namespace["SPAWN_PROOF"]
    script = PROOF.read_text()
    assert script.index("'-c', MISSING_LOGIN_PROOF, user='mimir'") < script.index("input_text=DAEMON_FIXTURE")
    assert "chmod 0755 /usr/local/bin/opencode" not in script


def test_wheel_identity_checks_metadata_and_cultivate_pair(tmp_path: Path) -> None:
    wheel_identity = runpy.run_path(str(PROOF))["wheel_identity"]
    wheel = tmp_path / "mimir_cultivate-1.0.0rc2-py3-none-any.whl"

    def write(metadata: str) -> None:
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("mimir_cultivate-1.0.0rc2.dist-info/METADATA", metadata)

    write("Name: mimir-cultivate\nVersion: 1.0.0rc2\nRequires-Dist: mimir-agent==1.0.0rc2\n")
    assert wheel_identity(wheel, "mimir-cultivate") == ("1.0.0rc2", hashlib.sha256(wheel.read_bytes()).hexdigest())
    write("Name: mimir-cultivate\nVersion: 1.0.0rc2\nRequires-Dist: mimir-agent==1.0.0rc1\n")
    with pytest.raises(ValueError, match="paired core"):
        wheel_identity(wheel, "mimir-cultivate")
    with pytest.raises(ValueError, match="expected mimir-agent"):
        wheel_identity(wheel, "mimir-agent")


def test_pypi_refuses_mismatched_versions_before_docker_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    namespace = runpy.run_path(str(PROOF))
    calls = []
    monkeypatch.setitem(namespace["prove_pypi"].__globals__, "run", lambda args, **kw: calls.append(args))
    monkeypatch.setitem(namespace["prove_pypi"].__globals__, "wheel_identity",
                        lambda path, name: ("1.0" if name == "mimir-agent" else "2.0", "0" * 64))
    with pytest.raises(ValueError, match="same exact version"):
        namespace["prove_pypi"](tmp_path / "agent.whl", tmp_path / "cultivate.whl")
    assert calls == [["docker", "info"]]

