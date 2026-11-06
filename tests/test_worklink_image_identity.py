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

