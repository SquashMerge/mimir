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
                      "installed content", "executor-install-identity.json", "worklink_install_status"):
        assert invariant in namespace["PYPI_RUNTIME_PROOF"]


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


def test_pypi_generated_setup_rebuild_executes_closed_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json
    import os
    import sys
    from mimir.plugin_dockerfile import _CONTEXT_SCRIPTS, _S6_FILES
    from mimir_cultivate.setup import _PYTHON_BASE_IMAGE

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("GH_USER_NAME", "proof-bot")
    monkeypatch.setenv("GH_USER_EMAIL", "proof@example.com")
    version = "1.0.0rc2"
    agent = tmp_path / f"mimir_agent-{version}-py3-none-any.whl"
    cultivate = tmp_path / f"mimir_cultivate-{version}-py3-none-any.whl"
    with zipfile.ZipFile(agent, "w") as archive:
        archive.writestr("mimir/docker_context/Dockerfile", (ROOT / "Dockerfile").read_bytes())
        for name in _S6_FILES:
            archive.writestr("mimir/docker_context/deploy/s6-overlay/s6-rc.d/" + name,
                             (ROOT / "deploy/s6-overlay/s6-rc.d" / name).read_bytes())
    with zipfile.ZipFile(cultivate, "w") as archive:
        for name in _CONTEXT_SCRIPTS:
            archive.writestr("mimir_cultivate/docker/" + name,
                             (ROOT / "plugins/cultivate/mimir_cultivate/docker" / name).read_bytes())
    agent_hash, cultivate_hash = (hashlib.sha256(path.read_bytes()).hexdigest() for path in (agent, cultivate))
    namespace = runpy.run_path(str(PROOF))
    deploy = namespace["prepare_pypi_deployment"](tmp_path, agent, cultivate, version, agent_hash, cultivate_hash)
    manifest = json.loads((deploy / "manifest.json").read_text())
    assert manifest["docker_platform"] == "linux/amd64"
    # Execute the saved script AND its isolated stdlib context helper. Only the
    # external daemon boundary is replaced; inspect the context while it exists.
    docker = tmp_path / "docker"
    log = tmp_path / "build-log.jsonl"
    docker.write_text(f"#!{sys.executable}\n" + '''import json, pathlib, sys
args = sys.argv[1:]
context = pathlib.Path(args[-1])
files = {str(p.relative_to(context)): p.read_bytes().hex() for p in context.rglob('*') if p.is_file()}
with open('BUILD_LOG', 'a') as stream:
    stream.write(json.dumps({'args': args, 'files': files}) + '\\n')
'''.replace("BUILD_LOG", str(log)))
    docker.chmod(0o755)
    result = subprocess.run(["sh", str(deploy / "build.sh")], capture_output=True, text=True,
                            env={**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"]})
    assert result.returncode == 0, result.stderr
    root, layer = [json.loads(line) for line in log.read_text().splitlines()]
    for build in (root, layer):
        assert build["args"][:3] == ["build", "--platform", "linux/amd64"]
    for value in ("BASE_IMAGE=" + _PYTHON_BASE_IMAGE, "MIMIR_INSTALL_MODE=pypi", "MIMIR_VERSION=" + version,
                  "MIMIR_AGENT_SHA256=" + agent_hash, "MIMIR_CULTIVATE_SHA256=" + cultivate_hash,
                  "MIMIR_EXTRAS=local-embeddings,mcp,openai", "MIMIR_PLUGIN_SELECTION=cultivate",
                  "CHAINLINK_ARTIFACT_STAGE=chainlink-build", "MIMIR_ENABLE_OPENCODE=1"):
        assert value in root["args"]
    assert bytes.fromhex(root["files"]["Dockerfile"]) == (ROOT / "Dockerfile").read_bytes()
    assert bytes.fromhex(root["files"]["plugins/cultivate/mimir_cultivate/docker/wheels/" + agent.name]) == agent.read_bytes()
    assert not any(name.startswith("mimir/") for name in root["files"])
    assert layer["args"][-3:] == ["-f", "Dockerfile.toolchain", "."]
    assert manifest["layer_tag"] in layer["args"]
