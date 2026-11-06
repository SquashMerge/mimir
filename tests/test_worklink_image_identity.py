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
    for invariant in ("build_pypi_context(context", "MIMIR_INSTALL_MODE=pypi",
                      "MIMIR_AGENT_SHA256=", "MIMIR_CULTIVATE_SHA256=",
                      "MIMIR_PLUGIN_SELECTION=cultivate", "CHAINLINK_ARTIFACT_STAGE=chainlink-build",
                      "MIMIR_EXTRAS=anthropic,discord,slack,mcp"):
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


def test_pypi_passes_exact_pair_to_context_and_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    namespace = runpy.run_path(str(PROOF))
    build_context = []
    from mimir import plugin_dockerfile

    monkeypatch.setattr(plugin_dockerfile, "build_pypi_context", lambda *args, **kwargs: build_context.append((args, kwargs)))
    globals_ = namespace["prove_pypi"].__globals__
    monkeypatch.setitem(globals_, "wheel_identity", lambda path, name: ("1.0.0rc2", name[0] * 64))

    class BuildReached(Exception):
        pass

    def run(args: list[str], **kwargs: object) -> None:
        if args[:2] == ["docker", "build"]:
            assert args[-1] == str(build_context[0][0][0])
            for value in ("MIMIR_INSTALL_MODE=pypi", "MIMIR_VERSION=1.0.0rc2",
                          "MIMIR_AGENT_WHEEL=agent.whl", "MIMIR_CULTIVATE_WHEEL=cultivate.whl",
                          f"MIMIR_AGENT_SHA256={'m' * 64}", f"MIMIR_CULTIVATE_SHA256={'m' * 64}",
                          "MIMIR_PLUGIN_SELECTION=cultivate", "CHAINLINK_ARTIFACT_STAGE=chainlink-build"):
                assert value in args
            raise BuildReached
        assert args == ["docker", "info"]

    monkeypatch.setitem(globals_, "run", run)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: None)
    with pytest.raises(BuildReached):
        namespace["prove_pypi"](tmp_path / "agent.whl", tmp_path / "cultivate.whl")
    assert build_context[0][1] == {
        "agent_wheel": tmp_path / "agent.whl", "cultivate_wheel": tmp_path / "cultivate.whl",
        "version": "1.0.0rc2", "agent_sha256": "m" * 64, "cultivate_sha256": "m" * 64,
    }
