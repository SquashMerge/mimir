"""Generated PyPI deployment proof requires the installed Cultivate provider.

Keep this integration test in the provider suite, which CI explicitly installs
with --all-packages; core-only jobs must not depend on an optional provider.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import zipfile

import pytest

from mimir.plugin_dockerfile import _CONTEXT_SCRIPTS, _S6_FILES
from mimir_cultivate.setup import _PYTHON_BASE_IMAGE


ROOT = Path(__file__).resolve().parents[3]
PROOF = ROOT / "scripts/worklink_image_identity.py"


def test_pypi_generated_setup_rebuild_executes_closed_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
