from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from mimir.plugin_dockerfile import select_plugins
from mimir.scaffold_docker import render_start_sh, scaffold


ROOT = Path(__file__).resolve().parents[1]
DOCKER = ROOT / "plugins/cultivate/mimir_cultivate/docker"


@pytest.fixture
def startup(tmp_path):
    """Image paths/uid helper are fixtures; activation and gate policy are real."""
    pytest.importorskip("mimir_cultivate")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    marker = tmp_path / "installed"
    marker.write_text("1\n")
    marker.chmod(0o644)
    selected = tmp_path / "selected"
    selected.write_text("1\n")
    generation = str(uuid4())
    run = tmp_path / "run"
    (run / "status").mkdir(parents=True, mode=0o755)
    (run / "agent").mkdir(mode=0o700)
    status = run / "status/status.json"
    status.write_text(json.dumps({"version": 1, "launch_generation": generation,
        "checked_at_ms": int(time.time() * 1000), "daemon_state": "ready",
        "daemon_version": "2.2.7", "login_state": "locally_unexpired",
        "login_expires_at_ms": int(time.time() * 1000) + 60000, "telemetry_state": "off"}))
    status.chmod(0o644)
    snapshot = run / "agent/launch.json"
    snapshot.write_text(json.dumps({"version": 1, "launch_generation": generation,
                                   "bot_token": None, "publishing_identity": None}))
    snapshot.chmod(0o600)
    python = bin_dir / "python"
    python.write_text(f"#!{sys.executable}\n" + '''import os, sys
from pathlib import Path
from types import SimpleNamespace
from mimir import worklink_daemon_activation as activation
from mimir_cultivate.worklink import daemon_health as health, daemon_gate as gate
from mimir_cultivate.worklink.daemon_service import RuntimePaths
from mimir_cultivate.worklink.identities import WorklinkIdentities
root = Path(os.environ['FIXTURE_ROOT'])
marker = root / 'installed'
inode = marker.stat().st_ino
fstat = os.fstat
def image_owner(fd):
    info = fstat(fd)
    if info.st_ino == inode:
        return SimpleNamespace(st_mode=info.st_mode, st_uid=0, st_size=info.st_size)
    return info
os.fstat = image_owner
activation.DAEMON_PATH = str(root / 'bin')
resolve = activation.resolve_activation
activation.resolve_activation = lambda env: resolve(env, marker=marker)
health.resolve_activation = activation.resolve_activation
health.get_identities = lambda: WorklinkIdentities(os.getuid(), os.getuid(), os.getgid())
wait = gate.wait_for_daemon
gate.wait_for_daemon = lambda env: wait(env, paths=RuntimePaths(root / 'daemon', root / 'run'), timeout=0)
assert sys.argv[1] == '-c'
exec(sys.argv[2])
''')
    python.chmod(0o755)
    daemon = bin_dir / "opencode"
    daemon.write_text("#!/bin/sh\nexit 0\n")
    daemon.chmod(0o755)
    gate = bin_dir / "gate"
    gate.write_text((DOCKER / "worklink-daemon-gate").read_text().replace(
        "/opt/mimir-worklink/venv/bin/python", str(python)))
    gate.chmod(0o755)
    workload = bin_dir / "workload"
    workload.write_text('#!/bin/sh\ntouch "$FIXTURE_ROOT/workload-ran"\n')
    workload.chmod(0o755)
    helper = bin_dir / "s6-setuidgid"
    helper.write_text('#!/bin/sh\n[ "$1" = mimir ] || exit 90\nshift\nexec "$@"\n')
    helper.chmod(0o755)
    uv = bin_dir / "uv"
    uv.write_text('#!/bin/sh\n[ "$1" = run ] && shift\n[ "$1" != --no-sync ] || shift\nexec "$@"\n')
    uv.chmod(0o755)
    env = {"PATH": f"{bin_dir}:/usr/local/bin:/usr/bin:/bin", "FIXTURE_ROOT": str(tmp_path),
           "MIMIR_PLUGINS": "cultivate", "HOME": str(tmp_path), "MIMIR_HOME": str(tmp_path)}
    return tmp_path, bin_dir, env


@pytest.mark.parametrize("writer", ["root", "scaffold", "scaffold-with-plugin", "workspace-final", "pypi-final"])
@pytest.mark.parametrize("state", ["core", "not-installed", "missing-login", "unexpired", "refreshable",
                                  "invalid-marker", "missing-binary", "missing-wrapper"])
def test_every_startup_writer_enforces_native_gate(startup, monkeypatch, writer, state):
    root, bin_dir, env = startup
    if writer == "root":
        text = (ROOT / "deploy/s6-overlay/s6-rc.d/mimir/run").read_text()
    elif writer.startswith("scaffold"):
        plugins = select_plugins(enable="cultivate")
        if writer.endswith("with-plugin"):
            from tests.test_scaffold_docker import _provider
            plugins = (*plugins, _provider(root))
        monkeypatch.setattr("mimir.scaffold_docker.select_plugins", lambda **kw: plugins)
        home = root / "scaffold"
        home.mkdir()
        scaffold(home, mode="workspace")
        text = (home / ".mimir-scaffold/s6-rc.d/mimir/run").read_text()
        for name in ("type", "run", "dependencies.d/base"):
            assert (home / ".mimir-scaffold/s6-rc.d/worklink-opencode" / name).read_bytes() == (
                ROOT / "deploy/s6-overlay/s6-rc.d/worklink-opencode" / name).read_bytes()
    else:
        text = render_start_sh(mode=writer.split("-")[0])
        # Run the exact final gate and workload exec, without clone/sync/setup.
        guard = next(line for line in text.splitlines() if "resolve_activation(dict(os.environ))" in line)
        final = next(line for line in text.splitlines() if line.startswith("exec "))
        assert text.index(guard) < text.index(final)
        text = "set -eu\n" + guard + "\n" + final + "\n"
    if state == "core":
        env["MIMIR_PLUGINS"] = ""
    elif state == "not-installed":
        (root / "installed").write_text("0\n")
        (bin_dir / "opencode").unlink()
    elif state == "invalid-marker":
        (root / "installed").write_text("invalid\n")
    elif state == "missing-binary":
        (bin_dir / "opencode").unlink()
    elif state == "missing-wrapper":
        (bin_dir / "gate").unlink()
    elif state in {"missing-login", "refreshable"}:
        path = root / "run/status/status.json"
        status = json.loads(path.read_text())
        status["login_state"] = "missing" if state == "missing-login" else "refreshable_unverified"
        path.write_text(json.dumps(status))
    text = (text.replace("/usr/local/libexec/worklink-daemon-gate", str(bin_dir / "gate"))
            .replace("/opt/cultivate-docker/selected", str(root / "selected"))
            .replace("/usr/local/bin/start.sh", str(bin_dir / "workload"))
            .replace("mimir run", str(bin_dir / "workload")))
    result = subprocess.run(["sh", "-c", text], env=env, text=True, capture_output=True, timeout=10)
    allowed = state in {"core", "not-installed", "unexpired", "refreshable"}
    assert (result.returncode == 0) is allowed, result.stderr
    assert (root / "workload-ran").exists() is allowed
    assert (bin_dir / "opencode").exists() is (state not in {"not-installed", "missing-binary"})
    if state == "refreshable":
        assert "unverified" in result.stderr


def test_reserved_service_and_wrappers_use_immutable_interpreter():
    run = (ROOT / "deploy/s6-overlay/s6-rc.d/worklink-opencode/run").read_text()
    assert run == "#!/command/with-contenv sh\nexec /usr/local/libexec/worklink-opencode-service\n"
    for name, module in (("worklink-opencode-service", "daemon_service"),
                         ("worklink-opencode-login", "daemon_login")):
        assert (DOCKER / name).read_text() == (
            "#!/bin/sh\nexec /opt/mimir-worklink/venv/bin/python -m mimir_cultivate.worklink."
            + module + ' "$@"\n')
