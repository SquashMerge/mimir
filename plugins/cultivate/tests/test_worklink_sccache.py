"""Exercise the rendered launcher without Docker, network, or a Rust install."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import runpy
import shlex
import stat
import subprocess
import sys

import pytest

from mimir_cultivate import setup as installer
from mimir_cultivate.worklink import evidence
from mimir_cultivate.worklink.backends.base import ComputeResult, LaunchHandle, WorkSpec
from mimir_cultivate.worklink.daemon_sessions import session_environment


SOURCE = Path(installer.__file__).parent / "docker" / "worklink-sccache"


def executable(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n" + body)
    path.chmod(0o755)
    return path


@pytest.fixture
def launcher(tmp_path, short_socket_dir):
    layer = installer.render_layer("rust", "1.99.1", "root:sha")
    command = next(line for line in layer.splitlines() if " | base64 -d > /usr/local/bin/sccache" in line)
    encoded = shlex.split(command.split("printf %s ", 1)[1])[0]
    source = base64.b64decode(encoded).decode()
    assert source == SOURCE.read_text()
    assert "mv /usr/local/bin/sccache /usr/local/libexec/sccache && " in command
    assert "chown root:root /usr/local/bin/sccache && chmod 0755 /usr/local/bin/sccache" in command
    assert "install -d -o worklink -g worklink -m 0700 /var/tmp/mimir-sccache\n" in layer
    uncached = installer.render_layer("rust", "1.99.1", "root:sha", sccache=False)
    assert "/var/tmp/mimir-sccache" not in uncached
    assert "/usr/local/libexec/sccache" not in uncached

    raw = executable(tmp_path / "pinned-sccache", """
import json, os, pathlib, socket, subprocess, sys, tempfile
if sys.argv[1:] == ['--version']:
    print('sccache 0.20.1')
    sys.exit(0)
# Model the upstream startup handshake with a real AF_UNIX bind, not a
# hand-written length check. tempfile uses the launcher's actual environment.
with tempfile.TemporaryDirectory(prefix='sccache') as directory:
    path = str(pathlib.Path(directory) / 'sock')
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(path)
        scratch = pathlib.Path(os.environ['TMPDIR'])
        cache = pathlib.Path(os.environ['SCCACHE_DIR'])
        (cache / 'launch.json').write_text(json.dumps({
            'socket': path, 'tmp': str(scratch),
            'mode': scratch.stat().st_mode & 0o777,
            'uid': scratch.stat().st_uid,
        }))
        sys.exit(subprocess.call(sys.argv[1:]))
""")
    wrapper = executable(tmp_path / "sccache", source.split("\n", 1)[1].replace(
        'SCCACHE = "/usr/local/libexec/sccache"', f"SCCACHE = {str(raw)!r}").replace(
        'SCCACHE_TMPDIR = "/var/tmp/mimir-sccache"', f"SCCACHE_TMPDIR = {str(short_socket_dir)!r}"))
    compiler = executable(tmp_path / "rustc", """
import json, os, pathlib, sys
if '--fail' in sys.argv:
    print('fixture compiler rejected the source', file=sys.stderr)
    sys.exit(17)
target = pathlib.Path(sys.argv[sys.argv.index('-o') + 1])
target.write_text(json.dumps({'argv': sys.argv[1:], 'tmp': os.environ['TMPDIR']}))
""")
    home = tmp_path / ("session-" + "x" * 120) / "home"
    (home / "tmp").mkdir(parents=True, mode=0o700)
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    env = session_environment(
        {**installer.toolchain_test_env("rust"), "SCCACHE_DIR": str(cache)},
        home=home, git_settings=(), credentials={},
    )
    assert len(os.fsencode(env["TMPDIR"])) > 108
    # Only remap the public executable for this fixture; the renderer and every
    # runtime environment continue to use /usr/local/bin/sccache in production.
    assert env["RUSTC_WRAPPER"] == "/usr/local/bin/sccache"
    env["RUSTC_WRAPPER"] = str(wrapper)
    return wrapper, raw, compiler, env, cache, short_socket_dir


def compile_command(launcher, tmp_path):
    wrapper, _, compiler, _, _, _ = launcher
    artifact = tmp_path / "compiled artifact"
    return [str(wrapper), str(compiler), "source with spaces.rs", "-o", str(artifact)], artifact


def assert_cached_compile(launcher, result, artifact):
    _, _, _, env, cache, scratch = launcher
    assert result.returncode == 0, result.stderr
    assert artifact.is_file()
    assert "worklink_sccache_fallback" not in result.stderr
    launch = json.loads((cache / "launch.json").read_text())
    assert len(os.fsencode(launch["socket"])) < 108
    assert launch["tmp"] != env["TMPDIR"]
    assert launch["mode"] == 0o700
    assert launch["uid"] == os.geteuid()
    # The server inherits TMPDIR, so its root must survive client exit.
    assert scratch.is_dir()
    assert cache.stat().st_uid == os.geteuid()
    assert stat.S_IMODE(cache.stat().st_mode) == 0o700
    # An unrelated tool launched in the same session still sees the private tmp.
    other = subprocess.run(
        [sys.executable, "-c", "import os; print(os.environ['TMPDIR'])"],
        env=env, capture_output=True, text=True, check=True,
    )
    assert other.stdout.strip() == env["TMPDIR"]


def test_sccache_long_session_tmpdir_socket_bound(launcher, tmp_path):
    command, artifact = compile_command(launcher, tmp_path)
    result = subprocess.run(command, env=launcher[3], capture_output=True, text=True)
    assert_cached_compile(launcher, result, artifact)


@pytest.mark.parametrize("diagnostic", [False, True], ids=["rust-gate", "focused-evidence"])
async def test_sccache_gate_and_evidence_use_same_launcher(launcher, tmp_path, diagnostic):
    command, artifact = compile_command(launcher, tmp_path)
    env = launcher[3]

    class Compute:
        async def _launch_contained(self, spec, argv):
            # The executor supplies HOME/TMPDIR, not the controller. Verify the
            # real gate/evidence planner retains the Rust environment settings.
            assert spec.env["RUSTC_WRAPPER"] == env["RUSTC_WRAPPER"]
            assert spec.env["SCCACHE_DIR"] == env["SCCACHE_DIR"]
            self.result = subprocess.run(argv, env=spec.env, capture_output=True, text=True)
            return LaunchHandle("fixture", "gate")

        async def wait(self, handle, timeout_s):
            return ComputeResult(self.result.returncode, self.result.stdout, self.result.stderr)

        async def cleanup(self, handle):
            pass

    spec = WorkSpec(
        issue_id=1, attempt=1, repo_url="fixture", base_ref="main", branch="fixture",
        prompt="", rules=None, test_command="", backend="opencode", timeout_s=30,
        env=env, backend_config={"pass_env": tuple(env)},
    )
    result = await evidence._run_compute_gate_command(
        shlex.join(command), checkout=tmp_path, work_spec=spec, compute=Compute(), diagnostic=diagnostic,
    )
    observed = subprocess.CompletedProcess(command, result.exit_code, result.stdout, result.stderr)
    assert_cached_compile(launcher, observed, artifact)


@pytest.mark.parametrize("failure", ["missing", "startup"])
def test_sccache_fallback_compiles_and_warns_with_cause(launcher, tmp_path, failure):
    _, raw, _, env, cache, _ = launcher
    if failure == "missing":
        raw.unlink()
        expected = "FileNotFoundError"
    else:
        executable(raw, """
import sys
print('sccache: error: path must be shorter than SUN_LEN', file=sys.stderr)
sys.exit(2)
""")
        expected = "path must be shorter than SUN_LEN"
    command, artifact = compile_command(launcher, tmp_path)
    before = cache.stat()
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    compiled = json.loads(artifact.read_text())
    assert compiled["tmp"] == env["TMPDIR"]
    assert compiled["argv"] == command[2:]
    warning = next(json.loads(line) for line in result.stderr.splitlines() if line.startswith("{"))
    assert warning["event"] == "worklink_sccache_fallback"
    assert warning["level"] == "warning"
    assert expected in warning["cause"]
    assert (cache.stat().st_uid, cache.stat().st_mode) == (before.st_uid, before.st_mode)


def test_sccache_fallback_preserves_compiler_failure(launcher, tmp_path):
    _, raw, compiler, env, _, _ = launcher
    raw.unlink()
    result = subprocess.run([env["RUSTC_WRAPPER"], str(compiler), "--fail"],
                            env=env, capture_output=True, text=True)
    assert result.returncode == 17
    assert "fixture compiler rejected the source" in result.stderr


def test_sccache_unsafe_tmp_directory_falls_back_without_using_cache(launcher, tmp_path):
    _, _, _, env, cache, scratch = launcher
    scratch.chmod(0o777)
    command, artifact = compile_command(launcher, tmp_path)
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(artifact.read_text())["tmp"] == env["TMPDIR"]
    assert not (cache / "launch.json").exists()
    warning = json.loads(result.stderr)
    assert warning["event"] == "worklink_sccache_fallback"
    assert "private and worker-owned" in warning["cause"]
    assert stat.S_IMODE(cache.stat().st_mode) == 0o700


def test_sccache_admin_version_uses_pinned_binary(launcher):
    wrapper, _, _, env, _, _ = launcher
    result = subprocess.run([str(wrapper), "--version"], env=env, capture_output=True, text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == "sccache 0.20.1"
    assert not result.stderr


@pytest.mark.parametrize("args", [[], ["--version"], ["--start-server"], ["--show-stats"]])
def test_sccache_admin_failure_never_falls_through(launcher, args):
    wrapper, raw, _, env, _, _ = launcher
    raw.unlink()
    result = subprocess.run([str(wrapper), *args], env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert "worklink_sccache_fallback" not in result.stderr
    assert "FileNotFoundError" in result.stderr


@pytest.mark.parametrize("failure", ["symlink", "owner", "group", "other", "file"])
def test_sccache_tmp_directory_guard_fails_closed(tmp_path, monkeypatch, failure):
    namespace = runpy.run_path(str(SOURCE))
    scratch = tmp_path / "scratch"
    scratch.mkdir(mode=0o700)
    if failure == "symlink":
        link = tmp_path / "link"
        link.symlink_to(scratch, target_is_directory=True)
        scratch = link
    elif failure == "owner":
        uid = os.geteuid()
        monkeypatch.setattr(os, "geteuid", lambda: uid + 1)
    elif failure == "file":
        scratch.rmdir()
        scratch.write_text("")
        scratch.chmod(0o700)
    else:
        scratch.chmod(0o770 if failure == "group" else 0o707)
    run_cache = namespace["run_cache"]
    run_cache.__globals__["SCCACHE_TMPDIR"] = str(scratch)
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: calls.append(a) or subprocess.CompletedProcess(a, 0))
    with pytest.raises(PermissionError, match="private and worker-owned"):
        run_cache(["rustc"])
    assert not calls
