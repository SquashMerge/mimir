from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import zipfile

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from mimir import server as core_server
from mimir_cultivate import install_identity as identity
from mimir_cultivate import server as cultivate_server
from mimir_cultivate.worklink import worker_client


@pytest.fixture
def installed_pair(tmp_path):
    wheels = tmp_path / "wheels"
    site = tmp_path / "site"
    wheels.mkdir()
    site.mkdir()
    pair = {}
    for name in identity.PROJECTS:
        stem = name.replace("-", "_")
        prefix = f"{stem}-1.2.dist-info"
        files = {
            f"{stem}/__init__.py": b"installed package\n",
            f"{prefix}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.2\n".encode(),
        }
        rows = []
        for path, data in files.items():
            target = site / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            rows.append((path, f"sha256={digest}", str(len(data))))
        record = f"{prefix}/RECORD"
        rows.append((record, "", ""))
        text = io.StringIO()
        csv.writer(text, lineterminator="\n").writerows(rows)
        files[record] = text.getvalue().encode()
        (site / record).write_bytes(files[record])
        filename = f"{stem}-1.2-py3-none-any.whl"
        with zipfile.ZipFile(wheels / filename, "w") as archive:
            for path, data in files.items():
                archive.writestr(path, data)
        pair[name] = {"version": "1.2", "wheel": filename,
                      "sha256": hashlib.sha256((wheels / filename).read_bytes()).hexdigest()}
    marker = tmp_path / "identity.json"
    declared = {"mode": "pypi", "distributions": pair}
    marker.write_text(json.dumps(declared))
    identity.write_startup_hooks(tmp_path / "hooks.json", site=site)
    return marker, wheels, site, declared


@pytest.fixture(params=["uv", "pip"])
def installed_wheels(tmp_path, request):
    """Install built wheels into an isolated venv using the production installer."""
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    for name in identity.PROJECTS:
        project = tmp_path / name
        project.mkdir()
        module = name.replace("-", "_")
        (project / module).mkdir()
        (project / module / "__init__.py").write_text("def main():\n    return 0\n")
        script = (f'\n[project.scripts]\nmimir = "{module}:main"\n'
                  f'mimir-agent = "{module}:main"\n') if name == "mimir-agent" else ""
        (project / "pyproject.toml").write_text(
            f'[build-system]\nrequires = ["hatchling"]\nbuild-backend = "hatchling.build"\n'
            f'\n[project]\nname = "{name}"\nversion = "1.2"\n{script}'
        )
        subprocess.run(["uv", "build", "--wheel", "--out-dir", str(wheels), str(project)], check=True)
    venv = tmp_path / "venv"
    if request.param == "uv":
        subprocess.run(["uv", "venv", "--python", sys.executable, str(venv)], check=True)
    else:
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = venv / "bin/python"
    installer = (["uv", "pip", "install", "--python", str(python)] if request.param == "uv"
                 else [str(python), "-m", "pip", "install", "--no-compile"])
    subprocess.run([*installer, "--no-deps",
                    *(str(wheel) for wheel in sorted(wheels.glob("*.whl")))], check=True)
    site = Path(subprocess.check_output([str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True).strip())
    pair = {name: {"version": "1.2", "wheel": wheel.name,
                   "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()}
            for name in identity.PROJECTS
            for wheel in wheels.glob(f"{name.replace('-', '_')}-*.whl")}
    declared = {"mode": "pypi", "distributions": pair}
    marker = tmp_path / "identity.json"
    marker.write_text(json.dumps(declared))
    identity.write_startup_hooks(tmp_path / "hooks.json", site=site)
    return marker, wheels, site, declared


def test_installed_wheel_scripts_are_verified(installed_wheels):
    marker, wheels, site, declared = installed_wheels
    record = site / "mimir_agent-1.2.dist-info/RECORD"
    assert "../../../bin/mimir" in record.read_text()
    assert identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json") == declared
    script = site.parents[2] / "bin/mimir"
    script.write_text("tampered\n")
    with pytest.raises(identity.InstallIdentityError, match="console script drift"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.parametrize("shell_kind", ["interactive", "direct"])
def test_shell_cli_preserves_installed_identity(installed_wheels, monkeypatch, shell_kind):
    from mimir.tools import _shell_env
    import shlex

    marker, wheels, site, declared = installed_wheels
    venv = site.parents[2]
    python = venv / "bin/python"
    cli = venv / "bin/mimir"
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    monkeypatch.setattr(_shell_env.sys, "executable", str(python))
    if shell_kind == "interactive":
        env = _shell_env.interactive_shell_env()
        command = ["/bin/bash", "-lc", _shell_env.login_shell_command(shlex.quote(str(cli)))]
    else:
        command = [str(cli)]
        env = _shell_env.direct_exec_env(command)
    assert env.get("PYTHONDONTWRITEBYTECODE") == "1"
    assert identity.verify_install_identity(marker, wheels=wheels, site=site,
                                          hooks_manifest=marker.parent / "hooks.json") == declared
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert not list((site / "mimir_agent").rglob("*.pyc"))
    assert identity.verify_install_identity(marker, wheels=wheels, site=site,
                                          hooks_manifest=marker.parent / "hooks.json") == declared
    # Discriminating control: the same real installed CLI without the policy
    # does write bytecode in this writable venv and makes identity fail closed.
    control = subprocess.run([str(cli)], env={"PATH": env["PATH"]},
                             capture_output=True, text=True, timeout=30)
    assert control.returncode == 0, control.stderr
    assert list((site / "mimir_agent").rglob("*.pyc"))
    with pytest.raises(identity.InstallIdentityError, match="bytecode drift"):
        identity.verify_install_identity(marker, wheels=wheels, site=site,
                                         hooks_manifest=marker.parent / "hooks.json")


def test_readonly_pair_import_preserves_identity_with_scrubbed_launcher(installed_wheels):
    import os
    import stat

    if os.geteuid() == 0:
        pytest.skip('root bypasses read-only mode bits; live image proof uses mimir uid')
    marker, wheels, site, declared = installed_wheels
    cli = site.parents[2] / 'bin/mimir'
    roots = [site / name.replace('-', '_') for name in identity.PROJECTS]
    saved = {path: path.stat().st_mode for root in roots for path in (root, *root.rglob('*'))}
    assert all(path.stat().st_mode & stat.S_IWUSR for path in roots)
    try:
        for path in saved:
            path.chmod(saved[path] & ~0o222)
        result = subprocess.run([str(cli)], env={'PATH': '/usr/bin:/bin'},
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        assert all(not list(root.rglob('*.pyc')) for root in roots)
        assert identity.verify_install_identity(marker, wheels=wheels, site=site,
                                              hooks_manifest=marker.parent / 'hooks.json') == declared
    finally:
        for path, mode in saved.items():
            path.chmod(mode)
    # Same CLI and scrubbed env, with writable package trees: prove the
    # filesystem policy, not inherited no-bytecode settings, prevents drift.
    subprocess.run([str(cli)], env={'PATH': '/usr/bin:/bin'}, check=True, timeout=30)
    assert list(roots[0].rglob('*.pyc'))
    with pytest.raises(identity.InstallIdentityError, match='bytecode drift'):
        identity.verify_install_identity(marker, wheels=wheels, site=site,
                                         hooks_manifest=marker.parent / 'hooks.json')


def test_installed_wheel_requires_each_generated_script(installed_wheels):
    marker, wheels, site, _ = installed_wheels
    record = site / "mimir_agent-1.2.dist-info/RECORD"
    rows = list(csv.reader(io.StringIO(record.read_text())))
    with record.open("w") as stream:
        csv.writer(stream).writerows(row for row in rows if row[0] != "../../../bin/mimir-agent")
    with pytest.raises(identity.InstallIdentityError, match="console script missing"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.parametrize("entry", ["../../../bin/other", "../../../../bin/mimir", "../../../bin/../bin/mimir"])
def test_installed_wheel_rejects_unrelated_parent_paths(installed_wheels, entry):
    marker, wheels, site, _ = installed_wheels
    with (site / "mimir_agent-1.2.dist-info/RECORD").open("a") as record:
        record.write(f"{entry},,\n")
    with pytest.raises(identity.InstallIdentityError, match="unsafe wheel RECORD path"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.asyncio
async def test_real_installed_pair_http_health(installed_wheels, tmp_path, monkeypatch):
    marker, wheels, site, declared = installed_wheels
    controller_marker = tmp_path / identity.CONTROLLER_MARKER
    controller_marker.write_bytes(marker.read_bytes())
    monkeypatch.setattr(cultivate_server.sys, "prefix", str(tmp_path))
    monkeypatch.setattr(identity, "controller_identity",
                        lambda: identity.verify_install_identity(controller_marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json"))
    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)

    async def executor():
        return identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")

    monkeypatch.setattr(worker_client, "verify_executor_identity", executor)
    monkeypatch.setattr(core_server, "_server_hooks", lambda policy, kind: {
        "capture_source": (cultivate_server.capture_controller_source_commit,),
        "health": (cultivate_server.executor_health,),
    }.get(kind, ()))
    app = web.Application()
    app["plugin_manager"] = SimpleNamespace(policy=None, plugins=())
    app["check_worker_executor_health"] = True
    app.on_startup.append(core_server._capture_controller_source_commit)
    app.router.add_get("/health", core_server._handle_health)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/health")
        assert response.status == 200
        assert (await response.json())["worklink_controller_distributions"] == declared["distributions"]
        (site.parents[2] / "bin/mimir").write_text("tampered\n")
        response = await client.get("/health")
        body = await response.json()
        assert response.status == 503
        assert body["ok"] is False
        assert body["worklink_install_status"] == "mismatched"


@pytest.mark.parametrize("relative", ["mimir_agent/backdoor.py", "mimir_cultivate/extra.py",
                                     "evil.pth", "sitecustomize.py", "usercustomize.py"])
def test_unrecorded_executable_files_fail_closed(installed_pair, relative):
    marker, wheels, site, _ = installed_pair
    assert identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")
    target = site / relative
    target.parent.mkdir(exist_ok=True)
    target.write_text("# injected without touching RECORD\n")
    with pytest.raises(identity.InstallIdentityError, match="unrecorded"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.parametrize("record_kind", ["hashed", "unhashed", "unrecorded"])
def test_first_party_bytecode_is_always_drift(installed_pair, record_kind):
    marker, wheels, site, _ = installed_pair
    import importlib.util
    import marshal
    source = site / "mimir_agent/__init__.py"
    cache = source.parent / "__pycache__" / f"__init__.cpython-{sys.version_info.major}{sys.version_info.minor}.pyc"
    cache.parent.mkdir()
    # A realistic timestamp header plus executable payload; identity must check
    # all bytes, not rely on import invalidation or the source digest.
    header = importlib.util.MAGIC_NUMBER + b'\x00' * 12
    cache.write_bytes(header + marshal.dumps(compile('value = 1', str(source), 'exec')))
    digest = base64.urlsafe_b64encode(hashlib.sha256(cache.read_bytes()).digest()).rstrip(b"=").decode()
    if record_kind != "unrecorded":
        with (site / "mimir_agent-1.2.dist-info/RECORD").open("a") as stream:
            csv.writer(stream).writerow((cache.relative_to(site).as_posix(),
                                        "sha256=" + digest if record_kind == "hashed" else "",
                                        cache.stat().st_size if record_kind == "hashed" else ""))
    with pytest.raises(identity.InstallIdentityError, match="bytecode drift"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.parametrize("hook", ["evil.pth", "sitecustomize.py", "usercustomize.py"])
def test_forged_dependency_record_cannot_authorize_hooks(installed_pair, hook):
    marker, wheels, site, _ = installed_pair
    target = site / hook
    target.write_text("# forged hook\n")
    fake = site / "evil-1.0.dist-info"
    fake.mkdir()
    (fake / "METADATA").write_text("Name: evil\nVersion: 1.0\n")
    digest = base64.urlsafe_b64encode(hashlib.sha256(target.read_bytes()).digest()).rstrip(b"=").decode()
    with (fake / "RECORD").open("w") as stream:
        csv.writer(stream).writerow((hook, "sha256=" + digest, target.stat().st_size))
    with pytest.raises(identity.InstallIdentityError, match="unrecorded or altered"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.parametrize("mutation", ["changed", "deleted", "missing_allowlist"])
def test_build_time_hook_allowlist_pins_exact_bytes(installed_pair, mutation):
    marker, wheels, site, _ = installed_pair
    hook = site / "_virtualenv.pth"
    hook.write_text("import _virtualenv\n")
    manifest = marker.parent / "hooks.json"
    # The fixture captured an empty build inventory and locked it read-only.
    # This test owns the simulated build step, before runtime tampering.
    manifest.chmod(0o644)
    identity.write_startup_hooks(manifest, site=site)
    assert identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=manifest)
    if mutation == "changed":
        hook.write_text("import evil\n")
    elif mutation == "deleted":
        hook.unlink()
    else:
        manifest.unlink()
    with pytest.raises(identity.InstallIdentityError, match="startup hook"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=manifest)


def test_package_symlink_is_rejected_even_with_matching_bytes(installed_pair):
    marker, wheels, site, _ = installed_pair
    package = site / "mimir_agent"
    saved = site / "saved"
    package.rename(saved)
    package.symlink_to(saved, target_is_directory=True)
    with pytest.raises(identity.InstallIdentityError, match="package symlink"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


def test_verified_pair_requires_archives_and_installed_record(installed_pair):
    marker, wheels, site, declared = installed_pair
    assert identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json") == declared


@pytest.mark.parametrize("mutation", ["marker", "mode", "hash", "version", "wheel", "record", "record_drift", "content", "missing_archive", "archive_drift"])
def test_identity_fails_closed(installed_pair, mutation):
    marker, wheels, site, declared = installed_pair
    agent = declared["distributions"]["mimir-agent"]
    if mutation == "marker":
        marker.unlink()
    elif mutation == "mode":
        declared["mode"] = "source"
    elif mutation == "hash":
        agent["sha256"] = "0" * 64
    elif mutation == "version":
        agent["version"] = "2.0"
    elif mutation == "wheel":
        agent["wheel"] = "mimir_cultivate-1.2-py3-none-any.whl"
    elif mutation == "record":
        (site / "mimir_agent-1.2.dist-info/RECORD").unlink()
    elif mutation == "record_drift":
        (site / "mimir_agent-1.2.dist-info/RECORD").write_text("mimir_agent/__init__.py,,\n")
    elif mutation == "content":
        (site / "mimir_agent/__init__.py").write_text("tampered")
    elif mutation == "missing_archive":
        (wheels / agent["wheel"]).unlink()
    else:
        with (wheels / agent["wheel"]).open("ab") as stream:
            stream.write(b"extra")
    if mutation in {"mode", "hash", "version", "wheel"}:
        marker.write_text(json.dumps(declared))
    with pytest.raises(identity.InstallIdentityError):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


def test_installed_record_rejects_extra_module_but_allows_installer_metadata(installed_pair):
    marker, wheels, site, _ = installed_pair
    record = site / "mimir_agent-1.2.dist-info/RECORD"
    with record.open("a") as stream:
        stream.write("mimir_agent-1.2.dist-info/INSTALLER,,\n")
    assert identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")
    with record.open("a") as stream:
        stream.write("mimir_agent/backdoor.py,,\n")
    with pytest.raises(identity.InstallIdentityError, match="unexpected installed RECORD"):
        identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [
    "marker", "archive_unreadable", "metadata", "metadata_error", "record", "content", "extra_module",
])
async def test_health_rejects_corrupt_installed_evidence(installed_pair, tmp_path, monkeypatch, mutation):
    marker, wheels, site, declared = installed_pair
    controller_marker = tmp_path / identity.CONTROLLER_MARKER
    controller_marker.write_bytes(marker.read_bytes())
    monkeypatch.setattr(cultivate_server.sys, "prefix", str(tmp_path))
    monkeypatch.setattr(
        identity, "controller_identity",
        lambda: identity.verify_install_identity(controller_marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json"),
    )
    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)

    async def executor():
        return identity.verify_install_identity(marker, wheels=wheels, site=site, hooks_manifest=marker.parent / "hooks.json")

    monkeypatch.setattr(worker_client, "verify_executor_identity", executor)
    monkeypatch.setattr(core_server, "_server_hooks", lambda policy, kind: {
        "capture_source": (cultivate_server.capture_controller_source_commit,),
        "health": (cultivate_server.executor_health,),
    }.get(kind, ()))
    app = web.Application()
    app["plugin_manager"] = SimpleNamespace(policy=None, plugins=())
    app["check_worker_executor_health"] = True
    app.on_startup.append(core_server._capture_controller_source_commit)
    app.router.add_get("/health", core_server._handle_health)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/health")
        assert response.status == 200
        assert (await response.json())["worklink_install_status"] == "matched"
        if mutation == "marker":
            controller_marker.write_text("not json")
        elif mutation == "archive_unreadable":
            archive = wheels / declared["distributions"]["mimir-agent"]["wheel"]
            digest = identity._digest

            def unreadable(path):
                if path == archive:
                    raise PermissionError("archive denied")
                return digest(path)

            monkeypatch.setattr(identity, "_digest", unreadable)
        elif mutation == "metadata":
            (site / "mimir_agent-1.2.dist-info/METADATA").write_bytes(b"\xff")
        elif mutation == "metadata_error":
            def malformed_metadata(*, path):
                raise TypeError("invalid distribution metadata")

            monkeypatch.setattr(identity.metadata, "distributions", malformed_metadata)
        elif mutation == "record":
            (site / "mimir_agent-1.2.dist-info/RECORD").write_text("invalid\n")
        elif mutation == "content":
            (site / "mimir_agent/__init__.py").write_text("changed")
        else:
            with (site / "mimir_agent-1.2.dist-info/RECORD").open("a") as stream:
                stream.write("mimir_agent/backdoor.py,,\n")
        response = await client.get("/health")
        body = await response.json()
        assert response.status == 503
        assert body["ok"] is False
        assert body["worklink_install_status"] == "mismatched"
