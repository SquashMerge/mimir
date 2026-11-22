from __future__ import annotations

import importlib.metadata
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from tests.uv_subprocess import isolated_uv_env

from mimir.plugin_dockerfile import cultivate_scripts, has_trusted_cultivate, services
from mimir.scaffold_docker import render_dockerfile, scaffold


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "plugins/cultivate/mimir_cultivate/docker"
SCRIPT_NAMES = ("base.sh", "users.sh", "executor.sh", "controller.sh", "bootstrap.sh", "service.sh",
                "worklink-opencode-service", "worklink-daemon-gate", "worklink-opencode-login")


def _candidate(*, name: str = "cultivate", distribution: str = "mimir-cultivate",
               missing: str | None = None):
    files = [Path("mimir-plugin.toml")]
    files.extend(Path("mimir_cultivate/docker") / path.name for path in SCRIPTS.iterdir()
                 if path.is_file() and path.name != missing)
    dist = SimpleNamespace(version="0.9.10", files=files,
                           locate_file=lambda path: ROOT / "plugins/cultivate" / path)
    return SimpleNamespace(name=name, distribution=distribution,
                           manifest=SimpleNamespace(dockerfile={}),
                           entry_point=SimpleNamespace(dist=dist))


def test_selected_cultivate_uses_only_inventoried_first_party_scripts() -> None:
    candidate = _candidate()
    assert has_trusted_cultivate((candidate,))
    scripts = cultivate_scripts(candidate)
    assert set(scripts) == set(SCRIPT_NAMES)
    assert 'origin "$MIMIR_EXECUTOR_COMMIT"' in scripts["executor.sh"]
    assert 'origin "$MIMIR_CONTROLLER_COMMIT"' in scripts["controller.sh"]
    assert 'mimir_cultivate.worklink.worker_exec' in scripts["service.sh"]
    assert not services((candidate,)), "the reserved worker service is not a generic manifest service"


def test_built_wheel_and_installed_inventory_contain_exact_scripts(tmp_path: Path) -> None:
    env = isolated_uv_env(tmp_path)
    subprocess.run(["uv", "build", "--package", "mimir-cultivate", "--wheel",
                    "--out-dir", str(tmp_path)], cwd=ROOT, check=True, capture_output=True, text=True,
                   env=env)
    wheel, = tmp_path.glob("mimir_cultivate-*.whl")
    with ZipFile(wheel) as archive:
        for name in SCRIPT_NAMES:
            relative = f"mimir_cultivate/docker/{name}"
            assert archive.read(relative) == (SCRIPTS / name).read_bytes()
    target = tmp_path / "installed"
    subprocess.run(["uv", "pip", "install", "--target", str(target), "--no-deps", str(wheel)],
                   check=True, capture_output=True, text=True, env=env)
    dist, = (dist for dist in importlib.metadata.distributions(path=[str(target)])
             if dist.metadata["Name"] == "mimir-cultivate")
    candidate = SimpleNamespace(name="cultivate", distribution="mimir-cultivate",
                                entry_point=SimpleNamespace(dist=dist))
    assert cultivate_scripts(candidate) == {name: (SCRIPTS / name).read_text() for name in SCRIPT_NAMES}
    for name in SCRIPT_NAMES:
        assert dist.locate_file(f"mimir_cultivate/docker/{name}").read_bytes() == (SCRIPTS / name).read_bytes()


def test_core_wheel_carries_unchanged_lease_helper(daemon_packages):
    _, wheels, _, _ = daemon_packages
    with ZipFile(wheels["mimir-agent"]) as archive:
        for name in ("heavy_run.sh", "heavy_run.py"):
            assert archive.read(f"mimir/heavy_run/{name}") == (ROOT / "scripts" / name).read_bytes()
    with ZipFile(wheels["mimir-cultivate"]) as archive:
        for name in ("pytest", "worklink_pytest.py", "worklink_pytest_args.py", "sitecustomize.py"):
            assert archive.read(f"mimir_cultivate/docker/pytest_guard/{name}") == (
                SCRIPTS / "pytest_guard" / name).read_bytes()


def test_missing_resource_or_untrusted_provider_cannot_get_privileged_slot() -> None:
    with pytest.raises(ValueError, match="resource is missing"):
        cultivate_scripts(_candidate(missing="executor.sh"))
    for candidate in (_candidate(distribution="third-party"), _candidate(name="impostor")):
        assert not has_trusted_cultivate((candidate,))
        with pytest.raises(ValueError, match="Cultivate distribution"):
            cultivate_scripts(candidate)
        assert "/opt/cultivate-docker" not in render_dockerfile([], mode="pypi", plugins=(candidate,))


@pytest.mark.parametrize("mode", ["workspace", "pypi"])
def test_selected_scaffold_composes_fixed_worker_and_supervisor(mode: str) -> None:
    try:
        text = render_dockerfile([], mode=mode, plugins=(_candidate(),))
    except ValueError as exc:
        assert mode == "pypi" and "cultivate setup" in str(exc).lower()
        return
    assert "RUN sh /opt/cultivate-docker/users.sh" in text
    assert 'RUN groupadd --gid "${USER_GID}" mimir' not in text
    assert 'ARG MIMIR_EXECUTOR_COMMIT' in text
    assert 'MIMIR_EXTRAS="$MIMIR_EXTRAS" sh /opt/cultivate-docker/executor.sh' in text
    # Cultivate is never installed from an index in either venv.
    assert 'mimir-cultivate==' not in text
    # pypi mode builds core + plugin for the controller venv from the pinned
    # commit; workspace mode's controller is the boot clone (start.sh).
    assert ('sh /opt/cultivate-docker/controller.sh' in text) is (mode == "pypi")
    if mode == "pypi":
        assert text.count("ARG MIMIR_EXTRAS") == 1, "a second default would replace the scaffold's extras"
    else:
        # The workspace template stays root for the following COPY + chmod.
        executor = text.index("sh /opt/cultivate-docker/executor.sh")
        assert "USER mimir" not in text[executor:text.index("RUN chmod +x /usr/local/bin/start.sh")]
    assert "COPY .mimir-scaffold/s6-rc.d/ /etc/s6-overlay/s6-rc.d/" in text
    assert 'ENTRYPOINT ["/init"]' in text
    assert 'ARG MIMIR_CODING_ENABLED' not in text


@pytest.mark.parametrize("mode", ["workspace", "pypi"])
def test_selected_scaffold_writes_worker_resources(tmp_path: Path, monkeypatch, mode: str) -> None:
    monkeypatch.setattr("mimir.scaffold_docker.select_plugins", lambda **kwargs: (_candidate(),))
    try:
        scaffold(tmp_path, mode=mode)
    except ValueError as exc:
        assert mode == "pypi" and "cultivate setup" in str(exc).lower()
        return
    context = tmp_path / ".mimir-scaffold"
    assert (context / "cultivate-docker/executor.sh").read_text() == (
        SCRIPTS / "executor.sh").read_text()
    assert (context / "s6-rc.d/user/contents.d/worklink-execd").is_file()
    assert (context / "s6-rc.d/worklink-execd/run").read_text().rstrip().endswith(
        "exec /usr/local/libexec/worklink-execd")
    for run in (context / "s6-rc.d").glob("*/run"):
        script = run.read_text()
        assert script.index("umask 022") < script.index("exec ")
        if "install " in script:
            assert script.index("umask 022") < script.index("install ")
    dockerfile = (tmp_path / "Dockerfile").read_text()
    assert "RUN sh /opt/cultivate-docker/service.sh" in dockerfile
    assert 'ENTRYPOINT ["/init"]' in dockerfile
    assert "MIMIR_ENABLE_OPENCODE" in (tmp_path / "compose.yml").read_text()
    start = (tmp_path / "start.sh").read_text()
    assert 'resolve_activation(dict(os.environ))' not in start
    assert 'worklink-daemon-gate' not in (context / "s6-rc.d/mimir/run").read_text()
    assert "MIMIR_CODING_ENABLED" not in start
    assert "mimir-cultivate==" not in start
    if mode == "workspace":
        # Every sync keeps the workspace member; a plain sync would remove it.
        assert 'UV_EXTRAS=" --all-packages"' in start or 'UV_EXTRAS="--all-packages"' in start
        assert "uv sync $UV_EXTRAS\n" in start and "uv sync $UV_EXTRAS $SKILL_EXTRAS" in start
        assert "uv pip install" not in start
        assert start.index("uv sync $UV_EXTRAS\n") < start.index('m.version("mimir-cultivate")') < start.index(
            "exec uv run --no-sync mimir run")


@pytest.mark.parametrize("mode", ["root", "pypi"])
def test_build_bootstrap_selects_provider_only_for_invocation(tmp_path: Path, mode: str) -> None:
    try:
        dockerfile = ((ROOT / "Dockerfile").read_text() if mode == "root"
                      else render_dockerfile([], mode="pypi", plugins=(_candidate(),)))
    except ValueError as exc:
        assert mode == "pypi" and "cultivate setup" in str(exc).lower()
        return
    assert "ENV MIMIR_PLUGINS=cultivate" not in dockerfile
    assert 'MIMIR_PLUGINS=cultivate MIMIR_ENABLE_OPENCODE="$MIMIR_ENABLE_OPENCODE"' in dockerfile
    assert dockerfile.index("sh /opt/cultivate-docker/controller.sh") < dockerfile.index(
        'MIMIR_PLUGINS=cultivate MIMIR_ENABLE_OPENCODE="$MIMIR_ENABLE_OPENCODE"')
    stub = tmp_path / "mimir"
    stub.write_text('#!/bin/sh\nprintf "%s:%s\\n" "$MIMIR_PLUGINS" "$*" >> "$BOOT_LOG"\n')
    stub.chmod(0o755)
    output = tmp_path / "bootstrap.log"
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "BOOT_LOG": str(output)}
    for enabled in ("0", "1"):
        subprocess.run(["sh", str(SCRIPTS / "bootstrap.sh")], check=True,
                       env={**env, "MIMIR_PLUGINS": "cultivate", "MIMIR_ENABLE_OPENCODE": enabled})
    assert not output.exists(), "image bootstrap must not seed agent OpenCode state"


def test_actual_selected_cli_bootstraps_and_missing_provider_fails(tmp_path: Path) -> None:
    env = {**os.environ, "MIMIR_PLUGINS": "cultivate"}
    result = subprocess.run([sys.executable, "-m", "mimir.cli", "opencode-bootstrap", "--home",
                             str(tmp_path / "home")], cwd=ROOT, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "home").exists()
    assert "codex login --device-auth" in result.stdout
    assert "automatically seeds and reconciles" in result.stdout

    code = ("import importlib.metadata as metadata; _real = metadata.entry_points; metadata.entry_points = lambda **kw: () if kw.get('group') == 'mimir.plugins' else _real(**kw); "
            "from mimir.cli import main; main()")
    missing = subprocess.run([sys.executable, "-c", code, "opencode-bootstrap", "--home", str(tmp_path)],
                             cwd=ROOT, env=env, capture_output=True, text=True)
    assert missing.returncode != 0
    assert "MIMIR_PLUGINS: missing plugin(s): cultivate" in missing.stderr
    assert "`uv sync --all-packages`" in missing.stderr
    assert "--build-arg MIMIR_PLUGIN_SELECTION=cultivate" in missing.stderr
    assert "--build-arg CHAINLINK_ARTIFACT_STAGE=chainlink-build" in missing.stderr


@pytest.mark.parametrize("mode", ["root", "workspace", "pypi"])
def test_shared_service_script_provisions_each_layout(tmp_path: Path, monkeypatch, mode: str) -> None:
    if mode != "root":
        monkeypatch.setattr("mimir.scaffold_docker.select_plugins", lambda **kwargs: (_candidate(),))
        home = tmp_path / "home"
        home.mkdir()
        try:
            scaffold(home, mode=mode)
        except ValueError as exc:
            assert mode == "pypi" and "cultivate setup" in str(exc).lower()
            return
        assert "RUN sh /opt/cultivate-docker/service.sh" in (home / "Dockerfile").read_text()
        assert (home / ".mimir-scaffold/s6-rc.d/worklink-execd/run").is_file()
    else:
        assert "sh /opt/cultivate-docker/service.sh" in (ROOT / "Dockerfile").read_text()

    s6 = tmp_path / "s6-rc.d"
    source_layout = (ROOT / "deploy/s6-overlay/s6-rc.d" if mode == "root"
                     else home / ".mimir-scaffold/s6-rc.d")
    for name in ("mimir", "worklink-execd", "worklink-opencode") + (("watchdog",) if mode == "root" else ()):
        run = s6 / name / "run"
        run.parent.mkdir(parents=True)
        run.write_bytes((source_layout / name / "run").read_bytes())
        run.chmod(0o644)
    libexec = tmp_path / "libexec"
    bindir = tmp_path / "bin"
    bindir.mkdir()
    resources = tmp_path / "resources"
    resources.mkdir()
    for name in ("worklink-opencode-service", "worklink-daemon-gate", "worklink-opencode-login"):
        (resources / name).write_bytes((SCRIPTS / name).read_bytes())
    (resources / "opencode-enabled").write_text("1\n")
    (resources / "worklink-host").write_text("opencode\n")
    source = (SCRIPTS / "service.sh").read_text()
    source = source.replace("/etc/s6-overlay/s6-rc.d", str(s6)).replace(
        "/usr/local/libexec", str(libexec)).replace("/usr/local/bin", str(bindir)).replace(
        "/opt/cultivate-docker", str(resources)).replace("-o root -g root ", "")
    subprocess.run(["sh", "-c", source], check=True)
    for name in ("mimir", "worklink-execd", "worklink-opencode") + (("watchdog",) if mode == "root" else ()):
        assert (s6 / name / "run").stat().st_mode & stat.S_IXUSR
    assert (libexec / "worklink-execd").stat().st_mode & stat.S_IXUSR
    assert "mimir_cultivate.worklink.worker_exec" in (libexec / "worklink-execd").read_text()
    assert (libexec / "worklink-execd").read_text().splitlines()[1] == "umask 022"
    for name in ("worklink-opencode-service", "worklink-daemon-gate"):
        assert (libexec / name).read_bytes() == (SCRIPTS / name).read_bytes()
        assert stat.S_IMODE((libexec / name).stat().st_mode) == 0o755
    assert (bindir / "worklink-opencode-login").read_bytes() == (SCRIPTS / "worklink-opencode-login").read_bytes()
    assert stat.S_IMODE((bindir / "worklink-opencode-login").stat().st_mode) == 0o755


@pytest.fixture
def daemon_packages(tmp_path: Path):
    import hashlib

    from mimir.plugin_dockerfile import _S6_FILES, _CONTEXT_SCRIPTS, build_pypi_context
    from mimir_cultivate.setup import _BUILD_CONTEXT_HELPER

    env = isolated_uv_env(tmp_path)
    # uv builds each wheel from its freshly built sdist, not the checkout.
    for package in ("mimir-agent", "mimir-cultivate"):
        subprocess.run(["uv", "build", "--package", package, "--out-dir", str(tmp_path)],
                       cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    wheels = {package: next(tmp_path.glob(package.replace("-", "_") + "-*.whl"))
              for package in ("mimir-agent", "mimir-cultivate")}
    resources = [("mimir-agent", f"mimir/docker_context/deploy/s6-overlay/s6-rc.d/{name}",
                  f"deploy/s6-overlay/s6-rc.d/{name}") for name in _S6_FILES]
    resources += [("mimir-cultivate", f"mimir_cultivate/docker/{name}",
                   f"plugins/cultivate/mimir_cultivate/docker/{name}") for name in _CONTEXT_SCRIPTS]
    version = wheels["mimir-agent"].name.split("-")[1]
    hashes = {key: hashlib.sha256(path.read_bytes()).hexdigest() for key, path in wheels.items()}
    context = tmp_path / "context"
    build_pypi_context(context, agent_wheel=wheels["mimir-agent"], cultivate_wheel=wheels["mimir-cultivate"],
                       version=version, agent_sha256=hashes["mimir-agent"], cultivate_sha256=hashes["mimir-cultivate"])
    helper = tmp_path / "build_context.py"
    helper.write_text(_BUILD_CONTEXT_HELPER)
    saved = tmp_path / "saved"
    subprocess.run([sys.executable, "-I", "-S", str(helper), str(saved), str(wheels["mimir-agent"]),
                    str(wheels["mimir-cultivate"]), version, hashes["mimir-agent"], hashes["mimir-cultivate"]],
                   check=True, capture_output=True, text=True)
    sdists = {package: next(tmp_path.glob(package.replace("-", "_") + "-*.tar.gz"))
              for package in wheels}
    return resources, wheels, sdists, (context, saved)


def _assert_daemon_resource_chain(resources, wheels, sdists, contexts) -> None:
    import tarfile

    for package, member, source in resources:
        # Hatch normalizes write permissions for reproducible archives. Source
        # executability is authoritative; checkout group-write bits are not.
        expected_mode = 0o755 if (ROOT / source).stat().st_mode & 0o111 else 0o644
        if source.endswith("/run") or Path(source).name in {
                "worklink-opencode-service", "worklink-daemon-gate", "worklink-opencode-login"}:
            assert expected_mode == 0o755, f"source executable mode: {source}"
        with tarfile.open(sdists[package]) as archive:
            path = next(name for name in archive.getnames() if name.endswith("/" + source.removeprefix("plugins/cultivate/")))
            assert archive.extractfile(path).read() == (ROOT / source).read_bytes()
            assert stat.S_IMODE(archive.getmember(path).mode) == expected_mode, f"sdist source mode: {source}"
        with ZipFile(wheels[package]) as archive:
            info = archive.getinfo(member)
            assert archive.read(member) == (ROOT / source).read_bytes()
            assert stat.S_IMODE(info.external_attr >> 16) == expected_mode, f"wheel source mode: {source}"
            for destination in contexts:
                assert (destination / source).read_bytes() == archive.read(member)
                assert stat.S_IMODE((destination / source).stat().st_mode) == expected_mode, f"context source mode: {destination.name}/{source}"


def test_sdist_wheel_source_free_context_preserves_daemon_resources(daemon_packages) -> None:
    _assert_daemon_resource_chain(*daemon_packages)


def test_claude_source_wheel_library_saved_helper_and_scaffold_parity(
        daemon_packages, tmp_path, monkeypatch):
    import hashlib
    from mimir.plugin_dockerfile import build_pypi_context, worklink_resources
    from mimir_cultivate.setup import _build_context_helper

    resources, wheels, sdists, _ = daemon_packages
    version = wheels["mimir-agent"].name.split("-")[1]
    hashes = {key: hashlib.sha256(path.read_bytes()).hexdigest() for key, path in wheels.items()}
    library = tmp_path / "claude-library"
    build_pypi_context(library, agent_wheel=wheels["mimir-agent"], cultivate_wheel=wheels["mimir-cultivate"],
                       version=version, agent_sha256=hashes["mimir-agent"],
                       cultivate_sha256=hashes["mimir-cultivate"], worklink_host="claude_code")
    helper = tmp_path / "claude-helper.py"
    helper.write_text(_build_context_helper("claude_code"))
    saved = tmp_path / "claude-saved"
    subprocess.run([sys.executable, "-I", "-S", str(helper), str(saved), str(wheels["mimir-agent"]),
                    str(wheels["mimir-cultivate"]), version, hashes["mimir-agent"], hashes["mimir-cultivate"]],
                   check=True, capture_output=True, text=True)
    def inventory(root):
        return {p.relative_to(root).as_posix(): (p.read_bytes(), stat.S_IMODE(p.stat().st_mode))
                for p in root.rglob("*") if p.is_file()}
    assert inventory(library) == inventory(saved)
    scripts, s6 = worklink_resources("claude_code")
    assert len(scripts) == 7 and len(s6) == 10
    selected = [row for row in resources if "worklink-opencode" not in row[2]
                and not row[2].endswith("/worklink-daemon-gate")]
    _assert_daemon_resource_chain(selected, wheels, sdists, (library, saved))
    assert not any("worklink-opencode" in name or "worklink-daemon-gate" in name
                   for name in inventory(library))

    home = tmp_path / "scaffold"
    home.mkdir()
    monkeypatch.setattr("mimir.scaffold_docker.select_plugins", lambda **kw: (_candidate(),))
    scaffold(home, worklink_host="claude_code")
    context = home / ".mimir-scaffold"
    for name in scripts:
        path = context / "cultivate-docker" / name
        assert path.read_bytes() == (SCRIPTS / name).read_bytes()
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
    assert {p.relative_to(context / "s6-rc.d").as_posix()
            for p in (context / "s6-rc.d").rglob("*") if p.is_file()} == set(s6)
    for name in s6:
        path = context / "s6-rc.d" / name
        assert path.read_bytes() == (library / "deploy/s6-overlay/s6-rc.d" / name).read_bytes()
        assert stat.S_IMODE(path.stat().st_mode) == (0o755 if name.endswith("/run") else 0o644)
    # Root resources are copied intact; the workspace image then binds its
    # start.sh bootstrap before service.sh applies executable permissions.
    import json
    from mimir.scaffold_docker import _mimir_service_run
    docker = (home / "Dockerfile").read_text()
    instruction, = [line[4:] for line in docker.splitlines()
                    if line.startswith('RUN ["python3"') and "s6-rc.d/mimir/run" in line]
    argv = json.loads(instruction)
    output = tmp_path / "workspace-run"
    argv[-1] = argv[-1].replace("/etc/s6-overlay/s6-rc.d/mimir/run", str(output))
    subprocess.run(argv, check=True, capture_output=True)
    assert output.read_text() == _mimir_service_run(cultivate=True)


def test_claude_service_installs_no_daemon_or_login_gate(tmp_path):
    s6 = tmp_path / "s6"
    for name in ("mimir", "watchdog", "worklink-execd", "worklink-opencode"):
        path = s6 / name / "run"
        path.parent.mkdir(parents=True)
        path.write_text("#!/bin/sh\n")
    bundle = s6 / "user/contents.d"
    bundle.mkdir(parents=True)
    (bundle / "worklink-opencode").touch()
    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "opencode-enabled").write_text("0\n")
    (resources / "worklink-host").write_text("claude_code\n")
    libexec = tmp_path / "libexec"
    bindir = tmp_path / "bin"
    source = (SCRIPTS / "service.sh").read_text().replace(
        "/etc/s6-overlay/s6-rc.d", str(s6)).replace("/usr/local/libexec", str(libexec)).replace(
        "/usr/local/bin", str(bindir)).replace("/opt/cultivate-docker", str(resources)).replace("-o root -g root ", "")
    subprocess.run(["sh", "-c", source], check=True, capture_output=True)
    assert {p.name for p in libexec.iterdir()} == {"worklink-execd"}
    assert not bindir.exists()
    assert not (s6 / "worklink-opencode").exists()
    assert not (bundle / "worklink-opencode").exists()
    for name in ("mimir", "watchdog", "worklink-execd"):
        assert (s6 / name / "run").stat().st_mode & stat.S_IXUSR


def test_mutation_sdist_executable_bits_lost(daemon_packages, tmp_path: Path) -> None:
    import tarfile

    resources, wheels, sdists, contexts = daemon_packages
    _assert_daemon_resource_chain(*daemon_packages)
    damaged = tmp_path / "damaged.tar.gz"
    target = "deploy/s6-overlay/s6-rc.d/mimir/run"
    with tarfile.open(sdists["mimir-agent"]) as original, tarfile.open(damaged, "w:gz") as archive:
        for info in original.getmembers():
            data = original.extractfile(info) if info.isfile() else None
            if info.name.endswith("/" + target):
                info.mode = 0o644
            try:
                archive.addfile(info, data)
            finally:
                if data is not None:
                    data.close()
    with pytest.raises(AssertionError, match="sdist source mode: " + target):
        _assert_daemon_resource_chain(resources, wheels, {**sdists, "mimir-agent": damaged}, contexts)
    _assert_daemon_resource_chain(*daemon_packages)


def test_mutation_wheel_executable_bits_lost(daemon_packages, tmp_path: Path) -> None:
    resources, wheels, sdists, contexts = daemon_packages
    _assert_daemon_resource_chain(*daemon_packages)
    damaged = tmp_path / "damaged.whl"
    target = "mimir_cultivate/docker/worklink-opencode-login"
    with ZipFile(wheels["mimir-cultivate"]) as original, ZipFile(damaged, "w") as archive:
        for info in original.infolist():
            if info.filename == target:
                info.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(info, original.read(info.filename))
    with pytest.raises(AssertionError, match="wheel source mode: plugins/cultivate/" + target):
        _assert_daemon_resource_chain(resources, {**wheels, "mimir-cultivate": damaged}, sdists, contexts)
    _assert_daemon_resource_chain(*daemon_packages)
