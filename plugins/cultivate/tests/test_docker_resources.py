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
    dockerfile = (tmp_path / "Dockerfile").read_text()
    assert "RUN sh /opt/cultivate-docker/service.sh" in dockerfile
    assert 'ENTRYPOINT ["/init"]' in dockerfile
    assert "MIMIR_ENABLE_OPENCODE" in (tmp_path / "compose.yml").read_text()
    start = (tmp_path / "start.sh").read_text()
    assert 'resolve_activation(dict(os.environ))' in start
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
    assert "worklink-opencode-login" in result.stdout

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
    source = (SCRIPTS / "service.sh").read_text()
    source = source.replace("/etc/s6-overlay/s6-rc.d", str(s6)).replace(
        "/usr/local/libexec", str(libexec)).replace("/usr/local/bin", str(bindir)).replace(
        "/opt/cultivate-docker", str(resources)).replace("-o root -g root ", "")
    subprocess.run(["sh", "-c", source], check=True)
    for name in ("mimir", "worklink-execd", "worklink-opencode") + (("watchdog",) if mode == "root" else ()):
        assert (s6 / name / "run").stat().st_mode & stat.S_IXUSR
    assert (libexec / "worklink-execd").stat().st_mode & stat.S_IXUSR
    assert "mimir_cultivate.worklink.worker_exec" in (libexec / "worklink-execd").read_text()
    for name in ("worklink-opencode-service", "worklink-daemon-gate"):
        assert (libexec / name).read_bytes() == (SCRIPTS / name).read_bytes()
        assert stat.S_IMODE((libexec / name).stat().st_mode) == 0o755
    assert (bindir / "worklink-opencode-login").read_bytes() == (SCRIPTS / "worklink-opencode-login").read_bytes()


def test_sdist_wheel_source_free_context_preserves_daemon_resources(tmp_path: Path) -> None:
    import hashlib
    import tarfile

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
    for package, member, source in resources:
        with tarfile.open(next(tmp_path.glob(package.replace("-", "_") + "-*.tar.gz"))) as archive:
            path = next(name for name in archive.getnames() if name.endswith("/" + source.removeprefix("plugins/cultivate/")))
            assert archive.extractfile(path).read() == (ROOT / source).read_bytes()
        with ZipFile(wheels[package]) as archive:
            info = archive.getinfo(member)
            assert archive.read(member) == (ROOT / source).read_bytes()
            for destination in (context, saved):
                assert (destination / source).read_bytes() == archive.read(member)
                assert stat.S_IMODE((destination / source).stat().st_mode) == (info.external_attr >> 16) & 0o777
