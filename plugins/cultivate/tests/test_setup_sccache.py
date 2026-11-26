"""Offline compiler-cache setup and fail-closed boundary contracts."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest
import yaml

from mimir.scaffold_docker import render_compose_yml
from mimir_cultivate import setup as installer
from mimir_cultivate.worklink.backends.opencode import validate_test_env


def test_sccache_layer_pins_both_release_assets_and_binary_permissions():
    layer = installer.render_layer("rust", "1.99.1", "root:sha")
    assert installer.DEFAULT_SCCACHE_VERSION == "0.12.0"
    assert "https://github.com/mozilla/sccache/releases/download/v0.12.0/$archive.tar.gz" in layer
    for triple, digest in (
        ("x86_64", "b0e89ead6899224a4ba2b90e9073bf1ce036d95bab30f3dc33c1e1468bc4ad44"),
        ("aarch64", "111ddd28fb108cb3e17edb69ab62cefe1dcc97b02e5006ff9c1330f4f2e78368"),
    ):
        assert f"triple={triple}-unknown-linux-musl; sha256={digest}" in layer
    assert "archive=sccache-v0.12.0-$triple" in layer
    assert layer.index("sha256sum -c - &&") < layer.index('tar -xzf "$tmp/sccache.tar.gz"')
    assert 'install -o root -g root -m 0755 "$tmp/$archive/sccache" /usr/local/bin/sccache' in layer
    assert "install -d -o worklink -g worklink -m 0700 /var/cache/mimir-sccache" in layer


@pytest.mark.parametrize("arch", ["amd64", "arm64", "unsupported"])
def test_sccache_layer_checksum_mismatch_stops_before_extraction(tmp_path, arch):
    layer = installer.render_layer("rust", "1.99.1", "root:sha")
    script = next(line.removeprefix("RUN ") for line in layer.splitlines()
                  if "archive=sccache-" in line)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    extracted = tmp_path / "extracted"
    downloaded = tmp_path / "downloaded"
    for name, body in {
        "dpkg": f"echo {arch}",
        "curl": f"touch {shlex.quote(str(downloaded))}\nprintf corrupt > \"$2\"",
        "tar": f"touch {shlex.quote(str(extracted))}",
        "mktemp": f"echo {shlex.quote(str(tmp_path))}",
    }.items():
        command = bin_dir / name
        command.write_text("#!/bin/sh\n" + body + "\n")
        command.chmod(0o755)
    # sha256sum is real; neither network, extraction nor privileged install runs.
    result = subprocess.run(["/bin/sh", "-c", script],
                            env={"PATH": f"{bin_dir}:/usr/bin:/bin"}, capture_output=True)
    assert result.returncode != 0
    assert not extracted.exists()
    assert downloaded.exists() == (arch != "unsupported")
    if arch != "unsupported":
        assert b"FAILED" in result.stdout


def test_sccache_toolchain_test_env():
    expected = {
        "RUSTUP_HOME": "/opt/rustup", "CARGO_HOME": "/var/cache/mimir-cargo",
        "RUSTC_WRAPPER": "/usr/local/bin/sccache", "SCCACHE_DIR": "/var/cache/mimir-sccache",
        "SCCACHE_CACHE_SIZE": "20G",
    }
    assert installer.toolchain_test_env("rust") == expected
    assert installer.toolchain_test_env("rust", sccache_cache_size="5G") == {
        **expected, "SCCACHE_CACHE_SIZE": "5G"}
    assert installer.toolchain_test_env("rust", sccache=False) == {
        "RUSTUP_HOME": "/opt/rustup", "CARGO_HOME": "/var/cache/mimir-cargo"}
    assert installer.toolchain_test_env("python") == {"UV_PYTHON_INSTALL_DIR": "/opt/uv-python"}
    assert installer.toolchain_test_env("node") == {}
    validate_test_env(expected)


@pytest.mark.parametrize("key,value", [
    ("RUSTC_WRAPPER", "/tmp/evil"),
    ("RUSTC_WRAPPER", "sccache"),
    ("RUSTC_WRAPPER", "/usr/local/bin/../bin/sccache"),
    ("RUSTC_WRAPPER", "/usr/local/bin/sccache "),
    ("RUSTC_WRAPPER", "/usr/local/bin/sccache\n"),
    ("SCCACHE_DIR", "relative"),
    ("SCCACHE_DIR", "/var/cache/../evil"),
    ("SCCACHE_DIR", "/var/cache/\x00evil"),
    ("SCCACHE_CACHE_SIZE", "/20G"),
    ("SCCACHE_CACHE_SIZE", "0G"),
    ("SCCACHE_CACHE_SIZE", "20"),
    ("SCCACHE_CACHE_SIZE", "20G;exit 0"),
    ("SCCACHE_CACHE_SIZE", "20G\n"),
    ("SCCACHE_CACHE_SIZE", 20),
    ("SCCACHE_CACHE_SIZE", "２０G"),
    ("SCCACHE_BUCKET", "/tmp/remote"),
])
def test_sccache_test_env_rejects_unsafe_values(key, value):
    with pytest.raises(ValueError):
        validate_test_env({key: value})


@pytest.mark.parametrize("size", ["1K", "200M", "20G", "1T"])
def test_sccache_test_env_accepts_size(size):
    validate_test_env({"SCCACHE_CACHE_SIZE": size, "SCCACHE_DIR": "/absolute/cache",
                       "RUSTC_WRAPPER": "/usr/local/bin/sccache"})


@pytest.mark.parametrize("kind,layer_hash,compose_hash", [
    ("rust", "f42061aa822a85caa52058027b404e45eda753e6dd3edd7648c5cb7b7aed8d34",
     "23b79630cf929573afbca3ca6bda2607979f44603b354c3338425088d6f8f3f6"),
    ("node", "a0afe9b11dfa69081a6a920256fb34049ddb27948003d47f5a49b31dcb1f6dbd",
     "d04344405359353053099796242e997c54f5db5375e3d87bef6e240ff854c5bf"),
    ("python", "f329c75e5c2ac3b0ff3321faa535d7f64c9b654ddcda4cea707eea4e4cc1d35b",
     "aac67a02e6d99d645fefbd16372fdba819863569cb806695d72dfd7c471b6de3"),
])
def test_sccache_opt_out_and_non_rust_are_byte_identical(kind, layer_hash, compose_hash):
    # Oracles captured from the pre-sccache renderer, not computed by the SUT.
    options = {"sccache": False} if kind == "rust" else {}
    layer = installer.render_layer(kind, "1.99.1", "root:sha", **options)
    template = render_compose_yml(service_name="agent", web_port=8090, mode="pypi")
    compose = installer.render_compose(template, "layer:sha", kind, "repo", **options)
    assert hashlib.sha256(layer.encode()).hexdigest() == layer_hash
    assert hashlib.sha256(compose.encode()).hexdigest() == compose_hash


@pytest.mark.parametrize("host", ["opencode", "claude_code"])
def test_sccache_compose_persists_worker_cache_without_host_mount(host):
    template = render_compose_yml(service_name="agent", web_port=8090, mode="pypi")
    doc = yaml.safe_load(installer.render_compose(
        template, "layer:sha", "rust", "repo", worklink_host=host))
    assert "sccache" in doc["volumes"]
    for service in ("agent", "init"):
        assert "sccache:/var/cache/mimir-sccache" in doc["services"][service]["volumes"]
        assert all(not mount.startswith(("/", "./")) for mount in doc["services"][service]["volumes"])
    assert doc["services"]["init"]["command"][0].endswith(
        "&& install -d -o worklink -g worklink -m 0700 /var/cache/mimir-sccache")
    assert doc["services"]["agent"]["environment"]["MIMIR_SHELL_PASS_ENV"] == (
        "RUSTUP_HOME,CARGO_HOME,RUSTC_WRAPPER,SCCACHE_DIR,SCCACHE_CACHE_SIZE")


def test_sccache_setup_options_persist_and_reject_bad_manifest():
    parser = argparse.ArgumentParser()
    installer.add_argparse(parser.add_subparsers(dest="command"))
    args = parser.parse_args(["cultivate", "setup", "--no-sccache", "--sccache-cache-size", "5G"])
    assert installer._sccache_settings(args, {}) == (False, "5G")
    args = parser.parse_args(["cultivate", "setup"])
    assert installer._sccache_settings(args, {}) == (True, "20G")
    assert installer._sccache_settings(args, {"sccache": False, "sccache_cache_size": "5G"}) == (False, "5G")
    for previous in ({"sccache": "false"}, {"sccache_cache_size": "20G;false"}):
        with pytest.raises(installer.SetupError):
            installer._sccache_settings(args, previous)


def test_sccache_repo_pin_requires_reviewed_checksums():
    for declaration in ("tool: sccache@0.12.0", "sccache-version: '0.12.0'"):
        assert installer._tool_pin({"ci": declaration}, "sccache", "0.0.0") == "0.12.0"
    with pytest.raises(installer.SetupError, match="Conflicting"):
        installer._tool_pin({"a": "sccache@0.12.0", "b": "sccache@0.11.0"}, "sccache", "0.12.0")
    with pytest.raises(installer.SetupError, match="reviewed"):
        installer.render_layer("rust", "1.99.1", "root:sha", sccache_version="9.9.9")
    # Disabling the cache must not demand a release checksum.
    installer.render_layer("rust", "1.99.1", "root:sha", sccache=False, sccache_version="9.9.9")


@pytest.mark.parametrize("failure", [None, "mode", "group", "readable", "owner", "file", "symlink", "write", "version"])
def test_sccache_worker_probe_verifies_private_writable_cache_and_version(
    tmp_path, monkeypatch, failure,
):
    cargo = tmp_path / "cargo"
    cargo.mkdir()
    cache = tmp_path / "sccache"
    cache.mkdir(mode=0o700)
    if failure == "mode":
        cache.chmod(0o702)  # world-writable must fail even if builder can write
    elif failure == "group":
        cache.chmod(0o720)
    elif failure == "readable":
        cache.chmod(0o744)
    elif failure == "owner":
        uid = os.geteuid()
        monkeypatch.setattr(os, "geteuid", lambda: uid + 1)
    elif failure == "file":
        cache.rmdir()
        cache.write_text("not a cache")
        cache.chmod(0o700)
    elif failure == "symlink":
        cache.rmdir()
        cache.symlink_to(cargo, target_is_directory=True)
    env = {**installer.toolchain_test_env("rust"), "HOME": str(tmp_path / "home"),
           "CARGO_HOME": str(cargo), "SCCACHE_DIR": str(cache)}
    launched = []
    real_named = tempfile.NamedTemporaryFile

    def named(*args, **kwargs):
        if failure == "write" and Path(kwargs["dir"]) == cache:
            raise PermissionError("cache write failed")
        return real_named(*args, **kwargs)

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", named)

    def command(argv, **kwargs):
        launched.append(argv)
        assert kwargs["env"]["SCCACHE_DIR"] == str(cache)
        if "-o" in argv:
            Path(argv[argv.index("-o") + 1]).write_text("fake executable")
        return SimpleNamespace(
            returncode=int(failure == "version" and argv[0] == "/usr/local/bin/sccache"),
            stdout="rustc 1.99.1" if argv[0] == "rustc" else "sccache 0.12.0")

    monkeypatch.setattr(subprocess, "run", command)

    def docker(argv, *, cwd):
        assert "-u" in argv and argv[argv.index("-u") + 1] == "worklink"
        monkeypatch.setattr(sys, "argv", ["probe", *argv[-3:]])
        try:
            exec(compile(installer._TOOLCHAIN_WORKER_PROBE, "worker-probe", "exec"), {})
        except SystemExit as exc:
            assert exc.code == 0
        return ""  # stdout is captured separately below

    monkeypatch.setattr(installer, "_agent_exec", lambda *args: json.dumps(env))
    # Exercise the actual verification Docker uid/argv and probe program.
    def run(argv, *, cwd):
        output = io.StringIO()
        with redirect_stdout(output):
            docker(argv, cwd=cwd)
        return output.getvalue().strip()

    monkeypatch.setattr(installer, "_run", run)
    if failure:
        with pytest.raises(installer.SetupError, match="toolchain_missing: sccache"):
            installer._verify_toolchain(tmp_path, "rust", "1.99.1")
    else:
        installer._verify_toolchain(tmp_path, "rust", "1.99.1")
        assert ["/usr/local/bin/sccache", "--version"] in launched
        assert not list(cache.iterdir())
    if failure not in (None, "version"):
        assert not launched
