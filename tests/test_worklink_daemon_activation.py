"""Activation never loads a provider or consults disabled daemon state."""

from __future__ import annotations

import ast
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from mimir import worklink_daemon_activation as policy


@pytest.mark.parametrize("selected,installed,enabled,reason", [
    (False, False, False, "unselected"), (False, True, False, "unselected"),
    (True, False, False, "not_installed"), (True, True, True, "enabled"),
])
def test_planned_activation(selected, installed, enabled, reason):
    result = policy.planned_activation(trusted_cultivate=selected, opencode_installed=installed)
    assert (result.enabled, result.reason) == (enabled, reason)


@pytest.fixture
def root_marker(tmp_path, monkeypatch):
    """Substitute only uid metadata; exercise real open, mode, size and contents."""
    marker = tmp_path / "opencode-enabled"
    marker.write_bytes(b"1\n")
    marker.chmod(0o644)
    original = os.fstat
    def root_stat(fd):
        info = original(fd)
        return SimpleNamespace(st_uid=0, st_mode=info.st_mode, st_size=info.st_size)
    monkeypatch.setattr(policy.os, "fstat", root_stat)
    return marker


@pytest.mark.parametrize("value,installed", [(b"0\n", False), (b"1\n", True)])
def test_exact_installed_bit(root_marker, value, installed):
    root_marker.write_bytes(value)
    assert policy.read_installed_marker(root_marker) is installed


@pytest.mark.parametrize("value", [b"", b"1", b"true\n", b"1\n\n", b"2\n", b"x" * 10000])
def test_invalid_marker_contents(root_marker, value):
    root_marker.write_bytes(value)
    with pytest.raises(policy.ActivationError, match="^invalid OpenCode installation marker$"):
        policy.read_installed_marker(root_marker)


def test_marker_missing_symlink_directory_and_writable(root_marker, tmp_path):
    assert policy.read_installed_marker(tmp_path / "missing") is False
    link = tmp_path / "link"
    link.symlink_to(root_marker)
    for path in (link, tmp_path):
        with pytest.raises(policy.ActivationError):
            policy.read_installed_marker(path)
    root_marker.chmod(0o664)
    with pytest.raises(policy.ActivationError):
        policy.read_installed_marker(root_marker)


def test_marker_requires_root_owner(root_marker, monkeypatch):
    monkeypatch.setattr(policy.os, "fstat", lambda fd: SimpleNamespace(
        st_uid=1002, st_mode=0o100644, st_size=2))
    with pytest.raises(policy.ActivationError):
        policy.read_installed_marker(root_marker)


def test_unselected_does_not_read_marker_or_probe_binary(monkeypatch):
    monkeypatch.setattr(policy, "select_plugins", lambda **kw: ())
    def forbidden(*args, **kwargs):
        pytest.fail("disabled path consulted daemon installation")
    monkeypatch.setattr(policy, "read_installed_marker", forbidden)
    monkeypatch.setattr(policy.shutil, "which", forbidden)
    assert policy.resolve_activation({"MIMIR_ENABLE_OPENCODE": "1"}).reason == "unselected"


@pytest.mark.parametrize("installed", [False, True])
def test_selected_activation_binary_requirement(root_marker, monkeypatch, installed):
    monkeypatch.setattr(policy, "select_plugins", lambda **kw: (object(),))
    monkeypatch.setattr(policy, "has_trusted_cultivate", lambda candidates: True)
    root_marker.write_bytes(b"1\n" if installed else b"0\n")
    monkeypatch.setattr(policy.shutil, "which", lambda *args, **kw: None)
    if installed:
        with pytest.raises(policy.ActivationError, match="enabled OpenCode binary is unavailable"):
            policy.resolve_activation({}, marker=root_marker)
        monkeypatch.setattr(policy.shutil, "which", lambda *args, **kw: "/fixture/opencode")
        assert policy.resolve_activation({}, marker=root_marker).enabled
    else:
        assert policy.resolve_activation({"MIMIR_ENABLE_OPENCODE": "1"}, marker=root_marker).reason == "not_installed"


def test_cli_sanitized_result(monkeypatch, capsys):
    monkeypatch.setattr(policy, "resolve_activation", lambda env: policy.DaemonActivation(False, "unselected"))
    assert policy.main() == 0
    assert '"enabled": false' in capsys.readouterr().out


def test_activation_source_has_no_cultivate_imports():
    tree = ast.parse(Path(policy.__file__).read_text())
    imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imports += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    assert not any(name and name.startswith("mimir_cultivate") for name in imports)


def test_real_metadata_selection_is_not_provider_loading(monkeypatch):
    from mimir import plugin_dockerfile
    class EntryPoint:
        def load(self):
            pytest.fail("activation loaded a provider entrypoint")
    candidate = SimpleNamespace(name="cultivate", distribution="untrusted-cultivate",
                                enabled=True, diagnostic=None, manifest=object(), entry_point=EntryPoint())
    monkeypatch.setattr(plugin_dockerfile, "discover_plugins", lambda **kw: (candidate,))
    assert policy.resolve_activation({"MIMIR_PLUGINS": "cultivate"}).reason == "unselected"
