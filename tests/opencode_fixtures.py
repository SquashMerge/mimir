"""Synthetic subscription stores for OpenCode integration tests."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest


@pytest.fixture
def opencode_binary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Exercise real binary admission without requiring an installed CLI."""
    from mimir_cultivate.worklink.backends import opencode

    binary = seed_opencode_binary(tmp_path)
    monkeypatch.setattr(opencode, "IMAGE_OPENCODE_BIN", binary)
    return binary


def seed_opencode_binary(root: Path) -> Path:
    """Provide an executable owned by the fixture, not the developer image."""
    binary = root / "controller-bin" / "opencode"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    binary.chmod(0o755)
    return binary


def seed_factory_install(root: Path, version: str = "0.14.0") -> Path:
    """Synthetic manifests only; never execute a downloaded or ambient factory."""
    modules = root / "lib/node_modules"
    factory = modules / "feature-factory"
    adapter = modules / "opencode2-feature-factory"
    (factory / "bin").mkdir(parents=True)
    adapter.mkdir(parents=True)
    entrypoint = factory / "bin/factory.js"
    entrypoint.write_text("")
    (factory / "package.json").write_text(json.dumps({"name": "feature-factory", "version": version}))
    (adapter / "package.json").write_text(json.dumps({"name": "opencode2-feature-factory", "version": version,
                                                    "dependencies": {"feature-factory": version}}))
    return entrypoint


def seed_codex_subscription(home: Path, refresh: str = "subscription") -> None:
    """Create a usable native OAuth bundle without plugins or literal tokens."""
    segment = base64.urlsafe_b64encode(json.dumps({"exp": 300}).encode()).decode().rstrip("=")
    codex = home / ".codex" / "auth.json"
    codex.parent.mkdir(parents=True, exist_ok=True)
    codex.write_text(json.dumps({"auth_mode": "chatgpt", "tokens": {
        "access_token": ".".join(("header", segment, "signature")),
        "refresh_token": refresh,
    }}))
    codex.chmod(0o600)
    auth = home / ".local" / "share" / "opencode" / "auth.json"
    if auth.exists():
        entries = json.loads(auth.read_text())
        entry = entries.get("openai")
        if isinstance(entry, dict) and entry.get("type") == "oauth" and not entry.get("access"):
            entry["access"] = ".".join(("header", segment, "signature"))
            auth.write_text(json.dumps(entries))
