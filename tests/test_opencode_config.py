"""Daemon model/config boundaries after retirement of agent-auth projection."""
from __future__ import annotations

import ast
import base64
import json
from pathlib import Path

import pytest

pytest.importorskip("mimir_cultivate")
from mimir_cultivate import opencode, setup
from mimir_cultivate.worklink import worker_client


@pytest.mark.parametrize("spec,expected", [
    ("codex-plus:gpt-test", {"providerID": "openai", "id": "gpt-test"}),
    ("codex-plus:gpt-test#high", {"providerID": "openai", "id": "gpt-test", "variant": "high"}),
    ("openai:gpt-test", {"providerID": "openai", "id": "gpt-test"}),
    ("anthropic:claude-test", {"providerID": "anthropic", "id": "claude-test"}),
    ("claude-code:sonnet", {"providerID": "anthropic", "id": "sonnet"}),
    ("github-copilot:model", {"providerID": "github-copilot", "id": "model"}),
])
def test_daemon_model_uses_agent_selection_without_auth_or_native_config(tmp_path, monkeypatch, spec, expected):
    config = tmp_path / "opencode.json"
    config.write_text('{"model":"different/model"}')
    auth = tmp_path / "auth.json"
    auth.write_text("not a credential")
    original = Path.open
    def guarded(path, *args, **kwargs):
        if path in {config, auth}:
            pytest.fail("daemon model resolution must not read controller auth/config")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", guarded)
    assert opencode.resolve_daemon_model({"MIMIR_MODEL_SPEC": spec,
        "OPENCODE_CONFIG": str(config), "XDG_DATA_HOME": str(tmp_path),
        "OPENAI_API_KEY": "irrelevant-provider-material"}) == expected


@pytest.mark.parametrize("spec", ["", "codex-plus:model#", "codex-plus:bad model", "openai:",
                                   "openai:model\x00suffix"])
def test_daemon_model_refuses_invalid_selection(spec):
    with pytest.raises(opencode.OpenCodeConfigError):
        opencode.resolve_daemon_model({"MIMIR_MODEL_SPEC": spec})


def test_explicit_model_and_effort_precedence():
    assert opencode.resolve_daemon_model({"MIMIR_MODEL_SPEC": "codex-plus:ignored",
        "MIMIR_MODEL_REASONING_EFFORT": "low", "MIMIR_WORKLINK_REASONING_EFFORT": "high"},
        "openai/chosen") == {"providerID": "openai", "id": "chosen", "variant": "high"}
    assert opencode.resolve_daemon_model({"MIMIR_MODEL_REASONING_EFFORT": "low"},
        "openai/chosen#xhigh")["variant"] == "xhigh"
    assert "variant" not in opencode.resolve_daemon_model({}, "openai/chosen#none")


@pytest.mark.parametrize("path", [".config/opencode/opencode.json", ".local/share/opencode/auth.json",
                                   "../auth.json", "/tmp/auth.json"])
def test_retired_worker_home_documents_are_refused(path):
    with pytest.raises(ValueError):
        worker_client.WorkerProjection(path=path, document=b"{}")


def test_retired_auth_sources_and_reconciliation_have_no_production_callers():
    forbidden = {"resolve_opencode_invocation", "resolve_worklink_opencode_invocation",
                 "opencode_worker_documents", "reconcile_codex_auth", "read_factory_auth"}
    root = Path(opencode.__file__).parent
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else (
                    node.func.attr if isinstance(node.func, ast.Attribute) else "")
                assert name not in forbidden, (path, name)
            elif isinstance(node, ast.ImportFrom):
                assert not forbidden.intersection(alias.name for alias in node.names), path
    assert not (root / "worklink/factory_auth.py").exists()
    for name in forbidden:
        assert not hasattr(opencode, name)
        assert name not in setup._OPENCODE_DAEMON_PROBE


def test_agent_jwt_expiry_helper_remains_independent():
    def token(value):
        payload = base64.urlsafe_b64encode(json.dumps({"exp": value}).encode()).decode().rstrip("=")
        return "header." + payload + ".signature"
    assert opencode._jwt_exp(token(123)) == 123
    assert opencode._jwt_exp(token(True)) == 0
    assert opencode._jwt_exp("malformed") == 0


def test_scrubber_preserves_whole_document_scalars_and_path_forms(tmp_path):
    secret = "credential-" + "runtime-fragment"
    document = json.dumps({"access": secret})
    source = tmp_path / "private auth.json"
    scrubber = opencode.SensitiveMaterialScrubber(home=tmp_path, source_paths=(source,))
    scrubber.add_scalar(secret)
    scrubber.add_document(document)
    for text in (secret, document, str(source), source.as_uri(), json.dumps(str(source))):
        assert not scrubber.contains_sensitive(scrubber.scrub_text(text))
    assert secret not in repr(scrubber)
