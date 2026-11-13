"""Cultivate's OpenCode configuration and contained proposal contract."""

from __future__ import annotations

import base64
import json
from pathlib import Path
import subprocess

import pytest

from mimir_cultivate import opencode, repository
from tests.opencode_fixtures import seed_codex_subscription


def test_converted_consumers_have_no_legacy_execution_or_auth_fallback():
    import ast
    import inspect
    import textwrap
    from mimir_cultivate import setup
    from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend

    forbidden = {"resolve_opencode_invocation", "resolve_worklink_opencode_invocation",
                 "opencode_worker_documents", "reconcile_codex_auth", "resolve_opencode_binary",
                 "opencode_worker_environment", "_verify_opencode_binary", "_verify_opencode_model"}
    for consumer in (OpenCodeBackend.work_spec, opencode._spawn_open_code_impl, setup._verify):
        tree = ast.parse(textwrap.dedent(inspect.getsource(consumer)))
        calls = {node.func.id for node in ast.walk(tree)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        assert not calls & forbidden
        constants = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        assert "OPENCODE_PERMISSION" not in constants
    wrapper = inspect.getsource(opencode.spawn_open_code.coroutine)
    assert "contained_runner=execute_contained_session" in wrapper
    assert "resolve_daemon_model" in setup._OPENCODE_DAEMON_PROBE
    assert "wait_for_daemon" in setup._OPENCODE_DAEMON_PROBE
    assert "resolve_opencode_invocation" not in setup._OPENCODE_DAEMON_PROBE


def test_selected_spawn_factory_is_provider_owned() -> None:
    from mimir_cultivate.policy import _factory

    selected = _factory("mimir_cultivate.opencode", "spawn_open_code")()
    assert selected is opencode.spawn_open_code
    assert selected.name == "spawn_open_code"


def _auth(home: Path, provider: str, entry: dict[str, str]) -> None:
    path = home / ".local" / "share" / "opencode" / "auth.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({provider: entry}), encoding="utf-8")


def test_daemon_selection_never_projects_controller_config_or_oauth(tmp_path: Path) -> None:
    path = tmp_path / "opencode.jsonc"
    path.write_text(
        '{"model":"openai/gpt-test","provider":{"openai":'
        '{"options":{"apiKey":"{env:OPENAI_TOKEN}"}},'
        '"other":{"options":{"key":"other-secret"}}}}',
        encoding="utf-8",
    )
    _auth(tmp_path, "openai", {"type": "oauth", "refresh": "private-refresh"})
    # This non-Codex projection test intentionally has no native Codex bundle.
    env = {
        "HOME": str(tmp_path), "OPENCODE_CONFIG": str(path),
        "MIMIR_MODEL_SPEC": "openai:gpt-test",
        "OPENAI_TOKEN": "private-token", "OPENAI_API_KEY": "ambient-token",
    }
    before = path.read_bytes()
    selected = opencode.resolve_daemon_model(env)
    assert selected == {"providerID": "openai", "id": "gpt-test"}
    assert path.read_bytes() == before
    assert "private-refresh" not in repr(selected)
    assert "private-token" not in repr(selected)
    assert not hasattr(opencode, "opencode_worker_documents")


@pytest.mark.parametrize("with_bundle", [False, True])
def test_unset_spec_does_not_seed_or_reconcile_agent_codex_oauth(
    tmp_path: Path, with_bundle: bool,
) -> None:
    # Even an ambient key and refresh-only OpenCode entry cannot substitute
    # for the subscription bundle when the omitted spec selects default Codex.
    _auth(tmp_path, "openai", {"type": "oauth", "refresh": "subscription-refresh"})
    env = {"HOME": str(tmp_path), "OPENAI_API_KEY": "ambient-token"}
    assert "MIMIR_MODEL_SPEC" not in env
    if with_bundle:
        seed_codex_subscription(tmp_path, refresh="native-refresh")
    native = tmp_path / ".codex/auth.json"
    before = native.read_bytes() if with_bundle else None
    legacy_auth = tmp_path / ".local/share/opencode/auth.json"
    legacy_before = legacy_auth.read_bytes()
    selected = opencode.resolve_daemon_model(env)
    assert selected["providerID"] == "openai"
    assert "ambient-token" not in repr(selected)
    assert (native.read_bytes() if native.exists() else None) == before
    assert legacy_auth.read_bytes() == legacy_before


@pytest.mark.parametrize("provider_config", [
    {"apiKey": "literal-secret"},
    {"credentials": {"nested": "{env:SECRET}"}},
    {"options": {"github-copilot": {}}},
    {"apiKey": "prefix-{env:SECRET}"},
])
def test_unsafe_provider_config_fails_closed(
    tmp_path: Path, provider_config: dict[str, object],
) -> None:
    path = tmp_path / "opencode.json"
    path.write_text(json.dumps({
        "model": "proxy/model", "provider": {"proxy": provider_config},
    }), encoding="utf-8")
    env = {"HOME": str(tmp_path), "OPENCODE_CONFIG": str(path), "PROXY_API_KEY": "ambient"}
    from mimir_cultivate.worklink.daemon_sessions import session_environment
    before = path.read_bytes()
    with pytest.raises((ValueError, RuntimeError)):
        session_environment({"OPENCODE_CONFIG": str(path), "PROXY_API_KEY": "ambient"},
                            home=tmp_path, git_settings=(), credentials={})
    assert path.read_bytes() == before


def test_github_provider_has_no_worker_projection(tmp_path: Path) -> None:
    _auth(tmp_path, "github-copilot", {"type": "api", "key": "private"})
    selected = opencode.resolve_daemon_model({"HOME": str(tmp_path), "MIMIR_MODEL_SPEC": "github-copilot:model"})
    assert selected == {"providerID": "github-copilot", "id": "model"}
    assert "private" not in repr(selected)
    assert not hasattr(opencode, "opencode_worker_documents")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(("git", "-C", str(repo), *args), check=True, stdout=subprocess.PIPE)


def test_proposal_is_lossless_and_stays_in_checkout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Worker")
    _git(repo, "config", "user.email", "worker@example.invalid")
    (repo / "file.txt").write_text("before\n")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-qm", "seed")
    (repo / "file.txt").write_text("after\n")
    scrubber = opencode.SensitiveMaterialScrubber(home=tmp_path / "home")
    result = opencode.build_opencode_proposal(repo, scrubber=scrubber)
    assert result.reason_code == "proposal_created"
    assert result.proposal is not None
    assert result.proposal.kind == "git_binary_patch"
    assert result.proposal.byte_length == len(base64.b64decode(result.proposal.patch, validate=True))
    assert opencode._sanitized_git_env() == repository._sanitized_git_env()
    assert b"after" in base64.b64decode(result.proposal.patch, validate=True)
    assert (repo / "file.txt").read_text() == "after\n"


def test_sensitive_patch_refused_and_artifacts_scrubbed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Worker")
    _git(repo, "config", "user.email", "worker@example.invalid")
    (repo / "file.txt").write_text("before\n")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-qm", "seed")
    (repo / "file.txt").write_text("private-token\n")
    scrubber = opencode.SensitiveMaterialScrubber(home=tmp_path / "home")
    scrubber.add_scalar("private-token")
    assert opencode.build_opencode_proposal(repo, scrubber=scrubber).reason_code == "proposal_sensitive_content"
    root = tmp_path / "artifacts"
    opencode.write_spawn_artifacts(
        root, "run-1", prompt="private-token", stdout="private-token", stderr="safe",
        manifest={"detail": "private-token"}, scrubber=scrubber,
    )
    directory = opencode.resolve_artifact_handle(root, "run-1")
    assert "private-token" not in "".join(path.read_text() for path in directory.iterdir())
    with pytest.raises(ValueError, match="unsafe artifact handle"):
        opencode.resolve_artifact_handle(root, "../escape")


def test_terminal_classification_and_prompt_path_policy(tmp_path: Path) -> None:
    home = tmp_path / "home"
    config = tmp_path / "config.json"
    for prompt, expected in (
        (str(home / "child"), "agent_home_path"),
        (config.as_uri(), "config_auth_source_path"),
        ("unrelated text", None),
    ):
        assert opencode.prompt_contains_sensitive_source_path(
            prompt, agent_home=home, source_paths=(config,),
        ) == expected
    for reason, status in (
        ("seed_changed", "seed_refused"),
        ("embedded_seed_repository", "seed_refused"),
        ("provisioning_unavailable", "provisioning_unavailable"),
    ):
        selected = opencode.classify_spawn_terminal_state(provisioning_reason=reason)
        assert (selected.status, selected.reason_code, selected.event_type) == (
            status, reason, "spawn_open_code_provisioning_refused",
        )
