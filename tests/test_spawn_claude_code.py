from __future__ import annotations

import asyncio
import json
import stat
from dataclasses import fields

import pytest

pytest.importorskip("mimir_cultivate")

from mimir.plugins.manager import load_plugins
from mimir_cultivate.claude_code import _spawn_claude_code_impl, CLAUDE_EXECUTABLE
from mimir_cultivate.contained_execution import CollectedExecutionResult
from tests.test_spawn_opencode import factory_for, spawn_tree


def test_claude_spawn_surface_is_route_exclusive(monkeypatch):
    monkeypatch.setenv("MIMIR_MODEL_SPEC", "claude-code:sonnet")
    selected = load_plugins(enable="cultivate").policy
    assert "spawn_claude_code" in selected.tools
    assert "spawn_open_code" not in selected.tools


@pytest.mark.parametrize("model", ["claude-code:sonnet", "codex-plus:test", "openai:test", "anthropic:test"])
def test_route_projection_preserves_closed_policy(monkeypatch, model):
    from mimir.tools.registry import all_mimir_tools
    from mimir_cultivate.policy import CULTIVATE_TOOL_NAMES, CODING_TOOLS

    monkeypatch.setenv("MIMIR_MODEL_SPEC", "codex-plus:test")
    original = load_plugins(enable="cultivate").policy
    monkeypatch.setenv("MIMIR_MODEL_SPEC", model)
    selected = load_plugins(enable="cultivate").policy
    claude = model.startswith("claude-code:")
    name = "spawn_claude_code" if claude else "spawn_open_code"
    expected = (CULTIVATE_TOOL_NAMES - {"spawn_open_code"}) | {name}
    assert {n for n, tool in selected.tools.items() if tool.owner == "cultivate"} == expected
    before, after = original.tools["spawn_open_code"], selected.tools[name]
    for field in fields(before):
        if field.name not in {"name", "factory"}:
            assert getattr(after, field.name) == getattr(before, field.name), field.name
    assert after.operation_class == "admin_required"
    assert after.flow == "both"
    assert after.service_sink_adapter == "spawn_workspace"
    assert after.descriptor.ifc_delegation
    assert "review" not in after.profiles
    assert after.factory().name == name
    surface = {tool.name for tool in all_mimir_tools(
        policy=selected, require_provider_available=False,
    )}
    assert name in surface
    assert ("spawn_open_code" if claude else "spawn_claude_code") not in surface
    assert len((surface & expected) - {"tracker_file_work", "tracker_comment", "tracker_set_state"}) == len(CODING_TOOLS)


def test_claude_spawn_keeps_delegated_ifc_boundary(monkeypatch):
    from mimir.plugins.core import policy_scope
    from mimir.models import AuthContext, TurnInteractivity
    from tests.test_information_flow import SinkGate, _labels

    monkeypatch.setenv("MIMIR_MODEL_SPEC", "claude-code:sonnet")
    selected = load_plugins(enable="cultivate").policy
    poller = AuthContext(
        principal="service:poller", canonical_principal="poller", roles=("service",),
        event_ingress=None, trigger="poller", channel_id="poller:external",
        interactivity=TurnInteractivity.NON_INTERACTIVE, is_service=True,
        enforcement_enabled=True,
    )
    with policy_scope(selected):
        decision = SinkGate.check_sink_flow(
            "spawn_claude_code", "untrusted task",
            _labels(sources=frozenset({"poller:external"})), poller, enforce=True,
        )
    assert decision.allowed is False
    assert decision.reason == "ifc_label_blocked:spawn"


def test_review_skill_is_route_neutral():
    from importlib.resources import files

    text = files("mimir_cultivate").joinpath("defaults/skills/review/SKILL.md").read_text()
    assert "spawn_open_code" not in text
    assert "spawn_claude_code" not in text
    assert "does not delegate" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("load_token_late", [False, True])
async def test_spawn_uses_restricted_runner_and_redacted_artifacts(spawn_tree, tmp_path, monkeypatch, load_token_late):
    home, seed = spawn_tree
    secret = "oauth-" + "owned-canary"
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", secret)
    monkeypatch.setenv("MIMIR_MODEL_SPEC", "claude-code:sonnet")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "api-" + "owned-canary")
    if load_token_late:
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN")

        def load_model():
            monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", secret)
            return "claude-code:sonnet"

        monkeypatch.setattr("mimir.config.model_spec_at_call_time", load_model)
    record = {}

    async def runner(argv, directory, env, projections, **kwargs):
        record.update(argv=argv, env=env, projections=projections, kwargs=kwargs)
        assert argv[0] == CLAUDE_EXECUTABLE
        assert env["CLAUDE_CODE_OAUTH_TOKEN"] == secret
        assert env["MIMIR_SPAWN_DEPTH"] == "1"
        assert not {"GH_TOKEN", "GITHUB_TOKEN", "ANTHROPIC_API_KEY", "HOME", "CLAUDE_CONFIG_DIR"} & env.keys()
        assert kwargs["home_projection_profile"] == "claude_leaf"
        assert kwargs["timeout_s"] == 19
        assert {p.path for p in projections} == {".claude/.claude.json", ".claude/settings.json"}
        assert argv[argv.index("--allowedTools") + 1] == "Read,Write,Edit,Glob,Grep,Bash(git:*),Bash(uv:*)"
        assert "--no-session-persistence" in argv
        assert argv[argv.index("--model") + 1] == "haiku"
        (directory.path / "generated.txt").write_text("generated\n")
        stdout = json.dumps({"type": "result", "subtype": "success", "is_error": False, "result": secret}).encode()
        return CollectedExecutionResult(0, stdout, secret.encode(), False, False, 0, 0)

    result = json.loads(await _spawn_claude_code_impl(
        "make a change " + secret, str(seed), timeout_s=19, name=secret, model="haiku",
        contained_runner=runner, checkout_factory=factory_for(tmp_path, record),
    ))
    assert result["status"] == "succeeded"
    assert result["proposal"]["byte_length"] > 0
    checkout = record["checkout"]
    assert checkout.closed and not checkout.retained
    directory = home / "artifacts" / result["artifact_dir"]
    assert {p.name for p in directory.iterdir()} == {
        "prompt.md", "stdout.txt", "stderr.txt", "manifest.json", "proposal.json",
    }
    for path in directory.iterdir():
        assert secret not in path.read_text()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert secret not in json.dumps(result)
    assert secret not in " ".join(record["argv"])


@pytest.mark.asyncio
async def test_spawn_depth_refuses_before_effects(spawn_tree, monkeypatch):
    _, seed = spawn_tree
    monkeypatch.setenv("MIMIR_SPAWN_DEPTH", "1")
    monkeypatch.setenv("MIMIR_SPAWN_MAX_DEPTH", "1")

    def forbidden(*args, **kwargs):
        pytest.fail("depth refusal must precede provisioning and execution")

    result = await _spawn_claude_code_impl(
        "implement", str(seed), contained_runner=forbidden, checkout_factory=forbidden,
    )
    assert "recursion depth cap reached" in result


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", ["GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY", "GIT_CONFIG_COUNT", "ANTHROPIC_API_KEY"])
async def test_spawn_refuses_epic_or_caller_config(spawn_tree, monkeypatch, credential):
    _, seed = spawn_tree
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-" + "owned")
    monkeypatch.setenv(credential, "caller-" + "owned")

    def forbidden(*args, **kwargs):
        pytest.fail("credentials must be refused before checkout or execution")

    result = json.loads(await _spawn_claude_code_impl(
        "implement", str(seed), pass_env=(credential,),
        contained_runner=forbidden, checkout_factory=forbidden,
    ))
    assert result["status"] == "configuration_refused"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected", [
    ("timeout", "timeout"), ("overflow", "output_overflow"),
    ("malformed", "failed"), ("auth", "authentication_required"),
    ("quota", "failed"), ("bounded", "proposal_overflow"),
])
async def test_spawn_terminal_and_proposal_bounds(spawn_tree, tmp_path, monkeypatch, mode, expected):
    from mimir_cultivate import opencode
    _, seed = spawn_tree
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-" + "owned")
    if mode == "bounded":
        monkeypatch.setattr(opencode, "MAX_PROPOSAL_PATCH_BYTES", 1)
    record = {}

    async def runner(argv, directory, env, projections, **kwargs):
        event = {"type": "result", "subtype": "success", "is_error": False, "result": "done"}
        if mode in {"auth", "quota"}:
            event.update(subtype="error_during_execution", is_error=True,
                         api_error_status=401 if mode == "auth" else 429)
        (directory.path / "generated.txt").write_text("generated\n")
        stdout = b"not-json" if mode == "malformed" else json.dumps(event).encode()
        return CollectedExecutionResult(0, stdout, b"", mode == "timeout", mode == "overflow", 0, 0)

    result = json.loads(await _spawn_claude_code_impl(
        "implement", str(seed), contained_runner=runner,
        checkout_factory=factory_for(tmp_path, record),
    ))
    assert result["status"] == expected
    if mode not in {"timeout"}:
        assert result["exit_code"] == 0
    assert result["proposal"] is None
    assert record["checkout"].closed


@pytest.mark.asyncio
async def test_spawn_cancellation_retains_uncertain_checkout_and_identity(spawn_tree, tmp_path, monkeypatch):
    home, seed = spawn_tree
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-" + "owned")
    record = {}

    async def runner(*args, **kwargs):
        record["identifier"] = kwargs["identifier"]
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _spawn_claude_code_impl(
            "implement", str(seed), contained_runner=runner,
            checkout_factory=factory_for(tmp_path, record),
        )
    assert record["checkout"].closed and record["checkout"].retained
    manifest, = (home / "artifacts").glob("*/manifest.json")
    document = json.loads(manifest.read_text())
    assert document["status"] == "interrupted"
    assert document["worker_identifier"] == record["identifier"]
    assert document["checkout_retained"] is True


@pytest.mark.asyncio
async def test_spawn_does_not_fall_back_to_api_key(spawn_tree, monkeypatch):
    _, seed = spawn_tree
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "api-" + "owned")

    def forbidden(*args, **kwargs):
        pytest.fail("missing subscription token must refuse before effects")

    result = json.loads(await _spawn_claude_code_impl(
        "implement", str(seed), contained_runner=forbidden, checkout_factory=forbidden,
    ))
    assert result["status"] == "configuration_refused"
