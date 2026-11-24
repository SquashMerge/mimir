"""Enforced escalation remains possible after write authority has refused."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import importlib
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain.agents.middleware import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.runtime import Runtime

from mimir._context import reset_current_turn, set_current_turn
from mimir.access_control import (
    BOUNDED_OPERATOR_NOTICE, BOUNDED_UNSUPPORTED_NOTICE, CapabilityTier,
    ToolRegistry, build_trigger_service_principal,
)
from mimir.agent import _create_turn_auth_context
from mimir.models import AgentEvent, InformationFlowLabels, InformationFlowState, SourceLabel, TurnContext
from mimir.plugins.core import policy_scope
from mimir.tools.budget_gate import BudgetGateMiddleware
from mimir_cultivate import forge
from mimir_cultivate.policy import RepoPRScopeRegistry, RepoReviewState, ServerDiscoveredPRStates
from test_scoped_grants import _remediation_turn


def _turn(tmp_path, monkeypatch, *, completion=False, tainted=False, service=True, retained=False):
    _, scope, state, auth, _ = _remediation_turn(tmp_path, monkeypatch)
    policy = auth._plugin_slot_policy
    principal = build_trigger_service_principal(
        canonical="poller:github-activity", trigger="poller", profile="github",
        tier=CapabilityTier.CODE_EXECUTION,
        capabilities=("operator_alert", "unsupported_operation"),
        creation_path="test",
    )
    labels = InformationFlowLabels(sources=(SourceLabel(
        principal="outsider", domain="web", resource_id="https://untrusted.invalid",
        bridge_instance="web", sensitivity="internal",
        integrity="untrusted", integrity_effect="active_ingest",
    ),)) if tainted else InformationFlowLabels()
    cache = ServerDiscoveredPRStates()
    if retained:
        cache.remember_escalation_scope(scope)
    auth = replace(
        auth, canonical_principal=principal.canonical,
        is_service=service, service_authority=principal if service else None,
        scoped_pr_grants=None, ifc_labels=labels, ifc_state=InformationFlowState(labels=labels),
        server_discovered_pr_states=cache,
        repo_pr_scope_registry=None if retained else RepoPRScopeRegistry((state,)),
    )
    if completion:
        event = AgentEvent(
            trigger="repo_test_complete", channel_id=auth.channel_id, source="system",
            continuation_auth_context=auth, ifc_labels=labels,
        )
        with policy_scope(policy):
            continued = _create_turn_auth_context(
                event, None, policy_version=None, enforce=True, ifc_labels=labels, policy=policy,
            )
        assert continued.service_authority is auth.service_authority
        assert continued.canonical_principal == auth.canonical_principal
        assert continued.scoped_pr_grants is None
        auth = continued
    # Failed/expired checkout authority must not prevent escalation.
    state.checkout_lease.revoke()
    monkeypatch.setenv("MIMIR_OPERATOR_ALERT_CHANNEL", "discord-operator")
    monkeypatch.setenv("MIMIR_HOME", str(tmp_path))
    return scope, auth, policy


@pytest.mark.asyncio
@pytest.mark.parametrize("enforce", [False, True])
@pytest.mark.parametrize("completion,tainted,service,retained", [
    (False, False, True, False),
    (True, False, True, False),
    (True, True, True, False),
    (True, True, False, False),  # observed null service principal
    (False, True, True, True),  # discovery retained only escalation authority
])
async def test_enforced_bounded_escalation(tmp_path, monkeypatch, completion, tainted, service, retained, enforce):
    alerts = importlib.import_module("mimir.tools.operator_alert")
    from mimir import event_logger

    scope, auth, policy = _turn(
        tmp_path, monkeypatch, completion=completion, tainted=tainted, service=service, retained=retained,
    )
    events = []
    monkeypatch.setattr(event_logger, "log_durable_event_sync", lambda *a, **kw: events.append((a, kw)))
    channels = SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(sent=True, message_id="one")))
    monkeypatch.setattr(alerts, "_channel_registry", channels)
    monkeypatch.setattr(alerts, "_config", SimpleNamespace(operator_alert_channel="discord-operator"))
    turn = TurnContext(
        turn_id="bounded-escalation", session_id=auth.channel_id, trigger=auth.trigger,
        channel_id=auth.channel_id, started_at=time.monotonic(),
        auth_context=auth, ifc_labels=auth.ifc_labels,
    )
    turn.escalation_job_id = "rt_" + "f" * 32 if completion else None
    turn.hard_boundary_denials.append({"tool": "repo_push", "reason": "repo_pr_grant_denied"})
    monkeypatch.setenv("MIMIR_ACCESS_CONTROL_ENFORCED", "1" if enforce else "0")
    token = set_current_turn(turn)
    args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number,
            "description": "SECRET untrusted instructions", "attempted_operations": ["SECRET attempt"]}
    gate = BudgetGateMiddleware(policy)
    try:
        with policy_scope(policy):
            for name, arguments in (("operator_alert", {"text": "SECRET caller prose"}),
                                    ("unsupported_operation", args)):
                decision = ToolRegistry().authorize_tool(
                    name, auth, enforce=enforce, arguments=arguments, policy=policy,
                )
                assert decision.allowed and not decision.would_block, decision
                request = ToolCallRequest(
                    tool_call={"name": name, "args": arguments, "id": name, "type": "tool_call"},
                    tool=policy.tools[name].factory(), state={}, runtime=Runtime(context=auth),
                )

                async def handler(call):
                    if name == "operator_alert":
                        result = await alerts.operator_alert.coroutine(**call.tool_call["args"])
                    else:
                        result = forge.unsupported_operation.func(
                            **call.tool_call["args"], runtime=SimpleNamespace(context=auth),
                        )
                    return ToolMessage(content=json.dumps(result), tool_call_id=name)

                result = await gate.awrap_tool_call(request, handler)
                assert result.status != "error", result.content
    finally:
        reset_current_turn(token)
    if tainted:
        expected = {"turn_id": "bounded-escalation", "trigger": auth.trigger,
                    "repository": scope.canonical_repo, "pull_request": scope.pr_number,
                    "head": scope.observed_head_sha, "tool": "repo_push", "reason": "repo_pr_grant_denied"}
        if completion:
            expected["job_id"] = "rt_" + "f" * 32
        alert_text = channels.send.await_args.args[1]
        assert alert_text.startswith(BOUNDED_OPERATOR_NOTICE + " ")
        assert json.loads(alert_text[len(BOUNDED_OPERATOR_NOTICE) + 1:]) == expected
        description = events[0][1]["description"]
        assert description.startswith(BOUNDED_UNSUPPORTED_NOTICE + " ")
        assert json.loads(description[len(BOUNDED_UNSUPPORTED_NOTICE) + 1:]) == expected
        assert events[0][1]["attempted_operations"] == []
        assert "SECRET" not in json.dumps(events) + alert_text
    else:
        channels.send.assert_awaited_once_with("discord-operator", "SECRET caller prose", final=False)
        assert events[0][1]["description"] == args["description"]
        assert events[0][1]["attempted_operations"] == args["attempted_operations"]
        assert events[0][1]["operation"] == forge._operation_key(args["description"])
    assert auth.ifc_state.has_untrusted_active_ingest() is tainted


@pytest.mark.parametrize("enforce", [False, True])
def test_live_taint_overrides_clean_auth_snapshot(tmp_path, monkeypatch, enforce):
    from mimir.access_control import bounded_escalation_turn

    scope, auth, policy = _turn(tmp_path, monkeypatch)
    assert not bounded_escalation_turn(auth)
    taint = InformationFlowLabels(sources=(SourceLabel(
        principal="outsider", domain="web", resource_id="https://untrusted.invalid/new",
        bridge_instance="web", sensitivity="internal",
        integrity="untrusted", integrity_effect="active_ingest",
    ),))
    auth.ifc_state.merge(taint)
    assert bounded_escalation_turn(auth)
    with policy_scope(policy):
        decision = ToolRegistry().authorize_tool(
            "unsupported_operation", auth, enforce=enforce, policy=policy,
            arguments={"repository": scope.canonical_repo, "pull_request": scope.pr_number},
        )
    assert decision.allowed and not decision.would_block


def test_tainted_escalation_dedup_uses_host_blocker_not_prose_or_turn(tmp_path, monkeypatch):
    from mimir import event_logger

    scope, auth, policy = _turn(tmp_path, monkeypatch, tainted=True)
    events = []
    monkeypatch.setattr(event_logger, "log_durable_event_sync", lambda *a, **kw: events.append(kw))
    turn = TurnContext(
        turn_id="first", session_id=auth.channel_id, trigger=auth.trigger,
        channel_id=auth.channel_id, started_at=time.monotonic(),
        auth_context=auth, ifc_labels=auth.ifc_labels,
    )
    turn.hard_boundary_denials.append({"tool": "repo_push", "reason": "repo_pr_grant_denied"})
    token = set_current_turn(turn)
    try:
        with policy_scope(policy):
            def emit(prose):
                return forge.unsupported_operation.func(
                    repository=scope.canonical_repo, pull_request=scope.pr_number,
                    description=prose, runtime=SimpleNamespace(context=auth),
                )
            assert emit("SECRET first")["escalated"]
            turn.turn_id = "second"
            assert not emit("SECRET changed prose")["escalated"]
            turn.hard_boundary_denials.append({"tool": "repo_test", "reason": "test_containment_unavailable"})
            assert emit("SECRET first")["escalated"]
            turn.hard_boundary_denials.append({"tool": "repo_test", "reason": "test_snapshot_unavailable"})
            assert emit("SECRET first")["escalated"]
    finally:
        reset_current_turn(token)
    assert len(events) == 3
    assert len({event["operation"] for event in events}) == 3
    assert "SECRET" not in json.dumps(events)


@pytest.mark.parametrize("invalid", [
    "channel", "repo", "pr", "target", "ingress", "no_scope",
    "untyped_state", "untyped_scope", "non_escalation", "no_destination", "no_capability",
    "boolean_pr", "no_protocol", "other_owner", "user_turn",
])
def test_bounded_escalation_negative_controls(tmp_path, monkeypatch, invalid):
    scope, auth, policy = _turn(tmp_path, monkeypatch, completion=True, tainted=True, service=False)
    name = "unsupported_operation"
    args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}
    target = None
    if invalid == "channel":
        name, target = "operator_alert", "discord-other"
    elif invalid == "repo":
        args["repository"] = "different/repo"
    elif invalid == "pr":
        args["pull_request"] += 1
    elif invalid == "target":
        target = "different/pr"
    elif invalid == "ingress":
        auth = replace(auth, event_ingress="http")
    elif invalid in {"no_scope", "untyped_state", "untyped_scope", "no_capability"}:
        registry = None
        if invalid == "untyped_state":
            registry = SimpleNamespace(review_states=(SimpleNamespace(action_scope=scope),))
        elif invalid == "untyped_scope":
            fake_scope = SimpleNamespace(**vars(scope))
            registry = SimpleNamespace(review_states=(RepoReviewState(fake_scope),))
        auth = replace(auth, repo_pr_scope_registry=registry)
        if invalid == "no_capability":
            name = "operator_alert"
    elif invalid == "non_escalation":
        name = "repo_push"
    elif invalid == "no_destination":
        name = "operator_alert"
        monkeypatch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL")
    elif invalid == "boolean_pr":
        # Match the bound PR numerically to isolate Python's bool-as-int hazard.
        fake_scope = replace(scope, pr_number=1)
        auth = replace(auth, repo_pr_scope_registry=RepoPRScopeRegistry((RepoReviewState(fake_scope),)))
        args["pull_request"] = True
    elif invalid == "no_protocol":
        policy._scoped_actions.pop("cultivate")
    elif invalid == "other_owner":
        policy._tools[name] = replace(policy.tools[name], owner="other")
    elif invalid == "user_turn":
        auth = replace(auth, trigger="user_message")
    with policy_scope(policy):
        decision = ToolRegistry().authorize_tool(
            name, auth, enforce=True, arguments=args, target_channel=target, policy=policy,
        )
    assert not decision.allowed, (invalid, decision)


@pytest.mark.parametrize("invalid", ["channel", "http", "inherited_http", "carrier"])
def test_completion_escalation_cannot_rebind_continuation(tmp_path, monkeypatch, invalid):
    from mimir.access_control import HTTP_EVENT_INGRESS_EXTRA_KEY
    from mimir.agent import _shell_continuation_auth_context

    scope, auth, policy = _turn(tmp_path, monkeypatch)
    event = AgentEvent(
        trigger="repo_test_complete", channel_id=auth.channel_id,
        continuation_auth_context=auth, source="system",
    )
    if invalid == "channel":
        event.channel_id = "other"
    elif invalid == "http":
        event.extra[HTTP_EVENT_INGRESS_EXTRA_KEY] = "http"
    elif invalid == "inherited_http":
        event.continuation_auth_context = replace(auth, event_ingress="http")
    elif invalid == "carrier":
        event.continuation_auth_context = SimpleNamespace(channel_id=auth.channel_id)
    assert _shell_continuation_auth_context(event) is None
    with policy_scope(policy):
        continued = _create_turn_auth_context(
            event, None, policy_version=None, enforce=True,
            ifc_labels=auth.ifc_labels, policy=policy,
        )
        decision = ToolRegistry().authorize_tool(
            "unsupported_operation", continued, enforce=True, policy=policy,
            arguments={"repository": scope.canonical_repo, "pull_request": scope.pr_number},
        )
    assert not decision.allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["operator_alert", "unsupported_operation"])
async def test_bounded_escalation_shadow_audits_only_out_of_scope(tmp_path, monkeypatch, name):
    from mimir import event_logger

    scope, auth, policy = _turn(tmp_path, monkeypatch, completion=True, tainted=True)
    records = []
    logged = asyncio.Event()

    async def record(event, **fields):
        if event == "shadow_tool_decision" and fields.get("tool") == name:
            records.append((event, fields))
            logged.set()

    monkeypatch.setattr(event_logger, "log_event", record)
    registry = ToolRegistry()
    registry.enable_shadow_logging()
    args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}
    with policy_scope(policy):
        allowed = registry.authorize_tool(name, auth, arguments=args, enforce=False, policy=policy)
        denied = registry.authorize_tool(
            name, auth, arguments=args, target_channel="out-of-scope",
            enforce=False, policy=policy,
        )
        await asyncio.wait_for(logged.wait(), timeout=10)
    assert allowed.allowed and not allowed.would_block
    assert not denied.allowed and denied.would_block
    assert len(records) == 1
    event, fields = records[0]
    assert event == "shadow_tool_decision"
    assert fields["would_block"] is True and fields["enforcement_enabled"] is False
    assert fields["requested_target"] == "out-of-scope"
