"""Fixed, content-free reasons from the real scoped-grant authorization path."""

from __future__ import annotations

from dataclasses import replace
from enum import StrEnum
from types import SimpleNamespace

import pytest

from mimir.access_control import (
    ScopedPRGrants, ToolFlowDirection, _SCOPED_PR_ISSUER,
    _valid_scoped_pr_grant, authorize_repo_pr_tool,
)


class Action(StrEnum):
    CHECKOUT = "repo.checkout"
    INSPECT = "repo.inspect"


def _authority():
    scope = SimpleNamespace(
        owner="test", scope_id="scope", canonical_repo="owner/repo", pr_number=7,
        observed_head_sha="a" * 40, allowed_operations=frozenset(Action),
    )
    lease = SimpleNamespace(path="/lease", recovery_id="recovery")
    protocol = SimpleNamespace(
        owner="test", action_type=Action, checkout_action=Action.CHECKOUT,
        checkout_tool=None, trusted_scope=None,
        live_lease=lambda *_: lease, validate_scope=lambda *_: True,
    )
    grants = ScopedPRGrants("principal", (scope,), protocols={"test": protocol}, _issuer=_SCOPED_PR_ISSUER)
    token = grants.consume_checkout("test", scope)
    assert grants.observe_checkout("test", scope, lease, token)
    state = SimpleNamespace(action_scope=scope, checkout_lease=lease)
    auth = SimpleNamespace(canonical_principal="principal", scoped_pr_grants=grants)
    return scope, state, auth, protocol


@pytest.mark.parametrize("code", [
    "grants_missing", "grant_protocol_mismatch", "grant_action_type_mismatch",
    "grant_scope_type_mismatch", "grant_principal_mismatch", "review_state_superseded",
    "grant_tool_missing", "grant_missing", "grant_issuer_mismatch", "grant_owner_mismatch",
    "grant_bound_principal_mismatch",
    "grant_turn_mismatch", "grant_expired", "grant_scope_mismatch", "grant_repository_mismatch",
    "grant_pr_mismatch", "grant_head_mismatch", "checkout_grant_has_lease",
    "lease_validation_failed", "lease_head_mismatch", "lease_invalid", "grant_lease_mismatch",
    "grant_scope_validation_failed", "grant_scope_validation_exception", "untrusted_plugin_reason",
])
@pytest.mark.parametrize("enforce", [True, False], ids=["enforced", "shadow"])
def test_scoped_grant_refusal_reason_identifies_failed_condition(code, enforce):
    scope, state, auth, protocol = _authority()
    grants = auth.scoped_pr_grants
    key = ("test", scope.scope_id)
    action = Action.INSPECT
    tool_name = "repo_diff"
    grant = grants._follow[key][action.value]
    if code == "grants_missing":
        auth.scoped_pr_grants = None
    elif code == "grant_protocol_mismatch":
        grants._protocols = {}
    elif code == "grant_action_type_mismatch":
        action = SimpleNamespace(value=Action.INSPECT.value)
    elif code == "grant_scope_type_mismatch":
        scope.owner = "foreign"
    elif code == "grant_principal_mismatch":
        auth.canonical_principal = "foreign"
    elif code == "review_state_superseded":
        auth._plugin_slot_policy = SimpleNamespace(state_slots={"test"}, scoped_actions={"test": protocol})
        auth.repo_pr_scope_registry = SimpleNamespace(resolve=lambda *_: object())
    elif code == "grant_tool_missing":
        protocol.checkout_tool = "repo_checkout"
        tool_name = None
    elif code == "grant_missing":
        grants._follow[key] = {}
    elif code == "checkout_grant_has_lease":
        action = Action.CHECKOUT
        grants._prelease[key] = replace(grant, action=action.value)
    elif code in {"lease_validation_failed", "lease_head_mismatch", "lease_invalid", "untrusted_plugin_reason"}:
        def refuse(*_, refusal_codes=None):
            if refusal_codes is not None and code != "lease_validation_failed":
                refusal_codes.append("FORGE CONTENT must never be logged" if code == "untrusted_plugin_reason" else code)
            return None
        protocol.live_lease = refuse
    elif code == "grant_scope_validation_failed":
        protocol.validate_scope = lambda *_: False
    elif code == "grant_scope_validation_exception":
        def invalid(*_):
            raise ValueError("FORGE CONTENT must never be logged")
        protocol.validate_scope = invalid
    else:
        field, value = {
            "grant_issuer_mismatch": ("_issuer", object()),
            "grant_owner_mismatch": ("owner", "foreign"),
            "grant_bound_principal_mismatch": ("principal", "foreign"),
            "grant_turn_mismatch": ("turn_nonce", "foreign"),
            "grant_expired": ("expires_at", 0),
            "grant_scope_mismatch": ("scope_id", "foreign"),
            "grant_repository_mismatch": ("repository", "foreign/repo"),
            "grant_pr_mismatch": ("pull_request", 8),
            "grant_head_mismatch": ("head", "b" * 40),
            "grant_lease_mismatch": ("lease_identity", ("/lease", "foreign")),
        }[code]
        grants._follow[key][action.value] = replace(grant, **{field: value})
    expected = {"untrusted_plugin_reason": "lease_validation_failed",
                "grant_bound_principal_mismatch": "grant_principal_mismatch",
                "grant_scope_validation_exception": "grant_scope_validation_failed"}.get(code, code)
    diagnostics = []
    assert not _valid_scoped_pr_grant(
        auth, "test", action, protocol, scope, state, tool_name=tool_name, diagnostics=diagnostics,
    )
    assert diagnostics == [expected]
    decision = authorize_repo_pr_tool(
        tool_name, scope, service_principal=None, enforce=enforce, flow_direction=ToolFlowDirection.SOURCE,
        required_actions=(action.value,), registered_plugin=True, auth_context=auth,
        owner="test", action=action, protocol=protocol, review_state=state,
    )
    assert decision.allowed is (not enforce)
    assert decision.would_block
    assert decision.as_log_fields()["refusal_reason"] == expected


def test_scoped_grant_success_has_no_refusal_reason():
    scope, state, auth, protocol = _authority()
    decision = authorize_repo_pr_tool(
        "repo_diff", scope, service_principal=None, enforce=True, flow_direction=ToolFlowDirection.SOURCE,
        required_actions=(Action.INSPECT.value,), registered_plugin=True, auth_context=auth,
        owner="test", action=Action.INSPECT, protocol=protocol, review_state=state,
    )
    assert decision.allowed
    assert decision.as_log_fields()["refusal_reason"] is None
