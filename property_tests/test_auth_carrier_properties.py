"""Same-turn matching must ignore label snapshots, not authority or cell identity."""

from __future__ import annotations

from dataclasses import dataclass, replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st

from mimir import _deepagents_subagent_auth as child_auth
from mimir._context import is_same_turn_auth_carrier
from mimir.models import (
    AuthContext,
    InformationFlowLabels,
    InformationFlowState,
    SessionACL,
    TurnContext,
    TurnInteractivity,
)
from mimir.plugins.registry import PolicyRegistry


def _auth() -> AuthContext:
    # Deliberately no factories or registry lookups: this tests carrier matching,
    # not ingress identity selection, and owns every authority field it uses.
    return AuthContext(
        principal="alice", canonical_principal="person:alice",
        roles=("operator",), event_ingress="test", trigger="user_message",
        channel_id="room", interactivity=TurnInteractivity.INTERACTIVE,
        domain="chat", resource_id="room", enforcement_enabled=True,
    )


def _turn(auth: AuthContext) -> TurnContext:
    return TurnContext(
        "turn", "session", "user_message", "room", 0.0, auth_context=auth,
    )


LABELS = st.frozensets(
    st.sampled_from(("public", "internal", "confidential", "private", "unknown")),
)

# An explicit oracle matrix, independent of AuthContext's equality implementation.
# A different value in any of these immutable identity/authority fields means it
# is not the same carrier, even if its mutable IFC cell is shared.
AUTHORITY_CHANGES = (
    ("principal", "bob"),
    ("canonical_principal", "person:bob"),
    ("roles", ("viewer",)),
    ("event_ingress", "other"),
    ("trigger", "scheduled_tick"),
    ("channel_id", "other-room"),
    ("interactivity", TurnInteractivity.NON_INTERACTIVE),
    ("policy_version", "other-policy"),
    ("is_service", True),
    ("service_authority", "other-service"),
    ("enforcement_enabled", False),
    ("domain", "other-domain"),
    ("resource_id", "other-resource"),
    ("bridge_instance", "other-bridge"),
    ("origin_trigger", "other-trigger"),
    ("origin_ref", "other-ref"),
    ("ci_run_targets", frozenset({("owner/repo", 1)})),
    ("ci_branch_targets", frozenset({("owner/repo", "main", None)})),
    ("tracker_issue_targets", frozenset({("tracker", "1", "https://example.test/1")})),
    ("saga_session_id", "other-session"),
    ("source_session_acl", SessionACL(
        owner_principal="bob", origin_channel="other-room", origin_domain="chat",
        visibility="private", provenance_complete=True,
    )),
    ("cross_platform_pull", False),
    ("domain_qualifier", "other-qualifier"),
)


@settings(max_examples=150, deadline=None)
@given(st.lists(LABELS, min_size=1, max_size=25))
def test_label_replacement_histories_preserve_same_turn_matching(
    snapshots: list[frozenset[str]],
) -> None:
    ingress = _auth()
    turn = _turn(ingress)
    history = [ingress]
    for labels in snapshots:
        updated = replace(turn.auth_context, ifc_labels=InformationFlowLabels(labels=labels))
        turn.auth_context = updated
        history.append(updated)
        assert all(is_same_turn_auth_carrier(turn, carrier) for carrier in history)
    event(f"history_length={min(len(snapshots), 5)}")


@settings(max_examples=150, deadline=None)
@given(LABELS, st.sampled_from(AUTHORITY_CHANGES))
def test_authority_changes_never_match_a_same_cell_carrier(
    labels: frozenset[str], change: tuple[str, object],
) -> None:
    ingress = _auth()
    live = replace(ingress, ifc_labels=InformationFlowLabels(labels=labels))
    field, value = change
    candidate = replace(ingress, **{field: value})
    assert candidate.ifc_state is live.ifc_state
    assert not is_same_turn_auth_carrier(_turn(live), candidate)
    event(f"changed_field={field}")


@settings(max_examples=100, deadline=None)
@given(LABELS, LABELS)
def test_equal_looking_foreign_ifc_cells_never_match(
    old: frozenset[str], new: frozenset[str],
) -> None:
    ingress = replace(_auth(), ifc_labels=InformationFlowLabels(labels=old))
    live = replace(ingress, ifc_labels=InformationFlowLabels(labels=new))
    foreign = replace(ingress, ifc_state=InformationFlowState())
    assert not is_same_turn_auth_carrier(_turn(live), foreign)


def test_distinct_plugin_policy_identity_never_matches() -> None:
    ingress = _auth()
    other_policy = PolicyRegistry()
    other_policy.freeze()
    foreign = replace(ingress, _plugin_slot_policy=other_policy)
    assert not is_same_turn_auth_carrier(_turn(ingress), foreign)


def test_authority_oracle_detects_an_equality_widening_mutation() -> None:
    # Calibrate the very assertion used by the generated property against a
    # plausible over-permissive identity comparison. The patch is restored even
    # when the detector raises; no production file is changed.
    with patch.object(AuthContext, "__eq__", lambda _self, _other: True):
        with pytest.raises(AssertionError):
            test_authority_changes_never_match_a_same_cell_carrier.hypothesis.inner_test(
                frozenset({"internal"}), ("roles", ("viewer",)),
            )


@settings(max_examples=100, deadline=None)
@given(st.lists(
    st.tuples(
        st.sampled_from(("ingress", "foreign", "lookalike", "missing")),
        st.booleans(),
    ),
    min_size=1, max_size=25,
))
def test_child_auth_dispatch_restores_context_after_success_and_failure(
    operations: list[tuple[str, bool]],
) -> None:
    """Replay dispatches with explicit failure schedules and context restoration."""
    @dataclass(frozen=True)
    class ProbeState:
        revision: int

    policy = PolicyRegistry()
    registrar = policy.registrar("probe")
    registrar.register_state_slot(ProbeState)
    policy.freeze()
    ingress = replace(_auth(), _plugin_slot_policy=policy).with_plugin_state(
        registrar, ProbeState(-1),
    )
    turn = _turn(ingress)
    turn.auth_context = replace(
        ingress, ifc_labels=InformationFlowLabels(labels=frozenset({"internal"})),
    )
    foreign = _auth()
    carriers = {
        "ingress": ingress, "foreign": foreign,
        "lookalike": SimpleNamespace(principal=ingress.principal), "missing": None,
    }
    outer = _auth()
    outer_token = child_auth._subagent_auth_context.set(outer)
    try:
        with patch.object(child_auth, "get_current_turn", return_value=turn):
            for revision, (kind, fails) in enumerate(operations):
                turn.set_plugin_state(registrar, ProbeState(revision))
                carrier = carriers[kind]
                expected = (
                    turn.auth_context if kind == "ingress"
                    else foreign if kind == "foreign" else None
                )

                def invoke(**_kwargs: object) -> None:
                    observed = child_auth._subagent_auth_context.get()
                    if expected is None:
                        assert observed is None
                    else:
                        assert observed == expected
                        assert observed.ifc_state is expected.ifc_state
                        assert observed.plugin_state("probe") == (
                            ProbeState(revision) if kind == "ingress" else None
                        )
                    if fails:
                        raise RuntimeError("injected child failure")

                tool = child_auth._wrap_task_tool(SimpleNamespace(
                    func=invoke, coroutine=None,
                ))
                runtime = SimpleNamespace(context=carrier)
                if fails:
                    with pytest.raises(RuntimeError, match="injected child failure"):
                        tool.func(runtime=runtime)
                else:
                    tool.func(runtime=runtime)
                # This is explicitly installed test-owned state, not an assertion
                # about an ambient task registry or another test's ContextVars.
                assert child_auth._subagent_auth_context.get() is outer
                event(f"dispatch={kind},failure={fails}")
    finally:
        child_auth._subagent_auth_context.reset(outer_token)
