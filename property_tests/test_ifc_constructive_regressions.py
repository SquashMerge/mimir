"""Deterministic constructive contract controls, not claimed discoveries."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from mimir import models
from mimir.access_control import ChannelResourceAdapter, SinkGate
from mimir.models import (
    AuthContext,
    InformationFlowLabels,
    InformationFlowState,
    IngestAcknowledgement,
    SourceLabel,
    TurnInteractivity,
    _mint_owner_attestation,
)


def test_ack_source_capacity_boundary_is_audit_atomic(monkeypatch) -> None:
    monkeypatch.setattr(models, "time", SimpleNamespace(monotonic=lambda: 10000.0))
    first = SourceLabel(
        principal="alice", domain="web", resource_id="first",
        bridge_instance="local", sensitivity="private",
        authorized_principals=frozenset({"alice"}), source_kind="protected_tool",
        integrity="untrusted", integrity_effect="active_ingest",
    )
    second = replace(first, resource_id="second")
    labels = InformationFlowLabels(sources=(first, second))
    ack = IngestAcknowledgement(last_used=10000.0)
    ack.MAX_SOURCES = 1
    state = InformationFlowState(labels=labels, ingest_acknowledgement=ack)
    snapshot, ordinal = state.ingest_snapshot()
    calls = []

    def audit(sources):
        calls.append(frozenset(source.resource_id for source in sources))
        return True

    assert not state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=audit,
    )
    assert calls == []
    assert state.has_untrusted_active_ingest()
    ack.MAX_SOURCES = 2
    assert state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=audit,
    )
    assert calls == [frozenset({"first", "second"})]
    assert not state.has_untrusted_active_ingest()
    assert state.current() == labels
    ack.last_used -= ack.IDLE_SECONDS
    assert state.has_untrusted_active_ingest()
    assert not state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=audit,
    )
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ("recent_activity_user", "owner_attested_feedback"))
def test_owner_same_channel_shortcut_is_not_an_attestation_requirement(monkeypatch, kind) -> None:
    """Owner attestation is needed for the cross-channel source arm, not shortcut."""
    monkeypatch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
    monkeypatch.setattr(ChannelResourceAdapter, "_global_resolver", None)
    source = SourceLabel(
        principal="alice", domain="channel", resource_id="slack-DEST",
        bridge_instance="slack", sensitivity="private",
        authorized_principals=frozenset({"alice"}), source_kind=kind,
        integrity="untrusted", integrity_effect="active_ingest",
    )
    labels = InformationFlowLabels(sources=(source,))
    auth = AuthContext(
        principal="slack-U1", canonical_principal="alice", roles=("user",),
        event_ingress=None, trigger="user_message", channel_id="slack-DEST",
        interactivity=TurnInteractivity.INTERACTIVE,
        resource_id="slack-DEST", domain="channel", bridge_instance="slack",
        ifc_labels=labels, ifc_state=InformationFlowState(labels=labels),
        audience_provider=None, cross_platform_pull=False,
    )
    assert SinkGate.check_sink_flow(
        "send_message", "slack-DEST", labels, auth, enforce=True,
    ).allowed
    # A single bridge-authority change declines the shortcut, so the missing
    # owner attestation must now fail closed.
    changed = InformationFlowLabels(sources=(replace(source, bridge_instance="other"),))
    auth = replace(auth, ifc_labels=changed, ifc_state=InformationFlowState(labels=changed))
    denied = SinkGate.check_sink_flow("send_message", "slack-DEST", changed, auth, enforce=True)
    assert not denied.allowed
    assert denied.reason == "ifc_label_blocked:same_channel"


def test_inconsistent_owner_channel_binding_does_not_gain_egress(monkeypatch) -> None:
    """An inconsistent owner-channel binding never gains egress (#2178)."""
    monkeypatch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
    monkeypatch.setattr(ChannelResourceAdapter, "_global_resolver", None)
    source = SourceLabel(
        principal="alice", domain="channel", resource_id="slack-SRC",
        bridge_instance="slack", sensitivity="private",
        authorized_principals=frozenset({"alice"}), source_kind="recent_activity_user",
        integrity="untrusted", integrity_effect="active_ingest",
        owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-WRONG"),
    )
    labels = InformationFlowLabels(sources=(source,))
    auth = AuthContext(
        principal="slack-U1", canonical_principal="alice", roles=("user",),
        event_ingress=None, trigger="user_message", channel_id="slack-DEST",
        interactivity=TurnInteractivity.INTERACTIVE,
        resource_id="slack-DEST", domain="channel", bridge_instance="slack",
        ifc_labels=labels, ifc_state=InformationFlowState(labels=labels),
        audience_provider=SimpleNamespace(audience_for=lambda *_a, **_k: frozenset({"alice"})),
        cross_platform_pull=False,
    )
    for enforce in (False, True):
        decision = SinkGate.check_sink_flow(
            "send_message", "slack-DEST", labels, auth, enforce=enforce,
        )
        assert decision.allowed is (not enforce)
        assert decision.would_block
        assert decision.reason == "ifc_label_blocked:same_channel"




def test_new_owner_attestation_channel_identity_retaints_primitive_state(monkeypatch) -> None:
    """A new active-ingest identity must not inherit another source's clearance.

    This is a local primitive contract, not a claim that the malformed owner
    binding is produced by ingress or accepted for ordinary channel egress.
    """
    monkeypatch.setattr(models, "time", SimpleNamespace(monotonic=lambda: 10000.0))
    first = SourceLabel(
        principal="alice", domain="channel", resource_id="slack-SRC",
        bridge_instance="slack", sensitivity="private",
        authorized_principals=frozenset({"alice"}), source_kind="recent_activity_user",
        integrity="untrusted", integrity_effect="active_ingest",
        owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-SRC"),
    )
    labels = InformationFlowLabels(sources=(first,))
    state = InformationFlowState(
        labels=labels, ingest_acknowledgement=IngestAcknowledgement(last_used=10000.0),
    )
    snapshot, ordinal = state.ingest_snapshot()
    assert state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=lambda _: True,
    )
    assert not state.has_untrusted_active_ingest()
    second = replace(
        first, owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-OTHER"),
    )
    assert first != second
    assert second.has_untrusted_active_ingest
    _, receipt = state.merge_with_receipt(InformationFlowLabels(sources=(second,)))
    assert receipt.source_changed and receipt.source_arrived
    assert state.current().sources == (first, second)
    assert state.has_untrusted_active_ingest(), (
        "a distinct owner-channel attestation inherited review clearance (#2178)"
    )


@pytest.mark.parametrize("active", (True, False), ids=("local-active", "real-prompt-informational"))
@pytest.mark.parametrize("coordinated", (True, False), ids=("bound-change", "channel-only-malformed"))
def test_attestation_identity_boundary_observations(monkeypatch, active, coordinated) -> None:
    """Every attestation change re-taints an active source (#2178).

    The active owner shape is accepted by SourceLabel but is a local API input,
    not a demonstrated current prompt-producer output. The informational shape
    matches prompt_source_label's operative trust assignment.
    """
    monkeypatch.setattr(models, "time", SimpleNamespace(monotonic=lambda: 10000.0))
    first = SourceLabel(
        principal="alice", domain="channel" if active else "recent_activity",
        resource_id="slack-SRC", bridge_instance="slack", sensitivity="private",
        authorized_principals=frozenset({"alice"}), source_kind="recent_activity_user",
        integrity="untrusted", integrity_effect="active_ingest" if active else "informational",
        owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-SRC"),
    )
    initial = InformationFlowLabels(sources=(first,))
    state = InformationFlowState(
        labels=initial, ingest_acknowledgement=IngestAcknowledgement(last_used=10000.0),
    )
    calls = []

    def audit(sources):
        calls.append(frozenset(
            (source.resource_id, source.owner_attestation.source_channel)
            for source in sources
        ))
        return True

    snapshot, ordinal = state.ingest_snapshot()
    assert state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=audit,
    ) is active
    assert calls == ([frozenset({("slack-SRC", "slack-SRC")})] if active else [])
    assert not state.has_untrusted_active_ingest()
    calls.clear()
    second = replace(
        first, resource_id="slack-OTHER" if coordinated else "slack-SRC",
        owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-OTHER"),
    )
    _, receipt = state.merge_with_receipt(InformationFlowLabels(sources=(second,)))
    assert receipt.source_changed and receipt.source_arrived
    assert state.current().sources == (first, second)
    assert state.source_arrival_ordinal() == 1
    # #2178: a channel-only attestation change is a new identity, too.
    expected_taint = active
    assert state.ingest_snapshot()[1] == int(expected_taint)
    assert state.has_untrusted_active_ingest() is expected_taint
    snapshot, ordinal = state.ingest_snapshot()
    assert state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=audit,
    ) is expected_taint
    resource = "slack-OTHER" if coordinated else "slack-SRC"
    assert calls == ([frozenset({(resource, "slack-OTHER")})] if expected_taint else [])
    assert state.current().sources == (first, second)
