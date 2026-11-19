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
