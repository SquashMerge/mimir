"""Acknowledgement coverage must agree with complete source identity."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from mimir.access_control import SinkGate, _source_is_triggering_channel_compatible
from mimir.models import (
    AuthContext,
    InformationFlowLabels,
    InformationFlowState,
    IngestAcknowledgement,
    SourceLabel,
    TurnInteractivity,
    _mint_owner_attestation,
)


@pytest.fixture
def owner_source() -> SourceLabel:
    return SourceLabel(
        principal="alice",
        domain="recent_activity",
        resource_id="slack-SRC",
        bridge_instance="slack",
        sensitivity="private",
        authorized_principals=frozenset({"alice"}),
        source_kind="recent_activity_user",
        integrity="untrusted",
        integrity_effect="active_ingest",
        owner_attestation=_mint_owner_attestation("alice", "slack-U1", "slack-SRC"),
    )


def _acknowledged_state(source: SourceLabel) -> InformationFlowState:
    state = InformationFlowState()
    labels = state.merge(InformationFlowLabels().with_source(source))
    assert state.has_untrusted_active_ingest()
    assert state.acknowledge_ingest_snapshot(
        expected=labels, durable_audit=lambda sources: sources == (source,),
    )
    assert not state.has_untrusted_active_ingest()
    return state


def test_owner_attestation_channel_only_variant_retaints(owner_source):
    """Standalone witness: a distinct attested channel is a new untrusted source."""
    state = _acknowledged_state(owner_source)
    reviewed, ingest_ordinal = state.ingest_snapshot()
    source_ordinal = state.source_arrival_ordinal()
    variant = replace(owner_source, owner_attestation=_mint_owner_attestation(
        "alice", "slack-U1", "slack-OTHER",
    ))

    merged = state.merge(InformationFlowLabels().with_source(variant))

    assert merged.sources == (owner_source, variant)
    assert state.source_arrival_ordinal() == source_ordinal + 1
    assert state.ingest_snapshot()[1] == ingest_ordinal + 1
    assert state.has_untrusted_active_ingest()
    assert state.permission_has_untrusted_active_ingest()
    assert not state.acknowledge_ingest_snapshot(
        expected=reviewed, expected_ordinal=ingest_ordinal,
        durable_audit=lambda _: pytest.fail("stale review must not be audited"),
    )
    audited = []
    assert state.acknowledge_ingest_snapshot(
        expected=merged, expected_ordinal=ingest_ordinal + 1,
        durable_audit=lambda sources: audited.append(sources) or True,
    )
    assert audited == [(variant,)]
    assert not state.has_untrusted_active_ingest()


@pytest.mark.parametrize("new_turn", [False, True])
def test_identical_owner_attestation_reinjection_stays_cleared(owner_source, new_turn):
    state = _acknowledged_state(owner_source)
    if new_turn:
        state = InformationFlowState(ingest_acknowledgement=state.ingest_acknowledgement)
    ingest_ordinal = state.ingest_snapshot()[1]
    # A separately minted, equal attestation must have the same stable identity.
    identical = replace(owner_source, owner_attestation=_mint_owner_attestation(
        "alice", "slack-U1", "slack-SRC",
    ))

    merged = state.merge(InformationFlowLabels().with_source(identical))

    assert merged.sources == (owner_source,)
    assert state.ingest_snapshot()[1] == ingest_ordinal
    assert not state.has_untrusted_active_ingest()
    assert not state.permission_has_untrusted_active_ingest()
    assert not state.acknowledge_ingest_snapshot(
        expected=merged,
        durable_audit=lambda _: pytest.fail("identical reinjection needs no new audit"),
    )


@pytest.mark.parametrize(("field", "value"), [
    ("canonical_principal", "bob"),
    ("raw_author", "slack-U2"),
    ("source_channel", "slack-OTHER"),
])
def test_each_owner_attestation_field_alone_retaints(owner_source, field, value):
    state = _acknowledged_state(owner_source)
    reviewed, ingest_ordinal = state.ingest_snapshot()
    attestation_fields = {
        "canonical_principal": "alice",
        "raw_author": "slack-U1",
        "source_channel": "slack-SRC",
    }
    attestation_fields[field] = value
    variant = replace(
        owner_source,
        owner_attestation=_mint_owner_attestation(**attestation_fields),
    )
    assert variant != owner_source
    assert IngestAcknowledgement.source_identity(variant) != (
        IngestAcknowledgement.source_identity(owner_source)
    )

    merged = state.merge(InformationFlowLabels().with_source(variant))

    assert merged.sources == (owner_source, variant)
    assert state.ingest_snapshot()[1] == ingest_ordinal + 1
    assert state.has_untrusted_active_ingest()
    # Reject the old snapshot even without an ordinal supplied.
    assert not state.acknowledge_ingest_snapshot(
        expected=reviewed,
        durable_audit=lambda _: pytest.fail("unreviewed attestation must not be covered"),
    )


@pytest.mark.parametrize("attested", [False, True])
@pytest.mark.parametrize("format_version", ["legacy", "unknown"])
def test_old_or_unknown_acknowledgement_hashes_fail_closed(
    owner_source, attested, format_version,
):
    source = owner_source if attested else replace(owner_source, owner_attestation=None)
    # Exact pre-fix wire hash, deliberately independent of source_identity().
    record = [
        source.principal, source.domain, source.domain_qualifier,
        source.resource_id, source.bridge_instance, str(source.sensitivity),
        sorted(source.authorized_principals), str(source.source_kind),
        str(source.integrity), str(source.integrity_effect),
        ["alice", "slack-U1"] if attested else None,
    ]
    if format_version == "unknown":
        record.insert(0, "mimir.ingest-source.v999")
        record[-1] = ["alice", "slack-U1", "slack-SRC"] if attested else None
    stored_identity = hashlib.sha256(json.dumps(record, ensure_ascii=True).encode()).hexdigest()
    ack = IngestAcknowledgement(
        _acknowledged_ingest=frozenset({stored_identity}),
        arrival_ordinal=1,
        acknowledged_ordinal=1,
    )
    # A restored carrier must re-taint even before a merge advances its ordinal.
    labels = InformationFlowLabels().with_source(source)
    restored = InformationFlowState(labels=labels, ingest_acknowledgement=ack)
    assert restored.has_untrusted_active_ingest()
    state = InformationFlowState(ingest_acknowledgement=ack)
    state.merge(labels)
    assert state.ingest_snapshot()[1] == 2
    assert state.has_untrusted_active_ingest()
    assert state.acknowledge_ingest_snapshot(
        expected=labels, expected_ordinal=2,
        durable_audit=lambda sources: sources == (source,),
    )
    assert not state.has_untrusted_active_ingest()


@pytest.mark.parametrize("enforce", [False, True])
def test_channel_mismatched_attestation_still_denied_at_egress(owner_source, enforce):
    class SingletonAudience:
        def audience_for(self, channel_id, *, principal):
            assert (channel_id, principal) == ("slack-DEST", "alice")
            return frozenset({"alice"})

    state = _acknowledged_state(owner_source)
    auth = AuthContext(
        principal="slack-U1", canonical_principal="alice", roles=("user",),
        event_ingress=None, trigger="user_message", channel_id="slack-DEST",
        interactivity=TurnInteractivity.INTERACTIVE, enforcement_enabled=enforce,
        domain="channel", resource_id="slack-DEST", bridge_instance="slack",
        audience_provider=SingletonAudience(), ifc_state=state,
    )
    assert SinkGate.check_sink_flow(
        "harness_auto_deliver", "slack-DEST", state.current(), auth, enforce=enforce,
    ).allowed
    variant = replace(owner_source, owner_attestation=_mint_owner_attestation(
        "alice", "slack-U1", "slack-OTHER",
    ))
    merged = state.current().with_source(variant)
    state = InformationFlowState(labels=merged)
    auth = replace(auth, ifc_state=state)
    # Even reviewing all ingest cannot confer an egress owner binding.
    assert state.acknowledge_ingest_snapshot(
        expected=merged, durable_audit=lambda _: True,
    )
    assert not state.has_untrusted_active_ingest()
    assert not _source_is_triggering_channel_compatible(
        variant, effective_principal="alice", triggering_principal=auth.principal,
        resolved_triggering=auth.channel_id, audience_provider=auth.audience_provider,
        cross_platform_pull=auth.cross_platform_pull,
        triggering_bridge_instance=auth.bridge_instance,
    )
    decision = SinkGate.check_sink_flow(
        "harness_auto_deliver", "slack-DEST", merged, auth, enforce=enforce,
    )
    assert decision.would_block
    # Preserve the existing shadow-mode policy, not an out-of-scope egress change.
    assert decision.allowed is (not enforce)
    assert decision.is_shadow_decision is (not enforce)
