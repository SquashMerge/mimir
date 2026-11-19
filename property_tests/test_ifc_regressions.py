"""Deterministic IFC contract witnesses.

Keep any subsequently confirmed unresolved failures strict-xfail with a narrow
``raises=`` constraint, after first demonstrating their unmarked failure.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from mimir.models import InformationFlowLabels, InformationFlowState, SourceLabel


def source(*, principal: str = "alice", trusted: bool = False) -> SourceLabel:
    return SourceLabel(
        principal=principal, domain="web", resource_id=principal,
        bridge_instance="web", sensitivity="private",
        authorized_principals=frozenset({principal}),
        source_kind="protected_tool",
        integrity="trusted" if trusted else "untrusted",
        integrity_effect="active_ingest",
    )


def test_disjoint_derived_acls_are_unknown_not_public() -> None:
    derived = SourceLabel.derived(
        (source(), source(principal="bob")),
        principal="service", domain="service", resource_id="derived",
        bridge_instance="local", sensitivity="private",
    )
    assert derived.authorized_principals == frozenset()
    assert not derived.is_complete
    assert derived.has_untrusted_active_ingest


def test_trusted_active_input_does_not_activate_untrusted_informational_input() -> None:
    derived = SourceLabel.derived(
        (source(trusted=True), replace(source(), integrity_effect="informational")),
        principal="service", domain="service", resource_id="derived",
        bridge_instance="local", sensitivity="private",
    )
    assert derived.integrity == "untrusted"
    assert derived.integrity_effect == "informational"
    assert not derived.has_untrusted_active_ingest


def test_failed_audit_is_atomic_and_fresh_duplicate_invalidates_snapshot() -> None:
    labels = InformationFlowLabels(sources=(source(),))
    state = InformationFlowState(labels=labels)
    snapshot, ordinal = state.ingest_snapshot()

    def failed_audit(_sources: tuple[SourceLabel, ...]) -> bool:
        raise RuntimeError("test-owned audit failure")

    with pytest.raises(RuntimeError, match="test-owned audit failure"):
        state.acknowledge_ingest_snapshot(
            expected=snapshot, expected_ordinal=ordinal, durable_audit=failed_audit,
        )
    assert state.has_untrusted_active_ingest()
    assert not state.ingest_acknowledgement._acknowledged_ingest
    assert state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=lambda _: True,
    )
    assert not state.has_untrusted_active_ingest()
    state.merge_with_receipt(labels, fresh_untrusted_ingest=True)
    assert state.current() == labels
    assert state.has_untrusted_active_ingest()
    assert not state.acknowledge_ingest_snapshot(
        expected=snapshot, expected_ordinal=ordinal, durable_audit=lambda _: True,
    )


def test_future_persisted_kind_is_preserved_but_not_producer_accepted() -> None:
    record = {**vars(source()), "source_kind": "future_ifc_kind"}
    decoded = SourceLabel.from_record(record)
    assert decoded.source_kind == "future_ifc_kind"
    assert SourceLabel.from_record(vars(decoded)) == decoded
    with pytest.raises(ValueError, match="invalid source kind"):
        SourceLabel(**record)
