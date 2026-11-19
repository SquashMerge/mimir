"""IFC egress and review-history properties against independent finite-set/history oracles."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any

import pytest
from hypothesis import event, example, given, settings
from hypothesis import strategies as st
from pydantic import TypeAdapter

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

CAMPAIGN = settings(max_examples=300, deadline=None)
PEOPLE = ("alice", "bob", "céline")
SENSITIVITIES = ("private", "confidential", "internal", "public")
KINDS = ("channel", "auto_recall", "mcp", "protected_prompt",
         "recent_activity_assistant", "service", "protected_tool", "future_kind")
ACL = st.frozensets(st.sampled_from(PEOPLE))


@dataclass(frozen=True)
class Input:
    acl: frozenset[str]
    trusted: bool
    active: bool
    sensitivity: str
    missing: str | None = None
    kind: str = "protected_tool"
    channel: str = "slack-SOURCE"
    bridge: str = "slack"

    @property
    def complete(self) -> bool:
        return self.missing is None and bool(self.acl) and self.sensitivity in SENSITIVITIES

    @property
    def tainted(self) -> bool:
        return not self.trusted and self.active

    def source(self, identity: int = 0) -> SourceLabel:
        values = dict(
            principal="alice", domain="channel", resource_id=self.channel,
            bridge_instance=self.bridge, sensitivity=self.sensitivity,
            authorized_principals=self.acl, source_kind=self.kind,
            integrity="trusted" if self.trusted else "untrusted",
            integrity_effect="active_ingest" if self.active else "informational",
            domain_qualifier=f"item-{identity}",
        )
        if self.missing is not None:
            values[self.missing] = None
        return SourceLabel.from_record(values)


INPUTS = st.builds(
    Input, acl=ACL, trusted=st.booleans(), active=st.booleans(),
    sensitivity=st.sampled_from((*SENSITIVITIES, "future_sensitivity")),
    missing=st.sampled_from((None, None, "principal", "domain", "resource_id",
                            "bridge_instance")),
    kind=st.sampled_from(KINDS),
)


def assert_derivation(inputs: list[Input]) -> None:
    sources = tuple(value.source(i) for i, value in enumerate(inputs))
    derived = SourceLabel.derived(
        sources, principal="derived", domain="service", resource_id="result",
        bridge_instance="local", sensitivity="private",
    )
    # Enumerate candidates, rather than using the production intersection loop.
    expected_acl = frozenset(
        person for person in PEOPLE
        if inputs and all(value.complete and person in value.acl for value in inputs)
    )
    assert derived.authorized_principals == expected_acl
    assert derived.integrity == (
        "trusted" if inputs and all(value.trusted for value in inputs) else "untrusted"
    )
    tainted = any(value.tainted for value in inputs)
    assert derived.integrity_effect == ("active_ingest" if tainted else "informational")
    assert derived.has_untrusted_active_ingest is tainted


@CAMPAIGN
@given(st.lists(INPUTS, max_size=8))
def test_derived_acl_and_operative_integrity(inputs: list[Input]) -> None:
    assert_derivation(inputs)
    event(f"derived inputs={'empty' if not inputs else 'nonempty'}")
    event(f"derived tainted={any(value.tainted for value in inputs)}")


@CAMPAIGN
@given(st.lists(INPUTS, min_size=1, max_size=8), st.booleans())
def test_persisted_and_model_facing_serialization(
    inputs: list[Input], legacy: bool,
) -> None:
    sources = tuple(value.source(i) for i, value in enumerate(inputs))
    carrier = InformationFlowLabels(sources=(*sources, *sources))
    assert carrier.sources == sources
    assert carrier.labels == frozenset(value.sensitivity for value in inputs)
    # Exercise both the declared and duck-typed nested carrier paths, not just
    # serialization of individual source records. This is the tuple/dedup
    # representation contract that previously failed on sets of model dicts.
    for adapter, value in (
        (TypeAdapter(InformationFlowLabels), carrier),
        (TypeAdapter(dict[str, Any]), {"carrier": carrier}),
    ):
        dumped = adapter.dump_python(value)
        record = dumped if "sources" in dumped else dumped["carrier"]
        assert len(record["sources"]) == len(sources)
        adapter.dump_json(value)
    for source in sources:
        persisted = vars(source)
        if legacy and source.domain is not None:
            persisted["domain"] = f"{source.domain}:{source.domain_qualifier}"
            persisted.pop("domain_qualifier")
        restored = SourceLabel.from_record(json.loads(json.dumps(
            persisted, default=lambda value: sorted(value),
        )))
        assert restored == source
        assert hash(restored) == hash(source)
        for adapter, value in (
            (TypeAdapter(SourceLabel), source),
            (TypeAdapter(dict[str, Any]), {"source": source}),
        ):
            dumped = adapter.dump_python(value)
            record = dumped if "principal" in dumped else dumped["source"]
            assert record.get("principal") is None
            assert record.get("resource_id") is None
            assert not record.get("authorized_principals")
            assert record.get("owner_attestation") is None
            adapter.dump_json(value)  # Tuple/set representation must not crash.
        event(f"persisted kind={'unknown' if source.source_kind == 'future_kind' else 'known'}")


def auth_for(carrier: InformationFlowLabels, provider: Any = None) -> AuthContext:
    return AuthContext(
        principal="slack-U1", canonical_principal="alice", roles=("user",),
        event_ingress=None, trigger="user_message", channel_id="slack-DEST",
        interactivity=TurnInteractivity.INTERACTIVE,
        domain="channel", resource_id="slack-DEST", bridge_instance="slack",
        ifc_labels=carrier, ifc_state=InformationFlowState(labels=carrier),
        audience_provider=provider,
    )


@dataclass
class Audience:
    destination: frozenset[str]
    source: frozenset[str]
    fails: bool

    def audience_for(self, channel: str, *, principal: str) -> frozenset[str]:
        if self.fails:
            raise RuntimeError("test-owned unavailable audience")
        return self.destination if channel == "slack-DEST" else self.source


EGRESS_INPUTS = st.builds(
    Input, acl=ACL, trusted=st.booleans(), active=st.booleans(),
    sensitivity=st.sampled_from((*SENSITIVITIES, "future_sensitivity")),
    missing=st.sampled_from((None, None, "principal", "resource_id", "bridge_instance")),
    kind=st.sampled_from(KINDS),
    channel=st.sampled_from(("slack-DEST", "slack-SOURCE")),
    bridge=st.sampled_from(("slack", "other-workspace")),
)


def compatible(value: Input, audience: Audience) -> bool:
    """Finite policy slice: no services, attestations, admins or approvals."""
    if not value.complete or "alice" not in value.acl or value.kind == "future_kind":
        return False
    if value.kind in ("auto_recall", "mcp"):
        return (not audience.fails and bool(audience.destination)
                and all(person in value.acl for person in audience.destination))
    if value.channel == "slack-DEST" and value.bridge == "slack":
        return True
    if value.kind in ("protected_prompt", "recent_activity_assistant"):
        # The provider is keyed by channel ID, not bridge. When source and
        # destination IDs collide, both queries return the destination audience.
        source_audience = (
            audience.destination if value.channel == "slack-DEST" else audience.source
        )
        return (not audience.fails and bool(audience.destination) and bool(source_audience)
                and all(person in source_audience for person in audience.destination))
    return value.kind == "protected_tool"


@CAMPAIGN
@given(st.lists(EGRESS_INPUTS, min_size=1, max_size=5), ACL, ACL, st.booleans())
def test_same_channel_egress_requires_every_source(
    inputs: list[Input], destination: frozenset[str],
    source: frozenset[str], fails: bool,
) -> None:
    audience = Audience(destination, source, fails)
    carrier = InformationFlowLabels(sources=tuple(
        value.source(i) for i, value in enumerate(inputs)
    ))
    auth = auth_for(carrier, audience)
    expected = all(compatible(value, audience) for value in inputs)
    with pytest.MonkeyPatch.context() as patch:
        # Operator-configured delivery is deliberately outside this policy slice.
        patch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
        patch.setattr(ChannelResourceAdapter, "_global_resolver", None)
        for enforce in (False, True):
            result = SinkGate.check_sink_flow(
                "send_message", "slack-DEST", carrier, auth, enforce=enforce,
            )
            assert result.allowed is (expected or not enforce)
            assert result.would_block is (not expected)
    event(f"egress={'allowed' if expected else 'blocked'}")
    event(f"audience={'failure' if fails else 'known'}")


@CAMPAIGN
@given(INPUTS, st.booleans(), st.booleans())
def test_missing_unknown_and_always_on_tainted_writes(
    value: Input, missing: bool, enforce: bool,
) -> None:
    carrier = InformationFlowLabels(sources=(value.source(),))
    auth = auth_for(carrier)
    # A tainted live state overrides missing snapshots for writes in both modes.
    with TemporaryDirectory(prefix="mimir-ifc-write-") as directory:
        home = Path(directory)
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("MIMIR_HOME", str(home))
            write = SinkGate.check_sink_flow(
                "write_file", str(home / "live-result"), None if missing else carrier,
                auth, enforce=enforce,
            )
    if value.tainted:
        assert not write.allowed
        assert write.reason == "write_blocked_by_untrusted_ingest"
        assert write.enforcement_enabled
    for tool, labels in (("send_message", None), ("unknown_ifc_test_tool", carrier)):
        result = SinkGate.check_sink_flow(tool, "slack-DEST", labels, auth, enforce=enforce)
        assert result.allowed is (not enforce)
        assert result.would_block


# A small source domain deliberately produces repeated historical merges.
HISTORY_INPUTS = (
    Input(frozenset({"alice"}), False, True, "private"),
    Input(frozenset({"alice", "bob"}), True, True, "confidential"),
    Input(frozenset({"alice"}), False, False, "internal"),
    Input(frozenset(), False, True, "public"),
)
OP = st.tuples(
    st.sampled_from(("merge", "merge", "snapshot", "ack", "expire", "audit_error")),
    st.integers(0, 3), st.booleans(), st.booleans(),
)


def replay_history(operations: list[tuple[str, int, bool, bool]], cap: int) -> None:
    # Replace this module's clock binding, not the process-wide time module.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(models, "time", SimpleNamespace(monotonic=lambda: 10_000.0))
        _replay_history(operations, cap)


def _replay_history(operations: list[tuple[str, int, bool, bool]], cap: int) -> None:
    state = InformationFlowState(
        labels=InformationFlowLabels(),
        ingest_acknowledgement=IngestAcknowledgement(last_used=10_000.0),
    )
    # Patch only this cell's bound, never the shared production class.
    state.ingest_acknowledgement.MAX_SOURCES = cap
    auth = auth_for(state.labels)
    auth = replace(auth, ifc_state=state)
    fork = replace(auth)
    seen: list[int] = []
    acknowledged: set[int] = set()
    arrival = acknowledged_ordinal = source_ordinal = 0
    labels: set[str] = set()
    channels: set[str] = set()
    snapshot = (state.labels, (), 0)
    for operation, identity, fresh, audit_ok in operations:
        value = HISTORY_INPUTS[identity]
        if operation == "merge":
            added = InformationFlowLabels(
                labels=frozenset({f"label-{identity}"}),
                source_channels=frozenset({f"channel-{identity}"}),
                sources=(value.source(identity),),
            )
            before = state.current()
            new = identity not in seen
            merged, receipt = state.merge_with_receipt(
                added, event_identity=identity if audit_ok else None,
                fresh_untrusted_ingest=fresh,
            )
            arrived = new or audit_ok or fresh
            if arrived:
                source_ordinal += 1
                if fresh or (new and value.tainted and identity not in acknowledged):
                    arrival += 1
            if new:
                seen.append(identity)
            labels.update((f"label-{identity}", value.sensitivity))
            channels.add(f"channel-{identity}")
            assert receipt.pre_carrier == before
            assert receipt.post_carrier == merged
            assert receipt.source_changed is new
            assert receipt.source_arrived is arrived
        elif operation == "snapshot":
            actual, ordinal = state.ingest_snapshot()
            snapshot = (actual, tuple(seen), arrival)
            assert ordinal == arrival
        elif operation == "expire":
            # No sleeps or ambient clock assertions: force this owned cell idle.
            state.ingest_acknowledgement.last_used -= IngestAcknowledgement.IDLE_SECONDS
            state.ingest_snapshot()
            acknowledged.clear()
            arrival += 1
        else:
            carrier, reviewed_ids, ordinal = snapshot
            reviewed = {i for i in reviewed_ids if HISTORY_INPUTS[i].tainted}
            uncovered = {i for i in seen if HISTORY_INPUTS[i].tainted} - reviewed - acknowledged
            eligible = (
                not uncovered and ordinal == arrival and bool(reviewed)
                and (bool(reviewed - acknowledged) or arrival > acknowledged_ordinal)
                and len(acknowledged | reviewed) <= cap
            )
            calls = []
            newly_reviewed = reviewed - acknowledged

            def audit(sources: tuple[SourceLabel, ...]) -> bool:
                calls.append(sources)
                if operation == "audit_error":
                    raise RuntimeError("test-owned durable audit failure")
                return audit_ok

            if operation == "audit_error" and eligible:
                with pytest.raises(RuntimeError, match="test-owned durable audit"):
                    state.acknowledge_ingest_snapshot(
                        expected=carrier, expected_ordinal=ordinal, durable_audit=audit,
                    )
            else:
                accepted = state.acknowledge_ingest_snapshot(
                    expected=carrier, expected_ordinal=ordinal, durable_audit=audit,
                )
                assert accepted is (eligible and audit_ok)
                if accepted:
                    acknowledged |= reviewed
                    acknowledged_ordinal = arrival
            assert bool(calls) is eligible
            if calls:
                assert set(calls[0]) == {
                    HISTORY_INPUTS[i].source(i) for i in newly_reviewed
                }
            event(f"ack eligible={eligible} audit={operation}:{audit_ok}")
        current = fork.ifc_state.current(fork.ifc_labels)
        assert current.labels == frozenset(labels)
        assert current.source_channels == frozenset(channels)
        assert current.sources == tuple(HISTORY_INPUTS[i].source(i) for i in seen)
        assert state.source_arrival_ordinal() == source_ordinal
        expected_taint = any(HISTORY_INPUTS[i].tainted for i in seen) and (
            arrival > acknowledged_ordinal
            or any(HISTORY_INPUTS[i].tainted and i not in acknowledged for i in seen)
        )
        assert state.has_untrusted_active_ingest() is expected_taint
        assert state.permission_has_untrusted_active_ingest() is expected_taint
    event(f"history distinct sources={len(seen)}")


@CAMPAIGN
@example(operations=[
    ("merge", 0, False, False), ("snapshot", 0, False, False),
    ("merge", 1, False, False), ("ack", 0, False, True),
    ("merge", 0, True, False), ("ack", 0, False, True),
    ("snapshot", 0, False, False), ("ack", 0, False, True),
    ("expire", 0, False, False), ("ack", 0, False, True),
], cap=4)
@example(operations=[
    ("merge", 0, False, False), ("snapshot", 0, False, False),
    ("audit_error", 0, False, True), ("ack", 0, False, False),
    ("ack", 0, False, True),
], cap=1)
@example(operations=[
    ("merge", 0, False, False), ("merge", 3, False, False),
    ("snapshot", 0, False, False), ("ack", 0, False, True),
], cap=1)
@given(st.lists(OP, min_size=1, max_size=35), st.integers(0, 4))
def test_merge_arrival_acknowledgement_histories(operations, cap) -> None:
    replay_history(operations, cap)


def test_derivation_oracle_kills_acl_union_mutant() -> None:
    original = SourceLabel.derived.__func__

    def union_mutant(cls, inputs, **kwargs):
        result = original(cls, inputs, **kwargs)
        object.__setattr__(result, "_authorized_principals",
                           frozenset(person for source in inputs
                                     for person in source.authorized_principals))
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(SourceLabel, "derived", classmethod(union_mutant))
        with pytest.raises(AssertionError):
            assert_derivation([
                Input(frozenset({"alice"}), True, True, "private"),
                Input(frozenset({"bob"}), True, True, "private"),
            ])
