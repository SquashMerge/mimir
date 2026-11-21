"""Constructive IFC properties: build a valid egress group, then corrupt one constraint."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import Any

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st
from pydantic import TypeAdapter

from mimir import access_control, models
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

CAMPAIGN = settings(max_examples=250, deadline=None)
PEOPLE = ("alice", "bob", "céline")
DEST = "slack-DEST"
RAW = "slack-U1"
SENSITIVITIES = ("private", "confidential", "internal", "public")


@dataclass(frozen=True)
class Spec:
    principal: str | None = "alice"
    domain: str | None = "channel"
    resource: str | None = DEST
    bridge: str | None = "slack"
    sensitivity: str = "private"
    acl: frozenset[str] = frozenset({"alice"})
    kind: str = "channel"
    trusted: bool = False
    active: bool = True
    qualifier: str | None = None
    owner: tuple[str, str, str] | None = None

    @property
    def tainted(self) -> bool:
        return not self.trusted and self.active

    def source(self) -> SourceLabel:
        # Construct a fresh attestation as well as a fresh SourceLabel on every
        # call; equality tests must not rely on object identity shortcuts.
        return SourceLabel.from_record(dict(
            principal=self.principal, domain=self.domain,
            resource_id=self.resource, bridge_instance=self.bridge,
            sensitivity=self.sensitivity, authorized_principals=self.acl,
            source_kind=self.kind,
            integrity="trusted" if self.trusted else "untrusted",
            integrity_effect="active_ingest" if self.active else "informational",
            domain_qualifier=self.qualifier,
            owner_attestation=_mint_owner_attestation(*self.owner) if self.owner else None,
        ))


def signature(source: SourceLabel) -> Spec:
    owner = source.owner_attestation
    return Spec(
        principal=source.principal, domain=source.domain,
        resource=source.resource_id, bridge=source.bridge_instance,
        sensitivity=source.sensitivity, acl=source.authorized_principals,
        kind=source.source_kind, trusted=source.integrity == "trusted",
        active=source.integrity_effect == "active_ingest",
        qualifier=source.domain_qualifier,
        owner=(owner.canonical_principal, owner.raw_author, owner.source_channel)
        if owner else None,
    )


def carrier(specs: list[Spec] | tuple[Spec, ...]) -> InformationFlowLabels:
    return InformationFlowLabels(sources=tuple(value.source() for value in specs))


@dataclass
class Audience:
    members: dict[str, frozenset[str]]
    identities: dict[str, tuple[str, bool]]
    failing: frozenset[str] = frozenset()

    @property
    def identity_resolver(self) -> Audience:
        return self

    def audience_for(self, channel: str, *, principal: str) -> frozenset[str]:
        if channel in self.failing:
            raise RuntimeError("owned audience unavailable")
        return self.members.get(channel, frozenset())

    def identity(self, value: str) -> Any:
        if value not in self.identities:
            return None
        canonical, service = self.identities[value]
        return SimpleNamespace(canonical=canonical, access=SimpleNamespace(is_service=service))


def auth_for(labels: InformationFlowLabels, audience: Audience) -> AuthContext:
    return AuthContext(
        principal=RAW, canonical_principal="alice", roles=("user",),
        event_ingress=None, trigger="user_message", channel_id=DEST,
        interactivity=TurnInteractivity.INTERACTIVE, domain="channel",
        resource_id=DEST, bridge_instance="slack", ifc_labels=labels,
        ifc_state=InformationFlowState(labels=labels), audience_provider=audience,
        cross_platform_pull=False,
    )


def compatible(value: Spec, audience: Audience) -> bool:
    """Independent declarative oracle for ordinary triggering-channel replies.

    No production policy, completeness, equality, hashing or resolution helpers.
    All channels here are already canonical and the owned resolver is disabled.
    """
    if not all((value.principal, value.domain, value.resource, value.bridge, value.acl)):
        return False
    if value.sensitivity not in SENSITIVITIES or "alice" not in value.acl:
        return False
    known = {
        "channel", "auto_recall", "mcp", "recent_activity_user",
        "owner_attested_feedback", "protected_prompt", "recent_activity_assistant",
        "channel_scoped_feedback", "channel_bound_unowned_feedback",
        "feedback_chain", "agent_self", "service", "protected_tool", "acp_hands_result",
    }
    if value.kind not in known:
        return False
    destination = audience.members.get(DEST, frozenset())
    destination_known = DEST not in audience.failing and bool(destination)
    if value.kind == "agent_self" and value.trusted and not value.active:
        return True
    if value.kind in {"auto_recall", "mcp"}:
        return destination_known and all(person in value.acl for person in destination)
    if value.resource == DEST and (value.domain != "channel" or value.bridge == "slack"):
        return True
    if value.kind in {"recent_activity_user", "owner_attested_feedback"}:
        if value.owner != ("alice", RAW, value.resource) or value.principal != "alice":
            return False
        if not destination_known:
            return False
        if value.kind == "recent_activity_user":
            return destination == frozenset({"alice"})
        humans = set()
        for person in destination:
            if person.startswith("service:"):
                continue
            resolved = audience.identities.get(person)
            if resolved is None:
                return False
            canonical, service = resolved
            if not service and not canonical.startswith("service:"):
                humans.add(canonical)
        return humans == {"alice"}
    if value.kind in {"protected_prompt", "recent_activity_assistant"}:
        readers = audience.members.get(value.resource, frozenset())
        return (
            destination_known and value.resource not in audience.failing
            and bool(readers) and all(person in readers for person in destination)
        )
    if value.kind == "service":
        return not value.domain.startswith("channel")
    return value.kind in {"protected_tool", "acp_hands_result"}


def check_group(
    specs: list[Spec], audience: Audience, *, expected: bool, record_events: bool = False,
) -> None:
    # Assert the generator/oracle precondition before observing the gate.
    assert all(compatible(value, audience) for value in specs) is expected, "oracle precondition mismatch"
    labels = carrier(specs)
    auth = auth_for(labels, audience)
    for enforce in (False, True):
        result = SinkGate.check_sink_flow("send_message", DEST, labels, auth, enforce=enforce)
        if result.allowed is not (expected or not enforce):
            raise AssertionError(
                f"sink allowed mismatch: enforce={enforce} expected={expected or not enforce} actual={result.allowed}"
            )
        if result.would_block is not (not expected):
            raise AssertionError(
                f"sink would_block mismatch: enforce={enforce} expected={not expected} actual={result.would_block}"
            )
        if not expected:
            assert result.reason == "ifc_label_blocked:same_channel"
    if record_events:
        event(f"mandatory group {'allow' if expected else 'reject'} in both modes")


@st.composite
def groups(draw):
    family = draw(st.sampled_from((
        "same", "acl", "audience", "owner", "feedback", "tool", "service", "self",
    )))
    destination = {"alice"}
    if family not in {"owner", "feedback"}:
        destination.update(draw(st.sets(st.sampled_from(PEOPLE[1:]))))
    if family == "acl":
        destination.add("bob")
    if family == "feedback":
        destination = {"alias-alice", "service:local"}
    audience = Audience(
        {DEST: frozenset(destination)},
        {"alice": ("alice", False), "alias-alice": ("alice", False), "bob": ("bob", False)},
    )
    size = draw(st.integers(2, 6))
    index = draw(st.integers(0, size - 1))
    mixed = draw(st.booleans())
    compatible_families = ["same", "acl", "audience", "tool", "service", "self"]
    if destination == {"alice"}:
        compatible_families.append("owner")
    if destination in ({"alice"}, {"alias-alice", "service:local"}):
        compatible_families.append("feedback")
    specs = []
    for i in range(size):
        source_family = (
            draw(st.sampled_from(compatible_families))
            if mixed and i != index else family
        )
        base = Spec(
            sensitivity=draw(st.sampled_from(SENSITIVITIES)),
            trusted=draw(st.booleans()), active=draw(st.booleans()),
            qualifier=f"source-{i}", acl=frozenset({"alice", *destination}),
        )
        if source_family == "same":
            base = replace(base, kind=draw(st.sampled_from((
                "channel", "feedback_chain", "channel_scoped_feedback",
                "channel_bound_unowned_feedback",
            ))))
        elif source_family == "acl":
            base = replace(base, resource=f"slack-SRC-{i}", kind=draw(st.sampled_from(
                ("auto_recall", "mcp"))))
        elif source_family == "audience":
            resource = f"slack-SRC-{i}"
            audience.members[resource] = frozenset({*destination, "reader-extra"})
            base = replace(base, resource=resource, kind=draw(st.sampled_from(
                ("protected_prompt", "recent_activity_assistant"))))
        elif source_family in {"owner", "feedback"}:
            resource = f"slack-SRC-{i}"
            base = replace(
                base, resource=resource,
                kind="recent_activity_user" if source_family == "owner" else "owner_attested_feedback",
                owner=("alice", RAW, resource),
            )
        elif source_family == "tool":
            base = replace(base, resource=f"slack-SRC-{i}", bridge="other-workspace",
                           kind="protected_tool")
        elif source_family == "service":
            base = replace(base, resource=f"service-result-{i}", domain="service", kind="service")
        else:
            base = replace(base, resource=f"self-{i}", trusted=True, active=False, kind="agent_self")
        specs.append(base)
    return family, specs, audience, index, mixed


@CAMPAIGN
@given(groups(), st.sampled_from(("principal", "domain", "resource", "bridge",
                                 "sensitivity", "acl", "kind")))
def test_constructive_complete_groups_and_single_corruption(case, axis: str) -> None:
    family, specs, audience, index, mixed = case
    with pytest.MonkeyPatch.context() as patch:
        patch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
        patch.setattr(ChannelResourceAdapter, "_global_resolver", None)
        check_group(specs, audience, expected=True, record_events=True)
        changes = {
            "principal": None, "domain": None, "resource": None, "bridge": None,
            "sensitivity": "future-sensitivity", "acl": frozenset({"outsider"}),
            "kind": "future-kind",
        }
        corrupted = list(specs)
        corrupted[index] = replace(specs[index], **{axis: changes[axis]})
        check_group(corrupted, audience, expected=False, record_events=True)
    event(f"family={family}")
    event(f"single common corruption={axis}")
    event(f"corrupt position={'first' if index == 0 else 'later'}")
    event(f"group size={len(specs)}")
    event(f"mixed-family generation={mixed}")


@CAMPAIGN
@given(groups(), st.integers(0, 4))
def test_constructive_deep_constraints_and_protected_tool_exception(case, selector: int) -> None:
    family, specs, audience, index, mixed = case
    value = specs[index]
    with pytest.MonkeyPatch.context() as patch:
        patch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
        patch.setattr(ChannelResourceAdapter, "_global_resolver", None)
        check_group(specs, audience, expected=True, record_events=True)
        bad_audience = Audience(dict(audience.members), dict(audience.identities))
        bad = list(specs)
        if family == "same":
            bad[index] = replace(value, bridge="other-workspace")
            defect = "bridge-authority"
        elif family == "acl":
            # Requester is still authorized; only another destination reader is
            # absent from this one ACL. This must reach the audience subset arm.
            bad[index] = replace(value, acl=value.acl - {"bob"})
            defect = "destination-not-in-source-acl"
        elif family == "audience":
            bad_audience.members[value.resource] = frozenset({"outsider"})
            defect = "destination-not-in-source-channel"
        elif family in {"owner", "feedback"}:
            defects = (
                None, ("bob", RAW, value.resource),
                ("alice", "slack-OTHER", value.resource),
                ("alice", RAW, "slack-WRONG"),
            )
            if selector < 4:
                bad[index] = replace(value, owner=defects[selector])
                defect = f"owner-binding-{selector}"
            else:
                bad_audience.members[DEST] |= {"bob"}
                defect = "not-requester-only"
        elif family == "tool":
            # Explicit allowance: changing bridge/channel/audience is NOT a
            # corruption for protected_tool. Change ONLY kind for the negative.
            bad[index] = replace(value, kind="channel")
            defect = "same-channel-only-kind"
        elif family == "service":
            bad[index] = replace(value, domain="channel_metadata")
            defect = "channel-prefixed-service-domain"
        else:
            bad[index] = replace(value, active=True)
            defect = "agent-self-not-informational"
        check_group(bad, bad_audience, expected=False, record_events=True)
        # Each case also proves the intentional protected_tool exception even
        # when every audience lookup fails and authority differs.
        tool = replace(value, kind="protected_tool", resource="slack-OTHER",
                       bridge="other-workspace", owner=None)
        unavailable = Audience({}, {}, failing=frozenset({DEST, "slack-OTHER"}))
        check_group([tool], unavailable, expected=True, record_events=True)
    event(f"deep constraint={defect}")
    event(f"mixed-family generation={mixed}")
    event("mandatory protected_tool audience/bridge exception")


IDENTITY_AXES = (
    "principal", "domain", "resource", "bridge", "sensitivity", "acl", "kind",
    "qualifier", "trusted", "active", "owner-canonical", "owner-raw", "owner-channel",
)


def one_axis(value: Spec, axis: str) -> Spec:
    changes = {
        "principal": "bob", "domain": "memory", "resource": "slack-NEW",
        "bridge": "other-workspace", "sensitivity": "confidential",
        "acl": frozenset({"alice", "bob"}), "kind": "mcp", "qualifier": "new-qualifier",
        "trusted": True, "active": False,
    }
    if axis in changes:
        return replace(value, **{axis: changes[axis]})
    assert value.owner is not None
    owner = list(value.owner)
    position = {"owner-canonical": 0, "owner-raw": 1, "owner-channel": 2}[axis]
    owner[position] += "-new"
    return replace(value, owner=tuple(owner))


@CAMPAIGN
@given(st.sampled_from(IDENTITY_AXES), st.integers(1, 12), st.integers(0, 1000))
def test_independently_equal_sources_one_axis_dedup_and_populated_redaction(axis, repeats, salt) -> None:
    base = Spec(owner=("alice", RAW, DEST), qualifier=f"base-{salt}")
    other = one_axis(base, axis)
    first, equal, different = base.source(), base.source(), other.source()
    assert first is not equal and first == equal
    assert first.owner_attestation is not equal.owner_attestation
    assert hash(first) == hash(equal)
    assert first != different
    labels = InformationFlowLabels(sources=tuple(
        source for _ in range(repeats) for source in (base.source(), other.source())
    ))
    assert tuple(map(signature, labels.sources)) == (base, other)
    for adapter, value in (
        (TypeAdapter(InformationFlowLabels), labels),
        (TypeAdapter(dict[str, Any]), {"labels": labels}),
    ):
        for representation in (
            adapter.dump_python(value), json.loads(adapter.dump_json(value)),
        ):
            record = representation if "sources" in representation else representation["labels"]
            for source in record["sources"]:
                # Duck-typed serialization omits InitVar authority entirely;
                # typed serialization may emit an explicit redaction sentinel.
                assert source.get("principal") is None
                assert source.get("resource_id") is None
                assert not source.get("authorized_principals")
                assert source.get("owner_attestation") is None
    event(f"identity one-axis={axis}")
    event("populated independently minted attestations redacted")


HISTORY_AXES = (
    "principal", "domain", "resource", "bridge", "sensitivity", "acl", "kind", "qualifier",
    "owner-canonical", "owner-raw",
)
# Bound histories coordinate owner/principal/channel changes. One-axis malformed
# local API cases, including owner-channel alone, are retained in regressions.
OP = st.tuples(
    st.sampled_from(("merge", "snapshot", "ack", "reject", "audit-error", "expire")),
    st.lists(st.sampled_from((0, 2, 3, 4, 5)), min_size=1, max_size=4),
    st.booleans(), st.booleans(),
)


class History:
    """Tiny ordered-set model; spec equality, not production source identities."""

    def __init__(
        self, specs: list[Spec], *, fallback: bool, cap: int = 32, record_events: bool = False,
    ):
        self.patch = pytest.MonkeyPatch()
        self.record_events = record_events
        try:
            self._initialize(specs, fallback=fallback, cap=cap)
        except BaseException:
            self.patch.undo()
            raise

    def _initialize(self, specs: list[Spec], *, fallback: bool, cap: int) -> None:
        self.specs = specs
        self.seen = [specs[0], specs[2], specs[4]]
        self.covered: set[Spec] = set()
        self.arrival = self.ack_ordinal = self.source_ordinal = 0
        self.labels = {value.sensitivity for value in self.seen}
        self.channels: set[str] = set()
        self.snapshot = (carrier(self.seen), tuple(self.seen), 0)
        self.clock = [10000.0]
        self.patch.setattr(models, "time", SimpleNamespace(monotonic=lambda: self.clock[0]))
        initial = carrier(self.seen)
        self.fallback = initial
        self.state = InformationFlowState(
            labels=None if fallback else initial,
            ingest_acknowledgement=IngestAcknowledgement(last_used=self.clock[0]),
        )
        self.state.ingest_acknowledgement.MAX_SOURCES = cap
        # Snapshot/current support fallback; acknowledgement intentionally takes
        # materialized state, so merge before using the acknowledgement API.
        assert self.state.current(initial) == initial
        assert self.state.ingest_snapshot(initial) == (initial, 0)
        self.state.merge(InformationFlowLabels(), fallback=initial)
        self.auth = auth_for(initial, Audience({}, {}))
        self.auth = replace(self.auth, ifc_state=self.state)
        self.fork = replace(self.auth)
        self.cap = cap
        self.audit_calls = 0
        self.accepted = 0

    def close(self) -> None:
        self.patch.undo()

    def record(self, message: str) -> None:
        if self.record_events:
            event(message)

    def merge(self, indices: list[int], fresh: bool, with_event: bool) -> None:
        values = list(dict.fromkeys(self.specs[i] for i in indices))
        new = [value for value in values if value not in self.seen]
        arrived = bool(new) or with_event or fresh
        if arrived:
            self.source_ordinal += 1
            if fresh or any(value.tainted and value not in self.covered for value in new):
                self.arrival += 1
        before = self.state.current()
        added = carrier(values)
        merged, receipt = self.state.merge_with_receipt(
            added, fallback=self.fallback,
            event_identity=object() if with_event else None,
            fresh_untrusted_ingest=fresh,
        )
        self.seen.extend(new)
        self.labels.update(value.sensitivity for value in values)
        # Channel-kind source additions use with_source's channel bookkeeping.
        self.channels.update(value.resource for value in new
                             if value.kind == "channel" and value.resource)
        assert receipt.pre_carrier == before
        assert receipt.post_carrier == merged
        assert receipt.source_changed is bool(new)
        assert receipt.source_arrived is arrived
        self.record(f"batch size={len(values)}")

    def take_snapshot(self) -> None:
        labels, ordinal = self.state.ingest_snapshot(self.fallback)
        assert ordinal == self.arrival
        self.snapshot = (labels, tuple(self.seen), self.arrival)

    def merge_channel_only(self, channel: str) -> None:
        before = self.state.current()
        merged, receipt = self.state.merge_with_receipt(
            InformationFlowLabels(source_channels=frozenset({channel})),
        )
        self.channels.add(channel)
        assert receipt.pre_carrier == before
        assert receipt.post_carrier == merged
        assert not receipt.source_changed and not receipt.source_arrived
        self.observe()
        self.record("mandatory channel-only carrier merge")

    def ack(self, outcome: str = "ack") -> bool:
        labels, seen, ordinal = self.snapshot
        reviewed = {value for value in seen if value.tainted}
        uncovered = {value for value in self.seen if value.tainted} - reviewed - self.covered
        new = reviewed - self.covered
        eligible = (
            not uncovered and ordinal == self.arrival and bool(reviewed)
            and (bool(new) or self.arrival > self.ack_ordinal)
            and len(self.covered | reviewed) <= self.cap
        )
        calls = []

        def audit(sources):
            calls.append({signature(source) for source in sources})
            if outcome == "audit-error":
                raise RuntimeError("owned durable audit failure")
            return outcome == "ack"

        if eligible and outcome == "audit-error":
            with pytest.raises(RuntimeError, match="^owned durable audit failure$"):
                self.state.acknowledge_ingest_snapshot(
                    expected=labels, expected_ordinal=ordinal, durable_audit=audit,
                )
            accepted = False
        else:
            accepted = self.state.acknowledge_ingest_snapshot(
                expected=labels, expected_ordinal=ordinal, durable_audit=audit,
            )
            if accepted is not (eligible and outcome == "ack"):
                raise AssertionError(
                    f"ack acceptance mismatch: outcome={outcome} expected={eligible and outcome == 'ack'} actual={accepted}"
                )
        assert len(calls) == int(eligible)
        if calls:
            assert calls == [new]
            self.audit_calls += 1
        if accepted:
            self.covered |= reviewed
            self.ack_ordinal = self.arrival
            self.accepted += 1
        self.record(f"ack eligible={bool(eligible)} outcome={outcome} accepted={accepted}")
        return accepted

    def observe(self) -> None:
        live = self.fork.ifc_state.current(self.fork.ifc_labels)
        assert tuple(map(signature, live.sources)) == tuple(self.seen)
        assert live.labels == frozenset(self.labels)
        assert live.source_channels == frozenset(self.channels)
        assert self.state.source_arrival_ordinal() == self.source_ordinal
        expected = any(value.tainted for value in self.seen) and (
            self.arrival > self.ack_ordinal
            or any(value.tainted and value not in self.covered for value in self.seen)
        )
        actual_taint = self.state.has_untrusted_active_ingest(self.fallback)
        if actual_taint is not expected:
            raise AssertionError(
                f"effective taint mismatch: expected={expected} actual={actual_taint}"
            )
        assert self.state.permission_has_untrusted_active_ingest(self.fallback) is expected
        assert self.state.ingest_snapshot()[1] == self.arrival

    def replay(self, operations) -> None:
        for name, indices, fresh, with_event in operations:
            if name == "merge":
                self.merge(indices, fresh, with_event)
            elif name == "snapshot":
                self.take_snapshot()
            elif name == "expire":
                self.clock[0] += IngestAcknowledgement.IDLE_SECONDS
                self.arrival += 1
                self.covered.clear()
            else:
                self.ack(name)
            self.observe()


def bound_change(value: Spec, axis: str) -> Spec:
    changed = one_axis(value, axis)
    if value.owner is None:
        return changed
    if axis == "resource":
        return replace(changed, owner=(value.owner[0], value.owner[1], changed.resource))
    if axis in {"principal", "owner-canonical"}:
        canonical = changed.principal if axis == "principal" else changed.owner[0]
        return replace(changed, principal=canonical,
                       owner=(canonical, value.owner[1], value.owner[2]),
                       acl=frozenset({canonical})
                       if value.acl == frozenset({value.principal}) else value.acl)
    return changed


def history_specs(axis: str) -> list[Spec]:
    # Real prompt-owner shape is informational. It participates in provenance,
    # but a separate actual protected-tool active source supplies reviewable taint.
    base = Spec(kind="recent_activity_user", domain="recent_activity", resource="slack-SRC",
                owner=("alice", RAW, "slack-SRC"), active=False)
    active = Spec(kind="protected_tool", domain="web", resource="second-untrusted")
    return [
        base, bound_change(base, axis),
        Spec(kind="protected_tool", domain="web", resource="trusted", trusted=True),
        Spec(kind="protected_tool", domain="web", resource="informational", active=False),
        active,
        replace(active, resource="third-untrusted"),
        replace(active, resource="reserved-new-untrusted"),
        Spec(kind="channel", domain="channel", resource="slack-GENUINE-CHANNEL",
             trusted=True, qualifier="mandatory-channel-arrival"),
    ]


def forced_history(
    prefix, suffix, *, axis: str, fallback: bool, fresh_duplicate: bool,
    record_events: bool = False,
) -> None:
    history = History(history_specs(axis), fallback=fallback, record_events=record_events)
    try:
        history.replay(prefix)
        # Arbitrary prefixes may already have cleared everything. A mandatory
        # fresh read creates a reviewable boundary without relying on chance.
        history.merge([4], True, True)
        history.merge([7], False, True)
        history.merge_channel_only("slack-BOOKKEEPING-ONLY")
        history.take_snapshot()
        # Trusted arrivals must not expire a pending review.
        history.merge([2, 3], False, True)
        history.observe()
        assert not history.ack("reject")
        assert not history.ack("audit-error")
        assert history.ack()
        history.observe()
        assert not history.state.has_untrusted_active_ingest()
        # A history-only duplicate is distinct from a fresh ingestion.
        history.merge([4, 4], False, False)
        history.observe()
        assert not history.state.has_untrusted_active_ingest()
        history.merge([4] if fresh_duplicate else [1, 6], fresh_duplicate, True)
        history.observe()
        assert history.state.has_untrusted_active_ingest()
        assert not history.ack()  # the reviewed snapshot/ordinal is now stale
        history.take_snapshot()
        assert history.ack()
        history.observe()
        history.replay(suffix)
        assert history.accepted >= 2
        history.record("mandatory ack success -> fresh arrival -> stale rejection -> re-ack")
        history.record(f"arrival={'fresh equal source' if fresh_duplicate else 'new multi-source batch'}")
        history.record(f"initial={'fallback materialized' if fallback else 'nonempty cell'}")
        history.record(f"history identity axis={axis}")
        history.record("mandatory genuine channel source arrival")
    finally:
        history.close()


@CAMPAIGN
@given(st.lists(OP, max_size=12), st.lists(OP, max_size=12),
       st.sampled_from(HISTORY_AXES), st.booleans(), st.booleans())
def test_constructive_ack_success_fresh_arrival_and_stale_histories(
    prefix, suffix, axis, fallback, fresh_duplicate,
) -> None:
    forced_history(
        prefix, suffix, axis=axis, fallback=fallback, fresh_duplicate=fresh_duplicate,
        record_events=True,
    )


def test_gate_oracle_kills_skip_constraint_mutant() -> None:
    good = Spec()
    bad = replace(good, bridge="other-workspace")
    with pytest.MonkeyPatch.context() as patch:
        patch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
        patch.setattr(ChannelResourceAdapter, "_global_resolver", None)
        check_group([good, bad], Audience({}, {}), expected=False)
        patch.setattr(access_control, "_source_is_triggering_channel_compatible", lambda *_a, **_k: True)
        with pytest.raises(
            AssertionError, match="^sink would_block mismatch: enforce=False expected=True actual=False$",
        ):
            check_group([good, bad], Audience({}, {}), expected=False)


def test_gate_positive_control_kills_always_deny_mutant() -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.delenv("MIMIR_OPERATOR_ALERT_CHANNEL", raising=False)
        patch.setattr(ChannelResourceAdapter, "_global_resolver", None)
        check_group([Spec(), replace(Spec(), qualifier="two")], Audience({}, {}), expected=True)
        patch.setattr(access_control, "_source_is_triggering_channel_compatible", lambda *_a, **_k: False)
        with pytest.raises(
            AssertionError, match="^sink would_block mismatch: enforce=False expected=False actual=True$",
        ):
            check_group([Spec(), replace(Spec(), qualifier="two")], Audience({}, {}), expected=True)


def test_history_oracle_kills_missing_fresh_invalidation_mutant() -> None:
    original = InformationFlowState.merge_with_receipt

    def mutant(self, added, *args, **kwargs):
        # Reach a successful reviewed boundary before suppressing invalidation.
        if self.ingest_acknowledgement._acknowledged_ingest:
            kwargs["fresh_untrusted_ingest"] = False
        return original(self, added, *args, **kwargs)

    forced_history([], [], axis="resource", fallback=True, fresh_duplicate=True)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(InformationFlowState, "merge_with_receipt", mutant)
        with pytest.raises(AssertionError, match="^effective taint mismatch: expected=True actual=False$"):
            forced_history([], [], axis="resource", fallback=True, fresh_duplicate=True)


def test_history_oracle_kills_constant_ack_identity_mutant() -> None:
    forced_history([], [], axis="resource", fallback=False, fresh_duplicate=False)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(IngestAcknowledgement, "source_identity", staticmethod(lambda _source: "constant"))
        with pytest.raises(AssertionError, match="^effective taint mismatch: expected=True actual=False$"):
            forced_history([], [], axis="resource", fallback=False, fresh_duplicate=False)


def test_history_oracle_kills_ignored_durable_audit_mutant() -> None:
    original = InformationFlowState.acknowledge_ingest_snapshot

    def mutant(self, **kwargs):
        kwargs["durable_audit"] = lambda _: True
        return original(self, **kwargs)

    forced_history([], [], axis="resource", fallback=False, fresh_duplicate=True)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(InformationFlowState, "acknowledge_ingest_snapshot", mutant)
        with pytest.raises(
            AssertionError, match="^ack acceptance mismatch: outcome=reject expected=False actual=True$",
        ):
            forced_history([], [], axis="resource", fallback=False, fresh_duplicate=True)


def test_history_constructor_restores_clock_on_initialization_failure() -> None:
    original_clock = models.time

    def fail_merge(*_args, **_kwargs):
        raise RuntimeError("owned initialization failure")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(InformationFlowState, "merge", fail_merge)
        with pytest.raises(RuntimeError, match="^owned initialization failure$"):
            History(history_specs("resource"), fallback=True)
        assert models.time is original_clock
