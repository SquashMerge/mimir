"""Independent, bounded authorization models; no IFC lattice assumptions."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
import yaml
from hypothesis import event, find, given, settings
from hypothesis import strategies as st

from mimir.access_control import ChannelResourceAdapter, create_auth_context
from mimir.identities import IdentityResolver
from mimir.models import AgentEvent, AuthContext
from mimir.plugins.registry import PolicyRegistry
from mimir.saga import ownership
from mimir.saga.ownership import AuthorizationScope, Ownership

CAMPAIGN = settings(max_examples=250, deadline=None)
OWNERS = ("alice", "bob", "service:reader", "legacy_admin", "service", "system", "")
DOMAINS = (None, "one", "two", "x' OR 1=1 --")
VISIBILITIES = ("public", "private", "service", "legacy_admin", "unknown")
ROW = st.tuples(
    st.sampled_from(OWNERS), st.sampled_from(VISIBILITIES), st.sampled_from(DOMAINS),
)
SCOPE = st.builds(
    AuthorizationScope,
    principal=st.sampled_from((None, *OWNERS)),
    is_admin=st.booleans(),
    is_service=st.booleans(),
    is_platform_service=st.booleans(),
    readable_domains=st.lists(
        st.sampled_from(DOMAINS[1:]), unique=True, max_size=3,
    ).map(tuple),
    service_canonical=st.sampled_from((None, "reader", "other")),
)


def _readable(scope: AuthorizationScope, row: tuple) -> bool:
    """Set-of-grants specification, independent of SQL construction."""
    owner, visibility, domain = row
    if scope.is_admin or scope.is_platform_service:
        return True
    owner_identity = scope.principal
    if scope.is_service and scope.service_canonical:
        owner_identity = "service:" + scope.service_canonical
    grants = {
        "public": visibility == "public",
        "owner": bool(owner_identity)
        and owner_identity not in {"legacy_admin", "service", "system"}
        and owner == owner_identity,
        "domain": scope.is_service and domain in scope.readable_domains,
    }
    return any(grants.values())


def _select(db: sqlite3.Connection, scope: AuthorizationScope, predicate) -> set[int]:
    where, params = predicate(scope, table="r")
    return {
        row[0]
        for row in db.execute(f"SELECT r.id FROM resources r WHERE {where}", params)
    }


MUTATION = st.tuples(
    st.sampled_from(("put", "put", "delete")),
    st.integers(min_value=0, max_value=3),
    ROW,
)


@CAMPAIGN
@given(scope=SCOPE, history=st.lists(MUTATION, min_size=1, max_size=35))
def test_saga_sql_read_grants_follow_row_histories(scope, history) -> None:
    """Overwrite/delete/reinsert the same four IDs and check every observation."""
    model: dict[int, tuple] = {}
    with closing(sqlite3.connect(":memory:")) as db:
        db.execute(
            "CREATE TABLE resources "
            "(id INTEGER PRIMARY KEY, owner_principal TEXT, visibility TEXT, origin_domain TEXT)"
        )
        for op, key, row in history:
            if op == "put":
                event("overwrite" if key in model else "new row")
                model[key] = row
                db.execute("INSERT OR REPLACE INTO resources VALUES (?, ?, ?, ?)", (key, *row))
            else:
                event("successful delete" if key in model else "absent delete")
                model.pop(key, None)
                db.execute("DELETE FROM resources WHERE id = ?", (key,))
            expected = {key for key, row in model.items() if _readable(scope, row)}
            for predicate in (
                ownership.authorization_predicate,
                ownership.authorization_predicate_for_sessions,
                ownership.authorization_predicate_for_triples,
            ):
                assert _select(db, scope, predicate) == expected, (scope, history, model)
        event(f"final rows={len(model)}")
        event("broad reader" if scope.is_admin or scope.is_platform_service else "restricted reader")


def _acl_model(acls: list[Ownership]) -> Ownership:
    """Whole-input sets and a restrictive precedence table, not an incremental fold."""
    if not acls:
        return Ownership()
    owners = {acl.owner_principal for acl in acls}
    domains = {acl.origin_domain for acl in acls}
    channels = {acl.origin_channel for acl in acls}
    if (
        len(owners) != 1 or "legacy_admin" in owners
        or len(domains) != 1 or None in domains
        or len(channels) != 1
        or any(not acl.provenance for acl in acls)
        or any(acl.visibility not in ("public", "private", "service") for acl in acls)
    ):
        return Ownership()
    visibilities = {acl.visibility for acl in acls}
    visibility = next(v for v in ("service", "private", "public") if v in visibilities)
    return Ownership(
        owner_principal=next(iter(owners)),
        origin_channel=next(iter(channels)),
        origin_domain=next(iter(domains)),
        visibility=visibility,
        provenance={key: value for acl in acls for key, value in acl.provenance.items()},
    )


@st.composite
def acl_groups(draw):
    # A common baseline makes compatible derivations common rather than vanishingly rare.
    base = Ownership("alice", "channel-one", "one", "public", {"seed": 1})
    variants = draw(st.lists(st.sampled_from((
        "unchanged", "unchanged", "private", "service", "owner", "domain",
        "channel", "legacy", "missing-provenance", "unknown-visibility",
    )), max_size=12))
    result = []
    for index, variant in enumerate(variants):
        acl = replace(base, provenance={f"source-{index}": index})
        changes = {
            "private": {"visibility": "private"},
            "service": {"visibility": "service"},
            "owner": {"owner_principal": "bob"},
            "domain": {"origin_domain": "two"},
            "channel": {"origin_channel": "channel-two"},
            "legacy": {"owner_principal": "legacy_admin"},
            "missing-provenance": {"provenance": {}},
            "unknown-visibility": {"visibility": "unknown"},
        }
        result.append(replace(acl, **changes.get(variant, {})))
    return result


@CAMPAIGN
@given(acls=acl_groups())
def test_derivation_acl_matches_restrictive_whole_input_model(acls) -> None:
    expected = _acl_model(acls)
    actual = ownership.intersect_acl(acls)
    assert actual == expected
    event("fail closed" if actual == Ownership() else "retained authority")
    # Append observations and vary order; do not demand provenance-value commutativity
    # when two sources supply conflicting keys (not a documented contract).
    assert ownership.intersect_acl(list(reversed(acls))) == expected
    for prefix in range(1, len(acls) + 1):
        assert ownership.intersect_acl(acls[:prefix]) == _acl_model(acls[:prefix])
    for principal in ("alice", "bob", "system", "legacy_admin"):
        scope = AuthorizationScope(principal=principal)
        output_row = (actual.owner_principal, actual.visibility, actual.origin_domain)
        if _readable(scope, output_row):
            assert all(
                _readable(scope, (acl.owner_principal, acl.visibility, acl.origin_domain))
                for acl in acls
            )


@CAMPAIGN
@given(acls=acl_groups(), encoded=st.booleans())
def test_acl_database_rows_preserve_valid_dictionary_provenance(acls, encoded) -> None:
    rows = [
        {
            "owner_principal": acl.owner_principal,
            "origin_channel": acl.origin_channel,
            "origin_domain": acl.origin_domain,
            "visibility": acl.visibility,
            "provenance": json.dumps(acl.provenance) if encoded else acl.provenance,
        }
        for acl in acls
    ]
    assert ownership.intersect_acl_from_rows(rows) == _acl_model(acls)


CHANNELS = (None, "", "one", "alias-one", "two", "alias-two", "unknown")
ALIASES = {"alias-one": "one", "alias-two": "two"}


def _channel_case(trigger, target, admin, enforce, operation) -> None:
    context = AuthContext(
        principal="alice", canonical_principal="alice",
        roles=("admin",) if admin else ("user",), event_ingress=None,
        trigger="user_message", channel_id=trigger, interactivity=None,
    )
    effective_target = target or trigger
    same = bool(trigger) and (
        ALIASES.get(effective_target, effective_target) == ALIASES.get(trigger, trigger)
    )
    denied = not trigger or (not same and not admin)
    actual = ChannelResourceAdapter.authorize_channel_operation(
        operation, target, context, enforce=enforce,
    )
    assert actual.allowed == (not enforce or not denied)
    assert actual.would_block == denied
    assert actual.is_shadow_decision == (denied and not enforce)
    if not trigger:
        assert actual.reason == "missing_triggering_channel"
    elif same:
        assert actual.reason == "same_scope_channel"
    elif not admin:
        assert actual.reason == "cross_channel_scope"


def test_channel_scoping_exhaustive_small_alias_domain() -> None:
    """588 cases: 7 trigger x 7 target x 2 admin x 2 enforcement x 3 operations."""
    # The adapter's global resolver is deliberately owned and restored by this test.
    resolver = SimpleNamespace(resolve_channel=lambda value: ALIASES.get(value, value))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ChannelResourceAdapter, "_global_resolver", resolver)
        for args in product(
            CHANNELS, CHANNELS, (False, True), (False, True),
            ("send_message", "react", "fetch_channel_history"),
        ):
            _channel_case(*args)


ROLE_METADATA = st.sampled_from((
    None, (), ("user",), ("admin",), ("user", "admin"),
    ("admin", "not-a-role"), ("service",),
))
ROLE_HISTORY = st.tuples(
    ROLE_METADATA, st.sampled_from(("alice", "slack-A", "discord-A", "unknown")),
    st.booleans(), st.booleans(),
)


@settings(max_examples=150, deadline=None)
@given(history=st.lists(ROLE_HISTORY, min_size=1, max_size=18))
def test_identity_reload_histories_do_not_widen_frozen_turns(history) -> None:
    policy = PolicyRegistry()
    policy.freeze()
    snapshots = []
    with TemporaryDirectory(prefix="mimir-authz-") as directory:
        home = Path(directory)
        (home / "state").mkdir()
        resolver = IdentityResolver(home)
        for raw_roles, author, http, enforce in history:
            person = {"canonical": "alice", "aliases": ["slack-A", "discord-A"]}
            if raw_roles is not None:
                person["access"] = {"roles": list(raw_roles)}
            (home / "state" / "identities.yaml").write_text(
                yaml.safe_dump({"people": [person]}), encoding="utf-8",
            )
            resolver.reload()
            expected_roles = (
                tuple(raw_roles) if raw_roles and set(raw_roles) <= {"user", "admin"}
                else ()
            )
            if author == "unknown" or http:
                expected_roles = ()
            context = create_auth_context(
                AgentEvent(trigger="user_message", channel_id="one", author=author),
                resolver, event_ingress="http_event" if http else None,
                enforce=enforce, policy=policy,
            )
            canonical = "unknown" if author == "unknown" else "alice"
            snapshots.append((context, canonical, expected_roles, http))
            for old, old_canonical, old_roles, old_http in snapshots:
                assert old.canonical_principal == old_canonical
                assert old.roles == old_roles
                scope = ownership.get_authorization_scope(old)
                assert scope.principal == old_canonical
                assert scope.is_admin == ("admin" in old_roles and not old_http)
                assert not scope.is_service
            event("HTTP attenuation" if http else "named ingress")
            event("snapshot admin" if "admin" in expected_roles else "snapshot nonadmin")


def test_sql_oracle_detects_temporary_missing_owner_grant_mutation() -> None:
    """Calibrate RIPR: skip the owner grant and let Hypothesis find a witness."""
    scope = AuthorizationScope(principal="alice")
    original = ownership.authorization_predicate
    with closing(sqlite3.connect(":memory:")) as db:
        db.execute(
            "CREATE TABLE resources "
            "(id INTEGER, owner_principal TEXT, visibility TEXT, origin_domain TEXT)"
        )

        def disagrees(row) -> bool:
            db.execute("DELETE FROM resources")
            db.execute("INSERT INTO resources VALUES (0, ?, ?, ?)", row)
            observed = bool(_select(db, scope, ownership.authorization_predicate))
            return observed != _readable(scope, row)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                ownership, "authorization_predicate",
                lambda scope, table="atoms": (f"{table}.visibility = ?", ["public"]),
            )
            witness = find(ROW, disagrees, settings=settings(max_examples=100, deadline=None))
            assert witness[0] == "alice" and witness[1] != "public"
        assert ownership.authorization_predicate is original
        assert not disagrees(witness)
