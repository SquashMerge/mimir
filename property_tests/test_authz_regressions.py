"""Small deterministic authorization witnesses retained beside the campaigns.

JSON-null provenance must fail closed (#2174). The other witnesses
exercise the models' important non-vacuous boundary cases.
"""

from __future__ import annotations

import sqlite3

from mimir.saga.ownership import (
    AuthorizationScope,
    Ownership,
    authorization_predicate,
    intersect_acl_from_rows,
)


def test_private_owner_grant_survives_unrelated_domain_replacement() -> None:
    scope = AuthorizationScope(principal="alice")
    db = sqlite3.connect(":memory:")
    try:
        db.execute(
            "CREATE TABLE atoms "
            "(id INTEGER PRIMARY KEY, owner_principal TEXT, visibility TEXT, origin_domain TEXT)"
        )
        db.execute("INSERT INTO atoms VALUES (1, 'alice', 'private', 'one')")
        for domain in ("one", "two", None, "one"):
            db.execute("UPDATE atoms SET origin_domain = ? WHERE id = 1", (domain,))
            where, params = authorization_predicate(scope)
            assert db.execute(f"SELECT id FROM atoms WHERE {where}", params).fetchall() == [(1,)]
        db.execute("UPDATE atoms SET owner_principal = 'bob' WHERE id = 1")
        where, params = authorization_predicate(scope)
        assert db.execute(f"SELECT id FROM atoms WHERE {where}", params).fetchall() == []
    finally:
        db.close()


def test_missing_provenance_cannot_be_repaired_by_a_later_proven_source() -> None:
    proven = {
        "owner_principal": "alice",
        "visibility": "public",
        "origin_channel": "one",
        "origin_domain": "one",
        "provenance": '{"source":"atom"}',
    }
    for unproven in ({}, {"provenance": ""}, {"provenance": "not JSON"}):
        missing = {**proven, **unproven}
        if not unproven:
            missing.pop("provenance")
        assert intersect_acl_from_rows([missing, proven]) == Ownership()
        assert intersect_acl_from_rows([proven, missing]) == Ownership()


def test_json_null_provenance_fails_closed_instead_of_crashing() -> None:
    # Regression for #2174: JSON-null provenance must yield the fail-closed
    # legacy_admin ACL, not a TypeError (docs/authorization.md).
    row = {
        "owner_principal": "alice",
        "visibility": "public",
        "origin_channel": "one",
        "origin_domain": "one",
        "provenance": "null",
    }
    assert intersect_acl_from_rows([row]) == Ownership()
