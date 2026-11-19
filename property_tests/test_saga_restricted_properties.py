"""Bounded restricted-reader properties against real SAGA queries.

The oracle is a set of independent grants, not a predicate/string comparison.
No admin/platform reader is generated. SQL writes model persisted ACL changes,
not an assertion that a user-facing mutation API permits these changes.
"""

from __future__ import annotations

import asyncio
import importlib
import sqlite3
import struct
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st

from mimir.models import AuthContext
from mimir.plugins.core import ServicePrincipal
from mimir.saga import ownership
from mimir.saga.client import SagaStore, _make_faiss_search_fn
from mimir.saga.fts import fts_search
from mimir.saga.ownership import SagaReadAuthorization
from mimir.saga.recall import recall
from mimir.saga.reflect import recent_session_boundaries
from mimir.saga.triples import (
    rank_triple_candidates,
    retrieve_by_entity,
    top_triples_with_payload,
    triple_augment_search,
)

SCHEMA = Path(__file__).resolve().parents[1] / "mimir/saga/schema.sql"
KINDS = ("user", "service-owner", "service-domain", "mismatched-service")
KEYS = tuple(range(13))
MUTABLE_KEY = 12
REPRESENTATION = st.sampled_from((
    ("alice", "reader", "tenant"),
    ("user:alice", "poller:reader", "tenant' OR 1=1 --"),
    ("álïce", "reader:二", "ténant/二"),
))


@dataclass(frozen=True)
class ACL:
    owner: str
    visibility: str
    domain: str | None


@dataclass(frozen=True)
class Reader:
    context: AuthContext
    owner: str
    domains: frozenset[str]
    service: bool


def _reader(kind: str, representation: tuple[str, str, str]) -> Reader:
    user, service, domain = representation
    trusted = kind in ("service-owner", "service-domain")
    mismatch = kind == "mismatched-service"
    canonical = service if trusted else user
    domains = (domain,) if kind == "service-domain" else ()
    # Direct construction is server/test-side. This investigates row reads, not
    # whether an external caller can mint a trusted carrier.
    authority = ServicePrincipal(
        canonical=service,
        trigger="restricted-property",
        readable_domains=(domain,) if mismatch else domains,
        saga_full_corpus_read=False,
    )
    context = AuthContext(
        principal="transport-alias",
        canonical_principal=canonical,
        roles=("user",),
        event_ingress=None,
        trigger="restricted-property",
        channel_id="channel",
        interactivity=None,
        enforcement_enabled=True,
        is_service=trusted or mismatch,
        service_authority=authority if trusted or mismatch else None,
    )
    expected = Reader(
        context, f"service:{service}" if trusted else user,
        frozenset(domains), trusted,
    )
    scope = ownership.get_authorization_scope(context)
    assert not scope.is_admin and not scope.is_platform_service
    assert scope.is_service == expected.service
    assert set(scope.readable_domains) == expected.domains
    return expected


def _grants(reader: Reader, acl: ACL) -> frozenset[str]:
    """No production helper determines expected membership."""
    grants = {
        "public": acl.visibility == "public",
        "owner": acl.owner == reader.owner,
        "domain": acl.domain in reader.domains,
    }
    return frozenset(name for name, holds in grants.items() if holds)


def _acls(reader: Reader, representation: tuple[str, str, str]) -> dict[str, ACL]:
    _, service, domain = representation
    return {
        "public": ACL("foreign", "public", None),
        "owned": ACL(reader.owner, "private", None),
        "domain": ACL("foreign", "service", domain),
        "foreign": ACL("foreign", "private", "other"),
        "raw-service": ACL(service, "service", None),
        "wrong-service": ACL("service:other", "private", None),
        "declared-service": ACL(f"service:{service}", "service", None),
        "sentinel": ACL("legacy_admin", "legacy_admin", None),
        "service-sentinel": ACL("service", "service", None),
        "system-sentinel": ACL("system", "private", None),
    }


def _date(key: int) -> str:
    return f"2026-11-{key + 1:02d}T00:00:00+00:00"


def _blob(key: int) -> bytes:
    return struct.pack("2f", 1.0, key + 1.0)


def _put(db, key: int, atom: ACL, session: ACL, triple: ACL) -> None:
    """Stable IDs and genuine schema/FTS triggers, including delete/reinsert."""
    db.execute(
        "INSERT INTO atoms(id, content, content_hash, created_at, owner_principal, "
        "visibility, origin_domain, provenance, is_pinned) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 1) "
        "ON CONFLICT(id) DO UPDATE SET owner_principal=excluded.owner_principal, "
        "visibility=excluded.visibility, origin_domain=excluded.origin_domain",
        (f"a{key}", f"needle resource {key}", f"hash-{key}", _date(key),
         atom.owner, atom.visibility, atom.domain),
    )
    db.execute(
        "INSERT INTO sessions(id, channel_id, started_at, ended_at, reflected_at, "
        "summary, owner_principal, visibility, origin_domain, provenance) "
        "VALUES (?, 'channel', ?, ?, ?, 'needle', ?, ?, ?, '{}') "
        "ON CONFLICT(id) DO UPDATE SET owner_principal=excluded.owner_principal, "
        "visibility=excluded.visibility, origin_domain=excluded.origin_domain",
        (f"s{key}", _date(key), _date(key), _date(key),
         session.owner, session.visibility, session.domain),
    )
    db.execute(
        "INSERT INTO triples(id, subject, predicate, object, source_atom_id, "
        "created_at, embedding, embedding_dim, owner_principal, visibility, "
        "origin_domain, provenance) VALUES (?, 'needle', 'status', ?, ?, ?, ?, "
        "2, ?, ?, ?, '{}') ON CONFLICT(id) DO UPDATE SET "
        "owner_principal=excluded.owner_principal, visibility=excluded.visibility, "
        "origin_domain=excluded.origin_domain",
        (f"t{key}", f"value-{key}", f"a{key}", _date(key), _blob(key),
         triple.owner, triple.visibility, triple.domain),
    )
    db.commit()


@contextmanager
def _database(candidate_limit: int = 100):
    """All state, configuration and telemetry seams are test-owned/restored."""
    with closing(sqlite3.connect(":memory:", check_same_thread=False)) as db:
        db.executescript(SCHEMA.read_text(encoding="utf-8"))
        db.execute("PRAGMA foreign_keys=ON")
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(ownership, "_emit_saga_event", lambda *a, **kw: None)
            patch.setattr(
                "mimir.saga._config_io.get_config",
                lambda: lambda section, name, default=None: (
                    candidate_limit if (section, name) == ("storage", "triple_candidate_limit")
                    else default
                ),
            )
            yield db


def _seed(db, reader, representation, insertion_order):
    acls = _acls(reader, representation)
    # Separate stored-triple and source-atom attenuation boundaries.
    rows = [
        (acls[name],) * 3 for name in acls
    ] + [
        (acls["owned"], acls["owned"], acls["foreign"]),
        (acls["foreign"], acls["foreign"], acls["owned"]),
        (acls["owned"],) * 3,  # Mutable row; fixed anchors remain discriminating.
    ]
    model = {key: row for key, row in enumerate(rows)}
    for key in insertion_order:
        _put(db, key, *model[key])
    return model


async def _observe(db, client, reader, model, request_order, batch_size, cap=100):
    allowed = [
        {key for key, row in model.items() if _grants(reader, row[axis])}
        for axis in range(3)
    ]
    atom_ids = {f"a{key}" for key in allowed[0]}
    request = [f"a{key}" for key in request_order] + ["absent", "a0", "a0"]
    # Request order/de-duplication and independent batching must preserve grants.
    for batch in [request] + [
        request[start:start + batch_size] for start in range(0, len(request), batch_size)
    ]:
        unique = list(dict.fromkeys(batch))
        expected = [aid for aid in unique if aid in atom_ids]
        payload = await client.get_atoms(batch, auth_context=reader.context)
        assert [a["id"] for a in payload["atoms"]] == expected, "atom grant membership/order"
        assert payload["missing"] == [aid for aid in unique if aid not in atom_ids]
        assert {s["resource_id"] for s in payload["_ifc_sources"]} == {
            f"atom:{aid}" for aid in expected
        }, "denied atom cannot enter source payload"

    auth = SagaReadAuthorization(reader.context, "restricted-property")
    found = fts_search(db, "needle", top_k=100, read_authorization=auth)
    assert {aid for aid, _ in found} == atom_ids, "FTS denied-row membership"
    first = fts_search(db, "needle", top_k=1, read_authorization=auth)
    assert len(first) == 1 and first[0][0] in atom_ids, "FTS filter before LIMIT"

    sessions = sorted(allowed[1], reverse=True)
    for limit in (1, 100):
        recent = recent_session_boundaries(db, count=limit, auth_context=reader.context)
        assert [row["session_id"] for row in recent] == [
            f"s{key}" for key in sessions[:limit]
        ], "session grant before recency LIMIT"
    searched = await client.search_sessions(
        "needle", alpha=0.0, limit=100, auth_context=reader.context,
    )
    assert {row["session_id"] for row in searched} == {f"s{k}" for k in sessions}

    eligible = allowed[0] & allowed[2]
    entities = retrieve_by_entity(db, "needle", top_k=100, auth_context=reader.context)
    assert {row["id"] for row in entities} == {f"t{k}" for k in eligible}, (
        "triple and source independent grant conjunction"
    )
    # Contract: newest eligible cap first, then cosine. Vectors (1,k+1)
    # have strictly decreasing cosine with query (1,0).
    pool = sorted(eligible, reverse=True)[:cap]
    expected = [f"t{k}" for k in sorted(pool)]
    ranked = rank_triple_candidates(
        db, [1.0, 0.0], dim=2, auth_context=reader.context,
    )
    assert [row["id"] for row in ranked] == expected, "triple authorization before cap/ranking"
    payload = top_triples_with_payload(
        db, [1.0, 0.0], dim=2, top_n=100, auth_context=reader.context,
    )
    assert [row["id"] for row in payload] == expected
    augmented = triple_augment_search(
        db, [1.0, 0.0], dim=2, top_k=100, auth_context=reader.context,
    )
    assert [aid for aid, _ in augmented] == [f"a{k}" for k in sorted(pool)]


@pytest.mark.parametrize("kind", KINDS)
@settings(max_examples=60, deadline=None)
@given(
    representation=REPRESENTATION,
    insertion_order=st.permutations(KEYS),
    request_order=st.permutations(KEYS),
    batch_size=st.integers(1, 6),
    cap=st.integers(1, 5),
    history=st.lists(
        st.tuples(st.integers(0, 2), st.sampled_from((
            "public", "owned", "domain", "foreign", "raw-service",
            "wrong-service", "declared-service", "sentinel", "service-sentinel", "system-sentinel",
        ))),
        min_size=1, max_size=9,
    ),
)
def test_restricted_queries_follow_observed_acl_histories(
    kind, representation, insertion_order, request_order, batch_size, cap, history,
) -> None:
    reader = _reader(kind, representation)
    event(f"restricted scope={kind}")
    acls = _acls(reader, representation)
    assert _grants(reader, acls["owned"]) == {"owner"}
    assert _grants(reader, acls["public"]) == {"public"}
    assert not _grants(reader, acls["foreign"])
    assert not _grants(reader, acls["raw-service"])
    assert bool(_grants(reader, acls["domain"])) == (kind == "service-domain")
    assert bool(_grants(reader, acls["declared-service"])) == reader.service

    async def run():
        with _database(cap) as db:
            client = SagaStore(conn=db, embedding_dim=2)
            model = _seed(db, reader, representation, insertion_order)

            async def observe():
                await _observe(db, client, reader, model, request_order, batch_size, cap)
                event("observed denied atom/session/triple alongside allowed anchors")
                event("observed sole domain grant" if reader.domains else "observed domain denial")

            await observe()  # Warm reads before any ownership changes.
            # Every example must reach revocation and re-grant on all three
            # resource types; arbitrary histories cannot shrink these away.
            operations = [
                (axis, label) for axis in range(3)
                for label in ("foreign", "public", "domain", "owned")
            ] + history
            for axis, label in operations:
                row = list(model[MUTABLE_KEY])
                row[axis] = acls[label]
                model[MUTABLE_KEY] = tuple(row)
                _put(db, MUTABLE_KEY, *model[MUTABLE_KEY])
                await observe()
                event(f"observed mutation axis={axis} grants={','.join(sorted(_grants(reader, row[axis]))) or 'denied'}")
            for table, prefix in (("triples", "t"), ("atoms", "a"), ("sessions", "s")):
                db.execute(f"DELETE FROM {table} WHERE id=?", (f"{prefix}{MUTABLE_KEY}",))
            db.commit()
            del model[MUTABLE_KEY]
            await observe()
            event("observed physical deletion")
            model[MUTABLE_KEY] = (acls["owned"],) * 3
            _put(db, MUTABLE_KEY, *model[MUTABLE_KEY])
            await observe()
            event("observed stable-ID reinsertion after delete")
            event(f"generated tail mutations={len(history)}")

    asyncio.run(run())


class RankedIndex:
    """Only the ANN ordering is controlled; authorization uses real SQL."""

    def __init__(self, ids):
        self.total_vectors = len(ids)
        self.candidates = [(aid, 1.0 - index / (len(ids) + 1)) for index, aid in enumerate(ids)]

    def search(self, query, *, top_k):
        return self.candidates[:top_k]


@st.composite
def _independent_pathways(draw):
    """Different supports as well as orders; a12 is exclusively extra-pathway."""
    paths = {}
    anchors = {
        "semantic": {0, 3},
        "keyword": {1, 3},
        "triple": {0, 1, 3},
        "boundary": {MUTABLE_KEY, 3},
    }
    for name, required in anchors.items():
        if name == "triple" and not draw(st.booleans()):
            paths[name] = []
            continue
        # Sole-public a0 cannot enter keyword; sole-owner a1 cannot enter
        # semantic. Swapping those wires is observably wrong in every example.
        excluded = {
            "semantic": {1, MUTABLE_KEY},
            "keyword": {0, MUTABLE_KEY},
            "triple": {MUTABLE_KEY},
            "boundary": set(),
        }[name]
        support = tuple(key for key in KEYS if key not in excluded)
        order = draw(st.permutations(support))
        selected = draw(st.sets(st.sampled_from(support))) | required
        ids = ["a3"] + [f"a{key}" for key in order if key in selected and key != 3]
        paths[name] = ids + ids if name == "boundary" else ids
    return paths


def _expected_pathways(paths, allowed):
    """Built-ins keep order; extra pathways additionally de-duplicate."""
    expected = {}
    for name, ids in paths.items():
        selected = [aid for aid in ids if aid in allowed]
        if name == "boundary":
            selected = list(dict.fromkeys(selected))
        # recall always wires semantic/keyword, and wires triple only when
        # its post-authorization candidates are nonempty (recall.py:385–392).
        if name != "triple" or selected:
            expected[name] = selected
    return expected


def _check_preranking(db, reader, allowed, paths, rrf_k=7, rrf_input_mutation=None):
    """Check exact named wiring, compacted ranks, fusion and reinforcement.

    rrf_input_mutation is a test-only calibration hook: corrupt a valid mapping
    immediately before the boundary checker, never edit production source.
    """
    expected = _expected_pathways(paths, allowed)
    expected_keys = {"semantic", "keyword", "boundary"}
    if paths["triple"]:
        expected_keys.add("triple")  # Enabled triples always include allowed anchors.
    assert set(expected) == expected_keys
    expected_ids = set().union(*(set(ids) for ids in expected.values()))
    assert "a12" in expected["boundary"]
    assert all("a12" not in ids for name, ids in expected.items() if name != "boundary")
    weights = {"semantic": 2.0, "keyword": 3.0, "triple": 5.0, "boundary": 7.0}
    # Independently apply the documented sum w_p / (k + one-based rank).
    contributions = {
        name: {aid: weights[name] / (rrf_k + rank) for rank, aid in enumerate(ids, 1)}
        for name, ids in expected.items()
    }
    expected_scores = {
        aid: sum(scores.get(aid, 0.0) for scores in contributions.values())
        for aid in expected_ids
    }
    auth = SagaReadAuthorization(reader.context, "restricted-preranking")
    adapter = _make_faiss_search_fn(
        RankedIndex(paths["semantic"]), db, read_authorization=auth,
    )
    assert [aid for aid, _ in adapter([1.0, 0.0], 1)] == expected["semantic"][:1], (
        "ANN denied-row filter before top-k"
    )
    recall_module = importlib.import_module("mimir.saga.recall")
    original_rrf = recall_module.reciprocal_rank_fusion
    calls = []

    def checked_rrf(lists, **kwargs):
        observed = {name: list(ids) for name, ids in lists.items()}
        if rrf_input_mutation is not None:
            observed = rrf_input_mutation(observed)
        calls.append(observed)
        assert set(observed) == expected_keys, "RRF exact pathway key set"
        for name in expected_keys:
            assert observed[name] == expected[name], f"RRF named pathway membership/order: {name}"
        assert kwargs["k"] == rrf_k and kwargs["weights"] == weights, "RRF parameter wiring"
        fused = original_rrf(observed, **kwargs)
        assert dict(fused) == pytest.approx(expected_scores), "RRF independent contribution sum"
        return fused

    before = Counter(row[0] for row in db.execute(
        "SELECT atom_id FROM access_events WHERE source='retrieval'"
    ))
    candidates = {
        name: [(aid, 1.0) for aid in ids]
        for name, ids in paths.items() if name != "boundary"
    }
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(recall_module, "reciprocal_rank_fusion", checked_rrf)
        result = recall(
            db, "needle",
            query_embed_fn=lambda q: [1.0, 0.0],
            faiss_search_fn=lambda q, k: candidates["semantic"],
            fts_search_fn=lambda q, k: candidates["keyword"],
            triple_search_fn=lambda q, k: candidates["triple"],
            extra_atom_ranked_pathways={"boundary": paths["boundary"]},
            rrf_pathway_weights=weights,
            weights={"rrf_k": rrf_k, "w_rrf": 1.0, "w_topic": 0.0, "w_act": 0.0},
            k=100, auth_context=reader.context,
            fire_access_events=True,
        )
    assert len(calls) == 1, "actual RRF observation is reachable exactly once"
    found = result.raws + result.observations
    assert {c.atom["id"] for c in found} == expected_ids, "recall exact candidate membership"
    for candidate in found:
        aid = candidate.atom["id"]
        ranks = tuple(
            expected[name].index(aid) + 1 if aid in expected.get(name, ()) else -1
            for name in ("semantic", "keyword", "triple")
        )
        assert (candidate.semantic_rank, candidate.keyword_rank, candidate.triple_rank) == ranks, (
            "named pathway ranks must compact after exclusion"
        )
        assert candidate.rrf_score == pytest.approx(expected_scores[aid]), (
            "candidate carries independent RRF contribution sum"
        )
        if aid == "a12":
            assert ranks == (-1, -1, -1), "boundary-only candidate has no built-in rank"
            assert candidate.rrf_score == pytest.approx(contributions["boundary"][aid])
    retrieved = Counter(row[0] for row in db.execute(
        "SELECT atom_id FROM access_events WHERE source='retrieval'"
    ))
    # The database is wholly test-owned and all candidates are pinned raws with
    # k=100: every surfaced candidate must receive exactly one retrieval event.
    assert retrieved == before + Counter(expected_ids), "exact allowed retrieval reinforcement"


@settings(max_examples=100, deadline=None)
@given(
    kind=st.sampled_from(KINDS),
    representation=REPRESENTATION,
    paths=_independent_pathways(),
    rrf_k=st.sampled_from((1, 7, 60)),
)
def test_restricted_readers_exclude_denials_before_top_k_and_rrf(kind, representation, paths, rrf_k):
    reader = _reader(kind, representation)
    event(f"restricted scope={kind}")
    with _database() as db:
        model = _seed(db, reader, representation, KEYS)
        allowed = {f"a{k}" for k, row in model.items() if _grants(reader, row[0])}
        _check_preranking(db, reader, allowed, paths, rrf_k)
        event("denied anchors reached ANN SQL and exact named RRF boundary")
        event("observed boundary-only allowed candidate and exact reinforcement")
        event("observed optional triple present" if paths["triple"] else "observed optional triple absent")


def test_owned_mutations_calibrate_membership_and_preranking_checkers():
    """Mutate only test-owned seams, then prove the restored checker passes."""
    representation = ("alice", "reader", "tenant")
    reader = _reader("service-domain", representation)
    with _database() as db:
        model = _seed(db, reader, representation, KEYS)
        allowed = {f"a{k}" for k, row in model.items() if _grants(reader, row[0])}
        paths = {
            "semantic": ["a3", "a0"],
            "keyword": ["a3", "a1"],
            "triple": ["a3", "a1", "a0"],
            "boundary": ["a3", "a12", "a0", "a12"],
        }
        original = SagaReadAuthorization.selection_predicate
        # Omit the owner grant: exact membership oracle must notice under-serving.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                SagaReadAuthorization, "selection_predicate",
                lambda self, table: (
                    f"({table}.visibility=? OR {table}.origin_domain=?)",
                    ["public", "tenant"],
                ),
            )
            with pytest.raises(AssertionError, match="atom grant membership/order"):
                asyncio.run(_observe(
                    db, SagaStore(conn=db, embedding_dim=2), reader, model, KEYS, 3,
                ))
        # Omit filtering only before RRF. Hydration still filters: a final-output
        # membership-only checker would miss this ranking defect.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                SagaReadAuthorization, "selection_predicate",
                lambda self, table: ("1=1", []) if table == "a" else original(self, table),
            )
            with pytest.raises(AssertionError, match="ANN denied-row filter before top-k"):
                _check_preranking(db, reader, allowed, paths)
            # Supply an already-filtered ANN ordering to isolate the independent
            # injected-adapter/RRF boundary, while hydration still filters.
            with pytest.MonkeyPatch.context() as inner:
                inner.setattr(
                    RankedIndex, "search",
                    lambda self, query, *, top_k: [
                        item for item in self.candidates if item[0] in allowed
                    ][:top_k],
                )
                with pytest.raises(AssertionError, match="RRF named pathway membership/order:"):
                    _check_preranking(db, reader, allowed, paths)
        # Corrupt the test-owned boundary mapping, not SQL: exact keys must
        # reject either an omitted built-in or the extra-only candidate's wire.
        for dropped in ("keyword", "boundary"):
            with pytest.raises(AssertionError, match="RRF exact pathway key set"):
                _check_preranking(
                    db, reader, allowed, paths,
                    rrf_input_mutation=lambda lists, dropped=dropped: {
                        name: ids for name, ids in lists.items() if name != dropped
                    },
                )

        def swap_keyword_semantic(lists):
            lists["semantic"], lists["keyword"] = lists["keyword"], lists["semantic"]
            return lists

        with pytest.raises(AssertionError, match="RRF named pathway membership/order:"):
            _check_preranking(
                db, reader, allowed, paths, rrf_input_mutation=swap_keyword_semantic,
            )
        # Zero retrieval reinforcement is not sufficient: kill that mutation
        # through the actual recall write seam in this owned database.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                importlib.import_module("mimir.saga.recall"), "mark_access",
                lambda *args, **kwargs: None,
            )
            with pytest.raises(AssertionError, match="exact allowed retrieval reinforcement"):
                _check_preranking(db, reader, allowed, paths)
        _check_preranking(db, reader, allowed, paths)
        asyncio.run(_observe(
            db, SagaStore(conn=db, embedding_dim=2), reader, model, KEYS, 3,
        ))
