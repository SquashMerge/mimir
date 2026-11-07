from __future__ import annotations

import asyncio
from dataclasses import replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import SimpleNamespace

import pytest
from langchain_core.tools import ToolException

from mimir import access_control
from mimir.access_control import (
    ServicePrincipal,
    SinkGate,
    ToolAuthorization,
    ToolFlowDirection,
    ToolRegistry,
    classify_protected_result,
)
from mimir.models import (
    AuthContext,
    InformationFlowLabels,
    InformationFlowState,
    NormalizedPullRequestSnapshot,
    SourceLabel,
)
try:
    version("mimir-cultivate")
except PackageNotFoundError:
    pytest.skip("repository tool wiring requires installed Cultivate", allow_module_level=True)

from mimir_cultivate.policy import (
    RepoPRAction, RepoPRActionScope, RepoPRScopeRegistry,
    RepoReviewState, ServerDiscoveredPRStates,
)
from mimir_cultivate.forge import (
    CheckProjection, ReviewProjection, remediation_checkout_preflight, set_forge_client,
)
from mimir_cultivate.repository import (
    _enforcement_enabled,
    repo_checkout,
    repo_cleanup,
    repo_status,
    repo_test,
)


@pytest.fixture(autouse=True)
def _reset_forge_client(monkeypatch: pytest.MonkeyPatch) -> None:
    # These checks exercise explicit provider tools and scoped policy without
    # selecting the independent heartbeat coding runtime for the whole process.
    monkeypatch.setenv("MIMIR_PLUGINS", "")
    set_forge_client(None)
    yield
    set_forge_client(None)


@pytest.fixture
def selected_repo_policy():
    from importlib.metadata import PackageNotFoundError, version
    from mimir.plugins.core import policy_scope
    from mimir.plugins.manager import load_plugins

    try:
        version("mimir-cultivate")
    except PackageNotFoundError:
        pytest.skip("repository remediation requires installed Cultivate")
    selected = load_plugins(enable="cultivate").policy
    with policy_scope(selected):
        yield selected


def _scope(
    *actions: RepoPRAction,
    number: int = 7,
    head_sha: str = "a" * 40,
    provenance: str = "server_discovered",
    event_type: str = "pr_changes_requested_stale",
) -> RepoPRActionScope:
    return RepoPRActionScope(
        provenance=provenance,
        canonical_repo="owner/repo",
        canonical_root="/srv/repo",
        canonical_origin="https://github.com/owner/repo.git",
        principal="mimir-bot",
        event_type=event_type,
        allowed_operations=frozenset(action.value for action in actions),
        pr_number=number,
        head_repo="owner/repo",
        head_remote="origin",
        destination_ref="refs/heads/fix",
        observed_head_sha=head_sha,
        base_ref="main",
        observed_base_sha="b" * 40,
    )


def _auth(scope: RepoPRActionScope, *additional: RepoPRActionScope) -> AuthContext:
    states = tuple(RepoReviewState(item) for item in (scope, *additional))
    state = states[0]
    labels = InformationFlowLabels().with_source(SourceLabel(
        principal="service:heartbeat",
        domain="channel",
        resource_id="scheduler:heartbeat",
        bridge_instance="service:heartbeat",
        sensitivity="internal",
        authorized_principals=frozenset({"service:heartbeat"}),
    )).with_channel("scheduler:heartbeat")
    service = ServicePrincipal(
        canonical="heartbeat",
        trigger="scheduled_tick",
        capabilities=("pr_comment",),
        readable_domains=("repository",),
        sink_destinations=("bound_pull_request",),
        authority_profile="heartbeat",
    )
    return AuthContext(
        principal=None,
        canonical_principal="heartbeat",
        roles=(),
        event_ingress=None,
        trigger="scheduled_tick",
        channel_id="scheduler:heartbeat",
        interactivity=None,
        is_service=True,
        service_authority=service,
        enforcement_enabled=True,
        ifc_labels=labels,
        repo_pr_scope_registry=RepoPRScopeRegistry(states),
        server_discovered_pr_states=ServerDiscoveredPRStates(),
        repo_review_state=state if len(states) == 1 else None,
        repo_pr_action_scope=scope if len(states) == 1 else None,
    )


@pytest.mark.parametrize(
    "runtime",
    [None, SimpleNamespace(), SimpleNamespace(context=SimpleNamespace())],
)
def test_unknown_repo_enforcement_state_fails_closed_and_emits_event(
    monkeypatch: pytest.MonkeyPatch,
    runtime: object | None,
) -> None:
    captured: list[tuple[str, dict[str, object]]] = []

    def capture(kind: str, **fields: object) -> None:
        captured.append((kind, fields))

    monkeypatch.setattr("mimir.event_logger.log_event_sync", capture)

    assert _enforcement_enabled(
        runtime,  # type: ignore[arg-type]
        repository="owner/repo",
        pull_request=7,
    ) is True

    assert captured == [(
        "repo_enforcement_state_unknown",
        {
            "repository": "owner/repo",
            "pull_request": 7,
            "fallback_enforcement": True,
        },
    )]


def test_repo_enforcement_state_preserves_explicit_flag() -> None:
    assert _enforcement_enabled(
        SimpleNamespace(context=SimpleNamespace(enforcement_enabled=False)),
        repository="owner/repo",
        pull_request=7,
    ) is False
    assert _enforcement_enabled(
        SimpleNamespace(context=SimpleNamespace(enforcement_enabled=True)),
        repository="owner/repo",
        pull_request=7,
    ) is True


@pytest.mark.parametrize("enforce", [False, True])
def test_repo_test_registry_vetoes_untrusted_checkout_even_in_shadow(
    enforce: bool, selected_repo_policy,
) -> None:
    scope = _scope(RepoPRAction.INSPECT, RepoPRAction.TEST)
    context = _auth(scope)
    source = SourceLabel(
        principal="external-author", domain="repository",
        resource_id=f"owner/repo#pull/7@{scope.observed_head_sha}",
        bridge_instance="forge", sensitivity="internal",
        authorized_principals=frozenset({"heartbeat"}),
        source_kind="protected_tool", integrity="untrusted",
        integrity_effect="active_ingest",
    )
    channel_labels = replace(context.ifc_labels, sources=(replace(
        context.ifc_labels.sources[0], integrity="trusted", integrity_effect="active_ingest",
    ),))
    labels = channel_labels.with_source(source)
    context = replace(
        context, ifc_labels=labels, ifc_state=InformationFlowState(labels),
        enforcement_enabled=False,
        service_authority=replace(context.service_authority, capabilities=("repo_test",)),
    )
    decision = ToolRegistry().authorize_tool(
        "repo_test", context, enforce=enforce,
        arguments={"repository": "owner/repo", "pull_request": 7},
    )
    assert decision.allowed is False
    assert decision.is_shadow_decision is False
    assert decision.reason == "write_blocked_by_untrusted_ingest"
    assert decision.refusal_detail == access_control._TAINTED_WRITE_REFUSAL


@pytest.mark.asyncio
async def test_repo_wrapper_refuses_unconfigured_repository_without_scope() -> None:
    runtime = SimpleNamespace(context=AuthContext(
        principal=None, canonical_principal=None, roles=(), event_ingress=None,
        trigger="scheduled_tick", channel_id="scheduler:heartbeat",
        interactivity=None,
    ))

    with pytest.raises(ToolException, match="not configured in GITHUB_REPOS"):
        repo_status.func(
            repository="owner/repo", pull_request=7, runtime=runtime,
        )

    with pytest.raises(ToolException, match="not configured in GITHUB_REPOS"):
        await repo_test.coroutine(
            repository="owner/repo", pull_request=7, runtime=runtime,
        )


@pytest.mark.asyncio
async def test_repo_wrapper_refuses_unconfigured_unlisted_pull_request() -> None:
    inspect = _scope(RepoPRAction.INSPECT)
    context = _auth(inspect)

    with pytest.raises(ToolException, match="not configured in GITHUB_REPOS"):
        repo_status.func(
            repository="owner/repo", pull_request=8,
            runtime=SimpleNamespace(context=context),
        )
    with pytest.raises(ToolException, match="not configured in GITHUB_REPOS"):
        await repo_test.coroutine(
            repository="owner/repo", pull_request=8,
            runtime=SimpleNamespace(context=context),
        )


def test_repo_cleanup_refuses_without_active_lease() -> None:
    context = _auth(_scope(RepoPRAction.CHECKOUT))

    with pytest.raises(ToolException, match="no active checkout lease"):
        repo_cleanup.func(
            repository="owner/repo", pull_request=7,
            runtime=SimpleNamespace(context=context),
        )


@pytest.mark.parametrize("tool_name", ["repo_status", "repo_diff"])
def test_repository_read_wrappers_publish_lease_attestation(
    tool_name: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir_cultivate.repository import GitOperationResult
    from mimir_cultivate import repository as repo_module

    scope = _scope(RepoPRAction.INSPECT)
    context = _auth(scope)
    state = context.repo_pr_scope_registry.resolve("owner/repo", 7)
    assert state is not None
    published = []

    class FakeGitTools:
        execution_started = False

        def __init__(self, *args, **kwargs):
            pass

        def execute(self, operation):
            return GitOperationResult(True, "ok", stdout="result")

    monkeypatch.setattr(repo_module, "RepoGitTools", FakeGitTools)
    monkeypatch.setattr(
        repo_module, "_publish_attested_lease_result",
        lambda runtime, published_state: published.append(published_state),
    )
    runtime = SimpleNamespace(context=context)

    getattr(repo_module, tool_name).func("owner/repo", 7, runtime=runtime)

    assert published == [state]


@pytest.mark.asyncio
async def test_repo_test_wrapper_publishes_lease_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mimir_cultivate.repository import ProjectTestResult
    from mimir_cultivate import repository as repo_module

    scope = _scope(RepoPRAction.INSPECT)
    context = _auth(scope)
    state = context.repo_pr_scope_registry.resolve("owner/repo", 7)
    assert state is not None
    published = []

    async def execute(self, selectors, *, suite):
        return ProjectTestResult(True, "tests_passed", 0)

    monkeypatch.setattr(repo_module.RepoProjectTests, "execute", execute)
    monkeypatch.setattr(
        repo_module, "_publish_attested_lease_result",
        lambda runtime, published_state: published.append(published_state),
    )

    await repo_module.repo_test.coroutine(
        "owner/repo", 7, runtime=SimpleNamespace(context=context),
    )

    assert published == [state]


def test_checkout_proof_is_isolated_to_named_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_forge_client(SimpleNamespace(author_is_trusted=None))
    first = _scope(RepoPRAction.CHECKOUT, RepoPRAction.INSPECT)
    second = _scope(RepoPRAction.CHECKOUT, RepoPRAction.INSPECT, number=8)
    context = _auth(first, second)
    registry = context.repo_pr_scope_registry
    assert registry is not None

    def fake_checkout(scope, *, owner, review_state, attest_resume=None):
        lease = SimpleNamespace(
            path=tmp_path / str(scope.pr_number), scope_id=scope.scope_id,
            head_sha=scope.observed_head_sha, owner=owner, is_active=True,
        )
        review_state.attach_checkout_lease(lease)
        return lease, ()

    monkeypatch.setattr("mimir_cultivate.repository.acquire_pr_checkout_lease", fake_checkout)
    runtime = SimpleNamespace(context=context)
    repo_checkout.func(repository="owner/repo", pull_request=7, runtime=runtime)

    first_state = registry.resolve("owner/repo", 7)
    second_state = registry.resolve("owner/repo", 8)
    assert first_state is not None and first_state.checked_out is True
    assert second_state is not None and second_state.checked_out is False
    with pytest.raises(ToolException, match="inactive_checkout"):
        repo_status.func(repository="owner/repo", pull_request=8, runtime=runtime)


class _RemediationForge:
    def __init__(self, *snapshots: NormalizedPullRequestSnapshot) -> None:
        self.snapshots = list(snapshots)
        self.snapshot_calls = 0
        self.review_calls = 0
        self.reviews = (
            ReviewProjection("1", "reviewer", "CHANGES_REQUESTED", "fix", "now", "a" * 40),
        )

    def get_pull_request_snapshot(self, repository, number):  # type: ignore[no-untyped-def]
        self.snapshot_calls += 1
        return self.snapshots.pop(0)

    def list_reviews(self, scope):  # type: ignore[no-untyped-def]
        self.review_calls += 1
        return self.reviews


def _snapshot(head_sha: str, *, state: str = "open") -> NormalizedPullRequestSnapshot:
    return NormalizedPullRequestSnapshot(
        repo="owner/repo",
        state=state,
        number=7,
        author="mimir-bot",
        head_repo="owner/repo",
        head_remote="origin",
        head_ref="fix",
        head_sha=head_sha,
        base_ref="main",
        base_sha="b" * 40,
    )


class _CIRemediationForge(_RemediationForge):
    def __init__(self, snapshot: NormalizedPullRequestSnapshot, conclusion: str) -> None:
        super().__init__(snapshot)
        self.conclusion = conclusion

    def list_checks(self, scope):  # type: ignore[no-untyped-def]
        return (
            CheckProjection("tests", "completed", self.conclusion, "now", "now"),
        )


@pytest.mark.parametrize(
    ("snapshot", "conclusion", "message"),
    [
        (_snapshot("c" * 40), "failure", "pull request head was superseded"),
        (_snapshot("a" * 40, state="closed"), "failure", "pull request is closed or merged"),
        (_snapshot("a" * 40), "success", "pull request checks are no longer failing"),
    ],
)
def test_ci_remediation_rechecks_live_state_before_checkout(
    snapshot: NormalizedPullRequestSnapshot,
    conclusion: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _scope(
        *RepoPRAction,
        provenance="poller_payload",
        event_type="pr_ci_failure",
    )
    context = _auth(scope)
    set_forge_client(_CIRemediationForge(snapshot, conclusion))
    monkeypatch.setattr(
        "mimir_cultivate.repository.acquire_pr_checkout_lease",
        lambda *_args, **_kwargs: pytest.fail("stale CI event reached checkout"),
    )

    result = repo_checkout.func(
        repository="owner/repo",
        pull_request=7,
        runtime=SimpleNamespace(context=context),
    )

    assert result["status"] == "stopped"
    assert result["message"] == message
    assert "nothing to remediate" in result["guidance"]


@pytest.mark.parametrize("conclusion", ["failure", "timed_out", "startup_failure", "action_required"])
def test_ci_superseded_head_remints_at_current_failing_head(monkeypatch, conclusion):
    old = _scope(*RepoPRAction, provenance="poller_payload", event_type="pr_ci_failure")
    context = _auth(old)
    snapshot = _snapshot("c" * 40)
    fresh_scope = replace(old, provenance="server_discovered", observed_head_sha=snapshot.head_sha)
    minted = []
    checked = []

    def mint(repository, observed, *, event_type):
        assert (repository, observed, event_type) == ("owner/repo", snapshot, "pr_ci_failure")
        minted.append(observed)
        return fresh_scope

    def checks(scope):
        checked.append(scope)
        assert scope is fresh_scope
        return (CheckProjection("tests", "completed", conclusion, "now", "now"),)

    client = _CIRemediationForge(snapshot, conclusion)
    monkeypatch.setattr(client, "list_checks", checks)
    monkeypatch.setattr("mimir.access_control.create_server_discovered_heartbeat_scope", mint)
    set_forge_client(client)
    fresh, stopped = remediation_checkout_preflight(context, "owner/repo", 7)
    assert stopped is None
    assert fresh.action_scope is fresh_scope
    assert fresh.action_scope.observed_head_sha == "c" * 40
    assert context.server_discovered_pr_states.resolve("owner/repo", 7) is fresh
    assert context.repo_pr_scope_registry.resolve("owner/repo", 7).action_scope is old
    assert not context.server_discovered_pr_states.begin_remint("owner/repo", 7)
    # A second observation cannot mint again, even if the provider advances again.
    client.snapshots.append(_snapshot("d" * 40))
    assert remediation_checkout_preflight(context, "owner/repo", 7) == (fresh, None)
    assert minted == [snapshot]
    assert checked == [fresh_scope]


@pytest.mark.parametrize("state,authorized,status,conclusion,message", [
    ("closed", True, "completed", "failure", "pull request is closed or merged"),
    ("merged", True, "completed", "failure", "pull request is closed or merged"),
    ("open", False, "completed", "failure", "pull request head was superseded"),
    ("open", True, "completed", "success", "pull request checks are no longer failing"),
    ("open", True, "in_progress", "failure", "pull request checks are no longer failing"),
    ("open", True, "completed", "cancelled", "pull request checks are no longer failing"),
    ("open", True, "completed", None, "pull request checks are no longer failing"),
])
def test_ci_superseded_head_preserves_stops(monkeypatch, state, authorized, status, conclusion, message):
    old = _scope(*RepoPRAction, provenance="poller_payload", event_type="pr_ci_failure")
    context = _auth(old)
    snapshot = _snapshot("c" * 40, state=state)
    fresh_scope = replace(old, provenance="server_discovered", observed_head_sha=snapshot.head_sha)
    minted = []
    checked = []

    def mint(*args, **kwargs):
        minted.append(snapshot)
        return fresh_scope if authorized else None

    def checks(scope):
        assert scope is fresh_scope
        checked.append(scope)
        return (CheckProjection("tests", status, conclusion, "now", "now"),)

    client = _CIRemediationForge(snapshot, conclusion)
    monkeypatch.setattr(client, "list_checks", checks)
    monkeypatch.setattr("mimir.access_control.create_server_discovered_heartbeat_scope", mint)
    set_forge_client(client)
    assert remediation_checkout_preflight(context, "owner/repo", 7) == (None, message)
    assert len(minted) == int(state == "open")
    assert len(checked) == int(state == "open" and authorized)
    assert context.server_discovered_pr_states.resolve("owner/repo", 7) is None


@pytest.mark.parametrize("event_type,provenance", [
    (event, "poller_payload") for event in (
        "pr_opened", "pr_review_comment", "pr_review", "pr_synchronize",
        "pr_review_requested", "pr_mergeability_rebase", "pr_mergeability_conflicting",
        "heartbeat_pr_maintenance", "unknown",
    )
] + [(event, "server_discovered") for event in ("pr_ci_failure", "pr_changes_requested_stale")])
def test_remediation_preflight_does_not_refresh_other_scopes(monkeypatch, event_type, provenance):
    scope = _scope(*RepoPRAction, event_type=event_type, provenance=provenance)
    context = _auth(scope)
    original = context.repo_pr_scope_registry.resolve("owner/repo", 7)
    client = _RemediationForge(_snapshot("c" * 40))
    set_forge_client(client)
    monkeypatch.setattr("mimir_cultivate.forge.resolve_review_state_for_context", lambda *args: original)
    assert remediation_checkout_preflight(context, "owner/repo", 7) == (original, None)
    assert client.snapshot_calls == 0
    assert context.server_discovered_pr_states.begin_remint("owner/repo", 7)


@pytest.mark.parametrize("event_type", ["pr_ci_failure", "pr_changes_requested_stale"])
def test_remember_remint_accepts_handled_remediation_events(event_type):
    original = RepoReviewState(_scope(event_type=event_type, provenance="poller_payload"))
    fresh = RepoReviewState(replace(original.action_scope, provenance="server_discovered", observed_head_sha="c" * 40))
    cache = ServerDiscoveredPRStates()
    assert cache.begin_remint("owner/repo", 7)
    assert cache.remember_remint(original, fresh) is fresh
    assert cache.resolve("owner/repo", 7) is fresh


@pytest.mark.parametrize("invalid", ["repository", "number", "event_mismatch", "provenance", "unreserved",
    "pr_opened", "pr_review_comment", "pr_review", "pr_synchronize", "pr_review_requested",
    "pr_mergeability_rebase", "pr_mergeability_conflicting", "heartbeat_pr_maintenance", "unknown"])
def test_remember_remint_rejects_invalid_authority(invalid):
    original = RepoReviewState(_scope(event_type="pr_ci_failure", provenance="poller_payload"))
    scope = replace(original.action_scope, provenance="server_discovered", observed_head_sha="c" * 40)
    if invalid == "repository":
        scope = replace(scope, canonical_repo="other/repo")
    elif invalid == "number":
        scope = replace(scope, pr_number=8)
    elif invalid == "event_mismatch":
        scope = replace(scope, event_type="pr_changes_requested_stale")
    elif invalid == "provenance":
        scope = replace(scope, provenance="poller_payload")
    elif invalid != "unreserved":
        original = RepoReviewState(replace(original.action_scope, event_type=invalid))
        scope = replace(scope, event_type=invalid)
    cache = ServerDiscoveredPRStates()
    if invalid != "unreserved":
        assert cache.begin_remint("owner/repo", 7)
    with pytest.raises(ValueError, match="reminted review scope"):
        cache.remember_remint(original, RepoReviewState(scope))
    assert cache.review_states == ()


def _remediation_runtime(monkeypatch: pytest.MonkeyPatch, client: _RemediationForge):
    old_scope = _scope(
        *RepoPRAction,
        provenance="poller_payload",
    )
    context = _auth(old_scope)
    service = replace(
        context.service_authority, canonical="poller:github-activity", trigger="poller",
        authority_profile="github", capabilities=("pr_metadata", "repo_checkout"),
    )
    context = replace(
        context, principal="service:poller:github-activity",
        canonical_principal=service.canonical, trigger="poller",
        channel_id=service.canonical, service_authority=service,
    )
    set_forge_client(client)
    monkeypatch.setenv("MIMIR_GITHUB_SELF_LOGIN", "mimir-bot")

    def mint(repository, snapshot, *, event_type):  # type: ignore[no-untyped-def]
        assert event_type == "pr_changes_requested_stale"
        return replace(
            old_scope,
            provenance="server_discovered",
            observed_head_sha=snapshot.head_sha,
        )

    monkeypatch.setattr(
        "mimir.access_control.create_server_discovered_heartbeat_scope", mint,
    )
    return old_scope, context, SimpleNamespace(context=context)


@pytest.mark.asyncio
@pytest.mark.parametrize("checkout", [True, False, "connect_error", "git_transport", "invalid_grant"])
async def test_poller_prechecks_own_remediation_before_first_pr_read(
    tmp_path, monkeypatch, selected_repo_policy, checkout,
):
    from mimir.access_control import ScopedPRGrants, _SCOPED_PR_ISSUER
    from mimir.models import AgentEvent
    from mimir.plugins.core import policy_scope
    from tests.test_agent import _build_agent, _FakeAgent, _FakeSessionManager
    from tests.test_budget_gate_and_alias import _make_request
    from tests.test_repo_tools import _repo_scope_and_state
    from mimir_cultivate import forge, repository
    from mimir.tools.budget_gate import BudgetGateMiddleware
    from langchain_core.messages import ToolMessage
    import json

    _, _, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    state.revoke_checkout_lease(lease)
    if checkout is False:
        scope = replace(scope, event_type="pr_review",
                        allowed_operations=frozenset({RepoPRAction.PR_COMMENT.value}))
        state = RepoReviewState(scope)
    auth = _auth(scope)
    auth = replace(
        auth, repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state,
        trigger="poller", canonical_principal="poller:github-activity",
        service_authority=replace(auth.service_authority, canonical="poller:github-activity",
                                  trigger="poller", authority_profile="github",
                                  capabilities=("pr_metadata", "repo_checkout")),
        scoped_pr_grants=ScopedPRGrants(
            "poller:github-activity", (scope,), protocols=selected_repo_policy.scoped_actions,
            _issuer=_SCOPED_PR_ISSUER,
        ),
    )
    if checkout == "invalid_grant":
        auth = replace(auth, canonical_principal="different-service")
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr("mimir.agent._create_turn_auth_context", lambda *a, **k: auth)
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    fetches = []

    def checkout_once(*args, **kwargs):
        fetches.append(True)
        if checkout == "connect_error":
            import httpx
            raise httpx.ConnectError("temporary connection failure")
        if checkout == "git_transport":
            raise repository.GitTransportError(
                "fatal: unable to access 'https://github.com/o/r/': Could not resolve host: github.com"
            )
        state.attach_checkout_lease(lease)
        return lease, ("candidate",)

    monkeypatch.setattr(repository, "acquire_pr_checkout_lease", checkout_once)
    if checkout is True:
        monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                            lambda *_args: SimpleNamespace(ok=True))
    seen = []
    checkout_results = []

    class FirstRead(_FakeAgent):
        async def astream(self, inputs, *, config, context=None, stream_mode="values"):
            decision = ToolRegistry().authorize_tool(
                "pr_metadata", context, enforce=True,
                arguments={"repository": scope.canonical_repo, "pull_request": scope.pr_number},
                policy=selected_repo_policy,
            )
            seen.append((decision, state.checkout_lease, str(inputs)))
            if checkout is True:
                monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                                    lambda *_args: SimpleNamespace(ok=True))
                args = {"repository": scope.canonical_repo, "pull_request": scope.pr_number}

                async def execute(request):
                    value = repo_checkout.func(**args, runtime=request.runtime)
                    return ToolMessage(content=json.dumps(value), tool_call_id=request.tool_call["id"])

                checkout_results.append(await BudgetGateMiddleware(selected_repo_policy).awrap_tool_call(
                    _make_request("repo_checkout", "reuse-checkout", context, args), execute,
                ))
            yield {"messages": []}

    agent = _build_agent(tmp_path, fake_agent=FirstRead([]), fake_saga=None,
                         session_manager=_FakeSessionManager())
    agent._config = replace(agent._config, plugin_manager=SimpleNamespace(
        policy=selected_repo_policy, plugins=(), turn_preflight=None,
    ))
    event = AgentEvent(trigger="poller", channel_id="poller:github-activity", content="review")
    with policy_scope(selected_repo_policy):
        if checkout in {"connect_error", "git_transport", "invalid_grant"}:
            message = {"connect_error": "temporary connection failure",
                       "git_transport": "Could not resolve host",
                       "invalid_grant": "scoped PR grant refused"}[checkout]
            with pytest.raises(Exception, match=message):
                await agent.run_turn(event)
        else:
            await agent.run_turn(event)
    assert len(fetches) == int(checkout not in {False, "invalid_grant"})
    if checkout == "invalid_grant":
        assert not seen and state.checkout_lease is None
        return
    if checkout in {"connect_error", "git_transport"}:
        import json
        events = [json.loads(line) for line in (
            tmp_path / "home" / "logs" / "events.jsonl"
        ).read_text().splitlines()]
        failed = [entry for entry in events if entry.get("type") == "turn_failed"]
        assert len(failed) == 1
        assert failed[0]["phase"] == "pre_model_loop"
        assert failed[0]["tool_call_count"] == 0
        assert failed[0]["attempt_disposition"] == "transient_provider"
        assert not seen
        return
    events = [json.loads(line) for line in (
        tmp_path / "home" / "logs" / "events.jsonl"
    ).read_text().splitlines()]
    terminal = [entry for entry in events if entry.get("type") in {"turn_failed", "turn_completed"}]
    assert len(terminal) == 1
    assert terminal[0]["type"] == "turn_completed"
    assert len(checkout_results) == int(checkout is True)
    if checkout is True:
        # Assert outside astream: run_turn catches exceptions from the model.
        # #39 keeps the active-ingest write veto mandatory even for framework
        # checkouts. The binding still permits the PR read.
        assert checkout_results[0].status == "error"
        assert "untrusted" in checkout_results[0].content
    assert len(seen) == 1
    if checkout:
        assert seen[0][0].allowed, seen[0][0]
        assert seen[0][1] is lease
        assert str(lease.path) in seen[0][2]
        monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                            lambda *_args: SimpleNamespace(ok=True))
        first = repo_checkout.func(scope.canonical_repo, scope.pr_number,
                                   runtime=SimpleNamespace(context=auth))
        second = repo_checkout.func(scope.canonical_repo, scope.pr_number,
                                    runtime=SimpleNamespace(context=auth))
        assert first == second == {
            "status": "resumed", "path": str(lease.path), "scope_id": lease.scope_id,
            "head_sha": lease.head_sha, "candidate_commits": ("candidate",),
        }
        assert len(fetches) == 1
        with pytest.raises(ToolException, match="existing lease attestation refused"):
            monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                                lambda *_args: SimpleNamespace(ok=False, reason="ancestry_failure"))
            repo_checkout.func(scope.canonical_repo, scope.pr_number,
                               runtime=SimpleNamespace(context=auth))
        assert not auth.ifc_state.framework_pr_checkouts
        assert state.checkout_lease is None
        assert len(fetches) == 1
    else:
        assert state.checkout_lease is None


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["stopped", "reminted", "refused", "post_refused", "post_ok_refusal", "invalid_reason", "ok_refusal"])
async def test_poller_precheckout_handles_stopped_and_reminted_preflight(
    tmp_path, monkeypatch, selected_repo_policy, outcome,
):
    from mimir.access_control import ScopedPRGrants, _SCOPED_PR_ISSUER
    from mimir.models import AgentEvent
    from mimir.plugins.core import policy_scope
    from tests.test_agent import _build_agent, _FakeAgent, _FakeSessionManager
    from tests.test_repo_tools import _repo_scope_and_state
    from mimir_cultivate import forge, repository
    from mimir_cultivate.policy import ServerDiscoveredPRStates
    from mimir_cultivate.repository import create_pr_checkout_lease
    import json

    _, _, scope, state = _repo_scope_and_state(tmp_path)
    lease = state.checkout_lease
    state.revoke_checkout_lease(lease)
    cache = ServerDiscoveredPRStates()
    fresh = RepoReviewState(replace(scope, provenance="server_discovered"))
    assert fresh.action_scope.scope_id != scope.scope_id
    auth = _auth(scope)
    auth = replace(
        auth, repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state,
        server_discovered_pr_states=cache, _plugin_slot_policy=selected_repo_policy,
        trigger="poller", canonical_principal="poller:github-activity",
        service_authority=replace(auth.service_authority, canonical="poller:github-activity",
                                  trigger="poller", authority_profile="github",
                                  capabilities=("pr_metadata", "repo_checkout")),
        scoped_pr_grants=ScopedPRGrants(
            "poller:github-activity", (scope,), protocols=selected_repo_policy.scoped_actions,
            _issuer=_SCOPED_PR_ISSUER,
        ),
    )
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr("mimir.agent._create_turn_auth_context", lambda *a, **k: auth)

    def preflight(*_args):
        if outcome == "stopped":
            return None, "pull request is closed or merged"
        # A moved head re-mints a fresh server scope; the lease attaches there.
        return cache.remember(fresh), None

    monkeypatch.setattr(forge, "remediation_checkout_preflight", preflight)
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    issued = []

    def acquire(target, **kwargs):
        candidate = create_pr_checkout_lease(
            target, owner=target.principal, lease_root=lease.lease_root,
            review_state=kwargs["review_state"],
        )
        issued.append(candidate)
        return candidate, ("candidate",)

    monkeypatch.setattr(repository, "acquire_pr_checkout_lease", acquire)
    if outcome in {"reminted", "post_refused", "post_ok_refusal"}:
        monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                            lambda *_args: SimpleNamespace(
                                ok=outcome == "reminted",
                                reason="ok" if outcome == "post_ok_refusal" else "ancestry_failure"))
    if outcome in {"refused", "invalid_reason", "ok_refusal"}:
        def refused_hook(*args):
            cache.remember(fresh)
            candidate = create_pr_checkout_lease(
                fresh.action_scope, owner=scope.principal, lease_root=lease.lease_root,
            )
            issued.append(candidate)
            fresh.attach_checkout_lease(candidate)
            reason = {"refused": "ancestry_failure", "invalid_reason": "adapter-secret",
                      "ok_refusal": "ok"}[outcome]
            return {"status": "attestation_refused", "reason": reason}

        monkeypatch.setitem(selected_repo_policy._turn_hooks, ("cultivate", "pr_checkout"), refused_hook)
    seen = []

    class FirstRead(_FakeAgent):
        async def astream(self, inputs, *, config, context=None, stream_mode="values"):
            decision = ToolRegistry().authorize_tool(
                "pr_metadata", context, enforce=True,
                arguments={"repository": scope.canonical_repo, "pull_request": scope.pr_number},
                policy=selected_repo_policy,
            )
            seen.append((decision, str(inputs)))
            yield {"messages": []}

    agent = _build_agent(tmp_path, fake_agent=FirstRead([]), fake_saga=None,
                         session_manager=_FakeSessionManager())
    agent._config = replace(agent._config, plugin_manager=SimpleNamespace(
        policy=selected_repo_policy, plugins=(), turn_preflight=None,
    ))
    with policy_scope(selected_repo_policy):
        if outcome in {"invalid_reason", "ok_refusal", "post_ok_refusal"}:
            expected = ValueError if outcome == "invalid_reason" else RuntimeError
            message = "adapter-secret" if outcome == "invalid_reason" else "invalid attestation refusal"
            with pytest.raises(expected, match=message):
                await agent.run_turn(AgentEvent(
                    trigger="poller", channel_id="poller:github-activity", content="review",
                ))
            assert not seen
            return
        await agent.run_turn(AgentEvent(
            trigger="poller", channel_id="poller:github-activity", content="review",
        ))
    events = [json.loads(line) for line in (
        tmp_path / "home" / "logs" / "events.jsonl"
    ).read_text().splitlines()]
    assert not [entry for entry in events if entry.get("type") == "turn_failed"]
    assert len(seen) == 1
    if outcome == "stopped":
        assert "found nothing to remediate" in seen[0][1]
        assert "pull request is closed or merged" in seen[0][1]
        assert state.checkout_lease is None and fresh.checkout_lease is None
        return
    if outcome in {"refused", "post_refused"}:
        assert len(issued) == 1 and issued[0].revoked
        assert fresh.checkout_lease is None and state.checkout_lease is None
        assert not auth.ifc_state.framework_pr_checkouts
        # Re-issuing through the normal authorization path cannot undo refusal.
        auth.scoped_pr_grants.issue(fresh.action_scope)
        protocol = selected_repo_policy.scoped_actions["cultivate"]
        assert not access_control._valid_scoped_pr_grant(
            auth, "cultivate", protocol.checkout_action, protocol,
            fresh.action_scope, fresh, tool_name="repo_checkout",
        )
        assert not seen[0][0].allowed
        return
    assert fresh.checkout_lease is not None and state.checkout_lease is None
    assert seen[0][0].allowed, seen[0][0]
    record = auth.ifc_state.framework_pr_checkouts[fresh.action_scope.scope_id]
    assert record[0] == (str(fresh.checkout_lease.path), fresh.checkout_lease.recovery_id)
    assert str(fresh.checkout_lease.path) in seen[0][1]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["ancestry_failure", "no_author_trust", "checkout_state_mismatch", "git_timeout"])
@pytest.mark.parametrize("followup", ["none", "pr_comment", "unsupported_operation", "operator_alert", "repo_push", "model_error", "hard_refusal_model_error"])
async def test_poller_precheckout_refused_resume_is_no_work_without_renewal(
    tmp_path, monkeypatch, selected_repo_policy, reason, followup,
):
    import json

    from mimir.access_control import ScopedPRGrants, _SCOPED_PR_ISSUER
    from mimir.models import AgentEvent
    from mimir.plugins.core import policy_scope
    from mimir_cultivate import forge
    from tests.test_agent import _build_agent, _FakeAgent, _FakeSessionManager
    from tests.test_repo_tools import _repo_scope_and_state

    _, _, scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    state = RepoReviewState(scope)
    auth = replace(
        _auth(scope), repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state,
        trigger="poller", canonical_principal="poller:github-activity",
        service_authority=replace(_auth(scope).service_authority,
            canonical="poller:github-activity", trigger="poller", authority_profile="github",
            capabilities=("pr_metadata", "repo_checkout", "pr_comment")),
        scoped_pr_grants=ScopedPRGrants(
            "poller:github-activity", (scope,), protocols=selected_repo_policy.scoped_actions,
            _issuer=_SCOPED_PR_ISSUER,
        ),
    )
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr("mimir.agent._create_turn_auth_context", lambda *a, **k: auth)
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                        lambda *_: SimpleNamespace(ok=False, reason=reason,
                                                  detail="fatal: adapter-secret"))
    original_hook = selected_repo_policy.turn_hooks[("cultivate", "pr_checkout")]

    def hook(*args):
        result = original_hook(*args)
        if result["status"] == "attestation_refused":
            result["message"] = "fatal: adapter-secret"
        return result

    monkeypatch.setitem(selected_repo_policy._turn_hooks, ("cultivate", "pr_checkout"), hook)
    metadata = lease.path / ".git/mimir-pr-checkout-lease.json"
    before = json.loads(metadata.read_text())
    seen = []
    observed_refusals = []

    class FirstRead(_FakeAgent):
        async def astream(self, inputs, *, config, context=None, stream_mode="values"):
            seen.append(str(inputs))
            from mimir.tools.budget_gate import _get_current_turn_context, _record_tool_outcome

            observed_refusals.append(tuple(_get_current_turn_context().prechecked_pr_refusals))
            if followup == "hard_refusal_model_error":
                _get_current_turn_context().hard_boundary_denials.append({
                    "boundary": "write", "reason": "write_blocked_by_untrusted_ingest",
                })
            if followup in {"model_error", "hard_refusal_model_error"}:
                raise RuntimeError("unrelated model failure")
            if followup != "none":
                if followup == "operator_alert":
                    _get_current_turn_context().remediation_effects.append(followup)
                else:
                    _record_tool_outcome(followup, policy=selected_repo_policy)
            yield {"messages": []}

    agent = _build_agent(tmp_path, fake_agent=FirstRead([]), fake_saga=None,
                         session_manager=_FakeSessionManager())
    agent._config = replace(agent._config, plugin_manager=SimpleNamespace(
        policy=selected_repo_policy, plugins=(), turn_preflight=None,
    ))
    with policy_scope(selected_repo_policy):
        await agent.run_turn(AgentEvent(
            trigger="poller", channel_id="poller:github-activity", content="review",
        ))

    events = [json.loads(line) for line in (tmp_path / "home/logs/events.jsonl").read_text().splitlines()]
    terminal = [event for event in events if event.get("type") in {"turn_failed", "turn_completed"}]
    assert observed_refusals == [({
        "repository": scope.canonical_repo, "pull_request": scope.pr_number, "reason": reason,
    },)]
    assert len(terminal) == 1
    assert terminal[0]["type"] == ("turn_failed" if followup in {"model_error", "hard_refusal_model_error"} else "turn_completed")
    charged = followup in {"repo_push", "model_error", "hard_refusal_model_error"}
    if followup == "hard_refusal_model_error":
        assert terminal[0]["remediation_effects"] == []
        assert {item["boundary"] for item in terminal[0]["hard_refusals"]} == {"write", "pr_checkout"}
    assert terminal[0]["attempt_disposition"] == ("charge" if charged else "exempt_hard_refusal")
    if not charged:
        expected_reason = f"lease_attestation_refused:{reason}"
        if followup == "unsupported_operation":
            expected_reason += ", unsupported_operation"
        assert terminal[0]["attempt_reason"] == expected_reason
    if followup == "pr_comment":
        assert terminal[0]["remediation_effects"] == ["pr_comment"]
    diagnostics = [event for event in events if event.get("type") == "pr_checkout_lease_attestation_refused"]
    assert diagnostics and diagnostics[0]["reason"] == reason
    if reason == "no_author_trust":
        assert diagnostics[0]["cause"] == "pr_identity_reader_missing"
    assert len(seen) == 1
    assert "Current PR remediation" in seen[0]
    assert reason in seen[0]
    assert "adapter-secret" not in seen[0]
    assert "may comment on the PR or escalate" in seen[0]
    assert "cannot read or write PR content this turn" in seen[0]
    assert str(lease.path) not in seen[0]
    assert not auth.ifc_state.framework_pr_checkouts
    assert state.checkout_lease is None
    assert json.loads(metadata.read_text())["expires_at"] == before["expires_at"]
    assert json.loads(metadata.read_text())["recovery_id"] == before["recovery_id"]


def test_tool_refused_resume_keeps_expiry_and_reports_reason(
    tmp_path, monkeypatch,
):
    import json

    from mimir.access_control import LeaseAttestation
    from mimir_cultivate import forge
    from tests.test_repo_tools import _repo_scope_and_state
    from mimir.tools.refusals import ToolPolicyRefusal

    _, _, scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    state = RepoReviewState(scope)
    state.attach_checkout_lease(lease)
    auth = replace(_auth(scope), repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state)
    auth.ifc_state.framework_pr_checkouts[scope.scope_id] = (
        (str(lease.path), lease.recovery_id), (),
    )
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                        lambda *_: LeaseAttestation(False, "ancestry_failure"))
    metadata = lease.path / ".git/mimir-pr-checkout-lease.json"
    before = json.loads(metadata.read_text())
    with pytest.raises(ToolPolicyRefusal, match="existing lease attestation refused \\(ancestry_failure\\)"):
        repo_checkout.func("owner/repo", 7, runtime=SimpleNamespace(context=auth))
    assert json.loads(metadata.read_text()) == before
    assert lease.revoked
    assert state.checkout_lease is None
    assert not auth.ifc_state.framework_pr_checkouts


def test_tool_attested_resume_renews_lease(tmp_path, monkeypatch):
    import json

    from mimir.access_control import LeaseAttestation
    from mimir_cultivate import forge
    from tests.test_repo_tools import _repo_scope_and_state

    _, _, scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    state = RepoReviewState(scope)
    auth = replace(_auth(scope), repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    monkeypatch.setattr(access_control, "pr_checkout_lease_attestation",
                        lambda *_: LeaseAttestation(True, "ok"))
    metadata = lease.path / ".git/mimir-pr-checkout-lease.json"
    before = json.loads(metadata.read_text())
    result = repo_checkout.func("owner/repo", 7, runtime=SimpleNamespace(context=auth))
    after = json.loads(metadata.read_text())
    assert result["status"] == "checked_out"
    assert result["path"] == str(lease.path)
    assert after["expires_at"] > before["expires_at"]
    assert state.checkout_lease.expires_at.isoformat() == after["expires_at"]


@pytest.mark.parametrize("foreign", [False, True])
@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("pre_turn", [False, True])
def test_checkout_resume_attests_before_any_persistence(
    tmp_path, monkeypatch, selected_repo_policy, foreign, expired, accepted, pre_turn,
):
    import json
    from datetime import UTC, datetime, timedelta
    from mimir.access_control import LeaseAttestation
    from mimir_cultivate import forge, repository
    from tests.test_repo_tools import _repo_scope_and_state

    _, _, old_scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    metadata = lease.path / repository._METADATA
    raw = json.loads(metadata.read_text())
    if expired:
        raw["expires_at"] = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        metadata.write_text(json.dumps(raw))
    before = metadata.read_bytes()
    scope = replace(old_scope, event_type="pr_ci_failure") if foreign else old_scope
    state = RepoReviewState(scope)
    auth = replace(_auth(scope), repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    monkeypatch.setattr(repository, "_observe_current_pr_head", lambda _: scope.observed_head_sha)
    observed = []

    def attest(context, target, candidate):
        assert metadata.read_bytes() == before
        assert candidate.is_active
        assert candidate.scope_id == scope.scope_id
        assert state.checkout_lease is candidate
        observed.append(candidate)
        return LeaseAttestation(accepted, "ok" if accepted else "no_author_trust")

    monkeypatch.setattr(access_control, "pr_checkout_lease_attestation", attest)
    runtime = SimpleNamespace(context=auth, pre_turn_checkout=pre_turn)
    if not accepted and not pre_turn:
        with pytest.raises(repository.LeaseAttestationRefusal, match="no_author_trust"):
            repository.checkout_scoped_pr(scope.canonical_repo, scope.pr_number, runtime)
    else:
        result = repository.checkout_scoped_pr(scope.canonical_repo, scope.pr_number, runtime)
        assert result["status"] == ("checked_out" if accepted else "attestation_refused")
    assert observed
    if accepted:
        after = json.loads(metadata.read_text())
        assert after["expires_at"] > raw["expires_at"]
        assert after["recovery_id"] != raw["recovery_id"]
        assert after["scope_id"] == scope.scope_id
        assert state.checkout_lease.is_active
    else:
        assert metadata.read_bytes() == before
        assert state.checkout_lease is None
        assert all(candidate.revoked for candidate in observed)
        assert not auth.ifc_state.framework_pr_checkouts
    assert access_control._resume_metadata_identity.get() is None


@pytest.mark.parametrize("foreign,failure", [(False, "write"), (True, "write"), (True, "replace")])
@pytest.mark.parametrize("pre_turn", [False, True])
def test_resume_persistence_failure_revokes_tentative_authority(
    tmp_path, monkeypatch, selected_repo_policy, foreign, failure, pre_turn,
):
    from mimir.access_control import LeaseAttestation
    from mimir_cultivate import forge, repository
    from tests.test_repo_tools import _repo_scope_and_state

    _, _, old_scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    metadata = lease.path / repository._METADATA
    before = metadata.read_bytes()
    scope = replace(old_scope, event_type="pr_ci_failure") if foreign else old_scope
    state = RepoReviewState(scope)
    auth = replace(_auth(scope), repo_pr_scope_registry=RepoPRScopeRegistry((state,)), repo_review_state=state)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(lease.lease_root))
    monkeypatch.setattr(forge, "remediation_checkout_preflight", lambda *_: (state, None))
    monkeypatch.setattr(forge, "_client", lambda *_: SimpleNamespace())
    monkeypatch.setattr(repository, "_observe_current_pr_head", lambda _: scope.observed_head_sha)
    candidates = []
    published = []
    failures = []

    def attest(context, target, candidate):
        assert state.checkout_lease is candidate
        assert candidate.is_active
        assert metadata.read_bytes() == before
        candidates.append(candidate)
        return LeaseAttestation(True, "ok")

    def fail_persistence():
        failures.append(True)
        assert candidates  # The failure must occur after successful attestation.
        raise OSError("injected metadata persistence failure")

    original_write = Path.write_text
    original_replace = repository.os.replace

    def write(path, *args, **kwargs):
        if path.parent == metadata.parent and (path == metadata or path.name.endswith(".tmp")):
            fail_persistence()
        return original_write(path, *args, **kwargs)

    def replace_metadata(src, dst):
        if Path(dst) == metadata:
            fail_persistence()
        return original_replace(src, dst)

    monkeypatch.setattr(access_control, "pr_checkout_lease_attestation", attest)
    monkeypatch.setattr(repository, "_publish_attested_lease_result", lambda *_: published.append(True))
    if failure == "write":
        monkeypatch.setattr(Path, "write_text", write)
    else:
        monkeypatch.setattr(repository.os, "replace", replace_metadata)
    runtime = SimpleNamespace(context=auth, pre_turn_checkout=pre_turn)
    with pytest.raises(ToolException, match="injected metadata persistence failure"):
        repository.checkout_scoped_pr(scope.canonical_repo, scope.pr_number, runtime)
    assert failures == [True]
    assert len(candidates) == 1
    assert candidates[0].revoked and not candidates[0].is_active
    assert state.checkout_lease is None and not state.checked_out
    assert state.git_expected_head is None
    assert not auth.ifc_state.framework_pr_checkouts
    assert published == []
    assert metadata.read_bytes() == before
    assert not list(metadata.parent.glob(f".{metadata.name}.*.tmp"))
    assert access_control._resume_metadata_identity.get() is None


def test_attestation_refusal_exception_rejects_ok():
    from mimir.access_control import LeaseAttestationReason
    from mimir_cultivate.repository import LeaseAttestationRefusal

    for reason in ("ok", LeaseAttestationReason.OK):
        with pytest.raises(ValueError, match="invalid attestation refusal"):
            LeaseAttestationRefusal(reason)
    assert LeaseAttestationRefusal("ancestry_failure").reason == "ancestry_failure"


@pytest.mark.parametrize("raises", [False, True])
def test_resume_metadata_context_is_exact_scoped_and_always_reset(
    tmp_path, monkeypatch, selected_repo_policy, raises,
):
    import json
    from subprocess import CompletedProcess
    from mimir_cultivate import repository

    head, published, base = "a" * 40, "b" * 40, "c" * 40
    prior = SimpleNamespace(scope_id="prior", canonical_repo="owner/repo", pr_number=7,
                            head_sha=head, base_sha=base)
    candidate = SimpleNamespace(**{**vars(prior), "scope_id": "candidate"})
    other = SimpleNamespace(**vars(candidate))
    raw = {**vars(prior), "lineage": {head: True}}
    metadata = tmp_path / repository._METADATA
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(json.dumps(raw))
    hooks = selected_repo_policy.policy_consumer_hooks
    monkeypatch.setattr(selected_repo_policy, "_policy_consumer_hooks", replace(
        hooks, git_command=lambda *_args, **_kwargs:
        CompletedProcess([], 0, head + "\n", ""),
    ))
    scope = SimpleNamespace(observed_base_sha=base)

    def check(lease):
        return access_control._lease_has_clean_lineage(tmp_path, lease, scope, published, head)

    assert not check(candidate)
    try:
        with access_control.pr_checkout_resume_metadata(candidate, prior):
            assert check(candidate)
            assert not check(other)  # Equal values do not carry transactional authority.
            metadata.write_text(json.dumps({**raw, "scope_id": "tampered"}))
            assert not check(candidate)  # Context does not replace the on-disk proof.
            metadata.write_text(json.dumps(raw))
            if raises:
                raise RuntimeError("attestation interrupted")
    except RuntimeError:
        assert raises
    assert access_control._resume_metadata_identity.get() is None
    assert not check(candidate)


@pytest.mark.parametrize("foreign", [False, True])
def test_recovery_divergence_is_typed_no_work_before_renewal(
    tmp_path, monkeypatch, selected_repo_policy, foreign,
):
    from mimir_cultivate import repository
    from tests.test_repo_tools import _repo_scope_and_state
    from subprocess import CompletedProcess

    _, _, scope, original = _repo_scope_and_state(tmp_path)
    lease = original.checkout_lease
    metadata = lease.path / repository._METADATA
    before = metadata.read_bytes()
    if foreign:
        scope = replace(scope, event_type="pr_ci_failure")
        assert scope.scope_id != lease.scope_id
    state = RepoReviewState(scope)

    def runner(argv):
        if (argv[3:5] == ["merge-base", "--is-ancestor"]
                and argv[5:] == [scope.observed_head_sha, lease.head_sha]):
            return CompletedProcess(argv, 1, "", "")
        return repository._default_runner(argv)

    attest = lambda _: pytest.fail("divergence reached renewal attestation")
    with pytest.raises(repository.LeaseAttestationRefusal, match="ancestry_failure"):
        if foreign:
            from datetime import timedelta
            repository._rebind_foreign_candidate(
                lease, scope, head=lease.head_sha, ttl=timedelta(hours=2),
                review_state=state, runner=runner, attest_resume=attest,
            )
        else:
            repository._recover_pr_checkout_lease_unlocked(
                lease.path, scope, owner=scope.principal, lease_root=lease.lease_root,
                renew_expired=True, review_state=state, runner=runner,
                attest_resume=attest,
            )
    assert metadata.read_bytes() == before
    assert state.checkout_lease is None


@pytest.mark.parametrize("reason", ["ancestry_failure", "adapter-secret", "ok"])
def test_refusal_prompt_formatter_accepts_only_enum_reason(reason):
    from mimir.models import AgentEvent
    from mimir.prompts import build_turn_prompt

    refusal = {"repository": "owner/repo", "pull_request": 7,
               "reason": reason, "message": "adapter-secret", "detail": "adapter-secret"}
    event = AgentEvent(trigger="poller", channel_id="poller:github-activity", content="review")
    if reason != "ancestry_failure":
        with pytest.raises(ValueError):
            build_turn_prompt(event, prechecked_pr_refusals=[refusal])
    else:
        rendered = build_turn_prompt(event, prechecked_pr_refusals=[refusal])
        assert "ancestry_failure" in rendered
        assert "adapter-secret" not in rendered


def test_core_only_head_advance_cannot_dispatch_without_owner(monkeypatch):
    from mimir.plugins.core import core_policy_registry

    client = _RemediationForge(_snapshot("c" * 40))
    old_scope, context, _runtime = _remediation_runtime(monkeypatch, client)
    decision = ToolRegistry().authorize_tool(
        "repo_checkout", context, enforce=True,
        arguments={"repository": "owner/repo", "pull_request": 7},
        policy=core_policy_registry(),
    )
    assert not decision.allowed
    assert decision.reason == "unknown_sink_category"
    assert old_scope.observed_head_sha == "a" * 40
    assert client.review_calls == 0
    assert context.ifc_state.own_push_lineage() == {}


def test_stale_own_remediation_head_is_reminted_and_checked_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected_repo_policy,
) -> None:
    fresh_head = "c" * 40
    old_scope, context, runtime = _remediation_runtime(
        monkeypatch, _RemediationForge(_snapshot(fresh_head)),
    )
    monkeypatch.setattr(selected_repo_policy, "_policy_consumer_hooks", replace(
        selected_repo_policy.policy_consumer_hooks, agent_push=lambda *_args: True,
    ))
    checked_out: list[RepoPRActionScope] = []

    def fake_checkout(scope, *, owner, review_state, attest_resume=None):  # type: ignore[no-untyped-def]
        checked_out.append(scope)
        lease = SimpleNamespace(
            path=tmp_path / "fresh", scope_id=scope.scope_id,
            head_sha=scope.observed_head_sha, owner=owner, is_active=True,
        )
        review_state.attach_checkout_lease(lease)
        return lease, ()

    monkeypatch.setattr("mimir_cultivate.repository.acquire_pr_checkout_lease", fake_checkout)

    result = repo_checkout.func(
        repository="owner/repo", pull_request=7, runtime=runtime,
    )

    assert result["head_sha"] == fresh_head
    assert checked_out[0].provenance.value == "server_discovered"
    assert context.repo_pr_scope_registry.resolve("owner/repo", 7).action_scope is old_scope
    assert context.server_discovered_pr_states.resolve(
        "owner/repo", 7,
    ).action_scope is checked_out[0]


def test_merged_remediation_pr_stops_before_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old_scope, _context, runtime = _remediation_runtime(
        monkeypatch, _RemediationForge(_snapshot("c" * 40, state="closed")),
    )
    monkeypatch.setattr(
        "mimir_cultivate.repository.acquire_pr_checkout_lease",
        lambda *_args, **_kwargs: pytest.fail("closed PR reached checkout"),
    )

    result = repo_checkout.func(
        repository="owner/repo", pull_request=7, runtime=runtime,
    )

    assert result["status"] == "stopped"
    assert result["message"] == "pull request is closed or merged"
    assert "without an operator alert" in result["guidance"]


def test_remediation_head_advancing_twice_does_not_remint_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected_repo_policy,
) -> None:
    second_head = "c" * 40
    third_head = "d" * 40
    client = _RemediationForge(_snapshot(second_head), _snapshot(third_head))
    old_scope, context, runtime = _remediation_runtime(monkeypatch, client)
    monkeypatch.setattr(selected_repo_policy, "_policy_consumer_hooks", replace(
        selected_repo_policy.policy_consumer_hooks, agent_push=lambda *_args: True,
    ))
    reminted_heads: list[str] = []

    def mint(repository, snapshot, *, event_type):  # type: ignore[no-untyped-def]
        reminted_heads.append(snapshot.head_sha)
        return replace(
            old_scope,
            provenance="server_discovered",
            observed_head_sha=snapshot.head_sha,
        )

    monkeypatch.setattr(
        "mimir.access_control.create_server_discovered_heartbeat_scope", mint,
    )
    live_heads = iter((second_head, third_head))
    checked_out_heads: list[str] = []

    def fake_checkout(scope, *, owner, review_state, attest_resume=None):  # type: ignore[no-untyped-def]
        live_head = next(live_heads)
        checked_out_heads.append(scope.observed_head_sha)
        if scope.observed_head_sha != live_head:
            raise RuntimeError(
                f"PR head advanced: scoped head {scope.observed_head_sha} is stale; "
                f"fetched head is {live_head}; fatal: unable to access "
                "'https://x-access-token:synthetic-secret@github.com/o/r/': rate limited"
            )
        lease = SimpleNamespace(
            path=tmp_path / "fresh", scope_id=scope.scope_id,
            head_sha=scope.observed_head_sha, owner=owner, is_active=True,
        )
        review_state.attach_checkout_lease(lease)
        return lease, ()

    monkeypatch.setattr("mimir_cultivate.repository.acquire_pr_checkout_lease", fake_checkout)
    repo_checkout.func(repository="owner/repo", pull_request=7, runtime=runtime)

    with pytest.raises(ToolException) as raised:
        repo_checkout.func(repository="owner/repo", pull_request=7, runtime=runtime)

    assert str(raised.value) == (
        f"repository checkout rejected: PR head advanced: scoped head {second_head} "
        f"is stale; fetched head is {third_head}; fatal: unable to access "
        "'https://[REDACTED]@github.com/o/r/': rate limited"
    )
    assert client.snapshot_calls == 2
    assert reminted_heads == [second_head]
    assert checked_out_heads == [second_head, second_head]
    assert context.server_discovered_pr_states.resolve(
        "owner/repo", 7,
    ).action_scope.observed_head_sha == second_head


def test_failed_remint_preserves_existing_stale_head_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh_head = "c" * 40
    old_scope, _context, runtime = _remediation_runtime(
        monkeypatch, _RemediationForge(_snapshot(fresh_head)),
    )
    monkeypatch.setattr(
        "mimir.access_control.create_server_discovered_heartbeat_scope",
        lambda *_args, **_kwargs: None,
    )

    def stale_checkout(scope, **_kwargs):  # type: ignore[no-untyped-def]
        assert scope is old_scope
        raise RuntimeError(
            f"PR head advanced: scoped head {scope.observed_head_sha} is stale; "
            f"fetched head is {fresh_head}; fatal: unable to access "
            "'https://x-access-token:synthetic-secret@github.com/o/r/': rate limited"
        )

    monkeypatch.setattr("mimir_cultivate.repository.acquire_pr_checkout_lease", stale_checkout)

    with pytest.raises(ToolException) as raised:
        repo_checkout.func(repository="owner/repo", pull_request=7, runtime=runtime)

    assert str(raised.value) == (
        f"repository checkout rejected: PR head advanced: scoped head "
        f"{old_scope.observed_head_sha} is stale; fetched head is {fresh_head}; "
        "fatal: unable to access 'https://[REDACTED]@github.com/o/r/': rate limited"
    )


def test_repo_cleanup_redacts_git_url_userinfo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth = _auth(_scope(RepoPRAction.CHECKOUT))
    state = auth.repo_review_state
    assert state is not None
    state.attach_checkout_lease(SimpleNamespace(
        is_active=True,
        scope_id=state.action_scope.scope_id,
        owner=state.action_scope.principal,
        path="/tmp/synthetic-checkout",
    ))
    monkeypatch.setattr(
        "mimir_cultivate.repository.cleanup_pr_checkout_lease",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(
            "fatal: unable to access "
            "'https://x-access-token:synthetic-secret@github.com/o/r/': authentication failed"
        )),
    )

    with pytest.raises(ToolException) as raised:
        repo_cleanup.func(
            repository="owner/repo",
            pull_request=7,
            runtime=SimpleNamespace(context=auth),
        )

    assert str(raised.value) == (
        "repository cleanup rejected: fatal: unable to access "
        "'https://[REDACTED]@github.com/o/r/': authentication failed"
    )


def test_external_head_advance_rereads_review_state_before_checkout(
    monkeypatch: pytest.MonkeyPatch, selected_repo_policy,
) -> None:
    client = _RemediationForge(_snapshot("c" * 40))
    client.reviews = (
        ReviewProjection("2", "reviewer", "APPROVED", "ok", "later", "c" * 40),
    )
    _old_scope, _context, runtime = _remediation_runtime(monkeypatch, client)
    monkeypatch.setattr(selected_repo_policy, "_policy_consumer_hooks", replace(
        selected_repo_policy.policy_consumer_hooks, agent_push=lambda *_args: False,
    ))
    monkeypatch.setattr(
        "mimir_cultivate.repository.acquire_pr_checkout_lease",
        lambda *_args, **_kwargs: pytest.fail("cleared review reached checkout"),
    )

    result = repo_checkout.func(
        repository="owner/repo", pull_request=7, runtime=runtime,
    )

    assert result["status"] == "stopped"
    assert "no longer has a blocking" in result["message"]
    assert client.review_calls == 1


@pytest.mark.parametrize("tool_name", [
    "pr_diff", "pr_comments", "pr_metadata", "pr_reviews", "pr_submit_review",
])
@pytest.mark.parametrize("failed", [False, True])
def test_repo_source_label_is_exact_and_survives_to_bound_forge_sink(
    tool_name: str, failed: bool, selected_repo_policy,
) -> None:
    scope = _scope(RepoPRAction.INSPECT, RepoPRAction.PR_COMMENT)
    auth = _auth(scope)
    authorization = ToolAuthorization(
        tool_name=tool_name,
        decision="resource_scoped",
        allowed=True,
        enforcement_enabled=True,
        flow_direction=ToolFlowDirection.SOURCE,
        repo_pr_action_scope=scope,
    )

    added = classify_protected_result(tool_name, {}, auth, authorization, failed=failed)
    assert added is not None
    source = next(source for source in added.sources if source.domain == "repository")
    assert source.resource_id == f"<unresolved-resource:protected_tool:{tool_name}>"
    assert source.integrity == "untrusted"
    assert source.integrity_effect == "active_ingest"
    assert added.has_untrusted_active_ingest

    merged = auth.ifc_labels
    assert merged is not None
    for item in added.sources:
        merged = merged.with_source(item)
    target = f"owner/repo#pull/7@{'a' * 40}:{scope.scope_id}"
    allowed = SinkGate.check_sink_flow(
        "pr_comment", target, merged, auth, enforce=True,
        repo_pr_action_scope=scope,
    )
    assert not allowed.allowed
    assert allowed.reason == "ifc_label_blocked:forge"
    assert allowed.forge_scope_mismatch["component"] == "resource_id"


@pytest.mark.parametrize("tool_name", [
    "repo_status", "repo_test", "repo_checkout", "repo_push",
])
@pytest.mark.parametrize("failed", [False, True])
def test_unattested_selected_repository_results_never_claim_an_exact_source(
    tool_name: str, failed: bool, selected_repo_policy,
) -> None:
    from mimir.plugins.core import selected_policy_registry

    scope = _scope(RepoPRAction.INSPECT, RepoPRAction.PR_COMMENT)
    auth = _auth(scope)
    labels = classify_protected_result(
        tool_name, {"repository": scope.canonical_repo, "pull_request": scope.pr_number},
        auth, ToolAuthorization(
            tool_name=tool_name, decision="resource_scoped", allowed=True,
            repo_pr_action_scope=scope, policy=selected_policy_registry(),
        ), result="unattested tool output", failed=failed,
    )
    assert labels is not None and labels.has_untrusted_active_ingest
    assert labels.sources[0].resource_id == f"<unresolved-resource:protected_tool:{tool_name}>"
    target = f"{scope.canonical_repo}#pull/{scope.pr_number}@{scope.observed_head_sha}:{scope.scope_id}"
    decision = SinkGate.check_sink_flow(
        "pr_comment", target, labels, auth, enforce=True,
        repo_pr_action_scope=scope,
    )
    assert not decision.allowed
    assert decision.reason == "ifc_label_blocked:forge"
    assert decision.forge_scope_mismatch["component"] == "resource_id"


def test_selected_repo_status_raw_lease_evidence_matches_host_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected_repo_policy,
) -> None:
    from langgraph.runtime import Runtime
    from mimir.plugins.core import selected_policy_registry
    from mimir_cultivate import repository
    from tests.test_repo_tools import _lineage_turn, _repo_scope_and_state

    _origin, _source, scope, state = _repo_scope_and_state(tmp_path)
    monkeypatch.setenv("MIMIR_PR_CHECKOUT_LEASE_ROOT", str(state.checkout_lease.lease_root))
    monkeypatch.setattr(
        access_control, "_github_repo_from_remote",
        lambda remote: scope.canonical_repo if remote == scope.canonical_origin else None,
    )
    auth = replace(
        _lineage_turn(scope, state), repo_pr_scope_registry=RepoPRScopeRegistry((state,)),
    )
    auth.ifc_state.pr_checkout_author_trust[scope.scope_id] = True
    capture = access_control.begin_protected_result_capture()
    try:
        result = repository.repo_status.func(
            scope.canonical_repo, scope.pr_number, runtime=Runtime(context=auth),
        )
    finally:
        provenance = access_control.end_protected_result_capture(capture)

    assert provenance is not None and len(provenance.sources) == 1
    source = provenance.sources[0]
    assert source.resource_id == f"{scope.canonical_repo}#pull/{scope.pr_number}@{scope.observed_head_sha}"
    assert source.integrity == "trusted"
    labels = classify_protected_result(
        "repo_status", {"repository": scope.canonical_repo, "pull_request": scope.pr_number},
        auth, ToolAuthorization(
            tool_name="repo_status", decision="resource_scoped", allowed=True,
            repo_pr_action_scope=scope, policy=selected_policy_registry(),
        ), result=result, provenance=provenance,
    )
    assert labels is not None and not labels.has_untrusted_active_ingest
    assert labels.sources[0].resource_id == source.resource_id
    assert labels.sources[0].integrity == "trusted"


def test_mixed_repo_input_denies_bound_forge_sink(selected_repo_policy) -> None:
    scope = _scope(RepoPRAction.PR_COMMENT)
    auth = _auth(scope)
    labels = auth.ifc_labels
    assert labels is not None
    mixed = labels.with_source(SourceLabel(
        principal="service:other",
        domain="channel",
        resource_id="scheduler:other",
        bridge_instance="service:other",
        sensitivity="internal",
        authorized_principals=frozenset({"service:other"}),
    )).with_channel("scheduler:other")
    target = f"owner/repo#pull/7@{'a' * 40}:{scope.scope_id}"

    denied = SinkGate.check_sink_flow(
        "pr_comment", target, mixed, auth, enforce=True,
    )
    assert denied.allowed is False
    assert denied.reason == "ifc_label_blocked:forge"


@pytest.mark.asyncio
async def test_core_only_pr_scope_policy_refuses_in_both_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _scope(RepoPRAction.PR_COMMENT)
    registry = ToolRegistry()
    registry.enable_shadow_logging()
    captured: list[tuple[str, dict[str, object]]] = []

    async def capture(kind: str, **fields: object) -> None:
        captured.append((kind, fields))

    monkeypatch.setattr("mimir.event_logger.log_event", capture)
    arguments = {"repository": "owner/repo", "pull_request": 7}
    shadow = registry.authorize_tool(
        "pr_metadata",
        replace(_auth(scope), enforcement_enabled=False),
        enforce=False,
        arguments=arguments,
    )
    await asyncio.sleep(0)
    enforced = registry.authorize_tool(
        "pr_metadata",
        _auth(scope),
        enforce=True,
        arguments=arguments,
    )

    assert shadow.allowed
    assert shadow.would_block is True
    assert enforced.allowed is False
    assert shadow.reason == "unknown_operation"
    assert enforced.reason == "unknown_sink_category"
    events = [fields for kind, fields in captured if kind == "shadow_tool_decision"]
    assert len(events) == 1
    assert events[0]["would_block"] is True

    from langchain.agents.middleware import ToolCallRequest
    from langchain_core.messages import ToolMessage
    from langgraph.runtime import Runtime
    from mimir.plugins.core import core_policy_registry
    from mimir.tools.budget_gate import BudgetGateMiddleware

    for enforce in (False, True):
        called = []
        request = ToolCallRequest(
            tool_call={"name": "pr_metadata", "args": arguments, "id": "core-only",
                       "type": "tool_call"},
            tool=None, state={}, runtime=Runtime(context=replace(
                _auth(scope), enforcement_enabled=enforce,
            )),
        )

        async def handler(_request):
            called.append(True)
            return ToolMessage(content="executed", tool_call_id="core-only")

        result = await BudgetGateMiddleware(core_policy_registry()).awrap_tool_call(request, handler)
        assert result.status == "error"
        assert ("unknown_sink_category" if enforce else "unknown_operation") in str(result.content)
        assert not called


@pytest.mark.asyncio
async def test_selected_repo_grant_refuses_even_in_shadow_with_the_same_reason(
    monkeypatch: pytest.MonkeyPatch, selected_repo_policy,
) -> None:
    scope = _scope(RepoPRAction.INSPECT)
    registry = ToolRegistry()
    registry.enable_shadow_logging()
    captured: list[tuple[str, dict[str, object]]] = []

    async def capture(kind: str, **fields: object) -> None:
        captured.append((kind, fields))

    monkeypatch.setattr("mimir.event_logger.log_event", capture)
    arguments = {"repository": "owner/repo", "pull_request": 7}
    shadow_context = replace(
        _auth(scope),
        enforcement_enabled=False,
        ifc_labels=InformationFlowLabels(),
    )
    enforced_context = replace(_auth(scope), ifc_labels=InformationFlowLabels())
    shadow = registry.authorize_tool(
        "repo_stage",
        shadow_context,
        enforce=False,
        arguments=arguments,
    )
    await asyncio.sleep(0)
    enforced = registry.authorize_tool(
        "repo_stage", enforced_context, enforce=True, arguments=arguments,
    )

    assert shadow.allowed is False
    assert shadow.would_block is True
    assert enforced.allowed is False
    assert shadow.reason == enforced.reason == "repo_pr_scope_denied"
    events = [fields for kind, fields in captured if kind == "shadow_tool_decision"]
    assert len(events) == 1
    assert events[0]["tool"] == "repo_stage"
    assert events[0]["reason"] == enforced.reason
    assert events[0]["would_block"] is True


def test_tool_modules_do_not_decide_pr_scope_policy_locally() -> None:
    tools_root = Path(__file__).parents[1] / "mimir" / "tools"
    offenders = {
        path.relative_to(tools_root).as_posix()
        for path in tools_root.rglob("*.py")
        if "allowed_operations" in path.read_text(encoding="utf-8")
    }

    assert offenders == set()


def test_repo_tools_does_not_decide_pr_scope_policy_locally() -> None:
    import ast

    source = (Path(__file__).parents[1] / "plugins/cultivate/mimir_cultivate/repository.py").read_text(
        encoding="utf-8",
    )
    tree = ast.parse(source)
    git_tools = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "RepoGitTools"
    )
    require = next(
        node for node in git_tools.body
        if isinstance(node, ast.FunctionDef) and node.name == "_require"
    )
    policy_check = ast.get_source_segment(source, require)
    assert policy_check is not None
    assert "allowed_operations" not in policy_check
    assert "authorize_repo_pr_tool(" in policy_check
