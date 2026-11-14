from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from mimir.plugins.core import core_policy_registry, policy_scope
from mimir.plugins.manager import load_plugins
from mimir.plugins.registry import PolicyRegistrationError, PolicyRegistry
from mimir.readonly_backend import RetainedCheckoutFilesystemBackend
from mimir_cultivate import registration
from mimir_cultivate.worklink import retained_scope, worker_client


async def test_contained_session_uses_shared_outcome_and_verified_release():
    from unittest.mock import AsyncMock
    from mimir_cultivate.contained_execution import execute_contained_session
    from mimir_cultivate.worklink.backends.base import LaunchHandle, SessionOutcome, SessionRequest

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    client = SimpleNamespace(session_create=AsyncMock(return_value=handle),
                             session_observe=AsyncMock(return_value=SessionOutcome("ses_owned", "succeeded")),
                             session_release=AsyncMock())
    directory = SimpleNamespace(_contained_worker_client=client)
    request = SessionRequest("spawn", {"providerID": "openai", "id": "synthetic"}, "build")
    result = await execute_contained_session(directory, request, timeout_s=5)
    assert result.exit_code == 0
    assert result.session_outcome.opencode_session_id == "ses_owned"
    client.session_create.assert_awaited_once_with(request, timeout_s=5, pass_env={})
    client.session_release.assert_awaited_once_with(handle)


def _backend(monkeypatch: pytest.MonkeyPatch, checkout: Path):
    backend = RetainedCheckoutFilesystemBackend(checkout)
    scope = SimpleNamespace(sandbox=str(checkout), run_id="run-1")
    monkeypatch.setattr(backend, "_binding", lambda path: (
        object(), scope, checkout / path.lstrip("/"),
    ))
    return backend


def test_retained_dispatch_fails_closed_without_selected_owner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    backend = _backend(monkeypatch, tmp_path)
    with policy_scope(core_policy_registry()):
        assert backend.write("/file.txt", "content").error == (
            "retained factory checkout provider is unavailable"
        )


def test_retained_dispatch_uses_selected_owner_and_lease(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    manager = load_plugins(enable="cultivate")
    backend = _backend(monkeypatch, tmp_path)
    calls: list[tuple[object, ...]] = []
    monkeypatch.setenv("MIMIR_HOME", str(tmp_path))
    monkeypatch.setattr(worker_client, "factory_checkout_for_path", lambda path: (tmp_path, 41, 2))

    @contextmanager
    def lease(home, scope):
        calls.append(("lease", home, scope.run_id))
        yield SimpleNamespace(scope=scope)

    class Client:
        def factory_file_operation(self, operation, relative, **kwargs):
            calls.append((operation, relative, kwargs))
            return {"status": "ok"}

    monkeypatch.setattr(retained_scope, "retained_factory_effect_lease", lease)
    monkeypatch.setattr(
        worker_client.WorkerClient, "for_factory_checkout",
        lambda checkout, **kwargs: Client(),
    )
    with policy_scope(manager.policy):
        assert backend.write("/file.txt", "content").path == "/file.txt"
    assert calls == [
        ("lease", tmp_path, "run-1"),
        ("write_file", "file.txt", {"run_id": "run-1", "content": "content"}),
    ]


def test_retained_dispatch_denies_missing_checkout_binding(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    manager = load_plugins(enable="cultivate")
    backend = _backend(monkeypatch, tmp_path)
    monkeypatch.setenv("MIMIR_HOME", str(tmp_path))
    monkeypatch.setattr(worker_client, "factory_checkout_for_path", lambda path: None)
    with policy_scope(manager.policy):
        assert backend.write("/file.txt", "content").error == (
            "retained factory checkout binding is unavailable"
        )


def test_registration_rejects_unselected_and_duplicate_hooks() -> None:
    registry = PolicyRegistry()
    with pytest.raises(PolicyRegistrationError, match="invalid retained checkout operation"):
        registry.registrar("foreign").register_retained_checkout_operation(
            registration._retained_checkout_operation
        )
    selected = load_plugins(enable="cultivate").policy
    registry.select_plugin_candidates(selected.selected_plugin_candidates)
    registry.registrar("cultivate").register_retained_checkout_operation(
        registration._retained_checkout_operation
    )
    with pytest.raises(PolicyRegistrationError, match="invalid retained checkout operation"):
        registry.registrar("cultivate").register_retained_checkout_operation(
            registration._retained_checkout_operation
        )
    with pytest.raises(PolicyRegistrationError, match="frozen"):
        selected.registrar("cultivate")
