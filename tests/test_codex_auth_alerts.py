from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest

from mimir import codex_auth_alerts as alerts, event_logger


@pytest.fixture
def login(tmp_path, monkeypatch):
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"last_refresh": "2026-11-22T01:21:29Z"}))
    log_path = tmp_path / "events.jsonl"
    monkeypatch.setattr(event_logger, "_logger", event_logger.EventLogger(log_path, "alerts"))
    # The module imported the durable function, not a captured logger.
    return path, log_path


async def test_only_permanent_codex_refresh_errors_notify(login, monkeypatch):
    from langchain_codex_plus.codex_auth import CodexAuthRefreshError

    path, log_path = login
    for error in [
        RuntimeError("HTTP 401"), CodexAuthRefreshError("HTTP 503"),
        CodexAuthRefreshError("HTTP 429"),
    ]:
        await alerts.notify_codex_refresh_failure(error, path=path)
    assert not log_path.exists()
    assert not path.with_name("auth.refresh-failure.json").exists()


async def test_unbounded_remote_error_code_is_refused(login):
    path, log_path = login
    with pytest.raises(ValueError, match="failure code"):
        await alerts.notify_codex_auth_failure("remote-body-secret", permanent=True, path=path)
    assert not log_path.exists()


@pytest.mark.parametrize("delivery", ["unset", "exception", "unsent"])
async def test_notification_retries_without_duplicate_event(login, monkeypatch, delivery):
    from langchain_codex_plus.codex_auth import CodexAuthRefreshError

    path, log_path = login
    operator = importlib.import_module("mimir.tools.operator_alert")
    calls = []
    ready = False

    async def send(channel, text, **kwargs):
        if not ready:
            if delivery == "exception":
                raise RuntimeError("sink failed")
            return SimpleNamespace(sent=False)
        calls.append(text)
        return SimpleNamespace(sent=True)

    monkeypatch.setattr(operator, "_channel_registry", SimpleNamespace(send=send))
    config = SimpleNamespace(operator_alert_channel="" if delivery == "unset" else "discord:owner")
    monkeypatch.setattr(operator, "_config", config)
    error = CodexAuthRefreshError("HTTP 401")
    await alerts.notify_codex_refresh_failure(error, path=path)
    ready = True
    config.operator_alert_channel = "discord:owner"
    await alerts.notify_codex_refresh_failure(error, path=path)
    await alerts.notify_codex_refresh_failure(error, path=path)
    assert len(calls) == 1
    assert len(log_path.read_text().splitlines()) == 1


def test_old_delivery_failure_cannot_rearm_new_login(login):
    path, _ = login
    old = alerts._claim_failure(path, "http_401")
    path.write_text(json.dumps({"last_refresh": "2026-11-22T16:27:00Z"}))
    new = alerts._claim_failure(path, "http_401")
    assert old != new
    alerts._retry_notification(path, old)
    assert alerts._claim_failure(path, "http_401") is None


@pytest.mark.parametrize("operation", ["claim", "retry"])
def test_alert_lock_symlink_is_refused(login, operation):
    path, log_path = login
    target = path.parent / "foreign"
    target.write_text("unchanged")
    path.with_name("auth.refresh-failure.json").write_text(json.dumps({
        "login": "old-login", "notified": True,
    }))
    path.with_name("auth.refresh-failure.lock").symlink_to(target)
    with pytest.raises(OSError):
        if operation == "claim":
            alerts._claim_failure(path, "http_401")
        else:
            alerts._retry_notification(path, "old-login")
    assert target.read_text() == "unchanged"
    assert not log_path.exists()


async def test_server_notice_never_sends_without_configured_destination(monkeypatch):
    operator = importlib.import_module("mimir.tools.operator_alert")
    calls = []

    async def send(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(sent=True)

    monkeypatch.setattr(operator, "_channel_registry", SimpleNamespace(send=send))
    monkeypatch.setattr(operator, "_config", SimpleNamespace(operator_alert_channel=""))
    assert await operator.send_operator_notice("fixed server notice") is False
    assert not calls


async def test_web_notification_is_durable_even_without_live_delivery(login, monkeypatch):
    from langchain_codex_plus.codex_auth import CodexAuthRefreshError
    from mimir import history

    path, _ = login
    operator = importlib.import_module("mimir.tools.operator_alert")
    buffer = history.MessageBuffer(path.parent / "history.jsonl")
    monkeypatch.setattr(history, "_global_buffer", buffer)
    calls = []

    async def send(channel, text, **kwargs):
        calls.append(kwargs)
        raise RuntimeError("no live subscriber")

    monkeypatch.setattr(operator, "_config", SimpleNamespace(operator_alert_channel="web-owner"))
    monkeypatch.setattr(operator, "_channel_registry", SimpleNamespace(send=send))
    for _ in range(2):
        await alerts.notify_codex_refresh_failure(CodexAuthRefreshError("HTTP 401"), path=path)
    assert len(calls) == 1
    assert len(buffer.history_path.read_text().splitlines()) == 1
    record, = buffer.recent_in_channel("web-owner", 10)
    assert record.integrity == "trusted"
    assert alerts.LOGIN_REMEDY in record.content
