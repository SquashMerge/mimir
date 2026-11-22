"""Fixed, model-free diagnostics for the controller's Codex login."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any

from ._atomic import atomic_write_json
from .event_logger import log_durable_event_sync


LOGIN_REMEDY = "docker exec -u mimir <container> codex login --device-auth"


def codex_auth_path(env: Any = None) -> Path:
    values = os.environ if env is None else env
    return (Path(values["CODEX_HOME"]).expanduser() if values.get("CODEX_HOME", "").strip()
            else Path(values.get("HOME", str(Path.home()))) / ".codex") / "auth.json"


def refresh_failure(exc: BaseException) -> tuple[str, bool]:
    """Classify locally, never return exception text or a remote error code."""
    code = getattr(exc, "code", None)
    text = str(exc)
    if code in {"refresh_token_reused", "invalid_grant"}:
        return code, True
    if re.search(r"\bHTTP 401\b", text):
        return "http_401", True
    permanent = getattr(exc, "permanent", False) is True or bool(re.search(r"\bHTTP 400\b", text))
    import httpx

    cause = exc.__cause__
    if isinstance(exc, (httpx.TransportError, ConnectionError, TimeoutError)) or isinstance(
        cause, (httpx.TransportError, ConnectionError, TimeoutError),
    ):
        return "network", permanent
    return "other", permanent


def _claim_failure(path: Path, error_code: str) -> str | None:
    """Persist the once-per-login reservation across jobs, turns and restarts."""
    with path.open("rb") as stream:
        raw = stream.read(65537)
    document = json.loads(raw) if len(raw) <= 65536 else None
    stamp = document.get("last_refresh") if isinstance(document, dict) else None
    # Hash only non-secret login metadata, not tokens or the auth document.
    fingerprint = hashlib.sha256(str(stamp).encode()).hexdigest()
    state = path.with_name("auth.refresh-failure.json")
    fd = os.open(state.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        previous = {}
        if state.exists():
            previous = json.loads(state.read_text())
        if previous.get("login") == fingerprint and previous.get("notified") is True:
            return None
        if previous.get("login") != fingerprint:
            log_durable_event_sync(
                "codex_auth_refresh_failed", error_code=error_code,
                login=fingerprint, remedy=LOGIN_REMEDY,
            )
        atomic_write_json(state, {"login": fingerprint, "notified": True}, mode=0o600)
        return fingerprint
    finally:
        os.close(fd)


def _retry_notification(path: Path, fingerprint: str) -> None:
    state = path.with_name("auth.refresh-failure.json")
    fd = os.open(state.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        previous = json.loads(state.read_text())
        if previous.get("login") == fingerprint:
            atomic_write_json(state, {"login": fingerprint, "notified": False}, mode=0o600)
    finally:
        os.close(fd)


async def notify_codex_refresh_failure(exc: BaseException, *, path: Path | None = None) -> None:
    """Permanent provider errors only; both proactive and reactive callers use this."""
    from langchain_codex_plus.codex_auth import CodexAuthRefreshError

    if not isinstance(exc, CodexAuthRefreshError):
        return
    code, permanent = refresh_failure(exc)
    await notify_codex_auth_failure(code, permanent=permanent, path=path)


async def notify_codex_auth_failure(
    code: str, *, permanent: bool, path: Path | None = None,
) -> None:
    """Deliver sanitized reconciliation diagnostics on the server's event loop."""
    if code not in {"http_401", "refresh_token_reused", "invalid_grant", "network", "other"}:
        raise ValueError("invalid Codex refresh failure code")
    if not permanent:
        return
    path = path or codex_auth_path()
    fingerprint = await asyncio.to_thread(_claim_failure, path, code)
    if fingerprint is not None:
        from .tools.operator_alert import send_operator_notice

        try:
            delivered = await send_operator_notice(f"Codex login refresh failed ({code}). Fix: {LOGIN_REMEDY}")
        except Exception:
            delivered = False
        if not delivered:
            await asyncio.to_thread(_retry_notification, path, fingerprint)
