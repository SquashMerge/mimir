"""Local, read-only native login inspection and credential-free health."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from mimir.worklink_daemon_activation import resolve_activation
from .daemon_client import DaemonClientError, OpenCodeDaemonClient
from .daemon_config import DAEMON_URL, parse_launch_snapshot, snapshot_matches
from .daemon_service import RuntimePaths, atomic_json, directory_fd, read_json
from .identities import WorklinkIdentities, get_identities

LOGIN_STATES = frozenset({"missing", "inactive", "malformed", "zero_expiry", "locally_unexpired",
                          "refreshable_unverified", "expired_no_refresh", "unavailable"})
ALLOWED_LOGIN = frozenset({"locally_unexpired", "refreshable_unverified"})
TELEMETRY_STATES = frozenset({"off", "metrics_only", "content_on", "disabled_sensitive_otlp"})
STATUS_FIELDS = frozenset({"version", "launch_generation", "checked_at_ms", "daemon_state", "daemon_version",
                           "login_state", "login_expires_at_ms", "telemetry_state"})


@dataclass(frozen=True)
class NativeLogin:
    state: str
    expires_at_ms: int | None = None


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def classify_native(value: object, method: object, *, now: int) -> NativeLogin:
    if (not isinstance(value, str) or len(value.encode("utf-8")) > 65536
            or method != "chatgpt-headless"):
        return NativeLogin("malformed")
    try:
        credential = json.loads(value)
    except (ValueError, RecursionError):
        return NativeLogin("malformed")
    if not isinstance(credential, dict):
        return NativeLogin("malformed")
    expires = credential.get("expires")
    access = credential.get("access")
    refresh = credential.get("refresh")
    if (credential.get("type") != "oauth" or credential.get("methodID") != method
            or not isinstance(access, str) or not access.strip()
            or any(ord(c) < 32 for c in access)
            or type(expires) is not int or not 0 <= expires <= 9007199254740991
            or (refresh is not None and (not isinstance(refresh, str)
                                        or any(ord(c) < 32 for c in refresh)))):
        return NativeLogin("malformed")
    if expires == 0:
        return NativeLogin("zero_expiry", expires)
    if expires > now + 5000:
        return NativeLogin("locally_unexpired", expires)
    if refresh and refresh.strip():
        return NativeLogin("refreshable_unverified", expires)
    return NativeLogin("expired_no_refresh", expires)


def inspect_native_login(path: Path, *, uid: int, now: int | None = None) -> NativeLogin:
    """Open the native table read-only; never refresh or return its values.

    A pinned directory descriptor prevents parent substitution. SQLite owns its
    WAL locking and migrations; immutable mode would incorrectly ignore WAL.
    Bound the selected value in SQL before materializing it in Python.
    """
    try:
        with directory_fd(path.parent) as parent:
            info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_mode & 0o077
                    or info.st_nlink != 1):
                return NativeLogin("unavailable")
            connection = sqlite3.connect(f"file:/proc/self/fd/{parent}/{path.name}?mode=ro", uri=True, timeout=1)
            try:
                connection.execute("PRAGMA query_only=ON")
                connection.execute("PRAGMA busy_timeout=1000")
                if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='credential'").fetchone():
                    return NativeLogin("missing")
                rows = connection.execute(
                    "SELECT method_id, CASE WHEN length(CAST(value AS BLOB)) <= 65536 THEN value ELSE NULL END "
                    "FROM credential WHERE integration_id='openai' AND active=1 LIMIT 2"
                ).fetchall()
                if not rows:
                    inactive = connection.execute(
                        "SELECT 1 FROM credential WHERE integration_id='openai' AND active=0 LIMIT 1"
                    ).fetchone()
                    return NativeLogin("inactive" if inactive else "missing")
                if len(rows) != 1:
                    return NativeLogin("malformed")
                return classify_native(rows[0][1], rows[0][0], now=now_ms() if now is None else now)
            finally:
                connection.close()
    except FileNotFoundError:
        return NativeLogin("missing")
    except (OSError, sqlite3.Error, ValueError):
        return NativeLogin("unavailable")


def validate_generation(value: object) -> str:
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("invalid daemon generation")
    return value


def parse_status(payload: object, *, now: int | None = None) -> dict[str, object]:
    if (not isinstance(payload, dict) or set(payload) != STATUS_FIELDS
            or type(payload["version"]) is not int or payload["version"] != 1):
        raise ValueError("invalid daemon status")
    validate_generation(payload["launch_generation"])
    checked = payload["checked_at_ms"]
    current = now_ms() if now is None else now
    version = payload["daemon_version"]
    expiry = payload["login_expires_at_ms"]
    if (type(checked) is not int or not 0 <= checked <= current + 5000 or current - checked > 15000
            or payload["daemon_state"] not in {"starting", "ready", "down", "version_mismatch"}
            or payload["login_state"] not in LOGIN_STATES
            or payload["telemetry_state"] not in TELEMETRY_STATES
            or (version is not None and (not isinstance(version, str) or len(version.encode()) > 128
                                        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None))
            or (expiry is not None and (type(expiry) is not int or not 0 <= expiry <= 9007199254740991))
            or (payload["daemon_state"] == "ready" and version != "2.2.7")):
        raise ValueError("invalid daemon status")
    return dict(payload)


def load_api(paths: RuntimePaths, *, uid: int) -> dict[str, str]:
    payload = read_json(paths.api, uid=uid, mode=0o600)
    if (not isinstance(payload, dict) or set(payload) != {"version", "url", "username", "password"}
            or type(payload["version"]) is not int or payload["version"] != 1
            or payload["url"] != DAEMON_URL or payload["username"] != "opencode"
            or not isinstance(payload["password"], str) or not payload["password"]
            or any(ord(c) < 33 for c in payload["password"])):
        raise ValueError("invalid daemon API configuration")
    return payload


async def inspect_and_publish(paths: RuntimePaths, identities: WorklinkIdentities, *, down: bool = False) -> None:
    launch = read_json(paths.daemon / "launch.json", uid=identities.worklink_uid, mode=0o600)
    if (not isinstance(launch, dict) or set(launch) != {"version", "launch_generation", "telemetry_state"}
            or type(launch["version"]) is not int or launch["version"] != 1
            or launch["telemetry_state"] not in TELEMETRY_STATES):
        raise ValueError("invalid daemon launch state")
    generation = validate_generation(launch["launch_generation"])
    state, version = "down", None
    if not down:
        try:
            api = load_api(paths, uid=identities.worklink_uid)
            async with OpenCodeDaemonClient(base_url=api["url"], username=api["username"],
                                           password=api["password"], timeout=2) as client:
                info = await client.info()
            version = info["version"]
            if len(version.encode()) > 128 or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
                version = None
            state = "ready" if version == "2.2.7" else "version_mismatch"
        except (OSError, ValueError, DaemonClientError):
            pass
    login = inspect_native_login(paths.daemon / "data/opencode/opencode.db", uid=identities.worklink_uid)
    payload = {"version": 1, "launch_generation": generation, "checked_at_ms": now_ms(),
               "daemon_state": state, "daemon_version": version, "login_state": login.state,
               "login_expires_at_ms": login.expires_at_ms, "telemetry_state": launch["telemetry_state"]}
    atomic_json(paths.status, payload, uid=identities.worklink_uid, gid=identities.worklink_gid, mode=0o644)


def daemon_health(environ: dict[str, str], *, paths: RuntimePaths = RuntimePaths(),
                  identities: WorklinkIdentities | None = None) -> dict[str, object]:
    """Agent-side projection; disabled paths do not read any daemon files."""
    try:
        activation = resolve_activation(environ)
        if not activation.enabled:
            return {"daemon_enabled": False, "daemon_reason": activation.reason}
        identities = identities or get_identities()
        status = parse_status(read_json(paths.status, uid=identities.worklink_uid, mode=0o644))
        snapshot = parse_launch_snapshot(read_json(paths.snapshot, uid=identities.mimir_uid, mode=0o600))
        if not snapshot_matches(snapshot, environ, status["launch_generation"]):
            return {"daemon_enabled": True, "ok": False, "daemon_error": "bot_token_stale"}
        ready = status["daemon_state"] == "ready" and status["login_state"] in ALLOWED_LOGIN
        result = {"daemon_enabled": True, "ok": ready, **status}
        if status["login_state"] == "refreshable_unverified":
            result["daemon_warning"] = "Native login refresh is unverified; provider validity is not checked"
        if status["telemetry_state"] == "disabled_sensitive_otlp":
            result["telemetry_warning"] = "Worklink telemetry disabled: sensitive or invalid OTLP configuration"
        return result
    except (OSError, ValueError, RuntimeError, TypeError, RecursionError):
        return {"daemon_enabled": True, "ok": False, "daemon_error": "daemon_status_unavailable"}
