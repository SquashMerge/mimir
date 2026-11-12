"""Pure configuration policy for the private Worklink OpenCode daemon.

Callers supply the effective container/agent environment, not a host env file.
This module does not read credentials, write configuration, or start processes.
"""

from __future__ import annotations

import hmac
import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal
from uuid import UUID

from mimir.otel_credentials import is_sensitive_otlp_env, sensitive_otlp_endpoint
from mimir.worklink_daemon_activation import DAEMON_PATH

DAEMON_ROOT = "/var/lib/mimir-worklink/daemon"
CONFIG_PATH = f"{DAEMON_ROOT}/config/opencode/opencode.json"
DAEMON_URL = "http://127.0.0.1:4096"
TelemetryState = Literal["off", "metrics_only", "content_on", "disabled_sensitive_otlp"]


class DaemonConfigError(ValueError):
    """Invalid policy input, with a fixed credential-free diagnostic."""


def _text(value: object, *, label: str, optional: bool = True) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise DaemonConfigError(f"invalid {label}")
    if any(unicodedata.category(char) in {"Cc", "Cs"} for char in value):
        raise DaemonConfigError(f"invalid {label}")
    value = value.strip()
    if not value:
        if optional:
            return None
        raise DaemonConfigError(f"invalid {label}")
    return value


def _token(value: object, *, label: str) -> str | None:
    # Preserve the publishing resolver's strip-first token semantics without
    # relaxing control-character validation for identities or API passwords.
    return _text(value.strip() if isinstance(value, str) else value, label=label)


@dataclass(frozen=True)
class PublishingCredentials:
    bot_token: str | None = field(repr=False)
    publishing_identity: str | None

    def __post_init__(self) -> None:
        for value in (self.bot_token, self.publishing_identity):
            if value is not None and _text(value, label="publishing credentials", optional=False) != value:
                raise DaemonConfigError("invalid publishing credentials")

    def environment(self) -> dict[str, str]:
        result = {}
        if self.bot_token is not None:
            result.update(GH_TOKEN=self.bot_token, GITHUB_TOKEN=self.bot_token)
        if self.publishing_identity is not None:
            result["FACTORY_PUBLISHING_IDENTITY"] = self.publishing_identity
        return result


def resolve_daemon_credentials(environ: Mapping[str, object]) -> PublishingCredentials:
    """Normalize optional daemon aliases without changing the caller's mapping."""
    gh = _token(environ.get("GH_TOKEN"), label="GH_TOKEN")
    github = _token(environ.get("GITHUB_TOKEN"), label="GITHUB_TOKEN")
    if gh and github and gh != github:
        raise DaemonConfigError("conflicting GH_TOKEN and GITHUB_TOKEN")
    identity = None
    if "MIMIR_FACTORY_PUBLISHING_IDENTITY" in environ:
        identity = _text(environ["MIMIR_FACTORY_PUBLISHING_IDENTITY"],
                         label="MIMIR_FACTORY_PUBLISHING_IDENTITY", optional=False)
    return PublishingCredentials(github or gh, identity)


def resolve_epic_credentials(
    environ: Mapping[str, object], *, repository_config: Mapping[str, object],
) -> PublishingCredentials:
    """Validate original epic inputs before session alias normalization.

    The trusted repository reader supplies the decoded config. Presence of the
    removed key requires an ordinary reviewed edit, even when its value is null.
    Forge/account/git verification remains the orchestrator's responsibility.
    """
    if "publishing_identity" in repository_config:
        raise DaemonConfigError(
            "remove legacy .factory.json publishing_identity and configure "
            "MIMIR_FACTORY_PUBLISHING_IDENTITY"
        )
    credentials = resolve_daemon_credentials(environ)
    if not _token(environ.get("GITHUB_TOKEN"), label="GITHUB_TOKEN"):
        raise DaemonConfigError("factory publication requires GITHUB_TOKEN")
    if credentials.publishing_identity is None:
        raise DaemonConfigError("factory publication requires MIMIR_FACTORY_PUBLISHING_IDENTITY")
    return credentials


def _generation(value: object) -> str:
    if not isinstance(value, str):
        raise DaemonConfigError("invalid launch generation")
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise DaemonConfigError("invalid launch generation") from None
    return value


@dataclass(frozen=True)
class LaunchSnapshot:
    launch_generation: str
    bot_token: str | None = field(repr=False)
    publishing_identity: str | None

    def to_dict(self) -> dict[str, object]:
        return {"version": 1, "launch_generation": self.launch_generation,
                "bot_token": self.bot_token, "publishing_identity": self.publishing_identity}


def make_launch_snapshot(
    credentials: PublishingCredentials, launch_generation: str,
) -> LaunchSnapshot:
    return parse_launch_snapshot({"version": 1, "launch_generation": launch_generation,
                                 "bot_token": credentials.bot_token,
                                 "publishing_identity": credentials.publishing_identity})


def parse_launch_snapshot(payload: object) -> LaunchSnapshot:
    """Validate the exact private launch.json schema; no IO or diagnostics values."""
    if (not isinstance(payload, dict)
            or set(payload) != {"version", "launch_generation", "bot_token", "publishing_identity"}
            or type(payload["version"]) is not int or payload["version"] != 1):
        raise DaemonConfigError("invalid launch snapshot")
    for key in ("bot_token", "publishing_identity"):
        value = payload[key]
        if value is not None and _text(value, label="launch snapshot", optional=False) != value:
            raise DaemonConfigError("invalid launch snapshot")
    if len(json.dumps(payload).encode("utf-8")) > 16 * 1024:
        raise DaemonConfigError("invalid launch snapshot")
    return LaunchSnapshot(_generation(payload["launch_generation"]),
                          payload["bot_token"], payload["publishing_identity"])


def snapshot_matches(
    snapshot: LaunchSnapshot, environ: Mapping[str, object], launch_generation: str,
) -> bool:
    """Compare fresh effective inputs privately; invalid current inputs refuse."""
    current = resolve_daemon_credentials(environ)
    generation = _generation(launch_generation)
    def equal(left: str | None, right: str | None) -> bool:
        if left is None or right is None:
            return left is right
        return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))
    return (snapshot.launch_generation == generation
            and equal(snapshot.bot_token, current.bot_token)
            and equal(snapshot.publishing_identity, current.publishing_identity))


def daemon_environment(
    credentials: PublishingCredentials, *, api_password: str,
) -> dict[str, str]:
    """Construct, rather than filter, the clean-exec supervisor/daemon environment."""
    password = _text(api_password, label="daemon API password", optional=False)
    if password != api_password:
        raise DaemonConfigError("invalid daemon API password")
    result = {
        "PATH": DAEMON_PATH, "HOME": f"{DAEMON_ROOT}/home",
        "USER": "worklink", "LOGNAME": "worklink", "SHELL": "/bin/bash",
        "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TMPDIR": f"{DAEMON_ROOT}/tmp",
        "OPENCODE_CONFIG": CONFIG_PATH, "OPENCODE_SERVER_PASSWORD": api_password,
        **{f"XDG_{name.upper()}_HOME": f"{DAEMON_ROOT}/{name}"
           for name in ("config", "data", "state", "cache")},
        **{f"OPENCODE_DISABLE_{name}": "1" for name in (
            "AUTOUPDATE", "MODELS_FETCH", "FREE_MODELS", "CLAUDE_CODE",
            "CLAUDE_CODE_SKILLS", "PROJECT_CONFIG", "FILEWATCHER")},
    }
    result.update(credentials.environment())
    return result


def _flag(environ: Mapping[str, object], name: str) -> bool:
    value = environ.get(name, "")
    if not isinstance(value, str):
        raise DaemonConfigError("invalid telemetry flag")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def telemetry_service_name(base: object, identity: str | None) -> str:
    """Normalize each bounded resource-name component, never interpolate options."""
    def component(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]", "_", value)[:128]
    if base is None or base == "":
        base = "mimir.worklink"
    base = _text(base, label="telemetry service name", optional=False)
    return f"{component(base)}.{component(identity or 'worklink')}"


@dataclass(frozen=True)
class DaemonConfiguration:
    config: dict[str, object]
    telemetry_state: TelemetryState
    explanation: str | None = None


def daemon_configuration(
    environ: Mapping[str, object], credentials: PublishingCredentials,
) -> DaemonConfiguration:
    """Generate the two pinned plugin entries, with no OTLP carriers in output."""
    options: dict[str, object] = {"enabled": False}
    state: TelemetryState = "off"
    explanation = None
    endpoint = environ.get("MIMIR_OTLP_ENDPOINT", "http://localhost:4318")
    sensitive = any(value not in (None, "") and is_sensitive_otlp_env(name, value)
                    for name, value in environ.items())
    if sensitive or sensitive_otlp_endpoint(endpoint):
        state = "disabled_sensitive_otlp"
        explanation = "Worklink telemetry disabled: sensitive or invalid OTLP configuration"
    elif _flag(environ, "MIMIR_TRACING_ENABLED"):
        capture = _flag(environ, "MIMIR_TRACING_CAPTURE_CONTENT")
        state = "content_on" if capture else "metrics_only"
        options = {
            "enabled": True, "endpoint": endpoint, "protocol": "http/protobuf",
            "resourceAttributes": "service.name=" + telemetry_service_name(
                environ.get("MIMIR_OTEL_SERVICE_NAME"), credentials.publishing_identity),
            "logsEnabled": capture, "disabledMetrics": [],
            "disabledTraces": [] if capture else ["session", "llm", "tool"],
            "capturePromptInLogs": capture, "captureModelContext": capture,
        }
    return DaemonConfiguration({"plugins": [
        {"package": "opencode2-feature-factory@0.14.0"},
        {"package": "@devtheops/opencode-plugin-otel@2.1.0", "options": options},
    ]}, state, explanation)
