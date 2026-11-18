"""Restricted official Claude CLI transport for contained coding workers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import re
import shlex
import time
from typing import Mapping, Sequence

from .contained_execution import SensitiveMaterialScrubber
from .worklink.backends.base import blocked_reason_from_output

CLAUDE_EXECUTABLE = "/opt/mimir-claude/bin/claude"
OAUTH_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
EPIC_CREDENTIALS = frozenset({"GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY"})
FILE_TOOLS = ("Read", "Write", "Edit", "Glob", "Grep")
MAX_STREAM_BYTES = 16 * 1024 * 1024
MAX_LINE_BYTES = 1024 * 1024
_MODEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
_PREFIX = re.compile(r"[A-Za-z0-9_./-]+(?: [A-Za-z0-9_./-]+)*\Z")
_PROVIDER_ENV_PREFIXES = (
    "ANTHROPIC_", "CLAUDE_", "OPENAI_", "CODEX_", "OPENCODE_", "MINIMAX_",
    "OPENROUTER_", "GROQ_", "GEMINI_", "GOOGLE_", "VOYAGE_", "AWS_",
)


def resolve_model(model_spec: str | None, explicit: str | None = None) -> str:
    suffix = None
    if model_spec is not None and model_spec.startswith("claude-code:"):
        suffix = model_spec[len("claude-code:"):]
        if _MODEL.fullmatch(suffix) is None:
            raise ValueError("invalid claude-code model specification")
    selected = explicit if explicit is not None else suffix or "sonnet"
    if not isinstance(selected, str) or _MODEL.fullmatch(selected) is None:
        raise ValueError("invalid Claude model")
    return selected


def bash_rules(test_command: str, requested: Sequence[str] | None = None) -> tuple[str, ...]:
    """Render CLI prefix rules only after proving coverage of a simple command.

    OpenCode globs are not Claude permission rules. Only the existing derived
    executable prefixes and Claude's explicit ``Bash(prefix:*)`` grammar are
    admitted; shell composition is deliberately not interpreted here.
    """
    from .worklink.backends.opencode import DERIVABLE_TEST_RUNNERS

    if any(char in test_command for char in "\x00\r\n;&|`$<>\\(){}*?[]"):
        raise ValueError("Claude test_command requires a simple runner command")
    try:
        argv = shlex.split(test_command)
    except ValueError as exc:
        raise ValueError("invalid Claude test_command") from exc
    if not argv or Path(argv[0]).name not in DERIVABLE_TEST_RUNNERS or not _PREFIX.fullmatch(argv[0]):
        raise ValueError("Claude test_command requires a derivable build runner")
    prefixes: list[str] = []
    for rule in requested if requested is not None else ("git *", f"{argv[0]} *"):
        if not isinstance(rule, str):
            raise ValueError("invalid Claude Bash rule")
        if rule.startswith("Bash(") and rule.endswith(":*)"):
            prefix = rule[5:-3]
        elif rule.endswith(" *"):
            prefix = rule[:-2]
        else:
            raise ValueError("Claude Bash rules must be explicit command prefixes")
        if not _PREFIX.fullmatch(prefix) or Path(prefix.split()[0]).name not in {"git", *DERIVABLE_TEST_RUNNERS}:
            raise ValueError("unsupported Claude Bash prefix")
        prefixes.append(prefix)
    if not prefixes or len(prefixes) > 128 or not any(
        argv[:len(prefix.split())] == prefix.split() for prefix in prefixes
    ):
        raise ValueError("Claude Bash allowlist does not cover test_command")
    return tuple(dict.fromkeys(f"Bash({prefix}:*)" for prefix in prefixes))


def leaf_argv(prompt: str, model: str, worker_home: Path, rules: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(prompt, str) or not prompt.strip() or "\x00" in prompt or len(prompt.encode()) > MAX_LINE_BYTES:
        raise ValueError("invalid Claude prompt")
    resolve_model(None, model)
    if not worker_home.is_absolute():
        raise ValueError("Claude worker HOME must be absolute")
    for rule in rules:
        if not rule.startswith("Bash(") or not rule.endswith(":*)") or not _PREFIX.fullmatch(rule[5:-3]):
            raise ValueError("invalid Claude Bash rule")
    return (
        CLAUDE_EXECUTABLE, "-p", prompt, "--output-format", "stream-json", "--verbose",
        "--model", model, "--setting-sources", "", "--settings", str(worker_home / ".claude/settings.json"),
        "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--permission-mode", "dontAsk",
        "--permission-prompts", "none", "--tools", ",".join((*FILE_TOOLS, "Bash")),
        "--allowedTools", ",".join((*FILE_TOOLS, *rules)), "--disable-slash-commands", "--no-session-persistence",
    )


def leaf_environment(values: Mapping[str, str], pass_env: Sequence[str] = ()) -> dict[str, str]:
    if isinstance(pass_env, (str, bytes)) or any(not isinstance(name, str) for name in pass_env):
        raise ValueError("Claude pass_env must be a sequence of names")
    if EPIC_CREDENTIALS.intersection(pass_env):
        raise ValueError("Claude leaf refuses epic credentials")
    if any(name in {"HOME", "CLAUDE_CONFIG_DIR"} or name.startswith("GIT_CONFIG")
           or name.startswith(_PROVIDER_ENV_PREFIXES) and name != OAUTH_ENV for name in pass_env):
        raise ValueError("Claude leaf refuses caller authentication or configuration")
    if len(set(pass_env)) != len(pass_env) or any(name not in values for name in pass_env):
        raise ValueError("Claude pass_env names must be unique and present")
    token = values.get(OAUTH_ENV)
    if not isinstance(token, str) or not token.strip() or any(c in token for c in "\x00\r\n"):
        raise ValueError("Claude requires CLAUDE_CODE_OAUTH_TOKEN from claude setup-token")
    return {**{name: values[name] for name in pass_env}, OAUTH_ENV: token}


def secret_scrubber(values: Mapping[str, str]) -> SensitiveMaterialScrubber:
    scrubber = SensitiveMaterialScrubber()
    for name in (OAUTH_ENV, *EPIC_CREDENTIALS, "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"):
        if values.get(name):
            scrubber.add_scalar(values[name])
            scrubber.add_scalar(json.dumps(values[name], ensure_ascii=False)[1:-1])
            scrubber.add_scalar(json.dumps(values[name], ensure_ascii=True)[1:-1])
    return scrubber


@dataclass(frozen=True)
class StreamResult:
    status: str
    error: str | None = None
    blocked_reason: str | None = None
    retry_after_ms: int | None = None


def _reject_json_constant(value: str) -> None:
    raise ValueError("nonfinite JSON number")


def _valid_content(content: object) -> bool:
    if not isinstance(content, list):
        return False
    for block in content:
        if not isinstance(block, dict) or not isinstance(block.get("type"), str):
            return False
        if block["type"] == "text" and not isinstance(block.get("text"), str):
            return False
        if block["type"] == "tool_use" and (
            not isinstance(block.get("id"), str) or not isinstance(block.get("name"), str)
            or not isinstance(block.get("input"), dict)
        ):
            return False
    return True


def parse_stream(stdout: str, exit_code: int, *, now: float | None = None) -> StreamResult:
    try:
        encoded = stdout.encode("utf-8", "strict")
    except UnicodeError:
        return StreamResult("failed", "Claude output is not UTF-8")
    if len(encoded) > MAX_STREAM_BYTES:
        return StreamResult("failed", "Claude output is truncated or oversized")
    final: dict | None = None
    rate: dict | None = None
    malformed = False
    auth = False
    assistant_error = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        try:
            if len(line.encode()) > MAX_LINE_BYTES:
                raise ValueError("oversized event")
            event = json.loads(line, parse_constant=_reject_json_constant)
            if not isinstance(event, dict):
                raise ValueError("nonobject event")
            kind = event.get("type")
            if not isinstance(kind, str) or not kind:
                raise ValueError("missing event type")
            if kind == "assistant":
                auth |= event.get("error") == "authentication_failed"
                message = event.get("message")
                error = event.get("error")
                if (final is not None or not isinstance(message, dict) or not _valid_content(message.get("content"))
                        or error is not None and not isinstance(error, str)):
                    raise ValueError("invalid assistant event")
                assistant_error |= error is not None
            elif kind == "user":
                message = event.get("message")
                if (final is not None or not isinstance(message, dict)
                        or not (isinstance(message.get("content"), str) or _valid_content(message.get("content")))):
                    raise ValueError("invalid user event")
            elif kind == "result":
                code = event.get("api_error_status")
                auth |= type(code) is int and code in {401, 403}
                if final is not None:
                    raise ValueError("duplicate final result")
                final = event
                if (type(event.get("is_error")) is not bool or not isinstance(event.get("subtype"), str)
                        or not event["subtype"]
                        or (code is not None and type(code) is not int)):
                    raise ValueError("invalid final result")
                if not event["is_error"] and event["subtype"] != "success":
                    raise ValueError("contradictory final result")
                if not event["is_error"] and (not isinstance(event.get("result"), str)
                        or code is not None or event.get("terminal_reason", "completed") != "completed"):
                    raise ValueError("invalid successful result")
            elif kind == "rate_limit_event":
                info = event.get("rate_limit_info")
                if not isinstance(info, dict) or info.get("status") not in {"allowed", "allowed_warning", "rejected"}:
                    raise ValueError("invalid rate limit event")
                reset = info.get("resetsAt")
                if reset is not None and (type(reset) not in {int, float} or not math.isfinite(reset)):
                    raise ValueError("invalid rate reset")
                rate = info
            elif kind == "system" and not isinstance(event.get("subtype"), str):
                raise ValueError("invalid system metadata")
            elif kind == "stream_event" and (final is not None or not isinstance(event.get("event"), dict)):
                raise ValueError("invalid streaming execution event")
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
            malformed = True
    # Typed authentication failures remain authoritative even when another event
    # is malformed. Transport incompleteness is handled before this parser.
    if auth:
        return StreamResult("auth_error", "Claude subscription authentication failed")
    if malformed or final is None:
        return StreamResult("failed", "Claude stream has no unambiguous valid final result")
    if final["is_error"] and (final.get("api_error_status") == 429 or rate is not None and rate["status"] == "rejected"):
        retry = None
        if rate is not None and rate.get("resetsAt") is not None:
            delta = rate["resetsAt"] - (time.time() if now is None else now)
            retry = max(0, math.ceil(delta * 1000) if math.isfinite(delta * 1000) else math.ceil(delta) * 1000)
        return StreamResult("quota_exhausted", "Claude subscription quota exhausted", retry_after_ms=retry)
    if final["is_error"] or exit_code != 0:
        return StreamResult("failed", "Claude execution failed")
    if assistant_error or rate is not None and rate["status"] == "rejected":
        return StreamResult("failed", "Claude success contradicts its execution errors")
    reason = blocked_reason_from_output(final["result"], "")
    return StreamResult("blocked", reason, reason) if reason else StreamResult("success")
