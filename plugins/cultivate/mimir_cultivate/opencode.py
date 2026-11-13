"""OpenCode daemon model selection, redaction, and proposal artifacts."""

from __future__ import annotations

import base64
import logging
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import shlex
import stat
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any

from langchain_core.tools import tool

from mimir.access_control import _MAINTENANCE_GIT_BASE_OVERRIDES, _maintenance_git_filter_overrides
from mimir.model_registry import DEFAULT_MODEL_SPEC
from mimir.contained_snapshot import (
    SnapshotCredentialsRefused, SnapshotEmbeddedRepository, SnapshotSourceChanged,
    SnapshotUnsafeEntry,
)
from mimir import event_logger
from mimir.tools.registry import (
    _STATE, _spawn_acquire_rate_slot, _spawn_current_depth, _spawn_guard_init,
    _spawn_release_rate_slot,
)

from . import contained_execution


_REDACTION = b"<redacted>"
log = logging.getLogger(__name__)


def opencode_available() -> bool:
    """Check CLI presence without reading authentication material."""
    return shutil.which("opencode") is not None


def startup_prerequisites() -> tuple[str, ...]:
    return () if opencode_available() else (
        "the opencode CLI is not installed or is not on PATH",
    )


class SensitiveMaterialScrubber:
    __slots__ = ("_materials",)

    def __init__(
        self, *, home: Path | str | None = None, checkout: Path | str | None = None,
        artifact_root: Path | str | None = None, source_paths: Sequence[Path | str] = (),
    ) -> None:
        self._materials: set[bytes] = set()
        controller_home = home if home is not None else os.environ.get("HOME")
        known_paths: list[Path | str] = list(source_paths)
        if controller_home is not None and str(controller_home):
            home_path = Path(controller_home).expanduser()
            config_root = home_path / ".config"
            data_root = home_path / ".local/share"
            known_paths.extend((
                config_root / "opencode/opencode.json",
                config_root / "opencode/opencode.jsonc",
                data_root / "opencode/auth.json",
            ))
        explicit_config = os.environ.get("OPENCODE_CONFIG")
        if explicit_config:
            known_paths.append(explicit_config)
        for path in (controller_home, checkout, artifact_root, *known_paths):
            if path is not None and str(path):
                self.add_path(path)

    def __repr__(self) -> str:
        return "SensitiveMaterialScrubber()"

    def add_path(self, path: Path | str) -> None:
        lexical = os.path.expanduser(os.fspath(path))
        forms = {lexical, str(Path(lexical).resolve(strict=False))}
        for value in tuple(forms):
            candidate = Path(value)
            if candidate.is_absolute():
                forms.add(candidate.as_uri())
        for value in tuple(forms):
            encoded = json.dumps(value, ensure_ascii=False)
            forms.add(encoded)
            forms.add(encoded[1:-1])
        for value in forms:
            self.add_scalar(value)

    def add_document(self, document: bytes | bytearray | memoryview | str) -> None:
        self._add(document)

    def add_scalar(self, value: bytes | bytearray | memoryview | str) -> None:
        self._add(value)

    def contains_sensitive(self, value: bytes | bytearray | memoryview | str) -> bool:
        payload = self._bytes(value)
        return any(material in payload for material in self._materials)

    def scrub_bytes(self, value: bytes | bytearray | memoryview | str) -> bytes:
        payload = self._bytes(value)
        for material in sorted(self._materials, key=len, reverse=True):
            payload = payload.replace(material, _REDACTION)
        return payload

    def scrub_text(
        self, value: bytes | bytearray | memoryview | str, *, errors: str = "replace",
    ) -> str:
        return self.scrub_bytes(value).decode("utf-8", errors=errors)

    def lookahead_bytes(self) -> int:
        if not self._materials:
            return 0
        return max(len(material) for material in self._materials) - 1

    def safe_truncation_length(self, value: bytes | bytearray | memoryview, limit: int) -> int:
        payload = self._bytes(value)
        cut = max(0, min(limit, len(payload)))
        while True:
            for material in self._materials:
                length = len(material)
                found = payload.find(material, max(0, cut - length + 1), cut + length - 1)
                if found != -1:
                    cut = found
                    break
            else:
                return cut

    def _add(self, value: bytes | bytearray | memoryview | str) -> None:
        payload = self._bytes(value)
        if payload:
            self._materials.add(payload)

    @staticmethod
    def _bytes(value: bytes | bytearray | memoryview | str) -> bytes:
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)


def _sanitized_git_env() -> dict[str, str]:
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_PAGER": "cat",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "/bin/false",
    }


_CONFIG_REASON_CODES = frozenset({
    "config_malformed", "config_missing", "config_oversize",
    "config_provider_selection", "config_unreadable", "config_unsafe_inline_secret",
})
_AUTH_REASON_CODES = frozenset({
    "auth_invalid", "auth_malformed", "auth_missing", "auth_oversize", "auth_unreadable",
})


class OpenCodeConfigError(ValueError):
    def __init__(self, reason_code: str) -> None:
        if reason_code not in _CONFIG_REASON_CODES:
            raise ValueError("invalid OpenCode configuration reason code")
        self.reason_code = reason_code
        super().__init__("OpenCode configuration unavailable")


class OpenCodeAuthError(OpenCodeConfigError):
    def __init__(self, reason_code: str) -> None:
        if reason_code not in _AUTH_REASON_CODES:
            raise ValueError("invalid OpenCode authentication reason code")
        self.reason_code = reason_code
        ValueError.__init__(self, "OpenCode authentication unavailable")


@dataclass(frozen=True)
class OpenCodeInvocation:
    model: str
    provider: str
    model_source: str
    config_path: Path = field(repr=False)
    auth_path: Path = field(repr=False)
    auth_type: str | None
    remove_env: tuple[str, ...] = ()
    pass_env: tuple[str, ...] = ()
    provider_config: object = field(default_factory=dict, repr=False, compare=False)
    auth_entry: object | None = field(default=None, repr=False, compare=False)
    scrubber: SensitiveMaterialScrubber = field(repr=False, compare=False, default_factory=SensitiveMaterialScrubber)


def _home(env: Mapping[str, str]) -> Path:
    return Path(env.get("HOME", str(Path.home()))).expanduser()


def opencode_config_path(
    env: Mapping[str, str] | None = None, *, override: Path | str | None = None,
) -> Path:
    values = os.environ if env is None else env
    if override is not None:
        return Path(override).expanduser()
    explicit = values.get("OPENCODE_CONFIG", "").strip()
    if explicit:
        return Path(explicit).expanduser()
    config_home = values.get("XDG_CONFIG_HOME", "").strip()
    root = Path(config_home).expanduser() if config_home else _home(values) / ".config"
    directory = root / "opencode"
    jsonc_path = directory / "opencode.jsonc"
    json_path = directory / "opencode.json"
    if jsonc_path.exists() or not json_path.exists():
        return jsonc_path
    return json_path


def opencode_auth_path(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    data_home = values.get("XDG_DATA_HOME", "").strip()
    root = Path(data_home).expanduser() if data_home else _home(values) / ".local" / "share"
    return root / "opencode" / "auth.json"


def opencode_model_from_agent_spec(model_spec: str) -> str:
    route, separator, model = model_spec.strip().partition(":")
    if not separator or not route or not model:
        raise OpenCodeConfigError("config_provider_selection")
    provider = {"codex-plus": "openai", "claude-code": "anthropic"}.get(route, route)
    return f"{provider}/{model}"


def _provider_from_model(model: str) -> str:
    provider, separator, name = model.partition("/")
    if not separator or not provider or not name:
        raise OpenCodeConfigError("config_provider_selection")
    return provider


def resolve_daemon_model(env: Mapping[str, str], explicit_model: str | None = None) -> dict[str, str]:
    """Resolve the session model without opening provider or agent auth files."""
    from mimir._reasoning_effort import validate_effort

    selected = (explicit_model or opencode_model_from_agent_spec(env.get("MIMIR_MODEL_SPEC", DEFAULT_MODEL_SPEC))).strip()
    model, separator, variant = selected.partition("#")
    provider = _provider_from_model(model)
    if any(not char.isprintable() or char.isspace() for char in selected):
        raise OpenCodeConfigError("config_provider_selection")
    effort = env.get("MIMIR_WORKLINK_REASONING_EFFORT") or env.get("MIMIR_MODEL_REASONING_EFFORT", "")
    if not separator and effort:
        try:
            variant = validate_effort("codex-plus", effort) if provider == "openai" else effort
        except ValueError as exc:
            name = "MIMIR_WORKLINK_REASONING_EFFORT" if env.get("MIMIR_WORKLINK_REASONING_EFFORT") else "MIMIR_MODEL_REASONING_EFFORT"
            raise ValueError(f"{name}: {exc}") from exc
    if separator and not variant:
        raise OpenCodeConfigError("config_provider_selection")
    reference = {"providerID": provider, "id": model.split("/", 1)[1]}
    if variant and variant != "none":
        reference["variant"] = variant
    if any(not value or len(value.encode()) > 1024 for value in reference.values()):
        raise OpenCodeConfigError("config_provider_selection")
    return reference


def redact_daemon_output(text: str) -> str:
    from mimir.otel_credentials import is_sensitive_otlp_env
    from mimir.redaction import redact_text

    for name, value in os.environ.items():
        if value and is_sensitive_otlp_env(name, value):
            text = text.replace(value, "<redacted>")
    return redact_text(text)


def _jwt_exp(token: str) -> int:
    try:
        segment = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
        exp = payload["exp"]
        return exp if isinstance(exp, int) and not isinstance(exp, bool) else 0
    except (IndexError, ValueError, KeyError, TypeError):
        return 0


MAX_PROPOSAL_PATHS = 1_000
MAX_PROPOSAL_PATH_BYTES = 4_096
MAX_PROPOSAL_NAME_STREAM_BYTES = 4_097_000
MAX_PROPOSAL_PATCH_BYTES = 1_048_576
_SAFE_HANDLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_PATH_TOKEN = re.compile(r"(?:file://)?/[^\s\"'<>]+")


@dataclass(frozen=True)
class OpenCodeProposal:
    schema_version: int
    kind: str
    base_tree: str
    encoding: str
    byte_length: int
    sha256: str
    patch: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProposalBuildResult:
    proposal: OpenCodeProposal | None
    reason_code: str


@dataclass(frozen=True)
class SpawnTerminalResult:
    status: str
    exit_code: int | None
    reason_code: str
    event_type: str
    proposal: OpenCodeProposal | None = None

    def event(self, run_id: str) -> dict[str, str]:
        return {
            "type": self.event_type, "run_id": run_id,
            "status": self.status, "reason_code": self.reason_code,
        }


def _path_forms(path: Path | str) -> set[str]:
    lexical = os.path.expanduser(os.fspath(path))
    resolved = str(Path(lexical).resolve(strict=False))
    forms = {lexical, resolved}
    for value in tuple(forms):
        candidate = Path(value)
        if candidate.is_absolute():
            forms.add(candidate.as_uri())
    for value in tuple(forms):
        encoded = json.dumps(value, ensure_ascii=False)
        forms.add(encoded)
        forms.add(encoded[1:-1])
    return {value for value in forms if value}


def _beneath(candidate: str, home: Path) -> bool:
    try:
        lexical = Path(os.path.normpath(candidate))
        resolved = lexical.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return False
    for value in (lexical, resolved):
        try:
            value.relative_to(home)
            return True
        except ValueError:
            pass
    return False


def _contains_path_form(prompt: str, form: str, *, descendants: bool) -> bool:
    start = 0
    while (index := prompt.find(form, start)) >= 0:
        before = prompt[index - 1] if index else ""
        end = index + len(form)
        after = prompt[end] if end < len(prompt) else ""
        before_boundary = not before or before.isspace() or before in "\"'([{<"
        after_boundary = not after or after.isspace() or after in "\"')]}>,;:!?"
        if before_boundary and (after_boundary or (descendants and after == "/")):
            return True
        start = index + 1
    return False


def prompt_contains_sensitive_source_path(
    prompt: str, *, agent_home: Path | str, source_paths: Sequence[Path | str] = (),
) -> str | None:
    if not isinstance(prompt, str):
        raise TypeError("prompt must be a string")
    home = Path(os.path.expanduser(os.fspath(agent_home))).resolve(strict=False)
    if any(
        _contains_path_form(prompt, form, descendants=True)
        for form in _path_forms(agent_home)
    ):
        return "agent_home_path"
    for match in _PATH_TOKEN.finditer(prompt):
        token = match.group(0).rstrip(".,;:!?)]}")
        if token.startswith("file://"):
            token = token[7:]
        if _beneath(token, home):
            return "agent_home_path"
    tokens = {match.group(0).rstrip(".,;:!?)]}") for match in _PATH_TOKEN.finditer(prompt)}
    for source_path in source_paths:
        forms = _path_forms(source_path)
        if tokens.intersection(forms):
            return "config_auth_source_path"
        if any(_contains_path_form(prompt, form, descendants=False) for form in forms):
            return "config_auth_source_path"
    return None


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _run_git(checkout: Path, *args: str, limit: int | None = None) -> bytes:
    env = _sanitized_git_env()
    filters = _maintenance_git_filter_overrides(checkout, "git", effective_config=True, env=env)
    if filters is None:
        raise OSError("proposal Git filter configuration unavailable")
    command = ("git", "-C", os.fspath(checkout), *_MAINTENANCE_GIT_BASE_OVERRIDES, *filters, *args)
    if limit is None:
        completed = subprocess.run(
            command, check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env,
        )
        if completed.returncode != 0:
            raise OSError("proposal Git operation failed")
        return completed.stdout
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    if process.stdout is None:
        _stop_process(process)
        raise OSError("proposal Git output unavailable")
    output = process.stdout.read(limit)
    if len(output) == limit:
        process.stdout.close()
        _stop_process(process)
        return output
    return_code = process.wait()
    process.stdout.close()
    if return_code != 0:
        raise OSError("proposal Git operation failed")
    return output


def build_opencode_proposal(
    checkout: Path | str, *, scrubber: SensitiveMaterialScrubber, base_tree: str | None = None,
) -> ProposalBuildResult:
    root = Path(checkout)
    try:
        _run_git(root, "add", "-A")
        name_stream = _run_git(
            root, "diff", "--cached", "--name-only", "-z", "--no-ext-diff", "--no-textconv",
            limit=MAX_PROPOSAL_NAME_STREAM_BYTES + 1,
        )
        if len(name_stream) > MAX_PROPOSAL_NAME_STREAM_BYTES:
            return ProposalBuildResult(None, "path_bytes")
        if not name_stream:
            return ProposalBuildResult(None, "no_changes")
        paths = name_stream[:-1].split(b"\0") if name_stream.endswith(b"\0") else []
        if not paths or any(not path for path in paths):
            return ProposalBuildResult(None, "proposal_unavailable")
        if len(paths) > MAX_PROPOSAL_PATHS:
            return ProposalBuildResult(None, "path_count")
        for path in paths:
            if len(path) > MAX_PROPOSAL_PATH_BYTES:
                return ProposalBuildResult(None, "path_bytes")
            pure = PurePosixPath(os.fsdecode(path))
            if pure.is_absolute() or ".." in pure.parts:
                return ProposalBuildResult(None, "proposal_unavailable")
        patch = _run_git(
            root, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-textconv",
            limit=MAX_PROPOSAL_PATCH_BYTES + 1,
        )
        if len(patch) > MAX_PROPOSAL_PATCH_BYTES:
            return ProposalBuildResult(None, "patch_bytes")
        if scrubber.contains_sensitive(patch):
            return ProposalBuildResult(None, "proposal_sensitive_content")
        tree = base_tree or _run_git(root, "rev-parse", "HEAD^{tree}").decode("ascii").strip()
        if re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", tree) is None:
            return ProposalBuildResult(None, "proposal_unavailable")
    except (OSError, UnicodeError, ValueError):
        return ProposalBuildResult(None, "proposal_unavailable")
    proposal = OpenCodeProposal(
        schema_version=1, kind="git_binary_patch", base_tree=tree.lower(), encoding="base64",
        byte_length=len(patch), sha256=hashlib.sha256(patch).hexdigest(),
        patch=base64.b64encode(patch).decode("ascii"),
    )
    return ProposalBuildResult(proposal, "proposal_created")


def resolve_artifact_handle(artifact_root: Path | str, handle: str) -> Path:
    if not isinstance(handle, str) or _SAFE_HANDLE.fullmatch(handle) is None:
        raise ValueError("unsafe artifact handle")
    if handle in {".", ".."}:
        raise ValueError("unsafe artifact handle")
    root = Path(artifact_root).resolve(strict=False)
    candidate = (root / handle).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("unsafe artifact handle") from exc
    return candidate


def write_spawn_artifacts(
    artifact_root: Path | str, handle: str, *, prompt: str, stdout: bytes | str,
    stderr: bytes | str, manifest: Mapping[str, Any], scrubber: SensitiveMaterialScrubber,
    proposal: OpenCodeProposal | None = None,
) -> str:
    directory = resolve_artifact_handle(artifact_root, handle)
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    prompt_text = scrubber.scrub_text(prompt)
    stdout_text = scrubber.scrub_text(stdout)
    stderr_text = scrubber.scrub_text(stderr)
    scrubbed_manifest = json.loads(scrubber.scrub_text(json.dumps(dict(manifest), ensure_ascii=False)))
    (directory / "prompt.md").write_text(prompt_text, encoding="utf-8")
    (directory / "stdout.txt").write_text(stdout_text, encoding="utf-8")
    (directory / "stderr.txt").write_text(stderr_text, encoding="utf-8")
    (directory / "manifest.json").write_text(
        json.dumps(scrubbed_manifest, ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8",
    )
    if proposal is not None:
        raw_patch = base64.b64decode(proposal.patch, validate=True)
        if scrubber.contains_sensitive(raw_patch):
            raise ValueError("proposal contains sensitive material")
        (directory / "proposal.json").write_text(
            json.dumps(proposal.as_dict(), ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8",
        )
    return handle


def classify_spawn_terminal_state(
    *, artifact_available: bool = True, configuration_reason: str | None = None,
    authentication_reason: str | None = None, prompt_reason: str | None = None,
    provisioning_reason: str | None = None, containment_available: bool = True,
    timed_out: bool = False, output_overflow: bool = False, exit_code: int | None = None,
    worker_authentication_required: bool = False,
    proposal_result: ProposalBuildResult | None = None,
) -> SpawnTerminalResult:
    if not artifact_available:
        return SpawnTerminalResult("artifact_unavailable", None, "artifact_unavailable", "spawn_open_code_provisioning_refused")
    if configuration_reason is not None:
        return SpawnTerminalResult("configuration_refused", None, configuration_reason, "spawn_open_code_configuration_refused")
    if authentication_reason is not None:
        return SpawnTerminalResult("authentication_refused", None, authentication_reason, "spawn_open_code_authentication_refused")
    if prompt_reason is not None:
        return SpawnTerminalResult("prompt_refused", None, prompt_reason, "spawn_open_code_prompt_refused")
    if provisioning_reason is not None:
        status_by_reason = {
            "seed_credentials": "seed_credentials_refused",
            "invalid_seed": "seed_refused", "unsafe_seed_entry": "seed_refused",
            "embedded_seed_repository": "seed_refused",
            "seed_changed": "seed_refused", "provisioning_unavailable": "provisioning_unavailable",
        }
        status = status_by_reason.get(provisioning_reason, "provisioning_unavailable")
        return SpawnTerminalResult(status, None, provisioning_reason, "spawn_open_code_provisioning_refused")
    if not containment_available:
        return SpawnTerminalResult("containment_unavailable", None, "containment_unavailable", "spawn_open_code_containment_refused")
    if timed_out:
        return SpawnTerminalResult("timeout", None, "timeout", "spawn_open_code_completed")
    if output_overflow:
        return SpawnTerminalResult("output_overflow", exit_code, "output_overflow", "spawn_open_code_completed")
    if exit_code != 0:
        if worker_authentication_required:
            return SpawnTerminalResult("authentication_required", exit_code, "worker_authentication_required", "spawn_open_code_completed")
        return SpawnTerminalResult("failed", exit_code, "worker_nonzero", "spawn_open_code_completed")
    if proposal_result is None:
        return SpawnTerminalResult("proposal_unavailable", 0, "proposal_unavailable", "spawn_open_code_completed")
    reason = proposal_result.reason_code
    if reason == "no_changes":
        return SpawnTerminalResult("succeeded", 0, reason, "spawn_open_code_completed")
    if reason in {"path_count", "path_bytes", "patch_bytes"}:
        return SpawnTerminalResult("proposal_overflow", 0, reason, "spawn_open_code_completed")
    if reason == "proposal_sensitive_content":
        return SpawnTerminalResult(reason, 0, reason, "spawn_open_code_completed")
    if reason != "proposal_created" or proposal_result.proposal is None:
        return SpawnTerminalResult("proposal_unavailable", 0, "proposal_unavailable", "spawn_open_code_completed")
    return SpawnTerminalResult("succeeded", 0, reason, "spawn_open_code_completed", proposal_result.proposal)


def cleanup_failure_event(run_id: str) -> dict[str, str]:
    return {"type": "spawn_open_code_cleanup_failed", "run_id": run_id, "reason_code": "cleanup_failed"}


def _confined_artifact_base(artifact_root: str, home: str | Path) -> Path:
    base = Path(artifact_root).expanduser().resolve()
    home_resolved = Path(home).expanduser().resolve()
    if base != home_resolved and not base.is_relative_to(home_resolved):
        raise ValueError(
            "artifact_root must be within the agent home "
            f"({home_resolved}); got {base}"
        )
    return base


async def _spawn_open_code_impl(
    prompt: str,
    cwd: str | None = None,
    timeout_s: int = 1800,
    name: str | None = None,
    model: str | None = None,
    agent: str | None = None,
    artifact_root: str | None = None,
    *,
    contained_runner: Any,
    checkout_factory: Any,
    execution_identifier: str | None = None,
) -> str:
    cfg = _STATE["spawn_config"]
    if cfg is None:
        return "spawn_open_code failed: no spawn config"
    if not prompt or not prompt.strip():
        return "spawn_open_code failed: prompt is required"

    guard = _spawn_guard_init()
    current_depth = _spawn_current_depth()
    if current_depth >= guard.max_depth:
        return (
            "spawn_open_code refused: recursion depth cap reached "
            f"({current_depth} >= MIMIR_SPAWN_MAX_DEPTH={guard.max_depth})."
        )
    rate_token, rate_err = await _spawn_acquire_rate_slot(guard, "spawn_open_code")
    if rate_err is not None:
        return rate_err

    run_id = f"opencode-{uuid.uuid4().hex[:12]}"
    default_cwd = Path(cfg.get("default_cwd") or os.environ.get("HOME", "."))
    seed = Path(cwd).expanduser() if cwd else default_cwd
    home = Path(os.environ.get("HOME", str(default_cwd))).expanduser()
    config_path = opencode_config_path(override=cfg.get("opencode_config_path"))
    auth_path = opencode_auth_path()
    scrubber = contained_execution.SensitiveMaterialScrubber(
        home=home, checkout=seed, artifact_root=artifact_root,
        source_paths=(config_path, auth_path),
    )
    from mimir.otel_credentials import is_sensitive_otlp_env
    for key, value in os.environ.items():
        if value and (is_sensitive_otlp_env(key, value) or key in {"GH_TOKEN", "GITHUB_TOKEN"}):
            scrubber.add_scalar(value)
    selected_artifact_root = Path(
        artifact_root or cfg.get("artifact_root") or (home / ".factory" / "runs")
    ).expanduser()
    artifact_directory_fd = -1
    try:
        artifact_base = _confined_artifact_base(str(selected_artifact_root), home)
        artifact_base.mkdir(mode=0o700, parents=True, exist_ok=True)
        artifact_directory = resolve_artifact_handle(artifact_base, run_id)
        artifact_directory.mkdir(mode=0o700, exist_ok=False)
        for artifact_name in ("prompt.md", "stdout.txt", "stderr.txt", "manifest.json"):
            artifact_path = artifact_directory / artifact_name
            artifact_path.write_bytes(b"")
            artifact_path.chmod(0o600)
        artifact_directory_fd = os.open(
            artifact_directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
    except (OSError, RuntimeError, ValueError):
        if artifact_directory_fd >= 0:
            os.close(artifact_directory_fd)
        terminal = classify_spawn_terminal_state(artifact_available=False)
        await event_logger.safe_log_event(
            terminal.event_type, run_id=run_id, status=terminal.status,
            reason_code=terminal.reason_code,
        )
        await _spawn_release_rate_slot(guard, rate_token)
        return json.dumps({
            "run_id": run_id, "status": terminal.status,
            "reason_code": terminal.reason_code, "exit_code": None,
            "stdout": "", "result": "", "stderr": "Artifact storage unavailable",
            "artifact_dir": None, "name": name, "proposal": None,
            "opencode_session_id": None,
        }, indent=2)

    stdout = b""
    stderr = b""
    invocation = None
    checkout = None
    execution = None
    terminal = None
    seed_tree = None
    retain_checkout = False
    session_id = None
    launch_generation = None

    def admitted(handle) -> None:
        nonlocal session_id, launch_generation
        session_id = handle.opencode_session_id
        launch_generation = handle.launch_generation

    def quiescent() -> None:
        nonlocal retain_checkout
        retain_checkout = False

    try:
        try:
            reference = resolve_daemon_model(dict(os.environ), model)
            invocation = OpenCodeInvocation(
                model=f"{reference['providerID']}/{reference['id']}", provider=reference['providerID'],
                model_source="explicit" if model else "agent_model", config_path=config_path,
                auth_path=auth_path, auth_type=None, scrubber=scrubber)
            scrubber.add_path(seed)
            scrubber.add_path(artifact_base)
        except OpenCodeAuthError as exc:
            terminal = classify_spawn_terminal_state(authentication_reason=exc.reason_code)
        except OpenCodeConfigError as exc:
            terminal = classify_spawn_terminal_state(configuration_reason=exc.reason_code)

        if terminal is None and invocation is not None:
            prompt_reason = prompt_contains_sensitive_source_path(
                prompt, agent_home=home,
                source_paths=(invocation.config_path, invocation.auth_path),
            )
            if prompt_reason is not None:
                terminal = classify_spawn_terminal_state(prompt_reason=prompt_reason)
        if terminal is None and invocation is not None and agent:
            agent_reason = prompt_contains_sensitive_source_path(
                agent, agent_home=home,
                source_paths=(invocation.config_path, invocation.auth_path),
            )
            if agent_reason is not None:
                terminal = classify_spawn_terminal_state(prompt_reason=agent_reason)

        if terminal is None and invocation is not None:
            try:
                from .worklink.backends.base import SessionRequest
                if os.environ.get("MIMIR_OPENCODE_SPAWN_ARGS", "").strip():
                    raise ValueError("daemon sessions do not accept CLI arguments")
                request = SessionRequest("spawn", reference, prompt,
                                         spawn_depth=current_depth + 1, agent=agent)
            except ValueError:
                terminal = classify_spawn_terminal_state(configuration_reason="config_provider_selection")

        if terminal is None and invocation is not None:
            sensitive_execution_value = any(
                prompt_contains_sensitive_source_path(
                    value, agent_home=home,
                    source_paths=(invocation.config_path, invocation.auth_path),
                ) is not None
                for value in reference.values()
            )
            if sensitive_execution_value:
                terminal = classify_spawn_terminal_state(configuration_reason="config_provider_selection")

        if terminal is None and invocation is not None:
            try:
                checkout = checkout_factory(
                    seed, default_cwd=default_cwd,
                    known_sensitive=(),
                )
                seed_tree = checkout.base_tree
            except SnapshotCredentialsRefused:
                terminal = classify_spawn_terminal_state(provisioning_reason="seed_credentials")
            except SnapshotEmbeddedRepository:
                terminal = classify_spawn_terminal_state(provisioning_reason="embedded_seed_repository")
            except SnapshotUnsafeEntry:
                terminal = classify_spawn_terminal_state(provisioning_reason="unsafe_seed_entry")
            except SnapshotSourceChanged:
                terminal = classify_spawn_terminal_state(provisioning_reason="seed_changed")
            except ValueError:
                terminal = classify_spawn_terminal_state(provisioning_reason="invalid_seed")
            except (OSError, RuntimeError):
                terminal = classify_spawn_terminal_state(provisioning_reason="provisioning_unavailable")

        if terminal is None and invocation is not None and checkout is not None:
            assert guard.sem is not None
            try:
                async with guard.sem:
                    retain_checkout = True
                    execution = await contained_runner(
                        checkout.capability, request, timeout_s=timeout_s,
                        stdout_limit=1024 * 1024,
                        on_session=admitted, on_quiescence=quiescent,
                    )
                    session_id = execution.handle.opencode_session_id if execution.handle else None
                    retain_checkout = False
                stdout = redact_daemon_output(execution.stdout).encode()
                stderr = redact_daemon_output(execution.stderr).encode()
            except ValueError:
                retain_checkout = False
                terminal = classify_spawn_terminal_state(containment_available=False)
            except (OSError, RuntimeError) as exc:
                from .worklink.backends.base import SessionAdmissionError
                if isinstance(exc, SessionAdmissionError):
                    retain_checkout = exc.uncertain and not exc.quiescent
                    launch_generation = exc.launch_generation
                    if exc.handle is not None:
                        session_id = exc.handle.opencode_session_id
                if isinstance(exc, contained_execution.SessionCleanupError):
                    session_id = exc.handle.opencode_session_id
                terminal = classify_spawn_terminal_state(containment_available=False)

        if terminal is None and execution is not None and checkout is not None:
            proposal_result = None
            if execution.exit_code == 0 and not execution.timed_out and not execution.output_overflow:
                proposal_result = build_opencode_proposal(
                    checkout.path, scrubber=scrubber, base_tree=seed_tree,
                )
            auth_required = execution.session_error is not None and execution.session_error.type == "provider.auth"
            terminal = classify_spawn_terminal_state(
                timed_out=execution.timed_out,
                output_overflow=execution.output_overflow,
                exit_code=execution.exit_code,
                worker_authentication_required=auth_required,
                proposal_result=proposal_result,
            )
    except BaseException as exc:
        from .worklink.backends.base import SessionAdmissionCancelled
        if isinstance(exc, SessionAdmissionCancelled):
            retain_checkout = exc.uncertain
            launch_generation = exc.launch_generation
        if isinstance(exc, contained_execution.SessionCancelledError):
            session_id = exc.handle.opencode_session_id
            retain_checkout = not exc.verified
        if session_id is not None or retain_checkout:
            descriptor = os.open("manifest.json", os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW,
                                 dir_fd=artifact_directory_fd)
            try:
                with os.fdopen(descriptor, "w", closefd=False) as stream:
                    json.dump({"schema_version": 2, "run_id": run_id, "launcher": "mimir.spawn_open_code",
                               "status": "interrupted", "reason_code": "cancelled",
                               "opencode_session_id": session_id,
                               "launch_generation": launch_generation,
                               "admission_state": "uncertain" if session_id is None else "admitted",
                               "cancellation_state": "unverified" if retain_checkout else "verified"}, stream)
            finally:
                os.close(descriptor)
        os.close(artifact_directory_fd)
        artifact_directory_fd = -1
        if execution is None and session_id is None and not retain_checkout:
            await _spawn_release_rate_slot(guard, rate_token)
        raise
    finally:
        if checkout is not None:
            try:
                if retain_checkout:
                    checkout.close(retain=True)
                    raise RuntimeError("session quiescence unverified; checkout retained")
                checkout.close()
            except (OSError, RuntimeError):
                event = cleanup_failure_event(run_id)
                await event_logger.safe_log_event(
                    event["type"], run_id=event["run_id"], reason_code=event["reason_code"],
                )
                log.warning("spawn_open_code cleanup failed")

    assert terminal is not None
    if execution is None and session_id is None and not retain_checkout:
        await _spawn_release_rate_slot(guard, rate_token)
    manifest = {
        "schema_version": 2, "run_id": run_id, "launcher": "mimir.spawn_open_code",
        "name": scrubber.scrub_text(name) if name is not None else None,
        "model": scrubber.scrub_text(invocation.model) if invocation is not None else None,
        "agent": scrubber.scrub_text(agent) if agent is not None else None,
        "status": terminal.status, "reason_code": terminal.reason_code,
        "exit_code": terminal.exit_code, "seed_tree": seed_tree,
        "timed_out": bool(execution and execution.timed_out),
        "output_overflow": bool(execution and execution.output_overflow),
        "stdout_dropped_bytes": 0,
        "stderr_dropped_bytes": 0,
        "opencode_session_id": session_id,
        "launch_generation": launch_generation,
        "admission_state": "uncertain" if retain_checkout and session_id is None else "admitted" if session_id else "refused",
    }
    artifact_handle = run_id
    try:
        directory_state = os.stat(artifact_directory, follow_symlinks=False)
        anchored_state = os.fstat(artifact_directory_fd)
        if not stat.S_ISDIR(directory_state.st_mode) or not os.path.samestat(directory_state, anchored_state):
            raise OSError("artifact directory identity changed")

        def write_existing_artifact(artifact_name: str, document: bytes) -> None:
            descriptor = os.open(
                artifact_name, os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW,
                dir_fd=artifact_directory_fd,
            )
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise OSError("artifact is not a regular file")
                with os.fdopen(descriptor, "wb", closefd=False) as destination:
                    destination.write(document)
            finally:
                os.close(descriptor)

        write_existing_artifact("prompt.md", scrubber.scrub_text(prompt).encode("utf-8"))
        write_existing_artifact("stdout.txt", scrubber.scrub_text(stdout).encode("utf-8"))
        write_existing_artifact("stderr.txt", scrubber.scrub_text(stderr).encode("utf-8"))
        scrubbed_manifest = json.loads(
            scrubber.scrub_text(json.dumps(manifest, ensure_ascii=False))
        )
        write_existing_artifact(
            "manifest.json",
            (json.dumps(scrubbed_manifest, ensure_ascii=True, sort_keys=True) + "\n").encode("utf-8"),
        )
        if terminal.proposal is not None:
            proposal_document = (
                json.dumps(terminal.proposal.as_dict(), ensure_ascii=True, sort_keys=True) + "\n"
            ).encode("utf-8")
            proposal_descriptor = os.open(
                ".proposal.json.tmp",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600, dir_fd=artifact_directory_fd,
            )
            try:
                with os.fdopen(proposal_descriptor, "wb", closefd=False) as destination:
                    destination.write(proposal_document)
            finally:
                os.close(proposal_descriptor)
            os.replace(
                ".proposal.json.tmp", "proposal.json",
                src_dir_fd=artifact_directory_fd, dst_dir_fd=artifact_directory_fd,
            )
    except (OSError, RuntimeError, ValueError):
        log.warning("spawn_open_code artifact write failed")
        for failed_proposal_name in ("proposal.json", ".proposal.json.tmp"):
            try:
                os.unlink(failed_proposal_name, dir_fd=artifact_directory_fd)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        terminal = classify_spawn_terminal_state(artifact_available=False)
        artifact_handle = None
    finally:
        os.close(artifact_directory_fd)

    await event_logger.safe_log_event(
        terminal.event_type, run_id=run_id, status=terminal.status,
        reason_code=terminal.reason_code,
    )
    safe_stdout = scrubber.scrub_text(stdout).strip()[:2000]
    safe_stderr = scrubber.scrub_text(stderr).strip()[:500]
    if terminal.status == "configuration_refused":
        safe_stderr = "OpenCode configuration unavailable"
    elif terminal.status == "authentication_refused":
        safe_stderr = "OpenCode authentication unavailable"
    elif terminal.status == "prompt_refused":
        safe_stderr = "Prompt contains a sensitive controller path"
    elif terminal.status == "containment_unavailable":
        safe_stderr = "Contained execution unavailable"
    return json.dumps({
        "run_id": run_id, "status": terminal.status,
        "reason_code": terminal.reason_code, "exit_code": terminal.exit_code,
        "stdout": safe_stdout, "result": safe_stdout, "stderr": safe_stderr,
        "artifact_dir": artifact_handle, "name": name,
        "proposal": terminal.proposal.as_dict() if terminal.proposal else None,
        "opencode_session_id": manifest["opencode_session_id"],
    }, indent=2)


@tool
async def spawn_open_code(
    prompt: str,
    cwd: str | None = None,
    timeout_s: int = 1800,
    name: str | None = None,
    model: str | None = None,
    agent: str | None = None,
    artifact_root: str | None = None,
) -> str:
    """Run OpenCode in a fresh contained checkout and return a bounded proposal."""
    from .contained_checkout import create_opencode_checkout
    from .contained_execution import execute_contained_session

    return await _spawn_open_code_impl(
        prompt, cwd, timeout_s, name, model, agent, artifact_root,
        contained_runner=execute_contained_session,
        checkout_factory=create_opencode_checkout,
        execution_identifier=str(uuid.uuid4()),
    )
