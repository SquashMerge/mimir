"""Exclusive, durable single-input sessions owned by the Worklink executor."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
from dataclasses import asdict, dataclass, replace
import json
import math
import os
import posixpath
from pathlib import Path
import re
import sys
import time
import uuid

from .backends.base import SessionError, SessionOutcome, SessionRequest
from .daemon_client import DaemonClientError, OpenCodeDaemonClient


def _number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(value)


def _identifier(value: object, prefix: str) -> bool:
    return isinstance(value, str) and re.fullmatch(prefix + r"_[A-Za-z0-9_-]{1,128}", value) is not None


def _structured_error(value: object) -> dict:
    if (not isinstance(value, dict) or not {"type", "message"} <= value.keys()
            or value.keys() - {"type", "message", "status", "response", "retryAfterMs"}
            or not isinstance(value["type"], str) or not isinstance(value["message"], str)):
        raise DaemonClientError("invalid structured error")
    if "status" in value and (type(value["status"]) is not int or not 100 <= value["status"] <= 599):
        raise DaemonClientError("invalid structured error")
    if "response" in value and (not isinstance(value["response"], dict)
            or set(value["response"]) != {"body"} or not isinstance(value["response"]["body"], str)):
        raise DaemonClientError("invalid structured error")
    if "retryAfterMs" in value and (type(value["retryAfterMs"]) is not int or not 0 <= value["retryAfterMs"] <= 9007199254740991):
        raise DaemonClientError("invalid structured error")
    return value


def _message(value: object, *, session_id: str | None = None) -> dict:
    if (not isinstance(value, dict) or not _identifier(value.get("id"), "msg")
            or not isinstance(value.get("time"), dict) or not _number(value["time"].get("created"))
            or not isinstance(value.get("type"), str)):
        raise DaemonClientError("invalid session message")
    if session_id is not None:
        if (set(value) != {"id", "time", "type", "sessionID", "payload", "delivery"}
                or set(value["time"]) != {"created"}
                or value.get("sessionID") != session_id or value["type"] != "user"
                or not isinstance(value.get("payload"), dict)
                or not isinstance(value["payload"].get("text"), str)
                or value.get("delivery") not in {"queue", "steer"}):
            raise DaemonClientError("invalid session inbox")
    elif value["type"] in {"user", "idle"}:
        allowed = {"id", "time", "type", "metadata"} | (
            {"text", "files", "agents", "skills"} if value["type"] == "user" else {"outcome", "error"})
        if value.keys() - allowed or set(value["time"]) != {"created"}:
            raise DaemonClientError("invalid outcome history")
    if value["type"] == "user" and session_id is None and not isinstance(value.get("text"), str):
        raise DaemonClientError("invalid user history")
    elif value["type"] == "idle" and value.get("outcome") not in {"succeeded", "failed", "interrupted"}:
        raise DaemonClientError("invalid idle history")
    elif value["type"] == "assistant":
        if not isinstance(value.get("content"), list):
            raise DaemonClientError("invalid assistant history")
        for part in value["content"]:
            if (not isinstance(part, dict) or not isinstance(part.get("type"), str)
                    or (part["type"] == "text" and not isinstance(part.get("text"), str))):
                raise DaemonClientError("invalid assistant content")
    if "error" in value:
        _structured_error(value["error"])
    return value


@dataclass(frozen=True)
class ControllerRecord:
    version: int
    operation_id: str
    issue_id: int
    attempt: int
    kind: str
    opencode_session_id: str
    launch_generation: str
    checkout_path: str
    checkout_device: int
    checkout_inode: int
    baseline_seq: int
    last_seq: int
    baseline_message_ids: list[str]
    input_id: str | None
    admission_state: str
    execution_state: str
    stop_requested: bool
    cancellation_state: str
    deadline_at_ms: int

    def validate(self) -> None:
        if (not isinstance(self.operation_id, str)
                or not re.fullmatch(r"[0-9a-f-]{36}", self.operation_id)
                or not isinstance(self.launch_generation, str)
                or not isinstance(self.checkout_path, str)
                or not isinstance(self.baseline_message_ids, list)):
            raise ValueError("invalid session controller binding")
        if (type(self.version) is not int or self.version != 1
                or self.kind not in {"leaf", "spawn", "epic"}
                or self.admission_state not in {"created", "dispatching", "admitted", "uncertain"}
                or self.execution_state not in {"pending", "started", "succeeded", "failed", "interrupted", "incomplete"}
                or self.cancellation_state not in {"none", "requested", "verified", "unverified"}):
            raise ValueError("invalid session controller state")
        for value in (self.issue_id, self.attempt, self.checkout_inode, self.deadline_at_ms):
            if type(value) is not int or value <= 0:
                raise ValueError("invalid session controller binding")
        for value in (self.checkout_device, self.baseline_seq, self.last_seq):
            if type(value) is not int or value < 0:
                raise ValueError("invalid session controller sequence")
        if (self.last_seq < self.baseline_seq or type(self.stop_requested) is not bool
                or not Path(self.checkout_path).is_absolute()
                or not re.fullmatch(r"ses_[A-Za-z0-9_-]{1,128}", self.opencode_session_id)
                or not self.launch_generation or len(self.launch_generation) > 128
                or len(self.baseline_message_ids) > 16
                or len(set(self.baseline_message_ids)) != len(self.baseline_message_ids)):
            raise ValueError("invalid session controller identity")
        for value in [*self.baseline_message_ids, *([self.input_id] if self.input_id is not None else [])]:
            if not isinstance(value, str) or not re.fullmatch(r"msg_[A-Za-z0-9_-]{1,128}", value):
                raise ValueError("invalid session message identity")
        if ((self.admission_state == "admitted" and not self.input_id)
                or (self.cancellation_state != "none" and not self.stop_requested)
                or (self.stop_requested and self.cancellation_state == "none")
                or (self.admission_state == "created" and self.input_id is not None)):
            raise ValueError("inconsistent session controller state")


def session_permissions(request: SessionRequest) -> list[dict[str, str]]:
    rules = [{"action": "external_directory", "resource": "*", "effect": "deny"}]
    if request.kind == "leaf":
        if not request.shell_allowlist or any(not rule.strip() or rule.strip() == "*" for rule in request.shell_allowlist):
            raise ValueError("leaf requires a bounded shell allowlist")
        rules.append({"action": "shell", "resource": "*", "effect": "deny"})
        rules.extend({"action": "shell", "resource": rule, "effect": "allow"} for rule in request.shell_allowlist)
    elif request.kind == "spawn":
        rules.append({"action": "shell", "resource": "*", "effect": "allow"})
    elif request.kind == "epic":
        rules.extend({"action": "read", "resource": rule, "effect": "deny"}
                     for rule in (".env", "**/.env", "**/.env.*"))
    else:
        raise ValueError("invalid session kind")
    return rules


class SessionController:
    def __init__(self, client: OpenCodeDaemonClient, path: Path, record: ControllerRecord) -> None:
        record.validate()
        self.client = client
        self.path = path
        self._record_path = path
        self._retired = False
        self.record = record
        self._lock = asyncio.Lock()
        self.sensitive_values: tuple[str, ...] = ()
        self._failure_error: SessionError | None = None
        self._cancelling = False

    async def _bounded(self, operation):
        if self._cancelling:
            return await operation
        remaining = (self.record.deadline_at_ms - time.time() * 1000) / 1000
        if remaining <= 0:
            operation.close()
            raise TimeoutError("session deadline exceeded")
        async with asyncio.timeout(remaining):
            result = await operation
        if time.time() * 1000 >= self.record.deadline_at_ms:
            raise TimeoutError("session deadline exceeded")
        return result

    def _error(self, raw: dict) -> SessionError:
        raw = _structured_error(raw)
        retry = raw.get("retryAfterMs")
        return SessionError(self._redacted(raw["type"], 128), self._redacted(raw["message"], 2048),
                            self._redacted(raw.get("response", {}).get("body", ""), 8192),
                            retry if type(retry) is int and 0 < retry <= 8 * 86400000 else None)

    async def _inbox(self) -> list[dict]:
        response = await self._bounded(self.client.inbox(self.record.opencode_session_id))
        if not isinstance(response, dict) or not isinstance(response.get("data"), list):
            raise DaemonClientError("invalid session inbox")
        items = [_message(item, session_id=self.record.opencode_session_id) for item in response["data"]]
        if len({item["id"] for item in items}) != len(items):
            raise DaemonClientError("duplicate inbox identity")
        return items

    def _redacted(self, value: str, bound: int) -> str:
        from mimir.redaction import redact_text

        for material in sorted(self.sensitive_values, key=len, reverse=True):
            if material:
                value = value.replace(material, "<redacted>")
        return redact_text(value).encode("utf-8")[:bound].decode("utf-8", "ignore")

    def _persist(self, **changes: object) -> None:
        from .daemon_service import atomic_json

        record = replace(self.record, **changes)
        record.validate()
        raw = json.dumps(asdict(record), allow_nan=False).encode()
        if len(raw) > 16384:
            raise ValueError("session controller exceeds size limit")
        atomic_json(self._record_path, asdict(record), uid=os.geteuid(), gid=os.getegid(), mode=0o600)
        self.record = record

    @classmethod
    def recover(cls, client: OpenCodeDaemonClient, path: Path, *, generation: str, checkout: Path) -> SessionController:
        from .daemon_service import read_json

        record = ControllerRecord(**read_json(path, uid=os.geteuid(), mode=0o600))
        controller = cls(client, path, record)
        controller.check_binding(generation, checkout)
        return controller

    def check_binding(self, generation: str, checkout: Path) -> None:
        info = checkout.stat()
        if (not checkout.is_absolute() or checkout.resolve() != checkout
                or generation != self.record.launch_generation
                or str(checkout) != self.record.checkout_path
                or (info.st_dev, info.st_ino) != (self.record.checkout_device, self.record.checkout_inode)):
            raise ValueError("session checkout or generation mismatch")

    @classmethod
    def recover_owned(cls, client: OpenCodeDaemonClient, path: Path, *, session_id: str,
                      generation: str, checkout: Path) -> SessionController:
        """Resolve the current controller or its durable verified retirement."""
        if not isinstance(session_id, str) or re.fullmatch(r"ses_[A-Za-z0-9_-]{1,128}", session_id) is None:
            raise ValueError("invalid session identity")
        retired_path = path.with_name(f"{path.stem}.{session_id}.retired.json")
        try:
            controller = cls.recover(client, retired_path, generation=generation, checkout=checkout)
        except FileNotFoundError:
            controller = cls.recover(client, path, generation=generation, checkout=checkout)
            if controller.record.opencode_session_id != session_id:
                raise ValueError("session ownership mismatch")
            return controller
        if controller.record.opencode_session_id != session_id or controller.record.cancellation_state != "verified":
            raise ValueError("invalid session retirement witness")
        controller._retired = True
        return controller

    @classmethod
    async def create(cls, client: OpenCodeDaemonClient, path: Path, *, request: SessionRequest,
                     issue_id: int, attempt: int, generation: str, checkout: Path,
                     environment: dict[str, str], timeout_s: int) -> SessionController:
        if not checkout.is_absolute() or checkout.resolve() != checkout or not checkout.is_dir():
            raise ValueError("session requires a verified absolute checkout")
        if type(timeout_s) is not int or timeout_s <= 0 or path.exists():
            raise ValueError("invalid session operation")
        if request.kind == "epic" and re.fullmatch(
            rf"--autonomous --max-retries [0-9]+ chainlink-{issue_id}", request.text
        ) is None:
            raise ValueError("invalid feature command")
        environment = dict(environment)
        model_spec = f"{request.model['providerID']}/{request.model['id']}"
        if "variant" in request.model:
            model_spec += "#" + request.model["variant"]
        environment.update(MIMIR_MODEL_SPEC=model_spec, MIMIR_SPAWN_DEPTH=str(request.spawn_depth),
                           MIMIR_WORKLINK_GATE="1")
        info = checkout.stat()
        deadline = int(time.time() * 1000) + timeout_s * 1000
        async with asyncio.timeout(timeout_s):
            response = await client.create_session(directory=str(checkout), model=request.model,
                                                   permissions=session_permissions(request),
                                                   **({"agent": request.agent} if request.agent is not None else {}))
        sid = response["data"]["id"]
        record = ControllerRecord(1, str(uuid.uuid4()), issue_id, attempt, request.kind, sid,
                                  generation, str(checkout), info.st_dev, info.st_ino, 0, 0, [],
                                  None, "created", "pending", False, "none",
                                  deadline)
        controller = cls(client, path, record)
        controller.sensitive_values = tuple(environment[k] for k in ("GH_TOKEN", "GITHUB_TOKEN") if k in environment)
        controller._persist()
        try:
            await controller._read_log(baseline=True)
            messages = await controller._messages()
            inbox = await controller._inbox()
            if len(messages) > 16 or inbox or any(m.get("type") == "user" for m in messages) or controller.record.execution_state != "pending":
                raise DaemonClientError("session is not fresh")
            controller._persist(baseline_message_ids=[m["id"] for m in messages])
            await controller._bounded(client.set_environment(sid, environment))
        except TimeoutError:
            await controller.cancel()
            return controller
        except DaemonClientError:
            if controller.record.execution_state == "pending":
                controller._persist(execution_state="incomplete")
            return controller
        controller._persist(admission_state="dispatching")
        try:
            if request.kind == "epic":
                await controller._bounded(client.command(sid, name="feature", text=request.text))
            else:
                admitted = await controller._bounded(client.prompt(sid, request.text, resume=True))
                _message(admitted["data"], session_id=sid)
                controller._persist(input_id=admitted["data"]["id"], admission_state="admitted")
        except TimeoutError:
            controller._persist(admission_state="uncertain")
            await controller.cancel()
        except DaemonClientError as exc:
            controller._persist(admission_state="uncertain", **(
                {"execution_state": "incomplete"} if exc.status == 401 else {}
            ))
        return controller

    async def _messages(self) -> list[dict]:
        messages = []
        cursor = None
        size = 0
        while True:
            page = await self._bounded(self.client.messages(self.record.opencode_session_id, cursor=cursor,
                                              order="asc" if cursor is None else None, limit=100))
            if (not isinstance(page, dict) or not isinstance(page.get("data"), list)
                    or not isinstance(page.get("cursor"), dict)
                    or set(page["cursor"]) != {"previous", "next"}
                    or any(v is not None and (not isinstance(v, str) or len(v.encode()) > 4096)
                           for v in page["cursor"].values())):
                raise DaemonClientError("invalid message page")
            messages.extend(_message(item) for item in page["data"])
            if len({item["id"] for item in messages}) != len(messages):
                raise DaemonClientError("duplicate history identity")
            size += len(json.dumps(page).encode())
            if len(messages) > 4096 or size > 64 * 1024 * 1024:
                raise DaemonClientError("session history exceeds bound")
            next_cursor = page["cursor"].get("next")
            if next_cursor is None:
                return messages
            if next_cursor == cursor:
                raise DaemonClientError("repeated message cursor")
            cursor = next_cursor

    async def _read_log(self, *, baseline: bool = False) -> None:
        sid = self.record.opencode_session_id
        replay = self.record.execution_state == "failed"
        cursor = self.record.baseline_seq if replay else self.record.last_seq
        state = "pending" if replay else self.record.execution_state
        stream = self.client.log(sid, after=cursor, follow=True)
        try:
            while True:
                try:
                    frame = await self._bounded(anext(stream))
                except StopAsyncIteration:
                    break
                event = frame.data
                if not isinstance(event, dict):
                    raise DaemonClientError("invalid session log item")
                if event.get("type") == "log.synced":
                    if (set(event) != {"type", "aggregateID", "seq"}
                            or event.get("aggregateID") != sid or type(event.get("seq")) is not int
                            or not max(cursor, self.record.last_seq) <= event["seq"] <= 9007199254740991):
                        raise DaemonClientError("invalid log watermark")
                    if replay and self._failure_error is None:
                        raise DaemonClientError("missing durable failure witness")
                    if baseline:
                        self._persist(baseline_seq=event["seq"], last_seq=event["seq"])
                    return
                durable = event.get("durable", {})
                if not isinstance(durable, dict) or not isinstance(event.get("data"), dict):
                    raise DaemonClientError("invalid durable session event")
                seq = durable.get("seq")
                if durable.get("aggregateID") != sid or event.get("data", {}).get("sessionID") != sid:
                    raise DaemonClientError("foreign session event")
                if (not _identifier(event.get("id"), "evt") or not _number(event.get("created"))
                        or event.keys() - {"id", "created", "type", "durable", "data", "metadata", "location"}
                        or not isinstance(event.get("type"), str) or not event["type"].startswith("session.")
                        or set(durable) != {"aggregateID", "seq", "version"}
                        or type(durable["version"]) is not int or durable["version"] != 1
                        or type(seq) is not int or not 1 <= seq <= 9007199254740991):
                    raise DaemonClientError("invalid durable session event")
                kind = event["type"]
                data = event["data"]
                if kind.startswith("session.execution."):
                    fields = {"sessionID"} | ({"error"} if kind == "session.execution.failed" else
                                             {"reason"} if kind == "session.execution.interrupted" else set())
                    if kind not in {"session.execution.started", "session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"} or set(data) != fields:
                        raise DaemonClientError("invalid execution event")
                    if kind == "session.execution.failed":
                        self._failure_error = self._error(data["error"])
                    if kind == "session.execution.interrupted" and data["reason"] not in {"user", "shutdown", "superseded", "inactivity"}:
                        raise DaemonClientError("invalid execution interruption")
                if kind.startswith("session.inbox.") and not _identifier(data.get("inboxID"), "msg"):
                    raise DaemonClientError("invalid inbox event")
                if seq <= cursor:
                    continue
                cursor = seq
                if (event.get("type") == "session.inbox.cancelled"
                        and event.get("data", {}).get("inboxID") == self.record.input_id
                        and self.record.stop_requested and state == "pending"):
                    state = "interrupted"
                if kind == "session.execution.started":
                    state = "started"
                elif kind in {"session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"}:
                    if state != "started":
                        raise DaemonClientError("execution terminal without start")
                    state = kind[len("session.execution."):]
                if seq > self.record.last_seq:
                    self._persist(last_seq=seq, execution_state=state)
        finally:
            await stream.aclose()
        raise DaemonClientError("missing log watermark")

    async def observe(self, *, stdout_limit: int = 64 * 1024 * 1024) -> SessionOutcome:
        try:
            return await self._bounded(self._observe(stdout_limit=stdout_limit))
        except TimeoutError:
            await self.cancel()
            return SessionOutcome(self.record.opencode_session_id, "incomplete", self.record.cancellation_state,
                                   SessionError("timeout", "Worklink session deadline exceeded"))

    async def _observe(self, *, stdout_limit: int) -> SessionOutcome:
        async with self._lock:
            await self._read_log()
            messages = await self._messages()
            inbox = await self._inbox()
            users = {m["id"] for m in [*messages, *inbox] if m.get("type") == "user"
                     and m["id"] not in self.record.baseline_message_ids}
            if len(users) > 1 or (self.record.input_id and users and users != {self.record.input_id}):
                raise DaemonClientError("ambiguous session admission")
            if users and self.record.admission_state == "created":
                raise DaemonClientError("unowned input in fresh session")
            if users and not self.record.input_id:
                self._persist(input_id=next(iter(users)), admission_state="admitted")
            state = self.record.execution_state
            if state == "succeeded":
                history = [m for m in messages if m["id"] not in self.record.baseline_message_ids]
                admitted = next((i for i, m in enumerate(history) if m["id"] == self.record.input_id and m.get("type") == "user"), None)
                if admitted is None or not any(m.get("type") == "idle" and m.get("outcome") == "succeeded" for m in history[admitted + 1:]):
                    state = "incomplete"
            error = self._failure_error if state == "failed" else None
            if state == "incomplete":
                error = SessionError("backend_error", "OpenCode session protocol witness incomplete")
            for message in reversed(messages):
                if error is None and message.get("type") == "idle" and isinstance(message.get("error"), dict):
                    raw = message["error"]
                    error = self._error(raw)
                    break
            chunks = []
            size = 0
            overflow = False
            for message in messages:
                if message.get("type") != "assistant":
                    continue
                for part in message.get("content", []):
                    if part.get("type") != "text" or not isinstance(part.get("text"), str):
                        continue
                    raw = self._redacted(part["text"], 1024 * 1024)
                    size += len(raw.encode()) + (1 if chunks else 0)
                    if size > stdout_limit:
                        overflow = True
                        break
                    chunks.append(raw)
                if overflow:
                    break
            return SessionOutcome(self.record.opencode_session_id, state, self.record.cancellation_state,
                                  error, "\n".join(chunks), overflow)

    async def cancel(self, timeout_s: float = 25) -> bool:
        async with self._lock:
            if self._retired:
                return True
            self._cancelling = True
            try:
                self._persist(stop_requested=True, cancellation_state="requested")
                async with asyncio.timeout(min(timeout_s, 25)):
                    while True:
                        try:
                            await self._read_log()
                            messages = await self._messages()
                            inbox = await self._inbox()
                            users = {m["id"] for m in [*messages, *inbox] if m.get("type") == "user" and m["id"] not in self.record.baseline_message_ids}
                            if len(users) > 1 or (self.record.input_id and users - {self.record.input_id}):
                                raise DaemonClientError("ambiguous cancellation ownership")
                            if users and self.record.admission_state == "created":
                                raise DaemonClientError("unowned input in fresh session")
                            if users and not self.record.input_id:
                                self._persist(input_id=next(iter(users)), admission_state="admitted")
                            pending = [m for m in inbox if m["id"] == self.record.input_id]
                            for item in pending:
                                await self.client.delete_inbox(self.record.opencode_session_id, item["id"])
                            if not pending and self.record.input_id and self.record.execution_state in {"succeeded", "failed", "interrupted"}:
                                self._persist(cancellation_state="verified")
                                return True
                            if (self.record.admission_state == "created" and not inbox and not users
                                    and self.record.last_seq == self.record.baseline_seq
                                    and self.record.execution_state in {"pending", "incomplete"}):
                                self._persist(cancellation_state="verified")
                                return True
                            await self.client.interrupt(self.record.opencode_session_id)
                        except DaemonClientError as exc:
                            if exc.reason not in {"transport failure", "missing log watermark"}:
                                raise
                        await asyncio.sleep(0.05)
            except (TimeoutError, DaemonClientError):
                self._persist(cancellation_state="unverified")
                return False
            finally:
                self._cancelling = False

    async def release(self) -> None:
        """Free the admission slot without discarding verified retirement proof."""
        if self._retired:
            return
        if not await self.cancel():
            raise RuntimeError("session cleanup unverified; retaining controller and checkout")
        retired_path = self.path.with_name(f"{self.path.stem}.{self.record.opencode_session_id}.retired.json")
        self.path.replace(retired_path)
        self._record_path = retired_path
        self._retired = True
        directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def session_environment(pass_env: object, *, home: Path, git_settings: object,
                        credentials: dict[str, str]) -> dict[str, str]:
    from mimir.otel_credentials import is_sensitive_otlp_env
    from .compute import _validate_dynamic_worker_name, _validate_worker_value

    if not isinstance(pass_env, dict) or len(pass_env) > 128:
        raise ValueError("invalid session pass_env")
    environment = {
        "PATH": "/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin",
        "USER": "worklink", "LOGNAME": "worklink", "SHELL": "/bin/sh",
        "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "HOME": str(home),
        "TMPDIR": str(home / "tmp"), "MIMIR_WORKLINK_GATE": "1", "MIMIR_SPAWN_DEPTH": "1",
    }
    for name, value in pass_env.items():
        _validate_dynamic_worker_name(name)
        _validate_worker_value(name, value)
        for candidate in re.findall(r"/[^\s,;:]+", value):
            normalized = posixpath.normpath(candidate)
            if normalized == "/home/mimir" or normalized.startswith("/home/mimir/"):
                raise ValueError("session pass_env refers to controller home")
        if (name.upper() in environment or name.upper().startswith(("XDG_", "API_"))
                or is_sensitive_otlp_env(name, value)
                or name.upper().startswith(("OPENAI_", "CODEX_", "ANTHROPIC_", "CLAUDE_", "OPENCODE_",
                                             "GOOGLE_", "GEMINI_", "GROQ_", "MINIMAX_", "OPENROUTER_", "VOYAGE_", "OTEL_", "OTLP_"))
                or any(part in name.upper() for part in ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "BASE_URL"))):
            raise ValueError("sensitive session pass_env refused")
        environment[name] = value
    if not isinstance(git_settings, (tuple, list)) or len(git_settings) > 32:
        raise ValueError("invalid session git settings")
    settings = [("safe.directory", str(home)), *git_settings]
    for pair in settings:
        if (not isinstance(pair, (tuple, list)) or len(pair) != 2
                or any(not isinstance(v, str) or "\x00" in v or "\n" in v for v in pair)):
            raise ValueError("invalid session git setting")
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
                       GIT_CONFIG_COUNT=str(len(settings)))
    for index, (key, value) in enumerate(settings):
        environment[f"GIT_CONFIG_KEY_{index}"] = key
        environment[f"GIT_CONFIG_VALUE_{index}"] = value
    environment.update(credentials)
    return environment


async def _executor_operation(request: dict) -> dict:
    from .daemon_config import DAEMON_ROOT
    from .daemon_health import load_api
    from .daemon_service import RuntimePaths, read_json
    from .identities import assert_worklink_identity, get_identities

    identities = get_identities()
    assert_worklink_identity(identities)
    paths = RuntimePaths()
    api = load_api(paths, uid=identities.worklink_uid)
    key = hashlib.sha256(f"{request['path']}:{request['issue']}:{request['attempt']}".encode()).hexdigest()
    root = Path(DAEMON_ROOT) / "sessions"
    lock = os.open(root / f"{key}.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        recovering_admission = request["op"] == "session_observe" and request["opencode_session_id"] is None
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if recovering_admission:
                return {"admission_state": "busy"}
            raise
        async with OpenCodeDaemonClient(base_url=api["url"], username=api["username"], password=api["password"], timeout=5) as client:
            path = root / f"{key}.json"
            checkout = Path(request["path"])
            info = checkout.stat()
            if (info.st_dev, info.st_ino) != (request["device"], request["inode"]):
                raise ValueError("session checkout identity changed")
            home = root / f"{key}.home"
            if recovering_admission:
                try:
                    path.lstat()
                except FileNotFoundError:
                    return {"admission_state": "absent"}
                controller = SessionController.recover(client, path, generation=request["generation"], checkout=checkout)
                if (controller.record.issue_id, controller.record.attempt) != (request["issue"], request["attempt"]):
                    raise ValueError("session attempt mismatch")
                return {"admission_state": "retained", "opencode_session_id": controller.record.opencode_session_id}
            if request["op"] == "session_create":
                launch = read_json(paths.daemon / "launch.json", uid=identities.worklink_uid, mode=0o600)
                if launch.get("launch_generation") != request["generation"]:
                    raise ValueError("session daemon generation is stale")
                if path.exists():
                    retained = SessionController.recover(client, path, generation=request["generation"], checkout=checkout)
                    return {"admission_state": "conflict", "opencode_session_id": retained.record.opencode_session_id}
                home.mkdir(mode=0o700, exist_ok=True)
                (home / "tmp").mkdir(mode=0o700, exist_ok=True)
                env = session_environment(request["pass_env"], home=home, git_settings=request["git_settings"],
                                          credentials={k: os.environ[k] for k in ("GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY") if k in os.environ})
                env["GIT_CONFIG_VALUE_0"] = str(checkout)
                controller = await SessionController.create(
                    client, path, request=SessionRequest(**request["request"]), issue_id=request["issue"],
                    attempt=request["attempt"], generation=request["generation"], checkout=checkout,
                    environment=env, timeout_s=request["timeout_s"],
                )
                return {"opencode_session_id": controller.record.opencode_session_id}
            controller = SessionController.recover_owned(
                client, path, session_id=request["opencode_session_id"],
                generation=request["generation"], checkout=checkout)
            controller.sensitive_values = (api["password"], *(os.environ[k] for k in ("GH_TOKEN", "GITHUB_TOKEN") if k in os.environ))
            if controller.record.opencode_session_id != request["opencode_session_id"]:
                raise ValueError("session ownership mismatch")
            if request["op"] == "session_observe":
                outcome = asdict(await controller.observe(stdout_limit=request["stdout_limit"]))
                raw = outcome.pop("stdout").encode()
                output_fd = int(os.environ["WORKLINK_SESSION_OUTPUT_FD"])
                os.ftruncate(output_fd, 0)
                os.lseek(output_fd, 0, os.SEEK_SET)
                while raw:
                    written = os.write(output_fd, raw)
                    if written <= 0:
                        raise RuntimeError("incomplete session output write")
                    raw = raw[written:]
                outcome["error_detail"] = outcome.pop("error")
                return outcome
            if request["op"] == "session_cancel":
                await controller.cancel()
                return {"cancellation_state": controller.record.cancellation_state}
            if request["op"] == "session_release":
                already_retired = controller._retired
                await controller.release()
                from mimir._rmtree import rmtree_missing_ok
                if not already_retired:
                    rmtree_missing_ok(home)
                return {"released": True}
            raise ValueError("invalid session operation")
    finally:
        os.close(lock)


def main() -> int:
    try:
        raw = sys.stdin.buffer.read(262145)
        if len(raw) > 262144:
            return 1
        response = asyncio.run(_executor_operation(json.loads(raw)))
        sys.stdout.write(json.dumps(response, allow_nan=False))
        return 0
    except DaemonClientError as exc:
        if exc.reason in {"transport failure", "missing log watermark"}:
            sys.stdout.write('{"transport_lost":true}')
            return 0
        return 1
    except Exception:
        # Remote/provider bodies and credential-bearing local errors never cross
        # the helper's stdout protocol or stderr boundary.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
