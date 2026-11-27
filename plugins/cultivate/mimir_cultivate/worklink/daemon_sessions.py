"""Exclusive, durable single-input sessions owned by the Worklink executor."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
from dataclasses import asdict, dataclass, field, replace
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


# Leave ample time for the operation itself before the executor kills the helper.
CONTROLLER_LOCK_TIMEOUT_S = 10.0
CONTROLLER_LOCK_RETRY_S = 0.05


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


# ``Session.Inbox.Info`` in the OpenCode 2.3.0 schema is the union of these four
# items, all with one envelope. Only ``user`` is input Worklink owns; the daemon
# enqueues the others itself (a finished background shell queues ``synthetic``
# to resume the session), and each means the session still has work to admit.
_INBOX_PAYLOAD_KEYS = {
    "user": ({"text"}, {"text", "files", "agents", "skills", "metadata"}),
    "synthetic": ({"text"}, {"text", "description", "metadata"}),
    "move": ({"projectID", "location"}, {"projectID", "location", "subpath"}),
}


def _inbox_payload(kind: object, payload: object) -> bool:
    if kind == "compaction":
        return payload is not None
    if kind not in _INBOX_PAYLOAD_KEYS or not isinstance(payload, dict):
        return False
    required, allowed = _INBOX_PAYLOAD_KEYS[kind]
    if not required <= payload.keys() or payload.keys() - allowed:
        return False
    return kind == "move" or isinstance(payload["text"], str)


def _message(value: object, *, session_id: str | None = None) -> dict:
    if (not isinstance(value, dict) or not _identifier(value.get("id"), "msg")
            or not isinstance(value.get("time"), dict) or not _number(value["time"].get("created"))
            or not isinstance(value.get("type"), str)):
        raise DaemonClientError("invalid session message")
    if session_id is not None:
        if (set(value) != {"id", "time", "type", "sessionID", "payload", "delivery"}
                or set(value["time"]) != {"created"}
                or value.get("sessionID") != session_id
                or value.get("delivery") not in {"queue", "steer"}
                or not _inbox_payload(value["type"], value.get("payload"))):
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
    cleanup_error: str | None = None
    cleanup_processes: list[list[int]] = field(default_factory=list)
    operator_resume: str = "none"

    def validate(self) -> None:
        if self.operator_resume not in {"none", "dispatching", "admitted"}:
            raise ValueError("invalid operator resume state")
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
        if self.cleanup_error is not None and (not isinstance(self.cleanup_error, str)
                                               or len(self.cleanup_error.encode()) > 8192):
            raise ValueError("invalid session cleanup diagnostic")
        if (not isinstance(self.cleanup_processes, list) or len(self.cleanup_processes) > 4096
                or any(not isinstance(w, list) or len(w) != 2
                       or any(type(v) is not int or v <= 0 for v in w) or w[0] <= 1
                       for w in self.cleanup_processes)):
            raise ValueError("invalid cleanup process witnesses")
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
    # Native subagents inherit this ruleset (Session.create), after agent rules.
    # Headless sessions must report decisions to their caller, never open a form.
    rules = [{"action": "external_directory", "resource": "*", "effect": "deny"},
             {"action": "question", "resource": "*", "effect": "deny"}]
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
        self._cancelling = False
        self._shells: list[dict] = []

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

    async def _messages(self, session_id: str | None = None) -> list[dict]:
        messages = []
        cursor = None
        size = 0
        limit = 100
        while True:
            try:
                page = await self._bounded(self.client.messages(session_id or self.record.opencode_session_id, cursor=cursor,
                                                  order="asc" if cursor is None else None, limit=limit))
            except DaemonClientError as exc:
                # A real build's history exceeds the client's per-response bound
                # long before 100 messages (tool output is large). Cursors are
                # positions, not page offsets, so re-read the same position with a
                # narrower page. A single message over the bound still fails closed.
                if exc.reason != "item too large" or limit == 1:
                    raise
                limit = max(1, limit // 4)
                continue
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
        """Advance the durable sequence watermark; never a source of execution state.

        OpenCode 2.3.0's queued-input probe returns only its ``log.synced``
        watermark. The historical 2.2.7 live-provider capture also omitted every
        ``session.execution.*`` event on follow and replay (see
        ``tests/fixtures/worklink/opencode_2_2_7/session-lifecycle.json``). Execution
        state therefore comes from the durable message history alone (see
        ``_execution_state``). Any event a daemon does deliver must still belong to
        this session, but it cannot change the observed outcome.
        """
        sid = self.record.opencode_session_id
        cursor = self.record.last_seq
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
                            or not cursor <= event["seq"] <= 9007199254740991):
                        raise DaemonClientError("invalid log watermark")
                    if baseline:
                        self._persist(baseline_seq=event["seq"], last_seq=event["seq"])
                    elif event["seq"] > self.record.last_seq:
                        self._persist(last_seq=event["seq"])
                    return
                durable = event.get("durable")
                data = event.get("data")
                if (not isinstance(durable, dict) or not isinstance(data, dict)
                        or durable.get("aggregateID") != sid or data.get("sessionID") != sid):
                    raise DaemonClientError("foreign session event")
        finally:
            await stream.aclose()
        raise DaemonClientError("missing log watermark")

    async def _owned_shells(self) -> list[dict]:
        """This session's own background shells that are still running.

        OpenCode 2 ends a turn (``idle``) while a shell the agent started in the
        background keeps running; when it completes, a ``synthetic`` message
        resumes the session. Until every owned shell has finished, ``idle`` is not
        quiescence (2026-11-16: #2149 and #2151 were stopped mid-resume).
        """
        shells = await self._bounded(self.client.running_shells(self.record.checkout_path))
        return [shell for shell in shells
                if shell.get("metadata", {}).get("sessionID") == self.record.opencode_session_id]

    async def tree_activity(self) -> dict:
        """Read durable activity for the owned driver and its entire descendant tree.

        Do not use execution state, log delivery, or controller-file mtimes as a
        heartbeat. A pending human form (including the question tool) blocks the
        tree: its creation/update must not buy another idle window (#2164).
        This read neither answers forms nor changes session state.
        """
        root = self.record.opencode_session_id
        sessions = await self._bounded(self.client.session_tree(root))
        seen: set[str] = set()
        directories: set[str] = set()
        latest = -1
        source = root
        pending_form = False
        for session in sessions:
            sid = session.get("id")
            updated = session.get("time", {}).get("updated")
            directory = session.get("location", {}).get("directory")
            if (not _identifier(sid, "ses") or sid in seen
                    or not _number(updated) or not 0 <= updated <= 253402300799999
                    or not isinstance(directory, str) or not Path(directory).is_absolute()):
                raise DaemonClientError("invalid session activity")
            seen.add(sid)
            directories.add(directory)
            if updated > latest:
                latest, source = updated, sid
            pending_form = bool(await self._bounded(self.client.pending_forms(sid))) or pending_form
        # The driver owns a lifetime lock-refresh shell. It is not work and
        # must not disable the idle watchdog; only descendant specialists'
        # running shells are activity. Ownership is metadata, not command text.
        specialists = seen - {root}
        running_shell = False
        for directory in sorted(directories):
            shells = await self._bounded(self.client.running_shells(directory))
            running_shell |= any(shell.get("metadata", {}).get("sessionID") in specialists for shell in shells)
        return {"opencode_session_id": root, "last_activity_at_ms": latest,
                "last_activity_session_id": source, "pending_form": pending_form,
                "running_shell": running_shell}

    async def _tree_shells(self, tree: list[dict]) -> list[tuple[dict, str]]:
        """List at current and historical locations, but authorize by session ID only."""
        owners = {session["id"] for session in tree}
        locations = {self.record.checkout_path}

        def location(value: object) -> None:
            directory = value.get("directory") if isinstance(value, dict) else None
            if not isinstance(directory, str) or not directory.startswith("/") or "\x00" in directory:
                raise DaemonClientError("invalid session location")
            locations.add(directory)

        for session in tree:
            location(session.get("location"))
            for message in await self._messages(session["id"]):
                if message["type"] == "location-switched":
                    location(message.get("location"))
                    if message.get("previous") is not None:
                        previous = message["previous"]
                        location(previous.get("location") if isinstance(previous, dict) else None)
        shells = []
        for directory in sorted(locations):
            for shell in await self.client.running_shells(directory):
                if shell.get("metadata", {}).get("sessionID") in owners:
                    shells.append((shell, directory))
        return shells

    async def _cancel_epic_shells(self) -> bool:
        """Stop producers before shells; a DELETE acknowledgment is not proof of exit."""
        from .daemon_processes import terminate_descendants

        tree = await self.client.session_tree(self.record.opencode_session_id)
        # Interrupt can itself stop a shell. Capture lineage before either
        # interrupt or DELETE, then persist before any destructive operation.
        await self._snapshot_shell_processes(await self._tree_shells(tree))
        await self._interrupt_tree(tree)
        shells = await self._tree_shells(tree)
        await self._snapshot_shell_processes(shells)
        self._record_remaining_shells(shells)
        for shell, directory in shells:
            try:
                await self.client.delete_shell(shell["id"], directory)
            except DaemonClientError:
                # Still try the other shells, then report the fresh witness below.
                pass
        fresh_tree = await self.client.session_tree(self.record.opencode_session_id)
        # Killing a shell can itself queue a synthetic resume. Settle those
        # deliveries AFTER deletion too, before taking the final shell witness.
        await self._snapshot_shell_processes(await self._tree_shells(fresh_tree))
        await self._interrupt_tree(fresh_tree)
        survivors = await terminate_descendants(self.record.cleanup_processes)
        self._persist(cleanup_processes=survivors)
        final_tree = await self.client.session_tree(self.record.opencode_session_id)
        remaining = await self._tree_shells(final_tree)
        self._record_remaining_shells(remaining)
        return not survivors and not remaining and {s["id"] for s in tree} == {s["id"] for s in fresh_tree} == {s["id"] for s in final_tree}

    async def _snapshot_shell_processes(self, shells: list[tuple[dict, str]]) -> None:
        from .daemon_processes import snapshot_descendants

        witnesses = {tuple(w) for w in self.record.cleanup_processes}
        for shell, _ in shells:
            witnesses.update(tuple(w) for w in snapshot_descendants(shell.get("pid")))
        self._persist(cleanup_processes=[list(w) for w in sorted(witnesses)])
        self._record_remaining_shells(shells)

    async def _interrupt_tree(self, tree: list[dict]) -> None:
        for session in tree:
            if session["id"] != self.record.opencode_session_id:
                # Child input and shell completion resumes must not restart writers.
                inbox = await self.client.inbox(session["id"])
                for item in inbox["data"]:
                    _message(item, session_id=session["id"])
                    await self.client.delete_inbox(session["id"], item["id"])
            # A queued delivery can win the DELETE race; interrupt after draining.
            await self.client.interrupt(session["id"])

    def _record_remaining_shells(self, shells: list[tuple[dict, str]]) -> None:
        detail = ", ".join([*(f"{shell['id']} ({shell['metadata']['sessionID']})" for shell, _ in shells),
                            *(f"PID {pid} (start {ticks})" for pid, ticks in self.record.cleanup_processes)])
        self._persist(cleanup_error=self._redacted("session tree cleanup incomplete: " + detail, 8192)
                      if detail else None)

    def _execution_state(self, messages: list[dict], inbox: list[dict], shells: list[dict] = (),
                         *, stable: bool = True) -> str:
        """Derive the owned input's state from durable history.

        The single admitted ``user`` message opens the execution; a trailing
        ``idle`` message closes it with its ``outcome``. Callers read the inbox
        before history, so an input absent from both was removed, not in flight.
        """
        input_id = self.record.input_id
        if input_id is None:
            return "incomplete" if self.record.execution_state == "incomplete" else "pending"
        history = [m for m in messages if m["id"] not in self.record.baseline_message_ids]
        admitted = next((i for i, m in enumerate(history)
                         if m["id"] == input_id and m.get("type") == "user"), None)
        if admitted is None:
            if any(item["id"] == input_id for item in inbox):
                return "pending"
            return "interrupted" if self.record.stop_requested else "incomplete"
        after = history[admitted + 1:]
        # A daemon-queued item (a finished shell's ``synthetic`` resume, a
        # compaction, a move) has not been admitted yet: the session will run again.
        daemon_pending = any(item["type"] != "user" for item in inbox)
        if after and after[-1].get("type") == "idle" and not shells and not daemon_pending and stable:
            return after[-1]["outcome"]
        return "started"

    async def _refresh(self) -> tuple[list[dict], list[dict]]:
        await self._read_log()
        inbox = await self._inbox()
        messages = await self._messages()
        self._shells = await self._owned_shells()
        stable = True
        if not self._shells and messages and messages[-1].get("type") == "idle":
            # The history was read before the shell listing. A shell can finish and
            # resume the session in between, so take a fresh history witness after
            # observing that no shell is left, and accept a terminal idle only if it
            # is the same idle on both sides of that observation. A resumed turn can
            # start another background shell and reach a *new* idle before the
            # re-read; the earlier empty listing says nothing about that shell.
            # The inbox snapshot above predates that listing too: a shell that finishes
            # between them queues its ``synthetic`` resume where only a fresh inbox
            # read sees it. Read inbox before history, so a resume admitted in
            # between still shows up after the idle.
            witnessed = messages[-1]["id"]
            inbox = await self._inbox()
            messages = await self._messages()
            stable = bool(messages) and messages[-1].get("id") == witnessed
        users = {m["id"] for m in [*messages, *inbox] if m.get("type") == "user"
                 and m["id"] not in self.record.baseline_message_ids}
        if len(users) > 1 or (self.record.input_id and users and users != {self.record.input_id}):
            raise DaemonClientError("ambiguous session admission")
        if users and self.record.admission_state == "created":
            raise DaemonClientError("unowned input in fresh session")
        if users and not self.record.input_id:
            self._persist(input_id=next(iter(users)), admission_state="admitted")
        state = self._execution_state(messages, inbox, self._shells, stable=stable)
        if state != self.record.execution_state:
            self._persist(execution_state=state)
        return messages, inbox

    async def observe(self, *, stdout_limit: int = 64 * 1024 * 1024) -> SessionOutcome:
        try:
            return await self._bounded(self._observe(stdout_limit=stdout_limit))
        except TimeoutError:
            await self.cancel()
            return SessionOutcome(self.record.opencode_session_id, "incomplete", self.record.cancellation_state,
                                   SessionError("timeout", "Worklink session deadline exceeded"))

    async def _pending_question(self) -> SessionError | None:
        """Read-only backstop for old sessions or a daemon permission regression.

        Forms belong to sessions, not just the observed driver. Walk every depth,
        including archived children; never grant, cancel, or answer a form here.
        """
        for session in await self._bounded(self.client.session_tree(self.record.opencode_session_id)):
            sid = session["id"]
            forms = await self._bounded(self.client.forms(sid))
            for form in forms:
                if form.get("metadata", {}).get("kind") == "question":
                    # Reserve space for both title and text in the operator-facing
                    # message (backend summaries omit error.body).
                    text = "\n".join(field.get("description", "") for field in form["fields"])
                    return SessionError(
                        "pending_question",
                        self._redacted(f"OpenCode session {sid} has a pending question ({form['id']}): "
                                       + self._redacted(form["title"], 512) + "\n"
                                       + self._redacted(text, 1024), 2048),
                        self._redacted(text, 8192),
                    )
        return None

    async def _observe(self, *, stdout_limit: int) -> SessionOutcome:
        async with self._lock:
            question = await self._pending_question()
            if question is not None:
                # Incomplete is already operator-visible through the supervisor's
                # error path. Do not persist a terminal state: the tool is waiting,
                # not quiescent, and cleanup still needs its normal witnesses.
                return SessionOutcome(self.record.opencode_session_id, "incomplete",
                                      self.record.cancellation_state, question)
            messages, _ = await self._refresh()
            state = self.record.execution_state
            error = None
            if state == "failed":
                terminal = messages[-1]
                error = (self._error(terminal["error"]) if isinstance(terminal.get("error"), dict)
                         else SessionError("backend_error", "OpenCode reported a failed execution without detail"))
            elif state == "incomplete":
                error = SessionError("backend_error", "OpenCode session protocol witness incomplete")
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
                            tree_clean = (await self._cancel_epic_shells() if self.record.kind == "epic" else True)
                            messages, inbox = await self._refresh()
                            users = {m["id"] for m in [*messages, *inbox] if m.get("type") == "user"
                                     and m["id"] not in self.record.baseline_message_ids}
                            # Our input, and the resumes our own shells queued, would
                            # restart the session after the interrupt below.
                            pending = [m for m in inbox
                                       if m["id"] == self.record.input_id or m["type"] == "synthetic"]
                            for item in pending:
                                await self.client.delete_inbox(self.record.opencode_session_id, item["id"])
                            # Epic shells discovered by this later refresh need
                            # the next tree pass's pre-deletion process snapshot.
                            if self.record.kind != "epic":
                                for shell in self._shells:
                                    await self.client.delete_shell(shell["id"], self.record.checkout_path)
                            if tree_clean and not self._shells and not pending and self.record.input_id and self.record.execution_state in {"succeeded", "failed", "interrupted"}:
                                self._persist(cancellation_state="verified")
                                return True
                            if (tree_clean and not self._shells and self.record.admission_state == "created" and not inbox and not users
                                    and self.record.last_seq == self.record.baseline_seq
                                    and self.record.execution_state in {"pending", "incomplete"}):
                                self._persist(cancellation_state="verified")
                                return True
                            if self.record.kind != "epic":
                                await self.client.interrupt(self.record.opencode_session_id)
                        except DaemonClientError as exc:
                            if exc.reason not in {"transport failure", "missing log watermark"}:
                                raise
                        await asyncio.sleep(0.05)
            except (TimeoutError, DaemonClientError) as exc:
                # A retained shell/PID witness is context, not the cause of this
                # failure. Preserve both so permission/identity failures cannot
                # look indistinguishable from reaching the cancellation deadline.
                reason = exc.reason if isinstance(exc, DaemonClientError) else "TimeoutError"
                detail = self.record.cleanup_error
                self._persist(cancellation_state="unverified", cleanup_error=self._redacted(
                    f"session cleanup incomplete: {reason}" + (f"; {detail}" if detail else ""), 8192))
                return False
            finally:
                self._cancelling = False

    async def release(self) -> None:
        """Free the admission slot without discarding verified retirement proof."""
        if self._retired:
            return
        if not await self.cancel():
            raise RuntimeError(self.record.cleanup_error or "session cleanup unverified; retaining controller and checkout")
        retired_path = self.path.with_name(f"{self.path.stem}.{self.record.opencode_session_id}.retired.json")
        self.path.replace(retired_path)
        self._record_path = retired_path
        self._retired = True
        directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def require_attempt(self, issue: int, attempt: int) -> None:
        if (self.record.issue_id, self.record.attempt) != (issue, attempt):
            raise DaemonClientError("retained_attempt_mismatch")
        if self.record.kind != "epic":
            raise DaemonClientError("retained_not_epic_driver")

    async def quiescence(self) -> str | None:
        """Read-only, stable ended witness across the entire owned tree.

        Unlike cancellation this must never stop live work or drain resumes.
        Ignore the old controller deadline: recovery has its own bounded RPC.
        """
        async def witness(*, shells: bool = True):
            tree = await self.client.session_tree(self.record.opencode_session_id)
            tails = {}
            for session in tree:
                sid = session["id"]
                if await self.client.pending_forms(sid):
                    raise DaemonClientError("retained_pending_form")
                inbox = await self.client.inbox(sid)
                for item in inbox["data"]:
                    _message(item, session_id=sid)
                if inbox["data"]:
                    reason = ("retained_pending_resume" if any(
                        m["type"] == "synthetic" for m in inbox["data"]) else "retained_pending_input")
                    raise DaemonClientError(reason)
                messages = await self._messages(sid)
                # An empty never-started descendant is idle; the driver must
                # have a durable ended turn, not merely an absent heartbeat.
                if not messages and sid != self.record.opencode_session_id:
                    tails[sid] = None
                elif not messages or messages[-1]["type"] != "idle":
                    return None
                else:
                    tails[sid] = messages[-1]["id"]
            if shells and await self._tree_shells(tree):
                raise DaemonClientError("retained_running_shells")
            if self.record.cleanup_processes:
                raise DaemonClientError("retained_process_cleanup_unverified")
            return tails

        self._cancelling = True
        try:
            async with asyncio.timeout(25):
                first = await witness()
                if first is None:
                    return "retained_running"
                second = await witness(shells=False)
                if first != second:
                    raise DaemonClientError("retained_activity_changed")
                return None
        except TimeoutError:
            raise DaemonClientError("retained_quiescence_timeout") from None
        finally:
            self._cancelling = False

    async def recover_epic(self, *, issue: int, attempt: int, action: str) -> dict:
        self.require_attempt(issue, attempt)
        if action == "session_rearm" and self._retired:
            return {"recovery": "fresh"}
        if self._retired:
            raise DaemonClientError("retained_already_released")
        # A dispatched resume is never replayed, including after a lost ACK.
        if action == "session_rearm" and self.record.operator_resume != "none":
            self._persist(operator_resume="none")
            return {"recovery": "continued"}
        if action == "session_resume" and self.record.operator_resume != "none":
            raise DaemonClientError("retained_resume_already_dispatched")
        try:
            reason = await self.quiescence()
        except DaemonClientError as exc:
            if action == "session_rearm" and exc.reason in {
                    "retained_running_shells", "retained_pending_resume", "retained_pending_input"}:
                return {"recovery": "reattach"}
            raise
        if reason:
            if action == "session_rearm":
                return {"recovery": "reattach"}
            raise DaemonClientError(reason)
        if action == "session_resume":
            self._persist(deadline_at_ms=int(time.time() * 1000) + 86400000)
            messages = await self._messages()
            users = [m["id"] for m in messages if m["type"] == "user"]
            if len(users) > 16:
                raise DaemonClientError("retained_resume_history_bound")
            # Persist intent BEFORE delivery. A lost ACK must not send twice.
            self._persist(baseline_message_ids=users, input_id=None, admission_state="dispatching",
                          execution_state="pending", stop_requested=False, cancellation_state="none",
                          deadline_at_ms=int(time.time() * 1000) + 86400000,
                          operator_resume="dispatching")
            text = (f"Continue the persisted factory run chainlink-{issue} using factory resume semantics. "
                    "Resume its saved state and next step; do not create a new run or story.")
            admitted = await self.client.prompt(self.record.opencode_session_id, text, resume=True)
            _message(admitted["data"], session_id=self.record.opencode_session_id)
            self._persist(input_id=admitted["data"]["id"], admission_state="admitted", operator_resume="admitted")
            return {"resumed": True}
        await self.release()
        return {"recovery": "fresh"} if action == "session_rearm" else {"released": True}


def session_environment(pass_env: object, *, home: Path, git_settings: object,
                        credentials: dict[str, str]) -> dict[str, str]:
    from mimir.otel_credentials import is_sensitive_otlp_env
    from ..contained_execution import heavy_lock_environment
    from .compute import _validate_dynamic_worker_name, _validate_worker_value
    from .pytest_environment import pytest_session_environment
    from .browser_environment import browser_environment

    if not isinstance(pass_env, dict) or len(pass_env) > 128:
        raise ValueError("invalid session pass_env")
    environment = {
        # Malformed lease settings fail closed (ValueError).
        **heavy_lock_environment(),
        **pytest_session_environment("/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin"),
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
    # Explicit pass_env (including operator-configured test_env) wins. Do this
    # after checking reserved names so callers still cannot override HOME/TMPDIR.
    environment.update(browser_environment({**os.environ, **environment}))
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


async def _ended_with_previous_daemon(client: OpenCodeDaemonClient, path: Path, *, session_id: str,
                                      request_generation: str, current_generation: object) -> bool:
    """Whether a retained session provably ended when its daemon was replaced.

    The daemon's state directory is not persistent: a container restart removes
    every controller record and the daemon's own sessions, and the daemon process
    that ran them exits. A run retained across that restart then has no record
    to recover, so every observe, cancel and release used to fail closed forever.
    It ended with its daemon only if all three hold: no controller record and no
    retirement witness exists; the current daemon's launch generation differs
    from the run's; and the current daemon reports the session absent (404).
    Anything else, including a daemon error, keeps today's fail-closed path.
    """
    retired = path.with_name(f"{path.stem}.{session_id}.retired.json")
    if path.exists() or retired.exists():
        return False
    if not isinstance(current_generation, str) or not current_generation or current_generation == request_generation:
        return False
    return not await client.session_exists(session_id)


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
        deadline = time.monotonic() + CONTROLLER_LOCK_TIMEOUT_S
        # Even read-only tree activity holds the exclusive lock: recovery and
        # controller replacement must not race its snapshot. Never unlink this
        # lock file; record replacement/retirement uses the same stable lock.
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if recovering_admission:
                    return {"admission_state": "busy"}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {"controller_busy": True}
                await asyncio.sleep(min(CONTROLLER_LOCK_RETRY_S, remaining))
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
                    if request["request"]["kind"] == "epic":
                        try:
                            decision = await retained.recover_epic(issue=request["issue"], attempt=request["attempt"],
                                                                   action="session_rearm")
                        except DaemonClientError as exc:
                            return {"admission_state": "refused", "reason": exc.reason}
                        if decision["recovery"] != "fresh":
                            return {"admission_state": "conflict", "opencode_session_id": retained.record.opencode_session_id}
                    else:
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
            launch = read_json(paths.daemon / "launch.json", uid=identities.worklink_uid, mode=0o600)
            controller = None
            if not await _ended_with_previous_daemon(
                    client, path, session_id=request["opencode_session_id"],
                    request_generation=request["generation"], current_generation=launch.get("launch_generation")):
                controller = SessionController.recover_owned(
                    client, path, session_id=request["opencode_session_id"],
                    generation=request["generation"], checkout=checkout)
                controller.sensitive_values = (api["password"], *(os.environ[k] for k in ("GH_TOKEN", "GITHUB_TOKEN") if k in os.environ))
                if controller.record.opencode_session_id != request["opencode_session_id"]:
                    raise ValueError("session ownership mismatch")
                if (controller.record.issue_id, controller.record.attempt) != (request["issue"], request["attempt"]):
                    raise DaemonClientError("retained_attempt_mismatch")
            if request["op"] in {"session_rearm", "session_release_quiescent", "session_resume"}:
                if controller is None:
                    if request["op"] == "session_rearm":
                        return {"recovery": "fresh"}
                    raise DaemonClientError("retained_session_absent")
                return await controller.recover_epic(issue=request["issue"], attempt=request["attempt"],
                                                     action=request["op"])
            if request["op"] == "session_tree_activity":
                if controller is None:
                    raise ValueError("session activity unavailable after daemon replacement")
                return await controller.tree_activity()
            if request["op"] == "session_observe":
                ended = SessionOutcome(request["opencode_session_id"], "interrupted", "verified", SessionError(
                    "daemon_restarted", "OpenCode daemon was replaced; the session ended with its daemon"))
                outcome = asdict(ended if controller is None else await controller.observe(stdout_limit=request["stdout_limit"]))
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
                if controller is None:
                    return {"cancellation_state": "verified"}
                await controller.cancel()
                return {"cancellation_state": controller.record.cancellation_state,
                        **({"cleanup_error": controller.record.cleanup_error} if controller.record.cleanup_error else {})}
            if request["op"] == "session_release":
                already_retired = controller is not None and controller._retired
                if controller is not None:
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
        # DaemonClientError reasons are fixed code literals, never remote bodies.
        sys.stdout.write(json.dumps({"failure": {"class": "DaemonClientError", "reason": exc.reason,
                                                 **({"status": exc.status} if type(exc.status) is int else {})}}))
        return 1
    except Exception as exc:
        # Remote/provider bodies and credential-bearing local errors never cross
        # the helper's stdout protocol or stderr boundary: only the class name does.
        sys.stdout.write(json.dumps({"failure": {"class": type(exc).__name__}}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
