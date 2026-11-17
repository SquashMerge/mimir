"""Explicitly imported, offline OpenCode 2.2.7 contract support.

The probe responses and the session-lifecycle capture are recorded. Execution
outcomes and fault/race controls below are synthetic scenarios shaped to match
that capture (message history, inbox and the watermark-only log endpoint).
"""
from __future__ import annotations

import asyncio
import base64
import copy
import json
import secrets
from collections import defaultdict, deque
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from jsonschema import Draft202012Validator, ValidationError

FIXTURES = Path(__file__).parent / "fixtures" / "worklink"


class OpenCodeSchema:
    def __init__(self) -> None:
        self.document = json.loads((FIXTURES / "opencode_2_2_7/openapi.json").read_bytes())

    def validate(self, value: Any, schema: dict[str, Any]) -> None:
        Draft202012Validator({**schema, "components": self.document["components"]}).validate(value)

    def named(self, name: str, value: Any) -> None:
        self.validate(value, {"$ref": f"#/components/schemas/{name}"})

    def operation(self, route: str, method: str) -> dict[str, Any]:
        return self.document["paths"][route][method.lower()]


def effective_permission(
    action: str, resource: str, *, agent: list[dict[str, str]],
    session: list[dict[str, str]], project: list[dict[str, str]] | None = None,
) -> str:
    """Finite native-core model: agent then session, final deny before grants.

    Source: retained supplement F3, embedded uD/H/YQ/findLast in 2.2.7.
    This is test support for the enumerated wildcard policies, not a public
    evaluator or a claim of live provider/tool execution.
    """
    def last(rules: list[dict[str, str]]) -> str:
        return next((rule["effect"] for rule in reversed(rules)
                     if fnmatchcase(action, rule["action"])
                     and fnmatchcase(resource, rule["resource"])), "ask")

    decision = last([*agent, *session])
    if decision == "deny":
        return decision
    grant = last(project or [])
    return grant if grant != "ask" else decision


@dataclass
class SyntheticSession:
    info: dict[str, Any]
    environment: dict[str, str] = field(default_factory=dict)
    inbox: list[dict[str, Any]] = field(default_factory=list)
    messages: list[dict[str, Any]] = field(default_factory=list)
    log: list[dict[str, Any]] = field(default_factory=list)
    active: bool = False
    changed: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class SyntheticFault:
    """One-shot wire fault; after=True applies the operation before losing ACK."""
    body: bytes = b""
    status: int = 200
    content_type: str = "application/json"
    disconnect: bool = False
    after: bool = False


class FakeOpenCodeDaemon:
    scenario_provenance = "synthetic: OpenCode 2.2.7 schema and inspected source"

    def __init__(self) -> None:
        self.schema = OpenCodeSchema()
        self.recorded = json.loads((FIXTURES / "opencode_2_2_7/probe-result.json").read_bytes())
        self.username = "opencode"
        self.password = secrets.token_urlsafe(24)
        self.sessions: dict[str, SyntheticSession] = {}
        self.requests: list[tuple[str, str, dict[str, str], Any]] = []
        self.faults: dict[tuple[str, str], deque[SyntheticFault]] = defaultdict(deque)
        self.auto_deliver = True
        self.delete_race = False
        self.interrupt_noop = False
        self.command_prompts = True
        #: Background shells by id. A shell outlives its session's turn, as in 2.2.7.
        self.shells: dict[str, dict[str, Any]] = {}
        self._counter = 0
        self._closing = False
        self.app = web.Application()
        routes = {
            "/api/info": ["GET"], "/api/event": ["GET"],
            "/api/session": ["GET", "POST"],
            "/api/session/{sessionID}": ["GET"],
            "/api/session/{sessionID}/environment": ["PUT"],
            "/api/session/{sessionID}/prompt": ["POST"],
            "/api/session/{sessionID}/command": ["POST"],
            "/api/session/{sessionID}/message": ["GET"],
            "/api/session/{sessionID}/inbox": ["GET"],
            "/api/session/{sessionID}/inbox/{inboxID}": ["DELETE"],
            "/api/session/{sessionID}/interrupt": ["POST"],
            "/api/experimental/session/{sessionID}/log": ["GET"],
            "/api/shell": ["GET"], "/api/shell/{id}": ["DELETE"],
        }
        for route, methods in routes.items():
            for method in methods:
                self.schema.operation(route, method)
                self.app.router.add_route(method, route, self._handle)
        self.server = TestServer(self.app, shutdown_timeout=0.1)

    @property
    def base_url(self) -> str:
        return str(self.server.make_url("/")).rstrip("/")

    @property
    def connection(self) -> dict[str, str]:
        return {"base_url": self.base_url, "username": self.username, "password": self.password}

    async def __aenter__(self) -> FakeOpenCodeDaemon:
        await self.server.start_server()
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._closing = True
        for state in self.sessions.values():
            state.changed.set()
        await self.server.close()

    def _id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter:024d}"

    def emit(self, session_id: str, event_type: str, **data: Any) -> dict[str, Any]:
        state = self.sessions[session_id]
        event = {
            "id": self._id("evt"), "created": 1, "type": event_type,
            "durable": {"aggregateID": session_id, "seq": len(state.log) + 1, "version": 1},
            "data": {"sessionID": session_id, **data},
        }
        self.schema.named(event_type, event)
        state.log.append(event)
        state.changed.set()
        return event

    def enqueue(self, session_id: str, text: str) -> dict[str, Any]:
        item = {"id": self._id("msg"), "sessionID": session_id, "time": {"created": 1},
                "type": "user", "payload": {"text": text}, "delivery": "queue"}
        self.schema.named("Session.Inbox.User", item)
        self.sessions[session_id].inbox.append(item)
        self.emit(session_id, "session.inbox.enqueued", inboxID=item["id"],
                  item={key: item[key] for key in ("type", "payload", "delivery")})
        return item

    def deliver(self, session_id: str, inbox_id: str) -> None:
        state = self.sessions[session_id]
        item = next(item for item in state.inbox if item["id"] == inbox_id)
        state.inbox.remove(item)
        message = {"id": item["id"], "time": item["time"], "type": "user", **item["payload"]}
        self.schema.named("Session.Message.User", message)
        state.messages.append(message)
        self.emit(session_id, "session.inbox.delivered", inboxID=inbox_id)
        if not state.active:
            state.active = True
            self.emit(session_id, "session.execution.started")

    def finish(
        self, session_id: str, *, outcome: str = "succeeded", error: dict[str, Any] | None = None,
        reason: str = "user",
    ) -> None:
        state = self.sessions[session_id]
        assert state.active, "synthetic execution must start before finishing"
        data = {"error": error} if outcome == "failed" else (
            {"reason": reason} if outcome == "interrupted" else {})
        self.emit(session_id, f"session.execution.{outcome}", **data)
        idle = {"id": self._id("msg"), "time": {"created": 2}, "type": "idle", "outcome": outcome}
        if error is not None:
            idle["error"] = error
        self.schema.named("Session.Message.Idle", idle)
        state.messages.append(idle)
        state.active = False
        state.info["outcome"] = outcome

    def start_shell(self, session_id: str, directory: str, command: str = "sh scripts/heavy_run.sh pytest") -> str:
        shell_id = self._id("sh")
        self.shells[shell_id] = {"id": shell_id, "status": "running", "command": command, "cwd": directory,
                                 "shell": "/bin/sh", "file": f"/tmp/{shell_id}.log", "pid": 4242,
                                 "metadata": {"sessionID": session_id}, "time": {"started": 1}}
        return shell_id

    def end_shell(self, shell_id: str, status: str = "exited") -> None:
        self.shells[shell_id].update(status=status, exit=0 if status == "exited" else 1, time={"started": 1, "completed": 2})

    def _response(self, request: web.Request, body: Any, status: int = 200) -> web.Response:
        operation = self.schema.operation(request.match_info.route.resource.canonical, request.method)
        response = operation["responses"][str(status)]
        if status == 204:
            return web.Response(status=204)
        self.schema.validate(body, response["content"]["application/json"]["schema"])
        return web.json_response(body, status=status)

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        auth = "Basic " + base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        if not secrets.compare_digest(request.headers.get("Authorization", ""), auth):
            return self._response(request, {"_tag": "UnauthorizedError", "message": "Authentication required"}, 401)
        route = request.match_info.route.resource.canonical
        operation = self.schema.operation(route, request.method)
        payload = await request.json() if request.can_read_body else None
        self.requests.append((request.method, request.path, dict(request.query), copy.deepcopy(payload)))
        if "requestBody" in operation:
            try:
                self.schema.validate(payload, operation["requestBody"]["content"]["application/json"]["schema"])
            except ValidationError:
                return self._response(request, {"_tag": "InvalidRequestError", "message": "Invalid synthetic request"}, 400)
        fault = self.faults[(request.method, request.path)].popleft() if self.faults[(request.method, request.path)] else None
        if fault is not None and not fault.after:
            return self._fault(request, fault)
        response = await self._dispatch(request, payload)
        return self._fault(request, fault) if fault is not None else response

    @staticmethod
    def _fault(request: web.Request, fault: SyntheticFault) -> web.Response:
        if fault.disconnect:
            assert request.transport is not None
            request.transport.abort()
        return web.Response(body=fault.body, status=fault.status, content_type=fault.content_type)

    async def _dispatch(self, request: web.Request, payload: Any) -> web.StreamResponse:
        path = request.path
        if path == "/api/info":
            return self._response(request, json.loads(self.recorded["responses"][path]["body"]))
        if path == "/api/event":
            return web.Response(body=self.recorded["responses"][path]["body"].encode(), content_type="text/event-stream")
        if path == "/api/session":
            if request.method == "GET":
                return self._response(request, {"data": [s.info for s in self.sessions.values()],
                                                "cursor": {"previous": None, "next": None}})
            info = json.loads(self.recorded["responses"]["POST /api/session location"]["body"])["data"]
            info.update(copy.deepcopy(payload))
            info["id"] = self._id("ses")
            self.sessions[info["id"]] = SyntheticSession(info)
            return self._response(request, {"data": info})
        if path == "/api/shell":
            directory = request.query.get("location[directory]")
            return self._response(request, {"location": {"directory": directory or "/"}, "data": [
                shell for shell in self.shells.values() if directory is None or shell["cwd"] == directory]})
        if path.startswith("/api/shell/"):
            shell = self.shells.get(request.match_info["id"])
            if shell is None:
                return self._response(request, {"_tag": "ShellNotFoundError", "id": request.match_info["id"],
                                                "message": "Synthetic shell not found"}, 404)
            if shell["status"] == "running":
                self.end_shell(shell["id"], "killed")
            return self._response(request, None, 204)
        session_id = request.match_info["sessionID"]
        if session_id not in self.sessions:
            return self._response(request, {"_tag": "SessionNotFoundError", "sessionID": session_id,
                                            "message": "Synthetic session not found"}, 404)
        state = self.sessions[session_id]
        if path.endswith("/log"):
            return await self._log(request, session_id)
        if path.endswith("/environment"):
            state.environment = dict(payload["variables"])
            return self._response(request, None, 204)
        if path.endswith("/prompt"):
            item = self.enqueue(session_id, payload["text"])
            if self.auto_deliver and payload.get("resume", True):
                self.deliver(session_id, item["id"])
            return self._response(request, {"data": item})
        if path.endswith("/command"):
            if self.command_prompts:
                item = self.enqueue(session_id, payload["text"])
                if self.auto_deliver:
                    self.deliver(session_id, item["id"])
            return self._response(request, None, 204)
        if path.endswith("/message"):
            return self._messages(request, state)
        if path.endswith("/inbox"):
            return self._response(request, {"data": state.inbox})
        if request.method == "DELETE":
            inbox_id = request.match_info["inboxID"]
            item = next((item for item in state.inbox if item["id"] == inbox_id), None)
            if item is not None:
                if self.delete_race:
                    self.deliver(session_id, inbox_id)
                else:
                    state.inbox.remove(item)
                    self.emit(session_id, "session.inbox.cancelled", inboxID=inbox_id)
            return self._response(request, None, 204)
        if path.endswith("/interrupt"):
            interrupted = state.active and not self.interrupt_noop
            if interrupted:
                self.finish(session_id, outcome="interrupted")
            return self._response(request, {"interrupted": interrupted})
        return self._response(request, {"data": state.info})

    def _messages(self, request: web.Request, state: SyntheticSession) -> web.Response:
        messages = [m for m in state.messages if request.query.get("type", m["type"]) == m["type"]]
        cursor = request.query.get("cursor")
        if cursor:
            try:
                order, offset_text = base64.urlsafe_b64decode(cursor).decode().split(":")
                offset = int(offset_text)
                if order not in {"asc", "desc"} or offset < 0 or "order" in request.query:
                    raise ValueError
            except (ValueError, UnicodeError):
                return self._response(request, {"_tag": "InvalidCursorError", "message": "Invalid cursor"}, 400)
        else:
            order, offset = request.query.get("order", "desc"), 0
        if order == "desc":
            messages.reverse()
        limit = int(request.query.get("limit", "100"))
        def encode(index: int) -> str:
            return base64.urlsafe_b64encode(f"{order}:{index}".encode()).decode()
        return self._response(request, {"data": messages[offset:offset + limit], "cursor": {
            "previous": encode(max(0, offset - limit)) if offset else None,
            "next": encode(offset + limit) if offset + limit < len(messages) else None,
        }})

    async def _log(self, request: web.Request, session_id: str) -> web.StreamResponse:
        """Captured 2.2.7 behaviour: only the ``log.synced`` watermark is served.

        Live follow and ``after=0`` replay both omitted every durable event on the
        real daemon (``opencode_2_2_7/session-lifecycle.json``); follow then stays
        open without further items. ``state.log`` only advances the sequence.
        """
        state = self.sessions[session_id]
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        item = {"type": "log.synced", "aggregateID": session_id, "seq": len(state.log)}
        self.schema.named("SessionLogItem", item)
        try:
            await response.write(f"data: {json.dumps(item)}\n\n".encode())
            while request.query.get("follow") == "true" and not self._closing:
                state.changed.clear()
                await state.changed.wait()
        except (ConnectionError, asyncio.CancelledError):
            pass
        return response


@pytest.fixture
async def fake_opencode_daemon():
    async with FakeOpenCodeDaemon() as daemon:
        yield daemon
