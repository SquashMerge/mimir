"""Private, bounded OpenCode 2 transport for the worklink executor.

Admission, replay and completion policy belong to the session controller. In
particular this transport never retries a request, including a lost POST ACK.
"""
from __future__ import annotations

import json
import math
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import aiohttp
from yarl import URL

MAX_ITEM_BYTES = 1024 * 1024
MAX_SEQUENCE = 9007199254740991


class DaemonClientError(RuntimeError):
    """Transport/protocol failure, with no remote body or credentials attached."""

    def __init__(self, reason: str, *, status: int | None = None) -> None:
        self.reason = reason
        self.status = status
        super().__init__(f"OpenCode daemon {reason}" + (f" (HTTP {status})" if status else ""))


@dataclass(frozen=True)
class ServerEvent:
    event: str
    data: dict[str, Any]
    id: str | None = None


def _identifier(value: str, prefix: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(prefix + r"_[A-Za-z0-9_-]{1,128}", value):
        raise ValueError("Invalid OpenCode identifier")
    return value


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DaemonClientError("invalid response")
    return value


def _decode(raw: bytes) -> dict[str, Any]:
    try:
        return _object(json.loads(raw.decode("utf-8"), parse_constant=_invalid_constant))
    except (ValueError, UnicodeError, RecursionError):
        raise DaemonClientError("invalid JSON") from None


def _invalid_constant(value: str) -> None:
    raise ValueError("Non-JSON number")


class OpenCodeDaemonClient:
    """Explicit connection/auth inputs; no environment or private-file discovery.

    Use as an async context manager. The owned session ignores proxy/netrc
    environment and redirects, so Basic credentials cannot follow a redirect.
    Only the named worklink API operations are exposed.
    """

    def __init__(
        self, *, base_url: str, username: str, password: str,
        timeout: float = 30.0, max_item_bytes: int = MAX_ITEM_BYTES,
    ) -> None:
        url = URL(base_url)
        if (url.scheme not in {"http", "https"} or not url.host or url.user is not None
                or url.path not in {"", "/"} or url.query_string or url.fragment):
            raise ValueError("Invalid daemon URL")
        if not username or not password or ":" in username:
            raise ValueError("Invalid daemon authentication")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Invalid daemon timeout")
        if type(max_item_bytes) is not int or not 0 < max_item_bytes <= MAX_ITEM_BYTES:
            raise ValueError("Invalid daemon item bound")
        self._url = str(url).rstrip("/")
        self._auth = aiohttp.BasicAuth(username, password)
        self._timeout = aiohttp.ClientTimeout(total=timeout, sock_connect=timeout, sock_read=timeout)
        self._bound = max_item_bytes
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> OpenCodeDaemonClient:
        if self._session is not None:
            raise RuntimeError("Daemon client already open")
        self._session = aiohttp.ClientSession(
            auth=self._auth, timeout=self._timeout, trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _connection(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Daemon client is not open")
        return self._session

    async def _read(self, response: aiohttp.ClientResponse) -> bytes:
        body = bytearray()
        async for chunk in response.content.iter_chunked(16384):
            if len(body) + len(chunk) > self._bound:
                raise DaemonClientError("item too large")
            body.extend(chunk)
        return bytes(body)

    async def _request(
        self, method: str, path: str, *, payload: Mapping[str, Any] | None = None,
        params: dict[str, str] | None = None, status: int = 200,
    ) -> dict[str, Any] | None:
        raw = None
        if payload is not None:
            raw = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
            if len(raw) > self._bound:
                raise DaemonClientError("item too large")
        try:
            async with self._connection().request(
                method, self._url + path, params=params, data=raw,
                headers={"Content-Type": "application/json"} if raw is not None else {},
                allow_redirects=False,
            ) as response:
                if response.status != status:
                    # Remote errors are untrusted and may contain credentials.
                    raise DaemonClientError("HTTP failure", status=response.status)
                body = await self._read(response)
                if status == 204:
                    if body:
                        raise DaemonClientError("unexpected response body")
                    return None
                if response.content_type != "application/json":
                    raise DaemonClientError("invalid content type")
                return _decode(body)
        except (aiohttp.ClientError, TimeoutError):
            raise DaemonClientError("transport failure") from None

    async def info(self) -> dict[str, Any]:
        result = _object(await self._request("GET", "/api/info"))
        if not isinstance(result.get("version"), str):
            raise DaemonClientError("invalid server identity")
        return result

    async def credentials(self) -> list[dict[str, Any]]:
        """Controller-only secret export. Callers must never log this result."""
        result = _object(await self._request("GET", "/api/credential"))
        self._list(result)
        return result["data"]

    async def create_credential(self, value: Mapping[str, Any]) -> dict[str, Any]:
        result = _object(await self._request("POST", "/api/credential", payload={
            "integrationID": "openai", "label": "agent Codex", "activate": True,
            "value": dict(value),
        }))
        return _object(result.get("data"))

    async def remove_credential(self, credential_id: str) -> None:
        # Credential IDs are opaque in the pinned schema. Quote the whole path
        # component rather than allowing a remote ID to change the endpoint.
        from urllib.parse import quote

        await self._request("DELETE", "/api/credential/" + quote(credential_id, safe=""), status=204)

    async def create_session(
        self, *, directory: str, model: Mapping[str, str], permissions: list[dict[str, str]],
        agent: str | None = None,
    ) -> dict[str, Any]:
        result = _object(await self._request("POST", "/api/session", payload={
            "location": {"directory": directory}, "model": dict(model), "permissions": permissions,
            **({"agent": agent} if agent is not None else {}),
        }))
        data = _object(result.get("data"))
        try:
            _identifier(data.get("id"), "ses")
        except ValueError:
            raise DaemonClientError("invalid session identity") from None
        if _object(data.get("location")).get("directory") != directory:
            raise DaemonClientError("session location mismatch")
        return result

    async def session_exists(self, session_id: str) -> bool:
        """True when this daemon knows the session; only an explicit 404 means absent."""
        try:
            result = _object(await self._request("GET", f"/api/session/{_identifier(session_id, 'ses')}"))
        except DaemonClientError as exc:
            if exc.status == 404:
                return False
            raise
        if _object(result.get("data")).get("id") != session_id:
            raise DaemonClientError("session identity mismatch")
        return True

    async def session_info(self, session_id: str) -> dict[str, Any]:
        result = _object(await self._request("GET", f"/api/session/{_identifier(session_id, 'ses')}"))
        data = _object(result.get("data"))
        if data.get("id") != session_id:
            raise DaemonClientError("session identity mismatch")
        return data

    async def children(self, session_id: str) -> list[dict[str, Any]]:
        """All direct children, including archived/moved sessions, with bounded pagination."""
        _identifier(session_id, "ses")
        children: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        cursors: set[str] = set()
        cursor = None
        limit = 100
        size = 0
        while True:
            # Native cursors retain the original query; do not repeat parent/order.
            params = {"cursor": cursor, "limit": str(limit)} if cursor is not None else {
                "parentID": session_id, "order": "asc", "limit": str(limit)}
            try:
                result = _object(await self._request("GET", "/api/session", params=params))
            except DaemonClientError as exc:
                if exc.reason != "item too large" or limit == 1:
                    raise
                limit = max(1, limit // 4)
                continue
            self._list(result)
            size += len(json.dumps(result).encode())
            if len(children) + len(result["data"]) > 4096 or size > 16 * MAX_ITEM_BYTES:
                raise DaemonClientError("session children exceed bound")
            for child in result["data"]:
                try:
                    _identifier(child.get("id"), "ses")
                except ValueError:
                    raise DaemonClientError("invalid child session identity") from None
                if child.get("parentID") != session_id:
                    raise DaemonClientError("foreign child session")
                if child["id"] in seen_ids:
                    raise DaemonClientError("invalid child session binding")
                seen_ids.add(child["id"])
            children.extend(result["data"])
            page = _object(result.get("cursor"))
            if (set(page) != {"previous", "next"} or any(
                    v is not None and (not isinstance(v, str) or len(v.encode()) > 4096)
                    for v in page.values())):
                raise DaemonClientError("invalid session page")
            cursor = page["next"]
            if cursor is None:
                return children
            if cursor in cursors:
                raise DaemonClientError("repeated session cursor")
            cursors.add(cursor)

    async def pending_forms(self, session_id: str) -> list[dict[str, Any]]:
        result = _object(await self._request("GET", self._path(session_id, "form")))
        self._list(result)
        for form in result["data"]:
            if form.get("sessionID") != session_id:
                raise DaemonClientError("foreign session form")
        return result["data"]

    async def set_environment(self, session_id: str, variables: Mapping[str, str]) -> None:
        await self._request("PUT", self._path(session_id, "environment"),
                            payload={"variables": dict(variables)}, status=204)

    async def forms(self, session_id: str) -> list[dict[str, Any]]:
        """Pending Form.Info entries only. Worklink deliberately has no reply API."""
        result = _object(await self._request("GET", self._path(session_id, "form")))
        self._list(result)
        seen = set()
        for form in result["data"]:
            try:
                _identifier(form.get("id"), "frm")
            except ValueError:
                raise DaemonClientError("invalid form identity") from None
            if (form.get("sessionID") != session_id or form["id"] in seen
                    or not isinstance(form.get("title"), str)
                    or not isinstance(form.get("metadata", {}), dict)
                    or not isinstance(form.get("fields"), list) or not form["fields"]):
                raise DaemonClientError("invalid session form")
            for field in form["fields"]:
                if (not isinstance(field, dict) or not isinstance(field.get("key"), str)
                        or field.get("type") not in {"string", "number", "integer", "boolean", "multiselect", "external"}
                        or any(not isinstance(field[key], str) for key in ("title", "description") if key in field)):
                    raise DaemonClientError("invalid form field")
            seen.add(form["id"])
        return result["data"]

    async def prompt(self, session_id: str, text: str, *, resume: bool = True) -> dict[str, Any]:
        result = _object(await self._request("POST", self._path(session_id, "prompt"),
                                            payload={"text": text, "resume": resume}))
        data = _object(result.get("data"))
        if data.get("sessionID") != session_id or data.get("type") != "user":
            raise DaemonClientError("input identity mismatch")
        try:
            _identifier(data.get("id"), "msg")
        except ValueError:
            raise DaemonClientError("invalid input identity") from None
        return result

    async def command(self, session_id: str, *, name: str, text: str) -> None:
        await self._request("POST", self._path(session_id, "command"),
                            payload={"name": name, "text": text}, status=204)

    async def messages(
        self, session_id: str, *, cursor: str | None = None, order: str | None = None,
        limit: int | None = None, message_type: str | None = None,
    ) -> dict[str, Any]:
        if cursor is not None and order is not None:
            raise ValueError("Cursor and order are mutually exclusive")
        if order is not None and order not in {"asc", "desc"}:
            raise ValueError("Invalid message order")
        if limit is not None and (type(limit) is not int or limit <= 0):
            raise ValueError("Invalid message limit")
        params = {key: str(value) for key, value in (
            ("cursor", cursor), ("order", order), ("limit", limit), ("type", message_type),
        ) if value is not None}
        result = _object(await self._request("GET", self._path(session_id, "message"), params=params))
        self._list(result)
        page = _object(result.get("cursor"))
        if set(page) - {"previous", "next"} or any(
            value is not None and not isinstance(value, str) for value in page.values()
        ):
            raise DaemonClientError("invalid message cursor")
        return result

    async def inbox(self, session_id: str) -> dict[str, Any]:
        result = _object(await self._request("GET", self._path(session_id, "inbox")))
        self._list(result)
        return result

    async def delete_inbox(self, session_id: str, inbox_id: str) -> None:
        await self._request("DELETE", self._path(session_id, "inbox/") + _identifier(inbox_id, "msg"),
                            status=204)

    async def running_shells(self, directory: str) -> list[dict[str, Any]]:
        """Background shell commands still running in ``directory`` (all sessions)."""
        result = _object(await self._request("GET", "/api/shell", params={"location[directory]": directory}))
        self._list(result)
        shells = []
        for item in result["data"]:
            try:
                _identifier(item.get("id"), "sh")
            except ValueError:
                raise DaemonClientError("invalid shell identity") from None
            if not isinstance(item.get("status"), str) or not isinstance(item.get("metadata", {}), dict):
                raise DaemonClientError("invalid shell entry")
            if item["status"] == "running":
                shells.append(item)
        return shells

    async def delete_shell(self, shell_id: str, directory: str) -> None:
        await self._request("DELETE", "/api/shell/" + _identifier(shell_id, "sh"),
                            params={"location[directory]": directory}, status=204)

    async def interrupt(self, session_id: str) -> dict[str, Any]:
        result = _object(await self._request("POST", self._path(session_id, "interrupt"),
                                            params={"resume": "false"}))
        if type(result.get("interrupted")) is not bool:
            raise DaemonClientError("invalid interrupt response")
        return result

    async def log(
        self, session_id: str, *, after: int | None = None, follow: bool = True,
    ) -> AsyncIterator[ServerEvent]:
        _identifier(session_id, "ses")
        if after is not None and (type(after) is not int or not 0 <= after <= MAX_SEQUENCE):
            raise ValueError("Invalid durable sequence")
        params = {"follow": "true" if follow else "false"}
        if after is not None:
            params["after"] = str(after)
        async for event in self._events(f"/api/experimental/session/{session_id}/log", params):
            yield event

    async def events(self) -> AsyncIterator[ServerEvent]:
        async for event in self._events("/api/event", {}):
            yield event

    async def _events(self, path: str, params: dict[str, str]) -> AsyncIterator[ServerEvent]:
        try:
            async with self._connection().get(
                self._url + path, params=params, allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise DaemonClientError("HTTP failure", status=response.status)
                if response.content_type != "text/event-stream":
                    raise DaemonClientError("invalid stream content type")
                pending = bytearray()
                lines: list[bytes] = []
                size = 0
                async for chunk in response.content.iter_chunked(16384):
                    for part in chunk.splitlines(keepends=True):
                        pending.extend(part)
                        if size + len(pending) > self._bound:
                            raise DaemonClientError("item too large")
                        if not pending.endswith(b"\n"):
                            continue
                        line = bytes(pending).rstrip(b"\r\n")
                        size += len(pending)
                        pending.clear()
                        if line:
                            lines.append(line)
                            continue
                        event = self._frame(lines)
                        lines.clear()
                        size = 0
                        if event is not None:
                            yield event
                if pending or lines:
                    raise DaemonClientError("truncated stream item")
                # Clean EOF is transport loss, not a completion signal.
        except (aiohttp.ClientError, TimeoutError):
            raise DaemonClientError("transport failure") from None

    @staticmethod
    def _frame(lines: list[bytes]) -> ServerEvent | None:
        data = []
        name = "message"
        event_id = None
        try:
            for raw in lines:
                line = raw.decode("utf-8")
                if line.startswith(":"):
                    continue
                key, _, value = line.partition(":")
                value = value.removeprefix(" ")
                if key == "data":
                    data.append(value)
                elif key == "event":
                    name = value
                elif key == "id":
                    if "\0" in value:
                        raise DaemonClientError("invalid stream ID")
                    event_id = value
        except UnicodeError:
            raise DaemonClientError("invalid stream encoding") from None
        if not data:
            return None
        if name == "effect/http-api/stream/failure":
            raise DaemonClientError("remote stream failure")
        return ServerEvent(event=name, id=event_id, data=_decode("\n".join(data).encode("utf-8")))

    @staticmethod
    def _path(session_id: str, suffix: str) -> str:
        return f"/api/session/{_identifier(session_id, 'ses')}/{suffix}"

    @staticmethod
    def _list(result: dict[str, Any]) -> None:
        if not isinstance(result.get("data"), list) or any(
            not isinstance(item, dict) for item in result["data"]
        ):
            raise DaemonClientError("invalid list response")
