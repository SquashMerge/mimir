"""Credential-free, run-local OpenCode 2.2.7 contract probe; not app code."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
BINARY = ROOT / "node_modules/@opencode/cli-linux-arm64/bin/opencode"
scratch = Path(tempfile.mkdtemp(prefix="api-proof-", dir=ROOT))
for name in ("home", "data", "config", "state", "cache", "checkout"):
    (scratch / name).mkdir()
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
password = secrets.token_urlsafe(24)
authorization = "Basic " + base64.b64encode(("opencode:" + password).encode()).decode()
env = {
    "PATH": os.environ["PATH"],
    "HOME": str(scratch / "home"),
    "XDG_DATA_HOME": str(scratch / "data"),
    "XDG_CONFIG_HOME": str(scratch / "config"),
    "XDG_STATE_HOME": str(scratch / "state"),
    "XDG_CACHE_HOME": str(scratch / "cache"),
    "OPENCODE_SERVER_PASSWORD": password,
    "OPENCODE_CONFIG_CONTENT": json.dumps({"plugins": []}),
    **{f"OPENCODE_DISABLE_{key}": "1" for key in (
        "AUTOUPDATE", "MODELS_FETCH", "FREE_MODELS", "CLAUDE_CODE",
        "CLAUDE_CODE_SKILLS", "PROJECT_CONFIG", "FILEWATCHER",
    )},
}
base = f"http://127.0.0.1:{port}"

def request(path, *, authenticated=True, method="GET", payload=None):
    headers = {"Authorization": authorization} if authenticated else {}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, headers=headers, method=method,
        data=json.dumps(payload).encode() if payload is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=2) as response:
            if "text/event-stream" in response.headers.get("Content-Type", ""):
                content = response.readline() + response.readline()
            else:
                content = response.read()
            return {"status": response.status, "content_type": response.headers.get("Content-Type"),
                    "body": content.decode(errors="replace")}
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "body": exc.read().decode(errors="replace")}
    except Exception as exc:
        return {"error": type(exc).__name__}

result = {"binary_version": "2.2.7", "scratch": str(scratch), "responses": {}}
with (scratch / "server.log").open("wb") as log:
    process = subprocess.Popen([str(BINARY), "serve", "--hostname", "127.0.0.1", "--port", str(port)],
        cwd=scratch / "checkout", env=env, stdout=log, stderr=subprocess.STDOUT)
    result["server_pid"] = process.pid
    try:
        for _ in range(150):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.1):
                    break
            except OSError:
                if process.poll() is not None:
                    raise RuntimeError("scratch server exited before readiness")
                time.sleep(.2)
        else:
            raise RuntimeError("scratch server did not become ready")
        for path in ("/api/info", "/api/config", "/api/health", "/health", "/api/session", "/api/event", "/api/openapi.json", "/openapi.json", "/api/doc", "/doc", "/api/provider", "/api/auth"):
            result["responses"][path] = request(path)
        openapi = json.loads(result["responses"]["/openapi.json"]["body"])
        (ROOT / "openapi.json").write_text(json.dumps(openapi, indent=2) + "\n")
        for path in ("/api/session", "/api/event"):
            result["responses"]["unauthenticated " + path] = request(path, authenticated=False)
        result["responses"]["POST /api/session empty"] = request("/api/session", method="POST", payload={})
        result["responses"]["POST /api/session location"] = request("/api/session", method="POST", payload={
            "location": {"directory": str(scratch / "checkout")},
            "permissions": [
                {"action": "external_directory", "resource": "*", "effect": "deny"},
                {"action": "shell", "resource": "*", "effect": "deny"},
                {"action": "shell", "resource": "uv run pytest*", "effect": "allow"},
            ],
        })
    finally:
        process.terminate()
        try:
            result["server_exit"] = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            result["server_exit"] = process.wait(timeout=5)
            result["forced_stop"] = True
        result["server_awaited"] = process.poll() is not None
(ROOT / "probe-result.json").write_text(json.dumps(result, indent=2) + "\n")
summary = {key: value for key, value in result.items() if key != "responses"}
summary["responses"] = {path: {k: v for k, v in value.items() if k != "body"} |
    {"body_prefix": value.get("body", "")[:500]} for path, value in result["responses"].items()}
print(json.dumps(summary, indent=2))
