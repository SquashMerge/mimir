"""Reproduce the real, credential-free 2.3.0 API and factory captures.

Run explicitly, not during pytest. All server/CLI environments are constructed
from scratch; never inherit provider credentials or managed-service discovery.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent


def write(name, value):
    (ROOT / name).write_text(json.dumps(value, indent=2) + "\n")


@contextmanager
def server(binary, *, factory=None):
    with tempfile.TemporaryDirectory(prefix="opencode-230-", dir=ROOT.parents[3] / ".factory") as directory:
        scratch = Path(directory)
        for name in ("home", "data", "config", "state", "cache", "checkout"):
            (scratch / name).mkdir()
        plugins = []
        if factory:
            replacement = scratch / "replacement"
            replacement.mkdir()
            (replacement / "index.js").write_text('''export default {
  id: "replacement-probe",
  async setup(ctx) {
    await ctx.agent.transform(editor => {
      editor.update("replacement-probe", draft => {
        draft.permissions = [{action: "shell", resource: "*", effect: "deny"}];
      });
    });
  }
};
''')
            plugins = [str(factory), str(replacement)]
        env = {
            "PATH": os.environ["PATH"], "HOME": str(scratch / "home"),
            **{f"XDG_{kind}_HOME": str(scratch / kind.lower()) for kind in ("DATA", "CONFIG", "STATE", "CACHE")},
            "OPENCODE_SERVER_PASSWORD": secrets.token_urlsafe(24),
            "OPENCODE_CONFIG_CONTENT": json.dumps({"plugins": plugins}),
            **{f"OPENCODE_DISABLE_{key}": "1" for key in (
                "AUTOUPDATE", "MODELS_FETCH", "FREE_MODELS", "CLAUDE_CODE",
                "CLAUDE_CODE_SKILLS", "PROJECT_CONFIG", "FILEWATCHER")},
        }
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        auth = "Basic " + base64.b64encode(("opencode:" + env["OPENCODE_SERVER_PASSWORD"]).encode()).decode()

        def request(path, *, method="GET", payload=None, authenticated=True):
            headers = {"Authorization": auth} if authenticated else {}
            if payload is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(base + path, method=method, headers=headers,
                data=None if payload is None else json.dumps(payload).encode())
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    content_type = response.headers.get("Content-Type", "")
                    raw = response.readline() + response.readline() if "text/event-stream" in content_type else response.read()
                    return {"status": response.status, "content_type": content_type, "body": raw.decode()}
            except urllib.error.HTTPError as exc:
                return {"status": exc.code, "body": exc.read().decode()}

        def cli(*args, input=None):
            completed = subprocess.run([str(binary), *args, "--server", base], cwd=scratch / "checkout",
                env={**env, "OPENCODE_PASSWORD": env["OPENCODE_SERVER_PASSWORD"]},
                input=input, capture_output=True, timeout=30)
            assert completed.returncode == 0, completed.stderr.decode()
            return completed.stdout.decode()

        result = {"binary_version": "2.3.0", "scratch": str(scratch), "responses": {}}
        with (scratch / "server.log").open("wb") as log:
            child = subprocess.Popen([str(binary), "serve", "--hostname", "127.0.0.1", "--port", str(port)],
                cwd=scratch / "checkout", env=env, stdout=log, stderr=subprocess.STDOUT)
            result["server_pid"] = child.pid
            try:
                for _ in range(150):
                    try:
                        response = request("/api/info")
                        if response["status"] == 200:
                            assert json.loads(response["body"])["version"] == "2.3.0"
                            break
                    except OSError:
                        pass
                    if child.poll() is not None:
                        raise RuntimeError((scratch / "server.log").read_text())
                    time.sleep(.2)
                else:
                    raise RuntimeError("scratch server did not become ready")
                yield request, cli, result, scratch
            finally:
                child.terminate()
                try:
                    result["server_exit"] = child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill()
                    result["server_exit"] = child.wait(timeout=5)
                    result["forced_stop"] = True
                result["server_awaited"] = child.poll() is not None


def capture(binary, factory):
    with server(binary) as (request, cli, result, scratch):
        responses = result["responses"]
        write("openapi.json", json.loads(request("/openapi.json")["body"]))
        for path in ("/api/info", "/api/config", "/api/health", "/health", "/api/session", "/api/event", "/api/shell"):
            responses[path] = request(path)
        for path in ("/api/session", "/api/event"):
            responses["unauthenticated " + path] = request(path, authenticated=False)
        responses["POST /api/session empty"] = request("/api/session", method="POST", payload={})
        responses["POST /api/session location"] = request("/api/session", method="POST", payload={
            "location": {"directory": str(scratch / "checkout")},
            "permissions": [{"action": "external_directory", "resource": "*", "effect": "deny"},
                            {"action": "shell", "resource": "*", "effect": "deny"},
                            {"action": "shell", "resource": "uv run pytest*", "effect": "allow"}],
        })
        sid = json.loads(responses["POST /api/session location"]["body"])["data"]["id"]
        route = f"/api/session/{sid}"
        responses["PUT environment"] = request(route + "/environment", method="PUT", payload={"variables": {"SYNTHETIC": "yes"}})
        responses["POST prompt queued"] = request(route + "/prompt", method="POST", payload={"text": "synthetic queued input; never execute", "resume": False})
        assert responses["PUT environment"]["status"] == 204
        assert responses["POST prompt queued"]["status"] == 200
        for suffix in ("inbox", "message"):
            responses["GET " + suffix] = request(route + "/" + suffix)
        responses["POST interrupt"] = request(route + "/interrupt?resume=false", method="POST", payload={})
        responses["GET log"] = request(f"/api/experimental/session/{sid}/log?after=0&follow=false")
        inbox = json.loads(responses["GET inbox"]["body"])["data"]
        assert len(inbox) == 1
        responses["DELETE inbox"] = request(route + "/inbox/" + inbox[0]["id"], method="DELETE")

        value = {"type": "oauth", "methodID": "chatgpt-browser", "refresh": "synthetic-refresh",
                 "access": "synthetic-access", "expires": 0, "metadata": {"accountID": "synthetic-account"}}
        created = request("/api/credential", method="POST", payload={
            "integrationID": "openai", "value": value, "label": "agent Codex", "activate": True})
        assert created["status"] == 200
        credential_id = json.loads(created["body"])["data"]["id"]
        exported = json.loads(cli("auth", "export"))
        imported = cli("auth", "import", input=json.dumps(exported).encode())
        assert json.loads(request("/api/credential")["body"])["data"] == exported
        with sqlite3.connect(f"file:{scratch / 'data/opencode/opencode.db'}?mode=ro", uri=True) as db:
            method_id = db.execute("SELECT method_id FROM credential WHERE id = ?", (credential_id,)).fetchone()[0]
        assert method_id is None
        replacement = request("/api/credential", method="POST", payload={
            "integrationID": "openai", "value": {**value, "methodID": "chatgpt-headless"},
            "label": "agent Codex", "activate": True})
        removed = request("/api/credential/" + credential_id, method="DELETE")
        write("auth-import-result.json", {"version": "2.3.0", "unauthenticated": request("/api/credential", authenticated=False)["status"],
            "import_stdout": imported, "create_status": created["status"], "replacement_method": "chatgpt-headless",
            "replacement_status": replacement["status"], "remove_status": removed["status"], "active_method_id": method_id})
        for index, entry in enumerate(exported):
            entry["id"] = f"<CREDENTIAL_ID_{index}>"
        write("auth-export.json", exported)
    write("probe-result.json", result)

    with server(binary, factory=factory) as (request, cli, result, scratch):
        for _ in range(150):
            plugins = json.loads(request("/api/plugin")["body"])["data"]
            active = {p["id"] for p in plugins if p["state"]["status"] == "active"}
            if {"feature-factory", "replacement-probe"} <= active:
                break
            time.sleep(.2)
        else:
            raise AssertionError(plugins)
        result["plugins"] = plugins
        result["agents"] = [{k: a[k] for k in ("id", "permissions")} for a in json.loads(request("/api/agent")["body"])["data"]]
        result["skills"] = [s["id"] for s in json.loads(request("/api/skill")["body"])["data"]]
        result["commands"] = [c["name"] for c in json.loads(request("/api/command")["body"])["data"]]
        metadata = json.loads((factory / "package.json").read_text())
        result["package"] = {k: metadata[k] for k in ("name", "version", "engines", "dependencies", "peerDependencies")}
        assert "feature" in result["skills"] and "feature" in result["commands"]
        agents = {a["id"]: a["permissions"] for a in result["agents"]}
        assert {"feature-factory", "run-orchestrator", "backend-builder", "frontend-builder", "story-reader",
                "story-writer", "codebase-researcher", "design-interpreter", "spec-writer", "work-decomposer",
                "work-reviewer", "test-verifier", "implementation-validator"} <= agents.keys()
        allow = {"action": "*", "resource": "*", "effect": "allow"}
        assert allow in agents["build"] and allow not in agents["replacement-probe"]
    write("factory-result.json", result)
    print(json.dumps({"api_server_exit": result["server_exit"], "factory_active": sorted(active)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--factory-package", type=Path, required=True)
    args = parser.parse_args()
    capture(args.binary.resolve(), args.factory_package.resolve())
