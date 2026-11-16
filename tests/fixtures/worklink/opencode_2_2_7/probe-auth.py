"""Offline 2.2.7 credential API/CLI probe. No provider calls or real login.

Run with the pinned CLI on PATH. Only normalized, synthetic fixtures reach stdout.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    binary = shutil.which("opencode")
    assert binary
    with tempfile.TemporaryDirectory(prefix="auth-probe-", dir=Path(__file__).resolve().parent) as directory:
        root = Path(directory)
        env = {"PATH": os.environ["PATH"], "HOME": str(root),
               **{f"XDG_{kind}_HOME": str(root / kind.lower()) for kind in ("DATA", "CONFIG", "CACHE", "STATE")},
               "OPENCODE_SERVER_PASSWORD": "synthetic-probe-password",
               "OPENCODE_CONFIG_CONTENT": json.dumps({"plugins": []}),
               **{f"OPENCODE_DISABLE_{key}": "1" for key in (
                   "AUTOUPDATE", "MODELS_FETCH", "FREE_MODELS", "CLAUDE_CODE",
                   "CLAUDE_CODE_SKILLS", "PROJECT_CONFIG", "FILEWATCHER")}}
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        auth = "Basic " + base64.b64encode(b"opencode:synthetic-probe-password").decode()

        def request(path, *, method="GET", payload=None, authenticated=True):
            headers = {"Authorization": auth} if authenticated else {}
            if payload is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(base + path, method=method, headers=headers,
                data=None if payload is None else json.dumps(payload).encode())
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    raw = response.read()
                    return response.status, json.loads(raw) if raw else None
            except urllib.error.HTTPError as exc:
                return exc.code, None

        child = subprocess.Popen([binary, "serve", "--hostname", "127.0.0.1", "--port", str(port)],
                                 cwd=root, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(150):
                try:
                    status, info = request("/api/info")
                    if status == 200:
                        break
                except OSError:
                    pass
                time.sleep(.2)
            else:
                raise RuntimeError("scratch daemon unavailable")
            assert info["version"] == "2.2.7"
            value = {"type": "oauth", "methodID": "chatgpt-browser", "refresh": "synthetic-refresh",
                     "access": "synthetic-access", "expires": 1900000000000,
                     "metadata": {"accountID": "synthetic-account"}}
            created = request("/api/credential", method="POST", payload={
                "integrationID": "openai", "value": value, "label": "agent Codex", "activate": True})
            assert created[0] == 200
            credential_id = created[1]["data"]["id"]
            cli_env = {**env, "OPENCODE_PASSWORD": env["OPENCODE_SERVER_PASSWORD"]}

            def cli(*args, input=None):
                completed = subprocess.run([binary, "auth", *args, "--server", base], env=cli_env,
                    cwd=root, input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                assert completed.returncode == 0, "synthetic CLI operation failed"
                return completed.stdout.decode()

            exported = json.loads(cli("export"))
            imported_output = cli("import", input=json.dumps(exported).encode())
            listed = request("/api/credential")
            replacement = request("/api/credential", method="POST", payload={
                "integrationID": "openai", "value": {**value, "methodID": "chatgpt-headless"},
                "label": "agent Codex", "activate": True})
            assert replacement[0] == 200
            removed = request("/api/credential/" + credential_id, method="DELETE")
            result = {"version": info["version"], "unauthenticated": request("/api/credential", authenticated=False)[0],
                      "created": created, "export": exported, "import_stdout": imported_output,
                      "listed_after_import": listed, "replacement": replacement, "removed": removed}
            text = json.dumps(result, indent=2)
            for index, identifier in enumerate(dict.fromkeys(
                    [credential_id, replacement[1]["data"]["id"]])):
                text = text.replace(identifier, f"<CREDENTIAL_ID_{index}>")
            print(text)
        finally:
            child.terminate()
            child.wait(timeout=20)


if __name__ == "__main__":
    main()
