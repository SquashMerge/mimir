from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import time
import zipfile

from packaging.utils import canonicalize_name, parse_wheel_filename

ROOT = Path(__file__).resolve().parents[1]
IMAGE = f"mimir-worklink-identity:{os.getpid()}"
CONTAINER = f"mimir-worklink-identity-{os.getpid()}"


async def prove_offline(directory: Path) -> dict[str, object]:
    """Exercise production session consumers, not native UID isolation or a provider."""
    from types import SimpleNamespace
    from unittest.mock import patch
    import uuid
    from mimir_cultivate import contained_execution, opencode
    from mimir_cultivate.worklink.backends.base import LaunchHandle, WorkOrder
    from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
    from mimir_cultivate.worklink.daemon_client import OpenCodeDaemonClient
    from mimir_cultivate.worklink.daemon_sessions import SessionController
    from mimir.tools.registry import _STATE, set_spawn_config
    from tests.worklink_daemon_fixtures import FakeOpenCodeDaemon

    checkout = directory / "checkout"
    checkout.mkdir()
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as api:
        class Executor:
            def __init__(self, path):
                self.path = path

            async def session_create(self, request, *, timeout_s, pass_env=None, git_settings=()):
                self.controller = await SessionController.create(
                    api, directory / (uuid.uuid4().hex + ".json"), request=request,
                    issue_id=2125, attempt=1, generation="offline-generation", checkout=self.path,
                    environment={"PATH": "/usr/bin:/bin", **(pass_env or {})}, timeout_s=timeout_s)
                sid = self.controller.record.opencode_session_id
                if request.kind == "spawn":
                    (self.path / "generated.txt").write_text("offline proposal\n")
                daemon.finish(sid)
                return LaunchHandle("opencode_daemon", sid, opencode_session_id=sid, launch_generation="offline-generation")

            async def session_observe(self, handle, *, stdout_limit=64 * 1024 * 1024):
                return await self.controller.observe(stdout_limit=stdout_limit)

            async def session_cancel(self, handle):
                if not await self.controller.cancel(timeout_s=1):
                    raise RuntimeError("offline cancellation unverified")

            async def session_release(self, handle):
                await self.controller.release()

        backend = OpenCodeBackend()
        order = WorkOrder(2125, checkout, "offline leaf", "rules", 30,
                          {"MIMIR_MODEL_SPEC": "codex-plus:fixture-model#high", "MIMIR_HOME": str(directory)}, directory / "transcripts")
        spec = backend.work_spec(order, attempt=1, repo_url="offline", base_ref="main",
                                 branch="issue/2125-a1", test_command="uv run pytest")
        compute = OpenCodeDaemonComputeBackend(client=Executor(checkout))
        handle = await compute.launch(spec)
        raw = await backend.interpret(order, await compute.wait(handle, 30))
        assert raw.backend_status == "success" and raw.opencode_session_id == handle.identifier
        await compute.cleanup(handle)

        seed = directory / "seed"
        seed.mkdir()
        for args in (("init", "-q"), ("config", "user.name", "Offline Fixture"),
                     ("config", "user.email", "offline@example.invalid"),
                     ("commit", "--allow-empty", "-qm", "seed")):
            subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                            "-C", str(seed), *args], check=True, capture_output=True, env=opencode._sanitized_git_env())

        class Checkout:
            def __init__(self):
                self.path = directory / "spawn-checkout"
                subprocess.run(["git", "clone", "-q", str(seed), str(self.path)], check=True, env=opencode._sanitized_git_env())
                self.base_tree = subprocess.check_output(["git", "-C", str(self.path), "rev-parse", "HEAD^{tree}"], text=True,
                                                        env=opencode._sanitized_git_env()).strip()
                self.capability = SimpleNamespace(_contained_worker_client=Executor(self.path))

            def close(self, *, retain=False):
                assert not retain

        previous = _STATE["spawn_config"]
        try:
            set_spawn_config({"default_cwd": seed, "artifact_root": directory / "artifacts"})
            # Like Executor above, readiness belongs to this synthetic offline
            # fixture. This proof does not establish native login validity.
            with patch("mimir_cultivate.worklink.daemon_health.daemon_health", return_value={"ok": True}):
                spawned = json.loads(await opencode._spawn_open_code_impl(
                    "offline spawn", str(seed), 30, model="openai/fixture-model", agent="build",
                    contained_runner=contained_execution.execute_contained_session,
                    checkout_factory=lambda *args, **kwargs: Checkout()))
        finally:
            _STATE["spawn_config"] = previous
        assert spawned["status"] == "succeeded" and spawned["proposal"]
        assert spawned["opencode_session_id"] != raw.opencode_session_id
        creates = [body for method, path, _, body in daemon.requests if method == "POST" and path == "/api/session"]
        assert len(creates) == 2
        assert creates[0]["permissions"][:2] == [
            {"action": "external_directory", "resource": "*", "effect": "deny"},
            {"action": "shell", "resource": "*", "effect": "deny"}]
        assert creates[1]["permissions"] == [
            {"action": "external_directory", "resource": "*", "effect": "deny"},
            {"action": "shell", "resource": "*", "effect": "allow"}]
        assert creates[1]["agent"] == "build"
        return {"leaf_session": raw.opencode_session_id, "spawn_session": spawned["opencode_session_id"],
                "creates": creates, "uid_isolation_proven": False, "native_login_proven": False}


def run(args: list[str], *, input_text: str | None = None, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args, cwd=ROOT, input=input_text, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, timeout=timeout, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {shlex.join(args)}\n{result.stdout}")
    return result


def docker_exec(*args: str, user: str = "root", input_text: str | None = None) -> str:
    command = ["docker", "exec", "--user", user]
    if input_text is not None:
        command.append("-i")
    command.extend([CONTAINER, *args])
    return run(command, input_text=input_text).stdout


def start_runtime(*environment: str) -> None:
    """Initialize the direct-run fixture before the shipped s6 services start.

    Setup's Compose init service provisions this controller-owned lease root.
    The proofs launch without Compose, so perform that same prerequisite in
    the container, then exec the real entrypoint as PID 1. Do not relax the
    selected plugin's production startup validation.
    """
    run([
        "docker", "run", "--detach", "--env", "MIMIR_PLUGINS=cultivate",
        "--env", "MIMIR_PR_CHECKOUT_LEASE_ROOT=/workspace/pr-leases",
        *(argument for value in environment for argument in ("--env", value)),
        "--name", CONTAINER, "--entrypoint", "/bin/sh", IMAGE, "-ceu",
        "install -d -o mimir -g mimir -m 0755 /workspace/pr-leases; exec /init",
    ])


def wait_for_runtime() -> None:
    deadline = time.monotonic() + 45
    last = ""
    while time.monotonic() < deadline:
        probe = subprocess.run(
            ["docker", "exec", CONTAINER, "/bin/sh", "-c",
             "test -S /run/mimir-worklink/socket/worklink-execd.sock"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
        )
        last = probe.stdout
        if probe.returncode == 0:
            return
        time.sleep(1)
    logs = run(["docker", "logs", CONTAINER]).stdout
    raise RuntimeError(f"shipped services did not become ready\n{last}\n{logs}")


def wait_for_health() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        probe = subprocess.run(
            ["docker", "exec", CONTAINER, "curl", "-fsS", "http://127.0.0.1:8080/health"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False,
        )
        if probe.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"PyPI image health did not become ready: {probe.stdout}\n"
                       + run(["docker", "logs", CONTAINER]).stdout)


# This image has no native daemon login. Exercise setup's real verification
# and require its refusal, rather than treating credential absence as readiness.
SETUP_DAEMON_PROOF = r'''
import subprocess
from pathlib import Path
from mimir_cultivate import setup

calls = []
def local_exec(argv, *, cwd):
    assert argv[:7] == ['docker', 'compose', 'exec', '-T', '-u', 'mimir', 'agent'], argv
    calls.append(argv[5])
    result = subprocess.run(['/command/s6-setuidgid', argv[5], *argv[7:]],
                            cwd='/tmp', capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()
setup._run = local_exec
try:
    setup._verify_opencode_daemon(Path('/tmp'))
except setup.SetupError as exc:
    assert 'worklink-opencode-login' in str(exc), str(exc)
else:
    raise AssertionError('credential-free image unexpectedly passed daemon verification')
assert calls == ['mimir'], calls
print('real-image setup OpenCode missing-login refusal passed')
'''


# Synthetic OpenCode wire peer only: production WorkerClient, root executor,
# SessionController, UID drop, checkout and publication paths remain real.
# No provider request or native login is established by this fixture.
DAEMON_FIXTURE = r'''
import json
import os
from pathlib import Path
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from mimir_cultivate.worklink.daemon_health import load_api
from mimir_cultivate.worklink.daemon_service import RuntimePaths

sessions = {}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, body, *, status=200, content_type='application/json'):
        raw = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def handle_request(self):
        import base64
        expected = 'Basic ' + base64.b64encode(
            (api['username'] + ':' + api['password']).encode()).decode()
        if self.headers.get('Authorization') != expected:
            return self.send({}, status=401)
        path = self.path.split('?')[0]
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or b'{}')
        if path == '/api/info':
            return self.send({'version': '2.2.7'})
        if path == '/api/session' and self.command == 'POST':
            sid = 'ses_image_' + str(len(sessions) + 1)
            rules = body['permissions']
            assert rules[0] == {'action': 'external_directory', 'resource': '*', 'effect': 'deny'}
            assert rules[1]['action'] == 'shell'
            kind = 'leaf' if rules[1]['effect'] == 'deny' else 'spawn'
            if kind == 'spawn':
                assert rules[1] == {'action': 'shell', 'resource': '*', 'effect': 'allow'}
            else:
                assert any(rule['effect'] == 'allow' for rule in rules[2:])
            info = {'id': sid, **body}
            sessions[sid] = {'info': info, 'kind': kind, 'messages': [], 'events': []}
            return self.send({'data': info})
        parts = path.split('/')
        sid = parts[4] if path.startswith('/api/experimental/session/') else parts[3]
        state = sessions[sid]
        if path.endswith('/environment'):
            state['environment'] = body['variables']
            return self.send('', status=204)
        if path.endswith('/prompt'):
            item = {'id': 'msg_' + sid + '_user', 'sessionID': sid, 'time': {'created': 1},
                    'type': 'user', 'payload': {'text': body['text']}, 'delivery': 'queue'}
            state['messages'].append({'id': item['id'], 'time': item['time'], 'type': 'user', 'text': body['text']})
            # The fake provider executes its fixed payload as the daemon's real
            # worklink uid, using the production session environment and cwd.
            payload = '/tmp/opencode-proof' if state['kind'] == 'leaf' else '/tmp/opencode-spawn-proof'
            result = subprocess.run([payload], cwd=state['info']['location']['directory'],
                                    env=state['environment'], capture_output=True, text=True, timeout=25)
            outcome = 'succeeded' if result.returncode == 0 else 'failed'
            state['messages'].append({'id': 'msg_' + sid + '_assistant', 'time': {'created': 2},
                                      'type': 'assistant', 'content': [{'type': 'text', 'text': result.stdout + result.stderr}]})
            idle = {'id': 'msg_' + sid + '_idle', 'time': {'created': 3}, 'type': 'idle', 'outcome': outcome}
            for index, event in enumerate(('started', outcome), 1):
                data = {'sessionID': sid}
                if event == 'failed':
                    data['error'] = {'type': 'fixture_payload_failed', 'message': 'fixed image payload failed'}
                    idle['error'] = data['error']
                state['events'].append({'id': 'evt_' + sid + '_' + str(index), 'created': index,
                    'type': 'session.execution.' + event,
                    'durable': {'aggregateID': sid, 'seq': index, 'version': 1}, 'data': data})
            state['messages'].append(idle)
            return self.send({'data': item})
        if path.endswith('/message'):
            return self.send({'data': state['messages'], 'cursor': {'previous': None, 'next': None}})
        if path.endswith('/inbox'):
            return self.send({'data': []})
        if path.endswith('/interrupt'):
            return self.send({'interrupted': False})
        if path.endswith('/log'):
            from urllib.parse import parse_qs, urlsplit
            after = int(parse_qs(urlsplit(self.path).query).get('after', ['0'])[0])
            events = [event for event in state['events'] if event['durable']['seq'] > after]
            events.append({'type': 'log.synced', 'aggregateID': sid, 'seq': len(state['events'])})
            return self.send(''.join('event: ' + event['type'] + '\ndata: ' + json.dumps(event) + '\n\n'
                                     for event in events), content_type='text/event-stream')
        return self.send({'data': state['info']})

    do_GET = do_POST = do_PUT = handle_request

if __name__ == '__main__':
    assert os.geteuid() == 1002
    api = load_api(RuntimePaths(), uid=os.geteuid())
    ThreadingHTTPServer(('127.0.0.1', 4096), Handler).serve_forever()
'''


MISSING_LOGIN_PROOF = r'''
import asyncio
import json
import os
from pathlib import Path
from unittest.mock import patch
from mimir.tools.registry import set_spawn_config
from mimir_cultivate.opencode import spawn_open_code
from mimir_cultivate.worklink.daemon_health import daemon_health
from mimir_cultivate.worklink.daemon_gate import workload_refusal
from mimir_cultivate.worklink.worker_client import WorkerClient

async def main():
    assert os.geteuid() == 1001, 'missing-login proof must run as mimir'
    # Do not mock health: wait for the real credential-free daemon to publish.
    for _ in range(150):
        health = daemon_health(dict(os.environ))
        if workload_refusal(health) == 'login_missing':
            break
        await asyncio.sleep(0.2)
    assert health.get('daemon_enabled') is True and health.get('login_state') == 'missing', health
    assert workload_refusal(health) == 'login_missing', health
    calls = []
    async def forbidden_session(*args, **kwargs):
        calls.append(True)
        raise AssertionError('missing login reached session admission')
    seed = Path(os.environ['WORKLINK_REPO'])
    artifacts = Path('/home/mimir/worklink-refusal-artifacts')
    set_spawn_config({'default_cwd': seed, 'artifact_root': artifacts})
    with patch.object(WorkerClient, 'session_create', forbidden_session):
        result = json.loads(await spawn_open_code.ainvoke({
            'prompt': 'credential-free refusal proof', 'cwd': str(seed), 'timeout_s': 30,
            'model': 'proof/model', 'artifact_root': str(artifacts)}))
    assert result['status'] == 'refused' and result['reason_code'] == 'login_missing', result
    assert result['opencode_session_id'] is None and not calls, result
    print(json.dumps({'proof': 'missing_login', 'reason_code': result['reason_code'], 'sessions_started': 0,
                      'native_login_proven': False}))

if __name__ == '__main__':
    asyncio.run(main())
'''


CONTROLLER_PROOF = r"""
from __future__ import annotations
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

from mimir_cultivate.worklink.backends.base import WorkOrder
from mimir_cultivate.worklink.backends.opencode import OpenCodeBackend
from mimir_cultivate.worklink.checkout import cleanup_checkout, create_isolated_checkout
from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend
from mimir_cultivate.worklink.evidence import observe_evidence
from mimir_cultivate.worklink.safe_git import ControllerGitPublication

REPO = Path(os.environ["WORKLINK_REPO"])
METADATA = Path("/home/mimir/worklink-publication")
CONFIG = Path("/home/mimir/worklink-opencode/opencode.json")
DATA = Path("/home/mimir/worklink-opencode/data")
CLI = "/tmp/opencode-proof"

GATE_TEST = r'''test "$(id -u)" = 1002
printf 'gate-euid=%s\\n' "$(id -u)"
test "$(cat tracked)" = worker-modified
test "$(cat created)" = worker-created
test "$(cat controller-continuation)" = continued
! test -e deleted
! test -e "$HOME/.config/opencode/opencode.json"
! test -e "$HOME/.local/share/opencode/auth.json"
! cat /home/mimir/worklink-canary
'''

async def main():
    if os.geteuid() != 1001:
        raise RuntimeError("controller proof is not running as mimir")
    os.environ.update({
        "MIMIR_PLUGINS": "cultivate",
        "MIMIR_MODEL_SPEC": "proof:model",
        "OPENCODE_CONFIG": str(CONFIG),
        "XDG_DATA_HOME": str(DATA),
    })
    lease = create_isolated_checkout(
        REPO, issue_id=1410, attempt=1, base="main", worker_eligible=True
    )
    sibling = create_isolated_checkout(
        REPO, issue_id=1411, attempt=1, base="main", worker_eligible=True
    )
    if lease.worker_authorized or lease.authorization is not None:
        raise RuntimeError("Worklink unexpectedly selected the contained checkout path")
    if sibling.worker_authorized or sibling.authorization is not None:
        raise RuntimeError("concurrent checkout unexpectedly selected the contained path")
    sibling.path.joinpath("sibling-canary").write_text("sibling-original")
    source_object = next(REPO.joinpath(".git/objects").glob("[0-9a-f][0-9a-f]/*"))
    checkout_object = lease.path / ".git/objects" / source_object.relative_to(REPO / ".git/objects")
    if source_object.stat().st_ino == checkout_object.stat().st_ino:
        raise RuntimeError("issued checkout retained a source hardlink")
    checkout_fd = os.open(lease.path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        publication = ControllerGitPublication.capture(
            checkout_fd, REPO, lease.branch, METADATA
        )
    finally:
        os.close(checkout_fd)
    compute = OpenCodeDaemonComputeBackend()
    backend = OpenCodeBackend()
    order = WorkOrder(
        issue_id=1410,
        checkout=lease.path,
        prompt="apply the runtime proof edit",
        rules=None,
        timeout_s=30,
        env={},
    )
    spec = backend.work_spec(
        order,
        attempt=1,
        repo_url=str(REPO),
        base_ref="main",
        branch=lease.branch,
        # The evidence gate runs only the command its worker-bound work spec
        # carries, as the orchestrator passes one test_cmd to both.
        test_command=GATE_TEST,
    )
    projections = spec.backend_config.get("worker_projections", ())
    if projections or spec.local_argv is not None:
        raise RuntimeError("daemon leaf attempted a legacy auth projection or process launch")
    from unittest.mock import patch
    from mimir_cultivate.worklink.daemon_service import RuntimePaths, read_json
    generation = read_json(RuntimePaths().snapshot, uid=1001, mode=0o600)["launch_generation"]
    # Readiness is a fixture, not evidence of native login. Session admission
    # still crosses WorkerClient -> root executor -> real SessionController.
    with patch("mimir_cultivate.worklink.daemon_health.daemon_health",
               return_value={"ok": True, "launch_generation": generation}):
        handle = await compute.launch(spec)
    try:
        result = await compute.wait(handle, spec.timeout_s)
    finally:
        await compute.cleanup(handle)
    if result.exit_code != 0 or "build-euid=1002" not in result.stdout:
        raise RuntimeError(f"production build failed: {result}")
    if lease.path.joinpath("tracked").read_text() != "worker-modified":
        raise RuntimeError("controller could not observe worker edit")
    lease.path.joinpath("controller-continuation").write_text("continued")
    gate = await observe_evidence(
        issue=1410,
        attempt=1,
        backend="opencode",
        branch=lease.branch,
        checkout=lease.path,
        started_at=datetime.now(UTC),
        base_ref=lease.local_base,
        backend_status="completed",
        test_command=GATE_TEST,
        safe_git=publication,
        work_spec=spec,
        compute=compute,
    )
    if gate.evidence.tests is None or gate.evidence.tests.exit_code != 0:
        raise RuntimeError(f"production evidence gate failed: {gate}")
    if "gate-euid=1002" not in (gate.evidence.tests.summary or ""):
        raise RuntimeError("controller did not observe evidence-gate worker identity")
    if "tracked" not in gate.evidence.files_changed:
        raise RuntimeError("controller evidence did not observe worker changes")
    publication.run("add", "-A", check=True)
    publication.run("commit", "-m", "runtime proof", check=True)
    publication.push(check=True)
    if Path("/tmp/worklink-publication-attack").exists():
        raise RuntimeError("controller publication executed worker-planted Git metadata")
    trusted_head = publication.run("rev-parse", "HEAD", check=True).stdout.strip()
    remote_head = os.popen(
        "git --git-dir=/home/mimir/worklink-remote.git rev-parse refs/heads/issue/1410-a1"
    ).read().strip()
    if remote_head != trusted_head:
        raise RuntimeError("controller publication followed the worker push URL")
    if os.system(
        "git --git-dir=/tmp/worklink-hostile.git show-ref --verify refs/heads/issue/1410-a1 >/dev/null 2>&1"
    ) == 0:
        raise RuntimeError("worker-controlled remote received the publication")
    if Path("/home/mimir/worklink-canary").read_text() != "controller-reset":
        raise RuntimeError("worker changed controller canary")
    if sibling.path.joinpath("sibling-canary").read_text() != "sibling-original":
        raise RuntimeError("worker changed concurrent sibling checkout")
    cleanup_checkout(lease, outcome="completed")
    cleanup_checkout(sibling, outcome="completed")
    publication.close()
    if lease.path.exists():
        raise RuntimeError("production checkout cleanup failed")
    print(json.dumps({"proof": "controller", "native_login_proven": False,
                      "uid_isolation_proven": True, "publication_proven": True}))

asyncio.run(main())
"""

TOOLCHAIN_PROOF = r"""
import json
from pathlib import Path
import subprocess
import tempfile
from mimir_cultivate.setup import toolchain_test_env, toolchain_probe_environment, _TOOLCHAIN_WORKER_PROBE
from mimir_cultivate.worklink.backends.registry import WorklinkConfig
import yaml

with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / 'worklink.yaml'
    path.write_text(yaml.safe_dump({'backends': {'opencode': {'test_env': toolchain_test_env('rust')}}}))
    config = WorklinkConfig.load(path)
    for missing in (None, 'RUSTUP_HOME'):
        if missing:
            del config.backend_settings['opencode']['test_env'][missing]
        selected = toolchain_probe_environment(config)
        # env -i is essential: Docker image ENV must not rescue a broken gate.
        result = subprocess.run(
            ['/command/s6-setuidgid', 'worklink', 'env', '-i',
             '/usr/local/bin/python', '-c', _TOOLCHAIN_WORKER_PROBE,
             json.dumps(selected), 'rust', '1.90.0'],
            cwd='/tmp', capture_output=True, text=True, timeout=120,
        )
        expected = 'toolchain_missing: rustc' if missing else 'ok'
        assert result.returncode == 0 and result.stdout.strip() == expected, (missing, result)
        if not missing:
            # Execute Cargo's own subcommand lookup with exactly the clean gate
            # environment. The probe's version checks must not hide a missing
            # /usr/local/bin/cargo-fmt behind the install-time CARGO_HOME.
            gate = ['/command/s6-setuidgid', 'worklink', 'env', '-i',
                    *(f'{key}={value}' for key, value in selected.items())]
            for command in (['cargo', 'fmt', '--version'], ['cargo', 'clippy', '--version'],
                            ['rustdoc', '--version']):
                checked = subprocess.run([*gate, *command], cwd='/tmp',
                                         capture_output=True, text=True, timeout=120)
                assert checked.returncode == 0, (command, checked)
            with tempfile.TemporaryDirectory() as project:
                root = Path(project)
                root.chmod(0o777)  # worklink must be able to write Cargo's target/.
                (root / 'Cargo.toml').write_text('[package]\nname = "toolchain-proof"\nversion = "0.1.0"\nedition = "2021"\n')
                (root / 'src').mkdir()
                (root / 'src/lib.rs').write_text('pub fn proof() {}\n')
                checked = subprocess.run([*gate, 'cargo', 'doc', '--offline', '--no-deps'],
                                         cwd=project, capture_output=True, text=True, timeout=120)
                assert checked.returncode == 0, checked
print('real-image clean gate toolchain proof passed (including missing-RUSTUP_HOME control)')
"""


SPAWN_PROOF = r"""
from __future__ import annotations
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

from mimir.tools.registry import set_spawn_config
from mimir_cultivate.opencode import spawn_open_code

SEED = Path(os.environ["WORKLINK_REPO"])
ARTIFACTS = Path("/home/mimir/worklink-spawn-artifacts")
CONFIG = Path("/home/mimir/worklink-opencode/opencode.json")
AUTH = Path("/home/mimir/worklink-opencode/data/opencode/auth.json")
CANARY = Path("/home/mimir/worklink-canary")
OAUTH = (b"oauth-access-proof", b"oauth-refresh-proof")

async def main():
    if os.geteuid() != 1001:
        raise RuntimeError("spawn controller proof is not running as mimir")
    os.environ.update({
        "OPENCODE_CONFIG": str(CONFIG),
        "XDG_DATA_HOME": str(AUTH.parents[1]),
        "MIMIR_MODEL_SPEC": "proof:model",
    })
    set_spawn_config({
        "default_cwd": SEED,
        "artifact_root": ARTIFACTS,
        "opencode_config_path": CONFIG,
    })
    source_head = (SEED / ".git/HEAD").read_bytes()

    def seed_status() -> bytes:
        return subprocess.run(
            ["git", "-C", str(SEED), "status", "--porcelain=v1", "-z"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout

    source_status = seed_status()
    from unittest.mock import patch
    from mimir_cultivate.worklink.daemon_service import RuntimePaths, read_json
    generation = read_json(RuntimePaths().snapshot, uid=1001, mode=0o600)["launch_generation"]
    # The fixed synthetic wire peer executes the payload, not a provider.
    with patch("mimir_cultivate.worklink.daemon_health.daemon_health",
               return_value={"ok": True, "launch_generation": generation}):
        raw = await spawn_open_code.ainvoke({
            "prompt": "generate and execute the containment proof payload",
            "cwd": str(SEED),
            "timeout_s": 30,
            "name": "shipped-image-proof",
            "model": "proof/model",
            "artifact_root": str(ARTIFACTS),
        })
    result = json.loads(raw)
    if result["status"] != "succeeded" or result["exit_code"] != 0:
        raise RuntimeError(f"live spawn failed: {result}")
    if not result["run_id"].startswith("opencode-"):
        raise RuntimeError("live spawn did not preserve its public run handle")
    if result["artifact_dir"] != result["run_id"]:
        raise RuntimeError("live spawn did not return its relative artifact handle")
    if "spawn-euid=1002" not in result["stdout"]:
        raise RuntimeError("live fake OpenCode did not report worker identity")
    if not isinstance(result.get("opencode_session_id"), str) or not result["opencode_session_id"].startswith("ses_"):
        raise RuntimeError("spawn did not preserve its owned OpenCode session identity")
    proposal = result.get("proposal")
    if not isinstance(proposal, dict):
        raise RuntimeError("live spawn did not produce a proposal")
    patch = base64.b64decode(proposal["patch"], validate=True)
    if proposal.get("schema_version") != 1 or proposal.get("kind") != "git_binary_patch":
        raise RuntimeError("live spawn proposal schema is invalid")
    if proposal.get("byte_length") != len(patch):
        raise RuntimeError("live spawn proposal length is invalid")
    if proposal.get("sha256") != hashlib.sha256(patch).hexdigest():
        raise RuntimeError("live spawn proposal digest is invalid")
    for marker in (b"spawn-payload-executed", b"spawn-worker-uid"):
        if marker not in patch:
            raise RuntimeError(f"live spawn proposal omitted {marker!r}")
    artifact = ARTIFACTS / result["artifact_dir"]
    observed = raw.encode()
    for path in artifact.rglob("*"):
        if path.is_file():
            observed += path.read_bytes()
    forbidden = (*OAUTH, os.fsencode(CONFIG), os.fsencode(AUTH))
    for material in forbidden:
        if material in observed:
            raise RuntimeError("spawn output or artifacts disclosed OAuth material or controller paths")
    if CANARY.read_text() != "controller-reset":
        raise RuntimeError("generated OpenCode payload changed controller canary")
    if (SEED / ".git/HEAD").read_bytes() != source_head:
        raise RuntimeError("spawn changed the seed repository HEAD")
    if seed_status() != source_status:
        raise RuntimeError("spawn changed the seed repository working tree")
    for path in sorted(ARTIFACTS.rglob("*"), reverse=True):
        if path.is_file() or path.is_symlink():
            path.unlink()
        else:
            path.rmdir()
    ARTIFACTS.rmdir()
    print(json.dumps({"proof": "spawn", "native_login_proven": False,
                      "uid_isolation_proven": True, "proposal_proven": True}))

asyncio.run(main())
"""


def source_ref() -> str:
    """Return the exact remote ref expected to contain this checkout's HEAD."""
    explicit = os.environ.get("MIMIR_GIT_REF") or os.environ.get("GITHUB_REF")
    if explicit:
        return explicit
    try:
        branch = run(["git", "symbolic-ref", "--quiet", "--short", "HEAD"]).stdout.strip()
        tracked_ref = run(["git", "config", "--get", f"branch.{branch}.merge"]).stdout.strip()
    except RuntimeError as exc:
        raise RuntimeError(
            "set MIMIR_GIT_REF to the fully qualified remote branch, tag, or pull ref "
            "that contains this commit"
        ) from exc
    if not tracked_ref.startswith("refs/"):
        raise RuntimeError(f"tracked Git ref is not fully qualified: {tracked_ref!r}")
    return tracked_ref


def source_url() -> str | None:
    """Return the clone URL for the repository CI is running in, if known.

    The Dockerfile default names the canonical upstream, which a fork, a
    renamed repository, or a pull ref absent from upstream cannot satisfy.
    """
    explicit = os.environ.get("MIMIR_GIT_URL")
    if explicit:
        return explicit
    repository = os.environ.get("GITHUB_REPOSITORY")
    if repository:
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
        return f"{server}/{repository}.git"
    return None


def require_rust_native_toolchain(layer: str) -> None:
    # The root image may acquire a compiler independently; require this
    # child layer to provide the tools whose linking is proved below.
    if 'apt-get install -y --no-install-recommends gcc libc6-dev binutils pkg-config && ' not in layer:
        raise RuntimeError('generated Rust layer omitted its native toolchain')


def wheel_identity(path: Path, name: str) -> tuple[str, str]:
    parsed_name, version, _, _ = parse_wheel_filename(path.name)
    if parsed_name != canonicalize_name(name):
        raise ValueError(f"expected {name} wheel: {path.name}")
    with zipfile.ZipFile(path) as archive:
        members = [member for member in archive.namelist() if member.endswith(".dist-info/METADATA")]
        if len(members) != 1:
            raise ValueError(f"missing or ambiguous {name} METADATA")
        from email.parser import Parser

        metadata = Parser().parsestr(archive.read(members[0]).decode("utf-8"))
        if (canonicalize_name(metadata.get("Name", "")) != parsed_name
                or metadata.get("Version") != str(version)):
            raise ValueError(f"{name} wheel filename and METADATA disagree")
        if name == "mimir-cultivate" and f"mimir-agent=={version}" not in [
            dependency.replace(" ", "") for dependency in metadata.get_all("Requires-Dist", [])
        ]:
            raise ValueError("Cultivate wheel does not pin the paired core version")
    return str(version), hashlib.sha256(path.read_bytes()).hexdigest()


PYPI_RUNTIME_PROOF = r'''
import asyncio
import json
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.request import urlopen

import os
assert os.geteuid() == 1001, 'protocol and health proof must run as mimir'

from mimir_cultivate.install_identity import controller_identity, executor_identity
from mimir_cultivate.worklink.worker_client import verify_executor_identity
expected = json.loads(sys.argv[1])
affected = sys.argv[2] if len(sys.argv) > 2 else ''

def health():
    try:
        with urlopen('http://127.0.0.1:8080/health', timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)

if affected:
    verify = (lambda: asyncio.run(verify_executor_identity())) if affected == 'worker' else controller_identity
    try:
        verify()
    except (RuntimeError, ValueError, OSError):
        pass
    else:
        raise AssertionError('identity accepted mutated ' + affected)
    status, payload = health()
    assert status == 503 and payload['ok'] is False, (affected, status, payload)
    sys.exit(0)

assert controller_identity() == expected
assert executor_identity() == expected
assert asyncio.run(verify_executor_identity()) == expected
status, payload = health()
assert status == 200 and payload['ok'] is True, (status, payload)
assert payload['worklink_install_mode'] == 'pypi', payload
assert payload['worklink_install_status'] == 'matched', payload
assert payload['worklink_controller_distributions'] == expected['distributions'], payload
assert payload['worklink_executor_distributions'] == expected['distributions'], payload
assert payload['worklink_executor_source_commit'] is None, payload
assert payload['worklink_controller_source_commit'] is None, payload
assert payload['worklink_executor_source_status'] == 'not_applicable', payload

print('live PyPI pair, protocol and health proof passed')
'''


PYPI_READONLY_IMPORT_PROOF = r'''
import os
from pathlib import Path
import subprocess
import sys

assert os.geteuid() == 1001
site = next(Path('/home/mimir/venv/lib').glob('python*/site-packages'))
for package in ('mimir', 'mimir_cultivate'):
    root = site / package
    for path in (root, *root.rglob('*')):
        assert path.stat().st_uid == 0, path
        assert path.stat().st_mode & 0o222 == 0, path
        assert not os.access(path, os.W_OK), path
# No image ENV can rescue this deliberately scrubbed launcher. Both packages
# really import, yet neither can create bytecode, even outside known launchers.
result = subprocess.run([sys.executable, '-c',
                         'import mimir.pollers, mimir_cultivate.install_identity'],
                        env={'PATH': '/home/mimir/venv/bin:/usr/bin:/bin', 'HOME': '/home/mimir'},
                        capture_output=True, text=True, timeout=30)
assert result.returncode == 0, result.stderr
for package in ('mimir', 'mimir_cultivate'):
    assert not list((site / package).rglob('*.pyc'))
'''


def prove_pypi_runtime(pair: dict) -> None:
    """Keep protocol/health as the controller; root only reads/mutates fixtures."""
    python = '/home/mimir/venv/bin/python'
    expected = json.dumps(pair, sort_keys=True)

    def check(affected: str = '') -> None:
        docker_exec(python, '-c', PYPI_RUNTIME_PROOF, expected, affected, user='mimir')

    docker_exec(python, '-c', PYPI_READONLY_IMPORT_PROOF, user='mimir')
    check()
    mutations = [('/home/mimir/venv/controller-install-identity.json', b'{}', 'controller'),
                 ('/opt/mimir-worklink/executor-install-identity.json', b'{}', 'worker')]
    for venv, affected in (('/home/mimir/venv', 'controller'), ('/opt/mimir-worklink/venv', 'worker')):
        path = docker_exec(python, '-c',
                           "from pathlib import Path; import sys; print(next(Path(sys.argv[1]).glob('lib/python*/site-packages/mimir_cultivate/install_identity.py')))",
                           venv, user='root').strip()
        mutations.append((path, b'# altered installed content\n', affected))
    mutations += [('/opt/mimir-worklink/wheels/' + item['wheel'], b'not a wheel', 'both')
                  for item in pair['distributions'].values()]
    for path, replacement, affected in mutations:
        # Keep full wheels inside the container: putting their bytes in argv
        # can exceed ARG_MAX. mkstemp creates a private root-owned backup.
        backup = docker_exec(python, '-c',
                             'import os, shutil, sys, tempfile; fd, backup = tempfile.mkstemp(); '
                             'os.close(fd); shutil.copyfile(sys.argv[1], backup); print(backup)',
                             path, user='root').strip()
        try:
            # Append to Python sources so fresh controller subprocesses can
            # still import the verifier and detect the changed wheel bytes.
            docker_exec(python, '-c',
                        'import base64, pathlib, sys; path = pathlib.Path(sys.argv[1]); '
                        'data = base64.b64decode(sys.argv[2]); '
                        'path.write_bytes(path.read_bytes() + data if path.suffix == ".py" else data)',
                        path, base64.b64encode(replacement).decode(), user='root')
            check(affected)
        finally:
            docker_exec(python, '-c',
                        'import pathlib, shutil, sys; shutil.copyfile(sys.argv[2], sys.argv[1]); '
                        'pathlib.Path(sys.argv[2]).unlink()', path, backup, user='root')
        check()
    print('live PyPI pair, protocol, health and mutation proof passed')


def prepare_pypi_deployment(directory: Path, agent_wheel: Path, cultivate_wheel: Path,
                            version: str, agent_hash: str, cultivate_hash: str) -> Path:
    """Drive the real setup writer; substitute only index/GitHub facts and stop
    at its first Docker invocation, then execute its saved build.sh verbatim.
    """
    from unittest.mock import patch
    from mimir_cultivate import setup as installer
    from mimir_cultivate.release_resolver import ReleasePair, Wheel

    parser = argparse.ArgumentParser()
    installer.add_argparse(parser.add_subparsers())
    token = directory / "proof-credential"
    token.write_text("offline-image-proof")
    token.chmod(0o600)
    deploy = directory / "deployment"
    args = parser.parse_args([
        "cultivate", "setup", "--accept-license", "--repo", "fixture/proof",
        "--bot-login", "proof-bot", "--bot-email", "proof@example.com",
        "--owner", "proof", "--display-name", "Proof", "--owner-email", "proof@example.com",
        "--branch", "main", "--model-spec", "openai:proof", "--saga-model-spec", "openai:proof:low",
        "--embedding", "local", "--github-token-file", str(token), "--model-key-file", str(token),
        "--operator-alert-adapter", "none", "--deploy", str(deploy), "--port", "8123",
        "--test-command", "uv run pytest -q", "--review-tests", "targeted",
        "--auto-review", "all", "--mimir-version", version, "--platform", "linux/amd64", "--no-start",
    ])
    pair = ReleasePair(version, *(
        Wheel(name, path.name, "https://files.pythonhosted.org/" + path.name, digest, path.read_bytes())
        for name, path, digest in (("mimir-agent", agent_wheel, agent_hash),
                                  ("mimir-cultivate", cultivate_wheel, cultivate_hash))))

    def request(url, token):
        import base64
        if url.endswith("/user"):
            return 200, {"login": "proof-bot"}
        if url.endswith("/repos/fixture/proof"):
            return 200, {"permissions": {"push": True}}
        if "/git/trees/" in url:
            return 200, {"tree": []}
        if "/contents/" in url:
            content = ("[project]\nname = 'proof'\nversion = '1.0'\n" if "/pyproject.toml?" in url
                       else "3.11.15" if "/.python-version?" in url else None)
            return ((200, {"encoding": "base64", "content": base64.b64encode(content.encode()).decode()})
                    if content else (404, {}))
        return 200, {}

    class Prepared(Exception):
        pass

    def stop_before_docker(argv, *, cwd):
        assert argv[:2] == ["docker", "build"], argv
        raise Prepared

    with patch.object(installer, "resolve_release", return_value=pair):
        try:
            installer.setup(args, request=request, run=stop_before_docker)
        except Prepared:
            pass
        else:
            raise RuntimeError("setup did not reach its Docker build")
    return deploy


def prove_pypi(agent_wheel: Path, cultivate_wheel: Path) -> None:
    global IMAGE

    run(["docker", "info"], timeout=30)
    agent_wheel, cultivate_wheel = agent_wheel.resolve(), cultivate_wheel.resolve()
    version, agent_hash = wheel_identity(agent_wheel, "mimir-agent")
    cultivate_version, cultivate_hash = wheel_identity(cultivate_wheel, "mimir-cultivate")
    if version != cultivate_version:
        raise ValueError("wheel pair must have the same exact version")
    pair = {"mode": "pypi", "distributions": {
        "mimir-agent": {"version": version, "wheel": agent_wheel.name, "sha256": agent_hash},
        "mimir-cultivate": {"version": version, "wheel": cultivate_wheel.name, "sha256": cultivate_hash},
    }}
    with tempfile.TemporaryDirectory(prefix="mimir-wheel-image-") as directory:
        deploy = prepare_pypi_deployment(Path(directory), agent_wheel, cultivate_wheel,
                                         version, agent_hash, cultivate_hash)
        manifest = json.loads((deploy / "manifest.json").read_text())
        IMAGE = manifest["layer_tag"]
        try:
            run(["sh", str(deploy / "build.sh")], timeout=2400)
            start_runtime()
            wait_for_runtime()
            wait_for_health()
            docker_exec("/bin/sh", "-ceu", """
                test -x /home/mimir/venv/bin/python
                test -x /opt/mimir-worklink/venv/bin/python
                test "$(stat -c %U:%G /opt/mimir-worklink/wheels)" = root:root
                test "$(stat -c %U:%G /home/mimir/venv/controller-install-identity.json)" = root:root
                test "$(stat -c %U:%G /opt/mimir-worklink/executor-install-identity.json)" = root:root
                test ! -e /opt/mimir-worklink/executor-source-commit
                test ! -e /home/mimir/venv/controller-source-commit
            """)
            for interpreter in ("/home/mimir/venv/bin/python", "/opt/mimir-worklink/venv/bin/python"):
                docker_exec(interpreter, "-c", "import importlib.metadata as m; "
                            "from mimir_cultivate.install_identity import controller_identity, executor_identity; "
                            f"assert {'controller_identity' if interpreter.startswith('/home') else 'executor_identity'}(); "
                            "[m.version(name) for name in ('langchain-openai', 'sentence-transformers', 'mcp')]; assert m.version('torch').endswith('+cpu')",
                            user="root")
            prove_pypi_runtime(pair)
            print("worklink shipped-image PyPI identity proof passed")
        finally:
            subprocess.run(["docker", "rm", "--force", CONTAINER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["docker", "image", "rm", "--force", IMAGE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def prove_source() -> None:
    repo = os.environ.get("WORKLINK_REPO", "").strip()
    if not repo or not Path(repo).is_absolute():
        raise RuntimeError("WORKLINK_REPO must name the absolute in-container proof repository")
    run(["docker", "info"], timeout=30)
    source_commit = run(["git", "rev-parse", "HEAD"]).stdout.strip()
    git_ref = source_ref()
    git_url = source_url()
    if run(["git", "status", "--porcelain=v1"]).stdout.strip():
        raise RuntimeError("live image proof requires a clean source checkout")
    service = (ROOT / "deploy/s6-overlay/s6-rc.d/mimir/run").read_text()
    # Match the shipped command after joining shell continuations. The identity
    # environment is part of the privilege-drop contract, not an optional wrapper.
    expected = (
        'exec s6-setuidgid mimir env HOME=/home/mimir USER=mimir LOGNAME=mimir '
        'mimir run --home "${MIMIR_HOME:-/home/mimir/agent}"'
    )
    if expected not in " ".join(service.replace("\\\n", " ").split()):
        raise RuntimeError("controller s6 service no longer drops to mimir with matching identity environment")
    try:
        run([
            "docker", "build",
            "--build-arg", "MIMIR_INSTALL_MODE=source",
            "--build-arg", f"MIMIR_GIT_REF={git_ref}",
            *(["--build-arg", f"MIMIR_GIT_URL={git_url}"] if git_url else []),
            "--build-arg", f"MIMIR_CONTROLLER_COMMIT={source_commit}",
            "--build-arg", f"MIMIR_EXECUTOR_COMMIT={source_commit}",
            # The worker exists only in a Cultivate image; select the in-repo
            # plugin so this proof exercises it from the same commit.
            "--build-arg", "MIMIR_PLUGIN_SELECTION=cultivate",
            # Setup's leaf-executable proof must run the real pinned OpenCode,
            # not only the fake protocol CLIs used by the other proofs below.
            "--build-arg", "MIMIR_ENABLE_OPENCODE=1",
            "--build-arg", "CHAINLINK_ARTIFACT_STAGE=chainlink-build",
            "--tag", IMAGE, ".",
        ], timeout=1200)
        # Exercise the actual generated Rust layer, not the root build's private
        # chainlink compiler. The tag now names the toolchain child image.
        layer = run(['docker', 'run', '--rm', '--entrypoint',
                     '/opt/mimir-worklink/venv/bin/python', IMAGE, '-c',
                     'from mimir_cultivate.setup import render_layer; '
                     f'print(render_layer("rust", "1.90.0", {IMAGE!r}))']).stdout
        require_rust_native_toolchain(layer)
        run(['docker', 'build', '--file', '-', '--tag', IMAGE, '.'],
            input_text=layer, timeout=1200)
        start_runtime("HOME=/home/mimir", f"WORKLINK_REPO={repo}")
        wait_for_runtime()
        docker_exec("/bin/sh", "-ceu", """
            test "$(id -u mimir)" = 1001
            test "$(id -u worklink)" = 1002
            test "$(stat -c %a:%u:%g /home/mimir)" = 700:1001:1001
            test "$(stat -c %U:%G /usr/local/libexec/worklink-execd)" = root:root
            test "$(stat -c %U:%G /opt/mimir-worklink/venv/bin/python)" = root:root
            test "$(stat -c %U:%G /opt/mimir-worklink/uv-cache)" = root:root
            test "$(stat -c %a /opt/mimir-worklink/uv-cache)" = 755
            test "$(cat /opt/mimir-worklink/executor-source-commit)" = "SOURCE_COMMIT"
            test "$(cat /home/mimir/venv/controller-source-commit)" = "SOURCE_COMMIT"
            test "$(stat -c %a:%u:%g /var/lib/mimir-worklink/homes)" = 710:0:1002
        """.replace("SOURCE_COMMIT", source_commit))
        # Both venvs must run Cultivate built from the pinned source tree, never an
        # index wheel, and the worker's core must be the overlay, not a pin's.
        docker_exec("/bin/sh", "-ceu", r"""
            for python in /opt/mimir-worklink/venv/bin/python /home/mimir/venv/bin/python; do
                "$python" - <<'PY'
import importlib.metadata as metadata, json, sys
for name in ("mimir-agent", "mimir-cultivate"):
    origin = json.loads(metadata.distribution(name).read_text("direct_url.json") or "{}")
    if not origin.get("url", "").startswith("file://"):
        raise SystemExit(f"{sys.executable}: {name} is not installed from the pinned source: {origin}")
PY
            done
        """)
        # Read the live uids from /proc INSIDE the container.
        #
        # ``docker top`` is unusable for this. It runs ps on the HOST, so its
        # ``user`` column resolves against the host's passwd database: on a
        # GitHub runner host uid 1001 is ``runner``, so a correctly-dropped
        # mimir service reports as ``runner`` and a name comparison fails while
        # the container is entirely correct. Asking that host ps for a numeric
        # ``uid`` column is not portable either — Docker Desktop's ps rejects it
        # outright (``bad -o argument 'uid'``), so the check would then pass on
        # GitHub and fail locally, trading one host dependency for another.
        #
        # /proc needs no ps at all, which also keeps this working in the shipped
        # image, where procps is absent.
        process_table = docker_exec("/bin/sh", "-ceu", r"""
            for proc in /proc/[0-9]*; do
                [ -r "$proc/cmdline" ] || continue
                cmd=$(tr '\0' ' ' < "$proc/cmdline")
                [ -n "$cmd" ] || continue
                uid=$(awk '/^Uid:/ {print $2; exit}' "$proc/status" 2>/dev/null) || continue
                printf '%s\t%s\t%s\n' "${proc#/proc/}" "$uid" "$cmd"
            done
        """)
        rows = [line.split("\t", 2) for line in process_table.splitlines() if line.count("\t") >= 2]
        if not any(uid == "1001" and "mimir run" in cmd for _, uid, cmd in rows):
            raise RuntimeError(f"live mimir service is not uid 1001\n{process_table}")
        if not any(uid == "0" and "mimir_cultivate.worklink.worker_exec" in cmd for _, uid, cmd in rows):
            raise RuntimeError(f"live worker executor is not root\n{process_table}")

        # Exercise setup's real generated probes before any fake OpenCode is
        # installed below. Only Docker's transport is adapted: the production
        # argv, named users, private controller venv and installed CLI are real.
        docker_exec('/opt/mimir-worklink/venv/bin/python', '-c', SETUP_DAEMON_PROOF)
        print(docker_exec('/opt/mimir-worklink/venv/bin/python', '-c', MISSING_LOGIN_PROOF, user='mimir'))

        # Replace only the wire peer after proving refusal with real health.
        # The shipped root executor and its session helpers stay untouched.
        docker_exec('/command/s6-svc', '-wD', '-T', '30000', '-d', '/run/service/worklink-opencode')
        docker_exec('/bin/sh', '-ceu', 'cat > /tmp/worklink-daemon-fixture.py; chmod 0644 /tmp/worklink-daemon-fixture.py',
                    input_text=DAEMON_FIXTURE)
        run(['docker', 'exec', '--detach', '--user', 'root', CONTAINER,
             '/opt/mimir-worklink/venv/bin/python', '-c',
             'from mimir_cultivate.worklink.daemon_service import exec_worklink; '
             'import os; exec_worklink(("/opt/mimir-worklink/venv/bin/python", '
             '"/tmp/worklink-daemon-fixture.py"), dict(os.environ), cwd="/")'])
        docker_exec('/opt/mimir-worklink/venv/bin/python', '-c', '''
import time
import urllib.request
for _ in range(100):
    try:
        urllib.request.urlopen('http://127.0.0.1:4096/api/info', timeout=1)
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            break  # synthetic peer is listening and still requires auth
        raise
    except OSError:
        time.sleep(0.1)
else:
    raise RuntimeError('synthetic daemon peer did not start')
''')

        docker_exec('/opt/mimir-worklink/venv/bin/python', '-c', TOOLCHAIN_PROOF)

        setup = r"""
            set -eu
            install -d -o mimir -g mimir -m 0755 /workspace
            /command/s6-setuidgid mimir sh -ceu 'printf controller-writable > /home/mimir/worklink-canary; printf controller-reset > /home/mimir/worklink-canary'
            test "$(cat /home/mimir/worklink-canary)" = controller-reset
            /command/s6-setuidgid mimir sh -ceu '
              rm -rf "$WORKLINK_REPO" /workspace/.worklink /home/mimir/worklink-remote.git /home/mimir/worklink-opencode /home/mimir/worklink-publication
              mkdir -p /workspace
              git init --bare -q /home/mimir/worklink-remote.git
              git init -q "$WORKLINK_REPO"
              cd "$WORKLINK_REPO"
              git remote add origin /home/mimir/worklink-remote.git
              printf base > tracked
              printf remove > deleted
              git add tracked deleted
              git -c user.name=test -c user.email=test@example.com commit -qm base
              git branch -M main
              git push -q -u origin main
              git config user.name test
              git config user.email test@example.com
              git config credential.helper "!f() { echo username=controller; echo password=trusted; }; f"
              mkdir -p /home/mimir/worklink-opencode/data/opencode
              rm -rf /tmp/worklink-hostile.git /tmp/worklink-publication-attack
              git init --bare -q /tmp/worklink-hostile.git
            '
            cat > /home/mimir/worklink-opencode/opencode.json <<'JSON'
{"model":"proof/model","provider":{"proof":{"endpoint":"https://proof.invalid","apiKey":"{env:PROOF_TOKEN}"},"unrelated":{"apiKey":"must-not-project"}}}
JSON
            cat > /home/mimir/worklink-opencode/data/opencode/auth.json <<'JSON'
{"proof":{"type":"api","key":"projected-secret"},"unrelated":{"type":"api","key":"must-not-project"}}
JSON
            chown mimir:mimir /home/mimir/worklink-opencode/opencode.json /home/mimir/worklink-opencode/data/opencode/auth.json
            chmod 0600 /home/mimir/worklink-opencode/opencode.json /home/mimir/worklink-opencode/data/opencode/auth.json
            printf '%s' "$WORKLINK_REPO" > /tmp/worklink-proof-repo
            chmod 0444 /tmp/worklink-proof-repo
            install -d -o worklink -g worklink -m 0770 /tmp/worklink-negative-control/a /tmp/worklink-negative-control/b
            cat > /tmp/opencode-proof <<'PY'
#!/opt/mimir-worklink/venv/bin/python
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

if os.geteuid() != 1002:
    raise SystemExit("coding CLI did not run as worklink")
home = Path(os.environ["HOME"])
if home.parent != Path("/var/lib/mimir-worklink/daemon/sessions") or not home.name.endswith('.home'):
    raise SystemExit("daemon payload received an invalid session HOME")
if (home / '.config/opencode/opencode.json').exists() or (home / '.local/share/opencode/auth.json').exists():
    raise SystemExit('daemon session received legacy credential projections')
if "PROOF_TOKEN" in os.environ:
    raise SystemExit("selected provider credential leaked into the worker environment")
checkout = Path.cwd()
checkout_fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
os.chdir(home)
os.fchdir(checkout_fd)
control = Path("/tmp/worklink-negative-control")
os.chdir(control / "a")
(Path("../b") / "cross-write").write_text("detector-live")
if (Path("../b") / "cross-write").read_text() != "detector-live":
    raise SystemExit("sibling-access negative control did not detect a cross-write")
os.fchdir(checkout_fd)
os.close(checkout_fd)
sibling = checkout.parent / "1411-1" / "sibling-canary"
if sibling.read_text() != "sibling-original":
    raise SystemExit("worker could not reach the intentionally shared sibling checkout")
parent = home.parent
repo = Path(Path("/tmp/worklink-proof-repo").read_text())
# Daemon sessions intentionally share the worklink uid and daemon-owned
# session root. Unlike the retired CLI HOME root, that parent is not a
# cross-session security boundary. Controller-owned paths remain denied.
checks = [
    ["ls", "/var/lib/mimir-worklink/homes"],
    ["touch", "/var/lib/mimir-worklink/homes/parent-write"],
    ["cat", "/home/mimir/worklink-opencode/opencode.json"],
    ["cat", "/home/mimir/worklink-opencode/data/opencode/auth.json"],
    ["cat", "/home/mimir/worklink-canary"],
    ["sh", "-c", "printf attacked > /home/mimir/worklink-canary"],
    ["sh", "-c", f"printf attacked > {shlex.quote(str(repo / 'tracked'))}"],
]
for command in checks:
    if subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        raise SystemExit(f"worker operation unexpectedly succeeded: {command}")
Path("tracked").write_text("worker-modified")
Path("created").write_text("worker-created")
Path("deleted").unlink()
hooks = Path("worker-hooks")
hooks.mkdir()
for name in ("pre-commit", "pre-push"):
    hook = hooks / name
    hook.write_text("#!/bin/sh\ntouch /tmp/worklink-publication-attack\nexit 91\n")
    hook.chmod(0o755)
# The executor trusts this one issued checkout for the worker's Git subprocesses.
# Planting and reading back the hostile metadata must work with plain Git.
checkout = str(Path.cwd().resolve())
git = ["git", "-C", checkout]
hostile_helper = "!f() { touch /tmp/worklink-publication-attack; echo username=worker; echo password=hostile; }; f"
subprocess.run([*git, "config", "--local", "core.hooksPath", str(hooks.resolve())], check=True)
subprocess.run([*git, "config", "--local", "credential.helper", hostile_helper], check=True)
subprocess.run([*git, "remote", "set-url", "--push", "origin", "/tmp/worklink-hostile.git"], check=True)

# Read every planted value back. Without this the proof passes vacuously whenever the
# plant silently fails, which is the failure mode it exists to rule out.
def planted(args, expected):
    got = subprocess.run([*git, *args], capture_output=True, text=True, check=True).stdout.strip()
    if expected not in got:
        raise SystemExit(f"hostile setup did not persist: {args} -> {got!r}")

planted(["config", "--local", "--get", "core.hooksPath"], str(hooks.resolve()))
planted(["config", "--local", "--get", "credential.helper"], "worklink-publication-attack")
planted(["remote", "get-url", "--push", "origin"], "/tmp/worklink-hostile.git")
print(f"build-euid={os.geteuid()}")
PY
            chmod 0755 /tmp/opencode-proof
        """
        docker_exec("/bin/sh", "-ceu", setup)
        docker_exec(
            "/bin/sh", "-c", "cat > /tmp/worklink-runtime-proof.py && chmod 0644 /tmp/worklink-runtime-proof.py",
            input_text=CONTROLLER_PROOF,
        )
        print(docker_exec(
            "/bin/sh", "-ceu",
            "PROOF_TOKEN=provider-reference /opt/mimir-worklink/venv/bin/python /tmp/worklink-runtime-proof.py",
            user="mimir",
        ))
        spawn_setup = r"""
            set -eu
            cat > /home/mimir/worklink-opencode/opencode.json <<'JSON'
{"model":"proof/model","provider":{"proof":{"endpoint":"https://proof.invalid"}}}
JSON
            cat > /home/mimir/worklink-opencode/data/opencode/auth.json <<'JSON'
{"proof":{"type":"oauth","access":"oauth-access-proof","refresh":"oauth-refresh-proof","expires":4102444800000}}
JSON
            chown mimir:mimir /home/mimir/worklink-opencode/opencode.json /home/mimir/worklink-opencode/data/opencode/auth.json
            chmod 0600 /home/mimir/worklink-opencode/opencode.json /home/mimir/worklink-opencode/data/opencode/auth.json
            cat > /tmp/opencode-spawn-proof <<'PY'
#!/opt/mimir-worklink/venv/bin/python
import json
import os
from pathlib import Path
import sys
import uuid

if os.geteuid() != 1002:
    raise SystemExit("spawn fake did not run as worklink")
home = Path(os.environ["HOME"])
if home.parent != Path('/var/lib/mimir-worklink/daemon/sessions') or not home.name.endswith('.home'):
    raise SystemExit('spawn payload did not receive a daemon session HOME')
if (home / '.config/opencode/opencode.json').exists() or (home / '.local/share/opencode/auth.json').exists():
    raise SystemExit('spawn session received legacy credential projections')
payload = Path("generated-containment-proof.py")
payload.write_text(
    "import os\n"
    "import pwd\n"
    "from pathlib import Path\n"
    "controller_home = Path(pwd.getpwnam('mimir').pw_dir)\n"
    "try:\n"
    "    (controller_home / 'worklink-canary').write_text('attacked')\n"
    "except OSError:\n"
    "    pass\n"
    "Path('spawn-payload-executed').write_text('executed')\n"
    "Path('spawn-worker-uid').write_text(str(os.geteuid()))\n"
)
exec(compile(payload.read_bytes(), str(payload), "exec"), {"__name__": "__main__"})
print("spawn-euid=1002")
PY
            chmod 0755 /tmp/opencode-spawn-proof
        """
        docker_exec("/bin/sh", "-ceu", spawn_setup)
        docker_exec(
            "/bin/sh", "-c", "cat > /tmp/worklink-spawn-proof.py && chmod 0644 /tmp/worklink-spawn-proof.py",
            input_text=SPAWN_PROOF,
        )
        print(docker_exec(
            # Readiness/wire outcomes are synthetic; executor and UID boundaries
            # remain shipped code. This does not exercise a provider or login.
            "/opt/mimir-worklink/venv/bin/python", "/tmp/worklink-spawn-proof.py",
            user="mimir",
        ))
        docker_exec("/bin/sh", "-ceu", """
            test "$(cat /home/mimir/worklink-canary)" = controller-reset
            test -z "$(find /var/lib/mimir-worklink/homes -mindepth 1 -maxdepth 1 -print -quit)"
            test -z "$(find /var/lib/mimir-worklink/checkouts -mindepth 2 -maxdepth 2 -print -quit)"
            test -z "$(find /var/lib/mimir-worklink/opencode-checkouts -mindepth 2 -maxdepth 2 -print -quit)"
        """)
        print("worklink shipped-image identity proof passed")
    finally:
        subprocess.run(["docker", "rm", "--force", CONTAINER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "image", "rm", "--force", IMAGE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> None:
    """Retain the source-mode proof entry point for existing callers."""
    prove_source()


def cli() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("source", "pypi", "offline"), required=True)
    parser.add_argument("--agent-wheel", type=Path)
    parser.add_argument("--cultivate-wheel", type=Path)
    args = parser.parse_args()
    if args.mode == "offline":
        import asyncio
        if args.agent_wheel or args.cultivate_wheel:
            parser.error("offline mode does not accept wheels")
        with tempfile.TemporaryDirectory(prefix="worklink-session-proof-") as directory:
            os.environ.update(HOME=directory, MIMIR_HOME=directory, MIMIR_MODEL_SPEC="codex-plus:fixture-model")
            result = asyncio.run(prove_offline(Path(directory)))
            print(json.dumps(result, sort_keys=True))
    elif args.mode == "source":
        if args.agent_wheel or args.cultivate_wheel:
            parser.error("source mode does not accept wheels")
        prove_source()
    else:
        if not args.agent_wheel or not args.cultivate_wheel:
            parser.error("pypi mode requires both wheels")
        prove_pypi(args.agent_wheel, args.cultivate_wheel)


if __name__ == "__main__":
    cli()
