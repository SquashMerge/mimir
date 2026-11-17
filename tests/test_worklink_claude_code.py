from __future__ import annotations

import asyncio
from dataclasses import fields, replace
import json
import os
from pathlib import Path
import stat
import signal
import sys
from types import SimpleNamespace
import uuid

import pytest

pytest.importorskip("mimir_cultivate")

from mimir_cultivate import contained_execution
from mimir_cultivate.worklink import compute, worker_client, worker_exec
from mimir_cultivate.worklink.backends import base
from mimir_cultivate.worklink.backends.claude_code import ClaudeCodeBackend
from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig
from mimir_cultivate.worklink.execution_policy import CLAUDE_LEAF, ExecutionPolicy, home_projection_documents

FIXTURES = Path(__file__).parent / "fixtures/worklink/claude_code_2_1_326"


@pytest.fixture
def order(tmp_path):
    return base.WorkOrder(2126, tmp_path, "build the leaf", "keep boundaries", 30,
                          {"CLAUDE_CODE_OAUTH_TOKEN": "unit-" + "subscription-material",
                           "MIMIR_MODEL_SPEC": "claude-code:opus"}, tmp_path / "transcripts")


def spec_for(order, backend=None):
    return (backend or ClaudeCodeBackend()).work_spec(order, attempt=1, repo_url="repo", base_ref="main",
                                                      branch="issue/2126-a1", test_command="uv run pytest -q")


def test_shipping_leaf_spec_and_duplicate_contracts(order):
    backend = BackendRegistry().get("claude_code")
    spec = spec_for(order, backend)
    assert spec.execution == backend.capabilities().execution == CLAUDE_LEAF
    assert backend.capabilities().quota_pool == "claude_code"
    assert backend.capabilities().persistent_sessions is False
    assert spec.session_request is None
    assert spec.local_argv[0] == "/opt/mimir-claude/bin/claude"
    assert spec.backend_config["model"] == "opus"
    assert spec.backend_config["pass_env"] == ("CLAUDE_CODE_OAUTH_TOKEN",)
    assert set(spec.env) == {"CLAUDE_CODE_OAUTH_TOKEN"}
    assert order.env["CLAUDE_CODE_OAUTH_TOKEN"] not in repr(spec)
    assert "keep boundaries" in spec.local_argv[2]
    assert [(f.name, f.type) for f in fields(base.WorkSpec)] == [(f.name, f.type) for f in fields(compute.WorkSpec)]
    assert ExecutionPolicy().home_projection_profile == "none"


def test_registry_model_configuration_and_retired_settings(order):
    registry = BackendRegistry(WorklinkConfig(backend_settings={"claude_code": {"model": "sonnet"}}))
    assert spec_for(order, registry.get("claude_code")).backend_config["model"] == "sonnet"
    for settings in ({"bin": "claude"}, {"args": ["--bare"]}, {"model": 1}, {"bash_allowlist": ["*"]}):
        with pytest.raises(ValueError):
            BackendRegistry(WorklinkConfig(backend_settings={"claude_code": settings}))


@pytest.mark.asyncio
async def test_claude_containment_does_not_depend_on_compute_name(order, monkeypatch):
    spec = spec_for(order, ClaudeCodeBackend(name="renamed-leaf"))
    launches = []

    class Process:
        pid = os.getpid()
        timed_out = output_overflow = False

        async def wait(self):
            return 0

    class Client:
        async def launch(self, **kwargs):
            launches.append(kwargs)
            kwargs["stdout_sink"].file.write((FIXTURES / "success.jsonl").read_bytes())
            return Process()

        async def cancel(self, identifier):
            pass

    monkeypatch.setattr(compute, "_local_child_env", lambda: pytest.fail("controller fallback"))
    backend = compute.LocalSubprocessComputeBackend(name="renamed-substrate", _worker_uid=os.getuid(), _worker_client=Client())
    handle = await backend.launch(spec)
    result = await backend.wait(handle, 30)
    await backend.cleanup(handle)
    assert result.children_reaped
    assert handle.substrate == "renamed-substrate"
    assert len(launches) == 1
    launch = launches[0]
    assert launch["home_projection_profile"] == "claude_leaf"
    assert launch["identifier"] == spec.backend_config["worker_identifier"]
    assert "HOME" not in launch["env"] and "CLAUDE_CONFIG_DIR" not in launch["env"]
    assert "OPENCODE_CONFIG" not in launch["env"]
    assert not {"GH_TOKEN", "GITHUB_TOKEN", "ANTHROPIC_API_KEY"}.intersection(launch["env"])
    assert result.stdout_path is result.stderr_path is None
    assert not list(order.transcript_root.glob("*.log"))


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"timed_out": True}, {"output_truncated": True},
                                    {"output_overflow": True}, {"launch_error": "launch failed"}])
async def test_incomplete_launch_precedes_auth_and_redacts_artifacts(order, changes):
    token = order.env["CLAUDE_CODE_OAUTH_TOKEN"]
    stdout = '{"type":"assistant","error":"authentication_failed","message":{"content":[]}}\n' + token
    result = compute.ComputeResult(1, stdout, token, command=("claude", "-p", token), **changes)
    raw = await ClaudeCodeBackend().interpret(order, result)
    assert raw.backend_status == "failed"
    assert raw.opencode_session_id is None
    assert token not in raw.transcript_path.read_text()
    assert stat.S_IMODE(raw.transcript_path.stat().st_mode) == 0o600


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture,status", [("success.jsonl", "success"), ("quota_exhausted.jsonl", "quota_exhausted")])
async def test_real_backend_interpret_uses_stream_fixtures(order, fixture, status):
    raw = await ClaudeCodeBackend().interpret(order, compute.ComputeResult(0, (FIXTURES / fixture).read_text(), ""))
    assert raw.backend_status == status
    assert raw.opencode_session_id is None
    assert json.loads(raw.transcript_path.read_text())["backend"] == "claude_code"


@pytest.mark.asyncio
async def test_deterministic_leaf_gates_do_not_inherit_model_credentials(order):
    from mimir_cultivate.worklink.evidence import _run_compute_gate

    spec = spec_for(order)
    observed = []

    class Compute:
        async def _launch_contained(self, gate, command):
            observed.append(gate)
            assert command == ("/bin/sh", "-c", "uv run pytest -q")
            return base.LaunchHandle("contained", "gate")

        async def wait(self, handle, timeout):
            return compute.ComputeResult(0, "ok", "", handle=handle)

        async def cleanup(self, handle):
            pass

    result = await _run_compute_gate("uv run pytest -q", checkout=order.checkout, work_spec=spec, compute=Compute())
    assert result.exit_code == 0
    gate = observed[0]
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in gate.env
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in gate.backend_config["pass_env"]
    assert "worker_identifier" not in gate.backend_config
    assert gate.execution.substrate == "contained_subprocess"
    assert "CLAUDE_CODE_OAUTH_TOKEN" in spec.env


@pytest.mark.parametrize("profile", ["claude_leaf", "claude_factory"])
@pytest.mark.parametrize("boundary", ["client", "runner", "executor"])
@pytest.mark.asyncio
async def test_projection_profiles_are_independently_checked(tmp_path, monkeypatch, profile, boundary):
    documents = home_projection_documents(profile)
    projections = [SimpleNamespace(path=p, document=d) for p, d in documents]
    expected = {p: json.loads(d) for p, d in documents}
    called = []

    class Client:
        async def launch(self, **kwargs):
            called.append(kwargs)
            raise RuntimeError("accepted profile reached owned runner")

    monkeypatch.setattr(contained_execution, "_worker_classes", lambda: (lambda _: Client(), SimpleNamespace))
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(worklink_uid=os.getuid(), worklink_gid=os.getgid()))

    async def validate(selected, items):
        if boundary == "client":
            worker_client._validate_projection_profile(selected, items)
        elif boundary == "executor":
            home = tmp_path / str(len(list(tmp_path.iterdir())))
            home.mkdir(mode=0o700)
            worker_exec._project_home(home, [{"path": p.path, "document": p.document.decode()} for p in items], selected)
            return home
        else:
            with pytest.raises(RuntimeError, match="accepted profile"):
                await contained_execution.execute_contained(("/bin/true",), SimpleNamespace(path=tmp_path), {}, items,
                    identifier="owned", home_projection_profile=selected, timeout_s=1, stdout_limit=10, stderr_limit=10)

    home = await validate(profile, projections)
    if home is not None:
        assert not (home / ".claude.json").exists()
        assert stat.S_IMODE((home / ".claude").stat().st_mode) == 0o700
        for path, doc in expected.items():
            target = home / path
            assert json.loads(target.read_bytes()) == doc
            assert stat.S_IMODE(target.stat().st_mode) == 0o600
            assert target.stat().st_uid == os.getuid()
    for selected, items in [
        ("unknown", projections), ("none", projections), (profile, []),
        (profile, [*projections, projections[0]]),
        (profile, [SimpleNamespace(path=".claude.json", document=documents[0][1])]),
        (profile, [SimpleNamespace(path="../.claude/.claude.json", document=documents[0][1])]),
        (profile, [SimpleNamespace(path="/.claude/.claude.json", document=documents[0][1])]),
        (profile, [SimpleNamespace(path=p, document=b" " * (1024 * 1024) + d) for p, d in documents]),
        (profile, [SimpleNamespace(path=p, document=b'{"hasCompletedOnboarding":1}') for p, d in documents]),
        ("claude_factory" if profile == "claude_leaf" else "claude_leaf", projections),
    ]:
        before = len(called)
        with pytest.raises((ValueError, RuntimeError), match="projection|profile|configuration"):
            await validate(selected, items)
        assert len(called) == before


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory"])
def test_executor_projection_refuses_existing_special_paths(tmp_path, monkeypatch, kind):
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(worklink_uid=os.getuid(), worklink_gid=os.getgid()))
    target = tmp_path / ".claude"
    if kind == "symlink":
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        target.symlink_to(elsewhere)
    elif kind == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    with pytest.raises(OSError):
        worker_exec._project_home(tmp_path, [{"path": p, "document": d.decode()} for p, d in home_projection_documents("claude_leaf")], "claude_leaf")


def test_claude_worker_environment_rechecks_credential_and_config_admission(order):
    spec = spec_for(order)
    env = compute._enabled_child_env(spec, spec.backend_config["worker_identifier"])
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == order.env["CLAUDE_CODE_OAUTH_TOKEN"]
    for name in ("GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY", "HOME", "CLAUDE_CONFIG_DIR",
                 "GIT_CONFIG_COUNT", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "OPENCODE_CONFIG"):
        poisoned = replace(spec, env={**spec.env, name: "test"}, backend_config={**spec.backend_config,
                           "pass_env": (*spec.backend_config["pass_env"], name)})
        with pytest.raises(compute.ComputeLaunchError):
            compute._enabled_child_env(poisoned, spec.backend_config["worker_identifier"])


@pytest.mark.parametrize("profile", ["claude_leaf", "claude_factory"])
def test_executor_fake_cli_consumes_private_config_and_retires_descendants(tmp_path, monkeypatch, profile):
    from mimir_cultivate.claude_code import leaf_argv
    from mimir.output_capture import open_output_pair

    homes = tmp_path / "homes"
    homes.mkdir()
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    identifier = str(uuid.uuid4())
    home = homes / identifier
    fake = checkout / "fake_claude.py"
    fake.write_text(
        "import json, os, pathlib, stat, subprocess, sys\n"
        "home = pathlib.Path(os.environ['HOME'])\n"
        "config = pathlib.Path(os.environ['CLAUDE_CONFIG_DIR'])\n"
        "assert config == home / '.claude'\n"
        "assert json.loads((config / '.claude.json').read_text()) == {'hasCompletedOnboarding': True}\n"
        "assert not (home / '.claude.json').exists()\n"
        "assert stat.S_IMODE(config.stat().st_mode) == 0o700\n"
        "for path in config.iterdir():\n"
        " assert stat.S_IMODE(path.stat().st_mode) == 0o600\n"
        " assert path.stat().st_uid == os.getuid()\n"
        "assert not {'GH_TOKEN', 'GITHUB_TOKEN', 'ANTHROPIC_API_KEY'} & os.environ.keys()\n"
        "if '--settings' in sys.argv:\n"
        " settings = pathlib.Path(sys.argv[sys.argv.index('--settings') + 1])\n"
        " assert settings == config / 'settings.json'\n"
        " assert json.loads(settings.read_text()) == {'disableAllHooks': True}\n"
        "else:\n"
        " assert not (config / 'settings.json').exists()\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'], start_new_session=True)\n"
        "print(json.dumps({'type':'fixture_metadata', 'cwd':os.getcwd(), 'uid':os.getuid(), 'child':child.pid}), flush=True)\n"
        "print(json.dumps({'type':'result','subtype':'success','is_error':False,'result':'ok'}), flush=True)\n"
    )
    monkeypatch.setattr(worker_exec, "HOME_ROOT", homes)
    monkeypatch.setattr(worker_exec, "_validate_checkout", lambda *_: None)
    monkeypatch.setattr(worker_exec, "get_identities", lambda: SimpleNamespace(worklink_uid=os.getuid(), worklink_gid=os.getgid()))
    # This harness already runs as the configured worker uid. Privileged uid-drop
    # ordering has separate executor tests; no root capability is available here.
    monkeypatch.setattr(worker_exec, "_drop_worker_privileges", lambda: None)
    args = leaf_argv("build", "sonnet", home, ("Bash(git:*)", "Bash(uv:*)"))[1:] if profile == "claude_leaf" else ("-p", "factory")
    request = {
        "version": 1, "op": "launch", "id": identifier, "issue": 2126, "attempt": 1,
        "device": 0, "inode": 0, "argv": [sys.executable, str(fake), *args],
        "env": {"PATH": "/usr/bin:/bin", "CLAUDE_CODE_OAUTH_TOKEN": "fixture-" + "oauth"},
        "projections": [{"path": p, "document": d.decode()} for p, d in home_projection_documents(profile)],
        "home_projection_profile": profile, "executor_identity": worker_exec.EXECUTOR_PROTOCOL_IDENTITY,
        "timeout_s": 10, "stdout_limit": 65536, "stderr_limit": 65536,
    }
    messages = []
    connection = SimpleNamespace(send=lambda data, *args: messages.append(json.loads(data)))
    out, err = open_output_pair(None, 65536, None, 65536)
    fds = [os.open(checkout, os.O_RDONLY | os.O_DIRECTORY), out.fd, err.fd]
    observed = None
    try:
        worker_exec._handle_launch(connection, request, fds)
        output = out.read_bounded()[0].decode()
        errors = err.read_bounded()[0].decode()
        assert messages[-1]["exit_code"] == 0, errors
        observed = json.loads(output.splitlines()[0])
        assert observed["cwd"] == str(checkout)
        assert observed["uid"] == os.getuid()
        assert not worker_exec._process_group_has_live_members(messages[0]["pid"])
        with pytest.raises(ProcessLookupError):
            os.kill(observed["child"], 0)
        assert not home.exists()
    finally:
        if observed is not None:
            try:
                os.kill(observed["child"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        for fd in fds[:1]:
            try:
                os.close(fd)
            except OSError:
                pass
        out.close()
        err.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["client", "runner", "executor"])
@pytest.mark.parametrize("name", ["GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY"])
async def test_leaf_publishing_credentials_are_refused_at_each_boundary(tmp_path, monkeypatch, boundary, name):
    env = {name: "fixture-" + "publishing"}
    projections = [worker_client.WorkerProjection(p, d) for p, d in home_projection_documents("claude_leaf")]
    identifier = str(uuid.uuid4())
    if boundary == "client":
        client = worker_client.WorkerClient(SimpleNamespace(verify=lambda _: None))
        with pytest.raises(ValueError, match="epic credentials"):
            await client.launch(local_checkout=tmp_path, argv=("/bin/true",), env=env, projections=projections,
                                home_projection_profile="claude_leaf", identifier=identifier, timeout_s=1)
    elif boundary == "runner":
        with pytest.raises(ValueError, match="epic credentials"):
            await contained_execution.execute_contained(("/bin/true",), SimpleNamespace(path=tmp_path), env, projections,
                home_projection_profile="claude_leaf", identifier=identifier, timeout_s=1, stdout_limit=10, stderr_limit=10)
    else:
        from mimir.output_capture import open_output_pair
        monkeypatch.setattr(worker_exec, "_validate_checkout", lambda *_: None)
        request = {
            "version": 1, "op": "launch", "id": identifier, "issue": 2126, "attempt": 1,
            "device": 0, "inode": 0, "argv": ["/bin/true"], "env": env,
            "projections": [{"path": p.path, "document": p.document.decode()} for p in projections],
            "home_projection_profile": "claude_leaf", "executor_identity": worker_exec.EXECUTOR_PROTOCOL_IDENTITY,
            "timeout_s": 1, "stdout_limit": 10, "stderr_limit": 10,
        }
        out, err = open_output_pair(None, 10, None, 10)
        checkout_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with pytest.raises(RuntimeError, match="epic credentials"):
                worker_exec._handle_launch(object(), request, [checkout_fd, out.fd, err.fd])
        finally:
            os.close(checkout_fd)
            out.close()
            err.close()
