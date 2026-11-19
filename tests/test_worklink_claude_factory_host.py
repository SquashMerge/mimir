from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import uuid

import pytest

pytest.importorskip("mimir_cultivate")

from mimir_cultivate.worklink import claude_factory_host as host, compute, worker_exec
from mimir_cultivate.worklink.backends.base import WorkOrder
from mimir_cultivate.worklink.backends.feature_factory import (
    FACTORY_VERSION, FactoryContractError, FeatureFactoryBackend, resolve_factory_entrypoint, parse_factory_status,
)
from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig
from mimir_cultivate.worklink.execution_policy import CLAUDE_FACTORY, home_projection_documents
from mimir_cultivate.worklink.factory_state import FactoryRunRecord, FactoryRecordError, factory_process_is_verified_dead

FIXTURES = Path(__file__).parent / "fixtures/worklink"


@pytest.fixture(scope="module")
def real_package():
    source = os.environ.get("MIMIR_TEST_CLAUDE_FACTORY_ROOT")
    if not source:
        pytest.skip("set MIMIR_TEST_CLAUDE_FACTORY_ROOT for offline installed-package tests")
    assert Path(source).is_absolute() and (Path(source) / "package.json").is_file()
    with tempfile.TemporaryDirectory(prefix="mimir-claude-factory-", dir="/tmp") as temporary:
        root = Path(temporary)
        root.chmod(0o755)
        package = root / "claude-code-feature-factory"
        shutil.copytree(source, package)
        yield package


def entrypoint(package):
    return package / "vendor/feature-factory/bin/factory.js"


def order_for(checkout):
    return WorkOrder(2126, checkout, "build", None, 30, {
        "MIMIR_MODEL_SPEC": "claude-code:opus",
        "CLAUDE_CODE_OAUTH_TOKEN": "unit-" + "oauth-material",
        "GITHUB_TOKEN": "unit-" + "publishing-material",
        "GH_TOKEN": "unit-" + "publishing-material",
        "FACTORY_PUBLISHING_IDENTITY": "fixture-bot",
        "MIMIR_WORK_ITEM_JSON": json.dumps({"run_id": "chainlink-2126"}),
    }, checkout / "transcripts")


def spec_for(package, order):
    return FeatureFactoryBackend(entrypoint=str(entrypoint(package)), host="claude_code").work_spec(
        order, attempt=1, repo_url="repo", base_ref="main", branch="feature/chainlink-2126",
        test_command="uv run pytest -q", max_retries=7)


def test_claude_factory_spec_and_lockstep(real_package, tmp_path):
    order = order_for(tmp_path)
    spec = spec_for(real_package, order)
    assert spec.execution == CLAUDE_FACTORY
    assert spec.session_request is None
    assert spec.backend_config["lock_session"] == "claude-code:" + spec.backend_config["worker_identifier"]
    assert spec.backend_config["launch_generation"] == spec.backend_config["worker_identifier"]
    payload = json.loads(spec.local_argv[-1])
    assert payload["request"] == "--autonomous --max-retries 7 chainlink-2126"
    assert payload["session"] == spec.backend_config["worker_identifier"]
    assert payload["model"] == "opus"
    assert spec.env["GH_TOKEN"] == spec.env["GITHUB_TOKEN"] == order.env["GITHUB_TOKEN"]
    for name in ("CLAUDE_CODE_OAUTH_TOKEN", "GITHUB_TOKEN"):
        assert order.env[name] not in repr(spec)
        assert order.env[name] not in repr(spec.local_argv)
    registry = BackendRegistry(WorklinkConfig(backend_settings={
        "feature_factory": {"host": "claude_code", "model": "sonnet", "entrypoint": str(entrypoint(real_package))},
    }))
    backend = registry.get("feature_factory")
    assert backend.host == "claude_code"
    assert backend.capabilities().quota_pool == "claude_code"
    assert BackendRegistry().get("feature_factory").host == "opencode"
    assert FACTORY_VERSION == host.FACTORY_VERSION == "0.14.1"
    assert resolve_factory_entrypoint(entrypoint(real_package), host="claude_code") == entrypoint(real_package)
    for settings in ({"host": "unknown"}, {"host": "claude_code", "model": 1}, {"host": "opencode", "model": "sonnet"}):
        with pytest.raises(ValueError):
            BackendRegistry(WorklinkConfig(backend_settings={"feature_factory": settings}))


@pytest.mark.parametrize("manifest", ["package.json", "vendor/feature-factory/package.json"])
def test_claude_factory_rejects_version_drift(real_package, tmp_path, manifest):
    package = tmp_path / "plugin"
    shutil.copytree(real_package, package)
    path = package / manifest
    data = json.loads(path.read_text())
    data["version"] = "0.14.0"
    path.write_text(json.dumps(data))
    with pytest.raises(FactoryContractError, match="0.14.1"):
        resolve_factory_entrypoint(entrypoint(package), host="claude_code")


@pytest.mark.parametrize("selection", ["empty", "matching", "warning"])
@pytest.mark.parametrize("model", ["sonnet", "opus"])
def test_real_exports_are_authoritative(real_package, tmp_path, selection, model):
    if selection != "empty":
        (tmp_path / ".factory.json").write_text(json.dumps({"mcp_servers": ["fixture-*", "missing"]}))
        if selection == "matching":
            (tmp_path / ".mcp.json").write_text(json.dumps({"mcpServers": {
                "fixture-one": {"command": "fixture", "args": ["--offline"]},
                "excluded": {"command": "not-selected"},
            }}))
    session = str(uuid.uuid4())
    request = "--autonomous --max-retries 7 chainlink-2126"
    actual, warnings = host.exported_arguments(entrypoint(real_package), cwd=tmp_path,
        request=request, model=model, session=session)
    independent = r"""
import { pathToFileURL } from "node:url";
const input = JSON.parse(process.env.FIXTURE_INPUT);
const installed = await import(pathToFileURL(input.root + "/launcher/launch.js"));
const projection = installed.projectMcpServers(input.cwd);
process.stdout.write(JSON.stringify({
  args: installed.claudeArgs({request: input.request, model: input.model, mcpServers: projection.servers},
                            input.session, input.root),
  warnings: projection.warnings
}));
"""
    expected = subprocess.run(["node", "--input-type=module", "-e", independent], check=True,
        env={**os.environ, "FIXTURE_INPUT": json.dumps({"root": str(real_package), "cwd": str(tmp_path),
            "session": session, "request": request, "model": model})},
        capture_output=True, timeout=30)
    assert json.loads(expected.stdout) == {"args": actual, "warnings": warnings}
    assert actual[actual.index("--session-id") + 1] == session
    assert actual[actual.index("--model") + 1] == model
    assert actual[actual.index("--permission-mode") + 1] == "acceptEdits"
    assert "Bash" in actual[actual.index("--allowedTools") + 1].split(",")
    assert bool(warnings) == (selection != "empty")


def local_factory_capture(package, root):
    repo, home = root / "repo", root / "home"
    repo.mkdir()
    home.mkdir()
    environment = {"HOME": str(home), "PATH": os.environ["PATH"], "LANG": "C.UTF-8",
                   "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                   "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                   "GIT_COMMITTER_NAME": "Fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid"}

    def git(*args):
        return subprocess.run(["git", "-C", str(repo), *args], env=environment, check=True,
                              capture_output=True, timeout=30)

    git("init", "-b", "main")
    (repo / ".gitignore").write_text(".factory/\n.factory-sandboxes/\n")
    (repo / ".factory.json").write_text(json.dumps({"resolve": "true", "verify": "true", "mcp_servers": ["fixture-mcp"]}))
    git("add", ".")
    git("commit", "-m", "Seed offline factory fixture")
    cli = ["node", str(entrypoint(package))]
    run_id = "chainlink-2126"

    def invoke(*args):
        return subprocess.run([*cli, *args], env=environment, cwd=repo,
                              capture_output=True, timeout=30)

    initialized = invoke("init", run_id, "--repo", str(repo), "--pr-base", "main",
                         "--mode", "autonomous", "--max-retries", "7", "--publishing-identity", "fixture-bot", "--json")
    assert initialized.returncode == 0, initialized.stderr.decode()
    sandbox = repo / ".factory-sandboxes" / run_id
    owner = "claude-code:11111111-1111-4111-8111-111111111111"
    claimed = invoke("lock", run_id, "claim", "--repo", str(sandbox), "--session", owner)
    assert claimed.returncode == 0, claimed.stderr.decode()
    status = invoke("status", run_id, "--repo", str(sandbox), "--json")
    refused = invoke("lock", run_id, "claim", "--repo", str(sandbox),
                     "--session", "claude-code:22222222-2222-4222-8222-222222222222")
    assert status.returncode == 0
    assert refused.returncode != 0
    assert owner in refused.stderr.decode()
    return status.stdout, refused.stderr


def test_real_local_factory_status_and_competing_lock(real_package, tmp_path):
    raw, refusal = local_factory_capture(real_package, tmp_path)
    status = parse_factory_status(raw)
    assert status.valid and status.status == "running"
    assert status.lock_session == "claude-code:11111111-1111-4111-8111-111111111111"
    assert status.mcp_servers == ("fixture-mcp",)
    assert "is held by session" in refusal.decode()
    committed = parse_factory_status((FIXTURES / "feature_factory_0_14_1/status.json").read_bytes())
    assert committed.status == status.status and committed.lock_session == status.lock_session


@pytest.mark.parametrize("location", ["result", "assistant", "tool_result"])
def test_captured_stream_lock_refusal_is_recognized(location):
    from mimir_cultivate.worklink.orchestrator import _factory_lock_refusal
    text = (FIXTURES / "feature_factory_0_14_1/lock-refusal.stderr.txt").read_text()
    if location == "result":
        stream = (FIXTURES / "claude_code_2_1_326/lock_refused.jsonl").read_text()
    elif location == "assistant":
        stream = json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}})
    else:
        stream = json.dumps({"type": "user", "message": {"content": [{"type": "tool_result",
            "tool_use_id": "fixture", "content": [{"type": "text", "text": text}]}]}})
    assert _factory_lock_refusal(compute.ComputeResult(0, stream, ""))
    assert not _factory_lock_refusal(compute.ComputeResult(0, json.dumps({"type": "unknown", "result": text}), ""))


def test_claude_factory_record_requires_exact_host_binding(tmp_path):
    identifier = str(uuid.uuid4())
    record = FactoryRunRecord(run_id="chainlink-2126", issue_id=2126, attempt=1,
        repository="owner/repo", base_ref="main", branch="feature/chainlink-2126",
        launcher=host.DEFAULT_ENTRYPOINT, sandbox=str(tmp_path / ".factory-sandboxes/chainlink-2126"),
        session="claude-code:" + identifier, host="claude_code", lock_session="claude-code:" + identifier,
        launch_generation=identifier, handle=compute.LaunchHandle("local_subprocess", identifier, 42, 999999999),
        status=None, observed_at=None, controller_phase="running")
    assert FactoryRunRecord.from_json(record.to_json()) == record
    assert not factory_process_is_verified_dead(record)
    assert factory_process_is_verified_dead(replace(record, children_reaped=True))
    for changes in ({"opencode_session_id": "ses_wrong"}, {"host": "invalid"},
                    {"session": "claude-code:" + str(uuid.uuid4())}, {"launch_generation": str(uuid.uuid4())}):
        with pytest.raises(FactoryRecordError):
            replace(record, **changes)
    legacy = record.to_json()
    legacy.update(session="ses_old", opencode_session_id="ses_old",
                  handle=None)
    for field in ("host", "lock_session", "launch_generation"):
        del legacy[field]
    assert FactoryRunRecord.from_json(legacy).host == "opencode"
    assert FactoryRunRecord.from_json(legacy).lock_session == "ses_old"


@pytest.mark.real_worklink_identities
def test_real_exports_and_foreground_wrapper_through_worker_executor(real_package, tmp_path, monkeypatch):
    from mimir.output_capture import open_output_pair
    from mimir_cultivate.worklink.identities import get_identities, assert_worklink_identity
    from mimir_cultivate.worklink.factory_state import save_factory_record, load_factory_record

    identities = get_identities()
    assert_worklink_identity(identities)
    checkout, homes = tmp_path / "operator", tmp_path / "homes"
    checkout.mkdir()
    homes.mkdir()
    fake = tmp_path / "fake-claude"
    fake.write_text(f"""#!{sys.executable}
import json, os, pathlib, stat, subprocess, sys
home = pathlib.Path(os.environ["HOME"])
config = pathlib.Path(os.environ["CLAUDE_CONFIG_DIR"])
assert os.getuid() == {identities.worklink_uid}
assert os.getcwd() == os.environ["PWD"] == {str(checkout)!r}
assert config == home / ".claude"
assert json.loads((config / ".claude.json").read_text()) == {{"hasCompletedOnboarding": True}}
assert stat.S_IMODE(config.stat().st_mode) == 0o700
assert stat.S_IMODE((config / ".claude.json").stat().st_mode) == 0o600
assert (config / ".claude.json").stat().st_uid == os.getuid()
assert not (home / ".claude.json").exists() and not (config / "settings.json").exists()
assert os.environ["GH_TOKEN"] == os.environ["GITHUB_TOKEN"]
assert os.environ["CLAUDE_CODE_OAUTH_TOKEN"] and os.environ["FACTORY_PUBLISHING_IDENTITY"]
assert "ANTHROPIC_API_KEY" not in os.environ and "OPENCODE_CONFIG" not in os.environ
assert os.environ["GIT_CONFIG_KEY_0"] == "credential.https://github.com.helper"
assert os.environ["GIT_CONFIG_VALUE_0"] == "!fixture-helper"
assert os.environ["PATH"] == {os.environ["PATH"]!r}
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], start_new_session=True)
print(json.dumps({{"type": "fixture", "args": sys.argv[1:], "cwd": os.getcwd(), "uid": os.getuid(), "child": child.pid}}), flush=True)
print(json.dumps({{"type": "result", "subtype": "success", "is_error": False, "result": "ok"}}), flush=True)
""")
    fake.chmod(0o755)
    order = order_for(checkout)
    (checkout / ".factory.json").write_text(json.dumps({"mcp_servers": [order.env["CLAUDE_CODE_OAUTH_TOKEN"]]}))
    spec = spec_for(real_package, order)
    identifier = spec.backend_config["worker_identifier"]
    payload = spec.local_argv[-1]
    # Substitute only the managed executable for this test. The installed
    # exports, production wrapper main, executor and supervisor all execute.
    wrapper = (
        "import importlib.util,sys; "
        "s=importlib.util.spec_from_file_location('host',sys.argv[1]); "
        "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
        "m.CLAUDE_EXECUTABLE=sys.argv[2]; sys.argv=[sys.argv[1],sys.argv[3]]; m.main()"
    )
    argv = [sys.executable, "-I", "-c", wrapper, str(Path(host.__file__).resolve()), str(fake), payload]
    monkeypatch.setattr(worker_exec, "HOME_ROOT", homes)
    monkeypatch.setattr(worker_exec, "_open_factory_checkout",
                        lambda request, **kwargs: os.open(request["path"], os.O_RDONLY | os.O_DIRECTORY))

    def worker(fd, home):
        # The test process is already the provisioned worker. Root-only identity
        # transition/FD authorization have separate executor boundary tests.
        assert_worklink_identity(identities)
        os.setsid()
        os.fchdir(fd)

    monkeypatch.setattr(worker_exec, "_drop_factory", worker)
    request = {
        "version": 1, "op": "launch_factory", "id": identifier, "issue": 2126, "attempt": 1,
        "path": str(checkout), "run_uid": identities.worklink_uid, "argv": argv,
        "env": {**spec.env, "PATH": os.environ["PATH"], "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "credential.https://github.com.helper", "GIT_CONFIG_VALUE_0": "!fixture-helper"},
        "projections": [{"path": p, "document": d.decode()} for p, d in home_projection_documents("claude_factory")],
        "home_projection_profile": "claude_factory", "executor_identity": worker_exec.EXECUTOR_PROTOCOL_IDENTITY,
        "timeout_s": 30, "stdout_limit": 65536, "stderr_limit": 65536,
    }
    messages, admissions = [], []

    def send(raw, *args):
        packet = json.loads(raw)
        messages.append(packet)
        if packet.get("status") == "started":
            handle = compute.LaunchHandle("local_subprocess", identifier, packet["process_start_ticks"], packet["pid"])
            record = FactoryRunRecord(run_id="chainlink-2126", issue_id=2126, attempt=1,
                repository="owner/repo", base_ref="main", branch="feature/chainlink-2126",
                launcher=str(entrypoint(real_package)), sandbox=str(checkout / ".factory-sandboxes/chainlink-2126"),
                session=spec.backend_config["lock_session"], host="claude_code",
                lock_session=spec.backend_config["lock_session"], launch_generation=identifier,
                handle=handle, status=None, observed_at=None, controller_phase="running")
            save_factory_record(tmp_path, record)
            assert load_factory_record(tmp_path, record.run_id) == record
            admission = {"version": 1, "op": "admit_factory_handle",
                         "executor_identity": worker_exec.EXECUTOR_PROTOCOL_IDENTITY,
                         "handle": {"substrate": handle.substrate, "identifier": identifier,
                                    "process_start_ticks": handle.process_start_ticks, "shim_pid": handle.shim_pid}}
            response = SimpleNamespace(send=lambda data: admissions.append(json.loads(data)))
            with pytest.raises(RuntimeError, match="identity no longer matches"):
                worker_exec._handle_cancel_factory_handle(response,
                    {**admission, "handle": {**admission["handle"], "process_start_ticks": handle.process_start_ticks + 1}}, [])
            assert admissions == []
            worker_exec._handle_cancel_factory_handle(response, admission, [])
            with pytest.raises(RuntimeError, match="no longer pending"):
                worker_exec._handle_cancel_factory_handle(response, admission, [])

    out, err = open_output_pair(None, 65536, None, 65536)
    fds = [out.fd, err.fd]
    child = None
    try:
        worker_exec._handle_launch(SimpleNamespace(send=send), request, fds)
        output, errors = out.read_bounded()[0].decode(), err.read_bounded()[0].decode()
        assert messages[-1]["exit_code"] == 0, errors
        assert admissions == [{"id": identifier, "status": "admitted"}]
        observed = json.loads(output.splitlines()[0])
        child = observed["child"]
        expected, _ = host.exported_arguments(entrypoint(real_package), cwd=checkout,
            request=json.loads(payload)["request"], session=identifier, model="opus")
        assert observed["args"] == expected
        assert observed["uid"] == identities.worklink_uid
        assert not (homes / identifier).exists()
        with pytest.raises(ProcessLookupError):
            os.kill(child, 0)
        for name in ("CLAUDE_CODE_OAUTH_TOKEN", "GITHUB_TOKEN"):
            assert spec.env[name] not in output + errors
    finally:
        if child is not None:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for fd in fds[:1] if len(fds) == 3 else ():
            os.close(fd)
        out.close()
        err.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "persist", "admit", "retirement"])
async def test_factory_admission_follows_durable_process_binding(real_package, tmp_path, monkeypatch, failure):
    from mimir_cultivate.worklink import orchestrator as orch
    from mimir_cultivate.worklink.checkout import CheckoutLease
    from mimir_cultivate.worklink.claims import ClaimRecord, ClaimResult
    from mimir_cultivate.worklink.factory_state import load_factory_record, save_factory_record

    repo, checkout = tmp_path / "repo", tmp_path / "operator"
    repo.mkdir()
    checkout.mkdir()
    events = []
    selected = FeatureFactoryBackend(host="claude_code", entrypoint=str(entrypoint(real_package)))
    registry = BackendRegistry()
    registry.register(selected)

    class Compute:
        name = "local_subprocess"

        async def launch(self, spec):
            pending = load_factory_record(tmp_path, "chainlink-2126")
            assert pending.handle is None and pending.host == "claude_code"
            assert pending.launch_generation == spec.backend_config["worker_identifier"]
            assert pending.launcher == str(entrypoint(real_package))
            self.spec = spec
            events.append("launch-held")
            return compute.LaunchHandle("local_subprocess", spec.backend_config["worker_identifier"], 42, 999999999)

        async def admit_factory_handle(self, handle):
            retained = load_factory_record(tmp_path, "chainlink-2126")
            assert retained.handle == handle
            assert retained.host == "claude_code"
            assert retained.session == retained.lock_session == "claude-code:" + handle.identifier
            assert retained.launch_generation == handle.identifier
            assert retained.session_checkout == str(checkout)
            assert retained.launcher == str(entrypoint(real_package))
            assert retained.opencode_session_id is None
            events.append("admit")
            if failure in {"admit", "retirement"}:
                raise RuntimeError("admission acknowledgement lost")

        async def cancel(self, handle):
            events.append("cancel-exact")
            assert handle.identifier == self.spec.backend_config["worker_identifier"]
            if failure == "retirement":
                raise RuntimeError("retirement unverified")

        async def cleanup(self, handle):
            events.append("cleanup")

    bound = Compute()
    monkeypatch.setattr(registry, "select_compute", lambda **kwargs: bound)
    monkeypatch.setattr(orch, "assert_base_repo_not_worker_writable", lambda *args, **kwargs: None)
    monkeypatch.setattr(FeatureFactoryBackend, "admit", lambda self: Path(self.entrypoint))
    monkeypatch.setattr(orch, "_create_backend_checkout", lambda *args, **kwargs:
        CheckoutLease(2126, 1, repo, checkout, "issue/2126-a1", "main", isolated_checkout=True))
    monkeypatch.setattr(orch, "_sweep_retained_attempts", lambda *args, **kwargs: None)
    monkeypatch.setattr(orch.WorklinkRunner, "_preserve_factory_attempt", lambda *args: None)
    monkeypatch.setattr(orch, "_git_credential_settings", lambda *args: ())
    order = order_for(checkout)
    order = replace(order, env={**order.env, "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid"})
    monkeypatch.setattr(orch.WorklinkRunner, "_factory_launch_order", lambda *args, **kwargs: (order, "fixture-bot"))
    monkeypatch.setattr(orch.ChainlinkClaims, "transition_issue", lambda *args, **kwargs: None)

    def claim(self, *args, **kwargs):
        kwargs["before_claim"]()
        return ClaimResult(True, ClaimRecord(2126, 1, "fixture", datetime.now(UTC)))

    monkeypatch.setattr(orch.ChainlinkClaims, "claim_issue", claim)

    def release(*args, **kwargs):
        assert load_factory_record(tmp_path, "chainlink-2126").children_reaped
        events.append("release")
        return True

    monkeypatch.setattr(orch, "_release_issue_and_clear_run_state", release)

    async def supervise(self, **kwargs):
        assert events == ["launch-held", "admit"]
        record = kwargs["factory_record"]
        save_factory_record(tmp_path, replace(record, children_reaped=True, controller_phase="stopped"))
        events.append("supervise")
        return orch.WorklinkRunResult(2126, 1, "interrupted")

    monkeypatch.setattr(orch.WorklinkRunner, "_supervise_factory_070", supervise)

    def save(home, record):
        if failure == "persist" and record.handle is not None and not events.count("cancel-exact"):
            raise OSError("durable process binding refused")
        return save_factory_record(home, record)

    monkeypatch.setattr(orch, "save_factory_record", save)

    def runner(args, **kwargs):
        output = json.dumps({"id": 2126, "title": "epic", "description": "build",
                             "labels": ["worklink", "worklink:epic"], "comments": []})
        if args[:3] == ["git", "-C", str(repo)] and "config" in args:
            output = "git@github.com:owner/repo.git\n"
        return subprocess.CompletedProcess(args, 0, output, "")

    controller = orch.WorklinkRunner(home=tmp_path, repo=repo, runner=runner, registry=registry)
    result = await controller.run_epic(2126)
    retained = load_factory_record(tmp_path, "chainlink-2126")
    assert retained is not None, result
    assert retained.host == "claude_code" and retained.opencode_session_id is None
    if failure is None:
        assert result.status == "interrupted"
        assert events == ["launch-held", "admit", "supervise", "release"]
    else:
        assert result.status == "failed"
        assert "cancel-exact" in events
        assert ("release" in events) == (failure != "retirement")
        assert retained.children_reaped == (failure != "retirement")
        if failure == "persist":
            assert "admit" not in events
        if failure == "retirement":
            # Recovery selects the recorded Claude host and exact CLI despite a
            # deployment change, and cannot admit over an unverified old worker.
            registry.register(FeatureFactoryBackend(host="opencode", entrypoint="/opt/changed/bin/factory.js"))
            observed = []

            def status(self, *args, **kwargs):
                observed.append((self.host, kwargs["launcher"]))
                from mimir_cultivate.worklink.backends.feature_factory import FactoryStatus
                return FactoryStatus(run_id=retained.run_id, valid=True, sandbox_path=retained.sandbox,
                    status="running", mode="autonomous", branch=retained.branch, pr_base="main",
                    pr_draft=False, dead_lock=False, lock="fresh", lock_session=retained.lock_session)

            monkeypatch.setattr(FeatureFactoryBackend, "status", status)
            monkeypatch.setattr(orch, "_verify_factory_recovery_target",
                                lambda **kwargs: Path(kwargs["retained"].sandbox))
            again = await controller.run_epic(2126)
            assert again.status == "blocked"
            assert "cannot verify" in again.reason
            assert observed == [("claude_code", retained.launcher)]
            assert events.count("launch-held") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status_kind", ["running", "completed", "foreign"])
async def test_claude_factory_status_authority_and_early_exit(tmp_path, monkeypatch, status_kind):
    from mimir_cultivate.worklink import orchestrator as orch
    from mimir_cultivate.worklink.backends.feature_factory import FactoryStatus
    from mimir_cultivate.worklink.claims import ClaimRecord
    from mimir_cultivate.worklink.factory_state import save_factory_record, load_factory_record

    identifier = str(uuid.uuid4())
    sandbox = tmp_path / "operator/.factory-sandboxes/chainlink-2126"
    sandbox.mkdir(parents=True)
    handle = compute.LaunchHandle("local_subprocess", identifier, 42, 999999999)
    status = FactoryStatus(run_id="chainlink-2126", valid=True, sandbox_path=str(sandbox),
        status="completed" if status_kind == "completed" else "running", mode="autonomous",
        branch="feature/chainlink-2126", pr_base="main", pr_draft=False, dead_lock=False, lock="fresh",
        lock_session="claude-code:" + ("33333333-3333-4333-8333-333333333333" if status_kind == "foreign" else identifier))
    record = FactoryRunRecord(run_id=status.run_id, issue_id=2126, attempt=1,
        repository="owner/repo", base_ref="main", branch=status.branch, launcher=host.DEFAULT_ENTRYPOINT,
        sandbox=str(sandbox), session="claude-code:" + identifier, lock_session="claude-code:" + identifier,
        host="claude_code", launch_generation=identifier, handle=handle, status=None,
        observed_at=None, controller_phase="running")
    save_factory_record(tmp_path, record)
    output = (FIXTURES / "claude_code_2_1_326" /
              ("lock_refused.jsonl" if status_kind == "foreign" else "success.jsonl")).read_text()
    events = []

    class Compute:
        async def wait(self, selected, timeout):
            return compute.ComputeResult(0, output, "", handle=handle, children_reaped=True)

        def job_alive(self, selected):
            return False

        async def cancel(self, selected):
            assert selected == handle
            events.append("cancel")

        async def cleanup(self, selected):
            events.append("cleanup")

    class Factory:
        poll_interval_s = 0.001

        def status(self, *args, **kwargs):
            assert kwargs["launcher"] == record.launcher
            return status

        def heartbeat(self, *args, **kwargs):
            assert kwargs["session"] == record.lock_session

        def terminal(self, *args, **kwargs):
            pytest.fail("host exit must not rewrite factory state")

    async def finish(self, **kwargs):
        assert kwargs["factory_record"].status.status == "completed"
        assert kwargs["factory_record"].children_reaped
        return orch.WorklinkRunResult(2126, 1, "review_ready")

    monkeypatch.setattr(orch.WorklinkRunner, "_finish_factory_070", finish)
    controller = orch.WorklinkRunner(home=tmp_path, repo=sandbox)
    invocation = controller._supervise_factory_070(
        issue=orch.IssueContext(2126, "epic", "build", {"worklink:epic"}),
        claim_record=ClaimRecord(2126, 1, "fixture", datetime.now(UTC)),
        claims=SimpleNamespace(heartbeat_issue=lambda *args, **kwargs: None,
                               transition_issue=lambda *args, **kwargs: None),
        backend=Factory(), compute=Compute(), factory_record=record, test_cmd="true",
        runner=lambda *args, **kwargs: pytest.fail("unexpected external command"), started_at=datetime.now(UTC))
    if status_kind == "completed":
        assert (await asyncio.wait_for(invocation, 10)).status == "review_ready"
    else:
        with pytest.raises(orch.WorklinkError, match="Claude host stopped while factory status was running" if status_kind == "running" else "did not acquire"):
            await asyncio.wait_for(invocation, 10)
        assert load_factory_record(tmp_path, record.run_id).opencode_session_id is None
    assert "cleanup" in events


def test_all_claude_factory_controls_use_recorded_vendor_and_scrub_credentials(real_package, tmp_path, monkeypatch):
    from mimir_cultivate.worklink import worker_client
    from mimir_cultivate.opencode import redact_daemon_output

    token = "unit-" + "control-publishing-material"
    monkeypatch.setenv("GH_TOKEN", token)
    monkeypatch.setenv("GITHUB_TOKEN", token)
    monkeypatch.setenv("MIMIR_FACTORY_PUBLISHING_IDENTITY", "fixture-bot")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "unit-" + "control-oauth-material")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "not-" + "admitted")
    monkeypatch.setattr(worker_client, "factory_checkout_for_path", lambda path: None)
    operator = tmp_path / "operator"
    sandbox = operator / ".factory-sandboxes/chainlink-2126"
    status = json.loads((FIXTURES / "feature_factory_0_14_1/status.json").read_text())
    status["sandbox_path"] = str(sandbox)
    commands = []

    def run(argv, **kwargs):
        assert argv[:2] == ["node", str(entrypoint(real_package))]
        environment = kwargs["env"]
        assert environment["GH_TOKEN"] == environment["GITHUB_TOKEN"] == token
        assert environment["FACTORY_PUBLISHING_IDENTITY"] == "fixture-bot"
        assert "ANTHROPIC_API_KEY" not in environment
        assert token not in repr(argv)
        commands.append(argv[2])
        return subprocess.CompletedProcess(argv, 0, json.dumps(status).encode(), b"")

    backend = FeatureFactoryBackend(host="claude_code", entrypoint=str(entrypoint(real_package)), runner=run)
    backend.initialize("chainlink-2126", operator_checkout=operator, base_ref="main",
                       publishing_identity="fixture-bot", credentials={"GITHUB_TOKEN": token})
    backend.status("chainlink-2126", sandbox=sandbox, launcher=backend.entrypoint)
    backend.resume("chainlink-2126", session=status["lock_session"], sandbox=sandbox, launcher=backend.entrypoint)
    backend.heartbeat("chainlink-2126", session=status["lock_session"], sandbox=sandbox, launcher=backend.entrypoint)
    backend.lock("chainlink-2126", "release", session=status["lock_session"], sandbox=sandbox, launcher=backend.entrypoint)
    backend.terminal("chainlink-2126", "needs-human", reason="fixture", sandbox=sandbox, launcher=backend.entrypoint)
    backend.snapshot("chainlink-2126", sandbox=sandbox, launcher=backend.entrypoint)
    assert commands == ["init", "status", "status", "resume", "status", "heartbeat", "lock", "terminal", "snapshot"]
    assert token not in redact_daemon_output("diagnostic " + token)


@pytest.mark.asyncio
async def test_claude_factory_containment_preserves_only_explicit_epic_inputs(real_package, tmp_path, monkeypatch):
    order = order_for(tmp_path)
    spec = spec_for(real_package, order)
    observed = []

    class Process:
        pid = os.getpid()
        process_start_ticks = 42
        timed_out = output_overflow = False

        async def wait(self):
            return 0

    class Client:
        async def launch(self, **kwargs):
            observed.append(kwargs)
            return Process()

        async def admit_factory_handle(self, handle):
            observed.append(handle)

        async def cancel(self, identifier):
            pass

    monkeypatch.setattr(compute, "_local_child_env", lambda: pytest.fail("Claude inherited controller credentials"))
    backend = compute.LocalSubprocessComputeBackend(name="renamed-compute", _worker_client=Client())
    handle = await backend.launch(replace(spec, backend="renamed-tool"))
    await backend.admit_factory_handle(handle)
    result = await backend.wait(handle, 30)
    await backend.cleanup(handle)
    request = observed[0]
    assert request["home_projection_profile"] == "claude_factory"
    assert request["env"]["GH_TOKEN"] == request["env"]["GITHUB_TOKEN"] == spec.env["GITHUB_TOKEN"]
    assert request["env"]["CLAUDE_CODE_OAUTH_TOKEN"] == spec.env["CLAUDE_CODE_OAUTH_TOKEN"]
    assert request["env"]["PATH"] == os.environ["PATH"]
    assert "HOME" not in request["env"] and "CLAUDE_CONFIG_DIR" not in request["env"]
    assert not {"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "OPENCODE_CONFIG"}.intersection(request["env"])
    assert observed[1] == handle
    assert result.children_reaped and result.stdout_path is result.stderr_path is None


@pytest.mark.asyncio
@pytest.mark.parametrize("verified", [False, True])
@pytest.mark.parametrize("lifecycle", ["running", "needs-human"])
async def test_claude_recovery_requires_proof_and_fresh_durable_uuid(real_package, tmp_path, monkeypatch, verified, lifecycle):
    from mimir_cultivate.worklink import orchestrator as orch, daemon_health
    from mimir_cultivate.worklink.backends.feature_factory import FactoryStatus
    from mimir_cultivate.worklink.claims import ClaimRecord
    from mimir_cultivate.worklink.factory_state import save_factory_record, load_factory_record

    identifier = str(uuid.uuid4())
    operator = tmp_path / "operator"
    sandbox = operator / ".factory-sandboxes/chainlink-2126"
    sandbox.mkdir(parents=True)
    status = FactoryStatus(run_id="chainlink-2126", valid=True, sandbox_path=str(sandbox),
        status=lifecycle, mode="autonomous", branch="feature/chainlink-2126", pr_base="main",
        pr_draft=False, dead_lock=False, lock="fresh", lock_session="claude-code:" + identifier,
        next="implementation", next_present=True,
        park_snapshot=str(operator / ".factory/.parked/chainlink-2126") if lifecycle == "needs-human" else None)
    record = FactoryRunRecord(run_id=status.run_id, issue_id=2126, attempt=1,
        repository="owner/repo", base_ref="main", branch=status.branch, launcher=str(entrypoint(real_package)),
        sandbox=str(sandbox), session="claude-code:" + identifier, lock_session="claude-code:" + identifier,
        host="claude_code", launch_generation=identifier,
        handle=compute.LaunchHandle("local_subprocess", identifier, 42, 999999999), status=status,
        observed_at=None, controller_phase="parked", children_reaped=verified, session_checkout=str(operator))
    save_factory_record(tmp_path, record)
    events = []

    class Factory(FeatureFactoryBackend):
        def status(self, *args, **kwargs):
            assert kwargs["launcher"] == record.launcher
            return replace(status, lock="absent", lock_session=None) if "release" in events else status

        def lock(self, run_id, action, **kwargs):
            assert action == "release" and kwargs["session"] == record.lock_session
            assert kwargs["launcher"] == record.launcher
            events.append("release")

    class Compute:
        async def launch(self, spec):
            pending = load_factory_record(tmp_path, record.run_id)
            assert pending.handle is None and pending.launch_generation != identifier
            assert pending.launch_generation == spec.backend_config["worker_identifier"]
            assert pending.session == pending.lock_session == "claude-code:" + pending.launch_generation
            assert pending.launcher == record.launcher and pending.host == "claude_code"
            events.append("launch")
            return compute.LaunchHandle("local_subprocess", pending.launch_generation, 43, 999999998)

        async def admit_factory_handle(self, handle):
            assert load_factory_record(tmp_path, record.run_id).handle == handle
            events.append("admit")

    monkeypatch.setattr(orch, "_verify_factory_recovery_binding", lambda **kwargs: sandbox)
    monkeypatch.setattr(orch, "_repo_remote_url", lambda *args, **kwargs: "git@github.com:owner/repo.git")
    monkeypatch.setattr(orch, "_git_credential_settings", lambda *args: ())
    monkeypatch.setattr(daemon_health, "daemon_health", lambda *args: pytest.fail("Claude consulted daemon health"))
    order = order_for(operator)
    order = replace(order, env={**order.env, "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid"})
    monkeypatch.setattr(orch.WorklinkRunner, "_factory_launch_order", lambda *args, **kwargs: (order, "fixture-bot"))

    async def supervise(self, **kwargs):
        current = kwargs["factory_record"]
        assert kwargs["recovery_session"] == current.lock_session != record.lock_session
        assert current.opencode_session_id is None
        events.append("supervise")
        return orch.WorklinkRunResult(2126, 1, "interrupted")

    monkeypatch.setattr(orch.WorklinkRunner, "_supervise_factory_070", supervise)
    controller = orch.WorklinkRunner(home=tmp_path, repo=operator)
    invocation = controller._recover_factory_070(
        issue=orch.IssueContext(2126, "epic", "build", {"worklink:epic"}),
        claim_record=ClaimRecord(2126, 2, "fixture", datetime.now(UTC)), claims=SimpleNamespace(),
        backend=Factory(host="claude_code", entrypoint=record.launcher), compute=Compute(), retained=record,
        launcher=Path(record.launcher), repo_slug="owner/repo", base="main", test_cmd="true",
        runner=lambda *args, **kwargs: pytest.fail("unexpected controller command"),
        work_item_json=json.dumps({"run_id": record.run_id}))
    if verified:
        assert (await invocation).status == "interrupted"
        assert events == ["release", "launch", "admit", "supervise"]
    else:
        with pytest.raises(orch.WorklinkError, match="verified descendant retirement"):
            await invocation
        assert events == []
