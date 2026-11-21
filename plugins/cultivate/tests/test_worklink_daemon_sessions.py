"""Synthetic pinned-protocol lifecycle tests; no provider requests."""
from __future__ import annotations

import json
import asyncio
import os
import time
from copy import deepcopy
from types import SimpleNamespace

import pytest

from mimir_cultivate.worklink.backends.base import SessionRequest
from mimir_cultivate.worklink.daemon_client import DaemonClientError, OpenCodeDaemonClient, ServerEvent
from mimir_cultivate.worklink.daemon_sessions import ControllerRecord, SessionController, session_environment
from tests.worklink_daemon_fixtures import FIXTURES, FakeOpenCodeDaemon, SyntheticFault, effective_permission


async def create(client, tmp_path, kind="leaf"):
    return await SessionController.create(
        client, tmp_path / "controller.json", request=SessionRequest(
            kind, {"providerID": "openai", "id": "gpt-5", "variant": "high"},
            "--autonomous --max-retries 2 chainlink-2125" if kind == "epic" else "build",
            ("git *", "pytest *") if kind == "leaf" else (),
        ), issue_id=2125, attempt=1, generation="generation-1", checkout=tmp_path,
        environment={"PATH": "/usr/bin:/bin"}, timeout_s=60,
    )


@pytest.mark.parametrize("action", ["session_rearm", "session_release_quiescent", "session_resume"])
@pytest.mark.parametrize("mismatch", ["issue", "attempt", "leaf"])
async def test_retained_recovery_requires_exact_epic_owner(tmp_path, action, mismatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "leaf" if mismatch == "leaf" else "epic")
        daemon.finish(controller.record.opencode_session_id, outcome="interrupted")
        with pytest.raises(DaemonClientError, match="retained_not_epic_driver" if mismatch == "leaf"
                           else "retained_attempt_mismatch"):
            await controller.recover_epic(issue=2126 if mismatch == "issue" else 2125,
                attempt=2 if mismatch == "attempt" else 1, action=action)
        assert controller.path.exists()
        assert not any(route.endswith("/prompt") for _, route, _, _ in daemon.requests if mismatch != "leaf")


@pytest.mark.parametrize("action", ["session_release_quiescent", "session_resume"])
@pytest.mark.parametrize("blocker", ["running", "child-running", "shell", "root-shell", "resume", "form", "question",
                                    "input", "process", "unavailable", "changed"])
async def test_retained_operator_recovery_refuses_unverified_quiescence(tmp_path, monkeypatch, action, blocker):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        child = daemon.child_session(root, str(tmp_path))
        daemon.finish(root, outcome="interrupted")
        reason = {
            "running": "retained_running", "child-running": "retained_running",
            "shell": "retained_running_shells", "root-shell": "retained_running_shells",
            "resume": "retained_pending_resume", "form": "retained_pending_form",
            "question": "retained_pending_form", "input": "retained_pending_input",
            "process": "retained_process_cleanup_unverified", "unavailable": "daemon HTTP failure",
            "changed": "retained_activity_changed",
        }[blocker]
        if blocker == "running":
            await client.prompt(root, "test-owned work")
        elif blocker == "child-running":
            await client.prompt(child, "test-owned work")
        elif blocker in {"shell", "root-shell"}:
            daemon.start_shell(root if blocker == "root-shell" else child, str(tmp_path))
        elif blocker == "resume":
            shell = daemon.start_shell(child, str(tmp_path))
            daemon.shells[shell]["status"] = "exited"
            daemon.enqueue_synthetic(child, shell)
        elif blocker in {"form", "question"}:
            form = daemon.question(child)
            if blocker == "form":
                form["metadata"]["kind"] = "other"
        elif blocker == "input":
            daemon.auto_deliver = False
            await client.prompt(child, "test-owned input")
        elif blocker == "process":
            controller._persist(cleanup_processes=[[200, 100]])
        elif blocker == "unavailable":
            daemon.faults[("GET", f"/api/session/{root}")].append(SyntheticFault(status=500))
        elif blocker == "changed":
            shells = controller._tree_shells
            async def race(tree):
                result = await shells(tree)
                item = daemon.enqueue(root, "racing input")
                daemon.deliver(root, item["id"])
                daemon.finish(root, outcome="interrupted")
                return result
            monkeypatch.setattr(controller, "_tree_shells", race)
        before = len(daemon.requests)
        with pytest.raises(DaemonClientError) as error:
            await controller.recover_epic(issue=2125, attempt=1, action=action)
        assert reason in str(error.value)
        assert controller.path.exists()
        assert all(method == "GET" for method, _, _, _ in daemon.requests[before:])


async def test_retained_resume_delivers_one_fixed_input_and_reconciles_lost_ack(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        daemon.finish(root, outcome="interrupted")
        controller._persist(deadline_at_ms=1)
        daemon.faults[("POST", f"/api/session/{root}/prompt")].append(SyntheticFault(after=True, disconnect=True))
        with pytest.raises(DaemonClientError):
            await controller.recover_epic(issue=2125, attempt=1, action="session_resume")
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        with pytest.raises(DaemonClientError, match="retained_resume_already_dispatched"):
            await recovered.recover_epic(issue=2125, attempt=1, action="session_resume")
        assert await recovered.recover_epic(issue=2125, attempt=1, action="session_rearm") == {"recovery": "continued"}
        prompts = [body for method, route, _, body in daemon.requests if method == "POST" and route.endswith("/prompt")]
        assert prompts == [{"text": "Continue the persisted factory run chainlink-2125 using factory resume semantics. "
                           "Resume its saved state and next step; do not create a new run or story.", "resume": True}]
        assert (await recovered.observe()).execution_state == "started"
        daemon.finish(root)
        assert (await recovered.observe()).execution_state == "succeeded"


async def test_operator_can_release_an_ended_resume_before_supervision(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        daemon.finish(root, outcome="interrupted")
        await controller.recover_epic(issue=2125, attempt=1, action="session_resume")
        daemon.finish(root, outcome="interrupted")
        assert await controller.recover_epic(issue=2125, attempt=1,
                                             action="session_release_quiescent") == {"released": True}
        assert not controller.path.exists()


async def test_operator_resume_refuses_excessive_prior_inputs(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        daemon.finish(root, outcome="interrupted")
        for _ in range(16):
            item = daemon.enqueue(root, "test-owned historical input")
            daemon.deliver(root, item["id"])
            daemon.finish(root, outcome="interrupted")
        before = len(daemon.requests)
        with pytest.raises(DaemonClientError, match="retained_resume_history_bound"):
            await controller.recover_epic(issue=2125, attempt=1, action="session_resume")
        assert not any(method == "POST" for method, _, _, _ in daemon.requests[before:])


@pytest.mark.parametrize("field", ["text", "input", "prompt", "run_id"])
def test_resume_session_rejects_caller_supplied_input_at_executor_boundary(field):
    from mimir_cultivate.worklink.worker_exec import _handle_session
    request = dict.fromkeys(("version", "op", "executor_identity", "issue", "attempt", "device", "inode",
                             "path", "generation", "opencode_session_id"), "fixture")
    request["op"] = "session_resume"
    request[field] = "operator or forge supplied instructions"
    with pytest.raises(RuntimeError, match="exact contract"):
        _handle_session(None, request, [123])


def test_resume_session_cli_has_no_free_text_option():
    import argparse
    from mimir_cultivate.commands import add_argparse
    parser = argparse.ArgumentParser()
    add_argparse(parser.add_subparsers())
    for extra in (["--text", "arbitrary instructions"], ["arbitrary instructions"]):
        with pytest.raises(SystemExit):
            parser.parse_args(["worklink", "resume-session", "2125", "--attempt", "1", "--confirm-quiescent", *extra])


@pytest.mark.parametrize("kind", ["leaf", "spawn", "epic"])
async def test_every_session_kind_denies_question(tmp_path, kind):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, kind)
        rules = daemon.sessions[controller.record.opencode_session_id].info["permissions"]
        assert {"action": "question", "resource": "*", "effect": "deny"} in rules
        assert effective_permission("question", "*", agent=[
            {"action": "*", "resource": "*", "effect": "allow"}], session=rules,
            project=[{"action": "question", "resource": "*", "effect": "allow"}]) == "deny"


async def test_epic_children_inherit_question_deny_for_factory_agents(tmp_path):
    agents = json.loads((FIXTURES / "opencode_2_3_0/factory-result.json").read_bytes())["agents"]
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        checked = set()
        for agent in agents:
            child = daemon.new_session({"parentID": root, "agent": agent["id"]})
            grandchild = daemon.new_session({"parentID": child["id"], "agent": agent["id"]})
            for info in (child, grandchild):
                assert {"action": "question", "resource": "*", "effect": "deny"} in info["permissions"]
                assert effective_permission("question", "*", agent=agent.get("permissions", []),
                                            session=info["permissions"]) == "deny"
            checked.add(agent["id"])
        assert {"backend-builder", "frontend-builder", "run-orchestrator"} <= checked


@pytest.mark.parametrize("depth", [0, 1, 3])
async def test_pending_question_surfaces_in_first_supervisor_observation(tmp_path, depth):
    from mimir_cultivate.worklink.backends.base import LaunchHandle
    from mimir_cultivate.worklink.compute import OpenCodeDaemonComputeBackend

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        sid = controller.record.opencode_session_id
        for _ in range(depth):
            sid = daemon.new_session({"parentID": sid, "agent": "backend-builder"})["id"]
        form = daemon.question(sid, title="Slice amendment", text="May I add the six test paths?")
        observed = []
        async def observe(handle):
            observed.append(handle)
            assert len(observed) == 1, "must surface in the first observe, not wait for the deadline"
            return await controller.observe()
        backend = OpenCodeDaemonComputeBackend(client=SimpleNamespace(session_observe=observe))
        handle = LaunchHandle("opencode_daemon", controller.record.opencode_session_id)
        result = await backend.wait(handle, timeout_s=60)
        assert result.exit_code == 1 and not result.timed_out
        assert result.session_error.type == "pending_question"
        assert sid in result.session_error.message and form["id"] in result.session_error.message
        assert "Slice amendment" in result.session_error.message
        assert "May I add the six test paths?" in result.session_error.message
        assert result.session_outcome.cancellation_state == "none"
        assert controller.record.execution_state == "pending"  # no false terminal/quiescent checkpoint
        assert daemon.sessions[sid].forms == [form]
        assert not daemon.sessions[sid].replies
        assert not any("/form/" in r[1] and r[0] != "GET" for r in daemon.requests)


async def test_worklink_never_replies_to_question_forms(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        child = daemon.new_session({"parentID": root})["id"]
        forms = [daemon.question(sid) for sid in (root, child)]
        before = len(daemon.requests)
        for _ in range(2):
            assert (await controller.observe()).error.type == "pending_question"
        assert await controller.cancel()
        await controller.release()
        assert all(r[0] == "GET" for r in daemon.requests[before:] if "/form" in r[1])
        assert [daemon.sessions[sid].forms[0] for sid in (root, child)] == forms
        assert not any(state.replies for state in daemon.sessions.values())


async def test_question_diagnostics_are_bounded_redacted_and_not_persisted(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        secret = "synthetic-" + "question-private-token"
        controller.sensitive_values = (secret,)
        daemon.question(controller.record.opencode_session_id, title=secret + "é" * 3000,
                        text="Question text " + secret + "é" * 10000)
        outcome = await controller.observe()
        assert outcome.error.type == "pending_question"
        assert secret not in repr(outcome)
        assert "Question text <redacted>" in outcome.error.message
        assert len(outcome.error.message.encode()) <= 2048
        assert len(outcome.error.body.encode()) <= 8192
        assert secret.encode() not in controller.path.read_bytes()


async def test_unrelated_and_non_question_forms_do_not_block_observed_session(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        other = daemon.new_session({"location": {"directory": str(tmp_path)}})["id"]
        daemon.question(other)
        form = daemon.question(controller.record.opencode_session_id)
        form["metadata"]["kind"] = "other"
        assert (await controller.observe()).error is None
        daemon.finish(controller.record.opencode_session_id)
        assert (await controller.observe()).execution_state == "succeeded"


@pytest.mark.parametrize("case", ["cycle", "too_many"])
async def test_question_observation_rejects_unbounded_descendant_tree(tmp_path, monkeypatch, case):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        calls = []
        async def children(sid):
            calls.append(sid)
            assert len(calls) < 3, "cyclic tree must fail closed, not revisit the root"
            return ([{"id": root}] if case == "cycle" else
                    [{"id": f"ses_child{i}"} for i in range(4096)])
        monkeypatch.setattr(client, "children", children)
        reason = "invalid session descendant tree" if case == "cycle" else "session tree exceeds bound"
        with pytest.raises(DaemonClientError, match=reason):
            await controller.observe()


async def test_question_on_later_child_page_is_observed(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        for _ in range(101):
            child = daemon.new_session({"parentID": root})
        daemon.question(child["id"])
        assert (await controller.observe()).error.type == "pending_question"
        assert any("cursor" in r[2] for r in daemon.requests if r[1] == "/api/session")


@pytest.mark.parametrize("kind", ["leaf", "spawn", "epic"])
async def test_authoritative_completion_and_restart(tmp_path, kind):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, kind)
        sid = controller.record.opencode_session_id
        assert daemon.sessions[sid].environment["MIMIR_MODEL_SPEC"] == "openai/gpt-5#high"
        assert daemon.sessions[sid].environment["MIMIR_WORKLINK_GATE"] == "1"
        assert (await controller.observe()).execution_state == "started"
        daemon.finish(sid)
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        assert (await recovered.observe()).execution_state == "succeeded"
        assert len([r for r in daemon.requests if r[0] == "POST" and r[1].endswith(("/prompt", "/command"))]) == 1
        assert json.loads(controller.path.read_bytes())["version"] == 1
        assert b"build" not in controller.path.read_bytes()
        await recovered.release()
        assert not controller.path.exists()


async def test_lost_ack_is_reconciled_without_repost(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("POST", "/api/session/ses_000000000000000000000001/prompt")].append(
            SyntheticFault(disconnect=True, after=True))
        controller = await create(client, tmp_path)
        assert controller.record.admission_state == "uncertain"
        assert (await controller.observe()).execution_state == "started"
        daemon.finish(controller.record.opencode_session_id)
        assert (await controller.observe()).execution_state == "succeeded"
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_initial_idle_is_not_success(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        daemon.sessions[controller.record.opencode_session_id].messages.append({
            "id": "msg_initial", "type": "idle", "time": {"created": 1}, "outcome": "succeeded",
        })
        assert (await controller.observe()).execution_state == "pending"


@pytest.mark.parametrize("race", [False, True])
async def test_queued_cancel_and_delivery_race(tmp_path, race):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        daemon.delete_race = race
        controller = await create(client, tmp_path)
        assert await controller.cancel(timeout_s=5)
        state = daemon.sessions[controller.record.opencode_session_id]
        assert state.inbox == []
        assert not state.active
        assert controller.record.cancellation_state == "verified"


async def test_interrupt_false_retains_controller(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.interrupt_noop = True
        controller = await create(client, tmp_path)
        assert not await controller.cancel(timeout_s=0.1)
        assert controller.path.exists()
        assert controller.record.cancellation_state == "unverified"


async def test_lost_interrupt_ack_reconciles_without_reprompt(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.faults[("POST", f"/api/session/{sid}/interrupt")].append(
            SyntheticFault(disconnect=True, after=True))
        assert await controller.cancel(timeout_s=5)
        assert not daemon.sessions[sid].active
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_setup_http_401_keeps_created_identity_and_is_not_provider_auth(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("PUT", "/api/session/ses_000000000000000000000001/environment")].append(
            SyntheticFault(status=401))
        controller = await create(client, tmp_path)
        outcome = await controller.observe()
        assert outcome.opencode_session_id == controller.record.opencode_session_id
        assert outcome.execution_state == "incomplete"
        assert outcome.error.type == "backend_error"
        assert not any(r[1].endswith("/prompt") for r in daemon.requests)
        assert await controller.cancel(timeout_s=5)


async def test_generation_and_inode_binding(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        with pytest.raises(ValueError, match="generation mismatch"):
            SessionController.recover(client, controller.path, generation="other", checkout=tmp_path)
        other = tmp_path / "other"
        other.mkdir()
        with pytest.raises(ValueError, match="generation mismatch"):
            controller.check_binding("generation-1", other)


async def test_tree_activity_paginates_all_descendants_and_ignores_unowned_shells(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        daemon.sessions[root].info["time"]["updated"] = 1000
        daemon.session_page_limit = 1
        child = daemon.child(root, updated_ms=2000)
        sibling = daemon.child(root, updated_ms=3000)
        leaf = daemon.child(sibling, updated_ms=4000, directory=str(tmp_path / "moved"))
        daemon.activity(leaf, 5000)
        daemon.start_shell("ses_foreign", str(tmp_path))
        # The root driver's lifetime heartbeat shell must not mask either the
        # idle tree or the later specialist-shell completion.
        daemon.start_shell(root, str(tmp_path))
        result = await controller.tree_activity()
        assert result == {"opencode_session_id": root, "last_activity_session_id": leaf,
                          "last_activity_at_ms": 5000, "pending_form": False, "running_shell": False}
        assert any(params.get("cursor") for method, route, params, _ in daemon.requests
                   if method == "GET" and route == "/api/session")
        daemon.question(child, 6000)
        shell = daemon.start_shell(leaf, str(tmp_path / "moved"))
        blocked = await controller.tree_activity()
        assert blocked["pending_form"] is True and blocked["running_shell"] is True
        assert blocked["last_activity_session_id"] == child
        assert blocked["last_activity_at_ms"] == 6000
        daemon.end_shell(shell)
        assert (await controller.tree_activity())["running_shell"] is False


async def test_activity_question_and_epic_cancel_share_the_session_tree_walker(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        daemon.sessions[root].info["time"]["updated"] = 1000
        daemon.session_page_limit = 1
        daemon.child(root, updated_ms=2000)
        archived = daemon.child(root, updated_ms=3000)
        daemon.sessions[archived].info["time"]["archived"] = 3
        leaf = daemon.child(archived, updated_ms=4000)
        shell = daemon.start_shell(leaf, str(tmp_path))
        read_tree = client.session_tree
        witnesses = []

        async def tracked_tree(sid):
            tree = await read_tree(sid)
            witnesses.append({session["id"] for session in tree})
            return tree

        monkeypatch.setattr(client, "session_tree", tracked_tree)
        activity = await controller.tree_activity()
        assert activity["last_activity_session_id"] == leaf
        assert activity["last_activity_at_ms"] == 4000 and activity["running_shell"]
        assert len(witnesses) == 1 and leaf in witnesses[-1]
        assert (await controller.observe()).error is None
        assert len(witnesses) == 2 and witnesses[-1] == witnesses[0]
        daemon.question(leaf, 5000)
        assert (await controller.observe()).error.type == "pending_question"
        before_cancel = len(witnesses)
        assert await controller.cancel()
        assert len(witnesses) > before_cancel
        assert all(tree == witnesses[0] for tree in witnesses)
        assert daemon.shells[shell]["status"] == "killed"
        assert not any(method != "GET" and "/form" in route for method, route, _, _ in daemon.requests)


async def test_shared_session_tree_narrows_pages_and_keeps_archived_descendants(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        children = [daemon.child(root, updated_ms=2000 + i) for i in range(30)]
        for sid in children:
            daemon.sessions[sid].info["title"] = "x" * 6000
        daemon.sessions[children[-1]].info["time"]["archived"] = 3
        leaf = daemon.child(children[-1], updated_ms=5000)
        tree = await client.session_tree(root)
        assert [session["id"] for session in tree] == [root, *children, leaf]
        pages = [params for method, route, params, _ in daemon.requests
                 if method == "GET" and route == "/api/session"]
        assert any(int(page["limit"]) < 100 for page in pages)
        assert any("cursor" in page for page in pages)
        assert all(set(page) == {"cursor", "limit"} for page in pages if "cursor" in page)


def session_page(daemon, children, next_cursor=None):
    daemon.faults[("GET", "/api/session")].append(SyntheticFault(body=json.dumps({
        "data": children, "cursor": {"previous": None, "next": next_cursor},
    }).encode()))


@pytest.mark.parametrize("operation", ["children", "observe"])
async def test_active_child_relisted_across_pages_keeps_latest_state(tmp_path, operation):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path, "epic")
        root = controller.record.opencode_session_id
        ids = [daemon.child(root, updated_ms=2000 + i) for i in range(3)]
        await client.prompt(ids[-1], "test-owned active child")
        first = [deepcopy(daemon.sessions[sid].info) for sid in ids]
        newer = deepcopy(first[-1])
        newer["time"]["updated"] += 1000
        newer["title"] = "Build acceptance mutation coverage"
        session_page(daemon, first, "updated-anchor")
        session_page(daemon, [newer])
        before = len(daemon.requests)
        if operation == "children":
            result = await client.children(root)
            assert [child["id"] for child in result] == ids
            assert result == [*first[:-1], newer]
        else:
            outcome = await controller.observe()
            assert outcome.error is None
            assert outcome.execution_state == "started"
            assert controller.record.execution_state == "started"
            assert controller.path.exists()
            assert daemon.sessions[ids[-1]].active
            # A duplicate must not enter the tree or cause repeated descendant reads.
            for sid in ids:
                assert sum(params.get("parentID") == sid for method, route, params, _
                           in daemon.requests[before:] if route == "/api/session") == 1
        pages = [params for method, route, params, _ in daemon.requests[before:]
                 if route == "/api/session"]
        assert pages[:2] == [
            {"parentID": root, "order": "asc", "limit": "100"},
            {"cursor": "updated-anchor", "limit": "100"},
        ]


async def test_relisted_child_with_foreign_parent_is_rejected(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        child = daemon.child(root, updated_ms=2000)
        info = deepcopy(daemon.sessions[child].info)
        session_page(daemon, [info], "next")
        session_page(daemon, [{**info, "parentID": "ses_foreign"}])
        with pytest.raises(DaemonClientError, match="foreign child session"):
            await client.children(root)


async def test_descendant_id_under_two_parents_is_rejected(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        parents = [daemon.child(root, updated_ms=2000 + i) for i in range(2)]
        child = daemon.child(parents[0], updated_ms=3000)
        info = deepcopy(daemon.sessions[child].info)
        session_page(daemon, [daemon.sessions[sid].info for sid in parents])
        session_page(daemon, [info])
        session_page(daemon, [{**info, "parentID": parents[1]}])
        with pytest.raises(DaemonClientError, match="invalid session descendant tree"):
            await client.session_tree(root)


@pytest.mark.parametrize("identity", [None, "", "../ses_bad", "msg_wrong", 17, []])
async def test_child_identity_is_validated(tmp_path, identity):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        session_page(daemon, [{"id": identity, "parentID": root}])
        with pytest.raises(DaemonClientError, match="invalid child session identity"):
            await client.children(root)


@pytest.mark.parametrize("page", [
    None, [], {}, {"data": {}}, {"data": [None]}, {"data": []},
    {"data": [], "cursor": []},
    {"data": [], "cursor": {"next": None}},
    {"data": [], "cursor": {"previous": None, "next": None, "extra": None}},
    {"data": [], "cursor": {"previous": 17, "next": None}},
    {"data": [], "cursor": {"previous": None, "next": "é" * 2049}},
])
async def test_malformed_child_page_is_rejected(tmp_path, monkeypatch, page):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        async def request(*args, **kwargs):
            return page
        monkeypatch.setattr(client, "_request", request)
        with pytest.raises(DaemonClientError, match="invalid session page"):
            await client.children(controller.record.opencode_session_id)


async def test_child_count_bound_counts_unique_latest_entries(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        children = [{"id": f"ses_child{i}", "parentID": root}
                    for i in range(4096)]
        newer = {**children[-1], "title": "new"}
        pages = iter([
            {"data": children, "cursor": {"previous": None, "next": "next"}},
            {"data": [newer], "cursor": {"previous": None, "next": None}},
        ])
        async def request(*args, **kwargs):
            return next(pages)
        monkeypatch.setattr(client, "_request", request)
        result = await client.children(root)
        assert result == [*children[:-1], newer]


@pytest.mark.parametrize("entries_per_page", [0, 1, 2])
async def test_child_pagination_work_is_bounded_with_fresh_cursors(
    tmp_path, monkeypatch, entries_per_page,
):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        child = {"id": "ses_relisted", "parentID": root}
        calls = 0
        expected_calls = 8192 // max(1, entries_per_page) + 1

        async def request(*args, **kwargs):
            nonlocal calls
            calls += 1
            # A removed work bound must fail promptly, not hang the test runner.
            assert calls <= expected_calls, "pagination escaped its work bound"
            return {"data": [child] * entries_per_page,
                    "cursor": {"previous": None, "next": f"cur{calls}"}}

        monkeypatch.setattr(client, "_request", request)
        with pytest.raises(DaemonClientError, match="session children exceed bound"):
            await client.children(root)
        assert calls == expected_calls


@pytest.mark.parametrize("payload", ["child", "metadata"])
async def test_child_pagination_counts_bytes_across_all_pages(tmp_path, monkeypatch, payload):
    from mimir_cultivate.worklink import daemon_client

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        monkeypatch.setattr(daemon_client, "MAX_ITEM_BYTES", 64)
        calls = 0

        async def request(*args, **kwargs):
            nonlocal calls
            calls += 1
            assert calls <= 2, "pagination escaped its cumulative byte bound"
            child = {"id": "ses_relisted", "parentID": root}
            result = {"data": [child], "cursor": {"previous": None, "next": f"cur{calls}"}}
            if payload == "child":
                child["title"] = "x" * 600
            else:
                result["padding"] = "x" * 600
            # A single full page fits; two exceed the read budget. Unique
            # retained state, occurrences, and page count remain below bounds.
            assert 512 < len(json.dumps(result).encode()) < 16 * daemon_client.MAX_ITEM_BYTES
            return result

        monkeypatch.setattr(client, "_request", request)
        with pytest.raises(DaemonClientError, match="session children exceed bound"):
            await client.children(root)
        assert calls == 2


@pytest.mark.parametrize("level", ["children", "replacement", "tree"])
async def test_session_enumeration_size_bounds(tmp_path, monkeypatch, level):
    from mimir_cultivate.worklink import daemon_client

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        monkeypatch.setattr(daemon_client, "MAX_ITEM_BYTES", 16)
        if level == "tree":
            async def info(sid):
                return {"id": sid, "title": "x" * 150}
            async def children(sid):
                return [{"id": "ses_child", "title": "x" * 150}] if sid == root else []
            monkeypatch.setattr(client, "session_info", info)
            monkeypatch.setattr(client, "children", children)
            operation = client.session_tree(root)
            reason = "session tree exceeds bound"
        else:
            child = {"id": "ses_child", "parentID": root}
            if level == "replacement":
                session_page(daemon, [child], "next")
            session_page(daemon, [{**child, "title": "x" * 300}])
            operation = client.children(root)
            reason = "session children exceed bound"
        with pytest.raises(DaemonClientError, match=reason):
            await operation


@pytest.mark.parametrize("boundary", ["root", "child", "form", "timestamp", "cursor", "page"])
async def test_tree_activity_rejects_foreign_or_incomplete_witness(tmp_path, boundary):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        child = daemon.child(root, updated_ms=2000)
        if boundary == "root":
            info = deepcopy(daemon.sessions[root].info)
            foreign = daemon.child(child, updated_ms=3000)
            info["id"] = foreign
            route, body, reason = f"/api/session/{root}", {"data": info}, "session identity mismatch"
        elif boundary == "child":
            info = deepcopy(daemon.sessions[child].info)
            info["parentID"] = "ses_foreign"
            route, body, reason = "/api/session", {"data": [info], "cursor": {"previous": None, "next": None}}, "foreign child session"
        elif boundary == "form":
            daemon.question(child, 3000)
            form = deepcopy(daemon.sessions[child].forms[0])
            form["sessionID"] = "ses_foreign"
            route, body, reason = f"/api/session/{child}/form", {"data": [form]}, "foreign session form"
        elif boundary == "timestamp":
            daemon.sessions[child].info["time"]["updated"] = True
            route, body, reason = "/api/session", {"data": [daemon.sessions[child].info], "cursor": {"previous": None, "next": None}}, "invalid session activity"
        elif boundary == "cursor":
            route, body, reason = "/api/session", {"data": [], "cursor": {"previous": None, "next": "same"}}, "repeated session cursor"
        else:
            route, body, reason = "/api/session", {"data": [], "cursor": {"previous": None, "next": 17}}, "invalid session page"
        daemon.faults[("GET", route)].extend([SyntheticFault(body=json.dumps(body).encode())] * (2 if boundary == "cursor" else 1))
        if boundary == "cursor":
            daemon.faults[("GET", route)].append(SyntheticFault(body=json.dumps({
                "data": [], "cursor": {"previous": None, "next": None}}).encode()))
        with pytest.raises(DaemonClientError, match=reason):
            await controller.tree_activity()


@pytest.mark.parametrize("bound", ["page", "tree"])
async def test_tree_activity_bounds_session_enumeration(tmp_path, monkeypatch, bound):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        root = controller.record.opencode_session_id
        if bound == "page":
            async def request(*args, **kwargs):
                return {"data": [{"id": f"ses_child{i}", "parentID": root} for i in range(4097)],
                        "cursor": {"previous": None, "next": None}}
            monkeypatch.setattr(client, "_request", request)
            operation = client.children(root)
            reason = "session children exceed bound"
        else:
            async def children(sid):
                return [{**daemon.sessions[root].info, "id": f"ses_child{i}", "parentID": root}
                        for i in range(4096)] if sid == root else []
            async def empty(*args):
                return []
            monkeypatch.setattr(client, "children", children)
            monkeypatch.setattr(client, "pending_forms", empty)
            monkeypatch.setattr(client, "running_shells", empty)
            operation = controller.tree_activity()
            reason = "session tree exceeds bound"
        with pytest.raises(DaemonClientError, match=reason):
            await operation


@pytest.mark.parametrize("invalid", ["root", "timestamp", "source", "form", "shell", "transport"])
async def test_tree_activity_rpc_validates_executor_witness(tmp_path, monkeypatch, invalid):
    from mimir_cultivate.worklink.backends.base import LaunchHandle
    from mimir_cultivate.worklink.worker_client import WorkerClient

    handle = LaunchHandle("opencode_daemon", "ses_owned", opencode_session_id="ses_owned", launch_generation="generation")
    client = WorkerClient(None, path_checkout=tmp_path, issue_id=2125, attempt=1)
    response = {"opencode_session_id": "ses_owned", "last_activity_session_id": "ses_child",
                "last_activity_at_ms": 1000, "pending_form": False, "running_shell": False}
    async def rpc(operation, selected):
        assert operation == "session_tree_activity" and selected == handle
        return response
    monkeypatch.setattr(client, "_session_handle_rpc", rpc)
    assert await client.session_tree_activity(handle) == response
    if invalid == "root":
        response["opencode_session_id"] = "ses_foreign"
    elif invalid == "timestamp":
        response["last_activity_at_ms"] = True
    elif invalid == "source":
        response["last_activity_session_id"] = "../../foreign"
    elif invalid == "form":
        response["pending_form"] = 0
    elif invalid == "shell":
        response["running_shell"] = 0
    else:
        response.clear()
        response["transport_lost"] = True
    with pytest.raises(RuntimeError, match="invalid session tree activity"):
        await client.session_tree_activity(handle)


@pytest.mark.parametrize("binding", ["owned", "session", "generation", "checkout"])
async def test_executor_tree_activity_recovers_only_the_bound_controller(tmp_path, monkeypatch, binding):
    import hashlib
    from mimir_cultivate.worklink import daemon_config, daemon_health, daemon_service, daemon_sessions, identities

    root = tmp_path / "daemon"
    (root / "sessions").mkdir(parents=True, mode=0o700)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    key = hashlib.sha256(f"{checkout}:2125:1".encode()).hexdigest()
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, checkout)
        controller.path.replace(root / "sessions" / f"{key}.json")
        sid = controller.record.opencode_session_id
        leaf = daemon.child(sid, updated_ms=253000000000000)
        before = len(daemon.requests)
        monkeypatch.setattr(daemon_config, "DAEMON_ROOT", str(root))
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        monkeypatch.setattr(identities, "assert_worklink_identity", lambda ids: None)
        monkeypatch.setattr(daemon_health, "load_api", lambda paths, uid: {
            "url": daemon.base_url, "username": daemon.username, "password": daemon.password})
        monkeypatch.setattr(daemon_service, "RuntimePaths", lambda: SimpleNamespace(daemon=root))
        read_json = daemon_service.read_json
        monkeypatch.setattr(daemon_service, "read_json", lambda path, **kw: (
            {"launch_generation": "generation-1"} if path.name == "launch.json" else read_json(path, **kw)))
        info = checkout.stat()
        request = {"op": "session_tree_activity", "issue": 2125, "attempt": 1,
                   "device": info.st_dev, "inode": info.st_ino + (binding == "checkout"),
                   "path": str(checkout), "generation": "other" if binding == "generation" else "generation-1",
                   "opencode_session_id": leaf if binding == "session" else sid}
        if binding == "owned":
            result = await daemon_sessions._executor_operation(request)
            assert result["last_activity_session_id"] == leaf
            assert result["last_activity_at_ms"] == 253000000000000
            assert all(method == "GET" for method, _, _, _ in daemon.requests[before:])
        else:
            with pytest.raises(ValueError, match="mismatch|identity changed"):
                await daemon_sessions._executor_operation(request)


async def test_foreign_success_is_rejected(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)

        async def foreign(*args, **kwargs):
            yield ServerEvent("message", {"type": "session.execution.succeeded", "data": {"sessionID": "ses_foreign"},
                                         "durable": {"seq": 99, "aggregateID": "ses_foreign"}})

        monkeypatch.setattr(client, "log", foreign)
        with pytest.raises(DaemonClientError, match="foreign session event"):
            await controller.observe()


async def test_success_requires_matching_user_history(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        daemon.sessions[sid].messages[:] = [m for m in daemon.sessions[sid].messages if m["type"] != "user"]
        assert (await controller.observe()).execution_state == "incomplete"


async def test_eof_reconnect_never_readmits(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        original = client.log

        async def eof(*args, **kwargs):
            for frame in ():
                yield frame

        monkeypatch.setattr(client, "log", eof)
        with pytest.raises(DaemonClientError, match="missing log watermark"):
            await controller.observe()
        monkeypatch.setattr(client, "log", original)
        daemon.finish(controller.record.opencode_session_id)
        assert (await controller.observe()).execution_state == "succeeded"
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


@pytest.mark.parametrize("name,value", [
    ("OPENAI_API_KEY", "fake"), ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=fake"),
    ("HOME", "/tmp"), ("home", "/tmp"), ("XDG_CONFIG_HOME", "/tmp"),
    ("API_URL", "http://localhost"), ("OPENCODE_SERVER_PASSWORD", "fake"),
    ("CFLAGS", "-I/home/mimir/private"), ("GIT_CONFIG_COUNT", "1"),
])
def test_session_environment_refuses_untrusted_overrides(tmp_path, name, value):
    with pytest.raises((ValueError, RuntimeError)):
        session_environment({name: value}, home=tmp_path, git_settings=(), credentials={})


def test_session_environment_is_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "fragment-" + "not-in-session")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "99")
    env = session_environment({"CFLAGS": "-O2"}, home=tmp_path, git_settings=(), credentials={})
    assert env == {
        "PATH": "/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin", "USER": "worklink",
        "LOGNAME": "worklink", "SHELL": "/bin/sh", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": str(tmp_path), "TMPDIR": str(tmp_path / "tmp"), "MIMIR_WORKLINK_GATE": "1",
        "MIMIR_SPAWN_DEPTH": "1", "CFLAGS": "-O2", "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory", "GIT_CONFIG_VALUE_0": str(tmp_path),
    }


@pytest.mark.parametrize("error_type", ["provider.auth", "provider.quota", "provider.timeout", "future.error"])
async def test_structured_errors_are_bounded_redacted_and_not_reclassified(tmp_path, error_type):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        secret = "fragment-" + "synthetic-private-value"
        controller.sensitive_values = (secret,)
        daemon.finish(controller.record.opencode_session_id, outcome="failed", error={
            "type": error_type, "message": secret + " auth quota succeeded " + "é" * 3000,
            "response": {"body": secret + "x" * 10000}, "retryAfterMs": 999999999999,
        })
        outcome = await controller.observe()
        assert outcome.execution_state == "failed"
        assert outcome.error.type == error_type
        assert secret not in repr(outcome.error)
        assert len(outcome.error.message.encode()) <= 2048
        assert len(outcome.error.body.encode()) <= 8192
        assert outcome.error.retry_after_ms is None
        assert secret.encode() not in controller.path.read_bytes()


async def test_output_bounds_and_final_text_redaction(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        secret = "runtime-" + "fragment-secret"
        controller.sensitive_values = (secret,)
        daemon.sessions[sid].messages.append({
            "id": "msg_assistant", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": secret + "\nWORKLINK_BLOCKED: needs design"}],
        })
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.stdout == "<redacted>\nWORKLINK_BLOCKED: needs design"
        assert not outcome.output_overflow
        limited = await controller.observe(stdout_limit=1)
        assert limited.output_overflow
        assert len(limited.stdout.encode()) <= 1


async def test_record_unknown_fields_and_oversize_are_rejected(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        payload = json.loads(controller.path.read_bytes())
        payload["prompt"] = "must never be persisted"
        controller.path.write_text(json.dumps(payload))
        with pytest.raises(TypeError):
            SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        controller.path.write_text(" " * 16385)
        with pytest.raises(ValueError):
            SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)


async def test_multipart_output_counts_separator_bytes(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.sessions[sid].messages.append({
            "id": "msg_output", "type": "assistant", "time": {"created": 2}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}],
        })
        daemon.finish(sid)
        outcome = await controller.observe(stdout_limit=2)
        assert outcome.output_overflow
        assert len(outcome.stdout.encode()) <= 2


@pytest.mark.parametrize("kind", ["leaf", "spawn", "epic"])
@pytest.mark.parametrize("variant", [None, "high"])
async def test_captured_per_kind_wire_contract_and_effective_permissions(tmp_path, kind, variant):
    model = {"providerID": "openai", "id": "synthetic"}
    if variant is not None:
        model["variant"] = variant
    text = "--autonomous --max-retries 2 chainlink-2125" if kind == "epic" else "rules\nrendered prompt"
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        await SessionController.create(client, tmp_path / "controller.json", request=SessionRequest(
            kind, model, text, ("git *", "pytest *") if kind == "leaf" else (),
        ), issue_id=2125, attempt=1, generation="generation-1", checkout=tmp_path,
            environment={}, timeout_s=60)
        actual = next(r[3] for r in daemon.requests if r[:2] == ("POST", "/api/session"))
        rules = [{"action": "external_directory", "resource": "*", "effect": "deny"},
                 {"action": "question", "resource": "*", "effect": "deny"}]
        if kind == "leaf":
            rules += [{"action": "shell", "resource": "*", "effect": "deny"},
                      {"action": "shell", "resource": "git *", "effect": "allow"},
                      {"action": "shell", "resource": "pytest *", "effect": "allow"}]
        elif kind == "spawn":
            rules += [{"action": "shell", "resource": "*", "effect": "allow"}]
        else:
            rules += [{"action": "read", "resource": p, "effect": "deny"} for p in (".env", "**/.env", "**/.env.*")]
        assert actual == {"location": {"directory": str(tmp_path)}, "model": model, "permissions": rules}
        inputs = [r for r in daemon.requests if r[0] == "POST" and r[1].endswith(("/prompt", "/command"))]
        assert len(inputs) == 1
        assert inputs[0][1].endswith("/command" if kind == "epic" else "/prompt")
        assert inputs[0][3] == ({"name": "feature", "text": text} if kind == "epic" else {"text": text, "resume": True})
        agent = [{"action": "*", "resource": "*", "effect": "allow"}]
        def effective(action, resource):
            return effective_permission(action, resource, agent=agent, session=actual["permissions"], project=agent)
        assert effective("external_directory", "/outside") == "deny"
        assert effective("shell", "git status") == "allow"
        assert effective("shell", "pytest -q") == "allow"
        assert effective("shell", "node factory.js status") == ("deny" if kind == "leaf" else "allow")
        if kind == "epic":
            for resource in (".env", "nested/.env", "nested/.env.secret"):
                assert effective("read", resource) == "deny"


@pytest.mark.parametrize("defect", ["aggregateID", "foreign", "seq_type", "regressed", "extra"])
async def test_malformed_watermark_is_refused(tmp_path, monkeypatch, defect):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        await controller.observe()
        watermark = {"type": "log.synced", "aggregateID": sid, "seq": controller.record.last_seq}
        if defect == "aggregateID":
            watermark.pop("aggregateID")
        elif defect == "foreign":
            watermark["aggregateID"] = "ses_foreign"
        elif defect == "seq_type":
            watermark["seq"] = True
        elif defect == "regressed":
            watermark["seq"] = controller.record.last_seq - 1
        else:
            watermark["extra"] = 1
        async def malformed(*args, **kwargs):
            yield ServerEvent("message", watermark)
        monkeypatch.setattr(client, "log", malformed)
        with pytest.raises(DaemonClientError, match="invalid log watermark"):
            await controller.observe()


async def test_log_events_cannot_forge_an_outcome(tmp_path, monkeypatch):
    """Only the message history decides execution state; log events are inert."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        forged = [{"id": "evt_forged", "created": 1, "type": "session.execution.succeeded",
                   "durable": {"aggregateID": sid, "seq": 90, "version": 1}, "data": {"sessionID": sid}}]
        async def forged_log(*args, **kwargs):
            for event in [*forged, {"type": "log.synced", "aggregateID": sid, "seq": 91}]:
                yield ServerEvent("message", event)
        monkeypatch.setattr(client, "log", forged_log)
        outcome = await controller.observe()
        assert outcome.execution_state == "started"
        assert controller.record.last_seq == 91


async def test_watermark_advances_last_seq_from_the_daemon(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        baseline = controller.record.baseline_seq
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"
        assert controller.record.baseline_seq == baseline
        assert controller.record.last_seq == len(daemon.sessions[sid].log) > baseline


@pytest.mark.parametrize("defect", ["user_text", "user_time", "idle_outcome", "inbox_payload", "inbox_session", "inbox_delivery"])
async def test_malformed_history_and_inbox_refused(tmp_path, monkeypatch, defect):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = not defect.startswith("inbox")
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        if defect.startswith("inbox"):
            original = client.inbox
            async def malformed(*args, **kwargs):
                page = deepcopy(await original(*args, **kwargs))
                item = page["data"][0]
                item.pop({"inbox_payload": "payload", "inbox_session": "sessionID", "inbox_delivery": "delivery"}[defect])
                return page
            monkeypatch.setattr(client, "inbox", malformed)
        else:
            daemon.finish(sid)
            original = client.messages
            async def malformed(*args, **kwargs):
                page = deepcopy(await original(*args, **kwargs))
                item = next(m for m in page["data"] if m["type"] == ("idle" if defect == "idle_outcome" else "user"))
                item.pop({"user_text": "text", "user_time": "time", "idle_outcome": "outcome"}[defect])
                return page
            monkeypatch.setattr(client, "messages", malformed)
        with pytest.raises(DaemonClientError):
            await controller.observe()


@pytest.mark.parametrize("error_type", ["provider.auth", "provider.quota", "future.error"])
async def test_idle_error_survives_transport_loss_and_recovery(tmp_path, monkeypatch, error_type):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        assert (await controller.observe()).execution_state == "started"
        secret = "runtime-" + "error-fragment"
        daemon.finish(sid, outcome="failed", error={"type": error_type, "message": secret + "é" * 3000,
                      "response": {"body": secret + "x" * 9000}, "retryAfterMs": 12345})
        original = client.messages
        async def disconnected(*args, **kwargs):
            raise DaemonClientError("transport failure")
        monkeypatch.setattr(client, "messages", disconnected)
        with pytest.raises(DaemonClientError, match="transport failure"):
            await controller.observe()
        assert controller.record.execution_state == "started"
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        recovered.sensitive_values = (secret,)
        monkeypatch.setattr(client, "messages", original)
        result = await recovered.observe()
        assert result.execution_state == "failed"
        assert result.error.type == error_type
        assert result.error.retry_after_ms == 12345
        assert secret not in repr(result.error)
        assert len(result.error.message.encode()) <= 2048 and len(result.error.body.encode()) <= 8192
        assert secret.encode() not in controller.path.read_bytes()
        assert sum(r[1].endswith("/prompt") for r in daemon.requests) == 1


async def test_failed_idle_without_error_still_fails_with_backend_error(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid, outcome="failed", error={"type": "provider.auth", "message": "x"})
        daemon.sessions[sid].messages[-1].pop("error")
        outcome = await controller.observe()
        assert outcome.execution_state == "failed"
        assert outcome.error.type == "backend_error"


async def test_new_execution_after_idle_reads_as_started(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"
        daemon.sessions[sid].messages.append({
            "id": "msg_resumed", "type": "assistant", "time": {"created": 3}, "agent": "build",
            "model": {"providerID": "openai", "id": "gpt-5"}, "content": []})
        assert (await controller.observe()).execution_state == "started"


async def test_vanished_input_without_stop_is_incomplete_and_with_stop_interrupted(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        assert (await controller.observe()).execution_state == "pending"
        daemon.sessions[sid].inbox.clear()
        assert (await controller.observe()).execution_state == "incomplete"
        controller._persist(stop_requested=True, cancellation_state="requested")
        assert (await controller.observe()).execution_state == "interrupted"


async def test_setup_cannot_post_after_persisted_deadline(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        original = client.set_environment
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)
        monkeypatch.setattr(client, "set_environment", slow)
        task = asyncio.create_task(create(client, tmp_path))
        # Hang guard, not a latency budget.
        await asyncio.wait_for(entered.wait(), 60)
        clock[0] += 61
        release.set()
        # Hang guard, not a latency budget; the owned clock enforces the deadline.
        controller = await asyncio.wait_for(task, 60)
        assert not any(r[1].endswith(("/prompt", "/command")) for r in daemon.requests)
        assert (await controller.observe()).error.type == "timeout"


async def test_read_crossing_deadline_cannot_return_success(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        daemon.finish(controller.record.opencode_session_id)
        original = client.inbox
        async def crossing(*args, **kwargs):
            result = await original(*args, **kwargs)
            clock[0] += 61
            return result
        monkeypatch.setattr(client, "inbox", crossing)
        outcome = await controller.observe()
        assert outcome.execution_state != "succeeded"
        assert outcome.error.type == "timeout"
        assert controller.record.stop_requested


async def test_expired_recovery_retains_unverified_work(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        recovered = SessionController.recover(client, controller.path, generation="generation-1", checkout=tmp_path)
        clock[0] += 61
        daemon.faults[("GET", f"/api/experimental/session/{controller.record.opencode_session_id}/log")].append(SyntheticFault(status=401))
        outcome = await recovered.observe()
        assert outcome.error.type == "timeout" and outcome.cancellation_state == "unverified"
        assert recovered.path.exists()
        assert daemon.sessions[controller.record.opencode_session_id].active


async def test_failed_stop_checkpoint_does_not_disable_deadline(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    clock = [1000.0]
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: clock[0]))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        original = controller._persist
        def failed_checkpoint(**changes):
            raise OSError("fixture checkpoint failure")
        monkeypatch.setattr(controller, "_persist", failed_checkpoint)
        with pytest.raises(OSError):
            await controller.cancel()
        assert not controller._cancelling
        monkeypatch.setattr(controller, "_persist", original)
        daemon.finish(controller.record.opencode_session_id)
        clock[0] += 61
        assert (await controller.observe()).error.type == "timeout"


async def test_blocked_read_uses_remaining_budget_not_http_timeout(tmp_path, monkeypatch):
    from mimir_cultivate.worklink import daemon_sessions
    # Freeze the owned wall clock while fsync/HTTP setup runs. The remaining
    # 50ms is then enforced by asyncio's monotonic timeout at the blocked read,
    # not consumed by fixture setup or checkpoint I/O under xdist contention.
    monkeypatch.setattr(daemon_sessions, "time", SimpleNamespace(time=lambda: 1000.0))
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        entered = asyncio.Event()
        original_log = controller._read_log
        async def arm_read_budget(**kwargs):
            await original_log(**kwargs)
            if not controller._cancelling:
                controller._persist(deadline_at_ms=int(daemon_sessions.time.time() * 1000) + 50)
        monkeypatch.setattr(controller, "_read_log", arm_read_budget)
        original = client.messages
        async def blocked(*args, **kwargs):
            if not controller._cancelling:
                entered.set()
                await asyncio.Event().wait()
            return await original(*args, **kwargs)
        monkeypatch.setattr(client, "messages", blocked)
        task = asyncio.create_task(controller.observe())
        # Hang guard, not a latency budget.
        await asyncio.wait_for(entered.wait(), 60)
        # Behavioural bound: stay below the client's 30-second HTTP timeout so
        # completion proves the remaining 50ms session budget interrupted the read.
        outcome = await asyncio.wait_for(task, 4)
        assert outcome.error.type == "timeout"
        assert controller.record.cancellation_state == "verified"


@pytest.mark.parametrize("parts,limit,overflow", [(["a", "b"], 3, False), (["é", "é"], 4, True), (["é", "é"], 5, False)])
async def test_utf8_multipart_output_boundaries(tmp_path, parts, limit, overflow):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.sessions[sid].messages.append({"id": "msg_parts", "type": "assistant", "time": {"created": 2},
            "agent": "build", "model": {"providerID": "openai", "id": "synthetic"},
            "content": [{"type": "text", "text": text} for text in parts]})
        daemon.finish(sid)
        outcome = await controller.observe(stdout_limit=limit)
        assert outcome.output_overflow is overflow
        assert len(outcome.stdout.encode()) <= limit
        if not overflow:
            assert outcome.stdout == "\n".join(parts)


@pytest.mark.parametrize("stop_requested", [False, True])
async def test_delivery_between_inbox_and_history_reads_is_not_removal(tmp_path, monkeypatch, stop_requested):
    """Delivery at the boundary of the two reads must read as in flight, never vanished."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.auto_deliver = False
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        if stop_requested:
            controller._persist(stop_requested=True, cancellation_state="requested")
        input_id = controller.record.input_id
        delivered = []

        def deliver_once():
            if not delivered:
                delivered.append(True)
                daemon.deliver(sid, input_id)

        for name in ("inbox", "messages"):
            original = getattr(client, name)
            async def first_read_then_deliver(*args, _original=original, **kwargs):
                result = await _original(*args, **kwargs)
                deliver_once()
                return result
            monkeypatch.setattr(client, name, first_read_then_deliver)
        assert (await controller.observe()).execution_state == "started"
        assert delivered


def _bulky(sid_messages, count, size):
    for index in range(count):
        sid_messages.append({"id": f"msg_bulk{index:04d}", "type": "assistant", "time": {"created": 2},
            "agent": "build", "model": {"providerID": "openai", "id": "gpt-5"},
            "content": [{"type": "text", "text": f"{index}:" + "x" * size}]})


async def test_history_larger_than_one_response_is_read_in_narrower_pages(tmp_path):
    """Captured 2026-11-16: 98 messages / 1.46 MB broke a 100-message page against the 1 MiB bound."""
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        _bulky(daemon.sessions[sid].messages, 40, 6 * 1024)
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.execution_state == "succeeded"
        assert outcome.stdout.count("\n") == 39 and outcome.stdout.startswith("0:")
        assert await controller.cancel(timeout_s=5)
        assert controller.record.cancellation_state == "verified"
        limits = [r[2].get("limit") for r in daemon.requests if r[1].endswith("/message")]
        assert "100" in limits and any(int(value) < 100 for value in limits if value)


async def test_narrowing_after_an_advanced_cursor_keeps_the_exact_ordered_history(tmp_path):
    """A first page fits; a later page must narrow from its non-null cursor without skipping or reordering."""
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        messages = daemon.sessions[sid].messages
        texts = [f"small-{index:03d}" for index in range(98)] + [f"large-{index:02d}:" + "y" * 6 * 1024 for index in range(20)]
        for index, text in enumerate(texts):
            messages.append({"id": f"msg_order{index:04d}", "type": "assistant", "time": {"created": 2},
                "agent": "build", "model": {"providerID": "openai", "id": "gpt-5"},
                "content": [{"type": "text", "text": text}]})
        daemon.finish(sid)
        outcome = await controller.observe()
        assert outcome.execution_state == "succeeded"
        assert outcome.stdout == "\n".join(texts)
        pages = [(r[2].get("cursor") is not None, r[2].get("limit")) for r in daemon.requests if r[1].endswith("/message")]
        # The first page (no cursor) fits at 100; narrowing happens only on cursor-bearing pages.
        assert pages[0] == (False, "100") and any(cursor and int(limit) < 100 for cursor, limit in pages)


async def test_single_message_over_the_response_bound_still_fails_closed(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, \
            OpenCodeDaemonClient(**daemon.connection, max_item_bytes=64 * 1024) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        _bulky(daemon.sessions[sid].messages, 1, 80 * 1024)
        daemon.finish(sid)
        with pytest.raises(DaemonClientError, match="item too large"):
            await controller.observe()
        assert controller.record.execution_state != "succeeded"


class _RecordedClient:
    """Serves one captured 2.2.7 snapshot at a time; no daemon, no provider."""

    def __init__(self, watermark: dict) -> None:
        self.watermark = watermark
        self.snapshot: dict = {}

    async def log(self, session_id, *, after=None, follow=True):
        yield ServerEvent("message", self.watermark)

    async def messages(self, session_id, *, cursor=None, order=None, limit=None, message_type=None):
        data = [item for page in self.snapshot["pages"] for item in page["data"]]
        return {"data": deepcopy(data), "cursor": {"previous": None, "next": None}}

    async def inbox(self, session_id):
        return deepcopy(self.snapshot["inbox"])

    async def running_shells(self, directory):
        # The capture ran no background shell (prompt 2's `sleep 60` was a foreground tool call).
        return []

    async def forms(self, session_id):
        # The historical capture predates form observation; no questions were asked.
        return []

    async def session_tree(self, session_id):
        # Both captured inputs ran on this single root session.
        return [{"id": session_id}]


def _captured_lifecycle():
    from tests.worklink_daemon_fixtures import FIXTURES
    return json.loads((FIXTURES / "opencode_2_2_7/session-lifecycle.json").read_bytes())


def test_captured_log_endpoint_carries_only_the_watermark():
    """The recorded defect: execution events reach /api/event, never the session log."""
    capture = _captured_lifecycle()
    sid = capture["session"]["data"]["id"]
    live = [frame["data"] for _, frame in capture["log_live"]]
    replay = [frame["data"] for frame in capture["log_replay"]]
    assert live == [{"type": "log.synced", "aggregateID": sid, "seq": 0}]
    assert [item["type"] for item in replay] == ["log.synced"] and replay[0]["seq"] > 0
    published = [frame["data"]["type"] for _, frame in capture["events"]]
    assert {"session.execution.started", "session.execution.succeeded",
            "session.execution.interrupted"} <= set(published)


@pytest.mark.parametrize("scenario,expected", [
    ("prompt1", {"after_prompt1": "started", "idle1": "succeeded"}),
    ("prompt2", {"busy2": "started", "after_interrupt": "interrupted"}),
])
async def test_captured_history_drives_observed_state(tmp_path, scenario, expected):
    capture = _captured_lifecycle()
    sid = capture["session"]["data"]["id"]
    snapshots = {snap["label"]: snap for snap in capture["snapshots"]}
    replay = capture["log_replay"][0]["data"]
    baseline = ([] if scenario == "prompt1" else
                [item["id"] for page in snapshots["idle1"]["pages"] for item in page["data"]])
    client = _RecordedClient(replay)
    info = tmp_path.stat()
    record = ControllerRecord(1, "00000000-0000-0000-0000-000000000000", 2125, 1, "leaf", sid,
                              "generation-1", str(tmp_path), info.st_dev, info.st_ino, 0, 0, baseline,
                              capture[scenario]["data"]["id"], "admitted", "pending", False, "none",
                              int(time.time() * 1000) + 600_000)
    controller = SessionController(client, tmp_path / "controller.json", record)
    for label, state in expected.items():
        client.snapshot = snapshots[label]
        outcome = await controller.observe()
        assert (label, outcome.execution_state) == (label, state)
        assert outcome.error is None
    assert controller.record.last_seq == replay["seq"]
    if scenario == "prompt1":
        assert outcome.stdout.strip() == "ok"


async def test_session_exists_is_true_for_a_known_session_and_false_only_on_404(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        assert await client.session_exists(controller.record.opencode_session_id)
        assert not await client.session_exists("ses_gone")
        daemon.faults[("GET", "/api/session/ses_broken")].append(SyntheticFault(status=500))
        with pytest.raises(DaemonClientError):
            await client.session_exists("ses_broken")


@pytest.mark.parametrize("case,expected", [
    ("orphaned", True), ("same_generation", False), ("session_present", False),
    ("record_present", False), ("retired_present", False), ("no_current_generation", False),
])
async def test_ended_with_previous_daemon_requires_every_proof(tmp_path, case, expected):
    from mimir_cultivate.worklink.daemon_sessions import _ended_with_previous_daemon

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        path = tmp_path / "controller.json"
        sid = "ses_previous"
        if case == "session_present":
            sid = (await client.create_session(directory=str(tmp_path), model={"providerID": "openai", "id": "x"},
                                               permissions=[]))["data"]["id"]
        if case == "record_present":
            path.write_text("{}")
        if case == "retired_present":
            path.with_name(f"{path.stem}.{sid}.retired.json").write_text("{}")
        current = {"same_generation": "generation-old", "no_current_generation": None}.get(case, "generation-new")
        assert await _ended_with_previous_daemon(
            client, path, session_id=sid, request_generation="generation-old",
            current_generation=current) is expected


async def test_ended_with_previous_daemon_fails_closed_on_daemon_error(tmp_path):
    from mimir_cultivate.worklink.daemon_sessions import _ended_with_previous_daemon

    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        daemon.faults[("GET", "/api/session/ses_previous")].append(SyntheticFault(status=500))
        with pytest.raises(DaemonClientError):
            await _ended_with_previous_daemon(client, tmp_path / "controller.json", session_id="ses_previous",
                                              request_generation="generation-old", current_generation="generation-new")


@pytest.mark.parametrize("op", ["session_observe", "session_cancel", "session_release", "session_tree_activity"])
@pytest.mark.parametrize("restarted", [True, False])
async def test_executor_reports_a_session_that_ended_with_its_daemon(tmp_path, monkeypatch, op, restarted):
    """2026-11-16: every retained leaf failed closed forever after a container restart wiped its record."""
    from mimir_cultivate.worklink import daemon_config, daemon_health, daemon_service, daemon_sessions, identities

    async with FakeOpenCodeDaemon() as daemon:
        root = tmp_path / "daemon"
        (root / "sessions").mkdir(parents=True)
        checkout = (tmp_path / "checkout").resolve()
        checkout.mkdir()
        info = checkout.stat()
        launch = {"launch_generation": "generation-new" if restarted else "generation-old"}
        monkeypatch.setattr(daemon_config, "DAEMON_ROOT", str(root))
        monkeypatch.setattr(identities, "get_identities", lambda: SimpleNamespace(worklink_uid=os.geteuid()))
        monkeypatch.setattr(identities, "assert_worklink_identity", lambda ids: None)
        monkeypatch.setattr(daemon_health, "load_api", lambda paths, uid: {
            "url": daemon.base_url, "username": daemon.username, "password": daemon.password})
        monkeypatch.setattr(daemon_service, "RuntimePaths", lambda: SimpleNamespace(daemon=root))
        def read_json(path, uid, mode):
            if path.name == "launch.json":
                return launch
            raise FileNotFoundError(path)
        monkeypatch.setattr(daemon_service, "read_json", read_json)
        import hashlib
        home = root / "sessions" / (hashlib.sha256(f"{checkout}:2151:2".encode()).hexdigest() + ".home")
        (home / "tmp").mkdir(parents=True)
        (home / "tmp" / "sentinel").write_text("attempt scratch")
        (checkout / "work.txt").write_text("retained work")
        output = tmp_path / "stdout"
        output.write_bytes(b"stale")
        fd = os.open(output, os.O_RDWR)
        monkeypatch.setenv("WORKLINK_SESSION_OUTPUT_FD", str(fd))
        request = {"op": op, "issue": 2151, "attempt": 2, "device": info.st_dev, "inode": info.st_ino,
                   "path": str(checkout), "generation": "generation-old", "opencode_session_id": "ses_gone",
                   **({"stdout_limit": 1024} if op == "session_observe" else {})}
        try:
            if not restarted:
                with pytest.raises(FileNotFoundError):
                    await daemon_sessions._executor_operation(request)
                return
            if op == "session_tree_activity":
                with pytest.raises(ValueError, match="activity unavailable after daemon replacement"):
                    await daemon_sessions._executor_operation(request)
                return
            response = await daemon_sessions._executor_operation(request)
        finally:
            os.close(fd)
        if op == "session_observe":
            assert response["execution_state"] == "interrupted"
            assert response["cancellation_state"] == "verified"
            assert response["error_detail"]["type"] == "daemon_restarted"
            assert output.read_bytes() == b""
        elif op == "session_cancel":
            assert response == {"cancellation_state": "verified"}
        else:
            assert response == {"released": True}
            assert not home.exists()
        assert (checkout / "work.txt").read_text() == "retained work"
        if op != "session_release":
            assert (home / "tmp" / "sentinel").exists()


async def test_idle_with_an_owned_background_shell_is_not_quiescent(tmp_path):
    """2026-11-16: #2149 went idle, its background suite finished, and the session resumed under the stop."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "started"
        daemon.end_shell(shell)
        assert (await controller.observe()).execution_state == "succeeded"


async def test_another_sessions_shell_does_not_hold_this_session(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.start_shell("ses_someone_else", str(tmp_path))
        daemon.start_shell(sid, str(tmp_path / "elsewhere"))
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"


async def test_cancel_stops_owned_background_shells_before_verifying(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        owned = daemon.start_shell(sid, str(tmp_path))
        foreign = daemon.start_shell("ses_someone_else", str(tmp_path))
        daemon.finish(sid)
        assert await controller.cancel(timeout_s=5)
        assert controller.record.cancellation_state == "verified"
        assert daemon.shells[owned]["status"] == "killed"
        assert daemon.shells[foreign]["status"] == "running"


async def test_shell_listing_failure_fails_closed(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        daemon.finish(controller.record.opencode_session_id)
        daemon.faults[("GET", "/api/shell")].append(SyntheticFault(status=500))
        with pytest.raises(DaemonClientError):
            await controller.observe()
        assert controller.record.execution_state != "succeeded"
        daemon.faults[("GET", "/api/shell")].extend(SyntheticFault(status=500) for _ in range(500))
        assert not await controller.cancel(timeout_s=1)
        assert controller.record.cancellation_state == "unverified"


async def test_shell_that_resumes_the_session_between_reads_is_not_success(tmp_path, monkeypatch):
    """Review P1 on #105: history (idle) -> shell exits and resumes the session -> empty shell list."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        original = client.running_shells
        async def finish_and_resume(directory):
            if daemon.shells[shell]["status"] == "running":
                daemon.end_shell(shell)
                daemon.sessions[sid].messages.append({
                    "id": "msg_resumed", "type": "assistant", "time": {"created": 3}, "agent": "build",
                    "model": {"providerID": "openai", "id": "gpt-5"}, "content": []})
            return await original(directory)
        monkeypatch.setattr(client, "running_shells", finish_and_resume)
        assert (await controller.observe()).execution_state == "started"


async def test_a_new_idle_after_the_empty_shell_listing_is_not_success(tmp_path, monkeypatch):
    """Review P1 on #105: idle A (S1 running) -> S1 exits, listing empty -> resumed turn starts S2, idle B."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        messages = daemon.sessions[sid].messages
        first = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        second = []
        original_shells, original_messages = client.running_shells, client.messages
        async def first_shell_exits(directory):
            if daemon.shells[first]["status"] == "running":
                daemon.end_shell(first)
            return await original_shells(directory)
        reads = []
        async def resumed_before_reread(*args, **kwargs):
            reads.append(1)
            if daemon.shells[first]["status"] != "running" and not second:
                # Between the empty listing and the re-read: resume, start S2, reach idle B.
                messages.append({"id": "msg_resumed", "type": "assistant", "time": {"created": 3},
                                 "agent": "build", "model": {"providerID": "openai", "id": "gpt-5"}, "content": []})
                second.append(daemon.start_shell(sid, str(tmp_path)))
                messages.append({"id": "msg_idle_b", "type": "idle", "time": {"created": 4}, "outcome": "succeeded"})
            return await original_messages(*args, **kwargs)
        monkeypatch.setattr(client, "running_shells", first_shell_exits)
        monkeypatch.setattr(client, "messages", resumed_before_reread)
        assert (await controller.observe()).execution_state == "started"
        # S2 is visible on the next refresh and still holds the session.
        assert (await controller.observe()).execution_state == "started"
        daemon.end_shell(second[0])
        assert (await controller.observe()).execution_state == "succeeded"


@pytest.mark.parametrize("raised,expected", [
    (DaemonClientError("invalid shell entry"), {"class": "DaemonClientError", "reason": "invalid shell entry"}),
    (DaemonClientError("HTTP failure", status=404), {"class": "DaemonClientError", "reason": "HTTP failure", "status": 404}),
    (ValueError("token=" + "fragment-secret"), {"class": "ValueError"}),
])
def test_helper_failure_reports_only_class_and_code_literal_reason(monkeypatch, capsys, raised, expected):
    import io
    from mimir_cultivate.worklink import daemon_sessions

    async def fail(request):
        raise raised
    monkeypatch.setattr(daemon_sessions, "_executor_operation", fail)
    monkeypatch.setattr(daemon_sessions.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"{}")))
    assert daemon_sessions.main() == 1
    out = capsys.readouterr().out
    assert json.loads(out) == {"failure": expected}
    assert "fragment-secret" not in out


async def test_a_finished_shells_queued_resume_is_valid_inbox_and_not_quiescent(tmp_path):
    """2026-11-17: #2151 attempt 5 failed "invalid session inbox" three minutes in.

    Its background suite finished, the daemon queued the 2.2.7 ``synthetic`` resume,
    and the observe read the inbox before the daemon admitted it (history then showed
    that ``synthetic`` message at 08:13:12, after the 08:13:08 failure).
    """
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        daemon.end_shell(shell, resume=True)
        assert [item["type"] for item in daemon.sessions[sid].inbox] == ["synthetic"]
        assert (await controller.observe()).execution_state == "started"
        assert controller.record.execution_state == "started"
        resume = daemon.sessions[sid].inbox[0]["id"]
        daemon.deliver(sid, resume)
        assert (await controller.observe()).execution_state == "started"
        daemon.finish(sid)
        assert (await controller.observe()).execution_state == "succeeded"
        assert controller.record.admission_state == "admitted"


def _daemon_item(sid: str, kind: str, payload: object) -> dict:
    return {"id": f"msg_{kind}_queued", "sessionID": sid, "time": {"created": 3}, "type": kind,
            "payload": payload, "delivery": "queue"}


@pytest.mark.parametrize("kind,payload", [
    ("synthetic", {"text": "resume"}),
    ("compaction", {"auto": True}),
    ("move", {"projectID": "prj_x", "location": {"directory": "/workspace/x"}}),
])
async def test_each_documented_daemon_inbox_item_holds_the_session_open(tmp_path, kind, payload):
    """``Session.Inbox.Info`` (OpenCode 2.3.0) is user | synthetic | compaction | move."""
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        daemon.sessions[sid].inbox.append(_daemon_item(sid, kind, payload))
        assert (await controller.observe()).execution_state == "started"
        daemon.sessions[sid].inbox.clear()
        assert (await controller.observe()).execution_state == "succeeded"


@pytest.mark.parametrize("item", [
    {"type": "unknown", "payload": {"text": "x"}},
    {"type": "synthetic", "payload": {"text": 7}},
    {"type": "synthetic", "payload": {"text": "x", "extra": 1}},
    {"type": "synthetic", "payload": "resume"},
    {"type": "compaction", "payload": None},
    {"type": "move", "payload": {"projectID": "prj_x"}},
    {"type": "synthetic", "payload": {"text": "x"}, "sessionID": "ses_someone_else"},
    {"type": "synthetic", "payload": {"text": "x"}, "delivery": "later"},
])
async def test_malformed_inbox_items_still_fail_closed(tmp_path, monkeypatch, item):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        daemon.finish(sid)
        original = client.inbox
        async def malformed(*args, **kwargs):
            # Past the fake's own schema check: the client's validation is under test.
            page = deepcopy(await original(*args, **kwargs))
            page["data"].append({**_daemon_item(sid, item["type"], item["payload"]), **item})
            return page
        monkeypatch.setattr(client, "inbox", malformed)
        with pytest.raises(DaemonClientError, match="invalid session inbox"):
            await controller.observe()
        assert controller.record.execution_state != "succeeded"


async def test_daemon_inbox_items_are_never_owned_input(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        input_id = controller.record.input_id
        daemon.sessions[sid].inbox.append(_daemon_item(sid, "synthetic", {"text": "resume"}))
        await controller.observe()
        assert controller.record.input_id == input_id


async def test_cancel_removes_the_resume_its_own_shell_queued(tmp_path):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        daemon.end_shell(shell, resume=True)
        assert await controller.cancel(timeout_s=5)
        assert controller.record.cancellation_state == "verified"
        assert daemon.sessions[sid].inbox == []
        assert not daemon.sessions[sid].active


def _resume_queued_during_shell_listing(daemon, client, monkeypatch, shell):
    """Review P1 on #109: inbox read (empty) -> history (idle) -> shell finishes and
    queues its resume -> empty shell listing -> history re-read (same idle)."""
    original = client.running_shells
    async def finishes_while_listing(directory):
        if daemon.shells[shell]["status"] == "running":
            daemon.end_shell(shell, resume=True)
        return await original(directory)
    monkeypatch.setattr(client, "running_shells", finishes_while_listing)


async def test_resume_queued_between_inbox_read_and_empty_shell_listing_is_not_success(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        _resume_queued_during_shell_listing(daemon, client, monkeypatch, shell)
        assert (await controller.observe()).execution_state == "started"
        assert [item["type"] for item in daemon.sessions[sid].inbox] == ["synthetic"]


async def test_cancel_cannot_verify_past_a_resume_queued_during_the_shell_listing(tmp_path, monkeypatch):
    async with FakeOpenCodeDaemon() as daemon, OpenCodeDaemonClient(**daemon.connection) as client:
        controller = await create(client, tmp_path)
        sid = controller.record.opencode_session_id
        shell = daemon.start_shell(sid, str(tmp_path))
        daemon.finish(sid)
        _resume_queued_during_shell_listing(daemon, client, monkeypatch, shell)
        assert await controller.cancel(timeout_s=5)
        assert controller.record.cancellation_state == "verified"
        assert daemon.sessions[sid].inbox == []
        assert not daemon.sessions[sid].active
