"""Operator-owned tuning must not widen setup or leaf retry authority."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import subprocess

import pytest
import yaml

pytest.importorskip("mimir_cultivate")

from mimir.cli import main
from mimir_cultivate import commands
from mimir_cultivate.worklink.backends.registry import BackendRegistry, WorklinkConfig
from mimir_cultivate.worklink.claims import ATTEMPT_GRANT_PREFIX, ChainlinkClaims, ClaimRecord
from mimir_cultivate.worklink.factory_state import factory_issue_resource_lock


def write_config(tmp_path, overlay, base=None):
    path = tmp_path / "worklink.yaml"
    path.write_text(yaml.safe_dump(base or {}))
    (tmp_path / "worklink.local.yaml").write_text(yaml.safe_dump(overlay))
    return path


@pytest.mark.parametrize("key,value", [
    ("timeout_s", 3000), ("reaper_ttl_s", 7200), ("max_claim_attempts", 7),
    ("ci_wait_timeout_s", 7200), ("max_concurrent", 4),
])
def test_overlay_allowed_defaults(tmp_path, key, value):
    path = write_config(tmp_path, {"defaults": {key: value}})
    config = WorklinkConfig.load(path)
    assert getattr(config.defaults, key) == value
    assert config.operator_overrides == {f"defaults.{key}": value}
    assert yaml.safe_load(path.read_text()) == {}


@pytest.mark.parametrize("overlay,key", [
    ({"repository": "evil/repo"}, "repository"),
    ({"routes": []}, "routes"),
    ({"arbitrary": {}}, "arbitrary"),
    ({"defaults": {"allow_autonomous_local_subprocess": True}}, "defaults.allow_autonomous_local_subprocess"),
    ({"defaults": {"allow_autonomous_local_subprocess": 1}}, "defaults.allow_autonomous_local_subprocess"),
    ({"defaults": {"max_ci_repair_rounds": 9}}, "defaults.max_ci_repair_rounds"),
    ({"defaults": {"backend": "claude_code"}}, "defaults.backend"),
    ({"defaults": {"test_command": "curl x"}}, "defaults.test_command"),
    ({"backends": {"opencode": {"bash_allowlist": ["*"]}}}, "backends.opencode.bash_allowlist"),
    ({"backends": {"opencode": {"args": ["--dangerous"]}}}, "backends.opencode.args"),
    ({"backends": {"unknown": {"bash_allowlist_extra": []}}}, "backends.unknown"),
    ({"backends": {"feature_factory": {"bash_allowlist_extra": []}}}, "backends.feature_factory"),
])
def test_overlay_unknown_key(tmp_path, overlay, key):
    path = write_config(tmp_path, overlay)
    with pytest.raises(ValueError) as exc:
        WorklinkConfig.load(path)
    assert "worklink.local.yaml" in str(exc.value)
    assert "unknown overlay key" in str(exc.value)
    assert key in str(exc.value)


@pytest.mark.parametrize("overlay,base", [
    ({"timeout_s": 5000}, {"defaults": {"reaper_ttl_s": 7200}}),
    ({"reaper_ttl_s": 3500}, {}),
    ({"reaper_ttl_s": 90000}, {"defaults": {"factory_resumable_retention_s": 86400}}),
])
def test_overlay_post_merge_invariants(tmp_path, overlay, base):
    path = write_config(tmp_path, {"defaults": overlay}, base)
    with pytest.raises(ValueError, match="worklink.local.yaml.*reaper_ttl_s"):
        WorklinkConfig.load(path)


@pytest.mark.parametrize("value", [0, -1, True, "3000", 1.5, None])
def test_overlay_rejects_invalid_default(tmp_path, value):
    path = write_config(tmp_path, {"defaults": {"max_claim_attempts": value}})
    with pytest.raises(ValueError, match="worklink.local.yaml.*positive integer"):
        WorklinkConfig.load(path)


@pytest.mark.parametrize("name", ["opencode", "claude_code"])
@pytest.mark.parametrize("explicit", [False, True])
def test_overlay_allowlist_appends(tmp_path, name, explicit):
    base = {"backends": {name: {"bash_allowlist": ["uv *", "git *", "npm *"]}}} if explicit else {}
    path = write_config(tmp_path, {"backends": {name: {"bash_allowlist_extra": ["cargo *"]}}}, base)
    config = WorklinkConfig.load(path)
    expected = (["uv *", "git *", "npm *"] if explicit else
                ["git *", "uv *"] if name == "opencode" else ["Bash(git:*)", "Bash(uv:*)"])
    assert config.backend_settings[name]["bash_allowlist"] == [*expected, "cargo *"]
    backend = BackendRegistry(config).get(name)
    assert list(backend.bash_allowlist) == [*expected, "cargo *"]
    assert config.operator_overrides[f"backends.{name}.bash_allowlist_extra"] == [*expected, "cargo *"]


@pytest.mark.parametrize("name,extra", [
    ("opencode", ["*"]), ("opencode", [""]), ("opencode", [False]),
    ("opencode", "npm *"), ("opencode", "npm"), ("opencode", None),
    ("claude_code", ["curl *"]), ("claude_code", ["Bash(*:*)"]),
])
def test_overlay_allowlist_validated(tmp_path, name, extra):
    path = write_config(tmp_path, {"backends": {name: {"bash_allowlist_extra": extra}}})
    with pytest.raises(ValueError, match="worklink.local.yaml"):
        WorklinkConfig.load(path)


@pytest.mark.parametrize("overlay", [[], False, {"defaults": []}, {"backends": []},
                                         {"backends": {"opencode": []}}])
def test_overlay_requires_mappings(tmp_path, overlay):
    path = write_config(tmp_path, overlay)
    with pytest.raises(ValueError, match="worklink.local.yaml.*mapping"):
        WorklinkConfig.load(path)


def test_overlay_yaml_error_named(tmp_path):
    path = write_config(tmp_path, {})
    (tmp_path / "worklink.local.yaml").write_text("defaults: [")
    with pytest.raises(ValueError, match="worklink.local.yaml"):
        WorklinkConfig.load(path)


def test_overlay_without_generated_file(tmp_path):
    path = write_config(tmp_path, {"defaults": {"max_concurrent": 5}})
    path.unlink()
    assert WorklinkConfig.load(path).defaults.max_concurrent == 5


def test_overlay_absent_preserves_config(tmp_path):
    path = tmp_path / "worklink.yaml"
    assert WorklinkConfig.load(path) == WorklinkConfig()
    path.write_text("defaults:\n  timeout_s: 6000\n  reaper_ttl_s: 3600\n")
    assert WorklinkConfig.load(path).defaults.reaper_ttl_s == 12000  # legacy compatibility


def test_overlay_empty_backend_section_is_noop(tmp_path):
    path = write_config(tmp_path, {"backends": {"opencode": {}}})
    assert WorklinkConfig.load(path) == WorklinkConfig()


def test_overlay_config_describe_cli(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("MIMIR_PLUGINS", "cultivate")
    write_config(tmp_path, {
        "defaults": {"max_concurrent": 7},
        "backends": {"opencode": {"bash_allowlist_extra": ["npm *"]}},
    })
    with pytest.raises(SystemExit) as exc:
        main(["worklink", "config-describe", "--home", str(tmp_path)])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "defaults.max_concurrent = 7 (from worklink.local.yaml)" in out
    assert 'backends.opencode.bash_allowlist_extra = ["git *", "uv *", "npm *"] (from worklink.local.yaml)' in out


@pytest.fixture
def grant_env(tmp_path, monkeypatch):
    now = datetime(2026, 11, 26, tzinfo=UTC)
    comments = {1: [
        ClaimRecord(1, i, "worker", now - timedelta(days=1)).to_comment() for i in range(1, 4)
    ], 2: [
        ClaimRecord(2, i, "worker", now - timedelta(days=1)).to_comment() for i in range(1, 4)
    ]}
    labels = {1: [], 2: []}
    locks, calls = {}, []

    def runner(args):
        calls.append(args)
        stdout = ""
        if args[1:3] == ["locks", "list"]:
            stdout = json.dumps({"locks": locks})
        elif args[1:3] == ["issue", "show"]:
            issue_id = int(args[3])
            stdout = json.dumps({"labels": labels[issue_id], "comments": comments[issue_id]})
        elif args[1:3] == ["issue", "comment"]:
            comments[int(args[3])].append(args[4])
        return subprocess.CompletedProcess(args, 0, stdout, "")

    from mimir_cultivate.worklink import control
    monkeypatch.setattr(control, "_runner", lambda home: runner)
    monkeypatch.setenv("MIMIR_PLUGINS", "cultivate")
    claims = ChainlinkClaims(agent_id="operator", home_path=tmp_path, runner=runner, clock=lambda: now)
    return claims, comments, labels, locks, calls


def grant_cli(home, issue=1, reason="Infrastructure fault"):
    return commands._grant_attempt(argparse.Namespace(home=home, issue_id=issue, reason=reason))


def test_grant_attempt_exhausted_once_accumulates_scoped_and_audited(tmp_path, grant_env):
    claims, comments, _, _, _ = grant_env
    assert grant_cli(tmp_path) == 0
    grant = json.loads(next(c for c in comments[1] if c.startswith(ATTEMPT_GRANT_PREFIX))[len(ATTEMPT_GRANT_PREFIX):])
    events = [json.loads(line) for line in (tmp_path / "logs/events.jsonl").read_text().splitlines()]
    assert any(e["type"] == "worklink_attempt_granted" and all(e[k] == v for k, v in grant.items()) for e in events)
    assert grant["reason"] == "Infrastructure fault"
    assert grant_cli(tmp_path) == 1  # Cannot stockpile while the granted attempt remains.
    # A new reader sees the durable credit, but another leaf remains exhausted.
    assert claims.max_attempts == 3
    assert claims.claim_issue(2, comments[2]).attempts_exhausted
    stale_comments = list(comments[1])
    result = claims.claim_issue(1, comments[1])
    assert result.claimed and result.record.attempt == 4
    assert result.record.budget_attempt == 4
    assert claims.claim_issue(1, stale_comments).attempts_exhausted
    assert claims.claim_issue(1, comments[1]).attempts_exhausted
    assert grant_cli(tmp_path, reason="Another infrastructure fault") == 0
    assert claims.claim_issue(1, comments[1]).claimed
    assert claims.claim_issue(1, comments[1]).attempts_exhausted
    saved = json.loads((tmp_path / "state/worklink/attempt-grants/1.json").read_text())
    assert saved["attempts"] == [4, 5]
    assert WorklinkConfig.load(tmp_path / "worklink.yaml").defaults.max_claim_attempts == 3


def test_grant_attempt_refuses_with_attempts_left(tmp_path, grant_env, capsys):
    _, comments, _, _, _ = grant_env
    comments[1].pop()
    assert grant_cli(tmp_path) == 1
    assert "attempts left" in capsys.readouterr().err
    assert not (tmp_path / "state/worklink/attempt-grants/1.json").exists()
    assert not any(c.startswith(ATTEMPT_GRANT_PREFIX) for c in comments[1])


def test_grant_attempt_history_already_over_limit(tmp_path, grant_env):
    claims, comments, _, _, _ = grant_env
    comments[1].append(ClaimRecord(1, 4, "worker", claims.clock()).to_comment())
    assert grant_cli(tmp_path) == 0
    assert claims.claim_issue(1, comments[1]).claimed
    assert claims.claim_issue(1, comments[1]).attempts_exhausted


def test_grant_attempt_cli_dispatch(tmp_path, grant_env, capsys):
    with pytest.raises(SystemExit) as exc:
        main(["worklink", "grant-attempt", "1", "--reason", "Infrastructure fault", "--home", str(tmp_path)])
    assert exc.value.code == 0
    assert "one extra attempt granted" in capsys.readouterr().out


def test_grant_attempt_requires_operator_storage_and_audit(tmp_path, grant_env):
    claims, _, _, _, _ = grant_env
    with pytest.raises(ValueError, match="durable event logger"):
        claims.grant_attempt(1, reason="fault")
    claims.event_logger = lambda *args, **kwargs: None
    claims.home_path = None
    with pytest.raises(ValueError, match="operator home"):
        claims.grant_attempt(1, reason="fault")


def test_grant_attempt_invalid_issue_id(tmp_path, grant_env):
    assert grant_cli(tmp_path, issue=0) == 1


@pytest.mark.parametrize("reason", ["", " \n "])
def test_grant_attempt_requires_reason(tmp_path, grant_env, reason):
    assert grant_cli(tmp_path, reason=reason) == 1
    assert not (tmp_path / "state/worklink/attempt-grants/1.json").exists()


@pytest.mark.parametrize("label", ["worklink:in-progress", "worklink:epic"])
def test_grant_attempt_refuses_running_or_epic(tmp_path, grant_env, label):
    _, _, labels, _, _ = grant_env
    labels[1].append(label)
    assert grant_cli(tmp_path) == 1


def test_grant_attempt_refuses_active_claim(tmp_path, grant_env):
    _, _, _, locks, _ = grant_env
    locks["1"] = {"issue_id": 1, "agent_id": "worker"}
    assert grant_cli(tmp_path) == 1


def test_grant_attempt_refuses_live_controller(tmp_path, grant_env):
    with factory_issue_resource_lock(tmp_path, 1) as acquired:
        assert acquired
        assert grant_cli(tmp_path) == 1


def test_grant_attempt_uses_effective_overlay_limit(tmp_path, grant_env):
    write_config(tmp_path, {"defaults": {"max_claim_attempts": 4}})
    assert grant_cli(tmp_path) == 1


@pytest.mark.parametrize("target", ["comments", "labels", "locks", "comment", "event"])
def test_grant_attempt_audit_and_reads_fail_closed(tmp_path, grant_env, monkeypatch, target):
    claims, comments, _, _, _ = grant_env
    from mimir_cultivate.worklink import control
    original = claims.runner
    shows = 0

    def runner(args):
        nonlocal shows
        fail = False
        if args[1:3] == ["issue", "show"]:
            shows += 1
            fail = (target == "comments" and shows == 1) or (target == "labels" and shows == 2)
        fail |= target == "locks" and args[1:3] == ["locks", "list"]
        fail |= target == "comment" and args[1:3] == ["issue", "comment"]
        return subprocess.CompletedProcess(args, 1, "", "unavailable") if fail else original(args)

    def event_failed(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(control, "_runner", lambda home: runner)
    if target == "event":
        monkeypatch.setattr(commands, "log_durable_event_sync", event_failed)
    assert grant_cli(tmp_path) == 1
    assert not (tmp_path / "state/worklink/attempt-grants/1.json").exists()
    assert claims.attempts_used(comments[1]) == 3


def test_grant_attempt_comment_alone_has_no_authority(tmp_path, grant_env):
    claims, comments, _, _, _ = grant_env
    comments[1].append(ATTEMPT_GRANT_PREFIX + json.dumps({"issue_id": 1, "count": 100}))
    assert claims.claim_issue(1, comments[1]).attempts_exhausted


def test_grant_attempt_no_home_has_no_credit(grant_env):
    claims, comments, _, _, _ = grant_env
    claims.home_path = None
    assert claims.claim_issue(1, comments[1]).attempts_exhausted


def test_grant_attempt_claim_refresh_fails_closed(tmp_path, grant_env):
    claims, comments, _, _, calls = grant_env
    assert grant_cli(tmp_path) == 0
    original = claims.runner

    def runner(args):
        if args[1:3] == ["issue", "show"]:
            return subprocess.CompletedProcess(args, 1, "", "history unavailable")
        return original(args)

    claims.runner = runner
    with pytest.raises(RuntimeError, match="reading comments"):
        claims.claim_issue(1, list(comments[1]))
    assert ["chainlink", "locks", "release", "1"] in calls


def test_grant_attempt_reset_does_not_reuse_old_credit(tmp_path, grant_env):
    claims, comments, _, _, _ = grant_env
    assert grant_cli(tmp_path) == 0
    comments[1].append('WORKLINK_CLAIM_RESET {"reason":"new generation"}')
    for _ in range(3):
        assert claims.claim_issue(1, comments[1]).claimed
    assert claims.claim_issue(1, comments[1]).attempts_exhausted


@pytest.mark.parametrize("change", [
    {"issue_id": 2}, {"issue_id": True}, {"generation": -1}, {"generation": True},
    {"attempts": -1}, {"attempts": [True]}, {"attempts": [0]},
])
def test_grant_attempt_invalid_record_fails_closed(tmp_path, grant_env, change):
    claims, comments, _, _, _ = grant_env
    path = tmp_path / "state/worklink/attempt-grants/1.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"issue_id": 1, "generation": 0, "attempts": [4], **change}))
    with pytest.raises(ValueError, match="attempt grant record"):
        claims.claim_issue(1, comments[1])


def test_grant_attempt_parser_requires_reason(tmp_path):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    commands.add_argparse(sub)
    with pytest.raises(SystemExit):
        parser.parse_args(["worklink", "grant-attempt", "1"])
