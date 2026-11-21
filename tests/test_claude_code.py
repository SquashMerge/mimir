from __future__ import annotations

import pytest
import hashlib
import json
from pathlib import Path

pytest.importorskip("mimir_cultivate")

from mimir_cultivate.claude_code import (
    bash_rules, leaf_argv, leaf_environment, parse_stream, resolve_model, secret_scrubber,
)

FIXTURES = Path(__file__).parent / "fixtures/worklink/claude_code_2_1_326"


def success() -> str:
    return (FIXTURES / "success.jsonl").read_text()


def terminal(**changes) -> str:
    event = json.loads(success().splitlines()[-1])
    event.update(changes)
    return json.dumps(event) + "\n"


def test_claude_leaf_rejects_epic_credentials() -> None:
    token = "subscription-" + "test-material"
    with pytest.raises(ValueError, match="epic credentials"):
        leaf_environment({"CLAUDE_CODE_OAUTH_TOKEN": token, "GH_TOKEN": "not-a-credential"}, ("GH_TOKEN",))


@pytest.mark.parametrize("name", ["GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY", "GH_TOKEN"])
def test_claude_leaf_refuses_explicit_publishing_environment(name):
    with pytest.raises(ValueError, match="epic credentials"):
        leaf_environment({"CLAUDE_CODE_OAUTH_TOKEN": "test-" + "oauth", name: "test"}, (name,))


@pytest.mark.parametrize("name", ["HOME", "CLAUDE_CONFIG_DIR", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
                                  "OPENAI_API_KEY", "CODEX_HOME", "OPENCODE_CONFIG", "GIT_CONFIG_COUNT"])
def test_claude_leaf_refuses_auth_and_configuration_override(name):
    with pytest.raises(ValueError, match="configuration"):
        leaf_environment({"CLAUDE_CODE_OAUTH_TOKEN": "test-" + "oauth", name: "test"}, (name,))


@pytest.mark.parametrize("token", [None, "", " ", "\t", "a\nb", "a\x00b"])
def test_claude_leaf_requires_explicit_nonblank_oauth(token):
    with pytest.raises(ValueError, match="setup-token"):
        leaf_environment({"CLAUDE_CODE_OAUTH_TOKEN": token, "ANTHROPIC_API_KEY": "irrelevant"})


@pytest.mark.parametrize("spec,explicit,expected", [
    (None, None, "sonnet"), ("openai:gpt-test", None, "sonnet"),
    ("claude-code:opus", None, "opus"), ("claude-code:opus", "sonnet", "sonnet"),
])
def test_model_precedence(spec, explicit, expected):
    assert resolve_model(spec, explicit) == expected


@pytest.mark.parametrize("model", ["", "-sonnet", "a\x00b", "a\nb", "a b", "x" * 257])
def test_model_refuses_invalid_spec_even_with_override(model):
    with pytest.raises(ValueError):
        resolve_model("claude-code:" + model, "sonnet")
    with pytest.raises(ValueError):
        resolve_model(None, model)


def test_leaf_argv_is_narrow_official_cli():
    rules = bash_rules("uv run pytest -q")
    assert rules == ("Bash(git:*)", "Bash(uv:*)")
    argv = leaf_argv("build", "sonnet", Path("/worker/home"), rules)
    # Keep the empty settings-source value as its own argv element. The pinned
    # 2.1.326 hostile-project probe and both positive controls are recorded in
    # fixtures/worklink/claude_code_2_1_326/PROVENANCE.md; rerun before a CLI bump.
    assert argv == (
        "/opt/mimir-claude/bin/claude", "-p", "build", "--output-format", "stream-json", "--verbose",
        "--model", "sonnet", "--setting-sources", "", "--settings", "/worker/home/.claude/settings.json",
        "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--permission-mode", "dontAsk",
        "--permission-prompts", "none", "--tools", "Read,Write,Edit,Glob,Grep,Bash", "--allowedTools",
        "Read,Write,Edit,Glob,Grep,Bash(git:*),Bash(uv:*)", "--disable-slash-commands", "--no-session-persistence",
    )


@pytest.mark.parametrize("command", ["uv run pytest; whoami", "uv run pytest && whoami", "uv run $(whoami)",
                                         "uv run `whoami`", "uv run pytest\nwhoami", "sh -c true", "", "uv run *.py"])
def test_allowlist_refuses_shell_evaluation(command):
    with pytest.raises(ValueError):
        bash_rules(command)


@pytest.mark.parametrize("rules", [["*"], ["Bash(*)"], ["Bash(uv:*)", "Bash(:*)"], ["uv ?"],
                                       ["uv run*"], ["Bash(uv;whoami:*)"], ["Bash(git:*)"], ["Bash(sh:*)"]])
def test_allowlist_refuses_broad_unsupported_or_uncovered_rules(rules):
    with pytest.raises(ValueError):
        bash_rules("uv run pytest", rules)


def test_allowlist_preserves_prefix_grammar_and_proves_argument_boundary():
    assert bash_rules("uv run pytest -q", ["Bash(git:*)", "Bash(uv run:*)"]) == ("Bash(git:*)", "Bash(uv run:*)")
    with pytest.raises(ValueError, match="cover"):
        bash_rules("uv run pytest", ["Bash(uv ru:*)"])


def test_supplied_fixture_is_verbatim_and_wire_metadata_remains_raw():
    raw = (FIXTURES / "success.jsonl").read_bytes()
    assert len(raw) == 6846
    assert hashlib.sha256(raw).hexdigest() == "1a386e8e95c4716bf3d1718fd364e02a9512cf29de56b756e2dc57d37174a327"
    assert raw.endswith(b"\n") and len(raw.splitlines()) == 4
    assert parse_stream(raw.decode(), 0).status == "success"
    rate = json.loads(raw.splitlines()[2])["rate_limit_info"]
    assert rate["overageStatus"] == "rejected" and rate["status"] == "allowed"
    assert "resetsAt" in rate and "resets_at" not in rate


def test_supplied_fixture_preserves_capability_order():
    capabilities = json.loads(success().splitlines()[0])["capabilities"]
    for sequence in (
        ["request_marker_lists_v1", "ask_user_question_unanswered_v1", "lifecycle_wakeup_source_v1"],
        ["mcp_tool_ui_meta_v1", "ui_surface_v1", "mcp_call_result_not_for_model_v1"],
    ):
        start = capabilities.index(sequence[0])
        assert capabilities[start:start + len(sequence)] == sequence


def test_lock_derivative_changes_only_final_result_text():
    events = [json.loads(line) for line in success().splitlines()]
    refusal = (FIXTURES.parent / "feature_factory_0_14_1/lock-refusal.stderr.txt").read_text()
    events[-1]["result"] = refusal
    expected = "".join(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n" for event in events)
    assert (FIXTURES / "lock_refused.jsonl").read_bytes() == expected.encode("utf-8")


def test_quota_and_warning_derivatives_preserve_retained_records_and_raw_metadata():
    source = [json.loads(line) for line in success().splitlines()]
    for filename, status in (
        ("quota_exhausted.jsonl", "rejected"),
        ("rate_limit_warning.jsonl", "allowed_warning"),
    ):
        events = [json.loads(line) for line in (FIXTURES / filename).read_text().splitlines()]
        expected_rate = {**source[2], "rate_limit_info": {**source[2]["rate_limit_info"], "status": status}}
        if status == "rejected":
            retained = ("type", "subtype", "result", "session_id", "duration_ms", "duration_api_ms", "num_turns")
            expected_result = {key: source[3][key] for key in retained}
            expected_result.update(is_error=True, api_error_status=429)
            assert events == [expected_rate, expected_result]
        else:
            assert events == [expected_rate]
        rate = events[0]["rate_limit_info"]
        for key in ("resetsAt", "rateLimitType", "overageStatus", "overageDisabledReason",
                    "isUsingOverage", "unifiedWindows"):
            assert rate[key] == source[2]["rate_limit_info"][key]


def test_derived_quota_and_warning_use_typed_fields():
    quota = (FIXTURES / "quota_exhausted.jsonl").read_text()
    result = parse_stream(quota, 1, now=1794908998.999)
    assert result.status == "quota_exhausted"
    assert result.retry_after_ms == 1001
    assert parse_stream(quota, 0, now=1794909001).retry_after_ms == 0
    warning = (FIXTURES / "rate_limit_warning.jsonl").read_text()
    assert parse_stream(warning + terminal(), 0).status == "success"


@pytest.mark.parametrize("stream,code,status", [
    ("", 0, "failed"), ("not JSON\n", 0, "failed"), ("[]\n", 0, "failed"),
    ("{}\n", 0, "failed"), ('{"type":"result"}\n', 0, "failed"),
    ('{"type":"assistant","error":"authentication_failed","message":{"content":[]}}\nnot JSON', 1, "auth_error"),
    ('{"type":"rate_limit_event","rate_limit_info":{"status":"allowed_warning"}}\n', 0, "failed"),
])
def test_malformed_and_auth_precedence(stream, code, status):
    assert parse_stream(stream, code).status == status


@pytest.mark.parametrize("code", [401, 403])
def test_numeric_auth_result_beats_other_malformed_lines(code):
    assert parse_stream("malformed\n" + terminal(is_error=True, api_error_status=code), 1).status == "auth_error"


@pytest.mark.parametrize("changes", [
    {"api_error_status": True}, {"api_error_status": "429"}, {"is_error": 0}, {"subtype": "error_max_turns"},
    {"result": None}, {"terminal_reason": "aborted_streaming"},
])
def test_recognized_bad_results_fail(changes):
    assert parse_stream(terminal(**changes), 0).status == "failed"


def test_finality_and_marker_are_authoritative():
    assert parse_stream(terminal() + terminal(), 0).status == "failed"
    assert parse_stream(terminal() + success().splitlines()[1], 0).status == "failed"
    assert parse_stream(terminal() + '{"type":"user","message":{}}\n', 0).status == "failed"
    assert parse_stream(terminal() + '{"type":"future_metadata","value":true}\n', 0).status == "success"
    assert parse_stream(terminal(result="WORKLINK_BLOCKED: design conflict\nfinished"), 0).status == "success"
    blocked = parse_stream(terminal(result="details\nWORKLINK_BLOCKED: design conflict"), 0)
    assert (blocked.status, blocked.blocked_reason) == ("blocked", "design conflict")
    assert parse_stream(terminal(result="quota exceeded, authentication failed"), 0).status == "success"
    assert parse_stream(terminal(), 1).status == "failed"


@pytest.mark.parametrize("reset", [True, "123", float("nan"), float("inf")])
def test_invalid_rate_reset_fails_closed(reset):
    rate = json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": "rejected", "resetsAt": reset}})
    assert parse_stream(rate + "\n" + terminal(is_error=True, api_error_status=429), 1).status == "failed"


def test_scrubber_registers_oauth_and_escaped_forms_before_artifacts():
    token = "test-" + 'secret"material'
    scrubber = secret_scrubber({"CLAUDE_CODE_OAUTH_TOKEN": token})
    assert token not in scrubber.scrub_text(token)
    assert json.dumps(token)[1:-1] not in scrubber.scrub_text(json.dumps(token))


@pytest.mark.parametrize("event", [
    {"type": "assistant", "message": {"content": [1]}},
    {"type": "assistant", "message": {"content": [{"type": "text", "text": 1}]}},
    {"type": "assistant", "message": {"content": []}, "error": 1},
    {"type": "user", "message": {}},
    {"type": "rate_limit_event", "rate_limit_info": {"status": "not-a-status"}},
])
def test_recognized_bad_events_cannot_be_hidden_by_success(event):
    assert parse_stream(json.dumps(event) + "\n" + terminal(), 0).status == "failed"


def test_deeply_nested_json_and_stream_bounds_fail_without_raising():
    assert parse_stream("[" * 2000 + "]" * 2000 + "\n" + terminal(), 0).status == "failed"
    assert parse_stream('{"type":"metadata","value":"' + "x" * (1024 * 1024) + '"}\n' + terminal(), 0).status == "failed"
    assert parse_stream("x" * (16 * 1024 * 1024 + 1), 0).status == "failed"


def test_latest_rate_status_is_used_without_free_text_quota_detection():
    rejected = json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": "rejected"}}) + "\n"
    allowed = json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": "allowed"}}) + "\n"
    assert parse_stream(rejected + allowed + terminal(), 0).status == "success"
    assert parse_stream(allowed + rejected + terminal(), 0).status == "failed"
    assert parse_stream(terminal(is_error=True, result="quota exceeded, authentication failed"), 1).status == "failed"
