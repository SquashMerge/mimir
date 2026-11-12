"""Pure daemon policy tests; no daemon, credentials, or provider is started."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from mimir_cultivate.worklink.daemon_config import (
    CONFIG_PATH, DAEMON_ROOT, DaemonConfigError, PublishingCredentials,
    daemon_configuration, daemon_environment, make_launch_snapshot,
    parse_launch_snapshot, resolve_daemon_credentials, resolve_epic_credentials,
    snapshot_matches, telemetry_service_name,
)


def test_mutation_capture_off_logs():
    env = {"MIMIR_TRACING_ENABLED": "1"}
    result = daemon_configuration(env, resolve_daemon_credentials(env))
    assert result.config["plugins"][1]["options"]["logsEnabled"] is False


def test_token_padding_stripped_before_control_validation():
    env = {"GH_TOKEN": "\tbot\r\n", "GITHUB_TOKEN": "bot",
           "MIMIR_FACTORY_PUBLISHING_IDENTITY": "account"}
    before = dict(env)
    assert resolve_daemon_credentials(env).bot_token == "bot"
    assert resolve_epic_credentials(env, repository_config={}).bot_token == "bot"
    assert env == before


@pytest.mark.parametrize("gh,github,expected,epic_ok", [
    (None, None, None, False), ("", " ", None, False),
    (" bot ", None, "bot", False), (None, " bot ", "bot", True),
    (" bot", "bot ", "bot", True),
    ("\t\r\n ", None, None, False), (None, "\t\r\n ", None, False),
    ("\t\r\n ", " \r\n\t", None, False),
    ("\tbot\r\n", None, "bot", False),
    ("\tbot\r\n", " \t\r\n", "bot", False),
    (None, "\tbot\r\n", "bot", True),
    ("\tbot\r\n", "bot", "bot", True),
    ("bot", "\tbot\r\n", "bot", True),
    ("\tbot\r\n", "\r\nbot\t", "bot", True),
])
def test_distinct_daemon_and_epic_token_contracts(gh, github, expected, epic_ok):
    env = {"MIMIR_FACTORY_PUBLISHING_IDENTITY": " bot-account "}
    if gh is not None:
        env["GH_TOKEN"] = gh
    if github is not None:
        env["GITHUB_TOKEN"] = github
    original = dict(env)
    daemon = resolve_daemon_credentials(env)
    assert daemon.bot_token == expected
    assert daemon.publishing_identity == "bot-account"
    aliases = daemon.environment()
    if expected:
        assert aliases["GH_TOKEN"] == aliases["GITHUB_TOKEN"] == expected
    else:
        assert "GH_TOKEN" not in aliases and "GITHUB_TOKEN" not in aliases
    if epic_ok:
        assert resolve_epic_credentials(env, repository_config={}) == daemon
    else:
        with pytest.raises(DaemonConfigError, match="requires GITHUB_TOKEN"):
            resolve_epic_credentials(env, repository_config={})
    canonical_env = {"MIMIR_FACTORY_PUBLISHING_IDENTITY": "bot-account"}
    if expected is not None:
        canonical_env["GITHUB_TOKEN"] = expected
    snapshot = make_launch_snapshot(resolve_daemon_credentials(canonical_env), str(uuid4()))
    assert snapshot_matches(snapshot, env, snapshot.launch_generation)
    assert env == original


@pytest.mark.parametrize("padding", ["", "\t\r\n"])
def test_conflicts_and_diagnostics_do_not_disclose_values(padding):
    first = "gh" + "p_" + "runtime-first"
    second = "gh" + "p_" + "runtime-second"
    env = {"GH_TOKEN": padding + first + padding, "GITHUB_TOKEN": padding + second + padding}
    before = dict(env)
    for resolver in (lambda: resolve_daemon_credentials(env),
                     lambda: resolve_epic_credentials(env, repository_config={})):
        with pytest.raises(DaemonConfigError) as error:
            resolver()
        assert first not in str(error.value) and second not in str(error.value)
    assert env == before


@pytest.mark.parametrize("name", ["GH_TOKEN", "GITHUB_TOKEN"])
@pytest.mark.parametrize("value", ["bo\tt", "bo\rt", "bo\nt", "\x00bot", "bot\x7f"])
def test_token_controls_remaining_after_strip_refused(name, value):
    env = {name: value, "MIMIR_FACTORY_PUBLISHING_IDENTITY": "account"}
    before = dict(env)
    with pytest.raises(DaemonConfigError, match=f"invalid {name}"):
        resolve_daemon_credentials(env)
    with pytest.raises(DaemonConfigError, match=f"invalid {name}"):
        resolve_epic_credentials(env, repository_config={})
    snapshot = make_launch_snapshot(PublishingCredentials(None, "account"), str(uuid4()))
    with pytest.raises(DaemonConfigError, match=f"invalid {name}"):
        snapshot_matches(snapshot, env, snapshot.launch_generation)
    assert env == before


def test_optional_identity_has_no_ambient_fallback():
    credentials = resolve_daemon_credentials({"FACTORY_PUBLISHING_IDENTITY": "ignored"})
    assert credentials.publishing_identity is None
    assert credentials.environment() == {}
    with pytest.raises(DaemonConfigError, match="requires MIMIR_FACTORY_PUBLISHING_IDENTITY"):
        resolve_epic_credentials({"GITHUB_TOKEN": "bot"}, repository_config={})


@pytest.mark.parametrize("value", [None, "", " ", 7, "bot\naccount", "bot\x00", "bot\x7f", "\tbot\r\n"])
def test_explicit_invalid_identity_refused(value):
    with pytest.raises(DaemonConfigError, match="invalid MIMIR_FACTORY_PUBLISHING_IDENTITY"):
        resolve_daemon_credentials({"MIMIR_FACTORY_PUBLISHING_IDENTITY": value})


@pytest.mark.parametrize("identity", [None, "bot"])
@pytest.mark.parametrize("legacy", [None, "old", ""])
def test_legacy_identity_presence_refuses_without_tracked_writes(tmp_path, identity, legacy):
    path = tmp_path / ".factory.json"
    raw = json.dumps({"publishing_identity": legacy})
    path.write_text(raw)
    env = {"GITHUB_TOKEN": "bot"}
    if identity:
        env["MIMIR_FACTORY_PUBLISHING_IDENTITY"] = identity
    with pytest.raises(DaemonConfigError, match="remove legacy .factory.json publishing_identity"):
        resolve_epic_credentials(env, repository_config=json.loads(path.read_text()))
    assert path.read_text() == raw


def test_closed_environment_exact_values_and_no_parent_changes():
    env = {"GH_TOKEN": "bot", "MIMIR_FACTORY_PUBLISHING_IDENTITY": "account",
           "HOME": "/home/mimir", "XDG_DATA_HOME": "/agent/data", "PYTHONPATH": "/inject",
           "OPENAI_API_KEY": "agent-key", "MIMIR_API_KEY": "agent-api"}
    before = dict(env)
    result = daemon_environment(resolve_daemon_credentials(env), api_password="private-api")
    expected = {
        "PATH": "/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin",
        "HOME": f"{DAEMON_ROOT}/home", "USER": "worklink", "LOGNAME": "worklink",
        "SHELL": "/bin/bash", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "TMPDIR": f"{DAEMON_ROOT}/tmp", "OPENCODE_CONFIG": CONFIG_PATH,
        "OPENCODE_SERVER_PASSWORD": "private-api", "GH_TOKEN": "bot", "GITHUB_TOKEN": "bot",
        "FACTORY_PUBLISHING_IDENTITY": "account",
        "XDG_CONFIG_HOME": f"{DAEMON_ROOT}/config", "XDG_DATA_HOME": f"{DAEMON_ROOT}/data",
        "XDG_STATE_HOME": f"{DAEMON_ROOT}/state", "XDG_CACHE_HOME": f"{DAEMON_ROOT}/cache",
        "OPENCODE_DISABLE_AUTOUPDATE": "1", "OPENCODE_DISABLE_MODELS_FETCH": "1",
        "OPENCODE_DISABLE_FREE_MODELS": "1", "OPENCODE_DISABLE_CLAUDE_CODE": "1",
        "OPENCODE_DISABLE_CLAUDE_CODE_SKILLS": "1", "OPENCODE_DISABLE_PROJECT_CONFIG": "1",
        "OPENCODE_DISABLE_FILEWATCHER": "1",
    }
    assert result == expected
    assert env == before
    optional = daemon_environment(resolve_daemon_credentials({}), api_password="private-api")
    assert set(optional) == set(expected) - {"GH_TOKEN", "GITHUB_TOKEN", "FACTORY_PUBLISHING_IDENTITY"}


@pytest.mark.parametrize("password", ["", " ", "pass\nword", " pass ", None, "\tpassword\r\n"])
def test_private_password_validation(password):
    with pytest.raises(DaemonConfigError):
        daemon_environment(resolve_daemon_credentials({}), api_password=password)


def _options(env):
    return daemon_configuration(env, resolve_daemon_credentials(env)).config["plugins"][1]["options"]


@pytest.mark.parametrize("capture", [False, True])
def test_exact_telemetry_options_and_plugin_entries(capture):
    env = {"MIMIR_TRACING_ENABLED": "1", "MIMIR_TRACING_CAPTURE_CONTENT": "1" if capture else "0",
           "MIMIR_OTLP_ENDPOINT": "https://collector.example/otel",
           "MIMIR_OTEL_SERVICE_NAME": "agent", "MIMIR_FACTORY_PUBLISHING_IDENTITY": "bot"}
    result = daemon_configuration(env, resolve_daemon_credentials(env))
    assert result.telemetry_state == ("content_on" if capture else "metrics_only")
    assert result.config == {"plugins": [
        {"package": "opencode2-feature-factory@0.14.0"},
        {"package": "@devtheops/opencode-plugin-otel@2.1.0", "options": {
            "enabled": True, "endpoint": "https://collector.example/otel", "protocol": "http/protobuf",
            "resourceAttributes": "service.name=agent.bot", "logsEnabled": capture,
            "disabledMetrics": [], "disabledTraces": [] if capture else ["session", "llm", "tool"],
            "capturePromptInLogs": capture, "captureModelContext": capture,
        }},
    ]}
    assert result.explanation is None


def test_telemetry_default_off():
    result = daemon_configuration({}, resolve_daemon_credentials({}))
    assert result.telemetry_state == "off"
    assert result.config["plugins"][1]["options"] == {"enabled": False}


def test_mutation_capture_off_session_trace():
    assert "session" in _options({"MIMIR_TRACING_ENABLED": "1"})["disabledTraces"]


def test_mutation_capture_off_llm_trace():
    assert "llm" in _options({"MIMIR_TRACING_ENABLED": "1"})["disabledTraces"]


def test_mutation_capture_off_tool_trace():
    assert "tool" in _options({"MIMIR_TRACING_ENABLED": "1"})["disabledTraces"]


@pytest.fixture
def sensitive_inputs():
    secret = "Bearer " + "runtime-" + "fragment-" + "credential"
    return secret, {"MIMIR_TRACING_ENABLED": "1", "OTEL_EXPORTER_OTLP_HEADERS": "authorization=" + secret}


def test_mutation_otlp_header_in_config(sensitive_inputs):
    secret, env = sensitive_inputs
    result = daemon_configuration(env, resolve_daemon_credentials(env))
    assert result.telemetry_state == "disabled_sensitive_otlp"
    assert result.config["plugins"][1]["options"] == {"enabled": False}
    output = json.dumps(result.config) + str(result.explanation)
    assert secret not in output and "otlpHeaders" not in output


def test_mutation_otlp_header_in_env(sensitive_inputs):
    secret, env = sensitive_inputs
    output = daemon_environment(resolve_daemon_credentials(env), api_password="private")
    assert secret not in json.dumps(output)
    assert not any("OTLP" in name for name in output)


@pytest.mark.parametrize("carrier", [
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
    "OTEL_EXPORTER_OTLP_METRICS_HEADERS", "OTEL_EXPORTER_OTLP_CLIENT_KEY",
    "OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
])
def test_all_sensitive_carriers_disable_telemetry(carrier):
    env = {"MIMIR_TRACING_ENABLED": "1", carrier: "runtime" + "-secret"}
    assert daemon_configuration(env, resolve_daemon_credentials(env)).telemetry_state == "disabled_sensitive_otlp"


@pytest.mark.parametrize("endpoint", [
    "http://user:password@collector", "https://collector?token=private",
    "https://collector#private", "file:///private", "http://collector:99999",
    " http://collector", "http://collector\n", "http://[broken", "http://collector:",
])
def test_invalid_endpoint_disables_without_echo(endpoint):
    env = {"MIMIR_TRACING_ENABLED": "1", "MIMIR_OTLP_ENDPOINT": endpoint}
    result = daemon_configuration(env, resolve_daemon_credentials(env))
    assert result.telemetry_state == "disabled_sensitive_otlp"
    assert endpoint not in json.dumps(result.config) + result.explanation


def test_resource_name_normalization_and_bounds():
    assert telemetry_service_name(None, None) == "mimir.worklink.worklink"
    assert telemetry_service_name("agent name,key=x", "bot@example") == "agent_name_key_x.bot_example"
    assert telemetry_service_name("x" * 200, "y" * 200) == "x" * 128 + "." + "y" * 128
    with pytest.raises(DaemonConfigError, match="invalid telemetry service name"):
        telemetry_service_name("bad\nname", None)


def test_snapshot_roundtrip_effective_inputs_and_generation(tmp_path):
    generation = str(uuid4())
    env = {"GH_TOKEN": "bot", "MIMIR_FACTORY_PUBLISHING_IDENTITY": "account"}
    snapshot = make_launch_snapshot(resolve_daemon_credentials(env), generation)
    assert parse_launch_snapshot(snapshot.to_dict()) == snapshot
    assert set(snapshot.to_dict()) == {"version", "launch_generation", "bot_token", "publishing_identity"}
    assert snapshot_matches(snapshot, env, generation)
    assert snapshot_matches(snapshot, {**env, "GITHUB_TOKEN": " bot "}, generation)
    assert not snapshot_matches(snapshot, {**env, "GH_TOKEN": "changed"}, generation)
    assert not snapshot_matches(snapshot, {**env, "MIMIR_FACTORY_PUBLISHING_IDENTITY": "changed"}, generation)
    assert not snapshot_matches(snapshot, env, str(uuid4()))
    host_file = tmp_path / "compose.env"
    host_file.write_text("GH_TOKEN=changed")
    assert snapshot_matches(snapshot, env, generation)
    recreated_env = {**env, "GH_TOKEN": "changed"}
    recreated = make_launch_snapshot(resolve_daemon_credentials(recreated_env), str(uuid4()))
    assert snapshot_matches(recreated, recreated_env, recreated.launch_generation)
    with pytest.raises(DaemonConfigError):
        snapshot_matches(snapshot, {**env, "GITHUB_TOKEN": "conflict"}, generation)
    assert "bot_token=" not in repr(snapshot)


@pytest.mark.parametrize("change", [
    {"version": True}, {"version": 2}, {"extra": 0}, {"launch_generation": "not-a-uuid"},
    {"bot_token": " "}, {"bot_token": 5}, {"publishing_identity": "bad\nidentity"},
    {"bot_token": "x" * 16384},
])
def test_snapshot_closed_schema(change):
    payload = make_launch_snapshot(PublishingCredentials(None, None), str(uuid4())).to_dict()
    with pytest.raises(DaemonConfigError):
        parse_launch_snapshot({**payload, **change})


def test_snapshot_missing_field_and_null_optional_values():
    snapshot = make_launch_snapshot(resolve_daemon_credentials({}), str(uuid4()))
    assert snapshot_matches(snapshot, {}, snapshot.launch_generation)
    payload = snapshot.to_dict()
    del payload["bot_token"]
    with pytest.raises(DaemonConfigError):
        parse_launch_snapshot(payload)
