"""Official SDK wire compatibility; required in claude-code-extra-smoke CI.

Provider-independent parsing/fixture checks stay in test_claude_code.py and
must execute without the optional SDK installed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

if os.environ.get("MIMIR_REQUIRE_CLAUDE_SDK") == "1":
    from claude_agent_sdk._internal.message_parser import parse_message
else:
    pytest.importorskip("claude_agent_sdk")
    from claude_agent_sdk._internal.message_parser import parse_message

FIXTURES = Path(__file__).parent / "fixtures/worklink/claude_code_2_1_326"


@pytest.mark.parametrize("filename", [
    "success.jsonl", "quota_exhausted.jsonl", "rate_limit_warning.jsonl",
])
def test_official_sdk_preserves_raw_rate_limit_metadata(filename):
    events = [json.loads(line) for line in (FIXTURES / filename).read_text().splitlines()]
    rate = next(event for event in events if event["type"] == "rate_limit_event")
    assert parse_message(rate).rate_limit_info.raw == rate["rate_limit_info"]


def test_official_sdk_preserves_typed_quota_error():
    events = (FIXTURES / "quota_exhausted.jsonl").read_text().splitlines()
    assert parse_message(json.loads(events[-1])).api_error_status == 429
