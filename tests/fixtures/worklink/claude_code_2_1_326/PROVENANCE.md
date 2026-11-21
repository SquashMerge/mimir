# Claude Code 2.1.326 stream fixtures

`success.jsonl` is the operator's four-line Claude 2.1.326 capture.
The be-leaf attempt1 relay omitted two system capabilities. In remediation-1,
the driver attested a comparison against the entire original fenced source:
inserting `ask_user_question_unanswered_v1` between `request_marker_lists_v1`
and `lifecycle_wakeup_source_v1`, and `ui_surface_v1` between
`mcp_tool_ui_meta_v1` and `mcp_call_result_not_for_model_v1`, restores exact
byte equality. These two literal insertions were applied without
reserializing the capture. The builder did not independently access the
original payload (`MIMIR_WORK_ITEM_JSON` was absent).
The restored capture is 6846 bytes, including its final LF; formatting,
event order and raw wire keys are preserved.
SHA-256: `1a386e8e95c4716bf3d1718fd364e02a9512cf29de56b756e2dc57d37174a327`.

Original operator command:
`claude -p "Reply with exactly: ok" --output-format stream-json --verbose --setting-sources project --strict-mcp-config --max-turns 1 --session-id <uuid>`.
It ran in an empty Git repository; the operator replaced only `cwd`,
`original_cwd`, and `memory_paths.auto`. The builder ran no live Claude session.

## Operator settings-source isolation probe (2026-11-21)

Source: [operator report on PR #150, item 7](https://github.com/SquashMerge/mimir/pull/150#issuecomment-7057681916).
This is a separate operator-run probe, not the source of `success.jsonl`,
not a builder rerun, and not a successful authenticated subscription session.

The operator used a throwaway `node:22-slim` container and installed
`@anthropic-ai/claude-code@2.1.326`; the CLI reported `2.1.326 (Claude Code)`.
The empty Git repository contained a hostile `.claude/settings.json` whose
`env` set `ANTHROPIC_BASE_URL` to `http://127.0.0.1:9999`, with a listener on
that port. Runs used `HOME=/worker`, `CLAUDE_CONFIG_DIR=/worker/.claude`,
and a **fake** `CLAUDE_CODE_OAUTH_TOKEN`.

The common command was `claude -p "Reply with exactly: ok" --output-format
stream-json --verbose … --strict-mcp-config --max-turns 1`. The operator
reported these discriminating results:

| Settings arguments | Hostile listener | Reported outcome |
|---|---|---|
| `--setting-sources "" --settings /worker/.claude/settings.json` (leaf shape) | No connection | First-party API request: `api_error_status 401 authentication_error`, `api_error_origin first_party` |
| `--setting-sources project` (control) | Connected: `POST /v1/messages?beta=true` | Hostile project base URL honoured |
| No `--setting-sources` (default control) | Connected: `POST /v1/messages?beta=true` | Hostile project base URL honoured |

This supports the specific claim that CLI **2.1.326** treats the explicit
empty settings-source value as disabling project settings. The positive
controls distinguish that behaviour from an inert listener or a CLI that
never issues a request. Raw probe logs are not stored in this fixture set;
the linked operator report is the evidence of record.

`tests/test_claude_code.py::test_leaf_argv_is_narrow_official_cli` asserts
the complete leaf argv, including exactly one `--setting-sources` followed
by a separate empty-string argument and the explicit private `--settings`
path. **Do not bump the Claude CLI pin without re-running this probe** with
the new pinned version, both positive controls, and the exact leaf settings
arguments, then recording the new evidence. This probe does not establish
same-UID credential isolation or executor-restart recovery.

## Derived stream fixtures

The quota and warning fixtures are **derived**, not observed subscription failures.
Their source is this capture plus the locked `claude-agent-sdk==0.2.180`
`types.py` (`RateLimitInfo`, `RateLimitStatus`, `ResultMessage`) and
`_internal/message_parser.py` (`rate_limit_event`, `result`). The SDK explicitly
models API errors with subtype `success`, `is_error=true`, and numeric
`api_error_status`. Its rate parser consumes native camelCase keys and retains
the full raw object. A rejected overage alone does not exhaust the main window.

`rate_limit_warning.jsonl` retains only capture line 3 and changes only
`rate_limit_info.status` from `allowed` to `allowed_warning`. Tests append the
unchanged successful result when testing a complete stream.

`quota_exhausted.jsonl` omits capture lines 1–2, changes line 3's
`rate_limit_info.status` to `rejected`, and retains the result fields `type`,
`subtype`, `result`, `session_id`, `duration_ms`, `duration_api_ms`, `num_turns`.
It changes `is_error` to true and `api_error_status` to 429. Every other result
field is omitted: `stop_reason`, `total_cost_usd`, `usage`, `modelUsage`,
`permission_denials`, `terminal_reason`, `fast_mode_state`,
`fast_mode_disabled_reason`, `subagent_stats`, `safety_stops`, `ttft_ms`, `uuid`,
`ttft_stream_ms`, `time_to_request_ms`, `first_content_frame_ms`,
`queued_turn_count`, `result_index`, `model_usage_since_last_result`, and
`credential_expires_at`. All retained raw rate metadata is unchanged, including
`unifiedWindows`; no claim is made that these window details describe a live
quota incident. The unchanged result text deliberately proves classification
does not rely on error prose.

`lock_refused.jsonl` is **derived**, not observed Claude output. It decodes the
four success events, replaces only the final result's `result` string with
the unmodified locally captured `feature_factory_0_14_1/lock-refusal.stderr.txt`,
and serializes all four events as compact UTF-8 JSONL. All other fields and
values, including raw camelCase rate metadata, are preserved. The corrected
complete success capture is the source for this derivative. The factory fixture's
provenance documents its isolated local capture.
