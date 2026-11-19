# Claude Code 2.1.326 stream fixtures

`success.jsonl` is the operator's four-line Claude 2.1.326 capture, relayed
verbatim by the driver in the be-leaf attempt1 continuation. Environment
forwarding was unavailable (`MIMIR_WORK_ITEM_JSON` was absent). No fields were
added or removed, wire keys were not normalized, and the final newline is kept.
SHA-256: `5ec588a0bb09881b804fd5e5a21fa68f19b7ed88060a0f6d33c02b2afb429913`.

Original operator command:
`claude -p "Reply with exactly: ok" --output-format stream-json --verbose --setting-sources project --strict-mcp-config --max-turns 1 --session-id <uuid>`.
It ran in an empty Git repository; the operator replaced only `cwd`,
`original_cwd`, and `memory_paths.auto`. The builder ran no live Claude session.

The other two fixtures are **derived**, not observed subscription failures.
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
values, including raw camelCase rate metadata, are preserved. The original
`success.jsonl` bytes and hash above remain unchanged. The factory fixture's
provenance documents its isolated local capture.
