# ACP 1.0 adapter design

**Status:** proposal for operator review, 2026-11-18. Chainlink #2170 supersedes
the mechanical migration #2160. No implementation leaf or upstream issue is
authorized by this document.

## Decision proposed

Choose **(b): a mimir-owned JSON-RPC connection and scheduler, with the Python
SDK supplying v1 schema models, not the connection runtime**. Keep the sole
third-party import boundary in `mimir/acp/sdk.py`. Preserve the externally
observable contracts below; do not reproduce SDK private classes or silently
replace fail-closed behaviour with the SDK's permissive defaults.

Stay on `agent-client-protocol==0.12.0` until an independently tested cutover.
Pursue narrowly scoped upstream extension points in parallel, not as a
prerequisite or an assumed promise. The operator can instead choose (a), but
only after its admission/ownership prototype proves the difficult rows in the
options matrix. A router semaphore alone is **not** that prototype.

This recommendation adds code ownership and security-review work. It does not
authorize weakening a guard, adding web transports, enabling protocol v2, or
advertising new optional capabilities. Review of this document happens after
publication; implementation leaves are filed only after the operator approves
or redirects the recommendation.

## 1. Scope and evidence conventions

The baseline is checkout commit `7225dca9e8269f821384f35c40f5a49491306111`
and its `pyproject.toml`/`uv.lock` pin, not the latest SDK.
`sdk.py` is the only production module importing `acp`; its direct callers
are `agent.py`, `daemon.py`, `updates.py`, `journal.py`, `session_store.py` and
`bridge.py`. Follow-on wire boundaries include `proxy.py`, `transport.py`,
`stdio.py`, `relay.py` and `ssh.py`. Confinement, AppArmor, credential storage
and tool execution internals are excluded; permission/credential **wire**
handling and generation ownership remain in scope.

**Documented** below means a statement in a primary specification, release
note or PR. **Source observation** means behaviour visible in the cited
version's implementation, not a promised extension contract. **Inference** and
**proposal** are explicitly labelled. A plan or suggested test is not evidence
that the new adapter already works.

SDK source references [S1]–[S13] are pinned to release commit
`9853d8b514d6340000bcbd01ec07808436db80cf` (tag `1.0.0`); the baseline tag
`0.12.0` resolves to `750c6ee7216d527cedcbd5b33b4e6178138e9f04`.
Protocol references [P1]–[P4] use `schema-v1.23.0`, commit
`6d08f412a7a1370d3cc9a124e3be3d6acf92641e`. Release and PR pages preserve
the upstream rationale separately from our interpretation.

## 2. Today's contract inventory

In this section, code paths are relative to `mimir/acp/`. Test shorthand expands
to these exact paths:

| Shorthand | File |
|---|---|
| SDK | `tests/test_acp_sdk_contract.py` |
| Agent | `tests/test_acp_agent.py` |
| Sessions | `tests/test_acp_sessions.py` |
| Updates | `tests/test_acp_updates.py` |
| Daemon | `tests/test_acp_daemon.py` |
| Transport | `tests/test_acp_transport.py` |
| Proxy | `tests/test_acp_proxy.py` |
| Journal | `tests/test_acp_journal.py` |
| Shutdown | `tests/test_acp_shutdown.py` |
| Packaging | `tests/test_acp_packaging.py` |

Each row is a preservation obligation, including negative paths. Rows group
related assertions rather than claiming each test has exactly one assertion.

### Connection, wire and authority

| ID | Behaviour and enforcing code | Tests pinning it |
|---|---|---|
| C01 | **Import/version boundary.** `sdk.py` rejects `PROTOCOL_VERSION != 1`, imports generated schema directly, and exposes the adapter surface without importing `acp.task`. Distribution pin and installed-artifact packaging must match. | SDK `test_no_mimir_module_imports_private_acp_task`, `test_installed_acp_public_symbols_used_by_sdk`, `test_pinned_distribution_protocol_and_schema_boundary`; Packaging `test_acp_and_mcp_dependency_declarations`, `test_wheel_guard_matches_finite_acp_source_inventory`; `tests/test_acp_dependency_closure.py::test_per_member_third_party_allowlist_rejects_mutations`. |
| C02 | **Outgoing bound and uniqueness.** `StrictMessageStateStore.register_outgoing` refuses duplicate live IDs and admits exactly 64 combined live requests and abandoned tombstones. An unanswered cancellation does not free capacity. `reject_all_outgoing` clears both on connection retirement. | SDK `test_pending_request_limit_admits_exact_boundary`, `test_abandoned_request_tombstones_share_pending_limit`, `test_request_store_close_unknown_duplicate_and_abandon_contract`. The explicit duplicate-registration branch needs a dedicated wire-independent mutation test in the migration; the existing test named “duplicate” mainly covers duplicate responses. |
| C03 | **Strict response correlation.** Store `resolve_outgoing`/`reject_outgoing` reject unknown, duplicate, already-completed or late responses. Abandonment cancels the future and consumes **one** subsequent response, result or error; a second is invalid. Direct transport-send errors must reach their own caller. | SDK `test_strict_outer_envelopes_and_duplicate_or_late_responses_fail`, `test_public_connection_correlates_direct_results_and_rejects_duplicates`, `test_public_connection_rejects_late_after_rejection_and_closed_outcomes`, `test_public_connection_mcp_request_correlates_direct_client_error`, `test_request_store_close_unknown_duplicate_and_abandon_contract`. |
| C04 | **Per-invocation start ownership.** `prepare_start`/`register_outgoing` use a `ContextVar` registration, method match and one-shot future. `AcpPeer.start_request` returns the actual outer ID and its task, not the next global ID or next request of that method. Cancellation/failure before registration cleans both start tasks; after registration it abandons that ID. `AcpRequestHandle.abandon` is idempotent and cancels owned completion tasks. | SDK `test_request_registration_is_per_invocation_under_same_method_concurrency`, `test_request_cancelled_after_prepare_before_registration_leaves_no_stale_pairing`, `test_request_connection_closed_after_prepare_before_registration_cleans_start`, `test_request_handle_start_failure_does_not_wait_for_unregistered_id`, `test_public_request_handle_exposes_outer_id_and_quarantines_one_late_reply`. |
| C05 | **NDJSON receive.** `StrictNdjsonTransport._read_frame` limits raw bytes to 8 MiB **before** decoding, bounds reads to remaining capacity plus the overflow witness, accepts a final non-newline frame at EOF, and distinguishes clean EOF. `receive` sends `-32700` for malformed JSON/UTF-8, permits recovery, resets the consecutive-error counter after decoded JSON, and retires at three consecutive parse errors. | SDK `test_ndjson_frame_limit_is_counted_before_decode`; Transport `test_parse_error_response_preserves_connection_for_next_request`, `test_repeated_parse_errors_hit_connection_limit`. Final-frame/reset details are source-observed; add explicit boundary tests before porting. |
| C06 | **Envelope strictness is separate from params.** `validate_jsonrpc_envelope` requires an object and `jsonrpc: "2.0"`, allowlists envelope/error keys, requires a string method, exactly one response result/error, integer non-boolean error code and string error message. Outer IDs allow string, integer or null, never bool/float/container. Request params can be malformed at this layer and receive a router invalid-params error rather than relaxing the envelope. | SDK `test_strict_outer_envelopes_and_duplicate_or_late_responses_fail`, `test_actual_runner_preserves_agent_router_and_generic_outer_ids`, `test_public_connection_exact_bidirectional_mcp_wire_and_generic_ids`; Agent `test_wrong_typed_params_return_errors_and_connection_survives`. |
| C07 | **Inbound backlog.** `BoundedMessageQueue` admits 64 items / 8 MiB, using received frame byte count (serialized fallback for test tasks). Producer lock makes item/byte commit atomic; timeout/cancellation does not leak capacity. Publication waits at most 2 s. A 65th physical slot is reserved for one shutdown sentinel; close wakes blocked publishers and forbids a tail publish. | SDK `test_input_queue_item_limit_times_out_only_after_exact_boundary`, `test_input_queue_byte_limit_admits_exact_boundary`, `test_input_queue_capacity_release_admits_waiting_publisher`, `test_input_queue_expiry_leaves_capacity_and_bytes_unchanged`, `test_input_queue_close_rejects_blocked_publisher_without_tail_item`. |
| C08 | **Active execution, not arbitrary task count.** `BoundedMessageDispatcher` caps request/notification runners at 64; releases slots on completion/failure; normal stop drains buffered requests. Stop has 30 s drain and 2 s cancellation budgets and survives repeated caller cancellation; transport-dead stop skips drain. Resistant runners are detached only after the bounded cleanup. | SDK `test_live_connection_bounds_active_inbound_runners`, `test_dispatcher_stop_drains_buffered_requests_before_shutdown`, `test_dispatcher_stop_is_bounded_when_queue_drain_stalls`, `test_dispatcher_stop_timeout_cancels_active_runners`, `test_dispatcher_stop_cancellation_waits_for_bounded_cleanup`. |
| C09 | **Authentication ordering.** Dispatcher sequences `authenticate` after earlier handlers and later work after authentication, without consuming runner slots for fence waiters. `session/cancel` bypasses the auth fence. This prevents reauthentication from deadlocking a prompt awaiting cancellation. | SDK `test_authentication_waiters_do_not_consume_runner_slots`, `test_second_authentication_waits_for_first_without_runner_slot`, `test_buffered_stateful_request_waits_for_authentication`, `test_notification_waits_behind_authentication_fence`; Sessions `test_reauthenticate_fence_still_dispatches_session_cancel`. |
| C10 | **Pre-auth refusal and physical identity.** The fence is ordering, not authorization. `MimirAcpAgent.authenticate`, `_begin_stateful` and `cancel` enforce authenticated current generation/principal, including rechecks after awaited work. Only a successfully validated admin/non-service auth context activates a candidate; errors are generic. `on_connect` binds handlers to the physical generation, currently by wrapping the SDK's private `_handler`. Unauthenticated/demoted/foreign-session cancellation is an audited no-op, not authority over the active peer. | Agent `test_stateful_methods_require_authentication_and_connection`, `test_candidate_activates_only_after_admin_authentication`, `test_factory_output_is_revalidated_before_storage`, `test_authentication_failures_are_context_free_and_generic`, `test_overlapping_connections_keep_authentication_and_dispatch_physical`, `test_cancel_from_unauthenticated_peer_leaves_existing_session_untouched`, `test_cancel_from_demoted_connection_leaves_successor_prompt_untouched`, `test_cancel_rejects_session_from_another_generation`, `test_cancel_rejects_session_owned_by_another_principal`. |
| C11 | **Busy versus replaced.** Daemon `_admit_peer` has one admitted/unretired peer and sends actionable `-32001` then closes a second connection. Successful authentication of a successor in the agent's overlapping-connection model marks the old generation replaced; stateful calls and completing old prompts return `-32002`. Old cleanup may not unbind successor state. These are distinct layers, not permission for a second daemon slot. | Daemon `test_single_peer_cap_refuses_second_with_actionable_error`, `test_clean_close_allows_immediate_reconnect`; Sessions `test_connection_replacement_old_cleanup_preserves_successor_ownership`, `test_replacement_retirement_waits_for_grace_then_completes_cancel_window`, `test_replacement_retirement_force_quarantines_old_and_preserves_successor`. |
| C12 | **Transport death is retirement before drain.** `run_stdio_agent` marks peer/store and dispatcher dead on EOF **and** exception, calls generation-scoped `on_transport_closed`, then closes. `AcpPeer` rejects later sends, clears active MCP connections, rejects outstanding futures. Primary errors survive callback/close errors. Agent teardown touches only owned generation resources; daemon watchdog races drain against the peer-dead event. | SDK `test_runner_retires_failed_transport_before_cancelling_handlers`, `test_runner_closes_and_generation_teardown_is_exception_safe`; Daemon `test_tcp_reset_retires_inflight_peer_and_allows_prompt_reconnect`, `test_eof_retires_write_only_inflight_peer_before_dispatcher_drain`, `test_transport_dead_peer_skips_watchdog_drain_timeout`, `test_connection_failure_leaves_separate_peer_and_runtime_turn_alive`; Sessions `test_transport_teardown_is_generation_scoped_and_requires_load`. |
| C13 | **Fail-closed retirement/admission.** Runner shields close; on caller cancellation it gives close two 0.5 s intervals and reports any non-clean close through `on_close_abandoned`. `_close_retired_cleanly` treats pending/exception as unretired, success/honoured cancellation as clean. Daemon `_unretired_generations` counts live abandoned closes and permanently fences failed retirements until recycle. A runner returning is not proof its generation retired. | Daemon `test_close_bound_fits_inside_the_daemon_cancel_budget`, `test_resistant_close_does_not_pin_a_cancelled_runner`, `test_second_peer_is_refused_while_an_old_close_is_still_pending`, `test_capacity_returns_once_the_old_close_terminates`, `test_an_abandoned_close_that_fails_keeps_admission_closed`, `test_an_abandoned_close_that_honours_cancellation_returns_capacity`, `test_close_that_raises_on_forced_cancel_still_fences_admission`, `test_close_that_raises_in_the_first_grace_interval_fences_admission`, `test_close_failure_surfaced_by_the_shield_fences_admission`. |
| C14 | **Finite I/O and peer lifetime.** Strict transport send validates and drains within 30 s. Daemon gives unauthenticated peers 10 s, watches authenticated drain every 5 s with 30 s budget, and bounds peer cancellation/abort (2/1 s) and total shutdown (20 s; at most four concurrent closes). Proxy/relay `transport.py` has 2/1/1 s writer drain/close/abort and 5 s force-close/EOF grace; slow normal pumps do not get an arbitrary first-output deadline. | SDK `test_ndjson_send_bounds_non_draining_writer`; Daemon `test_preauth_timeout_cancels_connection_runner`, `test_authenticated_connection_outlives_preauth_deadline`, `test_postauth_watchdog_terminates_non_draining_peer`, `test_total_shutdown_deadline_never_awaits_stuck_cleanup`, `test_shutdown_closes_at_most_four_peers_concurrently`; Transport `test_pump_allows_slow_reader_until_peer_eof`, `test_peer_eof_cancels_blocked_opposite_pump`, `test_force_close_deadline_bounds_writer_cleanup`, and the four `test_*deadline_before_and_after_witnesses` tests. |

### Routing, sessions, MCP and permission

| ID | Behaviour and enforcing code | Tests pinning it |
|---|---|---|
| C15 | **Finite stable-v1 surface.** `run_stdio_agent` uses stable agent routing plus a narrow MCP override. `initialize` advertises protocol 1, load, text/resource-link prompts, ACP MCP (not HTTP/SSE), and `mimir-web-key`. Agent exposes no terminal/filesystem/model/provider management handlers; unknown extension requests return method-not-found, notifications get no response. `_meta` is the metadata carrier, not arbitrary params expanded into authority. | Agent `test_initialize_is_exact_stable_v1`, `test_agent_exposes_no_out_of_scope_handlers`, `test_unknown_extension_request_and_notification_behavior`, `test_actual_router_uses_only_meta_key_and_emits_no_notification_response`; SDK `test_pinned_schema_aliases_and_handler_models`, `test_pinned_agent_signatures_and_error_objects`. |
| C16 | **Session identity and load transaction.** `SessionStore` uses canonical UUIDv4 IDs, owner principals and separate replay/live eligibility. `new_session` validates declaration/directories before durable state; `load_session` validates/admit/replays before success, rolls back candidates/restores old indexes on failure, uses fresh provider ownership, and refuses a still-stopping turn with `-32003`. Dirty/old-generation sessions cannot prompt until a fresh authenticated load. No kernel namespace is persisted. | Sessions `test_additional_directories_are_rejected_before_creation`, `test_malformed_provider_declaration_creates_no_state`, `test_unavailable_load_reasons_have_no_prefix`, `test_stalled_turn_detach_refuses_within_bound`, `test_detaching_state_blocks_prompts_without_unbinding_replacement`, `test_failed_load_readmission_preserves_prior_provider_binding`, `test_failed_replay_restores_all_prior_provider_indexes`, `test_session_load_restores_transcript_without_kernel_state`; Journal `test_owner_paths_modes_and_foreign_are_closed`. |
| C17 | **Prompt isolation and authoritative delivery.** Agent `prompt` permits one active turn per session, validates all blocks before output, binds exact-turn subscription/auth context/bridge publisher, and removes only its own queue/prompt/publisher identities. Text/resource links normalize into the core event; final text uses the bridge, not inferred tool results. Failure terminalizes open tools and returns generic error; final-text delivery refusal has structured refusal metadata. | Sessions `test_new_prompt_runs_bound_core_and_preserves_update_order`, `test_prompt_validates_all_blocks_before_any_update`, `test_one_active_prompt_per_session_and_guard_releases`, `test_prompt_auth_context_is_scoped_to_the_session_channel`, `test_finally_unsubscribes_only_its_exact_queue_identity`, `test_normal_core_failure_terminalizes_and_redacts`, `test_acp_send_message_rejects_slack_turn_without_journal_append_and_allows_owner`; `tests/test_acp_bridge.py::test_plain_text_is_authoritative_update_and_exact_result`, `test_connected_unbound_and_channel_bound_routing`. |
| C18 | **Session cancellation is a boundary, not just Task.cancel.** `cancel`/`_cancel_active` linearize under `_boundary_lock` and journal lock: close lease, withdraw permissions, abandon owned requests, cancel model/provider work, stop later registration, terminalize accepted tools, then respond `stopReason: cancelled`. MCP cancellation carries the exact **outer** ID. A 2 s non-cooperative cleanup dirties execution/revokes provider and requires load. Idle/repeated/foreign cancels are bounded audited no-ops. | Sessions `test_explicit_cancel_drains_accepted_tool_and_terminalizes_before_response`, `test_cancel_timeout_dirties_execution_and_requires_fresh_load`, `test_idle_and_repeated_cancel_are_structured_owned_noops`, `test_oversized_cancel_bounds_retained_and_logged_id_without_cancelling_live_turn`, `test_cancel_boundary_prevents_post_boundary_model_registration`, `test_cancel_boundary_prevents_permission_and_mcp_registration`; SDK `test_mcp_handle_cancel_notification_uses_exact_outer_id_and_retains_connection`, `test_message_mcp_cancellation_quarantines_late_reply`. |
| C19 | **MCP wire allowlists.** Local strict models forbid extras/coercion and validate `_meta` independently on each outer model. `AcpPeer` emits connect/message/disconnect, accepts only `initialize`, `tools/list`, `tools/call` requests and initialized/cancelled outbound notifications. `route_mcp` accepts ping requests on live connections and selected inbound tools-list/progress/message notifications; unknown connection requests error, unknown/stale notifications are ignored. Inbound connect/disconnect requests are not agent methods. Nested MCP params/results are not reinterpreted as outer IDs. | SDK `test_mcp_schema_v119_outer_models_have_independent_strict_meta`, `test_mcp_message_nested_params_are_preserved_and_typed`, `test_mcp_schema_v119_rejects_each_malformed_typed_field`, `test_mcp_connect_message_disconnect_exact_outbound_envelopes`, `test_mcp_response_validation_reuse_and_routing_fail_closed`, `test_public_connection_exact_bidirectional_mcp_wire_and_generic_ids`. |
| C20 | **Provider admission and ownership.** Agent accepts zero or exactly one `acp` / `mimir-hands` declaration, not arbitrary client executables. `connect_mcp` and `_admit_provider` reject empty/reused connection IDs even after disconnect. Discovery requires the exact unique finite Hands manifest (description, input/output schema; no pagination), initialized before installation. Revalidation is serialized, generation-scoped and detaches on mismatch. Results require `structuredContent`, not bare objects; tool errors are not successes. Progress tokens belong to provider/generation/epoch/object identity; stale cleanup cannot erase a successor token. | Sessions `test_provider_admission_rejects_before_connect_or_state`, `test_admission_rejects_every_mismatch_with_exact_top_level_message`, `test_conforming_provider_manifest_establishes_session`, `test_discovery_requires_exact_unique_rows_and_retains_used_ids`, `test_provider_indexes_are_session_owned_and_load_uses_fresh_connection`, `test_list_changed_revalidates_only_fresh_tools_list_serially`, `test_progress_token_collision_cleanup_preserves_successor_ownership`, `test_provider_tool_error_becomes_tool_message_but_taints_agent_reply`; `tests/test_client_provider.py` pins structured results for the Hands tools. |
| C21 | **Canonical permission snapshot.** `UpdateDispatcher` freezes the first lifecycle argument snapshot separately from presentation scrubbing. `ActivePrompt._request_permission` drains accepted updates, compares the snapshot to exact eligible arguments and verifies current provider/generation/epoch both before request registration and after completion. Missing/mismatched snapshots and stale approvals do not execute. `permission_request_params` copies raw JSON, fixes tool identity/status and option order; wrapper/taint/ingest metadata has strict allowed shapes. | Updates `test_permission_snapshot_pins_first_unscrubbed_arguments`; Sessions `test_active_prompt_permission_uses_admitted_snapshot_and_is_once_scoped`, `test_admin_hands_permissions_precede_execution_and_preserve_raw_arguments`, `test_cancel_boundary_prevents_permission_and_mcp_registration`; SDK `test_admin_permission_options_are_exact_and_ordered`, `test_trusted_permission_metadata_is_true_only_and_exact`, `test_python_permission_metadata_and_raw_arguments_are_exact`. |
| C22 | **Permission replies fail closed.** Local `RequestPermissionResponse` accepts selected allow_once/allow_session/reject_once or cancelled only; rejects malformed/unknown/persistent alternatives and cancelled-inner metadata. Errors/cancellation never become executable. `PermissionCompletion.session_grant` is attribution only. The daemon requests permission on every call and stores no reusable grant; waiting is until answer or prompt cleanup, not a new short permission timeout. | SDK `test_permission_response_accepted_shapes`, `test_permission_response_rejects_malformed_unknown_and_persistent`, `test_permission_meta_matrix_is_independent_and_cancelled_inner_meta_is_illegal`, `test_permission_completion_exposes_cancel_and_errors_without_execution`, `test_public_connection_permission_completion_correlates_exact_wire_outcomes`, `test_permission_completion_preserves_telemetry_only_marker_in_both_paths`; Sessions `test_permission_wait_survives_until_answer_or_prompt_cleanup`, `test_daemon_emits_permission_for_every_call_and_stores_no_grant`. |
| C33 | **Ingest trust uses a distinct session-owned permission exchange.** `ActivePrompt.offer_ingest_trust` permits one human request per active taint ordinal, binds the session's ingest-state identity and current prompt, and accepts only an error-free allow_once. Clearing checks the expected label snapshot/ordinal and durable audit; stale replies cannot clear newly ingested content. `acknowledge_ingest` advances the session boundary, not another session's state or a reusable wrapper grant. This covers the ACP carrier, not a redesign of information-flow policy. | Sessions `test_human_ingest_trust_persists_for_acp_session_until_new_source`, `test_acp_pending_trust_allows_trusted_arrival_rejects_new_untrusted`, `test_acp_ingest_acknowledgement_is_not_shared_between_sessions`; Proxy `test_ingest_trust_request_never_uses_or_creates_session_grant`; SDK `test_admin_permission_options_are_exact_and_ordered`. |

### Updates, persistence and the client-side connection

| ID | Behaviour and enforcing code | Tests pinning it |
|---|---|---|
| C23 | **Update shape and redaction.** `UpdateDispatcher._map` preserves FIFO tool lifecycle, synthesizes orphan starts, emits failed terminals and full-replacement todo plans, suppresses reasoning/model/internal/send_message events, scrubs client args/results consistently, and limits presentation detail. `AcpPeer.session_update` serializes aliases/discriminators and metadata. | SDK `test_pinned_session_update_models_and_aliases`, `test_pinned_session_update_wire_shapes`; Updates `test_complete_suppression_tool_and_redaction_mapping`, `test_tool_input_and_output_use_the_same_client_redaction_policy`, `test_orphan_ends_synthesize_pending_in_order`, `test_write_todos_is_one_ordered_full_replacement`, `test_invalid_partial_delta_or_failed_todos_emit_no_plan`, `test_every_send_message_lifecycle_phase_is_suppressed`, `test_is_error_discriminator_overrides_nominal_success`. |
| C24 | **Bounded updates/delivery.** Updates have a distinct 128 item / 8 MiB queue, atomic byte accounting and owner cancellation/quarantine on overflow. First delivery failure remains authoritative, pending items are acknowledged, and no false success follows failed publication. `UpdateClient` and drain use a progress-renewed 2 s delivery budget, not a total replay duration; close has 2 s grace plus 2 s cancel, retaining ownership on resistant failure. | Updates `test_update_item_limit_admits_exact_boundary_and_rejects_next`, `test_update_byte_limit_admits_exact_boundary_and_rejects_next`, `test_submit_overflow_cancels_owner_quarantines_follow_on_events`, `test_first_delivery_failure_is_retained_and_remaining_queue_is_acked`, `test_terminal_publish_failure_stops_publication_but_closes_all_tools`, `test_slow_dispatcher_progress_renews_drain_budget`, `test_close_backstop_reports_a_cancellation_resistant_publisher`; Sessions `test_slow_replay_renews_waiting_prompt_drain_budget`, `test_transport_death_releases_prompt_blocked_on_update_delivery`. |
| C25 | **Durable ordering and replay.** `SessionJournal` serializes prepared+fsync → send → sent+fsync under one lock; assigns monotonic `_meta["mimir.sequence"]`; replay sends all prepared entries at least once without changing storage/IDs and completes before load success. It validates the **entire** journal before any replay prefix, including duplicate keys, sequence, turn UUID and SDK update union. Replay/live/cancel share the boundary; cancellation waits for an already-running off-loop reader before releasing the lock. | Journal `test_publish_orders_prepared_fsync_send_sent_fsync`, `test_prepared_without_sent_replays_every_time_without_mutation`, `test_preassigned_message_ids_survive_prepare_live_and_repeated_replay`, `test_mixed_legacy_and_identified_messages_replay_without_backfill_or_rewrite`, `test_deferred_replay_validates_entire_file_before_output`, `test_cancelled_reader_holds_lock_until_worker_finishes`, `test_replay_and_live_are_excluded_by_one_lock`; Sessions `test_load_replays_at_least_once_without_mutating_journal`, `test_journal_boundary_linearizes_prepared_delivery_sent_and_close`. |
| C26 | **Persistence failure fences.** Journal capacity is exactly 64 MiB including prepared/sent pair; overflow durably marks nonreplayable before continuing live. Prepare/marker failure sends nothing; sent-record failure preserves observed send but forbids following sends. Fatal in-memory state survives failed markers/cache release; one lock/sequencer remains while referenced. Store checks owner/private regular files, durable delete/TTL markers precede unlink, and load cannot revive nonreplayable state. | Journal `test_journal_capacity_is_exactly_64_mib`, `test_capacity_equality_then_overflow_writes_no_pair_and_continues_live`, `test_overflow_marker_failure_writes_and_sends_nothing`, `test_prepared_failure_sends_nothing_and_disables`, `test_sent_failure_preserves_send_and_prevents_following_send`, `test_cache_release_keeps_fatal_fence_until_marker_is_durable`, `test_cache_release_during_delivery_keeps_one_lock_and_sequence`, `test_ttl_marker_before_unlink_failure_and_retry`, `test_delete_marker_before_unlink_failure_and_retry`; Sessions `test_nonreplayable_marker_survives_deleted_journal_and_reconstruction`. |
| C27 | **Proxy framing and credential carrier.** `FrameWriter`/`_route_stream` enforce a separate 1 MiB limit, require newline completion, reject malformed/non-object frames, inject the selected key only in authenticate `_meta`, overwrite reserved mimir metadata and never echo bad frames. `stdio.py` reserves protocol stdout before imports, including direct fd output. Unrelated accepted frames are byte-transparent in both directions, not reserialized for convenience. | Proxy `test_authenticate_overwrites_reserved_metadata`, `test_authenticate_requires_bounded_object_shape`, `test_transformer_enforces_input_and_output_bound`, `test_malformed_frames_fail_without_echoing_bytes`, `test_complete_frame_over_one_mebibyte_fails_closed`, `test_foreign_frames_are_byte_transparent_in_both_directions`, `test_local_proxy_connects_and_stdout_contains_only_protocol`; `tests/test_acp_stdio.py::test_direct_fd_and_import_banner_cannot_contaminate_protocol_stdout`. |
| C28 | **Proxy request/session bounds and ID namespaces.** `ProxyRouter` keys by `(type, value)` and direction, rejects bool/null locally, duplicate outstanding or unsolicited replies, retains cancelled ID tombstones without reuse. Limits: 1,024 outstanding requests (daemon/local combined), 1,024 tombstones, 1,024 active sessions/server IDs, 4,096 cumulative hosted connection IDs, 1,024 live hosted connections, and 1,024-character client IDs before retention. Explicit connection promotions are checked atomically against their own 4,096 bound. These are not the daemon's 64-request bound. | Proxy `test_duplicate_outstanding_typed_request_id_fails_closed`, `test_unsolicited_non_tombstoned_response_fails_closed`, `test_tombstoned_late_response_is_consumed_without_id_reuse`, `test_typed_directional_ids_boolean_rejection_and_hosted_cancellation`, `test_proxy_active_session_bound_and_reload_negative_control`, `test_proxy_explicit_server_bounds_pending_and_promotion`, `test_proxy_explicit_connection_bound_and_duplicate_negative_control`, `test_proxy_explicit_connection_promotion_is_bounded_atomically`, `test_proxy_client_id_lengths_before_retention_and_passthrough_control`, `test_live_and_cumulative_hosted_connection_bounds`. |
| C29 | **Hosted interception is generation-owned.** Proxy injects exactly one hosted Hands provider only if declaration absent/empty; explicit providers stay verbatim. Only minted current-generation IDs are intercepted. Load retires previous/provisional state even on failure; cancel/disconnect revoke calls appropriately. Daemon EOF first quiesces in-flight client routes/approvals/session transitions, then clears generation state/grants and closes workers. Foreign daemon traffic is not local execution authority. | Proxy `test_new_and_load_inject_exactly_one_provider_when_absent_or_empty`, `test_explicit_hands_declaration_is_preserved_verbatim`, `test_router_intercepts_only_ids_minted_by_current_generation`, `test_load_always_retires_hosted_state_and_failed_provisional_state`, `test_client_session_cancel_tombstones_hosted_request`, `test_hosted_failures_are_supervised_and_generation_state_is_bounded`; Shutdown `test_daemon_eof_quiesces_inflight_allow_session_response`, `test_daemon_eof_quiesces_inflight_session_transition`, `test_candidate_connection_does_not_retire_active_generation`. |
| C30 | **Proxy grants are local, narrow and revocable.** Reserved permission requests/replies use exact shapes/options/true-only metadata. `allow_session` is cached only after forwarding the response and only for that session/wrapper/generation/ingest boundary. Reuse answers daemon allow_once with telemetry attribution; tainted calls and ingest-trust prompts always reach the human. Load/exit/disconnect/changed ingest/generation/framing failure revoke grants; stale replies cannot restore them. Non-Hands permission frames remain transparent. | Proxy `test_allow_session_grant_is_recorded_only_after_wire_and_audit_request`, `test_grants_are_scoped_to_session_and_wrapper`, `test_ingest_trust_request_never_uses_or_creates_session_grant`, `test_ingest_acknowledgement_revokes_all_grants_and_stale_responses`, `test_tainted_matching_request_bypasses_grant_and_reaches_human`, `test_untainted_matching_request_is_answered_allow_once_upstream`, `test_malformed_reserved_permission_metadata_fails_closed`, `test_malformed_correlated_permission_responses_fail_closed`, `test_non_hands_permission_frames_remain_transparent`, `test_framing_and_generation_failures_revoke_grants`. |
| C31 | **Local execution permissions cannot be forged across ACP.** Proxy scope/risk requests require a current owned hosted task/session/connection/generation, their own reserved IDs, offered literal approval and bounded pending counts. Scope requests: 1,024 cumulative, one outstanding per session, 60 s timeout; risk requests: 128 cumulative. Late answers cannot install authority. This inventories only their connection/permission carrier, not confinement policies. | Proxy `test_scope_permission_rejects_unoffered_cancelled_and_malformed_answers`, `test_scope_permission_timeout_and_late_answer_fail_closed`, `test_scope_permission_cannot_outlive_its_hosted_request`, `test_scope_permission_requires_owned_hosted_task_and_bounds_requests`, `test_scope_permission_allows_only_one_outstanding_request_per_session`, `test_daemon_cannot_forge_local_scope_permission_metadata`, `test_unconfined_permission_is_bounded_and_needs_owned_client_request`. |
| C32 | **Operational boundary remains local and runtime-blind.** Daemon owns its socket/peer admission, not runtime construction. Proxy/relay/SSH remain finite dependency closures; relay is credential/protocol-blind. Signal/EOF teardown, owned descriptors and supervised write failures remain bounded and stderr-only diagnostics must not contaminate protocol output. No migration to an SDK subprocess helper may change these responsibilities. | `tests/test_acp_dependency_closure.py::test_proxy_relay_and_client_closures_are_finite_and_runtime_blind`, `test_daemon_consumes_runtime_without_constructing_or_closing_it`; `tests/test_acp_relay.py::test_relay_is_blind_to_protocol_profiles_credentials_and_runtime`, `test_dead_relay_client_promptly_closes_daemon_connection`; Shutdown `test_signal_exit_bounds_entire_teardown`, `test_frame_delivery_is_bounded_and_terminal`, `test_frame_delivery_sustained_ingress_is_ordered_and_bounded`, `test_transport_death_tears_down_only_bound_generation`; Daemon `test_peer_uid_rejection`. |

### Additional assertions within those contract groups

These details belong to the indicated IDs and are included in their option
assessments and migration acceptance criteria; they are not exemptions.

| IDs | Enforcing code and additional behaviour | Additional pinned tests |
|---|---|---|
| C10 | `authenticate` resolves the key per attempt, not from an adapter-construction snapshot; malformed identity reload/factory failure cannot create partial authority or expose resolver details. Credential storage/issuance implementation is otherwise out of scope. | Agent `test_authenticate_uses_resolved_identity_and_factory`, `test_authenticate_accepts_key_issued_after_agent_construction`, `test_authenticate_rejects_key_revoked_after_agent_construction`, `test_authenticate_fails_closed_once_for_corrupt_identity_reload`, `test_resolver_and_factory_failures_do_not_escape_details`. |
| C14 | `transport.pump_stream` half-closes at EOF; `pump_bidirectional` awaits both directions and preserves errors through bounded cleanup, not just the first completed task. Writer cleanup escalates drain → close → abort. | Transport `test_pump_stream_copies_bytes_and_half_closes`, `test_bidirectional_pump_awaits_both_directions`, `test_close_writer_drain_timeout_escalates_to_close`, `test_close_writer_close_timeout_escalates_to_abort`, `test_drain_deadline_before_and_after_witnesses`, `test_close_deadline_before_and_after_witnesses`, `test_abort_wait_deadline_before_and_after_witnesses`, `test_force_close_deadline_before_and_after_witnesses`. |
| C16/C25/C26 | `_begin_stateful` serializes off-loop TTL sweep once per hour; journal load reads/validates once off-loop. Repeated load does not rewrite metadata/journal or reset sequence. `_detach_state` waits for the prompt and releases only its own journal/indexes; replay excludes live publication. Overflowed sessions can still serve live prompts without becoming replayable. | Agent `test_load_session_reads_and_validates_once_off_loop`; Sessions `test_sweep_runs_off_loop_once_per_interval`, `test_repeated_load_preserves_metadata_mtime_bytes_and_sequence`, `test_detach_waits_for_prompt_before_reloading`, `test_load_replay_excludes_live_prompt_publication`, `test_failed_load_releases_only_unowned_journal`, `test_live_overflow_keeps_sequence_and_failed_load_keeps_cache`, `test_overflowed_session_accepts_later_live_prompts`. |
| C17/C20/C24 | `_auth_context_for` rescoping preserves principal/roles while giving each session its server-generated channel and fresh information-flow state bound to that session. `_UnavailableProvider` fails closed without remote authority. Dead update forwarders fail rather than wedging prompts; unrenderable tool output does not manufacture success or break the entire turn merely during presentation. | Sessions `test_acp_plugin_slots_rescope_without_sharing_session_or_connection_state`, `test_providerless_public_turn_fails_closed_without_remote_authority`, `test_dead_update_forwarder_fails_prompt_without_wedging_session`, `test_unscrubbable_tool_result_does_not_fail_acp_turn`, `test_exact_updates_survive_presentation_pressure`. |
| C20/C22 | Agent `_audit_progress`, `_audit_message`, `_bounded_audit_value` and `_audit_events` keep client-controlled telemetry finite: 256 retained events, text 256 characters, map/list traversal 16 items, map keys 64 characters, depth 3, oversized integer truncation and sensitive-key redaction. Permission outcome events are fixed-size attribution, not an authorization channel or duplicated proxy event. | Sessions `test_audit_mapping_traversal_stops_at_limit`, `test_audit_client_fields_are_bounded_with_scalar_controls`, `test_client_notification_audit_retention_is_bounded`, `test_permission_outcome_event`, `test_permission_outcome_after_trusted_cwd_read`, `test_permission_outcome_after_execute_result`; Proxy `test_session_grant_outcome_visibility`. |
| C23 | `_tool_presentation` applies the same finite mapping at ordinary/orphan starts, bounds detail to 80 characters and exposes only the first parsed shell argv, not the whole command. `_map` treats explicit `is_error` as failure even with a nominal success status; todo plans are all-or-nothing replacements. | Updates `test_finite_tool_presentation_matrix_at_every_start_site`, `test_tool_presentation_edges_at_every_start_site`, `test_shell_presentation_exposes_only_first_argv_at_every_start_site`, `test_every_accepted_success_status_completes`, `test_terminal_failure_is_recorded_before_nominal_success_queue_drains`. |
| C25/C26 | A send failure leaves prepared state replayable. Replay failure after a delivered prefix leaves storage unchanged and a retry replays the whole prepared sequence. Marker failure cannot erase the in-memory fatal fence. Store/journal file validation rejects symlinks, nonregular files, unsafe modes and foreign ownership before output. | Journal `test_send_failure_leaves_prepared_replayable`, `test_replay_send_failure_after_prefix_is_invariant_and_later_retries_all`, `test_replay_validation_failure_remains_fatal_when_marker_fails`, `test_sent_failure_and_marker_failure_preserve_observed_send`, `test_unsafe_root_and_journal_mode_are_rejected`, `test_symlink_and_unsafe_mode_are_rejected`, `test_nonregular_journal_is_rejected_without_output`. |
| C29 | `ProxyRouter._new_server_id` uses a fresh 18-byte random URL-safe token (24-character suffix); session new/load use declared cwd for the hosted binding and do not depend on editor MCP capability. Provider connection/discovery failures install no durable session; repeated retirement frees maps/peers but retains durable replay. | Proxy `test_hosted_server_id_uses_random_required_format`, `test_plain_client_without_mcp_capability_can_call_hosted_hands`, `test_session_new_captures_cwd_for_hosted_operations`; Sessions `test_provider_connect_and_discovery_failures_leave_no_durable_session`, `test_repeated_generation_retirement_releases_maps_and_peers_but_keeps_replay`. |
| C32 | Daemon `_acp_enablement_from_env` is off by default, rejects unrecognized booleans and fails explicit enablement on unsupported peer-credential platforms. `_safe_stat`, `_prepare_directory`, `_remove_stale_socket`, `_peer_uid`, `_unlink_owned_socket` require private owner-verifiable directory/socket (0700/0600), reject symlinks/foreign peers, revalidate stale socket identity and never unlink a live/successor socket. | Daemon `test_acp_is_disabled_without_explicit_truthy_value`, `test_acp_typo_is_startup_error`, `test_explicit_enable_fails_without_peer_credentials`, `test_daemon_creates_owner_only_socket_and_removes_it`, `test_daemon_rejects_symlink_parent`, `test_daemon_rejects_unsafe_existing_socket`, `test_stale_socket_is_revalidated_and_replaced`, `test_live_socket_is_not_unlinked`, `test_stop_does_not_unlink_successor_socket`, `test_peer_uid_fails_closed_when_unverifiable`, `test_startup_failure_rolls_back_owned_socket`. |

### Limits of the baseline, not promises to preserve bugs

* C08 bounds **executing handlers**, not every allocated task: auth-fence
  waiters in `_create_waiter` can accumulate without consuming runner slots.
  The existing C09 tests deliberately prove the latter. A claim of a global
  64-task bound would be false. The new scheduler must bound deferred records
  as backlog and avoid one waiter task per blocked message.
* The single receive loop can block publishing a full inbound queue while
  responses/cancellation are behind it on the stream. Queue timeout retires
  rather than allowing unbounded memory. “Cancellation bypasses auth fence”
  does not mean cancellation bypasses every saturation condition.
* C06 strictness is about envelopes/types/keys, **not** a strict JSON parser:
  wire `json.loads` does not reject duplicate JSON keys or non-finite numbers.
  Journal parsing does reject duplicate keys. No silent claim that the wire
  parser has that stronger property.
* C02 duplicate registration is a live-ID check; abandoned IDs are not an
  all-time nonreuse registry. SDK allocation is monotonic, so supported outgoing
  calls do not reuse them. Likewise C28 proxy tombstones are generation-long,
  whereas C03 store tombstones consume one late reply. Do not merge their rules.
* C13 observes the shielded **close task**, not every possible application task.
  C08's cancellation backstop can detach resistant runners and remove them from
  the SDK supervisor registry. Thus a successful close-task result alone is not
  a global quiescence proof. The owned backend must retain explicit knowledge
  of detached work when reporting retirement; add a resistant-handler admission
  test, not only the existing resistant-close tests.
* C05 permits null outer IDs at the daemon; C28 proxy accepts only string/int.
  Preserve the two measured surfaces rather than “standardizing” them silently.
* These are individual resource bounds, not a proven bound on every byte of a
  direct daemon connection. For example, used MCP connection-ID sets retain
  history until retirement. The proxy has additional cumulative bounds; do not
  assume those protect a client connected directly to the daemon.
* `initialize` currently returns version 1 even when asked for another version
  (Agent `test_initialize_is_exact_stable_v1` parameterizes this). There is no
  separate initialized-state admission flag in `MimirAcpAgent`. Do not silently
  introduce a v2-style negotiation/state machine while preserving v1 behaviour.
* `docs/internal/ACP_TESTING.md` is a historical test/live-harness map, not a
  current count or green interop certificate. It records live denial/interop
  gaps. This design neither runs Docker nor asserts live-editor success.

## 3. What “ACP 1.0” actually changes

### 3.1 Protocol, schema and Python release are different versions

**Source observation:** Python SDK 0.12.0 already bundles
`schema-v1.19.0`; SDK 1.0.0 bundles `schema-v1.23.0`, and both use wire
`protocolVersion: 1` ([S6], [S7], [S8]). The protocol repository's Rust/crate
changelog version is a fourth numbering series ([P4]). Therefore neither
“0.12 protocol → wire 1” nor “all new 1.0 SDK methods are new protocol
requirements” describes this migration.

| Change/status from primary sources | Consequence for mimir |
|---|---|
| **Documented:** stable `session/resume` reconnects without history replay; stable `session/close` cancels work then frees session resources. Both are capability-advertised, optional. They were stabilized in April, already before the 0.12.0 SDK's schema baseline ([P1], [P4], [S7]). Python 1.0 fixes stale unstable router gates; that is an SDK implementation correction, not a newly mandatory protocol feature ([U3]). | Keep current `session/load` replay contract. Do not alias resume to load (resume MUST NOT replay), close to delete (close is active-resource cleanup), or advertise either during the connection cutover. Optional follow-up leaves require the same owner/generation/provider/grant rollback tests. |
| **Documented:** `$/cancel_request` is optional request-level cancellation. A supporting receiver must answer the original request with a valid result or `-32800`; session-specific cancellation remains distinct ([P2]). Stable request cancellation also predates schema 1.19 ([P4]). SDK 1.0 adds missing runtime support ([U2]). | Do not replace `session/cancel`, its durable boundary or MCP's nested `notifications/cancelled`. Decide request-level support explicitly. If enabled, validate typed outer IDs, route only to that connection's request and retain the response-delivery task after handler cancellation. |
| **Documented/source observation:** schema 1.23 introduces session notices/capability, compaction-related draft updates, stable tool-call `name`, stabilized terminal authentication in place of env-var auth, and stabilized/updated elicitation ([R1], [U4], [P4], [S6]). | Existing aliases/discriminators, legacy replay and the exact initialize result must be tested against the new models. Do not advertise notices, compaction, terminal auth or emit tool names as part of a dependency cutover. Mimir uses `AuthMethodAgent`; credential issuance/storage is not being redesigned. |
| **Documented:** elicitation requires explicit form/url mode capability, explicit request mode, connection/verified-user binding, safe decline/cancel/failure, and forbids secret collection via forms ([P3]). **Source observation:** 1.0's accepted content is scalar/string-list values, not arbitrary nested objects; the schema base rejects reserved known discriminator values falling through catch-all variants ([S9], [S10], [U4]). | Elicitation is not mimir's `session/request_permission`. Do not substitute it or broaden approval parsing. It remains unsupported here. If added later, use a separate ownership and URL-security design; SDK model validity does not grant authorization. |
| **Documented:** missing v1 SDK interfaces now cover deletion/logout/providers/MCP/NES/document events. MCP and several other draft methods remain unstable; stable resume/close work without the flag ([U3], [U5], [S8], [S11]). | A larger SDK method table is not mimir's exposure allowlist. Preserve C15/C19's narrow router; do not turn `use_unstable_protocol=True` on globally to get MCP. Retain strict local MCP wrapper validation. |
| **Documented:** experimental v2 is separate, schema `2.0.0-alpha.5`, with negotiation and different init/permissions/updates. Prompt returns on user-message insertion with a non-null message ID, not end-of-turn; replay and null/omission patch semantics differ ([S12], [R1]). | Explicit non-goal. Do not feed v2 through v1 models/journal or infer support from SDK version. Supporting v2 would require a separate session/update/permission design and tests, not another version constant. |

**Source comparison (not a runtime spike):** comparing the two bundled
`schema/schema.json` `$defs` yielded eight additions (`NoticeSeverity`,
`Notice`, `CompactionId`, `CompactionStatus`, `CompactionUpdate`,
`CompactionSummaryChunk`, `CompactionCapabilities`, `NoticeCapabilities`) and
two removals (`AuthEnvVar`, `AuthMethodEnvVar`). The changed definitions
principally concern elicitation, terminal auth, tool calls, session updates
and client capabilities. `ResumeSessionRequest`, `CloseSessionRequest` and
`CancelRequestNotification` already exist in both. This is why the plan
separates schema compatibility tests from new feature implementation
([S6], [S7]).

### 3.2 SDK architecture and upstream rationale

**Documented rationale, limited to what upstream actually said:** PR #132 is
titled “simplify connection handling and remove unused components”; its body
describes removal of unused task classes/imports and corresponding test
updates. It has no issue link or review discussion explaining a supported
replacement for state/queue/dispatcher injection. The 0.12.1 release includes
that PR ([U1], [R2]). Thus the break happened in **0.12.1**, not first in
1.0.0. We found no rationale saying security/resource bounds should move to
the transport, nor a commitment to restore hooks. Do not attribute either to
maintainers.

**Source observations in 1.0.0 `src/acp/connection.py` ([S1]):**

* `Connection.__init__` accepts handler, message transport or writer/reader,
  observers, listening and receive timeout. It no longer accepts `queue`,
  `state_store`, `dispatcher_factory` or `sender_factory`. `_pending`,
  `_incoming`, request allocation and task supervision are owned internally.
* `_receive_loop` calls synchronous `_process_message`. Each request creates
  an `_execute_request` handler task **and** `_run_request` response task;
  each notification creates a task. There is no admission semaphore, queue
  bound or public scheduling policy. “One task per request” understates
  the current two-task request implementation.
* `_track_incoming` binds valid null/int/string IDs to handler tasks;
  invalid bool/float IDs cannot alias valid integer IDs. `_cancel_incoming`
  targets that table, ignores malformed/unknown/finished IDs. Repeated live
  inbound IDs overwrite this internal association rather than being a
  public duplicate-ID policy.
* `_run_request` waits without propagating handler cancellation into response
  delivery, then sends either its result or `request_cancelled()` error.
  `send_request` cancellation removes its future and schedules best-effort
  `$/cancel_request`; it does not wait for remote acknowledgment.
* `_handle_response` ignores invalid IDs and unknown/done responses. It does
  not provide mimir's strict late/duplicate-response refusal or tombstone
  accounting. `send_request` has no public start handle or caller-supplied ID;
  it returns the eventual result.
* EOF `_disconnect` rejects pending outgoing requests. `close` rejects them,
  closes transport and shuts down supervised tasks. `TaskSupervisor.shutdown`
  cancels and awaits each task without a timeout ([S3]); a
  cancellation-resistant handler still needs mimir's bounded retirement
  policy. Generic handler exceptions can become error data containing
  exception text in `_execute_request`, so mimir's own sanitized errors must
  remain at the application boundary.

**Documented rationale for the two-task cancellation design:** PR #158
explains that cancellation previously could leave a peer awaiting a response.
A separate response task handles cancellation before handler start and during
response delivery; closing the connection cancels both. It explicitly limits
the fix to cancellation-cooperative handlers/transports, preserves
`session/cancel`, and calls outgoing cancellation new wire behaviour ([U2]).
**Inference:** this explains the final request-task split, not the earlier
removal of constructor hooks.

**Routing:** `build_agent_router` delegates to
`MessageRouter.from_protocol(Agent, ...)`. Decorated protocol members supply
method, kind, unstable gate, parameter model, optional defaults and adapters.
Only the protocol's declarations become routes, not every method on an
implementation; requests and notifications have separate tables, including
`mcp/message`. `_make_func` validates models and expands snake-case kwargs;
legacy handlers and underscore extensions remain supported ([S4], [S11]).
PR #149 explicitly says this removes duplicated protocol/router registration
and centralizes adapters ([U6]). This is a good code-generation boundary,
not an authentication or concurrency policy.

**Documented API simplification:** 1.0 removes `acp.helpers` and its top-level
builders in favour of direct generated-model construction with discriminator
defaults ([R1]). `_schema_base.BaseModel._include_literal_defaults` retains
those single-valued tags even with exclude-unset serialization ([S9]).
Mimir already constructs schema models directly, but must still test the
serialized tags and aliases rather than pinning old helper/API shape.

**Transports and API stability:** message-level duck typing (`send`,
`receive`, `close`) is an intentional seam in `Connection.__init__`; it
discriminates on whether `reader` is supplied, not runtime protocol matching
([S1], [S2]). `_transport.py` defines that protocol, NDJSON and memory pairs,
but its underscore module is not a new public scheduling API. A custom
transport need not import it. NDJSON `_read_line` accumulates chunks across
reader-limit overruns; it has no mimir-style total byte ceiling. `receive`
skips blank lines, reports malformed/invalid roots and continues; non-empty
batches are unsupported and skipped by `_jsonrpc.require_message_object`,
not processed as strict singleton envelopes ([S2], [S5]).

SDK observers are diagnostics: exceptions are logged and ignored, async
observers become supervised tasks, and outgoing observation happens **after**
transport send. They cannot veto dispatch or synchronously reserve an ID
before send ([S1]). Public `send_request`, `send_notification`, `close`,
`main_loop`, model classes and router entry points are usable APIs;
`_pending`, `_incoming`, `_handler`, `_tasks`, `_process_message` and the
supervisor's registry are implementation details, not safe extension points.
Our current `_handler` rebinding and supervisor-registry surgery must not
be carried forward as alleged public APIs.

Web HTTP/SSE and WebSocket transports are documented as experimental, optional
`[http]` dependencies; they do not become stable because the package is 1.0
([S13], [R1]). Mimir's Unix socket/stdio/SSH topology stays unchanged. The
SDK's `spawn_stdio_transport` bounds stdin closure but its final kill/reap is
still awaited; adopting it is not proof of C14/C32 equivalence
([S13a]). **Inference:** owning transport-independent connection mechanics
fits upstream's simpler architecture, but upstream has not endorsed our
chosen design.

## 4. Options and contract-by-contract assessment

Legend: **P** = can preserve by keeping the current mimir layer; **O** =
implement equivalent in our owned connection; **W** = additional wrapper
machinery/proof required, not established by the public SDK alone;
**G** = unavailable until an upstream API exists. None means “already tested
on 1.0.”

### (a) Public Connection, guards in transport and router

Use a strict custom transport, a bounded ingress pump/ledger, an auth sequencer
and an outer handler wrapper. Reserve execution permits **before** returning
messages to the SDK, not in an already-created handler. Release request
permits only once response send completes/fails; notifications need a wrapper
`finally`. Keep response/cancellation progress independent of data admission.
Mirror outgoing IDs in transport send/receive and wrap every outgoing request
to reserve budget and carry per-invocation context.

This is potentially implementable without reading `_pending`, but needs a
prototype for start failures, cancellations before first send, auth waiters,
and response-task lifetime. Transport observation cannot expose the SDK's
registration future; binding a new handle at `transport.send` relies on the
observed calling-task path in [S1], not a documented start hook. A second
external correlation ledger duplicates much of option (b). Never infer ID
ownership from method/FIFO or use stream observers as interceptors.

**Contracts weakened by a naive implementation:** C02–C04, C07–C09,
C12–C14 (also C18/C21 through lost cancellation ownership). Merely retaining
the router and adding a semaphore would leave unbounded SDK tasks and
permissive response handling ([S1]). Those weakenings require explicit
operator approval; this proposal does not request or assume that approval.
Full option (a) is acceptable only when its wrappers demonstrably eliminate
them and preserve the matrix.

### (b) Own JSON-RPC/connection; use SDK models only — recommended

Own request allocation, registration, pending/tombstone table, ingress
accounting, scheduling, auth ordering, task ownership and retirement.
Retain strict NDJSON/local MCP/permission validation and the `AcpPeer` API.
Use generated v1 models and their public serialization/validation through
`sdk.py`, with an explicit local method table/kwargs adaptation for the small
supported surface. Do not depend on SDK connection, router, task or private
schema-base implementations. Keep the facade's error constructors and
`to_error_obj` payload contract, but make error conversion part of the owned
protocol layer rather than importing an SDK runtime to dispatch exceptions.

**Contracts weakened:** none intended. O rows are implementation obligations,
not permission to delete old behavioural tests. Additional fail-closed
admission and bounded auth-waiter records improve known gaps; new overload
response/retirement policy needs an operator decision, not a hidden change.
The main risk is implementing JSON-RPC/cancellation incorrectly ourselves.

### (c) Propose public extension points upstream

Ask for bounded pre-dispatch admission, exact outgoing-start ownership and
strict correlation policy, plus an explicit disconnect/retirement lifecycle.
Prefer policy interfaces to re-exporting old private task classes. Do not
assume an upstream release, compatibility guarantee or acceptance date.

While waiting, retain 0.12.0: no current contract is weakened, but this is not
a 1.0 migration. A future SDK with agreed hooks could meet all O/W obligations
with less runtime duplication; it must pass the same matrix. Reintroducing
the old hooks alone would not address the baseline's unbounded auth waiters.

### Preservation matrix

For (c), **P/G** means preserved by staying pinned now; gated on a new API
for a 1.0 cutover. Unchanged application code is still regression-tested.

| Contract | (a) public runtime + wrappers | (b) models only | (c) upstream proposal |
|---|---|---|---|
| C01 import/package/version | W: redefine tested public imports | O: schema-only boundary | P/G |
| C02 outgoing bound/unique IDs | W: reserve before SDK call, mirror tombstones | O: authoritative ledger | P/G |
| C03 strict response correlation | W: transport must reject before SDK ignores | O | P/G |
| C04 start ownership | W: caller context → actual transport ID; no observer/FIFO | O: allocate/register before send | P/G |
| C05 NDJSON | P: custom transport | P | P/G |
| C06 envelopes/typed IDs | P: custom transport before SDK | P | P/G |
| C07 queue bytes/items/sentinel | W: bounded pump outside SDK | O: bounded records | P/G |
| C08 active runners/shutdown | W: pre-task permits and response completion accounting | O | P/G |
| C09 auth ordering/cancel lane | W: ingress sequencer, bounded deferred records | O | P/G |
| C10 pre-auth/current physical identity | W: bind outer handler at construction, preserve agent checks | O: explicit bound generation; keep checks | P/G |
| C11 busy/replaced | P: daemon/agent plus C13 proof | P plus C13 proof | P/G |
| C12 EOF/error retirement | W: explicit peer-dead before SDK close | O | P/G |
| C13 uncertain-close admission fence | W: cannot assume SDK task shutdown is bounded | O: explicit retirement result | P/G |
| C14 finite I/O/lifetime | P transport/daemon; W SDK close | P transport/daemon; O close | P/G |
| C15 narrow routes/metadata | W: explicit allowlist around larger SDK table | O: finite local dispatch | P/G |
| C16 session/load transaction | P, test schema adapters | P, test schema adapters | P/G |
| C17 prompt/delivery isolation | P, test C10 binding | P, test C10 binding | P/G |
| C18 cancel boundary/dirty session | P application; W request handle integration | P application; O owned handles | P/G |
| C19 strict MCP/allowlists | P local override, not global unstable flag | P local models/routes | P/G |
| C20 provider discovery/ownership | P, test handle integration | P, test handle integration | P/G |
| C21 immutable permission snapshot | P application; W registration lifetime | P application; O registration lifetime | P/G |
| C22 deny malformed/stale outcomes | P local strict model, not SDK broker defaults | P | P/G |
| C23 update shape/redaction | P, old/new schema golden tests | P, old/new schema golden tests | P/G |
| C24 update bounds/delivery | P, test transport death integration | P, test transport death integration | P/G |
| C25 durable replay ordering | P, validate legacy union compatibility | P, validate legacy union compatibility | P/G |
| C26 persistence failure fences | P | P | P/G |
| C27 proxy frame/key/stdout | P, test new control traffic transparency | P, test new control traffic transparency | P/G |
| C28 proxy IDs/bounds | P, test null/bool/string distinctions end-to-end | P, same tests | P/G |
| C29 hosted generation lifecycle | P, integrate any new cancellation traffic | P, same integration | P/G |
| C30 proxy grant boundaries | P | P | P/G |
| C31 local permission ownership | P | P | P/G |
| C32 operational closure/teardown | P, no SDK subprocess takeover | P, no runtime dependency creep | P/G |
| C33 session-owned ingest acknowledgment | P: keep distinct snapshot/ordinal/audit exchange | P: same application guard and wire | P/G |

| Dimension | (a) | (b) | (c) |
|---|---|---|---|
| Security posture | Two ledgers/schedulers with a hard-to-observe boundary; guards must run before SDK task creation, not after. | One authoritative connection ledger and scheduler; larger owned security-critical code surface. | Best if upstream accepts precise invariants; no protection gained by an unanswered request. Staying pinned retains existing limitations. |
| Upstream tracking | Medium/high: behaviour of transport call context, send/cancel ordering, hidden tasks and router changes must be rechecked each release. | Medium: schemas/aliases/serialization and protocol semantics still tracked; no private runtime coupling. | Unbounded wait and coordination cost; future interfaces need conformance tests/review. |
| Testability | Good wire tests; difficult ownership/task-lifetime proof without private assertions. | Deterministic owned state plus raw-wire integration; clear mutation targets. | Existing baseline testable; hypothetical APIs cannot be certified. |
| Migration risk | Smaller-looking diff, substantial wrapper complexity and cancellation races. | Larger explicit rewrite, easier staged differential testing and rollback. | Lowest immediate code risk, highest schedule/continued-pin risk. |

Subclassing/monkeypatching `Connection._process_message`, borrowing
`TaskSupervisor._tasks` or vendoring a private SDK connection are not a fourth
“public API” option. They recreate the upgrade trap and require separate
operator approval.

## 5. Recommended architecture

**Proposal, not current implementation:**

```text
existing Unix/stdio transport
  → strict byte framing + singleton envelope validation
  → owned receive/classification loop
      → outgoing response/tombstone correlation (no application runner)
      → bounded cancellation/control handling
      → 64-item / 8-MiB data backlog (auth-deferred records included)
  → bounded scheduler (64 executing handlers, request response delivery owned)
      → physical-generation binding + auth sequencing
      → explicit v1 method/parameter adapters
      → existing MimirAcpAgent and narrow route_mcp
  ← serialized bounded writes, public SDK model serialization only
```

The connection owns monotonic IDs and a start primitive returning
`AcpRequestHandle` once its own future is registered **before any await/send**.
The existing `ContextVar` handshake can then become an implementation detail
of the old backend rather than a dependency on SDK send internals. Send
failure, close, cancellation before/after registration and a late response
have one authoritative state transition.

Proposed code boundaries for the implementation brief (no files added here):

| Module | Responsibility |
|---|---|
| `mimir/acp/connection.py` (new) | Owned transport-independent JSON-RPC ledger, bounded record scheduler, task/retirement ownership and protocol error representation. No `acp` imports; accept a normalized async method handler, not `MimirAcpAgent` or runtime services. |
| `mimir/acp/sdk.py` (existing facade) | Sole schema import boundary, explicit v1 method/parameter/result adaptation, strict local MCP/permission models, `AcpPeer`/handles and stream composition. Preserve caller-facing names while replacing SDK connection construction. Re-export owned error types if needed to avoid changing callers gratuitously. |
| `mimir/acp/agent.py` | Application/session authorization and durable cancellation. Replace private handler rewriting with an explicit generation-bound handler/context supplied during composition. Do not move principal/session/epoch decisions into generic JSON-RPC. |
| `mimir/acp/daemon.py` | Single-peer admission and watchdog; consume an explicit clean/unretired/failed retirement report without guessing from SDK-private tasks. |
| Updates/journal/proxy/transport modules | Keep their separate queues, durability, grant and I/O responsibilities; change only approved integration seams, not execution/confinement policy. |

Dependency direction is `sdk` → owned connection, not connection → `sdk`;
normalize model results/errors at the facade boundary to avoid an import
cycle. Preserve the existing protocol-facing tests while adding focused tests
of the owned connection state machine.

Admission must precede task allocation. Waiting on authentication stores
bounded records, not coroutine tasks. Keep authentication ordered against
previous work while allowing owned session cancellation to make progress.
Control traffic must be size/ownership validated, not a free unlimited task
lane. At data saturation, process reachable correlated replies/control without
spawning data tasks; when no bounded capacity remains, retire fail-closed
within the existing queue timeout rather than buffering unboundedly. The
precise control reserve and overload wire response are operator questions.

Preserve notification-no-response and typed-ID semantics. If request-level
cancellation is enabled, separate its handler lifetime from exactly-one
response delivery, following [P2], not by importing [S1]'s private methods.
Only the request's physical connection can name its task; session cancellation
still runs C18's owner/generation checks and durable cleanup. Do not let
generic request cancellation interrupt `authenticate` midway and install
partial authority: validate/stage auth state and commit it atomically, or
decline support for cancellation of that method under the optional policy.

Retirement is explicit: mark peer dead → revoke generation authority and
pending requests → bounded cleanup → report clean/unretired/failed to daemon.
Do not count a detached task as retired just because the runner returned.
Retain daemon admission fencing when clean retirement cannot be established.
Application update queues and journal sequencing remain separate mechanisms,
not folded into a new connection queue.

Schema use is not schema trust: retain local strict MCP/permission models and
raw envelope validation before generated-model parsing. SDK 1.0 generated
models intentionally have compatibility defaults, including malformed `_meta`
falling back to defaults ([S9], [S10]); that is not suitable as an integrity
default at mimir's authority boundaries.

## 6. Leaf-sized migration plan (only after operator approval)

All leaves run the unchanged core and Cultivate gate. Until L7, production
continues to pin 0.12.0; experimental 1.0 tests use an isolated **external**
environment, not an uncommitted pin change. No dual live daemon rollout or
deployment/Docker action is delegated to builders.

L1 characterizes known baseline gaps and specifies their discriminators;
new stricter assertions land with their implementing L3/L4 code, not as
failing or permanently skipped tests on 0.12. L5 runs the model/wire/replay
fixtures in scratch, not the intentionally 0.12-pinned packaging/version
assertions. Each leaf must leave the normal repository suites green.

| Leaf | Deliverable and acceptance criteria | Named evidence / mutations |
|---|---|---|
| L1 — freeze executable contracts | Turn C01–C33 into backend-independent wire fixtures. Separate exact-version/API-shape assertions from behavioural assertions; record baseline output. Add passing baseline tests for duplicate outgoing registration, final EOF frame and parse-counter reset; specify bounded auth-waiter/control-pressure tests for L3. No runtime/pin change. | Existing tests named per row plus new `test_duplicate_outgoing_registration_is_refused`, `test_final_frame_and_parse_error_reset`; proposed L3 names `test_auth_fence_backlog_is_bounded`, `test_control_progress_under_data_pressure`. G1–G5 below, applied when their enforcement changes. |
| L2 — own outbound ledger/handles | Implement allocation, pre-send registration, 64-entry combined pending/tombstone bound, exact per-invocation handles, direct-send failure and single late-reply quarantine behind an unselected test backend. Accept only C02–C04 equivalent outcomes; no production backend switch. | SDK's same-method/pre-registration/close tests, exact outer-ID MCP cancel test; G1/G2. |
| L3 — own bounded ingress/scheduler | Add byte-accounted bounded records, reserved shutdown capacity, bounded auth-deferred records, runner cap and bounded stop; no per-message waiting task. Demonstrate FIFO data handling and owned cancellation progress while auth is waiting. Decide overload behaviour before implementing it. | C05–C09 tests plus L1 pressure tests; G3–G5. Memory/task assertions scope only to the owned connection, never global `asyncio.all_tasks()`. |
| L4 — explicit routing and generation lifecycle | Add finite method/param adapters, owned error conversion and construction-time physical generation binding; remove need to rewrite `_handler` or inspect supervisor task sets in the new backend. Preserve C10–C20, including no SDK auto-exposure, response/error codes, exactly-once response delivery and uncertain-retirement admission fences. New runtime remains unselected. | Agent auth/cancel/router tests, Daemon failed-close matrix, Sessions replacement/provider/cancel tests; G6–G10. Add `test_detached_handler_keeps_generation_unretired` (new) to cover the baseline retirement gap. If enabling `$/cancel_request`, add `test_request_cancel_keeps_one_response_and_owned_cleanup` and malformed/wrong-direction/typed-ID negative controls. |
| L5 — schema 1.23 compatibility rehearsal | Run strict local wrappers, v1 aliases/discriminators, golden wire updates and old durable-journal fixtures against SDK 1.0.0 in scratch. Reject permissive SDK parsing at authority boundaries. No capability additions, no journal format rewrite and no production pin change. | SDK MCP/meta/permission/update tests, Journal legacy-ID/full-file-validation tests; G9/G11/G12. Add `test_v123_replays_legacy_journal_without_rewrite`, `test_new_optional_methods_remain_unadvertised`. |
| L6 — integration and adversarial equivalence | Exercise new connection with real in-process streams, actual agent/session/journal/proxy (not mocked send-only peers). Preserve C21–C33: late permission after cancel, provider result races, EOF quiescence, grants, ingest acknowledgments, bounded writes and stdout. Differential traces may differ only in operator-approved request-level cancellation/overload changes. | All inventory files; specifically Shutdown EOF/approval/session-transition cases, Proxy raw transparency/typed namespaces, Sessions registration/ingest-boundary tests; G10–G15. Produce per-contract results before cutover. |
| L7 — atomic 1.0 cutover | Select owned backend and update dependency/lock to exactly the approved SDK version in one implementation leaf. Replace/remove only tests pinning 0.12 **implementation shape**, not behaviours. Require installed wheel/sdist/import-closure checks and both complete suites. No unrelated optional features. | Replace version equality in SDK `test_pinned_distribution_protocol_and_schema_boundary` and Packaging `test_acp_and_mcp_dependency_declarations`; recast `test_installed_acp_public_symbols_used_by_sdk`, `test_pinned_schema_aliases_and_handler_models`, `test_pinned_agent_signatures_and_error_objects` for schema-only imports/local adapters. Migrate tests that construct SDK `Connection(...state_store/queue/dispatcher_factory...)` to the owned test backend. Retain all C02–C33 negative assertions and run G1–G15. |
| L8 — cleanup and maintenance | After green cutover and operator acceptance, remove old 0.12-only backend/start handshake tests and obsolete implementation assertions, keep historical replay fixtures and preservation tests. Lift the obsolete Dependabot ignore at the end, retain dedicated protocol grouping, and document owned schema/runtime upgrade review. | Packaging/dependency-closure/installed-artifact probes and both suites. Assert no `0.12.0` equality/hook requirement remains except intentional history/fixtures. Remove the `agent-client-protocol` ignore entry from `.github/dependabot.yml` regardless of its version range (currently `>=0.12.1, <1.0.0` plus exactly `1.0.0`, so later 1.x releases still open review-only PRs), together with its pinning test, `tests/test_dependabot_config.py::test_acp_held_at_0_12_until_redesign` (after #129). Keep the `agent-protocols` group. |

**Rollback:** before L7, only the old backend runs. If L7 fails, do not publish
the dependency/backend switch. After rollout, revert the entire cutover
dependency/lock/backend selection together under operator control; do not
mix new runtime internals with the old pin. No journal-format change means
rollback does not require a data rewrite. Optional capabilities stay off,
avoiding clients relying on a feature the rollback cannot serve.

### Required fail-closed mutation ledger for implementation leaves

This documentation-only issue changes **no executable guard**, so there is no
new branch to mutate and no production mutation was made. The following is
the acceptance ledger for implementation, not a claim that mutations ran.
For each applicable leaf: first commit the correct implementation; make one
mutation, confirm the actual diff landed, run the named discriminator and
require failure, restore only that mutation, prove the original file content
and rerun the test. A timeout alone is not proof of the intended assertion.
Semantically equivalent/unreachable mutations require an explanation, not
fabricated evidence. Never discard unrelated work while restoring.

| Guard | Deliberately permissive mutation | Required failing discriminator |
|---|---|---|
| G1 C02/C03 | Change `>= MAX_PENDING_REQUESTS` to `>`; drop tombstones from count; delete duplicate/unknown-response refusal, separately. | SDK `test_pending_request_limit_admits_exact_boundary`, `test_abandoned_request_tombstones_share_pending_limit`, `test_request_store_close_unknown_duplicate_and_abandon_contract`; L1 duplicate-registration test. |
| G2 C04/C18 | Replace per-invocation ownership with method/FIFO pairing; omit abandon on pre-registration cancellation; use another handle's outer ID in nested cancellation, separately. | SDK `test_request_registration_is_per_invocation_under_same_method_concurrency`, `test_request_cancelled_after_prepare_before_registration_leaves_no_stale_pairing`, `test_mcp_handle_cancel_notification_uses_exact_outer_id_and_retains_connection`. |
| G3 C05/C06 | Decode before size check; allow bool/float ID or extra/error keys; remove parse-error retirement, separately. | SDK `test_ndjson_frame_limit_is_counted_before_decode`, `test_strict_outer_envelopes_and_duplicate_or_late_responses_fail`; Transport `test_repeated_parse_errors_hit_connection_limit`; L1 EOF/reset controls. |
| G4 C07/C08 | Admit one extra item/byte; release accounting before commit; remove reserved sentinel slot; omit runner semaphore, separately. | SDK input queue equality/expiry/close tests and `test_live_connection_bounds_active_inbound_runners`; L1 bounded auth-backlog test. |
| G5 C09 | Dispatch later stateful work before authenticate completes; make auth waiters hold runner slots; put session cancellation behind auth, separately. | SDK `test_buffered_stateful_request_waits_for_authentication`, `test_authentication_waiters_do_not_consume_runner_slots`; Sessions `test_reauthenticate_fence_still_dispatches_session_cancel`; L1 pressure-control test. |
| G6 C10/C15 | Default missing auth to authorized; remove current-generation/principal equality after await; expose all SDK methods, separately. | Agent `test_stateful_methods_require_authentication_and_connection`, `test_factory_output_is_revalidated_before_storage`, `test_overlapping_connections_keep_authentication_and_dispatch_physical`, `test_agent_exposes_no_out_of_scope_handlers`; L5 unadvertised-method test. |
| G7 C11/C13 | Admit at capacity; treat failed/pending close as clean (`return False` → `True` or exception ignored); release admission merely on task done, separately. | Daemon `test_single_peer_cap_refuses_second_with_actionable_error`, `test_second_peer_is_refused_while_an_old_close_is_still_pending`, `test_an_abandoned_close_that_fails_keeps_admission_closed`, first-grace/shield/forced-cancel tests in C13. |
| G8 C12/C14 | Skip EOF death marking; drain dead peer; remove finite shutdown/write timeout, separately. | SDK `test_runner_retires_failed_transport_before_cancelling_handlers`, `test_ndjson_send_bounds_non_draining_writer`; Daemon `test_eof_retires_write_only_inflight_peer_before_dispatcher_drain`, `test_total_shutdown_deadline_never_awaits_stuck_cleanup`; Transport deadline witness tests. |
| G9 C19/C20 | Accept reused/unknown connection; relax manifest equality or result `structuredContent`; drop generation/epoch from progress ownership, separately. | SDK `test_mcp_response_validation_reuse_and_routing_fail_closed`; Sessions `test_admission_rejects_every_mismatch_with_exact_top_level_message`, `test_discovery_requires_exact_unique_rows_and_retains_used_ids`, `test_progress_token_collision_cleanup_preserves_successor_ownership`; `tests/test_client_provider.py`. |
| G10 C16–C18/C21/C22 | Delete session-owner/current-prompt/epoch equality, snapshot equality or post-reply currentness; map error/cancel to allow, separately. | Agent foreign-generation/principal cancel tests; Sessions `test_active_prompt_permission_uses_admitted_snapshot_and_is_once_scoped`, `test_cancel_boundary_prevents_permission_and_mcp_registration`, `test_cancel_timeout_dirties_execution_and_requires_fresh_load`; SDK `test_permission_completion_exposes_cancel_and_errors_without_execution`; L4 request-cancel cleanup test if supported. |
| G11 C20–C24 | Trust malformed reserved metadata; permit unknown permission option; replace frozen snapshot with mutable input; stop scrubbing/suppression or allow queue overflow; remove audit traversal/retention caps, separately. | SDK malformed-permission/meta matrix; Updates snapshot, redaction/suppression and exact item/byte boundary tests in C21–C24; Sessions audit traversal/scalar/retention tests in C20. |
| G12 C25/C26 | Send before prepared fsync; stream replay before full validation; default corrupt state to replayable; drop fatal cache fence; unlink before marker, separately. | Journal `test_publish_orders_prepared_fsync_send_sent_fsync`, `test_deferred_replay_validates_entire_file_before_output`, `test_cache_release_keeps_fatal_fence_until_marker_is_durable`, marker/prepare/sent failure and TTL/delete tests in C26. |
| G13 C27–C29/C32 | Stop overwriting reserved auth metadata; accept oversized frame/duplicate ID; intercept foreign provider IDs; omit EOF quiescence or stdout reservation, separately. | Proxy auth/frame/typed-ID/foreign-frame/interception tests; Shutdown `test_daemon_eof_quiesces_inflight_session_transition`, `test_daemon_eof_quiesces_inflight_allow_session_response`; stdio direct-fd test and dependency-closure mutation tests. |
| G14 C30/C31 | Cache grant before forward; allow tainted reuse; delete session/wrapper/generation/ingest equality; treat missing approval or stale owned task as trusted, separately. | Proxy grant-scope, ingest-revocation, taint, malformed-response and scope-owner tests in C30/C31; Shutdown `test_close_cancels_inflight_allow_session_response_before_clearing_grants`. |
| G15 C33 | Treat error/cancel/allow_session as ingest approval; remove expected snapshot/ordinal or session-state identity; ignore durable-audit refusal, separately. | Sessions `test_acp_pending_trust_allows_trusted_arrival_rejects_new_untrusted`, `test_acp_ingest_acknowledgement_is_not_shared_between_sessions`, `test_human_ingest_trust_persists_for_acp_session_until_new_source`; add `test_ingest_permission_error_and_audit_failure_do_not_clear` (new) for explicit error/audit negative controls. |

Preserving a guard without changing its code still requires regression tests
in the integration leaf. Mutations concentrate on moved/changed enforcement
branches, including each equality in composite ownership predicates, with a
positive control proving the valid path still works.

## 7. Operator questions and explicit decision boundaries

1. Approve model-only SDK use and ownership of JSON-RPC/scheduling, or fund
   the more complex public-Connection wrapper prototype first? No current
   guard may be weakened merely to reduce the diff.
2. Keep only the existing optional `session/cancel`/MCP cancellation at first
   cutover, or add optional `$/cancel_request` in L4? If adding it, which
   methods are cancellable, how does prompt cancellation map to C18, and how
   is partially completed authentication protected? The protocol permits
   declining request-level cancellation support ([P2]).
3. Approve a bounded control reserve and fail-closed overload retirement?
   Exact reserve size, maximum retained bytes and whether an admitted
   overloaded request receives a generic error before close must be chosen
   before L3. Do not silently claim today's queue bounds cover auth waiters.
4. Is the existing daemon-null-ID versus proxy-no-null-ID discrepancy to
   remain? Recommendation: preserve now; resolve in a separate approved
   compatibility leaf if desired.
5. Should wire duplicate-key/non-finite-JSON rejection become a stronger
   contract? It is not today's envelope validator contract. Recommendation:
   a separate explicit hardening decision and fixtures, not incidental parser
   replacement.
6. Confirm resume/close/elicitation/v2/notices/compaction remain unadvertised
   during cutover. New optional protocol features require distinct stories
   (especially replay-free resume and resource-freeing close).
7. Who owns protocol/schema review and installed-artifact regression review
   after cutover? A models-only SDK dependency still needs deliberate updates.
8. Approve filing the upstream draft below? Nothing has been filed here.
   There is no deadline or upstream acceptance assumption.
9. If request-level cancellation is enabled, approve refusal of duplicate live
   **inbound** request IDs rather than overwriting cancellation ownership?
   Today's strict duplicate-registration policy is outbound. This new inbound
   policy needs a named test and explicit compatibility decision.

### Upstream issue draft (not filed)

**Title:** Public pre-dispatch admission and outgoing request ownership seams
for bounded ACP agents

We embed Python SDK 0.12.0 behind a strict NDJSON transport in mimir.
We cannot migrate its constructor policies to 1.0.0: PR #132 removed
`state_store`, `queue`, `dispatcher_factory` and `sender_factory`, and
`Connection._process_message` now creates handler/response tasks internally.
We understand the simpler connection architecture and #158's need to deliver
a response even when a handler is cancelled.

Our required invariants are:

* bounded pending requests **including abandoned-response tombstones**;
* per-invocation outgoing start returning the allocated outer ID and result
  future before send (including concurrent requests of the same method);
* bounded inbound retained items/bytes and execution slots, with admission
  before task creation, an auth-ordering fence and cancellation progress;
* optional strict refusal of unknown/duplicate responses, rather than ignore;
* EOF/error notification before draining, plus a bounded-close/retirement
  result so an outer daemon can refuse replacement admission while cleanup
  remains uncertain.

Would you consider documented policy seams for admission, outgoing start and
response correlation, and disconnect/retirement lifecycle? We are not asking
to make old private task classes public or to impose strict policies on all
SDK users. A handler semaphore cannot bound tasks already created, and an
observer called after send cannot supply pre-send request ownership.

Suggested conformance cases: 65th pending/tombstoned request; same-method
concurrent start inversion; cancel/close before start registration; unknown
and second late response; inbound flood while authenticate waits on a prompt;
cancel during response send; EOF plus cancellation-resistant cleanup while
a replacement peer attempts admission.

If the intended supported boundary is instead a message transport plus an
application-owned connection using generated schema only, documenting that
would also help. Is transport call-context preservation a supported contract,
and are bounded scheduling hooks planned? We can supply a minimal reproduction
and tests after agreeing the API direction.

## 8. Sources, source probes and verification

### Primary sources read

* [R1] Python SDK 1.0.0 release notes/changelog (including migration notes and
  linked PRs); [R2] 0.12.1 release identifying the hook-removal refactor.
* [U1] #132 is the entire available stated hook-removal rationale; its API
  record reports zero comments/review comments. No undocumented motive is
  treated as fact.
* [U2] #158 explains handler/response task separation and request cancellation;
  [U3] #159 distinguishes stable lifecycle routing from schema availability;
  [U4] #146 explains schema 1.23 regeneration; [U5] #148 and [U6] #149 explain
  missing method coverage and metadata-derived routing.
* [P1]–[P4] are specification-repository sources, not SDK release summaries.
  In particular P1's general stdio-MCP requirement is broader than mimir's
  deliberately restricted client-provider support; this design does not
  claim full ACP feature compliance or authorize arbitrary client processes.

### Reproducible source probes run on 2026-11-18

The tool-based HTTP reads are equivalent to the following read-only commands;
the evidence here is the returned HTTP body/status, not a claim these exact
`curl` commands were executed in a shell:

```sh
curl -f https://pypi.org/pypi/agent-client-protocol/1.0.0/json
curl -f 'https://api.github.com/repos/agentclientprotocol/python-sdk/tags?per_page=15'
curl -f https://raw.githubusercontent.com/agentclientprotocol/python-sdk/1.0.0/src/acp/connection.py
curl -f https://api.github.com/repos/agentclientprotocol/python-sdk/releases/tags/1.0.0
curl -f https://api.github.com/repos/agentclientprotocol/python-sdk/releases/tags/0.12.1
curl -f https://api.github.com/repos/agentclientprotocol/python-sdk/pulls/132
curl -f https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/schema-v1.23.0/docs/protocol/v1/cancellation.mdx
```

Results: all above HTTP 200. PyPI reports 1.0.0 uploaded 2026-11-09, wheel
SHA256 `179c14266997841438ee15245a2668d31b2c695c13bff87d01c36e016185ddd0`,
sdist SHA256
`24914d9996aaeeb08931191382d0ca1028b83743e2797cc05ded39b3bb754425`.
No wheel was installed into this checkout. Initial guesses of tag `v1.0.0`
and a root `CHANGELOG.md` in python-sdk returned 404; tag discovery and GitHub
release bodies resolved those paths. No primary source was reconstructed
from memory.

The actual Code Mode source comparison used `fetch` on both pinned SDK
`schema/schema.json` URLs, parsed JSON, compared `$defs` keys and serialized
definition values, and selected lifecycle/elicitation/auth/tool-call
definitions for inspection:

```javascript
const tags = ["0.12.0", "1.0.0"];
const schemas = await Promise.all(tags.map(async tag => {
  const r = await fetch(
    `https://raw.githubusercontent.com/agentclientprotocol/python-sdk/${tag}/schema/schema.json`
  );
  if (!r.ok) throw new Error(`${tag}: ${r.status}`);
  return await r.json();
}));
const [a, b] = schemas.map(x => x.$defs);
const added = Object.keys(b).filter(k => !(k in a));
const removed = Object.keys(a).filter(k => !(k in b));
const changed = Object.keys(b).filter(k =>
  k in a && JSON.stringify(a[k]) !== JSON.stringify(b[k]));
```

Result: the eight additions/two removals reported in §3.1; changed definitions
include elicitation's stabilization, auth variants, tool call name and session
updates. This probe evaluates **source data**, not installed Python behaviour.
`Connection.__init__`, `_process_message`, `_handle_response`, `send_request`,
router, transport and supervisor findings were read directly from pinned
source. No runtime/performance result is inferred from a test name.

**Runtime spikes:** none completed or committed. The attempted scratch setup
command
`mktemp -d /var/lib/mimir-worklink/daemon/tmp/opencode/acp2170-XXXXXX`
was denied by shell permissions before creating an environment. It was not
retried through another mechanism. This is not a missing primary source:
network reads of the required PyPI, specification and SDK sources succeeded.
All 1.0 runtime behaviour claims above therefore cite source, not a fabricated
spike. Future L5 runtime rehearsals must use a permitted external scratch
directory, `uv venv <scratch>/.venv` and
`uv pip install --python <scratch>/.venv/bin/python agent-client-protocol==1.0.0`;
record commands, version, tests and results there and commit no spike code.

### Documentation-only verification

Only this design document is changed. No production code, dependency pin,
lockfile, tests or Dependabot configuration is changed. There are no changed
security branches to mutation-test in this leaf; §6 supplies implementation
mutation obligations rather than misrepresenting source inspection as tests.

Required gate (run from this checkout, not a deployment):

```sh
sh scripts/heavy_run.sh env -u MIMIR_PLUGINS uv run --all-packages --extra dev --extra bench pytest -q -n 6 && sh scripts/heavy_run.sh env MIMIR_PLUGINS=cultivate uv run --all-packages --extra dev --extra bench pytest -q -n 6 tests plugins/cultivate/tests
```

**Gate result (2026-11-18): both suites passed.** The core invocation
completed successfully, allowing the `&&`-chained Cultivate invocation to
run. Cultivate reported **31,677 passed, 92 skipped, 4,189 warnings in
824.60 s**. The completion notification retained the Cultivate summary but
truncated the earlier core summary, so no unobserved core count is claimed.
Warnings included deprecations, aiohttp key recommendations and an unraisable
subprocess-transport cleanup warning; no test failure was reported. Live
deployment and Docker checks are not part of this document's evidence.

Advisory scoped check actually run:
`uv run pytest -q -n 0 tests/test_acp_packaging.py` → **9 passed** (0.88 s).
The dependency pin and test files remained unchanged.

[S1]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/connection.py
[S2]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/_transport.py
[S3]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/task/supervisor.py
[S4]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/router.py
[S5]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/_jsonrpc.py
[S6]: https://github.com/agentclientprotocol/python-sdk/tree/9853d8b514d6340000bcbd01ec07808436db80cf/schema
[S7]: https://github.com/agentclientprotocol/python-sdk/tree/750c6ee7216d527cedcbd5b33b4e6178138e9f04/schema
[S8]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/meta.py
[S9]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/_schema_base.py
[S10]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/schema.py
[S11]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/interfaces.py
[S12]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/docs/experimental-v2.md
[S13]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/docs/web-transport.md
[S13a]: https://github.com/agentclientprotocol/python-sdk/blob/9853d8b514d6340000bcbd01ec07808436db80cf/src/acp/transports.py
[R1]: https://github.com/agentclientprotocol/python-sdk/releases/tag/1.0.0
[R2]: https://github.com/agentclientprotocol/python-sdk/releases/tag/0.12.1
[U1]: https://github.com/agentclientprotocol/python-sdk/pull/132
[U2]: https://github.com/agentclientprotocol/python-sdk/pull/158
[U3]: https://github.com/agentclientprotocol/python-sdk/pull/159
[U4]: https://github.com/agentclientprotocol/python-sdk/pull/146
[U5]: https://github.com/agentclientprotocol/python-sdk/pull/148
[U6]: https://github.com/agentclientprotocol/python-sdk/pull/149
[P1]: https://github.com/agentclientprotocol/agent-client-protocol/blob/6d08f412a7a1370d3cc9a124e3be3d6acf92641e/docs/protocol/v1/session-setup.mdx
[P2]: https://github.com/agentclientprotocol/agent-client-protocol/blob/6d08f412a7a1370d3cc9a124e3be3d6acf92641e/docs/protocol/v1/cancellation.mdx
[P3]: https://github.com/agentclientprotocol/agent-client-protocol/blob/6d08f412a7a1370d3cc9a124e3be3d6acf92641e/docs/protocol/v1/elicitation.mdx
[P4]: https://github.com/agentclientprotocol/agent-client-protocol/blob/6d08f412a7a1370d3cc9a124e3be3d6acf92641e/CHANGELOG.md
