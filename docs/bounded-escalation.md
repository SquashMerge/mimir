# Autonomous escalation

`operator_alert` and Cultivate's `unsupported_operation` have an escalation-only
authorization path for internal poller and `repo_test_complete` turns. A typed,
host-bound PR scope carried by the turn's auth context is sufficient; live
checkout leases and write grants are deliberately not required. Completion auth
inherits the queueing turn's service identity and scope through the existing
same-channel in-process continuation carrier. Recovered jobs without that carrier
gain no authority.

Alerts go only to the configured operator destination. A service with the explicit
alert capability can also alert without a PR scope. Unsupported-operation reports
must name an exact host-bound repository and PR. Neither tool mints grants.

Only autonomous turns with **live untrusted active ingest** withhold caller prose.
Trusted poller and completion turns retain their alert text, report descriptions
and attempted operations under the existing notification/forge IFC sink rules.
The decision includes live turn taint, not just the original auth snapshot, and
content withholding applies in both shadow and enforced modes.

Tainted alerts and reports carry fixed prose plus host-derived turn ID, trigger,
bound repository/PR/head (when present), admitted completion job ID (when present),
and the latest registered blocker tool and machine-shaped refusal code. Caller
arguments and exception bodies are not diagnostic fields. Tainted reports omit
attempted operations. Their deduplication key uses blocker tool/reason, excluding
turn/job IDs so repeated polls remain deduplicated while distinct blockers remain
visible. Trusted reports continue deriving their key from the description.

Only this bounded-content path bypasses the general notification/forge IFC sink
check, not the turn's labels: taint is preserved, and other tools still pass their
normal authorization and IFC gates. Existing alert length/rate and unsupported
report deduplication bounds remain in force.
