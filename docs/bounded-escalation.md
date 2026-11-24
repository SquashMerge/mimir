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

In these autonomous turns both implementations **withhold all caller prose**,
regardless of integrity: alerts use a fixed instruction to inspect the turn trace;
unsupported-operation reports use a fixed description and empty attempted-operation
list. The latter's repository, PR, head and scope identifiers come from the host
scope. This fixed-content path bypasses the general notification/forge IFC sink
check, not the turn's labels: taint is preserved, and other tools still pass their
normal authorization and IFC gates. Existing alert length/rate and unsupported
report deduplication bounds remain in force.
