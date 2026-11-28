"""Small, allowlisted retry defaults for provisioned toolchains."""
from __future__ import annotations

import os


def network_retry_environment() -> dict[str, str]:
    """Keep operator settings; bound Cargo's retries and per-request timeout.

    Five retries with 30-second HTTP timeouts leave room for backoff within the
    order prompt's five-minute recovery budget. That budget covers command
    re-runs too; these settings alone are not a whole-command deadline.
    """
    return {
        name: os.environ.get(name, default)
        for name, default in (
            ("CARGO_NET_RETRY", "5"),
            ("CARGO_HTTP_TIMEOUT", "30"),
        )
    }
