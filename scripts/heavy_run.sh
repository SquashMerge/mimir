#!/bin/sh
# Serialize heavy commands across hosts/containers sharing a writable directory.
# MIMIR_HEAVY_LOCK_FILE=/shared/heavy.lock uses /shared/heavy.lock.lease, acquired
# by atomic mkdir. All participants must use this protocol: old lockf callers
# don't interoperate, and kernel locks don't cross a VM bind mount.
#
# The Python helper records host, pid, start time and timeout for humans. Only
# its heartbeat counter determines staleness: waiters measure unchanged counters
# on their OWN monotonic clocks, never comparing host timestamps. Set the stale
# threshold above expected suspend durations (default 600 seconds).
# Stale leases are renamed before deletion, with a per-generation breaker claim
# so delayed waiters cannot rename a replacement lease. A breaker SIGKILLed after
# claiming but before renaming leaves a fail-closed lease needing manual removal.
#
# The command has its own process group. Heartbeats stop when it exits; on INT or
# TERM the helper kills that group before releasing. Residual limitation: if the
# helper is SIGKILLed while its child lives on, heartbeats stop and a waiter can
# eventually break the lease while that orphan still runs. Kill the orphan before
# restarting heavy work. No userspace lease can clean up after its own SIGKILL.
#
# Unset/empty LOCK_FILE means no lease. Nonempty LOCK_HELD means an ancestor owns
# it: run directly to avoid deadlock. Otherwise unusable paths or missing tools
# fail closed. The timeout covers both queueing and execution.
#
# Environment:
#   MIMIR_HEAVY_LOCK_FILE       shared path (parent must exist and be writable)
#   MIMIR_HEAVY_LOCK_TIMEOUT    overall timeout seconds (default 7200)
#   MIMIR_HEAVY_STALE_SECONDS   unchanged-counter threshold (default 600)
#   MIMIR_HEAVY_HEARTBEAT_SECONDS  heartbeat interval (default 1, < stale)
#   MIMIR_HEAVY_TIMEOUT_TOOL    override gtimeout (macOS) / timeout (Linux)
#   MIMIR_HEAVY_GTIMEOUT        legacy timeout-tool override, still honored
#   MIMIR_HEAVY_PYTHON          Python 3 interpreter (default python3)
set -eu

if [ "$#" -eq 0 ]; then
    echo "usage: scripts/heavy_run.sh <command> [args...]" >&2
    exit 64
fi

if [ -z "${MIMIR_HEAVY_LOCK_FILE:-}" ] || [ -n "${MIMIR_HEAVY_LOCK_HELD:-}" ]; then
    exec "$@"
fi

timeout_bin="${MIMIR_HEAVY_TIMEOUT_TOOL:-${MIMIR_HEAVY_GTIMEOUT:-}}"
if [ -z "$timeout_bin" ]; then
    case "$(uname -s)" in
        Darwin) timeout_bin="$(command -v gtimeout || command -v timeout || true)" ;;
        *) timeout_bin="$(command -v timeout || command -v gtimeout || true)" ;;
    esac
fi
if [ -z "$timeout_bin" ] || ! command -v "$timeout_bin" >/dev/null 2>&1; then
    echo "heavy_run: configured lease requires gtimeout or timeout (MIMIR_HEAVY_TIMEOUT_TOOL)" >&2
    exit 1
fi
python_bin="${MIMIR_HEAVY_PYTHON:-python3}"
if ! command -v "$python_bin" >/dev/null 2>&1; then
    echo "heavy_run: configured lease requires Python 3 (MIMIR_HEAVY_PYTHON)" >&2
    exit 1
fi
exec "$timeout_bin" -k 10s "${MIMIR_HEAVY_LOCK_TIMEOUT:-7200}s" "$python_bin" "$(dirname "$0")/heavy_run.py" "$@"
