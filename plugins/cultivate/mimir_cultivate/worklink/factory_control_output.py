"""Worker-side bounded projection of feature-factory control output.

The CLI owns its stdout schema. Keep its complete output in an unlinked file in
the worker process, and send only the fields the controller consumes across the
executor's bounded stdout channel.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile


def _status_projection(source: object) -> dict[str, object]:
    if not isinstance(source, dict):
        raise ValueError("factory status is not an object")
    fields = (
        "run_id", "valid", "sandbox_path", "issue_key", "status", "mode",
        "branch", "pr_base", "pr_draft", "lock", "dead_lock", "lock_session",
        "pr_url", "park_snapshot", "next", "next_action", "validator",
    )
    result = {key: source[key] for key in fields if key in source}
    # These lists are progress displays, not an archive. Retain a merged slice
    # even when it falls outside the display window (completion requires one).
    for key in ("steps", "slices"):
        rows = source.get(key)
        if rows is not None:
            if not isinstance(rows, list):
                raise ValueError(f"factory status {key} is not a list")
            selected = rows[:16]
            if key == "slices" and not any(
                isinstance(row, dict) and row.get("status") == "merged" for row in selected
            ):
                merged = next((row for row in rows[16:] if isinstance(row, dict)
                               and row.get("status") == "merged"), None)
                if merged is not None:
                    selected = selected[:15] + [merged]
            result[key] = selected
    gates = source.get("gates")
    if gates is not None:
        if not isinstance(gates, dict):
            raise ValueError("factory status gates is not an object")
        result["gates"] = dict(list(gates.items())[:16])
    terminal = source.get("terminal_result")
    if terminal is not None:
        if not isinstance(terminal, dict):
            raise ValueError("factory terminal result is not an object")
        encoded = json.dumps(terminal, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(encoded) <= 8192:
            result["terminal_result"] = terminal
        else:
            reason = terminal.get("reason")
            result["terminal_result"] = {
                "sha256": hashlib.sha256(encoded).hexdigest(),
                **({"reason": reason} if isinstance(reason, str) and len(reason.encode("utf-8")) <= 4096 else {}),
            }
    # Reject unexpectedly large *selected* fields, rather than sending a
    # partial JSON document that could be mistaken for authoritative status.
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"),
                         allow_nan=False).encode("utf-8")
    if len(encoded) > 256 * 1024:
        raise ValueError("factory status projection exceeds 256 KiB")
    return result


def main() -> int:
    if len(sys.argv) < 4 or sys.argv[1] not in {"status", "other"}:
        return 2
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as diagnostic:
        process = subprocess.run(sys.argv[2:], stdout=output, stderr=diagnostic, check=False)
        diagnostic.seek(0)
        sys.stderr.buffer.write(diagnostic.read(16 * 1024))
        output.seek(0)
        if process.returncode:
            sys.stdout.buffer.write(output.read(16 * 1024))
            return process.returncode
        if sys.argv[1] == "status":
            try:
                projected = _status_projection(json.load(output))
                sys.stdout.write(json.dumps(projected, ensure_ascii=False, separators=(",", ":")))
            except (ValueError, UnicodeError, TypeError) as exc:
                sys.stderr.write(f"factory status projection failed: {exc}\n")
                return 1
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
