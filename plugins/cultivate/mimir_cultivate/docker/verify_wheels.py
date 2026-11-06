"""Validate exact wheel build inputs before any package installation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile


def check(wheels: Path, version: str, agent: str, agent_sha: str,
          cultivate: str, cultivate_sha: str) -> dict[str, object]:
    if not re.fullmatch(r"[0-9][A-Za-z0-9.!+_]*", version):
        raise ValueError("invalid exact MIMIR_VERSION")
    pair = {}
    for name, filename, digest in (("mimir-agent", agent, agent_sha),
                                   ("mimir-cultivate", cultivate, cultivate_sha)):
        if (not re.fullmatch(r"[0-9a-f]{64}", digest) or
                not re.fullmatch(re.escape(name.replace("-", "_")) +
                                 r"-" + re.escape(version) + r"-[A-Za-z0-9_.-]+\.whl", filename)):
            raise ValueError(f"invalid {name} wheel name, version or SHA256")
        archive = wheels / filename
        if archive.is_symlink() or not archive.is_file():
            raise ValueError(f"missing {name} wheel archive")
        if hashlib.sha256(archive.read_bytes()).hexdigest() != digest:
            raise ValueError(f"{name} wheel archive hash mismatch")
        if not zipfile.is_zipfile(archive):
            raise ValueError(f"invalid {name} wheel archive")
        pair[name] = {"version": version, "wheel": filename, "sha256": digest}
    if {p.name for p in wheels.iterdir()} != {agent, cultivate}:
        raise ValueError("unexpected wheel archives in build context")
    return {"mode": "pypi", "distributions": pair}


if __name__ == "__main__":
    try:
        print(json.dumps(check(Path(sys.argv[1]), *sys.argv[2:]), sort_keys=True))
    except (ValueError, OSError, TypeError, IndexError) as exc:
        raise SystemExit(str(exc)) from exc
