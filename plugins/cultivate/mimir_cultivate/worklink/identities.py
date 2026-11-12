from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import grp
import pwd
import os
from pathlib import Path



@dataclass(frozen=True)
class WorklinkIdentities:
    mimir_uid: int
    worklink_uid: int
    worklink_gid: int


@lru_cache(maxsize=1)
def get_identities() -> WorklinkIdentities:
    try:
        mimir = pwd.getpwnam("mimir")
    except KeyError as exc:
        raise RuntimeError("required account 'mimir' is missing") from exc
    try:
        worklink = pwd.getpwnam("worklink")
    except KeyError as exc:
        raise RuntimeError("required account 'worklink' is missing") from exc
    try:
        worklink_group = grp.getgrnam("worklink")
    except KeyError as exc:
        raise RuntimeError("required group 'worklink' is missing") from exc
    return WorklinkIdentities(
        mimir_uid=mimir.pw_uid,
        worklink_uid=worklink.pw_uid,
        worklink_gid=worklink_group.gr_gid,
    )


def assert_worklink_identity(identities: WorklinkIdentities) -> None:
    """Check the result of the native uid helper, not just account metadata."""
    if (identities.worklink_uid in (0, identities.mimir_uid)
            or os.getresuid() != (identities.worklink_uid,) * 3
            or os.getresgid() != (identities.worklink_gid,) * 3
            or set(os.getgroups()) - {identities.worklink_gid}):
        raise RuntimeError("invalid Worklink process identity")
    status = Path("/proc/self/status").read_text()
    caps = {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
            for line in status.splitlines() if line.startswith("Cap")}
    if any(int(caps.get(name, "1"), 16) != 0
           for name in ("CapInh", "CapPrm", "CapEff", "CapAmb")):
        raise RuntimeError("Worklink process retains capabilities")
