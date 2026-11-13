"""Metadata-only activation policy shared by Worklink's runtime boundaries."""

from __future__ import annotations

import json
import os
import shutil
import stat
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from mimir.plugin_dockerfile import has_trusted_cultivate, select_plugins

INSTALLED_MARKER = Path("/opt/cultivate-docker/opencode-enabled")
DAEMON_PATH = "/opt/mimir-opencode/bin:/usr/local/bin:/usr/bin:/bin"


class ActivationError(ValueError):
    """A selected installation cannot safely determine daemon activation."""


@dataclass(frozen=True)
class DaemonActivation:
    enabled: bool
    reason: Literal["unselected", "not_installed", "enabled"]


def planned_activation(*, trusted_cultivate: bool, opencode_installed: bool) -> DaemonActivation:
    """Evaluate trusted build inputs without consulting runtime files or tools."""
    if not trusted_cultivate:
        return DaemonActivation(False, "unselected")
    if not opencode_installed:
        return DaemonActivation(False, "not_installed")
    return DaemonActivation(True, "enabled")


def read_installed_marker(path: Path = INSTALLED_MARKER) -> bool:
    """Read the exact, root-owned image bit without following a substituted link."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    except OSError:
        raise ActivationError("invalid OpenCode installation marker") from None
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0
                or info.st_mode & 0o022 or info.st_size != 2):
            raise ActivationError("invalid OpenCode installation marker")
        value = os.read(fd, 3)
        if value not in (b"0\n", b"1\n"):
            raise ActivationError("invalid OpenCode installation marker")
        return value == b"1\n"
    finally:
        os.close(fd)


def resolve_activation(
    environ: Mapping[str, str], *, marker: Path = INSTALLED_MARKER,
) -> DaemonActivation:
    """Resolve runtime selection without loading provider entrypoints.

    Disabled paths never read daemon state or even the installation marker when
    Cultivate is unselected. An installed but unavailable CLI is not disabled.
    """
    try:
        selected = has_trusted_cultivate(select_plugins(enable=environ.get("MIMIR_PLUGINS")))
    except ValueError:
        raise ActivationError("invalid runtime plugin selection") from None
    activation = planned_activation(
        trusted_cultivate=selected,
        opencode_installed=read_installed_marker(marker) if selected else False,
    )
    if activation.enabled and shutil.which("opencode", path=DAEMON_PATH) is None:
        raise ActivationError("enabled OpenCode binary is unavailable")
    return activation


def main() -> int:
    """Print a credential-free activation result for shell/runtime consumers."""
    try:
        activation = resolve_activation(dict(os.environ))
    except ActivationError as exc:
        print(json.dumps({"error": str(exc)}))
        return 1
    print(json.dumps(asdict(activation)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
