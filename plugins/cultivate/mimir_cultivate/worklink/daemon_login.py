"""Operator-only native login; never use this module as a readiness probe."""
from __future__ import annotations

import os
import sys

from .daemon_config import PublishingCredentials, daemon_environment
from .daemon_health import load_api
from .daemon_service import RuntimePaths
from .identities import assert_worklink_identity, get_identities

LOGIN_ARGV = ("/opt/mimir-opencode/bin/opencode", "auth", "login", "--server",
              "http://127.0.0.1:4096", "--method", "chatgpt-headless", "openai")


def login(paths: RuntimePaths = RuntimePaths()) -> None:
    identities = get_identities()
    assert_worklink_identity(identities)
    api = load_api(paths, uid=identities.worklink_uid)
    environ = daemon_environment(PublishingCredentials(None, None), api_password=api["password"])
    environ.pop("OPENCODE_SERVER_PASSWORD")
    environ["OPENCODE_PASSWORD"] = api["password"]
    os.chdir(str(paths.daemon / "home"))
    os.execve(LOGIN_ARGV[0], LOGIN_ARGV, environ)


def main() -> int:
    try:
        if sys.argv[1:]:
            raise ValueError("invalid login arguments")
        login()
        return 0
    except (OSError, ValueError, RuntimeError):
        print("Worklink native login unavailable", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
