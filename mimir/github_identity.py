"""Process-bound GitHub identity for credentialed ``gh`` children.

Core grants the operator's ``GITHUB_TOKEN`` to authorized ``gh`` service
commands (the agent home's session-close and maintenance reads). Before every
such call the token must belong to ``MIMIR_GITHUB_SELF_LOGIN``. The binding is
resolved once per process and never refreshed by an effect, so a changed or
missing credential fails closed until restart.

Providers that perform their own forge effects (Cultivate) bind through the
same cache; this module never imports a provider.
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
from collections.abc import Callable
from enum import StrEnum

_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})")
_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_REPO_URL = re.compile(
    r"https://github\.com/(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)"
    r"(?P<rest>/.*)"
)
_PR_PATH = re.compile(r"/pull/[1-9][0-9]*")
_USER_URL = "https://api.github.com/user"
_USER_MAX_BYTES = 65_536
_USER_TIMEOUT_SECONDS = 20.0

_identity_lock = threading.Lock()
_verified_identity: tuple[str, str] | None = None


def same_github_repo(a: object, b: object) -> bool:
    """Compare complete GitHub owner/repo slugs, case-insensitively."""
    return (isinstance(a, str) and isinstance(b, str)
            and _REPO.fullmatch(a) is not None and _REPO.fullmatch(b) is not None
            and a.casefold() == b.casefold())


def same_github_url(a: object, b: object) -> bool:
    """Allow only the owner/repo casing to differ in HTTPS GitHub URLs."""
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    left, right = _REPO_URL.fullmatch(a), _REPO_URL.fullmatch(b)
    return bool(left and right and same_github_repo(left['repo'], right['repo'])
                and left['rest'] == right['rest'])


def same_github_pr_url(a: object, b: object) -> bool:
    """Match canonical pull URLs with exact path shape and PR number."""
    if not same_github_url(a, b):
        return False
    assert isinstance(a, str)
    match = _REPO_URL.fullmatch(a)
    return match is not None and _PR_PATH.fullmatch(match['rest']) is not None


class GitHubIdentityFailureKind(StrEnum):
    """Typed provenance for deciding whether identity verification may retry."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"


class GitHubIdentityVerificationError(RuntimeError):
    """A safe, pre-effect failure to bind credentials to a declared login."""

    def __init__(
        self,
        message: str,
        *,
        declared_login: str = "",
        authenticated_login: str = "",
        failure_kind: GitHubIdentityFailureKind = GitHubIdentityFailureKind.PERMANENT,
    ) -> None:
        super().__init__(message)
        self.declared_login = declared_login
        self.authenticated_login = authenticated_login
        self.failure_kind = failure_kind


def credential_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def verified_identity() -> tuple[str, str] | None:
    """Return the process binding as ``(login, credential fingerprint)``."""
    with _identity_lock:
        return _verified_identity


def verify_github_identity(
    declared_login: str, token: str, fetch_login: Callable[[], str],
) -> str:
    """Resolve and process-cache the token owner, refusing any mismatch.

    *fetch_login* returns the authenticated login for *token*; it runs only
    while the cache is empty and raises ``GitHubIdentityVerificationError``
    (with a failure kind) when the forge cannot answer.
    """
    expected = declared_login.strip()
    if not expected:
        raise GitHubIdentityVerificationError("github declared identity is empty")
    fingerprint = credential_fingerprint(token.strip())
    global _verified_identity
    with _identity_lock:
        if _verified_identity is not None:
            login, cached_fingerprint = _verified_identity
            if cached_fingerprint != fingerprint:
                raise GitHubIdentityVerificationError(
                    "github identity verification cache does not match active credential",
                    declared_login=expected,
                    authenticated_login=login,
                )
            if login.casefold() != expected.casefold():
                raise GitHubIdentityVerificationError(
                    f"github identity mismatch: authenticated as {login}, declared as {expected}",
                    declared_login=expected,
                    authenticated_login=login,
                )
            return login
        login = str(fetch_login()).strip()
        if _LOGIN.fullmatch(login) is None:
            raise GitHubIdentityVerificationError(
                "github identity verification returned an invalid login",
                declared_login=expected,
            )
        if login.casefold() != expected.casefold():
            raise GitHubIdentityVerificationError(
                f"github identity mismatch: authenticated as {login}, declared as {expected}",
                declared_login=expected,
                authenticated_login=login,
            )
        _verified_identity = (login, fingerprint)
        return login


def confirm_github_identity(principal: str, token: str | None = None) -> str:
    """Confirm *principal* against the process-cached authenticated identity."""
    expected = principal.strip()
    credential = token if token is not None else os.environ.get("GITHUB_TOKEN", "")
    fingerprint = credential_fingerprint(credential.strip())
    verified = verified_identity()
    if verified is None:
        raise GitHubIdentityVerificationError(
            "github identity verification cache is empty",
            declared_login=expected,
        )
    login, verified_fingerprint = verified
    if fingerprint != verified_fingerprint:
        raise GitHubIdentityVerificationError(
            "github identity verification cache does not match active credential",
            declared_login=expected,
            authenticated_login=login,
        )
    if login.casefold() != expected.casefold():
        raise GitHubIdentityVerificationError(
            f"github acting identity mismatch: authenticated as {login}, scope principal is {principal}",
            declared_login=expected,
            authenticated_login=login,
        )
    return login


def fetch_github_login(token: str) -> str:
    """Return the login that owns *token* from the GitHub REST API."""
    import requests

    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "mimir-github-identity",
    }
    if token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"
    try:
        response = requests.get(
            _USER_URL, headers=headers, timeout=_USER_TIMEOUT_SECONDS, stream=True,
        )
        try:
            raw = response.raw.read(_USER_MAX_BYTES + 1, decode_content=True)
        finally:
            response.close()
    except requests.RequestException as exc:
        raise GitHubIdentityVerificationError(
            f"github identity transport failed: {type(exc).__name__}",
            failure_kind=GitHubIdentityFailureKind.TRANSIENT,
        ) from exc
    status = response.status_code
    if status >= 400:
        raise GitHubIdentityVerificationError(
            f"github identity verification failed: HTTP {status}",
            failure_kind=(
                GitHubIdentityFailureKind.TRANSIENT
                if status == 429 or status >= 500
                else GitHubIdentityFailureKind.PERMANENT
            ),
        )
    if len(raw) > _USER_MAX_BYTES:
        raise GitHubIdentityVerificationError("github identity response exceeded size limit")
    import json

    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise GitHubIdentityVerificationError(
            "github identity verification returned invalid JSON",
        ) from exc
    return str(data.get("login", "")) if isinstance(data, dict) else ""


def confirm_gh_cli_identity(token: str) -> str:
    """Bind on first use, then confirm *token* before a credentialed gh child.

    Raises ``ToolPolicyRefusal`` so the refusal reaches the model as a policy
    refusal; nothing is cached on failure.
    """
    declared = os.environ.get("MIMIR_GITHUB_SELF_LOGIN", "")
    try:
        if not token.strip():
            # Never ask the forge who an empty credential is.
            raise GitHubIdentityVerificationError(
                "GITHUB_TOKEN is not set", declared_login=declared.strip(),
            )
        # Binds on an empty cache; otherwise compares token and login with it.
        return verify_github_identity(declared, token, lambda: fetch_github_login(token))
    except GitHubIdentityVerificationError as exc:
        from .tools.refusals import ToolPolicyRefusal

        raise ToolPolicyRefusal(
            f"gh refused: GitHub identity verification failed: {exc}"
        ) from exc


__all__ = [
    "GitHubIdentityFailureKind",
    "GitHubIdentityVerificationError",
    "confirm_gh_cli_identity",
    "confirm_github_identity",
    "credential_fingerprint",
    "fetch_github_login",
    "verified_identity",
    "verify_github_identity",
]
