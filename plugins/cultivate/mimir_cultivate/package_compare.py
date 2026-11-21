"""Bound npm provenance without ingesting package text or executing package code."""

from __future__ import annotations

import gzip
import hashlib
import hmac
import io
import json
import os
import tarfile
import tempfile
import zlib
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from langchain.tools import ToolRuntime, tool

from mimir.models import AuthContext
from mimir.package_evidence import (
    MAX_FILES, integrity_tokens, parse_specs, requested_pairs, safe_path,
)

MAX_DOWNLOAD = 64 * 1024 * 1024
MAX_EXPANDED = 256 * 1024 * 1024
MAX_METADATA = 1024 * 1024


class PackageRefusal(ValueError):
    """Fixed diagnostic; external text is never exposed."""


def _registry() -> str:
    # Operator configuration only, never a tool argument or checkout .npmrc.
    registry = os.environ.get("MIMIR_NPM_REGISTRY", "https://registry.npmjs.org").rstrip("/")
    parsed = urlsplit(registry)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        raise PackageRefusal("registry_unavailable")
    return registry


def _get(client: httpx.Client, registry: str, url: str, limit: int) -> bytes:
    target, origin = urlsplit(url), urlsplit(registry)
    if (target.scheme, target.hostname, target.port) != (origin.scheme, origin.hostname, origin.port) or (
        target.username or target.password or target.fragment
    ):
        raise PackageRefusal("registry_unavailable")
    # No redirects (including same-host redirects), proxies, ambient auth, or npm.
    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
            raise PackageRefusal("registry_unavailable")
        data = bytearray()
        for chunk in response.iter_bytes():
            if len(data) + len(chunk) > limit:
                raise PackageRefusal("registry_unavailable")
            data.extend(chunk)
        return bytes(data)


def _verify(data: bytes, integrity: object) -> str:
    try:
        tokens = integrity_tokens(integrity)
    except ValueError:
        raise PackageRefusal("integrity_failed") from None
    strongest = max(tokens, key=lambda token: len(token[1]))[0]
    actual = hashlib.new(strongest, data).digest()
    if not any(algorithm == strongest and hmac.compare_digest(digest, actual) for algorithm, digest in tokens):
        raise PackageRefusal("integrity_failed")
    return str(integrity)


def _extract(data: bytes, root: Path) -> tuple[dict[str, str], int, int]:
    # Bound the *whole* decompressed stream before tarfile parses extended headers.
    # Archive paths never become filesystem paths: extract regular bytes to private
    # numbered files. No links, devices, executable modes, or install scripts.
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as compressed:
        expanded = compressed.read(MAX_EXPANDED + 1)
    if len(expanded) > MAX_EXPANDED:
        raise PackageRefusal("unsafe_archive")
    inventory: dict[str, str] = {}
    seen: set[str] = set()
    count = omitted = total_size = 0
    with tarfile.open(fileobj=io.BytesIO(expanded), mode="r:") as archive:
        for index, member in enumerate(archive):
            if index >= MAX_FILES:
                raise PackageRefusal("unsafe_archive")
            if member.isdir():
                continue
            if not member.isfile() or member.size < 0:
                raise PackageRefusal("unsafe_archive")
            total_size += member.size
            if total_size > MAX_EXPANDED:
                raise PackageRefusal("unsafe_archive")
            count += 1
            path = member.name.removeprefix("package/")
            if not member.name.startswith("package/") or not safe_path(path):
                omitted += 1
                continue
            if path in seen:
                raise PackageRefusal("unsafe_archive")
            seen.add(path)
            source = archive.extractfile(member)
            if source is None:
                raise PackageRefusal("unsafe_archive")
            digest = hashlib.sha256()
            with source, (root / str(index)).open("xb") as destination:
                while chunk := source.read(64 * 1024):
                    destination.write(chunk)
                    digest.update(chunk)
            inventory[path] = digest.hexdigest()
    return inventory, count, omitted


def compare(specs: list[str], pairs: list[list[int]] | None = None) -> dict[str, Any]:
    """Fetch only the configured registry and return a closed structural projection."""
    try:
        parsed = parse_specs(specs)
        indexes = requested_pairs(specs, pairs)
    except ValueError:
        return {"status": "refused", "reason": "invalid_request"}
    try:
        registry = _registry()
        packages, inventories = [], []
        with tempfile.TemporaryDirectory(prefix="mimir-npm-") as scratch, httpx.Client(
            timeout=30, follow_redirects=False, trust_env=False,
        ) as client:
            for index, (name, version) in enumerate(parsed):
                metadata = json.loads(_get(
                    client, registry, f"{registry}/{quote(name, safe='')}/{quote(version, safe='')}",
                    MAX_METADATA,
                ))
                if metadata.get("name") != name or metadata.get("version") != version:
                    raise PackageRefusal("registry_unavailable")
                distribution = metadata["dist"]
                data = _get(client, registry, distribution["tarball"], MAX_DOWNLOAD)
                integrity = _verify(data, distribution.get("integrity"))
                root = Path(scratch) / str(index)
                root.mkdir(mode=0o700)
                inventory, count, omitted = _extract(data, root)
                inventories.append(inventory)
                packages.append({
                    "name": name, "version": version,
                    "tarball_sha256": hashlib.sha256(data).hexdigest(),
                    "registry_integrity": integrity, "file_count": count,
                    "omitted_path_count": omitted,
                })
        comparisons = []
        for left, right in indexes:
            a, b = inventories[left], inventories[right]
            comparisons.append({
                "left": left, "right": right,
                "omitted_path_count": packages[left]["omitted_path_count"] + packages[right]["omitted_path_count"],
                "files": [{
                    "path": path,
                    "status": ("only_right" if path not in a else "only_left" if path not in b
                               else "identical" if a[path] == b[path] else "changed"),
                    "left_sha256": a.get(path), "right_sha256": b.get(path),
                } for path in sorted(a.keys() | b.keys())],
            })
        return {"status": "compared", "packages": packages, "pairs": comparisons}
    except PackageRefusal as exc:
        return {"status": "refused", "reason": str(exc)}
    except (
        httpx.HTTPError, httpx.InvalidURL, OSError, ValueError, TypeError, KeyError,
        AttributeError, tarfile.TarError, EOFError, zlib.error, RecursionError, OverflowError,
    ):
        return {"status": "refused", "reason": "registry_unavailable"}


@tool
def package_compare(
    repository: str,
    pull_request: int,
    specs: list[str],
    pairs: list[list[int]] | None = None,
    runtime: ToolRuntime[AuthContext] = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """Compare 1–4 exact npm name@version specs without ingesting contents.

    Optional pairs are ascending zero-based index pairs; default compares all.
    Returns hashes and safe relative paths only. Requires the bound PR inspect grant.
    """
    from .forge import _scope

    _scope(runtime, repository, pull_request)
    return compare(specs, pairs)


from .forge import _bind_injected_runtime  # noqa: E402

PACKAGE_TOOLS = (_bind_injected_runtime(package_compare),)
