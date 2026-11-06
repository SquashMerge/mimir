"""Resolve a hash-verified pair of Python 3.11 Linux PyPI wheels for setup."""

from __future__ import annotations

from dataclasses import dataclass
from email.parser import BytesParser
from email.policy import default
import hashlib
from io import BytesIO
import json
from queue import Empty, SimpleQueue
import re
from threading import Event, Thread
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.tags import Tag, compatible_tags, cpython_tags
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import InvalidVersion, Version


INDEX_LIMIT = 8 * 1024 * 1024
WHEEL_LIMIT = 128 * 1024 * 1024
METADATA_LIMIT = 1024 * 1024
FETCH_DEADLINE = 30
_PROJECTS = ("mimir-agent", "mimir-cultivate")


class ReleaseError(ValueError):
    """A release cannot be selected or verified; no source fallback is safe."""


@dataclass(frozen=True)
class Wheel:
    name: str
    filename: str
    url: str
    sha256: str
    data: bytes


@dataclass(frozen=True)
class ReleasePair:
    version: str
    agent: Wheel
    cultivate: Wheel


def fetch_bytes(url: str, limit: int) -> bytes:
    """Read at most limit bytes over HTTPS within a total wall-clock deadline."""
    if urlsplit(url).scheme != "https":
        raise ReleaseError("PyPI download requires HTTPS.")

    deadline = time.monotonic() + FETCH_DEADLINE
    cancelled = Event()
    result: SimpleQueue[bytes | Exception] = SimpleQueue()

    def download() -> None:
        try:
            with urlopen(Request(url, headers={"User-Agent": "mimir-cultivate-setup/1"}),
                         timeout=min(15, max(0.001, deadline - time.monotonic()))) as response:
                if urlsplit(response.geturl()).scheme != "https":
                    raise ReleaseError("PyPI redirected to an insecure download.")
                data = bytearray()
                while not cancelled.is_set():
                    if time.monotonic() >= deadline:
                        raise ReleaseError("PyPI download exceeded the time limit.")
                    chunk = response.read(min(64 * 1024, limit + 1 - len(data)))
                    if time.monotonic() >= deadline:
                        raise ReleaseError("PyPI download exceeded the time limit.")
                    if not chunk:
                        result.put(bytes(data))
                        return
                    data.extend(chunk)
                    if len(data) > limit:
                        raise ReleaseError("PyPI response exceeds the permitted size.")
        except Exception as exc:
            if not cancelled.is_set():
                result.put(exc)

    Thread(target=download, daemon=True).start()
    try:
        outcome = result.get(timeout=max(0, deadline - time.monotonic()))
    except Empty:
        raise ReleaseError("PyPI download exceeded the time limit.") from None
    finally:
        cancelled.set()
    if isinstance(outcome, ReleaseError):
        raise outcome
    if isinstance(outcome, Exception):
        raise ReleaseError("Cannot reach PyPI; check network access and retry setup.") from None
    return outcome


def _target_tags(architecture: str) -> set[Tag]:
    if architecture not in {"x86_64", "aarch64"}:
        raise ReleaseError("Unsupported Linux image architecture; choose x86_64 or aarch64.")
    # setup pins python:3.11-slim-bookworm (Debian 12, glibc 2.36).
    platforms = [f"manylinux_2_{minor}_{architecture}" for minor in range(36, 16, -1)]
    platforms.append(f"manylinux2014_{architecture}")
    if architecture == "x86_64":
        platforms.extend(("manylinux2010_x86_64", "manylinux1_x86_64"))
    return set(cpython_tags((3, 11), platforms=platforms)) | set(compatible_tags((3, 11), platforms=platforms))


def _index(project: str, fetch) -> dict:
    try:
        raw = fetch(f"https://pypi.org/pypi/{project}/json", INDEX_LIMIT)
        if len(raw) > INDEX_LIMIT:
            raise ReleaseError("PyPI response exceeds the permitted size.")
        document = json.loads(raw)
    except OSError:
        raise ReleaseError(f"Cannot reach PyPI for {project}; check network access and retry setup.") from None
    except (ValueError, TypeError, UnicodeError) as exc:
        if isinstance(exc, ReleaseError):
            raise
        raise ReleaseError(f"Invalid PyPI JSON for {project}.") from None
    if not isinstance(document, dict) or not isinstance(document.get("releases"), dict):
        raise ReleaseError(f"Invalid PyPI release listing for {project}.")
    return document["releases"]


def _candidates(project: str, releases: dict, tags: set[Tag]) -> dict[Version, dict]:
    result: dict[Version, dict] = {}
    for label, files in releases.items():
        try:
            version = Version(label)
        except (InvalidVersion, TypeError):
            continue
        if not isinstance(files, list):
            raise ReleaseError(f"Invalid PyPI release files for {project} {label}.")
        for file in files:
            if not isinstance(file, dict):
                raise ReleaseError(f"Invalid PyPI wheel entry for {project} {label}.")
            if file.get("packagetype") != "bdist_wheel" or file.get("yanked") is True:
                continue
            filename = file.get("filename")
            try:
                name, wheel_version, _, wheel_tags = parse_wheel_filename(filename)
            except (InvalidWheelFilename, TypeError):
                raise ReleaseError(f"Invalid PyPI wheel filename for {project} {label}.") from None
            if name != canonicalize_name(project) or wheel_version != version:
                raise ReleaseError(f"PyPI wheel name or version disagrees with {project} {label}.")
            if not wheel_tags.intersection(tags):
                continue
            requires_python = file.get("requires_python")
            try:
                if requires_python and Version("3.11.0") not in SpecifierSet(requires_python):
                    continue
            except (InvalidSpecifier, TypeError):
                raise ReleaseError(f"Invalid Python requirement for {project} {label}.") from None
            digest = file.get("digests")
            url = file.get("url")
            if (not isinstance(digest, dict) or not isinstance(digest.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", digest["sha256"]) or not isinstance(url, str)
                    or urlsplit(url).scheme != "https" or urlsplit(url).hostname != "files.pythonhosted.org"):
                raise ReleaseError(f"Invalid PyPI wheel URL or SHA-256 for {project} {label}.")
            if urlsplit(url).path.rsplit("/", 1)[-1] != filename:
                raise ReleaseError(f"PyPI wheel URL disagrees with filename for {project} {label}.")
            current = result.get(version)
            if current is None or filename < current["filename"]:
                result[version] = file
    return result


def _download(project: str, version: Version, entry: dict, fetch, architecture: str) -> Wheel:
    filename = entry["filename"]
    try:
        data = fetch(entry["url"], WHEEL_LIMIT)
    except OSError:
        raise ReleaseError(f"Cannot download PyPI wheel for {project} {version}; retry setup.") from None
    if not isinstance(data, bytes) or len(data) > WHEEL_LIMIT:
        raise ReleaseError(f"Invalid or oversized PyPI wheel for {project} {version}.")
    digest = entry["digests"]["sha256"]
    if hashlib.sha256(data).hexdigest() != digest:
        raise ReleaseError(f"PyPI wheel SHA-256 mismatch for {project} {version}.")
    try:
        with ZipFile(BytesIO(data)) as archive:
            path = f"{project.replace('-', '_')}-{version}.dist-info/METADATA"
            matching = [item for item in archive.infolist() if item.filename == path]
            if len(matching) != 1 or matching[0].file_size > METADATA_LIMIT:
                raise ReleaseError(f"Missing or oversized wheel metadata for {project} {version}.")
            metadata = BytesParser(policy=default).parsebytes(archive.read(matching[0]))
            # Validate build inputs while everything is still in memory, before
            # setup publishes any deployment files or changes an existing one.
            from mimir.plugin_dockerfile import _CONTEXT_SCRIPTS, _S6_FILES

            resources = (["mimir/docker_context/Dockerfile", *(
                "mimir/docker_context/deploy/s6-overlay/s6-rc.d/" + item for item in _S6_FILES)]
                if project == "mimir-agent" else ["mimir_cultivate/docker/" + item for item in _CONTEXT_SCRIPTS])
            for resource in resources:
                members = [item for item in archive.infolist() if item.filename == resource]
                if (len(members) != 1 or members[0].is_dir()
                        or (members[0].external_attr >> 16) & 0o170000 == 0o120000):
                    raise ReleaseError(f"{project} {version} lacks required Docker context resource: {resource}")
        if (canonicalize_name(metadata.get("Name", "")) != project
                or Version(metadata.get("Version", "")) != version):
            raise ReleaseError(f"Wheel metadata name or version mismatch for {project} {version}.")
        requires_python = metadata.get("Requires-Python")
        if requires_python and Version("3.11.0") not in SpecifierSet(requires_python):
            raise ReleaseError(f"Wheel metadata excludes Python 3.11 for {project} {version}.")
        if project == "mimir-cultivate":
            target = {
                "implementation_name": "cpython", "implementation_version": "3.11.0",
                "os_name": "posix", "platform_machine": architecture,
                "platform_python_implementation": "CPython", "platform_release": "",
                "platform_system": "Linux", "platform_version": "",
                "python_full_version": "3.11.0", "python_version": "3.11",
                "sys_platform": "linux", "extra": "",
            }
            dependencies = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
            core = [dep for dep in dependencies if canonicalize_name(dep.name) == "mimir-agent"
                    and (dep.marker is None or dep.marker.evaluate(environment=target))]
            if not core or any(version not in dep.specifier for dep in core):
                raise ReleaseError(f"Cultivate wheel does not require mimir-agent {version}.")
    except (BadZipFile, KeyError, ValueError, InvalidRequirement, RuntimeError) as exc:
        if isinstance(exc, ReleaseError):
            raise
        raise ReleaseError(f"Invalid wheel metadata for {project} {version}.") from None
    return Wheel(project, filename, entry["url"], digest, data)


def resolve_release(version: str | None = None, *, fetch=fetch_bytes,
                    architecture: str = "x86_64") -> ReleasePair:
    """Select the newest common stable wheel pair, or newest common prerelease.

    An explicit version must resolve as a complete pair. Only verified wheel
    bytes are returned; callers must not substitute an index URL at build time.
    """
    requested = None
    if version is not None:
        try:
            requested = Version(version)
        except (InvalidVersion, TypeError):
            raise ReleaseError("Invalid exact Mimir version; use a PEP 440 release version.") from None
    tags = _target_tags(architecture)
    agent = _candidates(_PROJECTS[0], _index(_PROJECTS[0], fetch), tags)
    cultivate = _candidates(_PROJECTS[1], _index(_PROJECTS[1], fetch), tags)
    common = agent.keys() & cultivate.keys()
    if requested is not None:
        if requested not in common:
            raise ReleaseError(f"No compatible, non-yanked PyPI wheel pair for Mimir {requested}.")
        selected = requested
    else:
        stable = [candidate for candidate in common if not candidate.is_prerelease and not candidate.is_devrelease]
        if not common:
            raise ReleaseError("No common compatible, non-yanked PyPI wheel release for Mimir and Cultivate.")
        selected = max(stable or common)
    return ReleasePair(str(selected), _download(_PROJECTS[0], selected, agent[selected], fetch, architecture),
                       _download(_PROJECTS[1], selected, cultivate[selected], fetch, architecture))
