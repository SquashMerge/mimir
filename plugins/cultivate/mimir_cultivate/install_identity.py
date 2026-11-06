"""Verify a deployed wheel pair against retained archives and installed files."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.metadata as metadata
import io
import json
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import sysconfig
import zipfile

from packaging.utils import canonicalize_name, parse_wheel_filename

PROJECTS = ("mimir-agent", "mimir-cultivate")
CONTROLLER_MARKER = "controller-install-identity.json"
EXECUTOR_MARKER = Path("/opt/mimir-worklink/executor-install-identity.json")
WHEEL_DIR = Path("/opt/mimir-worklink/wheels")


class InstallIdentityError(RuntimeError):
    """The image does not contain a verified, immutable install pair."""


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(contents: str) -> dict[str, tuple[str, str]]:
    entries: dict[str, tuple[str, str]] = {}
    for row in csv.reader(io.StringIO(contents)):
        if len(row) != 3 or row[0] in entries or not row[0]:
            raise InstallIdentityError("invalid wheel RECORD")
        path = PurePosixPath(row[0])
        if path.is_absolute() or any(part in {".", ".."} for part in path.parts):
            raise InstallIdentityError("unsafe wheel RECORD path")
        entries[row[0]] = row[1], row[2]
    return entries


def valid_pair(identity: object) -> bool:
    if not isinstance(identity, dict) or set(identity) != {"mode", "distributions"} or identity["mode"] != "pypi":
        return False
    pair = identity["distributions"]
    if not isinstance(pair, dict) or set(pair) != set(PROJECTS):
        return False
    versions = set()
    for name in PROJECTS:
        item = pair[name]
        if not isinstance(item, dict) or set(item) != {"version", "wheel", "sha256"}:
            return False
        version, filename, sha = (item[key] for key in ("version", "wheel", "sha256"))
        if not all(isinstance(value, str) for value in (version, filename, sha)) or re.fullmatch(r"[0-9a-f]{64}", sha) is None:
            return False
        if Path(filename).name != filename:
            return False
        try:
            wheel_name, wheel_version, _, _ = parse_wheel_filename(filename)
        except ValueError:
            return False
        if wheel_name != canonicalize_name(name) or str(wheel_version) != version:
            return False
        versions.add(version)
    return len(versions) == 1


def _verify_distribution(name: str, identity: object, wheels: Path, site: Path) -> None:
    if not isinstance(identity, dict) or set(identity) != {"version", "wheel", "sha256"}:
        raise InstallIdentityError(f"invalid {name} wheel identity")
    version, filename, sha = (identity[key] for key in ("version", "wheel", "sha256"))
    if not all(isinstance(value, str) for value in (version, filename, sha)) or not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise InstallIdentityError(f"unpinned {name} wheel")
    if Path(filename).name != filename or not filename.endswith(".whl"):
        raise InstallIdentityError(f"invalid {name} wheel filename")
    try:
        wheel_name, wheel_version, _, _ = parse_wheel_filename(filename)
    except ValueError as exc:
        raise InstallIdentityError(f"invalid {name} wheel filename") from exc
    if wheel_name != canonicalize_name(name) or str(wheel_version) != version:
        raise InstallIdentityError(f"{name} wheel name/version mismatch")
    try:
        archive = wheels / filename
        if archive.is_symlink() or not archive.is_file() or _digest(archive) != sha:
            raise InstallIdentityError(f"{name} wheel archive hash mismatch")
        distributions = [dist for dist in metadata.distributions(path=[str(site)])
                         if dist.metadata.get("Name") and canonicalize_name(dist.metadata["Name"]) == name]
        if len(distributions) != 1 or distributions[0].version != version:
            raise InstallIdentityError(f"{name} installed version mismatch")
        dist = distributions[0]
        installed_record = next((file for file in dist.files or () if str(file).endswith(".dist-info/RECORD")), None)
        if installed_record is None:
            raise InstallIdentityError(f"{name} installed RECORD missing")
        installed = _record(dist.locate_file(installed_record).read_text(encoding="utf-8"))
        with zipfile.ZipFile(archive) as wheel:
            records = [entry for entry in wheel.namelist() if entry.endswith(".dist-info/RECORD")]
            if len(records) != 1 or len(wheel.namelist()) != len(set(wheel.namelist())):
                raise InstallIdentityError(f"{name} wheel RECORD missing or duplicated")
            recorded = _record(wheel.read(records[0]).decode("utf-8"))
            if set(recorded) != set(wheel.namelist()):
                raise InstallIdentityError(f"{name} wheel RECORD inventory mismatch")
            prefix = records[0].removesuffix("RECORD")
            allowed = {records[0], prefix + "INSTALLER", prefix + "REQUESTED", prefix + "direct_url.json"}
            for entry in recorded:
                if entry.endswith(".py"):
                    path = PurePosixPath(entry)
                    tag = f"cpython-{sys.version_info.major}{sys.version_info.minor}"
                    allowed.add(str(path.parent / "__pycache__" / f"{path.stem}.{tag}.pyc"))
            if set(installed) - set(recorded) - allowed:
                raise InstallIdentityError(f"{name} unexpected installed RECORD entries")
            for entry in wheel.namelist():
                if entry == records[0]:
                    continue
                digest, size = recorded[entry]
                if not digest.startswith("sha256=") or not size.isdigit():
                    raise InstallIdentityError(f"{name} unverified RECORD entry")
                data = wheel.read(entry)
                expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")
                if digest != f"sha256={expected}" or int(size) != len(data):
                    raise InstallIdentityError(f"{name} wheel RECORD content mismatch")
                if installed.get(entry) != (digest, size):
                    raise InstallIdentityError(f"{name} installed RECORD mismatch")
                target = site.joinpath(*PurePosixPath(entry).parts)
                if not target.is_relative_to(site) or not stat.S_ISREG(target.lstat().st_mode) or _digest(target) != hashlib.sha256(data).hexdigest():
                    raise InstallIdentityError(f"{name} installed content drift")
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, csv.Error, zipfile.BadZipFile) as exc:
        raise InstallIdentityError(f"{name} wheel or installed RECORD unreadable") from exc


def verify_install_identity(
    marker: Path, *, wheels: Path = WHEEL_DIR, site: Path | None = None,
) -> dict[str, object]:
    """Return a validated tagged identity; never accept a marker as evidence alone."""
    try:
        identity = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError) as exc:
        raise InstallIdentityError("install identity marker missing or invalid") from exc
    if not valid_pair(identity):
        raise InstallIdentityError("install identity requires a pinned matching wheel pair")
    pair = identity["distributions"]
    site = site or Path(sysconfig.get_path("purelib"))
    for name in PROJECTS:
        _verify_distribution(name, pair[name], wheels, site)
    return identity


def controller_identity() -> dict[str, object]:
    return verify_install_identity(Path(sys.prefix) / CONTROLLER_MARKER)


def executor_identity() -> dict[str, object]:
    return verify_install_identity(EXECUTOR_MARKER)
