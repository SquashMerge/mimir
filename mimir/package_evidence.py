"""Closed, content-free npm evidence contract shared with result classification."""

from __future__ import annotations

import base64
import re
from itertools import combinations
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

MAX_FILES = 10_000
MAX_PATH = 240
_NAME = r"(?:@[a-z0-9][a-z0-9._-]*/)?[a-z0-9][a-z0-9._-]*"
_NUM = r"(?:0|[1-9][0-9]*)"
_PRE = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
_VERSION = rf"{_NUM}\.{_NUM}\.{_NUM}(?:-{_PRE}(?:\.{_PRE})*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
SPEC = re.compile(rf"({_NAME})@({_VERSION})", re.ASCII)
PATH = re.compile(rf"[A-Za-z0-9_.@+/-]{{1,{MAX_PATH}}}", re.ASCII)
HASH = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$", min_length=64, max_length=64)]
COUNT = Annotated[int, Field(ge=0, le=MAX_FILES)]
REASONS = frozenset({"invalid_request", "registry_unavailable", "integrity_failed", "unsafe_archive"})


def parse_specs(specs: object) -> list[tuple[str, str]]:
    if type(specs) is not list or not 1 <= len(specs) <= 4:
        raise ValueError("invalid_request")
    result = []
    for spec in specs:
        match = SPEC.fullmatch(spec) if type(spec) is str and len(spec) <= 350 else None
        if match is None or len(match[1]) > 214:
            raise ValueError("invalid_request")
        result.append((match[1], match[2]))
    return result


def requested_pairs(specs: object, pairs: object) -> list[list[int]]:
    size = len(parse_specs(specs))
    if pairs is None:
        return [list(pair) for pair in combinations(range(size), 2)]
    if type(pairs) is not list or len(pairs) > 6 or any(
        type(pair) is not list or len(pair) != 2
        or any(type(i) is not int or not 0 <= i < size for i in pair)
        or pair[0] >= pair[1] for pair in pairs
    ) or len({tuple(pair) for pair in pairs}) != len(pairs):
        raise ValueError("invalid_request")
    return pairs


def safe_path(path: object) -> bool:
    return (
        type(path) is str and PATH.fullmatch(path) is not None
        and all(part not in {"", ".", ".."} for part in path.split("/"))
    )


def integrity_tokens(value: object) -> list[tuple[str, bytes]]:
    """Only canonical SRI digests; never reflect arbitrary registry strings."""
    if type(value) is not str or not 1 <= len(value) <= 1024:
        raise ValueError("integrity_failed")
    tokens = []
    for token in value.split(" "):
        algorithm, separator, digest = token.partition("-")
        if not separator or algorithm not in {"sha1", "sha256", "sha384", "sha512"}:
            raise ValueError("integrity_failed")
        try:
            raw = base64.b64decode(digest, validate=True)
        except ValueError:
            raise ValueError("integrity_failed") from None
        expected_size = 20 if algorithm == "sha1" else int(algorithm[3:]) // 8
        if len(raw) != expected_size:
            raise ValueError("integrity_failed")
        if base64.b64encode(raw).decode("ascii") != digest:
            raise ValueError("integrity_failed")
        tokens.append((algorithm, raw))
    return tokens


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class PackageEvidence(_Closed):
    name: str
    version: str
    tarball_sha256: HASH
    registry_integrity: str
    file_count: COUNT
    omitted_path_count: COUNT


class FileEvidence(_Closed):
    path: str
    status: Literal["identical", "changed", "only_left", "only_right"]
    left_sha256: HASH | None
    right_sha256: HASH | None

    @model_validator(mode="after")
    def structural_only(self) -> FileEvidence:
        left, right = self.left_sha256, self.right_sha256
        expected = ("only_right" if left is None else "only_left" if right is None
                    else "identical" if left == right else "changed")
        if not safe_path(self.path) or left is right is None or self.status != expected:
            raise ValueError("invalid file evidence")
        return self


class PairEvidence(_Closed):
    left: int
    right: int
    files: Annotated[list[FileEvidence], Field(max_length=2 * MAX_FILES)]
    omitted_path_count: Annotated[int, Field(ge=0, le=2 * MAX_FILES)]


class ComparisonEvidence(_Closed):
    status: Literal["compared"]
    packages: Annotated[list[PackageEvidence], Field(min_length=1, max_length=4)]
    pairs: Annotated[list[PairEvidence], Field(max_length=6)]


def content_free_result(result: object, specs: object, pairs: object) -> bool:
    """Validate every returned field, not just a NON_INGESTING declaration."""
    if type(result) is dict and set(result) == {"status", "reason"}:
        return result["status"] == "refused" and type(result["reason"]) is str and result["reason"] in REASONS
    try:
        expected = parse_specs(specs)
        pair_indexes = requested_pairs(specs, pairs)
        evidence = ComparisonEvidence.model_validate(result)
        if [(p.name, p.version) for p in evidence.packages] != expected:
            return False
        if [[p.left, p.right] for p in evidence.pairs] != pair_indexes:
            return False
        for package in evidence.packages:
            integrity_tokens(package.registry_integrity)
            if package.omitted_path_count > package.file_count:
                return False
        for pair in evidence.pairs:
            paths = [file.path for file in pair.files]
            if paths != sorted(set(paths)):
                return False
        return True
    except (ValueError, TypeError, ValidationError):
        return False
