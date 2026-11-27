"""Operator release records, separate from immutable publication evidence."""

from __future__ import annotations

import json
from pathlib import Path


def publication_release_path(home: Path, evidence_path: Path) -> Path:
    return home / "state" / "worklink" / "publication-releases" / evidence_path.name


def publication_is_released(home: Path, evidence_path: Path, pr_url: object) -> bool:
    """A release only covers the exact attempt file and recorded PR."""
    try:
        payload = json.loads(publication_release_path(home, evidence_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(payload, dict) and payload.get("pr_url") == pr_url
