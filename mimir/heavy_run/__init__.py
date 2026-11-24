"""Installed location of the shared heavy-run protocol helper."""
from __future__ import annotations

from importlib import resources
from pathlib import Path


def helper_path() -> Path:
    """Resolve only core package data, never a worker or controller checkout."""
    helper = Path(str(resources.files(__package__).joinpath("heavy_run.py")))
    if not helper.is_file():
        raise FileNotFoundError("packaged heavy-run helper unavailable")
    return helper
