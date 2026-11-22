"""Shared invocation parsing, independent of the lease guard's importability."""
from __future__ import annotations

import os


def pytest_arguments(argv: object, original: object) -> list[str] | None:
    # Embedded interpreters need not expose normal argv/orig_argv sequences.
    if not isinstance(argv, (list, tuple)) or not argv or not isinstance(argv[0], str):
        return None
    if os.path.basename(argv[0]) in {"pytest", "py.test"}:
        return list(argv[1:])
    if argv[0] != "-m" or not isinstance(original, (list, tuple)):
        return None
    index = 1  # Skip the interpreter executable.
    while index < len(original):
        value = original[index]
        if not isinstance(value, str):
            return None
        if value == "-m":
            if index + 1 < len(original) and original[index + 1] == "pytest":
                return list(original[index + 2:])
            return None
        if value.startswith("-m"):
            return list(original[index + 1:]) if value == "-mpytest" else None
        if value in {"-W", "-X", "--check-hash-based-pycs"}:
            index += 2  # These interpreter options consume an operand.
            continue
        if value in {"-c", "--"} or not value.startswith("-"):
            return None
        index += 1
    return None
