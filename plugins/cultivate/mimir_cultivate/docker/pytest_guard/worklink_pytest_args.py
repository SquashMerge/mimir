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
        if value == "--check-hash-based-pycs":
            index += 2
            continue
        if value == "-" or not value.startswith("-") or value.startswith("--"):
            return None
        # CPython scans short-option bundles until an operand-taking flag.
        # Its operand is the remainder of this token, or the next token.
        for position, flag in enumerate(value[1:], start=1):
            if flag not in {"c", "m", "W", "X"}:
                continue
            operand = value[position + 1:]
            if not operand:
                index += 1
                if index >= len(original) or not isinstance(original[index], str):
                    return None
                operand = original[index]
            if flag == "m":
                return list(original[index + 1:]) if operand == "pytest" else None
            if flag == "c":
                return None
            break  # W/X consume the rest; it cannot contain another flag.
        index += 1
    return None
