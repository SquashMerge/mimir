"""Hypothesis profiles for the nightly property suite.

These tests live outside ``testpaths``: the default ``pytest`` run and every
PR/merge-queue job skip them. ``.github/workflows/nightly-property.yml`` runs
them on a schedule. Each property sets its own calibrated ``max_examples``;
the profile supplies the run-wide settings.

``HYPOTHESIS_PROFILE=nightly`` (the workflow's setting) additionally prints a
reproduction blob for every failure. The default ``local`` profile keeps
Hypothesis's own example database for quick reruns.
"""

from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

settings.register_profile(
    "local",
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow,),
)
settings.register_profile(
    "nightly",
    parent=settings.get_profile("local"),
    print_blob=True,
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "local"))


# The nightly job must not stay green while silently losing coverage: the
# Worklink campaigns skip at import time when Cultivate is absent. With
# PROPERTY_TESTS_FORBID_SKIPS=1 (set by the workflow) any skipped test or module
# fails the session.
_SKIPPED: list[str] = []


def pytest_collectreport(report) -> None:
    if report.skipped:
        _SKIPPED.append(report.nodeid)


def pytest_runtest_logreport(report) -> None:
    if report.skipped:
        _SKIPPED.append(report.nodeid)


def pytest_sessionfinish(session, exitstatus) -> None:
    if os.environ.get("PROPERTY_TESTS_FORBID_SKIPS") == "1" and _SKIPPED and exitstatus == 0:
        session.exitstatus = 1


def pytest_terminal_summary(terminalreporter) -> None:
    if os.environ.get("PROPERTY_TESTS_FORBID_SKIPS") == "1" and _SKIPPED:
        terminalreporter.write_line(
            f"PROPERTY_TESTS_FORBID_SKIPS: {len(_SKIPPED)} skipped (failing the run): "
            + ", ".join(sorted(set(_SKIPPED))),
            red=True,
        )
