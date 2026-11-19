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
