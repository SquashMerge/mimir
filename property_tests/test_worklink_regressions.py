"""Minimized Worklink lock-table regressions (#2175, W1/W2)."""

from __future__ import annotations

import json
import subprocess

import pytest

pytest.importorskip("mimir_cultivate", reason="Worklink belongs to the optional Cultivate plugin")
from mimir_cultivate.worklink.claims import ChainlinkClaims


def test_empty_bare_lock_table_counts_as_zero_reservations():
    claims = ChainlinkClaims(
        agent_id="test",
        runner=lambda argv: subprocess.CompletedProcess(argv, 0, "[]", ""),
    )
    assert claims.active_worklink_lock_count() == 0


def test_identity_required_lock_table_rejects_anonymous_list_entry():
    claims = ChainlinkClaims(
        agent_id="test",
        runner=lambda argv: subprocess.CompletedProcess(
            argv, 0, json.dumps({"locks": [{}]}), "",
        ),
    )
    with pytest.raises(RuntimeError, match="omitted issue identity"):
        claims._active_worklink_lock_ids(require_identity=True)
