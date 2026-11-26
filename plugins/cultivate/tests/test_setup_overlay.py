"""Additional setup contracts; existing setup regression tests stay untouched."""
from __future__ import annotations

import pytest

from test_setup import worklink_script_config  # noqa: F401


def test_setup_ignores_operator_overlay_and_preserves_edit_refusal(worklink_script_config):
    home, provision = worklink_script_config
    assert provision()
    generated = home / "worklink.yaml"
    before = generated.read_bytes()
    overlay = home / "worklink.local.yaml"
    contents = b"defaults:\n  max_concurrent: 7\nbackends:\n  opencode:\n    bash_allowlist_extra: ['npm *']\n"
    overlay.write_bytes(contents)
    assert not provision()
    assert generated.read_bytes() == before
    assert overlay.read_bytes() == contents
    from mimir_cultivate.worklink.backends.registry import WorklinkConfig

    assert WorklinkConfig.load(generated).defaults.max_concurrent == 7
    edited = before + b"# hand edit\n"
    generated.write_bytes(edited)
    with pytest.raises(RuntimeError, match="--force"):
        provision()
    assert generated.read_bytes() == edited
    assert overlay.read_bytes() == contents
    assert provision(force=True)
    assert generated.read_bytes() == before
    assert overlay.read_bytes() == contents
