"""Regressions for isolation shared by core and provider test trees."""
from types import SimpleNamespace

import pytest

from tests import isolation_fixtures


def test_maintenance_pins_share_config_lifetime_across_fixture_instances():
    """Two conftests' FixtureDefs must see the same active executable set."""
    from mimir import access_control

    before = access_control._MAINTENANCE_PINNED_EXECUTABLES.copy()
    cleanups = []
    config = SimpleNamespace(stash=pytest.Stash(), add_cleanup=cleanups.append)
    setup = isolation_fixtures.maintenance_pinned_executables.__wrapped__
    try:
        core_pins = setup(config)
        provider_pins = setup(config)
        assert core_pins is provider_pins
        assert len(cleanups) == 1
        assert access_control._MAINTENANCE_PINNED_EXECUTABLES == core_pins
        assert config.stash[isolation_fixtures._MAINTENANCE_PINS_KEY] is core_pins
        assert all(path.is_file() for path in core_pins.values())
        pin_directory = core_pins["git"].parent
    finally:
        for cleanup in reversed(cleanups):
            cleanup()
    assert access_control._MAINTENANCE_PINNED_EXECUTABLES == before
    assert isolation_fixtures._MAINTENANCE_PINS_KEY not in config.stash
    assert not pin_directory.exists()
