from __future__ import annotations

from pathlib import Path

import pytest

from mimir import access_control
from mimir._context import reset_current_turn, set_current_turn
from mimir.models import TurnContext
from mimir.read_policy import framework_large_tool_results_root
from mimir.readonly_backend import WriteGuardBackend


@pytest.fixture
def offloads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("MIMIR_HOME", str(home))
    root = framework_large_tool_results_root(home)
    assert root is not None
    own = root / "turn-1"
    other = root / "turn-2"
    own.mkdir(parents=True)
    other.mkdir()
    (own / "call_one").write_text("result needle\n", encoding="utf-8")
    (other / "call_one").write_text("result needle\n", encoding="utf-8")
    turn = TurnContext("turn-1", "chan", "interactive", "chan", 0)
    turn.evicted_tool_result_ids = ["call_one"]
    return WriteGuardBackend(home, ["state"]), turn, root, own, other


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["ls", "als"])
@pytest.mark.parametrize(
    "case", [
        "own", "other-turn", "artifact-root", "planted", "symlink", "no-ids",
        "no-ids-empty",
        "nested", "directory-entry", "directory-alias",
    ],
)
async def test_ls_only_exempts_own_complete_offload_directory(
    offloads, method: str, case: str,
) -> None:
    backend, turn, root, own, other = offloads
    directory = own
    if case == "other-turn":
        directory = other
    elif case == "artifact-root":
        directory = root
    elif case == "planted":
        (own / "planted").write_text("unlisted result\n", encoding="utf-8")
    elif case == "symlink":
        (own / "call_link").symlink_to(own / "call_one")
        turn.evicted_tool_result_ids.append("call_link")
    elif case == "no-ids":
        turn.evicted_tool_result_ids.clear()
    elif case == "no-ids-empty":
        (own / "call_one").unlink()
        turn.evicted_tool_result_ids.clear()
    elif case == "nested":
        directory = other / "turn-1"
        directory.mkdir()
        (directory / "call_one").write_text("result needle\n", encoding="utf-8")
    elif case == "directory-entry":
        (own / "call_two").mkdir()
        turn.evicted_tool_result_ids.append("call_two")
    elif case == "directory-alias":
        directory = root / "alias"
        directory.symlink_to(own, target_is_directory=True)

    context = set_current_turn(turn)
    capture = access_control.begin_protected_result_capture()
    try:
        result = getattr(backend, method)(str(directory))
        if method == "als":
            result = await result
    finally:
        provenance = access_control.end_protected_result_capture(capture)
        reset_current_turn(context)

    assert result.error is None
    assert provenance is not None
    expected_directory = own if case == "directory-alias" else directory
    assert {(source.domain, source.resource_id, source.integrity)
            for source in provenance.sources} == (
        set() if case == "own" else {("filesystem", str(expected_directory), "untrusted")}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["glob", "aglob", "grep", "agrep"])
@pytest.mark.parametrize("case", ["own", "other-turn", "planted", "no-ids"])
async def test_collection_only_exempts_own_evicted_matches(
    offloads, method: str, case: str,
) -> None:
    backend, turn, root, own, other = offloads
    directory = other if case == "other-turn" else own
    if case == "planted":
        (own / "planted").write_text("result needle\n", encoding="utf-8")
    elif case == "no-ids":
        turn.evicted_tool_result_ids.clear()

    context = set_current_turn(turn)
    capture = access_control.begin_protected_result_capture()
    try:
        pattern = "needle" if "grep" in method else "*"
        result = getattr(backend, method)(pattern, str(directory))
        if method.startswith("a"):
            result = await result
    finally:
        provenance = access_control.end_protected_result_capture(capture)
        reset_current_turn(context)

    assert result.error is None
    assert provenance is not None
    expected = {
        "own": set(),
        "other-turn": {str(other / "call_one")},
        "planted": {str(own / "planted")},
        "no-ids": {str(own / "call_one")},
    }[case]
    assert {(source.domain, source.resource_id, source.integrity)
            for source in provenance.sources} == {
        ("filesystem", path, "untrusted") for path in expected
    }
