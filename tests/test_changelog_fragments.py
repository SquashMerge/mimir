"""Release notes stay conflict-free until the release PR collects them."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
POINTER = "Changes awaiting release are recorded in [changelog.d/](changelog.d/)."
BASE = f"# Changelog\n\n## [Unreleased]\n\n{POINTER}\n\n## [0.9.6] — 2026-10-08\n\nOld release.\n"


def _unreleased(text: str) -> str:
    return text.split("## [Unreleased]\n", 1)[1].split("## [", 1)[0]


def _fragment_is_valid(text: str) -> bool:
    return re.match(r"^[-*+]\s", text) is not None


def test_unreleased_is_only_the_pointer() -> None:
    assert _unreleased((ROOT / "CHANGELOG.md").read_text(encoding="utf-8")) == f"\n{POINTER}\n\n"


def test_all_fragments_start_with_a_bullet() -> None:
    for path in (ROOT / "changelog.d").glob("*.md"):
        if path.name != "README.md":
            assert _fragment_is_valid(path.read_text(encoding="utf-8")), path.name


@pytest.mark.parametrize("bad", ["", "A paragraph\n", "\n- Delayed bullet\n"])
def test_fragment_format_rejects_empty_or_nonbullet(bad: str) -> None:
    assert not _fragment_is_valid(bad)


@pytest.fixture
def release_tree(tmp_path: Path) -> Path:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "changelog.d").mkdir()
    shutil.copyfile(ROOT / "scripts" / "changelog_collect.py", tmp_path / "scripts" / "changelog_collect.py")
    (tmp_path / "CHANGELOG.md").write_text(BASE, encoding="utf-8")
    (tmp_path / "changelog.d" / "README.md").write_text("Ignore this file.\n", encoding="utf-8")
    return tmp_path


def _run(root: Path, version: str = "1.2.3") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "changelog_collect.py"), version, "--date", "2026-10-09"],
        capture_output=True, text=True,
    )


def test_collects_fragments_sorted_and_verbatim(release_tree: Path) -> None:
    fragments = release_tree / "changelog.d"
    (fragments / "z.md").write_bytes(b"- Last  \n  with trailing spaces.\n")
    (fragments / "a.md").write_bytes(b"- First\n  continued.\n")
    assert _run(release_tree).returncode == 0
    assert (release_tree / "CHANGELOG.md").read_text() == (
        BASE.split("## [0.9.6]", 1)[0]
        + "## [1.2.3] — 2026-10-09\n\n"
        + "- First\n  continued.\n\n- Last  \n  with trailing spaces.\n\n"
        + "## [0.9.6] — 2026-10-08\n\nOld release.\n"
    )
    assert sorted(p.name for p in fragments.iterdir()) == ["README.md"]


def test_collects_legacy_unreleased_entries(release_tree: Path) -> None:
    original = (release_tree / "CHANGELOG.md").read_text()
    (release_tree / "CHANGELOG.md").write_text(
        original.replace(f"{POINTER}\n\n", f"{POINTER}\n\n- Legacy item\n  continuation.\n\n")
    )
    (release_tree / "changelog.d" / "a.md").write_text("- New item\n")
    assert _run(release_tree).returncode == 0
    result = (release_tree / "CHANGELOG.md").read_text()
    assert _unreleased(result) == f"\n{POINTER}\n\n"
    assert "## [1.2.3] — 2026-10-09\n\n- New item\n\n- Legacy item\n  continuation.\n\n" in result
    assert "## [0.9.6] — 2026-10-08\n\nOld release.\n" in result


def test_collects_legacy_entries_without_fragments(release_tree: Path) -> None:
    changelog = release_tree / "CHANGELOG.md"
    changelog.write_text(BASE.replace(f"{POINTER}\n\n", "- Older bullet\n\n- Another bullet\n\n"))
    assert _run(release_tree).returncode == 0
    assert "## [1.2.3] — 2026-10-09\n\n- Older bullet\n\n- Another bullet\n\n" in changelog.read_text()
    assert _unreleased(changelog.read_text()) == f"\n{POINTER}\n\n"


def test_nothing_to_collect_fails_without_changes(release_tree: Path) -> None:
    before = (release_tree / "CHANGELOG.md").read_bytes()
    result = _run(release_tree)
    assert result.returncode != 0
    assert "nothing to collect" in result.stderr
    assert (release_tree / "CHANGELOG.md").read_bytes() == before
    assert (release_tree / "changelog.d" / "README.md").exists()


def test_publish_workflow_extracts_collector_section(release_tree: Path) -> None:
    (release_tree / "changelog.d" / "a.md").write_text("- Released item\n")
    assert _run(release_tree).returncode == 0
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text()
    pattern = re.search(r'''notes="\$\(awk -v v="\$version" '([^']+)' CHANGELOG\.md\)"''', workflow)
    assert pattern, "publish.yml awk pattern changed; update this contract check"
    extracted = subprocess.run(
        ["awk", "-v", "v=1.2.3", pattern.group(1), "CHANGELOG.md"],
        cwd=release_tree, capture_output=True, text=True, check=True,
    ).stdout
    assert extracted == "## [1.2.3] — 2026-10-09\n\n- Released item\n\n"


@pytest.mark.parametrize("bad", ["", "Not a bullet\n"])
def test_collector_refuses_bad_fragment_without_changes(release_tree: Path, bad: str) -> None:
    fragment = release_tree / "changelog.d" / "a.md"
    fragment.write_text(bad)
    result = _run(release_tree)
    assert result.returncode != 0
    assert "must start with a Markdown bullet" in result.stderr
    assert (release_tree / "CHANGELOG.md").read_text() == BASE
    assert fragment.exists()


@pytest.mark.parametrize("version", ["1.0.0rc1", "1.0.0a2", "1.0.0b1"])
def test_collects_a_pep440_prerelease_section(release_tree: Path, version: str) -> None:
    (release_tree / "changelog.d" / "a.md").write_text("- Candidate item\n")
    assert _run(release_tree, version).returncode == 0
    assert f"## [{version}] — 2026-10-09\n\n- Candidate item\n\n" in (release_tree / "CHANGELOG.md").read_text()


@pytest.mark.parametrize("version", ["1.0", "1.0.0-rc1", "1.0.0rc", "1.0.0.dev1", "v1.0.0"])
def test_rejects_versions_that_are_not_release_or_prerelease(release_tree: Path, version: str) -> None:
    (release_tree / "changelog.d" / "a.md").write_text("- Item\n")
    result = _run(release_tree, version)
    assert result.returncode != 0
    assert "version must be" in result.stderr
    assert (release_tree / "CHANGELOG.md").read_text() == BASE


@pytest.mark.parametrize(("version", "prerelease"), [
    ("1.0.0rc1", True), ("1.0.0a2", True), ("1.0.0b1", True), ("1.0.0", False), ("0.9.11", False),
])
def test_publish_workflow_marks_only_prereleases(version: str, prerelease: bool) -> None:
    """A pre-release tag must not become the repo's "Latest" GitHub release."""
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text()
    pattern = re.search(r"""grep -Eq '([^']+)'; then\n\s+prerelease=\(--prerelease\)""", workflow)
    assert pattern, "publish.yml pre-release detection changed; update this contract check"
    assert workflow.count('--verify-tag "${prerelease[@]}"') == 2
    matched = subprocess.run(["grep", "-Eq", pattern.group(1)], input=f"{version}\n", text=True).returncode == 0
    assert matched is prerelease
