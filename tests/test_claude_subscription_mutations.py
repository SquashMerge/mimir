from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
CULTIVATE = "plugins/cultivate/mimir_cultivate/"


@dataclass(frozen=True)
class Mutation:
    name: str
    path: str
    edits: tuple[tuple[str, str], ...]
    test: str
    assertion: str


MUTATIONS = (
    Mutation(
        "old-class-compute-selection", CULTIVATE + "worklink/orchestrator.py",
        (('elif execution.home_projection_profile != "none":',
          'elif backend.__class__.__name__ == "OpenCodeBackend":'),),
        "tests/test_worklink_orchestrator.py::test_claude_compute_selection_uses_capabilities",
        "assert requested_uids == [4242]",
    ),
    Mutation(
        "old-name-uid-drop", CULTIVATE + "worklink/orchestrator.py",
        (('or execution.home_projection_profile != "none"\n        )',
          'or backend.name == "opencode"\n        )'),),
        "tests/test_worklink_orchestrator.py::test_claude_uid_drop_uses_capabilities",
        'assert checkout_requests[0]["worker_eligible"] is True',
    ),
    Mutation(
        "old-name-containment", CULTIVATE + "worklink/compute.py",
        (('if spec.execution.home_projection_profile != "none":',
          'if self.name == "opencode":'),),
        "tests/test_worklink_claude_code.py::test_claude_containment_does_not_depend_on_compute_name",
        "controller fallback",
    ),
    Mutation(
        "leaf-github-credential-leak", CULTIVATE + "claude_code.py",
        (("if EPIC_CREDENTIALS.intersection(pass_env):",
          "if False and EPIC_CREDENTIALS.intersection(pass_env):"),),
        "tests/test_claude_code.py::test_claude_leaf_rejects_epic_credentials",
        "DID NOT RAISE",
    ),
    Mutation(
        "dual-spawn-registration", CULTIVATE + "policy.py",
        (("for name, declaration in sorted(_TOOL_DECLARATIONS.items()):",
          'for name, declaration in sorted(_TOOL_DECLARATIONS.items()) + '
          '[("spawn_claude_code", _TOOL_DECLARATIONS["spawn_open_code"])]:'),
         ('if name == "spawn_open_code" and claude_route:',
          'if False and name == "spawn_open_code" and claude_route:')),
        "tests/test_spawn_claude_code.py::test_claude_spawn_surface_is_route_exclusive",
        'assert "spawn_open_code" not in selected.tools',
    ),
    Mutation(
        "unconditional-opencode-install", CULTIVATE + "docker/base.sh",
        (('if [ "$MIMIR_ENABLE_OPENCODE" = "1" ]; then', "if true; then"),),
        "tests/test_root_dockerfile.py::test_claude_route_does_not_install_opencode",
        "assert installs ==",
    ),
    Mutation(
        "unconditional-claude-install", CULTIVATE + "docker/base.sh",
        (('if [ "${MIMIR_WORKLINK_HOST:-opencode}" = claude_code ]; then', "if true; then"),),
        "tests/test_root_dockerfile.py::test_legacy_routes_do_not_install_claude[1]",
        "assert len(installs) == int(enabled)",
    ),
)


@pytest.fixture
def disposable_repository(tmp_path: Path) -> Path:
    """Copy source and tests, never the active checkout's Git or runtime state."""
    destination = tmp_path / "repository"
    destination.mkdir()
    for name in ("mimir", "plugins", "tests", "scripts", "deploy"):
        shutil.copytree(
            ROOT / name, destination / name,
            ignore=shutil.ignore_patterns(
                "__pycache__", "*.pyc", ".pytest_cache", ".venv", "node_modules",
            ),
        )
    for name in ("Dockerfile", "pyproject.toml", "uv.lock"):
        shutil.copy2(ROOT / name, destination / name)
    return destination


def run_control(repository: Path, mutation: Mutation, stage: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("MIMIR_PLUGINS", None)
    env.pop("MIMIR_ACCESS_CONTROL_ENFORCED", None)
    env["PYTHONPATH"] = os.pathsep.join((
        str(repository), str(repository / "plugins/cultivate"),
    ))
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--tb=short", "-o", "addopts=", mutation.test],
        cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, timeout=180,
    )
    (repository.parent / f"{mutation.name}-{stage}.log").write_text(result.stdout)
    return result


@pytest.mark.parametrize("mutation", MUTATIONS, ids=lambda mutation: mutation.name)
def test_reversible_subscription_mutation(disposable_repository: Path, mutation: Mutation) -> None:
    repository = disposable_repository
    path = repository / mutation.path
    original = path.read_bytes()
    baseline = run_control(repository, mutation, "baseline")
    assert baseline.returncode == 0, baseline.stdout
    assert "1 passed" in baseline.stdout and "skipped" not in baseline.stdout, baseline.stdout

    changed = original.decode()
    for before, after in mutation.edits:
        assert changed.count(before) == 1, (mutation.name, before)
        changed = changed.replace(before, after)
    try:
        path.write_text(changed)
        # A mutant must remain executable; collection/import/syntax failures
        # cannot stand in for the mapped behavioral assertion.
        if path.suffix == ".py":
            compile(changed, str(path), "exec")
        else:
            subprocess.run(["sh", "-n", str(path)], check=True)
        mutant = run_control(repository, mutation, "mutant")
    finally:
        path.write_bytes(original)
    assert path.read_bytes() == original
    restored = run_control(repository, mutation, "restored")
    assert restored.returncode == 0, restored.stdout
    assert "1 passed" in restored.stdout and "skipped" not in restored.stdout, restored.stdout
    assert mutant.returncode == 1, mutant.stdout
    assert "1 failed" in mutant.stdout and "ERROR" not in mutant.stdout, mutant.stdout
    assert mutation.assertion in mutant.stdout, mutant.stdout
    assert any(message in mutant.stdout for message in (
        "AssertionError", "DID NOT RAISE", "\nE   assert ", "\nE   Failed: controller fallback",
    )), mutant.stdout
