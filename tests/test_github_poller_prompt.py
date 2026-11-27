"""The selected Cultivate poller's changes-requested verification contract."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

from tests.plugin_selection import cultivate_selected

pytestmark = pytest.mark.skipif(
    not cultivate_selected(),
    reason="Cultivate poller is not selected",
)

_POLLER = (
    Path(__file__).resolve().parent.parent
    / "plugins/cultivate/mimir_cultivate/defaults/skills/github-poller/scripts/poller.py"
)


@pytest.fixture(scope="module")
def poller():
    """Load the skill's scripts/poller.py the way production runs it."""
    spec = importlib.util.spec_from_file_location("github_poller_under_test", _POLLER)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(spec.name, None)


def test_selected_poller_guidance_names_provider_test_tool(poller):
    assert "repo_test" in poller._verification_guidance()


def test_env_flag_alphabet_matches_mimir_config(poller):
    """The duplicated boolean parser must not drift from the canonical one.

    ``poller.py`` cannot import ``mimir`` in production, so the alphabet is
    copied. This test runs where ``mimir`` IS importable and fails if the copy
    diverges — which is how the `on`/`y` gap was found in the first place.
    """
    from mimir.config import _ENV_BOOL_FALSY, _ENV_BOOL_TRUTHY

    assert poller._ENV_TRUTHY == _ENV_BOOL_TRUTHY
    assert poller._ENV_FALSY == _ENV_BOOL_FALSY


def test_env_flag_agrees_with_config_env_bool(poller, monkeypatch):
    """Same inputs, same answer — compared against the canonical parser."""
    from mimir.config import _env_bool

    for value in ("1", "true", "yes", "on", "y", "0", "false", "no", "off",
                  "n", "", "  TRUE  ", "maybe", "2"):
        monkeypatch.setenv("MIMIR_FLAG_UNDER_TEST", value)
        for default in (False, True):
            assert poller._env_flag("MIMIR_FLAG_UNDER_TEST", default) == _env_bool(
                "MIMIR_FLAG_UNDER_TEST", default
            ), (value, default)

    monkeypatch.delenv("MIMIR_FLAG_UNDER_TEST", raising=False)
    for default in (False, True):
        assert poller._env_flag("MIMIR_FLAG_UNDER_TEST", default) is default


def test_enabled_guidance_is_runner_neutral_and_selector_accurate(poller, monkeypatch):
    guidance = poller._verification_guidance()
    # ``project_tests`` forwards ``path::node_id`` to whatever runner
    # worklink.yaml configures; only pytest-style runners accept it.
    assert "only when the configured runner accepts" in guidance
    # Selectors are resolved strictly and symlinks are refused.
    assert "already exist" in guidance
    assert "symlink" in guidance
    assert "no flags" in guidance
    assert "focused tests for the changed code with repo_test selectors" in guidance
    assert "omit selectors" not in guidance
    assert "whole suite" not in guidance
    # No runner is named: this file ships to every deployment.
    for runner in ("pytest", "uv ", "npm ", "gradle", "cargo", "go test"):
        assert runner not in guidance, runner


def test_changes_requested_prompt_embeds_the_guidance(poller, monkeypatch):
    """The guidance must actually reach the emitted prompt, not just exist."""
    monkeypatch.setattr(poller, "_github_author_is_trusted", lambda *args: True)
    emitted: list[str] = []
    monkeypatch.setattr(poller, "_emit", lambda prompt, **kw: emitted.append(prompt))

    pr = {
        "number": 42,
        "title": "a title",
        "html_url": "https://example.invalid/pr/42",
        "head": {"sha": "a" * 40, "repo": {"full_name": "owner/repo"}},
        "base": {"ref": "main", "sha": "b" * 40},
        "user": {"login": "self"},
    }
    review = {
        "state": "CHANGES_REQUESTED",
        "user": {"login": "reviewer"},
        "submitted_at": "2026-01-01T00:00:00Z",
    }

    def fake_gh_api(endpoint, token):
        return [review] if endpoint.endswith("/reviews") else [pr]

    monkeypatch.setattr(poller, "_gh_api", fake_gh_api)

    count, _state = poller._check_own_changes_requested("owner/repo", "token", "self", {})

    assert count == 1, "expected one reminder to be emitted"
    assert emitted, "no prompt was emitted"
    assert poller._verification_guidance() in emitted[0]
    assert "re-request review" in emitted[0]
    assert "Read the latest review, inline, and PR comments on this PR" in emitted[0]
    assert "Honour an explicit reviewer instruction to publish" in emitted[0]
    assert "'CI is the gate') on this turn" in emitted[0]
    _assert_remediation_publication_policy(emitted[0])
    assert "is NOT a publication blocker" in emitted[0]
    assert "counts observed and failed_tests non-empty" in emitted[0]
    assert "did not complete (not failed) and rely on PR CI" in emitted[0]
    assert "not authorization, branch protection, required checks" in emitted[0]


def _assert_remediation_publication_policy(guidance):
    text = " ".join(guidance.replace("`", "").split())
    for phrase in (
        "focused tests for the changed code with repo_test selectors",
        "Scoped tests must pass before pushing",
        "Commit before the turn ends",
        "turn might end before tests finish, commit locally first",
        "Never leave a repair uncommitted across a turn boundary",
        "uncommitted lease work is archived and reset at the next turn's checkout",
        "After focused tests pass, commit, push to the PR branch, and re-request review",
        "only when the scope grants pr.rerequest",
        "otherwise report the need via pr_comment or operator escalation",
        "Do not queue a default-suite repo_test before pushing for remediation",
        "Do not produce mutation evidence; reviewers run mutation checks",
        "PR CI is the full-suite gate: all required checks, both core and MIMIR_PLUGINS=cultivate legs",
        "must pass at the published head",
        "A completed failing run with counts observed and failed_tests non-empty blocks publication",
        "not authorization, branch protection, required checks, or required review",
    ):
        assert phrase in text
    assert "Attempt the full suite" not in text
    assert "omit selectors" not in text


@pytest.mark.parametrize("skill", ["github-poller", "github-ci-watch"])
def test_remediation_skill_guidance(skill):
    guidance = (_POLLER.parents[2] / skill / "SKILL.md").read_text()
    _assert_remediation_publication_policy(guidance)


@pytest.mark.parametrize("trigger", ["poller", "repo_test_complete"])
def test_rendered_remediation_and_completion_keep_publication_policy(trigger):
    from mimir.models import AgentEvent
    from mimir.prompts import build_turn_prompt
    from mimir.skill_resolver import find_skill_for_channel

    event = AgentEvent(
        trigger=trigger, channel_id="poller:github-activity",
        content=('{"job_id":"rt_fix","status":"completed","result":{"ok":true}}'
                 if trigger == "repo_test_complete" else "Repair requested changes"),
    )
    skill = find_skill_for_channel(event.channel_id, [_POLLER.parents[2]])
    assert skill is not None
    _assert_remediation_publication_policy(build_turn_prompt(event, auto_skill_block=skill))


def test_own_feedback_prompt_embeds_publication_policy(poller, monkeypatch):
    emitted = []
    monkeypatch.setattr(poller, "_emit", lambda prompt, **kw: emitted.append(prompt))
    poller._emit_own_feedback("owner/repo", "self", {
        "42": {
            "pr": {"number": 42, "head": {}, "base": {}, "user": {"login": "self"}},
            "feedback": [("pr_review:1", "Fix this", "https://example.invalid/pr/42", "reviewer")],
        },
    }, set())
    [prompt] = emitted
    _assert_remediation_publication_policy(prompt)


def test_ci_failure_prompt_embeds_publication_policy(poller, monkeypatch, tmp_path):
    from datetime import datetime, timezone

    emitted = []
    monkeypatch.setattr(poller, "STATE_DIR", tmp_path)
    monkeypatch.setattr(poller, "_emit", lambda prompt, **kw: emitted.append(prompt))
    monkeypatch.setattr(poller, "_remediation_logs", lambda *_: "")
    poller._deliver_pr_ci(
        "owner/repo",
        {"number": 42, "head": {"sha": "a" * 40}, "base": {},
         "user": {"login": "self"}},
        "token", "self",
        [{"id": 1, "name": "tests", "conclusion": "failure",
          "completed_at": "2026-11-26T12:00:00Z"}], {},
        datetime(2026, 11, 27, tzinfo=timezone.utc),
        datetime(2026, 11, 26, tzinfo=timezone.utc),
    )
    [prompt] = emitted
    _assert_remediation_publication_policy(prompt)


def test_conflict_skill_still_requires_both_full_legs_at_exact_merge_commit():
    guidance = (_POLLER.parent.parent / "SKILL.md").read_text()
    conflict = guidance.split("An eligible conflict emits", 1)[1].split(
        "- **Completed check failures", 1,
    )[0]
    text = " ".join(conflict.replace("`", "").split())
    assert (
        "Run repo_test without selectors using the configured default suite's full "
        "core and MIMIR_PLUGINS=cultivate legs after committing. Both must pass at "
        "exactly that commit before repo_push"
    ) in text


def test_skill_base_update_section_names_merge_default_and_rebase_exception():
    skill = (_POLLER.parent.parent / "SKILL.md").read_text()
    section = skill.split("## Updating a PR with its base\n", 1)[1].split("\n## ", 1)[0]
    assert "Default to `repo_merge`" in section
    assert "`repo_rebase`" in section
    assert "only on history-rewrite turns" in section
    section = " ".join(section.split())
    assert (
        "If it merges cleanly, the merge commit already exists; next call `repo_push`."
        in section
    )
    assert (
        "On conflicts: `repo_unmerged`, resolve, `repo_stage`, then `repo_commit` "
        "listing every staged path (the merge stages auto-merged files too), "
        "then `repo_push`."
        in section
    )
