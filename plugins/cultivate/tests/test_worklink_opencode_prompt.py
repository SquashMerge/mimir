"""Leaf shell guidance reflects the permissions actually sent to OpenCode."""

from __future__ import annotations

import pytest

from mimir_cultivate.worklink.backends.base import WorkOrder
from mimir_cultivate.worklink.backends.opencode import DEFAULT_BASH_ALLOWLIST, OpenCodeBackend


@pytest.mark.parametrize("rules", [None, "Repository rules\n"])
@pytest.mark.parametrize("allowlist", [
    DEFAULT_BASH_ALLOWLIST,
    ("git *", "uv *", "npm *", "npx *", "sh scripts/heavy_run.sh *"),
    ("git *",),
    ("uv *",),
])
def test_opencode_leaf_prompt_effective_allowlist_and_guidance(tmp_path, rules, allowlist):
    order = WorkOrder(
        2177, tmp_path, "Implement the leaf issue", rules, 30,
        {"MIMIR_HOME": str(tmp_path), "MIMIR_MODEL_SPEC": "codex-plus:gpt-test"},
    )
    spec = OpenCodeBackend(bash_allowlist=allowlist).work_spec(
        order, attempt=1, repo_url="repo", base_ref="main",
        branch="issue/2177", test_command="uv run pytest",
    )
    request = spec.session_request
    assert request is not None
    assert request.kind == "leaf"
    assert request.shell_allowlist == allowlist
    assert spec.backend_config["bash_allowlist"] == list(allowlist)

    prompt = request.text
    listed_patterns = [
        line.removeprefix("- `").removesuffix("`")
        for line in prompt.splitlines() if line.startswith("- `")
    ]
    assert listed_patterns == list(allowlist)
    assert listed_patterns == list(request.shell_allowlist)
    assert "Each shell command must itself start with an allowed prefix" in prompt
    assert "Compound commands are judged by their whole text" in prompt
    assert "do not prefix an allowed command with `pwd;`, `cd …;` or similar" in prompt
    assert (
        "Use the read, grep, glob and list tools for inspection "
        "instead of `cat`, `find`, `ls` or `pwd`"
    ) in prompt
    assert (
        'A single `permission.rejected` shell result means "use an allowed form", '
        'not "shell is unavailable"'
    ) in prompt
    assert "Retry using an allowed command" in prompt
    assert (
        "`WORKLINK_BLOCKED` for shell access is only appropriate when an allowed command "
        "(for example, `git status` when `git *` is listed above) is itself refused"
    ) in prompt
    expected_order = order.prompt if rules is None else f"{rules.rstrip()}\n\n{order.prompt}"
    assert prompt.endswith(expected_order)
    assert spec.prompt == order.prompt
    assert spec.rules == order.rules
