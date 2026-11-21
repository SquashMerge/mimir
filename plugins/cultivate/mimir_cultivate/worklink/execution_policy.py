"""Server-owned execution boundaries, independent of tool and compute names."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionPolicy:
    substrate: str = "contained_subprocess"
    retained_state: str = "none"
    checkout_protocol: str = "attempt"
    retirement: str = "contained_process_tree"
    home_projection_profile: str = "none"

    def __post_init__(self) -> None:
        choices = {
            "substrate": {"contained_subprocess", "opencode_daemon"},
            "retained_state": {"none", "opencode_session", "factory_record"},
            "checkout_protocol": {"attempt", "factory_worker"},
            "retirement": {"contained_process_tree", "daemon_session"},
            "home_projection_profile": {"none", "claude_leaf", "claude_factory"},
        }
        for name, allowed in choices.items():
            if getattr(self, name) not in allowed:
                raise ValueError(f"invalid execution policy {name}")


OPENCODE_LEAF = ExecutionPolicy("opencode_daemon", "opencode_session", "attempt", "daemon_session")
OPENCODE_FACTORY = ExecutionPolicy("opencode_daemon", "factory_record", "factory_worker", "daemon_session")
CLAUDE_LEAF = ExecutionPolicy(home_projection_profile="claude_leaf")
CLAUDE_FACTORY = ExecutionPolicy(retained_state="factory_record", checkout_protocol="factory_worker",
                                 home_projection_profile="claude_factory")


def execution_for_backend(backend: object) -> ExecutionPolicy:
    """Older extensions receive the same fail-closed execution default."""
    capabilities = getattr(backend, "capabilities", None)
    policy = getattr(capabilities() if callable(capabilities) else None, "execution", None)
    return policy if isinstance(policy, ExecutionPolicy) else ExecutionPolicy()


def home_projection_documents(profile: str) -> tuple[tuple[str, bytes], ...]:
    if profile == "none":
        return ()
    onboarding = (".claude/.claude.json", b'{"hasCompletedOnboarding":true}')
    if profile == "claude_factory":
        return (onboarding,)
    if profile == "claude_leaf":
        return (onboarding, (".claude/settings.json", b'{"disableAllHooks":true}'))
    raise ValueError("unknown worker home projection profile")
