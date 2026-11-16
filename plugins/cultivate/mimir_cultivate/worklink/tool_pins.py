"""Source-controlled Worklink tool-pin data."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ToolPin:
    name: str
    category: str
    pin: str
    smoke: str
    source: str | None = None
    package: str | None = None
    repo: str | None = None
    install: str | None = None
    risk: str | None = None


OPENCODE_VERSION = "2.2.7"


DEFAULT_TOOL_PINS: tuple[ToolPin, ...] = (
    ToolPin(
        name="chainlink",
        category="issue-cli",
        pin="chainlink-1.6.0",
        smoke="chainlink --version && chainlink issue ready",
        source="github-release",
        repo="dollspace-gay/chainlink",
        install="chainlink bundled-skill dockerfile.fragment",
        risk="High: Worklink coordination depends on issue, lock, comment, and dependency semantics staying compatible.",
    ),
    ToolPin(
        name="mermaid-cli",
        category="renderer",
        pin="11.16.0",
        smoke="mmdc --version",
        source="npm",
        package="@mermaid-js/mermaid-cli",
        install="scaffold Dockerfiles; used by the mermaid-diagrams skill rather than Worklink execution",
        risk="Low: renderer drift is usually isolated to diagram generation and Chromium dependencies.",
    ),
    ToolPin(
        name="osv-scanner",
        category="security-scanner",
        pin="v2.4.0",
        smoke="osv-scanner --version",
        source="github-release",
        repo="google/osv-scanner",
        install="dependency-advisory-watch optional-skill dockerfile.fragment",
        risk="High: dependency security findings and fail-open behavior depend on scanner CLI semantics.",
    ),
    ToolPin(
        name="gogcli",
        category="integration-cli",
        pin="v0.9.0",
        smoke=(
            "gog --version && gog gmail messages search 'in:inbox newer_than:1d' "
            "--account \"$GOG_ACCOUNT\" --max 1 --json --no-input"
        ),
        source="github-release",
        repo="steipete/gogcli",
        install="gmail-poller optional-skill dockerfile.fragment",
        risk="High: Google Workspace helper CLI; pre-1.0 minor-version drift can break Gmail polling subcommands on Muninn, so version jumps need an authenticated smoke before merge.",
    ),
    ToolPin(
        name="opencode",
        category="coding-cli",
        pin=OPENCODE_VERSION,
        smoke="opencode --version",
        source="npm",
        package="@opencode/cli",
        install="root/scaffold Dockerfiles only when MIMIR_ENABLE_OPENCODE=1",
        risk="High: Worklink coding backend; changes can affect prompt execution, plugin loading, and auth flow.",
    ),
    ToolPin(
        name="codex",
        category="coding-cli",
        pin="0.139.0",
        smoke="codex --version",
        source="npm",
        package="@openai/codex",
        install="Cultivate docker/base.sh only when MIMIR_ENABLE_OPENCODE=1; deployment-owned subscription login",
        risk="High: Codex subscription login CLI; changes can affect device authentication and credential format.",
    ),
    ToolPin(
        name="feature-factory",
        category="coding-cli",
        pin="0.14.0",
        smoke="test -f \"$MIMIR_FACTORY_ENTRYPOINT\"",
        source="npm",
        package="feature-factory",
        install="root/scaffold Dockerfiles only when MIMIR_ENABLE_OPENCODE=1; owns the absolute factory.js CLI entrypoint",
        risk="High: Feature factory state-machine CLI; changes can affect Worklink epic lifecycle and recovery.",
    ),
    ToolPin(
        name="opencode2-feature-factory",
        category="coding-plugin",
        pin="0.14.0",
        smoke="opencode --version",
        source="npm",
        package="opencode2-feature-factory",
        install="root/scaffold Dockerfiles only when MIMIR_ENABLE_OPENCODE=1; registered as OpenCode plugin",
        risk="High: Feature factory plugin; changes can affect Worklink epic/leaf dispatch and review flow.",
    ),
    ToolPin(
        name="opencode-plugin-otel",
        category="coding-plugin",
        pin="2.1.0",
        smoke="opencode --version",
        source="npm",
        package="@devtheops/opencode-plugin-otel",
        install="root/scaffold Dockerfiles only when MIMIR_ENABLE_OPENCODE=1; daemon inline telemetry policy",
        risk="High: Telemetry content and credential isolation depend on exact plugin gates and options.",
    ),
)


def default_tool_pins() -> tuple[ToolPin, ...]:
    """Return the source-controlled initial Worklink external executable inventory."""

    return DEFAULT_TOOL_PINS
