"""Claude subscription leaf backend; execution remains in shared compute."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Sequence
import uuid

from mimir.config import model_spec_at_call_time
from mimir.redaction import redact_text
from ...claude_code import bash_rules, leaf_argv, leaf_environment, parse_stream, resolve_model, secret_scrubber
from ..compute import ComputeResult, WorkSpec
from ..execution_policy import CLAUDE_LEAF
from .base import Caps, CheckoutShape, RawResult, WorkOrder


@dataclass(frozen=True)
class ClaudeCodeBackend:
    model: str | None = None
    bash_allowlist: Sequence[str] | None = None
    pass_env: tuple[str, ...] = ()
    name: str = "claude_code"
    checkout_shape: CheckoutShape = CheckoutShape.ISOLATED_CLONE

    def __post_init__(self) -> None:
        if self.model is not None:
            resolve_model(None, self.model)

    def capabilities(self) -> Caps:
        return Caps("coding-cli", False, True, False, "claude_code", CLAUDE_LEAF)

    def work_spec(self, order: WorkOrder, *, attempt: int, repo_url: str,
                  base_ref: str, branch: str, test_command: str) -> WorkSpec:
        values = {**os.environ, **order.env}
        home = values.get("MIMIR_HOME")
        configured = values.get("MIMIR_MODEL_SPEC")
        if configured is None:
            configured = model_spec_at_call_time(Path(home) if home else None)
        model = resolve_model(configured, self.model)
        rules = bash_rules(test_command, self.bash_allowlist)
        environment = leaf_environment(values, self.pass_env)
        scrubber = secret_scrubber(values)
        identifier = str(uuid.uuid4())
        worker_home = Path("/var/lib/mimir-worklink/homes") / identifier
        prompt = order.prompt + (f"\n\n{order.rules}" if order.rules else "")
        prompt = scrubber.scrub_text(prompt)
        return WorkSpec(
            issue_id=order.issue_id, attempt=attempt, repo_url=repo_url, base_ref=base_ref,
            branch=branch, prompt=prompt, rules=None, test_command=test_command,
            backend=self.name, timeout_s=order.timeout_s, env=environment,
            backend_config={"model": model, "bash_allowlist": rules, "pass_env": tuple(environment),
                            "worker_identifier": identifier},
            local_checkout=order.checkout, local_argv=leaf_argv(prompt, model, worker_home, rules),
            output_root=order.transcript_root, execution=CLAUDE_LEAF,
        )

    async def interpret(self, order: WorkOrder, result: object) -> RawResult:
        if not isinstance(result, ComputeResult):
            raise TypeError("ClaudeCodeBackend.interpret expects ComputeResult")
        scrubber = secret_scrubber({**os.environ, **order.env})
        def clean(value: str) -> str:
            return redact_text(scrubber.scrub_text(value))

        def bounded(value: str) -> str:
            return clean(value).encode("utf-8")[-1024 * 1024:].decode("utf-8", "ignore")

        if result.launch_error or result.timed_out or result.output_overflow or result.output_truncated:
            from ...claude_code import StreamResult
            parsed = StreamResult("failed", "Claude execution did not produce complete output")
        else:
            parsed = parse_stream(result.stdout, result.exit_code)
        directory = order.transcript_root or Path(os.environ.get("MIMIR_HOME", ".")) / "state/worklink/transcripts"
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        path = directory / f"claude_code-{order.issue_id}-{stamp}.json"
        payload = {
            "backend": self.name, "status": parsed.status, "exit_code": result.exit_code,
            "command": [clean(arg) for arg in result.command],
            "stdout": bounded(result.stdout), "stderr": bounded(result.stderr),
            "timed_out": result.timed_out, "output_overflow": result.output_overflow,
        }
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "w") as stream:
            json.dump(payload, stream, ensure_ascii=False)
        for stale in sorted(directory.glob(f"claude_code-{order.issue_id}-*.json"), reverse=True)[10:]:
            stale.unlink(missing_ok=True)
        return RawResult(result.exit_code, path, parsed.status, clean(parsed.error) if parsed.error else None,
                         clean(parsed.blocked_reason) if parsed.blocked_reason else None,
                         output_overflow=result.output_overflow, retry_after_ms=parsed.retry_after_ms)
