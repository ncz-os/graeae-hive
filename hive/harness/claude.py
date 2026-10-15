"""ClaudeAdapter — priority 3 (Claude desktop + OAuth subscription).

Auth is OAuth (Claude Pro subscription), not a bearer/API key — the auth plugin
(Phase C) resolves the token. Lowest priority of the first-class harnesses.
"""

from __future__ import annotations

import subprocess

from hive.core.quota import extract_tokens, parse_cli_json, quota_block
from hive.harness.generic import GenericAdapter, job_prompt


def claude_executor(*, binary: str = "claude", timeout: int = 120, cwd: str | None = None):
    """Executor for the unified worker: runs ``claude -p`` headless.

    claude is a SEPARATE harness (Anthropic/Claude Pro subscription) — its
    credits are usable only through the Claude CLI, never via zoder.
    """

    def _run(urn: str, job: dict) -> dict:
        proc = subprocess.run(  # noqa: S603
            [binary, "-p", job_prompt(job)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
        toks = extract_tokens(parse_cli_json(proc.stdout)) or {}
        return {
            "harness": "claude",
            "summary": (proc.stdout or "").strip()[-4000:],
            "stderr": (proc.stderr or "").strip()[-1000:],
            "exit_code": proc.returncode,
            "quota": quota_block(
                provider="anthropic",
                model="claude",
                tokens_in=toks.get("in"),
                tokens_out=toks.get("out"),
            ),
        }

    return _run


class ClaudeAdapter(GenericAdapter):
    harness_kind = "claude"
    priority = 3
    cost_tier = "subscription"
    auth_scheme = "oauth"
    base_capabilities = frozenset(
        {
            "code",
            "review",
            "refactor",
            "debug",
            "reasoning",
            "architecture_design",
            "python",
            "typescript",
            "bash",
        }
    )
