"""CodexAdapter — priority 2 (secondary/escalation only).

Operator-gated: CLAUDE.md 2026-10-10 says codex must NEVER auto-run.

Gate semantics (unambiguous): ``HARNESS_CODEX_AUTORUN`` is a POSITIVE opt-in
named for the action it enables. The adapter is DISABLED unless the variable is
exactly the string ``"1"``. Unset, ``"0"``, ``""``, ``"true"``, ``"yes"`` — all
mean disabled. This is the safe default: nothing enables codex by accident, and
an operator must deliberately set ``HARNESS_CODEX_AUTORUN=1`` to turn it on.
"""

from __future__ import annotations

import os
import subprocess

from hive.core.quota import extract_tokens, parse_cli_json, quota_block
from hive.harness.generic import GenericAdapter, job_prompt
from hive.harness.registry import _CODEX_AUTORUN_ENV


def codex_executor(*, binary: str = "codex", timeout: int = 120, cwd: str | None = None):
    """Executor for the unified worker: runs ``codex exec`` headless.

    codex is a SEPARATE harness (OpenAI/ChatGPT subscription) — never dispatched
    through zoder. Captures tokens from json output for the capacity ledger.
    """

    def _run(urn: str, job: dict) -> dict:
        proc = subprocess.run(  # noqa: S603
            [binary, "exec", job_prompt(job)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
        toks = extract_tokens(parse_cli_json(proc.stdout)) or {}
        return {
            "harness": "codex",
            "summary": (proc.stdout or "").strip()[-4000:],
            "stderr": (proc.stderr or "").strip()[-1000:],
            "exit_code": proc.returncode,
            "quota": quota_block(
                provider="openai",
                model="codex",
                tokens_in=toks.get("in"),
                tokens_out=toks.get("out"),
            ),
        }

    return _run


class CodexAdapter(GenericAdapter):
    harness_kind = "codex"
    priority = 2
    cost_tier = "metered"
    auth_scheme = "api_key"
    base_capabilities = frozenset(
        {
            "code",
            "fix",
            "review",
            "debug",
            "refactor",
            "python",
            "typescript",
            "rust",
            "go",
        }
    )

    def is_enabled(self) -> bool:
        """Codex never auto-runs: requires HARNESS_CODEX_AUTORUN=1."""
        return os.environ.get(_CODEX_AUTORUN_ENV, "0") == "1"
