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

from hive.harness.generic import GenericAdapter
from hive.harness.registry import _CODEX_AUTORUN_ENV


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
