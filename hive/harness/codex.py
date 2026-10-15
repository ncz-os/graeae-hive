"""CodexAdapter — priority 2 (secondary/escalation only).

Operator-gated: CLAUDE.md 2026-10-10 says codex must NEVER auto-run. The adapter
is registered so operators can debug it, but ``is_enabled()`` is False unless
``HARNESS_CODEX_AUTORUN=1`` is explicitly set in the environment.
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
