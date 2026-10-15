"""ClaudeAdapter — priority 3 (Claude desktop + OAuth subscription).

Auth is OAuth (Claude Pro subscription), not a bearer/API key — the auth plugin
(Phase C) resolves the token. Lowest priority of the first-class harnesses.
"""

from __future__ import annotations

from hive.harness.generic import GenericAdapter


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
