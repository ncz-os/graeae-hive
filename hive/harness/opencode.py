"""OpenCodeAdapter — priority 1 (fleet's backup harness).

opencode runs on every zoder host and speaks the MCP bridge + zc_native_build.
It is the fallback when zoder capacity is unavailable.
"""

from __future__ import annotations

from hive.harness.generic import GenericAdapter


class OpenCodeAdapter(GenericAdapter):
    harness_kind = "opencode"
    priority = 1
    cost_tier = "metered"
    auth_scheme = "bearer"
    base_capabilities = frozenset(
        {
            "code",
            "build",
            "test",
            "debug",
            "refactor",
            "review",
            "python",
            "typescript",
            "rust",
            "bash",
            "docker",
            "linux",
        }
    )
