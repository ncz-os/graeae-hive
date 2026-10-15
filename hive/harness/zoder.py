"""ZoderAdapter — priority 0 (fleet's primary author + reviewer).

zoder is the long-range coding tool driving the zeroclaw engine. Fleet config
(CLAUDE.md 2026-10-01): coder=qwen38 (TYDEUS :8006), reviewer=gemma4-31b
(CERBERUS :8080), backup=nemotron-3-ultra-550b (EIH). Highest routing priority.
"""

from __future__ import annotations

from hive.harness.generic import GenericAdapter


class ZoderAdapter(GenericAdapter):
    harness_kind = "zoder"
    priority = 0
    cost_tier = "metered"
    auth_scheme = "bearer"
    base_capabilities = frozenset(
        {
            "code",
            "review",
            "refactor",
            "build",
            "test",
            "debug",
            "python",
            "rust",
            "typescript",
            "go",
            "c",
            "cpp",
            "linux",
            "docker",
            "long-horizon",
        }
    )
