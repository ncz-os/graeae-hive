"""Harness adapters — pluggable interface to zoder, opencode, codex, claude, generic.

Worker dispatch goes through adapters, not hardcoded if/elif on kind.

Priority order (CLAUDE.md 2026-10-15 directive):
  zoder > opencode > codex > claude

Codex is operator-gated (CLAUDE.md 2026-10-10: NEVER auto-run).
"""

from hive.harness.base import (
    HarnessAdapter,
    HarnessCapabilities,
    HarnessRegistration,
)
from hive.harness.registry import (
    DEFAULT_PRIORITY,
    get_harness_adapter,
    is_harness_enabled,
    list_enabled_harnesses,
    list_harnesses,
    register_external_adapter,
)

__all__ = [
    "DEFAULT_PRIORITY",
    "HarnessAdapter",
    "HarnessCapabilities",
    "HarnessRegistration",
    "get_harness_adapter",
    "is_harness_enabled",
    "list_enabled_harnesses",
    "list_harnesses",
    "register_external_adapter",
]
