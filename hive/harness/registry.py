"""Harness adapter registry.

Mirrors the pattern in hive.persistence.factory: env-driven selection +
module:Class spec for backend loading.

Priority order (CLAUDE.md 2026-10-15 directive):
    0: zoder
    1: opencode
    2: codex    (operator-gated)
    3: claude
"""

from __future__ import annotations

import importlib
import os

from hive.harness.base import HarnessAdapter

# Built-in adapters — loaded lazily via module:Class spec.
_BUILTIN_ADAPTERS: dict[str, str] = {
    "zoder": "hive.harness.zoder:ZoderAdapter",
    "opencode": "hive.harness.opencode:OpenCodeAdapter",
    "codex": "hive.harness.codex:CodexAdapter",
    "claude": "hive.harness.claude:ClaudeAdapter",
    "generic": "hive.harness.generic:GenericAdapter",
}

# Lower priority number = preferred.  CLAUDE.md 2026-10-15 directive.
DEFAULT_PRIORITY: dict[str, int] = {
    "zoder": 0,
    "opencode": 1,
    "codex": 2,
    "claude": 3,
    "generic": 99,
}

# Codex is operator-gated (CLAUDE.md 2026-10-10: NEVER auto-run).
# Adapter is registered (so operators can debug it) but disabled by default.
_CODEX_AUTORUN_ENV = "HARNESS_CODEX_AUTORUN"


def get_harness_adapter(harness_kind: str) -> HarnessAdapter:
    """Load a harness adapter by kind.

    Looks up harness_kind in _BUILTIN_ADAPTERS and instantiates the
    configured class. If harness_kind is a fully-qualified module:Class
    spec, loads that directly (escape hatch for external adapters).

    Raises:
        KeyError: unknown harness_kind
        ImportError: adapter module not importable
        AttributeError: adapter module does not expose the named class
    """
    spec = _BUILTIN_ADAPTERS.get(harness_kind)
    if spec is None:
        if ":" in harness_kind:
            spec = harness_kind  # treat as module:Class
        else:
            raise KeyError(
                f"unknown harness kind: {harness_kind!r}. "
                f"known: {sorted(_BUILTIN_ADAPTERS)}"
            )
    module_path, _, attr = spec.partition(":")
    if not module_path or not attr:
        raise ValueError(f"invalid adapter spec: {spec!r} (need module:Class)")
    module = importlib.import_module(module_path)
    cls = getattr(module, attr)
    return cls()


def is_harness_enabled(harness_kind: str) -> bool:
    """Check if a harness is enabled (env-controlled).

    Codex is operator-gated: HARNESS_CODEX_AUTORUN must be "1" to enable.
    All other built-in harnesses are enabled by default.
    """
    if harness_kind == "codex":
        return os.environ.get(_CODEX_AUTORUN_ENV, "0") == "1"
    if harness_kind not in _BUILTIN_ADAPTERS:
        return False  # unknown kinds default to disabled
    return True


def list_harnesses() -> list[str]:
    """List known harness kinds (in priority order)."""
    return sorted(
        _BUILTIN_ADAPTERS.keys(),
        key=lambda k: (DEFAULT_PRIORITY.get(k, 99), k),
    )


def list_enabled_harnesses() -> list[str]:
    """List harness kinds that are both known AND env-enabled."""
    return [k for k in list_harnesses() if is_harness_enabled(k)]


def register_external_adapter(harness_kind: str, module_class_spec: str) -> None:
    """Register an external adapter (escape hatch).

    Example:
        register_external_adapter("mycompany-coder", "my_pkg.adapters:CoderAdapter")

    This is intended for fleet operators who want to plug in a private
    harness without modifying this file. Prefer the built-in entries for
    first-class harnesses.
    """
    if ":" not in module_class_spec:
        raise ValueError(f"invalid module:Class spec: {module_class_spec!r}")
    _BUILTIN_ADAPTERS[harness_kind] = module_class_spec
    DEFAULT_PRIORITY.setdefault(harness_kind, 50)
