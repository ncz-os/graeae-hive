"""HarnessAdapter Protocol — pluggable interface for any agent harness.

A harness is the implementing engine behind a worker. The bus (agent_bus.py)
does not know or care which harness a worker uses — it speaks this protocol.

The four first-class harnesses (priority order, lower = preferred):
    0: zoder     — long-range coding tool, fleet's primary author + reviewer
    1: opencode  — fleet's backup harness, runs on every zoder host
    2: codex     — secondary/escalation only, operator-gated (NEVER auto-run)
    3: claude    — Claude desktop + OAuth subscription

Adding a new harness = implementing this Protocol and registering in registry.py.
The bus does not need to change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class HarnessCapabilities:
    """What a harness can do.

    capabilities is a free-form frozenset of strings (e.g., {"code", "review",
    "build", "test", "debug", "refactor", "python", "rust"}). The bus matches
    job-required capabilities against this set during claim eligibility.
    """

    capabilities: frozenset[str] = field(default_factory=frozenset)
    cost_tier: str = "metered"  # "free" | "metered" | "subscription"
    priority: int = 99  # lower = preferred (0 = highest)
    auth_scheme: str = "bearer"  # "bearer" | "oauth" | "api_key" | "none"


@dataclass
class HarnessRegistration:
    """An agent's harness registration payload.

    Sent on POST /v1/agents/register. The bus stores harness_kind +
    harness_version alongside the agent row so future dispatch routes
    correctly without re-asking the worker.
    """

    harness_kind: str  # "zoder" | "opencode" | "codex" | "claude" | "generic"
    harness_version: str
    capabilities: list[str] = field(default_factory=list)
    cost_tier: str = "metered"
    auth_scheme: str = "bearer"
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class HarnessAdapter(Protocol):
    """Pluggable harness adapter.

    A harness_kind maps to a concrete class via hive.harness.registry. The
    class implements this Protocol and exposes the harness contract. The bus
    calls these methods; the adapter translates to the harness's native API.
    """

    harness_kind: str  # class attribute, e.g., "zoder"

    def capabilities(self) -> HarnessCapabilities:
        """Return what this harness can do."""
        ...

    def register(self, registration: HarnessRegistration) -> str:
        """Register a worker using this harness. Return agent URN."""
        ...

    def heartbeat(self, urn: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Process heartbeat. Return status dict."""
        ...

    def claim(self, urn: str, jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Try to claim one job from the offered list. Return claimed job or None."""
        ...

    def execute(self, urn: str, job: dict[str, Any]) -> dict[str, Any]:
        """Execute a claimed job. Return result dict.

        The result includes enough context for the bus to mark the job done
        (job_id, status, output, error). Adapters SHOULD run the harness's
        native engine and surface its output verbatim — no opinionated
        post-processing at this layer.
        """
        ...

    def complete(self, urn: str, job_id: str, result: dict[str, Any]) -> None:
        """Mark job done with result. Adapter persists the result if needed."""
        ...

    def fail(self, urn: str, job_id: str, error: str) -> None:
        """Mark job failed with error message."""
        ...

    def status(self, urn: str) -> dict[str, Any]:
        """Return current harness status (model, queue depth, etc.)."""
        ...
