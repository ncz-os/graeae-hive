"""Per-service capacity ledger + admission control (Phase 5).

Dole out jobs against each model service's *available capacity* so the fleet
never exceeds a service's throughput / credit pool. Generalizes the MiniMax
slot cap (the first instance) to every service.

Four dimensions per service (see docs/decision-capacity-ledger-20261015.md):

  slots     concurrent in-flight sessions      (MiniMax ~4-5 on the $500/yr plan)
  rate      tokens / requests per minute       (codex / minimax API limits)
  credits   spend vs a pool over a window      (subscription / prepaid)
  cost×time $/MTok scaled by time-of-day       (DeepSeek peak 2x)

The ledger is a plain in-memory structure (no DB dependency) so it is testable
and reusable; the bus owns persistence + the worker-side telemetry that feeds it.
Subscriptions are estimated+self-correcting; DeepSeek/API surfaces are exact.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class ServiceCapacity:
    """Declared capacity for one model service (a provider, scoped by model)."""

    service: str  # provider key, e.g. "minimax" / "deepseek"
    slots: int | None = None  # max concurrent in-flight sessions (None = unbounded)
    tpm: int | None = None  # tokens per minute ceiling
    rpm: int | None = None  # requests per minute ceiling
    credit_cap_usd: float | None = None  # spend pool over the window
    credit_window_sec: float = 24 * 3600.0
    is_subscription: bool = False  # estimated (no quota API) vs metered (exact)


# DeepSeek bills 2x during peak: 01:00-04:00 and 06:00-10:00 UTC, Mon-Fri.
# (CLAUDE.md / rules/deepseek-peak-pricing.md.) Offpeak is the base rate.
DEEPSEEK_PEAK_HOURS_UTC = ((1, 4), (6, 10))
DEEPSEEK_PEAK_MULTIPLIER = 2.0


def time_multiplier(provider: str, now: float | None = None) -> float:
    """Cost multiplier for ``provider`` at ``now`` (DeepSeek peak = 2x)."""
    if (provider or "").lower() != "deepseek":
        return 1.0
    dt = datetime.fromtimestamp(now if now is not None else time.time(), tz=UTC)
    if dt.weekday() >= 5:  # Sat/Sun: offpeak all day
        return 1.0
    for start, end in DEEPSEEK_PEAK_HOURS_UTC:
        if start <= dt.hour < end:
            return DEEPSEEK_PEAK_MULTIPLIER
    return 1.0


@dataclass
class CapacityLedger:
    """Live counters + admission for a set of services.

    Not thread-safe by itself; the bus serializes access (asyncio, one loop).
    """

    capacities: dict[str, ServiceCapacity] = field(default_factory=dict)
    in_flight: dict[str, int] = field(default_factory=dict)
    tokens_used: dict[str, int] = field(default_factory=dict)
    spent_usd: dict[str, float] = field(default_factory=dict)
    limited_until: dict[str, float] = field(default_factory=dict)

    # -- registration -------------------------------------------------------

    def register(self, capacity: ServiceCapacity) -> None:
        self.capacities[capacity.service] = capacity
        self.in_flight.setdefault(capacity.service, 0)

    # -- admission ----------------------------------------------------------

    def headroom(
        self,
        service: str,
        *,
        est_tokens: int = 0,
        est_cost_usd: float = 0.0,
        now: float | None = None,
    ) -> tuple[bool, str]:
        """Return ``(ok, reason)`` for admitting one job to ``service``.

        Fails closed on an unknown service (must be declared). A service marked
        ``limited_until > now`` is denied until the learned window resets.
        """
        now = time.time() if now is None else now
        cap = self.capacities.get(service)
        if cap is None:
            return False, f"{service}: unknown service (no declared capacity)"
        until = self.limited_until.get(service, 0.0)
        if until > now:
            return False, f"{service}: limited until {int(until - now)}s (rate/usage window)"
        if cap.slots is not None and self.in_flight.get(service, 0) >= cap.slots:
            return False, f"{service}: slots full ({self.in_flight.get(service, 0)}/{cap.slots})"
        if cap.tpm is not None and est_tokens:
            # admit only if the projected minute usage stays under the ceiling
            if self.tokens_used.get(service, 0) + est_tokens > cap.tpm:
                return False, f"{service}: token rate would exceed {cap.tpm}/min"
        if cap.credit_cap_usd is not None:
            projected = self.spent_usd.get(service, 0.0) + est_cost_usd * time_multiplier(service, now)
            if projected > cap.credit_cap_usd:
                return False, f"{service}: credit pool would exceed ${cap.credit_cap_usd}"
        return True, "ok"

    def acquire(self, service: str) -> None:
        self.in_flight[service] = self.in_flight.get(service, 0) + 1

    def release(self, service: str) -> None:
        self.in_flight[service] = max(0, self.in_flight.get(service, 0) - 1)

    def record_usage(self, service: str, *, tokens: int = 0, cost_usd: float = 0.0, now: float | None = None) -> None:
        """Fold observed usage (from worker telemetry) into the window counters."""
        self.tokens_used[service] = self.tokens_used.get(service, 0) + max(0, tokens)
        self.spent_usd[service] = self.spent_usd.get(service, 0.0) + cost_usd * time_multiplier(service, now)

    def mark_limited(self, service: str, *, until: float) -> None:
        """Mark a service limited until ``until`` (from a 429/usage-limit signal)."""
        self.limited_until[service] = max(self.limited_until.get(service, 0.0), until)

    def reset_window(self, service: str) -> None:
        """Clear the window counters for a service (on its credit/rate reset)."""
        self.tokens_used[service] = 0
        self.spent_usd[service] = 0.0
        self.limited_until[service] = 0.0
