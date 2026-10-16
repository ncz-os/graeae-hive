"""Capacity ledger singleton (Phase 5 / Phase 1c extraction).

Per-service admission: MiniMax is the first service with a declared capacity;
its GLOBAL slot count flows through the ledger while the per-worker cap stays in
hive.scheduler. Moved verbatim from agent_bus.py so the jobs router (dequeue)
can import it without a cycle.

The dequeue path mutates `_capacity_ledger.in_flight[...]` and calls
`.headroom(...)`; importers share this one instance.
"""

from __future__ import annotations

import time
from typing import Any

from hive import scheduler as sched
from hive.bus import capacity as _capacity

_capacity_ledger = _capacity.CapacityLedger()
_capacity_ledger.register(
    _capacity.ServiceCapacity(
        service=sched.MINIMAX_PROVIDER,
        slots=sched.MINIMAX_GLOBAL_MAX,
        is_subscription=True,
    )
)


async def load_capacity_ledger(repo: Any) -> int:
    """Hydrate the shared ledger from persisted state (ABC). Idempotent: a reload
    converges to the stored counters. Returns rows loaded."""
    rows = 0
    async with repo.connection() as tx:
        for service, in_flight, tokens_used, spent_usd, limited_until, _window, _updated in await repo.capacity_state_load(tx):
            _capacity_ledger.in_flight[service] = int(in_flight or 0)
            _capacity_ledger.tokens_used[service] = int(tokens_used or 0)
            _capacity_ledger.spent_usd[service] = float(spent_usd or 0.0)
            _capacity_ledger.limited_until[service] = float(limited_until or 0.0)
            rows += 1
    return rows


async def flush_capacity_ledger(repo: Any) -> int:
    """Persist the shared ledger through the ABC as keyed SET-upserts (idempotent —
    re-flushing the same state is a no-op). Returns rows written."""
    now = time.time()
    written = 0
    async with repo.connection() as tx:
        for service in _capacity_ledger.capacities:
            await repo.capacity_state_save(
                tx,
                service=service,
                in_flight=_capacity_ledger.in_flight.get(service, 0),
                tokens_used=_capacity_ledger.tokens_used.get(service, 0),
                spent_usd=_capacity_ledger.spent_usd.get(service, 0.0),
                limited_until=_capacity_ledger.limited_until.get(service, 0.0),
                window_start=0.0,
                updated_at=now,
            )
            written += 1
        await tx.commit()
    return written


__all__ = ["_capacity_ledger", "load_capacity_ledger", "flush_capacity_ledger"]
