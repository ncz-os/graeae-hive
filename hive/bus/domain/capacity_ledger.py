"""Capacity ledger singleton (Phase 5 / Phase 1c extraction).

Per-service admission: MiniMax is the first service with a declared capacity;
its GLOBAL slot count flows through the ledger while the per-worker cap stays in
hive.scheduler. Moved verbatim from agent_bus.py so the jobs router (dequeue)
can import it without a cycle.

The dequeue path mutates `_capacity_ledger.in_flight[...]` and calls
`.headroom(...)`; importers share this one instance.
"""

from __future__ import annotations

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

__all__ = ["_capacity_ledger"]
