"""Stats endpoints (Phase 1c router extraction).

Served under /v1/stats. Read-only; all storage via the HiveMindRepository ABC.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException

from hive.bus.deps import clamp_limit, hive_repo

router = APIRouter()

_COST_GROUP_BY = {"provider", "model", "runtime", "cost_tier", "claimed_by", "day", "kind"}


@router.get("/v1/stats/workers")
async def worker_stats(kind: str | None = None, top_n: int = 30, include_system: bool = False):
    """Per-worker per-kind capability scores. Submitters use this to pick best worker for a kind.

    #8 FIX (review 2026-05-23): exclude `system` kind agents by default (they're host monitors,
    not compute workers — including them was misleading). Pass include_system=true to override.
    """
    top_n = clamp_limit(top_n, default=30, max_limit=500)
    async with hive_repo().connection() as tx:
        rows = await hive_repo().worker_stats_rows(
            tx, kind=kind, include_system=include_system, top_n=top_n
        )
    return {"count": len(rows), "workers": rows}


@router.get("/v1/stats/cache")
async def cache_stats(top_n: int = 20):
    """Result-cache hit-rate + cost savings."""
    top_n = clamp_limit(top_n, default=20, max_limit=500)
    async with hive_repo().connection() as tx:
        stats = await hive_repo().cache_stats(tx, top_n=top_n)
    return stats


@router.get("/v1/stats/costs")
async def cost_stats(since_hours: int = 168, group_by: str = "provider"):
    """Aggregated cost stats. group_by: provider | model | runtime | cost_tier | claimed_by | day.
    since_hours: window in hours (default 168 = 7 days).
    """
    if group_by not in _COST_GROUP_BY:
        raise HTTPException(
            422,
            "group_by must be one of: provider, model, runtime, cost_tier, claimed_by, day, kind",
        )
    cutoff = time.time() - since_hours * 3600
    async with hive_repo().connection() as tx:
        rows = await hive_repo().cost_stats_rows(tx, group_by=group_by, cutoff=cutoff)
    totals = {
        "job_count": sum(r["job_count"] for r in rows),
        "tokens_in": sum(r["tokens_in"] or 0 for r in rows),
        "tokens_out": sum(r["tokens_out"] or 0 for r in rows),
        "estimated_cost_usd": round(sum(r["estimated_cost_usd"] or 0 for r in rows), 4),
    }
    return {"since_hours": since_hours, "group_by": group_by, "totals": totals, "buckets": rows}
