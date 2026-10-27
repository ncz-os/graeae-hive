"""Scheduled-jobs endpoints (Phase 1c router extraction).

Served under /v1/schedules. Depends only on hive.bus.deps + hive.bus.schemas.
"""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, Depends, HTTPException

from hive.bus.deps import connect_db, hive_repo, require_mutation_actor, require_orchestrator_submitter, uuidv7
from hive.bus.schemas import JobCreate, ScheduleCreate

router = APIRouter()


@router.post("/v1/schedules", dependencies=[Depends(require_mutation_actor)])
async def create_schedule(req: ScheduleCreate):
    """Create a recurring scheduled job. Re-fires every `interval_seconds`.
    job_template is the JobCreate body that will be submitted each tick.
    Submitter_urn auto-injected from caller — they take responsibility for the cron loop.
    """
    sid = uuidv7()
    now = time.time()
    # Validate the template parses as a JobCreate
    try:
        tpl_copy = dict(req.job_template)
        if not tpl_copy.get("submitter_urn"):
            raise ValueError("job_template.submitter_urn is required and must be a registered orchestrator")
        JobCreate(**tpl_copy)
    except Exception as e:
        raise HTTPException(422, f"job_template invalid: {e}") from e
    await require_orchestrator_submitter(tpl_copy["submitter_urn"])
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        await hive_repo().create_schedule(
            tx,
            id=sid,
            name=req.name,
            created_by_urn=tpl_copy.get("submitter_urn"),
            interval_seconds=int(req.interval_seconds),
            job_template_json=json.dumps(req.job_template),
            enabled=1 if req.enabled else 0,
            next_fire_at=now + req.interval_seconds,
            created_at=now,
        )
        await tx.commit()
    return {"id": sid, "name": req.name, "next_fire_at": now + req.interval_seconds}


@router.get("/v1/schedules")
async def list_schedules():
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        raw = await hive_repo().list_schedules(tx)
    rows = [
        {
            "id": r["id"],
            "name": r["name"],
            "created_by": r["created_by_urn"],
            "interval_seconds": r["interval_seconds"],
            "enabled": r["enabled"],
            "last_fired_at": r["last_fired_at"],
            "next_fire_at": r["next_fire_at"],
            "fire_count": r["fire_count"],
            "created_at": r["created_at"],
        }
        for r in raw
    ]
    return {"count": len(rows), "schedules": rows}


@router.patch("/v1/schedules/{sid}", dependencies=[Depends(require_mutation_actor)])
async def patch_schedule(sid: str, enabled: bool | None = None, interval_seconds: int | None = None):
    sets, args = [], []
    if enabled is not None:
        sets.append("enabled=?")
        args.append(1 if enabled else 0)
    if interval_seconds is not None:
        if interval_seconds < 60 or interval_seconds > 86400 * 30:
            raise HTTPException(422, "interval_seconds must be 60..2592000")
        sets.append("interval_seconds=?")
        args.append(int(interval_seconds))
        sets.append("next_fire_at=?")
        args.append(time.time() + int(interval_seconds))
    if not sets:
        raise HTTPException(422, "no fields to update")
    args.append(sid)
    async with connect_db() as db:
        cur = await db.execute(f"UPDATE scheduled_jobs SET {', '.join(sets)} WHERE id=?", args)
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}


@router.delete("/v1/schedules/{sid}", dependencies=[Depends(require_mutation_actor)])
async def delete_schedule(sid: str):
    async with connect_db() as db:
        cur = await db.execute("DELETE FROM scheduled_jobs WHERE id=?", (sid,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}
