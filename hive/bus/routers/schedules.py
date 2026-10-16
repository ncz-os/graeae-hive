"""Scheduled-jobs endpoints (Phase 1c router extraction).

Served under /v1/schedules. Depends only on hive.bus.deps + hive.bus.schemas.
"""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException

from hive.bus.deps import connect_db, require_orchestrator_submitter, uuidv7
from hive.bus.schemas import JobCreate, ScheduleCreate

router = APIRouter()


@router.post("/v1/schedules")
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
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO scheduled_jobs (id, name, created_by_urn, interval_seconds, "
            "job_template, enabled, last_fired_at, next_fire_at, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (
                sid,
                req.name,
                tpl_copy.get("submitter_urn"),
                int(req.interval_seconds),
                json.dumps(req.job_template),
                1 if req.enabled else 0,
                now + req.interval_seconds,
                now,
            ),
        )
        await db.commit()
    return {"id": sid, "name": req.name, "next_fire_at": now + req.interval_seconds}


@router.get("/v1/schedules")
async def list_schedules():
    rows = []
    async with (
        connect_db() as db,
        db.execute(
            "SELECT id, name, created_by_urn, interval_seconds, enabled, "
            "last_fired_at, next_fire_at, fire_count, created_at FROM scheduled_jobs"
        ) as cur,
    ):
        async for r in cur:
            rows.append(
                {
                    "id": r[0],
                    "name": r[1],
                    "created_by": r[2],
                    "interval_seconds": r[3],
                    "enabled": bool(r[4]),
                    "last_fired_at": r[5],
                    "next_fire_at": r[6],
                    "fire_count": r[7],
                    "created_at": r[8],
                }
            )
    return {"count": len(rows), "schedules": rows}


@router.patch("/v1/schedules/{sid}")
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


@router.delete("/v1/schedules/{sid}")
async def delete_schedule(sid: str):
    async with connect_db() as db:
        cur = await db.execute("DELETE FROM scheduled_jobs WHERE id=?", (sid,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}
