"""Job admin endpoints (Phase 1c router extraction).

/v1/admin/jobs/* (requeue/purge/reclassify), DELETE /v1/jobs/{id}, and
POST /v1/jobs/purge. Depends on hive.bus.deps + hive.bus.schemas.
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from hive.bus.deps import (
    ACTIVE_JOB_STATUSES,
    _cascade_delete_jobs,
    connect_db,
    emit_event,
    require_mutation_actor,
)
from hive.bus.schemas import (
    JobPurgeRequest,
    PurgeRequest,
    ReclassifyRequest,
    RequeueRequest,
)

router = APIRouter()

# Admin reclassify is an explicit override of the normal transition guard, so the
# target is restricted to a small safe set (e.g. dead-letter -> needs-review for a
# human). Used by the doctor for ambiguous dead-letter jobs.
RECLASSIFY_ALLOWED_TARGETS = {"needs-review", "cancelled", "queued"}


@router.post("/v1/admin/jobs/requeue", dependencies=[Depends(require_mutation_actor)])
async def admin_requeue_jobs(req: RequeueRequest):
    """Force-requeue terminal (failed/cancelled) or any non-done jobs back to queued."""
    results = {"ok": [], "not_found": [], "skipped_done": []}
    async with connect_db() as db:
        for job_id in req.job_ids:
            async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] == "done":
                results["skipped_done"].append(job_id)
                continue
            await db.execute(
                """UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL,
                   ended_at=NULL, result=NULL,
                   claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL,
                   claimed_cost_tier=NULL, claimed_host_caps=NULL,
                   claim_lease_expires_at=NULL,
                   tokens_in=NULL, tokens_out=NULL, estimated_cost_usd=NULL,
                   result_mnemos_id=NULL,
                   retry_count=0, retry_backoff_until=NULL
                   WHERE id=?""",
                (job_id,),
            )
            await db.commit()
            results["ok"].append(job_id)
    return results


@router.post("/v1/admin/jobs/purge", dependencies=[Depends(require_mutation_actor)])
async def admin_purge_jobs(req: PurgeRequest):
    """Permanently remove terminal dead-letter jobs (doctor 'remove' action).

    Safety: ONLY purges jobs currently in 'dead-letter'. Anything else
    (active, or other-terminal done/failed/cancelled) is refused so a stray or
    buggy caller cannot delete live or historical work."""
    results: dict = {"ok": [], "not_found": [], "skipped_not_dead_letter": []}
    async with connect_db() as db:
        for job_id in req.job_ids:
            async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] != "dead-letter":
                results["skipped_not_dead_letter"].append(job_id)
                continue
            await db.execute("DELETE FROM jobs WHERE id=? AND status='dead-letter'", (job_id,))
            await db.commit()
            results["ok"].append(job_id)
    async with connect_db() as event_db:
        for job_id in results["ok"]:
            await emit_event(event_db, "job.purged", {"id": job_id, "reason": req.reason})
    return results


@router.delete("/v1/jobs/{job_id}", dependencies=[Depends(require_mutation_actor)])
async def delete_job(job_id: str, force: bool = False):
    """Delete ONE job record (+ cascade its job_audit_log rows).

    Refuses to delete an in-flight job (status running/claimed) unless
    ?force=true, so a live worker is never orphaned. Returns {deleted:1} or 404.
    """
    async with connect_db() as db:
        async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, f"job {job_id} not found")
        status = row[0]
        if status in ("running", "claimed") and not force:
            raise HTTPException(
                409,
                f"job {job_id} is {status} (in-flight); pass ?force=true to delete anyway",
            )
        # allow_active=force: without force, the cascade re-checks status at delete
        # time, so a queued/offered job that got CLAIMED in the race window is left
        # intact (deleted=0) rather than orphaning the worker (codex review #3).
        deleted = await _cascade_delete_jobs(db, [job_id], allow_active=force)
        await db.commit()
    if not deleted:
        # Lost the race: the job became active (claimed) between read and delete.
        raise HTTPException(
            409,
            f"job {job_id} became in-flight during delete; pass ?force=true to override",
        )
    async with connect_db() as event_db:
        await emit_event(
            event_db,
            "job.deleted",
            {"id": job_id, "prior_status": status, "forced": force},
        )
    return {"deleted": 1, "id": job_id, "prior_status": status}


@router.post("/v1/jobs/purge", dependencies=[Depends(require_mutation_actor)])
async def purge_jobs(req: JobPurgeRequest):
    """Bulk housekeeping purge. JSON-body filters are AND-combined; at least one
    filter is REQUIRED (an unfiltered purge is refused so nothing can delete
    every job). By default jobs in an active status (queued/offered/claimed/
    running/needs-review) are excluded; pass include_active=true to override.

    older_than_seconds compares against COALESCE(ended_at, started_at) so a job
    that has not yet ended is aged from when it entered the queue.

    Returns {deleted:N, matched_ids:[...]}.
    """
    wheres: list[str] = []
    params: list[Any] = []

    statuses: list[str] = []
    if req.status:
        statuses.append(req.status)
    if req.statuses:
        statuses.extend(req.statuses)

    # "Narrow" selectors target a specific subset of jobs (a kind, a submitter, a
    # description fragment, or explicit ids). Age/status are "broad" — on their own
    # they can match nearly the whole table. include_active=true bypasses the
    # in-flight guard, so it MUST be paired with at least one narrow selector
    # (codex review #2 round 2): age/status-only + include_active could otherwise
    # delete ~all jobs incl. running ones.
    has_narrow = any(
        [
            req.kind_prefix,
            req.description_contains,
            req.submitter_urn,
            req.ids,
        ]
    )
    if req.include_active and not has_narrow:
        raise HTTPException(
            422,
            "include_active=true requires a narrow selector "
            "(kind_prefix / description_contains / submitter_urn / ids); "
            "a broad age/status-only purge may not target in-flight jobs",
        )

    has_filter = any(
        [
            req.kind_prefix,
            statuses,
            req.description_contains,
            req.submitter_urn,
            req.older_than_seconds is not None,
            req.ids,
        ]
    )
    if not has_filter:
        raise HTTPException(
            422,
            "refusing unfiltered purge: supply at least one of "
            "kind_prefix/status(es)/description_contains/submitter_urn/"
            "older_than_seconds/ids",
        )

    if req.older_than_seconds is not None and req.older_than_seconds <= 0:
        # A non-positive window yields a future/now cutoff -> COALESCE(...) < cutoff
        # matches essentially every job. Refuse it so older_than_seconds can never
        # be a delete-all wildcard (codex review #2).
        raise HTTPException(422, "older_than_seconds must be > 0")
    if req.kind_prefix:
        # Escape LIKE metacharacters so a literal '%'/'_' in the prefix cannot turn
        # into a match-all wildcard (codex review #2). 'kind%' anchors the prefix.
        kp = req.kind_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        wheres.append("kind LIKE ? ESCAPE '\\'")
        params.append(kp + "%")
    if statuses:
        wheres.append("status IN (%s)" % ",".join("?" for _ in statuses))
        params.extend(statuses)
    if req.description_contains:
        wheres.append("description LIKE ?")
        # escape LIKE metacharacters in caller input so % / _ are literal
        esc = req.description_contains.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append("%" + esc + "%")
        # NB: ESCAPE clause appended below for the description predicate only
        wheres[-1] = "description LIKE ? ESCAPE '\\'"
    if req.submitter_urn:
        wheres.append("submitter_urn=?")
        params.append(req.submitter_urn)
    if req.older_than_seconds is not None:
        wheres.append("COALESCE(ended_at, started_at) < ?")
        params.append(time.time() - float(req.older_than_seconds))
    if req.ids:
        wheres.append("id IN (%s)" % ",".join("?" for _ in req.ids))
        params.extend(req.ids)

    # Active-job guard: unless explicitly included, never match in-flight work.
    if not req.include_active:
        wheres.append("status NOT IN (%s)" % ",".join("?" for _ in ACTIVE_JOB_STATUSES))
        params.extend(ACTIVE_JOB_STATUSES)

    select_sql = "SELECT id FROM jobs WHERE " + " AND ".join(wheres)
    async with connect_db() as db:
        async with db.execute(select_sql, tuple(params)) as cur:
            matched = [r[0] async for r in cur]
        deleted: list[str] = []
        if matched:
            # allow_active mirrors include_active: when not including active jobs the
            # cascade re-checks status at delete time, so a job that got claimed
            # between SELECT and DELETE is skipped (atomic guard, codex review #3).
            deleted = await _cascade_delete_jobs(db, matched, allow_active=req.include_active)
            await db.commit()
    if deleted:
        async with connect_db() as event_db:
            await emit_event(event_db, "jobs.purged", {"count": len(deleted), "reason": req.reason})
    return {"deleted": len(deleted), "matched_ids": deleted}


@router.post("/v1/admin/jobs/reclassify", dependencies=[Depends(require_mutation_actor)])
async def admin_reclassify_jobs(req: ReclassifyRequest):
    """Admin override to move terminal (esp. dead-letter) jobs to a safe target
    status, bypassing ALLOWED_TRANSITIONS. The doctor uses status='needs-review'
    to surface ambiguous dead-letter jobs for a human."""
    if req.status not in RECLASSIFY_ALLOWED_TARGETS:
        raise HTTPException(422, f"status must be one of {sorted(RECLASSIFY_ALLOWED_TARGETS)}")
    results: dict = {"ok": [], "not_found": [], "skipped_done": []}
    async with connect_db() as db:
        for job_id in req.job_ids:
            async with db.execute("SELECT status FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] == "done":
                results["skipped_done"].append(job_id)
                continue
            if req.status == "queued":
                # full requeue semantics (reset claim + retry/backoff)
                await db.execute(
                    "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                    "retry_count=0, retry_backoff_until=NULL WHERE id=?",
                    (job_id,),
                )
            else:
                await db.execute(
                    "UPDATE jobs SET status=?, claimed_by=NULL, claimed_at=NULL WHERE id=?",
                    (req.status, job_id),
                )
            await db.commit()
            results["ok"].append(job_id)
    async with connect_db() as event_db:
        for job_id in results["ok"]:
            await emit_event(
                event_db,
                "job.reclassified",
                {"id": job_id, "new_status": req.status, "reason": req.reason},
            )
    return results
