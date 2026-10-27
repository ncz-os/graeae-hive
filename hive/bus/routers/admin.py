"""Job admin endpoints (Phase 1c router extraction).

/v1/admin/jobs/* (requeue/purge/reclassify), DELETE /v1/jobs/{id}, and
POST /v1/jobs/purge. Depends on hive.bus.deps + hive.bus.schemas.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from hive.bus.deps import (
    ACTIVE_JOB_STATUSES,
    DEP_PURGED_STATUS,
    _cascade_delete_jobs,
    connect_db,
    emit_event,
    hive_repo,
    release_dependencies,
    require_mutation_actor,
)
from hive.bus.domain.review_dispatch import dispatch_needs_review
from hive.bus.schemas import (
    JobPurgeRequest,
    PurgeRequest,
    ReclassifyRequest,
    RequeueRequest,
)

router = APIRouter()
log = logging.getLogger("hive.bus.admin")


def _retry_budget_exhausted(max_retries: Any, retry_count: Any) -> bool:
    """max_retries 0, or retry_count >= max_retries: the submitter's retry budget is spent.

    Same rule as the doctor's ``retry_budget_exhausted``: an unreadable budget fails
    closed. ``max_retries`` is NOT NULL in every backend schema, so a real row always
    has it."""
    if max_retries is None or isinstance(max_retries, bool):
        return True
    try:
        mr = int(max_retries)
        rc = int(retry_count or 0)
    except (TypeError, ValueError):
        return True
    return mr <= 0 or rc >= mr


def _override_ok(req: Any) -> bool:
    return bool(getattr(req, "override_max_retries", False)) and bool(
        str(getattr(req, "override_reason", "") or "").strip()
    )


async def _budget_row(tx, job_id: str):
    """(status, max_retries, retry_count) for job_id, or None."""
    async with tx.execute("SELECT status, max_retries, retry_count FROM jobs WHERE id=?", (job_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    row = tuple(row)
    return (row[0], row[1] if len(row) > 1 else None, row[2] if len(row) > 2 else None)


async def _emit_overrides(overrides: list[dict], actor: str, reason: str) -> None:
    if not overrides:
        return
    async with hive_repo().connection() as event_tx:
        for item in overrides:
            await emit_event(event_tx, "job.requeue_override", {**item, "actor": actor, "reason": reason})
        await event_tx.commit()


def _actor_name(actor: Any) -> str:
    # Called directly (tests, internal callers) the Depends default is not a string.
    return actor if isinstance(actor, str) else "unknown"


# Admin reclassify is an explicit override of the normal transition guard, so the
# target is restricted to a small safe set (e.g. dead-letter -> needs-review for a
# human). Used by the doctor for ambiguous dead-letter jobs.
RECLASSIFY_ALLOWED_TARGETS = {"needs-review", "cancelled", "queued"}


@router.post("/v1/admin/jobs/requeue")
async def admin_requeue_jobs(req: RequeueRequest, actor: str = Depends(require_mutation_actor)):
    """Force-requeue terminal (failed/cancelled) or any non-done jobs back to queued.

    A job with no retry budget left (max_retries 0, or retry_count >= max_retries)
    is skipped into ``skipped_retry_budget`` unless the request carries
    ``override_max_retries: true`` with a non-empty ``override_reason``; an
    override is logged and emitted as ``job.requeue_override``."""
    actor = _actor_name(actor)
    results = {"ok": [], "not_found": [], "skipped_done": [], "skipped_retry_budget": [], "skipped_conflict": []}
    overrides: list[dict] = []
    # Storage via the HiveMindRepository ABC on one transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        for job_id in req.job_ids:
            row = await _budget_row(tx, job_id)
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] == "done":
                results["skipped_done"].append(job_id)
                continue
            if _retry_budget_exhausted(row[1], row[2]):
                if not _override_ok(req):
                    log.warning(
                        "requeue of %s refused: no retry budget (max_retries=%s retry_count=%s) actor=%s reason=%s",
                        job_id,
                        row[1],
                        row[2],
                        actor,
                        req.reason,
                    )
                    results["skipped_retry_budget"].append(job_id)
                    continue
                log.warning(
                    "requeue of %s OVERRIDES its retry budget (max_retries=%s retry_count=%s) actor=%s override_reason=%s",
                    job_id,
                    row[1],
                    row[2],
                    actor,
                    req.override_reason,
                )
                overrides.append(
                    {
                        "id": job_id,
                        "max_retries": row[1],
                        "retry_count": row[2],
                        "override_reason": req.override_reason,
                    }
                )
            changed = await hive_repo().patch_job_status(
                tx,
                job_id,
                sets={
                    "status": "queued",
                    "claimed_by": None,
                    "claimed_at": None,
                    "ended_at": None,
                    "result": None,
                    "claimed_runtime": None,
                    "claimed_model": None,
                    "claimed_provider": None,
                    "claimed_cost_tier": None,
                    "claimed_host_caps": None,
                    "claim_lease_expires_at": None,
                    "tokens_in": None,
                    "tokens_out": None,
                    "estimated_cost_usd": None,
                    "result_mnemos_id": None,
                    "retry_count": 0,
                    "retry_backoff_until": None,
                },
                expected_status=row[0],
            )
            if changed != 1:
                results["skipped_conflict"].append(job_id)
                continue
            results["ok"].append(job_id)
        await tx.commit()
    await _emit_overrides([item for item in overrides if item["id"] in results["ok"]], actor, req.reason)
    return results


@router.post("/v1/admin/jobs/purge", dependencies=[Depends(require_mutation_actor)])
async def admin_purge_jobs(req: PurgeRequest):
    """Permanently remove terminal dead-letter jobs (doctor 'remove' action).

    Safety: ONLY purges jobs currently in 'dead-letter'. Anything else
    (active, or other-terminal done/failed/cancelled) is refused so a stray or
    buggy caller cannot delete live or historical work."""
    results: dict = {"ok": [], "not_found": [], "skipped_not_dead_letter": []}
    deleted: list[str] = []
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
            deleted.append(job_id)
            results["ok"].append(job_id)
            # DEPENDENCY RELEASE (incident 2026-10-27): released in the SAME
            # transaction as the deletes, per purged id.
            await release_dependencies(db, job_id, DEP_PURGED_STATUS, emit_event=emit_event)
        await db.commit()
    if deleted:
        async with connect_db() as event_db:
            await emit_event(event_db, "jobs.purged", {"count": len(deleted), "reason": req.reason})
    return {**results, "deleted": len(deleted), "matched_ids": deleted}


@router.post("/v1/jobs/purge", dependencies=[Depends(require_mutation_actor)])
async def purge_jobs(req: JobPurgeRequest):
    """Housekeeping purge: delete the terminal jobs matching the given filters.

    Every filter that is set is ANDed. Active statuses (queued/offered/claimed/
    running/needs-review) are excluded unless ``include_active`` is set, so a
    stray call cannot delete live work. This is the route the doctor's retention
    sweep uses (``{"ids": [...], "reason": "doctor-retention-sweep"}``) and the
    one the bus SOP contract names; dependents of a purged row are released in
    the same transaction."""
    where: list[str] = []
    args: list[Any] = []
    if req.ids:
        where.append("id IN (%s)" % ",".join("?" for _ in req.ids))
        args.extend(req.ids)
    if req.kind_prefix:
        where.append("kind LIKE ?")
        args.append(req.kind_prefix + "%")
    statuses: list[str] = []
    if req.status:
        statuses.append(req.status)
    if req.statuses:
        statuses.extend(req.statuses)
    if statuses:
        where.append("status IN (%s)" % ",".join("?" for _ in statuses))
        args.extend(statuses)
    if req.description_contains:
        where.append("description LIKE ?")
        args.append("%" + req.description_contains + "%")
    if req.submitter_urn:
        where.append("submitter_urn = ?")
        args.append(req.submitter_urn)
    if req.older_than_seconds is not None:
        where.append("started_at <= ?")
        args.append(time.time() - float(req.older_than_seconds))
    if not req.include_active:
        where.append("status NOT IN (%s)" % ",".join("?" for _ in ACTIVE_JOB_STATUSES))
        args.extend(ACTIVE_JOB_STATUSES)
    if not where:
        raise HTTPException(422, "refusing an unfiltered purge: set ids or at least one filter")
    sql = "SELECT id FROM jobs WHERE " + " AND ".join(where)
    if req.ids:
        # One row per requested id, so the response can report what matched.
        sql += " LIMIT %d" % (len(req.ids) * 4)
    matched: list[str] = []
    deleted: list[str] = []
    async with connect_db() as db:
        async with db.execute(sql, tuple(args)) as cur:
            matched = [r[0] for r in await cur.fetchall()]
        if matched:
            deleted = await _cascade_delete_jobs(db, matched, allow_active=req.include_active)
            for jid in deleted:
                await release_dependencies(db, jid, DEP_PURGED_STATUS, emit_event=emit_event)
            await db.commit()
    if deleted:
        async with connect_db() as event_db:
            await emit_event(event_db, "jobs.purged", {"count": len(deleted), "reason": req.reason})
    return {"deleted": len(deleted), "matched_ids": deleted}


@router.post("/v1/admin/jobs/reclassify")
async def admin_reclassify_jobs(req: ReclassifyRequest, actor: str = Depends(require_mutation_actor)):
    """Admin override to move terminal (esp. dead-letter) jobs to a safe target
    status, bypassing ALLOWED_TRANSITIONS. The doctor uses status='needs-review'
    to surface ambiguous dead-letter jobs for a human."""
    if req.status not in RECLASSIFY_ALLOWED_TARGETS:
        raise HTTPException(422, f"status must be one of {sorted(RECLASSIFY_ALLOWED_TARGETS)}")
    actor = _actor_name(actor)
    results: dict = {"ok": [], "not_found": [], "skipped_done": [], "skipped_retry_budget": [], "skipped_conflict": []}
    overrides: list[dict] = []
    # Storage via the HiveMindRepository ABC on one transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        for job_id in req.job_ids:
            row = await _budget_row(tx, job_id)
            if not row:
                results["not_found"].append(job_id)
                continue
            if row[0] == "done":
                results["skipped_done"].append(job_id)
                continue
            if req.status == "queued" and _retry_budget_exhausted(row[1], row[2]):
                # reclassify -> queued is a requeue: same retry-budget rule.
                if not _override_ok(req):
                    log.warning(
                        "reclassify->queued of %s refused: no retry budget (max_retries=%s retry_count=%s) actor=%s",
                        job_id,
                        row[1],
                        row[2],
                        actor,
                    )
                    results["skipped_retry_budget"].append(job_id)
                    continue
                log.warning(
                    "reclassify->queued of %s OVERRIDES its retry budget (max_retries=%s retry_count=%s) "
                    "actor=%s override_reason=%s",
                    job_id,
                    row[1],
                    row[2],
                    actor,
                    req.override_reason,
                )
                overrides.append(
                    {
                        "id": job_id,
                        "max_retries": row[1],
                        "retry_count": row[2],
                        "override_reason": req.override_reason,
                    }
                )
            if req.status == "queued":
                # full requeue semantics (reset claim + retry/backoff)
                sets = {
                    "status": "queued",
                    "claimed_by": None,
                    "claimed_at": None,
                    "retry_count": 0,
                    "retry_backoff_until": None,
                }
            else:
                sets = {"status": req.status, "claimed_by": None, "claimed_at": None}
            changed = await hive_repo().patch_job_status(tx, job_id, sets=sets, expected_status=row[0])
            if changed != 1:
                results["skipped_conflict"].append(job_id)
                continue
            if req.status == "needs-review":
                await dispatch_needs_review(tx, hive_repo(), job_id, now=time.time(), emit_event=emit_event)
            results["ok"].append(job_id)
        await tx.commit()
    async with hive_repo().connection() as event_tx:
        for job_id in results["ok"]:
            await emit_event(
                event_tx,
                "job.reclassified",
                {"id": job_id, "new_status": req.status, "reason": req.reason},
            )
        await event_tx.commit()
    await _emit_overrides([item for item in overrides if item["id"] in results["ok"]], actor, req.reason)
    return results
