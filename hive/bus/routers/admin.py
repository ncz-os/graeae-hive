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
    results = {"ok": [], "not_found": [], "skipped_done": [], "skipped_retry_budget": []}
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
            await hive_repo().patch_job_status(
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
            )
            results["ok"].append(job_id)
        await tx.commit()
    await _emit_overrides(overrides, actor, req.reason)
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
            # DEPENDENCY RELEASE (incident 2026-10-27): the row is gone, so any
            # queued/offered dependent listing it would be blocked forever.
            # Released in the SAME transaction as the DELETE, below.
            await release_dependencies(db, job_id, DEP_PURGED_STATUS)
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
        wheres.append(f"status IN ({','.join('?' for _ in statuses)})")
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
        wheres.append(f"id IN ({','.join('?' for _ in req.ids)})")
        params.extend(req.ids)

    # Active-job guard: unless explicitly included, never match in-flight work.
    if not req.include_active:
        wheres.append(f"status NOT IN ({','.join('?' for _ in ACTIVE_JOB_STATUSES)})")
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
            # DEPENDENCY RELEASE (incident 2026-10-27): released in the SAME
            # transaction as the deletes, per purged id.
            for jid in deleted:
                await release_dependencies(db, jid, DEP_PURGED_STATUS)
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
    results: dict = {"ok": [], "not_found": [], "skipped_done": [], "skipped_retry_budget": []}
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
            await hive_repo().patch_job_status(tx, job_id, sets=sets)
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
    await _emit_overrides(overrides, actor, req.reason)
    return results
