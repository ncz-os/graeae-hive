"""Job endpoints (Phase 1c router extraction).

Started with the read routes (GET /v1/jobs, GET /v1/jobs/{job_id}); the write
routes (submit/next/patch/claim/release/auto-retry) migrate here next.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException

from hive.bus.deps import (
    VALID_JOB_STATUSES,
    clamp_limit,
    connect_db,
    hive_repo,
)

router = APIRouter()

_JOB_COLUMNS = (
    "id, submitter_urn, parent_job_id, kind, description, priority, status, "
    "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
    "required_capabilities, eligible_kinds, eligible_hosts, site, repo, head_branch, pr_number, max_retries, retry_count"
)


@router.get("/v1/jobs")
async def list_jobs(
    status: str | None = None,
    agent_urn: str | None = None,
    parent_job_id: str | None = None,
    since: float | None = None,
    limit: int = 100,
    include_result: bool = True,
):
    limit = clamp_limit(limit, default=100, max_limit=1000)
    if status:
        # Normalize separator variants (dead_letter -> dead-letter,
        # failed-completion -> failed_completion) and reject unknown values
        # with 400 instead of silently returning an empty list. A wrong
        # separator previously made 82 dead-letter rows API-invisible
        # (2026-06-06): the canonical spelling is hyphenated dead-letter
        # while callers naturally write dead_letter.
        normalized = status.strip().lower().replace("_", "-")
        status = {"failed-completion": "failed_completion"}.get(normalized, normalized)
        if status not in VALID_JOB_STATUSES:
            raise HTTPException(
                400,
                f"unknown status {status!r}; valid: {sorted(VALID_JOB_STATUSES)}",
            )
    # Storage via the HiveMindRepository ABC (no raw SQL in the router).
    total, rows = await hive_repo().list_jobs(
        status=status,
        agent_urn=agent_urn,
        parent_job_id=parent_job_id,
        since=since,
        limit=limit,
        include_result=include_result,
    )
    return {"count": len(rows), "total": total, "jobs": rows}


@router.get("/v1/jobs/{job_id}")
async def get_job(job_id: str):
    """Fetch a single job by id (previously 405 -> callers had to scan the list
    or query the DB directly to find one job, e.g. a just-submitted build)."""
    async with connect_db() as db:
        async with db.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"job not found: {job_id}")
    return {
        "id": r[0],
        "submitter_urn": r[1],
        "parent_job_id": r[2],
        "kind": r[3],
        "description": r[4],
        "priority": r[5],
        "status": r[6],
        "claimed_by": r[7],
        "started_at": r[8],
        "ended_at": r[9],
        "result": json.loads(r[10]) if r[10] else None,
        "estimated_cost_usd": r[11],
        "required_capabilities": json.loads(r[12]) if r[12] else None,
        "eligible_kinds": json.loads(r[13]) if r[13] else None,
        "eligible_hosts": json.loads(r[14]) if r[14] else None,
        "site": r[15],
        "repo": r[16],
        "head_branch": r[17],
        "pr_number": r[18],
        "max_retries": r[19],
        "retry_count": r[20],
    }


@router.get("/v1/jobs/metrics")
async def job_metrics():
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "total": 0}
    async with (
        connect_db() as db,
        db.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status") as cur,
    ):
        async for status, count in cur:
            counts["total"] += count
            if status in {"queued", "offered"}:
                counts["pending"] += count
            elif status in {"claimed", "running"}:
                counts["running"] += count
            elif status == "done":
                counts["completed"] += count
            elif status == "failed":
                counts["failed"] += count
    return counts


@router.get("/v1/jobs/{job_id}/preview")
async def zcgate_get_preview(job_id: str):
    """Return the zerocode-gate PreviewBundle stored on a needs-review job."""
    async with (
        connect_db() as db,
        db.execute(
            "SELECT result, status, repo, head_branch, pr_number FROM jobs WHERE id=?",
            (job_id,),
        ) as cur,
    ):
        r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"job not found: {job_id}")
    result = json.loads(r[0]) if r[0] else {}
    preview = result.get("preview")
    if not preview:
        raise HTTPException(404, f"no preview bundle on job {job_id}")
    return {
        "job_id": job_id,
        "status": r[1],
        "repo": r[2],
        "head_branch": r[3],
        "pr_number": r[4],
        "preview": preview,
    }
