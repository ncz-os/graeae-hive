"""Job endpoints (Phase 1c router extraction).

Started with the read routes (GET /v1/jobs, GET /v1/jobs/{job_id}); the write
routes (submit/next/patch/claim/release/auto-retry) migrate here next.
"""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException

import queue_logic
from hive.bus.deps import (
    VALID_JOB_STATUSES,
    clamp_limit,
    connect_db,
    emit_event,
    hive_repo,
    require_orchestrator_submitter,
    uuidv7,
)
from hive.bus.domain.eligibility import (
    _norm_str,
    _norm_str_set,
    agent_kind_aliases,
    heavy_repo_required_for_kind,
)
from hive.bus.domain.jobs import (
    CACHE_TTL_SECONDS,
    cache_key_for,
    is_dedup_violation,
)
from hive.bus.domain.workspace import resolve_workspace_shadow
from hive.bus.schemas import JobCreate
from hive.core.models import COST_TIERS

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
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        total, rows = await hive_repo().list_jobs(
            tx,
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


@router.post("/v1/jobs")
async def create_job(req: JobCreate):
    """Submit work to the triage queue. No agent assignment — workers self-claim via /v1/jobs/next.

    ROLE ENFORCEMENT (user directive 2026-05-23):
    Worker runtimes (opencode/codex/hermes/claw-family/ic-engine) are CLAIMERS, not submitters.
    Posting jobs requires submitter to be registered with an orchestrator runtime (claude-code/human/mnemos).
    Workers attempting to POST jobs get 403 — they should call /v1/jobs/next instead.
    """
    job_id = uuidv7()
    now = time.time()
    max_cost_tier = (req.max_cost_tier or "B").upper()
    if max_cost_tier not in COST_TIERS:
        raise HTTPException(422, f"max_cost_tier must be one of {COST_TIERS}, got {max_cost_tier!r}")
    # 2026-05-26: codex-only eligibility auto-rewrite, ORIGINALLY for all of
    # codex/review/hermes-cli — codex was a CLI zeroclaw shells out to, with
    # no standalone claimer. RESCINDED for codex 2026-09-14 (operator): a real
    # kind=codex claimer daemon now exists and self-registers, so
    # eligible_kinds=['codex'] is claimable again. review/hermes-cli still
    # have no claimer and stay rewritten to ['zeroclaw'].
    NON_CLAIMER_KINDS = {"review", "hermes-cli"}
    if req.eligible_kinds:
        ek = list(req.eligible_kinds)
        kept = [k for k in ek if _norm_str(k) not in NON_CLAIMER_KINDS]
        if not kept:
            req = req.model_copy(update={"eligible_kinds": ["zeroclaw"]})
        elif kept != ek:
            req = req.model_copy(update={"eligible_kinds": kept})
    if req.mnemos_refs:
        bad = [r for r in req.mnemos_refs if not (isinstance(r, str) and r.startswith("mem_"))]
        if bad:
            raise HTTPException(422, f"mnemos_refs must be mem_XXX ids — bad entries: {bad}")
    # DAG: validate depends_on targets exist + no self-cycle
    if req.depends_on:
        if job_id in req.depends_on:
            raise HTTPException(422, "depends_on cannot include self")
        async with hive_repo().connection() as tx:
            found = await hive_repo().jobs_exist(tx, ids=list(req.depends_on))
        missing = [d for d in req.depends_on if d not in found]
        if missing:
            raise HTTPException(422, f"depends_on references unknown job ids: {missing}")
    # AFFINITY INJECTION REMOVED (operator 2026-06-03): jobs are not pinned to
    # hosts/kinds/capabilities — all workers eligible for everything.
    # NARROW HEAVY-REPO CARVE-OUT (operator 2026-06-04, GRAEAE consult): the only
    # documented exception. Large-repo classes (zeroclaw) strain a lightweight
    # worker's in-job clone -> dead-letter; tag them 'heavy-repo' so only build
    # hosts that advertise it claim them.
    if heavy_repo_required_for_kind(req.kind):
        _caps = list(req.required_capabilities or [])
        if "heavy-repo" not in _norm_str_set(_caps):
            _caps.append("heavy-repo")
            req = req.model_copy(update={"required_capabilities": _caps})
    # ROLE ENFORCEMENT: check submitter runtime BEFORE the cache lookup so
    # worker-only runtimes get a 403 consistently for both cache hits and misses.
    await require_orchestrator_submitter(req.submitter_urn)

    if req.eligible_kinds and "*" not in req.eligible_kinds:
        async with hive_repo().connection() as tx:
            krs = await hive_repo().agent_kind_runtimes(tx)
        registered_aliases: set[str] = set()
        for _kind, _runtime in krs:
            registered_aliases.update(agent_kind_aliases(_kind, _runtime))
        requested = _norm_str_set(req.eligible_kinds)
        if requested and not requested.intersection(registered_aliases):
            raise HTTPException(
                422,
                f"eligible_kinds has no registered worker: requested={sorted(requested)!r}; "
                f"registered_agent_kinds={sorted(registered_aliases)!r}",
            )

    # GRAEAE permanent logjam-fix: SUBMIT-TIME WORKSPACE ADMISSION (SHADOW / log-only).
    try:
        _ws, _wsrc = resolve_workspace_shadow(req.kind, req.description, req.target_workspace, req.project)
        if _wsrc == "UNRESOLVABLE":
            print(
                f"ADMISSION_SHADOW would-422 no_resolvable_workspace kind={req.kind!r} "
                f"project={req.project!r} submitter={req.submitter_urn}",
                flush=True,
            )
        elif _wsrc == "prefix-fallback":
            print(
                f"ADMISSION_SHADOW deprecated_prefix_resolve kind={req.kind!r} -> ws={_ws} "
                f"(migrate submitter to target_workspace)",
                flush=True,
            )
        if _wsrc in ("UNRESOLVABLE", "prefix-fallback"):
            try:
                import json as _sj

                with open("/srv/agent-bus/admission_shadow.jsonl", "a") as _sf:
                    _sf.write(
                        _sj.dumps(
                            {
                                "ts": now,
                                "verdict": _wsrc,
                                "kind": req.kind,
                                "project": req.project,
                                "submitter": req.submitter_urn,
                                "resolved_ws": _ws,
                            }
                        )
                        + "\n"
                    )
            except Exception:
                pass
    except Exception:
        pass

    # RESULT-CACHE CHECK: identical (kind, description, max_cost_tier, required_caps) within TTL → return cached result, mark new job done immediately
    ck = cache_key_for(req.kind, req.description, max_cost_tier, req.required_capabilities)
    _common = dict(
        id=job_id,
        submitter_urn=req.submitter_urn,
        parent_job_id=req.parent_job_id,
        kind=req.kind,
        description=req.description,
        priority=req.priority,
        deadline=req.deadline,
        required_capabilities_json=json.dumps(req.required_capabilities) if req.required_capabilities else None,
        eligible_kinds_json=json.dumps(req.eligible_kinds) if req.eligible_kinds else None,
        eligible_hosts_json=json.dumps(req.eligible_hosts) if req.eligible_hosts else None,
        project=req.project,
        max_cost_tier=max_cost_tier,
        preferred_providers_json=json.dumps(req.preferred_providers) if req.preferred_providers else None,
        preferred_models_json=json.dumps(req.preferred_models) if req.preferred_models else None,
        mnemos_refs_json=json.dumps(req.mnemos_refs) if req.mnemos_refs else None,
        depends_on_json=json.dumps(req.depends_on) if req.depends_on else None,
        max_retries=int(req.max_retries),
        site=req.site,
        repo=req.repo,
        head_branch=req.head_branch,
        pr_number=(int(req.pr_number) if req.pr_number is not None else None),
        started_at=now,
    )
    async with hive_repo().connection() as tx:
        cached = await hive_repo().cache_lookup(tx, cache_key=ck, cutoff=now - CACHE_TTL_SECONDS)
        # Repository jobs must execute against their actual checkout.
        if req.repo:
            cached = None
        if cached:
            # short-circuit: store job as done with cached result + cost=0
            await hive_repo().submit_job(
                tx,
                **_common,
                status="done",
                ended_at=now,
                result_json=json.dumps(
                    {
                        **(cached["result"] or {}),
                        "cache_hit": True,
                        "source_job_id": cached["source_job_id"],
                    }
                ),
                claimed_provider=cached["provider"],
                claimed_model=cached["model"],
                claimed_cost_tier="A",
                estimated_cost_usd=0,
                result_mnemos_id=cached.get("result_mnemos_id"),
            )
            saved = 0.01  # conservative fallback if no token data
            await hive_repo().cache_record_hit(tx, cache_key=ck, cost_saved_usd=saved, ts=now)
            await emit_event(
                tx,
                "job.cached",
                {
                    "id": job_id,
                    "source_job_id": cached["source_job_id"],
                    "kind": req.kind,
                    "cost_saved_usd": saved,
                },
            )
            await tx.commit()
            return {
                "id": job_id,
                "created_at": now,
                "status": "done",
                "cache_hit": True,
                "source_job_id": cached["source_job_id"],
                "result": cached["result"],
                "result_mnemos_id": cached.get("result_mnemos_id"),
            }
        # role check moved to the top of create_job() so cache hits and cache
        # misses share the same enforcement.
        dedup_scope_version = json.dumps(
            {
                "tier": max_cost_tier,
                "caps": sorted(req.required_capabilities or []),
                "eligible_hosts": sorted(req.eligible_hosts or []),
                "eligible_kinds": sorted(req.eligible_kinds or []),
                "preferred_providers": req.preferred_providers or [],
                "preferred_models": req.preferred_models or [],
                "depends_on": sorted(req.depends_on or []),
                "project": req.project,
                "parent_job_id": req.parent_job_id,
                "mnemos_refs": sorted(req.mnemos_refs or []),
                "deadline": req.deadline,
            },
            sort_keys=True,
            default=str,
        )
        dedup_hash = queue_logic.dedup_key(
            tenant="_default",
            kind=req.kind,
            description=req.description,
            version=dedup_scope_version,
            idempotency_key=req.idempotency_key,
        )
        try:
            await hive_repo().submit_job(tx, **_common, dedup_hash=dedup_hash, status="queued")
        except Exception as _dexc:
            if not is_dedup_violation(_dexc):
                raise
            _row = await hive_repo().find_active_dedup_job(tx, dedup_hash=dedup_hash)
            if _row:
                return {"id": _row[0], "created_at": _row[1], "coalesced": True}
            raise
        await emit_event(
            tx,
            "job.queued",
            {
                "id": job_id,
                "submitter": req.submitter_urn,
                "kind": req.kind,
                "description": req.description,
                "priority": req.priority,
                "required_capabilities": req.required_capabilities,
                "eligible_kinds": req.eligible_kinds,
                "max_cost_tier": max_cost_tier,
                "mnemos_refs": req.mnemos_refs,
            },
        )
        await tx.commit()
    return {"id": job_id, "created_at": now, "mnemos_refs": req.mnemos_refs}
