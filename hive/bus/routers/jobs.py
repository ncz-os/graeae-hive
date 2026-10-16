"""Job endpoints (Phase 1c router extraction).

Started with the read routes (GET /v1/jobs, GET /v1/jobs/{job_id}); the write
routes (submit/next/patch/claim/release/auto-retry) migrate here next.
"""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

import queue_logic

try:
    import aiosqlite
except ModuleNotFoundError:
    from hive.persistence import _sqlite_async as aiosqlite

from hive import scheduler as sched
from hive.bus.deps import (
    ACTIVE_AGENT_STATUSES,
    CLAIM_SCAN_WINDOW,
    HOST_DENYLIST,
    VALID_JOB_STATUSES,
    clamp_limit,
    connect_db,
    emit_event,
    hive_repo,
    require_orchestrator_submitter,
    uuidv7,
)
from hive.bus.domain.capacity_ledger import _capacity_ledger
from hive.bus.domain.eligibility import (
    THROTTLE_HEADROOM,
    _norm_str,
    _norm_str_set,
    agent_kind_aliases,
    heavy_repo_required_for_kind,
    job_agent_eligible,
)
from hive.bus.domain.jobs import (
    CACHE_TTL_SECONDS,
    CLAIM_LEASE_SECONDS,
    cache_key_for,
    is_dedup_violation,
    json_list,
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


@router.post("/v1/jobs/next")
async def dequeue_next_job(agent_urn: str):
    """Atomic dequeue: highest-priority queued job that this agent is eligible for.

    Algorithm (eligibility / MiniMax pool / role-aware ordering) lives here in the
    router; every storage op goes through the HiveMindRepository ABC on ONE
    transaction (agent_claim_context -> pool_occupancy -> queued_candidates ->
    completed_job_ids -> claim_job -> emit_event).
    """
    now = time.time()
    urn_parts = agent_urn.split(":")
    agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()
    if agent_host in HOST_DENYLIST:
        return Response(
            status_code=204,
            headers={
                "X-Hive-Claim-Result": "host_denylisted",
                "X-Hive-Claim-Detail": f"host {agent_host} is in HIVE_HOST_DENYLIST",
            },
        )

    try:
        async with hive_repo().connection() as tx:
            agent_row = await hive_repo().agent_claim_context(tx, agent_urn)
            if not agent_row:
                raise HTTPException(404, f"claim failed: agent not registered: {agent_urn}")
            (
                agent_kind,
                caps_json,
                a_runtime,
                a_model,
                a_provider,
                a_tier,
                a_auth,
                a_cap,
                a_used,
                agent_status,
                agent_current_load,
            ) = agent_row
            if agent_status not in ACTIVE_AGENT_STATUSES:
                raise HTTPException(
                    409,
                    f"claim failed: agent status is {agent_status!r}, not online/idle: {agent_urn}",
                )
            agent_caps = set(json.loads(caps_json)) if caps_json else set()
            a_tier = (a_tier or "C").upper()
            if a_tier not in COST_TIERS:
                a_tier = "C"
            a_auth = (a_auth or "unknown").lower()
            agent = {
                "urn": agent_urn,
                "host": agent_host,
                "kind": agent_kind,
                "runtime": a_runtime,
                "model": a_model,
                "provider": a_provider,
                "cost_tier": a_tier,
                "capabilities": agent_caps,
                "eligible_aliases": agent_kind_aliases(agent_kind, a_runtime),
                "subscription_throttled": (
                    a_auth == "subscription" and a_cap and a_used and a_used >= THROTTLE_HEADROOM * a_cap
                ),
            }

            # MiniMax capacity pool: a MiniMax-routed worker may not start a
            # session when the pool is full (global cap OR per-worker cap).
            if sched.is_minimax_route(a_provider, a_model):
                pool_rows = await hive_repo().pool_occupancy(
                    tx, provider=sched.MINIMAX_PROVIDER, model=sched.MINIMAX_MODEL, now=now
                )
                pool = sched.build_pool_snapshot(pool_rows)
                _capacity_ledger.in_flight[sched.MINIMAX_PROVIDER] = pool.active_assignments
                ok, reason = _capacity_ledger.headroom(sched.MINIMAX_PROVIDER, now=now)
                if ok and pool.leases_by_worker.get(sched._norm_str(agent_urn)):
                    ok = False
                    reason = (
                        f"minimax:worker {agent_urn!r} already holds a lease "
                        f"(per-worker cap={sched.MINIMAX_PER_WORKER_MAX})"
                    )
                if not ok:
                    await emit_event(
                        tx,
                        "minimax.pool.denied",
                        {
                            "urn": agent_urn,
                            "pool_active": pool.active_assignments,
                            "pool_cap": sched.MINIMAX_GLOBAL_MAX,
                            "reason": reason,
                        },
                    )
                    return Response(
                        status_code=204,
                        headers={
                            "X-Hive-Claim-Result": "minimax_pool_saturated",
                            "X-Hive-Claim-Detail": f"minimax pool saturated: {reason}",
                        },
                    )

            candidates = await hive_repo().queued_candidates(
                tx, agent_urn=agent_urn, now=now, limit=CLAIM_SCAN_WINDOW
            )
            # Role-aware ordering (hive.scheduler is the single source of truth).
            worker_profile = sched.worker_profile_from_metadata(
                urn=agent_urn,
                host=agent_host,
                capabilities=agent_caps,
                current_load=(json.loads(agent_current_load) if agent_current_load else None),
            )
            _job_dicts = [
                {
                    "id": r[0],
                    "submitter_urn": r[1],
                    "kind": r[3],
                    "priority": r[5],
                    "required_capabilities": json_list(r[7]),
                }
                for r in candidates
            ]
            _ordered = sched.order_candidates_for_worker(candidates=_job_dicts, worker=worker_profile, now=now)
            _by_id = {r[0]: r for r in candidates}
            candidates = [_by_id[d["id"]] for d in _ordered]

            # Resolve ALL candidate dependencies in ONE ABC query (depends_on is
            # tuple index 15).
            _all_deps: set[str] = set()
            for _r in candidates:
                _all_deps.update(json_list(_r[15]))
            done_dep_ids = await hive_repo().completed_job_ids(tx, ids=list(_all_deps)) if _all_deps else set()

            # Canonical pool key for MiniMax-routed claims only.
            if sched.is_minimax_route(a_provider, a_model):
                _claim_provider, _claim_model = sched.MINIMAX_PROVIDER, sched.MINIMAX_MODEL
            else:
                _claim_provider, _claim_model = a_provider, a_model

            claimed = None
            for r in candidates:
                (
                    j_id,
                    j_submitter,
                    j_parent,
                    j_kind,
                    j_desc,
                    j_priority,
                    j_deadline,
                    j_caps_json,
                    j_kinds_json,
                    j_hosts_json,
                    j_project,
                    j_max_tier,
                    j_pref_providers,
                    j_pref_models,
                    j_mnemos_refs,
                    j_deps_json,
                    _j_retry_backoff_until,
                    j_started_at,
                    j_site,
                    j_repo,
                    j_head_branch,
                    j_pr_number,
                ) = r
                deps = json_list(j_deps_json)
                if deps and not set(deps).issubset(done_dep_ids):
                    continue
                job = {
                    "id": j_id,
                    "kind": j_kind,
                    "required_capabilities": json_list(j_caps_json),
                    "eligible_kinds": json_list(j_kinds_json),
                    "eligible_hosts": json_list(j_hosts_json),
                    "max_cost_tier": j_max_tier,
                    "preferred_providers": json_list(j_pref_providers),
                    "preferred_models": json_list(j_pref_models),
                    "queued_at": j_started_at,
                }
                eligible, _reason = job_agent_eligible(job, agent)
                if not eligible:
                    continue
                rc = await hive_repo().claim_job(
                    tx,
                    job_id=j_id,
                    agent_urn=agent_urn,
                    now=now,
                    lease_expires_at=now + CLAIM_LEASE_SECONDS,
                    runtime=a_runtime,
                    model=_claim_model,
                    provider=_claim_provider,
                    cost_tier=a_tier,
                )
                if rc == 0:
                    continue
                claimed = {
                    "id": j_id,
                    "submitter_urn": j_submitter,
                    "parent_job_id": j_parent,
                    "kind": j_kind,
                    "description": j_desc,
                    "priority": j_priority,
                    "deadline": j_deadline,
                    "required_capabilities": json_list(j_caps_json),
                    "eligible_kinds": json_list(j_kinds_json),
                    "eligible_hosts": json_list(j_hosts_json),
                    "project": j_project,
                    "max_cost_tier": j_max_tier,
                    "preferred_providers": json_list(j_pref_providers),
                    "preferred_models": json_list(j_pref_models),
                    "mnemos_refs": json_list(j_mnemos_refs),
                    "site": j_site,
                    "repo": j_repo,
                    "head_branch": j_head_branch,
                    "pr_number": j_pr_number,
                    "status": "claimed",
                    "claimed_by": agent_urn,
                    "claimed_at": now,
                    "claim_lease_expires_at": now + CLAIM_LEASE_SECONDS,
                    "claimed_resources": {
                        "runtime": a_runtime,
                        "model": a_model,
                        "provider": a_provider,
                        "cost_tier": a_tier,
                    },
                }
                break
            await tx.commit()
            if claimed:
                resources = claimed.get("claimed_resources") or {}
                await emit_event(
                    tx,
                    "job.claimed",
                    {
                        "id": claimed["id"],
                        "claimed_by": agent_urn,
                        "kind": claimed["kind"],
                        "runtime": resources.get("runtime"),
                        "model": resources.get("model"),
                        "provider": resources.get("provider"),
                        "cost_tier": resources.get("cost_tier"),
                    },
                )
                return claimed
    except HTTPException:
        raise
    except aiosqlite.Error as e:
        raise HTTPException(500, f"claim failed: database error while claiming next job: {e}") from e

    # HTTP 204 = No Content. By spec the response body must be EMPTY.
    return Response(
        status_code=204,
        headers={
            "X-Hive-Claim-Result": "no_jobs_available",
            "X-Hive-Claim-Detail": "no queued jobs matched agent eligibility, dependencies, backoff, or cost tier",
        },
    )
