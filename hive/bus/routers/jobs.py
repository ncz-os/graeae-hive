"""Job endpoints (Phase 1c router extraction).

Started with the read routes (GET /v1/jobs, GET /v1/jobs/{job_id}); the write
routes (submit/next/patch/claim/release/auto-retry) migrate here next.
"""

from __future__ import annotations

import json
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response

import queue_logic
import retry_policy

try:
    import aiosqlite
except ModuleNotFoundError:
    from hive.persistence import _sqlite_async as aiosqlite

from hive import scheduler as sched
from hive.bus.deps import (
    ACTIVE_AGENT_STATUSES,
    CLAIM_SCAN_WINDOW,
    HOST_DENYLIST,
    TERMINAL_JOB_STATUSES,
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
    MAX_DECLINE_REQUEUES,
    NO_REPO_MAPPING_DECLINE_BUDGET,
    STATUS_TRANSITIONS,
    SUBSCRIPTION_PROVIDERS,
    _cache_json,
    _safe_nonneg_int,
    _safe_str,
    cache_key_for,
    estimate_cost,
    field_was_set,
    hallucination_check,
    is_dedup_violation,
    is_unresolvable_release,
    json_list,
    kind_requires_commit,
    normalize_result_payload,
    stable_worker_id,
    valid_completion_proof,
)
from hive.bus.domain.workspace import resolve_workspace_shadow
from hive.bus.schemas import JobCreate, JobUpdate
from hive.core.models import COST_TIERS
from hive.persistence.sqlite import SqlExpr

router = APIRouter()

_JOB_COLUMNS = (
    "id, submitter_urn, parent_job_id, kind, description, priority, status, "
    "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
    "required_capabilities, eligible_kinds, eligible_hosts, site, repo, head_branch, pr_number, max_retries, retry_count, "
    "claimed_at, claim_lease_expires_at, last_update_at, claimed_model, claimed_provider, "
    "claimed_runtime, claimed_cost_tier"
)


def _result_model_author(result: Any) -> str | None:
    """The author model a worker recorded on a completion, or None.

    `result['models']` is worker-supplied and therefore untrusted: it may be
    absent, a string, a list, or nested one level deeper than expected. Every
    level is type-checked, so a malformed payload yields None instead of an
    AttributeError. `normalize_result_payload` preserves the key (it only adds
    commits/files_changed), so the author survives the done-path rewrite.
    """
    if not isinstance(result, dict):
        return None
    models = result.get("models")
    if not isinstance(models, dict):
        return None
    author = models.get("author")
    if not isinstance(author, str):
        return None
    author = author.strip()
    return author or None


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
        "claimed_at": r[21],
        "claim_lease_expires_at": r[22],
        "last_update_at": r[23],
        "claimed_model": r[24],
        "claimed_provider": r[25],
        "claimed_runtime": r[26],
        "claimed_cost_tier": r[27],
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
                # 409 (not 404): matches what hive_worker.py's claim loop already
                # handles by re-registering, so an unregistered worker self-heals.
                raise HTTPException(409, f"claim failed: agent not registered: {agent_urn}")
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


@router.patch("/v1/jobs/{job_id}")
async def update_job(job_id: str, req: JobUpdate):
    now = time.time()
    if req.status not in VALID_JOB_STATUSES:
        raise HTTPException(
            422,
            f"unsupported job status {req.status!r}; valid statuses: {sorted(VALID_JOB_STATUSES)}",
        )
    original_claimed_by_was_set = field_was_set(req, "claimed_by")
    original_result_mnemos_was_set = field_was_set(req, "result_mnemos_id")
    # #3 FIX: Hallucination guard now surfaces failure_reason + exit_code=-2.
    halluc_reason = None
    completion_rejected = False
    if req.status == "done" and req.result:
        halluc_reason = hallucination_check(req.result)
        if halluc_reason:
            patched = dict(req.result or {})
            patched["exit_code"] = -2
            patched["failure_reason"] = f"hallucination_guard:{halluc_reason}"
            patched["_hallucination_guard"] = halluc_reason
            req = JobUpdate(
                status="failed",
                result=patched,
                claimed_by=req.claimed_by,
                tokens_in=req.tokens_in,
                tokens_out=req.tokens_out,
                result_mnemos_id=req.result_mnemos_id,
                claimed_model=req.claimed_model,
                claimed_provider=req.claimed_provider,
                claimed_runtime=req.claimed_runtime,
                claimed_cost_tier=req.claimed_cost_tier,
            )
    req.result = normalize_result_payload(req.status, req.result)

    claimed_by_was_set = original_claimed_by_was_set
    result_mnemos_was_set = original_result_mnemos_was_set
    patch_payload = req.model_dump(exclude_unset=True) if hasattr(req, "model_dump") else req.dict(exclude_unset=True)
    cost_estimate = None

    # Skip releases are not failures. DECLINE_REASON_PREFIXES is intentionally
    # narrow unless a future real failure release reason needs dead-letter counting.
    # ``no_repo_mapping`` is deployment-dependent (worker REPO_MAP vs bus catalog),
    # so it only counts when the job's repo is genuinely absent from the catalog
    # (audit BLOCKING-1) -- see is_unresolvable_release.
    decline_reason = None
    dead_lettered = False
    if req.status == "queued" and isinstance(req.result, dict):
        _werr = str(req.result.get("worker_error") or "")
        if _werr and is_unresolvable_release(_werr):
            decline_reason = _werr

    # Storage via the HiveMindRepository ABC on ONE transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        current = await hive_repo().job_update_context(tx, job_id)
        if not current:
            raise HTTPException(404, f"job not found: {job_id}")
        (
            old_status,
            old_claimed_by,
            prov,
            mod,
            retry_count,
            max_retries,
            decline_count,
            job_kind,
            job_repo,
            existing_result,
        ) = current

        # Preserve reviewer-owned keys across a worker's result write. The hive
        # auto-reviewer appends hive_reviews to result, but a worker's later
        # result PATCH REPLACED the whole result column and silently dropped the
        # review (34 jobs lost their review this way; found 2026-10-16). Carry
        # the reviewer keys over when the incoming result does not supply them,
        # so a review is never clobbered.
        if isinstance(req.result, dict):
            _prev = existing_result
            if isinstance(_prev, str):
                try:
                    _prev = json.loads(_prev)
                except Exception:
                    _prev = None
            if isinstance(_prev, dict):
                for _rk in ("hive_reviews", "deepseek_reviews"):
                    if _prev.get(_rk) and not req.result.get(_rk):
                        req.result[_rk] = _prev[_rk]

        if req.status not in STATUS_TRANSITIONS.get(old_status, set()):
            raise HTTPException(409, f"invalid status transition {old_status!r} -> {req.status!r}")

        if (
            old_status in TERMINAL_JOB_STATUSES
            and req.status != old_status
            and not (old_status in {"done", "failed"} and req.status == "cancelled")
        ):  # retire fake/dup completions and superseded failures
            raise HTTPException(409, f"terminal job status {old_status!r} cannot be reopened by PATCH")

        if old_status in {"queued", "offered"} and req.status in {"claimed", "running"}:
            raise HTTPException(
                409,
                "claim bypass rejected: use POST /v1/jobs/next or POST /v1/jobs/{job_id}/claim "
                "so agent registration, capabilities, dependencies, and cost tier are checked",
            )

        release_to_queue = req.status == "queued" and old_status in {"claimed", "running", "offered"}
        if req.status == "queued" and old_status in {"claimed", "running", "offered"}:
            if not old_claimed_by:
                raise HTTPException(409, "cannot release a job with no current claimant")
            if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                raise HTTPException(403, "release to queued requires claimed_by to match the current claimant")

        if claimed_by_was_set and req.claimed_by is None:
            raise HTTPException(422, "claimed_by=null is not accepted; provide the current claimant urn")

        if claimed_by_was_set and req.claimed_by is not None:
            if old_claimed_by and req.claimed_by != old_claimed_by:
                raise HTTPException(
                    403,
                    f"claimed_by overwrite rejected: current={old_claimed_by!r} requested={req.claimed_by!r}",
                )

        if old_claimed_by and req.status in {"claimed", "running", "done", "failed", "cancelled"}:
            if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                raise HTTPException(403, "job update requires claimed_by to match the current claimant")

        effective_claimed_by = None if release_to_queue else (req.claimed_by or old_claimed_by)
        if req.status in {"claimed", "running"} and not effective_claimed_by:
            raise HTTPException(422, f"status={req.status!r} requires an existing or explicit claimed_by")

        commit_required = kind_requires_commit(job_kind, job_repo)
        if req.status == "done" and commit_required:
            proof = req.result or {}
            if not valid_completion_proof(proof):
                req.status = "needs-review"
                completion_rejected = True
                req.result = {**proof, "failure_reason": "completion_evidence_missing"}
                patch_payload["status"] = req.status
                patch_payload["result"] = req.result

        # Build the SET map for the CAS UPDATE (column names are code-side).
        sets: dict = {"status": req.status, "last_update_at": now}
        if req.status in TERMINAL_JOB_STATUSES:
            sets["ended_at"] = now
        if req.result is not None:
            sets["result"] = json.dumps(req.result)
        if release_to_queue:
            sets.update(
                {
                    "claimed_by": None,
                    "claimed_at": None,
                    "claimed_runtime": None,
                    "claimed_model": None,
                    "claimed_provider": None,
                    "claimed_cost_tier": None,
                    "ended_at": None,
                }
            )
            # GRAEAE dispatch consensus 1.0 SHIP-FIRST: exponential backoff on EVERY
            # release (±20% jitter) so a job no local worker can run cannot hot-loop.
            import random as _rnd

            _rc = int(retry_count or 0) + 1
            _bo = min(5.0 * (2 ** min(_rc, 6)), 300.0) * (1.0 + _rnd.uniform(-0.2, 0.2))
            sets["retry_count"] = SqlExpr("retry_count+1")
            sets["retry_backoff_until"] = now + _bo
        if req.status in {"claimed", "running"}:
            sets["claim_lease_expires_at"] = now + CLAIM_LEASE_SECONDS
        if req.status in TERMINAL_JOB_STATUSES or req.status == "queued":
            sets["claim_lease_expires_at"] = None
        if result_mnemos_was_set:
            sets["result_mnemos_id"] = req.result_mnemos_id

        # ---- model provenance (2026-10-16) --------------------------------
        # The worker reports the model it ACTUALLY ran (attempt start + lease
        # renewal, plus the terminal write). Rules:
        #   * only on a claimed/running or terminal done/failed write, and never
        #     on a release-to-queued (that path clears the provenance columns);
        #   * only for the CURRENT claimant -- there must BE a current claimant
        #     and the caller must present that exact urn. An unclaimed job has
        #     no claimant, so nobody may stamp provenance onto it;
        #   * never with None/blank: an absent value leaves the column untouched.
        if not release_to_queue and req.status in {"claimed", "running", "done", "failed"}:
            prov_sets = {
                "claimed_model": req.claimed_model,
                "claimed_provider": req.claimed_provider,
                "claimed_runtime": req.claimed_runtime,
                "claimed_cost_tier": req.claimed_cost_tier,
            }
            prov_sets = {col: val.strip() for col, val in prov_sets.items() if isinstance(val, str) and val.strip()}
            if req.status == "done":
                author = _result_model_author(req.result)
                if author and "claimed_model" not in prov_sets:
                    prov_sets["claimed_model"] = author
            if prov_sets:
                # Positive, strict claimant match (no truthiness shortcuts).
                is_current_claimant = isinstance(old_claimed_by, str) and old_claimed_by != "" and req.claimed_by == old_claimed_by
                if not is_current_claimant:
                    raise HTTPException(
                        403,
                        "provenance update requires claimed_by to match the current claimant",
                    )
                sets.update(prov_sets)

        if req.tokens_in is not None or req.tokens_out is not None:
            t_in = int(req.tokens_in or 0)
            t_out = int(req.tokens_out or 0)
            cost_estimate = estimate_cost(prov or "unknown", mod or "unknown", t_in, t_out)
            sets["tokens_in"] = t_in
            sets["tokens_out"] = t_out
            sets["estimated_cost_usd"] = cost_estimate

        # Routing re-assignment: only allowed for non-terminal, non-claimed jobs.
        if field_was_set(req, "eligible_kinds") and req.eligible_kinds is not None:
            if old_status not in {"queued", "offered"}:
                raise HTTPException(
                    409, f"eligible_kinds can only be updated on queued/offered jobs, not {old_status!r}"
                )
            sets["eligible_kinds"] = json.dumps(req.eligible_kinds)
        if field_was_set(req, "eligible_hosts") and req.eligible_hosts is not None:
            if old_status not in {"queued", "offered"}:
                raise HTTPException(
                    409, f"eligible_hosts can only be updated on queued/offered jobs, not {old_status!r}"
                )
            sets["eligible_hosts"] = json.dumps(req.eligible_hosts)

        # Thrash fix: compare-and-set on the observed status.
        if await hive_repo().patch_job_status(tx, job_id, sets=sets, expected_status=old_status) == 0:
            raise HTTPException(
                409, f"job {job_id} status changed concurrently (expected {old_status!r}); retry"
            )

        # Optional real-failure release counting.
        # (audit BLOCKING-1) ``no_repo_mapping`` is deployment-dependent, so it
        # counts only when this job's repo is genuinely absent from the bus
        # catalog; a catalog-present repo means a STALE WORKER, not a dead job.
        decline_threshold = MAX_DECLINE_REQUEUES
        if decline_reason is not None:
            # Classification (not a threshold decision): should this reason be
            # counted at all? no_repo_mapping on a CATALOG-PRESENT repo is a
            # stale worker, so it is dropped here and the job simply requeues.
            if not is_unresolvable_release(decline_reason, job_repo=job_repo):
                decline_reason = None
            elif decline_reason == "no_repo_mapping" or decline_reason.startswith("no_repo_mapping:"):
                # Genuinely absent repo: count, but on a larger budget.
                decline_threshold = NO_REPO_MAPPING_DECLINE_BUDGET
        if decline_reason is not None and release_to_queue:
            new_decline_count = (decline_count or 0) + 1
            if new_decline_count >= decline_threshold:
                dl_result = {
                    "dead_letter": True,
                    "decline_count": new_decline_count,
                    "last_decline_reason": decline_reason,
                    "note": "thrash_guard:max_decline_requeues_exceeded",
                    "last_decline_result": req.result,
                }
                rc_dl = await hive_repo().patch_job_status(
                    tx,
                    job_id,
                    sets={
                        "status": "dead-letter",
                        "decline_count": new_decline_count,
                        "ended_at": now,
                        "claim_lease_expires_at": None,
                        "result": json.dumps(dl_result),
                    },
                    expected_status="queued",
                )
                dead_lettered = rc_dl >= 1
            else:
                await hive_repo().patch_job_status(
                    tx, job_id, sets={"decline_count": new_decline_count}, expected_status="queued"
                )

        # Roll MTD spend onto claimer (subscription throttle requires this).
        if cost_estimate and cost_estimate > 0 and effective_claimed_by:
            await hive_repo().agent_spend_add(tx, urn=effective_claimed_by, delta=cost_estimate)

        # On failed: auto-retry if under max_retries (exponential backoff).
        retried = False
        if req.status == "failed" and retry_count < max_retries:
            backoff = 30.0 * (2**retry_count)
            next_at = time.time() + backoff
            await hive_repo().patch_job_status(
                tx,
                job_id,
                sets={
                    "status": "queued",
                    "retry_count": SqlExpr("retry_count+1"),
                    "retry_backoff_until": next_at,
                    "claimed_by": None,
                    "claimed_at": None,
                    "claimed_runtime": None,
                    "claimed_model": None,
                    "claimed_provider": None,
                    "claimed_cost_tier": None,
                    "ended_at": None,
                    "result": None,
                    "tokens_in": None,
                    "tokens_out": None,
                    "estimated_cost_usd": None,
                    "claim_lease_expires_at": None,
                },
            )
            retried = True

        # On done/failed/cancelled: roll per-worker per-kind stats (capability scoring).
        if req.status in TERMINAL_JOB_STATUSES and old_status not in TERMINAL_JOB_STATUSES:
            jrow = await hive_repo().job_stats_context(tx, job_id)
            if jrow:
                (
                    kind_j,
                    desc_j,
                    mtier,
                    reqcaps_json,
                    mdl_j,
                    prov_j,
                    claimed_by_j,
                    result_j,
                    started_j,
                ) = jrow
                try:
                    _rd = json.loads(result_j) if result_j else (req.result or {})
                except Exception:
                    _rd = req.result or {}
                if not isinstance(_rd, dict):
                    _rd = {}
                rt_in = _safe_nonneg_int(req.tokens_in if req.tokens_in is not None else _rd.get("tokens_in"))
                rt_out = _safe_nonneg_int(req.tokens_out if req.tokens_out is not None else _rd.get("tokens_out"))
                rt_reason = _safe_nonneg_int(_rd.get("tokens_reasoning"))
                if rt_in == 0 and rt_out == 0 and _rd.get("tokens_total"):
                    rt_out = _safe_nonneg_int(_rd.get("tokens_total"))
                eff_model = _safe_str(_rd.get("gateway_model") or mdl_j)
                eff_prov = _safe_str(_rd.get("gateway_provider") or prov_j)
                try:
                    job_cost = estimate_cost(eff_prov, eff_model, rt_in, rt_out) or 0
                    job_cost = max(job_cost, 0)
                except Exception as _ce:  # noqa: BLE001
                    print(f"cost estimate skipped for {job_id}: {_ce}", flush=True)
                    job_cost = 0
                if cost_estimate is None and job_cost:
                    cost_estimate = job_cost
                if claimed_by_j and kind_j:
                    worker_key = stable_worker_id(claimed_by_j)
                    duration = (time.time() - (started_j or time.time())) if started_j else 0
                    col = {"done": "success_count", "failed": "fail_count", "cancelled": "cancelled_count"}[req.status]
                    await hive_repo().upsert_worker_kind_stats(
                        tx,
                        urn=worker_key,
                        kind=kind_j,
                        column=col,
                        tokens_in=rt_in,
                        tokens_out=rt_out,
                        cost_usd=job_cost or 0,
                        duration_sec=duration,
                        last_run=time.time(),
                    )
                    # Per-job cost ledger row. Best-effort.
                    try:
                        await hive_repo().usage_ledger_insert(
                            tx,
                            provider=eff_prov,
                            model=eff_model,
                            task_kind=kind_j,
                            tokens_in=rt_in,
                            tokens_out=rt_out,
                            tokens_reasoning=rt_reason,
                            est_cost_usd=job_cost or 0,
                            latency_ms=max(0, int(duration * 1000)),
                            outcome="ok" if req.status == "done" else "err",
                            caller_subsystem="hive",
                            tier=(mtier or "A").upper(),
                            session_id=claimed_by_j,
                            request_count=1,
                            subscription_amortized=1 if (eff_prov or "").lower() in SUBSCRIPTION_PROVIDERS else 0,
                            path_kind=str(_rd.get("via") or "worker"),
                        )
                    except Exception as _le:
                        print(f"usage_ledger write skipped for {job_id}: {_le}", flush=True)
                if req.status == "done":
                    ck = cache_key_for(kind_j, desc_j, (mtier or "A").upper(), json_list(reqcaps_json))
                    rdict = json.loads(result_j) if result_j else (req.result or {})
                    ec = (rdict or {}).get("exit_code")
                    _nr = bool((rdict or {}).get("needs_review")) or (rdict or {}).get("status") == "needs-review"
                    if (ec == 0 or ec is None) and not _nr:
                        await hive_repo().cache_store(
                            tx,
                            cache_key=ck,
                            source_job_id=job_id,
                            result_json=_cache_json(rdict),
                            result_mnemos_id=req.result_mnemos_id,
                            model=mdl_j or "unknown",
                            provider=prov_j or "unknown",
                            cached_at=time.time(),
                        )

        await hive_repo().insert_job_audit(
            tx,
            job_id=job_id,
            ts=now,
            actor_urn=req.claimed_by or old_claimed_by,
            old_status=old_status,
            new_status="dead-letter" if dead_lettered else ("queued" if retried else req.status),
            old_claimed_by=old_claimed_by,
            new_claimed_by=None if release_to_queue else effective_claimed_by,
            patch_json=json.dumps(patch_payload, default=str),
        )
        await tx.commit()

    async with hive_repo().connection() as event_tx:
        if req.status == "failed" and retry_count < max_retries:
            await emit_event(
                event_tx,
                "job.retry",
                {"id": job_id, "retry_count": retry_count + 1, "max_retries": max_retries},
            )
        if dead_lettered:
            await emit_event(
                event_tx,
                "job.dead-letter",
                {
                    "id": job_id,
                    "claimed_by": req.claimed_by or old_claimed_by,
                    "last_decline_reason": decline_reason,
                    "decline_threshold": decline_threshold,
                },
            )
        else:
            await emit_event(
                event_tx,
                f"job.{req.status}",
                {
                    "id": job_id,
                    "claimed_by": req.claimed_by or old_claimed_by,
                    "tokens_in": req.tokens_in,
                    "tokens_out": req.tokens_out,
                    "estimated_cost_usd": cost_estimate,
                },
            )
        await event_tx.commit()
    if completion_rejected:
        return JSONResponse(
            status_code=409,
            content={"ok": False, "status": "needs-review", "reason": "completion_evidence_missing"},
        )
    return {"ok": True, "ts": now, "estimated_cost_usd": cost_estimate, "dead_lettered": dead_lettered}


@router.post("/v1/jobs/{job_id}/auto-retry")
async def auto_retry_job(job_id: str):
    """Recheck stored retry policy under a row lock before consuming a retry."""
    async with hive_repo().connection() as tx:
        row = await hive_repo().job_retry_context(tx, job_id)
        if row is None:
            raise HTTPException(404, "job not found")
        status, kind, max_retries, retry_count, raw_result = row
        try:
            result = json.loads(raw_result) if raw_result else None
        except (ValueError, TypeError):
            result = None
        job = {
            "status": status,
            "kind": kind,
            "max_retries": max_retries,
            "retry_count": retry_count,
            "result": result,
        }
        if not retry_policy.auto_retry_allowed(job):
            raise HTTPException(409, "job is not eligible for automatic retry")
        await hive_repo().patch_job_status(
            tx,
            job_id,
            sets={
                "status": "queued",
                "retry_count": SqlExpr("retry_count+1"),
                "claimed_by": None,
                "claimed_at": None,
                "claimed_runtime": None,
                "claimed_model": None,
                "claimed_provider": None,
                "claimed_cost_tier": None,
                "claim_lease_expires_at": None,
                "ended_at": None,
                "retry_backoff_until": None,
            },
        )
        await tx.commit()
    return {"ok": True, "id": job_id, "retry_count": retry_count + 1}


@router.post("/v1/jobs/{job_id}/claim")
async def claim_job(job_id: str, by: str):
    """Targeted single-job claim with the same eligibility checks as /v1/jobs/next."""
    # HOST_DENYLIST was enforced on /v1/jobs/next but NOT here historically, so a
    # denylisted host could take work by skipping the dequeue path entirely.
    _hp = by.split(":")
    _host = (_hp[3] if len(_hp) > 3 else "").lower()
    if _host in HOST_DENYLIST:
        raise HTTPException(403, f"host {_host} is in HIVE_HOST_DENYLIST")
    now = time.time()
    async with hive_repo().connection() as tx:
        agent_row = await hive_repo().agent_claim_context(tx, by)
        if not agent_row:
            raise HTTPException(409, f"claim failed: agent not registered: {by}")
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
            _agent_load,
        ) = agent_row
        if agent_status not in ACTIVE_AGENT_STATUSES:
            raise HTTPException(
                409, f"claim failed: agent status is {agent_status!r}, not online/idle: {by}"
            )
        agent_caps = set(json.loads(caps_json)) if caps_json else set()
        eligible_aliases = agent_kind_aliases(agent_kind, a_runtime)
        a_tier = (a_tier or "C").upper()
        if a_tier not in COST_TIERS:
            a_tier = "C"
        a_auth = (a_auth or "unknown").lower()
        sub_throttled = a_auth == "subscription" and a_cap and a_used and a_used >= THROTTLE_HEADROOM * a_cap

        job_row = await hive_repo().job_claim_context(tx, job_id)
        if not job_row:
            raise HTTPException(404, f"job not found: {job_id}")
        (
            j_status,
            j_kind,
            j_caps_json,
            j_kinds_json,
            j_hosts_json,
            j_max_tier,
            j_pref_providers,
            j_pref_models,
            j_deps_json,
            j_retry_backoff_until,
            j_started_at,
        ) = job_row

        urn_parts = by.split(":")
        agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()

        if j_status not in ("queued", "offered"):
            raise HTTPException(409, f"job already in status={j_status!r}; cannot claim")
        if j_retry_backoff_until is not None and j_retry_backoff_until > now:
            raise HTTPException(409, f"job is in retry backoff until {j_retry_backoff_until} (now={now})")

        if j_deps_json:
            deps = json_list(j_deps_json)
            if deps:
                done_ids = await hive_repo().completed_job_ids(tx, ids=list(deps))
                if len(done_ids) < len(deps):
                    raise HTTPException(
                        409, f"job has unsatisfied DAG dependencies ({len(done_ids)}/{len(deps)} done)"
                    )

        agent = {
            "urn": by,
            "host": agent_host,
            "kind": agent_kind,
            "runtime": a_runtime,
            "model": a_model,
            "provider": a_provider,
            "cost_tier": a_tier,
            "capabilities": agent_caps,
            "eligible_aliases": eligible_aliases,
            "subscription_throttled": sub_throttled,
        }
        job = {
            "id": job_id,
            "kind": j_kind,
            "required_capabilities": json_list(j_caps_json),
            "eligible_kinds": json_list(j_kinds_json),
            "eligible_hosts": json_list(j_hosts_json),
            "max_cost_tier": j_max_tier,
            "preferred_providers": json_list(j_pref_providers),
            "preferred_models": json_list(j_pref_models),
            "queued_at": j_started_at,
        }
        eligible, reason = job_agent_eligible(job, agent)
        if not eligible:
            status = (
                422 if reason.startswith("job max_cost_tier") else (429 if reason.startswith("subscription agent throttled") else 403)
            )
            raise HTTPException(status, reason)

        rc = await hive_repo().claim_job(
            tx,
            job_id=job_id,
            agent_urn=by,
            now=now,
            lease_expires_at=now + CLAIM_LEASE_SECONDS,
            runtime=a_runtime,
            model=a_model,
            provider=a_provider,
            cost_tier=a_tier,
        )
        await tx.commit()
        if rc == 0:
            raise HTTPException(409, "job already claimed or not available (race)")
        await emit_event(
            tx,
            "job.claimed",
            {
                "id": job_id,
                "claimed_by": by,
                "claimed_runtime": a_runtime,
                "claimed_model": a_model,
                "claimed_provider": a_provider,
                "claimed_cost_tier": a_tier,
                "manual_claim": True,
            },
        )
    return {
        "claimed": True,
        "ts": now,
        "claimed_runtime": a_runtime,
        "claimed_model": a_model,
        "claimed_provider": a_provider,
        "claimed_cost_tier": a_tier,
    }


@router.post("/v1/jobs/{job_id}/release")
async def zcgate_release(job_id: str):
    """Human-gated 'Release & Push': enqueue a release:<repo> job that a worker
    claims and runs `zc-gate release` on (push branch + open upstream PR)."""
    async with hive_repo().connection() as tx:
        r = await hive_repo().job_release_context(tx, job_id)
        if not r:
            raise HTTPException(404, f"job not found: {job_id}")
    result = json.loads(r[0]) if r[0] else {}
    preview = result.get("preview")
    if not preview:
        raise HTTPException(404, f"no preview bundle on job {job_id}")
    if (preview.get("verdict") or "").lower() == "blocked":
        raise HTTPException(409, "bundle verdict is blocked; not releasable")
    if result.get("release_job_id"):
        return {"ok": True, "already": True, "release_job_id": result["release_job_id"]}
    async with hive_repo().connection() as tx:
        submitter = await hive_repo().active_orchestrator_urn(tx)
    if not submitter:
        raise HTTPException(503, "no active orchestrator to submit the release job")
    repo = preview.get("repo") or "zeroclaw-labs/zeroclaw"
    rel = await create_job(
        JobCreate(
            submitter_urn=submitter,
            kind="release:zeroclaw",
            description=job_id,
            eligible_kinds=["zeroclaw"],
            eligible_hosts=["zcgate-release-sink"],
            priority=5,
        )
    )
    rel_id = rel.get("id") if isinstance(rel, dict) else None
    result["release_job_id"] = rel_id
    result["release_requested_at"] = time.time()
    async with hive_repo().connection() as tx:
        await hive_repo().patch_job_status(tx, job_id, sets={"result": json.dumps(result)})
        await tx.commit()
    return {"ok": True, "release_job_id": rel_id, "repo": repo, "submitter": submitter}
