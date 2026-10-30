"""Job endpoints (Phase 1c router extraction).

Started with the read routes (GET /v1/jobs, GET /v1/jobs/{job_id}); the write
routes (submit/next/patch/claim/release/auto-retry) migrate here next.
"""

from __future__ import annotations

import asyncio
import json
import logging
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

from hive import review_record
from hive import scheduler as sched
from hive.bus.deps import (
    ACTIVE_AGENT_STATUSES,
    CLAIM_SCAN_WINDOW,
    DEP_DEAD_STATUSES,
    HOST_DENYLIST,
    TERMINAL_JOB_STATUSES,
    VALID_JOB_STATUSES,
    clamp_limit,
    connect_db,
    emit_event,
    hive_repo,
    release_dependencies,
    require_orchestrator_submitter,
    uuidv7,
)
from hive.bus.domain import branch_admission as _ba_mod
from hive.bus.domain.branch_admission import branch_admission
from hive.bus.domain.capacity_ledger import _capacity_ledger
from hive.bus.domain.eligibility import (
    host_pin_warnings,
    THROTTLE_HEADROOM,
    _norm_str,
    _norm_str_set,
    agent_kind_aliases,
    heavy_repo_required_for_kind,
    job_agent_eligible,
)
from hive.bus.domain.failure import engine_unavailable, infrastructure_failure
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
    is_readonly_prefix_kind,
    is_unresolvable_release,
    json_list,
    kind_requires_commit,
    normalize_result_payload,
    stable_worker_id,
    valid_completion_proof,
    valid_readonly_completion_proof,
)
from hive.bus.domain.model_admission import admission_warnings, forbidden_model, served_model_names
from hive.bus.domain.review_dispatch import dispatch_needs_review, reconcile_review_parent
from hive.bus.domain.workspace import resolve_workspace_shadow
from hive.bus.schemas import JobCreate, JobUpdate
from hive.core.models import COST_TIERS
from hive.persistence.sqlite import DependencyNotReady, SqlExpr

router = APIRouter()
log = logging.getLogger("hive.bus.jobs")


async def _claim_health(tx, job_id: str, urn: str) -> bool:
    async with tx.execute("SELECT metadata FROM agents WHERE urn=?", (urn,)) as cur:
        agent = await cur.fetchone()
    if not agent or engine_unavailable(agent[0]):
        return False
    async with tx.execute("SELECT result FROM jobs WHERE id=?", (job_id,)) as cur:
        row = await cur.fetchone()
    try:
        result = json.loads(row[0]) if row and isinstance(row[0], str) else (row[0] if row else {})
    except (ValueError, TypeError):
        return False
    if not isinstance(result, dict):
        return True
    if result.get("infrastructure_parked"):
        return False
    previous = result.get("last_failed_claimant")
    if result.get("failure_class") and previous:
        # Re-registration on the same host must not defeat failover.
        return str(previous).split(":")[3:4] != urn.split(":")[3:4]
    return True


def _reconcile_reviewer_record(job_id: str, result: Any) -> None:
    """One reviewer record per job (2026-10-26).

    Warn when a submitted result names different reviewers in its reviewer
    fields, then derive them all from the structured verdict (see
    hive/review_record.py) and add the ``hive_reviews`` delivery entry. A result
    with neither a review_evidence record nor a ``reviewer`` record (an older
    worker) is only checked, never rewritten. Never raises."""
    if not isinstance(result, dict):
        return
    try:
        disagree = review_record.disagreements(result)
        if disagree:
            log.warning("job %s submitted disagreeing reviewer fields: %s", job_id, ", ".join(disagree))
        if isinstance(result.get("review_evidence"), dict) or isinstance(result.get("reviewer"), dict):
            review_record.apply(result, hive_reviews=True)
    except Exception:  # noqa: BLE001 -- a record problem must never fail a result write
        log.exception("job %s: reviewer record reconciliation failed", job_id)


_JOB_COLUMNS = (
    "id, submitter_urn, parent_job_id, kind, description, priority, status, "
    "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
    "required_capabilities, eligible_kinds, eligible_hosts, site, repo, head_branch, base_branch, "
    "branch_created_from, pr_number, max_retries, retry_count, "
    "claimed_at, claim_lease_expires_at, last_update_at, claimed_model, claimed_provider, "
    "claimed_runtime, claimed_cost_tier, "
    # Fault 2026-10-17: JobCreate accepted these and the row carried them, but
    # GET /v1/jobs/{id} never selected/returned them.
    "preferred_providers, preferred_models, depends_on, project, max_cost_tier, mnemos_refs"
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


#: Capability that marks an agent as an explicit cross-job actor (a human
#: operator console or a triage authority). It is the ONLY second key that can
#: cancel somebody else's claimed job, and it must be declared at registration
#: — it is never inferred from a runtime or a role table.
ORCHESTRATOR_CAPABILITY = "orchestrator"


#: The reason keys that explain WHY a job left a worker (2026-10-27): the
#: release fields written on every release-to-queued, and the dead-letter
#: note written when a job burns its decline budget. A job's result carries
#: whichever of these happened last to it.
RESULT_REASON_KEYS = ("last_release_reason", "last_release_at", "last_released_by", "last_decline_reason", "note")


def _result_reason_fields(result: Any) -> dict[str, Any]:
    """The release/dead-letter reason fields present on a result payload.

    GET /v1/jobs/{id} must always surface these when they are present (the
    dead-letter rows of 2026-10-27 were invisible from the job detail
    otherwise). Tolerant of str/None results: those carry no fields."""
    if not isinstance(result, dict):
        return {}
    return {k: result[k] for k in RESULT_REASON_KEYS if k in result}


async def _has_orchestrator_capability(actor_urn: str | None, tx=None) -> bool:
    """Whether ``actor_urn`` is REGISTERED and declares the orchestrator capability.

    Identity always comes from the server-side ``agents`` row, never from the
    request body: a caller cannot mint the right to cancel someone else's job
    by writing ``cancel_actor``/``submitter_urn`` into the PATCH. An unknown or
    unregistered urn is simply not orchestrator-capable.

    ``tx`` is the caller's open transaction. Reusing it matters: opening a
    second connection inside the PATCH transaction takes a second SQLite write
    lock (deadlock risk) and a second pooled Db2/Oracle connection per cancel.
    A connection is only opened when no transaction is supplied (direct calls).
    """
    urn = (actor_urn or "").strip()
    if not urn:
        return False
    if tx is not None:
        async with tx.execute("SELECT capabilities FROM agents WHERE urn=?", (urn,)) as cur:
            row = await cur.fetchone()
    else:
        async with (
            connect_db() as db,
            db.execute("SELECT capabilities FROM agents WHERE urn=?", (urn,)) as cur,
        ):
            row = await cur.fetchone()
    if not row:
        return False
    caps = row[0]
    if isinstance(caps, str):
        try:
            caps = json.loads(caps)
        except (TypeError, ValueError):
            caps = None
    if not isinstance(caps, list):
        return False
    return ORCHESTRATOR_CAPABILITY in {_norm_str(c) for c in caps if isinstance(c, str)}


def _reason_prefix_filter(rows: list[dict[str, Any]], reason_prefix: str) -> list[dict[str, Any]]:
    """Backends without a SQL reason predicate filter here, in Python.

    Case-insensitive prefix match on the job's result, across the deploy-line
    reason fields: ``last_decline_reason`` (dead-letter), ``last_release_reason``
    (release), ``failure_reason``, ``error`` and ``note``. ``result`` is JSON
    on sqlite but may surface as a string on some backends — both tolerated.
    """
    prefix = reason_prefix.lower()
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = _result_as_dict(row.get("result"))
        if not isinstance(payload, dict):
            continue
        for key in ("last_decline_reason", "last_release_reason", "failure_reason", "error", "note"):
            value = payload.get(key)
            if isinstance(value, str) and value.lower().startswith(prefix):
                out.append(row)
                break
    return out


@router.get("/v1/jobs")
async def list_jobs(
    status: str | None = None,
    agent_urn: str | None = None,
    parent_job_id: str | None = None,
    since: float | None = None,
    limit: int = 100,
    include_result: bool = True,
    reason_prefix: str | None = None,
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
    # reason_prefix: expressed IN SQL on backends that can do it (sqlite,
    # postgres, mysql — LIKE on the result JSON text); those also filter the
    # COUNT in SQL. Backends whose dialect cannot express it (oracle, db2)
    # filter in PYTHON over the fetched rows: the returned count is the
    # post-filter count over the fetched window and `total` stays the
    # pre-filter count (so `total > count` can hold there — the filter
    # response is still correct, just less precise on total).
    _sql_reason_filter = reason_prefix and reason_prefix.strip() != ""
    async with hive_repo().connection() as tx:
        total, rows = await hive_repo().list_jobs(
            tx,
            status=status,
            agent_urn=agent_urn,
            parent_job_id=parent_job_id,
            since=since,
            limit=limit,
            include_result=include_result,
            reason_prefix=reason_prefix if _sql_reason_filter else None,
        )
    if _sql_reason_filter and not type(hive_repo()).list_jobs_sql_reason:
        # Python-side filter for backends whose dialect cannot express it
        # (oracle, db2): rows are filtered, but `total` is preserved as the
        # PRE-FILTER count so callers can see `total > count` and know the
        # filter ran client-side over the fetched window.
        rows = _reason_prefix_filter(rows, reason_prefix)
    return {"count": len(rows), "total": total, "jobs": rows}


# ---- job summary (2026-10-27): queue state + dead-letter reasons, one call ---
#
# The operator pain this exists for: 70+ dead rows and a dead-letter with no
# visible reason were invisible from GET /v1/jobs. The reasons are now
# persisted in the job result (last_release_reason / last_decline_reason /
# failure_reason / error / note), and the summary surfaces them grouped.
#
# READ-ONLY BY CONSTRUCTION: it issues only SELECTs through the repository
# (no commit on the transaction) and the zero-writes test pins that.


def _result_as_dict(result: Any) -> dict[str, Any] | None:
    """The job's result payload as a dict, or None when it is not one.

    The repository already decodes result JSON on sqlite/postgres, but a
    backend may surface it as a string — parse it here so reason extraction
    works on both shapes."""
    if isinstance(result, dict):
        return result
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _reason_prefix_key(result: Any) -> str | None:
    """The job's normalised reason key for the summary's dead_by_reason.

    First present of the deploy-line reason fields, truncated to 60 chars.
    None means "no reason" — the caller buckets those under ``(none)``.
    Tolerates str/None/other results (they count as no reason)."""
    payload = _result_as_dict(result)
    if not isinstance(payload, dict):
        return None
    for key in ("last_decline_reason", "last_release_reason", "failure_reason", "error", "note"):
        value = payload.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value[:60]
    return None


@router.get("/v1/jobs/summary")
async def jobs_summary():
    """Queue state + dead-letter reasons in one read-only call.

    Shape::

        {
          "by_status": {<status>: n},
          "dead": {"failed", "dead-letter", "cancelled", "total",
                   "oldest_age_sec", "cap"},
          "dead_by_reason": {<60-char reason prefix>: n, "other": n?, "(none)": n?},
          "triage": {"queued", "done", "oldest_age_sec"},
          "needs_review": {"n", "oldest_age_sec"},
          "oldest_queued_sec"
        }

    * dead = the operator's dead set: failed + dead-letter + cancelled.
    * dead_by_reason keys are the FIRST 60 CHARS of the reason (normalised);
      the top 20 by count (ties: name) are listed and the rest roll into
      ``other``; dead rows with no reason count under ``(none)``.
    * triage = ``triage:*`` work orders: queued vs done/cancelled/failed/
      dead-letter (a processed work order), aged from ended/last_update.
    * ages are seconds at the caller's clock, null when no row carries a
      usable timestamp.
    """
    import time as _time

    from hive.workers import retention as _retention

    now = _time.time()
    async with hive_repo().connection() as tx:
        total, rows = await hive_repo().list_jobs(tx, limit=100000, include_result=True)
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row["status"]] = by_status.get(row["status"], 0) + 1

    dead_rows = [r for r in rows if r["status"] in _retention.DEAD_STATUSES]
    dead_ages = [_retention._age_at(r, now) for r in dead_rows]

    reasons: dict[str, int] = {}
    for row in dead_rows:
        key = _reason_prefix_key(row.get("result")) or "(none)"
        reasons[key] = reasons.get(key, 0) + 1
    ranked = sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))
    dead_by_reason = dict(ranked[:20])
    if len(ranked) > 20:
        dead_by_reason["other"] = sum(n for _k, n in ranked[20:])

    triage_queued = sum(1 for r in rows if r["status"] == "queued" and _retention.is_triage_job(r["kind"]))
    triage_done = sum(
        1 for r in rows if r["status"] in _retention.TRIAGE_PURGE_STATUSES and _retention.is_triage_job(r["kind"])
    )
    triage_ages = [
        _retention._age_at(r, now)
        for r in rows
        if r["status"] in ("queued", *(_retention.TRIAGE_PURGE_STATUSES)) and _retention.is_triage_job(r["kind"])
    ]

    nr_rows = [r for r in rows if r["status"] == "needs-review"]
    nr_ages = [_retention._age_at(r, now) for r in nr_rows]
    queued_ages = [_retention._age_at(r, now) for r in rows if r["status"] == "queued"]

    return {
        "total": total,
        "by_status": by_status,
        "dead": {
            "failed": by_status.get("failed", 0),
            "dead_letter": by_status.get("dead-letter", 0),
            "cancelled": by_status.get("cancelled", 0),
            "total": len(dead_rows),
            "oldest_age_sec": max((a for a in dead_ages if a is not None), default=None),
            "cap": _retention.DEAD_CAP,
        },
        "dead_by_reason": dead_by_reason,
        "triage": {
            "queued": triage_queued,
            "done": triage_done,
            "oldest_age_sec": max((a for a in triage_ages if a is not None), default=None),
        },
        "needs_review": {
            "n": len(nr_rows),
            "oldest_age_sec": max((a for a in nr_ages if a is not None), default=None),
        },
        "oldest_queued_sec": max((a for a in queued_ages if a is not None), default=None),
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
    _result = json.loads(r[10]) if r[10] else None
    # The release-reason fields (last_release_reason/at/by on a release,
    # last_decline_reason + note on a dead-letter) live INSIDE the result
    # JSON (the column-based attempt was reverted: production runs on
    # Oracle). GET must surface them at the top level whenever present, so
    # an operator reads WHY from the job itself.
    out = {
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
        "result": _result,
        "estimated_cost_usd": r[11],
        "required_capabilities": json.loads(r[12]) if r[12] else None,
        "eligible_kinds": json.loads(r[13]) if r[13] else None,
        "eligible_hosts": json.loads(r[14]) if r[14] else None,
        "site": r[15],
        "repo": r[16],
        "head_branch": r[17],
        "base_branch": r[18],
        "branch_created_from": r[19],
        "pr_number": r[20],
        "max_retries": r[21],
        "retry_count": r[22],
        "claimed_at": r[23],
        "claim_lease_expires_at": r[24],
        "last_update_at": r[25],
        "claimed_model": r[26],
        "claimed_provider": r[27],
        "claimed_runtime": r[28],
        "claimed_cost_tier": r[29],
        "preferred_providers": json.loads(r[30]) if r[30] else None,
        "preferred_models": json.loads(r[31]) if r[31] else None,
        "depends_on": json.loads(r[32]) if r[32] else None,
        "project": r[33],
        "max_cost_tier": r[34],
        "mnemos_refs": json.loads(r[35]) if r[35] else None,
    }
    out.update(_result_reason_fields(_result))
    return out


@router.get("/v1/jobs/{job_id}/preview")
async def zcgate_get_preview(job_id: str):
    """Return the zerocode-gate PreviewBundle stored on a needs-review job."""
    async with (
        connect_db() as db,
        db.execute(
            "SELECT result, status, repo, head_branch, base_branch, pr_number FROM jobs WHERE id=?",
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
        "base_branch": r[4],
        "pr_number": r[5],
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

    # Match the complete routing contract, not independent kind/host sets.
    async with hive_repo().connection() as tx:
        candidates = await hive_repo().list_agents(tx, include_offline=True)
    matching = []
    for candidate in candidates:
        if candidate.get("status") not in ACTIVE_AGENT_STATUSES:
            continue
        if candidate.get("host", "").lower() in HOST_DENYLIST:
            continue
        candidate["eligible_aliases"] = agent_kind_aliases(candidate["kind"], candidate.get("runtime"))
        if job_agent_eligible(req.model_dump(), candidate)[0] and not engine_unavailable(candidate.get("metadata")):
            matching.append(candidate)
    if not matching:
        raise HTTPException(422, "no_eligible_worker: no active healthy worker matches host, kind, capabilities and cost tier")

    # SUBMIT-TIME ADMISSION (2026-10-25): the two ways a job used to be
    # accepted in a state it could not start from.
    #
    #  1. preferred_models: a forbidden model or a name no registered worker
    #     serves silently fell back (e.g. to qwen38). A forbidden name is
    #     rejected (422); an unserved name is ACCEPTED with a warning, because
    #     a preference is not a requirement and a worker may register later.
    #  2. head_branch: nine jobs died because the ref did not exist on the
    #     ARGONAS remote. We now probe it here: present -> fine; missing with
    #     a usable base_branch -> accepted and recorded as branch_created_from;
    #     missing with no base -> 422 naming the ref.
    warnings: list[str] = []
    warnings.extend(host_pin_warnings(req.kind, req.eligible_hosts, req.pin_reason))
    if req.preferred_models:
        bad_models = [m for m in req.preferred_models if forbidden_model(m)]
        if bad_models:
            raise HTTPException(
                422,
                f"preferred_models contains forbidden model(s): {bad_models}. The fleet's "
                "routing policy forbids these routes (they can never run); request a "
                "served model or drop the preference.",
            )
        async with hive_repo().connection() as tx:
            _agent_models = await hive_repo().active_agent_models(tx)
        warnings.extend(admission_warnings(req.preferred_models, served_model_names(_agent_models)))

    # Off the event loop (2026-10-25 review): branch_admission shells out to
    # `git ls-remote` (up to PROBE_TIMEOUT per ref); a blocking call here would
    # stall EVERY concurrent request (including /v1/jobs/next claims) on the
    # single-threaded bus loop. to_thread runs it in the default executor.
    # Gated on head_branch AND repo: with no head there is nothing to probe
    # or create, and with no repo there is no remote to probe — skip the
    # thread dispatch (and the branch_not_verified warning) entirely.
    # Pre-check the resolver on the request thread: resolve_remote_url is a
    # catalog read (no git, no network), so it cannot stall the event loop.
    # When no remote can be resolved there is nothing to probe — skip the
    # thread dispatch entirely (the in-thread 'unprobed' state would warn for
    # a common shape with no actionable content).
    for ref in (req.head_branch, req.base_branch):
        if ref and not _ba_mod.valid_branch_name(ref):
            raise HTTPException(422, "invalid_ref: branch name is invalid")
    _probeable_remote = req.head_branch and req.repo and _ba_mod.resolve_remote_url(req.repo) is not None
    if req.head_branch and req.repo and not _probeable_remote and kind_requires_commit(req.kind, req.repo):
        raise HTTPException(422, "branch_unverified: repository must resolve to a trusted catalog remote")
    if _probeable_remote:
        _branches = await asyncio.to_thread(
            branch_admission,
            repo=req.repo,
            head_branch=req.head_branch,
            base_branch=req.base_branch,
        )
        warnings.extend(_branches["warnings"])
        if not _branches["ok"]:
            raise HTTPException(422, _branches["detail"])
        if _branches["state"] == "head_present" and kind_requires_commit(req.kind, req.repo):
            raise HTTPException(422, "output_branch_exists: choose a fresh head_branch; published branches are read-only inputs")
        if _branches["state"] in {"unreachable", "unprobed"}:
            # 2026-10-28: refusing here made EVERY commit-producing submission
            # fail whenever the bus could not reach origin (its own ssh key, a
            # probe timeout, a catalog entry with no remote) - that is a
            # transport fault, not a reason to reject work. A ref we could not
            # verify is recorded so the failure class is visible, and the worker
            # treats an identical already-pushed head as its own delivery, so the
            # collision this guard exists for is still caught.
            warnings.append(
                f"branch_probe_unverified: could not verify {req.head_branch!r} on origin "
                f"({_branches['state']}); the worker will deliver idempotently if it exists"
            )
        _branch_created_from = _branches["branch_created_from"]
    else:
        _branch_created_from = None

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
        base_branch=req.base_branch,
        branch_created_from=_branch_created_from,
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
                "warnings": warnings,
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
                "site": req.site,
                "repo": req.repo,
                "head_branch": req.head_branch,
                "base_branch": req.base_branch,
                "pr_number": req.pr_number,
                "max_retries": req.max_retries,
                "zero_retry_reason": req.zero_retry_reason,
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
            await hive_repo().submit_job(
                tx, **_common, dedup_hash=dedup_hash, status="queued",
                result_json=json.dumps({"zero_retry_reason": req.zero_retry_reason}) if req.max_retries == 0 else None,
            )
        except Exception as _dexc:
            if not is_dedup_violation(_dexc):
                raise
            _row = await hive_repo().find_active_dedup_job(tx, dedup_hash=dedup_hash)
            if _row:
                return {"id": _row[0], "created_at": _row[1], "coalesced": True, "warnings": warnings}
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
                "max_retries": req.max_retries,
                "zero_retry_reason": req.zero_retry_reason,
                "required_capabilities": req.required_capabilities,
                "eligible_kinds": req.eligible_kinds,
                "max_cost_tier": max_cost_tier,
                "mnemos_refs": req.mnemos_refs,
            },
        )
        await tx.commit()
    # ``warnings[]`` is the submitter-facing half of preferred_models /
    # head_branch admission: a submitted preference no worker serves, or a
    # branch that could not be verified, is reported here instead of being
    # discovered as a silent fallback (or a dead job) later.
    return {
        "id": job_id,
        "created_at": now,
        "mnemos_refs": req.mnemos_refs,
        "branch_created_from": _branch_created_from,
        "warnings": warnings,
    }


@router.post("/v1/jobs/next")
async def dequeue_next_job(agent_urn: str):
    """Atomic dequeue: highest-priority queued job that this agent is eligible for.

    Algorithm (eligibility / MiniMax pool / role-aware ordering) lives here in the
    router; every storage op goes through the HiveMindRepository ABC on ONE
    transaction (agent_claim_context -> pool_occupancy ->
    jobs_with_unsatisfied_deps -> queued_candidates -> claim_job -> emit_event).
    The candidate filter is advisory only; the binding depends_on gate is the
    in-transaction re-check inside claim_job.
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

            # DEPENDENCY ADMISSION (fault 2026-10-25): ONE candidate scan,
            # then the blocked set computed over that scan's ids and applied
            # in Python — no second SQL scan on the hot path. The exclusion
            # is ADVISORY here — the binding gate is the in-transaction
            # re-check inside claim_job (the SAME _deps_all_done evaluation),
            # so a candidate that slips through (deps not yet done at scan
            # time, or a stale pre-read) is still refused at the
            # compare-and-set.
            candidates = await hive_repo().queued_candidates(tx, agent_urn=agent_urn, now=now, limit=CLAIM_SCAN_WINDOW)
            # Only candidates that actually carry a depends_on column need the
            # evaluation: dep-free jobs (the common case) cost nothing here.
            # Index 15 is the depends_on column of queued_candidates (id=0,
            # submitter_urn=1, parent_job_id=2, kind=3, description=4,
            # priority=5, deadline=6, required_capabilities=7,
            # eligible_kinds=8, eligible_hosts=9, project=10, max_cost_tier=11,
            # preferred_providers=12, preferred_models=13, mnemos_refs=14,
            # depends_on=15) — the same index the loop unpacks below.
            _dep_candidates = [r for r in candidates if r[15]]
            _blocked_dep_jobs = (
                await hive_repo().jobs_with_unsatisfied_deps(tx, ids=[r[0] for r in _dep_candidates])
                if _dep_candidates
                else set()
            )
            if _blocked_dep_jobs:
                candidates = [r for r in candidates if r[0] not in _blocked_dep_jobs]
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
                    j_base_branch,
                    j_pr_number,
                ) = r
                # No per-candidate query here (2026-10-25 review): the
                # pre-scan above already excluded unsatisfied deps from THIS
                # read, and a fresh DB read per candidate in the claim loop
                # was N round-trips on the hot path. The binding gate is the
                # in-transaction re-check inside claim_job.
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
                try:
                    if not await _claim_health(tx, j_id, agent_urn):
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
                except DependencyNotReady:
                    # The DAG gate re-checked inside the claim transaction: the
                    # deps changed after the pre-scan, so skip this candidate.
                    continue
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
                    # Carried for the worker: depends_on is already enforced
                    # at claim time (the pre-scan above + the in-transaction
                    # re-check in claim_job), so a claimed job's deps are done
                    # by construction; the field lets the worker/log see the
                    # DAG edge.
                    "depends_on": json_list(j_deps_json),
                    "site": j_site,
                    "repo": j_repo,
                    "head_branch": j_head_branch,
                    "base_branch": j_base_branch,
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
    # `req.result` is normalized (assigned) further down, which ADDS "result" to
    # model_fields_set. Capture the caller's intent now, so a submitter cancel
    # is not refused for a field the caller never sent.
    original_result_was_set = field_was_set(req, "result")
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
    release_reason = None
    decline_reason = None
    dead_lettered = False
    if req.status == "queued" and isinstance(req.result, dict):
        _werr = str(req.result.get("worker_error") or "").strip()
        if _werr:
            release_reason = _werr
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
            job_submitter,
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
                for _rk in ("hive_reviews", "deepseek_reviews", "auto_review_job_id"):
                    if _prev.get(_rk) and not req.result.get(_rk):
                        req.result[_rk] = _prev[_rk]
            _reconcile_reviewer_record(job_id, req.result)

        # ---- SUBMITTER CANCELLATION (2026-10-25) -------------------------
        # A submitter could not cancel or supersede its OWN claimed job: the
        # claimant-only rule below answered 403 "job update requires claimed_by
        # to match the current claimant", so an orchestrator had to force-DELETE
        # the record and kill the worker process by hand.
        #
        # This grants exactly one field — status=cancelled with a reason — to
        # two callers besides the claimant:
        #   * the job's own submitter_urn;
        #   * an agent carrying the explicit `orchestrator` capability
        #     (a human operator / triage authority acting across jobs).
        # Everything else stays claimant-only: the block below is scoped to
        # `submitted_cancel`, and any non-status field the caller also sent is
        # rejected rather than silently applied.
        submitted_cancel = req.status == "cancelled" and req.claimed_by is None
        cancel_actor = None
        cancel_reason = None
        cancel_authorized_by = None
        if submitted_cancel:
            if old_claimed_by is None:
                # Nobody is holding it: there is no claim to override, and the
                # unclaimed paths below (with their own authorization) must not
                # be bypassed by a caller who omits claimed_by.
                raise HTTPException(
                    403,
                    "cancelling an unclaimed job requires claimed_by to match the current claimant "
                    "or its current status",
                )
            # ALLOWLIST, not a denylist. The elevation is exactly
            # {status, reason, cancel_actor}; every other field the caller
            # supplied is refused. A denylist silently let through fields the
            # author forgot to enumerate (priority, repo, head_branch,
            # base_branch, deadline, depends_on, ...), so a non-claimant could
            # mutate routing/identity under the guise of a cancel.
            _CANCEL_ALLOWED_FIELDS = frozenset({"status", "reason", "cancel_actor"})
            # `result` is normalized by an unconditional assignment at line ~876,
            # which adds it to fields_set whether or not the caller sent it.
            # Trust the capture taken BEFORE that (original_result_was_set).
            _supplied = set(req.model_fields_set) - {"result"}
            if original_result_was_set:
                _supplied.add("result")
            _extra_fields = sorted(_supplied - _CANCEL_ALLOWED_FIELDS)
            if _extra_fields:
                raise HTTPException(
                    403,
                    "a submitter/orchestrator cancel may set only status, reason and cancel_actor "
                    "(a cancellation note belongs in reason; a result is the claimant's to write); "
                    f"refusing to also update {_extra_fields}",
                )
            # The actor is the caller-declared principal, but it is only an
            # IDENTITY CLAIM: it must match the job's own submitter_urn, or the
            # agent row for that urn must carry the orchestrator capability.
            # Request-body fields can never widen that -- `submitter_urn` on the
            # PATCH is deliberately NOT consulted (a caller cannot become the
            # submitter by writing the field), and an unknown urn has no row,
            # so it can never be orchestrator-capable.
            cancel_actor = (req.cancel_actor or "").strip() or None
            cancel_reason = (req.reason or "").strip() or None
            if not cancel_actor:
                raise HTTPException(
                    403,
                    "cancel requires claimed_by to match the current claimant, or cancel_actor naming "
                    "the job's submitter_urn, or an agent registered with the 'orchestrator' capability",
                )
            is_own_submitter = isinstance(job_submitter, str) and bool(job_submitter) and cancel_actor == job_submitter
            # Reuse the OPEN transaction: a second connection here would take a
            # second SQLite write lock (deadlock risk) or a second pooled
            # Db2/Oracle connection per cancel.
            is_orchestrator = (not is_own_submitter) and await _has_orchestrator_capability(cancel_actor, tx)
            if not (is_own_submitter or is_orchestrator):
                raise HTTPException(
                    403,
                    "cancel requires claimed_by to match the current claimant, or the job's "
                    "submitter_urn, or an agent with the 'orchestrator' capability; "
                    f"actor={cancel_actor!r} submitter={job_submitter!r} claimant={old_claimed_by!r}",
                )
            cancel_authorized_by = "submitter" if is_own_submitter else "orchestrator-capability"

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

        # DEFECT 2026-10-25: this was the wall that made a submitter unable to
        # cancel its own claimed job ("job update requires claimed_by to match
        # the current claimant"), forcing a manual force-DELETE + worker kill.
        # A `submitted_cancel` -- already authorized above against the job's
        # submitter_urn or a registered orchestrator capability, and already
        # restricted to status/reason/cancel_actor -- is the ONE case that may
        # pass without matching the claimant. Everything else still must.
        if (
            old_claimed_by
            and req.status in {"claimed", "running", "done", "failed", "cancelled"}
            and not submitted_cancel
        ):
            if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                raise HTTPException(403, "job update requires claimed_by to match the current claimant")

        effective_claimed_by = None if release_to_queue else (req.claimed_by or old_claimed_by)
        if req.status in {"claimed", "running"} and not effective_claimed_by:
            raise HTTPException(422, f"status={req.status!r} requires an existing or explicit claimed_by")

        # 2026-10-27: the deployed worker's delivery-integrity check counts the
        # branch a read-only job's workspace was CHECKED OUT on as a delivery,
        # so a review that produced no commits died as
        # delivery_integrity_check_failed ("delivery landed on default branch
        # 'master'") even though it pushed nothing. Within one hour that filled
        # triage with review: rows. The tracked worker copy is fixed, but hosts
        # still running the older worker must not keep killing read-only work, so
        # normalize the exact contradiction here: read-only kind, failed, no
        # commits, no push, and default-branch as the ONLY complaint.
        if req.status == "failed" and is_readonly_prefix_kind(job_kind):
            _r = req.result or {}
            _errs = _r.get("delivery_integrity_errors") or []
            _only_default_branch = bool(_errs) and all(
                str(e).startswith("delivery landed on default branch") for e in _errs
            )
            _push = ((_r.get("codex_delivery") or {}).get("push") or {})
            _no_artifact = not (_r.get("commits") or []) and (
                _push.get("status") != "pushed"
            )
            if (
                _r.get("failure_reason") == "delivery_integrity_check_failed"
                and _only_default_branch
                and _no_artifact
            ):
                log.warning(
                    "job %s: normalizing a read-only delivery_integrity false positive (%s) to done",
                    job_id,
                    "; ".join(str(e) for e in _errs),
                )
                req.status = "done"
                req.result = {
                    **_r,
                    "failure_reason": None,
                    "delivery_normalized": "readonly-no-artifact-default-branch",
                }
                patch_payload["status"] = "done"
                patch_payload["result"] = req.result

        commit_required = kind_requires_commit(job_kind, job_repo)
        if req.status == "done" and commit_required:
            # A read-only kind (READONLY_KINDS) proves completion with its
            # verdict/deliverable, or with a git proof when it did commit.
            # Everything else still needs the full commit-evidence gate.
            proof = req.result or {}
            ok = (
                valid_readonly_completion_proof(job_kind, proof)
                if is_readonly_prefix_kind(job_kind)
                else valid_completion_proof(proof)
            )
            if not ok:
                req.status = "needs-review"
                completion_rejected = True
                req.result = {**proof, "failure_reason": "completion_evidence_missing"}
                patch_payload["status"] = req.status
                patch_payload["result"] = req.result

        # Infrastructure attempts retain evidence and never become work failures.
        # Exhaustion parks the row visibly without bypassing the submitter budget.
        infra_class = infrastructure_failure(req.result) if req.status == "failed" else None
        if infra_class and old_status in {"claimed", "running"}:
            req.result = {**(req.result or {}), "failure_class": infra_class,
                          "last_failed_claimant": old_claimed_by,
                          "infrastructure_parked": retry_count >= max_retries}
            req.status = "queued"
            release_to_queue = True
            patch_payload["status"] = "queued"
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
            # WHY the job left a worker, inside the row's own result JSON
            # (2026-10-27). Backend-neutral on purpose: PANTHEON/HIVE run on
            # Oracle in production, where adding columns to the shared SELECT
            # means a DDL migration on the live DB; result is already returned
            # by GET, so the row explains itself with no schema change.
            _res = dict(req.result) if isinstance(req.result, dict) else {}
            if release_reason:
                _res["last_release_reason"] = release_reason
            _res["last_release_at"] = now
            _res["last_released_by"] = old_claimed_by
            sets["result"] = json.dumps(_res)
            # GRAEAE dispatch consensus 1.0 SHIP-FIRST: exponential backoff on EVERY
            # release (±20% jitter) so a job no local worker can run cannot hot-loop.
            import random as _rnd

            _rc = int(retry_count or 0) + 1
            _bo = min(5.0 * (2 ** min(_rc, 6)), 300.0) * (1.0 + _rnd.uniform(-0.2, 0.2))
            # Routing declines are not failed work attempts and must not spend
            # the failure budget before execution has even started.
            if infra_class and retry_count < max_retries:
                sets["retry_count"] = SqlExpr("retry_count+1")
            sets["retry_backoff_until"] = now + _bo
        if req.status in {"claimed", "running"}:
            sets["claim_lease_expires_at"] = now + CLAIM_LEASE_SECONDS
        if req.status in TERMINAL_JOB_STATUSES or req.status in {"queued", "needs-review"}:
            # needs-review is a quarantine stop: the worker is done, so its lease
            # must not stay live (a stale lease makes the job look claimed).
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
                is_current_claimant = (
                    isinstance(old_claimed_by, str) and old_claimed_by != "" and req.claimed_by == old_claimed_by
                )
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
            raise HTTPException(409, f"job {job_id} status changed concurrently (expected {old_status!r}); retry")

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
                    "note": f"dead_letter:thrash_guard:max_decline_requeues_exceeded after {new_decline_count} declines",
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
        if req.status == "failed" and retry_policy.auto_retry_allowed({
            "status": "failed", "kind": job_kind, "retry_count": retry_count,
            "max_retries": max_retries, "result": req.result,
        }):
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
                    "result": json.dumps(req.result) if req.result is not None else None,
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

        # AUDIT: a submitter/orchestrator cancel is recorded with WHO did it and
        # WHY, on the row's own transition. The claimant's own cancel keeps the
        # claimant as actor, so the audit trail never hides a forced stop.
        await hive_repo().insert_job_audit(
            tx,
            job_id=job_id,
            ts=now,
            actor_urn=(cancel_actor if submitted_cancel else (req.claimed_by or old_claimed_by)),
            old_status=old_status,
            new_status="dead-letter" if dead_lettered else ("queued" if retried else req.status),
            old_claimed_by=old_claimed_by,
            new_claimed_by=None if release_to_queue else effective_claimed_by,
            patch_json=json.dumps(
                {
                    **patch_payload,
                    **(
                        {
                            "cancel": {
                                "actor": cancel_actor,
                                "reason": cancel_reason,
                                "authorized_by": cancel_authorized_by,
                                "claimant_observed": False,
                            }
                        }
                        if submitted_cancel
                        else {}
                    ),
                },
                default=str,
            ),
        )
        # DEPENDENCY RELEASE (incident 2026-10-27): a job entering a status its
        # dependents can never satisfy (failed / failed_completion / dead-letter
        # / cancelled) releases every queued/offered job that lists it in
        # depends_on, in the SAME transaction as this status write (before the
        # single commit below), so there is no window where the dep is terminal
        # and its dependents still believe it is pending. ``done`` is NOT in
        # DEP_DEAD_STATUSES: a completed dep satisfies its dependents the normal
        # way, and the edge is kept for provenance.
        #
        # The retried-failed case (status bounced back to queued, see above)
        # must NOT release: the dep is not dead, it is retrying — its
        # dependents stay blocked on it. The row's FINAL status in this
        # transaction decides (retried -> 'queued' -> no release; a job whose
        # retries are exhausted stays 'failed' -> release).
        if (not retried) and req.status in DEP_DEAD_STATUSES:
            await release_dependencies(tx, job_id, req.status, emit_event=emit_event)
        # Triage rows describe remediation, not code branches for review.
        if not retried and req.status == "needs-review" and not str(job_kind or "").startswith("triage:"):
            await dispatch_needs_review(tx, hive_repo(), job_id, now=now, emit_event=emit_event)
        if not retried and req.status == "done":
            await reconcile_review_parent(tx, hive_repo(), job_id, now=now, emit_event=emit_event)
        await tx.commit()

    async with hive_repo().connection() as event_tx:
        if submitted_cancel:
            # The claimant learns of the stop from its next heartbeat/update:
            # the row is terminal, so its renewal PATCH gets a 409 and its
            # worker shuts down cleanly (hive_worker stops on 403/409). The
            # event makes that visible to dashboards in the meantime.
            await emit_event(
                event_tx,
                "job.cancelled",
                {
                    "id": job_id,
                    "actor": cancel_actor,
                    "reason": cancel_reason,
                    "authorized_by": cancel_authorized_by,
                    "submitter": job_submitter,
                    "previous_claimant": old_claimed_by,
                },
            )
        if retried:
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
            raise HTTPException(409, f"claim failed: agent status is {agent_status!r}, not online/idle: {by}")
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

        # PRE-READ GATE (2026-10-25): fail CLOSED on any non-empty depends_on
        # whose entries are not all done — including malformed JSON, non-string
        # entries, and references to unknown job ids (a dep that does not exist
        # can never reach done, so completed_job_ids simply never lists it and
        # the job stays blocked). The in-transaction re-check in claim_job
        # repeats the SAME evaluation (_deps_all_done) under the write lock;
        # the pre-read exists so the caller gets a named 409 naming the
        # unsatisfied dep ids instead of the same refusal from inside the tx.
        # Dep-free jobs (the common case) cost nothing here: the column is read
        # in the context SELECT already, and NULL/empty means satisfied.
        if j_deps_json:
            _parsed_deps = None
            try:
                _parsed_deps = json.loads(j_deps_json)
            except (TypeError, ValueError):
                _parsed_deps = None
            if not isinstance(_parsed_deps, list) or any(not isinstance(d, str) or not d.strip() for d in _parsed_deps):
                raise HTTPException(
                    409,
                    f"job has unsatisfied DAG dependencies: {job_id} "
                    "(depends_on must be a list of job-id strings; refusing to claim a job whose "
                    "dependency set cannot be evaluated)",
                )
            _missing = sorted({d.strip() for d in _parsed_deps})
            if _missing:
                _done = await hive_repo().completed_job_ids(tx, ids=_missing)
                _unsat = sorted(set(_missing) - _done)
                if _unsat:
                    raise HTTPException(
                        409,
                        f"job has unsatisfied DAG dependencies: {job_id} (not done: {_unsat})",
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
        if not await _claim_health(tx, job_id, by):
            raise HTTPException(409, "infrastructure_hold: worker unavailable, previous failed host, or budget exhausted")
        eligible, reason = job_agent_eligible(job, agent)
        if not eligible:
            status = (
                422
                if reason.startswith("job max_cost_tier")
                else (429 if reason.startswith("subscription agent throttled") else 403)
            )
            raise HTTPException(status, reason)

        try:
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
        except DependencyNotReady as exc:
            raise HTTPException(409, str(exc)) from exc
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
