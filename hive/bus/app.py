"""
GRAEAE Hive Mind — fleet-wide MCP-compatible agent coordination + triage queue.

Brand: GRAEAE Hive Mind (extension of GRAEAE consensus engine — sister to mnemos memory)
Identity: urn:agent:<kind>:<host>:<session_uuid>   (kinds: claude, opencode, codex, zeroclaw, openclaw, hermes, human, ic-engine, mnemos, ...)
Backend: SQLite WAL (Phase 1) — PG migration documented for Phase 2 when >50 agents OR LISTEN/NOTIFY needed
Pub/sub: SSE (Phase 1) — NATS migration Phase 2 when >20 concurrent OR >100 msg/s
Triage queue: /v1/jobs/next dequeues highest-priority eligible work; no central scheduler — agents self-claim.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from hive.bus.deps import (
    _cascade_delete_jobs,
    connect_db,
    emit_event,
    hive_repo,
)

# Capacity ledger (Phase 5) moved to hive.bus.domain.capacity_ledger (Phase 1c).
from hive.bus.domain.capacity_ledger import flush_capacity_ledger, load_capacity_ledger
from hive.bus.domain.reaper import select_orphan_claims
from hive.bus.routers.admin import admin_requeue_jobs  # noqa: F401 (re-export for tests)
from hive.bus.routers.admin import router as _admin_router
from hive.bus.routers.agents import router as _agents_router
from hive.bus.routers.events import router as _events_router
from hive.bus.routers.knemon import router as _knemon_router
from hive.bus.routers.mcp import router as _mcp_router
from hive.bus.routers.system import router as _system_router
from hive.bus.routers.jobs import (  # noqa: F401 (re-export)
    auto_retry_job,
    claim_job,
    create_job,
    get_job,
    list_jobs,
    update_job,  # noqa: F401 (re-export)
    zcgate_release,
)
from hive.bus.routers.jobs import router as _jobs_router
from hive.bus.routers.messages import router as _messages_router
from hive.bus.routers.repos import router as _repos_router
from hive.bus.routers.schedules import router as _schedules_router
from hive.bus.routers.stats import router as _stats_router

# Compat re-exports — symbols moved into routers during Phase 1c, kept on the
# agent_bus facade for tests + external callers that still import from here.
from hive.bus.routers.agents import heartbeat, list_agents, register  # noqa: F401 (re-export)
from hive.bus.routers.knemon import (  # noqa: F401 (re-export)
    KNEMON_BYPASS_FILE,
    KNEMON_CAP_FILE,
    OAUTH_CAP_RESET_SEC,
    clear_oauth_cap,
    knemon_bypass_active,
    oauth_cap_state,
)
from hive.bus.routers.mcp import mcp_rpc  # noqa: F401 (re-export)
from hive.bus.routers.messages import list_messages, publish_message  # noqa: F401 (re-export)
from hive.bus.routers.system import dashboard, health  # noqa: F401 (re-export)
from hive.persistence.sqlite import DDL as SCHEMA  # noqa: F401 (re-export: sqlite DDL lives in the backend)
from hive.bus.schemas import (
    JobCreate,
    JobPurgeRequest,  # noqa: F401 (re-export for tests)
    PurgeRequest,  # noqa: F401 (re-export for tests)
    ReclassifyRequest,  # noqa: F401 (re-export for tests)
    RequeueRequest,  # noqa: F401 (re-export for tests)
)

# Re-exported for back-compat: mcp_bridge consumers + tests read these off agent_bus.
from hive.core.roles import ORCHESTRATOR_RUNTIMES, WORKER_ONLY_RUNTIMES  # noqa: F401

HIVE_BACKEND = os.environ.get("HIVE_BACKEND", "sqlite").lower()
HEARTBEAT_REAP_INTERVAL = 30.0
HEARTBEAT_STALE_AFTER = 90.0
HEARTBEAT_OFFLINE_AFTER = 300.0
# Hard-delete agents that have been offline this long. Every worker restart
# registers a NEW session URN, so without this the agents table grows without
# bound (10,930 rows / 10,882 offline observed 2026-06-08) and poisons the
# dashboard worker panel + per-worker stats. 6h keeps a generous recent-history
# window while reaping long-dead session rows.
AGENT_PURGE_AFTER = float(os.environ.get("HIVE_AGENT_PURGE_AFTER", "21600"))
EVENTS_RETAIN_HOURS = 168  # 7 days
# Job retention sweep (housekeeping — the dashboard fills with test/duplicate
# jobs fast). Conservative defaults; both windows env-overridable.
#  - Terminal jobs (done/failed/failed_completion/cancelled/dead-letter) older
#    than RETAIN_DONE_JOBS_DAYS are auto-deleted.
#  - Obvious test/e2e jobs (description matches TEST_JOB_DESC_PATTERNS) terminal
#    AND older than RETAIN_TEST_JOBS_DAYS are deleted sooner.
# Active jobs (queued/offered/claimed/running/needs-review) are NEVER swept.
RETAIN_DONE_JOBS_DAYS = float(os.environ.get("RETAIN_DONE_JOBS_DAYS", "7"))
RETAIN_TEST_JOBS_DAYS = float(os.environ.get("RETAIN_TEST_JOBS_DAYS", "1"))
# Statuses considered terminal & sweepable. needs-review is intentionally
# EXCLUDED (it is a human-attention queue, not finished work).
SWEEPABLE_STATUSES = ("done", "failed", "failed_completion", "cancelled", "dead-letter")
# Active/in-flight statuses that delete endpoints + the sweep refuse to touch
# (without explicit force/include_active) so an in-flight worker is never orphaned.
# ACTIVE_JOB_STATUSES moved to hive.bus.deps (imported above).
# Substrings (case-insensitive) that mark a job description as obvious test junk.
TEST_JOB_DESC_PATTERNS = (
    "e2e",
    "testdispatch",
    "append a one-line",
    "dated comment",
    "xfam",
    "validate native",
    "hive functional",
    "hive deploy e2e",
)
SSE_PING_INTERVAL = 15.0
# EVENT_QUEUE now lives in hive.bus.deps (imported above).
MAX_EVENT_SUBSCRIBERS = int(os.environ.get("AGENT_BUS_MAX_EVENT_SUBSCRIBERS", "100"))


# Legacy raw-SQL→Oracle translation shim (BackendCursor / OracleConnection /
# _translate_sql / _rewrite_* / _TABLE_MAP / _parse_oracle_url) REMOVED
# (Phase 1d): superseded by the HiveMindRepository ABC + the persistence
# factory. All storage ops go through repo methods on the resolved backend.

# ---------- schema ----------



# ---------- models ----------

# Runtime → eligible kinds map (Kimi-K2.6 advisory 2026-05-23).
# AUTONOMY_LEVELS / AUTH_METHODS / DEFAULT_PLAN_CAPS / VALID_AGENT_STATUSES
# moved to hive.bus.deps (imported above).
# ACTIVE_AGENT_STATUSES moved to hive.bus.deps (imported above).

# ROLE SPLIT (user directive 2026-05-23): opencode + codex + hermes +
# claw-family + ic-engine + unknown = WORKERS (claim-only). Cannot submit jobs.
# Orchestrators include a distinct Codex role. Ordinary Codex workers stay claim-only.
# WORKER_ONLY_RUNTIMES / ORCHESTRATOR_RUNTIMES now live in hive.core.roles
# (single source of truth, shared with mcp_bridge.py). Imported above.



# (Removed 2026-10-15): the bus-side host-quarantine constant/probe. The only
# host it ever guarded was the NVIDIA-LAN Spark (spark-0c53, out-claiming
# zeroclaw jobs); Spark is unsupported going forward — the fleet is local-LAN
# only. Live host exclusion is the systemd `HIVE_HOST_DENYLIST` env, not this.


# 2026-10-02 (operator): hold sentinels are DEPRECATED. They existed to park jobs
# behind the zeroclaw pre-submission gate, which is no longer enforced. Treat
# these as unrestricted so their jobs are schedulable by capable workers.
# ``rescue-superseded-hold`` is deliberately NOT in this set: those jobs are
# superseded and must be cancelled rather than run.

# WORKSPACE AFFINITY — automatic required_capabilities injection + claim guard.
# Workspace-scoped jobs must only be offered to workers that explicitly
# advertise the matching workspace capability. This protects both newly
# submitted jobs and older queued rows that were created before the mapping.


# COST-TIER MAP (per ~/.claude/rules/llm-usage-policy-2026-05-22.md):
#   A = FREE   — local + NGC NIM (try first, token-miser)
#   B = CHEAP  — Groq Dev tier, xAI, DeepSeek direct, Together cheap, Gemini-Flash, OpenAI-mini
#   C = RESERVE — Anthropic Opus/Sonnet, OpenAI GPT-5.5/Pro, Gemini Pro, Together DeepSeek-Pro
#                 (Together DeepSeek-V4-Pro = anti-pattern — use DeepSeek direct instead)
# PROVIDER_COST_TIER / COST_TIERS now live in hive.core.models (imported above).
# VALID_JOB_STATUSES / TERMINAL_JOB_STATUSES moved to hive.bus.deps (imported above).
# `released` was terminal here but is NOT in the jobs.status CHECK constraint and
# is produced by no transition, so PATCH status=released passed validation then
# died in the DB as a raw IntegrityError (500, not 4xx). An unreachable
# terminal state is worse than no such state.
# Skip releases (host_declines_kind/released_by_host/no_workspace_for_kind) are
# legitimate "not me" routing signals. They must not burn the decline dead-letter
# threshold; real execution failures use the failed/retry path.
# job consts (MAX_DECLINE_REQUEUES/DECLINE_REASON_PREFIXES/STATUS_TRANSITIONS/CLAIM_LEASE_SECONDS) moved to hive.bus.domain.jobs (imported above).


#: Eligibility is evaluated in Python (job_agent_eligible), so the candidate
#: query cannot know which rows THIS agent can take. A single fixed window
#: therefore starves any agent whose work sits behind rows it is ineligible
#: for: it gets 204 forever while claimable work sits queued. Scan deeper,
#: bounded on both sides so a huge queue cannot turn a claim into a scan.
# CLAIM_SCAN_WINDOW moved to hive.bus.deps (imported above).
# Hosts the bus refuses to offer jobs to — typically because their workers
# have a broken config (e.g. enc2 secret-key mismatch) and would fail any
# claim immediately. User directive 2026-05-26: pegasus (.79) is flooding
# the failed queue with 241/243 recent fails — denylist it until config-fix.
# Format: lowercase short hostname. Updated via env or admin endpoint.
# HOST_DENYLIST moved to hive.bus.deps (imported above).

# Phase 2 provider-as-capability gate. When ON, an executor job (build / agent
# coding kind) can only be claimed by a host advertising >=1 live llm-* cap, so a
# host whose only LLM provider is dead self-excludes (no host denylist needed).
# Default OFF until workers advertise llm-* fleet-wide (flip after Phase 4).


# cost_tier_for moved to hive.core.models (imported above).


# json_list moved to hive.bus.domain.jobs (imported above).


# field_was_set moved to hive.bus.domain.jobs (imported above).


# valid_completion_proof moved to hive.bus.domain.jobs (imported above).


# job result helpers + result-cache moved to hive.bus.domain.jobs (Phase 1c).


# ---------- lifecycle ----------

CLAIM_STALE_AFTER = 1800.0  # claim → still 'claimed'/'running' without update >30min ⇒ orphan
# REVIEW #2 fix: kind-specific overrides. Orchestration jobs that fan out
# + collect can legitimately run 60+ minutes without a PATCH while
# sub-jobs execute. Reaper also measures against last_update_at (any PATCH
# touches it) instead of just claimed_at — so a steady stream of progress
# patches keeps the orphan reaper at bay.
CLAIM_STALE_AFTER_BY_KIND = {
    "orchestration": 3600.0,
    "investigation": 3600.0,
    "benchmark": 3600.0,
    "migration": 7200.0,  # multi-host db / fleet migrations need wide window
}

# Absolute, UN-REFRESHABLE claim ceiling (operator 2026-06-20). The lease +
# last_update_at staleness checks are BOTH refreshed by any PATCH, so a worker
# stuck re-attempting+failing the SAME job kept them fresh and a 51h 'running'
# job never reaped (019ed7a4-7e33). This cap measures wall-clock since the FIRST
# claim (claimed_at — never touched by PATCH), so a genuinely-stuck claim is
# reaped no matter how often its claimant pokes it.
CLAIM_HARD_MAX_AGE = float(os.environ.get("HIVE_CLAIM_HARD_MAX_AGE", "14400"))  # 4h
CLAIM_HARD_MAX_AGE_BY_KIND = {
    "migration": 21600.0,  # 6h — multi-host fleet migrations may legitimately run long
}


async def reaper_task(app: FastAPI):
    while True:
        await asyncio.sleep(HEARTBEAT_REAP_INTERVAL)
        try:
            async with connect_db() as db:
                # 1. Heartbeat reaper: online/idle -> stale after 90s, offline after 5m.
                now_ts = time.time()
                stale_cutoff = now_ts - HEARTBEAT_STALE_AFTER
                offline_cutoff = now_ts - HEARTBEAT_OFFLINE_AFTER
                repo = hive_repo()
                stale = await repo.select_stale_agent_urns(
                    db, stale_cutoff=stale_cutoff, offline_cutoff=offline_cutoff
                )
                if stale:
                    await repo.mark_agents_stale(db, stale)
                    await db.commit()
                    for urn in stale:
                        await emit_event(db, "agent.stale", {"urn": urn, "reason": "heartbeat_stale"})

                dead = await repo.select_dead_agent_urns(db, offline_cutoff=offline_cutoff)
                if dead:
                    for urn in dead:
                        # A MISSED HEARTBEAT IS NOT PROOF OF AN ABANDONED JOB.
                        # release_unstarted_jobs_for_agent releases only work that
                        # has not STARTED (offered/claimed) — no result can be lost.
                        # In-flight reclaim is step 2's job (staleness measured
                        # against COALESCE(last_update_at, claimed_at)).
                        assigned_jobs = await repo.release_unstarted_jobs_for_agent(db, urn)
                        await repo.mark_agent_offline(db, urn)
                        for job_id in assigned_jobs:
                            await emit_event(
                                db,
                                "job.unclaimed",
                                {
                                    "id": job_id,
                                    "prior_claimer": urn,
                                    "reason": "agent_offline_heartbeat_timeout",
                                },
                            )
                    await db.commit()
                    for urn in dead:
                        await emit_event(
                            db,
                            "agent.offline",
                            {"urn": urn, "reason": "heartbeat_timeout"},
                        )

                # 1b. AGENT PURGE: hard-delete agents offline beyond AGENT_PURGE_AFTER.
                # Each worker restart registers a fresh session URN; without this the
                # agents table grows unbounded (10,930 rows / 10,882 offline observed
                # 2026-06-08) and poisons the dashboard worker panel. Done as a single
                # SET-BASED delete that re-checks every safety condition AT DELETE TIME
                # (status, heartbeat age, no in-flight claim) so an agent that heartbeats
                # or claims work between a select and a delete cannot be removed — there
                # is no select/delete window. Claim recovery (step 2) runs after, so the
                # NOT EXISTS guard is the authoritative protection for actively-claimed
                # URNs, not ordering.
                purge_cutoff = now_ts - AGENT_PURGE_AFTER
                purged = await repo.purge_offline_agents(db, cutoff=purge_cutoff)
                if purged:
                    await db.commit()
                    print(
                        f"agent purge: removed {purged} agents offline > {AGENT_PURGE_AFTER:.0f}s",
                        flush=True,
                    )
                # 2. ORPHAN CLAIM RECOVERY — jobs claimed by dead/stale workers go back to queue.
                # REVIEW #2 fix: measure staleness against COALESCE(last_update_at, claimed_at)
                # so steady PATCH progress keeps reaper at bay. Kind-specific cutoffs let
                # orchestration / investigation / migration jobs run longer than default 30min.
                now_ts = time.time()
                default_cutoff = now_ts - CLAIM_STALE_AFTER
                hard_cutoff = now_ts - CLAIM_HARD_MAX_AGE
                candidates = await repo.select_orphan_claim_candidates(
                    db, now=now_ts, hard_cutoff=hard_cutoff, default_cutoff=default_cutoff
                )
                # Apply kind-specific override: keep job if its kind has a longer TTL
                # AND its latest progress timestamp is within that window.
                orphans = select_orphan_claims(
                    candidates,
                    now=now_ts,
                    stale_after=CLAIM_STALE_AFTER,
                    hard_max_age=CLAIM_HARD_MAX_AGE,
                    stale_by_kind=CLAIM_STALE_AFTER_BY_KIND,
                    hard_by_kind=CLAIM_HARD_MAX_AGE_BY_KIND,
                )
                if orphans:
                    for job_id, claimer in orphans:
                        await repo.requeue_orphan_job(db, job_id)
                        await emit_event(
                            db,
                            "job.unclaimed",
                            {
                                "id": job_id,
                                "prior_claimer": claimer,
                                "reason": "worker_offline_or_stale_claim",
                            },
                        )
                    await db.commit()
                # 3. Scheduler — fire due interval-based jobs
                now_ts = time.time()
                due = await repo.select_due_schedules(db, now=now_ts)
                for sched_id, sname, sub_urn, interval, tpl_json, fcount in due:
                    try:
                        tpl = json.loads(tpl_json)
                        tpl["submitter_urn"] = sub_urn
                        # synth a JobCreate + go through the cache+role machinery
                        await create_job(JobCreate(**tpl))  # type: ignore
                        await repo.mark_schedule_fired(
                            db, sched_id=sched_id, now=now_ts, next_at=now_ts + interval
                        )
                        await emit_event(
                            db,
                            "schedule.fired",
                            {
                                "schedule_id": sched_id,
                                "name": sname,
                                "fire_count": fcount + 1,
                                "next_at": now_ts + interval,
                            },
                        )
                    except Exception as se:
                        print(f"scheduler error {sched_id}: {se}", flush=True)
                if due:
                    await db.commit()
                # 4. Auto-cancel stale jobs (queued > 7 days untouched)
                stale_job_cutoff = time.time() - 7 * 24 * 3600
                stale_ids = await repo.select_stale_queued_job_ids(db, cutoff=stale_job_cutoff)
                if stale_ids:
                    await repo.cancel_jobs(db, stale_ids, ended_at=time.time())
                    for j in stale_ids:
                        await emit_event(db, "job.cancelled", {"id": j, "reason": "stale_>7d"})
                    await db.commit()
                # 5. Purge old events
                retain_cutoff = time.time() - EVENTS_RETAIN_HOURS * 3600
                await repo.purge_events_before(db, cutoff=retain_cutoff)
                await db.commit()
                # 6. JOB RETENTION SWEEP (housekeeping — keeps the dashboard clean).
                # Auto-delete OLD TERMINAL jobs (+ cascade job_audit_log). Two tiers:
                #   (a) obvious test/e2e junk older than RETAIN_TEST_JOBS_DAYS
                #   (b) any terminal job older than RETAIN_DONE_JOBS_DAYS
                # Active statuses (queued/offered/claimed/running/needs-review) are
                # NEVER candidates — the status filter excludes them, so an in-flight
                # worker can never be reaped here. Conservative + logged. Age is taken
                # from COALESCE(ended_at, started_at). Done as SELECT-then-cascade so
                # the same single-id cascade helper handles audit-log rows on both
                # backends; the re-check is implicit (status is in the SELECT predicate
                # and terminal jobs do not transition back to active).
                now_ts = time.time()
                sweep_ids: list[str] = []
                # (a) test/e2e junk — terminal + matches any test pattern + > test window
                test_cutoff = now_ts - RETAIN_TEST_JOBS_DAYS * 86400
                sweep_ids += await repo.select_sweepable_test_job_ids(
                    db,
                    statuses=list(SWEEPABLE_STATUSES),
                    patterns=TEST_JOB_DESC_PATTERNS,
                    cutoff=test_cutoff,
                )
                # (b) any terminal job older than the done-retention window
                done_cutoff = now_ts - RETAIN_DONE_JOBS_DAYS * 86400
                sweep_ids += await repo.select_sweepable_terminal_job_ids(
                    db, statuses=list(SWEEPABLE_STATUSES), cutoff=done_cutoff
                )
                sweep_ids = list(dict.fromkeys(sweep_ids))  # de-dupe, keep order
                if sweep_ids:
                    # allow_active defaults False: the cascade re-checks status at
                    # delete time, so a job that somehow flipped back to active
                    # between SELECT and DELETE is left alone (atomic guard).
                    swept = await _cascade_delete_jobs(db, sweep_ids)
                    await db.commit()
                    if swept:
                        print(
                            f"job retention sweep: removed {len(swept)} terminal "
                            f"jobs (test<{RETAIN_TEST_JOBS_DAYS}d / done<{RETAIN_DONE_JOBS_DAYS}d)",
                            flush=True,
                        )
                # Persist the capacity ledger (Phase 5) — idempotent keyed SET-upsert
                # so a restart/replay converges instead of double-counting.
                try:
                    await flush_capacity_ledger(hive_repo())
                except Exception as ce:  # noqa: BLE001
                    print(f"capacity ledger flush skipped: {ce}", flush=True)
        except Exception as e:
            print(f"reaper error: {e}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    repo = hive_repo()
    # Every backend owns its DDL/migration: sqlite converges via its SCHEMA +
    # additive migrations (SqliteHiveRepository.init); Oracle + Db2 converge via
    # their repository init(). No per-backend branching in the bus.
    await repo.init()
    # Hydrate the per-service capacity ledger from persisted state (Phase 5).
    # Best-effort: never fatal (a repo without the capacity surface, or a fresh DB).
    try:
        loaded = await load_capacity_ledger(repo)
        if loaded:
            print(f"capacity ledger: hydrated {loaded} service(s)", flush=True)
    except Exception as ce:  # noqa: BLE001
        print(f"capacity ledger hydrate skipped: {ce}", flush=True)
    task = asyncio.create_task(reaper_task(app))
    app.state.reaper_task = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await repo.close()


app = FastAPI(title="GRAEAE Hive Mind", version="0.1.0", lifespan=lifespan)
app.include_router(_repos_router)
app.include_router(_stats_router)
app.include_router(_events_router)
app.include_router(_knemon_router)
app.include_router(_system_router)
app.include_router(_mcp_router)
app.include_router(_messages_router)
app.include_router(_agents_router)
app.include_router(_admin_router)
app.include_router(_jobs_router)
app.include_router(_schedules_router)


# ---------- endpoints ----------


# /health + / (dashboard) moved to hive.bus.routers.system (Phase 1c).

# /v1/agents + /v1/hosts routes moved to hive.bus.routers.agents (Phase 1c).


# GRAEAE permanent logjam-fix workspace resolution + _is_dedup_violation moved
# to hive.bus.domain.{workspace,jobs} (imported above).


# POST /v1/jobs (create_job) moved to hive.bus.routers.jobs (Phase 1c).


# POST /v1/jobs/next (dequeue_next_job) moved to hive.bus.routers.jobs (Phase 1c).


# PATCH /v1/jobs/{id} (update_job) moved to hive.bus.routers.jobs (Phase 1c).


# /v1/jobs/{job_id}/auto-retry moved to hive.bus.routers.jobs (Phase 1c).


# /v1/admin/jobs/* + DELETE /v1/jobs/{id} + /v1/jobs/purge moved to hive.bus.routers.admin (Phase 1c).


# /v1/schedules routes moved to hive.bus.routers.schedules (Phase 1c).


# /v1/stats/* routes moved to hive.bus.routers.stats (Phase 1c).


# KNEMON router + OAuth-cap breaker moved to hive.bus.routers.knemon (Phase 1c).

# /v1/jobs/metrics moved to hive.bus.routers.jobs (Phase 1c).


# /v1/jobs/{job_id}/claim moved to hive.bus.routers.jobs (Phase 1c).


# /v1/jobs (list) + /v1/jobs/{id} (get) moved to hive.bus.routers.jobs (Phase 1c).


# /v1/messages routes moved to hive.bus.routers.messages (Phase 1c).


# /v1/events + /v1/events/log moved to hive.bus.routers.events (Phase 1c).


# POST /mcp shim moved to hive.bus.routers.mcp (Phase 1c).

# ===== zerocode-gate: pre-submission gate preview + release (patch) =====
# /v1/jobs/{id}/preview moved to hive.bus.routers.jobs (Phase 1c).


# /v1/jobs/{job_id}/release moved to hive.bus.routers.jobs (Phase 1c).


# ===== end zerocode-gate patch =====


# ===== zerocode-gate static mount =====
try:
    import os as _zcos

    from fastapi.staticfiles import StaticFiles as _ZCStatic

    _zcdir = "/srv/agent-bus/static"
    _zcos.makedirs(_zcdir, exist_ok=True)
    app.mount("/static", _ZCStatic(directory=_zcdir), name="zcstatic")
except Exception as _zce:
    print("zcgate static mount failed:", _zce, flush=True)
# ===== end zerocode-gate static mount =====
