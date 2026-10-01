"""Deterministic concurrency tests for the GRAEAE Hive Mind scheduler.

Pure-stdlib (no pytest, no external deps) — same shape as ``test_queue_logic.py``.

Coverage matrix (per task 2026-10-01):
  * role priority vs utilization (idle build worker beats busy build worker for
    ordinary work; idle general worker beats idle build worker for ordinary work)
  * build-required exceptions (``build:`` kinds + ``hard:build-*`` capabilities
    prefer build-role workers; non-build workers still eligible but never
    picked when a build-role worker is available)
  * Minos/Forge/TYPHON/TYDEUS — explicit fleet names used as canonical
    worker urns in the test corpus so the regression covers the four canonical
    workers named in the rollout directive
  * mixed local/EIH/MiniMax jobs — local = any worker (general pool),
    EIH = NGC Enterprise Inference Hub = ngc-proxy provider chain,
    MiniMax = MiniMax-M3 capacity pool; verifies the MiniMax pool rejects
    jobs beyond the 5-slot cap while letting other routes through unaffected
  * retries — a job with retry_count > 0 retains its role/build requirement
    and routes through the same scoring; a failed job's role requirement is
    not "demoted" on retry
  * lease expiry — a MiniMax lease whose holder's claim_lease_expires_at has
    passed does NOT count against the global pool cap (i.e. a stale lease
    does not block new assignments)
  * starvation protection — a worker whose ``last_served_at`` is older than
    ``STARVATION_AGE_SECONDS`` (or None) wins over an equally-eligible
    fresh worker at the same priority
  * five-slot saturation — the 6th MiniMax claim is rejected; the 5th
    succeeds; releasing one of the 5 unblocks the next claim

The test uses ONLY the pure API (``hive.scheduler.*``) — no DB, no FastAPI,
no FastAPI imports. The agent_bus integration is exercised by the
dequeue-flow wiring test in ``test_defect_fixes.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCHED_PATH = ROOT / "hive" / "scheduler.py"
spec = importlib.util.spec_from_file_location("hive_scheduler", SCHED_PATH)
sched = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = sched
spec.loader.exec_module(sched)

# ── Tiny harness ──────────────────────────────────────────────────────────
n = 0


def check(cond: bool, label: str) -> None:
    global n
    assert cond, f"FAIL: {label}"
    n += 1


def make_job(
    *,
    jid: str,
    kind: str = "feat:edit",
    priority: int = 0,
    caps: list[str] | None = None,
    submitter: str = "urn:submitter",
) -> dict:
    return {
        "id": jid,
        "kind": kind,
        "priority": priority,
        "submitter_urn": submitter,
        "required_capabilities": caps or [],
        "eligible_kinds": [],
        "eligible_hosts": [],
        "max_cost_tier": "B",
    }


def make_worker(
    *,
    urn: str,
    host: str,
    role: str | None = None,
    util: float = 0.0,
    last_served_at: float | None = None,
    deficit: float = 0.0,
    held: str | None = None,
) -> sched.WorkerProfile:
    caps: list[str] = []
    if role:
        caps.append(f"role:{role}")
    return sched.worker_profile_from_metadata(
        urn=urn,
        host=host,
        capabilities=caps,
        current_load={
            "utilization": util,
            "last_served_at": last_served_at,
            "deficit": deficit,
        },
        active_minimax_lease=held,
    )


NOW = 1_700_000_000.0


# ── Role derivation ───────────────────────────────────────────────────────
check(
    sched.derive_worker_role([], "proteus") == sched.ROLE_BUILD,
    "conventional build host -> build role",
)
check(
    sched.derive_worker_role([], "hydra") == sched.ROLE_BUILD,
    "conventional build host hydra -> build role",
)
check(
    sched.derive_worker_role([], "achilles") == sched.ROLE_BUILD,
    "conventional build host achilles -> build role",
)
check(
    sched.derive_worker_role([], "argos") == sched.ROLE_BUILD,
    "conventional build host argos -> build role",
)
check(
    sched.derive_worker_role([], "ultra") == sched.ROLE_BUILD,
    "conventional build host ultra -> build role",
)
check(
    sched.derive_worker_role([], "typhon") == sched.ROLE_GENERAL,
    "typhon (not in build set) -> general default",
)
check(
    sched.derive_worker_role([], "minos") == sched.ROLE_GENERAL,
    "minos -> general default",
)
check(
    sched.derive_worker_role([], "forge") == sched.ROLE_GENERAL,
    "forge -> general default",
)
check(
    sched.derive_worker_role(["role:general"], "proteus") == sched.ROLE_GENERAL,
    "explicit role capability overrides host hint",
)
check(
    sched.derive_worker_role(["role:build"], "studio") == sched.ROLE_BUILD,
    "explicit build role on non-build host is honored",
)
check(
    sched.derive_worker_role(["role:data"], "typhon") == sched.ROLE_DATA,
    "data role is preserved from capability metadata",
)
check(
    sched.derive_worker_role(["role:review"], "typhon") == sched.ROLE_REVIEW,
    "review role is preserved from capability metadata",
)
check(
    sched.derive_worker_role(["role:unknown-thing"], "proteus") == sched.ROLE_BUILD,
    "unknown role label falls back to host-derived default",
)


# ── job_requires_build ────────────────────────────────────────────────────
check(
    sched.job_requires_build({"kind": "build:compile"}) is True,
    "build: prefix kind is build-required",
)
check(
    sched.job_requires_build({"kind": "BUILD:Compile"}) is True,
    "build: prefix is case-insensitive",
)
check(
    sched.job_requires_build({"kind": "build-rust"}) is False,
    "build-rust kind (no colon) is NOT build-required",
)
check(
    sched.job_requires_build(
        {"kind": "feat:edit", "required_capabilities": ["hard:build-rust"]}
    )
    is True,
    "hard:build-rust capability is build-required",
)
check(
    sched.job_requires_build(
        {"kind": "feat:edit", "required_capabilities": ["hard:build"]}
    )
    is True,
    "bare hard:build capability is build-required",
)
check(
    sched.job_requires_build(
        {"kind": "feat:edit", "required_capabilities": ["soft:build"]}
    )
    is False,
    "soft:build is NOT build-required (only hard:)",
)
check(
    sched.job_requires_build(
        {"kind": "review:audit", "required_capabilities": ["hard:gpu"]}
    )
    is False,
    "review without build cap is not build-required",
)


# ── Role priority vs utilization ──────────────────────────────────────────
# Two build-role workers, one idle, one busy. For an ordinary (non-build)
# job, the idle general worker should still beat the idle build worker (role
# match) but a busy build worker is worse than an idle build worker (util).
typhon_busy_general = make_worker(
    urn="urn:agent:zeroclaw:typhon:s1", host="typhon", role=None, util=0.9
)
minos_idle = make_worker(
    urn="urn:agent:zeroclaw:minos:s1", host="minos", role=None, util=0.0
)
hydra_idle = make_worker(
    urn="urn:agent:zeroclaw:hydra:s1", host="hydra", role=None, util=0.0
)
hydra_busy = make_worker(
    urn="urn:agent:zeroclaw:hydra:s-busy", host="hydra", role=None, util=0.9
)
job_ordinary = make_job(jid="job-1", kind="feat:edit", priority=10)

# Tie-break by worker: lower utilization wins; among equals, role match wins.
scored_typhon = sched.score_candidate(
    job=job_ordinary, worker=typhon_busy_general, now=NOW
)
scored_minos = sched.score_candidate(job=job_ordinary, worker=minos_idle, now=NOW)
scored_hydra = sched.score_candidate(job=job_ordinary, worker=hydra_idle, now=NOW)
scored_hydra_busy = sched.score_candidate(job=job_ordinary, worker=hydra_busy, now=NOW)
check(
    scored_minos.score < scored_hydra.score,
    "idle non-build worker beats idle build worker for ordinary work",
)
check(
    scored_hydra_busy.score > scored_hydra.score,
    "idle build worker beats busy build worker (utilization dominates role)",
)
check(
    scored_typhon.score < scored_hydra.score,
    "role match beats utilization: a busy general worker still beats "
    "an idle build worker for ordinary work (per task: 'prefer non-build "
    "hosts when they are eligible and available')",
)

# Among non-build roles for a non-build job, general is the canonical pick;
# review/data workers have a small +1 penalty so general workers win ties.
typhon_general_idle = make_worker(
    urn="urn:agent:zeroclaw:typhon:s-gen", host="typhon", role="general", util=0.0
)
typhon_review_idle = make_worker(
    urn="urn:agent:zeroclaw:typhon:s-rev", host="typhon", role="review", util=0.0
)
typhon_data_idle = make_worker(
    urn="urn:agent:zeroclaw:typhon:s-data", host="typhon", role="data", util=0.0
)
scored_general = sched.score_candidate(job=job_ordinary, worker=typhon_general_idle, now=NOW)
scored_review = sched.score_candidate(job=job_ordinary, worker=typhon_review_idle, now=NOW)
scored_data = sched.score_candidate(job=job_ordinary, worker=typhon_data_idle, now=NOW)
check(
    scored_general.score < scored_review.score,
    "for ordinary work, general role beats review role (general is the canonical non-build pick)",
)
check(
    scored_general.score < scored_data.score,
    "for ordinary work, general role beats data role",
)
# review and data share the same role_penalty for non-build work; the
# tie-break that decides among them is the worker urn (deterministic,
# lexicographic), NOT the role. Verify the penalty slot is identical.
check(
    scored_general.score[1] == 0 and scored_data.score[1] == 1 and scored_review.score[1] == 1,
    "review and data roles share the same +1 non-build penalty; general is 0",
)

# Utilization dominance — same role, different utilization.
hydra_60 = make_worker(
    urn="urn:agent:zeroclaw:hydra:s2", host="hydra", role="general", util=0.6
)
hydra_20 = make_worker(
    urn="urn:agent:zeroclaw:hydra:s3", host="hydra", role="general", util=0.2
)
scored_60 = sched.score_candidate(job=job_ordinary, worker=hydra_60, now=NOW)
scored_20 = sched.score_candidate(job=job_ordinary, worker=hydra_20, now=NOW)
check(
    scored_20.score < scored_60.score,
    "lower utilization worker is preferred when roles are equal",
)


# ── Build-required exceptions ─────────────────────────────────────────────
job_build = make_job(
    jid="job-build", kind="build:compile", priority=10, caps=["hard:build-rust"]
)
job_build_cap = make_job(
    jid="job-build-cap", kind="feat:edit", priority=10, caps=["hard:build"]
)
scored_minos_build = sched.score_candidate(job=job_build, worker=minos_idle, now=NOW)
scored_hydra_build = sched.score_candidate(job=job_build, worker=hydra_idle, now=NOW)
check(
    scored_hydra_build.score < scored_minos_build.score,
    "build worker is preferred for build: kind",
)
scored_minos_buildcap = sched.score_candidate(
    job=job_build_cap, worker=minos_idle, now=NOW
)
scored_hydra_buildcap = sched.score_candidate(
    job=job_build_cap, worker=hydra_idle, now=NOW
)
check(
    scored_hydra_buildcap.score < scored_minos_buildcap.score,
    "build worker is preferred for hard:build capability",
)

# Among non-build roles for a build-required job, build is preferred; review
# and data are penalized MORE than general (we don't want data workers running
# build jobs). Verify the role-penalty slot is monotonically:
#   build(0) < review(2) < data(3) < general(4)
review_worker = make_worker(
    urn="urn:agent:zeroclaw:review:s1", host="review-host", role="review", util=0.0
)
data_worker = make_worker(
    urn="urn:agent:zeroclaw:data:s1", host="data-host", role="data", util=0.0
)
build_worker = make_worker(
    urn="urn:agent:zeroclaw:hydra:s-build", host="hydra", role="build", util=0.0
)
general_worker = make_worker(
    urn="urn:agent:zeroclaw:typhon:s-gen", host="typhon", role="general", util=0.0
)
scored_build_on_build = sched.score_candidate(job=job_build, worker=build_worker, now=NOW)
scored_review_on_build = sched.score_candidate(job=job_build, worker=review_worker, now=NOW)
scored_data_on_build = sched.score_candidate(job=job_build, worker=data_worker, now=NOW)
scored_general_on_build = sched.score_candidate(job=job_build, worker=general_worker, now=NOW)
check(
    scored_build_on_build.score[1] == 0
    and scored_review_on_build.score[1] == 2
    and scored_data_on_build.score[1] == 3
    and scored_general_on_build.score[1] == 4,
    "build-required role-penalty ordering: build=0 < review=2 < data=3 < general=4",
)

# Even though build workers are preferred, general workers MUST remain
# eligible (not starved) when no build worker is registered.
candidates = [job_build]
ordered = sched.order_candidates_for_worker(
    candidates=candidates,
    worker=minos_idle,
    now=NOW,
)
check(
    ordered and ordered[0]["id"] == "job-build",
    "general worker still sees build: jobs (eligibility preserved)",
)

# When both kinds of workers exist, build workers win even when busy (their
# role match outranks a slightly-busier non-build worker's utilization).
hydra_busy_50 = make_worker(
    urn="urn:agent:zeroclaw:hydra:s4", host="hydra", role=None, util=0.5
)
minos_busy_10 = make_worker(
    urn="urn:agent:zeroclaw:minos:s2", host="minos", role=None, util=0.1
)
scored_hydra_50 = sched.score_candidate(job=job_build, worker=hydra_busy_50, now=NOW)
scored_minos_10 = sched.score_candidate(job=job_build, worker=minos_busy_10, now=NOW)
check(
    scored_hydra_50.score < scored_minos_10.score,
    "build worker at 0.5 util beats general worker at 0.1 util for build: job",
)


# ── Fleet workers (Minos/Forge/TYPHON/TYDEUS) ─────────────────────────────
# Per task: cover Minos/Forge/TYPHON/TYDEUS. These are registered explicitly
# so the scheduler sees them by their canonical names.
tydeus_idle = make_worker(
    urn="urn:agent:zeroclaw:tydeus:s1", host="tydeus", role=None, util=0.0
)
typhon_idle = make_worker(
    urn="urn:agent:zeroclaw:typhon:s2", host="typhon", role=None, util=0.0
)
forge_idle = make_worker(
    urn="urn:agent:codex:forge:s1", host="forge", role=None, util=0.0
)
minos_idle_alt = make_worker(
    urn="urn:agent:claude:minos:s1", host="minos", role=None, util=0.0
)

ordinary_job = make_job(jid="job-ord", kind="review:audit", priority=5)
ordered = sched.order_candidates_for_worker(
    candidates=[ordinary_job],
    worker=tydeus_idle,
    now=NOW,
)
check(ordered[0]["id"] == "job-ord", "tydeus sees ordinary review jobs")

# Among equally-eligible candidates at the same priority, tie-break is
# deterministic (job id lexicographic).
a = make_job(jid="job-a", kind="review:audit", priority=5)
b = make_job(jid="job-b", kind="review:audit", priority=5)
c = make_job(jid="job-c", kind="review:audit", priority=5)
ordered_3 = sched.order_candidates_for_worker(
    candidates=[b, a, c],
    worker=forge_idle,
    now=NOW,
)
check(
    [j["id"] for j in ordered_3] == ["job-a", "job-b", "job-c"],
    "deterministic tie-break sorts equal candidates by id lexicographically",
)

# Different priorities: priority dominates.
p_high = make_job(jid="job-p-high", kind="review:audit", priority=10)
p_low = make_job(jid="job-p-low", kind="review:audit", priority=1)
ordered_p = sched.order_candidates_for_worker(
    candidates=[p_low, p_high],
    worker=minos_idle_alt,
    now=NOW,
)
check(
    ordered_p[0]["id"] == "job-p-high" and ordered_p[1]["id"] == "job-p-low",
    "higher-priority job wins over lower-priority for the same worker",
)


# ── Mixed local / EIH / MiniMax job dispatch ──────────────────────────────
# "local" = local-llamacpp provider chain (general worker, no special pool).
# "EIH"   = ngc-proxy (NGC Enterprise Inference Hub) provider chain —
#            also general worker (no special pool).
# "MiniMax" = MiniMax-M3 capacity pool — this IS the global pool, not a
#             per-worker route. The dispatch decision for a MiniMax job is
#             governed by ``MiniMaxPoolState.can_assign``.
job_local = make_job(jid="job-local", kind="feat:edit")
job_eih = make_job(jid="job-eih", kind="review:audit")
job_minimax = make_job(jid="job-minimax", kind="feat:edit")

# A general worker with no MiniMax lease is eligible for any of the three.
typhon_general = make_worker(
    urn="urn:agent:zeroclaw:typhon:s3", host="typhon", role=None, util=0.1
)
for j in (job_local, job_eih, job_minimax):
    check(
        sched.score_candidate(job=j, worker=typhon_general, now=NOW).score
        == sched.score_candidate(job=j, worker=typhon_general, now=NOW).score,
        "score is pure (no randomness / no IO)",
    )

# A worker holding a MiniMax lease is still eligible for non-MiniMax jobs.
typhon_holding = make_worker(
    urn="urn:agent:zeroclaw:typhon:s3",
    host="typhon",
    role=None,
    util=0.1,
    held="job-minimax-running",
)
local_score_free = sched.score_candidate(
    job=job_local,
    worker=typhon_general,
    now=NOW,
).score
local_score_held = sched.score_candidate(
    job=job_local,
    worker=typhon_holding,
    now=NOW,
).score
# Strip the worker-urn tie-break field (last element) so we compare only
# the scheduling-relevant fields (priority/role/util/deficit/age/job_id/
# submitter). The lease must not influence the score for non-MiniMax work.
check(
    local_score_free[:-1] == local_score_held[:-1],
    "MiniMax lease holder is still scored-eligible for non-MiniMax jobs",
)


# ── MiniMax-M3 capacity pool ──────────────────────────────────────────────
# Build a pool state with 4 active assignments (one held by typhon). 5th
# assignment should succeed; 6th should fail.
state = sched.MiniMaxPoolState()
for i in range(4):
    state = state.assign(f"urn:agent:zeroclaw:w{i}:s1", f"job-mx-{i}")
check(state.active_assignments == 4, "4 active assignments counted in the pool")
ok, reason = state.can_assign("urn:agent:zeroclaw:typhon:s5")
check(ok, "5th MiniMax assignment is allowed (within global cap)")
state = state.assign("urn:agent:zeroclaw:typhon:s5", "job-mx-4")
ok, reason = state.can_assign("urn:agent:zeroclaw:tydeus:s1")
check(
    not ok and "global cap" in reason,
    "6th MiniMax assignment is rejected with global-cap reason",
)
check(state.active_assignments == 5, "pool is saturated at 5/5 (no overflow)")

# Per-worker cap: the same worker trying to claim a second MiniMax job is
# rejected, regardless of global headroom. The constant MINIMAX_PER_WORKER_MAX
# is the source of truth — the reason mentions per-worker cap so any future
# bump to that constant is reflected here too. Round-4 hardening: the
# assign() guard is self-checking, so the second call raises ValueError
# rather than silently inflating the counter.
try:
    state.assign("urn:agent:zeroclaw:typhon:s5", "job-mx-dup")
    check(False, "MiniMaxPoolState.assign MUST raise on a second lease by the same worker")
except ValueError as exc:
    check(
        "per-worker cap" in str(exc),
        f"MiniMaxPoolState.assign raises on the per-worker cap; got {exc!r}",
    )
ok, reason = state.can_assign("urn:agent:zeroclaw:typhon:s5")
check(
    not ok and "per-worker cap" in reason,
    f"per-worker MiniMax cap (={sched.MINIMAX_PER_WORKER_MAX}) is enforced; got reason={reason!r}",
)

# Release unblocks a slot AND frees that worker.
released = state.release("urn:agent:zeroclaw:typhon:s5")
check(
    released.active_assignments == 4,
    "releasing a MiniMax lease decrements the global count",
)
ok, reason = released.can_assign("urn:agent:zeroclaw:tydeus:s1")
check(ok, "after release, the next assignment is allowed")
ok, reason = released.can_assign("urn:agent:zeroclaw:typhon:s5")
check(ok, "the worker whose lease was released can take a new MiniMax job")

# Releasing a worker with no lease is a no-op (idempotent).
state2 = sched.MiniMaxPoolState()
state3 = state2.release("urn:agent:zeroclaw:nobody:s1")
check(state3 is state2, "releasing a non-holder is a no-op (same state)")


# ── Five-slot saturation (explicit) ───────────────────────────────────────
sat = sched.MiniMaxPoolState()
for i in range(sched.MINIMAX_GLOBAL_MAX):
    sat, _reason = (sat.assign(f"urn:w{i}", f"j-{i}"), None)
ok5, _ = sat.can_assign("urn:w-new")
check(not ok5, "with 5 active assignments, the next assignment is rejected")


# ── build_pool_snapshot: from a list of (worker_urn, job_id) pairs ──────
# The dequeue flow feeds raw SQL rows through build_pool_snapshot to get a
# MiniMaxPoolState. Verify the snapshot handles empty input, de-duplication,
# and normalisation correctly.
snap_empty = sched.build_pool_snapshot([])
check(
    snap_empty.active_assignments == 0 and not snap_empty.leases_by_worker,
    "build_pool_snapshot([]) yields an empty pool",
)
snap_two = sched.build_pool_snapshot([
    ("urn:agent:zeroclaw:typhon:s1", "job-mx-0"),
    ("urn:agent:zeroclaw:forge:s2", "job-mx-1"),
])
check(
    snap_two.active_assignments == 2
    and snap_two.leases_by_worker == {
        "urn:agent:zeroclaw:typhon:s1": "job-mx-0",
        "urn:agent:zeroclaw:forge:s2": "job-mx-1",
    },
    "build_pool_snapshot builds leases dict keyed by worker urn",
)
# De-duplicate by worker urn (last entry wins; same SQL row counts once).
snap_dup = sched.build_pool_snapshot([
    ("urn:agent:zeroclaw:w0:s1", "job-mx-old"),
    ("urn:agent:zeroclaw:w0:s1", "job-mx-new"),
])
check(
    snap_dup.active_assignments == 1
    and snap_dup.leases_by_worker["urn:agent:zeroclaw:w0:s1"] == "job-mx-new",
    "build_pool_snapshot de-duplicates by worker urn (last entry wins)",
)
# Drop empty urns / empty job ids (defensive against malformed SQL).
snap_empty_fields = sched.build_pool_snapshot([
    ("", "job-x"),
    ("urn:w1", ""),
    ("urn:w2", "job-y"),
])
check(
    snap_empty_fields.active_assignments == 1
    and snap_empty_fields.leases_by_worker == {"urn:w2": "job-y"},
    "build_pool_snapshot drops rows with empty urn or empty job id",
)


# ── Retry: failed jobs retain their role/build semantics ──────────────────
# A retry does NOT demote the job's role requirement. We verify by feeding
# the same job dict (with retry_count > 0) into the scorer and comparing
# scores: identical inputs produce identical outputs.
job_retry = make_job(
    jid="job-retry", kind="build:compile", caps=["hard:build-rust"], priority=10
)
job_retry["retry_count"] = 3
job_retry["max_retries"] = 5
job_retry["status"] = "queued"
score_retry = sched.score_candidate(
    job=job_retry,
    worker=hydra_idle,
    now=NOW,
).score
score_fresh = sched.score_candidate(
    job=make_job(
        jid="job-fresh", kind="build:compile", caps=["hard:build-rust"], priority=10
    ),
    worker=hydra_idle,
    now=NOW,
).score
check(
    score_retry[:2] == score_fresh[:2],
    "retried build job keeps its build-role preference (-priority + role-penalty)",
)
# The job id differs so the tie-break differs; that's expected.

# A job retried for a non-build kind does not suddenly become build-required.
job_retry_nb = make_job(jid="job-retry-nb", kind="feat:edit", priority=10)
job_retry_nb["retry_count"] = 2
check(
    sched.job_requires_build(job_retry_nb) is False,
    "non-build retry does not silently become build-required",
)


# ── Lease expiry: stale MiniMax leases do not block new assignments ───────
# When a worker's claim_lease_expires_at has passed, the bus considers the
# lease dead and releases it from the pool. We model that here: caller
# passes only ACTIVE leases to ``MiniMaxPoolState`` (the dequeue flow does
# the filtering — ``WHERE claim_lease_expires_at IS NULL OR > now``).
stale_state = sched.MiniMaxPoolState(
    active_assignments=5,
    leases_by_worker={
        "urn:stale1": "job-stale-1",
        "urn:stale2": "job-stale-2",
        "urn:stale3": "job-stale-3",
        "urn:stale4": "job-stale-4",
        "urn:stale5": "job-stale-5",
    },
)
ok, reason = stale_state.can_assign("urn:new")
check(not ok, "with 5 active assignments, a new assignment is rejected")
# Now the caller passes only the 3 still-valid leases (the other 2 expired).
trimmed = sched.MiniMaxPoolState(
    active_assignments=3,
    leases_by_worker={
        "urn:stale1": "job-stale-1",
        "urn:stale2": "job-stale-2",
        "urn:stale3": "job-stale-3",
    },
)
ok, reason = trimmed.can_assign("urn:new")
check(ok, "after lease expiry, trimmed pool count unblocks new assignment")


# ── Starvation protection ─────────────────────────────────────────────────
starved_worker = make_worker(
    urn="urn:agent:zeroclaw:forge:s9",
    host="forge",
    role=None,
    util=0.0,
    last_served_at=NOW - (sched.STARVATION_AGE_SECONDS + 1),
    deficit=0.0,
)
fresh_worker = make_worker(
    urn="urn:agent:zeroclaw:forge:s10",
    host="forge",
    role=None,
    util=0.0,
    last_served_at=NOW,  # just served
    deficit=0.0,
)
# Two equal-priority jobs; the starved worker should pick differently
# because the scheduler boosts its deficit. We test via select_best_job:
jobs_equal = [
    make_job(jid="job-x", kind="feat:edit", priority=5),
    make_job(jid="job-y", kind="feat:edit", priority=5),
]
# Both workers see the same candidate set; the starved one gets the same
# lexicographic pick (deterministic tie-break), but the "boosted" form wins
# the deficit-tie so its pick is identical — starvation is structural: an
# idle worker is preferred for the OLDEST idle interval, not for arbitrary
# boosts.
starved_pick = sched.select_best_job(
    candidates=jobs_equal,
    worker=starved_worker,
    now=NOW,
)
fresh_pick = sched.select_best_job(
    candidates=jobs_equal,
    worker=fresh_worker,
    now=NOW,
)
# Tie-break is lexicographic, so both pick job-x at equal priority; the
# protection mechanism operates when there IS a difference. Verify the
# starved path is reachable via direct deficit comparison:
scored_starved = sched.score_candidate(
    job=jobs_equal[0], worker=starved_worker, now=NOW
)
scored_fresh = sched.score_candidate(job=jobs_equal[0], worker=fresh_worker, now=NOW)
check(
    scored_starved.score < scored_fresh.score,
    "starved worker (older last_served_at) wins over fresh worker at the same priority",
)
check(
    starved_pick == fresh_pick,
    "with equal candidates, tie-break is stable across starved/fresh",
)

# A worker that has never served (last_served_at=None) is also boosted.
never_served = make_worker(
    urn="urn:agent:zeroclaw:forge:s11",
    host="forge",
    role=None,
    util=0.0,
    last_served_at=None,
    deficit=0.0,
)
check(
    sched.is_starved(never_served, now=NOW),
    "worker with no last_served_at is treated as starved",
)

# A freshly-served worker is NOT starved.
check(
    not sched.is_starved(fresh_worker, now=NOW),
    "worker that just served is NOT starved",
)


# ── Determinism: same input → same output across re-invocations ──────────
for trial in range(3):
    w = make_worker(
        urn="urn:agent:zeroclaw:minos:sX",
        host="minos",
        role=None,
        util=0.3,
        last_served_at=NOW - 100,
    )
    j = make_job(
        jid="job-det", kind="build:compile", caps=["hard:build-rust"], priority=7
    )
    s1 = sched.score_candidate(job=j, worker=w, now=NOW).score
    s2 = sched.score_candidate(job=j, worker=w, now=NOW).score
    check(s1 == s2, f"score is deterministic (trial {trial})")


# ── Role capability precedence over conventional host ────────────────────
# A worker on a conventional build host that explicitly declares a different
# role is treated by THAT role. We cover all four VALID_ROLES to lock it in.
for declared in sched.VALID_ROLES:
    w = sched.worker_profile_from_metadata(
        urn=f"urn:test:{declared}:s1",
        host="proteus",
        capabilities=[f"role:{declared}"],
        current_load={},
    )
    check(
        w.role == declared,
        f"role:{declared} capability wins over proteus build-host hint",
    )


# ── Defensive: malformed current_load values are clamped, not raised ──────
# A misreported worker (e.g. NaN, infinity, out-of-range float, non-numeric)
# must NOT crash the scheduler. The bus trusts the worker to send JSON, but
# a buggy client should never wedge the claim loop.
malformed_cases = [
    {"utilization": float("nan")},          # NaN -> default 0
    {"utilization": float("inf")},          # inf -> clamped to 1
    {"utilization": -0.5},                  # negative -> clamped to 0
    {"utilization": 7.3},                   # over-range -> clamped to 1
    {"utilization": "0.42"},                # numeric string -> parsed
    {"utilization": "bogus"},               # non-numeric string -> default 0
    {"utilization": None},                  # None -> default 0
    {"deficit": float("nan")},              # NaN deficit -> default 0
    {"deficit": "huge"},                    # non-numeric -> default 0
    {"last_served_at": "not-a-timestamp"},  # malformed last_served_at -> None
    {"last_served_at": None},               # None last_served_at -> None
]
for i, load in enumerate(malformed_cases):
    w = sched.worker_profile_from_metadata(
        urn=f"urn:test:malformed:{i}",
        host="typhon",
        capabilities=[],
        current_load=load,
    )
    # Every profile must come back with utilization clamped to [0,1] and
    # the never-crash invariant: it must be a finite float.
    check(
        isinstance(w.utilization, float) and 0.0 <= w.utilization <= 1.0,
        f"malformed load {load!r} yields a sane utilization (got {w.utilization!r})",
    )
    check(
        isinstance(w.deficit, float),
        f"malformed load {load!r} yields a finite deficit (got {w.deficit!r})",
    )
    check(
        w.last_served_at is None or isinstance(w.last_served_at, float),
        f"malformed load {load!r} yields a sane last_served_at (got {w.last_served_at!r})",
    )

# worker_profile_from_metadata with a flat float (legacy current_load) works.
flat_low = sched.worker_profile_from_metadata(
    urn="urn:flat:low", host="typhon", capabilities=[], current_load=0.25
)
flat_high = sched.worker_profile_from_metadata(
    urn="urn:flat:high", host="typhon", capabilities=[], current_load=0.75
)
check(
    flat_low.utilization == 0.25 and flat_high.utilization == 0.75,
    "flat-float current_load is honoured for legacy heartbeat payloads",
)
# Flat-float NaN -> defaults to 0 (defensive).
flat_nan = sched.worker_profile_from_metadata(
    urn="urn:flat:nan", host="typhon", capabilities=[], current_load=float("nan")
)
check(
    flat_nan.utilization == 0.0,
    "flat-float NaN utilization defaults to 0 (never wedges the claim loop)",
)


# ── MiniMax-M3 pool: explicit per-worker cap is consulted, not implied ─────
# The constant MINIMAX_PER_WORKER_MAX is the source of truth for the cap. Bumping
# the constant relaxes the cap; the can_assign check uses the live value, not
# a hardcoded threshold. We verify by checking the reason string mentions
# the per-worker cap rather than just "already holds lease".
ps = sched.MiniMaxPoolState()
ps = ps.assign("urn:w1", "j1")
ok, reason = ps.can_assign("urn:w1")
check(
    not ok and f"per-worker cap={sched.MINIMAX_PER_WORKER_MAX}" in reason,
    f"per-worker cap reason mentions the live MINIMAX_PER_WORKER_MAX constant (got {reason!r})",
)

# After release, the same worker can take a fresh one (per-worker cap=1).
ps = ps.release("urn:w1")
ok, _ = ps.can_assign("urn:w1")
check(ok, "after release, per-worker cap permits a new MiniMax lease")


# ── build_pool_snapshot: input normalisation ────────
# Empty-string worker urns and empty-string job ids are dropped (defensive
# against malformed SQL projections). Case is normalised to lower; whitespace
# is stripped. Mixed-case duplicates collapse to last-write-wins.
snap_norm = sched.build_pool_snapshot([
    ("  URN:W1 ", "  JOB-1  "),
    ("URN:w1", "JOB-2"),
    ("urn:w2", "JOB-3"),
])
check(
    snap_norm.active_assignments == 2
    and snap_norm.leases_by_worker == {
        "urn:w1": "job-2",
        "urn:w2": "job-3",
    },
    "build_pool_snapshot lowercases+strips input fields and de-dupes by normalised worker urn",
)


# ── select_best_job: starved worker's deficit boost doesn't change pick order
# when jobs differ on priority. The starvation boost only affects candidates
# tied at (priority, role-penalty). Higher-priority jobs still win.
starved_high_prio = sched.select_best_job(
    candidates=[
        make_job(jid="job-low", kind="feat:edit", priority=1),
        make_job(jid="job-high", kind="feat:edit", priority=10),
    ],
    worker=make_worker(
        urn="urn:starved:boost",
        host="typhon",
        role=None,
        util=0.0,
        last_served_at=NOW - (sched.STARVATION_AGE_SECONDS + 60),
        deficit=0.0,
    ),
    now=NOW,
)
check(
    starved_high_prio["id"] == "job-high",
    "starvation boost does NOT override priority ordering",
)


# ── Job without an id still scores deterministically (job_id falls back to "")
# A malformed job without an id should not crash the scheduler; it just gets
# the empty-string tie-break (lexicographically first).
no_id_job = {
    "submitter_urn": "sub-x",
    "kind": "feat:edit",
    "priority": 5,
    "required_capabilities": [],
}
scored_no_id = sched.score_candidate(
    job=no_id_job, worker=make_worker(urn="urn:w", host="typhon"), now=NOW,
)
check(
    scored_no_id.score is not None and scored_no_id.job_id == "",
    "score_candidate tolerates a job without an id field",
)


# ── MiniMaxPoolState.assign is self-checking ──────────────────────────────
# Round-4 hardening: assign raises ValueError when the same worker urn
# already holds a non-None lease. This eliminates the entire bug class of
# "caller forgot to honour a can_assign deny" — the second assign is loud
# (raises) rather than silently overwriting the lease + inflating the
# counter. The previous implementation was a documented weakness (the
# caller contract could be forgotten); this is a hard guard.
s2 = sched.MiniMaxPoolState()
s2 = s2.assign("urn:w", "j1")
try:
    s2.assign("urn:w", "j1-again")
    check(False, "MiniMaxPoolState.assign MUST raise on double-assignment")
except ValueError as exc:
    check(
        "per-worker cap" in str(exc),
        f"MiniMaxPoolState.assign raises ValueError mentioning per-worker cap; got {exc!r}",
    )
# Empty urns raise too (defensive — caller passed a malformed urn).
try:
    sched.MiniMaxPoolState().assign("", "j1")
    check(False, "MiniMaxPoolState.assign MUST raise on empty worker urn")
except ValueError as exc:
    check(
        "empty worker urn" in str(exc),
        f"MiniMaxPoolState.assign raises ValueError on empty urn; got {exc!r}",
    )
# A new (urn, jid) on a fresh state: counter increments by exactly 1 and
# the leases_by_worker map holds exactly one entry for that urn.
s3 = sched.MiniMaxPoolState()
s3 = s3.assign("urn:w", "j1")
check(
    s3.active_assignments == 1 and s3.leases_by_worker == {"urn:w": "j1"},
    "MiniMaxPoolState.assign on a clean state increments counter by 1 and stores the lease",
)
# Release-then-reassign: a worker that previously held a lease (now released
# to None) can take a fresh one without raising — the dict slot for the urn
# is preserved (still keyed) so the new lease replaces the None. This is the
# common path: a worker finishes its MiniMax job, the lease is released,
# and the same worker takes a different MiniMax job.
s3_released = s3.release("urn:w")
s3_reassigned = s3_released.assign("urn:w", "j2")
check(
    s3_reassigned.active_assignments == 1
    and s3_reassigned.leases_by_worker == {"urn:w": "j2"},
    "release-then-reassign is the supported path (dict slot for the urn is reused, "
    "not raised)",
)


# ── score_candidate defensive role fallback ───────────────────────────────
# If a WorkerProfile is constructed directly with an unknown role (e.g.
# test fixture bug, or future caller forgets to use derive_worker_role), the
# score_candidate MUST NOT raise KeyError on the role-penalty lookup.
# The fallback treats an unknown role as ROLE_GENERAL (the canonical
# non-build fallback), preserving the dequeue loop's stability.
unknown_role_worker = sched.WorkerProfile(
    urn="urn:unknown-role",
    host="typhon",
    role="unknown-role-from-test",  # not in VALID_ROLES
    utilization=0.0,
    last_served_at=None,
    deficit=0.0,
    active_minimax_lease=None,
)
scored_unknown_role = sched.score_candidate(
    job=make_job(jid="job-ur", kind="feat:edit"),
    worker=unknown_role_worker,
    now=NOW,
)
# The unknown-role worker is treated as a general-role worker for both
# build-required and non-build work. Verify the role_penalty slot (index 1)
# matches ROLE_GENERAL's expected values.
general_worker_profile = sched.WorkerProfile(
    urn="urn:general",
    host="typhon",
    role=sched.ROLE_GENERAL,
    utilization=0.0,
    last_served_at=None,
    deficit=0.0,
    active_minimax_lease=None,
)
scored_general = sched.score_candidate(
    job=make_job(jid="job-ur-g", kind="feat:edit"),
    worker=general_worker_profile,
    now=NOW,
)
check(
    scored_unknown_role.score[1] == scored_general.score[1] == 0,
    "score_candidate falls back to ROLE_GENERAL penalty for an unknown role "
    "(no KeyError on a malformed WorkerProfile)",
)


# ── Determinism across worker instantiations: same inputs yield same outputs
# across fresh worker profile objects (regression guard against any hidden
# global state in the scheduler module).
for _ in range(3):
    w1 = sched.worker_profile_from_metadata(
        urn="urn:det",
        host="typhon",
        capabilities=[],
        current_load={"utilization": 0.4, "last_served_at": NOW - 5, "deficit": 1.5},
    )
    w2 = sched.worker_profile_from_metadata(
        urn="urn:det",
        host="typhon",
        capabilities=[],
        current_load={"utilization": 0.4, "last_served_at": NOW - 5, "deficit": 1.5},
    )
    check(
        sched.score_candidate(job=make_job(jid="det"), worker=w1, now=NOW).score
        == sched.score_candidate(job=make_job(jid="det"), worker=w2, now=NOW).score,
        "identical worker profiles yield identical scores (no hidden globals)",
    )


# ── MINIMAX_ACTIVE_STATUSES: assigned-vs-active coverage ───────────────
# The rollout directive (2026-10-01) defines the global cap as "5
# concurrent OR ASSIGNED MiniMax sessions". This means ``offered`` jobs
# (routed to a worker but not yet actively claimed) MUST count against
# the per-worker and global caps — they're reserved, not free. The set
# is the single source of truth used by both SQL queries in the dequeue
# flow (per-worker held-lease lookup + global pool snapshot).
check(
    set(sched.MINIMAX_ACTIVE_STATUSES) == {"offered", "claimed", "running"},
    f"MINIMAX_ACTIVE_STATUSES is the assigned-or-active set "
    f"({{'offered','claimed','running'}}); got {set(sched.MINIMAX_ACTIVE_STATUSES)!r}",
)


# ── can_assign honours per-worker cap when worker already has an offered lease
# Model the "worker is being polled for a 2nd MiniMax claim while it still
# holds a previously-offered MiniMax lease" scenario. The pool state only
# tracks currently-held leases; the dequeue flow feeds it rows that include
# both offered and claimed. A worker with one offered lease MUST be denied
# the second claim — the per-worker cap is exclusive on the MiniMax model.
state_offered = sched.MiniMaxPoolState(
    active_assignments=1,
    leases_by_worker={"urn:agent:zeroclaw:typhon:s-held": "job-mx-offered"},
)
ok, reason = state_offered.can_assign("urn:agent:zeroclaw:typhon:s-held")
check(
    not ok and "per-worker cap" in reason,
    f"can_assign denies a 2nd MiniMax claim when the worker already holds "
    f"an offered lease; got ok={ok} reason={reason!r}",
)


# ── build_pool_snapshot: offered rows count against the global cap ────
# The dequeue flow uses build_pool_snapshot to project SQL rows (status
# in (offered, claimed, running)) into a MiniMaxPoolState. Verify the
# snapshot counts offered rows: 5 offered rows saturate the global cap
# the same way 5 running rows do.
snap_offered = sched.build_pool_snapshot([
    (f"urn:agent:zeroclaw:w{i}:s1", f"job-offered-{i}") for i in range(5)
])
check(
    snap_offered.active_assignments == sched.MINIMAX_GLOBAL_MAX,
    f"5 offered rows saturate the global cap "
    f"(active={snap_offered.active_assignments}, "
    f"expected={sched.MINIMAX_GLOBAL_MAX})",
)
ok, reason = snap_offered.can_assign("urn:agent:zeroclaw:w-new:s1")
check(
    not ok and "global cap" in reason,
    f"6th claim against 5 offered leases is rejected by the global cap; "
    f"got ok={ok} reason={reason!r}",
)


# ── build_pool_snapshot: mixed offered + claimed = same cap math ─────
# The directive makes no distinction between offered and claimed for
# pool-cap purposes ("concurrent OR assigned"). A mix of 3 offered + 2
# claimed must also saturate the global cap (3+2=5, exactly at the limit).
snap_mixed = sched.build_pool_snapshot([
    ("urn:agent:zeroclaw:w0:s1", "job-offered-0"),
    ("urn:agent:zeroclaw:w1:s1", "job-offered-1"),
    ("urn:agent:zeroclaw:w2:s1", "job-offered-2"),
    ("urn:agent:zeroclaw:w3:s1", "job-claimed-3"),
    ("urn:agent:zeroclaw:w4:s1", "job-claimed-4"),
])
check(
    snap_mixed.active_assignments == 5
    and len(snap_mixed.leases_by_worker) == 5,
    f"mixed offered+claimed snapshot has 5 active assignments; "
    f"got active={snap_mixed.active_assignments}, "
    f"workers={len(snap_mixed.leases_by_worker)}",
)
ok, reason = snap_mixed.can_assign("urn:agent:zeroclaw:w-new:s1")
check(
    not ok and "global cap" in reason,
    f"6th claim against 3 offered + 2 claimed leases is rejected by the "
    f"global cap; got ok={ok} reason={reason!r}",
)


# ── release of an offered lease: same as release of a claimed lease ───
# The release path must work the same way regardless of which status the
# held lease was in — the pool only sees a worker urn + job id pair.
state_pre = sched.MiniMaxPoolState(
    active_assignments=2,
    leases_by_worker={
        "urn:agent:zeroclaw:typhon:s1": "job-offered",
        "urn:agent:zeroclaw:typhon:s2": "job-claimed",
    },
)
state_post = state_pre.release("urn:agent:zeroclaw:typhon:s1")
check(
    state_post.active_assignments == 1
    and state_post.leases_by_worker == {
        "urn:agent:zeroclaw:typhon:s1": None,
        "urn:agent:zeroclaw:typhon:s2": "job-claimed",
    },
    "releasing an offered lease decrements the global count and clears the worker's slot",
)
# And the same worker can immediately take a fresh lease.
ok, _ = state_post.can_assign("urn:agent:zeroclaw:typhon:s1")
check(
    ok, "after releasing the offered lease, the worker can take a new one"
)


print(f"ALL {n} SCHEDULER CHECKS PASSED")
