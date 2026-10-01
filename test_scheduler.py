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
# rejected, regardless of global headroom.
held_worker_state = state.assign("urn:agent:zeroclaw:typhon:s5", "job-mx-dup")
# (state is unchanged because can_assign would have failed; we test via the
# can_assign check directly.)
ok, reason = state.can_assign("urn:agent:zeroclaw:typhon:s5")
check(
    not ok and "already holds lease" in reason,
    "per-worker MiniMax cap is enforced (worker holds a lease)",
)
del held_worker_state  # keep test scope tidy

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


print(f"ALL {n} SCHEDULER CHECKS PASSED")
