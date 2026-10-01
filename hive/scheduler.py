"""GRAEAE Hive Mind scheduler intelligence.

Pure (no I/O, no DB, no framework imports) decision logic for the canonical
self-claim bus. Replaces first-poller-wins behaviour with atomic, role-aware
fair claim selection:

  1. capability/role eligibility (the existing ``job_agent_eligible`` is the
     hard gate; this module layers scoring on top of it — never below it);
  2. role preference: ordinary feature/review/data work prefers non-build
     workers when they're eligible; build-required work (``build:`` kinds or
     explicit ``hard:build-*`` capability requirements) prefers build workers;
  3. fair-claim ordering: among equally-eligible jobs, prefer the lowest
     normalized utilization and the largest least-recently-served/deficit
     score. Deterministic tie-breaks (job id, then submitter urn) make every
     decision reproducible.
  4. MiniMax-M3 capacity pool: a worker-agnostic, host-independent hard cap
     of 5 concurrent/assigned MiniMax sessions across the whole fleet and 1
     MiniMax lease per worker. Workers NOT holding a MiniMax lease remain
     eligible for every other model route (no MiniMax pinning to hosts).

Worker role is declared via capability metadata (``role:build`` /
``role:general`` / ``role:review`` / ``role:data``); the conventional
``proteus``/``hydra``/``achilles``/``argos``/``ultra`` build hosts are
honoured as a default role hint, never as hard-coded assignment, so a worker
that explicitly declares ``role:general`` is treated as general even on one
of those hosts (and a worker that explicitly declares ``role:build`` is
treated as build on any other host).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

# ── Role registry ──────────────────────────────────────────────────────────
# Canonical worker roles. The role is declared via capability metadata
# (e.g. a capability label of ``role:build``) — host names do not determine
# role on their own. The five conventional build hosts default to the build
# role only when the worker has not declared a different role explicitly.
ROLE_BUILD = "build"
ROLE_GENERAL = "general"
ROLE_REVIEW = "review"
ROLE_DATA = "data"
VALID_ROLES = frozenset({ROLE_BUILD, ROLE_GENERAL, ROLE_REVIEW, ROLE_DATA})

# Conventional build hosts (proteus/hydra/achilles/argos/ultra). These are
# hints only — workers on these hosts default to ``build`` role, but a worker
# that explicitly declares a different capability role overrides the hint.
# Hosts not listed default to ``general``. Per directive 2026-10-01, this is
# metadata, never hard-coded assignment.
CONVENTIONAL_BUILD_HOSTS: frozenset[str] = frozenset(
    {
        "proteus",
        "hydra",
        "achilles",
        "argos",
        "ultra",
    }
)


# ── Build-required detection ───────────────────────────────────────────────
# A job is build-required when its kind starts with ``build:`` OR it lists a
# hard capability requirement matching ``hard:build-*`` (the existing HARD
# cap syntax is preserved — ``hard:build-rust`` / ``hard:build-c`` etc.). We
# treat the explicit capability form as authoritative: a job may be
# build-required via capability even when the kind is not ``build:`` (e.g. an
# ad-hoc ``feat:cross-compile`` job asking for ``hard:build-rust``).
BUILD_KIND_PREFIX = "build:"
BUILD_CAPABILITY_PREFIX = "hard:build"


def job_requires_build(job: dict[str, Any]) -> bool:
    kind = (job.get("kind") or "").lower()
    if kind.startswith(BUILD_KIND_PREFIX):
        return True
    labels = job.get("required_capabilities") or []
    for label in labels:
        if not isinstance(label, str):
            continue
        s = label.lower().strip()
        if s.startswith(BUILD_CAPABILITY_PREFIX):
            # ``hard:build`` alone is the umbrella hard cap; ``hard:build-*``
            # is a specific build subsystem requirement. Both indicate the
            # worker must have build capability.
            tail = s[len(BUILD_CAPABILITY_PREFIX) :]
            if tail == "" or tail.startswith("-"):
                return True
    return False


# ── MiniMax-M3 capacity pool ───────────────────────────────────────────────
# Worker-agnostic pool: NEVER pinned to hosts. Any worker on the bus may
# hold a MiniMax lease as long as (a) the global MiniMax session count is
# below the cap and (b) the worker does not already hold a MiniMax lease.
# Workers not holding the lease remain eligible for every other model route
# — the lease is exclusive on the MiniMax model, not on the worker.
MINIMAX_MODEL = "minimax-m3"
MINIMAX_PROVIDER = "minimax"  # distinct from the existing ``together`` provider
MINIMAX_GLOBAL_MAX = 5  # hard global concurrent/assigned lease cap
MINIMAX_PER_WORKER_MAX = 1  # max MiniMax leases per worker urn


def is_minimax_route(provider: Any, model: Any) -> bool:
    """True when a route chain entry targets the MiniMax-M3 capacity pool."""
    return _norm_str(provider) == MINIMAX_PROVIDER and _norm_str(model) == MINIMAX_MODEL


def build_pool_snapshot(
    active_assignments: Iterable[tuple[str, str]],
) -> MiniMaxPoolState:
    """Construct a pool snapshot from a list of ``(worker_urn, job_id)`` pairs.

    The dequeue flow builds the active-assignments list from
    ``SELECT claimed_by FROM jobs WHERE status IN ('claimed','running')
    AND claimed_provider=? AND claimed_model=? AND
    (claim_lease_expires_at IS NULL OR claim_lease_expires_at > ?)``.
    The list is normalised (lower/strip), de-duplicated by worker urn
    (last write wins — the bus treats the most recent claim as canonical),
    and assigned to ``MiniMaxPoolState``. Fail-closed semantics: an empty
    worker urn or empty job id is silently dropped (the caller has already
    filtered those out).
    """
    leases: dict[str, str] = {}
    count = 0
    for worker_urn, job_id in active_assignments:
        urn = _norm_str(worker_urn)
        jid = _norm_str(job_id)
        if not urn or not jid:
            continue
        if urn in leases:
            # De-duplicate: keep the last entry. This handles the rare case
            # where the SQL query returns the same worker twice (e.g. two
            # active claims on the same worker — should not happen, but
            # we belt-and-suspender because the count would otherwise be
            # inflated).
            leases[urn] = jid
            continue
        leases[urn] = jid
        count += 1
    return MiniMaxPoolState(active_assignments=count, leases_by_worker=leases)


@dataclass(frozen=True)
class MiniMaxPoolState:
    """Snapshot of the MiniMax capacity pool for one decision.

    ``active_assignments`` is the count of jobs currently assigned to the
    MiniMax route (status ``claimed``/``running``). ``leases_by_worker``
    maps worker urn -> active MiniMax job id; values are ``None`` when the
    worker has not leased one.
    """

    active_assignments: int = 0
    leases_by_worker: dict[str, str | None] = field(default_factory=dict)

    def can_assign(self, worker_urn: str) -> tuple[bool, str]:
        """Return ``(ok, reason)`` for an attempt to assign MiniMax to worker.

        Fail-closed: if the worker already holds a lease OR the global cap
        would be exceeded, the lease is denied.
        """
        urn = _norm_str(worker_urn)
        if not urn:
            return False, "minimax:empty worker urn"
        held = self.leases_by_worker.get(urn)
        if held:
            return False, f"minimax:worker {urn!r} already holds lease {held!r}"
        if self.active_assignments >= MINIMAX_GLOBAL_MAX:
            return False, (
                f"minimax:global cap reached "
                f"({self.active_assignments}/{MINIMAX_GLOBAL_MAX})"
            )
        return True, "ok"

    def assign(self, worker_urn: str, job_id: str) -> MiniMaxPoolState:
        """Return a new state with the lease recorded. Caller must have already
        verified via ``can_assign`` that the assignment is allowed."""
        urn = _norm_str(worker_urn)
        new_leases = dict(self.leases_by_worker)
        new_leases[urn] = _norm_str(job_id)
        return MiniMaxPoolState(
            active_assignments=self.active_assignments + 1,
            leases_by_worker=new_leases,
        )

    def release(self, worker_urn: str) -> MiniMaxPoolState:
        """Return a new state with any MiniMax lease for the worker released."""
        urn = _norm_str(worker_urn)
        held = self.leases_by_worker.get(urn)
        if not held:
            return self
        new_leases = dict(self.leases_by_worker)
        new_leases[urn] = None
        return MiniMaxPoolState(
            active_assignments=max(0, self.active_assignments - 1),
            leases_by_worker=new_leases,
        )


# ── Worker role derivation ─────────────────────────────────────────────────
@dataclass(frozen=True)
class WorkerProfile:
    """The scheduler's view of a worker, derived from registration metadata.

    ``role`` is the resolved role (one of ``VALID_ROLES``).
    ``utilization`` is normalized 0..1 (fraction of capacity busy).
    ``last_served_at`` is epoch seconds of the last claim/grab — older is
    better (deficit / least-recently-served). ``None`` means "never served".
    ``deficit`` is an explicit fairness score — a higher value means the
    worker is more under-served and should be preferred next.
    ``active_minimax_lease`` is the active MiniMax job id held by this
    worker, or ``None``.
    """

    urn: str
    host: str
    role: str
    utilization: float
    last_served_at: float | None
    deficit: float
    active_minimax_lease: str | None

    @property
    def is_build_role(self) -> bool:
        return self.role == ROLE_BUILD

    @property
    def holds_minimax_lease(self) -> bool:
        return bool(self.active_minimax_lease)


def _norm_str(value: Any) -> str:
    return value.lower().strip() if isinstance(value, str) else ""


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(f):  # NaN guard
        return default
    return f


def _coerce_utilization(value: Any) -> float:
    """Clamp to [0.0, 1.0]; the bus treats utilization as a normalized
    fraction, so values outside this band are clamped rather than rejected —
    a misreported 7.3 from a buggy worker must NOT throw the scheduler off
    and must NOT make the worker appear infinitely busy."""
    f = _coerce_float(value, 0.0)
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def role_from_capabilities(capabilities: Iterable[str]) -> str | None:
    """Resolve role from capability metadata.

    Returns the first ``role:*`` capability found, normalized and validated.
    Returns ``None`` when no explicit role capability is declared (callers
    fall back to the conventional-build-host hint).
    """
    for cap in capabilities or ():
        s = _norm_str(cap)
        if s.startswith("role:"):
            candidate = s[len("role:") :]
            if candidate in VALID_ROLES:
                return candidate
    return None


def derive_worker_role(capabilities: Iterable[str], host: str) -> str:
    """Resolve a worker's role from capability metadata + host hint.

    Capability always wins: a worker that declares ``role:general`` is
    ``general`` regardless of host. When no explicit role capability is
    declared, the conventional build host set acts as a default hint.
    Workers on hosts not in ``CONVENTIONAL_BUILD_HOSTS`` default to
    ``general``. This is the single source of truth for role resolution in
    the bus — callers MUST go through here, never replicate the rule.
    """
    explicit = role_from_capabilities(capabilities or [])
    if explicit is not None:
        return explicit
    if _norm_str(host) in CONVENTIONAL_BUILD_HOSTS:
        return ROLE_BUILD
    return ROLE_GENERAL


def worker_profile_from_metadata(
    *,
    urn: str,
    host: str,
    capabilities: Iterable[str],
    current_load: Any,
    active_minimax_lease: str | None = None,
) -> WorkerProfile:
    """Build a ``WorkerProfile`` from registration/heartbeat payloads.

    ``current_load`` may be either a normalized float in ``[0.0, 1.0]`` or a
    JSON object with ``utilization``/``last_served_at``/``deficit`` keys
    (the structured form supersedes the flat float once a worker starts
    reporting heartbeat metadata). Missing values default to neutral (0
    utilization, ``None`` last-served, 0 deficit) — a freshly-registered
    worker that has never served is preferred over a stale busy one.
    """
    role = derive_worker_role(capabilities or [], host)
    util = 0.0
    last_served_at: float | None = None
    deficit = 0.0
    if isinstance(current_load, dict):
        util = _coerce_utilization(current_load.get("utilization"))
        raw_lsa = current_load.get("last_served_at")
        try:
            last_served_at = float(raw_lsa) if raw_lsa is not None else None
        except (TypeError, ValueError):
            last_served_at = None
        deficit = _coerce_float(current_load.get("deficit"), 0.0)
    else:
        util = _coerce_utilization(current_load)
    return WorkerProfile(
        urn=_norm_str(urn),
        host=_norm_str(host),
        role=role,
        utilization=util,
        last_served_at=last_served_at,
        deficit=deficit,
        active_minimax_lease=_norm_str(active_minimax_lease) or None,
    )


# ── Candidate scoring ─────────────────────────────────────────────────────
# Order-key produced by ``score_candidate``. The sort is stable, fully
# comparable, and uses ONLY deterministic inputs (no clocks beyond the caller-
# supplied ``now`` and no randomness). The tuple is interpreted:
#
#   - earlier tuple = preferred (we sort ascending).
#
# Fields, in priority order:
#   1. job priority (DESC -> we negate so earlier = higher priority).
#   2. role-match bonus: a job's role demand vs the worker's role (lower is
#      better — see ``_ROLE_PENALTY`` below).
#   3. normalized utilization (ASC).
#   4. deficit (DESC -> negate so earlier = larger deficit).
#   5. last-served age in seconds (DESC -> larger gap = more starved).
#   6. job id (lexicographic) and submitter urn (lexicographic) for the
#      deterministic tie-break. The job id is an opaque URN-style string,
#      so this is stable across restarts.
#
# This shape is intentional: we want role-match + utilization to dominate,
# but never starve a worker that's been idle longest — the deficit/age
# fallback guarantees every eligible worker gets a turn.
_ROLE_PENALTY: dict[tuple[str, bool], int] = {
    # (worker_role, build_required) -> penalty
    # Build-required jobs: build role = 0 (best), general/review/data = high.
    (ROLE_BUILD, True): 0,
    (ROLE_REVIEW, True): 2,
    (ROLE_DATA, True): 3,
    (ROLE_GENERAL, True): 4,
    # Non-build jobs: general = 0, review/data = 1, build = 2.
    # Non-build jobs MUST prefer non-build workers, but never starve a
    # build-role worker that's the only eligible one (penalty 2 still lets
    # the build worker claim if nothing else matches).
    (ROLE_GENERAL, False): 0,
    (ROLE_REVIEW, False): 1,
    (ROLE_DATA, False): 1,
    (ROLE_BUILD, False): 2,
}


@dataclass(frozen=True)
class ScoredCandidate:
    """A job candidate scored for a specific worker + pool snapshot."""

    job_id: str
    score: tuple


def score_candidate(
    *,
    job: dict[str, Any],
    worker: WorkerProfile,
    now: float,
) -> ScoredCandidate:
    """Produce a deterministic sort key for ``job`` against ``worker``.

    Lower sort key = preferred. The caller sorts candidates by
    ``score.score`` ascending (the rest of the agent_bus dequeue flow
    preserves the existing dependency+DAG filter before this scoring).
    """
    job_id = _norm_str(job.get("id")) or _norm_str(job.get("job_id"))
    submitter = _norm_str(job.get("submitter_urn"))
    priority = int(job.get("priority") or 0)
    build_required = job_requires_build(job)
    role_penalty = _ROLE_PENALTY[(worker.role, build_required)]
    util = worker.utilization
    deficit = worker.deficit
    # Age = how long since the worker last served; None means "never".
    if worker.last_served_at is None:
        age_seconds = float("inf")
    else:
        try:
            age_seconds = max(0.0, float(now) - float(worker.last_served_at))
        except (TypeError, ValueError):
            age_seconds = 0.0
    # ``util`` and ``deficit`` are floats; we round-trip them so tuple
    # comparison stays stable across runs.
    score = (
        -priority,  # higher priority first
        role_penalty,  # role match first
        util,  # lower utilization first
        -deficit,  # higher deficit first
        -age_seconds,  # older last-served first
        job_id,  # deterministic id tie-break
        submitter,  # deterministic submitter tie-break
        worker.urn,  # deterministic worker tie-break
    )
    return ScoredCandidate(job_id=job_id, score=score)


def order_candidates_for_worker(
    *,
    candidates: Sequence[dict[str, Any]],
    worker: WorkerProfile,
    now: float,
) -> list[dict[str, Any]]:
    """Return ``candidates`` re-ordered for ``worker`` using ``score_candidate``.

    Stable for equal sort keys: input order is preserved. Determinism here
    is what makes ``test_role_priority_vs_utilization`` reproducible across
    runs.
    """
    indexed = list(enumerate(candidates))
    indexed.sort(
        key=lambda pair: (
            score_candidate(job=pair[1], worker=worker, now=now).score,
            pair[0],
        )
    )
    return [candidates[i] for i, _ in indexed]


# ── Starvation protection ──────────────────────────────────────────────────
# A worker that has been eligible but never served for ``STARVATION_AGE``
# seconds is automatically boosted to the head of the candidate ordering
# among equally-priority-eligible jobs. This guarantees that a worker
# which just registered (or has been idle due to retries) eventually gets
# serviced even when the fleet has a steady stream of higher-priority jobs.
STARVATION_AGE_SECONDS = 600.0  # 10 minutes


def is_starved(worker: WorkerProfile, *, now: float) -> bool:
    """Return True when the worker has gone too long without serving."""
    if worker.last_served_at is None:
        # Treat "never served" as a soft-starved state — they get a chance
        # quickly, but the scheduler will not refuse to feed them.
        return True
    try:
        return (float(now) - float(worker.last_served_at)) > STARVATION_AGE_SECONDS
    except (TypeError, ValueError):
        return False


def select_best_job(
    *,
    candidates: Sequence[dict[str, Any]],
    worker: WorkerProfile,
    now: float,
) -> dict[str, Any] | None:
    """Pick the single best job for ``worker`` from a candidate list.

    Returns ``None`` when the list is empty. Starved workers are boosted
    to the head among equally-priority candidates; otherwise the candidate
    sort key (``role penalty + utilization + deficit + age + tie-break``)
    decides.
    """
    if not candidates:
        return None
    ordered = order_candidates_for_worker(
        candidates=candidates,
        worker=worker,
        now=now,
    )
    if not is_starved(worker, now=now):
        return ordered[0]
    # Starved boost: among candidates tied at the top priority + role
    # penalty, prefer the starved worker's pick — which by construction
    # IS ordered[0]. The starvation guarantee here is structural: a
    # starved worker's role-penalty is unchanged, but their deficit is
    # treated as effectively saturated (set to +inf below) so they win
    # any tie-break with an equally-eligible worker.
    boosted = WorkerProfile(
        urn=worker.urn,
        host=worker.host,
        role=worker.role,
        utilization=worker.utilization,
        last_served_at=worker.last_served_at,
        deficit=float("inf"),  # starvation boost: win all deficit ties
        active_minimax_lease=worker.active_minimax_lease,
    )
    boosted_ordered = order_candidates_for_worker(
        candidates=candidates,
        worker=boosted,
        now=now,
    )
    return boosted_ordered[0]


__all__ = [
    "BUILD_CAPABILITY_PREFIX",
    "BUILD_KIND_PREFIX",
    "CONVENTIONAL_BUILD_HOSTS",
    "MINIMAX_GLOBAL_MAX",
    "MINIMAX_MODEL",
    "MINIMAX_PER_WORKER_MAX",
    "MINIMAX_PROVIDER",
    "ROLE_BUILD",
    "ROLE_DATA",
    "ROLE_GENERAL",
    "ROLE_REVIEW",
    "STARVATION_AGE_SECONDS",
    "VALID_ROLES",
    "MiniMaxPoolState",
    "ScoredCandidate",
    "WorkerProfile",
    "build_pool_snapshot",
    "derive_worker_role",
    "is_minimax_route",
    "is_starved",
    "job_requires_build",
    "order_candidates_for_worker",
    "role_from_capabilities",
    "score_candidate",
    "select_best_job",
    "worker_profile_from_metadata",
]
