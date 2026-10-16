"""Orphan-claim selection — pure, unit-testable queue-health logic.

The reaper requeues jobs whose worker is gone or has stopped making progress.
The subtlety is the three-way TTL:

  1. lease expiry  — ``claim_lease_expires_at`` passed (the worker must renew);
  2. hard cap      — wall-clock since FIRST claim (un-refreshable) — defeats the
                     retry-loop evasion where lease + last_update stay fresh but
                     the job never progresses;
  3. kind TTL      — staleness vs ``COALESCE(last_update_at, claimed_at)``, so a
                     worker still PATCHing progress is never reclaimed.

This lives here (not inline in the reaper loop) so it can be tested directly.
"""

from __future__ import annotations

from typing import Any


def _kind_ttl(kind: str | None, mapping: dict[str, float], default: float) -> float:
    """Resolve a kind-specific TTL.

    Kind overrides are keyed by the BASE kind ('investigation', 'migration') but
    real kinds carry a suffix ('investigation:image-build-preflight'). An exact
    dict lookup therefore never matched and the overrides were dead. Match the
    exact key first, else the base kind (kind before the first ':').
    """
    if not kind:
        return default
    if kind in mapping:
        return mapping[kind]
    base = kind.split(":", 1)[0]
    return mapping.get(base, default)


def select_orphan_claims(
    candidates: list[tuple],
    *,
    now: float,
    stale_after: float,
    hard_max_age: float,
    stale_by_kind: dict[str, float] | None = None,
    hard_by_kind: dict[str, float] | None = None,
) -> list[tuple[str, Any]]:
    """Return ``[(job_id, claimed_by)]`` for claims the reaper should requeue.

    Each candidate is ``(job_id, kind, claimed_by, claimed_at, last_update_at,
    claim_lease_expires_at)``. ``claimed_at`` is None for a corrupt/phantom
    claim — surfaced so it is not un-reapable (its latest is treated as 0).
    """
    stale_by_kind = stale_by_kind or {}
    hard_by_kind = hard_by_kind or {}
    orphans: list[tuple[str, Any]] = []
    for jid, jkind, claimer, c_at, u_at, lease_exp in candidates:
        # 1. explicit lease expired
        if lease_exp is not None and lease_exp <= now:
            orphans.append((jid, claimer))
            continue
        # 2. hard cap since FIRST claim (un-refreshable)
        hard_ttl = _kind_ttl(jkind, hard_by_kind, hard_max_age)
        if c_at is not None and (now - c_at) >= hard_ttl:
            orphans.append((jid, claimer))
            continue
        # 3. kind-specific staleness vs latest progress (or first claim)
        kind_ttl = _kind_ttl(jkind, stale_by_kind, stale_after)
        latest = u_at if u_at is not None else (c_at or 0)
        if (now - latest) >= kind_ttl:
            orphans.append((jid, claimer))
    return orphans
