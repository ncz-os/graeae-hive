"""Shared bus dependencies (Phase 1c extraction).

Moved verbatim out of agent_bus.py so routers (`hive/bus/routers/*`) can import
the DB connection and the small shared helpers WITHOUT a cycle back into the
monolith. Behavior-preserving: same env config, same singletons, same code.

The config values are re-derived here from the same env vars agent_bus.py reads,
so the two stay in step (the live deploy sets them once per process).
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import os
import secrets
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Header, HTTPException

from hive.core.roles import ORCHESTRATOR_RUNTIMES, WORKER_ONLY_RUNTIMES
from hive.persistence.factory import get_hive_repository

# ---- repository ---------------------------------------------------------
# The bus does NOT choose a backend or carry a DSN. `get_hive_repository()`
# resolves HIVE_BACKEND / AGENT_BUS_DB / ORACLE_DSN / DB2_DSN from the
# environment in hive.persistence.factory — the single place backend+dialect
# knowledge lives. The bus layer only ever touches the HiveMindRepository ABC.

ACTIVE_AGENT_STATUSES = {"online", "idle"}
VALID_AGENT_STATUSES = {"online", "idle", "stale", "offline", "error"}
AUTONOMY_LEVELS = {"autonomous", "confirm-risky", "interactive", "unknown"}
AUTH_METHODS = {"subscription", "api", "free", "unknown"}
DEFAULT_PLAN_CAPS = {
    "subscription": 200.0,
    "api": 1000.0,
    "free": 0.0,
    "unknown": 50.0,
}
HOST_DENYLIST: set[str] = {h.strip().lower() for h in os.environ.get("HIVE_HOST_DENYLIST", "").split(",") if h.strip()}


def make_urn(kind: str, host: str, session_id: str) -> str:
    return f"urn:agent:{kind}:{host}:{session_id}"


# subscriber_id -> queue (shared object; agent_bus imports THIS dict)
EVENT_QUEUE: dict[str, asyncio.Queue] = {}
#: SSE stream knobs (used by the events router).
SSE_PING_INTERVAL = 15.0
MAX_EVENT_SUBSCRIBERS = int(os.environ.get("AGENT_BUS_MAX_EVENT_SUBSCRIBERS", "100"))

_HIVE_REPO = None


def hive_repo():
    global _HIVE_REPO
    if _HIVE_REPO is None:
        _HIVE_REPO = get_hive_repository()
    return _HIVE_REPO


def repo() -> object:
    """The shared HiveMindRepository ABC instance (storage facade)."""
    return hive_repo()


@asynccontextmanager
async def connect_db():
    async with hive_repo().connection() as db:
        yield db


async def emit_event(db, kind: str, payload: dict, *, commit: bool = True) -> None:
    ts = time.time()
    payload_json = json.dumps(payload, separators=(",", ":"))
    await db.execute(
        "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
        (ts, kind, payload_json, payload.get("urn") or payload.get("agent_urn")),
    )
    if not commit:
        # The caller owns this transaction. Publishing before its commit could
        # advertise an event that later rolls back; durable SSE replay reads it
        # after commit instead.
        return
    await db.commit()
    # broadcast to live SSE subscribers
    for q in list(EVENT_QUEUE.values()):
        with contextlib.suppress(asyncio.QueueFull):
            q.put_nowait({"kind": kind, "ts": ts, "payload": payload})


def uuidv7() -> str:
    """Time-ordered UUID for monotonic index inserts."""
    ts_ms = int(time.time() * 1000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    val = (ts_ms & ((1 << 48) - 1)) << 80 | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
    return str(uuid.UUID(int=val))


def clamp_limit(value: int, *, default: int = 100, max_limit: int = 1000) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return default
    if limit < 1:
        return default
    return min(limit, max_limit)


async def require_registered_agent(urn: str, *, active_only: bool = False) -> tuple[str, str]:
    if not urn or not urn.strip():
        raise HTTPException(422, "agent urn is required")
    async with (
        connect_db() as db,
        db.execute(
            "SELECT runtime, status FROM agents WHERE urn=?",
            (urn,),
        ) as cur,
    ):
        row = await cur.fetchone()
    if not row:
        raise HTTPException(403, f"agent is not registered: {urn}")
    runtime = (row[0] or "unknown").lower()
    status = (row[1] or "unknown").lower()
    if active_only and status not in ACTIVE_AGENT_STATUSES:
        raise HTTPException(403, f"agent {urn} is not active; current status={status!r}")
    return runtime, status


async def require_orchestrator_submitter(submitter_urn: str) -> str:
    runtime, _status = await require_registered_agent(submitter_urn, active_only=True)
    if runtime in WORKER_ONLY_RUNTIMES or runtime not in ORCHESTRATOR_RUNTIMES:
        raise HTTPException(
            status_code=403,
            detail=(
                f"role-violation: runtime={runtime!r} is not an orchestrator. "
                f"Workers CLAIM jobs via POST /v1/jobs/next; only registered active "
                f"orchestrators may SUBMIT jobs. Orchestrators: {sorted(ORCHESTRATOR_RUNTIMES)}."
            ),
        )
    return runtime


ACTIVE_JOB_STATUSES = ("queued", "offered", "claimed", "running", "needs-review")
#: Dequeue candidate scan depth (bounded on both sides so a huge queue cannot
#: turn a claim into a full scan). See the dequeue route comment.
CLAIM_SCAN_WINDOW = int(os.environ.get("CLAIM_SCAN_WINDOW", "10000"))
VALID_JOB_STATUSES = {
    "queued",
    "offered",
    "claimed",
    "running",
    "done",
    "failed",
    "failed_completion",
    "cancelled",
    "dead-letter",
    "needs-review",
    "released",
}
TERMINAL_JOB_STATUSES = {"done", "failed", "cancelled", "dead-letter"}

#: Statuses that still have a LIVE worker attached — the only ones the delete
#: cascade must refuse without ``force``/``include_active``. This is
#: ACTIVE_JOB_STATUSES minus ``needs-review``: a needs-review job is a
#: QUARANTINE state (its worker has already stopped, the completion was
#: rejected for missing evidence), so no worker can be orphaned by deleting it.
#: It stays out of TERMINAL_JOB_STATUSES on purpose — it is still PROMOTABLE
#: (needs-review -> done/failed/cancelled) and a human-attention queue, not
#: finished work.
IN_FLIGHT_JOB_STATUSES = ("queued", "offered", "claimed", "running")

#: Terminal statuses a job's dependents can NEVER satisfy: when a job enters
#: one of these, every queued/offered job listing it in ``depends_on`` must
#: have that id removed (see :func:`release_dependencies`). ``done`` is
#: deliberately absent — a completed dep satisfies its dependents the normal
#: way and the edge is kept for provenance. ``released`` is a release-to-queued
#: state (the job is not dead), so it is absent as well.
DEP_DEAD_STATUSES = frozenset({"failed", "failed_completion", "dead-letter", "cancelled"})
#: The ``dep_status`` a PURGED dep reports in the release record/event: its
#: row is gone, so no terminal status describes it.
DEP_PURGED_STATUS = "purged"


async def release_dependencies(db, dep_id: str, dep_status: str, *, emit_event=None) -> list[dict[str, Any]]:
    """Release dependents after a dependency dies or is purged.

    Incident 2026-10-27: a dep that failed was purged; its dependent declared
    ``depends_on: [<dep>]`` and sat ``queued`` with ``claimed_by=null`` forever
    — the claim gate requires every dep to reach status='done', a dead/purged
    id can never be done, and nothing else touched the edge.

    For every job in status 'queued' or 'offered' whose ``depends_on`` lists
    ``dep_id``, the id is removed: an emptied list becomes NULL (the canonical
    no-dependencies value — the job is claimable again), a surviving list
    keeps its ids (still blocked on them). A dependent that is already
    claimed/running is NEVER touched — its work is in flight.

    Runs in the CALLER's transaction (``db`` is the tx handle) — in the same
    transaction as the status write or the DELETE that killed the dep — so no
    intermediate state is observable: a concurrent reader sees either the old
    state or the fully-released state, never "dep terminal + dependent still
    listing it".

    Emits ONE ``job.dependency_released`` event per released dependent, in
    this same transaction, with the exact release record as payload:
    ``{"dependent_id", "failed_dep_id", "dep_status", "remaining"}`` where
    ``remaining`` is the dependent's ``depends_on`` after the removal (empty
    list = NULL = claimable now).

    ``dep_status`` is the dep's final terminal status (one of
    :data:`DEP_DEAD_STATUSES`) or :data:`DEP_PURGED_STATUS` on the purge
    paths. Returns the release records; empty when nothing was released.
    """
    # ``db`` is the caller's OPEN tx handle, so the dialect method that runs
    # on it is looked up on the backend that OWNS the connection (``db._repo``
    # is set when the backend yields the handle) — not via hive_repo(), which
    # is the singleton a router may have mocked to a different (temp)
    # repository for the whole transaction. A test spy wrapping the tx keeps
    # the real connection under ``_inner``; a ``mock.patch.object(repo, ...)``
    # patch survives on the owning backend, so resolving through it keeps the
    # patch in effect while the work runs on the wrapped handle. Falls back
    # to the shared singleton when the handle carries no back-ref (legacy
    # backends / connections opened outside the sqlite factory).
    conn = db
    while hasattr(conn, "_inner"):
        conn = conn._inner
    owner = getattr(conn, "_repo", None)
    if owner is None or not hasattr(owner, "release_dependents"):
        owner = repo()

    released = await owner.release_dependents(db, dep_id=dep_id, dep_status=dep_status)
    # One ``job.dependency_released`` event per released dependent, in this
    # same transaction (before the caller's commit) — never a second
    # connection here, or the release would commit on its own and the status
    # write could commit without it. The event goes through the caller's own
    # ``emit_event`` (passed by the router) so a test patching the ROUTER's
    # binding observes exactly the events this path emits; callers that omit
    # it fall back to this module's emitter, resolved from the CURRENT module
    # namespace at call time (never a def-time default, which would freeze
    # the binding and hide a test's patch of this module's emit_event).
    if emit_event is not None:
        _emit = emit_event
    else:
        _module = sys.modules[__name__]
        _emit = _module.emit_event if _module is not None else emit_event
    for record in released:
        await _emit(db, "job.dependency_released", record, commit=False)
    return released


async def require_mutation_actor(
    x_hive_actor: str | None = Header(default=None, alias="X-Hive-Actor"),
    x_hive_token: str | None = Header(default=None, alias="X-Hive-Token"),
) -> str:
    """Gate mutating endpoints that previously had NO guard at all.

    Two layers: (1) if HIVE_SHARED_SECRET is set, a matching X-Hive-Token is
    required; (2) X-Hive-Actor must name a REGISTERED, ACTIVE agent. (Moved
    verbatim from agent_bus.py, Phase 1c.)
    """
    secret = os.environ.get("HIVE_SHARED_SECRET")
    if secret and (
        not x_hive_token or not hmac.compare_digest(str(x_hive_token), str(secret))
    ):
        raise HTTPException(401, "missing or incorrect X-Hive-Token")
    if not x_hive_actor or not str(x_hive_actor).strip():
        raise HTTPException(401, "X-Hive-Actor header is required for mutating calls")
    await require_registered_agent(str(x_hive_actor).strip(), active_only=True)
    return str(x_hive_actor).strip()


def is_admin_tier(authorization: str | None) -> bool:
    """True when the request carries the bus ADMIN-tier bearer token.

    The admin token is ``HIVE_BUS_ADMIN_TOKEN`` (the same variable the doctor and
    the MCP bridge send as ``Authorization: Bearer`` for admin calls). Fail
    closed: when the variable is unset nobody is admin-tier, so only an agent's
    own identity (X-Hive-Actor) can use an admin-or-self route.
    """
    admin_token = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
    if not admin_token or not authorization:
        return False
    scheme, _, supplied = authorization.partition(" ")
    if scheme.lower() != "bearer" or not supplied.strip():
        return False
    return hmac.compare_digest(supplied.strip(), admin_token)


async def _cascade_delete_jobs(db, job_ids: list[str], *, allow_active: bool = False) -> list[str]:
    """Delete the given jobs and their job_audit_log rows. ATOMIC active-job guard.

    (Moved verbatim from agent_bus.py, Phase 1c — see the original for the full
    codex-review #3 rationale on the compare-and-delete predicate.)
    """
    if not job_ids:
        return []
    rows = [(jid,) for jid in job_ids]
    if allow_active:
        await db.executemany("DELETE FROM jobs WHERE id=?", rows)
    else:
        active_ph = ",".join("?" for _ in IN_FLIGHT_JOB_STATUSES)
        await db.executemany(
            f"DELETE FROM jobs WHERE id=? AND status NOT IN ({active_ph})",
            [(jid, *IN_FLIGHT_JOB_STATUSES) for jid in job_ids],
        )
    survivors: set[str] = set()
    for jid in job_ids:
        async with db.execute("SELECT 1 FROM jobs WHERE id=?", (jid,)) as cur:
            if await cur.fetchone() is not None:
                survivors.add(jid)
    deleted = [jid for jid in job_ids if jid not in survivors]
    if deleted:
        await db.executemany(
            "DELETE FROM job_audit_log WHERE job_id=?",
            [(jid,) for jid in deleted],
        )
    return deleted
