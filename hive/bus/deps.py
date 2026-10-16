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
import time
import uuid
from contextlib import asynccontextmanager

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


async def emit_event(db, kind: str, payload: dict) -> None:
    ts = time.time()
    payload_json = json.dumps(payload, separators=(",", ":"))
    await db.execute(
        "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
        (ts, kind, payload_json, payload.get("urn") or payload.get("agent_urn")),
    )
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
        active_ph = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
        await db.executemany(
            f"DELETE FROM jobs WHERE id=? AND status NOT IN ({active_ph})",
            [(jid, *ACTIVE_JOB_STATUSES) for jid in job_ids],
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
