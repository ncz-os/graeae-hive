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
import json
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import HTTPException

from hive.core.roles import ORCHESTRATOR_RUNTIMES, WORKER_ONLY_RUNTIMES
from hive.persistence.factory import get_hive_repository

# ---- config (mirrors agent_bus.py) --------------------------------------
_DB_PATH = os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db")
_HIVE_BACKEND = os.environ.get("HIVE_BACKEND", "sqlite").lower()
_ORACLE_DSN = os.environ.get("ORACLE_DSN", "oracle://mnemos:mnemos_dev@127.0.0.1:1521/ORCLPDB1")
_SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", "5000"))

ACTIVE_AGENT_STATUSES = {"online", "idle"}

# subscriber_id -> queue (shared object; agent_bus imports THIS dict)
EVENT_QUEUE: dict[str, asyncio.Queue] = {}

_HIVE_REPO = None


def hive_repo():
    global _HIVE_REPO
    if _HIVE_REPO is None:
        _HIVE_REPO = get_hive_repository(
            _HIVE_BACKEND,
            db_path=_DB_PATH,
            busy_timeout_ms=_SQLITE_BUSY_TIMEOUT_MS,
            dsn=_ORACLE_DSN,
            db2_dsn=os.environ.get("DB2_DSN") or os.environ.get("HIVE_DB2_DSN", ""),
        )
    return _HIVE_REPO


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
