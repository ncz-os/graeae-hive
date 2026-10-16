"""Messages endpoints (Phase 1c router extraction).

Served under /v1/messages. Depends only on hive.bus.deps + hive.bus.schemas,
not on the agent_bus monolith.
"""

from __future__ import annotations

import json
import time

from fastapi import APIRouter

from hive.bus.deps import (
    clamp_limit,
    connect_db,
    emit_event,
    hive_repo,
    require_registered_agent,
    uuidv7,
)
from hive.bus.schemas import MessagePublish

router = APIRouter()


@router.post("/v1/messages")
async def publish_message(req: MessagePublish):
    msg_id = uuidv7()
    now = time.time()
    await require_registered_agent(req.from_urn, active_only=True)
    async with connect_db() as db:
        await db.execute(
            "INSERT INTO messages (id, from_urn, to_urn, in_reply_to, topic, payload, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                msg_id,
                req.from_urn,
                req.to_urn,
                req.in_reply_to,
                req.topic,
                json.dumps(req.payload),
                now,
            ),
        )
        await db.commit()
        await emit_event(
            db,
            "message",
            {
                "id": msg_id,
                "from": req.from_urn,
                "to": req.to_urn,
                "topic": req.topic,
                "payload": req.payload,
            },
        )
    return {"id": msg_id, "ts": now}


@router.get("/v1/messages")
async def list_messages(
    to_urn: str | None = None,
    topic: str | None = None,
    since: float | None = None,
    limit: int = 100,
):
    limit = clamp_limit(limit, default=100, max_limit=1000)
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        rows = await hive_repo().list_messages(tx, to_urn=to_urn, topic=topic, since=since, limit=limit)
    return {"count": len(rows), "messages": rows}
