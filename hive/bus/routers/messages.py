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
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        await hive_repo().post_message(
            tx,
            id=msg_id,
            from_urn=req.from_urn,
            to_urn=req.to_urn,
            in_reply_to=req.in_reply_to,
            topic=req.topic,
            payload_json=json.dumps(req.payload),
            ts=now,
        )
        await emit_event(
            tx,
            "message",
            {
                "id": msg_id,
                "from": req.from_urn,
                "to": req.to_urn,
                "topic": req.topic,
                "payload": req.payload,
            },
        )
        await tx.commit()
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
