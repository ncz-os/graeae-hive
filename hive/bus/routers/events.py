"""Events endpoints (Phase 1c router extraction).

GET /v1/events (SSE) + GET /v1/events/log (JSON polling). Storage via the
HiveMindRepository ABC (tail_events); the live SSE fan-out uses the shared
EVENT_QUEUE.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from hive.bus.deps import (
    EVENT_QUEUE,
    MAX_EVENT_SUBSCRIBERS,
    SSE_PING_INTERVAL,
    clamp_limit,
    hive_repo,
)

router = APIRouter()


@router.get("/v1/events")
async def stream_events(request: Request, since_id: int | None = None):
    """SSE stream of events. Optional since_id for catch-up."""
    if len(EVENT_QUEUE) >= MAX_EVENT_SUBSCRIBERS:
        raise HTTPException(503, f"too many event subscribers; max={MAX_EVENT_SUBSCRIBERS}")
    sub_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    EVENT_QUEUE[sub_id] = queue

    async def gen():
        try:
            # catch-up phase (one ABC read; bounded like tail_events)
            if since_id is not None:
                async with hive_repo().connection() as tx:
                    rows = await hive_repo().tail_events(tx, since_id=since_id, limit=1000)
                for r in rows:
                    yield {
                        "id": str(r["id"]),
                        "data": json.dumps(
                            {"ts": r["ts"], "kind": r["kind"], "payload": r["payload"]}
                        ),
                    }
            # live stream — use un-typed events (event: message) so browser onmessage fires
            last_ping = time.time()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=SSE_PING_INTERVAL)
                    yield {
                        "data": json.dumps(evt),
                    }
                except asyncio.TimeoutError:  # noqa: UP041 (keep explicit for the SSE ping path)
                    if time.time() - last_ping >= SSE_PING_INTERVAL:
                        yield {"event": "ping", "data": str(int(time.time()))}
                        last_ping = time.time()
        finally:
            EVENT_QUEUE.pop(sub_id, None)

    return EventSourceResponse(gen())


@router.get("/v1/events/log")
async def events_log(since_id: int | None = None, limit: int = 100):
    """JSON polling alternative to SSE."""
    limit = clamp_limit(limit, default=100, max_limit=1000)
    async with hive_repo().connection() as tx:
        rows = await hive_repo().tail_events(tx, since_id=since_id, limit=limit)
    return {
        "count": len(rows),
        "events": rows,
        "last_id": rows[-1]["id"] if rows else since_id,
    }
