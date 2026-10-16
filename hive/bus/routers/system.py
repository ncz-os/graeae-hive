"""Liveness + dashboard shell (Phase 1c).

/health is the external-monitor liveness probe; / serves the single-page
dashboard. Both are read-only and touch no persistence beyond the DB liveness
probe.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from hive.bus.deps import connect_db

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    """Liveness — 200 only if the DB probe succeeds AND the reaper is running.

    Returning 200 unconditionally (as the argonas line did) hides a downed DB or
    a stopped reaper from any external monitor. 503 carries the reason."""
    base = {"ts": time.time(), "service": "graeae-hive-mind", "version": "0.1.0"}
    try:
        async with connect_db() as db:
            async with db.execute("SELECT 1") as cur:
                await cur.fetchone()
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            {**base, "status": "unhealthy", "detail": f"database probe failed: {e}"},
            status_code=503,
        )
    reaper = getattr(request.app.state, "reaper_task", None)
    # 200 only if the reaper is running: a missing task (e.g. lifespan never
    # completed) or a finished one is unhealthy.
    if reaper is None or reaper.done():
        return JSONResponse(
            {**base, "status": "unhealthy", "detail": "reaper task is not running"},
            status_code=503,
        )
    return {**base, "status": "healthy"}


@router.get("/")
async def dashboard():
    """Minimal HTML+JS dashboard. Auto-refreshes /v1/agents + /v1/jobs + /v1/stats/* + SSE."""
    p = "/srv/agent-bus/dashboard.html"
    try:
        with open(p) as f:
            return HTMLResponse(
                f.read(),
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate",
                    "Pragma": "no-cache",
                },
            )
    except FileNotFoundError:
        return HTMLResponse(f"<h1>dashboard.html missing</h1><p>Expected at {p}</p>", status_code=404)
