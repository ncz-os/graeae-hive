"""Minimal MCP-style JSON-RPC shim (Phase 1c).

Maps a few common MCP-style JSON-RPC calls to the REST handlers.
Phase 2: replace with the full mcp-server-sdk.
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from hive.bus.routers.agents import heartbeat, list_agents, register
from hive.bus.routers.jobs import create_job, list_jobs
from hive.bus.routers.messages import list_messages, publish_message
from hive.bus.schemas import AgentHeartbeat, AgentRegister, JobCreate, MessagePublish

router = APIRouter()


@router.post("/mcp")
async def mcp_rpc(body: dict):
    method = body.get("method", "")
    params = body.get("params", {})
    handlers = {
        "agent.register": register,
        "agent.heartbeat": heartbeat,
        "agent.list": list_agents,
        "job.create": create_job,
        "job.list": list_jobs,
        "message.publish": publish_message,
        "message.list": list_messages,
    }
    if method not in handlers:
        return JSONResponse({"error": f"unknown method: {method}"}, status_code=404)
    # crude param coercion - production MCP would use proper validation
    try:
        if method == "agent.register":
            return await register(AgentRegister(**params))
        if method == "agent.heartbeat":
            return await heartbeat(AgentHeartbeat(**params))
        if method == "agent.list":
            return await list_agents(**params)
        if method == "job.create":
            return await create_job(JobCreate(**params))
        if method == "job.list":
            return await list_jobs(**params)
        if method == "message.publish":
            return await publish_message(MessagePublish(**params))
        if method == "message.list":
            return await list_messages(**params)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
