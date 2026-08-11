"""
GRAEAE Hive Mind — MCP SSE bridge.

Exposes Hive (HYDRA :5005) + MNEMOS REST (PYTHIA :5002) as a single MCP server
on HYDRA :5006/sse. Speaks proper SSE MCP transport for opencode/Claude-Desktop.

Endpoint matrix:
  /sse  : SSE GET — server→client event stream
  /messages : POST — client→server JSON-RPC

Tools:
  hive.agent_list           -> GET /v1/agents on HIVE
  hive.agent_register       -> POST /v1/agents/register
  hive.job_create           -> POST /v1/jobs
  hive.job_next             -> POST /v1/jobs/next?agent_urn=
  hive.job_list             -> GET /v1/jobs
  hive.job_update           -> PATCH /v1/jobs/{id}
  hive.message_publish      -> POST /v1/messages
  hive.message_list         -> GET /v1/messages
  mnemos.memory_search      -> POST /v1/memories/search on PYTHIA
  mnemos.memory_create      -> POST /v1/memories
  mnemos.memory_get         -> GET /v1/memories/{id}
"""
from __future__ import annotations
import json
import os
import sys
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import httpx
import uvicorn

try:
    MCP_VERSION = version("mcp")
except PackageNotFoundError as exc:
    raise RuntimeError("mcp_bridge requires mcp>=2,<3") from exc
if MCP_VERSION.split(".", 1)[0] != "2":
    raise RuntimeError(f"mcp_bridge requires mcp>=2,<3; found {MCP_VERSION}")

from mcp.server import Server, NotificationOptions
from mcp.server.sse import SseServerTransport
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ListPromptsResult,
    ListResourcesResult,
    ListToolsResult,
    PaginatedRequestParams,
    Tool,
    TextContent,
    Resource,
    Prompt,
)
from starlette.applications import Starlette
from starlette.routing import Mount, Route

HIVE_URL = os.environ.get("HIVE_URL", "http://127.0.0.1:5005")
# C1: bus transport auth bearer token (env-sourced). Injected only on bus calls
# (_hive), kept separate from the MNEMOS token used by _mnemos.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
HIVE_BUS_ADMIN_TOKEN = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
MNEMOS_URL = os.environ.get("MNEMOS_URL", "http://192.0.2.67:5002")
# MNEMOS bearer token comes from env. No source-baked default — having a
# token literal in source meant every clone of this repo carried a working
# Authorization header for the live MNEMOS instance. mnemos.* tool calls
# now require the operator to set MNEMOS_TOKEN explicitly (via systemd
# EnvironmentFile or container env); when missing, calls to /v1/memories/*
# return a clear MCP tool error instead of being silently unauthenticated.
MNEMOS_TOKEN = os.environ.get("MNEMOS_TOKEN", "")

server = Server("graeae-hive-mind")
client = httpx.AsyncClient(timeout=15.0)


def _mcp_handler(method: str, params_type: type):
    """Register a typed request callback on the supported MCP 2.x server."""
    def decorate(fn):
        async def adapter(_context, params):
            if method == "tools/list":
                return ListToolsResult(tools=await fn())
            if method == "tools/call":
                content = await fn(params.name, params.arguments or {})
                return CallToolResult(content=content)
            if method == "resources/list":
                return ListResourcesResult(resources=await fn())
            if method == "prompts/list":
                return ListPromptsResult(prompts=await fn())
            raise RuntimeError(f"unsupported MCP method registration: {method}")

        server.add_request_handler(method, params_type, adapter)
        return fn
    return decorate


@asynccontextmanager
async def lifespan(app):
    try:
        yield
    finally:
        await client.aclose()


async def _hive(method: str, path: str, **kw) -> dict:
    body = kw.get("json")
    registering_orchestrator = (
        path == "/v1/agents/register"
        and isinstance(body, dict)
        and str(body.get("runtime") or body.get("kind") or "").lower() in {
            "claude-code", "claude-cli", "claude", "human", "mnemos",
            "doctor", "dream-walker", "unknown",
        }
    )
    token = HIVE_BUS_ADMIN_TOKEN if registering_orchestrator else HIVE_BUS_TOKEN
    if token:
        headers = dict(kw.pop("headers", {}) or {})
        headers.setdefault("Authorization", f"Bearer {token}")
        kw["headers"] = headers
    r = await client.request(method, f"{HIVE_URL}{path}", **kw)
    r.raise_for_status()
    if r.status_code == 204:
        return {"_status": 204, "_note": "no eligible job"}
    return r.json()


async def _mnemos(method: str, path: str, **kw) -> dict:
    if not MNEMOS_TOKEN:
        raise RuntimeError(
            "MNEMOS_TOKEN is not set in the environment. The mcp_bridge no "
            "longer carries a source-baked default; configure MNEMOS_TOKEN "
            "via systemd EnvironmentFile (/etc/default/graeae-mcp-bridge) "
            "or container env before invoking mnemos.* tools."
        )
    headers = kw.pop("headers", {})
    headers["Authorization"] = f"Bearer {MNEMOS_TOKEN}"
    r = await client.request(method, f"{MNEMOS_URL}{path}", headers=headers, **kw)
    r.raise_for_status()
    return r.json()


# ---------- tool definitions ----------

@_mcp_handler("tools/list", PaginatedRequestParams)
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(name="hive_agent_list", description="List registered agents in the GRAEAE Hive Mind.",
             inputSchema={"type": "object", "properties": {
                 "kind": {"type": "string"},
                 "status": {"type": "string"},
                 "include_offline": {"type": "boolean"},
             }}),
        Tool(name="hive_agent_register",
             description="Register this session as an agent in the Hive. Returns urn + session_id. Bridge fills sensible defaults for runtime/model/provider/autonomy_level/auth_method so urn always populates.",
             inputSchema={"type": "object",
                          "properties": {
                              "kind": {"type": "string", "description": "claude/opencode/codex/zeroclaw/openclaw/hermes/ic-engine/mnemos/human"},
                              "host": {"type": "string"},
                              "capabilities": {"type": "array", "items": {"type": "string"}},
                              "version": {"type": "string"},
                              "metadata": {"type": "object"},
                              "runtime": {"type": "string", "description": "Claude-desktop / opencode / codex-cli / etc — defaults to kind."},
                              "model": {"type": "string"},
                              "provider": {"type": "string"},
                              "autonomy_level": {"type": "string", "enum": ["autonomous", "confirm-risky", "interactive", "unknown"]},
                              "auth_method": {"type": "string", "enum": ["subscription", "api", "free", "unknown"]},
                          }, "required": ["kind", "host"]}),
        Tool(name="hive_job_create",
             description="Submit work into the Hive Mind triage queue. Eligible agents self-claim.",
             inputSchema={"type": "object",
                          "properties": {
                              "submitter_urn": {"type": "string"},
                              "kind": {"type": "string"},
                              "description": {"type": "string"},
                              "priority": {"type": "integer", "default": 0},
                              "required_capabilities": {"type": "array", "items": {"type": "string"}},
                              "eligible_kinds": {"type": "array", "items": {"type": "string"}},
                              "deadline": {"type": "number"},
                              "parent_job_id": {"type": "string"},
                          }, "required": ["submitter_urn", "kind"]}),
        Tool(name="hive_job_next",
             description="Atomic dequeue: claim highest-priority eligible job for this agent. Returns job or {204: no work}.",
             inputSchema={"type": "object",
                          "properties": {"agent_urn": {"type": "string"}},
                          "required": ["agent_urn"]}),
        Tool(name="hive_job_list",
             description="List jobs in the Hive. Filter by status/agent/since.",
             inputSchema={"type": "object",
                          "properties": {
                              "status": {"type": "string"},
                              "agent_urn": {"type": "string"},
                              "since": {"type": "number"},
                              "limit": {"type": "integer", "default": 100},
                          }}),
        Tool(name="hive_job_update",
             description="Update job status/result. Status: queued/claimed/running/done/failed/cancelled.",
             inputSchema={"type": "object",
                          "properties": {
                              "id": {"type": "string"},
                              "status": {"type": "string"},
                              "result": {"type": "object"},
                              "claimed_by": {"type": "string"},
                          }, "required": ["id", "status"]}),
        Tool(name="hive_message_publish",
             description="Publish a Hive message. to_urn=null for broadcast.",
             inputSchema={"type": "object",
                          "properties": {
                              "from_urn": {"type": "string"},
                              "to_urn": {"type": "string"},
                              "topic": {"type": "string"},
                              "payload": {"type": "object"},
                              "in_reply_to": {"type": "string"},
                          }, "required": ["from_urn", "topic", "payload"]}),
        Tool(name="hive_message_list",
             description="List Hive messages. Filter by recipient/topic.",
             inputSchema={"type": "object",
                          "properties": {
                              "to_urn": {"type": "string"},
                              "topic": {"type": "string"},
                              "since": {"type": "number"},
                              "limit": {"type": "integer", "default": 100},
                          }}),
        Tool(name="mnemos_memory_search",
             description="Search MNEMOS memory (PYTHIA). Semantic+keyword.",
             inputSchema={"type": "object",
                          "properties": {
                              "query": {"type": "string"},
                              "category": {"type": "string"},
                              "limit": {"type": "integer", "default": 10},
                              "min_score": {"type": "number"},
                          }, "required": ["query"]}),
        Tool(name="mnemos_memory_create",
             description="Save a memory to MNEMOS. Category: infrastructure/solutions/patterns/decisions/projects/standards.",
             inputSchema={"type": "object",
                          "properties": {
                              "content": {"type": "string"},
                              "category": {"type": "string"},
                              "tags": {"type": "array", "items": {"type": "string"}},
                          }, "required": ["content", "category"]}),
        Tool(name="mnemos_memory_get",
             description="Get a MNEMOS memory by id (mem_XXX).",
             inputSchema={"type": "object",
                          "properties": {"id": {"type": "string"}}, "required": ["id"]}),
        Tool(name="graeae_consult",
             description="Submit a multi-LLM consensus consultation to GRAEAE (PYTHIA). Modes: auto/single/all/debate/majority.",
             inputSchema={"type": "object",
                          "properties": {
                              "prompt": {"type": "string"},
                              "task_type": {"type": "string", "description": "reasoning/architecture_design/code_generation/web_search"},
                              "mode": {"type": "string", "enum": ["auto","local","external","all","single","debate","majority"], "default": "auto"},
                              "models": {"type": "array", "items": {"type": "string"}, "description": "explicit model list (overrides mode)"},
                              "limit_chars": {"type": "integer"},
                              "format": {"type": "string", "default": "full"},
                          }, "required": ["prompt"]}),
        Tool(name="graeae_muses",
             description="List available GRAEAE muses (LLM providers + models).",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="graeae_modes",
             description="List GRAEAE consultation modes + their descriptions.",
             inputSchema={"type": "object", "properties": {}}),
        Tool(name="graeae_get",
             description="Get a previous GRAEAE consultation by id.",
             inputSchema={"type": "object",
                          "properties": {"consultation_id": {"type": "string"}},
                          "required": ["consultation_id"]}),
    ]


@_mcp_handler("tools/call", CallToolRequestParams)
async def handle_call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:
    try:
        if name == "hive_agent_list":
            r = await _hive("GET", "/v1/agents", params={k: v for k, v in args.items() if v})
        elif name == "hive_agent_register":
            # REVIEW #5 fix: ensure urn-populating defaults at the bridge layer
            # so MCP clients that omit runtime/model/autonomy still get a
            # valid urn back. Server-side already defaults missing fields to
            # 'unknown' — this just keeps the bridge surface explicit.
            args.setdefault("runtime", args.get("kind", "unknown"))
            args.setdefault("model", "unknown")
            args.setdefault("provider", "unknown")
            args.setdefault("autonomy_level", "unknown")
            args.setdefault("auth_method", "unknown")
            r = await _hive("POST", "/v1/agents/register", json=args)
            # Defensive: if response somehow lacks urn (server bug / proxy
            # truncation), surface as explicit error rather than letting
            # the client cache urn=None and brick the session.
            if not isinstance(r, dict) or not r.get("urn"):
                r = {
                    "error": "register-incomplete: agent_bus returned response without urn",
                    "raw_response": r,
                }
        elif name == "hive_job_create":
            r = await _hive("POST", "/v1/jobs", json=args)
        elif name == "hive_job_next":
            r = await _hive("POST", f"/v1/jobs/next", params={"agent_urn": args["agent_urn"]})
        elif name == "hive_job_list":
            r = await _hive("GET", "/v1/jobs", params={k: v for k, v in args.items() if v})
        elif name == "hive_job_update":
            jid = args.pop("id")
            r = await _hive("PATCH", f"/v1/jobs/{jid}", json=args)
        elif name == "hive_message_publish":
            r = await _hive("POST", "/v1/messages", json=args)
        elif name == "hive_message_list":
            r = await _hive("GET", "/v1/messages", params={k: v for k, v in args.items() if v})
        elif name == "mnemos_memory_search":
            r = await _mnemos("POST", "/v1/memories/search", json=args)
        elif name == "mnemos_memory_create":
            r = await _mnemos("POST", "/v1/memories", json=args)
        elif name == "mnemos_memory_get":
            r = await _mnemos("GET", f"/v1/memories/{args['id']}")
        elif name == "graeae_consult":
            r = await _mnemos("POST", "/v1/consultations", json=args)
        elif name == "graeae_muses":
            r = await _mnemos("GET", "/v1/consultations/muses")
        elif name == "graeae_modes":
            r = await _mnemos("GET", "/v1/consultations/modes")
        elif name == "graeae_get":
            r = await _mnemos("GET", f"/v1/consultations/{args['consultation_id']}")
        else:
            return [TextContent(type="text", text=json.dumps({"error": f"unknown tool: {name}"}))]
        return [TextContent(type="text", text=json.dumps(r, default=str))]
    except httpx.HTTPStatusError as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e), "status": e.response.status_code, "body": e.response.text[:500]}))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": type(e).__name__, "msg": str(e)}))]


# Silence -32601 method-not-found warnings on opencode boot.
@_mcp_handler("resources/list", PaginatedRequestParams)
async def _list_resources() -> list[Resource]:
    return []


@_mcp_handler("prompts/list", PaginatedRequestParams)
async def _list_prompts() -> list[Prompt]:
    return []


# ---------- SSE transport ----------

sse = SseServerTransport("/messages/")


async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as (read_stream, write_stream):
        options = server.create_initialization_options(
            notification_options=NotificationOptions(),
            experimental_capabilities={},
        )
        await server.run(read_stream, write_stream, options)


app = Starlette(
    debug=False,
    lifespan=lifespan,
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ],
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5006")))
