"""Agent endpoints (Phase 1c router extraction).

Served under /v1/agents + /v1/hosts. Depends on hive.bus.deps (DB + shared
consts/helpers) and hive.bus.schemas — not on the agent_bus monolith.
"""

from __future__ import annotations

import hmac
import json
import os
import time
import uuid

from fastapi import APIRouter, Header, HTTPException

from hive.bus.deps import (
    AUTH_METHODS,
    AUTONOMY_LEVELS,
    DEFAULT_PLAN_CAPS,
    HOST_DENYLIST,
    VALID_AGENT_STATUSES,
    connect_db,
    emit_event,
    hive_repo,
    is_admin_tier,
    make_urn,
)
from hive.bus.domain.eligibility import RUNTIME_KIND_MAP, THROTTLE_HEADROOM
from hive.bus.schemas import AgentDeregister, AgentHeartbeat, AgentRegister
from hive.core.models import cost_tier_for

router = APIRouter()


@router.post("/v1/agents/register")
async def register(req: AgentRegister):
    # #5 FIX (review 2026-05-23): reject minimally-incomplete registrations with 422 so
    # callers see the error instead of getting a null URN that bricks their session.
    if (req.host or "").lower().strip() in HOST_DENYLIST:
        raise HTTPException(403, f"host {req.host!r} is in HIVE_HOST_DENYLIST")
    if not req.host or not req.host.strip():
        raise HTTPException(422, "host is required (e.g., 'studio' or hostname -s)")
    runtime = (req.runtime or "unknown").lower()
    kind = (req.kind or runtime).lower()
    if runtime == "unknown" and not req.kind:
        raise HTTPException(
            422,
            "must provide runtime (claude-code/opencode/codex/...) OR explicit kind. "
            "Defaulting to 'unknown' was masking session-bricking misregistrations.",
        )
    # If kind was provided but runtime omitted/unknown, default runtime to the
    # kind when the kind matches a known runtime identifier. Previously a
    # kind-only registration left runtime='unknown', which cascaded into
    # provider='unknown' + cost_tier='C', so default-A jobs never dequeued
    # AND the WORKER_ONLY_RUNTIMES role check stopped applying (unknown is
    # in ORCHESTRATOR_RUNTIMES). README, test/smoke.sh
    # all register kind-only. Fix: lift kind to runtime when safe.
    if runtime == "unknown" and req.kind:
        if kind in RUNTIME_KIND_MAP:
            runtime = kind
        else:
            # kind is not a recognised runtime alias — keep runtime='unknown'
            # but make the registration fail loud so callers fix it.
            raise HTTPException(
                422,
                f"kind={kind!r} provided without runtime; cannot infer "
                f"runtime. Provide an explicit runtime from "
                f"{sorted(RUNTIME_KIND_MAP.keys())}.",
            )
    allowed = RUNTIME_KIND_MAP.get(runtime, {runtime, "unknown"})
    if runtime != "unknown" and kind not in allowed and kind != runtime:
        raise HTTPException(
            status_code=422,
            detail=(
                f"identity-mismatch: runtime={runtime!r} cannot register as kind={kind!r}. "
                f"Allowed kinds for this runtime: {sorted(allowed)}. "
                f"Set kind to one of those (or omit it) — fixes opencode-misregistering-as-claude per advisory."
            ),
        )
    autonomy = (req.autonomy_level or "unknown").lower()
    if autonomy not in AUTONOMY_LEVELS:
        raise HTTPException(
            422,
            f"autonomy_level must be one of {sorted(AUTONOMY_LEVELS)}, got {autonomy!r}",
        )
    auth_method = (req.auth_method or "unknown").lower()
    if auth_method not in AUTH_METHODS:
        raise HTTPException(
            422,
            f"auth_method must be one of {sorted(AUTH_METHODS)}, got {auth_method!r}",
        )
    plan_cap_usd = req.plan_cap_usd if req.plan_cap_usd is not None else DEFAULT_PLAN_CAPS.get(auth_method, 50.0)
    provider = (req.provider or "unknown").lower()
    model = (req.model or "unknown").lower()
    # EIH ban (operator 2026-06-18, reaffirmed 2026-10-17): the hive must never
    # advertise an EIH or closed-frontier authoring label. Legacy workers may
    # still register with an "eih"/"gpt-5.5" model string; the EIH gpt-5.5 route
    # no longer exists, and closed frontier models are forbidden on EIH, so
    # normalize any such label to the real served upstream (MiniMax) and drop the
    # openai provider so the registry reflects reality.
    if "gpt-5.5" in model or "eih" in model:
        model = "minimax-m2.7"
        if provider == "openai":
            provider = "minimax"
    tier = cost_tier_for(provider)
    # User directive 2026-05-26: allow opencode + codex to claim work at ALL
    # tiers (A/B/C) regardless of provider. They are flexible multi-provider
    # agentic CLIs and the tier-ceiling is a routing aid, not a real cost gate
    # for them.
    if kind in {"opencode", "codex", "doctor", "zeroclaw"}:
        tier = "A"
    session_id = str(uuid.uuid4())
    urn = make_urn(kind, req.host, session_id)
    now = time.time()
    # REGISTRATION DEDUP (2026-06-15): a stable per-worker-instance identity lets a
    # worker restart REPLACE its prior agents row instead of minting an endless
    # stream of uuid4 URNs (10,930 rows / 10,882 offline observed 2026-06-08 — the
    # root of dashboard duplicate-registration/inflation). The instance id is
    # carried in the metadata JSON (no schema change / no DDL on the live Oracle DB)
    # under the reserved key "_instance", so the dedup key is (host, kind, instance).
    #
    # Backward-compat invariant: dedup engages ONLY when req.instance is present.
    # claude/codex/system/mnemos and old workers send no instance -> the historical
    # "always a fresh URN" behavior is preserved untouched. This block can therefore
    # never break an existing agent kind.
    #
    # Concurrency note: the bus is a single-process asyncio app and a worker
    # registers exactly once per (re)start, so a self-collision (two simultaneous
    # registers for the SAME (host,kind,instance)) is not a real workload. We
    # deliberately do NOT add a DB unique key — that is DDL on the live Oracle prod
    # DB, which is out of scope for this change. The dedup is therefore best-effort:
    # in the pathological double-register race the worst case is one residual
    # duplicate row, which the existing agent-purge reaper still collects. This
    # trades a vanishingly-rare extra row for guaranteed bus availability (a register
    # is never blocked or serialized), which is the correct safety posture for the
    # most critical fleet component.
    instance = req.instance
    effective_metadata = dict(req.metadata) if isinstance(req.metadata, dict) else ({} if instance else None)
    if instance:
        effective_metadata["_instance"] = instance
    # Storage via the HiveMindRepository ABC on one transaction (EPIMONE shape):
    # insert-then-dedup is atomic, so an observer sees {old} or {new}, never
    # {empty} and never {old,new} post-commit.
    async with hive_repo().connection() as tx:
        await hive_repo().register_agent(
            tx,
            urn=urn,
            kind=kind,
            runtime=runtime,
            model=model,
            provider=provider,
            cost_tier=tier,
            autonomy_level=autonomy,
            auth_method=auth_method,
            plan_cap_usd=plan_cap_usd,
            subscription_pools_json=json.dumps(req.subscription_pools or []),
            host=req.host,
            session_id=session_id,
            pid=req.pid,
            capabilities_json=json.dumps(req.capabilities) if req.capabilities else None,
            version=req.version,
            started_at=now,
            last_heartbeat=now,
            metadata_json=json.dumps(effective_metadata) if effective_metadata else None,
        )
        if instance:
            try:
                await hive_repo().dedup_agent_instance(
                    tx, host=req.host, kind=kind, instance=instance, keep_urn=urn
                )
            except Exception as _dedup_exc:
                # Dedup is best-effort: a failure never fails a register. The new
                # row is already inserted; worst case a stale duplicate lingers.
                print(
                    f"register dedup skipped for host={req.host} kind={kind} instance={instance!r}: {_dedup_exc!r}",
                    flush=True,
                )
        await emit_event(
            tx,
            "agent.online",
            {
                "urn": urn,
                "kind": kind,
                "runtime": runtime,
                "model": model,
                "provider": provider,
                "cost_tier": tier,
                "host": req.host,
                "autonomy_level": autonomy,
                "subscription_pools": req.subscription_pools or [],
            },
        )
        await tx.commit()
    return {
        "urn": urn,
        "session_id": session_id,
        "registered_at": now,
        "kind": kind,
        "runtime": runtime,
        "model": model,
        "provider": provider,
        "cost_tier": tier,
        "autonomy_level": autonomy,
        "auth_method": auth_method,
        "plan_cap_usd": plan_cap_usd,
        "subscription_pools": req.subscription_pools or [],
    }


@router.post("/v1/agents/heartbeat")
async def heartbeat(req: AgentHeartbeat):
    now = time.time()
    status = (req.status or "online").lower()
    if status not in VALID_AGENT_STATUSES:
        raise HTTPException(
            422,
            f"agent status must be one of {sorted(VALID_AGENT_STATUSES)}, got {status!r}",
        )
    # Storage via the HiveMindRepository ABC: metadata merge happens in the
    # storage layer; the caller owns the transaction + its commit (EPIMONE shape).
    async with hive_repo().connection() as tx:
        rowcount = await hive_repo().heartbeat_agent(
            tx,
            urn=req.urn,
            ts=now,
            status=status,
            metadata_patch=req.metadata if req.metadata is not None else None,
        )
        if rowcount:
            await tx.commit()
    if rowcount == 0:
        # 409 (not 404): the worker must (re)register. hive_worker.py's main loop
        # re-registers on 409, so a worker whose URN the bus dropped self-heals
        # instead of hammering 404s forever (2026-10-16: hydra zombie worker).
        raise HTTPException(409, f"agent not registered: {req.urn}")
    return {"ack": True, "ts": now}


def _authorize_deregister(urn: str, actor: str | None, authorization: str | None, token: str | None) -> None:
    """Admin tier, or the agent itself (X-Hive-Actor == urn). Else 401/403."""
    if is_admin_tier(authorization):
        return
    actor = (actor or "").strip()
    if not actor:
        raise HTTPException(401, "X-Hive-Actor header (or the admin token) is required to deregister")
    if actor != urn:
        raise HTTPException(403, "only the admin tier or the agent itself may deregister an agent")
    # Same shared-secret layer as every other mutating route; the agent may be
    # offline already (a repeat call), so it is NOT required to be 'active'.
    secret = os.environ.get("HIVE_SHARED_SECRET")
    if secret and (not token or not hmac.compare_digest(str(token), str(secret))):
        raise HTTPException(401, "missing or incorrect X-Hive-Token")


@router.post("/v1/agents/{urn_path:path}/deregister")
async def deregister_agent(
    urn_path: str,
    req: AgentDeregister | None = None,
    x_hive_actor: str | None = Header(default=None, alias="X-Hive-Actor"),
    x_hive_token: str | None = Header(default=None, alias="X-Hive-Token"),
    authorization: str | None = Header(default=None),
):
    """Take an agent offline with a recorded reason. Idempotent.

    Allowed for the admin tier or the agent itself. The record lives in the
    agent's metadata under ``deregistered`` (no schema change). Repeating the
    call on an agent already offline WITH that record returns the same record
    and writes nothing: no second event, no second job release."""
    _authorize_deregister(urn_path, x_hive_actor, authorization, x_hive_token)
    reason = ((req.reason if req else None) or "").strip() or "deregistered"
    repo = hive_repo()
    async with repo.connection() as tx:
        for _attempt in range(3):
            async with tx.execute(
                "SELECT status, last_heartbeat, metadata FROM agents WHERE urn=?", (urn_path,)
            ) as cur:
                row = await cur.fetchone()
            if not row:
                raise HTTPException(404, f"agent not found: {urn_path}")
            status, last_hb, raw_meta = row
            try:
                meta = json.loads(raw_meta) if raw_meta else {}
            except Exception:
                meta = {}
            if not isinstance(meta, dict):
                meta = {}
            prior = meta.get("deregistered")
            if status == "offline" and isinstance(prior, dict):
                return {"ok": True, "urn": urn_path, "status": "offline", **prior}
            record = {
                "reason": reason,
                "deregistered_at": time.time(),
                "deregistered_by": "admin" if is_admin_tier(authorization) else urn_path,
            }
            meta["deregistered"] = record
            # Compare-and-set on the state we read, so two racing calls cannot
            # both win (and both emit): the loser re-reads and returns the record.
            cur = await tx.execute(
                "UPDATE agents SET status='offline', metadata=? WHERE urn=? AND status=? AND last_heartbeat=?",
                (json.dumps(meta, default=str), urn_path, status, last_hb),
            )
            if (getattr(cur, "rowcount", 0) or 0) != 1:
                continue
            released = await repo.release_unstarted_jobs_for_agent(tx, urn_path)
            for job_id in released:
                await emit_event(
                    tx,
                    "job.unclaimed",
                    {"id": job_id, "prior_claimer": urn_path, "reason": "agent_deregistered"},
                    commit=False,
                )
            await emit_event(
                tx,
                "agent.offline",
                {"urn": urn_path, "reason": "deregistered", "detail": reason},
            )
            return {"ok": True, "urn": urn_path, "status": "offline", **record}
    raise HTTPException(409, f"agent {urn_path} changed concurrently; retry")


@router.get("/v1/agents")
async def list_agents(
    status: str | None = None,
    kind: str | None = None,
    host: str | None = None,
    runtime: str | None = None,
    pid: int | None = None,
    cost_tier: str | None = None,
    include_offline: bool = False,
):
    if status and status not in VALID_AGENT_STATUSES:
        raise HTTPException(
            422,
            f"status must be one of {sorted(VALID_AGENT_STATUSES)}, got {status!r}",
        )
    # Storage via the HiveMindRepository ABC on a transaction (EPIMONE shape).
    async with hive_repo().connection() as tx:
        rows = await hive_repo().list_agents(
            tx,
            status=status,
            kind=kind,
            host=host,
            runtime=runtime,
            pid=pid,
            cost_tier=cost_tier,
            include_offline=include_offline,
        )
    for r in rows:
        # build display name: kind@host[pid] cwd:cwd_basename — distinguishes multiple sessions on same host
        meta = r.get("metadata") or {}
        cwd = meta.get("cwd") if isinstance(meta, dict) else None
        cwd_short = cwd.split("/")[-1] if cwd else None
        display = f"{r.get('kind')}@{r.get('host')}"
        if r.get("pid") is not None:
            display += f"[pid={r.get('pid')}]"
        if cwd_short:
            display += f" cwd={cwd_short}"
        r["display"] = display
    return {"count": len(rows), "agents": rows}


@router.get("/v1/agents/stats")
async def agent_stats():
    counts = {status: 0 for status in VALID_AGENT_STATUSES}
    async with (
        connect_db() as db,
        db.execute("SELECT status, COUNT(*) FROM agents GROUP BY status") as cur,
    ):
        async for status, count in cur:
            counts[status] = count
    total = sum(counts.values())
    return {
        "online": counts.get("online", 0),
        "stale": counts.get("stale", 0),
        "offline": counts.get("offline", 0),
        "total": total,
    }


@router.get("/v1/hosts")
async def list_hosts(include_stale: bool = False):
    """Aggregate view of all system-watcher agents: specs + current load.

    Returns one entry per host (latest system agent wins if multiple).
    Agents that last heartbeated > 60s ago are flagged as stale unless
    include_stale=true, in which case all are returned.
    Used by workers to make resource-aware routing decisions.

    Also the single source of truth for host ADDRESSES: `ip` is the routable
    LAN address a peer SSHes to, `addresses` lists every reachable IPv4, and
    `network` carries the raw block (ipv6, all_addresses). The system watcher
    reports these, so an agent can refresh its own infrastructure notes from
    this response instead of sweeping the segment and guessing.
    """
    cutoff = time.time() - 60  # 60s stale threshold
    sql = (
        "SELECT urn, host, status, last_heartbeat, capabilities, metadata, subscription_pools "
        "FROM agents WHERE kind='system'"
    )
    if not include_stale:
        sql += " AND last_heartbeat >= ?"
        args: list = [cutoff]
    else:
        args = []
    sql += " ORDER BY last_heartbeat DESC"
    seen_hosts: dict[str, dict] = {}
    async with connect_db() as db, db.execute(sql, args) as cur:
        async for r in cur:
            h = r[1]
            if h in seen_hosts:
                continue  # already have a newer entry for this host
            meta = json.loads(r[5]) if r[5] else {}
            specs = meta.get("specs", {})
            load = meta.get("load", {})
            age_s = time.time() - (r[3] or 0)
            # Addresses arrive in specs at registration and are refreshed in load on
            # every heartbeat; prefer load so a DHCP change is picked up promptly.
            net = load.get("network") or specs.get("network") or {}
            seen_hosts[h] = {
                "ip": net.get("primary_ip"),
                "addresses": net.get("addresses") or [],
                "network": net,
                "host": h,
                "urn": r[0],
                "status": r[2],
                "last_heartbeat": r[3],
                "age_s": round(age_s, 1),
                "stale": age_s > 60,
                "capabilities": json.loads(r[4]) if r[4] else [],
                "subscription_pools": json.loads(r[6]) if len(r) > 6 and r[6] else [],
                # static specs (from registration)
                "cpu_model": specs.get("cpu_model"),
                "cpu_threads": specs.get("cpu_threads") or specs.get("cpu_count"),
                "ram_gb": specs.get("ram_gb"),
                "os": specs.get("os"),
                "arch": specs.get("arch"),
                "gpus": specs.get("gpus", []),
                "has_docker": specs.get("has_docker"),
                "has_podman": specs.get("has_podman"),
                "has_npu": specs.get("has_npu", False),
                # dynamic load (updated each heartbeat)
                "load_1min": load.get("load_1min"),
                "load_5min": load.get("load_5min"),
                "ram_used_pct": load.get("ram_used_pct"),
                "srv_free_gb": load.get("srv_free_gb"),
                "uptime_sec": load.get("uptime_sec"),
                # Multi-volume + cpu utilization (added 2026-05-26)
                "volumes": load.get("volumes") or [],
                "cpu_used_pct": load.get("cpu_used_pct"),
                "gpus_runtime": load.get("gpus_runtime") or [],
            }
    hosts = sorted(seen_hosts.values(), key=lambda x: x["host"])
    return {"count": len(hosts), "hosts": hosts}


@router.get("/v1/agents/{urn_path:path}/throttle")
async def agent_throttle(urn_path: str):
    """Inspect an agent's plan-cap throttle state."""
    async with (
        connect_db() as db,
        db.execute(
            "SELECT urn, kind, runtime, auth_method, plan_cap_usd, plan_period_used_usd FROM agents WHERE urn=?",
            (urn_path,),
        ) as cur,
    ):
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, f"agent not found: {urn_path}")
    used = row[5] or 0
    cap = row[4] or 0
    pct = (100 * used / cap) if cap else 0
    return {
        "urn": row[0],
        "kind": row[1],
        "runtime": row[2],
        "auth_method": row[3],
        "plan_cap_usd": cap,
        "plan_period_used_usd": round(used, 4),
        "plan_period_pct": round(pct, 1),
        "throttled": row[3] == "subscription" and pct >= 85.0,
        "headroom_pct": THROTTLE_HEADROOM * 100,
    }


@router.post("/v1/agents/{urn_path:path}/plan-reset")
async def reset_plan_period(urn_path: str):
    """Operator zeros the MTD usage — call monthly on billing rollover."""
    async with connect_db() as db:
        cur = await db.execute("UPDATE agents SET plan_period_used_usd = 0 WHERE urn=?", (urn_path,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, f"agent not found: {urn_path}")
    return {"ok": True, "urn": urn_path, "reset_at": time.time()}


@router.get("/v1/agents/whoami")
async def whoami(host: str, pid: int):
    """Help a session find ITS OWN urn by (host, pid) — most recent online registration wins.
    Useful when a session forgets its urn after restart and needs to re-discover its identity."""
    async with connect_db() as db:
        async with db.execute(
            "SELECT urn, kind, runtime, model, provider, cost_tier, autonomy_level, started_at "
            "FROM agents WHERE host=? AND pid=? AND status='online' "
            "ORDER BY started_at DESC LIMIT 1",
            (host, pid),
        ) as cur:
            r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"no online agent matches host={host} pid={pid}")
    return {
        "urn": r[0],
        "kind": r[1],
        "runtime": r[2],
        "model": r[3],
        "provider": r[4],
        "cost_tier": r[5],
        "autonomy_level": r[6],
        "started_at": r[7],
    }
