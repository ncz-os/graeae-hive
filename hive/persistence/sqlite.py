"""SQLite Hive Mind repository."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

try:
    import aiosqlite
except ModuleNotFoundError:
    from . import _sqlite_async as aiosqlite

from .base import HiveMindRepository, Transaction

NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}
NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")


class SqliteHiveRepository(HiveMindRepository):
    def __init__(self, db_path: str, *, busy_timeout_ms: int = 5000) -> None:
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms

    async def init(self, schema: str | None = None) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        async with self.connection() as db:
            if schema:
                await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
                await db.executescript(schema)
            await self._ensure_column(db, "jobs", "dedup_hash", "dedup_hash TEXT")
            await self._ensure_column(db, "jobs", "decline_count", "decline_count INTEGER NOT NULL DEFAULT 0")
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_dedup_uq ON jobs (dedup_hash) "
                "WHERE dedup_hash IS NOT NULL AND status IN ('queued', 'offered', 'claimed', 'running')"
            )
            await db.commit()

    async def close(self) -> None:
        return None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Transaction]:
        db = await aiosqlite.connect(
            self.db_path,
            timeout=self.busy_timeout_ms / 1000,
            check_same_thread=False,
        )
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            yield db
        finally:
            await db.close()

    async def _ensure_column(self, db: Any, table: str, col: str, ddl_fragment: str) -> None:
        try:
            cursor = await db.execute(f"PRAGMA table_info({table})")
            cols = {row[1] for row in await cursor.fetchall()}
        except Exception:
            return
        if col not in cols:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")

    async def emit_event(
        self,
        tx: Transaction,
        *,
        ts: float,
        kind: str,
        payload_json: str,
        agent_urn: str | None,
    ) -> None:
        await tx.execute(
            "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
            (ts, kind, payload_json, agent_urn),
        )

    async def register_agent(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, runtime, model, provider, cost_tier, autonomy_level, "
                "auth_method, plan_cap_usd, plan_period_used_usd, subscription_pools, host, session_id, "
                "pid, capabilities, version, started_at, last_heartbeat, status, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 'online', ?)",
                (
                    fields["urn"],
                    fields["kind"],
                    fields["runtime"],
                    fields["model"],
                    fields["provider"],
                    fields["cost_tier"],
                    fields["autonomy_level"],
                    fields["auth_method"],
                    fields["plan_cap_usd"],
                    fields.get("subscription_pools_json"),
                    fields["host"],
                    fields["session_id"],
                    fields.get("pid"),
                    fields.get("capabilities_json"),
                    fields.get("version"),
                    fields["started_at"],
                    fields["last_heartbeat"],
                    fields.get("metadata_json"),
                ),
            )
            await db.commit()

    async def heartbeat_agent(
        self,
        *,
        urn: str,
        ts: float,
        status: str,
        metadata_json: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> int:
        async with self.connection() as db:
            # A patch is merged with the stored metadata INSIDE the storage layer
            # (one connection) so read-merge-write is atomic — the endpoint no
            # longer needs a raw SELECT. Mirrors the old endpoint merge idiom.
            if metadata_patch is not None:
                async with db.execute(
                    "SELECT metadata FROM agents WHERE urn=?", (urn,)
                ) as meta_cur:
                    meta_row = await meta_cur.fetchone()
                if not meta_row:
                    return 0
                try:
                    existing = json.loads(meta_row[0]) if meta_row[0] else {}
                except Exception:
                    existing = {}
                if not isinstance(existing, dict):
                    existing = {}
                existing.update(metadata_patch)
                metadata_json = json.dumps(existing, default=str)
            if metadata_json is not None:
                cur = await db.execute(
                    "UPDATE agents SET last_heartbeat=?, status=?, metadata=? WHERE urn=?",
                    (ts, status, metadata_json, urn),
                )
            else:
                cur = await db.execute(
                    "UPDATE agents SET last_heartbeat=?, status=? WHERE urn=?",
                    (ts, status, urn),
                )
            await db.commit()
            return cur.rowcount

    async def require_agent(self, *, urn: str) -> tuple[Any, ...] | None:
        async with self.connection() as db:
            async with db.execute("SELECT runtime, status FROM agents WHERE urn=?", (urn,)) as cur:
                return await cur.fetchone()

    async def list_agents(self, **filters: Any) -> list[dict[str, Any]]:
        sql = (
            "SELECT urn, kind, host, status, last_heartbeat, capabilities, version, metadata, "
            "pid, runtime, model, provider, cost_tier, autonomy_level FROM agents WHERE 1=1"
        )
        args: list[Any] = []
        if filters.get("status"):
            sql += " AND status=?"
            args.append(filters["status"])
        elif not filters.get("include_offline", False):
            sql += " AND status='online'"
        for key in ("kind", "host", "runtime", "pid", "cost_tier"):
            if filters.get(key) is not None:
                sql += f" AND {key}=?"
                args.append(filters[key])
        sql += " ORDER BY last_heartbeat DESC"
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(sql, args) as cur:
                async for r in cur:
                    rows.append(
                        {
                            "urn": r[0],
                            "kind": r[1],
                            "host": r[2],
                            "status": r[3],
                            "last_heartbeat": r[4],
                            "capabilities": json.loads(r[5]) if r[5] else None,
                            "version": r[6],
                            "metadata": json.loads(r[7]) if r[7] else {},
                            "pid": r[8],
                            "runtime": r[9],
                            "model": r[10],
                            "provider": r[11],
                            "cost_tier": r[12],
                            "autonomy_level": r[13],
                        }
                    )
        return rows

    async def submit_job(self, **fields: Any) -> None:
        async with self.connection() as db:
            try:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
                    "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, "
                    "preferred_models, mnemos_refs, depends_on, max_retries, dedup_hash, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)",
                    (
                        fields["id"],
                        fields["submitter_urn"],
                        fields.get("parent_job_id"),
                        fields["kind"],
                        fields.get("description"),
                        fields.get("priority", 0),
                        fields.get("deadline"),
                        fields.get("required_capabilities_json"),
                        fields.get("eligible_kinds_json"),
                        fields.get("eligible_hosts_json"),
                        fields.get("project"),
                        fields.get("max_cost_tier"),
                        fields.get("preferred_providers_json"),
                        fields.get("preferred_models_json"),
                        fields.get("mnemos_refs_json"),
                        fields.get("depends_on_json"),
                        int(fields.get("max_retries", 2)),
                        fields.get("dedup_hash"),
                        fields["started_at"],
                    ),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def claim_next_job(
        self,
        *,
        agent_urn: str,
        now: float,
        active_statuses: set[str],
        claim_lease_seconds: float,
        host_denylist: set[str],
        match_capabilities: Any,
        kind_aliases: Any,
        json_list: Any,
        cost_tiers: list[str],
        throttle_headroom: float,
    ) -> dict[str, Any] | None:
        async with self.connection() as db:
            async with db.execute(
                "SELECT kind, capabilities, runtime, model, provider, cost_tier, auth_method, "
                "plan_cap_usd, plan_period_used_usd, status FROM agents WHERE urn=?",
                (agent_urn,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return {"error": "not_registered"}
            (
                agent_kind,
                caps_json,
                a_runtime,
                a_model,
                a_provider,
                a_tier,
                a_auth,
                a_cap,
                a_used,
                agent_status,
            ) = row
            if agent_status not in active_statuses:
                return {"error": "inactive", "status": agent_status}
            urn_parts = agent_urn.split(":")
            urn_host = (urn_parts[3] if len(urn_parts) >= 4 else "").lower()
            if urn_host in host_denylist:
                return {"error": "host_denylisted", "host": urn_host}
            agent_caps = set(json.loads(caps_json)) if caps_json else set()
            eligible_aliases = kind_aliases(agent_kind, a_runtime)
            a_tier = (a_tier or "C").upper()
            if a_tier not in cost_tiers:
                a_tier = "C"
            a_auth = (a_auth or "unknown").lower()
            sub_throttled = a_auth == "subscription" and a_cap and a_used and a_used >= throttle_headroom * a_cap

            await db.execute("BEGIN IMMEDIATE")
            committed = False
            try:
                async with db.execute(
                    "SELECT id, kind, description, priority, deadline, required_capabilities, eligible_kinds, "
                    "eligible_hosts, submitter_urn, parent_job_id, started_at, max_cost_tier, preferred_providers, "
                    "preferred_models, mnemos_refs, depends_on FROM jobs WHERE status='queued' "
                    "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?) "
                    "ORDER BY priority DESC, started_at ASC",
                    (now,),
                ) as cur:
                    async for r in cur:
                        (
                            job_id,
                            j_kind,
                            j_desc,
                            j_prio,
                            j_dead,
                            j_caps_json,
                            _j_kinds_json,
                            _j_hosts_json,
                            j_sub,
                            j_par,
                            j_started,
                            j_max_tier,
                            j_pref_providers,
                            j_pref_models,
                            j_mnemos_refs,
                            j_deps_json,
                        ) = r
                        if urn_host in NARROW_HOSTS and not (j_kind or "").startswith(NARROW_ALLOWLIST):
                            continue
                        if j_deps_json:
                            deps = json_list(j_deps_json)
                            if deps:
                                ph = ",".join("?" * len(deps))
                                async with db.execute(
                                    f"SELECT COUNT(*) FROM jobs WHERE id IN ({ph}) AND status='done'",
                                    tuple(deps),
                                ) as dc:
                                    done_count = (await dc.fetchone())[0]
                                if done_count < len(deps):
                                    continue
                        if _j_kinds_json:
                            kinds = set(json_list(_j_kinds_json))
                            if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
                                continue
                        if _j_hosts_json:
                            hosts = set(json_list(_j_hosts_json))
                            host_lc = {h.lower() for h in hosts}
                            agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()
                            if hosts and "*" not in hosts and agent_host not in host_lc:
                                continue
                        if j_caps_json:
                            labels = json_list(j_caps_json)
                            if labels and not match_capabilities(labels, agent_caps).eligible:
                                continue
                        job_max_tier = (j_max_tier or "B").upper()
                        if job_max_tier not in cost_tiers:
                            continue
                        if a_tier not in cost_tiers:
                            continue
                        if cost_tiers.index(a_tier) > cost_tiers.index(job_max_tier):
                            continue
                        if sub_throttled and job_max_tier != "A":
                            continue
                        if j_pref_providers:
                            provs = json.loads(j_pref_providers)
                            if provs and a_provider not in provs:
                                continue
                        if j_pref_models:
                            models = json.loads(j_pref_models)
                            if models and a_model not in models:
                                continue
                        claim_cur = await db.execute(
                            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                            "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
                            "claim_lease_expires_at=? WHERE id=? AND status='queued'",
                            (
                                agent_urn,
                                now,
                                a_runtime,
                                a_model,
                                a_provider,
                                a_tier,
                                now + claim_lease_seconds,
                                job_id,
                            ),
                        )
                        if (getattr(claim_cur, "rowcount", 0) or 0) < 1:
                            continue
                        await db.execute("COMMIT")
                        committed = True
                        return {
                            "id": job_id,
                            "kind": j_kind,
                            "description": j_desc,
                            "priority": j_prio,
                            "deadline": j_dead,
                            "max_cost_tier": j_max_tier,
                            "submitter_urn": j_sub,
                            "parent_job_id": j_par,
                            "claimed_at": now,
                            "queued_at": j_started,
                            "mnemos_refs": json.loads(j_mnemos_refs) if j_mnemos_refs else [],
                            "claimed_resources": {
                                "runtime": a_runtime,
                                "model": a_model,
                                "provider": a_provider,
                                "cost_tier": a_tier,
                            },
                        }
                await db.execute("COMMIT")
                committed = True
                return None
            finally:
                if not committed:
                    await db.execute("ROLLBACK")

    async def patch_job_status(self, **fields: Any) -> int:
        """Backend-neutral UPDATE of one job row.

        fields: job_id (required) + sets (dict of column -> value; None renders
        SQL NULL). Returns rowcount. Callers own the business logic; the facade
        owns the dialect. `sets` keys are column names (never caller input) and
        every VALUE is bound.
        """
        job_id = fields["job_id"]
        sets: dict[str, Any] = dict(fields.get("sets") or {})
        if not sets:
            return 0
        assign = ", ".join(f"{col}=?" for col in sets)
        args = list(sets.values())
        args.append(job_id)
        async with self.connection() as db:
            cur = await db.execute(f"UPDATE jobs SET {assign} WHERE id=?", args)
            await db.commit()
            return cur.rowcount

    async def get_job(self, *, job_id: str) -> tuple[Any, ...] | None:
        async with self.connection() as db:
            async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
                return await cur.fetchone()

    async def list_jobs(self, **filters: Any) -> tuple[int, list[dict[str, Any]]]:
        """Backend-neutral job listing (GET /v1/jobs).

        Filters: status, agent_urn, parent_job_id, since, limit, include_result.
        Returns (total_count, rows). Sorting: terminal statuses by ended/started
        DESC; otherwise recency-first with priority as tiebreak.
        """
        status = filters.get("status")
        agent_urn = filters.get("agent_urn")
        parent_job_id = filters.get("parent_job_id")
        since = filters.get("since")
        limit = int(filters.get("limit") or 100)
        include_result = bool(filters.get("include_result", True))
        col_sql = (
            "SELECT id, submitter_urn, parent_job_id, kind, description, priority, status, "
            "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
            "required_capabilities, eligible_kinds, eligible_hosts, site, repo, head_branch, "
            "pr_number, max_retries, retry_count FROM jobs WHERE 1=1"
        )
        cnt_sql = "SELECT COUNT(*) FROM jobs WHERE 1=1"
        args: list[Any] = []
        cnt_args: list[Any] = []
        if status:
            col_sql += " AND status=?"
            cnt_sql += " AND status=?"
            args.append(status)
            cnt_args.append(status)
        if agent_urn:
            col_sql += " AND (submitter_urn=? OR claimed_by=?)"
            cnt_sql += " AND (submitter_urn=? OR claimed_by=?)"
            args.extend([agent_urn, agent_urn])
            cnt_args.extend([agent_urn, agent_urn])
        if parent_job_id:
            col_sql += " AND parent_job_id=?"
            cnt_sql += " AND parent_job_id=?"
            args.append(parent_job_id)
            cnt_args.append(parent_job_id)
        if since:
            col_sql += " AND started_at >= ?"
            cnt_sql += " AND started_at >= ?"
            args.append(since)
            cnt_args.append(since)
        terminal = {"done", "failed", "cancelled", "dead-letter"}
        if status in terminal:
            col_sql += " ORDER BY COALESCE(ended_at, started_at) DESC LIMIT ?"
        else:
            col_sql += " ORDER BY COALESCE(ended_at, started_at) DESC, priority DESC LIMIT ?"
        args.append(limit)
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(cnt_sql, cnt_args) as cur:
                r = await cur.fetchone()
                total = r[0] if r else 0
            async with db.execute(col_sql, args) as cur:
                async for r in cur:
                    job = {
                        "id": r[0],
                        "submitter_urn": r[1],
                        "parent_job_id": r[2],
                        "kind": r[3],
                        "description": r[4],
                        "priority": r[5],
                        "status": r[6],
                        "claimed_by": r[7],
                        "started_at": r[8],
                        "ended_at": r[9],
                        "result": json.loads(r[10]) if r[10] else None,
                        "estimated_cost_usd": r[11],
                        "required_capabilities": json.loads(r[12]) if r[12] else None,
                        "eligible_kinds": json.loads(r[13]) if r[13] else None,
                        "eligible_hosts": json.loads(r[14]) if r[14] else None,
                        "site": r[15],
                        "repo": r[16],
                        "head_branch": r[17],
                        "pr_number": r[18],
                        "max_retries": r[19],
                        "retry_count": r[20],
                    }
                    if not include_result:
                        job.pop("result", None)
                    rows.append(job)
        return total, rows

    async def find_active_dedup_job(self, *, dedup_hash: str) -> tuple[Any, ...] | None:
        async with (
            self.connection() as db,
            db.execute(
                "SELECT id, started_at FROM jobs WHERE dedup_hash=? "
                "AND status IN ('queued', 'offered', 'claimed', 'running') "
                "ORDER BY started_at ASC LIMIT 1",
                (dedup_hash,),
            ) as cur,
        ):
            return await cur.fetchone()

    async def post_message(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO messages (id, from_urn, to_urn, in_reply_to, topic, payload, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    fields["id"],
                    fields["from_urn"],
                    fields.get("to_urn"),
                    fields.get("in_reply_to"),
                    fields["topic"],
                    fields["payload_json"],
                    fields["ts"],
                ),
            )
            await db.commit()

    async def list_messages(self, **filters: Any) -> list[dict[str, Any]]:
        """Backend-neutral message listing (GET /v1/messages)."""
        to_urn = filters.get("to_urn")
        topic = filters.get("topic")
        since = filters.get("since")
        limit = int(filters.get("limit") or 100)
        sql = "SELECT id, from_urn, to_urn, in_reply_to, topic, payload, ts FROM messages WHERE 1=1"
        args: list[Any] = []
        if to_urn:
            sql += " AND (to_urn=? OR to_urn IS NULL)"
            args.append(to_urn)
        if topic:
            sql += " AND topic LIKE ?"
            args.append(topic.replace("*", "%"))
        if since:
            sql += " AND ts >= ?"
            args.append(since)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        rows: list[dict[str, Any]] = []
        async with self.connection() as db, db.execute(sql, args) as cur:
            async for r in cur:
                rows.append(
                    {
                        "id": r[0],
                        "from": r[1],
                        "to": r[2],
                        "in_reply_to": r[3],
                        "topic": r[4],
                        "payload": json.loads(r[5]),
                        "ts": r[6],
                    }
                )
        return rows

    async def tail_events(self, *, since_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
        args: list[Any] = []
        if since_id is not None:
            sql += " WHERE id > ?"
            args.append(since_id)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(limit)
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(sql, args) as cur:
                async for r in cur:
                    rows.append(
                        {
                            "id": r[0],
                            "ts": r[1],
                            "kind": r[2],
                            "payload": json.loads(r[3]),
                            "agent_urn": r[4],
                        }
                    )
        return rows

    async def cache_lookup(self, *, cache_key: str, cutoff: float) -> dict[str, Any] | None:
        async with self.connection() as db:
            async with db.execute(
                "SELECT result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at "
                "FROM hive_cache WHERE cache_key=? AND cached_at >= ?",
                (cache_key, cutoff),
            ) as cur:
                r = await cur.fetchone()
        if not r:
            return None
        return {
            "result": json.loads(r[0]) if r[0] else None,
            "source_job_id": r[1],
            "result_mnemos_id": r[2],
            "hit_count": r[3],
            "cost_saved_usd": r[4],
            "model": r[5],
            "provider": r[6],
            "cached_at": r[7],
        }

    async def cache_store(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO hive_cache (cache_key, result_json, source_job_id, result_mnemos_id, hit_count, "
                "cost_saved_usd, model, provider, cached_at, last_hit_at) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
                "ON CONFLICT(cache_key) DO UPDATE SET result_json=excluded.result_json, "
                "source_job_id=excluded.source_job_id, result_mnemos_id=excluded.result_mnemos_id, "
                "cached_at=excluded.cached_at, model=excluded.model, provider=excluded.provider",
                (
                    fields["cache_key"],
                    fields["result_json"],
                    fields.get("source_job_id"),
                    fields.get("result_mnemos_id"),
                    fields.get("model"),
                    fields.get("provider"),
                    fields["cached_at"],
                ),
            )
            await db.commit()

    async def cache_record_hit(self, *, cache_key: str, cost_saved_usd: float, ts: float) -> None:
        async with self.connection() as db:
            await db.execute(
                "UPDATE hive_cache SET hit_count=hit_count+1, cost_saved_usd=COALESCE(cost_saved_usd,0)+?, "
                "last_hit_at=? WHERE cache_key=?",
                (cost_saved_usd, ts, cache_key),
            )
            await db.commit()

    async def upsert_worker_kind_stats(self, **fields: Any) -> None:
        col = fields["column"]
        if col not in {"success_count", "fail_count", "cancelled_count"}:
            raise ValueError(f"invalid worker stats column: {col!r}")
        async with self.connection() as db:
            await db.execute(
                f"INSERT INTO worker_kind_stats (urn, kind, {col}, total_tokens_in, total_tokens_out, "
                f"total_cost_usd, total_duration_sec, last_run) VALUES (?, ?, 1, ?, ?, ?, ?, ?) "
                f"ON CONFLICT(urn, kind) DO UPDATE SET {col}={col}+1, "
                f"total_tokens_in=total_tokens_in+?, total_tokens_out=total_tokens_out+?, "
                f"total_cost_usd=total_cost_usd+?, total_duration_sec=total_duration_sec+?, last_run=?",
                (
                    fields["urn"],
                    fields["kind"],
                    fields.get("tokens_in", 0),
                    fields.get("tokens_out", 0),
                    fields.get("cost_usd", 0),
                    fields.get("duration_sec", 0),
                    fields["last_run"],
                    fields.get("tokens_in", 0),
                    fields.get("tokens_out", 0),
                    fields.get("cost_usd", 0),
                    fields.get("duration_sec", 0),
                    fields["last_run"],
                ),
            )
            await db.commit()

    async def create_schedule(self, **fields: Any) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO scheduled_jobs (id, name, created_by_urn, interval_seconds, job_template, enabled, "
                "last_fired_at, next_fire_at, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                (
                    fields["id"],
                    fields["name"],
                    fields["created_by_urn"],
                    fields["interval_seconds"],
                    fields["job_template_json"],
                    fields["enabled"],
                    fields["next_fire_at"],
                    fields["created_at"],
                ),
            )
            await db.commit()

    async def list_schedules(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        async with self.connection() as db:
            async with db.execute(
                "SELECT id, name, created_by_urn, interval_seconds, enabled, last_fired_at, next_fire_at, fire_count, created_at "
                "FROM scheduled_jobs ORDER BY next_fire_at ASC"
            ) as cur:
                async for r in cur:
                    rows.append(
                        {
                            "id": r[0],
                            "name": r[1],
                            "created_by_urn": r[2],
                            "interval_seconds": r[3],
                            "enabled": bool(r[4]),
                            "last_fired_at": r[5],
                            "next_fire_at": r[6],
                            "fire_count": r[7],
                            "created_at": r[8],
                        }
                    )
        return rows
