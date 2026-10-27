"""SQLite Hive Mind repository."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

try:
    import aiosqlite
except ModuleNotFoundError:
    from . import _sqlite_async as aiosqlite

from .base import HiveMindRepository, Transaction


@dataclass(frozen=True)
class SqlExpr:
    """A raw SQL expression value for patch_job_status ``sets`` (code-side only).

    Lets a caller express ``col = col + 1`` / ``col = NULL`` / ``col = SYSTIMESTAMP``
    without string-building SQL in the router. The expression text is never
    caller input.
    """

    sql: str


class DependencyNotReady(RuntimeError):
    """Raised when a claim is attempted on a job whose ``depends_on`` are not
    all ``done``. Raised inside the caller's transaction so the claim path can
    skip the candidate (batch dequeue) or answer 409 (targeted claim)."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"job has unsatisfied DAG dependencies: {job_id}")
        self.job_id = job_id


NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}
NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")


DDL = """
-- Fresh-DB shape; matches what the code actually reads & writes
-- (graeae:schema-drift fix 2026-05-23). For live DBs the lifespan
-- migrator below issues idempotent ALTER/CREATE to converge.
CREATE TABLE IF NOT EXISTS agents (
  urn TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  host TEXT NOT NULL,
  session_id TEXT NOT NULL,
  pid INTEGER,
  capabilities TEXT,
  version TEXT,
  started_at REAL NOT NULL,
  last_heartbeat REAL NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('online','idle','stale','offline','error')),
  metadata TEXT,
  -- v0.2 cost/runtime/autonomy columns (graeae:schema-drift)
  runtime TEXT,
  model TEXT,
  provider TEXT,
  autonomy_level TEXT,
  cost_tier TEXT,
  current_load TEXT,
  auth_method TEXT,
  plan_cap_usd REAL,
  plan_period_used_usd REAL DEFAULT 0,
  subscription_pools TEXT
);
CREATE INDEX IF NOT EXISTS idx_agents_status      ON agents(status);
CREATE INDEX IF NOT EXISTS idx_agents_kind        ON agents(kind);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  submitter_urn TEXT NOT NULL,                  -- who created the work (user/human OR delegating agent)
  parent_job_id TEXT,
  kind TEXT NOT NULL,                           -- code-edit/research/review/build/test/etc
  description TEXT,
  priority INTEGER NOT NULL DEFAULT 0,          -- higher = more urgent
  deadline REAL,
  required_capabilities TEXT,                   -- json array; worker must have ALL
  eligible_kinds TEXT,                          -- json array; agent kinds eligible (null = any)
  eligible_hosts TEXT,                          -- json array; agent hosts eligible (null = any); e.g. ["cixmini"]
  site TEXT,                                    -- git_sites.json key (github/gitlab-ncz/codeberg/internal)
  repo TEXT,                                    -- owner/name path on that site
  head_branch TEXT,                             -- branch / PR head to check out
  base_branch TEXT,                             -- optional upstream branch head_branch forks off (worker clones base_branch then creates head_branch locally if head_branch doesn't exist upstream)
  branch_created_from TEXT,                     -- submit-time admission: base_branch the worker will create head_branch FROM (head_branch absent on origin, base present)
  pr_number INTEGER,                            -- PR/MR number (forge-specific ref scheme)
  project TEXT,                                 -- #10 FIX: separate project tag from capabilities (riskyeats/investorclaw/etc)
  status TEXT NOT NULL CHECK(status IN ('queued','offered','claimed','running','done','failed','failed_completion','cancelled','dead-letter','needs-review')),
  claimed_by TEXT,                              -- worker urn (set on claim/dequeue)
  claimed_at REAL,
  started_at REAL NOT NULL,                     -- when job ENTERED queue
  ended_at REAL,
  result TEXT,
  -- v0.2 cost/autonomy/retry columns (graeae:schema-drift)
  required_autonomy TEXT,
  max_cost_tier TEXT,
  preferred_providers TEXT,                     -- json array
  preferred_models TEXT,                        -- json array
  claimed_runtime TEXT,
  claimed_model TEXT,
  claimed_provider TEXT,
  claimed_cost_tier TEXT,
  tokens_in INTEGER,
  tokens_out INTEGER,
  estimated_cost_usd REAL,
  mnemos_refs TEXT,                             -- json array of mem_* ids
  result_mnemos_id TEXT,
  required_resources TEXT,                      -- json
  claimed_host_caps TEXT,                       -- json
  tags TEXT,                                    -- json array
  depends_on TEXT,                              -- json array of job ids
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 2,
  retry_backoff_until REAL,
  last_update_at REAL,
  claim_lease_expires_at REAL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status            ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_submitter         ON jobs(submitter_urn);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by        ON jobs(claimed_by);
CREATE INDEX IF NOT EXISTS idx_jobs_parent            ON jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_queue             ON jobs(status, priority DESC, started_at ASC);

CREATE TABLE IF NOT EXISTS job_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  ts REAL NOT NULL,
  actor_urn TEXT,
  old_status TEXT,
  new_status TEXT,
  old_claimed_by TEXT,
  new_claimed_by TEXT,
  patch TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_audit_job_ts ON job_audit_log(job_id, ts);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  from_urn TEXT NOT NULL,
  to_urn TEXT,
  in_reply_to TEXT,
  topic TEXT NOT NULL,
  payload TEXT NOT NULL,
  ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_to    ON messages(to_urn);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
CREATE INDEX IF NOT EXISTS idx_messages_ts    ON messages(ts);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  agent_urn TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts    ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_kind  ON events(kind);
CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_urn);

-- v0.2 tables (graeae:schema-drift fix 2026-05-23)
CREATE TABLE IF NOT EXISTS hive_cache (
  cache_key TEXT PRIMARY KEY,
  result_json TEXT NOT NULL,
  source_job_id TEXT,
  result_mnemos_id TEXT,
  hit_count INTEGER NOT NULL DEFAULT 0,
  cost_saved_usd REAL NOT NULL DEFAULT 0,
  model TEXT,
  provider TEXT,
  cached_at REAL NOT NULL,
  last_hit_at REAL
);
CREATE INDEX IF NOT EXISTS idx_hive_cache_cached_at ON hive_cache(cached_at);

CREATE TABLE IF NOT EXISTS worker_kind_stats (
  urn TEXT NOT NULL,
  kind TEXT NOT NULL,
  success_count INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,
  cancelled_count INTEGER NOT NULL DEFAULT 0,
  total_tokens_in INTEGER NOT NULL DEFAULT 0,
  total_tokens_out INTEGER NOT NULL DEFAULT 0,
  total_cost_usd REAL NOT NULL DEFAULT 0,
  total_duration_sec REAL NOT NULL DEFAULT 0,
  last_run REAL,
  PRIMARY KEY (urn, kind)
);
CREATE INDEX IF NOT EXISTS idx_wkstats_kind ON worker_kind_stats(kind);

CREATE TABLE IF NOT EXISTS scheduled_jobs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_by_urn TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  job_template TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_fired_at REAL,
  next_fire_at REAL NOT NULL,
  fire_count INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sched_next ON scheduled_jobs(next_fire_at, enabled);

-- Per-service capacity ledger (Phase 5). Persisted so admission state survives a
-- bus restart and is shared; every write is a keyed SET-upsert (idempotent).
CREATE TABLE IF NOT EXISTS capacity_ledger (
  service TEXT PRIMARY KEY,
  in_flight INTEGER NOT NULL DEFAULT 0,
  tokens_used INTEGER NOT NULL DEFAULT 0,
  spent_usd REAL NOT NULL DEFAULT 0,
  limited_until REAL NOT NULL DEFAULT 0,
  window_start REAL NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL DEFAULT 0
);

-- Dedup keys for usage folds: a replayed telemetry report (same key) must not
-- double-count into the ledger. The UNIQUE key makes the fold idempotent.
CREATE TABLE IF NOT EXISTS capacity_usage (
  dedup_key TEXT PRIMARY KEY,
  service TEXT NOT NULL,
  tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL NOT NULL DEFAULT 0,
  ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cap_usage_service_ts ON capacity_usage(service, ts);
"""


class SqliteHiveRepository(HiveMindRepository):
    #: The reference dialect can express the reason prefix filter: case-
    #: insensitive prefix match on the result JSON text (LIKE is
    #: case-insensitive for ASCII in sqlite, so LOWER is unneeded — the
    #: postgres subclass adds its own explicit LOWER to be safe).
    list_jobs_sql_reason = True
    #: Backend lock clause for the auto-retry row read. SQLite serialises via the
    #: connection; Oracle/Db2 override this to add FOR UPDATE. Kept as a class
    #: attr so the dialect lives in the backend, never in the router.
    _retry_lock_clause = ""

    def __init__(self, db_path: str, *, busy_timeout_ms: int = 5000) -> None:
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms

    async def init(self, schema: str | None = None) -> None:
        """Create/converge the schema. Owns ALL sqlite DDL (Phase 1e): base
        schema, additive live-column migrations, status-CHECK rebuilds, and
        idempotent index re-creates. Called by the bus lifespan via repo.init()."""
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        async with self.connection() as db:
            await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            await db.executescript(schema or DDL)

            # graeae:schema-drift (2026-05-23): additive migrations for live DBs.
            # New-column indexes stay out of SCHEMA because CREATE INDEX fails on
            # old live tables until the corresponding ALTER TABLE has run.
            async def _ensure_column(conn, table: str, col: str, ddl_fragment: str) -> None:
                cursor = await conn.execute(f"PRAGMA table_info({table})")
                cols = {row[1] for row in await cursor.fetchall()}
                if col not in cols:
                    await conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")

            async def _ensure_agents_status_allows_stale(conn) -> None:
                async with conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='agents'") as cur:
                    row = await cur.fetchone()
                table_sql = row[0] if row else ""
                if "stale" in table_sql:
                    return
                cols = (
                    "urn",
                    "kind",
                    "host",
                    "session_id",
                    "pid",
                    "capabilities",
                    "version",
                    "started_at",
                    "last_heartbeat",
                    "status",
                    "metadata",
                    "runtime",
                    "model",
                    "provider",
                    "autonomy_level",
                    "cost_tier",
                    "current_load",
                    "auth_method",
                    "plan_cap_usd",
                    "plan_period_used_usd",
                    "subscription_pools",
                )
                col_list = ", ".join(cols)
                await conn.execute("ALTER TABLE agents RENAME TO agents__pre_stale_migration")
                await conn.execute(
                    "CREATE TABLE agents ("
                    "urn TEXT PRIMARY KEY, "
                    "kind TEXT NOT NULL, "
                    "host TEXT NOT NULL, "
                    "session_id TEXT NOT NULL, "
                    "pid INTEGER, "
                    "capabilities TEXT, "
                    "version TEXT, "
                    "started_at REAL NOT NULL, "
                    "last_heartbeat REAL NOT NULL, "
                    "status TEXT NOT NULL CHECK(status IN ('online','idle','stale','offline','error')), "
                    "metadata TEXT, "
                    "runtime TEXT, "
                    "model TEXT, "
                    "provider TEXT, "
                    "autonomy_level TEXT, "
                    "cost_tier TEXT, "
                    "current_load TEXT, "
                    "auth_method TEXT, "
                    "plan_cap_usd REAL, "
                    "plan_period_used_usd REAL DEFAULT 0, "
                    "subscription_pools TEXT)"
                )
                await conn.execute(
                    f"INSERT INTO agents ({col_list}) SELECT {col_list} FROM agents__pre_stale_migration"
                )
                await conn.execute("DROP TABLE agents__pre_stale_migration")

            async def _ensure_jobs_status_allows_dead_letter(conn) -> None:
                # Thrash fix (2026-06-02): older live jobs tables have a CHECK(status
                # IN (...)) that predates 'dead-letter'/'failed_completion'. SQLite
                # cannot ALTER a CHECK in place, so rebuild the table preserving its
                # (possibly drifted) columns + data. No-op when the constraint is
                # already permissive or absent (fresh DBs are created from SCHEMA,
                # which already lists dead-letter).
                async with conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'") as cur:
                    row = await cur.fetchone()
                table_sql = row[0] if row else ""
                if not table_sql:
                    return  # no jobs table yet (e.g. 0-byte/mid-recovery DB) — SCHEMA will create it
                if "CHECK" not in table_sql.upper() or "dead-letter" in table_sql:
                    return  # no status CHECK, or already permissive — nothing to do
                async with conn.execute("PRAGMA table_info(jobs)") as cur:
                    old_cols = [r[1] for r in await cur.fetchall()]
                await conn.execute("ALTER TABLE jobs RENAME TO jobs__pre_deadletter_migration")
                await conn.executescript(DDL)  # recreate jobs (+others, all IF NOT EXISTS) with new CHECK
                # Copy only columns common to old + recreated table — guards against
                # drift columns the recreated SCHEMA lacks (the live_columns ALTERs
                # below re-add any expected-but-missing ones afterwards).
                async with conn.execute("PRAGMA table_info(jobs)") as cur:
                    new_cols = {r[1] for r in await cur.fetchall()}
                common = [c for c in old_cols if c in new_cols]
                col_list = ", ".join(common)
                await conn.execute(
                    f"INSERT INTO jobs ({col_list}) SELECT {col_list} FROM jobs__pre_deadletter_migration"
                )
                await conn.execute("DROP TABLE jobs__pre_deadletter_migration")

            live_columns = (
                # agents — v0.2 cost/runtime/autonomy columns
                ("agents", "runtime", "runtime TEXT"),
                ("agents", "model", "model TEXT"),
                ("agents", "provider", "provider TEXT"),
                ("agents", "autonomy_level", "autonomy_level TEXT"),
                ("agents", "cost_tier", "cost_tier TEXT"),
                ("agents", "current_load", "current_load TEXT"),
                ("agents", "auth_method", "auth_method TEXT"),
                ("agents", "plan_cap_usd", "plan_cap_usd REAL"),
                ("agents", "plan_period_used_usd", "plan_period_used_usd REAL DEFAULT 0"),
                ("agents", "subscription_pools", "subscription_pools TEXT"),
                # jobs — project tag + v0.2 cost/autonomy/retry columns
                ("jobs", "project", "project TEXT"),
                ("jobs", "eligible_hosts", "eligible_hosts TEXT"),
                ("jobs", "required_autonomy", "required_autonomy TEXT"),
                ("jobs", "max_cost_tier", "max_cost_tier TEXT"),
                ("jobs", "preferred_providers", "preferred_providers TEXT"),
                ("jobs", "preferred_models", "preferred_models TEXT"),
                ("jobs", "claimed_runtime", "claimed_runtime TEXT"),
                ("jobs", "claimed_model", "claimed_model TEXT"),
                ("jobs", "claimed_provider", "claimed_provider TEXT"),
                ("jobs", "claimed_cost_tier", "claimed_cost_tier TEXT"),
                ("jobs", "tokens_in", "tokens_in INTEGER"),
                ("jobs", "tokens_out", "tokens_out INTEGER"),
                ("jobs", "estimated_cost_usd", "estimated_cost_usd REAL"),
                ("jobs", "mnemos_refs", "mnemos_refs TEXT"),
                ("jobs", "result_mnemos_id", "result_mnemos_id TEXT"),
                ("jobs", "required_resources", "required_resources TEXT"),
                ("jobs", "claimed_host_caps", "claimed_host_caps TEXT"),
                ("jobs", "tags", "tags TEXT"),
                ("jobs", "depends_on", "depends_on TEXT"),
                ("jobs", "retry_count", "retry_count INTEGER NOT NULL DEFAULT 0"),
                ("jobs", "max_retries", "max_retries INTEGER NOT NULL DEFAULT 2"),
                ("jobs", "retry_backoff_until", "retry_backoff_until REAL"),
                ("jobs", "last_update_at", "last_update_at REAL"),
                ("jobs", "claim_lease_expires_at", "claim_lease_expires_at REAL"),
                # Thrash fix (2026-06-02): per-job count of worker-decline requeues.
                ("jobs", "decline_count", "decline_count INTEGER NOT NULL DEFAULT 0"),
                ("jobs", "site", "site TEXT"),
                ("jobs", "repo", "repo TEXT"),
                ("jobs", "head_branch", "head_branch TEXT"),
                # base_branch (2026-10-21): optional upstream branch head_branch forks off.
                # Live HIVE_JOBS rows created before this migration have no value here;
                # hive_worker.py on 4d4fc85 already reads job.get("base_branch") so the
                # optional/nullable contract is the one it expects.
                ("jobs", "base_branch", "base_branch TEXT"),
                # branch_created_from (2026-10-25): the base_branch the worker will
                # create head_branch from, recorded at SUBMIT when head_branch is
                # absent on origin but base_branch exists (submit-time branch
                # admission). NULL means "nothing to create".
                ("jobs", "branch_created_from", "branch_created_from TEXT"),
                ("jobs", "pr_number", "pr_number INTEGER"),
            )
            # Run CHECK-constraint rebuilds BEFORE additive column ALTERs: the
            # rebuild recreates the table from SCHEMA (sans drift columns), then the
            # loop re-adds any missing columns (incl. decline_count) to the new table.
            # (SQLite-only path — Oracle/Db2 converge via their repository init().)
            await _ensure_jobs_status_allows_dead_letter(db)
            for table, col, ddl_fragment in live_columns:
                await _ensure_column(db, table, col, ddl_fragment)
            await _ensure_agents_status_allows_stale(db)

            # (Oracle decline_count / CK_HIVE_JOBS_STATUS DDL removed: it was a
            # superseded duplicate of OracleHiveRepository.init/_apply_migrations,
            # which Oracle now runs via repo.init() above.)

            # Idempotent index re-creates (in case live DB pre-dates an index).
            live_indexes = (
                "CREATE INDEX IF NOT EXISTS idx_agents_status      ON agents(status)",
                "CREATE INDEX IF NOT EXISTS idx_agents_kind        ON agents(kind)",
                "CREATE INDEX IF NOT EXISTS idx_agents_runtime     ON agents(runtime)",
                "CREATE INDEX IF NOT EXISTS idx_agents_model       ON agents(model)",
                "CREATE INDEX IF NOT EXISTS idx_agents_provider    ON agents(provider)",
                "CREATE INDEX IF NOT EXISTS idx_agents_cost_tier   ON agents(cost_tier)",
                "CREATE INDEX IF NOT EXISTS idx_agents_auth_method ON agents(auth_method)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_project           ON jobs(project)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_autonomy          ON jobs(required_autonomy)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_max_cost_tier     ON jobs(max_cost_tier)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_claimed_provider  ON jobs(claimed_provider)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_claimed_tier      ON jobs(claimed_cost_tier)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_backoff           ON jobs(retry_backoff_until)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_claim_lease       ON jobs(claim_lease_expires_at)",
                "CREATE TABLE IF NOT EXISTS job_audit_log ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, ts REAL NOT NULL, "
                "actor_urn TEXT, old_status TEXT, new_status TEXT, old_claimed_by TEXT, "
                "new_claimed_by TEXT, patch TEXT NOT NULL)",
                "CREATE INDEX IF NOT EXISTS idx_job_audit_job_ts ON job_audit_log(job_id, ts)",
            )
            for stmt in live_indexes:
                await db.execute(stmt)
            await db.commit()
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
            # Back-ref to the repository that opened this handle: bus helpers
            # that receive ONLY the tx (e.g. hive.bus.deps.release_dependencies)
            # resolve dialect methods on the owning backend instead of
            # re-resolving the module-global repo singleton, which a router
            # may have mocked to a different (temp) repository.
            db._repo = self  # type: ignore[attr-defined]
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

    async def register_agent(self, tx: Transaction, **fields: Any) -> None:
        db = tx
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
        tx: Transaction,
        *,
        urn: str,
        ts: float,
        status: str,
        metadata_json: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> int:
        db = tx
        # A patch is merged with the stored metadata INSIDE the storage layer
        # (one connection) so read-merge-write is atomic — the endpoint no
        # longer needs a raw SELECT. Mirrors the old endpoint merge idiom.
        if metadata_patch is not None:
            async with db.execute("SELECT metadata FROM agents WHERE urn=?", (urn,)) as meta_cur:
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

    async def require_agent(self, tx: Transaction, *, urn: str) -> tuple[Any, ...] | None:
        db = tx
        async with db.execute("SELECT runtime, status FROM agents WHERE urn=?", (urn,)) as cur:
            return await cur.fetchone()

    async def list_agents(self, tx: Transaction, **filters: Any) -> list[dict[str, Any]]:
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
        db = tx
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

    async def dedup_agent_instance(
        self, tx: Transaction, *, host: str, kind: str, instance: str, keep_urn: str
    ) -> list[str]:
        """Best-effort removal of stale prior agent rows for a stable instance.

        Select-then-filter in Python (metadata is a CLOB normalized to str on
        Oracle) so the path is identical on every backend. NEVER deletes a row
        that currently holds an in-flight job claim (so a stale duplicate can't
        orphan a running job). Returns the URNs deleted. A failure here must
        never fail a register (the caller catches it).
        """
        db = tx
        async with db.execute("SELECT urn, metadata FROM agents WHERE host=? AND kind=?", (host, kind)) as dup_cur:
            dup_rows = await dup_cur.fetchall()
        stale: list[str] = []
        for d_row in dup_rows or []:
            try:
                d_urn = d_row[0]
                d_meta = d_row[1] if len(d_row) > 1 else None
            except (IndexError, TypeError):
                continue
            if d_urn == keep_urn:
                continue
            if isinstance(d_meta, dict):
                m = d_meta
            else:
                if isinstance(d_meta, (bytes, bytearray)):
                    try:
                        d_meta = d_meta.decode("utf-8", errors="replace")
                    except Exception:
                        d_meta = None
                try:
                    m = json.loads(d_meta) if d_meta else {}
                except Exception:
                    m = {}
            if isinstance(m, dict) and m.get("_instance") == instance:
                stale.append(d_urn)
        for s_urn in stale:
            await db.execute(
                "DELETE FROM agents WHERE urn=? "
                "AND ? NOT IN (SELECT claimed_by FROM jobs "
                "WHERE claimed_by IS NOT NULL AND status IN ('offered','claimed','running'))",
                (s_urn, s_urn),
            )
        return stale

    async def submit_job(self, tx: Transaction, **fields: Any) -> None:
        """Insert one job row.

        Defaults to a fresh ``queued`` row; callers may pass
        ``status``/``result_json``/``ended_at``/``claimed_provider``/``claimed_model``/
        ``claimed_cost_tier``/``estimated_cost_usd``/``result_mnemos_id`` (the
        cache-hit path inserts an already-``done`` row). No commit here — the
        caller owns the transaction (EPIMONE shape).
        """
        db = tx
        await db.execute(
            "INSERT INTO jobs (id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
            "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, preferred_providers, "
            "preferred_models, mnemos_refs, depends_on, max_retries, dedup_hash, site, repo, head_branch, base_branch, branch_created_from, pr_number, "
            "status, started_at, ended_at, result, claimed_provider, claimed_model, claimed_cost_tier, "
            "estimated_cost_usd, result_mnemos_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                fields.get("site"),
                fields.get("repo"),
                fields.get("head_branch"),
                fields.get("base_branch"),
                fields.get("branch_created_from"),
                fields.get("pr_number"),
                fields.get("status", "queued"),
                fields["started_at"],
                fields.get("ended_at"),
                fields.get("result_json"),
                fields.get("claimed_provider"),
                fields.get("claimed_model"),
                fields.get("claimed_cost_tier"),
                fields.get("estimated_cost_usd"),
                fields.get("result_mnemos_id"),
            ),
        )

    async def claim_next_job(
        self,
        tx: Transaction,
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
        db = tx
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
                "preferred_models, mnemos_refs, depends_on, retry_count FROM jobs WHERE status='queued' "
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
                        j_retry_count,
                    ) = r
                    if urn_host in NARROW_HOSTS and not (j_kind or "").startswith(NARROW_ALLOWLIST):
                        continue
                    # Same fail-closed gate as claim_job: distinct ids, and
                    # unreadable depends_on never skips its prerequisites.
                    if j_deps_json and not await self._deps_all_done(db, j_deps_json):
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
                        # Fallback claim path: surface the routed prefs + the
                        # CURRENT attempt so a worker honours model preference
                        # and climbs an escalation rung the same way it does
                        # on POST /v1/jobs/next.
                        "preferred_providers": json.loads(j_pref_providers) if j_pref_providers else [],
                        "preferred_models": json.loads(j_pref_models) if j_pref_models else [],
                        "depends_on": json.loads(j_deps_json) if j_deps_json else [],
                        "retry_count": int(j_retry_count or 0),
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

    async def patch_job_status(
        self,
        tx: Transaction,
        job_id: str,
        *,
        sets: Mapping[str, Any],
        expected_status: str | None = None,
    ) -> int:
        """Backend-neutral UPDATE of one job row, on the caller's transaction.

        Follows the EPIMONE/MNEMOS convention: the method takes the `tx` handle
        the caller opened (`async with repo.connection() as tx:`) so multi-step
        writes stay atomic. Column names come from code (never caller input);
        every VALUE is bound; None renders SQL NULL. When ``expected_status`` is
        given the UPDATE is a compare-and-set on ``status=?`` (rowcount 0 = lost
        the race / status changed). Returns rowcount.

        ``sets`` may carry a raw SQL-expression value via the sentinel
        ``SqlExpr`` (e.g. ``retry_count=retry_count+1``) — used by the PATCH
        release/retry paths; the expression text is always code-side.
        """
        if not sets:
            return 0
        assign_parts = []
        args: list[Any] = []
        for col, val in sets.items():
            if isinstance(val, SqlExpr):
                assign_parts.append(f"{col}={val.sql}")
            else:
                assign_parts.append(f"{col}=?")
                args.append(val)
        args.append(job_id)
        where = "id=?"
        if expected_status is not None:
            where += " AND status=?"
            args.append(expected_status)
        cur = await tx.execute(f"UPDATE jobs SET {', '.join(assign_parts)} WHERE {where}", args)
        return cur.rowcount

    def _reason_prefix_sql(self, pattern: str) -> str:
        """The reason_prefix predicate, one WHERE fragment, one bound arg.

        ``pattern`` already carries the trailing ``%`` (the caller builds it);
        it is bound, never interpolated. sqlite's LIKE is already
        case-insensitive for ASCII, so the reference is a bare
        ``result LIKE ?``; subclasses may override to wrap the text in an
        explicit LOWER() where LIKE is case-sensitive."""
        return " AND result LIKE ?"

    async def get_job(self, tx: Transaction, *, job_id: str) -> tuple[Any, ...] | None:
        db = tx
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            return await cur.fetchone()

    async def list_jobs(self, tx: Transaction, **filters: Any) -> tuple[int, list[dict[str, Any]]]:
        """Backend-neutral job listing (GET /v1/jobs).

        Filters: status, agent_urn, parent_job_id, since, limit, include_result.
        Returns (total_count, rows). Sorting: terminal statuses by ended/started
        DESC; otherwise recency-first with priority as tiebreak.

        ``preferred_providers``/``preferred_models``/``depends_on`` are part of
        the row (fault 2026-10-17: JobCreate accepted them and the row stored
        them, but the response dropped them).
        """
        status = filters.get("status")
        agent_urn = filters.get("agent_urn")
        parent_job_id = filters.get("parent_job_id")
        since = filters.get("since")
        limit = int(filters.get("limit") or 100)
        include_result = bool(filters.get("include_result", True))
        reason_prefix = filters.get("reason_prefix")
        col_sql = (
            "SELECT id, submitter_urn, parent_job_id, kind, description, priority, status, "
            "claimed_by, started_at, ended_at, result, estimated_cost_usd, "
            "required_capabilities, eligible_kinds, eligible_hosts, site, repo, head_branch, "
            "base_branch, pr_number, max_retries, retry_count, claimed_at, claim_lease_expires_at, "
            "last_update_at, claimed_model, claimed_provider, claimed_runtime, "
            "claimed_cost_tier, preferred_providers, preferred_models, depends_on, "
            "project, max_cost_tier, mnemos_refs FROM jobs WHERE 1=1"
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
        if reason_prefix:
            # reason_prefix (2026-10-27): case-insensitive prefix match on the
            # deploy-line reason fields — result.last_decline_reason (dead-
            # letter), result.last_release_reason (release), result.failure_reason
            # and result.error, plus the dead-letter result.note (it starts
            # with "dead_letter:" and must stay searchable). The match runs on
            # the raw result JSON text: the field keys carry unique prefixes
            # (no cross-field false positives, e.g. "dead_letter:" appears in
            # "note":"dead_letter:..." and nowhere else) and the prefix is a
            # bound parameter, so it is a pure text predicate the dialect can
            # express. Combined with the status filter by AND (both in WHERE).
            reason_sql = self._reason_prefix_sql(f"{reason_prefix}%")
            col_sql += reason_sql
            cnt_sql += reason_sql
            args.append(f"{reason_prefix}%")
            cnt_args.append(f"{reason_prefix}%")
        terminal = {"done", "failed", "cancelled", "dead-letter"}
        if status in terminal:
            col_sql += " ORDER BY COALESCE(ended_at, started_at) DESC LIMIT ?"
        else:
            col_sql += " ORDER BY COALESCE(ended_at, started_at) DESC, priority DESC LIMIT ?"
        # The status filter is bound exactly once, in the WHERE clause above.
        # (A previous revision appended it a second time after the ORDER BY and
        # then string-injected `status=?` into WHERE, leaving three
        # placeholders for two args — "Incorrect number of bindings supplied".)
        args.append(limit)
        rows: list[dict[str, Any]] = []
        db = tx
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
                    "base_branch": r[18],
                    "pr_number": r[19],
                    "max_retries": r[20],
                    "retry_count": r[21],
                    "claimed_at": r[22],
                    "claim_lease_expires_at": r[23],
                    "last_update_at": r[24],
                    "claimed_model": r[25],
                    "claimed_provider": r[26],
                    "claimed_runtime": r[27],
                    "claimed_cost_tier": r[28],
                    "preferred_providers": json.loads(r[29]) if r[29] else None,
                    "preferred_models": json.loads(r[30]) if r[30] else None,
                    "depends_on": json.loads(r[31]) if r[31] else None,
                    "project": r[32],
                    "max_cost_tier": r[33],
                    "mnemos_refs": json.loads(r[34]) if r[34] else None,
                }
                if not include_result:
                    job.pop("result", None)
                rows.append(job)
        return total, rows

    async def find_active_dedup_job(self, tx: Transaction, *, dedup_hash: str) -> tuple[Any, ...] | None:
        db = tx
        async with (
            db.execute(
                "SELECT id, started_at FROM jobs WHERE dedup_hash=? "
                "AND status IN ('queued', 'offered', 'claimed', 'running') "
                "ORDER BY started_at ASC LIMIT 1",
                (dedup_hash,),
            ) as cur,
        ):
            return await cur.fetchone()

    async def jobs_exist(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]:
        """Of ``ids``, which job ids exist? (one query, bound placeholders)."""
        if not ids:
            return set()
        db = tx
        placeholders = ",".join("?" * len(ids))
        async with db.execute(f"SELECT id FROM jobs WHERE id IN ({placeholders})", tuple(ids)) as cur:
            return {row[0] async for row in cur}

    async def agent_kind_runtimes(self, tx: Transaction) -> list[tuple[str, str]]:
        """(kind, runtime) for every registered agent (kind-alias checking)."""
        db = tx
        async with db.execute("SELECT kind, runtime FROM agents") as cur:
            return [(r[0], r[1]) async for r in cur]

    async def agent_claim_context(self, tx: Transaction, urn: str) -> tuple[Any, ...] | None:
        """The agent row fields the dequeue eligibility check needs."""
        db = tx
        async with db.execute(
            "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
            "auth_method, plan_cap_usd, plan_period_used_usd, status, current_load "
            "FROM agents WHERE urn=?",
            (urn,),
        ) as cur:
            return await cur.fetchone()

    async def pool_occupancy(self, tx: Transaction, *, provider: str, model: str, now: float) -> list[tuple[Any, ...]]:
        """Active (claimed_by, id) leases for a (provider, model) pool."""
        db = tx
        async with db.execute(
            "SELECT claimed_by, id FROM jobs "
            "WHERE status IN ('offered','claimed','running') "
            "AND claimed_provider=? AND claimed_model=? "
            "AND (claim_lease_expires_at IS NULL OR claim_lease_expires_at > ?)",
            (provider, model, now),
        ) as cur:
            return [tuple(r) async for r in cur]

    async def queued_candidates(
        self,
        tx: Transaction,
        *,
        agent_urn: str,
        now: float,
        limit: int,
        unsatisfied_dep_job_ids: set[str] | None = None,
    ) -> list[tuple[Any, ...]]:
        """The dequeue candidate scan (queued, plus this agent's own offered).

        ``unsatisfied_dep_job_ids`` (2026-10-25): ids of jobs with a NOT-done
        ``depends_on`` entry, computed inside the caller's transaction. They
        are excluded here ONLY to bound the scan; the binding gate is the
        in-transaction re-check in :meth:`claim_job`, which holds even when a
        caller passes no hint. ``None`` is the legacy no-hint call: no filter.
        """
        db = tx
        blocked = sorted(unsatisfied_dep_job_ids or ())
        sql = (
            "SELECT id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
            "required_capabilities, eligible_kinds, eligible_hosts, project, max_cost_tier, "
            "preferred_providers, preferred_models, mnemos_refs, depends_on, retry_backoff_until, started_at, "
            "site, repo, head_branch, base_branch, pr_number "
            "FROM jobs WHERE (status='queued' OR (status='offered' AND claimed_by=?)) "
            "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?) "
        )
        args: list[Any] = [agent_urn, now]
        if blocked:
            sql += f"AND id NOT IN ({','.join(['?'] * len(blocked))}) "
            args.extend(blocked)
        sql += "ORDER BY priority DESC, started_at ASC LIMIT ?"
        args.append(limit)
        async with db.execute(sql, args) as cur:
            return [tuple(r) async for r in cur]

    async def jobs_with_unsatisfied_deps(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]:
        """Of ``ids``, which jobs have a ``depends_on`` entry not all ``done``?

        The fail-closed evaluation is SHARED with :meth:`claim_job`'s
        in-transaction gate (``_deps_all_done``), so a candidate that looks
        open here is re-checked at claim time under the write lock.

        Batched (2026-10-25 review fix): ONE ``SELECT ... WHERE id IN (...)``
        fetches every ``depends_on`` value for the scan, then each row is
        evaluated with the same fail-closed ``_deps_all_done`` the claim gate
        uses. The previous per-id loop was an N+1 inside the claim
        transaction — exactly the pattern the original ``completed_job_ids``
        batching removed.
        """
        db = tx
        dedup = list(dict.fromkeys(ids))
        if not dedup:
            return set()
        placeholders = ",".join("?" * len(dedup))
        async with db.execute(
            f"SELECT id, depends_on FROM jobs WHERE id IN ({placeholders})",
            dedup,
        ) as cur:
            rows = [(r[0], r[1]) async for r in cur]
        out: set[str] = set()
        for job_id, depends_on in rows:
            if not await self._deps_all_done(db, depends_on):
                out.add(job_id)
        return out

    async def completed_job_ids(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]:
        """Of ``ids``, which are status='done'? (dependency gate)."""
        if not ids:
            return set()
        db = tx
        placeholders = ",".join("?" * len(ids))
        async with db.execute(
            f"SELECT id FROM jobs WHERE id IN ({placeholders}) AND status='done'",
            tuple(ids),
        ) as cur:
            return {row[0] async for row in cur}

    async def dependencies_satisfied(self, tx: Transaction, job_id: str) -> bool:
        """True when EVERY ``depends_on`` job of ``job_id`` is status='done'.

        A missing job, a non-JSON/empty value, or any non-array JSON counts as
        satisfied (only a real, non-empty list of ids can block). The read is
        plain SQL (no dialect-specific JSON functions) so sqlite, Oracle and
        Db2 all evaluate it identically.
        """
        db = tx
        async with db.execute("SELECT depends_on FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        return await self._deps_all_done(db, row[0] if row else None)

    @staticmethod
    async def _deps_all_done(db: Any, depends_on: Any) -> bool:
        # Fail closed: unreadable or non-array depends_on must never let a
        # job skip its prerequisites. Absent / JSON null / [] means no deps.
        try:
            deps = json.loads(depends_on) if depends_on else []
        except (TypeError, ValueError):
            return False
        if deps is None:
            return True
        if not isinstance(deps, list):
            return False
        if not deps:
            return True
        ids = list(dict.fromkeys(str(d) for d in deps if d is not None))
        if not ids:
            return True
        placeholders = ",".join("?" * len(ids))
        async with db.execute(
            f"SELECT COUNT(*) FROM jobs WHERE id IN ({placeholders}) AND status='done'",
            tuple(ids),
        ) as cur:
            done = (await cur.fetchone())[0]
        return int(done or 0) >= len(ids)

    async def _require_dependencies_done(self, db: Any, job_id: str) -> None:
        """Raise ``DependencyNotReady`` unless the job's DAG prerequisites are done."""
        async with db.execute("SELECT depends_on FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if not await self._deps_all_done(db, row[0] if row else None):
            raise DependencyNotReady(job_id)

    async def claim_job(
        self,
        tx: Transaction,
        *,
        job_id: str,
        agent_urn: str,
        now: float,
        lease_expires_at: float,
        runtime: str,
        model: str,
        provider: str,
        cost_tier: str,
    ) -> int:
        """Atomic claim: queued -> claimed, or this agent's own offered -> claimed.

        The compare-and-set predicate is what makes it race-safe (rowcount 0 =
        lost the race / not claimable). No commit — the caller owns the tx.

        DAG GATE (fault 2026-10-17; hardened 2026-10-25): the SELECT in the
        caller and the UPDATE here are separate statements. The gate is
        evaluated INSIDE this transaction, immediately before the
        compare-and-set — so a job whose ``depends_on`` rows are not all
        ``done`` is never handed out, even if the caller's pre-read saw a
        different state. The 2026-10-25 hardening matters because the
        caller's pre-scan (``queued_candidates`` + the router's candidate
        filter) is advisory: it can miss a dependency row (or see a dep as
        done that is not yet), and a job whose deps finished AFTER the
        pre-scan is the legitimate case the gate exists to handle correctly
        either way. ``_deps_all_done`` fail-closes on malformed values (a
        corrupt ``depends_on`` string is NEVER handed out), and a missing dep
        row (deleted / unknown id) also blocks: a reference to a job that does
        not exist can never reach ``done``. A NULL/empty ``depends_on`` or
        ``[]`` counts as satisfied.

        Test seam: a mocked ``claim_job`` on the dequeue router must apply the
        SAME gate (``repo._require_dependencies_done``) or it would pin only
        the advisory pre-scan half of the chain — see ``_stub_claim`` in
        test_job_prefs_deps_20261017.
        """
        db = tx
<<<<<<< HEAD
=======
        for _dropped in await self.drop_missing_dependencies(db, job_id=job_id):
            log.info(
                "dependency release at claim time: %s dropped missing dependency %s (remaining=%s)",
                _dropped["dependent_id"],
                _dropped["failed_dep_id"],
                _dropped["remaining"],
            )
            # One release event per dropped id (incident 2026-10-27), in this
            # same claim transaction. Imported lazily: hive.bus.deps loads
            # hive.persistence.factory (which imports this module) at module
            # import, so a top-level import here would cycle.
            import hive.bus.deps as _deps_mod

            _module_emit = getattr(_deps_mod, "emit_event", None)
            await _module_emit(
                db,
                "job.dependency_released",
                {
                    "dependent_id": _dropped["dependent_id"],
                    "failed_dep_id": _dropped["failed_dep_id"],
                    "dep_status": _dropped["dep_status"],
                    "remaining": _dropped["remaining"],
                },
            )
>>>>>>> 325f811eed75b81508c82223a5937701d7757cca
        await self._require_dependencies_done(db, job_id)
        cur = await db.execute(
            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
            "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
            "claim_lease_expires_at=?, last_update_at=?, "
            "ended_at=NULL, result=NULL "
            "WHERE id=? AND (status='queued' OR (status='offered' AND claimed_by=?))",
            (
                agent_urn,
                now,
                runtime,
                model,
                provider,
                cost_tier,
                lease_expires_at,
                now,
                job_id,
                agent_urn,
            ),
        )
        return cur.rowcount

    async def job_update_context(self, tx: Transaction, job_id: str) -> tuple[Any, ...] | None:
        """The PATCH pre-read: status/claim/retry/decline/kind/repo/result for one job."""
        db = tx
        async with db.execute(
            "SELECT status, claimed_by, claimed_provider, claimed_model, retry_count, "
            "max_retries, COALESCE(decline_count, 0), kind, repo, result, submitter_urn "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            return await cur.fetchone()

    async def job_stats_context(self, tx: Transaction, job_id: str) -> tuple[Any, ...] | None:
        """The terminal-stats pre-read (worker_kind_stats + usage_ledger inputs)."""
        db = tx
        async with db.execute(
            "SELECT kind, description, max_cost_tier, required_capabilities, "
            "claimed_model, claimed_provider, claimed_by, result, started_at "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            return await cur.fetchone()

    async def agent_spend_add(self, tx: Transaction, *, urn: str, delta: float) -> None:
        """Roll MTD spend onto the claiming agent (subscription-throttle input)."""
        db = tx
        await db.execute(
            "UPDATE agents SET plan_period_used_usd = COALESCE(plan_period_used_usd,0) + ? WHERE urn=?",
            (delta, urn),
        )

    async def usage_ledger_insert(self, tx: Transaction, **fields: Any) -> None:
        """Append one per-job cost-ledger row (ts uses SYSTIMESTAMP)."""
        db = tx
        await db.execute(
            "INSERT INTO usage_ledger (provider, model, task_kind, tokens_in, "
            "tokens_out, tokens_reasoning, est_cost_usd, latency_ms, outcome, "
            "caller_subsystem, tier, ts, session_id, request_count, "
            "subscription_amortized, path_kind) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP, ?, ?, ?, ?)",
            (
                fields.get("provider"),
                fields.get("model"),
                fields.get("task_kind"),
                fields.get("tokens_in", 0),
                fields.get("tokens_out", 0),
                fields.get("tokens_reasoning", 0),
                fields.get("est_cost_usd", 0),
                fields.get("latency_ms", 0),
                fields.get("outcome", "err"),
                fields.get("caller_subsystem", "hive"),
                (fields.get("tier") or "A").upper(),
                fields.get("session_id"),
                fields.get("request_count", 1),
                fields.get("subscription_amortized", 0),
                fields.get("path_kind", "worker"),
            ),
        )

    async def insert_job_audit(self, tx: Transaction, **fields: Any) -> None:
        """Append one job_audit_log row (the PATCH audit trail)."""
        db = tx
        await db.execute(
            "INSERT INTO job_audit_log (job_id, ts, actor_urn, old_status, new_status, "
            "old_claimed_by, new_claimed_by, patch) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                fields["job_id"],
                fields["ts"],
                fields.get("actor_urn"),
                fields.get("old_status"),
                fields.get("new_status"),
                fields.get("old_claimed_by"),
                fields.get("new_claimed_by"),
                fields.get("patch_json"),
            ),
        )

    async def job_claim_context(self, tx: Transaction, job_id: str) -> tuple[Any, ...] | None:
        """The by-id claim pre-read (status/kind/caps/kinds/hosts/tier/prefs/deps)."""
        db = tx
        async with db.execute(
            "SELECT status, kind, required_capabilities, eligible_kinds, "
            "eligible_hosts, max_cost_tier, preferred_providers, preferred_models, "
            "depends_on, retry_backoff_until, started_at "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            return await cur.fetchone()

    async def job_retry_context(self, tx: Transaction, job_id: str) -> tuple[Any, ...] | None:
        """The auto-retry pre-read, row-locked per this backend's dialect."""
        db = tx
        async with db.execute(
            "SELECT status, kind, max_retries, retry_count, result FROM jobs WHERE id=?" + self._retry_lock_clause,
            (job_id,),
        ) as cur:
            return await cur.fetchone()

    async def job_release_context(self, tx: Transaction, job_id: str) -> tuple[Any, ...] | None:
        """The zcgate release pre-read (result/status)."""
        db = tx
        async with db.execute("SELECT result, status FROM jobs WHERE id=?", (job_id,)) as cur:
            return await cur.fetchone()

    async def active_orchestrator_urn(self, tx: Transaction) -> str | None:
        """Latest non-offline orchestrator URN (the release-job submitter)."""
        db = tx
        async with db.execute(
            "SELECT urn FROM agents WHERE runtime IN ('mnemos','human','claude-code','claude') "
            "AND status != 'offline' ORDER BY last_heartbeat DESC LIMIT 1"
        ) as cur:
            r = await cur.fetchone()
        return r[0] if r else None

    async def active_agent_models(self, tx: Transaction) -> list[dict[str, Any]]:
        """``[{"model", "provider"}]`` for every non-offline agent (narrow read)."""
        db = tx
        async with db.execute("SELECT model, provider FROM agents WHERE status != 'offline'") as cur:
            return [{"model": r[0], "provider": r[1]} for r in await cur.fetchall()]

    async def worker_stats_rows(
        self, tx: Transaction, *, kind: str | None, include_system: bool, top_n: int
    ) -> list[dict[str, Any]]:
        """Per-worker per-kind capability scores (GET /v1/stats/workers)."""
        db = tx
        sql = (
            "SELECT urn, kind, success_count, fail_count, cancelled_count, "
            "total_tokens_in, total_tokens_out, ROUND(total_cost_usd,4), "
            "ROUND(total_duration_sec/NULLIF(success_count+fail_count,0),1) AS avg_dur, "
            "datetime(last_run,'unixepoch') AS last_run, "
            "ROUND(100.0 * success_count / NULLIF(success_count+fail_count+cancelled_count,0), 1) AS success_pct "
            "FROM worker_kind_stats WHERE 1=1"
        )
        args: list[Any] = []
        if kind:
            sql += " AND kind=?"
            args.append(kind)
        if not include_system:
            sql += " AND urn NOT LIKE 'urn:agent:system:%'"
        sql += " ORDER BY success_count DESC, success_pct DESC LIMIT ?"
        args.append(top_n)
        rows: list[dict[str, Any]] = []
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append(
                    {
                        "urn": r[0],
                        "kind": r[1],
                        "success_count": r[2],
                        "fail_count": r[3],
                        "cancelled_count": r[4],
                        "total_tokens_in": r[5],
                        "total_tokens_out": r[6],
                        "total_cost_usd": r[7],
                        "avg_duration_sec": r[8],
                        "last_run": r[9],
                        "success_pct": r[10],
                    }
                )
        return rows

    async def cache_stats(self, tx: Transaction, *, top_n: int) -> dict[str, Any]:
        """Result-cache hit-rate + cost savings (GET /v1/stats/cache)."""
        db = tx
        async with db.execute("SELECT COUNT(*), SUM(hit_count), ROUND(SUM(cost_saved_usd),4) FROM hive_cache") as cur:
            r = await cur.fetchone()
        totals = {
            "cached_jobs": r[0] or 0,
            "total_hits": r[1] or 0,
            "total_cost_saved_usd": r[2] or 0,
        }
        async with db.execute("SELECT COUNT(*) FROM jobs WHERE result LIKE '%\"cache_hit\": true%'") as cur:
            r2 = await cur.fetchone()
        totals["jobs_short_circuited"] = r2[0] or 0
        entries = []
        async with db.execute(
            "SELECT cache_key, hit_count, cost_saved_usd, model, provider, "
            "datetime(cached_at,'unixepoch') AS cached_at, source_job_id "
            "FROM hive_cache ORDER BY hit_count DESC, cost_saved_usd DESC LIMIT ?",
            (top_n,),
        ) as cur:
            async for row in cur:
                entries.append(
                    {
                        "cache_key": row[0][:16],
                        "hit_count": row[1],
                        "cost_saved_usd": row[2],
                        "model": row[3],
                        "provider": row[4],
                        "cached_at": row[5],
                        "source_job_id": row[6],
                    }
                )
        return {"totals": totals, "top_entries": entries}

    async def cost_stats_rows(self, tx: Transaction, *, group_by: str, cutoff: float) -> list[dict[str, Any]]:
        """Aggregated cost stats (GET /v1/stats/costs)."""
        db = tx
        if group_by == "day":
            sel = "DATE(ended_at, 'unixepoch')"
        elif group_by == "provider":
            sel = (
                "CASE "
                "WHEN JSON_VALUE(result,'$.agent_alias') IN ('hive_deepseek_pro_1','hive_groq_1','hive_xai_1','hive_nvidia_1') THEN 'codex' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_deepseek%' THEN 'deepseek-direct' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_siliconflow%' THEN 'siliconflow' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_xai%' THEN 'xai' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_minimax%' THEN 'minimax' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_groq%' THEN 'groq' "
                "WHEN JSON_VALUE(result,'$.agent_alias') LIKE 'hive_nvidia%' THEN 'nvidia' "
                "ELSE COALESCE(claimed_provider,'unknown') END"
            )
        elif group_by == "model":
            sel = "COALESCE(claimed_model,'unknown')"
        elif group_by == "runtime":
            sel = "COALESCE(claimed_runtime,'unknown')"
        elif group_by == "cost_tier":
            sel = "COALESCE(claimed_cost_tier,'unknown')"
        elif group_by == "claimed_by":
            sel = "COALESCE(claimed_by,'unknown')"
        else:  # kind
            sel = "kind"
        sql = (
            f"SELECT {sel} AS bucket, "
            "COUNT(*) AS job_count, "
            "SUM(COALESCE(tokens_in,0)) AS tot_in, "
            "SUM(COALESCE(tokens_out,0)) AS tot_out, "
            "ROUND(SUM(COALESCE(estimated_cost_usd,0)),4) AS tot_cost_usd, "
            "ROUND(AVG(CASE WHEN ended_at IS NOT NULL THEN ended_at-started_at END),2) AS avg_dur_s "
            "FROM jobs WHERE ended_at IS NOT NULL AND ended_at >= ? "
            f"GROUP BY bucket ORDER BY tot_cost_usd DESC, job_count DESC"
        )
        rows: list[dict[str, Any]] = []
        async with db.execute(sql, (cutoff,)) as cur:
            async for r in cur:
                rows.append(
                    {
                        "bucket": r[0],
                        "job_count": r[1],
                        "tokens_in": r[2],
                        "tokens_out": r[3],
                        "estimated_cost_usd": r[4],
                        "avg_duration_sec": r[5],
                    }
                )
        return rows

    async def post_message(self, tx: Transaction, **fields: Any) -> None:
        db = tx
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

    async def list_messages(self, tx: Transaction, **filters: Any) -> list[dict[str, Any]]:
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
        db = tx
        async with db.execute(sql, args) as cur:
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

    async def tail_events(
        self, tx: Transaction, *, since_id: int | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
        args: list[Any] = []
        if since_id is not None:
            sql += " WHERE id > ?"
            args.append(since_id)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(limit)
        rows: list[dict[str, Any]] = []
        db = tx
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

    async def cache_lookup(self, tx: Transaction, *, cache_key: str, cutoff: float) -> dict[str, Any] | None:
        db = tx
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

    async def cache_store(self, tx: Transaction, **fields: Any) -> None:
        db = tx
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

    async def cache_record_hit(self, tx: Transaction, *, cache_key: str, cost_saved_usd: float, ts: float) -> None:
        db = tx
        await db.execute(
            "UPDATE hive_cache SET hit_count=hit_count+1, cost_saved_usd=COALESCE(cost_saved_usd,0)+?, "
            "last_hit_at=? WHERE cache_key=?",
            (cost_saved_usd, ts, cache_key),
        )
        await db.commit()

    async def upsert_worker_kind_stats(self, tx: Transaction, **fields: Any) -> None:
        col = fields["column"]
        if col not in {"success_count", "fail_count", "cancelled_count"}:
            raise ValueError(f"invalid worker stats column: {col!r}")
        db = tx
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

    async def create_schedule(self, tx: Transaction, **fields: Any) -> None:
        db = tx
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

    async def list_schedules(self, tx: Transaction) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        db = tx
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

    # ---- reaper: agent lifecycle maintenance ----
    async def select_stale_agent_urns(
        self, tx: Transaction, *, stale_cutoff: float, offline_cutoff: float
    ) -> list[str]:
        db = tx
        async with db.execute(
            "SELECT urn FROM agents "
            "WHERE status IN ('online','idle','error') "
            "AND last_heartbeat < ? AND last_heartbeat >= ?",
            (stale_cutoff, offline_cutoff),
        ) as cur:
            return [row[0] async for row in cur]

    async def mark_agents_stale(self, tx: Transaction, urns: list[str]) -> None:
        db = tx
        await db.executemany(
            "UPDATE agents SET status='stale' WHERE urn=? AND status != 'offline'",
            [(u,) for u in urns],
        )

    async def select_dead_agent_urns(self, tx: Transaction, *, offline_cutoff: float) -> list[str]:
        db = tx
        async with db.execute(
            "SELECT urn FROM agents WHERE status != 'offline' AND last_heartbeat < ?",
            (offline_cutoff,),
        ) as cur:
            return [row[0] async for row in cur]

    async def release_unstarted_jobs_for_agent(self, tx: Transaction, urn: str) -> list[str]:
        """Release only NOT-YET-STARTED work (offered/claimed) for a dead agent.

        A missed heartbeat is not proof of an abandoned job: reclaiming running
        work here would discard a live worker's result (PATCH 'done' is illegal
        out of 'queued'). In-flight reclaim is the orphan-claim path's job.
        Returns the released job ids (for event emission)."""
        db = tx
        async with db.execute(
            "SELECT id FROM jobs WHERE claimed_by=? AND status IN ('offered','claimed')",
            (urn,),
        ) as cur:
            ids = [row[0] async for row in cur]
        await db.execute(
            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL, "
            "ended_at=NULL, result=NULL "
            "WHERE claimed_by=? AND status IN ('offered','claimed')",
            (urn,),
        )
        return ids

    async def mark_agent_offline(self, tx: Transaction, urn: str) -> None:
        db = tx
        await db.execute("UPDATE agents SET status='offline' WHERE urn=?", (urn,))

    async def purge_offline_agents(self, tx: Transaction, *, cutoff: float) -> int:
        """Set-based hard-delete of agents offline beyond the purge window.

        Re-checks every safety condition AT DELETE TIME (status, heartbeat age,
        no in-flight claim) so an agent that heartbeats or claims work between a
        select and delete cannot be removed. Returns rows deleted."""
        db = tx
        cur = await db.execute(
            "DELETE FROM agents WHERE status='offline' AND last_heartbeat < ? "
            # non-correlated NOT IN (was a correlated `agents.urn` subquery): the
            # sqlite->Oracle table-name rewrite only translates FROM/JOIN/ON
            # positions, so a table-qualified `agents.urn` survived untranslated
            # and ORA-00904'd, crashing the reaper every 30s. IS NOT NULL guards
            # NOT-IN null-poison.
            "AND (urn IS NULL OR urn NOT IN (SELECT claimed_by FROM jobs "
            "WHERE claimed_by IS NOT NULL AND status IN ('offered','claimed','running')))",
            (cutoff,),
        )
        return max(0, getattr(cur, "rowcount", 0) or 0)  # some drivers report -1

    # ---- reaper: job lifecycle maintenance ----
    async def select_orphan_claim_candidates(
        self, tx: Transaction, *, now: float, hard_cutoff: float, default_cutoff: float
    ) -> list[tuple]:
        """Jobs claimed by dead/stale workers (kind-specific TTL applied in Python).

        Staleness is measured against COALESCE(last_update_at, claimed_at) so a
        worker still PATCHing progress is never reclaimed. j.claimed_at IS NULL
        surfaces the corrupt/phantom claim whose all-NULL timestamps would
        otherwise make the staleness predicates UNKNOWN."""
        db = tx
        async with db.execute(
            "SELECT j.id, j.kind, j.claimed_by, j.claimed_at, j.last_update_at, "
            "j.claim_lease_expires_at "
            "FROM jobs j "
            "LEFT JOIN agents a ON a.urn = j.claimed_by "
            "WHERE j.status IN ('claimed','running') "
            "AND ( a.status = 'offline' OR a.urn IS NULL OR "
            "      j.claimed_at IS NULL OR "
            "      j.claim_lease_expires_at <= ? OR "
            "      j.claimed_at < ? OR "
            "      COALESCE(j.last_update_at, j.claimed_at) < ? )",
            (now, hard_cutoff, default_cutoff),
        ) as cur:
            return [(r[0], r[1], r[2], r[3], r[4], r[5]) async for r in cur]

    async def requeue_orphan_job(self, tx: Transaction, job_id: str) -> None:
        db = tx
        await db.execute(
            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL, "
            "ended_at=NULL, result=NULL "
            "WHERE id=? AND status IN ('claimed','running')",
            (job_id,),
        )

    async def select_due_schedules(self, tx: Transaction, *, now: float) -> list[tuple]:
        db = tx
        async with db.execute(
            "SELECT id, name, created_by_urn, interval_seconds, job_template, fire_count "
            "FROM scheduled_jobs WHERE enabled=1 AND next_fire_at <= ?",
            (now,),
        ) as cur:
            return [tuple(r) async for r in cur]

    async def mark_schedule_fired(self, tx: Transaction, *, sched_id: str, now: float, next_at: float) -> None:
        db = tx
        await db.execute(
            "UPDATE scheduled_jobs SET last_fired_at=?, next_fire_at=?, fire_count=fire_count+1 WHERE id=?",
            (now, next_at, sched_id),
        )

    async def select_stale_queued_job_ids(self, tx: Transaction, *, cutoff: float) -> list[str]:
        db = tx
        async with db.execute(
            "SELECT id FROM jobs WHERE status='queued' AND started_at < ?",
            (cutoff,),
        ) as cur:
            return [r[0] async for r in cur]

    async def cancel_jobs(self, tx: Transaction, ids: list[str], *, ended_at: float) -> None:
        db = tx
        await db.executemany(
            "UPDATE jobs SET status='cancelled', ended_at=? WHERE id=?",
            [(ended_at, j) for j in ids],
        )

    async def purge_events_before(self, tx: Transaction, *, cutoff: float) -> None:
        db = tx
        await db.execute("DELETE FROM events WHERE ts < ?", (cutoff,))

    async def select_sweepable_test_job_ids(
        self, tx: Transaction, *, statuses: list[str], patterns: tuple[str, ...], cutoff: float
    ) -> list[str]:
        """Terminal jobs matching a test/e2e description pattern older than cutoff."""
        db = tx
        status_ph = ",".join("?" for _ in statuses)
        like_ph = " OR ".join("LOWER(description) LIKE ?" for _ in patterns)
        params: list = list(statuses)
        params += [f"%{p.lower()}%" for p in patterns]
        params.append(cutoff)
        async with db.execute(
            f"SELECT id FROM jobs WHERE status IN ({status_ph}) "
            f"AND description IS NOT NULL AND ({like_ph}) "
            f"AND COALESCE(ended_at, started_at) < ?",
            tuple(params),
        ) as cur:
            return [r[0] async for r in cur]

    async def select_sweepable_terminal_job_ids(
        self, tx: Transaction, *, statuses: list[str], cutoff: float
    ) -> list[str]:
        """Any terminal job older than the retention window (age = COALESCE(ended_at, started_at))."""
        db = tx
        status_ph = ",".join("?" for _ in statuses)
        params: list = list(statuses)
        params.append(cutoff)
        async with db.execute(
            f"SELECT id FROM jobs WHERE status IN ({status_ph}) AND COALESCE(ended_at, started_at) < ?",
            tuple(params),
        ) as cur:
            return [r[0] async for r in cur]

    # ---- capacity ledger (Phase 5) ----
    async def capacity_state_load(self, tx: Transaction) -> list[tuple[Any, ...]]:
        """Every persisted per-service counter row (service, in_flight, tokens_used,
        spent_usd, limited_until, window_start, updated_at)."""
        db = tx
        async with db.execute(
            "SELECT service, in_flight, tokens_used, spent_usd, limited_until, window_start, updated_at "
            "FROM capacity_ledger"
        ) as cur:
            return [tuple(r) async for r in cur]

    async def capacity_state_save(self, tx: Transaction, **fields: Any) -> None:
        """Idempotent keyed SET-upsert of one service's counters.

        SET semantics (not increments) so re-saving the same state is a no-op —
        a restart or a replay converges instead of double-counting. Written as
        SELECT + INSERT/UPDATE (backend-neutral; the Oracle adapter translates)."""
        db = tx
        service = fields["service"]
        vals = (
            int(fields.get("in_flight", 0)),
            int(fields.get("tokens_used", 0)),
            float(fields.get("spent_usd", 0.0)),
            float(fields.get("limited_until", 0.0)),
            float(fields.get("window_start", 0.0)),
            float(fields.get("updated_at", 0.0)),
        )
        async with db.execute("SELECT 1 FROM capacity_ledger WHERE service=?", (service,)) as cur:
            exists = await cur.fetchone()
        if exists:
            await db.execute(
                "UPDATE capacity_ledger SET in_flight=?, tokens_used=?, spent_usd=?, limited_until=?, "
                "window_start=?, updated_at=? WHERE service=?",
                (*vals, service),
            )
        else:
            await db.execute(
                "INSERT INTO capacity_ledger (in_flight, tokens_used, spent_usd, limited_until, window_start, "
                "updated_at, service) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (*vals, service),
            )

    async def capacity_fold_usage(
        self, tx: Transaction, *, dedup_key: str, service: str, tokens: int, cost_usd: float, now: float
    ) -> bool:
        """Fold one usage report into a service's window counters — idempotently.

        The ``dedup_key`` (e.g. a job id + attempt) is the idempotency key: if it
        was already folded, this is a no-op and returns False. Returns True when
        the fold applied. The increments are guarded by the key, so replaying the
        same report never double-counts."""
        db = tx
        async with db.execute("SELECT 1 FROM capacity_usage WHERE dedup_key=?", (dedup_key,)) as cur:
            if await cur.fetchone():
                return False  # already folded — idempotent
        await db.execute(
            "INSERT INTO capacity_usage (dedup_key, service, tokens, cost_usd, ts) VALUES (?, ?, ?, ?, ?)",
            (dedup_key, service, int(tokens), float(cost_usd), float(now)),
        )
        async with db.execute("SELECT 1 FROM capacity_ledger WHERE service=?", (service,)) as cur:
            exists = await cur.fetchone()
        if exists:
            await db.execute(
                "UPDATE capacity_ledger SET tokens_used=COALESCE(tokens_used,0)+?, "
                "spent_usd=COALESCE(spent_usd,0)+?, updated_at=? WHERE service=?",
                (int(tokens), float(cost_usd), float(now), service),
            )
        else:
            await db.execute(
                "INSERT INTO capacity_ledger (tokens_used, spent_usd, updated_at, service) VALUES (?, ?, ?, ?)",
                (int(tokens), float(cost_usd), float(now), service),
            )
        return True
