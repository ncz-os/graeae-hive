"""IBM Db2 Hive Mind repository."""

from __future__ import annotations

import asyncio
import datetime as _dt
import json as _json
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal as _Decimal
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

from .sqlite import SqliteHiveRepository


_TABLE_MAP = {
    "agents": "hive_agents",
    "jobs": "hive_jobs",
    "messages": "hive_messages",
    "events": "hive_events",
    "hive_cache": "hive_cache",
    "scheduled_jobs": "hive_scheduled_jobs",
    "worker_kind_stats": "hive_worker_kind_stats",
    "job_audit_log": "hive_job_audit_log",
}


def _rewrite_tables(sql: str) -> str:
    names = "|".join(sorted(map(re.escape, _TABLE_MAP), key=len, reverse=True))

    def repl(match: re.Match) -> str:
        return f"{match.group(1)} {_TABLE_MAP[match.group(2).lower()]}"

    sql = re.sub(
        rf"\b(FROM|INTO|UPDATE|DELETE\s+FROM|JOIN)\s+({names})\b",
        repl, sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        rf"\b(ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE)\s+({names})\b",
        repl, sql, flags=re.IGNORECASE,
    )
    sql = re.sub(rf"\b(ON)\s+({names})\b", repl, sql, flags=re.IGNORECASE)
    return sql


DB2_DDL = """
CREATE TABLE agents (
  urn VARCHAR(512) PRIMARY KEY,
  kind VARCHAR(64) NOT NULL,
  host VARCHAR(255) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  pid INTEGER,
  capabilities CLOB,
  version VARCHAR(128),
  started_at DOUBLE NOT NULL,
  last_heartbeat DOUBLE NOT NULL,
  status VARCHAR(32) NOT NULL,
  metadata CLOB,
  runtime VARCHAR(64),
  model VARCHAR(255),
  provider VARCHAR(255),
  autonomy_level VARCHAR(64),
  cost_tier VARCHAR(8),
  current_load CLOB,
  auth_method VARCHAR(64),
  plan_cap_usd DOUBLE,
  plan_period_used_usd DOUBLE DEFAULT 0,
  subscription_pools CLOB
);
CREATE INDEX idx_agents_status ON agents(status);
CREATE INDEX idx_agents_kind ON agents(kind);

CREATE TABLE jobs (
  id VARCHAR(64) PRIMARY KEY,
  submitter_urn VARCHAR(512) NOT NULL,
  parent_job_id VARCHAR(64),
  kind VARCHAR(255) NOT NULL,
  description CLOB,
  priority INTEGER NOT NULL DEFAULT 0,
  deadline DOUBLE,
  required_capabilities CLOB,
  eligible_kinds CLOB,
  eligible_hosts CLOB,
  project VARCHAR(255),
  status VARCHAR(32) NOT NULL,
  claimed_by VARCHAR(512),
  claimed_at DOUBLE,
  started_at DOUBLE NOT NULL,
  ended_at DOUBLE,
  result CLOB,
  required_autonomy VARCHAR(64),
  max_cost_tier VARCHAR(8),
  preferred_providers CLOB,
  preferred_models CLOB,
  claimed_runtime VARCHAR(64),
  claimed_model VARCHAR(255),
  claimed_provider VARCHAR(255),
  claimed_cost_tier VARCHAR(8),
  tokens_in INTEGER,
  tokens_out INTEGER,
  estimated_cost_usd DOUBLE,
  mnemos_refs CLOB,
  result_mnemos_id VARCHAR(128),
  required_resources CLOB,
  claimed_host_caps CLOB,
  tags CLOB,
  depends_on CLOB,
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 2,
  retry_backoff_until DOUBLE,
  last_update_at DOUBLE,
  claim_lease_expires_at DOUBLE,
  decline_count INTEGER NOT NULL DEFAULT 0,
  dedup_hash VARCHAR(64),
  dedup_active_hash VARCHAR(64) GENERATED ALWAYS AS
    (CASE WHEN status IN ('queued', 'offered', 'claimed', 'running') THEN dedup_hash ELSE NULL END)
);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_submitter ON jobs(submitter_urn);
CREATE INDEX idx_jobs_claimed_by ON jobs(claimed_by);
CREATE INDEX idx_jobs_parent ON jobs(parent_job_id);
CREATE INDEX idx_jobs_queue ON jobs(status, priority DESC, started_at ASC);

CREATE TABLE events (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts DOUBLE NOT NULL,
  kind VARCHAR(255) NOT NULL,
  payload CLOB NOT NULL,
  agent_urn VARCHAR(512)
);
CREATE INDEX idx_events_ts ON events(ts);
CREATE INDEX idx_events_kind ON events(kind);
CREATE INDEX idx_events_agent ON events(agent_urn);

CREATE TABLE messages (
  id VARCHAR(64) PRIMARY KEY,
  from_urn VARCHAR(512) NOT NULL,
  to_urn VARCHAR(512),
  in_reply_to VARCHAR(64),
  topic VARCHAR(255) NOT NULL,
  payload CLOB NOT NULL,
  ts DOUBLE NOT NULL
);
CREATE INDEX idx_messages_to ON messages(to_urn);
CREATE INDEX idx_messages_topic ON messages(topic);
CREATE INDEX idx_messages_ts ON messages(ts);

CREATE TABLE job_audit_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  ts DOUBLE NOT NULL,
  actor_urn VARCHAR(512),
  old_status VARCHAR(32),
  new_status VARCHAR(32),
  old_claimed_by VARCHAR(512),
  new_claimed_by VARCHAR(512),
  patch CLOB NOT NULL
);
CREATE INDEX idx_job_audit_job_ts ON job_audit_log(job_id, ts);

CREATE TABLE hive_cache (
  cache_key VARCHAR(128) PRIMARY KEY,
  result_json CLOB NOT NULL,
  source_job_id VARCHAR(64),
  result_mnemos_id VARCHAR(128),
  hit_count INTEGER NOT NULL DEFAULT 0,
  cost_saved_usd DOUBLE NOT NULL DEFAULT 0,
  model VARCHAR(255),
  provider VARCHAR(255),
  cached_at DOUBLE NOT NULL,
  last_hit_at DOUBLE
);

CREATE TABLE worker_kind_stats (
  urn VARCHAR(512) NOT NULL,
  kind VARCHAR(255) NOT NULL,
  success_count INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,
  cancelled_count INTEGER NOT NULL DEFAULT 0,
  total_tokens_in INTEGER NOT NULL DEFAULT 0,
  total_tokens_out INTEGER NOT NULL DEFAULT 0,
  total_cost_usd DOUBLE NOT NULL DEFAULT 0,
  total_duration_sec DOUBLE NOT NULL DEFAULT 0,
  last_run DOUBLE,
  PRIMARY KEY (urn, kind)
);

CREATE TABLE scheduled_jobs (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_by_urn VARCHAR(512) NOT NULL,
  interval_seconds INTEGER NOT NULL,
  job_template CLOB NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_fired_at DOUBLE,
  next_fire_at DOUBLE NOT NULL,
  fire_count INTEGER NOT NULL DEFAULT 0,
  created_at DOUBLE NOT NULL
);

CREATE TABLE hive_schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at DOUBLE NOT NULL
);
"""


_DB2_ADDITIVE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "HIVE_AGENTS": (
        ("RUNTIME", "runtime VARCHAR(64)"),
        ("MODEL", "model VARCHAR(255)"),
        ("PROVIDER", "provider VARCHAR(255)"),
        ("AUTONOMY_LEVEL", "autonomy_level VARCHAR(64)"),
        ("COST_TIER", "cost_tier VARCHAR(8)"),
        ("CURRENT_LOAD", "current_load CLOB"),
        ("AUTH_METHOD", "auth_method VARCHAR(64)"),
        ("PLAN_CAP_USD", "plan_cap_usd DOUBLE"),
        ("PLAN_PERIOD_USED_USD", "plan_period_used_usd DOUBLE DEFAULT 0"),
        ("SUBSCRIPTION_POOLS", "subscription_pools CLOB"),
    ),
    "HIVE_JOBS": (
        ("ELIGIBLE_HOSTS", "eligible_hosts CLOB"),
        ("PROJECT", "project VARCHAR(255)"),
        ("REQUIRED_AUTONOMY", "required_autonomy VARCHAR(64)"),
        ("MAX_COST_TIER", "max_cost_tier VARCHAR(8)"),
        ("PREFERRED_PROVIDERS", "preferred_providers CLOB"),
        ("PREFERRED_MODELS", "preferred_models CLOB"),
        ("CLAIMED_RUNTIME", "claimed_runtime VARCHAR(64)"),
        ("CLAIMED_MODEL", "claimed_model VARCHAR(255)"),
        ("CLAIMED_PROVIDER", "claimed_provider VARCHAR(255)"),
        ("CLAIMED_COST_TIER", "claimed_cost_tier VARCHAR(8)"),
        ("TOKENS_IN", "tokens_in INTEGER"),
        ("TOKENS_OUT", "tokens_out INTEGER"),
        ("ESTIMATED_COST_USD", "estimated_cost_usd DOUBLE"),
        ("MNEMOS_REFS", "mnemos_refs CLOB"),
        ("RESULT_MNEMOS_ID", "result_mnemos_id VARCHAR(128)"),
        ("REQUIRED_RESOURCES", "required_resources CLOB"),
        ("CLAIMED_HOST_CAPS", "claimed_host_caps CLOB"),
        ("TAGS", "tags CLOB"),
        ("DEPENDS_ON", "depends_on CLOB"),
        ("RETRY_COUNT", "retry_count INTEGER NOT NULL DEFAULT 0"),
        ("MAX_RETRIES", "max_retries INTEGER NOT NULL DEFAULT 2"),
        ("RETRY_BACKOFF_UNTIL", "retry_backoff_until DOUBLE"),
        ("LAST_UPDATE_AT", "last_update_at DOUBLE"),
        ("CLAIM_LEASE_EXPIRES_AT", "claim_lease_expires_at DOUBLE"),
        ("DECLINE_COUNT", "decline_count INTEGER NOT NULL DEFAULT 0"),
        ("DEDUP_HASH", "dedup_hash VARCHAR(64)"),
        ("DEDUP_ACTIVE_HASH", "dedup_active_hash VARCHAR(64) GENERATED ALWAYS AS "
         "(CASE WHEN status IN ('queued', 'offered', 'claimed', 'running') THEN dedup_hash ELSE NULL END)"),
    ),
}

_DB2_REQUIRED_BASE_COLUMNS = {
    "HIVE_AGENTS": {
        "URN", "KIND", "HOST", "SESSION_ID", "PID", "CAPABILITIES", "VERSION",
        "STARTED_AT", "LAST_HEARTBEAT", "STATUS", "METADATA",
    },
    "HIVE_JOBS": {
        "ID", "SUBMITTER_URN", "PARENT_JOB_ID", "KIND", "DESCRIPTION", "PRIORITY",
        "DEADLINE", "REQUIRED_CAPABILITIES", "ELIGIBLE_KINDS", "STATUS", "CLAIMED_BY",
        "CLAIMED_AT", "STARTED_AT", "ENDED_AT", "RESULT",
    },
}


def _parse_db2_dsn(dsn: str) -> dict[str, str]:
    if "://" not in dsn:
        return {"DSN": dsn}
    parsed = urlparse(dsn)
    return {
        "DATABASE": (parsed.path or "/").lstrip("/") or "HIVE",
        "HOSTNAME": parsed.hostname or "localhost",
        "PORT": str(parsed.port or 50000),
        "PROTOCOL": "TCPIP",
        "UID": unquote(parsed.username or "db2inst1"),
        "PWD": unquote(parsed.password or ""),
    }


def _dsn_string(parts: dict[str, str]) -> str:
    for key, value in parts.items():
        if key != "PORT" and (";" in value or "=" in value):
            raise ValueError(f"DB2 DSN attribute {key} contains a forbidden character")
    return ";".join(f"{key}={value}" for key, value in parts.items()) + ";"


def _split_script(script: str) -> list[str]:
    return [stmt.strip() for stmt in script.split(";") if stmt.strip()]


def _is_already_exists(exc: BaseException) -> bool:
    text = str(exc).upper()
    return "SQLSTATE=42710" in text or "SQLSTATE 42710" in text or "ALREADY EXISTS" in text


def _adapt_sql(sql: str, params: Any = None) -> tuple[str | None, tuple[Any, ...]]:
    stripped = sql.strip()
    upper = stripped.upper()
    values = tuple(params or ())
    if not stripped:
        return None, values
    if upper in {"BEGIN", "BEGIN IMMEDIATE"}:
        return None, values
    if upper.startswith("PRAGMA TABLE_INFO"):
        table = re.search(r"\((\w+)\)", stripped, flags=re.IGNORECASE)
        logical_name = table.group(1).lower() if table else ""
        name = _TABLE_MAP.get(logical_name, logical_name).upper()
        return (
            "SELECT colno, LOWER(colname), typename, NULL, NULL, NULL "
            "FROM syscat.columns WHERE tabschema = CURRENT SCHEMA AND tabname = ? "
            "ORDER BY colno",
            (name,),
        )

    cache_upsert = re.search(r"INSERT INTO hive_cache .* ON CONFLICT\(cache_key\) DO UPDATE SET", " ".join(stripped.split()), re.IGNORECASE)
    if cache_upsert:
        return (
            _rewrite_tables("MERGE INTO hive_cache t USING (VALUES (?, ?, ?, ?, ?, ?, ?)) "
            "s(cache_key, result_json, source_job_id, result_mnemos_id, model, provider, cached_at) "
            "ON t.cache_key = s.cache_key "
            "WHEN MATCHED THEN UPDATE SET result_json=s.result_json, source_job_id=s.source_job_id, "
            "result_mnemos_id=s.result_mnemos_id, cached_at=s.cached_at, model=s.model, provider=s.provider "
            "WHEN NOT MATCHED THEN INSERT "
            "(cache_key, result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
            "VALUES (s.cache_key, s.result_json, s.source_job_id, s.result_mnemos_id, 0, 0, s.model, s.provider, s.cached_at, NULL)"),
            values,
        )

    stats_match = re.search(
        r"INSERT INTO worker_kind_stats \(urn, kind, (success_count|fail_count|cancelled_count), .* ON CONFLICT\(urn, kind\) DO UPDATE SET",
        " ".join(stripped.split()),
        re.IGNORECASE,
    )
    if stats_match:
        col = stats_match.group(1)
        return (
            _rewrite_tables("MERGE INTO worker_kind_stats t USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) "
            "s(urn, kind, ins_tokens_in, ins_tokens_out, ins_cost, ins_duration, ins_last_run, "
            "upd_tokens_in, upd_tokens_out, upd_cost, upd_duration, upd_last_run) "
            "ON t.urn = s.urn AND t.kind = s.kind "
            f"WHEN MATCHED THEN UPDATE SET {col}={col}+1, "
            "total_tokens_in=total_tokens_in+s.upd_tokens_in, total_tokens_out=total_tokens_out+s.upd_tokens_out, "
            "total_cost_usd=total_cost_usd+s.upd_cost, total_duration_sec=total_duration_sec+s.upd_duration, last_run=s.upd_last_run "
            f"WHEN NOT MATCHED THEN INSERT (urn, kind, {col}, total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run) "
            "VALUES (s.urn, s.kind, 1, s.ins_tokens_in, s.ins_tokens_out, s.ins_cost, s.ins_duration, s.ins_last_run)"),
            values,
        )

    limit_match = re.search(r"\s+LIMIT\s+\?\s*$", stripped, flags=re.IGNORECASE)
    if limit_match:
        if not values:
            raise ValueError("Db2 LIMIT ? translation requires a bound limit value")
        limit = int(values[-1])
        stripped = re.sub(r"\s+LIMIT\s+\?\s*$", f" FETCH FIRST {limit} ROWS ONLY", stripped, flags=re.IGNORECASE)
        values = values[:-1]
    stripped = re.sub(r"\s+LIMIT\s+(\d+)\s*$", r" FETCH FIRST \1 ROWS ONLY", stripped, flags=re.IGNORECASE)
    return _rewrite_tables(stripped), values


class _Db2Cursor:
    def __init__(self, cursor: Any | None = None, rows: list[tuple[Any, ...]] | None = None, rowcount: int = -1):
        self._cursor = cursor
        self._rows = rows
        self._index = 0
        self.rowcount = rowcount if cursor is None else getattr(cursor, "rowcount", rowcount)
        self.lastrowid = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, _Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, _dt.datetime):
            return value.isoformat()
        raise TypeError(f"unhandled type {type(value).__name__}")

    @classmethod
    async def _normalize_row(cls, row: Any) -> Any:
        if row is None:
            return None
        out = []
        for value in row:
            if isinstance(value, (dict, list)):
                out.append(_json.dumps(value, separators=(",", ":"), default=cls._json_default))
            elif hasattr(value, "read") and callable(value.read):
                result = await asyncio.to_thread(value.read)
                out.append(result)
            elif isinstance(value, _Decimal):
                out.append(int(value) if value == value.to_integral_value() else float(value))
            elif isinstance(value, _dt.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=_dt.timezone.utc)
                out.append(value.timestamp())
            elif isinstance(value, (bytes, bytearray)):
                out.append(value.decode("utf-8", errors="replace"))
            else:
                out.append(value)
        return tuple(out)

    async def fetchone(self):
        if self._rows is not None:
            if self._index >= len(self._rows):
                return None
            row = self._rows[self._index]
            self._index += 1
            return await self._normalize_row(row)
        return await self._normalize_row(await asyncio.to_thread(self._cursor.fetchone))

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index:]
            self._index = len(self._rows)
            return [await self._normalize_row(row) for row in rows]
        return [await self._normalize_row(row) for row in await asyncio.to_thread(self._cursor.fetchall)]

    async def close(self):
        if self._cursor is not None:
            await asyncio.to_thread(self._cursor.close)


class _Db2ExecuteResult:
    def __init__(self, conn: "_Db2Connection", sql: str, params: Any = None):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor: _Db2Cursor | None = None

    def __await__(self):
        return self._run().__await__()

    async def _run(self):
        self._cursor = await self._conn._execute(self._sql, self._params)
        return self._cursor

    async def __aenter__(self):
        return await self._run()

    async def __aexit__(self, exc_type, exc, tb):
        if self._cursor is not None:
            await self._cursor.close()


class _Db2Connection:
    def __init__(self, raw: Any):
        self._raw = raw

    def execute(self, sql: str, params: Any = None):
        return _Db2ExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None) -> _Db2Cursor:
        adapted, adapted_params = _adapt_sql(sql, params)
        if adapted is None:
            return _Db2Cursor(rows=[])

        def _go():
            cur = self._raw.cursor()
            if len(adapted_params) > 0:
                cur.execute(adapted, adapted_params)
            else:
                cur.execute(adapted)
            return cur

        return _Db2Cursor(await asyncio.to_thread(_go))

    async def executemany(self, sql: str, seq_of_params: list[tuple[Any, ...]]) -> _Db2Cursor:
        if not seq_of_params:
            return _Db2Cursor(rows=[])
        adapted, _ = _adapt_sql(sql, seq_of_params[0])
        if adapted is None:
            return _Db2Cursor(rows=[])

        def _go():
            cur = self._raw.cursor()
            cur.executemany(adapted, seq_of_params)
            return cur

        return _Db2Cursor(await asyncio.to_thread(_go))

    async def executescript(self, script: str) -> _Db2Cursor:
        for stmt in _split_script(script):
            try:
                await self._execute(stmt)
            except Exception as exc:
                if not _is_already_exists(exc):
                    raise
        return _Db2Cursor(rows=[])

    async def commit(self) -> None:
        await asyncio.to_thread(self._raw.commit)

    async def rollback(self) -> None:
        await asyncio.to_thread(self._raw.rollback)

    async def close(self) -> None:
        await asyncio.to_thread(self._raw.close)


class _Db2Pool:
    def __init__(self, dsn: str, *, min_size: int = 0, max_size: int = 8, acquire_timeout: float = 30.0):
        self._parts = _parse_db2_dsn(dsn)
        self._min_size = max(0, min_size)
        self._max_size = max(1, max_size)
        self._acquire_timeout = acquire_timeout
        self._idle: list[_Db2Connection] = []
        self._in_use: set[_Db2Connection] = set()
        self._reserved = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._not_full = asyncio.Condition(self._lock)

    async def _open(self) -> _Db2Connection:
        try:
            import ibm_db_dbi  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"HIVE_BACKEND=db2 requires ibm_db/ibm_db_dbi importable before connecting: {exc}") from exc
        raw = await asyncio.to_thread(ibm_db_dbi.connect, _dsn_string(self._parts), "", "")
        return _Db2Connection(raw)

    async def warmup(self) -> None:
        opened = []
        for _ in range(self._min_size):
            try:
                opened.append(await self._open())
            except Exception:
                break
        async with self._lock:
            self._idle.extend(opened)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_Db2Connection]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._acquire_timeout
        conn: _Db2Connection | None = None
        need_open = False
        async with self._lock:
            while True:
                if self._closed:
                    raise RuntimeError("DB2 pool is closed")
                if self._idle:
                    conn = self._idle.pop()
                    self._in_use.add(conn)
                    break
                if len(self._in_use) + self._reserved < self._max_size:
                    self._reserved += 1
                    need_open = True
                    break
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"DB2 pool acquire timeout after {self._acquire_timeout:.1f}s")
                await asyncio.wait_for(self._not_full.wait(), remaining)
        if need_open:
            try:
                conn = await self._open()
            finally:
                async with self._lock:
                    self._reserved -= 1
                    self._not_full.notify()
            async with self._lock:
                self._in_use.add(conn)
        try:
            yield conn
        finally:
            async with self._lock:
                self._in_use.discard(conn)
                if self._closed:
                    await conn.close()
                else:
                    self._idle.append(conn)
                self._not_full.notify()

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            self._not_full.notify_all()
            while self._idle:
                await self._idle.pop().close()


class Db2HiveRepository(SqliteHiveRepository):
    def __init__(self, dsn: str, *, min_size: int | None = None, max_size: int | None = None, acquire_timeout: float | None = None) -> None:
        super().__init__(":memory:")
        self.dsn = dsn
        self._pool: _Db2Pool | None = None
        self._min_size = int(min_size if min_size is not None else os.environ.get("HIVE_DB2_POOL_MIN", "0"))
        self._max_size = int(max_size if max_size is not None else os.environ.get("HIVE_DB2_POOL_MAX", "8"))
        self._acquire_timeout = float(acquire_timeout if acquire_timeout is not None else os.environ.get("HIVE_DB2_ACQUIRE_TIMEOUT", "30"))

    async def init(self, schema: str | None = None) -> None:
        if not self.dsn:
            raise RuntimeError("HIVE_BACKEND=db2 requires DB2_DSN or HIVE_DB2_DSN")
        async with self.connection() as db:
            await db.executescript(DB2_DDL)
            try:
                await self._apply_migrations(db)
                await self._verify_schema(db)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    @staticmethod
    async def _columns(db: Any, table: str) -> set[str]:
        async with db.execute(
            "SELECT colname FROM syscat.columns "
            "WHERE tabschema = CURRENT SCHEMA AND tabname = ?",
            (table.upper(),),
        ) as cur:
            return {str(row[0]).strip().upper() for row in await cur.fetchall()}

    @staticmethod
    async def _column_lengths(db: Any, table: str) -> dict[str, int]:
        async with db.execute(
            "SELECT colname, length FROM syscat.columns "
            "WHERE tabschema = CURRENT SCHEMA AND tabname = ?",
            (table.upper(),),
        ) as cur:
            return {
                str(row[0]).strip().upper(): int(row[1])
                for row in await cur.fetchall()
            }

    @staticmethod
    async def _indexes(db: Any, table: str) -> set[str]:
        async with db.execute(
            "SELECT indname FROM syscat.indexes "
            "WHERE tabschema = CURRENT SCHEMA AND tabname = ?",
            (table.upper(),),
        ) as cur:
            return {str(row[0]).strip().upper() for row in await cur.fetchall()}

    @staticmethod
    async def _status_checks(db: Any, table: str) -> dict[str, str]:
        async with db.execute(
            "SELECT checkname, text FROM syscat.checks "
            "WHERE tabschema = CURRENT SCHEMA AND tabname = ?",
            (table.upper(),),
        ) as cur:
            return {
                str(row[0]).strip().upper(): str(row[1]).upper()
                for row in await cur.fetchall()
                if "STATUS" in str(row[1]).upper()
            }

    async def _apply_migrations(self, db: Any) -> None:
        async with db.execute("SELECT version FROM hive_schema_migrations") as cur:
            applied = {int(row[0]) for row in await cur.fetchall()}

        # Version 1 converges every additive runtime column. It is safe for a
        # populated database because nullable columns and non-null counters with
        # defaults do not rewrite or discard existing rows.
        # Always converge the canonical tables. Older releases could record
        # versions 1/2 after migrating their parallel AGENTS/JOBS tables; those
        # markers must not suppress migration of MNEMOS' HIVE_* tables.
        for table, definitions in _DB2_ADDITIVE_COLUMNS.items():
            columns = await self._columns(db, table)
            for name, fragment in definitions:
                if name not in columns:
                    await db.execute(f"ALTER TABLE {table.lower()} ADD COLUMN {fragment}")
                    columns.add(name)

        # Core created HIVE_JOBS.STATUS as VARCHAR(16), but failed_completion
        # needs 17 characters. Widen before allowing that value in the check.
        if (await self._column_lengths(db, "HIVE_JOBS")).get("STATUS", 0) < 32:
            await db.execute(
                "ALTER TABLE hive_jobs ALTER COLUMN status SET DATA TYPE VARCHAR(32)"
            )

        # Core's original checks predate the stale/failed_completion/
        # dead-letter/needs-review states used by the current service.
        for table, values, constraint in (
            ("HIVE_AGENTS", "'online','idle','stale','offline','error'", "ck_hive_agents_status_runtime"),
            ("HIVE_JOBS", "'queued','offered','claimed','running','done','failed','failed_completion','cancelled','dead-letter','needs-review'", "ck_hive_jobs_status_runtime"),
        ):
            checks = await self._status_checks(db, table)
            required_values = {value.strip("'") for value in values.split(",")}
            if not any(all(value.upper() in text for value in required_values)
                       for text in checks.values()):
                for old_constraint in checks:
                    await db.execute(f"ALTER TABLE {table.lower()} DROP CHECK {old_constraint}")
                await db.execute(
                    f"ALTER TABLE {table.lower()} ADD CONSTRAINT {constraint} "
                    f"CHECK (status IN ({values}))"
                )

        if 1 not in applied:
            await db.execute(
                "INSERT INTO hive_schema_migrations (version, applied_at) VALUES (?, ?)",
                (1, _dt.datetime.now(tz=_dt.timezone.utc).timestamp()),
            )

        # Version 2 supplies the concurrency guarantee used by create_job.
        # EXCLUDE NULL KEYS permits any number of terminal jobs while the
        # generated value makes active jobs unique by dedup_hash.
        # Pre-constraint deployments may already contain duplicate active
        # rows. Lock submissions out, preserve the oldest row for each hash,
        # and dead-letter later duplicates before creating the index.
        await db.execute("LOCK TABLE hive_jobs IN EXCLUSIVE MODE")
        async with db.execute(
            "SELECT id, dedup_hash FROM jobs "
            "WHERE dedup_hash IS NOT NULL "
            "AND status IN ('queued', 'offered', 'claimed', 'running') "
            "ORDER BY dedup_hash, started_at, id"
        ) as cur:
            active = await cur.fetchall()
        seen_hashes: set[str] = set()
        now = _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
        migration_result = _json.dumps({
            "dead_letter": True,
            "reason": "db2_migration:duplicate_active_dedup_hash",
        })
        for job_id, dedup_hash in active:
            if dedup_hash not in seen_hashes:
                seen_hashes.add(dedup_hash)
                continue
            await db.execute(
                "UPDATE jobs SET status='dead-letter', ended_at=?, last_update_at=?, "
                "result=COALESCE(result, ?) WHERE id=? "
                "AND status IN ('queued', 'offered', 'claimed', 'running')",
                (now, now, migration_result, job_id),
            )
        indexes = await self._indexes(db, "HIVE_JOBS")
        if "JOBS_ACTIVE_DEDUP_UQ" not in indexes:
            await db.execute(
                "CREATE UNIQUE INDEX jobs_active_dedup_uq "
                "ON jobs(dedup_active_hash) EXCLUDE NULL KEYS"
            )
        if 2 not in applied:
            await db.execute(
                "INSERT INTO hive_schema_migrations (version, applied_at) VALUES (?, ?)",
                (2, _dt.datetime.now(tz=_dt.timezone.utc).timestamp()),
            )

    async def _verify_schema(self, db: Any) -> None:
        for table, base in _DB2_REQUIRED_BASE_COLUMNS.items():
            expected = base | {name for name, _fragment in _DB2_ADDITIVE_COLUMNS[table]}
            missing = expected - await self._columns(db, table)
            if missing:
                raise RuntimeError(f"Db2 schema verification failed: {table} missing {sorted(missing)}")
        if "JOBS_ACTIVE_DEDUP_UQ" not in await self._indexes(db, "HIVE_JOBS"):
            raise RuntimeError("Db2 schema verification failed: jobs_active_dedup_uq is missing")
        status_length = (await self._column_lengths(db, "HIVE_JOBS")).get("STATUS", 0)
        if status_length < 17:
            raise RuntimeError(
                f"Db2 schema verification failed: HIVE_JOBS.STATUS length is {status_length}, expected at least 17"
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _ensure_pool(self) -> _Db2Pool:
        if self._pool is None:
            pool = _Db2Pool(self.dsn, min_size=self._min_size, max_size=self._max_size, acquire_timeout=self._acquire_timeout)
            await pool.warmup()
            self._pool = pool
        return self._pool

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if not self.dsn:
            raise RuntimeError("HIVE_BACKEND=db2 requires DB2_DSN or HIVE_DB2_DSN")
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            yield conn
