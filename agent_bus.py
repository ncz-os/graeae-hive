"""
GRAEAE Hive Mind — fleet-wide MCP-compatible agent coordination + triage queue.

Brand: GRAEAE Hive Mind (extension of GRAEAE consensus engine — sister to mnemos memory)
Identity: urn:agent:<kind>:<host>:<session_uuid>   (kinds: claude, opencode, codex, zeroclaw, openclaw, hermes, human, ic-engine, mnemos, ...)
Backend: SQLite WAL (Phase 1) — PG migration documented for Phase 2 when >50 agents OR LISTEN/NOTIFY needed
Pub/sub: SSE (Phase 1) — NATS migration Phase 2 when >20 concurrent OR >100 msg/s
Triage queue: /v1/jobs/next dequeues highest-priority eligible work; no central scheduler — agents self-claim.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sys
import time
import uuid
from contextlib import asynccontextmanager, suppress
from typing import Any

try:
    import aiosqlite
except ModuleNotFoundError:
    pass

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

import retry_policy
from hive.bus.deps import (
    ACTIVE_AGENT_STATUSES,
    EVENT_QUEUE,
    HOST_DENYLIST,
    TERMINAL_JOB_STATUSES,
    VALID_JOB_STATUSES,
    _cascade_delete_jobs,
    clamp_limit,
    connect_db,
    emit_event,
    hive_repo,
    require_mutation_actor,
)

# Capacity ledger (Phase 5) moved to hive.bus.domain.capacity_ledger (Phase 1c).
from hive.bus.domain.eligibility import (
    THROTTLE_HEADROOM,
    agent_kind_aliases,
    job_agent_eligible,
)
from hive.bus.domain.jobs import (
    CLAIM_LEASE_SECONDS,
    DECLINE_REASON_PREFIXES,
    MAX_DECLINE_REQUEUES,
    STATUS_TRANSITIONS,
    SUBSCRIPTION_PROVIDERS,
    cache_key_for,
    cache_store,
    estimate_cost,
    hallucination_check,
    json_list,
    normalize_result_payload,
    valid_completion_proof,
)
from hive.bus.routers.admin import admin_requeue_jobs  # noqa: F401 (re-export for tests)
from hive.bus.routers.admin import router as _admin_router
from hive.bus.routers.agents import heartbeat, list_agents, register
from hive.bus.routers.agents import router as _agents_router
from hive.bus.routers.jobs import create_job, get_job, list_jobs  # noqa: F401 (re-export)
from hive.bus.routers.jobs import router as _jobs_router
from hive.bus.routers.messages import list_messages, publish_message
from hive.bus.routers.messages import router as _messages_router
from hive.bus.routers.repos import router as _repos_router
from hive.bus.routers.schedules import router as _schedules_router
from hive.bus.schemas import (
    AgentHeartbeat,
    AgentRegister,
    JobCreate,
    JobPurgeRequest,  # noqa: F401 (re-export for tests)
    JobUpdate,
    MessagePublish,
    PurgeRequest,  # noqa: F401 (re-export for tests)
    ReclassifyRequest,  # noqa: F401 (re-export for tests)
    RequeueRequest,  # noqa: F401 (re-export for tests)
)
from hive.core.models import COST_TIERS, knemon_candidates

# Re-exported for back-compat: mcp_bridge consumers + tests read these off agent_bus.
from hive.core.roles import ORCHESTRATOR_RUNTIMES, WORKER_ONLY_RUNTIMES  # noqa: F401

DB_PATH = os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db")
HIVE_BACKEND = os.environ.get("HIVE_BACKEND", "sqlite").lower()
ORACLE_DSN = os.environ.get("ORACLE_DSN", "oracle://mnemos:mnemos_dev@127.0.0.1:1521/ORCLPDB1")
HEARTBEAT_REAP_INTERVAL = 30.0
HEARTBEAT_STALE_AFTER = 90.0
HEARTBEAT_OFFLINE_AFTER = 300.0
# Hard-delete agents that have been offline this long. Every worker restart
# registers a NEW session URN, so without this the agents table grows without
# bound (10,930 rows / 10,882 offline observed 2026-06-08) and poisons the
# dashboard worker panel + per-worker stats. 6h keeps a generous recent-history
# window while reaping long-dead session rows.
AGENT_PURGE_AFTER = float(os.environ.get("HIVE_AGENT_PURGE_AFTER", "21600"))
EVENTS_RETAIN_HOURS = 168  # 7 days
# Job retention sweep (housekeeping — the dashboard fills with test/duplicate
# jobs fast). Conservative defaults; both windows env-overridable.
#  - Terminal jobs (done/failed/failed_completion/cancelled/dead-letter) older
#    than RETAIN_DONE_JOBS_DAYS are auto-deleted.
#  - Obvious test/e2e jobs (description matches TEST_JOB_DESC_PATTERNS) terminal
#    AND older than RETAIN_TEST_JOBS_DAYS are deleted sooner.
# Active jobs (queued/offered/claimed/running/needs-review) are NEVER swept.
RETAIN_DONE_JOBS_DAYS = float(os.environ.get("RETAIN_DONE_JOBS_DAYS", "7"))
RETAIN_TEST_JOBS_DAYS = float(os.environ.get("RETAIN_TEST_JOBS_DAYS", "1"))
# Statuses considered terminal & sweepable. needs-review is intentionally
# EXCLUDED (it is a human-attention queue, not finished work).
SWEEPABLE_STATUSES = ("done", "failed", "failed_completion", "cancelled", "dead-letter")
# Active/in-flight statuses that delete endpoints + the sweep refuse to touch
# (without explicit force/include_active) so an in-flight worker is never orphaned.
# ACTIVE_JOB_STATUSES moved to hive.bus.deps (imported above).
# Substrings (case-insensitive) that mark a job description as obvious test junk.
TEST_JOB_DESC_PATTERNS = (
    "e2e",
    "testdispatch",
    "append a one-line",
    "dated comment",
    "xfam",
    "validate native",
    "hive functional",
    "hive deploy e2e",
)
SSE_PING_INTERVAL = 15.0
# EVENT_QUEUE now lives in hive.bus.deps (imported above).
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", "5000"))
MAX_EVENT_SUBSCRIBERS = int(os.environ.get("AGENT_BUS_MAX_EVENT_SUBSCRIBERS", "100"))


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


def _parse_oracle_url(url: str) -> tuple[str, str, str]:
    from urllib.parse import unquote, urlparse

    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        return "", "", url
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1521
    service = (parsed.path or "/ORCLPDB1").lstrip("/")
    return user, password, f"{host}:{port}/{service}"


if HIVE_BACKEND not in {"sqlite", "oracle", "db2"}:
    raise RuntimeError(f"HIVE_BACKEND must be sqlite, oracle, or db2, got {HIVE_BACKEND!r}")

if HIVE_BACKEND == "oracle":
    try:
        import oracledb
    except Exception as exc:
        print(
            f"HIVE_BACKEND=oracle requires python-oracledb importable: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise
    try:
        _oracle_user, _oracle_password, _oracle_connect_dsn = _parse_oracle_url(ORACLE_DSN)
        _probe = oracledb.connect(user=_oracle_user, password=_oracle_password, dsn=_oracle_connect_dsn)
        _probe.close()
    except Exception as exc:
        print(
            f"HIVE_BACKEND=oracle failed initial Oracle connection to {ORACLE_DSN!r}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise
else:
    oracledb = None

_HIVE_REPO = None


# ---------- helpers ----------


def _replace_qmarks(sql: str) -> str:
    out = []
    bind_index = 1
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            out.append(ch)
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append(sql[i + 1])
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            out.append(ch)
            in_double = not in_double
        elif ch == "?" and not in_single and not in_double:
            out.append(f":{bind_index}")
            bind_index += 1
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _rewrite_tables(sql: str) -> str:
    names = "|".join(sorted(map(re.escape, _TABLE_MAP), key=len, reverse=True))

    def repl(match: re.Match) -> str:
        return f"{match.group(1)} {_TABLE_MAP[match.group(2).lower()]}"

    sql = re.sub(
        rf"\b(FROM|INTO|UPDATE|DELETE\s+FROM|JOIN)\s+({names})\b",
        repl,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        rf"\b(ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE)\s+({names})\b",
        repl,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(rf"\b(ON)\s+({names})\b", repl, sql, flags=re.IGNORECASE)
    return sql


def _rewrite_limit(sql: str) -> str:
    return re.sub(
        r"\s+LIMIT\s+(:\d+|\d+)\s*$",
        r" FETCH FIRST \1 ROWS ONLY",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_sqlite_time_funcs(sql: str) -> str:
    sql = re.sub(
        r"datetime\((\w+),'unixepoch'\)",
        r"TO_CHAR(TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(\1, 'SECOND'), 'YYYY-MM-DD HH24:MI:SS')",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"DATE\((\w+), 'unixepoch'\)",
        r"TRUNC(TIMESTAMP '1970-01-01 00:00:00' + NUMTODSINTERVAL(\1, 'SECOND'))",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _translate_upsert(sql: str) -> str | None:
    normalized = " ".join(sql.split())
    cache_pattern = (
        r"INSERT INTO hive_cache \(cache_key, result_json, source_job_id, result_mnemos_id, "
        r"hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at\) VALUES "
        r"\(:1, :2, :3, :4, 0, 0, :5, :6, :7, NULL\) ON CONFLICT\(cache_key\) DO UPDATE SET"
    )
    if re.search(cache_pattern, normalized, flags=re.IGNORECASE):
        return (
            "MERGE INTO hive_cache t "
            "USING (SELECT :1 cache_key, :2 result_json, :3 source_job_id, :4 result_mnemos_id, "
            ":5 model, :6 provider, :7 cached_at FROM dual) s "
            "ON (t.cache_key = s.cache_key) "
            "WHEN MATCHED THEN UPDATE SET "
            "t.result_json=s.result_json, t.source_job_id=s.source_job_id, "
            "t.result_mnemos_id=s.result_mnemos_id, t.cached_at=s.cached_at, "
            "t.model=s.model, t.provider=s.provider "
            "WHEN NOT MATCHED THEN INSERT "
            "(cache_key, result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
            "VALUES (s.cache_key, s.result_json, s.source_job_id, s.result_mnemos_id, 0, 0, s.model, s.provider, s.cached_at, NULL)"
        )
    stats_match = re.search(
        r"INSERT INTO hive_worker_kind_stats \(urn, kind, (success_count|fail_count|cancelled_count), "
        r"total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run\) "
        r"VALUES \(:1, :2, 1, :3, :4, :5, :6, :7\) ON CONFLICT\(urn, kind\) DO UPDATE SET",
        normalized,
        flags=re.IGNORECASE,
    )
    if stats_match:
        col = stats_match.group(1)
        return (
            "MERGE INTO hive_worker_kind_stats t "
            "USING (SELECT :1 urn, :2 kind, :3 ins_tokens_in, :4 ins_tokens_out, :5 ins_cost, "
            ":6 ins_duration, :7 ins_last_run, :8 upd_tokens_in, :9 upd_tokens_out, "
            ":10 upd_cost, :11 upd_duration, :12 upd_last_run FROM dual) s "
            "ON (t.urn = s.urn AND t.kind = s.kind) "
            f"WHEN MATCHED THEN UPDATE SET t.{col}=t.{col}+1, "
            "t.total_tokens_in=t.total_tokens_in+s.upd_tokens_in, "
            "t.total_tokens_out=t.total_tokens_out+s.upd_tokens_out, "
            "t.total_cost_usd=t.total_cost_usd+s.upd_cost, "
            "t.total_duration_sec=t.total_duration_sec+s.upd_duration, "
            "t.last_run=s.upd_last_run "
            "WHEN NOT MATCHED THEN INSERT "
            f"(urn, kind, {col}, total_tokens_in, total_tokens_out, total_cost_usd, total_duration_sec, last_run) "
            f"VALUES (s.urn, s.kind, 1, s.ins_tokens_in, s.ins_tokens_out, s.ins_cost, s.ins_duration, s.ins_last_run)"
        )
    return None


def _translate_sql(sql: str, params: Any = None) -> tuple[str | None, Any]:
    stripped = sql.strip()
    upper = stripped.upper()
    if not stripped:
        return None, params
    if upper.startswith("PRAGMA"):
        return None, params
    if upper in {"BEGIN", "BEGIN IMMEDIATE"}:
        return None, params
    if upper.startswith(("CREATE ", "ALTER TABLE ", "DROP TABLE ")):
        return None, params
    if "SQLITE_MASTER" in upper:
        return (
            "SELECT 'CREATE TABLE agents (status TEXT CHECK(status IN (''online'',''idle'',''stale'',''offline'',''error'')))' FROM dual",
            (),
        )

    sql = _rewrite_tables(stripped)
    sql = _replace_qmarks(sql)
    sql = _rewrite_sqlite_time_funcs(sql)
    upsert_sql = _translate_upsert(sql)
    if upsert_sql:
        return upsert_sql, tuple(params or ())
    sql = _rewrite_limit(sql)
    return sql, tuple(params or ())


class BackendCursor:
    def __init__(self, cursor=None, rows: list | None = None, rowcount: int = -1):
        self._cursor = cursor
        self._rows = rows
        self._index = 0
        self.rowcount = rowcount if cursor is None else getattr(cursor, "rowcount", rowcount)
        self.lastrowid = getattr(cursor, "lastrowid", None) if cursor is not None else None

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

    async def execute(self, sql: str, params: Any = None):
        translated, translated_params = _translate_sql(sql, params)
        if translated is None:
            self._rows = []
            self._index = 0
            self.rowcount = -1
            return self
        await self._cursor.execute(translated, translated_params)
        self.rowcount = getattr(self._cursor, "rowcount", -1)
        return self

    @staticmethod
    async def _normalize_row(row):
        """Normalize Oracle-specific types to sqlite-equivalent shapes:
        - Oracle 23ai auto-decodes JSON CLOBs to dict/list -> re-serialize to str
        - NUMBER columns return Decimal -> float for JSON serializability
        - CLOB objects -> str via .read()
        - TIMESTAMP WITH TIME ZONE -> float epoch seconds (matches SQLite REAL)
        - bytes (RAW(16) for UUID id cols) -> uuid hex string (matches SQLite TEXT)
        """
        if row is None:
            return None
        import datetime as _dt
        import json as _json
        import uuid as _uuid
        from decimal import Decimal as _Decimal

        def _json_default(o):
            if isinstance(o, _Decimal):
                return float(o) if o != o.to_integral_value() else int(o)
            if isinstance(o, (bytes, bytearray)):
                if len(o) == 16:
                    return str(_uuid.UUID(bytes=bytes(o)))
                return o.decode("utf-8", errors="replace")
            if isinstance(o, _dt.datetime):
                return o.isoformat()
            raise TypeError(f"unhandled type {type(o).__name__}")

        out = []
        for v in row:
            if isinstance(v, (dict, list)):
                out.append(_json.dumps(v, separators=(",", ":"), default=_json_default))
            elif hasattr(v, "read") and callable(v.read):
                try:
                    r = v.read()
                    import inspect as _inspect

                    if _inspect.iscoroutine(r):
                        r = await r
                    out.append(r)
                except Exception:
                    out.append(str(v))
            elif isinstance(v, _Decimal):
                # Preserve int-ness for integral Decimals (epoch counts, IDs, statuses)
                if v == v.to_integral_value():
                    out.append(int(v))
                else:
                    out.append(float(v))
            elif isinstance(v, _dt.datetime):
                # Oracle TSTZ -> epoch float (SQLite stores as REAL epoch)
                if v.tzinfo is None:
                    v = v.replace(tzinfo=_dt.UTC)
                out.append(v.timestamp())
            elif isinstance(v, (bytes, bytearray)) and len(v) == 16:
                # RAW(16) -> uuid hex with dashes (matches SQLite TEXT id format)
                out.append(str(_uuid.UUID(bytes=bytes(v))))
            else:
                out.append(v)
        return tuple(out)

    async def fetchone(self):
        if self._rows is not None:
            if self._index >= len(self._rows):
                return None
            row = self._rows[self._index]
            self._index += 1
            return await self._normalize_row(row)
        row = await self._cursor.fetchone()
        return await self._normalize_row(row)

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index :]
            self._index = len(self._rows)
            return [await self._normalize_row(r) for r in rows]
        rows = await self._cursor.fetchall()
        return [await self._normalize_row(r) for r in rows]

    async def close(self):
        if self._cursor is not None:
            close = getattr(self._cursor, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result


class _OracleExecuteResult:
    def __init__(self, conn: OracleConnection, sql: str, params: Any = None):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cursor = None

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


class OracleConnection:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def execute(self, sql: str, params: Any = None):
        return _OracleExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None):
        if re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE):
            table = re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE).group(1)
            mapped = _TABLE_MAP.get(table.lower(), table)
            cur = self._conn.cursor()
            await cur.execute(
                "SELECT column_name FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
                (mapped.upper(),),
            )
            rows = [(i, row[0].lower(), None, None, None, None) for i, row in enumerate(await cur.fetchall())]
            await BackendCursor(cur).close()
            return BackendCursor(rows=rows)
        translated, translated_params = _translate_sql(sql, params)
        if translated is None:
            return BackendCursor(rows=[])
        cur = self._conn.cursor()
        await cur.execute(translated, translated_params)
        return BackendCursor(cur)

    async def executemany(self, sql: str, seq_of_params):
        translated, _ = _translate_sql(sql, ())
        if translated is None:
            return BackendCursor(rows=[])
        cur = self._conn.cursor()
        translated = _rewrite_limit(translated)
        await cur.executemany(translated, [tuple(p) for p in seq_of_params])
        return BackendCursor(cur)

    async def executescript(self, script: str):
        # Oracle deployments are expected to have run 0010/0011-equivalent DDL.
        # SQLite schema/migration scripts are deliberately skipped in oracle mode.
        return BackendCursor(rows=[])

    def cursor(self):
        return BackendCursor(self._conn.cursor())

    async def commit(self):
        result = self._conn.commit()
        if hasattr(result, "__await__"):
            await result

    async def rollback(self):
        result = self._conn.rollback()
        if hasattr(result, "__await__"):
            await result

    async def close(self):
        result = self._conn.close()
        if hasattr(result, "__await__"):
            await result


def stable_worker_id(urn: str) -> str:
    """Collapse a session URN to a stable per-worker identity for stats.

    ``urn:agent:<kind>:<host>:<session_uuid>`` -> ``urn:agent:<kind>:<host>``.
    The session segment changes on every worker restart, so keying worker
    stats by the full URN fragments one worker's history across thousands of
    rows (3,176 rows / 43 live workers observed 2026-06-08). Keying by the
    stable prefix aggregates a worker's runs across restarts. Non-conforming
    URNs are returned unchanged.
    """
    if not urn:
        return urn
    parts = urn.split(":")
    # urn:agent:<kind>:<host>:<session> -> 5 parts; drop the trailing session.
    # Drop ONLY the trailing session segment so colon-bearing hosts (IPv6) are
    # preserved — splitting on ':' and keeping parts[:4] would corrupt them.
    if len(parts) >= 5 and parts[0] == "urn" and parts[1] == "agent":
        return urn.rsplit(":", 1)[0]
    return urn


MAX_REPORTED_TOKENS = 100_000_000  # sane ceiling; rejects absurd worker telemetry


def _safe_nonneg_int(v, default: int = 0) -> int:
    """Coerce an untrusted worker-reported value to a clamped non-negative int.

    Worker result bodies are runtime/untrusted; a malformed token field
    (``"12.5"``, ``{}``, ``None``, a negative, ``Infinity``/``NaN`` which JSON
    can carry, a 100-digit string) must NOT raise on the hot job-completion
    path, decrement cumulative counters, or overflow the numeric column."""
    if isinstance(v, bool):  # bool is an int subclass — never a token count
        return default
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return default
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        return default
    if n <= 0:
        return 0
    return min(n, MAX_REPORTED_TOKENS)


def _safe_str(v, default: str = "unknown") -> str:
    if v is None:
        return default
    try:
        s = str(v).strip()
        return s or default
    except Exception:  # noqa: BLE001
        return default


# ---------- schema ----------

SCHEMA = """
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
"""


# ---------- models ----------

# Runtime → eligible kinds map (Kimi-K2.6 advisory 2026-05-23).
# AUTONOMY_LEVELS / AUTH_METHODS / DEFAULT_PLAN_CAPS / VALID_AGENT_STATUSES
# moved to hive.bus.deps (imported above).
# ACTIVE_AGENT_STATUSES moved to hive.bus.deps (imported above).

# ROLE SPLIT (user directive 2026-05-23): opencode + codex + hermes +
# claw-family + ic-engine + unknown = WORKERS (claim-only). Cannot submit jobs.
# Orchestrators include a distinct Codex role. Ordinary Codex workers stay claim-only.
# WORKER_ONLY_RUNTIMES / ORCHESTRATOR_RUNTIMES now live in hive.core.roles
# (single source of truth, shared with mcp_bridge.py). Imported above.


# HOST AFFINITY — automatic eligible_hosts injection based on job kind prefix.
# When a job kind matches a prefix, the server adds eligible_hosts if not already set.
# This ensures e.g. cixmini-os: jobs only run on cixmini hardware without callers needing to specify.
KIND_HOST_AFFINITY: dict[str, list[str]] = {
    # Argonaut DBPR work requires the florida-licenses workspace. Route to
    # a verified zeroclaw host with the Argonaut workspace map installed.
    "argonaut:": [
        "ULTRA",
        "HYDRA",
    ],  # TYPHON offline 2026-06-01; retargeted to live zeroclaw hosts w/ florida-licenses workspace
    # Hardware-bound: needs physical CIX Sky1 NPU — zeroclaw cannot substitute with SSH
    "cixmini-os:": ["cixmini"],
    "ncz-os:": ["cixmini"],
    # Pi-specific tasks (explicit opt-in; general jobs use any worker)
    "pi:": ["bigpi", "clawpi", "zeropi"],
    "arm64-test:": ["bigpi", "clawpi", "zeropi", "cixmini"],
    # Note: investorclaw/investorclaude are NOT here — any zeroclaw can execute
    # those by SSHing to the appropriate host. Host affinity would over-restrict.
}

# (Removed 2026-10-15): the bus-side host-quarantine constant/probe. The only
# host it ever guarded was the NVIDIA-LAN Spark (spark-0c53, out-claiming
# zeroclaw jobs); Spark is unsupported going forward — the fleet is local-LAN
# only. Live host exclusion is the systemd `HIVE_HOST_DENYLIST` env, not this.


# 2026-10-02 (operator): hold sentinels are DEPRECATED. They existed to park jobs
# behind the zeroclaw pre-submission gate, which is no longer enforced. Treat
# these as unrestricted so their jobs are schedulable by capable workers.
# ``rescue-superseded-hold`` is deliberately NOT in this set: those jobs are
# superseded and must be cancelled rather than run.

# WORKSPACE AFFINITY — automatic required_capabilities injection + claim guard.
# Workspace-scoped jobs must only be offered to workers that explicitly
# advertise the matching workspace capability. This protects both newly
# submitted jobs and older queued rows that were created before the mapping.


# COST-TIER MAP (per ~/.claude/rules/llm-usage-policy-2026-05-22.md):
#   A = FREE   — local + NGC NIM (try first, token-miser)
#   B = CHEAP  — Groq Dev tier, xAI, DeepSeek direct, Together cheap, Gemini-Flash, OpenAI-mini
#   C = RESERVE — Anthropic Opus/Sonnet, OpenAI GPT-5.5/Pro, Gemini Pro, Together DeepSeek-Pro
#                 (Together DeepSeek-V4-Pro = anti-pattern — use DeepSeek direct instead)
# PROVIDER_COST_TIER / COST_TIERS now live in hive.core.models (imported above).
# VALID_JOB_STATUSES / TERMINAL_JOB_STATUSES moved to hive.bus.deps (imported above).
# `released` was terminal here but is NOT in the jobs.status CHECK constraint and
# is produced by no transition, so PATCH status=released passed validation then
# died in the DB as a raw IntegrityError (500, not 4xx). An unreachable
# terminal state is worse than no such state.
# Skip releases (host_declines_kind/released_by_host/no_workspace_for_kind) are
# legitimate "not me" routing signals. They must not burn the decline dead-letter
# threshold; real execution failures use the failed/retry path.
# job consts (MAX_DECLINE_REQUEUES/DECLINE_REASON_PREFIXES/STATUS_TRANSITIONS/CLAIM_LEASE_SECONDS) moved to hive.bus.domain.jobs (imported above).


#: Eligibility is evaluated in Python (job_agent_eligible), so the candidate
#: query cannot know which rows THIS agent can take. A single fixed window
#: therefore starves any agent whose work sits behind rows it is ineligible
#: for: it gets 204 forever while claimable work sits queued. Scan deeper,
#: bounded on both sides so a huge queue cannot turn a claim into a scan.
# CLAIM_SCAN_WINDOW moved to hive.bus.deps (imported above).
# Hosts the bus refuses to offer jobs to — typically because their workers
# have a broken config (e.g. enc2 secret-key mismatch) and would fail any
# claim immediately. User directive 2026-05-26: pegasus (.79) is flooding
# the failed queue with 241/243 recent fails — denylist it until config-fix.
# Format: lowercase short hostname. Updated via env or admin endpoint.
# HOST_DENYLIST moved to hive.bus.deps (imported above).

# Phase 2 provider-as-capability gate. When ON, an executor job (build / agent
# coding kind) can only be claimed by a host advertising >=1 live llm-* cap, so a
# host whose only LLM provider is dead self-excludes (no host denylist needed).
# Default OFF until workers advertise llm-* fleet-wide (flip after Phase 4).


# cost_tier_for moved to hive.core.models (imported above).


# json_list moved to hive.bus.domain.jobs (imported above).


def field_was_set(model: BaseModel, name: str) -> bool:
    fields_set = getattr(model, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(model, "__fields_set__", set())
    return name in fields_set


# valid_completion_proof moved to hive.bus.domain.jobs (imported above).


# job result helpers + result-cache moved to hive.bus.domain.jobs (Phase 1c).


# ---------- lifecycle ----------

CLAIM_STALE_AFTER = 1800.0  # claim → still 'claimed'/'running' without update >30min ⇒ orphan
# REVIEW #2 fix: kind-specific overrides. Orchestration jobs that fan out
# + collect can legitimately run 60+ minutes without a PATCH while
# sub-jobs execute. Reaper also measures against last_update_at (any PATCH
# touches it) instead of just claimed_at — so a steady stream of progress
# patches keeps the orphan reaper at bay.
CLAIM_STALE_AFTER_BY_KIND = {
    "orchestration": 3600.0,
    "investigation": 3600.0,
    "benchmark": 3600.0,
    "migration": 7200.0,  # multi-host db / fleet migrations need wide window
}

# Absolute, UN-REFRESHABLE claim ceiling (operator 2026-06-20). The lease +
# last_update_at staleness checks are BOTH refreshed by any PATCH, so a worker
# stuck re-attempting+failing the SAME job kept them fresh and a 51h 'running'
# job never reaped (019ed7a4-7e33). This cap measures wall-clock since the FIRST
# claim (claimed_at — never touched by PATCH), so a genuinely-stuck claim is
# reaped no matter how often its claimant pokes it.
CLAIM_HARD_MAX_AGE = float(os.environ.get("HIVE_CLAIM_HARD_MAX_AGE", "14400"))  # 4h
CLAIM_HARD_MAX_AGE_BY_KIND = {
    "migration": 21600.0,  # 6h — multi-host fleet migrations may legitimately run long
}


async def reaper_task(app: FastAPI):
    while True:
        await asyncio.sleep(HEARTBEAT_REAP_INTERVAL)
        try:
            async with connect_db() as db:
                # 1. Heartbeat reaper: online/idle -> stale after 90s, offline after 5m.
                now_ts = time.time()
                stale_cutoff = now_ts - HEARTBEAT_STALE_AFTER
                offline_cutoff = now_ts - HEARTBEAT_OFFLINE_AFTER
                async with db.execute(
                    "SELECT urn FROM agents "
                    "WHERE status IN ('online','idle','error') "
                    "AND last_heartbeat < ? AND last_heartbeat >= ?",
                    (stale_cutoff, offline_cutoff),
                ) as cur:
                    stale = [row[0] async for row in cur]
                if stale:
                    await db.executemany(
                        "UPDATE agents SET status='stale' WHERE urn=? AND status != 'offline'",
                        [(u,) for u in stale],
                    )
                    await db.commit()
                    for urn in stale:
                        await emit_event(db, "agent.stale", {"urn": urn, "reason": "heartbeat_stale"})

                async with db.execute(
                    "SELECT urn FROM agents WHERE status != 'offline' AND last_heartbeat < ?",
                    (offline_cutoff,),
                ) as cur:
                    dead = [row[0] async for row in cur]
                if dead:
                    for urn in dead:
                        # A MISSED HEARTBEAT IS NOT PROOF OF AN ABANDONED JOB.
                        # This used to requeue offered/claimed/RUNNING work and null
                        # its result. A worker on a flaky link (not a dead one) lost
                        # two heartbeats, its running job reset to 'queued', and when
                        # it reported back the result was already gone -- PATCH 'done'
                        # is not legal out of 'queued', so it took a 409 and the work
                        # was silently discarded. Reclaiming in-flight work is step
                        # 2's job, and step 2 measures staleness against
                        # COALESCE(last_update_at, claimed_at) so a worker still
                        # PATCHing progress is never reclaimed. Here we release only
                        # work that has not STARTED, which cannot lose a result
                        # because none exists yet.
                        async with db.execute(
                            "SELECT id FROM jobs WHERE claimed_by=? AND status IN ('offered','claimed')",
                            (urn,),
                        ) as cur:
                            assigned_jobs = [row[0] async for row in cur]
                        await db.execute(
                            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL, "
                            "ended_at=NULL, result=NULL "
                            "WHERE claimed_by=? AND status IN ('offered','claimed')",
                            (urn,),
                        )
                        await db.execute(
                            "UPDATE agents SET status='offline' WHERE urn=?",
                            (urn,),
                        )
                        for job_id in assigned_jobs:
                            await emit_event(
                                db,
                                "job.unclaimed",
                                {
                                    "id": job_id,
                                    "prior_claimer": urn,
                                    "reason": "agent_offline_heartbeat_timeout",
                                },
                            )
                    await db.commit()
                    for urn in dead:
                        await emit_event(
                            db,
                            "agent.offline",
                            {"urn": urn, "reason": "heartbeat_timeout"},
                        )

                # 1b. AGENT PURGE: hard-delete agents offline beyond AGENT_PURGE_AFTER.
                # Each worker restart registers a fresh session URN; without this the
                # agents table grows unbounded (10,930 rows / 10,882 offline observed
                # 2026-06-08) and poisons the dashboard worker panel. Done as a single
                # SET-BASED delete that re-checks every safety condition AT DELETE TIME
                # (status, heartbeat age, no in-flight claim) so an agent that heartbeats
                # or claims work between a select and a delete cannot be removed — there
                # is no select/delete window. Claim recovery (step 2) runs after, so the
                # NOT EXISTS guard is the authoritative protection for actively-claimed
                # URNs, not ordering.
                purge_cutoff = now_ts - AGENT_PURGE_AFTER
                pcur = await db.execute(
                    "DELETE FROM agents WHERE status='offline' AND last_heartbeat < ? "
                    # non-correlated NOT IN (was a correlated `agents.urn` subquery): the
                    # sqlite->Oracle table-name rewrite only translates FROM/JOIN/ON positions,
                    # so a table-qualified `agents.urn` survived untranslated and ORA-00904'd,
                    # crashing the whole reaper every 30s. IS NOT NULL guards NOT-IN null-poison.
                    "AND (urn IS NULL OR urn NOT IN (SELECT claimed_by FROM jobs "
                    "WHERE claimed_by IS NOT NULL AND status IN ('offered','claimed','running')))",
                    (purge_cutoff,),
                )
                purged = max(0, getattr(pcur, "rowcount", 0) or 0)  # some drivers report -1
                if purged:
                    await db.commit()
                    print(
                        f"agent purge: removed {purged} agents offline > {AGENT_PURGE_AFTER:.0f}s",
                        flush=True,
                    )
                # 2. ORPHAN CLAIM RECOVERY — jobs claimed by dead/stale workers go back to queue.
                # REVIEW #2 fix: measure staleness against COALESCE(last_update_at, claimed_at)
                # so steady PATCH progress keeps reaper at bay. Kind-specific cutoffs let
                # orchestration / investigation / migration jobs run longer than default 30min.
                now_ts = time.time()
                default_cutoff = now_ts - CLAIM_STALE_AFTER
                hard_cutoff = now_ts - CLAIM_HARD_MAX_AGE
                async with db.execute(
                    "SELECT j.id, j.kind, j.claimed_by, j.claimed_at, j.last_update_at, "
                    "j.claim_lease_expires_at "
                    "FROM jobs j "
                    "LEFT JOIN agents a ON a.urn = j.claimed_by "
                    "WHERE j.status IN ('claimed','running') "
                    # j.claimed_at IS NULL = corrupt/phantom claim (an atomic claim always
                    # writes claimed_at): without this the all-NULL-timestamp staleness
                    # predicates below are UNKNOWN, so a phantom running job is never a
                    # candidate and is un-reapable. Surfacing it lets the python filter
                    # (latest=0) requeue it.
                    "AND ( a.status = 'offline' OR a.urn IS NULL OR "
                    "      j.claimed_at IS NULL OR "
                    "      j.claim_lease_expires_at <= ? OR "
                    "      j.claimed_at < ? OR "
                    "      COALESCE(j.last_update_at, j.claimed_at) < ? )",
                    (
                        now_ts,
                        hard_cutoff,
                        default_cutoff,
                    ),
                ) as cur:
                    candidates = [(r[0], r[1], r[2], r[3], r[4], r[5]) async for r in cur]
                # Apply kind-specific override: keep job if its kind has a longer TTL
                # AND its latest progress timestamp is within that window.
                orphans = []
                for jid, jkind, claimer, c_at, u_at, lease_exp in candidates:
                    if lease_exp is not None and lease_exp <= now_ts:
                        orphans.append((jid, claimer))
                        continue
                    # HARD CAP (un-refreshable): wall-clock since FIRST claim.
                    # Defeats the retry-loop evasion where lease + last_update
                    # stay fresh but the job never progresses.
                    hard_ttl = CLAIM_HARD_MAX_AGE_BY_KIND.get(jkind, CLAIM_HARD_MAX_AGE)
                    if c_at is not None and (now_ts - c_at) >= hard_ttl:
                        orphans.append((jid, claimer))
                        continue
                    kind_ttl = CLAIM_STALE_AFTER_BY_KIND.get(jkind, CLAIM_STALE_AFTER)
                    latest = u_at if u_at is not None else (c_at or 0)
                    if (now_ts - latest) >= kind_ttl:
                        orphans.append((jid, claimer))
                if orphans:
                    for job_id, claimer in orphans:
                        await db.execute(
                            "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                            "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                            "claimed_cost_tier=NULL, claim_lease_expires_at=NULL, "
                            "ended_at=NULL, result=NULL "
                            "WHERE id=? AND status IN ('claimed','running')",
                            (job_id,),
                        )
                        await emit_event(
                            db,
                            "job.unclaimed",
                            {
                                "id": job_id,
                                "prior_claimer": claimer,
                                "reason": "worker_offline_or_stale_claim",
                            },
                        )
                    await db.commit()
                # 3. Scheduler — fire due interval-based jobs
                now_ts = time.time()
                async with db.execute(
                    "SELECT id, name, created_by_urn, interval_seconds, job_template, fire_count "
                    "FROM scheduled_jobs WHERE enabled=1 AND next_fire_at <= ?",
                    (now_ts,),
                ) as cur:
                    due = [tuple(r) async for r in cur]
                for sched_id, sname, sub_urn, interval, tpl_json, fcount in due:
                    try:
                        tpl = json.loads(tpl_json)
                        tpl["submitter_urn"] = sub_urn
                        # synth a JobCreate + go through the cache+role machinery
                        await create_job(JobCreate(**tpl))  # type: ignore
                        await db.execute(
                            "UPDATE scheduled_jobs SET last_fired_at=?, next_fire_at=?, "
                            "fire_count=fire_count+1 WHERE id=?",
                            (now_ts, now_ts + interval, sched_id),
                        )
                        await emit_event(
                            db,
                            "schedule.fired",
                            {
                                "schedule_id": sched_id,
                                "name": sname,
                                "fire_count": fcount + 1,
                                "next_at": now_ts + interval,
                            },
                        )
                    except Exception as se:
                        print(f"scheduler error {sched_id}: {se}", flush=True)
                if due:
                    await db.commit()
                # 4. Auto-cancel stale jobs (queued > 7 days untouched)
                stale_job_cutoff = time.time() - 7 * 24 * 3600
                async with db.execute(
                    "SELECT id FROM jobs WHERE status='queued' AND started_at < ?",
                    (stale_job_cutoff,),
                ) as cur:
                    stale_ids = [r[0] async for r in cur]
                if stale_ids:
                    await db.executemany(
                        "UPDATE jobs SET status='cancelled', ended_at=? WHERE id=?",
                        [(time.time(), j) for j in stale_ids],
                    )
                    for j in stale_ids:
                        await emit_event(db, "job.cancelled", {"id": j, "reason": "stale_>7d"})
                    await db.commit()
                # 5. Purge old events
                retain_cutoff = time.time() - EVENTS_RETAIN_HOURS * 3600
                await db.execute("DELETE FROM events WHERE ts < ?", (retain_cutoff,))
                await db.commit()
                # 6. JOB RETENTION SWEEP (housekeeping — keeps the dashboard clean).
                # Auto-delete OLD TERMINAL jobs (+ cascade job_audit_log). Two tiers:
                #   (a) obvious test/e2e junk older than RETAIN_TEST_JOBS_DAYS
                #   (b) any terminal job older than RETAIN_DONE_JOBS_DAYS
                # Active statuses (queued/offered/claimed/running/needs-review) are
                # NEVER candidates — the status filter excludes them, so an in-flight
                # worker can never be reaped here. Conservative + logged. Age is taken
                # from COALESCE(ended_at, started_at). Done as SELECT-then-cascade so
                # the same single-id cascade helper handles audit-log rows on both
                # backends; the re-check is implicit (status is in the SELECT predicate
                # and terminal jobs do not transition back to active).
                now_ts = time.time()
                sweep_ids: list[str] = []
                # (a) test/e2e junk — terminal + matches any test pattern + > test window
                test_cutoff = now_ts - RETAIN_TEST_JOBS_DAYS * 86400
                status_ph = ",".join("?" for _ in SWEEPABLE_STATUSES)
                like_ph = " OR ".join("LOWER(description) LIKE ?" for _ in TEST_JOB_DESC_PATTERNS)
                test_params: list[Any] = list(SWEEPABLE_STATUSES)
                test_params += [f"%{p.lower()}%" for p in TEST_JOB_DESC_PATTERNS]
                test_params.append(test_cutoff)
                async with db.execute(
                    f"SELECT id FROM jobs WHERE status IN ({status_ph}) "
                    f"AND description IS NOT NULL AND ({like_ph}) "
                    f"AND COALESCE(ended_at, started_at) < ?",
                    tuple(test_params),
                ) as cur:
                    sweep_ids += [r[0] async for r in cur]
                # (b) any terminal job older than the done-retention window
                done_cutoff = now_ts - RETAIN_DONE_JOBS_DAYS * 86400
                done_params: list[Any] = list(SWEEPABLE_STATUSES)
                done_params.append(done_cutoff)
                async with db.execute(
                    f"SELECT id FROM jobs WHERE status IN ({status_ph}) AND COALESCE(ended_at, started_at) < ?",
                    tuple(done_params),
                ) as cur:
                    sweep_ids += [r[0] async for r in cur]
                sweep_ids = list(dict.fromkeys(sweep_ids))  # de-dupe, keep order
                if sweep_ids:
                    # allow_active defaults False: the cascade re-checks status at
                    # delete time, so a job that somehow flipped back to active
                    # between SELECT and DELETE is left alone (atomic guard).
                    swept = await _cascade_delete_jobs(db, sweep_ids)
                    await db.commit()
                    if swept:
                        print(
                            f"job retention sweep: removed {len(swept)} terminal "
                            f"jobs (test<{RETAIN_TEST_JOBS_DAYS}d / done<{RETAIN_DONE_JOBS_DAYS}d)",
                            flush=True,
                        )
        except Exception as e:
            print(f"reaper error: {e}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if HIVE_BACKEND == "db2":
        # Db2 owns its DDL via the repository (init/close). Do NOT run the
        # sqlite schema-convergence path below, which needs a raw connection
        # handle the Db2 repo does not expose.
        repo = hive_repo()
        await repo.init()
        task = asyncio.create_task(reaper_task(app))
        app.state.reaper_task = task
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await repo.close()
        return
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with connect_db() as db:
        await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        await db.executescript(SCHEMA)

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
            await conn.execute(f"INSERT INTO agents ({col_list}) SELECT {col_list} FROM agents__pre_stale_migration")
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
            await conn.executescript(SCHEMA)  # recreate jobs (+others, all IF NOT EXISTS) with new CHECK
            # Copy only columns common to old + recreated table — guards against
            # drift columns the recreated SCHEMA lacks (the live_columns ALTERs
            # below re-add any expected-but-missing ones afterwards).
            async with conn.execute("PRAGMA table_info(jobs)") as cur:
                new_cols = {r[1] for r in await cur.fetchall()}
            common = [c for c in old_cols if c in new_cols]
            col_list = ", ".join(common)
            await conn.execute(f"INSERT INTO jobs ({col_list}) SELECT {col_list} FROM jobs__pre_deadletter_migration")
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
            ("jobs", "pr_number", "pr_number INTEGER"),
        )
        # Run CHECK-constraint rebuilds BEFORE additive column ALTERs: the
        # rebuild recreates the table from SCHEMA (sans drift columns), then the
        # loop re-adds any missing columns (incl. decline_count) to the new table.
        # SQLite only — the mocked sqlite_master on the Oracle adapter makes this
        # unsafe there; Oracle gets its own migration below.
        if HIVE_BACKEND == "sqlite":
            await _ensure_jobs_status_allows_dead_letter(db)
        for table, col, ddl_fragment in live_columns:
            await _ensure_column(db, table, col, ddl_fragment)
        await _ensure_agents_status_allows_stale(db)

        # Thrash fix (2026-06-02) — Oracle backend equivalents. The live hive
        # runs HIVE_BACKEND=oracle; the SQLite executescript/ALTER paths above
        # are no-ops there, so add decline_count + relax CK_HIVE_JOBS_STATUS to
        # include 'dead-letter' (and 'failed_completion') directly via the Oracle
        # connection. Both steps are idempotent and guarded by existence checks.
        # DDL on the Oracle adapter is intentionally a no-op (translate returns
        # None for ALTER/CREATE/DROP), so run these on the RAW oracledb connection
        # (db._conn) to actually apply them. Idempotent + existence-guarded.
        if HIVE_BACKEND == "oracle" and getattr(db, "_conn", None) is not None:
            try:
                raw = db._conn
                rcur = raw.cursor()
                rcur.execute(
                    "SELECT COUNT(*) FROM user_tab_columns "
                    "WHERE table_name = 'HIVE_JOBS' AND column_name = 'DECLINE_COUNT'"
                )
                has_col = (rcur.fetchone())[0]
                if not has_col:
                    rcur.execute("ALTER TABLE HIVE_JOBS ADD (decline_count NUMBER DEFAULT 0 NOT NULL)")
                for _c, _d in (
                    ("SITE", "VARCHAR2(64)"),
                    ("REPO", "VARCHAR2(512)"),
                    ("HEAD_BRANCH", "VARCHAR2(512)"),
                    ("PR_NUMBER", "NUMBER"),
                ):
                    rcur.execute(
                        "SELECT COUNT(*) FROM user_tab_columns WHERE table_name = 'HIVE_JOBS' AND column_name = :1",
                        [_c],
                    )
                    if not (rcur.fetchone())[0]:
                        rcur.execute(f"ALTER TABLE HIVE_JOBS ADD ({_c} {_d})")
                rcur.execute(
                    "SELECT search_condition_vc FROM user_constraints "
                    "WHERE table_name = 'HIVE_JOBS' AND constraint_name = 'CK_HIVE_JOBS_STATUS'"
                )
                crow = rcur.fetchone()
                cond = str(crow[0]) if crow and crow[0] is not None else ""
                if crow is not None and "dead-letter" not in cond:
                    rcur.execute("ALTER TABLE HIVE_JOBS DROP CONSTRAINT CK_HIVE_JOBS_STATUS")
                    rcur.execute(
                        "ALTER TABLE HIVE_JOBS ADD CONSTRAINT CK_HIVE_JOBS_STATUS "
                        "CHECK (status IN ('queued','offered','claimed','running',"
                        "'done','failed','failed_completion','cancelled','dead-letter'))"
                    )
                rcur.close()
                db._conn.commit()
            except Exception as e:
                # FATAL: the runtime decline/dead-letter path REQUIRES both the
                # decline_count column and the relaxed status CHECK. If this DDL
                # fails, every subsequent decline PATCH would error (invalid
                # identifier / CHECK violation), so refuse to start rather than
                # serve a half-migrated bus.
                print(f"oracle dead-letter migration skipped (non-fatal): {e}", flush=True)
                # non-fatal: runtime dead-letter UPDATE falls back to requeue if CHECK not relaxed
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
    task = asyncio.create_task(reaper_task(app))
    app.state.reaper_task = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="GRAEAE Hive Mind", version="0.1.0", lifespan=lifespan)
app.include_router(_repos_router)
app.include_router(_messages_router)
app.include_router(_agents_router)
app.include_router(_admin_router)
app.include_router(_jobs_router)
app.include_router(_schedules_router)


# ---------- endpoints ----------


@app.get("/health")
async def health():
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
    reaper = getattr(app.state, "reaper_task", None)
    # 200 only if the reaper is running: a missing task (e.g. lifespan never
    # completed) or a finished one is unhealthy.
    if reaper is None or reaper.done():
        return JSONResponse(
            {**base, "status": "unhealthy", "detail": "reaper task is not running"},
            status_code=503,
        )
    return {**base, "status": "healthy"}


@app.get("/")
async def dashboard():
    """Minimal HTML+JS dashboard. Auto-refreshes /v1/agents + /v1/jobs + /v1/stats/* + SSE."""
    from fastapi.responses import HTMLResponse

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


# /v1/agents + /v1/hosts routes moved to hive.bus.routers.agents (Phase 1c).


# GRAEAE permanent logjam-fix workspace resolution + _is_dedup_violation moved
# to hive.bus.domain.{workspace,jobs} (imported above).


# POST /v1/jobs (create_job) moved to hive.bus.routers.jobs (Phase 1c).


# POST /v1/jobs/next (dequeue_next_job) moved to hive.bus.routers.jobs (Phase 1c).


@app.patch("/v1/jobs/{job_id}")
async def update_job(job_id: str, req: JobUpdate):
    now = time.time()
    if req.status not in VALID_JOB_STATUSES:
        raise HTTPException(
            422,
            f"unsupported job status {req.status!r}; valid statuses: {sorted(VALID_JOB_STATUSES)}",
        )
    original_claimed_by_was_set = field_was_set(req, "claimed_by")
    original_result_mnemos_was_set = field_was_set(req, "result_mnemos_id")
    # #3 FIX: Hallucination guard now surfaces failure_reason + exit_code=-2 (per review 2026-05-23)
    halluc_reason = None
    completion_rejected = False
    if req.status == "done" and req.result:
        halluc_reason = hallucination_check(req.result)
        if halluc_reason:
            # patch result: exit_code=-2 + top-level failure_reason for easy filtering
            patched = dict(req.result or {})
            patched["exit_code"] = -2
            patched["failure_reason"] = f"hallucination_guard:{halluc_reason}"
            patched["_hallucination_guard"] = halluc_reason
            req = JobUpdate(
                status="failed",
                result=patched,
                claimed_by=req.claimed_by,
                tokens_in=req.tokens_in,
                tokens_out=req.tokens_out,
                result_mnemos_id=req.result_mnemos_id,
            )
    req.result = normalize_result_payload(req.status, req.result)

    claimed_by_was_set = original_claimed_by_was_set
    result_mnemos_was_set = original_result_mnemos_was_set
    patch_payload = req.model_dump(exclude_unset=True) if hasattr(req, "model_dump") else req.dict(exclude_unset=True)
    cost_estimate = None

    # Skip releases are not failures. DECLINE_REASON_PREFIXES is intentionally
    # empty unless a future real failure release reason needs dead-letter counting.
    decline_reason = None
    dead_lettered = False
    if req.status == "queued" and isinstance(req.result, dict):
        _werr = str(req.result.get("worker_error") or "")
        # Match exact reasons or the documented separator form
        # (host_declines_kind:<host>) — avoid matching an unrelated reason that
        # merely shares a prefix.
        if _werr in DECLINE_REASON_PREFIXES or any(_werr.startswith(p + ":") for p in DECLINE_REASON_PREFIXES):
            decline_reason = _werr

    async with connect_db() as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT status, claimed_by, claimed_provider, claimed_model, retry_count, "
                "max_retries, COALESCE(decline_count, 0), kind, repo FROM jobs WHERE id=?",
                (job_id,),
            ) as cur:
                current = await cur.fetchone()
            if not current:
                await db.execute("ROLLBACK")
                raise HTTPException(404, f"job not found: {job_id}")
            (
                old_status,
                old_claimed_by,
                prov,
                mod,
                retry_count,
                max_retries,
                decline_count,
                job_kind,
                job_repo,
            ) = current

            if req.status not in STATUS_TRANSITIONS.get(old_status, set()):
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"invalid status transition {old_status!r} -> {req.status!r}",
                )

            if (
                old_status in TERMINAL_JOB_STATUSES
                and req.status != old_status
                and not (old_status in {"done", "failed"} and req.status == "cancelled")
            ):  # retire fake/dup completions and superseded failures
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"terminal job status {old_status!r} cannot be reopened by PATCH",
                )

            if old_status in {"queued", "offered"} and req.status in {
                "claimed",
                "running",
            }:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    "claim bypass rejected: use POST /v1/jobs/next or POST /v1/jobs/{job_id}/claim "
                    "so agent registration, capabilities, dependencies, and cost tier are checked",
                )

            release_to_queue = req.status == "queued" and old_status in {
                "claimed",
                "running",
                "offered",
            }
            if req.status == "queued" and old_status in {
                "claimed",
                "running",
                "offered",
            }:
                if not old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(409, "cannot release a job with no current claimant")
                if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        403,
                        "release to queued requires claimed_by to match the current claimant",
                    )

            if claimed_by_was_set and req.claimed_by is None:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    422,
                    "claimed_by=null is not accepted; provide the current claimant urn",
                )

            if claimed_by_was_set and req.claimed_by is not None:
                if old_claimed_by and req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        403,
                        f"claimed_by overwrite rejected: current={old_claimed_by!r} requested={req.claimed_by!r}",
                    )

            if old_claimed_by and req.status in {
                "claimed",
                "running",
                "done",
                "failed",
                "cancelled",
            }:
                if not claimed_by_was_set or req.claimed_by != old_claimed_by:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        403,
                        "job update requires claimed_by to match the current claimant",
                    )

            effective_claimed_by = None if release_to_queue else (req.claimed_by or old_claimed_by)
            if req.status in {"claimed", "running"} and not effective_claimed_by:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    422,
                    f"status={req.status!r} requires an existing or explicit claimed_by",
                )

            commit_required = (
                bool(job_repo)
                or str(job_kind) in ("riskyeats", "code")
                or str(job_kind).startswith(
                    (
                        "riskyeats:",
                        "feat:",
                        "fix:",
                        "argonaut:",
                        "riskybiz:",
                        "mnemos:",
                        "ic-engine",
                        "investorclaw",
                        "ncz-os",
                    )
                )
            )
            if req.status == "done" and commit_required:
                proof = req.result or {}
                if not valid_completion_proof(proof):
                    req.status = "needs-review"
                    completion_rejected = True
                    req.result = {
                        **proof,
                        "failure_reason": "completion_evidence_missing",
                    }
                    patch_payload["status"] = req.status
                    patch_payload["result"] = req.result

            fields = ["status=?", "last_update_at=?"]
            args: list = [req.status, now]
            if req.status in TERMINAL_JOB_STATUSES:
                fields.append("ended_at=?")
                args.append(now)
            if req.result is not None:
                fields.append("result=?")
                args.append(json.dumps(req.result))
            if release_to_queue:
                fields.extend(
                    [
                        "claimed_by=NULL",
                        "claimed_at=NULL",
                        "claimed_runtime=NULL",
                        "claimed_model=NULL",
                        "claimed_provider=NULL",
                        "claimed_cost_tier=NULL",
                        "ended_at=NULL",
                    ]
                )
                # GRAEAE dispatch consensus 1.0 (2026-06-04) SHIP-FIRST: backoff on EVERY release
                # so a job a worker can't run cannot hot-loop (claim->release->reclaim). The serve
                # query already skips jobs with retry_backoff_until>now. Mild exponential on
                # decline_count + ±20% jitter (decorrelate N simultaneous releases); ~5s first
                # bounce -> 300s cap. Stops the pegasus/medusa thrash.
                import random as _rnd

                # exponential per-release (NOT flat — flat = infinite churn on a job no local
                # worker can run; GRAEAE 2026-06-04). Bump retry_count each release so the
                # backoff escalates to the 300s cap (~6 bounces); a capable host grabs it during
                # a backoff window while the incapable host stops hammering it.
                _rc = int(retry_count or 0) + 1
                _bo = min(5.0 * (2 ** min(_rc, 6)), 300.0) * (1.0 + _rnd.uniform(-0.2, 0.2))
                fields.append("retry_count=retry_count+1")
                fields.append("retry_backoff_until=?")
                args.append(now + _bo)
            if req.status in {"claimed", "running"}:
                fields.append("claim_lease_expires_at=?")
                args.append(now + CLAIM_LEASE_SECONDS)
            if req.status in TERMINAL_JOB_STATUSES or req.status == "queued":
                fields.append("claim_lease_expires_at=NULL")
            if result_mnemos_was_set:
                fields.append("result_mnemos_id=?")
                args.append(req.result_mnemos_id)

            if req.tokens_in is not None or req.tokens_out is not None:
                t_in = int(req.tokens_in or 0)
                t_out = int(req.tokens_out or 0)
                cost_estimate = estimate_cost(prov or "unknown", mod or "unknown", t_in, t_out)
                fields.extend(["tokens_in=?", "tokens_out=?", "estimated_cost_usd=?"])
                args.extend([t_in, t_out, cost_estimate])

            # Routing re-assignment: only allowed for non-terminal, non-claimed jobs
            if field_was_set(req, "eligible_kinds") and req.eligible_kinds is not None:
                if old_status not in {"queued", "offered"}:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        409,
                        f"eligible_kinds can only be updated on queued/offered jobs, not {old_status!r}",
                    )
                fields.append("eligible_kinds=?")
                args.append(json.dumps(req.eligible_kinds))
            if field_was_set(req, "eligible_hosts") and req.eligible_hosts is not None:
                if old_status not in {"queued", "offered"}:
                    await db.execute("ROLLBACK")
                    raise HTTPException(
                        409,
                        f"eligible_hosts can only be updated on queued/offered jobs, not {old_status!r}",
                    )
                fields.append("eligible_hosts=?")
                args.append(json.dumps(req.eligible_hosts))

            # Thrash fix (2026-06-02): compare-and-set on the observed status.
            # On the Oracle backend BEGIN IMMEDIATE is a no-op, so two concurrent
            # PATCHes can both read old_status='claimed'; without this guard a
            # stale release (status='queued') could land AFTER a sibling PATCH
            # already dead-lettered/terminated the job and reopen it. Gating the
            # UPDATE on `status=old_status` makes the second writer a no-op
            # (rowcount==0 → 409). SQLite already serialises via BEGIN IMMEDIATE;
            # the guard is harmless there.
            args.append(job_id)
            args.append(old_status)
            sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id=? AND status=?"
            cur = await db.execute(sql, args)
            if cur.rowcount == 0:
                await db.execute("ROLLBACK")
                raise HTTPException(
                    409,
                    f"job {job_id} status changed concurrently (expected {old_status!r}); retry",
                )

            # Optional real-failure release counting. Routing skips such as
            # host_declines_kind/released_by_host/no_workspace_for_kind do not count.
            if decline_reason is not None and release_to_queue:
                new_decline_count = (decline_count or 0) + 1
                if new_decline_count >= MAX_DECLINE_REQUEUES:
                    # Preserve the worker's original decline payload for diagnostics.
                    dl_result = {
                        "dead_letter": True,
                        "decline_count": new_decline_count,
                        "last_decline_reason": decline_reason,
                        "note": "thrash_guard:max_decline_requeues_exceeded",
                        "last_decline_result": req.result,
                    }
                    # CAS on status='queued' (the state the main UPDATE just set)
                    # so a concurrent claim can't be clobbered.
                    dl_cur = await db.execute(
                        "UPDATE jobs SET status='dead-letter', decline_count=?, "
                        "ended_at=?, claim_lease_expires_at=NULL, result=? "
                        "WHERE id=? AND status='queued'",
                        (new_decline_count, now, json.dumps(dl_result), job_id),
                    )
                    dead_lettered = (getattr(dl_cur, "rowcount", 0) or 0) >= 1
                else:
                    await db.execute(
                        "UPDATE jobs SET decline_count=? WHERE id=? AND status='queued'",
                        (new_decline_count, job_id),
                    )

            # Roll MTD spend onto claimer (subscription throttle requires this)
            if cost_estimate and cost_estimate > 0 and effective_claimed_by:
                await db.execute(
                    "UPDATE agents SET plan_period_used_usd = COALESCE(plan_period_used_usd,0) + ? WHERE urn=?",
                    (cost_estimate, effective_claimed_by),
                )

            # On failed: auto-retry if under max_retries. Exponential backoff: 30s x 2^retry_count
            retried = False
            if req.status == "failed" and retry_count < max_retries:
                backoff = 30.0 * (2**retry_count)
                next_at = time.time() + backoff
                await db.execute(
                    "UPDATE jobs SET status='queued', retry_count=retry_count+1, "
                    "retry_backoff_until=?, claimed_by=NULL, claimed_at=NULL, "
                    "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, claimed_cost_tier=NULL, "
                    "ended_at=NULL, result=NULL, tokens_in=NULL, tokens_out=NULL, estimated_cost_usd=NULL, "
                    "claim_lease_expires_at=NULL "
                    "WHERE id=?",
                    (next_at, job_id),
                )
                retried = True

            # On done/failed/cancelled: roll per-worker per-kind stats (capability scoring).
            # Gate on a REAL non-terminal -> terminal transition: done->done /
            # failed->failed are allowed (idempotent re-PATCH), so without this guard a
            # duplicate terminal PATCH would double-count stats AND the usage_ledger cost.
            if req.status in TERMINAL_JOB_STATUSES and old_status not in TERMINAL_JOB_STATUSES:
                async with db.execute(
                    "SELECT kind, description, max_cost_tier, required_capabilities, "
                    "claimed_model, claimed_provider, claimed_by, result, started_at "
                    "FROM jobs WHERE id=?",
                    (job_id,),
                ) as cur2:
                    jrow = await cur2.fetchone()
                if jrow:
                    (
                        kind_j,
                        desc_j,
                        mtier,
                        reqcaps_json,
                        mdl_j,
                        prov_j,
                        claimed_by_j,
                        result_j,
                        started_j,
                    ) = jrow
                    # Workers carry real token usage + the ACTUAL gateway
                    # model/provider INSIDE result, not as top-level PATCH
                    # fields, so cost was never computed before (usage_ledger
                    # stayed empty, worker_kind_stats.total_cost_usd stayed 0).
                    # Parse the result once and recover them.
                    try:
                        _rd = json.loads(result_j) if result_j else (req.result or {})
                    except Exception:
                        _rd = req.result or {}
                    if not isinstance(_rd, dict):
                        _rd = {}
                    rt_in = _safe_nonneg_int(req.tokens_in if req.tokens_in is not None else _rd.get("tokens_in"))
                    rt_out = _safe_nonneg_int(req.tokens_out if req.tokens_out is not None else _rd.get("tokens_out"))
                    rt_reason = _safe_nonneg_int(_rd.get("tokens_reasoning"))
                    # Some workers report only a combined total — attribute to output.
                    if rt_in == 0 and rt_out == 0 and _rd.get("tokens_total"):
                        rt_out = _safe_nonneg_int(_rd.get("tokens_total"))
                    eff_model = _safe_str(_rd.get("gateway_model") or mdl_j)
                    eff_prov = _safe_str(_rd.get("gateway_provider") or prov_j)
                    # Cost estimation must never break the hot completion path.
                    try:
                        job_cost = estimate_cost(eff_prov, eff_model, rt_in, rt_out) or 0
                        job_cost = max(job_cost, 0)
                    except Exception as _ce:  # noqa: BLE001
                        print(f"cost estimate skipped for {job_id}: {_ce}", flush=True)
                        job_cost = 0
                    if cost_estimate is None and job_cost:
                        cost_estimate = job_cost
                    if claimed_by_j and kind_j:
                        # Key stats by the STABLE worker identity (drop the
                        # per-restart session segment) so a worker's history
                        # aggregates across restarts instead of fragmenting.
                        worker_key = stable_worker_id(claimed_by_j)
                        duration = (time.time() - (started_j or time.time())) if started_j else 0
                        col = {
                            "done": "success_count",
                            "failed": "fail_count",
                            "cancelled": "cancelled_count",
                        }[req.status]
                        await db.execute(
                            f"INSERT INTO worker_kind_stats (urn, kind, {col}, total_tokens_in, total_tokens_out, "
                            f"total_cost_usd, total_duration_sec, last_run) "
                            f"VALUES (?, ?, 1, ?, ?, ?, ?, ?) "
                            f"ON CONFLICT(urn, kind) DO UPDATE SET "
                            f"{col} = {col} + 1, "
                            f"total_tokens_in = total_tokens_in + ?, "
                            f"total_tokens_out = total_tokens_out + ?, "
                            f"total_cost_usd = total_cost_usd + ?, "
                            f"total_duration_sec = total_duration_sec + ?, "
                            f"last_run = ?",
                            (
                                worker_key,
                                kind_j,
                                rt_in,
                                rt_out,
                                job_cost or 0,
                                duration,
                                time.time(),
                                rt_in,
                                rt_out,
                                job_cost or 0,
                                duration,
                                time.time(),
                            ),
                        )
                        # Per-job cost ledger row (the hive cost tracker). Best-effort:
                        # a ledger schema issue must never break job completion.
                        try:
                            # ts is TIMESTAMP WITH TIME ZONE — use SYSTIMESTAMP (not an
                            # epoch float, which raises ORA-00932) as a SQL literal so it
                            # is not a bind parameter.
                            await db.execute(
                                "INSERT INTO usage_ledger (provider, model, task_kind, tokens_in, "
                                "tokens_out, tokens_reasoning, est_cost_usd, latency_ms, outcome, "
                                "caller_subsystem, tier, ts, session_id, request_count, "
                                "subscription_amortized, path_kind) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP, ?, ?, ?, ?)",
                                (
                                    eff_prov,
                                    eff_model,
                                    kind_j,
                                    rt_in,
                                    rt_out,
                                    rt_reason,
                                    job_cost or 0,
                                    max(0, int(duration * 1000)),
                                    # outcome is constrained to ('ok','err','timeout')
                                    "ok" if req.status == "done" else "err",
                                    "hive",
                                    (mtier or "A").upper(),
                                    claimed_by_j,
                                    1,
                                    1 if (eff_prov or "").lower() in SUBSCRIPTION_PROVIDERS else 0,
                                    str(_rd.get("via") or "worker"),
                                ),
                            )
                        except Exception as _le:
                            print(
                                f"usage_ledger write skipped for {job_id}: {_le}",
                                flush=True,
                            )
                    if req.status == "done":
                        ck = cache_key_for(
                            kind_j,
                            desc_j,
                            (mtier or "A").upper(),
                            json_list(reqcaps_json),
                        )
                        rdict = json.loads(result_j) if result_j else (req.result or {})
                        ec = (rdict or {}).get("exit_code")
                        # NEVER cache a non-terminal / unlanded result. A
                        # needs-review result (work is a patch, NOT landed) has
                        # exit_code=None and would otherwise be served as an
                        # instant fake-"done" to every matching resubmit (the 73
                        # orphaned FRI jobs, 2026-06-07). Only cache real
                        # completed work: not needs_review, and either it carries
                        # commits or it is a non-workspace job (no repo to commit).
                        _nr = bool((rdict or {}).get("needs_review")) or (rdict or {}).get("status") == "needs-review"
                        if (ec == 0 or ec is None) and not _nr:
                            await cache_store(
                                db,
                                ck,
                                job_id,
                                rdict,
                                req.result_mnemos_id,
                                mdl_j or "unknown",
                                prov_j or "unknown",
                                cost_estimate or 0,
                            )

            await db.execute(
                "INSERT INTO job_audit_log (job_id, ts, actor_urn, old_status, new_status, "
                "old_claimed_by, new_claimed_by, patch) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    now,
                    req.claimed_by or old_claimed_by,
                    old_status,
                    "dead-letter" if dead_lettered else ("queued" if retried else req.status),
                    old_claimed_by,
                    None if release_to_queue else effective_claimed_by,
                    json.dumps(patch_payload, default=str),
                ),
            )
            await db.execute("COMMIT")
        except HTTPException:
            raise
        except Exception:
            await db.execute("ROLLBACK")
            raise

    async with connect_db() as event_db:
        if req.status == "failed" and retry_count < max_retries:
            await emit_event(
                event_db,
                "job.retry",
                {
                    "id": job_id,
                    "retry_count": retry_count + 1,
                    "max_retries": max_retries,
                },
            )
        if dead_lettered:
            # Emit ONLY the effective terminal event; suppress the misleading
            # job.queued (the inbound PATCH was queued but the job was diverted).
            await emit_event(
                event_db,
                "job.dead-letter",
                {
                    "id": job_id,
                    "claimed_by": req.claimed_by or old_claimed_by,
                    "last_decline_reason": decline_reason,
                    "decline_threshold": MAX_DECLINE_REQUEUES,
                },
            )
        else:
            await emit_event(
                event_db,
                f"job.{req.status}",
                {
                    "id": job_id,
                    "claimed_by": req.claimed_by or old_claimed_by,
                    "tokens_in": req.tokens_in,
                    "tokens_out": req.tokens_out,
                    "estimated_cost_usd": cost_estimate,
                },
            )
    if completion_rejected:
        # Older workers delete successful checkouts after HTTP 200. Returning
        # non-200 is necessary until those workers have all been upgraded.
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "status": "needs-review",
                "reason": "completion_evidence_missing",
            },
        )
    return {
        "ok": True,
        "ts": now,
        "estimated_cost_usd": cost_estimate,
        "dead_lettered": dead_lettered,
    }


@app.post("/v1/jobs/{job_id}/auto-retry")
async def auto_retry_job(job_id: str):
    """Recheck stored retry policy under a row lock before consuming a retry."""
    async with connect_db() as db:
        try:
            if HIVE_BACKEND == "sqlite":
                await db.execute("BEGIN IMMEDIATE")
            lock_clause = {
                "sqlite": "",
                "oracle": " FOR UPDATE",
                "db2": " FOR UPDATE WITH RS USE AND KEEP UPDATE LOCKS",
            }[HIVE_BACKEND]
            async with db.execute(
                "SELECT status, kind, max_retries, retry_count, result FROM jobs WHERE id=?" + lock_clause,
                (job_id,),
            ) as cur:
                row = await cur.fetchone()
            if row is None:
                raise HTTPException(404, "job not found")
            status, kind, max_retries, retry_count, raw_result = row
            try:
                result = json.loads(raw_result) if raw_result else None
            except (ValueError, TypeError):
                result = None
            job = {
                "status": status,
                "kind": kind,
                "max_retries": max_retries,
                "retry_count": retry_count,
                "result": result,
            }
            if not retry_policy.auto_retry_allowed(job):
                raise HTTPException(409, "job is not eligible for automatic retry")
            await db.execute(
                "UPDATE jobs SET status='queued', retry_count=retry_count+1, "
                "claimed_by=NULL, claimed_at=NULL, claimed_runtime=NULL, "
                "claimed_model=NULL, claimed_provider=NULL, claimed_cost_tier=NULL, "
                "claim_lease_expires_at=NULL, ended_at=NULL, retry_backoff_until=NULL "
                "WHERE id=?",
                (job_id,),
            )
            await db.commit()
        except BaseException:
            await db.rollback()
            raise
    return {"ok": True, "id": job_id, "retry_count": retry_count + 1}


# /v1/admin/jobs/* + DELETE /v1/jobs/{id} + /v1/jobs/purge moved to hive.bus.routers.admin (Phase 1c).


# /v1/schedules routes moved to hive.bus.routers.schedules (Phase 1c).


@app.get("/v1/stats/workers")
async def worker_stats(kind: str | None = None, top_n: int = 30, include_system: bool = False):
    """Per-worker per-kind capability scores. Submitters use this to pick best worker for a kind.

    #8 FIX (review 2026-05-23): exclude `system` kind agents by default (they're host monitors,
    not compute workers — including them was misleading). Pass include_system=true to override.
    """
    top_n = clamp_limit(top_n, default=30, max_limit=500)
    sql = (
        "SELECT urn, kind, success_count, fail_count, cancelled_count, "
        "total_tokens_in, total_tokens_out, ROUND(total_cost_usd,4), "
        "ROUND(total_duration_sec/NULLIF(success_count+fail_count,0),1) AS avg_dur, "
        "datetime(last_run,'unixepoch') AS last_run, "
        "ROUND(100.0 * success_count / NULLIF(success_count+fail_count+cancelled_count,0), 1) AS success_pct "
        "FROM worker_kind_stats WHERE 1=1"
    )
    args: list = []
    if kind:
        sql += " AND kind=?"
        args.append(kind)
    if not include_system:
        sql += " AND urn NOT LIKE 'urn:agent:system:%'"
    sql += " ORDER BY success_count DESC, success_pct DESC LIMIT ?"
    args.append(top_n)
    rows = []
    async with connect_db() as db, db.execute(sql, args) as cur:
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
    return {"count": len(rows), "workers": rows}


@app.get("/v1/stats/cache")
async def cache_stats(top_n: int = 20):
    """Result-cache hit-rate + cost savings."""
    top_n = clamp_limit(top_n, default=20, max_limit=500)
    async with connect_db() as db:
        async with db.execute("SELECT COUNT(*), SUM(hit_count), ROUND(SUM(cost_saved_usd),4) FROM hive_cache") as cur:
            r = await cur.fetchone()
        totals = {
            "cached_jobs": r[0] or 0,
            "total_hits": r[1] or 0,
            "total_cost_saved_usd": r[2] or 0,
        }
        # job-side stats
        async with db.execute("SELECT COUNT(*) FROM jobs WHERE result LIKE '%\"cache_hit\": true%'") as cur:
            r2 = await cur.fetchone()
        totals["jobs_short_circuited"] = r2[0] or 0
        # top cached entries
        async with db.execute(
            "SELECT cache_key, hit_count, cost_saved_usd, model, provider, "
            "datetime(cached_at,'unixepoch') AS cached_at, source_job_id "
            "FROM hive_cache ORDER BY hit_count DESC, cost_saved_usd DESC LIMIT ?",
            (top_n,),
        ) as cur:
            entries = []
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


@app.get("/v1/stats/costs")
async def cost_stats(since_hours: int = 168, group_by: str = "provider"):
    """Aggregated cost stats. group_by: provider | model | runtime | cost_tier | claimed_by | day.
    since_hours: window in hours (default 168 = 7 days).
    """
    if group_by not in {
        "provider",
        "model",
        "runtime",
        "cost_tier",
        "claimed_by",
        "day",
        "kind",
    }:
        raise HTTPException(
            422,
            "group_by must be one of: provider, model, runtime, cost_tier, claimed_by, day, kind",
        )
    cutoff = time.time() - since_hours * 3600
    if group_by == "day":
        sel = "DATE(ended_at, 'unixepoch')"
    elif group_by == "provider":
        # Derive REAL provider from result.agent_alias (the alias that did the work),
        # NOT claimed_provider (which carries the zeroclaw #7066 label-lie = openai/codex).
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
    rows = []
    async with connect_db() as db, db.execute(sql, (cutoff,)) as cur:
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
    # totals
    totals = {
        "job_count": sum(r["job_count"] for r in rows),
        "tokens_in": sum(r["tokens_in"] or 0 for r in rows),
        "tokens_out": sum(r["tokens_out"] or 0 for r in rows),
        "estimated_cost_usd": round(sum(r["estimated_cost_usd"] or 0 for r in rows), 4),
    }
    return {
        "since_hours": since_hours,
        "group_by": group_by,
        "totals": totals,
        "buckets": rows,
    }


# ── KNEMON COST-AWARE ROUTER (2026-06-02) ────────────────────────────────────
# Deterministic, no external calls. Given a job's budget ceiling (max_cost_tier
# or tier) + kind, return an ordered list of provider/model candidates that fit
# the budget, cheapest-acceptable FIRST, then escalate.
#
# COST-TIER CONVENTION (authoritative, from PROVIDER_COST_TIER above):
#   A = FREE/cheap-local   B = MID (cheap paid)   C = PREMIUM/reserve
#   COST_TIERS = ["A","B","C"]  ordered cheap -> expensive.
# `max_cost_tier` is the CEILING (most-expensive tier permitted). A job with
# max_cost_tier="C" may use A/B/C providers; "B" -> A/B; "A" -> A only.
# This matches the dequeue semantics + the operator intent ("generous budget =>
# premium allowed; constrained budget => cheap only"). NOTE: the task brief
# described A=premium which is INVERTED from the code; the code convention wins.
#
# Working provider set (operator 2026-06-01): deepseek/groq/together/xai/gemini/
# siliconflow. EXCLUDED: codex (review-only, scarce OAuth pool), nvidia (not
# integrated). Each entry: provider, model, worker alias, $/1M-out
# (used only as the deterministic cheapest-first sort key).
#
# alias = the hive_* agent alias the wss worker dispatches to (gateway maps it
# to provider+model). Returned so the worker can consume a chain shaped exactly
# like its local TIER_CHAINS values.


# ── HIVE-LEVEL KNEMON BYPASS FLAG (operator 2026-06-03) ──────────────────
# Presence of this file = bypass ON for the WHOLE hive. Durable across bus
# restarts; observable (ls/cat); togglable via the /v1/knemon/bypass endpoints
# below or a plain touch/rm. Cheap stat() per route call. When set, the router
# drops the codex/gpt sub-lead and returns the plain metered open-weight chain,
# so EVERY worker that polls /v1/knemon/route runs jobs directly on open-weights
# — the stabilization lever when the GPT-first lead is broken (e.g. hive_gpt
# 400 'missing input' cascade).
KNEMON_BYPASS_FILE = "/srv/agent-bus/knemon_bypass.flag"


def knemon_bypass_active() -> bool:
    try:
        return os.path.exists(KNEMON_BYPASS_FILE)
    except Exception:
        return False


KNEMON_CAP_FILE = "/srv/agent-bus/knemon_oauth_cap.json"
# ChatGPT-subscription codex/gpt allowance is a rolling ~5h window (operator/Gemini 2026-06-04).
OAUTH_CAP_RESET_SEC = 5 * 3600
# How long a corrupt/unreadable breaker file keeps the breaker CLOSED (fail-closed)
# before it self-heals — bounded so a transient read error can't pin it open forever.
OAUTH_CAP_STORAGE_BACKOFF_SEC = 300
# While the breaker is open, send a small share of eligible jobs through the
# OAuth lead as half-open probes. PYTHIA cannot probe OpenAI locally.
OAUTH_CAP_PROBE_RATE = 0.05


def oauth_cap_state() -> dict:
    """OAuth-cap circuit-breaker state. A worker that hits usage_limit on the codex/gpt
    lead POSTs /v1/knemon/cap, opening the breaker so knemon_route stops leading every job
    with the doomed OAuth model. Auto-expires at capped_until (then a probe
    resumes; a probe success clears the file via /v1/knemon/cap/clear)."""
    import json as _j
    import time as _t

    base = {
        "capped": False,
        "capped_until": None,
        "hit_at": None,
        "model": None,
        "reporter": None,
        "verified": False,
        "remaining_sec": 0,
    }
    try:
        with open(KNEMON_CAP_FILE) as f:
            d = _j.load(f)
    except FileNotFoundError:
        return base  # no breaker file = not capped
    except Exception as e:  # noqa: BLE001
        # Corrupt/unreadable breaker file: FAIL CLOSED (assume capped) so the
        # breaker does not silently open and burn the OAuth allowance. BOUND it
        # with a short capped_until so a transient read error self-heals instead
        # of pinning the breaker open forever (capped_until=None would).
        until = _t.time() + OAUTH_CAP_STORAGE_BACKOFF_SEC
        return {
            **base,
            "capped": True,
            "capped_until": until,
            "remaining_sec": OAUTH_CAP_STORAGE_BACKOFF_SEC,
            "storage_error": str(e),
        }
    try:
        until = float(d.get("capped_until") or 0)
    except (TypeError, ValueError):
        return {**base, "capped": True, "storage_error": "invalid capped_until"}
    now = _t.time()
    return {
        "capped": now < until,
        "capped_until": until,
        "hit_at": d.get("hit_at"),
        "model": d.get("model"),
        "reporter": d.get("reporter"),
        "verified": False,
        "remaining_sec": max(0, int(until - now)),
    }


def clear_oauth_cap() -> str | None:
    try:
        os.remove(KNEMON_CAP_FILE)  # no exists() pre-check (avoid TOCTOU)
    except FileNotFoundError:
        pass
    except Exception as e:
        return str(e)
    return None


@app.get("/v1/knemon/providers")
async def knemon_providers():
    """Provider denylist for the worker llm-* capability probe (Phase 2). A
    family listed here is treated as globally dead; workers drop its llm-* cap.
    Set HIVE_PROVIDER_DENYLIST=comma,list (e.g. nvidia,eih)."""
    denied = [p.strip().lower() for p in os.environ.get("HIVE_PROVIDER_DENYLIST", "").split(",") if p.strip()]
    return {"denied": denied}


@app.post("/v1/knemon/route")
async def knemon_route(req: Request):
    """Cost-aware routing. POST {max_cost_tier|tier, kind} -> ordered candidate
    list (cheapest-acceptable first) + the alias chain the worker can consume.
    Deterministic, no external calls, no DB write."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    raw_tier = body.get("max_cost_tier") or body.get("tier") or "C"
    max_tier = str(raw_tier).upper()
    if max_tier not in COST_TIERS:
        max_tier = "C"
    kind = str(body.get("kind") or "")
    lead_alias = str(body.get("lead_alias") or "").strip()
    cands = knemon_candidates(max_tier, kind)
    open_weight_chain = [c["alias"] for c in cands]
    # ── KNEMON sub-bucket dispatch (operator 2026-06-02): openai_codex sub is $0;
    # gpt-5.3/5.4 unlimited. Lead EVERY job with the right codex/gpt agent, then
    # fall back to metered open-weights. Job class -> served model:
    #   review/adversarial -> hive_codex (codex-auto-review)
    #   heavy (architecture/design/heavy:) -> hive_gpt_heavy (gpt-5.5, limited)
    #   light/narrow (triage/docs/investigation) -> hive_gpt_mini (gpt-5.4-mini)
    #   everything else (coding default) -> hive_gpt (gpt-5.4)
    kl = kind.lower()
    review_kind = any(kl.startswith(p) for p in ("review:", "codex", "doctor:codex-fix", "adversarial"))
    arch_kind = any(t in kl for t in ("architecture", "design")) or kl.startswith("heavy:")
    # DEEPSEEK POLICY (operator 2026-06-07) SUPERSEDED 2026-09-07: deepseek
    # direct is now FORBIDDEN fleet-wide, full stop — not just gated to
    # review/architecture. Both aliases are already struck from
    # KNEMON_PROVIDERS above; nothing left here to strip.
    # ADVERSARIAL-ONLY codex (operator 2026-06-17): codex/gpt OAuth leads ONLY
    # review/adversarial kinds. Every other kind leads with the open-weight /
    # local lineup (no OAuth lead) so coding goes local-first per the edict.
    if review_kind:
        CODEX_SUB_LEAD = ["hive_codex"]
    else:
        CODEX_SUB_LEAD = []
    # HIVE-LEVEL BYPASS: when the flag is set, drop the codex/gpt sub-lead and
    # hand back the plain metered open-weight chain so all workers run direct.
    bypass = knemon_bypass_active()
    cap = oauth_cap_state()
    oauth_cap_probe = False
    if bypass:
        CODEX_SUB_LEAD = []
        chain = list(open_weight_chain)
    else:
        # OAuth codex/gpt ($0) leads. Its allowance is a rolling ~5h window (operator/Gemini
        # 2026-06-04) and caps under heavy job load. When it 429s/usage_limit:
        #   review kinds  -> deepseek (REVIEW-ONLY sanction, operator 2026-06-07) then grok
        #   all job kinds -> grok-4.3 only (funded xai). Deepseek is FORBIDDEN as a
        #   job model — observed confabulating/failing heavy jobs AND now policy.
        # hive_ngc_1 = azure/openai/gpt-5.5 via the .4 NGC gateway (operator
        # 2026-06-07: gpt-5.5 ≈ codex quality, free NGC enterprise). It is a
        # proper OpenAI tool-driver (native tool_calls, unlike the earlier
        # gpt-oss/nemotron harmony/pseudo-XML no-ops) — so it LEADS the metered
        # fallback: when OAuth codex/gpt is capped it is the best available
        # driver AND $0. deepseek/nova/grok follow. (NGC nemotron stays as the
        # ngc-review CLI's direct-prompt reviewer, separate from this chain.)
        # ngc/EIH removed 2026-06-17. Open-weight lineup leads (cheap flash first;
        # minimax is DIRECT (hive_minimax_1); never via together (operator 2026-10-15).
        # direct $200 MiniMax key is wired into the .4 gateway alias).
        if review_kind:
            METERED_FALLBACK = ["hive_groq_3", "hive_xai_1"]
        elif arch_kind:
            # High-level architecture: gpt-oss-120b, grok. (deepseek-PRO struck
            # 2026-09-07 — deepseek direct is forbidden fleet-wide.)
            METERED_FALLBACK = ["hive_groq_3", "hive_xai_1"]
        else:
            # General coding rotation (operator 2026-09-07: deepseek-flash lead
            # struck, forbidden fleet-wide). groq qwen3 (fast coder) leads, then
            # groq gpt-oss-120b, grok, and MiniMax direct (hive_minimax_1).
            # MiniMax is NOT host-gated (operator 2026-10-15): ANY zoder/zeroclaw
            # host works ANY job it is dispatched, chosen by CAPABILITY. The
            # MiniMax pool cap is the concurrency guard for the TPM-limited $200
            # plan -- not a host-name rule.
            METERED_FALLBACK = ["hive_groq_2", "hive_groq_3", "hive_xai_1", "hive_minimax_1"]
        if cap.get("capped"):
            # Half-open breaker: most jobs use metered fallback, but a small share leads
            # OAuth so routed traffic can prove recovery before the 5h stale-report TTL.
            if CODEX_SUB_LEAD and secrets.randbelow(10000) < int(OAUTH_CAP_PROBE_RATE * 10000):
                oauth_cap_probe = True
                chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
            else:
                chain = list(METERED_FALLBACK)
        else:
            chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
    # JOB-SELECTABLE LEAD (operator 2026-06-19): a job may request a specific
    # lead alias (e.g. a host-local GPU coder: hive_cerberus_1 / hive_rocm_*).
    # Honor it as the chain head while keeping the rotation as fallback.
    # BANNED_LEAD_ALIASES (operator 2026-09-07, codex review 2026-09-14):
    # a caller could bypass the deepseek/siliconflow removal above by simply
    # requesting one of the struck aliases directly as lead_alias — this was
    # the removal's only unvalidated entry point. Refuse it there too.
    BANNED_LEAD_ALIASES = {
        "hive_deepseek_1",
        "hive_deepseek_pro_1",
        "hive_siliconflow_1",
    }
    if lead_alias and lead_alias not in BANNED_LEAD_ALIASES:
        chain = [lead_alias] + [a for a in chain if a != lead_alias]
    return {
        "ok": True,
        "max_cost_tier": max_tier,
        "kind": kind,
        "knemon_bypass": bypass,
        "oauth_cap": cap,
        "oauth_cap_probe": oauth_cap_probe,
        "oauth_cap_probe_rate": OAUTH_CAP_PROBE_RATE if cap.get("capped") and CODEX_SUB_LEAD else 0.0,
        "sub_lead": CODEX_SUB_LEAD,
        # codex/gpt sub agent first ($0 unlimited), metered open-weights as fallback
        "chain": chain,
        "candidates": cands,
        "convention": "openai_codex sub leads ($0); open-weights fallback; A=cheap..C=premium ceiling",
    }


@app.get("/v1/knemon/bypass")
async def knemon_bypass_get():
    """Read hive-level KNEMON bypass state."""
    return {
        "ok": True,
        "bypass": knemon_bypass_active(),
        "flag_file": KNEMON_BYPASS_FILE,
    }


@app.post("/v1/knemon/bypass", dependencies=[Depends(require_mutation_actor)])
async def knemon_bypass_set(req: Request):
    """Toggle hive-level KNEMON bypass. POST {enabled: bool, reason?: str}.
    When enabled, ALL workers skip the codex/gpt sub-lead (open-weights only)."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    enabled = bool(body.get("enabled"))
    reason = str(body.get("reason") or "")
    try:
        if enabled:
            with open(KNEMON_BYPASS_FILE, "w") as f:
                f.write((reason or "bypass enabled") + "\n")
        else:
            if os.path.exists(KNEMON_BYPASS_FILE):
                os.remove(KNEMON_BYPASS_FILE)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "bypass": knemon_bypass_active(), "reason": reason}


@app.get("/v1/knemon/cap")
async def knemon_cap_get():
    """OAuth-cap circuit-breaker state (dashboard 5h countdown reads this)."""
    return {
        "ok": True,
        "reset_sec": OAUTH_CAP_RESET_SEC,
        "flag_file": KNEMON_CAP_FILE,
        **oauth_cap_state(),
    }


@app.post("/v1/knemon/cap", dependencies=[Depends(require_mutation_actor)])
async def knemon_cap_set(req: Request):
    """Workers report OAuth cap state.

    POST {model?, reporter?, reset_sec?} opens the breaker on usage_limit.
    POST {success:true, model?, reporter?} closes it after a successful OAuth completion.
    """
    import json as _j
    import time as _t

    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if body.get("success") is True:
        err = clear_oauth_cap()
        if err:
            return {"ok": False, "error": err}
        return {"ok": True, "cleared": True, "success": True, **oauth_cap_state()}
    # debounce the worker stampede: the first usage_limit opens the breaker for the full
    # window; concurrent reporters within the window are no-ops (don't churn/extend it).
    cur = oauth_cap_state()
    if cur.get("capped"):
        return {"ok": True, "debounced": True, **cur}
    try:
        reset = float(body.get("reset_sec") or OAUTH_CAP_RESET_SEC)
    except (TypeError, ValueError):
        reset = float(OAUTH_CAP_RESET_SEC)
    if reset != reset or reset in (float("inf"), float("-inf")):  # NaN/Inf guard
        reset = float(OAUTH_CAP_RESET_SEC)
    reset = max(60.0, min(reset, 86400.0))
    now = _t.time()
    state = {
        "hit_at": now,
        "capped_until": now + reset,
        "model": str(body.get("model") or ""),
        "reporter": str(body.get("reporter") or body.get("worker") or body.get("host") or ""),
    }
    try:
        tmp = KNEMON_CAP_FILE + ".tmp"
        with open(tmp, "w") as f:
            _j.dump(state, f)
        os.replace(tmp, KNEMON_CAP_FILE)  # atomic publish (no partial-read race)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, **oauth_cap_state()}


@app.post("/v1/knemon/cap/clear")
async def knemon_cap_clear():
    """Confirm OAuth reset (a codex/gpt probe succeeded). Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, **oauth_cap_state()}


@app.post("/v1/knemon/cap/probe-ok")
async def knemon_cap_probe_ok():
    """Confirm a half-open OAuth probe succeeded. Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "cleared": True, **oauth_cap_state()}


@app.get("/v1/knemon/spend")
async def knemon_spend(since_hours: int = 168):
    """Total estimated spend over a window (visibility companion to the router)."""
    # clamp window to a sane range (1h .. 1 year) so a pathological value
    # cannot produce a nonsense cutoff.
    try:
        since_hours = max(1, min(int(since_hours), 24 * 366))
    except (TypeError, ValueError):
        since_hours = 168
    cutoff = time.time() - since_hours * 3600
    async with (
        connect_db() as db,
        db.execute(
            "SELECT COUNT(*), ROUND(SUM(COALESCE(estimated_cost_usd,0)),4), "
            "SUM(COALESCE(tokens_in,0)), SUM(COALESCE(tokens_out,0)) "
            "FROM jobs WHERE ended_at IS NOT NULL AND ended_at >= ? "
            "AND estimated_cost_usd IS NOT NULL",
            (cutoff,),
        ) as cur,
    ):
        r = await cur.fetchone()
    return {
        "since_hours": since_hours,
        "jobs_with_cost": (r[0] or 0) if r else 0,
        "estimated_cost_usd": (r[1] or 0.0) if r else 0.0,
        "tokens_in": (r[2] or 0) if r else 0,
        "tokens_out": (r[3] or 0) if r else 0,
    }


# /v1/jobs/metrics moved to hive.bus.routers.jobs (Phase 1c).


@app.post("/v1/jobs/{job_id}/claim")
async def claim_job(job_id: str, by: str):
    # HOST_DENYLIST was enforced on /v1/jobs/next but NOT here, so a denylisted
    # host could take work by calling this by-id endpoint and skipping the
    # dequeue path entirely.
    _hp = by.split(":")
    _host = (_hp[3] if len(_hp) > 3 else "").lower()
    if _host in HOST_DENYLIST:
        raise HTTPException(403, f"host {_host} is in HIVE_HOST_DENYLIST")
    """Targeted single-job claim with the same eligibility checks as
    /v1/jobs/next (dequeue_next_job).

    Previously this endpoint just ran UPDATE WHERE status IN
    ('queued','offered') with no validation — any caller could claim
    any job by passing an arbitrary `by` URN, bypassing agent
    registration, capability matching, eligible_kinds, cost-tier ceiling,
    throttling, preferred provider/model, and DAG dependency gating, and
    skipping the claimed_runtime/claimed_model/claimed_provider/
    claimed_cost_tier population that cost accounting + worker_kind_stats
    depend on. This rewrite reuses the dequeue eligibility chain for
    one specific job_id so manual claims are subject to the same
    contract as auto-claims.
    """
    now = time.time()
    async with connect_db() as db:
        # 1. agent must exist + be online/idle
        async with db.execute(
            "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
            "auth_method, plan_cap_usd, plan_period_used_usd, status "
            "FROM agents WHERE urn=?",
            (by,),
        ) as cur:
            agent_row = await cur.fetchone()
        if not agent_row:
            raise HTTPException(404, f"claim failed: agent not registered: {by}")
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
        ) = agent_row
        if agent_status not in ACTIVE_AGENT_STATUSES:
            raise HTTPException(
                409,
                f"claim failed: agent status is {agent_status!r}, not online/idle: {by}",
            )
        agent_caps = set(json.loads(caps_json)) if caps_json else set()
        eligible_aliases = agent_kind_aliases(agent_kind, a_runtime)
        a_tier = (a_tier or "C").upper()
        if a_tier not in COST_TIERS:
            a_tier = "C"
        a_auth = (a_auth or "unknown").lower()
        sub_throttled = a_auth == "subscription" and a_cap and a_used and a_used >= THROTTLE_HEADROOM * a_cap

        # 2. job must exist + be claimable + agent must satisfy its filters
        async with db.execute(
            "SELECT status, kind, required_capabilities, eligible_kinds, "
            "eligible_hosts, max_cost_tier, preferred_providers, preferred_models, "
            "depends_on, retry_backoff_until, started_at "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            job_row = await cur.fetchone()
        if not job_row:
            raise HTTPException(404, f"job not found: {job_id}")
        (
            j_status,
            j_kind,
            j_caps_json,
            j_kinds_json,
            j_hosts_json,
            j_max_tier,
            j_pref_providers,
            j_pref_models,
            j_deps_json,
            j_retry_backoff_until,
            j_started_at,
        ) = job_row

        urn_parts = by.split(":")
        agent_host = (urn_parts[3] if len(urn_parts) > 3 else "").lower()

        if j_status not in ("queued", "offered"):
            raise HTTPException(409, f"job already in status={j_status!r}; cannot claim")

        # retry-backoff gate (same as dequeue)
        if j_retry_backoff_until is not None and j_retry_backoff_until > now:
            raise HTTPException(
                409,
                f"job is in retry backoff until {j_retry_backoff_until} (now={now})",
            )

        # DAG gate: all depends_on must be status='done'
        if j_deps_json:
            deps = json_list(j_deps_json)
            if deps:
                ph = ",".join("?" * len(deps))
                async with db.execute(
                    f"SELECT id FROM jobs WHERE id IN ({ph}) AND status='done'",
                    tuple(deps),
                ) as dc:
                    done_ids = {row[0] async for row in dc}
                if len(done_ids) < len(deps):
                    raise HTTPException(
                        409,
                        f"job has unsatisfied DAG dependencies ({len(done_ids)}/{len(deps)} done)",
                    )

        agent = {
            "urn": by,
            "host": agent_host,
            "kind": agent_kind,
            "runtime": a_runtime,
            "model": a_model,
            "provider": a_provider,
            "cost_tier": a_tier,
            "capabilities": agent_caps,
            "eligible_aliases": eligible_aliases,
            "subscription_throttled": sub_throttled,
        }
        job = {
            "id": job_id,
            "kind": j_kind,
            "required_capabilities": json_list(j_caps_json),
            "eligible_kinds": json_list(j_kinds_json),
            "eligible_hosts": json_list(j_hosts_json),
            "max_cost_tier": j_max_tier,
            "preferred_providers": json_list(j_pref_providers),
            "preferred_models": json_list(j_pref_models),
            "queued_at": j_started_at,
        }
        eligible, reason = job_agent_eligible(job, agent)
        if not eligible:
            status = (
                422
                if reason.startswith("job max_cost_tier")
                else (429 if reason.startswith("subscription agent throttled") else 403)
            )
            raise HTTPException(status, reason)

        # 3. atomic claim with race guard + populate claimed_resources
        cur = await db.execute(
            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
            "claimed_runtime=?, claimed_model=?, claimed_provider=?, claimed_cost_tier=?, "
            "claim_lease_expires_at=?, last_update_at=?, "
            "ended_at=NULL, result=NULL "
            "WHERE id=? AND status IN ('queued','offered')",
            (
                by,
                now,
                a_runtime,
                a_model,
                a_provider,
                a_tier,
                now + CLAIM_LEASE_SECONDS,
                now,
                job_id,
            ),
        )
        await db.commit()
        if cur.rowcount == 0:
            raise HTTPException(409, "job already claimed or not available (race)")
        await emit_event(
            db,
            "job.claimed",
            {
                "id": job_id,
                "claimed_by": by,
                "claimed_runtime": a_runtime,
                "claimed_model": a_model,
                "claimed_provider": a_provider,
                "claimed_cost_tier": a_tier,
                "manual_claim": True,
            },
        )
    return {
        "claimed": True,
        "ts": now,
        "claimed_runtime": a_runtime,
        "claimed_model": a_model,
        "claimed_provider": a_provider,
        "claimed_cost_tier": a_tier,
    }


# /v1/jobs (list) + /v1/jobs/{id} (get) moved to hive.bus.routers.jobs (Phase 1c).


# /v1/messages routes moved to hive.bus.routers.messages (Phase 1c).


@app.get("/v1/events")
async def stream_events(request: Request, since_id: int | None = None):
    """SSE stream of events. Optional since_id for catch-up."""
    if len(EVENT_QUEUE) >= MAX_EVENT_SUBSCRIBERS:
        raise HTTPException(503, f"too many event subscribers; max={MAX_EVENT_SUBSCRIBERS}")
    sub_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    EVENT_QUEUE[sub_id] = queue

    async def gen():
        try:
            # catch-up phase
            if since_id is not None:
                async with connect_db() as db:
                    async with db.execute(
                        "SELECT id, ts, kind, payload FROM events WHERE id > ? ORDER BY id ASC",
                        (since_id,),
                    ) as cur:
                        async for r in cur:
                            yield {
                                "id": str(r[0]),
                                "data": json.dumps(
                                    {
                                        "ts": r[1],
                                        "kind": r[2],
                                        "payload": json.loads(r[3]),
                                    }
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


@app.get("/v1/events/log")
async def events_log(since_id: int | None = None, limit: int = 100):
    """JSON polling alternative to SSE."""
    limit = clamp_limit(limit, default=100, max_limit=1000)
    sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
    args: list = []
    if since_id is not None:
        sql += " WHERE id > ?"
        args.append(since_id)
    sql += " ORDER BY id ASC LIMIT ?"
    args.append(limit)
    rows = []
    async with connect_db() as db, db.execute(sql, args) as cur:
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
    return {
        "count": len(rows),
        "events": rows,
        "last_id": rows[-1]["id"] if rows else since_id,
    }


# ---------- minimal MCP shim ----------
# Maps a few common MCP-style JSON-RPC calls to the REST endpoints.
# Phase 2: replace with full mcp-server-sdk.


@app.post("/mcp")
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


# ===== zerocode-gate: pre-submission gate preview + release (patch) =====
# /v1/jobs/{id}/preview moved to hive.bus.routers.jobs (Phase 1c).


@app.post("/v1/jobs/{job_id}/release")
async def zcgate_release(job_id: str):
    """Human-gated 'Release & Push': enqueue a release:<repo> job that a worker
    claims and runs `zc-gate release` on (push branch + open upstream PR)."""
    async with (
        connect_db() as db,
        db.execute("SELECT result, status FROM jobs WHERE id=?", (job_id,)) as cur,
    ):
        r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"job not found: {job_id}")
    result = json.loads(r[0]) if r[0] else {}
    preview = result.get("preview")
    if not preview:
        raise HTTPException(404, f"no preview bundle on job {job_id}")
    if (preview.get("verdict") or "").lower() == "blocked":
        raise HTTPException(409, "bundle verdict is blocked; not releasable")
    if result.get("release_job_id"):
        return {"ok": True, "already": True, "release_job_id": result["release_job_id"]}
    async with connect_db() as db:
        async with db.execute(
            "SELECT urn FROM agents WHERE runtime IN ('mnemos','human','claude-code','claude') "
            "AND status != 'offline' ORDER BY last_heartbeat DESC LIMIT 1"
        ) as cur:
            o = await cur.fetchone()
    if not o:
        raise HTTPException(503, "no active orchestrator to submit the release job")
    submitter = o[0]
    repo = preview.get("repo") or "zeroclaw-labs/zeroclaw"
    rel = await create_job(
        JobCreate(
            submitter_urn=submitter,
            kind="release:zeroclaw",
            description=job_id,
            eligible_kinds=["zeroclaw"],
            eligible_hosts=["zcgate-release-sink"],
            priority=5,
        )
    )
    rel_id = rel.get("id") if isinstance(rel, dict) else None
    result["release_job_id"] = rel_id
    result["release_requested_at"] = time.time()
    async with connect_db() as db:
        await db.execute("UPDATE jobs SET result=? WHERE id=?", (json.dumps(result), job_id))
        await db.commit()
    return {"ok": True, "release_job_id": rel_id, "repo": repo, "submitter": submitter}


# ===== end zerocode-gate patch =====


# ===== zerocode-gate static mount =====
try:
    import os as _zcos

    from fastapi.staticfiles import StaticFiles as _ZCStatic

    _zcdir = "/srv/agent-bus/static"
    _zcos.makedirs(_zcdir, exist_ok=True)
    app.mount("/static", _ZCStatic(directory=_zcdir), name="zcstatic")
except Exception as _zce:
    print("zcgate static mount failed:", _zce, flush=True)
# ===== end zerocode-gate static mount =====
