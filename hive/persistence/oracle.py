"""Oracle Hive Mind repository.

The live Oracle SQL translation shim remains behavior-compatible with the
pre-refactor bus. This module owns Oracle backend selection; the FastAPI layer
uses the repository factory instead of branching on HIVE_BACKEND.
"""

from __future__ import annotations

import datetime as _dt
import inspect
import json as _json
import re
import uuid as _uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal as _Decimal
from typing import Any
from urllib.parse import unquote, urlparse

from .sqlite import SqliteHiveRepository

_ORACLE_JOB_ADDITIONS = {
    "REQUIRED_AUTONOMY": "required_autonomy VARCHAR2(32)",
    "REQUIRED_RESOURCES": "required_resources CLOB CHECK (required_resources IS JSON)",
    "CLAIMED_HOST_CAPS": "claimed_host_caps CLOB CHECK (claimed_host_caps IS JSON)",
    "PROJECT": "project VARCHAR2(120)",
    "TAGS": "tags CLOB CHECK (tags IS JSON)",
    "DEPENDS_ON": "depends_on CLOB CHECK (depends_on IS JSON)",
    "RETRY_COUNT": "retry_count NUMBER(5) DEFAULT 0 NOT NULL",
    "MAX_RETRIES": "max_retries NUMBER(5) DEFAULT 2 NOT NULL",
    "RETRY_BACKOFF_UNTIL": "retry_backoff_until NUMBER",
    "LAST_UPDATE_AT": "last_update_at NUMBER",
    "ELIGIBLE_HOSTS": "eligible_hosts CLOB CHECK (eligible_hosts IS JSON)",
    "CLAIM_LEASE_EXPIRES_AT": "claim_lease_expires_at NUMBER",
    "DECLINE_COUNT": "decline_count NUMBER(5) DEFAULT 0 NOT NULL",
    "DEDUP_HASH": "dedup_hash VARCHAR2(64)",
}
_ORACLE_REQUIRED_JOB_COLUMNS = {
    "ID",
    "SUBMITTER_URN",
    "PARENT_JOB_ID",
    "KIND",
    "DESCRIPTION",
    "PRIORITY",
    "DEADLINE",
    "REQUIRED_CAPABILITIES",
    "ELIGIBLE_KINDS",
    "ELIGIBLE_HOSTS",
    "PROJECT",
    "STATUS",
    "CLAIMED_BY",
    "CLAIMED_AT",
    "STARTED_AT",
    "ENDED_AT",
    "RESULT",
    "REQUIRED_AUTONOMY",
    "MAX_COST_TIER",
    "PREFERRED_PROVIDERS",
    "PREFERRED_MODELS",
    "CLAIMED_RUNTIME",
    "CLAIMED_MODEL",
    "CLAIMED_PROVIDER",
    "CLAIMED_COST_TIER",
    "TOKENS_IN",
    "TOKENS_OUT",
    "ESTIMATED_COST_USD",
    "MNEMOS_REFS",
    "RESULT_MNEMOS_ID",
    "REQUIRED_RESOURCES",
    "CLAIMED_HOST_CAPS",
    "TAGS",
    "DEPENDS_ON",
    "RETRY_COUNT",
    "MAX_RETRIES",
    "RETRY_BACKOFF_UNTIL",
    "LAST_UPDATE_AT",
    "CLAIM_LEASE_EXPIRES_AT",
    "DECLINE_COUNT",
    "DEDUP_HASH",
}
_ORACLE_JOB_STATUSES = (
    "queued",
    "offered",
    "claimed",
    "running",
    "done",
    "failed",
    "failed_completion",
    "cancelled",
    "dead-letter",
    "needs-review",
)


async def _await_if_needed(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _raw_execute(raw: Any, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    cursor = raw.cursor()
    try:
        if params:
            await _await_if_needed(cursor.execute(sql, params))
        else:
            await _await_if_needed(cursor.execute(sql))
        fetchall = getattr(cursor, "fetchall", None)
        if fetchall is None or getattr(cursor, "description", None) is None:
            return []
        return list(await _await_if_needed(fetchall()))
    finally:
        await _await_if_needed(cursor.close())


def _parse_oracle_url(url: str) -> tuple[str, str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        return "", "", url
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1521
    service = (parsed.path or "/ORCLPDB1").lstrip("/")
    return user, password, f"{host}:{port}/{service}"


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


def _replace_qmarks(sql: str) -> str:
    out: list[str] = []
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
            "WHEN MATCHED THEN UPDATE SET t.result_json=s.result_json, t.source_job_id=s.source_job_id, "
            "t.result_mnemos_id=s.result_mnemos_id, t.cached_at=s.cached_at, t.model=s.model, t.provider=s.provider "
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
            "t.total_tokens_in=t.total_tokens_in+s.upd_tokens_in, t.total_tokens_out=t.total_tokens_out+s.upd_tokens_out, "
            "t.total_cost_usd=t.total_cost_usd+s.upd_cost, t.total_duration_sec=t.total_duration_sec+s.upd_duration, "
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

    @staticmethod
    async def _normalize_row(row):
        if row is None:
            return None

        def default(value):
            if isinstance(value, _Decimal):
                return float(value) if value != value.to_integral_value() else int(value)
            if isinstance(value, (bytes, bytearray)):
                if len(value) == 16:
                    return str(_uuid.UUID(bytes=bytes(value)))
                return value.decode("utf-8", errors="replace")
            if isinstance(value, _dt.datetime):
                return value.isoformat()
            raise TypeError(f"unhandled type {type(value).__name__}")

        out = []
        for value in row:
            if isinstance(value, (dict, list)):
                out.append(_json.dumps(value, separators=(",", ":"), default=default))
            elif hasattr(value, "read") and callable(value.read):
                result = value.read()
                if hasattr(result, "__await__"):
                    result = await result
                out.append(result)
            elif isinstance(value, _Decimal):
                out.append(int(value) if value == value.to_integral_value() else float(value))
            elif isinstance(value, _dt.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=_dt.UTC)
                out.append(value.timestamp())
            elif isinstance(value, (bytes, bytearray)) and len(value) == 16:
                out.append(str(_uuid.UUID(bytes=bytes(value))))
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
        return await self._normalize_row(await self._cursor.fetchone())

    async def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._index :]
            self._index = len(self._rows)
            return [await self._normalize_row(row) for row in rows]
        return [await self._normalize_row(row) for row in await self._cursor.fetchall()]

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

    def execute(self, sql: str, params: Any = None):
        return _OracleExecuteResult(self, sql, params)

    async def _execute(self, sql: str, params: Any = None):
        match = re.match(r"^\s*PRAGMA\s+table_info\((\w+)\)", sql, flags=re.IGNORECASE)
        if match:
            mapped = _TABLE_MAP.get(match.group(1).lower(), match.group(1))
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
        await cur.executemany(_rewrite_limit(translated), [tuple(p) for p in seq_of_params])
        return BackendCursor(cur)

    async def executescript(self, script: str):
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


class OracleHiveRepository(SqliteHiveRepository):
    """Oracle backend using the repository-owned SQL compatibility adapter."""

    connection_class: Any = OracleConnection

    def __init__(self, dsn: str) -> None:
        super().__init__(":memory:")
        self.dsn = dsn

    async def init(self, schema: str | None = None) -> None:
        try:
            import oracledb
        except Exception as exc:
            raise RuntimeError(f"HIVE_BACKEND=oracle requires python-oracledb importable: {exc}") from exc
        user, password, dsn = _parse_oracle_url(self.dsn)
        raw = await oracledb.connect_async(user=user, password=password, dsn=dsn)
        try:
            await self._apply_migrations(raw)
            await self._verify_schema(raw)
            await _await_if_needed(raw.commit())
        except Exception:
            await _await_if_needed(raw.rollback())
            raise
        finally:
            await _await_if_needed(raw.close())

    @staticmethod
    async def _job_columns(raw: Any) -> dict[str, tuple[str, int | None]]:
        rows = await _raw_execute(
            raw,
            "SELECT column_name, data_type, data_length FROM user_tab_columns WHERE table_name = 'HIVE_JOBS'",
        )
        return {
            str(name).strip().upper(): (
                str(data_type).strip().upper(),
                int(length) if length is not None else None,
            )
            for name, data_type, length in rows
        }

    @staticmethod
    async def _indexes(raw: Any) -> set[str]:
        rows = await _raw_execute(
            raw,
            "SELECT index_name FROM user_indexes WHERE table_name = 'HIVE_JOBS'",
        )
        return {str(row[0]).strip().upper() for row in rows}

    async def _apply_migrations(self, raw: Any) -> None:
        columns = await self._job_columns(raw)
        if not columns:
            raise RuntimeError(
                "Oracle schema migration failed: HIVE_JOBS is missing; apply the canonical MNEMOS Oracle schema first"
            )
        for name, definition in _ORACLE_JOB_ADDITIONS.items():
            if name not in columns:
                await _raw_execute(raw, f"ALTER TABLE HIVE_JOBS ADD ({definition})")
        columns = await self._job_columns(raw)
        status_type, status_length = columns.get("STATUS", ("", 0))
        if status_type not in {"VARCHAR2", "VARCHAR"} or (status_length or 0) < 32:
            await _raw_execute(raw, "ALTER TABLE HIVE_JOBS MODIFY (status VARCHAR2(32))")

        checks = await _raw_execute(
            raw,
            "SELECT constraint_name, search_condition_vc FROM user_constraints "
            "WHERE table_name = 'HIVE_JOBS' AND constraint_type = 'C'",
        )
        status_checks = [
            (str(name).strip().upper(), str(condition or ""))
            for name, condition in checks
            if re.search(r"\bSTATUS\b\s+IN\s*\(", str(condition or ""), flags=re.IGNORECASE)
        ]
        if not any(
            all(value.upper() in condition.upper() for value in _ORACLE_JOB_STATUSES)
            for _name, condition in status_checks
        ):
            for name, _condition in status_checks:
                await _raw_execute(raw, f"ALTER TABLE HIVE_JOBS DROP CONSTRAINT {name}")
            values = ",".join(f"'{value}'" for value in _ORACLE_JOB_STATUSES)
            await _raw_execute(
                raw,
                f"ALTER TABLE HIVE_JOBS ADD CONSTRAINT CK_HIVE_JOBS_STATUS_RUNTIME CHECK (status IN ({values}))",
            )

        indexes = await self._indexes(raw)
        if "IDX_HIVE_JOBS_CLAIM_LEASE" not in indexes:
            await _raw_execute(
                raw,
                "CREATE INDEX IDX_HIVE_JOBS_CLAIM_LEASE ON HIVE_JOBS(CLAIM_LEASE_EXPIRES_AT)",
            )
        if not ({"JOBS_ACTIVE_DEDUP_UQ", "HIVE_JOBS_ACTIVE_DEDUP_UQ"} & indexes):
            await _raw_execute(raw, "LOCK TABLE HIVE_JOBS IN EXCLUSIVE MODE")
            active = await _raw_execute(
                raw,
                "SELECT id, dedup_hash FROM HIVE_JOBS WHERE dedup_hash IS NOT NULL "
                "AND status IN ('queued','offered','claimed','running') "
                "ORDER BY dedup_hash, started_at, id",
            )
            seen_hashes: set[str] = set()
            migration_result = _json.dumps(
                {
                    "reason": "oracle_migration:duplicate_active_dedup_hash",
                }
            )
            for job_id, dedup_hash in active:
                if dedup_hash not in seen_hashes:
                    seen_hashes.add(dedup_hash)
                    continue
                # Only nulling dedup_hash satisfied the unique index while
                # leaving the duplicate ACTIVE, so a worker still claimed and
                # ran it -- the exact double-execution the index exists to
                # prevent. Db2 dead-letters these (db2.py); match that.
                await _raw_execute(
                    raw,
                    "UPDATE HIVE_JOBS SET status='dead-letter', dedup_hash=NULL, "
                    "ended_at=SYSTIMESTAMP, last_update_at=SYSTIMESTAMP, "
                    "result=COALESCE(result, :1) WHERE id=:2 "
                    "AND status IN ('queued','offered','claimed','running')",
                    (migration_result, job_id),
                )
            await _raw_execute(
                raw,
                "CREATE UNIQUE INDEX JOBS_ACTIVE_DEDUP_UQ ON HIVE_JOBS "
                "(CASE WHEN status IN ('queued','offered','claimed','running') THEN dedup_hash END)",
            )

    async def _verify_schema(self, raw: Any) -> None:
        columns = await self._job_columns(raw)
        missing = _ORACLE_REQUIRED_JOB_COLUMNS - set(columns)
        if missing:
            raise RuntimeError(f"Oracle schema verification failed: HIVE_JOBS missing {sorted(missing)}")
        status_type, status_length = columns["STATUS"]
        if status_type not in {"VARCHAR2", "VARCHAR"} or (status_length or 0) < 17:
            raise RuntimeError(
                f"Oracle schema verification failed: HIVE_JOBS.STATUS is {status_type}({status_length}), expected VARCHAR2(>=17)"
            )
        dedup_type, dedup_length = columns["DEDUP_HASH"]
        if dedup_type not in {"VARCHAR2", "VARCHAR"} or (dedup_length or 0) < 64:
            raise RuntimeError(
                f"Oracle schema verification failed: HIVE_JOBS.DEDUP_HASH is {dedup_type}({dedup_length}), expected VARCHAR2(>=64)"
            )
        if columns["CLAIM_LEASE_EXPIRES_AT"][0] != "NUMBER":
            raise RuntimeError("Oracle schema verification failed: HIVE_JOBS.CLAIM_LEASE_EXPIRES_AT must be NUMBER")
        if columns["ELIGIBLE_HOSTS"][0] not in {"CLOB", "JSON"}:
            raise RuntimeError("Oracle schema verification failed: HIVE_JOBS.ELIGIBLE_HOSTS must be CLOB or JSON")
        indexes = await self._indexes(raw)
        if "IDX_HIVE_JOBS_CLAIM_LEASE" not in indexes:
            raise RuntimeError("Oracle schema verification failed: claim lease index is missing")
        if not ({"JOBS_ACTIVE_DEDUP_UQ", "HIVE_JOBS_ACTIVE_DEDUP_UQ"} & indexes):
            raise RuntimeError("Oracle schema verification failed: active dedup index is missing")

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        try:
            import oracledb
        except Exception as exc:
            raise RuntimeError(f"HIVE_BACKEND=oracle requires python-oracledb importable: {exc}") from exc
        user, password, dsn = _parse_oracle_url(self.dsn)
        raw = await oracledb.connect_async(user=user, password=password, dsn=dsn)
        db = self.connection_class(raw)
        try:
            yield db
        finally:
            await db.close()
