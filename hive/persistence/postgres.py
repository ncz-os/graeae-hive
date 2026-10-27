"""PostgreSQL repository — the ABC over asyncpg.

Dialect differences vs the sqlite reference are handled in ``dialect.py``
(numbered placeholders, BIGSERIAL identity, no PRAGMAs); postgres speaks the
sqlite ``ON CONFLICT`` upsert natively, so no upsert rewrite is needed.

Driver is imported lazily so the package imports without asyncpg installed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from .dialect import postgres_ddl, to_numbered
from .sqlite import DDL as _SQLITE_DDL
from .sqlite import SqliteHiveRepository


class _PgCursor:
    """Mimics the aiosqlite cursor surface the ABC uses (fetchone/fetchall/iterate)."""

    def __init__(self, rows: list[Any] | None = None, rowcount: int = 0):
        self._rows = rows or []
        self._i = 0
        self.rowcount = rowcount

    async def __aenter__(self) -> _PgCursor:
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    def __aiter__(self) -> _PgCursor:
        return self

    async def __anext__(self) -> Any:
        if self._i >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._i]
        self._i += 1
        return row

    async def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    async def fetchall(self) -> list[Any]:
        return list(self._rows)

    async def close(self) -> None:
        return None


class _PgConnection:
    def __init__(self, conn: Any):
        self._conn = conn
        self._transaction = None

    def execute(self, sql: str, params: Any = None):
        return _PgExecute(self, sql, params)

    async def _begin(self) -> None:
        if self._transaction is None:
            transaction = self._conn.transaction()
            await transaction.start()
            self._transaction = transaction

    async def _run(self, sql: str, params: Any = None) -> _PgCursor:
        await self._begin()
        translated = to_numbered(sql)
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH", "PRAGMA")):
            rows = await self._conn.fetch(translated, *(params or ()))
            return _PgCursor(rows=[tuple(r) for r in rows], rowcount=len(rows))
        tag = await self._conn.execute(translated, *(params or ()))
        # asyncpg returns command tags (UPDATE 0, DELETE 2, INSERT 0 1).
        # A lost compare-and-set must remain zero; never retry failed writes.
        count = tag.rsplit(" ", 1)[-1]
        return _PgCursor(rowcount=int(count) if count.isdigit() else 0)

    async def executemany(self, sql: str, seq: list[tuple[Any, ...]]) -> _PgCursor:
        await self._begin()
        translated = to_numbered(sql)
        await self._conn.executemany(translated, [tuple(p) for p in seq])
        return _PgCursor(rowcount=len(seq))

    async def executescript(self, script: str) -> _PgCursor:
        await self._begin()
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            await self._conn.execute(stmt)
        return _PgCursor()

    async def commit(self) -> None:
        if self._transaction is not None:
            await self._transaction.commit()
            self._transaction = None

    async def rollback(self) -> None:
        if self._transaction is not None:
            await self._transaction.rollback()
            self._transaction = None

    async def close(self) -> None:
        return None


class _PgExecute:
    def __init__(self, conn: _PgConnection, sql: str, params: Any):
        self._conn, self._sql, self._params = conn, sql, params

    def __await__(self):
        return self._conn._run(self._sql, self._params).__await__()

    async def __aenter__(self) -> _PgCursor:
        return await self._conn._run(self._sql, self._params)

    async def __aexit__(self, *_exc) -> None:
        return None


class PostgresHiveRepository(SqliteHiveRepository):
    list_jobs_sql_reason = True  # reason_prefix is expressed in SQL here too

    def __init__(self, dsn: str) -> None:
        super().__init__(":memory:")
        self.dsn = dsn

    def _reason_prefix_sql(self, pattern: str) -> str:
        # postgres LIKE is case-SENSITIVE by default: wrap both sides in
        # LOWER so the prefix match stays case-insensitive (the contract the
        # sqlite reference provides for free). The pattern (prefix + %) is
        # bound, never interpolated.
        return " AND LOWER(result) LIKE LOWER(?)"

    async def init(self, schema: str | None = None) -> None:
        try:
            import asyncpg  # noqa: F401
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"HIVE_BACKEND=postgres requires asyncpg importable: {exc}") from exc
        async with self.connection() as db:
            await db.executescript(postgres_ddl(schema or _SQLITE_DDL))
            await db.commit()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        try:
            import asyncpg
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"HIVE_BACKEND=postgres requires asyncpg importable: {exc}") from exc
        conn = await asyncpg.connect(self.dsn)
        db = _PgConnection(conn)
        db._repo = self
        try:
            yield db
        finally:
            try:
                await db.rollback()
            finally:
                await conn.close()
