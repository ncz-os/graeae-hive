"""MySQL / MariaDB repository — the ABC over aiomysql.

Dialect differences vs the sqlite reference are handled in ``dialect.py``:
``?`` -> ``%s``, ``ON CONFLICT`` upserts -> ``ON DUPLICATE KEY UPDATE``, and the
DDL types/identity. Driver is imported lazily.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

from .dialect import mysql_ddl, mysql_upsert, to_format
from .sqlite import DDL as _SQLITE_DDL
from .sqlite import SqliteHiveRepository


class _MyCursor:
    def __init__(self, cur: Any):
        self._cur = cur
        self.rowcount = getattr(cur, "rowcount", 0)

    async def __aenter__(self) -> _MyCursor:
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    def __aiter__(self) -> _MyCursor:
        return self

    async def __anext__(self) -> Any:
        row = await self._cur.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def fetchone(self) -> Any:
        return await self._cur.fetchone()

    async def fetchall(self) -> list[Any]:
        return list(await self._cur.fetchall() or [])

    async def close(self) -> None:
        try:
            await self._cur.close()
        except Exception:  # noqa: BLE001
            pass


class _MyConnection:
    def __init__(self, conn: Any):
        self._conn = conn

    async def _cursor(self) -> _MyCursor:
        return _MyCursor(await self._conn.cursor())

    async def _run(self, sql: str, params: Any = None) -> _MyCursor:
        cur = await self._cursor()
        await cur._cur.execute(to_format(mysql_upsert(sql)), params or ())
        return cur

    def execute(self, sql: str, params: Any = None) -> Any:
        return _MyExecute(self, sql, params)

    async def executemany(self, sql: str, seq: list[tuple[Any, ...]]) -> _MyCursor:
        cur = await self._cursor()
        await cur._cur.executemany(to_format(mysql_upsert(sql)), [tuple(p) for p in seq])
        return cur

    async def executescript(self, script: str) -> _MyCursor:
        cur = await self._cursor()
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            await cur._cur.execute(stmt)
        return cur

    async def commit(self) -> None:
        await self._conn.commit()

    async def rollback(self) -> None:
        await self._conn.rollback()

    async def close(self) -> None:
        return None


class _MyExecute:
    def __init__(self, conn: _MyConnection, sql: str, params: Any):
        self._conn, self._sql, self._params = conn, sql, params

    def __await__(self):
        return self._conn._run(self._sql, self._params).__await__()

    async def __aenter__(self) -> _MyCursor:
        return await self._conn._run(self._sql, self._params)

    async def __aexit__(self, *_exc) -> None:
        return None


class MySqlHiveRepository(SqliteHiveRepository):
    def __init__(self, dsn: str) -> None:
        super().__init__(":memory:")
        self.dsn = dsn

    async def init(self, schema: str | None = None) -> None:
        async with self.connection() as db:
            await db.executescript(mysql_ddl(schema or _SQLITE_DDL))

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        try:
            import aiomysql
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"HIVE_BACKEND=mysql requires aiomysql importable: {exc}") from exc
        u = urlparse(self.dsn)
        conn = await aiomysql.connect(
            host=u.hostname or "127.0.0.1",
            port=u.port or 3306,
            user=unquote(u.username or "root"),
            password=unquote(u.password or ""),
            db=(u.path or "/").lstrip("/") or None,
            autocommit=True,
        )
        db = _MyConnection(conn)
        try:
            yield db
        finally:
            conn.close()
