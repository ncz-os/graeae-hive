"""Dialect helpers: turn the reference (sqlite) SQL + DDL into each backend's form.

The ABC is written once against the sqlite reference. Postgres and MySQL adapt it
here — the same way ``oracle.py``/``db2.py`` do for their dialects:

  * placeholders: ``?``  ->  ``$1..$n`` (postgres) / ``%s`` (mysql)
  * DDL: sqlite types/identity -> the backend's (BIGSERIAL / AUTO_INCREMENT, etc.)
  * upserts: sqlite ``ON CONFLICT ... DO UPDATE`` -> mysql ``ON DUPLICATE KEY UPDATE``
    (postgres speaks ``ON CONFLICT`` natively, so it needs no rewrite)

Kept pure + dependency-free so it is unit-testable without a live server.
"""

from __future__ import annotations

import re

# --------------------------------------------------------------------------- #
# placeholders
# --------------------------------------------------------------------------- #


def to_numbered(sql: str) -> str:
    """``?`` placeholders -> ``$1..$n`` (postgres/asyncpg)."""
    n = 0

    def repl(_m: re.Match) -> str:
        nonlocal n
        n += 1
        return f"${n}"

    return re.sub(r"\?", repl, sql)


def to_format(sql: str) -> str:
    """``?`` placeholders -> ``%s`` (mysql). Literal ``%`` is escaped."""
    return sql.replace("%", "%%").replace("?", "%s")


# --------------------------------------------------------------------------- #
# upserts
# --------------------------------------------------------------------------- #

_OC = re.compile(
    r"ON CONFLICT\((?P<key>[^)]+)\) DO UPDATE SET (?P<set>.*?)(?=(?:\"|\)|$))",
    re.IGNORECASE | re.DOTALL,
)


def mysql_upsert(sql: str) -> str:
    """sqlite ``ON CONFLICT(cols) DO UPDATE SET a=excluded.a, ...`` -> mysql
    ``ON DUPLICATE KEY UPDATE a=VALUES(a), ...``."""

    def repl(m: re.Match) -> str:
        set_clause = m.group("set")
        converted = re.sub(r"excluded\.(\w+)", r"VALUES(\1)", set_clause)
        return f"ON DUPLICATE KEY UPDATE {converted}"

    return _OC.sub(repl, sql)


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #


def postgres_ddl(sqlite_ddl: str) -> str:
    """sqlite DDL -> postgres DDL (types + identity + drop PRAGMAs)."""
    out = []
    for line in sqlite_ddl.splitlines():
        s = line.strip()
        if s.upper().startswith("PRAGMA"):
            continue
        out.append(line)
    ddl = "\n".join(out)
    ddl = re.sub(r"\bINTEGER PRIMARY KEY AUTOINCREMENT\b", "BIGSERIAL PRIMARY KEY", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\bAUTOINCREMENT\b", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\bREAL\b", "double precision", ddl)
    ddl = re.sub(r"\bTEXT\b", "text", ddl)
    return ddl


def mysql_ddl(sqlite_ddl: str) -> str:
    """sqlite DDL -> mysql DDL (types + identity; mysql lacks IF NOT EXISTS on
    indexes in older versions, so those become plain CREATEs guarded elsewhere)."""
    ddl = re.sub(
        r"\bINTEGER PRIMARY KEY AUTOINCREMENT\b", "BIGINT AUTO_INCREMENT PRIMARY KEY", sqlite_ddl, flags=re.IGNORECASE
    )
    ddl = re.sub(r"\bAUTOINCREMENT\b", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\bREAL\b", "double", ddl)
    ddl = re.sub(r"\bTEXT\b", "text", ddl)
    ddl = re.sub(r"CREATE INDEX IF NOT EXISTS", "CREATE INDEX", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"CREATE TABLE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS", ddl, flags=re.IGNORECASE)
    return ddl
