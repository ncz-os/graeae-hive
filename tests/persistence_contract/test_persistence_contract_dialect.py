"""FB38: every SQL string the reference repository emits must translate.

The reference repository (``hive.persistence.sqlite``) writes the SQL once; each
backend subclasses it and rewrites that text for its own engine. That only works
while the shared SQL avoids sqlite-only syntax. This module captures the real
statements the repository emits for the contract workload and pushes every one
through each backend's own adapter.

Two guarantees:

* ``test_planted_sqlite_only_statement_is_flagged`` proves the detector fires on
  a known-bad statement, so a green run below is not a green no-op.
* ``test_no_statement_needs_a_sqlite_only_construct`` asserts the violation set
  equals ``KNOWN_DIALECT_GAPS`` exactly: a NEW sqlite-only construct fails, and
  fixing a known gap also fails until the entry is deleted.
"""

from __future__ import annotations

import re

import contract_support as cs
import pytest

BACKENDS = ("postgres", "mysql", "oracle", "db2")

#: Real gaps this suite found, pinned so they cannot grow silently.
#: FB38-FINDING-2: ``cache_stats`` and ``worker_stats_rows`` format timestamps
#: with sqlite's ``datetime(col,'unixepoch')``. Oracle's adapter rewrites it
#: (TO_CHAR/NUMTODSINTERVAL); postgres, mysql and db2 pass it through, so those
#: routes would raise on a real server.
KNOWN_DIALECT_GAPS = {
    ("postgres", "datetime(cached_at)"),
    ("postgres", "datetime(last_run)"),
    ("mysql", "datetime(cached_at)"),
    ("mysql", "datetime(last_run)"),
    ("db2", "datetime(cached_at)"),
    ("db2", "datetime(last_run)"),
}


@pytest.fixture(scope="module")
def captured_sql():
    import asyncio
    import tempfile
    import uuid
    from pathlib import Path

    from hive.persistence.factory import get_hive_repository

    with tempfile.TemporaryDirectory() as tmp:
        repo = get_hive_repository(backend="sqlite", db_path=str(Path(tmp) / "capture.db"))
        asyncio.run(repo.init())
        statements = cs.capture_sql(repo, uuid.uuid4().hex)
    return statements


def test_the_workload_really_emits_sql(captured_sql):
    """Guard the guard: an empty or thin capture would make the checks vacuous."""
    assert len(captured_sql) >= 40, f"only {len(captured_sql)} statements captured"
    verbs = {sql.strip().split()[0].upper() for sql, _ in captured_sql if sql.strip()}
    assert {"SELECT", "INSERT", "UPDATE"} <= verbs, verbs
    assert all(sql.strip() for sql, _ in captured_sql)


def test_planted_sqlite_only_statement_is_flagged():
    """A statement the detector must reject, in each of its flavours."""
    planted = {
        "postgres": "SELECT json_extract(result, '$.a') FROM jobs",
        "mysql": "INSERT OR REPLACE INTO hive_cache (cache_key) VALUES (?)",
        "oracle": "PRAGMA table_info(jobs)",
        "db2": "SELECT * FROM sqlite_master WHERE type='table'",
    }
    for backend, sql in planted.items():
        assert cs.find_violations(backend, sql), f"{backend}: the detector missed {sql!r}"


def test_planted_ddl_is_flagged():
    """The DDL path uses the same detector, so a planted AUTOINCREMENT fails too."""
    ddl = "CREATE TABLE t (\n  id INTEGER PRIMARY KEY AUTOINCREMENT\n);"
    assert cs.find_violations("postgres", ddl), "AUTOINCREMENT must be flagged"


def test_no_statement_needs_a_sqlite_only_construct(captured_sql):
    """Translate every captured statement and compare against the pinned gaps."""
    unexpected: list[str] = []
    found_gaps: set[tuple[str, str]] = set()

    for backend in BACKENDS:
        for sql, params in captured_sql:
            translated, _params = cs.translate(backend, sql, params)
            if translated is None:
                # Oracle/Db2 own their DDL: their adapters answer None for
                # CREATE/ALTER/PRAGMA because the backend issues its own schema.
                first = sql.strip().split()[0].upper()
                assert first in {"CREATE", "ALTER", "PRAGMA", "BEGIN"}, (
                    f"{backend}: adapter dropped a DML statement: {sql[:120]}"
                )
                continue
            for violation in cs.find_violations(backend, translated):
                if violation.startswith("datetime("):
                    found_gaps.update((backend, gap) for gap in cs.dialect_gaps(translated))
                    continue
                unexpected.append(f"{backend}: {violation} <- {sql.strip()[:100]}")

    assert not unexpected, "sqlite-only constructs leaked into a backend's SQL:\n" + "\n".join(unexpected)
    assert found_gaps == KNOWN_DIALECT_GAPS, (
        "the set of known sqlite-only date expressions changed; update KNOWN_DIALECT_GAPS "
        f"(new={sorted(found_gaps - KNOWN_DIALECT_GAPS)}, fixed={sorted(KNOWN_DIALECT_GAPS - found_gaps)})"
    )


def test_ddl_translates_for_postgres_and_mysql():
    """The reference DDL must lose its sqlite identity/PRAGMA syntax."""
    from hive.persistence.dialect import mysql_ddl, postgres_ddl
    from hive.persistence.sqlite import DDL

    pg = cs.find_violations("postgres", postgres_ddl(DDL))
    my = cs.find_violations("mysql", mysql_ddl(DDL))
    assert pg == [], pg
    assert my == [], my
    assert "AUTOINCREMENT" in DDL, "the reference DDL is expected to use sqlite identity syntax"


def test_oracle_drops_ddl_and_db2_rewrites_it():
    """The two adapters disagree about DDL, and the contract records which is which.

    Oracle answers None (its repository issues its own schema); Db2 passes the
    statement through its table-name rewrite.
    """
    from hive.persistence.db2 import _adapt_sql
    from hive.persistence.oracle import _translate_sql

    ddl = "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY)"
    assert _translate_sql(ddl, ())[0] is None
    assert _adapt_sql(ddl, ())[0] is not None, "db2 rewrites the table name instead of dropping DDL"

    dml = "SELECT id FROM jobs WHERE status=? LIMIT ?"
    oracle_sql, _ = _translate_sql(dml, ("queued", 10))
    db2_sql, db2_params = _adapt_sql(dml, ("queued", 10))
    assert "?" not in oracle_sql
    assert re.search(r":\d", oracle_sql)
    assert "FETCH FIRST" in oracle_sql.upper()
    assert "FETCH FIRST" in db2_sql.upper()
    assert db2_params == ("queued",), "the LIMIT value is inlined, so its bind must be dropped"
