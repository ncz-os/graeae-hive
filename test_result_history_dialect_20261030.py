"""FB51: the result-history SQL must survive every backend's translator.

``archive_and_requeue_job`` / ``list_job_result_history`` are written once
against the sqlite reference; postgres, mysql, oracle and db2 subclass
``SqliteHiveRepository`` and rewrite that text for their own engines. This
test captures the statements the reference backend really emits for one
archive + one history read and pushes every one through each backend's own
adapter. FB38's contract suite (a separate branch) was the model, but it is
not a dependency: this file is self-contained and runs against sqlite only.

Two guarantees:

* ``test_planted_sqlite_only_statements_are_flagged`` proves the detector
  fires on known-bad statements, so a green run below is not vacuous.
* ``test_no_emitted_statement_needs_a_sqlite_only_construct`` asserts the
  violation set is empty: a NEW sqlite-only construct in the archive path
  fails this test.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("HIVE_BACKEND", "sqlite")

from hive.persistence.sqlite import SqliteHiveRepository  # noqa: E402

BACKENDS = ("postgres", "mysql", "oracle", "db2")

#: Constructs no non-sqlite backend understands. Each maps to the reason.
FORBIDDEN = {
    "json_extract": "sqlite's json_extract(); the shared SQL must use JSON_VALUE",
    "INSERT OR ": "sqlite's INSERT OR REPLACE/IGNORE upsert spelling",
    "AUTOINCREMENT": "sqlite identity syntax; other backends use sequences/identity",
    "PRAGMA ": "sqlite-only pragma",
    "datetime(": "sqlite date function; other backends need their own expression",
    "strftime(": "sqlite date function",
    "sqlite_master": "sqlite catalog table",
}

#: Extra constructs a particular backend cannot express.
FORBIDDEN_BY_BACKEND = {
    "mysql": {"ON CONFLICT", "excluded."},
    "oracle": {"ON CONFLICT", "excluded."},
    "db2": {"ON CONFLICT", "excluded."},
}

#: The placeholder style each backend must END UP with (None = same as sqlite).
PLACEHOLDER = {"postgres": "$", "mysql": "%s", "oracle": ":", "db2": "?"}


def _strip_literals(sql: str) -> str:
    """Drop string literals so a '?' inside 'text' is not read as a placeholder."""
    out = []
    in_single = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            out.append(" ")
        elif not in_single:
            out.append(ch)
        i += 1
    return "".join(out)


def translate(backend: str, sql: str, params=()):
    """Run ``sql`` through the backend's own adapter (None = not translatable)."""
    if backend == "postgres":
        from hive.persistence.dialect import to_numbered

        return to_numbered(sql), params
    if backend == "mysql":
        from hive.persistence.dialect import mysql_upsert, to_format

        return mysql_upsert(to_format(sql)), params
    if backend == "oracle":
        from hive.persistence.oracle import _translate_sql

        return _translate_sql(sql, params)
    if backend == "db2":
        from hive.persistence.db2 import _adapt_sql

        return _adapt_sql(sql, params)
    raise AssertionError(f"no translation adapter for {backend!r}")


def find_violations(backend: str, sql: str) -> list[str]:
    """Backend-specific problems in ``sql`` (already translated, or raw)."""
    problems = []
    body = _strip_literals(sql)
    lowered = body.lower()
    tokens = dict(FORBIDDEN)
    for token in FORBIDDEN_BY_BACKEND.get(backend, ()):
        tokens[token] = f"not expressible on {backend} (its upsert/lock form differs)"
    for token, why in tokens.items():
        if token.lower() in lowered:
            problems.append(f"{token}: {why}")
    style = PLACEHOLDER.get(backend)
    if style and style != "?" and "?" in body.rstrip():
        problems.append(f"? placeholder survived the {style} conversion")
    return problems


class _RecordingTx:
    """Record every statement, then run it against the real sqlite handle."""

    def __init__(self, tx, sink: list) -> None:
        self._tx = tx
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append((sql, () if params is None else tuple(params)))
        return self._tx.execute(sql, () if params is None else params)

    def executemany(self, sql, seq_of_params=None):
        rows = list(seq_of_params or [])
        self._sink.append((sql, tuple(rows[0]) if rows else ()))
        return self._tx.executemany(sql, rows)


def captured_statements() -> list:
    """The real statements sqlite emits for one archive + one history read."""
    temp = tempfile.TemporaryDirectory()
    repo = SqliteHiveRepository(str(Path(temp.name) / "dialect.db"))
    statements: list = []

    async def body():
        await repo.init()
        async with repo.connection() as tx:
            rec = _RecordingTx(tx, statements)
            await rec.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, "
                "max_retries, retry_count, result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "job-dialect",
                    "urn:submitter",
                    "fix:dialect",
                    "failed",
                    1.0,
                    2,
                    1,
                    json.dumps({"exit_code": 1}),
                ),
            )
            await repo.archive_and_requeue_job(
                rec, job_id="job-dialect", actor="urn:actor", reason="dialect", archived_at=1.0
            )
            await repo.list_job_result_history(rec, job_id="job-dialect", limit=50)
            await tx.commit()

    asyncio.run(body())
    temp.cleanup()
    return statements


class ResultHistoryDialectTests(unittest.TestCase):
    def test_the_archive_path_really_emits_sql(self):
        """Guard the guard: a thin capture would make the checks vacuous."""
        statements = captured_statements()
        verbs = {sql.strip().split()[0].upper() for sql, _ in statements if sql.strip()}
        self.assertIn("SELECT", verbs, statements)
        self.assertIn("INSERT", verbs, statements)
        self.assertIn("UPDATE", verbs, statements)
        self.assertTrue(all(sql.strip() for sql, _ in statements))

    def test_planted_sqlite_only_statements_are_flagged(self):
        """A statement the detector must reject, in each of its flavours."""
        planted = {
            "postgres": "SELECT json_extract(result, '$.a') FROM jobs",
            "mysql": "INSERT OR REPLACE INTO hive_cache (cache_key) VALUES (?)",
            "oracle": "PRAGMA table_info(jobs)",
            "db2": "SELECT * FROM sqlite_master WHERE type='table'",
        }
        for backend, sql in planted.items():
            self.assertTrue(find_violations(backend, sql), f"{backend}: missed {sql!r}")

    def test_no_emitted_statement_needs_a_sqlite_only_construct(self):
        """Translate every captured statement; no sqlite-only constructs allowed."""
        unexpected: list[str] = []
        for backend in BACKENDS:
            for sql, params in captured_statements():
                translated, _params = translate(backend, sql, params)
                self.assertIsNotNone(translated, f"{backend}: adapter dropped DML: {sql[:120]}")
                for violation in find_violations(backend, translated):
                    unexpected.append(f"{backend}: {violation} <- {sql.strip()[:100]}")
        self.assertEqual(unexpected, [])

    def test_history_table_name_survives_translation(self):
        """Oracle/Db2 prefix the archive table; the rewrite must apply here too."""
        history_statements = [
            (sql, params) for sql, params in captured_statements() if "job_result_history" in sql.lower()
        ]
        self.assertTrue(history_statements, "no captured statement mentions job_result_history")
        for sql, params in history_statements:
            oracle_sql, _ = translate("oracle", sql, params)
            db2_sql, _ = translate("db2", sql, params)
            self.assertIn("hive_job_result_history", (oracle_sql or "").lower())
            self.assertIn("hive_job_result_history", (db2_sql or "").lower())

    def test_reference_ddl_translates_for_postgres_and_mysql(self):
        """The shared DDL (history table included) must lose sqlite-only syntax."""
        from hive.persistence.dialect import mysql_ddl, postgres_ddl
        from hive.persistence.sqlite import DDL

        self.assertEqual(find_violations("postgres", postgres_ddl(DDL)), [])
        self.assertEqual(find_violations("mysql", mysql_ddl(DDL)), [])


if __name__ == "__main__":
    unittest.main()
