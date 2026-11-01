"""The reclassify field patch's payload compare-and-set must parse on every backend.

PYTHIA, 2026-11-01 14:33 UTC, Oracle backend: ``POST /v1/admin/jobs/reclassify``
with a ``fields`` patch returned HTTP 500 -- ``ORA-00908 ... IS or IS NOT, when
expecting a predicate``. ``admin._patch_result_cas`` issued
``UPDATE jobs SET ... WHERE id=? AND status=? AND result IS ?``; ``x IS ?`` is a
sqlite-only null-safe equality, and the Oracle adapter turned it into
``IS :n``. Postgres (``IS $n``), MySQL (``IS %s``) and Db2 (``IS ?``) reject it
the same way, and on Oracle/Db2 ``jobs.result`` is a CLOB, which cannot be an
operand of ``=`` either.

Two halves, as the dialect contract suite does it:

* Dialect: drive ``_patch_result_cas`` with each backend's repository in place,
  record the SQL it emits, push it through that backend's own adapter and check
  the translated text against the backend's predicate grammar. No server is
  needed; a live DSN run is the behaviour suite's job.
* Semantics, on a real SQLite database: a matching payload updates 1 row, a
  payload a concurrent writer replaced updates 0 (conflict), a NULL expected
  payload matches a NULL result, and the status guard still applies -- for the
  SQL compare path AND for the locked read-compare path Oracle/Db2 use.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from unittest import mock

import contract_support as cs
import pytest

from hive.bus.routers import admin
from hive.persistence.db2 import Db2HiveRepository
from hive.persistence.mysql import MySqlHiveRepository
from hive.persistence.oracle import OracleHiveRepository
from hive.persistence.postgres import PostgresHiveRepository
from hive.persistence.sqlite import SqliteHiveRepository

#: Placeholder DSNs: the constructors only store them, nothing connects.
DIALECT_REPOS = {
    "postgres": lambda: PostgresHiveRepository("postgres://u:p@db.invalid/hive"),
    "mysql": lambda: MySqlHiveRepository("mysql://u:p@db.invalid/hive"),
    "oracle": lambda: OracleHiveRepository("oracle://u:p@db.invalid:1521/HIVE"),
    "db2": lambda: Db2HiveRepository("db2://u:p@db.invalid:50000/HIVE"),
}

RAW = json.dumps({"baseline": "0a1c9f3e", "worker_error": "merge-base exited 128"})
PATCH = {"baseline": "4b8c1e0a7d92f36b58a0c1d4e7f9b2a3c6d8e0f1"}
SETS = {"status": "needs-review", "claimed_by": None, "claimed_at": None}


# --------------------------------------------------------------------------- #
# a scripted transaction: records SQL, answers like the row is unchanged
# --------------------------------------------------------------------------- #


class _Cursor:
    def __init__(self, rows, rowcount):
        self._rows = list(rows)
        self.rowcount = rowcount

    def __await__(self):
        async def _self():
            return self

        return _self().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _ScriptedTx:
    def __init__(self, current_result):
        self.current_result = current_result
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=None):
        params = tuple(params or ())
        self.statements.append((sql, params))
        verb = sql.strip().split()[0].upper()
        if verb == "SELECT":
            return _Cursor([(self.current_result,)], 1)
        return _Cursor([], 1)


def _emit(backend: str, raw):
    """The statements ``_patch_result_cas`` sends with ``backend``'s repository."""
    repo = DIALECT_REPOS[backend]()
    tx = _ScriptedTx(raw)
    with mock.patch.object(admin, "hive_repo", lambda: repo):
        changed = asyncio.run(admin._patch_result_cas(tx, "job-1", dict(SETS), "failed", raw, PATCH))
    return changed, tx.statements


# --------------------------------------------------------------------------- #
# dialect half
# --------------------------------------------------------------------------- #


def test_the_grammar_check_flags_the_production_statement():
    """Guard the guard: the exact statement from the 14:33 journal entry."""
    oracle_sql, _ = cs.translate(
        "oracle",
        "UPDATE jobs SET status=?, claimed_by=?, claimed_at=?, result=? WHERE id=? AND status=? AND result IS ?",
        ("needs-review", None, None, "{}", "j", "failed", None),
    )
    assert "IS :7" in oracle_sql
    assert any("IS <bind>" in p for p in cs.predicate_violations("oracle", oracle_sql))
    # The LOB rule: a plain equality on a CLOB is just as unparseable there.
    assert cs.predicate_violations("db2", "UPDATE hive_jobs SET result=? WHERE id=? AND result=?")
    # The assignment side alone is fine.
    assert not cs.predicate_violations("db2", "UPDATE hive_jobs SET result=? WHERE id=? AND status=?")
    assert cs.predicate_violations("oracle", "SELECT 1 FROM dual WHERE a IS NOT DISTINCT FROM :1")
    assert cs.predicate_violations("postgres", "SELECT 1 WHERE a <=> $1")


@pytest.mark.parametrize("backend", sorted(DIALECT_REPOS))
@pytest.mark.parametrize("raw", [RAW, None], ids=["json-result", "null-result"])
def test_the_payload_cas_translates_to_valid_sql(backend, raw):
    changed, statements = _emit(backend, raw)
    assert changed == 1, statements
    writes = [(sql, p) for sql, p in statements if sql.strip().upper().startswith("UPDATE")]
    assert len(writes) == 1, statements
    problems = []
    for sql, params in statements:
        translated, _ = cs.translate(backend, sql, params)
        assert translated is not None, sql
        # find_violations includes the predicate-grammar rules.
        problems += [f"{p} <- {translated}" for p in cs.find_violations(backend, translated)]
    assert not problems, f"{backend}:\n" + "\n".join(problems)


@pytest.mark.parametrize("backend", sorted(DIALECT_REPOS))
def test_the_write_still_guards_status_and_carries_the_merge(backend):
    """Router behaviour is unchanged: the UPDATE keeps the status CAS and
    writes the merged payload."""
    _changed, statements = _emit(backend, RAW)
    sql, params = next((s, p) for s, p in statements if s.strip().upper().startswith("UPDATE"))
    assert "status=?" in sql.replace(" ", "").split("WHERE", 1)[1]
    assert "failed" in params and "job-1" in params
    merged = next(p for p in params if isinstance(p, str) and p.startswith("{"))
    assert json.loads(merged) == {**json.loads(RAW), **PATCH}


@pytest.mark.parametrize("backend", ["oracle", "db2"])
def test_lob_backends_guard_the_payload_under_a_row_lock(backend):
    """Oracle/Db2 cannot compare the CLOB in SQL, so the payload is read under
    the backend's own row lock and compared before the write; a payload that
    changed is a conflict and no UPDATE is sent."""
    repo = DIALECT_REPOS[backend]()
    tx = _ScriptedTx('{"worker_error": "theirs"}')
    with mock.patch.object(admin, "hive_repo", lambda: repo):
        changed = asyncio.run(admin._patch_result_cas(tx, "job-1", dict(SETS), "failed", RAW, PATCH))
    assert changed == 0
    assert not [s for s, _ in tx.statements if s.strip().upper().startswith("UPDATE")], tx.statements
    reads = [s for s, _ in tx.statements if s.strip().upper().startswith("SELECT")]
    assert reads and all(s.rstrip().endswith(repo._retry_lock_clause.strip()) for s in reads), reads


# --------------------------------------------------------------------------- #
# semantics half: a real SQLite database
# --------------------------------------------------------------------------- #


class _LockedCompareSqlite(SqliteHiveRepository):
    """The Oracle/Db2 compare strategy, run against a real database."""

    _result_cas_sql = OracleHiveRepository.__dict__.get("_result_cas_sql", "<unset>")


@pytest.fixture(params=["sql-compare", "locked-compare"])
def sqlite_repo(request):
    cls = SqliteHiveRepository if request.param == "sql-compare" else _LockedCompareSqlite
    with tempfile.TemporaryDirectory() as tmp:
        repo = cls(str(Path(tmp) / "cas.db"))
        asyncio.run(repo.init())
        yield repo


def _seed(repo, job_id: str, result):
    async def go():
        async with repo.connection() as tx:
            await repo.submit_job(tx, **cs.job_fields(job_id, "urn:agent:test", 1_800_000_000.0))
            await repo.patch_job_status(tx, job_id, sets={"status": "failed", "result": result})
            await tx.commit()

    asyncio.run(go())


def _cas(repo, job_id: str, raw, *, expected_status="failed", concurrent=None):
    async def go():
        if concurrent is not None:
            async with repo.connection() as other:
                await repo.patch_job_status(other, job_id, sets={"result": concurrent})
                await other.commit()
        async with repo.connection() as tx:
            with mock.patch.object(admin, "hive_repo", lambda: repo):
                changed = await admin._patch_result_cas(tx, job_id, dict(SETS), expected_status, raw, PATCH)
            await tx.commit()
        async with repo.connection() as tx:
            async with tx.execute("SELECT status, result FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
        return changed, row[0], row[1]

    return asyncio.run(go())


def test_the_locked_strategy_is_what_oracle_uses():
    """The second fixture param must really be the Oracle strategy, not a copy
    of the sqlite one (otherwise the semantics below prove nothing for it)."""
    assert "_result_cas_sql" in OracleHiveRepository.__dict__
    assert OracleHiveRepository._result_cas_sql is None
    assert Db2HiveRepository._result_cas_sql is None


def test_matching_payload_updates_one_row(sqlite_repo):
    _seed(sqlite_repo, "j-match", RAW)
    changed, status, result = _cas(sqlite_repo, "j-match", RAW)
    assert changed == 1
    assert status == "needs-review"
    assert json.loads(result) == {**json.loads(RAW), **PATCH}


def test_a_concurrent_payload_write_is_a_conflict(sqlite_repo):
    _seed(sqlite_repo, "j-race", RAW)
    theirs = '{"worker_error": "theirs"}'
    changed, status, result = _cas(sqlite_repo, "j-race", RAW, concurrent=theirs)
    assert changed == 0
    assert status == "failed"
    assert result == theirs


def test_a_null_expected_payload_matches_a_null_result(sqlite_repo):
    _seed(sqlite_repo, "j-null", None)
    changed, status, result = _cas(sqlite_repo, "j-null", None)
    assert changed == 1
    assert status == "needs-review"
    assert json.loads(result) == PATCH


def test_a_null_expected_payload_does_not_match_a_written_result(sqlite_repo):
    _seed(sqlite_repo, "j-null-race", None)
    changed, _status, result = _cas(sqlite_repo, "j-null-race", None, concurrent=RAW)
    assert changed == 0
    assert result == RAW


def test_the_status_guard_is_unchanged(sqlite_repo):
    _seed(sqlite_repo, "j-status", RAW)
    changed, status, result = _cas(sqlite_repo, "j-status", RAW, expected_status="dead-letter")
    assert changed == 0
    assert status == "failed"
    assert result == RAW
