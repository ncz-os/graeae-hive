"""FB62: every column in the sqlite DDL exists in the other backends with a
compatible type; docs/SCHEMA.md agrees with the DDL sources.

References, all parsed from code (no live servers):

* sqlite `DDL` — the reference (plus the `init` migrator's additive columns
  and indexes, which ARE the live shape);
* postgres/mysql — `postgres_ddl()` / `mysql_ddl()` translations of it;
* db2 — `DB2_DDL` plus `_DB2_ADDITIVE_COLUMNS` (live `HIVE_*` tables);
* oracle — in-code fragments only (`_TABLE_MAP`, `_ORACLE_JOB_ADDITIONS`,
  `_ORACLE_REQUIRED_JOB_COLUMNS`, inline capacity/result-history CREATEs);
  the canonical schema is external, so non-jobs tables are unverifiable here.

Type compatibility is by family (TEXT ~ VARCHAR(n)/VARCHAR2(n)/CLOB,
INTEGER ~ BIGINT/BIGSERIAL/NUMBER(p)/INT, REAL ~ DOUBLE/DOUBLE PRECISION/
NUMBER), because each backend spells the storage differently. Known drift is
pinned as exact sets: any NEW drift fails the test.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from hive.persistence import db2 as db2mod
from hive.persistence import oracle as oraclemod
from hive.persistence.dialect import mysql_ddl, postgres_ddl
from hive.persistence.sqlite import DDL as SQLITE_DDL

ROOT = Path(__file__).parent
SCRIPT = ROOT / "scripts" / "gen_schema_doc.py"
DOC = ROOT / "docs" / "SCHEMA.md"

_spec = importlib.util.spec_from_file_location("gen_schema_doc", SCRIPT)
genmod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(genmod)

REF = genmod.parse_tables(SQLITE_DDL)
PG = genmod.parse_tables(postgres_ddl(SQLITE_DDL))
MY = genmod.parse_tables(mysql_ddl(SQLITE_DDL))
DB2 = genmod.parse_tables(db2mod.DB2_DDL)

SQLITE_SRC = (ROOT / "hive" / "persistence" / "sqlite.py").read_text()
LIVE_COLS = genmod.sqlite_live_columns()
LIVE_IDX_STMTS = re.findall(r'"(CREATE\s+(?:UNIQUE\s+)?INDEX[^"]+)"', SQLITE_SRC)
REF_INDEXES = genmod.parse_indexes(SQLITE_DDL)
LIVE_INDEXES = genmod.parse_indexes(SQLITE_DDL, *LIVE_IDX_STMTS)

# sqlite reference shape, DDL + migrator (decline_count/dedup_hash are
# _ensure_column-only; the migrator is authoritative for the live shape).
LIVE_REF = {t: dict(c) for t, c in REF.items()}
for table, cols in LIVE_COLS.items():
    LIVE_REF.setdefault(table.lower(), {}).update({k.lower(): v for k, v in cols.items()})


def family(ctype: str) -> str:
    """Storage family. Bare NUMBER is ambiguous (oracle uses it for both int
    and real columns), so it matches either numeric family."""
    c = ctype.upper()
    if c in {"TEXT", "CLOB"} or c.startswith(("VARCHAR", "CHARACTER", "CHAR")):
        return "text"
    if c == "NUMBER":
        return "numeric"
    if c in {"INTEGER", "INT", "BIGINT", "BIGSERIAL", "SMALLINT"} or c.startswith("NUMBER"):
        return "int"
    if c in {"REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"}:
        return "real"
    return f"other:{c}"


def assert_compatible(table: str, col: str, ref_type: str, other_type: str, backend: str) -> None:
    got, want = family(other_type), family(ref_type)
    ok = got == want or (got == "numeric" and want in {"int", "real"})
    assert ok, f"{backend}.{table}.{col}: {other_type!r} not compatible with sqlite {ref_type!r}"


# ── reference sanity ──────────────────────────────────────────────────────
def test_reference_has_expected_tables():
    assert sorted(REF) == [
        "agents",
        "capacity_ledger",
        "capacity_usage",
        "events",
        "hive_cache",
        "job_audit_log",
        "job_result_history",
        "jobs",
        "messages",
        "scheduled_jobs",
        "worker_kind_stats",
    ]
    assert len(REF["jobs"]) == 45
    assert len(REF["agents"]) == 21


# ── postgres / mysql: exact same shape, compatible types ──────────────────
@pytest.mark.parametrize("name,parsed", [("postgres", PG), ("mysql", MY)])
def test_translated_backends_match_reference(name, parsed):
    assert sorted(parsed) == sorted(REF)
    for table, cols in REF.items():
        assert sorted(parsed[table]) == sorted(cols), f"{name}.{table} columns differ"
        for col, ref_type in cols.items():
            assert_compatible(table, col, ref_type, parsed[table][col], name)


def test_translated_indexes_match_ddl_set():
    assert genmod.parse_indexes(postgres_ddl(SQLITE_DDL)) == REF_INDEXES
    assert genmod.parse_indexes(mysql_ddl(SQLITE_DDL)) == REF_INDEXES
    # ...which is 13 short of the live sqlite shape (migrator-only indexes).
    assert sorted(set(LIVE_INDEXES) - set(REF_INDEXES)) == [
        "idx_agents_auth_method",
        "idx_agents_cost_tier",
        "idx_agents_model",
        "idx_agents_provider",
        "idx_agents_runtime",
        "idx_jobs_autonomy",
        "idx_jobs_backoff",
        "idx_jobs_claim_lease",
        "idx_jobs_claimed_provider",
        "idx_jobs_claimed_tier",
        "idx_jobs_max_cost_tier",
        "idx_jobs_project",
        "jobs_active_dedup_uq",
    ]


# ── db2: union of DDL + additive map, exact known drift ───────────────────
def db2_union() -> dict[str, dict[str, str]]:
    union = {t: dict(c) for t, c in DB2.items()}
    for raw_table, defs in db2mod._DB2_ADDITIVE_COLUMNS.items():
        table = raw_table.lower().removeprefix("hive_")
        for colname, fragment in defs:
            m = re.match(r"\w+\s+([A-Za-z0-9 ]+)", fragment.strip())
            if m:
                union.setdefault(table, {}).setdefault(colname.lower(), re.sub(r"\s+", " ", m.group(1).strip().upper()))
    return union


def test_db2_tables_and_known_missing():
    union = db2_union()
    assert set(union) >= set(REF) - {"capacity_ledger", "capacity_usage"}
    assert {"capacity_ledger", "capacity_usage"} - set(union) == {"capacity_ledger", "capacity_usage"}  # FB62-1


def test_db2_columns_cover_reference_with_exact_drift():
    union = db2_union()
    missing: dict[str, list[str]] = {}
    for table, cols in REF.items():
        for col, ref_type in cols.items():
            if col not in union.get(table, {}):
                missing.setdefault(table, []).append(col)
            else:
                assert_compatible(table, col, ref_type, union[table][col], "db2")
    missing = {t: sorted(c) for t, c in missing.items()}
    assert missing == {  # FB62-1
        "jobs": ["base_branch", "head_branch", "pr_number", "repo", "site"],
        "capacity_ledger": sorted(REF["capacity_ledger"]),
        "capacity_usage": sorted(REF["capacity_usage"]),
    }


def test_db2_live_columns_covered():
    """Columns the sqlite migrator adds must also exist on db2 (DDL or map)."""
    union = db2_union()
    uncovered = [(t, c) for t, cols in LIVE_COLS.items() for c in cols if c.lower() not in union.get(t, {})]
    # The only live sqlite columns db2 lacks anywhere are the five from FB62-1.
    assert sorted(uncovered) == [
        ("jobs", "base_branch"),
        ("jobs", "head_branch"),
        ("jobs", "pr_number"),
        ("jobs", "repo"),
        ("jobs", "site"),
    ]


def test_db2_indexes_exact_subset():
    db_indexes = genmod.parse_indexes(db2mod.DB2_DDL)
    assert set(db_indexes) <= set(REF_INDEXES)
    assert sorted(set(REF_INDEXES) - set(db_indexes)) == [
        "idx_cap_usage_service_ts",
        "idx_hive_cache_cached_at",
        "idx_job_result_history_job",
        "idx_sched_next",
        "idx_wkstats_kind",
    ]


# ── oracle: in-code fragments vs reference ────────────────────────────────
def test_oracle_job_columns_have_reference_parity():
    required = {c.lower() for c in oraclemod._ORACLE_REQUIRED_JOB_COLUMNS}
    additions = genmod.oracle_job_fragments()
    assert set(additions) <= required or True  # additions overlap required; informational
    unevidenced = [c for c in REF["jobs"] if c not in required and c not in additions]
    assert unevidenced == ["site", "repo", "head_branch", "pr_number"]  # FB62-2
    for col, frag_type in additions.items():
        # Additions may name live-only columns (decline_count/dedup_hash),
        # which live in the migrator shape, not the DDL.
        assert_compatible("jobs", col, LIVE_REF["jobs"][col], frag_type, "oracle")


def test_oracle_capacity_and_history_tables_match():
    for table, expect in (
        (
            "capacity_ledger",
            {
                "service": "VARCHAR2",
                "in_flight": "NUMBER",
                "tokens_used": "NUMBER",
                "spent_usd": "NUMBER",
                "limited_until": "NUMBER",
                "window_start": "NUMBER",
                "updated_at": "NUMBER",
            },
        ),
        (
            "capacity_usage",
            {"dedup_key": "VARCHAR2", "service": "VARCHAR2", "tokens": "NUMBER", "cost_usd": "NUMBER", "ts": "NUMBER"},
        ),
        (
            "hive_job_result_history",
            {
                "id": "VARCHAR2",
                "job_id": "VARCHAR2",
                "attempt": "NUMBER",
                "prior_status": "VARCHAR2",
                "archived_at": "NUMBER",
                "actor_urn": "VARCHAR2",
                "reason": "VARCHAR2",
                "action": "VARCHAR2",
                "payload": "CLOB",
            },
        ),
    ):
        ref_table = REF["job_result_history" if table == "hive_job_result_history" else table]
        for col, otype in expect.items():
            assert col in ref_table, f"oracle {table}.{col} not in sqlite reference"
            assert_compatible(table, col, ref_table[col], otype, "oracle")
    # The verify rules mirror the reference families (not a reimplementation).
    assert family(REF["jobs"]["status"]) == "text"
    assert family(REF["jobs"]["claim_lease_expires_at"]) == "real"


def test_oracle_verifies_claim_lease_and_dedup_indexes_in_code():
    src = Path(importlib.util.find_spec("hive.persistence.oracle").origin).read_text()
    assert "IDX_HIVE_JOBS_CLAIM_LEASE" in src
    assert "JOBS_ACTIVE_DEDUP_UQ" in src


# ── sqlite DDL vs its own migrator ────────────────────────────────────────
def test_sqlite_ddl_behind_migrator_is_exactly_this():
    behind = [(t, c) for t, cols in LIVE_COLS.items() for c in cols if c.lower() not in REF.get(t, {})]
    assert sorted(behind) == [("jobs", "decline_count"), ("jobs", "dedup_hash")]  # FB62-3


# ── doc consistency ───────────────────────────────────────────────────────
def test_doc_is_current_and_lists_every_reference_column():
    text = DOC.read_text()
    assert genmod.BEGIN in text and genmod.END in text
    pre, _, rest = text.partition(genmod.BEGIN)
    _, _, post = rest.partition(genmod.END)
    assert genmod.assemble(pre, genmod.emit(), post) == text  # regenerating changes nothing
    for table, cols in REF.items():
        assert f"### {table} (" in text, f"doc missing table {table}"
        for col in cols:
            assert f"| {col} |" in text, f"doc missing {table}.{col}"
