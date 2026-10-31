# PLAN-FB62 — Schema document and cross-backend DDL check

## Brief
`docs/SCHEMA.md` from the DDL: tables, columns, types, indexes per backend
(sqlite, oracle, postgres, mysql, db2). A test that every column in the
sqlite DDL exists in the other backends' DDL with a compatible type. Report drift.

## Recon
- sqlite `DDL` (`hive/persistence/sqlite.py`) is the reference: 11 tables,
  all-lowercase names, types TEXT/INTEGER/REAL (+`INTEGER PRIMARY KEY
  AUTOINCREMENT` identity), `CREATE INDEX IF NOT EXISTS` set.
- postgres/mysql reuse the reference through pure translators
  (`dialect.postgres_ddl` / `mysql_ddl`): same tables/columns, `text`,
  `BIGSERIAL`/`BIGINT AUTO_INCREMENT`, `double precision`/`double`.
- db2 `DB2_DDL` is hand-written: same lowercase table names (DB2 folds
  unquoted to UPPER), `VARCHAR(n)`/`CLOB`/`INTEGER`/`DOUBLE`, `GENERATED
  ALWAYS AS IDENTITY`; plus `hive_schema_migrations`, plus
  `_DB2_ADDITIVE_COLUMNS` for the live `HIVE_*` core tables; live tables are
  `HIVE_AGENTS`/`HIVE_JOBS` (core-owned), also converged by `_apply_migrations`.
- oracle has no full DDL in code: external canonical schema + in-code
  `_TABLE_MAP` (sqlite→`hive_*` names), `_ORACLE_JOB_ADDITIONS` (19 cols),
  `_ORACLE_REQUIRED_JOB_COLUMNS`, inline `capacity_ledger`/`capacity_usage`/
  `hive_job_result_history` CREATEs, index names. Non-jobs tables are
  unverifiable from code — finding, not failure.
- Already spotted drift: db2 `jobs` lacks `site/repo/head_branch/base_branch/
  pr_number` (present in sqlite + oracle additions? verify); db2 lacks
  `capacity_ledger`/`capacity_usage` tables; db2 missing some indexes
  (hive_cache, scheduled, result-history). Confirm in test, report.

## Files
- New: `scripts/gen_schema_doc.py` (stdlib only: parses the 4 DDL strings +
  oracle fragments, emits markdown tables).
- New: `docs/SCHEMA.md` (generated, checked in; findings section hand-written
  at the end — generator preserves a `<!-- findings -->` block? Simpler:
  generator writes the reference tables; I append findings manually and the
  test only checks the generated marker + table/column presence).
- New: `test_schema_ddl_20261031.py` (stdlib + pytest only):
  1. parse sqlite DDL → reference map;
  2. pg/mysql translated DDL: same tables, same columns, family-compatible types;
  3. db2 DDL + additive map: same (union), family-compatible;
  4. oracle in-code fragments: jobs required+additions ⊆ reference-compatible;
     capacity + result-history tables exact;
  5. index-set comparison per backend (pg/mysql equal; db2/oracle drift reported);
  6. doc consistency: every reference table+column string appears in SCHEMA.md.
- No production code changes. Drift that is intended (external canonical
  schemas) becomes `xfail(strict=False)`? No — better: passing test with a
  `DRIFT` list asserted non-empty and documented (a regression would be a NEW
  drift entry, which fails). Simplest honest shape: test asserts the exact
  known-drift set; any new drift fails. Do that.

## Risks
- DDL parser is regex-based; CHECK constraints contain commas/parens —
  parse column lists by paren depth, split top-level commas only.
- `GENERATED ALWAYS AS (...)` in db2 contains commas — same handling.
- Type family map must accept oracle bare `NUMBER` for both int and real.
