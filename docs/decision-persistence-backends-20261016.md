# Decision: persistence backends vs the HiveMindRepository ABC

**Date:** 2026-10-16  **Status:** accepted  **Phase:** refactor-plan Phase 3

## Context

The bus reaches storage only through `hive.persistence.base.HiveMindRepository`
(the ABC, EPIMONE tx-taking shape: every method takes the caller's `tx` first).
Backends: `sqlite.py` (reference impl), `oracle.py` and `db2.py` (subclass the
sqlite repo, overriding dialect via `_retry_lock_clause`, `_translate_sql`,
`_rewrite_*`, `_translate_upsert`). The factory
(`hive.persistence.factory.get_hive_repository`) resolves the backend from
`HIVE_BACKEND`/DSN env only — the bus never branches on backend.

## Which backend is canonical

- **Live = Oracle** (`HIVE_BACKEND=oracle`, `ORACLE_DSN=…ORCLPDB1` on PYTHIA).
  `HIVE_JOBS`/`HIVE_AGENTS` live there; `HIVE_JOBS` carries MNEMOS's columns
  (`DECLINE_COUNT`, `SITE`, `REPO`, `HEAD_BRANCH`, `PR_NUMBER`, the
  `CK_HIVE_JOBS_STATUS` check, the function-based `HIVE_JOBS_ACTIVE_DEDUP_UQ`).
- **Sqlite = the dev/test reference** (`AGENT_BUS_DB`), used by the suite.
- **Db2 = experimental** (its own DDL + migration versions).

`SqliteHiveRepository` is the reference implementation; Oracle/Db2 override only
dialect. A method added to the ABC + sqlite is therefore live-usable via the
Oracle adapter's translations.

## Consequences / invariants

1. **No raw SQL in the bus.** Routers, the reaper, and the lifespan call ABC
   methods only (verified: 0 `execute` calls in `hive/bus/app.py`).
2. **DDL belongs to the backend.** `SqliteHiveRepository.init()` owns the sqlite
   schema + additive migrations + index re-creates; `OracleHiveRepository.init()`
   owns `_apply_migrations`/`_verify_schema`; the lifespan just calls
   `await repo.init()` with no per-backend branching.
3. **`ON CONFLICT` upserts are Oracle-safe** — `oracle._translate_upsert`
   rewrites the sqlite `INSERT … ON CONFLICT(…) DO UPDATE` (cache + worker-kind
   stats) to `MERGE`. No separate Oracle `cache_store` is needed.
4. **Non-fatal convergence.** Oracle DDL that the live schema forbids (e.g.
   widening an indexed column) is logged + skipped, never fatal — a schema
   hiccup must not down the queue (the pre-refactor inline block was non-fatal
   by design; that behaviour is preserved in `OracleHiveRepository.init`).

## Dead paths removed (Phase 1d)

The legacy `agent_bus`-side raw-SQL→Oracle shim (`BackendCursor`,
`OracleConnection`, `_translate_sql`, `_rewrite_*`, `_TABLE_MAP`,
`_parse_oracle_url`) was unreachable (superseded by `oracle.py`) and deleted.

## Known gaps (tracked, not blocking)

- **`HIVE_JOBS.STATUS` is `VARCHAR2(16)` on the live Oracle**, and widening is
  blocked by `HIVE_JOBS_ACTIVE_DEDUP_UQ` (a function-based index on `STATUS`) →
  `ORA-30556`. `needs-review`(12)/`dead-letter`(11) fit; `failed_completion`(17)
  does not. The bus's own completion path writes `needs-review`, so this is
  latent; enabling `failed_completion` needs a maintenance window that drops +
  recreates the dedup index around the `MODIFY`.
- **sqlite schema has no `usage_ledger` table**, so the ledger write is skipped
  on a fresh sqlite DB (Oracle/MNEMOS has it). Parity gap; the bus logs and
  continues.
