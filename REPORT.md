# Fix: doctor auto-triage dedupe + dead-letter reason persistence (2026-10-27)

Two production defects fixed on branch `fix/doctor-triage-dedupe-20261027`
(BASE `9d4c3ef2`):

## Defect A — triage churn

`scan_for_failure_clusters()` only looked at queued/running triage jobs and
`AUTO_TRIAGE_COOLDOWN` lived in an in-process dict, so a finished triage (or a
doctor restart) re-created `triage:<base_kind>` for the same failures — 61
duplicate rows in one night, triage rows recreated within minutes of a purge
with zero new failures.

### The fix

The triage job row on the bus is now the durable state. `_recent_auto_triage`
(the in-memory dict) is a cache only; every decision is derived from the
newest existing `triage:<base_kind>` job (any status), so a restart or a purge
race can no longer reset the cooldown.

`should_auto_triage(base_kind, failed_jobs)` implements the dedupe predicate:

1. **active** — a `triage:<base_kind>` in queued/offered/claimed/running
   exists → block.
2. **supersede** — the newest job of the same base_kind in ANY status is
   done/needs-review and newer (started_at) than the newest failure → the
   failures are old news; skip.
3. **no-new-fails** — no failed job ended after the newest triage was created
   (`failure.ended_at > triage.created_at` is the ONLY condition that makes a
   new triage legitimate) → skip; otherwise a new triage is legitimate.

The `AUTO_TRIAGE_COOLDOWN` window (env-overridable, default 1h, unchanged)
still suppresses re-triage of GENUINE NEW failures while the newest triage is
young; its start time is the newest triage's `created_at` (durable). A failure
that ended BEFORE the newest triage was created is blocked by the
no-new-failures rule regardless of the cooldown.

A self-terminating (no_action) triage is recorded as evaluated via
`_finalize_triage_cache` and participates in dedupe exactly like a real fix.

`CLUSTER_THRESHOLD`, `CLUSTER_WINDOW_SEC`, `AUTO_TRIAGE_COOLDOWN` remain
env-overridable with their original defaults (1 / 21600 / 3600).

## Defect B — dead-letters with no visible reason

A job submitted with `base_branch=master` against a repo whose default branch
is `main` is claimed, probed, and released with reason `invalid_base_branch`;
the bus counted it as a job-level decline and dead-lettered it. The row then
showed `status=dead-letter`, `claimed_by=null`, `retry_count=3` and no reason
anywhere in `GET /v1/jobs`.

### The fix

Three additive columns on `jobs`:

| column | type | meaning |
|---|---|---|
| `last_decline_reason` | TEXT | the `worker_error` string from the last release (any reason, not just counted prefixes) |
| `last_decline_at` | REAL | epoch time of the last release |
| `last_declined_by` | TEXT | the agent URN that released the job |

- Persisted on **every** release in the PATCH router (not just counted
  declines).
- Re-set on the dead-letter transition (belt-and-braces).
- Exposed in `GET /v1/jobs` and `GET /v1/jobs/{id}` (NULL on older rows).
- `DECLINE_REASON_PREFIXES` extended with `invalid_base_branch` /
  `invalid_head_branch` so the counted set matches the real fleet vocabulary
  (a branch that doesn't exist on origin can never be cloned by ANY host —
  the job is doomed no matter who claims it). Reason strings themselves are
  unchanged; existing writers keep working.

### Migration (deploy order)

The migration is purely additive (`ALTER TABLE jobs ADD COLUMN` × 3, no
rebuild, no backfill). It runs automatically in `SqliteHiveRepository.init()`
on the next bus start — no manual SQL needed on an existing database.

**Deploy order (on PYTHIA):**
1. Run the schema migration (automatic on bus restart — the `repo.init()`
   call in the bus lifespan adds the three columns to the live DB).
2. Restart the bus (`hive.bus.app`) so the new columns are created and the
   router code is live.
3. Restart the doctor (`zeroclaw_doctor.py`) so the dedupe gate is live.

**Rollback:** none required — the columns are additive; the old code simply
ignores them.

## Tests

- `test_doctor_triage_dedupe.py` — 14 tests + 4 subtests: done triage blocks,
  active triage blocks, failure-after-triage allows exactly one new triage,
  restart sees durable cooldown, supersede, threshold/window, scan integration,
  submit seeds cache, finalize records outcome.
- `test_decline_reason.py` — 6 tests: release records reason + GET returns it,
  reason survives dead-letter transition, plain release records actor+time,
  older row deserialises (NULLs), non-counted reason still recorded,
  migration adds columns to a pre-migration DB.

## Changed files

- `hive/workers/doctor.py` — durable dedupe gate, restart-proof cooldown,
  supersede rule, self-terminating triage cache.
- `hive/bus/routers/jobs.py` — persist reason on every release, expose in
  job repr, dead-letter transition carries reason.
- `hive/bus/domain/jobs.py` — `DECLINE_REASON_PREFIXES` extended.
- `hive/persistence/sqlite.py` — DDL + additive migration + list_jobs SELECT.
- `test_doctor_triage_dedupe.py` — new.
- `test_decline_reason.py` — new.
