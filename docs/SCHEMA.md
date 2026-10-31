# Schema

<!-- GENERATED-BEGIN: do not edit between the markers -->

_Reference: sqlite DDL (11 tables). Generated from the sources; findings are hand-written below the markers._

## sqlite (reference)

### agents (21 columns)

| column | type |
| --- | --- |
| auth_method | TEXT |
| autonomy_level | TEXT |
| capabilities | TEXT |
| cost_tier | TEXT |
| current_load | TEXT |
| host | TEXT |
| kind | TEXT |
| last_heartbeat | REAL |
| metadata | TEXT |
| model | TEXT |
| pid | INTEGER |
| plan_cap_usd | REAL |
| plan_period_used_usd | REAL |
| provider | TEXT |
| runtime | TEXT |
| session_id | TEXT |
| started_at | REAL |
| status | TEXT |
| subscription_pools | TEXT |
| urn | TEXT |
| version | TEXT |

### capacity_ledger (7 columns)

| column | type |
| --- | --- |
| in_flight | INTEGER |
| limited_until | REAL |
| service | TEXT |
| spent_usd | REAL |
| tokens_used | INTEGER |
| updated_at | REAL |
| window_start | REAL |

### capacity_usage (5 columns)

| column | type |
| --- | --- |
| cost_usd | REAL |
| dedup_key | TEXT |
| service | TEXT |
| tokens | INTEGER |
| ts | REAL |

### events (5 columns)

| column | type |
| --- | --- |
| agent_urn | TEXT |
| id | INTEGER |
| kind | TEXT |
| payload | TEXT |
| ts | REAL |

### hive_cache (10 columns)

| column | type |
| --- | --- |
| cache_key | TEXT |
| cached_at | REAL |
| cost_saved_usd | REAL |
| hit_count | INTEGER |
| last_hit_at | REAL |
| model | TEXT |
| provider | TEXT |
| result_json | TEXT |
| result_mnemos_id | TEXT |
| source_job_id | TEXT |

### job_audit_log (9 columns)

| column | type |
| --- | --- |
| actor_urn | TEXT |
| id | INTEGER |
| job_id | TEXT |
| new_claimed_by | TEXT |
| new_status | TEXT |
| old_claimed_by | TEXT |
| old_status | TEXT |
| patch | TEXT |
| ts | REAL |

### job_result_history (9 columns)

| column | type |
| --- | --- |
| action | TEXT |
| actor_urn | TEXT |
| archived_at | REAL |
| attempt | INTEGER |
| id | TEXT |
| job_id | TEXT |
| payload | TEXT |
| prior_status | TEXT |
| reason | TEXT |

### jobs (45 columns)

| column | type |
| --- | --- |
| base_branch | TEXT |
| branch_created_from | TEXT |
| claim_lease_expires_at | REAL |
| claimed_at | REAL |
| claimed_by | TEXT |
| claimed_cost_tier | TEXT |
| claimed_host_caps | TEXT |
| claimed_model | TEXT |
| claimed_provider | TEXT |
| claimed_runtime | TEXT |
| deadline | REAL |
| depends_on | TEXT |
| description | TEXT |
| eligible_hosts | TEXT |
| eligible_kinds | TEXT |
| ended_at | REAL |
| estimated_cost_usd | REAL |
| head_branch | TEXT |
| id | TEXT |
| kind | TEXT |
| last_update_at | REAL |
| max_cost_tier | TEXT |
| max_retries | INTEGER |
| mnemos_refs | TEXT |
| parent_job_id | TEXT |
| pr_number | INTEGER |
| preferred_models | TEXT |
| preferred_providers | TEXT |
| priority | INTEGER |
| project | TEXT |
| repo | TEXT |
| required_autonomy | TEXT |
| required_capabilities | TEXT |
| required_resources | TEXT |
| result | TEXT |
| result_mnemos_id | TEXT |
| retry_backoff_until | REAL |
| retry_count | INTEGER |
| site | TEXT |
| started_at | REAL |
| status | TEXT |
| submitter_urn | TEXT |
| tags | TEXT |
| tokens_in | INTEGER |
| tokens_out | INTEGER |

### messages (7 columns)

| column | type |
| --- | --- |
| from_urn | TEXT |
| id | TEXT |
| in_reply_to | TEXT |
| payload | TEXT |
| to_urn | TEXT |
| topic | TEXT |
| ts | REAL |

### scheduled_jobs (10 columns)

| column | type |
| --- | --- |
| created_at | REAL |
| created_by_urn | TEXT |
| enabled | INTEGER |
| fire_count | INTEGER |
| id | TEXT |
| interval_seconds | INTEGER |
| job_template | TEXT |
| last_fired_at | REAL |
| name | TEXT |
| next_fire_at | REAL |

### worker_kind_stats (10 columns)

| column | type |
| --- | --- |
| cancelled_count | INTEGER |
| fail_count | INTEGER |
| kind | TEXT |
| last_run | REAL |
| success_count | INTEGER |
| total_cost_usd | REAL |
| total_duration_sec | REAL |
| total_tokens_in | INTEGER |
| total_tokens_out | INTEGER |
| urn | TEXT |

## postgres (`postgres_ddl`)

### agents (21 columns)

| column | type |
| --- | --- |
| auth_method | TEXT |
| autonomy_level | TEXT |
| capabilities | TEXT |
| cost_tier | TEXT |
| current_load | TEXT |
| host | TEXT |
| kind | TEXT |
| last_heartbeat | DOUBLE |
| metadata | TEXT |
| model | TEXT |
| pid | INTEGER |
| plan_cap_usd | DOUBLE |
| plan_period_used_usd | DOUBLE |
| provider | TEXT |
| runtime | TEXT |
| session_id | TEXT |
| started_at | DOUBLE |
| status | TEXT |
| subscription_pools | TEXT |
| urn | TEXT |
| version | TEXT |

### capacity_ledger (7 columns)

| column | type |
| --- | --- |
| in_flight | INTEGER |
| limited_until | DOUBLE |
| service | TEXT |
| spent_usd | DOUBLE |
| tokens_used | INTEGER |
| updated_at | DOUBLE |
| window_start | DOUBLE |

### capacity_usage (5 columns)

| column | type |
| --- | --- |
| cost_usd | DOUBLE |
| dedup_key | TEXT |
| service | TEXT |
| tokens | INTEGER |
| ts | DOUBLE |

### events (5 columns)

| column | type |
| --- | --- |
| agent_urn | TEXT |
| id | BIGSERIAL |
| kind | TEXT |
| payload | TEXT |
| ts | DOUBLE |

### hive_cache (10 columns)

| column | type |
| --- | --- |
| cache_key | TEXT |
| cached_at | DOUBLE |
| cost_saved_usd | DOUBLE |
| hit_count | INTEGER |
| last_hit_at | DOUBLE |
| model | TEXT |
| provider | TEXT |
| result_json | TEXT |
| result_mnemos_id | TEXT |
| source_job_id | TEXT |

### job_audit_log (9 columns)

| column | type |
| --- | --- |
| actor_urn | TEXT |
| id | BIGSERIAL |
| job_id | TEXT |
| new_claimed_by | TEXT |
| new_status | TEXT |
| old_claimed_by | TEXT |
| old_status | TEXT |
| patch | TEXT |
| ts | DOUBLE |

### job_result_history (9 columns)

| column | type |
| --- | --- |
| action | TEXT |
| actor_urn | TEXT |
| archived_at | DOUBLE |
| attempt | INTEGER |
| id | TEXT |
| job_id | TEXT |
| payload | TEXT |
| prior_status | TEXT |
| reason | TEXT |

### jobs (45 columns)

| column | type |
| --- | --- |
| base_branch | TEXT |
| branch_created_from | TEXT |
| claim_lease_expires_at | DOUBLE |
| claimed_at | DOUBLE |
| claimed_by | TEXT |
| claimed_cost_tier | TEXT |
| claimed_host_caps | TEXT |
| claimed_model | TEXT |
| claimed_provider | TEXT |
| claimed_runtime | TEXT |
| deadline | DOUBLE |
| depends_on | TEXT |
| description | TEXT |
| eligible_hosts | TEXT |
| eligible_kinds | TEXT |
| ended_at | DOUBLE |
| estimated_cost_usd | DOUBLE |
| head_branch | TEXT |
| id | TEXT |
| kind | TEXT |
| last_update_at | DOUBLE |
| max_cost_tier | TEXT |
| max_retries | INTEGER |
| mnemos_refs | TEXT |
| parent_job_id | TEXT |
| pr_number | INTEGER |
| preferred_models | TEXT |
| preferred_providers | TEXT |
| priority | INTEGER |
| project | TEXT |
| repo | TEXT |
| required_autonomy | TEXT |
| required_capabilities | TEXT |
| required_resources | TEXT |
| result | TEXT |
| result_mnemos_id | TEXT |
| retry_backoff_until | DOUBLE |
| retry_count | INTEGER |
| site | TEXT |
| started_at | DOUBLE |
| status | TEXT |
| submitter_urn | TEXT |
| tags | TEXT |
| tokens_in | INTEGER |
| tokens_out | INTEGER |

### messages (7 columns)

| column | type |
| --- | --- |
| from_urn | TEXT |
| id | TEXT |
| in_reply_to | TEXT |
| payload | TEXT |
| to_urn | TEXT |
| topic | TEXT |
| ts | DOUBLE |

### scheduled_jobs (10 columns)

| column | type |
| --- | --- |
| created_at | DOUBLE |
| created_by_urn | TEXT |
| enabled | INTEGER |
| fire_count | INTEGER |
| id | TEXT |
| interval_seconds | INTEGER |
| job_template | TEXT |
| last_fired_at | DOUBLE |
| name | TEXT |
| next_fire_at | DOUBLE |

### worker_kind_stats (10 columns)

| column | type |
| --- | --- |
| cancelled_count | INTEGER |
| fail_count | INTEGER |
| kind | TEXT |
| last_run | DOUBLE |
| success_count | INTEGER |
| total_cost_usd | DOUBLE |
| total_duration_sec | DOUBLE |
| total_tokens_in | INTEGER |
| total_tokens_out | INTEGER |
| urn | TEXT |

## mysql (`mysql_ddl`)

### agents (21 columns)

| column | type |
| --- | --- |
| auth_method | TEXT |
| autonomy_level | TEXT |
| capabilities | TEXT |
| cost_tier | TEXT |
| current_load | TEXT |
| host | TEXT |
| kind | TEXT |
| last_heartbeat | DOUBLE |
| metadata | TEXT |
| model | TEXT |
| pid | INTEGER |
| plan_cap_usd | DOUBLE |
| plan_period_used_usd | DOUBLE |
| provider | TEXT |
| runtime | TEXT |
| session_id | TEXT |
| started_at | DOUBLE |
| status | TEXT |
| subscription_pools | TEXT |
| urn | TEXT |
| version | TEXT |

### capacity_ledger (7 columns)

| column | type |
| --- | --- |
| in_flight | INTEGER |
| limited_until | DOUBLE |
| service | TEXT |
| spent_usd | DOUBLE |
| tokens_used | INTEGER |
| updated_at | DOUBLE |
| window_start | DOUBLE |

### capacity_usage (5 columns)

| column | type |
| --- | --- |
| cost_usd | DOUBLE |
| dedup_key | TEXT |
| service | TEXT |
| tokens | INTEGER |
| ts | DOUBLE |

### events (5 columns)

| column | type |
| --- | --- |
| agent_urn | TEXT |
| id | BIGINT |
| kind | TEXT |
| payload | TEXT |
| ts | DOUBLE |

### hive_cache (10 columns)

| column | type |
| --- | --- |
| cache_key | TEXT |
| cached_at | DOUBLE |
| cost_saved_usd | DOUBLE |
| hit_count | INTEGER |
| last_hit_at | DOUBLE |
| model | TEXT |
| provider | TEXT |
| result_json | TEXT |
| result_mnemos_id | TEXT |
| source_job_id | TEXT |

### job_audit_log (9 columns)

| column | type |
| --- | --- |
| actor_urn | TEXT |
| id | BIGINT |
| job_id | TEXT |
| new_claimed_by | TEXT |
| new_status | TEXT |
| old_claimed_by | TEXT |
| old_status | TEXT |
| patch | TEXT |
| ts | DOUBLE |

### job_result_history (9 columns)

| column | type |
| --- | --- |
| action | TEXT |
| actor_urn | TEXT |
| archived_at | DOUBLE |
| attempt | INTEGER |
| id | TEXT |
| job_id | TEXT |
| payload | TEXT |
| prior_status | TEXT |
| reason | TEXT |

### jobs (45 columns)

| column | type |
| --- | --- |
| base_branch | TEXT |
| branch_created_from | TEXT |
| claim_lease_expires_at | DOUBLE |
| claimed_at | DOUBLE |
| claimed_by | TEXT |
| claimed_cost_tier | TEXT |
| claimed_host_caps | TEXT |
| claimed_model | TEXT |
| claimed_provider | TEXT |
| claimed_runtime | TEXT |
| deadline | DOUBLE |
| depends_on | TEXT |
| description | TEXT |
| eligible_hosts | TEXT |
| eligible_kinds | TEXT |
| ended_at | DOUBLE |
| estimated_cost_usd | DOUBLE |
| head_branch | TEXT |
| id | TEXT |
| kind | TEXT |
| last_update_at | DOUBLE |
| max_cost_tier | TEXT |
| max_retries | INTEGER |
| mnemos_refs | TEXT |
| parent_job_id | TEXT |
| pr_number | INTEGER |
| preferred_models | TEXT |
| preferred_providers | TEXT |
| priority | INTEGER |
| project | TEXT |
| repo | TEXT |
| required_autonomy | TEXT |
| required_capabilities | TEXT |
| required_resources | TEXT |
| result | TEXT |
| result_mnemos_id | TEXT |
| retry_backoff_until | DOUBLE |
| retry_count | INTEGER |
| site | TEXT |
| started_at | DOUBLE |
| status | TEXT |
| submitter_urn | TEXT |
| tags | TEXT |
| tokens_in | INTEGER |
| tokens_out | INTEGER |

### messages (7 columns)

| column | type |
| --- | --- |
| from_urn | TEXT |
| id | TEXT |
| in_reply_to | TEXT |
| payload | TEXT |
| to_urn | TEXT |
| topic | TEXT |
| ts | DOUBLE |

### scheduled_jobs (10 columns)

| column | type |
| --- | --- |
| created_at | DOUBLE |
| created_by_urn | TEXT |
| enabled | INTEGER |
| fire_count | INTEGER |
| id | TEXT |
| interval_seconds | INTEGER |
| job_template | TEXT |
| last_fired_at | DOUBLE |
| name | TEXT |
| next_fire_at | DOUBLE |

### worker_kind_stats (10 columns)

| column | type |
| --- | --- |
| cancelled_count | INTEGER |
| fail_count | INTEGER |
| kind | TEXT |
| last_run | DOUBLE |
| success_count | INTEGER |
| total_cost_usd | DOUBLE |
| total_duration_sec | DOUBLE |
| total_tokens_in | INTEGER |
| total_tokens_out | INTEGER |
| urn | TEXT |

## db2 (`DB2_DDL`)

### agents (21 columns)

| column | type |
| --- | --- |
| auth_method | VARCHAR |
| autonomy_level | VARCHAR |
| capabilities | CLOB |
| cost_tier | VARCHAR |
| current_load | CLOB |
| host | VARCHAR |
| kind | VARCHAR |
| last_heartbeat | DOUBLE |
| metadata | CLOB |
| model | VARCHAR |
| pid | INTEGER |
| plan_cap_usd | DOUBLE |
| plan_period_used_usd | DOUBLE |
| provider | VARCHAR |
| runtime | VARCHAR |
| session_id | VARCHAR |
| started_at | DOUBLE |
| status | VARCHAR |
| subscription_pools | CLOB |
| urn | VARCHAR |
| version | VARCHAR |

### events (5 columns)

| column | type |
| --- | --- |
| agent_urn | VARCHAR |
| id | BIGINT |
| kind | VARCHAR |
| payload | CLOB |
| ts | DOUBLE |

### hive_cache (10 columns)

| column | type |
| --- | --- |
| cache_key | VARCHAR |
| cached_at | DOUBLE |
| cost_saved_usd | DOUBLE |
| hit_count | INTEGER |
| last_hit_at | DOUBLE |
| model | VARCHAR |
| provider | VARCHAR |
| result_json | CLOB |
| result_mnemos_id | VARCHAR |
| source_job_id | VARCHAR |

### hive_schema_migrations (2 columns)

| column | type |
| --- | --- |
| applied_at | DOUBLE |
| version | INTEGER |

### job_audit_log (9 columns)

| column | type |
| --- | --- |
| actor_urn | VARCHAR |
| id | BIGINT |
| job_id | VARCHAR |
| new_claimed_by | VARCHAR |
| new_status | VARCHAR |
| old_claimed_by | VARCHAR |
| old_status | VARCHAR |
| patch | CLOB |
| ts | DOUBLE |

### job_result_history (9 columns)

| column | type |
| --- | --- |
| action | VARCHAR |
| actor_urn | VARCHAR |
| archived_at | DOUBLE |
| attempt | INTEGER |
| id | VARCHAR |
| job_id | VARCHAR |
| payload | CLOB |
| prior_status | VARCHAR |
| reason | VARCHAR |

### jobs (43 columns)

| column | type |
| --- | --- |
| branch_created_from | VARCHAR |
| claim_lease_expires_at | DOUBLE |
| claimed_at | DOUBLE |
| claimed_by | VARCHAR |
| claimed_cost_tier | VARCHAR |
| claimed_host_caps | CLOB |
| claimed_model | VARCHAR |
| claimed_provider | VARCHAR |
| claimed_runtime | VARCHAR |
| deadline | DOUBLE |
| decline_count | INTEGER |
| dedup_active_hash | VARCHAR |
| dedup_hash | VARCHAR |
| depends_on | CLOB |
| description | CLOB |
| eligible_hosts | CLOB |
| eligible_kinds | CLOB |
| ended_at | DOUBLE |
| estimated_cost_usd | DOUBLE |
| id | VARCHAR |
| kind | VARCHAR |
| last_update_at | DOUBLE |
| max_cost_tier | VARCHAR |
| max_retries | INTEGER |
| mnemos_refs | CLOB |
| parent_job_id | VARCHAR |
| preferred_models | CLOB |
| preferred_providers | CLOB |
| priority | INTEGER |
| project | VARCHAR |
| required_autonomy | VARCHAR |
| required_capabilities | CLOB |
| required_resources | CLOB |
| result | CLOB |
| result_mnemos_id | VARCHAR |
| retry_backoff_until | DOUBLE |
| retry_count | INTEGER |
| started_at | DOUBLE |
| status | VARCHAR |
| submitter_urn | VARCHAR |
| tags | CLOB |
| tokens_in | INTEGER |
| tokens_out | INTEGER |

### messages (7 columns)

| column | type |
| --- | --- |
| from_urn | VARCHAR |
| id | VARCHAR |
| in_reply_to | VARCHAR |
| payload | CLOB |
| to_urn | VARCHAR |
| topic | VARCHAR |
| ts | DOUBLE |

### scheduled_jobs (10 columns)

| column | type |
| --- | --- |
| created_at | DOUBLE |
| created_by_urn | VARCHAR |
| enabled | INTEGER |
| fire_count | INTEGER |
| id | VARCHAR |
| interval_seconds | INTEGER |
| job_template | CLOB |
| last_fired_at | DOUBLE |
| name | VARCHAR |
| next_fire_at | DOUBLE |

### worker_kind_stats (10 columns)

| column | type |
| --- | --- |
| cancelled_count | INTEGER |
| fail_count | INTEGER |
| kind | VARCHAR |
| last_run | DOUBLE |
| success_count | INTEGER |
| total_cost_usd | DOUBLE |
| total_duration_sec | DOUBLE |
| total_tokens_in | INTEGER |
| total_tokens_out | INTEGER |
| urn | VARCHAR |

## oracle (in-code fragments only)

No full DDL in code; the canonical schema is external. In-code evidence:

- `_TABLE_MAP`: agents → hive_agents, events → hive_events, hive_cache → hive_cache, job_audit_log → hive_job_audit_log, job_result_history → hive_job_result_history, jobs → hive_jobs, messages → hive_messages, scheduled_jobs → hive_scheduled_jobs, worker_kind_stats → hive_worker_kind_stats
- `_ORACLE_JOB_ADDITIONS` (18 columns), `_ORACLE_REQUIRED_JOB_COLUMNS` (41 columns)
- inline `capacity_ledger` / `capacity_usage` / `hive_job_result_history` CREATEs

<!-- GENERATED-END -->


## Known drift and findings (hand-written, 2026-10-31)

- FB62-1: db2 has no `capacity_ledger` / `capacity_usage` tables and its
  `jobs` (and the `HIVE_JOBS` additive map) lack `site`, `repo`,
  `head_branch`, `base_branch`, `pr_number`. The bus writes those columns, so
  db2 cannot serve submit or claim paths that touch them.
- FB62-2: oracle has no in-code evidence for `site`, `repo`, `head_branch`,
  `pr_number` on `HIVE_JOBS` (neither in `_ORACLE_REQUIRED_JOB_COLUMNS` nor
  `_ORACLE_JOB_ADDITIONS`) and no in-code DDL at all for the non-jobs tables
  (canonical schema is external). Cross-backend parity for those is
  unverifiable from this repo.
- FB62-3: the sqlite DDL itself is behind its own migrator — `jobs`
  `decline_count` and `dedup_hash` (plus the partial unique dedup index) exist
  only as `_ensure_column` / `CREATE UNIQUE INDEX` calls in `init`, not in
  `DDL`. Fresh DBs still converge (the migrator runs on every start), but the
  DDL is not the whole fresh-DB shape its header comment claims.
- FB62-4: index parity — the sqlite DDL block declares 19 indexes but a
  live sqlite DB carries 32 (13 more from the `init` migrator: 5 agent-attribute
  and 7 job-attribute indexes plus the partial unique `jobs_active_dedup_uq`).
  postgres/mysql translate only the DDL, so they match the 19 exactly and miss
  all 13 live-only ones; db2 misses those 13 plus 5 DDL ones (`hive_cache`,
  `scheduled_jobs`, `capacity_usage`, `job_result_history`, `worker_kind_stats`)
  and keys its dedup unique index on the generated `dedup_active_hash` column
  instead of the partial index. Missing indexes cost queries, not correctness.
  Oracle index names differ by convention (`IDX_HIVE_JOBS_*`); only the
  claim-lease and active-dedup indexes are asserted in code (`_verify_schema`).
