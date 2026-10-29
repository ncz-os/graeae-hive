# Idempotency audit — doctor and worker actions

Operator rule: **an action must be idempotent.** A second identical run must not
double-dispatch, double-purge, double-restart into a loop, or spend a retry the
first run did not spend.

Baseline: `deploy/integrate-20261017` @ `90a7875` (lane branch
`freebuff/f1-20261029`). Every verdict below names the code that decides it; no
verdict is inferred from a function's name.

Reading the table:

- **identity key** — what makes two runs "the same intent".
- **second identical run** — what actually happens, in the code as it is.
- **verdict** — `idempotent`, `unsafe` (fixed on this branch), or `unknown`
  (with the reason it could not be established here).

| action | identity key | second identical run | evidence | verdict |
|---|---|---|---|---|
| `action_resubmit_jobs` — requeue rows with budget left | the failed row ids of `base_kind` that still have budget | `POST /v1/admin/jobs/requeue` skips rows that are no longer failed or have no budget; the summary reports the ids the route confirmed | `hive/workers/doctor.py:1365` (`retry_budget_exhausted`, `target_ids`); `hive/bus/routers/admin.py:89`; `test_doctor_retry_budget.py::test_bus_requeue_refuses_no_budget_without_override` | idempotent |
| `action_resubmit_jobs` — budget-spent row as a NEW job | `idempotency_key=doctor-resubmit-<original id>`, `parent_job_id`, and the one-generation guard | the bus dedups the submit on `idempotency_key` (`queue_logic.dedup_key` + `find_active_dedup_job`); the original is PATCHed `resubmitted_as`, and `_already_a_replacement` refuses to re-dispatch a row that has `parent_job_id`/`resubmitted_from`/`resubmitted_as` | `hive/workers/doctor.py:1365` (`_already_a_replacement`, payload build); `hive/bus/routers/jobs.py:845-855` | idempotent |
| `action_cancel_jobs` — purge failed rows | scoped approval digest over `sorted(purge_ids)` | the grant is single-use (`consume_grant`), so a second run is refused without a fresh operator grant; with a fresh grant the purge reports `removed=0` because the ids are gone | `doctor_gates.ApprovalGate.require/consume_grant`; `doctor_gates.purge_result`; `doctor.py:1661` | idempotent |
| `_release_job` / `action_release_to_queue` | job id + target `eligible_kinds` | two `PATCH {status: queued}` writes on a row that is already queued. The bus's release bookkeeping is keyed on `result.worker_error`, which this path does not send, so no release reason, no backoff and no `retry_count` increment | `doctor.py:1798`; `hive/bus/routers/jobs.py:1500-1519` (block runs only with a `result`); `test_doctor_retry_budget.py::test_triage_release_of_max_retries_zero_job_does_not_requeue` | idempotent |
| `action_restart_service` | (host, service) + scoped approval digest | a second SSH `systemctl restart` runs again. The end state is identical and the run is bounded by the single-use grant, the durable SSH rate cap and the per-host cooldown | `doctor.py:1292`; `_ssh_rate_ok`/`host_in_cooldown`; `test_doctor_sop_executor.py::test_cixmini_without_a_password_is_refused_before_any_ssh` | idempotent (side effect repeats; budget-capped) |
| `handle_protection_violation` | original job id: durable retry marker + `idempotency_key=protection-violation-retry:<id>` | the purge finds nothing (`removed=0`) and falls through to the cancel; the re-dispatch is blocked by the durable marker and, behind it, by the bus dedup on the stable key | `doctor.py:2846-2849` (`_violation_retry_idempotency_key`), `doctor.py:2920` | idempotent |
| `_park_for_review` | `loop_park:<job id>` durable marker (30 d TTL) | returns immediately on the marker; if the first reclassify landed, the row is already `needs-review`, and reclassifying to the same status is a same-status write | `doctor.py:1926` | idempotent |
| `remediate_dead_letter` — resubmit | job id | the row is queued after the first run, so the admin requeue skips it; the outcome is read from the route's own response shape, not assumed | `doctor.py:3832`; `hive/bus/routers/admin.py:89` | idempotent |
| `remediate_dead_letter` — remove | scoped approval over `[job_id]` | the grant is consumed; a second run is refused without a fresh grant, and with one the purge reports `not_found`/no-op | `doctor.__init__` gate; `doctor_gates.purge_result` | idempotent |
| `submit_auto_triage` | base kind: cooldown markers (`triage_seen`, `note_triage`) written before submit, and `_active_triage_exists`/`triaged_through` consulted by the scanner | the second run inside the window is suppressed. RESIDUAL: the marker is written BEFORE the POST, so a submit that fails consumes the window — missed work, never duplicate work | `doctor.py:2560`; `scan_for_failure_clusters` | idempotent (residual: failed submit suppresses the retry) |
| `action_dispatch_codex_fix` | nothing stable: kind embedded `int(time.time())` and no `idempotency_key` | a second run submits a SECOND fix job for the same parent. `run_job`'s exception path can call it twice for one row | `doctor.py:2152` (body build); `deploy/hive_worker.py:5538` (always-terminal catch) | **unsafe — fixed on this branch** (stable kind + `idempotency_key=doctor-codex-fix:<parent id>`; see `test_doctor_idempotency_20261029.py`) |
| worker claim `POST /v1/jobs/next` | agent URN + server-side atomic claim | returns no eligible job (204) or a different row; the bus never hands the same claimed row to two callers | `deploy/hive_worker.py:6959`; `hive/bus/routers/jobs.py:894` | idempotent |
| worker `patch_job` (terminal status) | (job id, status) per process | a second `failed` PATCH re-enters the bus's auto-retry branch, which increments `retry_count` and requeues the job — so `_run_job_inner` patching `failed` and then raising, with `run_job` patching `failed` again, spends a retry the job never used | `deploy/hive_worker.py:2851`, `deploy/hive_worker.py:5538`; `hive/bus/routers/jobs.py:1637-1650` | **unsafe — fixed on this branch** (terminal PATCH deduped per (job id, status) in the worker; see `test_worker_patch_idempotency_20261029.py`) |
| worker `patch_job` (claimed/running renewal) | job id | a repeat renewal rewrites the same status and extends `claim_lease_expires_at` — the intended effect | `hive/bus/routers/jobs.py:1521-1522` | idempotent |
| worker finalizer commit + push | worktree state + branch | nothing left uncommitted on the second pass, so it reports `nothing_to_commit` and pushes nothing. RESIDUAL: a commit left local by a failed push is NOT re-pushed by a later finalizer run — the job fails and the operator/workspace rescue owns it | `deploy/hive_worker.py:3288` (`report["commits"]`, `push_failed` path) | idempotent (residual: no push retry) |
| token-mismatch auto-fix | offending host + `MAX_TOKEN_FIX_PER_HOST` window; dispatch goes through `action_dispatch_codex_fix` | the host window refuses a repeat; the dispatched fix task is now keyed like every other codex-fix dispatch | `doctor.py:2360` (`_check_token_fix_rate_limit`), `doctor.py:2152` | idempotent (after the dispatch fix above) |
| SOP executor (`execute_sop_decision`) | the decision envelope's `job_id` + action | each verb delegates to the row above, so it inherits that verb's verdict; a replayed envelope re-runs the verb, which then no-ops | `doctor.py:3144` | idempotent (delegates) |

## What this audit did not establish

- **`unknown` rows: none.** Every action named in the brief has a verdict above.
- The bus's dedup is checked in process (`create_job` → `dedup_hash` →
  `find_active_dedup_job`), not against a live database; a concurrent double
  submit on the same key outside one transaction is argued from that code, not
  measured on the fleet.
- No live bus, doctor, worker, SSH or fleet host was touched. Every "what
  happens" above is a reading of the code plus the named regression test.

## Fixes on this branch

1. `action_dispatch_codex_fix` — stable kind and `idempotency_key` per parent job
   id, so a replayed dispatch coalesces on the bus instead of minting a second
   fix. Regression test: `test_doctor_idempotency_20261029.py`.
2. Worker terminal `patch_job` — one terminal PATCH per (job id, status) per
   process, so the always-terminal exception path cannot re-fail a job the inner
   run already failed (which would spend a retry through the bus's auto-retry
   branch). Regression test: `test_worker_patch_idempotency_20261029.py`.
