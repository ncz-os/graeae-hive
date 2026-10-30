# Deterministic paths for the doctor

Every reason string the hive worker and the bus can put on a failed or
declined job resolves to exactly one bounded action in code — never to a
prompt. This file is the **contract**: one row per reason, and a row that
names a test which does not exist fails the suite
(`test_doctor_decline_registry.py::test_registry_row_names_an_existing_test`).

The main failure-matrix rows (integrity / dirty worktree / no-commits /
not_task_branch / SIGHUP / push_failed / model_mismatch / review-not-approved /
loop_unresolved) are owned by the in-flight failure-matrix job; this branch
implements them in a **clearly separated function** — `doctor._handle_decline_job`
+ `hive.workers.doctor_decline_paths` — so the two can be reconciled when that
branch lands. `REPORT.md` names the split.

## Rows this branch owns

| Reason (or prefix) | Classifier decision | Handler | Test | Owner |
|---|---|---|---|---|
| `no_workspace_for_repo:<repo>` (found in catalog) | resubmit once with canonical repo id, `target_workspace` unset | `_handle_decline_job` → `_resubmit_job_once` | `test_no_workspace_for_repo_alias_resubmits_with_canonical_id` | fix/doctor-decline-paths-20261027 |
| `no_workspace_for_repo:<repo>` (not in catalog) | needs-review, `needs_operator: true`, repo name in reason | `_handle_decline_job` | `test_no_workspace_for_repo_absent_is_needs_review_with_repo_named` | fix/doctor-decline-paths-20261027 |
| `no_workspace_for_repo:<id>` (exact catalog id) | resubmit once with canonical repo id | `_handle_decline_job` → `_resubmit_job_once` | `test_no_workspace_for_repo_exact_id_resubmits` | fix/doctor-decline-paths-20261027 |
| `no_workspace_for_kind:<kind>` | routing signal: report-only — logged once per signature, durable telemetry tally only; never escalated at any count | `_handle_decline_job` → `routing_signal_seen` | `test_no_workspace_for_kind_repeated_five_times_stays_no_action` | fix/doctor-decline-paths-20261027 |
| `host_declines_kind:<host>` | routing signal: report-only — never counted toward the decline threshold, durable telemetry tally only, never escalated at any count | `_handle_decline_job` → `routing_signal_seen` | `test_host_routing_signals_repeated_five_times_stay_report_only` | fix/doctor-decline-paths-20261027 |
| `released_by_host` | routing signal: report-only — never counted, durable telemetry tally only, never escalated at any count | `_handle_decline_job` → `routing_signal_seen` | `test_host_routing_signals_repeated_five_times_stay_report_only` | fix/doctor-decline-paths-20261027 |
| `invalid_head_branch` (bare) | resubmit once with sanitized branch (kind slug prefix, leading `-` stripped, lowercase, max 60 chars); keep `branch_created_from` when present | `_handle_decline_job` → `sanitize_head_branch` | `test_invalid_head_branch_resubmits_sanitized_once` | fix/doctor-decline-paths-20261027 |
| `invalid_head_branch:<branch>` | resubmit once with sanitized branch; leading `-` cannot reach git | `_handle_decline_job` → `sanitize_head_branch` | `test_invalid_head_branch_keeps_branch_created_from` | fix/doctor-decline-paths-20261027 |
| `attribution_check_failed` | never retry: needs-review, `needs_operator: true`, violating commit/author in result, `doctor.attribution_violation` message | `_handle_decline_job` | `test_attribution_check_failed_also_never_retried` | fix/doctor-decline-paths-20261027 |
| `ai_attribution_trailer` | never retry: needs-review, `needs_operator: true`, violating commit/author in result, `doctor.attribution_violation` message | `_handle_decline_job` | `test_attribution_violation_is_never_retried_and_sends_one_message` | fix/doctor-decline-paths-20261027 |
| `gate_fail_closed` | never retry: needs-review, `needs_operator: true` | `_handle_decline_job` | `test_classifier_returns_a_bounded_decision_for_every_registry_row` | fix/doctor-decline-paths-20261027 |
| `dlq_protected` (reason string) | log only: never act, never resubmit, never LLM | `_handle_decline_job` (`log_only` branch) | `test_dlq_protected_reason_is_log_only_and_never_llm` | fix/doctor-decline-paths-20261027 |
| `dlq_protected` (kind/project protected) | log only: never act, never resubmit, never LLM | `process_triage_job` (protected-lane guard) | `test_dlq_protected_kind_is_log_only_and_never_llm` | fix/doctor-decline-paths-20261027 |
| (any unowned reason) | needs-review, `needs_operator: true` — the registry is a contract, an unknown row is a missing row | `_handle_decline_job` | `test_unowned_decline_reason_escalates_instead_of_vanishing` | fix/doctor-decline-paths-20261027 |
| (no decline reason on record) | fall through to v2 rules, then the LLM path — the decline table only owns decline rows | `_handle_decline_job` returns None | `test_no_decline_reason_falls_through_to_the_llm_path` | fix/doctor-decline-paths-20261027 |
| bus decline counter (host routing signals stay OUT) | verified: `is_unresolvable_release` returns False for `host_declines_kind` / `released_by_host`, True for job-level reasons | `hive.bus.domain.jobs.is_unresolvable_release` | `test_bus_classifier_keeps_host_signals_out_of_the_decline_count` | fix/doctor-decline-paths-20261027 |

## Rows owned by other in-flight jobs

These rows appear in the 24 h audit (`DETERMINISM-GAPS-20261027.md`). They are
listed here so the registry covers EVERY reason, but the handler lives in the
named branch — this branch must not double-implement them.

| Reason (or prefix) | Intended decision | Owner |
|---|---|---|
| `integrity` / `delivery_integrity_check_failed` | failure matrix: needs-review with verdict text | failure-matrix job (in flight) |
| `dirty worktree` / `worktree_dirty` | failure matrix: needs-review | failure-matrix job (in flight) |
| `no-commits` / `completion_evidence_missing` | failure matrix: needs-review | failure-matrix job (in flight) |
| `not_task_branch` | failure matrix: needs-review | failure-matrix job (in flight) |
| `SIGHUP` / `signal 1` | failure matrix: resubmit once (host infra) | failure-matrix job (in flight) |
| `push_failed` | failure matrix: resubmit once with transport evidence | failure-matrix job (in flight) |
| `model_mismatch` | failure matrix: needs-review | failure-matrix job (in flight) |
| `review-not-approved` / `review_not_approved` | failure matrix: needs-review with verdict text | failure-matrix job (in flight) |
| `loop_unresolved` | failure matrix: R6/R7 rules in `docs/doctor_v2_rules.toml` | failure-matrix job (in flight) |
| `codex_delivery_failed` | failure matrix: needs-review with delivery error | failure-matrix job (in flight) |
| `protection_violation` | failure matrix: needs-review, `needs_operator: true` | failure-matrix job (in flight) |
| `no_repo_mapping:<url>` | deployment-dependent: counts only for a catalog-absent repo (audit BLOCKING-1) | read-only kind exemption job (bus, in flight) |
| `malformed_job_id` | job-level unschedulable: count toward decline dead-letter | existing (`test_decline_reasons.py`) |
| `branch_not_found` | v2 rule R1: escalate to submitter | existing (`test_doctor_triage_actions.py`) |
| `invalid_base_branch` | job-level unschedulable: count toward decline dead-letter | existing (`test_decline_reasons.py`) |

## Needs-review rows: the review evidence must cover the head

These are not decline reasons: they are what the doctor does with a
`needs-review` row that already carries review evidence. The tests are in
`test_doctor_needs_review_head_aware_20261030.py`; the full policy is
`docs/NEEDS_REVIEW_POLICY.md`.

| Row state | Doctor action |
|---|---|
| evidence for another head, or a loop whose final round was `reviewer_unavailable`, or `request_changes` with newer commits | one re-review per (row, delivered head); `result.re_review_head`, `result.doctor_reason`; never while a review child is running |
| evidence covers the delivered head, or `child_advanced_branch` | none; a human confirms |
| no action left and untouched longer than `DOCTOR_NEEDS_REVIEW_ESCALATE_SECONDS` | one `doctor.needs_review_stuck` message until the row's state changes |

## Known worker-side bugs (needs_operator)

These are NOT doctor failures. The doctor's deterministic job for them is to
recognize them, not retry them, and point at the owner.

| Reason (or prefix) | Classification | Owner |
|---|---|---|
| `unhandled_exception` (TypeError inside the worker) | needs_operator: worker-side bug, not a job condition — R4 retries once, a repeated signature is a doctor/worker defect | worker owner (hive_worker.py) |
| delivery-packaging artefacts (`delivery_integrity_check_failed` packaging rows) | needs_operator: the packager emits rows no worker can run — fix the packager, not the queue | packager owner (delivery pipeline) |

## How to read a row

1. **Reason** — the exact string (or prefix) on `result.worker_error`,
   `result.last_decline_reason`, or `result.failure_reason`.
2. **Classifier decision** — what `classify_decline_reason` returns for it.
3. **Handler** — the function that carries the decision out.
4. **Test** — the pytest function that asserts the decision AND that no
   `invoke_doctor_agent` call follows it. A row without an existing test
   fails the suite.
5. **Owner** — the branch that implements the handler. Rows owned by another
   in-flight job are listed in the second table and carry no test here.
