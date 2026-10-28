# GRAEAE Hive queue audit — 2026-10-27

## Delivery and evidence boundary

Baseline and unchanged HEAD: `f2c43020c4e91965a802f665354ff9cd52dc8717`, branch `feat/hive-astra-audit-20261027`.

This audit records **39 grouped findings: 20 fixed and 19 remaining** (5 partial, 14 left) after Terra grind round 1. The original remaining-effort counts were **LOW 5 / MEDIUM 11 / HIGH 4**. “Fixed” means the specified source path and regression test changed; it does not mean deployed or verified on the fleet. Some fixed findings have separately named residuals.

No live bus/database, remote forge, MNEMOS, service restart, or push was used. The explicit repository-only restriction overrides standing network registration/persistence workflows for this task. The sandbox declares `.git` read-only. No staging, commit, push, or attempt to bypass that restriction was made. The index is unchanged; the working tree necessarily contains the unstaged fixes. A literally clean working tree would require discarding them. No new commit SHA or primary zoder APPROVE is claimed. Independent local reviews caught and corrected overbroad infrastructure classification and a review-parent completion gap; they are not a substitute for the external gate.

The incident's deployed `hive_worker.py` is **not tracked in this repository**. References to it in comments/tests are not its implementation. This audit fixes the tracked `hive/workers/run.py` and harness adapters, plus bus handling of reports from existing workers. It cannot establish that the absent zoder loop stops consuming internal review iterations, or repairs its push/finalizer behavior.

## Incident mechanism

The supplied snapshot reports 131 rows: 108 done, 14 claimed, 3 needs-review, 2 queued, 4 failed. Its retry distribution is 92 zero / 6 one / 33 two. These are operator-supplied observations, not a fresh query. The example categories include more failure descriptions than the four failed rows; they are treated as incident examples, not five independently verified current failed records.

* The API's existing `JobCreate.max_retries` default was already **2**, not 0. The defect was accepting explicit zero without justification or bounds, while upstream callers could send zero freely. The source and supplied data do not establish which external caller wrote each of the 92 zero values.
* A socket/cancel failure was reported as no commits; the bus treated every `failed` alike. Zero budget made it terminal. A positive budget requeued it but discarded the result, erasing the evidence needed to distinguish host trouble from work failure. Routing releases also spent retry count before useful execution.
* Branch admission deliberately accepted an existing head. A fresh-output worker with a no-force-push policy could then spend its loop discovering a predictable delivery collision. Submission now rejects that shape for commit-producing jobs. Read-only review of an existing branch remains supported.
* Completion required `worktree_clean=True`, so untracked cache/build files could reject a pushed artifact. The new exception requires explicit tracked-clean proof, a nonempty list of untracked files, pushed/remote-verified flags, and matching remote/head SHAs; tracked dirt and mismatches remain rejected.
* `needs-review` had no transactional bus dispatch. Doctor followups were pinned to a family/model/tier and could treat lookup failure as no children. Dispatch alone also did not consume a child verdict into the code parent's completion. Both paths now use durable parent/child identity and exact-head completion checks.

## Fixed findings

Locations refer to the final source, not the baseline. Tests named here drive routes, transaction adapters, or the actual harness worker; external Git/driver/transport boundaries are replaced where no external access is allowed.

| ID | Location | Defect and implemented behavior | Regression |
|---|---|---|---|
| F01 | `hive/bus/schemas.py:78`; `hive/bus/routers/jobs.py:842` | Zero/negative/coerced retry budgets could enter without explanation. Strict integer 0..100, default 2; zero requires nonblank `zero_retry_reason`, retained in initial result and queued event. | `test_zero_budget_requires_reason_and_persists_opt_in` |
| F02 | `hive/bus/routers/jobs.py:668` | Existing output refs and unchecked code refs could burn a worker. Reject existing output head, invalid refs, unresolved catalog remote for an explicit code head, and unreachable probe. | `test_submit_refuses_existing_output_branch`; updated submitter/base-branch production-route fixtures |
| F03 | `hive/bus/routers/jobs.py:617`, `:90` | Kind-only admission missed host/capability/tier intersections and explicit daemon-down metadata. Full matcher at submit; reported-down engines excluded at submit and both claim routes. Unknown health remains R02. | `test_submit_rejects_host_capability_intersection_and_down_engine` |
| F04 | `hive/bus/routers/jobs.py:808` | Active dedup omitted repository/ref/retry contract, coalescing distinct artifacts. Include site/repo/head/base/PR/budget/reason. | `test_submit_dedup_includes_artifact_and_retry_contract` |
| F05 | `hive/bus/domain/failure.py:26`; `hive/bus/routers/jobs.py:1420` | Infrastructure reports became work failures and retry cleared evidence. Classify explicit engine/cancel/provider-idle evidence; preserve it; requeue within budget, exclude previous host even after re-registration, and park exhausted rows as queued with `infrastructure_parked=true`. Protection, explicit retry refusal, acknowledged cancellation and successful socket controls stay out. | `test_infra_requeues_preserves_evidence_excludes_failed_host_and_parks_at_budget`; `test_work_failures_and_explicit_refusals_are_not_infrastructure_retries` |
| F06 | `hive/bus/routers/jobs.py:1465` | Routing releases consumed failure budget. They retain backoff without incrementing work retries. | `test_host_routing_release_does_not_spend_work_retry_budget` |
| F07 | `retry_policy.py:22`; `hive/bus/routers/jobs.py:1586` | Auto-retry missed diagnostic protection/hold/routing markers; direct failure retry bypassed policy. Both paths use guarded policy; retained results preserve evidence. Build retries require explicit retryable evidence, consistent with existing auto-retry policy. | `test_stored_protection_and_holds_cannot_auto_retry`; `test_retryable_failure_still_requeues` |
| F08 | `hive/workers/run.py:180`; `hive/harness/generic.py:194` | Nonzero CLI results were reported as complete, and exceptions lost structure. Report failed with exit code, stderr, error and exception type. | `test_nonzero_cli_is_reported_with_evidence`; `test_execution_exception_preserves_type_and_message` |
| F09 | `hive/workers/run.py:195` | A lost completion response fell into execution failure reporting and could overwrite success. Separate execution from reporting failure; leave uncertain report for recovery. | `test_lost_completion_response_does_not_overwrite_success` |
| F10 | `hive/bus/domain/jobs.py:310` | Untracked leftovers invalidated proven delivery. Accept the bounded remote-verified, tracked-clean exception described above. | `test_verified_pushed_head_with_only_untracked_files_is_delivered` (positive plus tracked-dirty, mismatch, unverified, unpushed controls) |
| F11 | `hive/bus/domain/review_dispatch.py:20`; `hive/bus/routers/jobs.py:1753`; `hive/workers/doctor.py:3800` | Review waiting room, family pinning and duplicate-dispatch crash window. Insert high-priority >=100, tier-C, unpinned review child and parent marker in one transaction; doctor uses the same idempotency key and durable STATE marker. Preserve branch/head and never-act lanes. | `test_production_patch_dispatches_once_with_head_and_family_unpinned`; `test_rollback_does_not_leave_child_or_parent_marker`; `test_immediate_patch_and_restart_reconciliation_share_marker` |
| F12 | `hive/bus/domain/review_dispatch.py:103`; `hive/bus/routers/jobs.py:1755` | Completed review children did not finish their code parent. Consume structured approve verdict only for exact parent SHA plus valid artifact proof; rejection/mismatch/incomplete proof records blocking evidence. Doctor replays completed children through this verifier, including corrected verdicts. | Review-child production PATCH tests in `test_review_dispatch_transaction.py` and replay tests in `test_doctor_review_reconciliation.py` |
| F13 | `hive/workers/doctor.py:2143`, `:3316`, `:3502`, `:3773` | Failed review/DLQ/child listings and retention failure could resemble empty work. Raise/report explicit sweep errors; retention returns `scanned=None,error`, and per-row dispatch failures are counted. | `test_listing_error_is_not_empty_queue`; `test_child_listing_error_cannot_dispatch`; `test_retention_fetch_error_visible_without_purge` |
| F14 | `hive/persistence/postgres.py:63` | PostgreSQL commits/rollbacks were no-ops, writes reported rowcount 1, failed writes were replayed. Use explicit driver transactions, actual command-tag counts, rollback on exit and no replay. | Three `test_postgres_*` tests in `test_persistence_queue_audit_20261027.py` |
| F15 | `hive/persistence/mysql.py:25`, `:129` | MySQL autocommitted and captured rowcount before execution. Disable autocommit; read live cursor rowcount; rollback on exit. | `test_mysql_cas_reads_rowcount_after_execution_and_disables_autocommit`; `test_mysql_failed_transaction_rolled_back_on_exit` |
| F16 | `hive/bus/deps.py:80`, `:256`; `hive/persistence/sqlite.py:1509` | Dependency events committed caller transactions before claim/terminal work finished. Add noncommitting event mode for these paths; rollback now includes event and dependency mutations. | `test_release_events_do_not_commit_terminal_transition_on_failure`; `test_claim_missing_dep_event_rolls_back_when_other_dep_blocks` |
| F17 | `hive/bus/routers/admin.py:160`, `:323` | Admin requeue/reclassification could overwrite a concurrent completion and falsely report success. Expected-status CAS, checked rowcount, additive `skipped_conflict`; review reclassification dispatches atomically. | Five production admin/schedule tests in `test_admin_schedule_audit_20261027.py` |
| F18 | `hive/bus/routers/schedules.py:19`, `:75`, `:99` | Schedule mutation lacked the existing actor/token gate. POST/PATCH/DELETE now require `require_mutation_actor`. | `test_schedule_mutations_require_actor_and_token` |
| F19 | `hive/workers/run.py:52`, `:77`; `hive/harness/generic.py:132` | Tracked worker registration omitted required host; fake transports hid the 422. Optional registration host preserves positional compatibility; worker supplies configured/local host, adapter has local fallback. | Actual worker→adapter→ASGI registration tests in `test_worker_execution_evidence.py` |

## Remaining work and acceptance gates

No row in this table is represented as repaired. Effort counts: **LOW 5, MEDIUM 11, HIGH 4**. Risk describes the failure if left unresolved. Work involving the absent deployed source or real concurrency needs the orchestrator's canonical source and backend environment.

| ID | What / why left | Where | Effort | Risk | Acceptance test |
|---|---|---|---|---|---|
| R01 | Deployed zoder worker/finalizer absent; cannot fix its inner review-iteration accounting, already-published push recovery or actual socket preflight in this repo. | External `hive_worker.py`; tracked adapter `hive/harness/zoder.py:14` only declares capabilities | HIGH | Original live loop may still waste iterations and reject delivery before reporting evidence. | In canonical worker, fail socket before engine starts; assert no review iteration; retry another host; already-pushed matching ref succeeds; no force-push. |
| R02 | Engine health is optional metadata, not an attested fresh socket probe. Legacy unknown metadata remains compatible; bus cannot infer remote daemon health from registration alone. | `hive/bus/domain/failure.py:10`; `hive/workers/run.py:84` | MEDIUM | A healthy-looking registration can still claim with a dead daemon. | Publish timestamped readiness from each actual executor; expire it; both claim routes and submission reject down/unknown/stale executor health. |
| R03 | Branch probe caches absence and submit/execute has no reservation. Fresh-output refusal does not eliminate a remote ref created after the probe. | `hive/bus/domain/branch_admission.py:90`, `:217` | HIGH | Time-of-check/time-of-use collision remains possible. | Two concurrent submissions targeting same ref yield one reservation; stale negative cache cannot admit published ref; worker handles identical already-pushed SHA idempotently. |
| R04 | Existing clean-local completion proof still does not require verified origin. This patch adds a strict dirty exception without silently changing every legacy clean-proof caller. | `hive/bus/domain/jobs.py:310` | HIGH | Clean local commits can be called done without remote delivery; submitted attestations are trusted. | Migrate all executors to verified remote proof; reject clean-but-unpushed and forged/mismatched artifact evidence under a trusted verifier. |
| R05 | Review listing is bounded at 1,000 and API lacks pagination; child listing is also bounded. A failed/exhausted child can retain a dispatch marker indefinitely. Truncation is now explicit, not falsely empty. | `hive/workers/doctor.py:2576`, `:3502`, `:3800`; `hive/bus/routers/jobs.py:252` | MEDIUM | Old unreviewed rows beyond the window or failed child reviews can remain stuck. | >1,000 parents plus failed review children, restart and repeated sweeps: every unprotected parent gets one durable child and a visible retry/recovery disposition. |
| R06 | Reaper stale cancellation still selects then blindly updates IDs and bypasses dependency release. Requires changed repository contract and interleaving tests. | `hive/bus/app.py:360`; `hive/persistence/sqlite.py:2111` | MEDIUM | Newly claimed job can be cancelled; cancelled parent can strand dependents; parked infra rows can age into cancellation. | Claim/renew between stale select and cancel; zero cancellations. Actual stale cancellation releases dependencies atomically; parked infra state remains explicit. |
| R07 | Orphan requeue compares status but not observed owner/lease/progress revision. | `hive/persistence/sqlite.py:2076` | MEDIUM | A renewed or reassigned running job can be reclaimed and executed twice. | Renew lease/change claimant after selection; stale reaper CAS must affect zero rows on every dialect. |
| R08 | Dependency list read/modify/write only status-CASes; swallowed SQL errors can poison PostgreSQL transactions without savepoints. | `hive/persistence/sqlite.py:1249`, `:1297`, `:1325` | MEDIUM | Concurrent releases lose an edge removal; “best effort” failure aborts outer transaction. | Concurrent distinct dependency deaths remove both edges; injected statement failure either rolls back a savepoint or propagates cleanly. |
| R09 | Schedule submission and occurrence marker advancement are separate transactions. | `hive/bus/app.py:331` | MEDIUM | Crash or two reapers can execute one occurrence twice. | Crash after child insert before mark; two schedulers race; stable occurrence key produces exactly one job even after it finishes. |
| R10 | Cost stats use SQLite-unsupported `JSON_VALUE`; day expression is SQLite-specific for PostgreSQL/MySQL. | `hive/persistence/sqlite.py:1727` | MEDIUM | `/v1/stats/costs` fails or groups incorrectly on a supported backend. | Route test for provider/day grouping on SQLite and live backend matrix with identical fixtures. |
| R11 | Noncommitting dependency/review events persist but intentionally skip precommit SSE fan-out; no postcommit outbox exists. | `hive/bus/deps.py:87` | MEDIUM | Subscribers need log polling/reconnect to see these committed events. | Rollback emits no event; commit emits once to live subscribers, including recovery after publisher crash. |
| R12 | Remaining doctor fetches still return empty on HTTP failure; sweep-error broadcasts are not a durable local sweep ledger and can fail with the bus. | `hive/workers/doctor.py:856`, `:2121`; `hive/workers/doctor_state.py` | MEDIUM | Some diagnostics still conflate unavailable input with no findings; task-wide “every sweep visible” is not fully met. | Fault every sweep input/output/store path; persist failed/partial/success with reason and scanned=null where unknown across restart. |
| R13 | Capacity singleton hydrates/flushes process counters; occupancy check and claim are not a cross-process provider-slot reservation. | `hive/bus/domain/capacity_ledger.py:20`; `hive/bus/routers/jobs.py:950` | HIGH | Multiple bus processes can oversubscribe a global provider cap. | Parallel claims from independent processes against each backend cannot exceed cap or per-worker lease, including restart. |
| R14 | `check_gap_promotion` has no production caller; manifest names 11 unavailable test functions. Wiring without a policy decision would claim automation not actually executed. | `hive/workers/remediation.py:417`; `hive/workers/remediation_manifest.py:28` | LOW | Repeated unknown failures never become durable named remediation gaps; manifest overstates coverage. | Drive repeated unknown failures through production SOP executor, observe one gap; validate every manifest test resolves to collected tests. |
| R15 | Synchronous worker execute blocks heartbeat/lease renewal; failed result delivery has no durable outbox. | `hive/workers/run.py:180` | MEDIUM | Long execution can be reclaimed while running; lost final report relies on reaper recovery and may duplicate work. | Run beyond lease with heartbeat assertions; restart between execution and report; recover same result without rerunning. |
| R16 | ABC omits dependency-release/drop-missing methods inherited from SQLite. Structural tests cannot enforce their presence/behavior. | `hive/persistence/base.py`; `test_backend_contract.py:35` | LOW | A new backend can pass structural coverage while missing essential queue behavior. | Declare methods in ABC; abstract incomplete backend fails; behavioral rollback/CAS matrix passes. |
| R17 | Date-pinned, filesystem-case, stale-baseline and optional-dependency test assumptions prevent a clean local full suite. No unrelated source/test weakening. | `test_eih_probe.py:556`; `test_repo_argonas_fallback.py:504`, `:862`; `test_gitguardian_config.py:45` | LOW | False failures obscure real regressions. | Freeze/inject clock; explicitly test case-sensitive storage or detect platform; pin a genuinely pre-change baseline; install declared optional test dependencies in production venv. |
| R18 | Direct auto-retry context omits project, so configured protected project/prefix lanes are not uniformly applied outside doctor routing. Reason-level never-act checks are fixed. | `hive/persistence/sqlite.py:1621`; `hive/bus/routers/jobs.py:1815` | MEDIUM | A protected owner lane can be retried through another API path. | Exercise `/auto-retry` and initial failure routing for configured protected project/prefix with benign error; neither may mutate it. |
| R19 | Lifecycle test expects DELETE `/v1/jobs/{id}`, but baseline/current routers do not define it. No endpoint was removed by this patch; avoid inventing destructive semantics during queue repair. | `test_agent_lifecycle.py` test `test_delete_needs_review_allowed_running_still_refused`; `hive/bus/routers/admin.py:203` | LOW | Test/API expectation mismatch; callers relying on DELETE receive 405. | Decide canonical contract; either authorized guarded DELETE path tested against running/needs-review rows, or migrate caller/test to supported purge contract. |
| R20 | Explicit packaging list omits bus/domain/router packages. Source-tree tests do not establish installed wheel behavior. | `pyproject.toml:31` | LOW | Installed artifact may omit the code just repaired. | Build/install wheel in isolated off-host validation environment and import/start all router modules; no source checkout on import path. |

## API and caller compatibility

All requested public route paths remain present with their existing methods: `/v1/jobs`, `/v1/jobs/next`, `/v1/jobs/{id}` (GET/PATCH), `/v1/admin/jobs/*`, `/v1/jobs/purge`, `/v1/jobs/summary`. The missing per-ID DELETE is a pre-existing mismatch, R19.

Intentional changes:

* Submit explicit zero with `zero_retry_reason`, or omit `max_retries` for default 2. Existing in-repo zero-budget submission fixtures were migrated; database legacy zero values are not rewritten. Schedule templates also validate through `JobCreate`. External submitters are unavailable here and must handle the new 422.
* Code submissions naming an existing output head must choose a fresh head and valid base; read-only reviews may name existing heads. Explicit code refs must resolve to a trusted remote and pass admission. Storage/round-trip tests now inject trusted remote answers instead of expecting unsafe acceptance.
* No matching active worker is now 422. Explicit engine-down metadata blocks claim. Retry exhaustion for classified infra is a visible queued hold, not successful completion. Operator override remains the existing admin workflow.
* Immediate review children are internal transaction work, may queue until a reviewer becomes available, and carry `auto_review_job_id` on the parent. Any family/tier can claim within ordinary capability/host policy. Never-act/protected rows are intentionally excluded; “every row” never overrides those guards.
* Admin responses add `skipped_conflict`; existing `ok`/skip fields remain. Schedule mutations now require the existing bearer/admin-token actor contract. No in-repo unauthenticated production schedule client was found.
* Initial work-failure retries use the same policy as `/auto-retry`, including explicit retryable proof for build jobs. Their evidence is retained. Routing releases no longer spend work-failure retries.

## Coverage audit and verification

Reviewed the requested bus routes/app/deps/domain, queue/retry policy, all five persistence implementations and their common ABC/dialects, tracked harness worker, doctor/state/gates/SOP/remediation, and associated tests. Oracle/Db2 behavior was inspected and exercised through existing adapter doubles, not real servers. There is no claim of real multi-dialect concurrency or deployed-worker execution.

Tests that could not detect the incident before this change:

* `test_backend_contract.py` checked subclass/abstract-method/SQL translation shape, never affected-row results or commit visibility. All adapters could pass despite PostgreSQL's hardcoded rowcount and no-op commit.
* Harness transports returned success for arbitrary registration bodies, hiding missing host. New registration tests reach the real ASGI validator and stored row.
* `test_queue_logic.py` has 18 import-time assertions but zero pytest test functions: `pytest test_queue_logic.py` exits 5 even though collection executed assertions. It was also run directly.
* Helper-only remediation promotion tests do not establish that the production executor calls the helper; manifest strings are not executable coverage.
* A dependency test inadvertently expected an uncommitted repair to survive, relying on the premature event commit; it now asserts rollback when another dependency blocks claim.
* Test fixtures sharing the cached global repository leaked state across independent temp databases. Updated lifecycle/base-branch/submitter fixtures reset and restore that cache instead of accepting order-dependent behavior.

Production venv `/opt/agent-bus-venv/bin/python` is absent. System Python cannot collect bus tests because `sse_starlette` is missing. Tests therefore use the already-installed `/Users/jasonperlow/Projects/graeae-hive-work/.venv/bin/python`, temporary SQLite databases and driver/HTTP doubles. No packages were fetched. This is local verification, not the requested production-venv/database run.

Final command totals and exact new test names follow the commit handoff below.

## Commit handoff

No commits created: `.git` is read-only by sandbox policy. The orchestrator should stage logical hunks, with author `Jason Perlow <jperlow@gmail.com>`, no attribution trailers, and push only to the authorized ARGONAS remote. Suggested imperative subjects (not claims that commits exist):

1. `Validate queue submission and retry contracts` — F01–F04.
2. `Preserve infrastructure failure evidence and retry safety` — F05–F07.
3. `Report worker outcomes and register the host correctly` — F08–F09, F19.
4. `Accept verified delivery with untracked leftovers` — F10.
5. `Dispatch reviews atomically and reconcile verdicts` — F11–F13.
6. `Honor database transactions and compare-and-set results` — F14–F16.
7. `Guard admin races and authenticate schedule mutations` — F17–F18.
8. `Document queue audit evidence and remaining gates` — this document.

Several commits share router/test files and require hunk staging. No fake commits, force-push, integration merge, or artifact build was attempted.


## Final verification receipt

Let `VENV=/Users/jasonperlow/Projects/graeae-hive-work/.venv/bin/python` (existing interpreter; no installation).

| Command / check | Result |
|---|---|
| `/opt/agent-bus-venv/bin/python` availability | File absent; production-venv suite not run. No live database/backend connection used. |
| `$VENV -m pytest -o addopts='' -q --tb=short` | **1083 passed, 7 failed, 9 skipped, 131 subtests passed**, 61.56 seconds. Full suite is NOT green. |
| `$VENV -m pytest -o addopts='' -q` followed by the seven new test modules listed below | **58 passed, 18 subtests passed**, 2.85 seconds. |
| Focused doctor/review/retention/admin run | **384 passed, 4 subtests passed**. Included again in the full suite; do not sum runs as unique coverage. |
| `python3 test_queue_logic.py` | **18 checks passed**. `python3 -m pytest -q test_queue_logic.py` collects zero tests and exits 5 (import-time assertions only). |
| `python3 -m py_compile` with every changed/new `.py` path | **33 files, exit 0**. Temporary isolated bytecode directory removed. |
| Ruff comparison on those 33 files against `git show HEAD:<file>` | **35 baseline findings, 32 current findings, 0 added code/message findings**. Existing lint baseline not represented as clean. |
| `git diff --check` | Exit 0; no whitespace errors. |
| `git grep -l '^<<<<<<<'` | Empty output; exit 1 means no matches. |
| `git diff --cached --stat` | Empty; nothing staged. |
| `git rev-parse HEAD` | `f2c43020c4e91965a802f665354ff9cd52dc8717`; no new commits. |

The seven full-suite failures (none suppressed):

1. `test_agent_lifecycle.py::AgentLifecycleTests::test_delete_needs_review_allowed_running_still_refused` — HTTP 405; DELETE absent in baseline and current source.
2. `test_eih_probe.py::OutputFileTests::test_main_writes_rows_and_exits_zero_on_probe_failure` — expects October 26 filename, actual test clock is October 27.
3. `test_gitguardian_config.py::GitGuardianConfigShapeTests::test_config_declares_version_2` — optional `yaml` unavailable.
4. `test_gitguardian_config.py::GitGuardianConfigShapeTests::test_ignored_paths_is_present_and_iterable` — same missing `yaml`.
5. `test_gitguardian_config.py::GitGuardianConfigShapeTests::test_ignored_paths_is_scoped_to_test_fixtures` — same missing `yaml`.
6. `test_repo_argonas_fallback.py::IdContractTests::test_case_is_not_a_tolerance_for_the_fallback` — local case-insensitive filesystem resolves differently cased paths.
7. `test_repo_argonas_fallback.py::OldCodeContrastTests::test_old_lookup_misses_what_the_new_one_resolves` — selected baseline already resolves the supposedly unsupported repo.

These specific pre-existing source/environment mismatches were inspected; no claim is made that a complete unmodified-baseline suite was rerun. No test dependency was downloaded to make the result look green.

### Added tests by exact name


`test_queue_audit_20261027.py`:

* `test_zero_budget_requires_reason_and_persists_opt_in` (line 23).
* `test_submit_rejects_host_capability_intersection_and_down_engine` (line 35).
* `test_submit_refuses_existing_output_branch` (line 48).
* `test_infra_requeues_preserves_evidence_excludes_failed_host_and_parks_at_budget` (line 63).
* `test_host_routing_release_does_not_spend_work_retry_budget` (line 116).
* `test_submit_dedup_includes_artifact_and_retry_contract` (line 131).
* `test_work_failures_and_explicit_refusals_are_not_infrastructure_retries` (line 152).

`test_persistence_queue_audit_20261027.py`:

* `test_postgres_cas_preserves_zero_and_actual_rowcount` (line 23).
* `test_postgres_failure_rolls_back_without_replaying_statement` (line 38).
* `test_postgres_uncommitted_work_rolled_back_and_new_work_starts_transaction` (line 51).
* `test_mysql_cas_reads_rowcount_after_execution_and_disables_autocommit` (line 63).
* `test_mysql_failed_transaction_rolled_back_on_exit` (line 90).
* `test_release_events_do_not_commit_terminal_transition_on_failure` (line 122).
* `test_claim_missing_dep_event_rolls_back_when_other_dep_blocks` (line 141).

`test_admin_schedule_audit_20261027.py`:

* `test_requeue_does_not_overwrite_completion_after_preread` (line 38).
* `test_reclassify_does_not_dispatch_after_lost_cas` (line 53).
* `test_reclassify_dispatches_exactly_one_review_before_commit` (line 71).
* `test_reclassify_dispatch_failure_rolls_back_transition` (line 83).
* `test_schedule_mutations_require_actor_and_token` (line 96).

`test_auto_retry_protection.py`:

* `test_stored_protection_and_holds_cannot_auto_retry` (line 12).
* `test_retryable_failure_still_requeues` (line 40).

`test_doctor_review_reconciliation.py`:

* `test_immediate_patch_and_restart_reconciliation_share_marker` (line 37).
* `test_child_listing_error_cannot_dispatch` (line 54).
* `test_listing_error_is_not_empty_queue` (line 73).
* `test_existing_verdict_and_never_act_not_dispatched` (line 81).
* `test_protected_lane_not_dispatched_or_verdict_completed` (line 88).
* `test_failed_submit_retries_same_idempotency_key` (line 97).
* `test_retention_fetch_error_visible_without_purge` (line 115).
* `test_generic_failure_does_not_hide_nested_routing_signal` (line 125).
* `test_completed_child_replayed_even_with_submission_marker` (line 132).
* `test_truncated_backlog_is_explicit_and_uses_api_maximum` (line 152).

`test_review_dispatch_transaction.py`:

* `test_production_patch_dispatches_once_with_head_and_family_unpinned` (line 43).
* `test_rollback_does_not_leave_child_or_parent_marker` (line 62).
* `test_reconciliation_winner_is_reused` (line 73).
* `test_protected_lane_preserved` (line 90).
* `test_generic_failure_does_not_hide_nested_protected_reason` (line 99).
* `test_child_verdict_completes_only_approved_matching_artifact` (line 116).
* `test_corrected_child_verdict_can_unblock_parent` (line 151).

`test_worker_execution_evidence.py`:

* `test_nonzero_cli_is_reported_with_evidence` (line 18).
* `test_execution_exception_preserves_type_and_message` (line 41).
* `test_lost_completion_response_does_not_overwrite_success` (line 52).
* `test_verified_pushed_head_with_only_untracked_files_is_delivered` (line 63).
* `test_worker_host_registers_through_real_asgi_schema_and_route` (line 104).
* `test_default_worker_host_is_local_hostname` (line 135).

Total: **44 new test functions**, expanded by parameterization into the **58 passing cases** above (plus 18 subtests).

## Cleanup receipt

* Created/kept: this audit, two bus domain modules, seven regression modules, and the listed unstaged source/test fixes — required handoff artifacts.
* Removed: `.audit-suite.log` after recording the complete result here; `.audit-pycache/` created solely for compilation; bytecode for the nine new Python source/test files. Temporary SQLite databases and test bare repositories were managed by fixture cleanup. No persistent clone/worktree, container, remote file, unit, mount, service, or fleet process was created.
* Existing interpreter/pytest cache directories may have been refreshed by tests; they are ignored and left intact rather than deleting directories whose prior ownership was not established. No other session's scratch was deleted.
* `df -h . /private/tmp`: both `/dev/disk3s1`, APFS data volume, **460 GiB total / 378 GiB used / 58 GiB available / 87%**. No tmpfs was involved. No background validation process remains.


## Terra remediation 2026-10-27

The original audit above is historical: `deploy/hive_worker.py` is now tracked at
`9fcd71f`. This addendum records local source changes only. No live bus, fleet,
remote, deployment, or zoder review was accessed. The deployed worker's
sidecar imports are missing from this checkout, so worker tests stub those
imports; they do not demonstrate an installed runtime. A socket connection
proves connectability only, not daemon readiness or its protocol. The worker
comment describes zoder cold-starting an ephemeral daemon, so requiring an
already-listening socket may reject valid cold starts; deployment requires
verification of that contract before rollout. The socket-positive test uses a
stub because the sandbox denies binding a Unix socket.

| ID | Status | Change / remaining acceptance evidence | Regression / evidence |
|---|---|---|---|
| R01 | Partially fixed | `deploy/hive_worker.py:2825`, `:2891`, `:3841`, `:4052`, `:5579`; `hive/bus/domain/failure.py:59`: socket preflight, infrastructure classification, remote-matched delivery with recorded untracked files. Push rejection recovery, finalizer classification, and inner loop iteration behavior after launch remain open. | `test_worker_preflight_stops_before_driver_when_daemon_unreachable`, `test_worker_preflight_accepts_connectable_socket`, `test_worker_delivery_accepts_untracked_but_rejects_tracked_changes`, `test_daemon_lost_preflight_is_routed_as_infrastructure`; local tests only. |
| R02 | Left | Timestamped attested executor health and expiry remain required. | No new regression or acceptance evidence. |
| R03 | Left | Durable branch reservation remains required. | No new regression or acceptance evidence. |
| R04 | Left | Trusted origin verification for every executor remains required. | No new regression or acceptance evidence. |
| R05 | Left | Paginated review dispatch and failed-child recovery remain required. | No new regression or acceptance evidence. |
| R06 | Left | Stale-cancel CAS and dependency release remain required. | No new regression or acceptance evidence. |
| R07 | Left | Owner/lease/revision CAS remains required. | No new regression or acceptance evidence. |
| R08 | Left | Concurrent dependency release and error handling remain required. | No new regression or acceptance evidence. |
| R09 | Left | Atomic scheduled occurrence and child insert remain required. | No new regression or acceptance evidence. |
| R10 | Left | Cross-backend cost statistics remain required. | No new regression or acceptance evidence. |
| R11 | Left | Durable postcommit event publication remains required. | No new regression or acceptance evidence. |
| R12 | Left | Durable doctor sweep ledger and error propagation remain required. | No new regression or acceptance evidence. |
| R13 | Left | Cross-process provider reservations remain required. | No new regression or acceptance evidence. |
| R14 | Left | Production gap promotion and manifest validation remain required. | No new regression or acceptance evidence. |
| R15 | Left | Worker lease renewal and durable result outbox remain required. | No new regression or acceptance evidence. |
| R16 | Left | Persistence ABC and backend behavioral contract remain required. | No new regression or acceptance evidence. |
| R17 | Left | Environment-dependent suite failures remain. | No new regression or acceptance evidence. |
| R18 | Left | Protected project/prefix lanes must be enforced in both retry paths. | No new regression or acceptance evidence. |
| R19 | Left | DELETE contract remains undecided. | No new regression or acceptance evidence. |
| R20 | Left | Installed wheel validation remains required. | No new regression or acceptance evidence. |


## Terra priority-A remediation 2026-10-27

Local source only; no bus, database service, fleet, network, or deployment was accessed. The executor and reviewer files are outside the permitted paths. No primary zoder review was performed.

| ID | Status | Source and remaining work | Regression and acceptance evidence |
|---|---|---|---|
| R02 | Left | Actual executors must publish expiring readiness; unknown readiness must fail closed at submission and both claim routes. Requires excluded executor sources and coordinated migration. | No regression; acceptance not established. |
| R03 | Left | Cross-process ref reservations and fresh remote probing remain; the excluded worker must handle a matching already-pushed SHA. | No regression; acceptance not established. |
| R04 | Left | A trusted origin verifier and executor proof migration remain required; submitted flags alone are not attestation. | No regression; acceptance not established. |
| R06 | Partial | `hive/bus/app.py` `_cancel_stale_jobs`; `hive/persistence/sqlite.py` `stale_cancel_candidates` and `cancel_stale_candidate`: compare observed status, owner, lease and timestamps; skip parked infrastructure evidence; invoke dependency release in the cancellation transaction. The inherited dependency release suppresses some failures, bounded scans may be starved by old parked rows, and hold-result revision and backend concurrency remain unverified. | `test_stale_cancellation_rechecks_claim_and_releases_dependencies`: SQLite claim interleaving, parked row, and dependent release pass. Rollback on release failure and real backend concurrency remain open. |
| R07 | Partial | `hive/bus/app.py` reaper loop; `hive/persistence/sqlite.py` `requeue_orphan_job`; `hive/persistence/base.py`: compare observed status, claimant, claim time, progress timestamp and lease; emit only on success. A monotonic claim/progress revision is still needed to eliminate ABA; real backend concurrency remains unverified. | `test_orphan_requeue_compare_and_set_observed_claim`: SQLite owner, lease, progress and status interleavings pass. |
| R13 | Left | Cross-process provider occupancy reservation and claim transaction remain required for every backend. | No regression; acceptance not established. |

Verification: local existing venv `python -m pytest -o addopts='' -q test_priority_a_reaper.py test_reaper.py test_reaper_sweeps.py test_reaper_agents.py test_reaper_schedules.py test_persistence_queue_audit_20261027.py test_backend_contract.py`: 38 passed. Production venv and real backend concurrency were not run.
## Terra priority-B remediation 2026-10-27

This addendum supersedes the priority-B statuses in the preceding table. The
repository-only restriction prevented fleet, MNEMOS, Hive, zoder and remote
access. The persistence and domain exclusions prevent changes to their
contracts and implementations; no cross-backend or deployment claim is made.

| ID | Status | Location and acceptance evidence / remaining work |
|---|---|---|
| R05 | Left | `hive/workers/doctor.py:3502`; `hive/bus/routers/jobs.py:252`: pagination needs repository changes and dead-child recovery needs a transactional marker design. No new regression. |
| R09 | Left | `hive/bus/app.py:331`: occurrence insert and marker require a shared persistence transaction and stable unique key. No new regression. |
| R10 | Left | `hive/persistence/sqlite.py:1727`: cost query needs backend-specific persistence work and backend matrix. No new regression. |
| R12 | Left | `hive/workers/doctor.py:856`: fetch error propagation and durable sweep ledger remain. No new regression. |
| R15 | Left | `hive/workers/run.py:180`: independent lease renewal and restart-safe report outbox remain. Agent heartbeat alone does not renew the job lease. No new regression. |
| R11 | Left | `hive/bus/deps.py:87`: transactional outbox and recovery remain. No new regression. |
| R18 | Left | `hive/bus/routers/jobs.py:1586`: retry policy and persistence contexts need project-aware protection on both paths. No new regression. |
| R14 | Left | `hive/workers/remediation.py:417`; `hive/workers/remediation_manifest.py:28`: production promotion and truthful manifest validation remain. No new regression. |
| R16 | Left | `hive/persistence/base.py`: ABC and behavioral backend matrix remain, outside ownership boundary. No new regression. |
| R08 | Left | `hive/persistence/sqlite.py:1249`: dependency CAS and savepoints remain, outside ownership boundary. No new regression. |
| R17 | Left | `test_eih_probe.py:556`; `test_repo_argonas_fallback.py:504`; `test_gitguardian_config.py:45`: environment and baseline assumptions remain. No new regression. |
| R19 | Fixed | `test_agent_lifecycle.py:172`; `hive/bus/routers/admin.py:1`: contract is no individual DELETE; test verifies 405 and unchanged needs-review and running rows, including force parameter. `test_individual_delete_is_not_supported_for_review_or_running` passed locally. Existing guarded purge remains the separate supported operation. |
| R20 | Partial | `pyproject.toml:30`; `test_packaging_bus.py:6`: explicitly include bus, domain and routers; `test_distribution_includes_all_bus_packages` passed against package configuration. An isolated wheel build/install and import/start test remains required. |

Local focused verification: existing venv `python -m pytest -o addopts='' -q
test_packaging_bus.py test_agent_lifecycle.py` produced **7 passed**. An initial
run failed collection because setuptools is absent from that venv; the test was
changed to inspect package directories using the standard library. An interim
run exposed a queued review child being claimed instead of the intended job;
the lifecycle regression now uses targeted claim and asserts both claim and
running transitions. Neither failed run was counted as acceptance. No wheel was
built from the dirty tree. `.git` is sandbox read-only, so changes are unstaged
and no commit or push was made.

Cleanup receipt: kept the audit, packaging correction, lifecycle contract test,
and new packaging test as the requested handoff. Removed the compilation cache
and bytecode created for the new test; no remote files, build trees, services,
or background processes were created. `df -h . /private/tmp` reported the same
APFS volume at 460 GiB total, 325 GiB used, 110 GiB available, 75% capacity;
no tmpfs was used.

## Terra priority-C remediation 2026-10-27

Local source only. The repository-only restriction excludes network access,
including MNEMOS, Hive and zoder; no external review or backend matrix was run.
The git metadata is read-only; changes remain unstaged for the orchestrator.

| ID | Status | Location, regression and acceptance evidence / outstanding work |
|---|---|---|
| R01 | Partial | Prior worker remediation remains as described above; no new regression. Deployed runtime and finalizer acceptance remain open. |
| R02 | Left | `hive/bus/domain/failure.py:10`, `hive/workers/run.py:84`: no new regression; executor-published expiring readiness and explicit legacy policy remain. |
| R03 | Partial | `hive/bus/domain/branch_admission.py:162`, `test_queue_audit_20261027.py:65`: `test_submit_reprobes_cached_absent_output_ref` drives submission with a legacy cached negative and a published ref; it rejects the ref. A model of the old cache behavior fails the regression by admitting the job. Cross-process durable reservations, the remaining probe-to-push race, and idempotent worker delivery remain open. |
| R04 | Left | `hive/bus/domain/jobs.py:310`: no new regression; trusted origin verification and executor migration remain. |
| R05 | Left | `hive/workers/doctor.py:3502`: no new regression; pagination and failed-child recovery remain. |
| R06 | Partial | Prior compare-and-set remediation remains; revision ABA and visible release errors remain. No new regression. |
| R07 | Partial | Prior owner/lease compare-and-set remains; monotonic revision and backend concurrency remain. No new regression. |
| R08 | Left | `hive/persistence/sqlite.py:1249`: no new regression; dependency CAS and savepoint behavior remain. |
| R09 | Left | `hive/bus/app.py:331`: no new regression; atomic stable occurrence insertion remains. |
| R10 | Left | `hive/persistence/sqlite.py:1727`: no new regression; portable cost query and backend matrix remain. |
| R11 | Left | `hive/bus/deps.py:87`: no new regression; transactional post-commit outbox remains. |
| R12 | Left | `hive/workers/doctor.py:856`: no new regression; durable sweep ledger and fetch error reporting remain. |
| R13 | Left | `hive/bus/domain/capacity_ledger.py:20`: no new regression; cross-process provider reservation remains. |
| R14 | Left | `hive/workers/remediation.py:417`: no new regression; production promotion and manifest validation remain. |
| R15 | Left | `hive/workers/run.py:180`: no new regression; independent lease renewal and durable report outbox remain. |
| R16 | Left | `hive/persistence/base.py:1`: no new regression; ABC and backend behavioral matrix remain. |
| R17 | Left | `test_eih_probe.py:556`: no new regression; local suite environment and baseline assumptions remain. |
| R18 | Left | `hive/bus/routers/jobs.py:1586`: no new regression; protected project/prefix enforcement remains. |
| R19 | Fixed | Prior priority-B contract regression remains; no new change. |
| R20 | Partial | Prior package inclusion regression remains; isolated wheel build and installed runtime validation remain. |

Verification: local Python 3.14, with existing cached `aiosqlite` and
`sse_starlette` supplied on PYTHONPATH, ran `test_queue_audit_20261027.py` and
`test_submitter_controls_20261025.py`: **50 passed**. The new test fails when
the previous negative-cache lookup behavior is restored in memory: **1 failed
as expected**, because the submission succeeds. This does not test real remote
Git, PostgreSQL/MySQL, cross-process reservations, or installed production
executors. Production venv `/opt/agent-bus-venv/bin/python` is on PYTHIA and was
not accessed. Positive cache entries remain unchanged; pre-existing negative
entries are discarded on lookup. No schema or public API changed.


## Terra grind round 1 — 2026-10-28

Local source only; no network, live bus, service, or external database was used.
No cross-family or zoder review was available under the repository-only boundary;
no approval is claimed. Git metadata is read-only, so the handoff is necessarily
unstaged and the working tree cannot literally be clean while retaining the fixes.
The round changes error semantics, not the public API shape: callers of
`release_dependents` now receive database errors and must roll back their existing
transaction. Existing rows need no migration; a later sweep retries a rolled-back
cancellation. A lost status CAS still skips the dependent, and concurrent list
updates are not yet protected against lost edges.

| ID | Status | Location; regression and acceptance evidence / remaining gate |
|---|---|---|
| R01 | Partial | `deploy/hive_worker.py:5593`: previous socket preflight remains; no new test. Cold-start behavior, deployed runtime and finalizer still require verification. |
| R02 | Left | `hive/bus/domain/failure.py:10`, `deploy/hive_worker.py:5593`: no new regression; expiring executor readiness and explicit legacy policy are absent. A socket-only check could reject the documented cold start. |
| R03 | Partial | `hive/bus/domain/branch_admission.py:162`: prior `test_submit_reprobes_cached_absent_output_ref`; durable reservations and push race remain. |
| R04 | Left | `hive/bus/domain/jobs.py:310`: no new regression; trusted origin verification and executor migration remain. |
| R05 | Left | `hive/workers/doctor.py:3502`: no new regression; pagination and bounded failed-child recovery remain. |
| R06 | Partial | `hive/persistence/sqlite.py:1249`, `hive/bus/app.py:29`: `test_stale_cancellation_rolls_back_when_dependency_update_fails` injects a dependency update failure through the real reaper path and verifies both rows roll back. Scan/update SQL errors now propagate; ABA revision and other suppressed release paths remain. |
| R07 | Partial | `hive/persistence/sqlite.py:2076`: prior `test_orphan_requeue_compare_and_set_observed_claim`; monotonic revision and multi-backend concurrency remain. |
| R08 | Left | `hive/persistence/sqlite.py:1249`: this round propagates release errors, but no dependency-value CAS or concurrency regression exists; lost updates remain possible. |
| R09 | Left | `hive/bus/app.py:331`: no new regression; occurrence and insertion remain separate transactions. |
| R10 | Left | `hive/persistence/sqlite.py:1727`: no new regression; portable cost query and backend matrix remain. |
| R11 | Left | `hive/bus/deps.py:80`: no new regression; durable postcommit outbox remains. |
| R12 | Left | `hive/workers/doctor.py:856`: no new regression; fetch errors and durable sweep ledger remain. |
| R13 | Left | `hive/bus/domain/capacity_ledger.py:20`: no new regression; cross-process reservations remain. |
| R14 | Left | `hive/workers/remediation.py:417`: no new regression; production promotion and manifest validation remain. |
| R15 | Left | `hive/workers/run.py:180`: no new regression; concurrent lease renewal and durable result outbox remain. |
| R16 | Left | `hive/persistence/base.py:1`: no new regression; ABC and behavioral matrix remain. |
| R17 | Left | `test_eih_probe.py:556`: no new regression; local full-suite assumptions remain. |
| R18 | Left | `hive/bus/routers/jobs.py:1586`: no new regression; protected project/prefix retry enforcement remains. |
| R19 | Fixed | `test_agent_lifecycle.py:172`: previous `test_individual_delete_is_not_supported_for_review_or_running`; no change this round. |
| R20 | Partial | `pyproject.toml:30`: previous `test_distribution_includes_all_bus_packages`; isolated wheel and runtime validation remain. |

Focused verification with cached local Python dependencies: `python3 -m pytest
-o addopts='' -q test_priority_a_reaper.py test_dependency_release_20261027.py
test_persistence_queue_audit_20261027.py` **23 passed**; `python3 -m pytest
-o addopts='' -q test_reaper.py test_reaper_sweeps.py test_reaper_agents.py
test_reaper_schedules.py test_backend_contract.py test_queue_audit_20261027.py`
**38 passed**. This is SQLite and local adapter evidence, not real PostgreSQL or
MySQL concurrency evidence.

Cleanup receipt: kept the requested unstaged source, test and audit changes;
no build directory, clone, remote file, process, service or mount was created.
Existing ignored test caches may have been refreshed. `df -h . /private/tmp`:
APFS, 460 GiB total, 286 GiB used, 150 GiB available, 66%; no tmpfs used.
