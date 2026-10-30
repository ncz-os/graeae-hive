# Needs-review policy (design — no code)

Status: design only. No behaviour is changed by this document. The operator
decides before any implementation.

## 1. Current behaviour (with function names)

All references are `hive/workers/doctor.py` unless noted, base
`deploy/integrate-20261017`.

- Listing: `fetch_needs_review_jobs(limit=1000)` GETs
  `/v1/jobs?status=needs-review`. Truncation (total > fetched) is logged and
  broadcast; the API has no pagination (Source: `fetch_needs_review_jobs`).
- Report-only classification: `report_needs_review_sweep()` calls
  `doctor_v2.sweep_needs_review(jobs,
  older_than_hours=NEEDS_REVIEW_OLDER_HOURS)` (default `24`, env
  `NEEDS_REVIEW_OLDER_HOURS`; sweep cadence `NEEDS_REVIEW_SCAN_INTERVAL`
  default `3600`) and for each returned row logs plus
  `broadcast_doctor_message("doctor.needs_review", ...)` (Source:
  `report_needs_review_sweep`, constants at top of `doctor.py`).
  `doctor_v2.sweep_needs_review` itself never mutates jobs; it emits
  `{job_id, classification, orchestrator_note}` where classification is
  `superseded` (same `base_kind`/`kind` has a later `done` row),
  `stale` (old, no `head_branch`), or `actionable` (Source:
  `hive/workers/doctor_v2.py:sweep_needs_review`).
- Verdict completion (the only writer on this sweep):
  `complete_stuck_verdict_jobs(jobs)` → `complete_verdict_job(job)` heals
  rows where `failure_reason == codex_delivery_failed` (or tail says
  "produced no workspace edits") but `result.tail` already holds a verdict
  JSON (`extract_verdict_from_tail` takes the LAST object with a `verdict`
  key; `build_verdict_completion_result` crafts a read-only `done` result).
  Markers: success `verdict_completion:<jid>` TTL 24 h (written only after
  bus PATCH 200/201/204); failure leaves only `verdict_completion_retry:<jid>`
  10 min; dry-run via `DOCTOR_VERDICT_COMPLETION_DRY_RUN=1` still records the
  success marker (Source: `_is_verdict_completion_candidate`,
  `extract_verdict_from_tail`, `build_verdict_completion_result`,
  `complete_verdict_job`, `complete_stuck_verdict_jobs`).
- Review follow-up: `run_needs_review_sweep()` = report + 
  `run_review_followup_sweep()` → `ensure_review_followup(job)` (Source:
  `run_needs_review_sweep`, `run_review_followup_sweep`,
  `ensure_review_followup`).
  - Skips: non-needs-review status; `triage:*` kinds; `dlq_protected` or
    routing-only rows (`review_followup_protected`); no durable state;
    rows with `result.verdict`/`review_evidence`/`auto_review_job_id`/child
    `review:*` jobs (Source: `ensure_review_followup`,
    `review_followup_row_has_verdict`, `_needs_review_child_review_jobs`).
  - Else submits ONE read-only review via `_submit_review_followup` with
    stable `idempotency_key: needs-review-followup:<jid>`, then
    `_record_auto_review` (durable `STATE.mark("review:submitted:<jid>")`
    TTL 30 d + PATCH `result.auto_review_job_id`/`doctor_reason`) and
    broadcasts `doctor.needs_review` (Source: `_submit_review_followup`,
    `_record_auto_review`, `review_followup_submitted`).
  - Cap: oldest-first, `REVIEW_FOLLOWUP_MAX_PER_SWEEP` (default 3, env)
    per sweep; beyond-cap rows return `deferred` (Source:
    `run_review_followup_sweep`).
- Parking into needs-review happens elsewhere and is one-shot:
  `_park_for_review(job, cls)` POSTs `/v1/admin/jobs/reclassify` once per
  `loop_park:<jid>` TTL window (Source: `_park_for_review`); the orphan
  sweep `sweep_orphan_failures()` parks `loop_outcome.decide()==
  needs_review` rows the same way (Source: `sweep_orphan_failures`).
  `loop_outcome.classify`/`decide` map `ZODER_LOOP_RESULT` teardown reasons
  and `zoder-loop-events` to `requeue`/`hold`/`needs_review`/`skip`/`default`
  (Source: `hive/workers/loop_outcome.py:classify`, `decide`,
  `fetch_loop_events`).
- Durable state: `STATE = state_store_from_env()`; every dedup/marker above
  is `STATE.seen`/`mark`/`touch` with a TTL (Source: `doctor.py:STATE` and
  each function cited).
- Invariant comment: "a needs-review row is NEVER auto-purged: if it carries
  commits, those are delivered work" (Source: needs-review follow-up header
  comment and `run_needs_review_sweep` docstring).

### Review children that advance the branch

A `review:*` child is a review-and-fix loop, not a read-only pass,
whatever its description says. On the live bus every observed
review child had `result.base_sha` equal to the parent's delivered
`head_sha` and a different `result.head_sha`/`remote_sha`: it pushed
new commits. Its approval therefore names a commit the parent never
delivered. The bus consumer
(`hive/bus/domain/review_dispatch.py:reconcile_review_parent`) reads
the verdict from `result.reviewer.verdict` when `result.verdict` is
null, closes the parent only when the reviewed head equals the
delivered head, and otherwise keeps it `needs-review` with
`review_evidence.head_matches: false`, `child_advanced_branch: true`
and `approved_unmerged_tip` (the approved tip). `approved_unmerged_tip`
is an evidence field, not a status. The doctor treats a row with
`review_evidence` as reviewed (`review_followup_row_has_verdict`), so it
will not submit a second review: confirming the tip before merging is
for a human or the owner. Rules and outcomes: `docs/JOB_LIFECYCLE.md`,
"Review reconcile outcomes".

## 2. Failure modes seen

- Rows sit in `needs-review` forever: the sweep is report-only by design
  (classify + broadcast, no state change except the two narrow writers
  above). On 2026-10-29 two rows sat for hours: (a) work already merged into
  its target — no writer checks merged-ness; (b) superseded by a newer job on
  the same head branch — `sweep_needs_review` only detects same-kind-later-done
  inside the fetched window and only reports it.
- `sweep_needs_review` superseded signal is weak: same `kind`/`base_kind`
  match, not same `head_branch`; cross-window and cross-fetch supersession is
  invisible; and it never acts (Source:
  `doctor_v2.py:sweep_needs_review`).
- No merged-into-target check exists anywhere on this path (Source: grep of
  `doctor.py` needs-review functions shows no merge/branch-comparison call).
- No age-out for doctor-parked rows: `_park_for_review` has no re-visit;
  `NEEDS_REVIEW_OLDER_HOURS` only gates the report, not resolution.
- Backlog truncation risk: `fetch_needs_review_jobs` warns when
  `total > fetched` but has no pagination, so oldest-first follow-up can miss
  rows beyond 1000 (Source: `fetch_needs_review_jobs`).

## 3. Proposed policy

Classes below are checked in order; first match wins. All checks are
read-only until §6's conservative step is lifted.

### 3a. Safe to auto-resolve (after operator approval of this doc)

1. **Work already merged into its target.**
   Condition: `head_branch`'s reviewed `head_sha` (from
   `result.head_sha`/`result.verdict.head`/`result.commits`) is an ancestor
   of the current `base_branch` tip (merge-base == head_sha), or the exact
   diff is empty against target. Evidence: target tip sha, merge-base sha,
   diff stat. Action: PATCH row to `done` with
   `result.doctor_resolution={action:"auto_merged",...}`; never delete.
2. **Superseded by a newer job on the same head branch.**
   Condition: same `repo` + same `head_branch`, a strictly newer job
   (greater `created_at`/`ended_at`) on that branch reached `done`, and this
   row's `head_sha` is an ancestor of (or equal to and older than) the newer
   row's reviewed sha. Same-branch is required — same-kind alone (today's
   heuristic) is not enough. Evidence: both job ids, branch, both shas,
   newer row's `done` timestamp. Action: PATCH to `done` (or a new
   `superseded` terminal status if the bus gains one) with
   `result.doctor_resolution={action:"auto_superseded", superseded_by:<id>}`.
3. **Doctor-parked and stale.**
   Condition: row entered needs-review via `_park_for_review`/orphan sweep
   (`result.doctor_reason`/`doctor.needs-review` broadcast or
   `failure_reason` in the loop-outcome set), no human comment/marker since,
   no commits not already covered by (1)/(2), and age > N days (propose N=7,
   env `NEEDS_REVIEW_AUTO_RESOLVE_DAYS`, default 7). Evidence: park timestamp,
   park reason, age. Action: PATCH to `done` with
   `result.doctor_resolution={action:"auto_expired_park", age_days:N}`.
   Rows WITH commits outside (1) stay human-only (delivered work invariant).

### 3b. Must wait for a human

- Any row with commits/deliverables not proven merged under (1).
- Any row with a real `request-changes`/`blocked` verdict and no superseding
  `done` under (2).
- `triage:*`, `dlq_protected`, routing-only, or `review_is_never_act` rows
  (today's `review_followup_protected` set).
- Verdict-completion candidates that fail to parse (no verdict in tail).
- Anything where the merge/supersession evidence fetch fails (fail-closed:
  leave for human, do not auto-resolve on error).

## 4. Evidence recorded on each decision

On every auto-resolution PATCH, set (merging into existing `result`, never
dropping fields):
`result.doctor_resolution = {action, at, by:"doctor-needs-review-sweep",
evidence:{...}, prior_status:"needs-review"}` plus `result.doctor_reason`
human line. Broadcast `doctor.needs_review_resolved` with
`{job_id, action, evidence_summary, superseded_by?}`. Keep the row (never
DELETE); status change is the only mutation.

## 5. Idempotency

- Pre-check: if `result.doctor_resolution.action` already set, skip (no-op).
- Guard marker per action: `STATE.mark("needs-review-resolved:<jid>",
  ttl=30 d)` written only after bus PATCH 200/201/204; failed PATCH writes
  only a short `needs-review-resolve-retry:<jid>` 10-min backoff (same split
  as `complete_verdict_job`'s success vs retry keys).
- Stable `idempotency_key` on any child job (reuse the
  `needs-review-followup:<jid>` pattern).
- Replays (restart, double sweep) observe marker/result and return
  `skipped_marker`/`skipped_resolved` with no bus traffic.
- Dry-run env (`DOCTOR_NEEDS_REVIEW_RESOLVE_DRY_RUN=1`, mirroring
  `DOCTOR_VERDICT_COMPLETION_DRY_RUN`) logs + records marker, no PATCH —
  matching the existing DLQ/verdict dry-run semantics.

## 6. First conservative step (report before any write)

Ship only a reporter first: extend `report_needs_review_sweep` output rows
with `{proposed_action, evidence, confidence}` for (1)-(3) and broadcast
`doctor.needs_review_proposal`; take no PATCH path. The operator reviews
proposals against the bus for at least one week of sweeps before enabling
any writer. The writer, when approved, is a separate change behind the
dry-run flag above defaulting to report-only.

## 7. Test plan (fixtures, no live bus)

- Fixture builder: needs-review rows covering merged (head_sha ancestor of
  target tip), superseded (same-branch newer done), stale park (old
  `loop_park` + age), with-commits-unmerged (must stay), blocked verdict
  (must stay), unparseable tail (must stay), plus bus doubles for merge-base
  and job-listing endpoints.
- Unit: (a) merged detector true/false incl. fetch-failure fail-closed;
  (b) supersession requires same repo+branch+newer done (same-kind-different-branch
  stays); (c) stale-park age boundary N days ± 1 s; (d) protected/triage rows
  never proposed; (e) evidence payload shape exact; (f) idempotency: second
  sweep no-ops (marker + `doctor_resolution` pre-check), failed PATCH leaves
  only retry marker; (g) dry-run writes marker, no PATCH.
- Sweep-level: `sweep_needs_review`-style oldest-first ordering preserved;
  truncation (>1000 rows) surfaces `backlog_truncated` instead of silently
  resolving a partial window.
- Regression: existing `test_doctor_*needs_review*`, `test_*verdict_completion*`,
  `test_*review_followup*` suites stay green; add `test_needs_review_policy_*`
  in the same style (fake `_http`, fake `STATE`, frozen time).
