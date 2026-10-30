# Job lifecycle

Every job status and every legal transition, derived from the bus code.
Citations are `file:function`. Placeholders only; no addresses, no tokens.

Status set: `hive/bus/deps.py:VALID_JOB_STATUSES` — `cancelled`,
`dead-letter`, `done`, `failed`, `failed_completion`, `needs-review`,
`offered`, `queued`, `running`, `claimed` (10). `released` is not a
stored status (FB21: removed from the valid set; the doctor used the
word as an in-memory variable only, and the sqlite CHECK never
accepted it).

Terminal: `hive/bus/deps.py:TERMINAL_JOB_STATUSES` — `done`, `failed`,
`cancelled`, `dead-letter`. In-flight: `IN_FLIGHT_JOB_STATUSES` —
`queued`, `offered`, `claimed`, `running`. Dependents can never be
satisfied by `DEP_DEAD_STATUSES` — `failed`, `failed_completion`,
`dead-letter`, `cancelled` (`hive/bus/deps.py`).

## Who triggers what

- Submit: `POST /v1/jobs` creates `queued`
  (`hive/bus/routers/jobs.py:create_job`). Requires an orchestrator
  submitter; `max_retries: 0` needs a `zero_retry_reason`
  (`hive/bus/schemas.py:JobCreate`).
- Claim: `POST /v1/jobs/next` or `POST /v1/jobs/{id}/claim` moves
  `queued`/`offered` to `claimed`
  (`hive/bus/routers/jobs.py:dequeue_next_job`, `claim_job`).
- Lease/renew, progress: `PATCH` stays in `claimed`/`running`
  (`hive/bus/routers/jobs.py:update_job`).
- Complete: `PATCH` to `done` with commit evidence
  (`hive/bus/domain/jobs.py:valid_completion_proof`; read-only kinds use
  `valid_readonly_completion_proof`). Without proof the bus stores
  `needs-review` with `completion_evidence_missing` but answers HTTP 409
  (not 200) with the same reason
  (`hive/bus/routers/jobs.py:update_job`, `completion_rejected`).
- Fail: `PATCH` to `failed`. With `retryable is not False` and budget
  left, the bus auto-retries to `queued` with backoff instead
  (`hive/bus/routers/jobs.py:update_job`,
  `retry_policy.py:auto_retry_allowed`). Infrastructure-shaped failures
  reroute to `queued` with evidence preserved
  (`hive/bus/domain/failure.py:infrastructure_failure`).
- Requeue: `POST /v1/admin/jobs/requeue` moves terminal or queued rows
  back to `queued`, resetting claim/retry/backoff
  (`hive/bus/routers/admin.py:admin_requeue_jobs`). Budget-spent rows are
  skipped unless `override_max_retries` with a reason is set.
- Cancel: `PATCH` to `cancelled` by the claimant, the submitter, or an
  orchestrator-capable agent
  (`hive/bus/routers/jobs.py:update_job`); `done`/`failed` may only be
  retired as `cancelled`, never reopened (same function).
- Release: `PATCH` to `queued` from `claimed`/`running`/`offered` by the
  current claimant (`hive/bus/routers/jobs.py:update_job`).
- Reclassify: `POST /v1/admin/jobs/reclassify` overrides to
  `needs-review`, `cancelled`, or `queued`
  (`hive/bus/routers/admin.py:admin_reclassify_jobs`;
  `RECLASSIFY_ALLOWED_TARGETS`).
- Retry budget: `max_retries`/`retry_count`; the doctor and the bus
  refuse requeues when spent (`hive/bus/routers/admin.py:_budget_row`,
  `hive/workers/doctor.py:retry_budget_exhausted`).
- Dead letter: terminal quarantine; only an idempotent same-status
  `PATCH` is accepted (`hive/bus/domain/jobs.py:STATUS_TRANSITIONS`).

## PATCH transition table

The machine-readable contract. Source:
`hive/bus/domain/jobs.py:STATUS_TRANSITIONS`, enforced by
`hive/bus/routers/jobs.py:update_job`
(`invalid status transition ... -> ...` is HTTP 409). Columns are exact
status strings; the test parses this table.

| from | to | via |
| ---- | -- | --- |
| queued | queued | PATCH |
| queued | offered | PATCH |
| queued | cancelled | PATCH |
| offered | queued | PATCH |
| offered | cancelled | PATCH |
| claimed | queued | PATCH (release; claimant must match) |
| claimed | claimed | PATCH |
| claimed | running | PATCH |
| claimed | done | PATCH (commit evidence or read-only proof required) |
| claimed | failed | PATCH |
| claimed | cancelled | PATCH |
| claimed | needs-review | PATCH |
| running | queued | PATCH (release; claimant must match) |
| running | running | PATCH |
| running | done | PATCH (commit evidence or read-only proof required) |
| running | failed | PATCH |
| running | cancelled | PATCH |
| running | needs-review | PATCH |
| needs-review | needs-review | PATCH |
| needs-review | done | PATCH |
| needs-review | failed | PATCH |
| needs-review | cancelled | PATCH (claimant must match; an unclaimed row cannot be submitter-cancelled) |
| done | done | PATCH |
| done | cancelled | PATCH (retire fake/duplicate completions) |
| failed | failed | PATCH |
| failed | cancelled | PATCH (retire superseded failures) |
| cancelled | cancelled | PATCH |
| dead-letter | dead-letter | PATCH |
| failed_completion | failed_completion | PATCH (legacy terminal; same-status only) |

### Claim-only transitions (not PATCH cells)

Claiming never goes through PATCH: `queued`/`offered` to `claimed`
happens only via `POST /v1/jobs/next` (atomic next-claim CAS) and
`POST /v1/jobs/{id}/claim` (direct CAS), not via `STATUS_TRANSITIONS`.
PATCH refuses these pairs with 409 (claim bypass rule). They are listed
here, outside the parsed table above, so the doc stays honest both ways.

| source | target | endpoint |
| ------ | ------ | -------- |
| queued | claimed | POST /v1/jobs/next, POST /v1/jobs/{id}/claim |
| offered | claimed | POST /v1/jobs/next, POST /v1/jobs/{id}/claim |

Notes: `queued`/`offered` to `running` is refused everywhere except
through a claim first (claim bypass rule,
`hive/bus/routers/jobs.py:update_job`). `done` without proof is stored
as `needs-review` (completion gate, same function). `failed` without
`retryable: false` may auto-retry to `queued` instead of staying
`failed` (same function). Requeue/reclassify of `failed`/`cancelled`/
`needs-review` to `queued` bypass this table via the admin endpoints
above.

## State diagram

```mermaid
stateDiagram-v2
    [*] --> queued: submit
    queued --> offered: offer
    queued --> claimed: claim
    queued --> cancelled: cancel
    offered --> queued: release
    offered --> claimed: claim
    offered --> cancelled: cancel
    claimed --> running: progress
    claimed --> queued: release
    claimed --> done: complete
    claimed --> failed: fail
    claimed --> needs-review: park
    claimed --> cancelled: cancel
    running --> queued: release
    running --> done: complete
    running --> failed: fail
    running --> needs-review: park
    running --> cancelled: cancel
    failed --> queued: admin requeue
    failed --> failed: fail
    failed --> cancelled: retire
    needs-review --> done: review pass
    needs-review --> failed: review fail
    needs-review --> cancelled: review cancel
    needs-review --> queued: admin requeue
    done --> cancelled: retire
    done --> [*]
    cancelled --> [*]
    dead-letter --> dead-letter
    dead-letter --> [*]
```

## Review reconcile outcomes (`review:*` children)

A completed `review:*` child (status `done`, has `parent_job_id`) is
consumed by `hive/bus/domain/review_dispatch.py:reconcile_review_parent`,
called from the `done` branch of `hive/bus/routers/jobs.py:update_job`
in the same transaction as the child's status write. It acts only on a
parent that is `needs-review`; it never adds a job status. Rows it skips
(untouched): parent not `needs-review`, `triage:*` parent, protected row
(`doctor_decline_paths.review_is_never_act`), or a parent whose
`result.auto_review_job_id` names a different review job.

The child's verdict is normalised first
(`review_dispatch.py:child_verdict`):

- legacy shape: `result.verdict` is a dict with a string `verdict`; used
  as is (`head` or `head_sha` names the reviewed commit).
- worker shape: `result.verdict` is null and the verdict is
  `result.reviewer.verdict` (string, case-insensitive; written by
  `hive/review_record.py`). The reviewed head is `result.remote_sha`
  when `result.remote_verified` is true, else `result.head_sha`.
- neither: no usable verdict (see below).

The close rule is unchanged: the parent becomes `done` only when the
verdict is `approve` AND the reviewed head equals the parent's
delivered `result.head_sha` AND
`hive/bus/domain/jobs.py:valid_completion_proof` holds for the parent's
result. Every outcome with a verdict writes `result.review_evidence`, sets
`result.auto_review_job_id`, and appends one `job_audit_log` row and one
`job.review_completed` event. Reconciling the same child again with
identical evidence writes neither.

| child result | parent status | `review_evidence` / reason |
| ------------ | ------------- | -------------------------- |
| approve, reviewed head == delivered head, parent proof valid | `done` | `head_matches: true`; "approved exact delivered HEAD" |
| approve, reviewed head == delivered head, parent proof invalid | `needs-review` | `artifact_valid: false`; "parent artifact evidence incomplete" |
| anything but approve | `needs-review` | "review did not approve" |
| approve, no reviewed head or no delivered head | `needs-review` | `head_matches: false`; "review HEAD does not match delivered HEAD" |
| approve, reviewed head != delivered head | `needs-review` | `head_matches: false`, `reviewed_head`, `delivered_head`, `child_advanced_branch`; reason below |
| no usable verdict | unchanged | `{review_job_id, verdict: null, reason: "review result has no usable verdict"}`; one log line |

Approved but a different head: the review job is a review-and-fix loop
and may have pushed new commits. `child_advanced_branch` is `true` when
the child's `result.base_sha` equals the delivered head (and its head
differs); then the reason is "approved, but the review job advanced the
branch: reviewed head <12 chars> differs from delivered head <12 chars>;
a human or the owner must confirm the branch tip before merging" and
`approved_unmerged_tip` carries the reviewed head. Without that base the
bus cannot say the branch was advanced, and the reason omits that
clause. In both cases `result.doctor_reason` equals the reason and
`result.needs_review` is `true`. `approved_unmerged_tip` is an evidence
field only, not a status.

No usable verdict is never silent: one structured `WARNING` line from
logger `hive.bus.review_dispatch` per distinct (review job id, reason),
`review_reconcile review_job_id=... parent_job_id=... reason="..."
outcome=...`, plus the evidence record on the parent (only when the
parent is `needs-review` and not skipped). Status, `doctor_reason` and
`auto_review_job_id` are left as they were. Because the parent now
carries `review_evidence`, neither the bus dispatch nor the doctor
submits another review for it; a human or the owner decides.

## Findings (code-path inconsistencies; production code untouched)

1. `released` and `failed_completion` are members of
   `VALID_JOB_STATUSES` but have no entry in `STATUS_TRANSITIONS`, so a
   `PATCH` to or from either is schema-valid yet always refused with
   409 (`hive/bus/deps.py` vs `hive/bus/domain/jobs.py`). No router
   writes `released` as a stored status (grep finds no assignment), so
   it is currently unreachable.
2. `queued → claimed` and `offered → claimed` are listed in the map but
   refused via `PATCH` by the claim-bypass rule; only the claim
   endpoints perform them (`hive/bus/routers/jobs.py:update_job`).
3. Asking for `done` without proof stores `needs-review`: the endpoint
   behavior differs from the map cell, which lists `done`
   (`hive/bus/routers/jobs.py:update_job`, completion gate).
