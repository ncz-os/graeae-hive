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
| queued | claimed | claim endpoint only (PATCH refuses: claim bypass) |
| queued | cancelled | PATCH |
| offered | queued | PATCH |
| offered | claimed | claim endpoint only (PATCH refuses: claim bypass) |
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
