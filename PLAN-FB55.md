# PLAN — FB55: reaper and lease edge cases (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `f50f9d7`. Branch `freebuff/fb55-20261030`.
Extends FB37 (lease-expiry-vs-renew race); no production code changes.

## Mechanics (verified against this base)

- Renew = `PATCH` with `status: claimed/running` by the current claimant; sets
  `claim_lease_expires_at = now + CLAIM_LEASE_SECONDS` (1800 s) with NO lease
  precondition (`jobs.py:1551-1552`; `claimed→claimed` is legal per
  `STATUS_TRANSITIONS`, `domain/jobs.py:188`). A renew always succeeds while
  the row is still claimed/running by this claimant — even at/past expiry.
- Reaper = `select_orphan_claim_candidates` SQL (lease `<= now` flags regardless
  of agent liveness) + pure `select_orphan_claims` (`lease_exp <= now` reaps at
  exact equality; negative `now - latest` from a backward clock never reaps) +
  `requeue_orphan_job` full-row-snapshot CAS (loses if anything changed).
- Heartbeat touches only `agents.last_heartbeat`; never the job lease.

## The four scenarios → tests (`test_reaper_lease_edges_20261031.py`)

1. **Renew exactly at expiry.** Fake clock T0: claim (lease T0+1800).
   (a) renew PATCH at exactly T0+1800 succeeds, lease extends to T0+3600, and a
   reaper scan at T0+1800 finds no orphan. (b) reversed: scan at T0+1800 flags
   the orphan, snapshot taken, renew lands first, requeue CAS loses (rowcount 0)
   — job stays claimed with the fresh lease. Both orders pinned.
2. **Clock goes backwards.** Pure-function half: `select_orphan_claims` with
   `now` before `claimed_at`/`last_update_at` never reaps (negative durations,
   all three rules). Router half (scoped `mock.patch("time.time")` around one
   PATCH only): claim at T0, clock back to T0−3600, renew → lease T0−1800, which
   is already expired against the real clock → reaper flags a live worker's job.
   Recorded as FINDING FB55-1 (renew trusts the wall clock; no monotonic guard).
3. **Heartbeats but never renews.** Claim at T0, heartbeat agent repeatedly
   (agent stays `online`), advance past lease → reaper candidate select +
   `select_orphan_claims` flag it; requeue succeeds; agent row untouched.
   Pins that liveness and lease are independent by design.
4. **Many orphans at once.** 60 claimed jobs, agents offline, leases expired
   (mixed: some lease-expired, some stale, some hard-cap, some phantom NULL
   claimed_at). One pass: all 60 requeued exactly once (`requeue_orphan_job`
   rowcount 1 each; second pass finds zero), 60 `job.unclaimed` events.
   Deterministic (fake clock constants, no sleeps/threads).

## Risks

- `mock.patch("time.time")` in test 2 is scoped to the single renew PATCH call;
  sqlite/aiohttp internals use monotonic for timeouts, so a frozen wall clock
  for one await is safe. If flaky, fall back to asserting the arithmetic
  (lease = faked_now + 1800 < real now) without the live reaper check.
- `update_job` needs the router's module-level `hive_repo()` patched to the temp
  repo (pattern from `test_dependency_release_20261027.py`) plus `emit_event`
  mocked. Renew call shape: `JobUpdate(status="claimed", claimed_by=urn)`.
- Full suite ~150 s; run file first, then full suite once.
