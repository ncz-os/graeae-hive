# Authorization matrix: who may call every route

For every route in the bus OpenAPI schema: the declared dependency (what the
code says guards it) and the effective gate (what actually happens), verified
against `deploy/integrate-20261017` (`f50f9d7`) and enforced live by
`test_auth_matrix_20261031.py`, which parses the machine-readable table below
and the app's OpenAPI paths and asserts the two sets are equal, then asserts
each cell in-process.

Roles: **anon** (no credential of any kind), **registered** (agent row exists),
**active** (status online/idle), **claimant** (matches the job's `claimed_by`),
**submitter** (matches the job's `submitter_urn`), **orchestrator** (agent row
carries the orchestrator capability), **admin-tier** (valid
`HIVE_BUS_ADMIN_TOKEN` bearer). `require_mutation_actor` = X-Hive-Actor naming
a registered active agent (+ `X-Hive-Token` matching `HIVE_SHARED_SECRET` when
set). Unless otherwise noted, gates were verified with the secret/token env
vars UNSET, so only the actor half applies.

`<!-- FB56-MATRIX-START -->`

| Method | Path | Gate |
|---|---|---|
| GET | / | anon |
| GET | /health | anon |
| POST | /mcp | passthrough |
| POST | /v1/admin/jobs/purge | any-active-agent |
| POST | /v1/admin/jobs/reclassify | any-active-agent |
| POST | /v1/admin/jobs/requeue | any-active-agent |
| GET | /v1/admin/jobs/{job_id}/history | any-active-agent |
| GET | /v1/agents | anon |
| GET | /v1/agents/stats | anon |
| GET | /v1/agents/{urn_path}/throttle | anon |
| GET | /v1/agents/whoami | anon |
| GET | /v1/hosts | anon |
| POST | /v1/agents/heartbeat | registered |
| POST | /v1/agents/register | anon |
| POST | /v1/agents/{urn_path}/deregister | admin-or-self |
| POST | /v1/agents/{urn_path}/plan-reset | anon |
| GET | /v1/events | anon |
| GET | /v1/events/log | anon |
| PATCH | /v1/jobs/{job_id} | claimant-or-submitter-cancel |
| GET | /v1/jobs | anon |
| GET | /v1/jobs/metrics | anon |
| GET | /v1/jobs/summary | anon |
| GET | /v1/jobs/{job_id} | anon |
| GET | /v1/jobs/{job_id}/preview | anon |
| POST | /v1/jobs | orchestrator-submitter-urn |
| POST | /v1/jobs/next | registered-active |
| POST | /v1/jobs/purge | any-active-agent |
| POST | /v1/jobs/{job_id}/auto-retry | anon |
| POST | /v1/jobs/{job_id}/claim | registered-active |
| POST | /v1/jobs/{job_id}/release | anon |
| GET | /v1/messages | anon |
| POST | /v1/messages | registered-active-from |
| GET | /v1/repos | anon |
| GET | /v1/repos/resolve | anon |
| GET | /v1/repos/{repo_id} | anon |
| DELETE | /v1/schedules/{sid} | any-active-agent |
| GET | /v1/schedules | anon |
| PATCH | /v1/schedules/{sid} | any-active-agent |
| POST | /v1/schedules | any-active-agent |
| GET | /v1/stats/cache | anon |
| GET | /v1/stats/costs | anon |
| GET | /v1/stats/workers | anon |

`<!-- FB56-MATRIX-END -->`

Gate glossary (effective behaviour, not aspiration):

- **anon**: no credential checked. Reads, registration, heartbeat, plan-reset,
  auto-retry, release trigger, and one submit path (see below) are all anon.
- **passthrough** (`POST /mcp`): dispatches to handler functions directly, so
  each method inherits its handler's gate (`job.create` → orchestrator-
  submitter-urn, `message.publish` → registered-active-from, the rest anon);
  unknown methods 404. No transport-level gate of its own.
- **registered** (heartbeat): the URN must have an agent row (409 otherwise);
  status is not re-checked.
- **registered-active** (`/v1/jobs/next`, `/claim`, `from_urn` on messages):
  409/403 unless the agent is registered AND online/idle. Dequeue and claim
  both refuse unknown (409) and stale/offline (409) agents.
- **orchestrator-submitter-urn** (`POST /v1/jobs`): the CALLER is anonymous —
  the check is on the body's `submitter_urn`, which must be a registered
  active agent whose runtime is an orchestrator runtime (worker runtimes 403).
  No proof that the caller owns that URN (FB56-3).
- **claimant-or-submitter-cancel** (`PATCH /v1/jobs/{id}`): writes need
  `claimed_by` matching the current claimant (403 otherwise); a submitter (or
  orchestrator-capable agent via `cancel_actor`) may move to `cancelled` with
  only status/reason/cancel_actor; eligible_* reassignment only on
  queued/offered; claim bypass via PATCH is refused (409).
- **admin-or-self** (deregister): `is_admin_tier` bearer OR X-Hive-Actor == the
  target URN (+ shared secret if set). 401 with neither, 403 for a third agent.
- **any-active-agent** (the five `require_mutation_actor` routes + schedules
  mutations): any registered active agent. NOT admin-tier despite the
  `/v1/admin` prefix (FB56-4).

## Findings (behaviour differs from what the dependency or path declares)

- **FB56-1 (open, strongest): `POST /v1/agents/{urn}/plan-reset` has no gate
  at all.** Anyone can zero any agent's `plan_period_used_usd`, defeating the
  subscription throttle that claim-time spend accounting enforces. The test
  pins the anonymous-200 so the fix has a tripwire.
- **FB56-2 (open): `POST /v1/jobs/{job_id}/auto-retry` has no gate at all.**
  Anyone can consume a failed job's retry budget and bounce it to `queued`
  (the policy check is on the ROW, not the caller). Pinned as anonymous-200.
- **FB56-3 (open, needs operator call): submitter impersonation by design?**
  `POST /v1/jobs` authenticates nobody — only the body's `submitter_urn` is
  checked for orchestrator runtime. Any caller can submit jobs attributed to
  any orchestrator. If the bus network is trusted this is fine; if not, the
  submit path needs caller authentication. Pinned: anonymous submit AS the
  orchestrator succeeds; worker-runtime submitter 403s.
- **FB56-4 (open): `/v1/admin/*` and both purge routes are any-agent, not
  admin.** The path prefix promises admin-only, `is_admin_tier` exists and is
  used by deregister, but these five routes declare only
  `require_mutation_actor`. Any active worker can requeue / reclassify /
  dead-letter-purge / bulk-purge. Pinned: plain-worker X-Hive-Actor gets 200.
- **FB56-5 (open): `POST /v1/jobs/{job_id}/release` is anonymous.** It mints a
  `release:<repo>` job (submitted as the active orchestrator) for any job with
  a non-blocked preview bundle. Low blast radius (still needs a worker to
  claim it) but unauthenticated job creation. Pinned as anonymous-200/404.
