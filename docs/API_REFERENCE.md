# Hive Bus API Reference

Generated from the live FastAPI app (`app.openapi()` cross-checked
against `hive/bus/deps.py` and `hive/bus/routers/`). Do not edit by
hand: run `scripts/gen_api_reference.py` (or `--check` in tests).
No addresses, no tokens; examples use a placeholder host.

Base URL: `http://<bus-host>:5005`
App: GRAEAE Hive Mind 0.1.0

Auth tiers used below:

- `admin tier`: `Authorization: Bearer` with the bus admin token.
- `X-Hive-Actor`: header naming a registered, active agent.
- `X-Hive-Token`: required with `X-Hive-Actor` when `HIVE_SHARED_SECRET` is set.
- `none`: no header; the route gates on its payload instead.

## `GET /`

Handler: `dashboard`. Dashboard — Minimal HTML+JS dashboard. Auto-refreshes /v1/agents + /v1/jobs + /v1/stats/* + SSE.

Auth: none

Parameters: none.

Body: none.

Status codes: 200, 404 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/`

## `GET /health`

Handler: `health`. Health — Liveness — 200 only if the DB probe succeeds AND the reaper is running. Returning 200 unconditionally (as the argonas line did) hides a downed DB or a stopped reaper from any external monitor. 503 car

Auth: none

Parameters: none.

Body: none.

Status codes: 200, 503 (raised explicitly in code: 503)

Example: `curl http://<bus-host>:5005/health`

## `POST /mcp`

Handler: `mcp_rpc`. Mcp Rpc

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters: none.

Body type: object.

Status codes: 200, 400, 403, 404, 409, 422 (raised explicitly in code: 400, 403, 404, 409, 422)

Example: `curl http://<bus-host>:5005/mcp`

## `POST /v1/admin/jobs/purge`

Handler: `admin_purge_jobs`. Admin Purge Jobs — Permanently remove terminal dead-letter jobs (doctor 'remove' action). Safety: ONLY purges jobs currently in 'dead-letter'. Anything else (active, or other-terminal done/failed/cancelled) is refused s

Auth: X-Hive-Actor (admin operation; must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |

Body (`PurgeRequest`):

| field | required | type |
| --- | --- | --- |
| `job_ids` | True | array |
| `reason` | False | string (default 'doctor-dlq-remove') |

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/admin/jobs/purge`

## `POST /v1/admin/jobs/reclassify`

Handler: `admin_reclassify_jobs`. Admin Reclassify Jobs — Admin override to move terminal (esp. dead-letter) jobs to a safe target status, bypassing ALLOWED_TRANSITIONS. The doctor uses status='needs-review' to surface ambiguous dead-letter jobs for a human.

Auth: X-Hive-Actor (admin operation; must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |

Body (`ReclassifyRequest`):

| field | required | type |
| --- | --- | --- |
| `job_ids` | True | array |
| `override_max_retries` | False | boolean (default False) |
| `override_reason` | False | string | null |
| `reason` | False | string (default 'doctor-dlq-reclassify') |
| `status` | True | string |

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/admin/jobs/reclassify`

## `POST /v1/admin/jobs/requeue`

Handler: `admin_requeue_jobs`. Admin Requeue Jobs — Force-requeue terminal (failed/cancelled) or any non-done jobs back to queued. A job with no retry budget left (max_retries 0, or retry_count >= max_retries) is skipped into ``skipped_retry_budget`` u

Auth: X-Hive-Actor (admin operation; must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |

Body (`RequeueRequest`):

| field | required | type |
| --- | --- | --- |
| `job_ids` | True | array |
| `override_max_retries` | False | boolean (default False) |
| `override_reason` | False | string | null |
| `reason` | False | string (default 'manual-requeue') |

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/admin/jobs/requeue`

## `GET /v1/admin/jobs/{job_id}/history`

Handler: `admin_job_result_history`. Admin Job Result History — Read-only view of the result payloads an admin requeue archived (FB51). Before this existed there was no way to see the ``hive_reviews`` evidence a force-requeue had destroyed; now every cleared paylo

Auth: X-Hive-Actor (admin operation; must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |
| `job_id` | path | True | string |
| `limit` | query | False | integer (default 50) |

Body: none.

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/admin/jobs/{job_id}/history`

## `GET /v1/agents`

Handler: `list_agents`. List Agents

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `cost_tier` | query | False | string | null |
| `host` | query | False | string | null |
| `include_offline` | query | False | boolean (default False) |
| `kind` | query | False | string | null |
| `pid` | query | False | integer | null |
| `runtime` | query | False | string | null |
| `status` | query | False | string | null |

Body: none.

Status codes: 200, 422 (raised explicitly in code: 422)

Example: `curl http://<bus-host>:5005/v1/agents`

## `POST /v1/agents/heartbeat`

Handler: `heartbeat`. Heartbeat

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters: none.

Body (`AgentHeartbeat`):

| field | required | type |
| --- | --- | --- |
| `metadata` | False | object | null |
| `status` | False | string (default 'online') |
| `urn` | True | string |

Status codes: 200, 409, 422 (raised explicitly in code: 409, 422)

Example: `curl http://<bus-host>:5005/v1/agents/heartbeat`

## `POST /v1/agents/register`

Handler: `register`. Register

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters: none.

Body (`AgentRegister`):

| field | required | type |
| --- | --- | --- |
| `auth_method` | False | string (default 'unknown') |
| `autonomy_level` | False | string (default 'unknown') |
| `capabilities` | False | array | null |
| `host` | True | string |
| `instance` | False | string | null |
| `kind` | False | string | null |
| `metadata` | False | object | null |
| `model` | False | string (default 'unknown') |
| `pid` | False | integer | null |
| `plan_cap_usd` | False | number | null |
| `provider` | False | string (default 'unknown') |
| `runtime` | False | string (default 'unknown') |
| `subscription_pools` | False | array | null |
| `version` | False | string | null |

Status codes: 200, 403, 422 (raised explicitly in code: 403, 422)

Example: `curl http://<bus-host>:5005/v1/agents/register`

## `GET /v1/agents/stats`

Handler: `agent_stats`. Agent Stats

Auth: none

Parameters: none.

Body: none.

Status codes: 200

Example: `curl http://<bus-host>:5005/v1/agents/stats`

## `GET /v1/agents/whoami`

Handler: `whoami`. Whoami — Help a session find ITS OWN urn by (host, pid) — most recent online registration wins. Useful when a session forgets its urn after restart and needs to re-discover its identity.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `host` | query | True | string |
| `pid` | query | True | integer |

Body: none.

Status codes: 200, 404, 422 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/v1/agents/whoami`

## `POST /v1/agents/{urn_path}/deregister`

Handler: `deregister_agent`. Deregister Agent — Take an agent offline with a recorded reason. Idempotent. Allowed for the admin tier or the agent itself. The record lives in the agent's metadata under ``deregistered`` (no schema change). Repeating 

Auth: admin tier (Authorization: Bearer) or self via X-Hive-Actor

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |
| `authorization` | header | False | string | null |
| `urn_path` | path | True | string |

Body type: AgentDeregister | null.

Status codes: 200, 401, 403, 404, 409, 422 (raised explicitly in code: 401, 403, 404, 409)

Example: `curl http://<bus-host>:5005/v1/agents/{urn_path}/deregister`

## `POST /v1/agents/{urn_path}/plan-reset`

Handler: `reset_plan_period`. Reset Plan Period — Operator zeros the MTD usage — call monthly on billing rollover.

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `urn_path` | path | True | string |

Body: none.

Status codes: 200, 404, 422 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/v1/agents/{urn_path}/plan-reset`

## `GET /v1/agents/{urn_path}/throttle`

Handler: `agent_throttle`. Agent Throttle — Inspect an agent's plan-cap throttle state.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `urn_path` | path | True | string |

Body: none.

Status codes: 200, 404, 422 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/v1/agents/{urn_path}/throttle`

## `GET /v1/events`

Handler: `stream_events`. Stream Events — SSE stream of events. Optional since_id for catch-up.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `since_id` | query | False | integer | null |

Body: none.

Status codes: 200, 422, 503 (raised explicitly in code: 503)

Example: `curl http://<bus-host>:5005/v1/events`

## `GET /v1/events/log`

Handler: `events_log`. Events Log — JSON polling alternative to SSE.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `limit` | query | False | integer (default 100) |
| `since_id` | query | False | integer | null |

Body: none.

Status codes: 200, 422

Example: `curl http://<bus-host>:5005/v1/events/log`

## `GET /v1/hosts`

Handler: `list_hosts`. List Hosts — Aggregate view of all system-watcher agents: specs + current load. Returns one entry per host (latest system agent wins if multiple). Agents that last heartbeated > 60s ago are flagged as stale unless

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `include_stale` | query | False | boolean (default False) |

Body: none.

Status codes: 200, 422

Example: `curl http://<bus-host>:5005/v1/hosts`

## `GET /v1/jobs`

Handler: `list_jobs`. List Jobs

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `agent_urn` | query | False | string | null |
| `include_result` | query | False | boolean (default True) |
| `limit` | query | False | integer (default 100) |
| `parent_job_id` | query | False | string | null |
| `reason_prefix` | query | False | string | null |
| `review_state` | query | False | string | null |
| `since` | query | False | number | null |
| `status` | query | False | string | null |

Body: none.

Status codes: 200, 400, 422 (raised explicitly in code: 400)

Example: `curl http://<bus-host>:5005/v1/jobs`

## `POST /v1/jobs`

Handler: `create_job`. Create Job — Submit work to the triage queue. No agent assignment — workers self-claim via /v1/jobs/next. ROLE ENFORCEMENT (user directive 2026-05-23): Worker runtimes (opencode/codex/hermes/claw-family/ic-engine)

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters: none.

Body (`JobCreate`):

| field | required | type |
| --- | --- | --- |
| `base_branch` | False | string | null |
| `deadline` | False | number | null |
| `depends_on` | False | array | null |
| `description` | False | string | null |
| `eligible_hosts` | False | array | null |
| `eligible_kinds` | False | array | null |
| `head_branch` | False | string | null |
| `idempotency_key` | False | string | null |
| `kind` | True | string |
| `max_cost_tier` | False | string (default 'B') |
| `max_retries` | False | integer (default 2) |
| `mnemos_refs` | False | array | null |
| `parent_job_id` | False | string | null |
| `pin_reason` | False | string | null |
| `pr_number` | False | integer | null |
| `preferred_models` | False | array | null |
| `preferred_providers` | False | array | null |
| `priority` | False | integer (default 0) |
| `project` | False | string | null |
| `repo` | False | string | null |
| `required_capabilities` | False | array | null |
| `site` | False | string | null |
| `submitter_urn` | True | string |
| `target_workspace` | False | string | null |
| `zero_retry_reason` | False | string | null |

Status codes: 200, 403, 422 (raised explicitly in code: 403, 422)

Example: `curl http://<bus-host>:5005/v1/jobs`

## `GET /v1/jobs/metrics`

Handler: `job_metrics`. Job Metrics

Auth: none

Parameters: none.

Body: none.

Status codes: 200

Example: `curl http://<bus-host>:5005/v1/jobs/metrics`

## `POST /v1/jobs/next`

Handler: `dequeue_next_job`. Dequeue Next Job — Atomic dequeue: highest-priority queued job that this agent is eligible for. Algorithm (eligibility / MiniMax pool / role-aware ordering) lives here in the router; every storage op goes through the Hi

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `agent_urn` | query | True | string |

Body: none.

Status codes: 200, 204, 409, 422, 500 (raised explicitly in code: 204, 409, 500)

Example: `curl http://<bus-host>:5005/v1/jobs/next`

## `POST /v1/jobs/purge`

Handler: `purge_jobs`. Purge Jobs — Housekeeping purge: delete the terminal jobs matching the given filters. Every filter that is set is ANDed. Active statuses (queued/offered/claimed/ running/needs-review) are excluded unless ``include

Auth: X-Hive-Actor (must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |

Body (`JobPurgeRequest`):

| field | required | type |
| --- | --- | --- |
| `description_contains` | False | string | null |
| `ids` | False | array | null |
| `include_active` | False | boolean (default False) |
| `kind_prefix` | False | string | null |
| `older_than_seconds` | False | number | null |
| `reason` | False | string (default 'housekeeping-purge') |
| `status` | False | string | null |
| `statuses` | False | array | null |
| `submitter_urn` | False | string | null |

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/jobs/purge`

## `GET /v1/jobs/summary`

Handler: `jobs_summary`. Jobs Summary — Queue state + dead-letter reasons in one read-only call. Shape:: { "by_status": {<status>: n}, "dead": {"failed", "dead-letter", "cancelled", "total", "oldest_age_sec", "cap"}, "dead_by_reason": {<60-

Auth: none

Parameters: none.

Body: none.

Status codes: 200

Example: `curl http://<bus-host>:5005/v1/jobs/summary`

## `GET /v1/jobs/{job_id}`

Handler: `get_job`. Get Job — Fetch a single job by id (previously 405 -> callers had to scan the list or query the DB directly to find one job, e.g. a just-submitted build).

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |

Body: none.

Status codes: 200, 404, 422 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}`

## `PATCH /v1/jobs/{job_id}`

Handler: `update_job`. Update Job

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |

Body (`JobUpdate`):

| field | required | type |
| --- | --- | --- |
| `cancel_actor` | False | string | null |
| `claimed_by` | False | string | null |
| `claimed_cost_tier` | False | string | null |
| `claimed_model` | False | string | null |
| `claimed_provider` | False | string | null |
| `claimed_runtime` | False | string | null |
| `eligible_hosts` | False | array | null |
| `eligible_kinds` | False | array | null |
| `reason` | False | string | null |
| `result` | False | object | null |
| `result_mnemos_id` | False | string | null |
| `status` | True | string |
| `tokens_in` | False | integer | null |
| `tokens_out` | False | integer | null |

Status codes: 200, 403, 404, 409, 422 (raised explicitly in code: 403, 404, 409, 422)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}`

## `POST /v1/jobs/{job_id}/auto-retry`

Handler: `auto_retry_job`. Auto Retry Job — Recheck stored retry policy under a row lock before consuming a retry.

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |

Body: none.

Status codes: 200, 404, 409, 422 (raised explicitly in code: 404, 409)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}/auto-retry`

## `POST /v1/jobs/{job_id}/claim`

Handler: `claim_job`. Claim Job — Targeted single-job claim with the same eligibility checks as /v1/jobs/next.

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |
| `by` | query | True | string |

Body: none.

Status codes: 200, 403, 404, 409, 422 (raised explicitly in code: 403, 404, 409)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}/claim`

## `GET /v1/jobs/{job_id}/preview`

Handler: `zcgate_get_preview`. Zcgate Get Preview — Return the zerocode-gate PreviewBundle stored on a needs-review job.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |

Body: none.

Status codes: 200, 404, 422 (raised explicitly in code: 404)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}/preview`

## `POST /v1/jobs/{job_id}/release`

Handler: `zcgate_release`. Zcgate Release — Human-gated 'Release & Push': enqueue a release:<repo> job that a worker claims and runs `zc-gate release` on (push branch + open upstream PR).

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `job_id` | path | True | string |

Body: none.

Status codes: 200, 404, 409, 422, 503 (raised explicitly in code: 404, 409, 422, 503)

Example: `curl http://<bus-host>:5005/v1/jobs/{job_id}/release`

## `GET /v1/messages`

Handler: `list_messages`. List Messages

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `limit` | query | False | integer (default 100) |
| `since` | query | False | number | null |
| `to_urn` | query | False | string | null |
| `topic` | query | False | string | null |

Body: none.

Status codes: 200, 422

Example: `curl http://<bus-host>:5005/v1/messages`

## `POST /v1/messages`

Handler: `publish_message`. Publish Message

Auth: none (payload-gated: e.g. claimed_by must match; no header)

Parameters: none.

Body (`MessagePublish`):

| field | required | type |
| --- | --- | --- |
| `from_urn` | True | string |
| `in_reply_to` | False | string | null |
| `payload` | True | object |
| `to_urn` | False | string | null |
| `topic` | True | string |

Status codes: 200, 403, 422 (raised explicitly in code: 403, 422)

Example: `curl http://<bus-host>:5005/v1/messages`

## `GET /v1/repos`

Handler: `list_repos`. List Repos — List repos in the registry. `host` adds per-host load instructions.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `host` | query | False | string | null |
| `language` | query | False | string | null |
| `owner` | query | False | string | null |
| `source` | query | False | string | null |
| `status` | query | False | string | null |
| `tag` | query | False | string | null |

Body: none.

Status codes: 200, 422, 503 (raised explicitly in code: 503)

Example: `curl http://<bus-host>:5005/v1/repos`

## `GET /v1/repos/resolve`

Handler: `resolve_repos`. Resolve Repos — Fuzzy-resolve a project name to catalog entries (id/alias/display_name).

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `q` | query | True | string |

Body: none.

Status codes: 200, 422, 503 (raised explicitly in code: 503)

Example: `curl http://<bus-host>:5005/v1/repos/resolve`

## `GET /v1/repos/{repo_id}`

Handler: `get_repo`. Get Repo — One repo: location, remotes, forges, Buildkite pipeline, load recipe (when `host` is given), and the REQUIRED MNEMOS queries to run first.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `repo_id` | path | True | string |
| `host` | query | False | string | null |

Body: none.

Status codes: 200, 404, 422, 503 (raised explicitly in code: 404, 503)

Example: `curl http://<bus-host>:5005/v1/repos/{repo_id}`

## `GET /v1/schedules`

Handler: `list_schedules`. List Schedules

Auth: none

Parameters: none.

Body: none.

Status codes: 200

Example: `curl http://<bus-host>:5005/v1/schedules`

## `POST /v1/schedules`

Handler: `create_schedule`. Create Schedule — Create a recurring scheduled job. Re-fires every `interval_seconds`. job_template is the JobCreate body that will be submitted each tick. Submitter_urn auto-injected from caller — they take responsibi

Auth: X-Hive-Actor (must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |

Body (`ScheduleCreate`):

| field | required | type |
| --- | --- | --- |
| `enabled` | False | boolean (default True) |
| `interval_seconds` | True | integer |
| `job_template` | True | object |
| `name` | True | string |

Status codes: 200, 401, 403, 422 (raised explicitly in code: 401, 403, 422)

Example: `curl http://<bus-host>:5005/v1/schedules`

## `DELETE /v1/schedules/{sid}`

Handler: `delete_schedule`. Delete Schedule

Auth: X-Hive-Actor (must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |
| `sid` | path | True | string |

Body: none.

Status codes: 200, 401, 403, 404, 422 (raised explicitly in code: 401, 403, 404, 422)

Example: `curl http://<bus-host>:5005/v1/schedules/{sid}`

## `PATCH /v1/schedules/{sid}`

Handler: `patch_schedule`. Patch Schedule

Auth: X-Hive-Actor (must name a registered, active agent)

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `X-Hive-Actor` | header | False | string | null |
| `X-Hive-Token` | header | False | string | null |
| `sid` | path | True | string |
| `enabled` | query | False | boolean | null |
| `interval_seconds` | query | False | integer | null |

Body: none.

Status codes: 200, 401, 403, 404, 422 (raised explicitly in code: 401, 403, 404, 422)

Example: `curl http://<bus-host>:5005/v1/schedules/{sid}`

## `GET /v1/stats/cache`

Handler: `cache_stats`. Cache Stats — Result-cache hit-rate + cost savings.

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `top_n` | query | False | integer (default 20) |

Body: none.

Status codes: 200, 422

Example: `curl http://<bus-host>:5005/v1/stats/cache`

## `GET /v1/stats/costs`

Handler: `cost_stats`. Cost Stats — Aggregated cost stats. group_by: provider | model | runtime | cost_tier | claimed_by | day. since_hours: window in hours (default 168 = 7 days).

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `group_by` | query | False | string (default 'provider') |
| `since_hours` | query | False | integer (default 168) |

Body: none.

Status codes: 200, 422 (raised explicitly in code: 422)

Example: `curl http://<bus-host>:5005/v1/stats/costs`

## `GET /v1/stats/workers`

Handler: `worker_stats`. Worker Stats — Per-worker per-kind capability scores. Submitters use this to pick best worker for a kind. #8 FIX (review 2026-05-23): exclude `system` kind agents by default (they're host monitors, not compute worke

Auth: none

Parameters:

| name | in | required | type |
| --- | --- | --- | --- |
| `include_system` | query | False | boolean (default False) |
| `kind` | query | False | string | null |
| `top_n` | query | False | integer (default 30) |

Body: none.

Status codes: 200, 422

Example: `curl http://<bus-host>:5005/v1/stats/workers`

## Status values

- `VALID_JOB_STATUSES`: cancelled, claimed, dead-letter, done, failed, failed_completion, needs-review, offered, queued, running
- `TERMINAL_JOB_STATUSES`: cancelled, dead-letter, done, failed
- `IN_FLIGHT_JOB_STATUSES`: claimed, offered, queued, running
- `DEP_DEAD_STATUSES`: cancelled, dead-letter, failed, failed_completion
- `VALID_AGENT_STATUSES`: error, idle, offline, online, stale

Job lifecycle: queued → offered → claimed → running → done/failed;
failed rows may be requeued to queued (budget permitting) or parked
as needs-review; dead-letter/cancelled are terminal.

## Pagination and errors

Cross-cutting contract for every list route. It is generated (not hand-
written) so a regeneration keeps it, and it is asserted against live
behaviour by `test_pagination_20261030.py` (limits, ordering) and
`test_error_shapes_20261030.py` (error bodies).

### Pagination

A page parameter is an integer query parameter named `limit` (a few
aggregates call it `top_n`). The rules are the same everywhere:

- Absent, `0` and negative values all mean the route's **default**; none of
  them means "no rows" and none means "unbounded".
- A value above the route's **max** is clamped to the max, never rejected:
  a caller asking for a million rows gets one page of at most the max.
- A value that is not an integer (`limit=abc`, `limit=1.5`) is a **422** from
  the framework's own query validation, with the standard `detail` list.
- Ordering is deterministic: the same request twice returns the same rows in
  the same order, and `limit=N` is a prefix of `limit=M` for `M > N`.
- Only `/v1/events/log` offers a cursor (`since_id`): its ids strictly
  increase and `last_id` from one page is the `since_id` of the next.
- A listing's order is not a claim's order. `GET /v1/jobs` is newest first so
  an operator sees recent work; `POST /v1/jobs/next` dispatches priority DESC
  then started_at ASC, so the oldest high-priority row goes first.

| route | parameter | default | max | ordering |
| --- | --- | --- | --- | --- |
| `GET /v1/jobs` | `limit` | 100 | 1000 | started_at/ended_at DESC (newest first), then priority DESC for open rows |
| `GET /v1/messages` | `limit` | 100 | 1000 | ts DESC, newest first |
| `GET /v1/events/log` | `limit` | 100 | 1000 | id ASC, cursor via since_id |
| `GET /v1/admin/jobs/{job_id}/history` | `limit` | 50 | 500 | archived_at DESC, attempt DESC; out of range is 422, not a clamp |
| `GET /v1/stats/cache` | `top_n` | 20 | 500 | hit_count DESC, cost_saved_usd DESC |
| `GET /v1/stats/workers` | `top_n` | 30 | 500 | success_count DESC, success_pct DESC |

`GET /v1/agents`, `/v1/events`, `/v1/hosts`, `/v1/repos` and `/v1/schedules`
take no page parameter: they return the whole (small, bounded) collection.

### Errors

One shape for every non-2xx JSON body:

- a route's own refusal: `{"detail": "<one sentence, no internals>"}`;
- a framework schema refusal: `{"detail": [{"loc": [...], "msg": "...", "type": "..."}]}`.

No error body carries a traceback, a filesystem path, a SQL fragment or a
driver message. The framework's own 422 echoes only the offending field
value, under `input`. `500` is reserved for an unhandled fault and is not
used for a client mistake.

| status | when |
| --- | --- |
| 400 | a filter or enum value that is not valid (`status=`, `review_state=`, unknown MCP method) |
| 401 | a mutating call with no `X-Hive-Actor` (and no admin token), or a bad `X-Hive-Token` |
| 403 | an actor that is neither the admin tier nor the resource's owner, or a denylisted host |
| 404 | an unknown job, agent, repo or schedule id |
| 409 | an illegal status transition, a claim conflict, or a concurrent modification (retry) |
| 422 | a schema violation, or a limit outside a route's accepted range |
| 503 | `GET /health` only: the bus is up but the DB probe or the reaper is not |

Documented exceptions to the envelope: `GET /health` answers 503 with the
health document (`ts`, `service`, `version`, `status`, `detail`) rather than
a bare error, and `POST /mcp` is JSON-RPC and answers `{"error": ...}`.
