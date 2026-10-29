# GRAEAE Hive Mind — architecture

This document describes the system **as written in this tree**. Every section
ends with a **Source** line naming the file(s) and function(s) each claim came
from. If a claim cannot be traced to code, it is not here.

Environment variables are documented in [ENVIRONMENT.md](ENVIRONMENT.md) and
deliberately not duplicated in this file.

## 1. The product

GRAEAE Hive Mind is a fleet-wide agent coordination system with a triage queue.
It has three roles:

- **The bus** — one FastAPI application that registers agents, holds the job
  queue, enforces roles and eligibility, and streams events over SSE. There is
  no central scheduler: work is dispatched exclusively by workers polling a
  dequeue endpoint and self-claiming.
- **The workers** — thin claim/execute loops, one per host (or per slot). A
  worker claims exactly one kind of job, resolves the job's repo to a fresh
  per-job clone, shells out to a local driving tool (zoder or codex), and
  reports the result back to the bus. The bus does not know which harness a
  worker uses.
- **The doctor** — a triage authority that is a *brain, not an executor*. It
  claims `triage:` / `doctor:` jobs, runs a structured LLM diagnosis, and
  executes a small allowlist of playbook actions (restart service, resubmit
  jobs, cancel jobs, dispatch a codex fix, no action, escalate). It never
  executes ordinary worker work.

**Source:** `hive/bus/app.py` (module docstring: "fleet-wide
MCP-compatible agent coordination + triage queue … no central scheduler —
agents self-claim"); `deploy/hive_worker.py` (module docstring: "thin
claim/execute worker … the hive's job-claim path should be a THIN wrapper that
shells out to `zoder`/`codex`"); `hive/workers/doctor.py` (module docstring:
doctor authority and constraints — "No raw shell from LLM output — LLM emits a
JSON action, doctor maps to a hardcoded playbook function");
`hive/harness/base.py` (module docstring: "The bus … does not know or care
which harness a worker uses — it speaks this protocol").

## 2. How the bus, the doctor and the workers relate

```
                submits (POST /v1/jobs)
 orchestrators ─────────────────────────────►  BUS (FastAPI app)
 human / mnemos / doctor / freebuff …               │
                                                   │ /v1/jobs/next (self-claim)
 workers (zeroclaw, codex, freebuff, opencode …) ◄──┘
        │  heartbeats (POST /v1/agents/heartbeat)
        └──────────────────────────────────────────► BUS
        │
        │  PATCH /v1/jobs/{id}  (running / done / failed / release to queued)
        └──────────────────────────────────────────► BUS
                                                   │
                        failed jobs get triage:* work orders ─► doctor claims,
                        (auto-submitted by the doctor's scan)      diagnoses,
                                                                   runs allowlisted
                                                                   actions, patches
                                                                   triage job done
```

- **Registration.** Every agent (worker, doctor, orchestrator, or the system
  watcher) registers once per (re)start with `POST /v1/agents/register` and
  receives a URN of the form `urn:agent:<kind>:<host>:<session_uuid>`. It then
  heartbeats on an interval; a `409` from the heartbeat means the bus dropped
  the URN and the worker re-registers (self-heal).
- **Submission.** `POST /v1/jobs` is role-gated: only an orchestrator runtime
  may submit. Workers receive `403`. Submission validates that an active,
  healthy worker exists that matches the job's kinds/hosts/capabilities/cost
  tier, admits the branch (`head_branch`/`base_branch` probed against the
  remote), and rejects forbidden `preferred_models`.
- **Claim.** A worker polls `POST /v1/jobs/next?agent_urn=<urn>`. The bus
  scans queued candidates (bounded by `CLAIM_SCAN_WINDOW`, default 10000),
  orders them role-aware, and compares-and-sets the claim in one transaction
  with a claim lease (`CLAIM_LEASE_SECONDS`, default 1800). The response is the
  claimed job, or HTTP `204` with an `X-Hive-Claim-Result` header when
  nothing matched (`no_jobs_available`, `host_denylisted`,
  `minimax_pool_saturated`).
- **Execution.** The worker patches the job to `running`, executes, then
  patches it to `done` or `failed` (with token usage, model provenance and,
  for commit-producing kinds, commit evidence).
- **Release.** A worker that cannot run a job (no repo mapping, wrong kind,
  etc.) releases it back to `queued` by PATCHing `status=queued` with a
  `worker_error`. Job-level unschedulable releases count toward a decline
  dead-letter threshold; host-level "not me" releases route the job to a
  capable host instead.
- **Doctor loop.** The doctor registers as kind `doctor`, claims via
  `/v1/jobs/next` exactly like a worker, but refuses (releases) anything
  outside its scope, and its scan loop auto-submits `triage:` work orders for
  clusters of failed jobs. Its playbook actions call back into the bus HTTP
  API (resubmit, cancel, requeue) — the doctor acts *through* the bus, never
  around it.
- **Events.** Every state change emits an event (`job.claimed`, `job.done`,
  `job.cancelled`, `job.dead-letter`, `agent.online`, …) visible over SSE at
  `/v1/events` and `/v1/events/log`.

**Source:** `hive/bus/routers/agents.py` (`register`, `heartbeat`);
`hive/bus/deps.py` (`make_urn`, `CLAIM_SCAN_WINDOW`);
`hive/bus/routers/jobs.py` (`create_job`, `dequeue_next_job`, `update_job`,
`claim_job`); `hive/bus/domain/jobs.py` (`CLAIM_LEASE_SECONDS`,
`STATUS_TRANSITIONS`, `is_unresolvable_release`);
`hive/bus/domain/eligibility.py` (`job_agent_eligible`,
`_doctor_scope_allowed`, `agent_kind_aliases`); `hive/scheduler.py`
(`order_candidates_for_worker`, module docstring); `hive/workers/doctor.py`
(`register`, `claim_next_job`, `is_doctor_claimable`, `main` loop:
`submit_auto_triage`, `patch_job`); `hive/bus/routers/events.py`
(`/v1/events`, `/v1/events/log`); `hive/bus/app.py` (lifespan, router
inclusion list).

## 3. Job lifecycle and every status

### 3.1 The statuses

`VALID_JOB_STATUSES` (`hive/bus/deps.py`) is the complete set the API
accepts:

| Status | Meaning |
|---|---|
| `queued` | Waiting to be claimed (initial status on submission) |
| `offered` | Transient offered state; still dequeueable (`ACTIVE_JOB_STATUSES`, `IN_FLIGHT_JOB_STATUSES`) |
| `claimed` | A worker holds the job (with a claim lease) |
| `running` | Worker actively executing |
| `needs-review` | Quarantine: completion rejected for missing evidence; the worker has stopped, the job is promotable to `done`/`failed`/`cancelled` |
| `done` | Terminal: completed (with evidence, or a verdict for read-only kinds) |
| `failed` | Terminal: execution failed |
| `failed_completion` | API-level spelling of a rejected completion; a `done` write that fails the completion gate lands here via `needs-review` + `completion_evidence_missing` in the result |
| `cancelled` | Terminal: retired by submitter, orchestrator, stale-job sweep, or dependency release |
| `dead-letter` | Terminal: burned its decline budget; idempotent same-status PATCH only |
| `released` | A release-to-queued marker state (the job is *not* dead); accepted by the API but jobs are always PATCHed to `queued` on a release |

`TERMINAL_JOB_STATUSES = {done, failed, cancelled, dead-letter}`.
`ACTIVE_JOB_STATUSES = (queued, offered, claimed, running, needs-review)`;
`IN_FLIGHT_JOB_STATUSES` is that minus `needs-review` (a live worker is
attached to exactly those).

**Source:** `hive/bus/deps.py` (`VALID_JOB_STATUSES`, `TERMINAL_JOB_STATUSES`,
`ACTIVE_JOB_STATUSES`, `IN_FLIGHT_JOB_STATUSES`, `DEP_DEAD_STATUSES`, and the
comment explaining `needs-review` as a quarantine state);
`hive/bus/routers/jobs.py` (`list_jobs` status normalization, including
`failed-completion -> failed_completion`; `update_job` completion gate).

### 3.2 The transition table

`STATUS_TRANSITIONS` (`hive/bus/domain/jobs.py`) is enforced by
`update_job` on every PATCH: any transition not in the table is a `409`.

| From | Allowed next |
|---|---|
| `queued` | `queued`, `offered`, `claimed`, `cancelled` |
| `offered` | `queued`, `claimed`, `cancelled` |
| `claimed` | `queued`, `claimed`, `running`, `done`, `failed`, `cancelled`, `needs-review` |
| `running` | `queued`, `running`, `done`, `failed`, `cancelled`, `needs-review` |
| `needs-review` | `needs-review`, `done`, `failed`, `cancelled` |
| `done` | `done`, `cancelled` (exception: retiring fake/duplicate completions) |
| `failed` | `failed`, `cancelled` (exception: retiring superseded failures; never reopened) |
| `cancelled` | `cancelled` |
| `dead-letter` | `dead-letter` (idempotent same-status PATCH only) |

Additional guards in `update_job`:

- A terminal job cannot be reopened, except `done`→`cancelled` and
  `failed`→`cancelled`.
- `queued`/`offered` → `claimed`/`running` via PATCH is rejected ("claim
  bypass rejected"): claiming only happens through `/v1/jobs/next` or
  `/v1/jobs/{id}/claim`, where registration, capabilities, dependencies and
  cost tier are checked.
- A release to `queued` (from `claimed`/`running`/`offered`) requires
  `claimed_by` to match the current claimant, clears the claim fields, and
  sets an exponential retry backoff (5 s × 2^n, capped at 300 s, ±20 %
  jitter).
- A `done` write by a non-read-only commit-producing kind needs commit
  evidence (`valid_completion_proof`); read-only kinds may instead carry a
  structured verdict (`valid_readonly_completion_proof`). A failed gate
  rewrites the status to `needs-review` with
  `failure_reason=completion_evidence_missing` and answers `409`.
- A `done` write that looks like an LLM token-loop is demoted to `failed` by
  the hallucination guard (`hallucination_check`).
- A `failed` write classified as an *infrastructure* failure (engine
  unavailable, etc.) is parked back in `queued` with `failure_class` recorded,
  so infra attempts never count as work failures; when the retry budget is
  exhausted the row is parked with `infrastructure_parked`.
- When a job enters `DEP_DEAD_STATUSES` (`failed`, `failed_completion`,
  `dead-letter`, `cancelled`), every queued/offered job that lists it in
  `depends_on` is released in the same transaction.

**Source:** `hive/bus/domain/jobs.py` (`STATUS_TRANSITIONS` and its inline
comments; `valid_completion_proof`, `valid_readonly_proof`,
`valid_readonly_completion_proof`, `hallucination_check`);
`hive/bus/routers/jobs.py` (`update_job` — all the guards above, the
`release_to_queue` block, the `infra_class` block, the `DEP_DEAD_STATUSES`
release); `hive/bus/domain/failure.py` (`engine_unavailable`,
`infrastructure_failure`); `hive/bus/deps.py` (`release_dependencies`,
`DEP_DEAD_STATUSES`).

### 3.3 Declines, dead-letter, retries

- A release carries a `worker_error` reason. Reasons in
  `DECLINE_REASON_PREFIXES` (`malformed_job_id`, `invalid_head_branch`,
  `invalid_base_branch`, the workspace reasons `no_workspace_for_kind[:...]`
  / `no_workspace_for_repo[:...]`) are **job-level**: no host can run the
  job, so each occurrence counts toward the decline budget
  (`MAX_DECLINE_REQUEUES = 3`), after which the job is dead-lettered with
  `last_decline_reason`.
- `no_repo_mapping` is **deployment-dependent**: it counts only when the
  job's repo is genuinely absent from the bus catalog (exact match,
  `repo_in_catalog`), and then against the larger
  `NO_REPO_MAPPING_DECLINE_BUDGET = 20`.
- Host-level releases (`host_declines_kind`, `released_by_host`) never count:
  they are routing signals.
- Real execution failures use `max_retries`/`retry_count` (default 2;
  `max_retries=0` requires a `zero_retry_reason`). `POST
  /v1/jobs/{id}/auto-retry` rechecks the stored retry policy under a row lock
  before consuming a retry. Admin `POST /v1/admin/jobs/requeue` can requeue
  with an explicit `override_max_retries`.

**Source:** `hive/bus/domain/jobs.py` (`MAX_DECLINE_REQUEUES`,
`JOB_LEVEL_DECLINE_REASONS`, `NO_REPO_MAPPING_DECLINE_BUDGET`,
`WORKSPACE_DECLINE_REASONS`, `DECLINE_REASON_PREFIXES`,
`is_unresolvable_release`, `repo_in_catalog`); `hive/bus/routers/jobs.py`
(`update_job` decline counting, `auto_retry_job`); `hive/bus/routers/admin.py`
(`admin_requeue_jobs`); `hive/bus/schemas.py` (`JobCreate.max_retries`,
`zero_retry_reason`, `RequeueRequest`).

## 4. Agent kinds and roles

### 4.1 Role sets

`hive/core/roles.py` is the single source of truth for which runtimes may
submit and which may only execute:

- **Orchestrator runtimes** (may submit jobs; register with the admin tier):
  `claude-code`, `claude-cli`, `claude`, `codex-orchestrator`, `codex`,
  `codex-cli`, `opencode`, `opencode-cli`, `freebuff-cli`, `codebuff`,
  `freebuff`, `human`, `mnemos`, `doctor`, `dream-walker`, `unknown`.
- **Worker-only runtimes** (may claim/execute, never submit): `hermes`,
  `zeroclaw`, `openclaw`, `ic-engine`, `system`.

`opencode`/`opencode-cli` and `codex`/`codex-cli` appear only in the
orchestrator set by design: they submit their own orchestration jobs **and**
claim work, so they are not worker-only.

**Source:** `hive/core/roles.py` (`ORCHESTRATOR_RUNTIMES`,
`WORKER_ONLY_RUNTIMES`, module and inline comments);
`hive/bus/routers/jobs.py` (`create_job` role enforcement via
`require_orchestrator_submitter` in `hive/bus/deps.py`).

### 4.2 Kinds and aliasing

`RUNTIME_KIND_MAP` (`hive/bus/domain/eligibility.py`) maps a runtime to the
kind(s) it may claim. `agent_kind_aliases(kind, runtime)` builds the alias
set used for the `eligible_kinds` match: a job is claimable when the agent's
alias set intersects the job's `eligible_kinds` (or the job has the `*`
wildcard). Notable entries:

- `freebuff` (operator 2026-10-29): the newer agentic-CLI runtime. It is its
  own kind — `RUNTIME_KIND_MAP["freebuff"] = {"freebuff"}`, no aliasing. The
  CLI names it ships as, `freebuff-cli` and `codebuff`, also register as kind
  `freebuff`. It is in `ORCHESTRATOR_RUNTIMES` (registers, heartbeats, may
  submit) and is *not* in `WORKER_ONLY_RUNTIMES`.
- `zeroclaw`, `opencode`, `codex`, `claude`, `openclaw`, `ic-engine`,
  `hermes`, `mnemos`, `human`, `dream-walker`, `system`, `doctor`, `unknown`
  each map to themselves (or, for `claude-code`/`claude-cli`, to `claude`/
  `claude-code`).
- The "all-executors-eligible union" that once let a `codex` worker claim a
  `zeroclaw` job was removed 2026-09-15; kind is strict again — a job's
  `eligible_kinds` means what it says.

**Source:** `hive/bus/domain/eligibility.py` (`RUNTIME_KIND_MAP`,
`agent_kind_aliases` and its removal note); `test_freebuff_runtime_20261029.py`
(registration and role invariants for the freebuff runtime).

### 4.3 Doctor isolation

The doctor is scope-gated in code, not by convention:

- `agent_is_doctor(agent)` is true when the agent's kind **or** runtime is
  `doctor`.
- `_doctor_scope_allowed(job, kinds)` allows a doctor only when: the job kind
  starts with `triage:` or `doctor:`; **or** the job's `eligible_kinds`
  literally contains `doctor`. The wildcard `*` and every executor alias are
  stripped before the opt-in check, so a job listing only `["zeroclaw"]` (or
  `["*"]`) can never claim the doctor.
- `DOCTOR_JOB_KIND_EXCLUDE_PREFIXES = ("doctor:codex-fix:",)` is **always**
  excluded, even with an explicit `doctor` opt-in: those dispatches are
  submitted by the doctor with `eligible_kinds=["zeroclaw"]` and round-tripped
  through a zeroclaw worker — the doctor dispatches them but never executes
  one.
- An import-time invariant locks the `RUNTIME_KIND_MAP` so no executor
  runtime may ever alias `doctor`; a violating edit raises at import.
- Defense in depth: `agent_kind_aliases` strips executor aliases from a
  doctor agent's alias set (`_DOCTOR_EXECUTOR_ALIASES`), so the alias-set
  match in the non-doctor branch of `job_agent_eligible` cannot bridge the
  two worlds.
- Even on the wire, the doctor's `claim_next_job` re-checks the kind
  (`is_doctor_claimable`) and releases an out-of-scope job unfailed.

**Source:** `hive/bus/domain/eligibility.py` (`DOCTOR_KIND`,
`DOCTOR_JOB_KIND_PREFIXES`, `DOCTOR_JOB_KIND_EXCLUDE_PREFIXES`,
`DOCTOR_OPT_IN_EXCLUDE_ALIASES`, `_DOCTOR_EXECUTOR_ALIASES`,
`_DOCTOR_EXECUTOR_RUNTIMES` import-time check, `agent_is_doctor`,
`_doctor_scope_allowed`, `job_agent_eligible`); `hive/workers/doctor.py`
(`claim_next_job`, `is_doctor_claimable`).

### 4.4 Registration and identity

- `POST /v1/agents/register` validates the runtime→kind pairing: the declared
  `kind` must be in `RUNTIME_KIND_MAP[runtime]` (or equal the runtime), else
  `422 identity-mismatch`. A kind-only registration is lifted to a runtime
  only when the kind is a known runtime name; an unknown kind fails loud.
- Cost tier is derived from the provider (`cost_tier_for`), with
  `opencode`/`codex`/`doctor`/`zeroclaw` pinned to tier `A` (they may claim at
  all tiers).
- `instance` is a stable per-worker-instance id: when present, the bus
  dedups on `(host, kind, instance)` and a restart *replaces* the prior row
  instead of minting a new URN. Agents without an `instance` keep the
  historical always-new-URN behavior.
- The URN is `urn:agent:<kind>:<host>:<session_uuid>`; the bus keys worker
  stats on the stable prefix `urn:agent:<kind>:<host>`
  (`stable_worker_id`).

**Source:** `hive/bus/routers/agents.py` (`register` — all of the above);
`hive/core/models.py` (`cost_tier_for`); `hive/bus/domain/jobs.py`
(`stable_worker_id`); `hive/bus/schemas.py` (`AgentRegister`, `AgentHeartbeat`).

### 4.5 Eligibility at claim time

`job_agent_eligible(job, agent)` is the hard gate, applied on both
`/v1/jobs/next` and `/v1/jobs/{id}/claim`. It checks, in order:

1. Doctor scope (section 4.3) or, for non-doctors, the `eligible_kinds`
   alias match.
2. Narrow-host allowlist: agents on the narrow hosts
   (`NARROW_HOSTS` — four lightweight hosts, see the constant) may only take
   the `NARROW_ALLOWLIST` kinds (`cixmini-os:`, `ncz-os-`, `fleet-infra:`).
3. Kind affinity (`KIND_KIND_AFFINITY`, e.g. `dream-walker:` → `dream-walker`).
4. `eligible_hosts` (with the `_HOLD_HOSTS` rescue hosts excluded).
5. Required capabilities, with `queue_logic.match`: HARD labels (e.g.
   `hard:linux-compile` for compile/build kinds) must all be present; SOFT
   labels only bias the score; the job's `heavy-repo` requirement is implied
   by the kind prefix.
6. A flag-gated provider-as-capability gate (`HIVE_LLM_CAP_GATE`).
7. Cost tier: the agent's tier must not exceed the job's `max_cost_tier`
   (tiers `A < B < C`).
8. Subscription throttling: an agent at ≥ 85 % of its monthly plan cap
   (`THROTTLE_HEADROOM = 0.85`) may not claim tier-B/C jobs.

The scheduler (`hive/scheduler.py`) then *orders* the eligible candidates
role-aware (build vs general workers, least-recently-served fairness) and
enforces the MiniMax capacity pool (5 concurrent MiniMax sessions fleet-wide,
1 lease per worker) — it layers scoring on top of the eligibility gate, never
below it.

**Source:** `hive/bus/domain/eligibility.py` (`job_agent_eligible`,
`NARROW_HOSTS`, `NARROW_ALLOWLIST`, `_HOLD_HOSTS`, `KIND_KIND_AFFINITY`,
`KIND_WORKSPACE_CAPABILITY`, `HEAVY_REPO_KIND_PREFIXES`, `THROTTLE_HEADROOM`,
`LLM_CAP_GATE`); `queue_logic.py` (`match`, module docstring on HARD vs SOFT);
`hive/scheduler.py` (module docstring, `order_candidates_for_worker`,
`is_minimax_route`, `MiniMaxPoolState`); `hive/core/models.py`
(`COST_TIERS`); `hive/bus/routers/jobs.py` (`dequeue_next_job`).

## 5. The doctor in depth

- Registers as kind `doctor` (runtime `doctor`), one in-flight triage job per
  process.
- Its main loop: scan for clusters of failed jobs → auto-submit up to 5
  `triage:` work orders → claim the next job via `/v1/jobs/next` → patch to
  `running` → `process_triage_job` → patch to `done`/`failed`/`needs-review`
  (or leave it `queued` when a playbook action already released it; the loop
  reports that as `released`).
- Playbook actions (allowlisted, mapped from the LLM's JSON — never raw
  shell): restart service (allowlisted services/hosts, SSH rate-limited to
  3/hour with a 1800 s per-host cooldown), resubmit jobs, cancel jobs,
  dispatch a codex fix, no action, escalate.
- Safety state is process-global by design (loop counter, per-kind cooldown,
  DLQ-seen set, SSH rate limiter), persisted via `hive/workers/doctor_state.py`;
  a loop that escalates 3 times in 30 min stops and escalates to a human.
- The doctor's bus calls require `HIVE_BUS_ADMIN_TOKEN` to register as an
  orchestrator and call `/v1/admin/*`; without `HIVE_URL` its bus features are
  disabled with one log line.

**Source:** `hive/workers/doctor.py` (module docstring; `main`,
`process_triage_job`, `claim_next_job`, `release_out_of_scope_job`,
`submit_auto_triage`, `MAX_SSH_PER_HOUR`, `MIN_HOST_COOLDOWN`,
`LOOP_ESCALATE_THRESHOLD`, `LOOP_WINDOW_SEC`); `hive/workers/doctor_state.py`;
`hive/workers/doctor_gates.py` (restart allowlist);
`docs/ENVIRONMENT.md` (doctor variables, token roles).

## 6. Persistence

The bus never chooses a backend or carries a DSN. `get_hive_repository()`
(`hive/persistence/factory.py`) resolves the backend from the environment
(`HIVE_BACKEND` / `AGENT_BUS_DB` / `ORACLE_DSN` / `DB2_DSN`) and returns one
implementation of the `HiveMindRepository` ABC. The routers and domain code
only ever touch that ABC on an explicit transaction ("EPIMONE shape": the
caller owns the transaction and its commit). Backends: SQLite (WAL, default,
via `aiosqlite`), PostgreSQL, MySQL, Oracle, DB2. Job status writes are
compare-and-set on the observed status, so a concurrent change is a `409`
retried by the caller.

**Source:** `hive/bus/deps.py` (`hive_repo`, `connect_db`, module docstring);
`hive/persistence/factory.py` (`get_hive_repository`);
`hive/persistence/{sqlite,postgres,mysql,oracle,db2}.py`;
`hive/bus/routers/jobs.py` (`update_job` CAS on `expected_status`).

## 7. Supporting subsystems

- **Events** — SSE streams at `/v1/events` (live) and `/v1/events/log`
  (recent), with a ping interval and a max-subscriber cap.
- **Admin** — `/v1/admin/jobs/requeue`, `/v1/admin/jobs/purge`,
  `/v1/admin/jobs/reclassify`; mutation endpoints require a mutation actor.
- **MCP bridge** — `hive/mcp/bridge.py` (MCP SSE on its own port) plus the
  bus-embedded `/mcp` RPC route, exposing `hive.*` and `mnemos.*` tools to
  MCP clients.
- **Schedules** — recurring job templates (`ScheduleCreate`); the bus submits
  the template each tick.
- **Reaper** — orphan-claim selection and retention sweeps
  (`hive/bus/domain/reaper.py`, `hive/workers/retention.py`) keep dead-job
  backlogs under a cap and cancel stale (>7 d) queued jobs.
- **Result cache** — `(kind, description, max_cost_tier,
  required_capabilities)` → result memoization with a 24 h TTL
  (`CACHE_TTL_SECONDS`); identical resubmissions return the cached result
  instantly.
- **Cost accounting** — `estimate_cost` prices metered providers from
  `rate_for` (per-million-token rates generated from
  `hive/core/model-registry.toml`); subscription providers (openai/codex and
  family) price at $0 (flat subscription).

**Source:** `hive/bus/routers/events.py`; `hive/bus/routers/admin.py`;
`hive/mcp/bridge.py` (module docstring endpoint matrix);
`hive/bus/routers/schedules.py`, `hive/bus/schemas.py` (`ScheduleCreate`);
`hive/bus/domain/reaper.py` (`select_orphan_claims`);
`hive/workers/retention.py` (retention sweep); `hive/bus/app.py`
(`_cancel_stale_jobs`); `hive/bus/domain/jobs.py` (`cache_key_for`,
`cache_lookup`, `cache_store`, `CACHE_TTL_SECONDS`, `estimate_cost`,
`SUBSCRIPTION_PROVIDERS`); `hive/core/models.py` (module docstring: tables
generated from `hive/core/model-registry.toml`).

## 8. Configuration

All operator-facing environment variables — the bus (`HIVE_URL`,
`HIVE_BUS_TOKEN`, `HIVE_BUS_ADMIN_TOKEN`), fleet inventory
(`HIVE_FLEET_INVENTORY`, `ARGONAS_SSH_HOST`), doctor model endpoints
(`DOCTOR_*`), MNEMOS, alerts — are defined in
[ENVIRONMENT.md](ENVIRONMENT.md). The governing rule: **an address comes from
the environment or a fleet inventory, never from source.** No variable in
that list has a baked-in default; where a variable is unset, the feature it
configures is disabled with one log line.

**Source:** `docs/ENVIRONMENT.md`; `hive/workers/doctor.py`
(`HIVE_URL`/token reads and the disabled-with-log-line behavior);
`deploy/hive_worker.py` (module docstring: "HIVE_URL … required … unset
refuses to poll"); `hive/workers/watcher.py` (module docstring: "No baked-in
bus address").

## 9. Running the tests

```sh
python3 -m pytest
```

Python ≥ 3.11. `pytest` and `ruff` come from the `dev` extra; the bus, routers
and persistence layers import at module level: `fastapi`, `starlette`,
`uvicorn`, `pydantic`, `httpx`, `sse-starlette`, `aiosqlite`, `mcp` (≥2,<3),
and `websockets` (used by the WSS test driver in `tests/fixtures/`). The suite
is self-contained: `conftest.py` gives every test a fresh memory-backed doctor
safety store (never calling the destructive `reset_safety_state`), and
declares the test fleet from `tests/fixtures/test_fleet_env.json` at import
time. No live infrastructure is required.

**Source:** `pyproject.toml` (`[project]`, `[project.optional-dependencies]`,
`[tool.pytest.ini_options]`); `conftest.py` (module docstring and
`_export_test_fleet_env`); `tests/fixtures/wss_driver.py` (`import websockets`).
