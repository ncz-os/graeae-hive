# GRAEAE Hive Mind

Fleet-wide agent coordination + triage queue. Orchestrator runtimes submit work
orders, worker runtimes self-claim and execute them, and a doctor worker
triages whatever fails. One bus, many workers, no central scheduler: the
`/v1/jobs/next` dequeue is the only dispatch mechanism.

- **Bus** — FastAPI app (`hive/bus/app.py`): registers agents, holds the job
  queue, enforces role and eligibility, and publishes events over SSE.
- **Workers** — thin claim/execute loops on each fleet host
  (`deploy/hive_worker.py`, `hive/workers/`): one kind of job per worker,
  fresh per-job clone, drive a local tool, report the result.
- **Doctor** — a triage authority, not an executor
  (`hive/workers/doctor.py`): claims `triage:` / `doctor:` jobs, runs a
  structured LLM diagnosis, and executes allowlisted playbook actions.

Full detail, with file-and-function sources for every claim, lives in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Job lifecycle

A job moves through a fixed set of statuses. A job is **created** as `queued`
by `POST /v1/jobs`, **claimed** (`claimed`) by a worker via
`POST /v1/jobs/next` (or a targeted `POST /v1/jobs/{id}/claim`), marked
`running`, then **finished** as `done` or `failed`. `needs-review` is a
quarantine stop for completions that lack evidence; `cancelled` and
`dead-letter` retire work; `offered` and `released` are transient states the
API still validates.

The complete, machine-checked transition table (`STATUS_TRANSITIONS` in
`hive/bus/domain/jobs.py`) is:

| From | Allowed next statuses |
|---|---|
| `queued` | `queued`, `offered`, `claimed`, `cancelled` |
| `offered` | `queued`, `claimed`, `cancelled` |
| `claimed` | `queued`, `claimed`, `running`, `done`, `failed`, `cancelled`, `needs-review` |
| `running` | `queued`, `running`, `done`, `failed`, `cancelled`, `needs-review` |
| `needs-review` | `needs-review`, `done`, `failed`, `cancelled` |
| `done` | `done`, `cancelled` |
| `failed` | `failed`, `cancelled` |
| `cancelled` | `cancelled` |
| `dead-letter` | `dead-letter` |

`failed_completion` is the API-level spelling of a rejected completion
(`hive/bus/deps.py`: `VALID_JOB_STATUSES`); `TERMINAL_JOB_STATUSES` is
`{done, failed, cancelled, dead-letter}`. See the architecture doc for the
release, dead-letter and completion-gate paths in detail.

## Roles: orchestrators, workers, doctor

- **Orchestrator runtimes** may submit jobs and register with the admin tier
  (`ORCHESTRATOR_RUNTIMES` in `hive/core/roles.py`): `claude-code`,
  `claude-cli`, `claude`, `codex-orchestrator`, `codex`, `codex-cli`,
  `opencode`, `opencode-cli`, `freebuff-cli`, `codebuff`, `freebuff`, `human`,
  `mnemos`, `doctor`, `dream-walker`, `unknown`.
- **Worker-only runtimes** may claim and execute jobs but never submit
  (`WORKER_ONLY_RUNTIMES`): `hermes`, `zeroclaw`, `openclaw`, `ic-engine`,
  `system`.
- `opencode`/`codex` are deliberately on both sides of the fence: they submit
  their own orchestration jobs **and** claim work.
- **freebuff** (operator 2026-10-29) is the newer agentic-CLI runtime: it
  registers, heartbeats, and may submit, like `opencode`. It is its own
  routing kind with no aliasing: `RUNTIME_KIND_MAP["freebuff"] = {"freebuff"}`,
  and the CLI names `freebuff-cli` / `codebuff` also register as kind
  `freebuff` (`hive/bus/domain/eligibility.py`). A doctor agent is isolated
  from every executor kind by a locked import-time invariant in the same file.
- The **doctor** is scope-gated in code: it may only take jobs whose kind
  starts with `triage:` or `doctor:` (minus the `doctor:codex-fix:` dispatch
  prefix, which round-trips through a zeroclaw worker) or jobs that explicitly
  list `doctor` in `eligible_kinds`. A wildcard `*` is never an opt-in for a
  doctor (`_doctor_scope_allowed` in `hive/bus/domain/eligibility.py`).

Registration validates that the declared `kind` matches the `runtime`
(`RUNTIME_KIND_MAP` is the authority), so an `opencode` process cannot register
as `claude`.

## Configuration

All operator-facing environment variables — `HIVE_URL`, the bus tokens, fleet
inventory, doctor model endpoints, MNEMOS — are documented in one place:
[docs/ENVIRONMENT.md](docs/ENVIRONMENT.md). No address has a baked-in default;
where a variable is unset, the feature it configures is disabled with one log
line rather than guessed.

## Running the tests

```sh
python3 -m pytest
```

Python ≥ 3.11. The bus, routers and persistence layers import these packages
at module level (`pyproject.toml` core dependencies):

- `fastapi`, `starlette`, `uvicorn`, `pydantic` — the bus app and schemas
- `httpx` — HTTP client for the worker and doctor loops
- `sse-starlette` — the `/v1/events` SSE stream
- `aiosqlite` — the default SQLite persistence backend
- `mcp` (≥2,<3) — the MCP bridge
- `websockets` — used by the WSS test driver (`tests/fixtures/wss_driver.py`)

`pytest` and `ruff` are the `dev` extra. The suite is self-contained:
`conftest.py` swaps in a fresh memory-backed doctor safety store per test and
declares the test fleet from `tests/fixtures/test_fleet_env.json`, so it runs
without any live infrastructure.

## Registering an agent

Registration is open: any agent may register, and identity is recorded for
transparency. Use `http://<bus-host>:5005` as the bus base in these examples
(substitute the real bus URL from your environment; no tokens are required for
worker-tier registration).

A zeroclaw worker:

```sh
curl -s -X POST 'http://<bus-host>:5005/v1/agents/register' \
  -H 'Content-Type: application/json' \
  -d '{
    "runtime": "zeroclaw",
    "kind": "zeroclaw",
    "host": "myhost",
    "instance": "hive-worker-zeroclaw-1",
    "pid": 4242,
    "model": "local-llm",
    "provider": "local",
    "autonomy_level": "autonomous",
    "auth_method": "free",
    "capabilities": ["python", "git", "bash", "linux-compile", "zeroclaw"]
  }'
```

The response carries the URN (`urn:agent:<kind>:<host>:<session_uuid>`) the
agent uses for every later call. `instance` (a stable per-worker identity) lets
a restart **replace** the prior row instead of minting a new URN; agents that
omit it keep the historical always-new-URN behavior.

An orchestrator (for example the doctor, or an agentic CLI):

```sh
curl -s -X POST 'http://<bus-host>:5005/v1/agents/register' \
  -H 'Content-Type: application/json' \
  -d '{
    "runtime": "freebuff",
    "kind": "freebuff",
    "host": "myhost",
    "model": "local-llm",
    "provider": "local",
    "autonomy_level": "autonomous",
    "auth_method": "api"
  }'
```

Heartbeat keeps the registration alive:

```sh
curl -s -X POST 'http://<bus-host>:5005/v1/agents/heartbeat' \
  -H 'Content-Type: application/json' \
  -d '{"urn": "urn:agent:zeroclaw:myhost:<session_uuid>", "status": "online"}'
```

A `409` from the heartbeat means the bus dropped the URN; workers re-register
on `409` and self-heal.

## Submitting a job

```sh
curl -s -X POST 'http://<bus-host>:5005/v1/jobs' \
  -H 'Content-Type: application/json' \
  -d '{
    "submitter_urn": "urn:agent:human:myhost:<session_uuid>",
    "kind": "code",
    "description": "fix the flaky parser test",
    "repo": "owner/repo",
    "head_branch": "fix/flaky-parser",
    "eligible_kinds": ["zeroclaw"],
    "priority": 0
  }'
```

Workers then poll `POST /v1/jobs/next?agent_urn=<urn>` and self-claim; the bus
enforces kind affinity, required capabilities, host scope, DAG dependencies
and cost tier at claim time.

## Layout

| Path | What |
|---|---|
| `hive/bus/` | FastAPI app: `app.py`, routers, domain logic, schemas, deps |
| `hive/bus/routers/` | `jobs`, `agents`, `events`, `admin`, `mcp`, `messages`, `repos`, `schedules`, `stats`, `system` |
| `hive/core/` | `roles.py` (role sets), `models.py` + `model-registry.toml` (cost tiers/rates), `config.py`, `secrets.py`, `fleet_inventory.py` |
| `hive/workers/` | `doctor.py` (+ `doctor_*` modules), `watcher.py` (system watcher), `remediation.py`, `retention.py`, `fanout.py`, `run.py` |
| `deploy/` | `hive_worker.py` (the thin claim/execute worker), systemd units |
| `hive/persistence/` | storage backends (SQLite, PostgreSQL, MySQL, Oracle, DB2) behind one repository ABC |
| `hive/mcp/bridge.py` | MCP SSE bridge exposing the bus + MNEMOS as tools |
| `hive/harness/` | pluggable harness adapters (zoder, opencode, codex, claude) |
| `queue_logic.py`, `retry_policy.py`, `hive/scheduler.py` | pure queue/dispatch/retry/scheduling decision logic |
| `tests/`, `test_*.py` | the pytest suite (root-level `test_*.py` plus `tests/`) |

## License

Apache-2.0.
