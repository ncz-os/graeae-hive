# Refactor plan: graeae-hive-mind → a real package

**Status:** Phase 0 in progress (2026-10-15)
**Branch:** `refactor/full-20261015`
**Owner:** opencode

---

## Why

The codebase grew as flat scripts around a **5965-line `agent_bus.py` monolith**
plus root-level scripts and root-level tests, with **no packaging or tooling
config** (no `pyproject.toml`, no ruff/pytest config, no importable project).
That makes every change risky: no importable API, no test isolation, no
enforced lint, no single entrypoint. A full refactor needs a foundation first.

## Current shape

```
agent_bus.py            5965  FastAPI bus: routes + persistence + eligibility +
                              scheduling + cost + helpers, all in one module
mcp_bridge.py            519  MCP SSE bridge
zeroclaw_doctor.py      1474  doctor CLI
test_*.py (6 files)     3300  tests at REPO ROOT
system_watcher.py        524  worker
repository.py            276  (legacy?) repo helper
hive/                        persistence/ + harness/ + registry + repos + scheduler
tests/fixtures/              moved wss fixtures
data/ docs/ migrations/ scripts/
<none>                       pyproject / ruff / pytest config
```

## Target shape

```
pyproject.toml               project metadata, ruff + pytest config, entrypoints
hive/
  core/                      NEW (see docs/analysis-mnemos-reuse-20261015.md)
    config.py                typed get_settings() — replaces 106 scattered os.environ reads
    secrets.py               credential-shape detection (adapted from MNEMOS)
  bus/                       the FastAPI app, decomposed
    app.py                   create_app() + lifespan
    routers/                 agents.py jobs.py messages.py schedules.py
                             repos.py admin.py metrics.py events.py
    domain/                  eligibility.py routing.py cost.py validation.py
    schemas.py               pydantic models
  persistence/               (exists) sqlite/oracle/db2/factory
  harness/                   (exists) adapters
  registry.py repos.py       (exist) repo registry
  workers/                   system_watcher.py doctor.py
  mcp/                       mcp_bridge.py
tests/                       all tests, mirroring the package
scripts/                     build_repo_registry.py
```

## Guiding rules

1. **The deploy must stay green at every step.** The live bus runs from
   `/srv/agent-bus` (flat) with a 10-min auto-commit daemon that stages specific
   files. Refactor steps must keep that working, or explicitly update the daemon.
2. **Behavior-preserving.** Refactor = move/rename with identical behavior.
   No behavior changes ride along; those get their own reviewed commits.
3. **Test safety net first.** No monolith decomposition until the suite runs
   green (or every failure is understood + tracked).
4. **Small, reviewable, deployable commits.** zoder review on each.
5. **No auth work** — the fleet is local-LAN only (operator 2026-10-15).

## Phases

### Phase 0 — Foundation (this branch)
- **0a** `pyproject.toml`: project metadata, `ruff` config (the fleet uses
  `ruff check --fix && ruff format`), `pytest` config, console entry points.
- **0b** Dev venv on STUDIO (deps: fastapi, uvicorn, starlette, pydantic,
  httpx, sse-starlette, mcp>=2, aiosqlite, …); establish the test baseline
  (currently 58 ran / 13 fail / 28 error per session notes — make it green or
  document each failure with a tracking note).
- **0c** Move root `test_*.py` → `tests/`; fix imports; wire `pytest`.
- **0d** `hive/core/`: typed `get_settings()` (pydantic-settings + TOML)
  replacing the 106 scattered `os.environ` reads; `secrets.py` credential
  detection; `_coerce_text` CLOB helper. Adapted from MNEMOS —
  see `docs/analysis-mnemos-reuse-20261015.md`.

### Phase 1 — Decompose `agent_bus.py`
- **1a** Extract pure domain logic (eligibility, routing, cost, validation) into
  `hive/bus/domain/` — no FastAPI, unit-testable. Behavior-preserving.
- **1b** Extract pydantic schemas → `hive/bus/schemas.py`.
- **1c** Extract routers → `hive/bus/routers/*`, assembly in `hive/bus/app.py`.
- **1d** `agent_bus.py` becomes a thin shim (`from hive.bus.app import app`) so
  the systemd `ExecStart=… -m uvicorn agent_bus:app` keeps working.

### Phase 2 — Consolidate the satellites
- `system_watcher.py` → `hive/workers/`; `zeroclaw_doctor.py` → `hive/workers/`;
  `mcp_bridge.py` → `hive/mcp/`. Keep thin root shims for existing service
  ExecStart paths.

### Phase 3 — Persistence cleanup
- Review the sqlite/oracle/db2 trio against the `HiveMindRepository` protocol;
  document which backend is canonical (live = Oracle). Remove dead paths.

### Phase 4 — Docs + registry items (folded in)
- `PROJECT_LOCATIONS.md` emitter (`build_repo_registry.py --format md`).
- Registry rebuild cadence (systemd timer on PYTHIA).

### Phase 5 — Capacity ledger + admission control
See `docs/decision-capacity-ledger-20261015.md`. Per-service capacity
(slots / rate / credits / cost×time) declared in the registry; enforced by a
`hive/bus/capacity` module in the claim path; fed by worker-side per-response
capture (token counts + rate-limit headers + optional `count_tokens` pre-flight).
Replaces KNEMON's per-agent budget bookkeeping. First instance = the MiniMax
slot cap (currently missing from `dequeue_next_job`).

### Phase 6 — Central model + router registry
Single source of truth (FleetOps, private) → GENERATE the bus routing tables
AND the per-host zoder/zeroclaw TOMLs (materialized; no runtime dependency).
Kills the bus-tables-vs-TOML drift (the `:8002` / coder-vs-reviewer incidents).

## Definition of done (per phase)
- `ruff check` clean, `ruff format` applied.
- `pytest` green.
- Deploy (ff `/srv/agent-bus` + restart) verified healthy.
- zoder review → approve.
- MNEMOS record.

## Refs
- `docs/refactor-proposal-harness-agnostic-20261015.md` (Phases A–E, harness layer — done/ongoing)
- `docs/decision-repo-registry-20261015.md`
- `docs/decision-mcp-a2a-plane-20261015.md`
