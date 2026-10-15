# Analysis: what the hive can reuse from MNEMOS

**Date:** 2026-10-15 · **Author:** opencode · **Related:** `docs/refactor-plan-20261015.md`

MNEMOS (`ncz-os/mnemos`, v7.0.5, 256 py files) and the hive are both Python,
both FastAPI, both target the same fleet + the same backends (SQLite / Oracle
23ai / IBM Db2). MNEMOS is far more mature. Verdict: **yes, but selectively —
the generic infrastructure, not the memory domain.**

## Reusable — adapt into a new `hive/core/`

| MNEMOS source | Why the hive wants it | Fit |
|---|---|---|
| `mnemos/core/config.py` | Typed `get_settings()` (pydantic-settings + TOML); bans direct `os.environ`. The hive has **106 scattered `os.environ`/`os.getenv` reads** (agent_bus 16, zeroclaw_doctor 38, mcp_bridge 8, …). | **High** — pattern + shape |
| `mnemos/core/secret_detection.py` | Credential-shape detection (PAT / private key / `api_key=sk-…`) + vault model. The fleet's own auto-commit daemon carries plaintext creds; the hive has nothing here. | **High** — copy/adapt |
| `mnemos/core/security.py` | Shared authz/regex validators (`\A…\Z` fail-closed). | Medium |
| `mnemos/core/pool.py` | Connection-pool wrapper + transient-error retry (the hive's Oracle/Db2 pools are ad-hoc). | Medium — pattern (asyncpg-specific) |
| `mnemos/core/eligibility.py` | Named SQL predicate + `qualify_*` helper — the same shape the hive's `job_agent_eligible` wants. | Medium — pattern |
| `mnemos/core/audit_chain.py` | Tamper-evident audit chain; the hive has a flat `job_audit_log` table. | Medium |
| `mnemos/core/ids.py` | ID namespace prefixes + derivation; the hive's `uuidv7()` lives inline in `agent_bus.py`. | Medium |
| `mnemos/core/{resilience,rate_limit,observability,numeric,net_validation,network_guard}.py` | Generic infra the hive lacks. | Low–Medium |
| `mnemos/persistence/types.py::_coerce_text` (+ `Row`) | Exact CLOB/LOB unwrap for async Oracle/Db2 drivers — a real hive need. | **High** — copy |

## Reusable — as a structural MODEL (not a dependency)

| MNEMOS | The hive's target |
|---|---|
| `mnemos/api/main.py` + `api/routes/*` + `api/dependencies.py` + `api/lifecycle_hooks.py` | the decomposition target for the 5965-line `agent_bus.py` (routers per concern + DI + lifespan hooks) |
| `mnemos/mcp/{http,stdio,tools}` | structure for the hive's `mcp_bridge.py` |
| `mnemos/persistence/base.py` (one ABC + swappable backends + registry) | the hive's thinner `hive/persistence/base.py` — adopt the registry shape |
| ADR-per-decision docs style | the hive's `docs/decision-*.md` |

## NOT reusable — the memory domain

`mnemos/domain/*` (search, compression, federation, kronos, morpheus, …),
`MEMORY_COLS`, `DuplicateMemoryError`, the EPIMONE repository contract, and
`persistence/{pg,mysql,mariadb}.py` — all bound to the memory model. Do **not**
pull these into the hive.

## How to reuse — recommended

**Do NOT depend on `mnemos-core` as a library.** Its dependency closure includes
`llama-cpp-python` (heavy, platform-specific) + `asyncpg`, and `core/config.py`
already imports other `mnemos.core.*` internals. Coupling the hive's release to
MNEMOS's release + heavy deps is the wrong trade.

**Do this instead:**
1. Create `hive/core/` and **adapt** the small, generic modules that fit
   (`config` pattern, `secret_detection`, `security` validators, `_coerce_text`).
2. Adopt the **structural models** (routers, DI, backend registry) in the hive
   refactor.
3. Longer term, if the two keep converging, **extract a shared `fleet-core`**
   package (dependency-light: pydantic, pydantic-settings, stdlib) that both
   import — rather than one depending on the other.

## Net for the refactor plan

Add a `hive/core/` layer in the refactor and use MNEMOS as the reference for
config + secrets + audit + backend registry. This turns the hive's 106 scattered
env reads into one typed settings object and gives it credential detection +
CLOB coercion for free (adapted).

## Refs
- MNEMOS source analysed: shallow clone `~/Projects/mnemos-ref` (ncz-os/mnemos @ depth 1)
- `mnemos/core/`, `mnemos/persistence/{base,types,oracle,oracle_helpers}.py`, `mnemos/api/`, `mnemos/mcp/`
