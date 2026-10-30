# Decision: Phase 6 — central model + router registry

**Date:** 2026-10-16  **Status:** proposed (kickoff)  **Phase:** refactor-plan Phase 6
**Owner:** opencode  **Depends on:** FleetOps `gitops/fleet/zoder-config/`, `gitops/zeroclaw/config.audited.toml`

> **Update 2026-10-26:** the bus's KNEMON router (formerly under `hive/bus/routers/`, route `/v1/knemon/*`) and the bus `KNEMON_PROVIDERS` table were removed. The
> registry's `"knemon"` route data still generates the `[router.providers]`
> section of each host's zoder.toml. The text below is the original record.

## Problem

Model/provider routing facts live in **three** places that drift independently:

1. **Bus tables** — `hive/core/models.py`: `PROVIDER_COST_TIER`, `LLM_RATES`,
   `KNEMON_PROVIDERS` (consumed by the former KNEMON router, removed 2026-10-26).
2. **Per-host zoder TOMLs** — FleetOps `gitops/fleet/zoder-config/config.toml` +
   `config.fleet.toml` (the engine `coder`/`reviewer` agents + router providers).
3. **Per-host zeroclaw TOML** — FleetOps `gitops/zeroclaw/config.audited.toml`
   (`[providers.models.*]`).

Drift between (1) and (2) produced the documented incidents: the retired TYDEUS
`:8002` `nemotron35` coder route, and coder-pinned-in-one-file/reviewer-in-another
mismatches. A model can be "known" to the bus while a host's TOML points elsewhere
(or at a dead endpoint) with no single place that is wrong.

## Shape

**One source of truth → GENERATE the consumers (materialized, no runtime dep).**

```
FleetOps (private)  gitops/fleet/model-registry.toml      <-- THE source
                         │
                         ├─ gen-bus-tables ──> hive/core/models.py tables (in-repo, committed)
                         ├─ gen-zoder-toml ──> per-host config.toml / config.fleet.toml fragments
                         └─ gen-zeroclaw-toml ─> per-host config.audited.toml [providers.models.*]
```

Each consumer is **generated and committed**, never hand-edited. A `--check` mode
fails CI if a committed consumer diverges from the source.

## Source schema (minimal)

```toml
[model."deepseek-flash"]
provider       = "deepseek"
cost_tier      = "A"          # maps to PROVIDER_COST_TIER / tier hint
price_in       = 0.0          # $/MTok, from the provider's own block (not the stale catalog)
price_out      = 0.0
routes         = ["knemon", "zoder.coder"]   # who may select it
endpoint       = "https://api.deepseek.com/v1"
notes          = "DeepSeek V4.1-Flash; metered, 2x at peak"
```

Per-host assignment (`[host.<name>]` coder/reviewer/…) is a separate table so
endpoint facts and placement facts do not mix.

## Interface (bus side, in-repo)

- `hive/core/model_registry.py`:
  - `load_registry(path) -> Registry`
  - `emit_bus_tables(registry) -> str` (a `# GENERATED` block for `models.py`)
  - `emit_zoder_toml(registry, host) -> str`
  - `emit_zeroclaw_toml(registry, host) -> str`
  - `main()` CLI: `--check` / `--write`
- **Drift test:** `test_model_registry.py` regenerates the committed bus tables and
  asserts byte-equality; asserts every `routes` entry resolves to a real consumer;
  asserts the Nemotron / EIH invariants (placement NOT EIH-exclusive, the shipped
  Nemotron is the EIH backup reviewer and never a knemon/coder candidate),
  DeepSeek is direct-only, and retired endpoints (:8002 / :4100) are rejected.

## Non-goals / guardrails

- **Behavior-preserving.** The first cut only *re-derives* the existing tables from
  a registry that encodes today's values; it must not change routing. Any routing
  change is a separate reviewed commit.
- **No runtime dependency.** The bus never reads FleetOps at runtime.
- **Nemotron / EIH rule** (operator correction 2026-10-17):
  `nvidia/nemotron-3-ultra-550b-a55b` is the only Nemotron routed through EIH and
  is a backup reviewer; it must never carry a "knemon" route. Placement is NOT
  EIH-exclusive — a Nemotron model may sit in ANY `[host.<name>]` slot whose
  route it allows, on ANY host (TYDEUS serves a Nemotron backup model under test
  as `nemotron35` on vLLM :8010, zoder provider `custom.nemotron`). The EIH
  placement rule still forbids closed frontier models on EIH and keeps DeepSeek
  direct-only. The generator must encode all of this as an invariant, not a
  comment.
- **DeepSeek direct-only**, key resolution via the JSON registry (no env var).

## Why not implemented in this pass

Phase 6 rewrites the live bus routing tables and every host's zoder/zeroclaw TOML —
a high-blast-radius, behavior-preserving refactor. Per the plan's guiding rule 2
("no behavior changes ride along") it needs its own branch, its own zoder review,
and a staged rollout (bus first, then hosts idle-gated). Doing it unattended in a
mixed session would violate that. This doc pins the design so the implementing
session starts from a fixed target instead of re-deriving it.

## Acceptance (for the implementing pass)

- `gen --check` green in CI; committed bus tables == generated.
- `test_model_registry.py` green; Nemotron + DeepSeek invariants asserted.
- One host's zoder TOML regenerated and byte-identical to today's (proof of
  behavior preservation) before any host is switched to generated content.
- zoder review → APPROVE; deploy verified; MNEMOS record.

## Refs

- `docs/refactor-plan-20261015.md` (Phase 6)
- CLAUDE.md fleet/provider rules (DeepSeek window, Nemotron placement, zoder routes)
- `hive/core/models.py`, the former KNEMON router (removed 2026-10-26)
