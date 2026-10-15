# Architecture: the hive as one agent with many arms (octopus model)

**Status:** north-star frame (operator 2026-10-15)
**Owner:** opencode · **Supersedes:** none · **Refs:** all `docs/decision-*.md` this session

The hive is **not a job board** — it is **one agent** whose body is the fleet.
Intelligence is centralized; execution is distributed. Like an octopus: one
brain, eight arms, each arm partly autonomous with its own local reflexes, all
bound by a shared nervous system and a shared sense of self.

## The mapping

| Octopus | Hive component | What it does |
|---|---|---|
| **Central brain** | the bus (`agent_bus.py`) | holds global state; decides routing/dispatch; the single point of intent |
| **Arms / sub-brains** | workers (`hive/workers/run.py`) | each runs a local harness+model; executes autonomously; reports |
| **Nervous system** | `/v1/messages` + `/v1/events` + `roster()` | inter-arm signalling + master commands; the arms feel each other |
| **Proprioception** | **working-set** + `roster()` | each arm knows *where it is* (files/languages/architectures) and *where the others are* |
| **Energy / metabolism** | **capacity ledger** (`hive/bus/capacity.py`) | don't overexert a limb past its throughput/credits (slots/rate/credits/cost×time) |
| **Reflex arcs** | claim lease + `offload()` | one arm reaches for a task (no two arms grab the same); an arm can hand a task to a better-suited arm |
| **World map** | **repo registry** (`hive/registry.py`) | where every project lives + how to load it |
| **Which arm reaches** | capability routing + harness adapters | pick the arm whose capabilities fit (arch, language, build role) |
| **Memory** | MNEMOS | the organism's long-term memory (shared across arms + sessions) |
| **Hormones / drive** | model+router registry (`hive/core/models.py`, zoder-sourced) | what to run, at what price, under which policy |

## Properties this frame forces

1. **One intent, many hands.** A goal enters once (submit a job / a DAG via
   `depends_on`) and the arms realize it. The user talks to *the agent*, not to
   individual workers.
2. **Arms are peers that can converse** — cross-harness, cross-model
   (`docs/decision-agent-coordination-20261015.md`). A MiniMax arm and an
   Anthropic arm coordinate in the same topics.
3. **Each arm has local intelligence** (its harness reads files, chooses how to
   edit) but **the master holds the plan** (capacity, routing, registry, lease).
4. **No limb is over-driven** — the capacity ledger is the metabolic limit; a
   saturated arm sheds load to a peer (`offload`) rather than 429-ing.
5. **Self-awareness is the default** — the roster + working-set are how the
   organism knows its own posture without an operator polling.
6. **Harmonization over coordination** — arms pursue the same objective; the bus
   removes conflict (lease) and imbalance (fairness/deficit scoring in
   `hive/scheduler.py`).

## What already exists (this session)

- Central state + routing: `agent_bus.py` (+ extracted `hive/bus/*`, `hive/core/*`)
- Arms: `hive/workers/run.py` (one loop, per-harness adapters)
- Nervous system + proprioception: messages/events/roster + working-set
- Energy: capacity ledger, wired into the claim path
- World map: repo registry
- Which-arm-reaches: capability routing + role-aware scoring
- Reflex: claim lease + `offload()`
- Memory: MNEMOS

## Gaps to close toward the frame

1. **Master goal-decomposition** — nothing today turns a high-level goal into a
   job DAG automatically. That is the orchestrator's job (an orchestrator arm, or
   a bus-side planner). `depends_on` + `parent_job_id` already carry the shape.
2. **Continuous proprioception** — working-set is advertised but nothing *acts*
   on it yet (e.g. auto-offload when an arm is saturated or a file conflict is
   detected). The sensor exists; the reflex loop is the next build.
3. **Arm lifecycle** — arms register/heartbeat but the organism doesn't yet
   grow/shrink its arm count from load (autoscaling).
4. **A single-arm-per-goal ledger** — a "goal" grouping over jobs (beyond one
   level of `parent_job_id`) would let the master report whole-task progress.

## Refs
- `docs/decision-agent-coordination-20261015.md` (nervous system + proprioception)
- `docs/decision-capacity-ledger-20261015.md` (metabolism)
- `docs/decision-repo-registry-20261015.md` (world map)
- `docs/refactor-plan-20261015.md` (the body's construction)
