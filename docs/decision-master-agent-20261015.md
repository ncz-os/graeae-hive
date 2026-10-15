# Decision: where the master lives — bus reflexes + doctor cortex

**Status:** Recommended (2026-10-15) · **Owner:** opencode
**Refs:** `docs/architecture-octopus-20261015.md` (the frame), `docs/decision-agent-coordination-20261015.md`

## Question

Should the master agent run *on the hive* — i.e. is the **doctor** the master,
and should we expand its capabilities?

## Answer

**Split the master — don't make the doctor the whole brain.** The octopus has a
brainstem (fast, deterministic, always-on) *and* a cortex (slow, deliberative).
Map them separately:

| Layer | Lives where | Nature | What it does |
|---|---|---|---|
| **Brainstem / reflexes** | **the bus** (`agent_bus.py`, in-process on PYTHIA) | deterministic, no LLM | holds state; arbitrates claims; enforces capacity; **the reflex loop** (auto-offload on saturation / file conflict) |
| **Cortex / planner** | the **doctor** (an orchestrator agent) | LLM-driven, deliberative | goal decomposition → job DAG; triage; dispatch; proprioception-driven rebalancing |

So: **yes, the master runs on the hive — but as two pieces.** The bus is already
running there; the doctor should become the *cortex* and be **expanded + promoted
to an always-on orchestrator service** (like the existing `hive-*` timers), not
just a claim-loop worker that happens to triage.

## Why not the doctor alone

- The doctor is a **worker**: it can only act after *claiming* a job. Continuous
  reflexes (shed load the instant an arm saturates) can't wait on a claim — they
  belong in the bus's claim path, where the state already is.
- Putting the LLM planner in the bus process would make the nervous system
  non-deterministic and heavy. Keep the brainstem pure.
- Putting the reflexes in the doctor makes them claim-gated + slow. Keep them in
  the bus.

## Doctor expansion (from triage-only → general orchestrator)

Today the doctor: registers, claims `doctor:*` jobs, scans failure clusters →
auto-triage, and runs actions (restart/resubmit/cancel/release/dispatch-fix).
Expand it to:

1. **Goal decomposition** — accept a high-level goal and emit a **job DAG**
   (`depends_on`, `parent_job_id`) instead of only per-cluster triage jobs. This
   is gap #1 in the octopus doc.
2. **Capacity-aware dispatch** — consult the capacity ledger (per-service
   headroom) before dispatching, so the planner never schedules into a rate limit.
3. **Proprioception-driven** — read `roster()` + `peers_working_set()`; detect a
   saturated arm or a file conflict and **`offload()`** / rebalance.
4. **Always-on service** — run as a systemd unit on PYTHIA (like
   `hive-reviewer.timer`), heartbeating continuously, rather than a bare loop.
5. **Goal-level reporting** — aggregate a goal's jobs (beyond one `parent_job_id`
   level) so the master can report whole-task progress.

## Bus expansion (the reflex loop — deterministic, no LLM)

- On claim, if the worker's route service is saturated → shed to a peer via the
  existing `offload` path instead of returning 204 (today it 204s).
- Emit `arm.saturated` / `arm.conflict` events when proprioception shows an arm
  over-driven or two arms touching the same file — the doctor cortex consumes
  these to replan.

## The cortex runs on a LOCAL model (operator 2026-10-15)

The brain must be **on-prem** — low latency, no metered cost, no external
dependency. Primary is **CERBERUS** (durable); **TYDEUS is not assumed
long-term**, so it is *not* a default:

| Role | Model | Endpoint | Durability |
|---|---|---|---|
| **Primary brain** | `gemma4-31b` | **CERBERUS** `192.0.2.96:8080/v1` (RTX 4500 Ada) | durable |
| Coder route (not default) | `qwen38` | TYDEUS `192.0.2.73:8006/v1` | **not assumed long-term** |
| **Light orchestration** | (light model) | **HYDRA** `192.0.2.78` / **ACHILLES** `192.0.2.11` (Ryzen 8700G, **ROCm** llama-server) | cheap, always-on |

The doctor's phase defaults were repointed to CERBERUS (were `gateway`/`codex`
via the **retired `:4100`**):

- `DOCTOR_TRIAGE_MODEL` / `DOCTOR_REVIEW_MODEL` / `DOCTOR_FIX_MODEL` → `gemma4-31b`
- `DOCTOR_OPENWEIGHT_BASE_URL` → CERBERUS `:8080/v1`; all phase bases default there
- Override any phase to a **light ROCm model on HYDRA/ACHILLES**:
  `DOCTOR_TRIAGE_BASE_URL=http://192.0.2.78:8080/v1`
- `_invoke_openweight(prompt, alias, base_url=…, model=…)`; local endpoints accept
  placeholder auth (a missing key is only fatal for a base that is explicitly remote)

The codex CLI / gateway paths stay reachable via env override but are **no
longer defaults**. DeepSeek/Anthropic/Together remain forbidden as the brain.

## Net

- **Bus** = brainstem (state, arbitration, capacity, reflexes) — *already running on the hive*.
- **Doctor** = cortex (planning, decomposition, triage) — **expand + run as a hive service**.
- Neither alone; both, with a clean line between deterministic reflex and LLM cognition.
