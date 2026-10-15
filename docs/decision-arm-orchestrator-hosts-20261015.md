# Arm orchestrator hosts (HYDRA / ACHILLES)

**Status:** ACHILLES live (2026-10-15); HYDRA pending the same build.
**Owner:** opencode · **Refs:** `docs/decision-rocm-libxml2-fix-20261015.md`, `docs/decision-master-agent-20261015.md`

## Role

HYDRA `.78` and ACHILLES `.11` (Ryzen 7 8700G, Radeon 780M iGPU, ROCm) can each
host the **light orchestration model** (`gemma-4-E2B`, ~3.2 GB) so the doctor's
cortex can run on an arm instead of the CERBERUS 31B. **Either host can take the
orchestrator role** — they are interchangeable.

## They are BUILD HOSTS first → dynamic load/unload

The 780M shares system RAM (GTT), so the orchestrator must yield to builds. A
60-second systemd timer (`arm-orchestrator.timer`) runs
`~/llama-scripts/arm-orchestrator-manage.sh`, which:

- **unloads** the model (kills `llama-server`) when the host is busy —
  `cc1plus`/`rustc`/`bitbake|ninja|cargo build|go build` running, load > 0.6×cores,
  or `MemAvailable < 8 GB`;
- **loads** it when the host is idle and the model isn't already running.

So the model appears when there's headroom and disappears the moment a build
starts. Idempotent; safe to run on every tick.

```
arm-orchestrator.timer (60s) → arm-orchestrator-manage.sh
   busy?  → kill llama-server   (unloaded)
   idle?  → start llama-server :8080 -ngl 99 (loaded)
```

## Serving

`~/llama-scripts/serve-gemma4-arm.sh [model.gguf]` — `HSA_OVERRIDE_GFX_VERSION=11.0.2`,
`-ngl 99`, `:8080`, `--alias gemma4-e2b`, `--jinja`.

**E2B is a reasoning model** — it emits `reasoning_content` (chain-of-thought)
with `content` empty until it finishes. For structured orchestration (JSON
decisions) pass `chat_template_kwargs: {"enable_thinking": false}` → clean
`{"action":…,"reason":…}`. Verified: `2+2=4`; a build-failure triage returns
valid JSON; ~51 tok/s prompt, ~20 tok/s gen.

## Failover (either host)

The doctor should treat the arm as a **pool**, not one URL: try `ACHILLES:8080`,
fall back to `HYDRA:8080`. Because the model is loaded on demand, a health probe
that returns `503 Loading model` means "this arm is bringing the model up" —
retry, or fall through to the other arm, or to CERBERUS.

## Status

| Host | ROCm build | Timer | Verified |
|---|---|---|---|
| **ACHILLES** `.11` | ✅ latest `59d5fad` | ✅ `arm-orchestrator.timer` active | ✅ JSON decision, model loaded on idle |
| **HYDRA** `.78` | ⏳ needs the same rebuild + shim | ⏳ | — |

Replicate on HYDRA via `docs/decision-rocm-libxml2-fix-20261015.md`, then copy
`arm-orchestrator-manage.sh` + the `.service`/`.timer`.

## Caveat

A 3–4B model (even reasoning) is **weaker than the 31B** for complex planning.
Use the arms for **light** orchestration (triage, classification, JSON dispatch,
routing); keep CERBERUS `gemma4-31b` as the quality brain for decomposition.
