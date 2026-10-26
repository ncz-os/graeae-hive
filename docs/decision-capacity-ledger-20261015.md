# Decision: per-service capacity ledger + admission control

**Status:** Proposed (2026-10-15) · **Owner:** opencode · **Phase 5 of** `docs/refactor-plan-20261015.md`

> **Update 2026-10-26:** the bus's KNEMON router, its OAuth-cap breaker
> (`oauth_cap_state`, `/v1/knemon/cap`) and the bus `KNEMON_PROVIDERS` table
> were removed. References below are to the code as it was on 2026-10-15.

## Problem

KNEMON bookkeeps *budget* per **agent** (`plan_cap_usd` / `plan_period_used_usd`)
and infers from 429s (`oauth_cap_state`). The real goal is to **dole out jobs
against each model service's *available capacity* so the fleet never exceeds a
service's API throughput / credit pool.** The current axis (per-agent) is wrong:
many agents each "under cap" can still blow one shared service's throughput.

## Capacity is 4-dimensional, declared PER SERVICE

| Dimension | Meaning | Example |
|---|---|---|
| **slots** | concurrent in-flight sessions | MiniMax **~4–5** ($500/yr plan) |
| **rate** | tokens/requests per minute | OpenAI / Anthropic TPM·RPM |
| **credits** | spend vs a pool over a window | DeepSeek prepaid; OAuth/subscription windows |
| **cost × time** | $/MTok scaled by time | **DeepSeek peak 2×** (01–04 & 06–10 UTC Mon–Fri) |

## Harness scoping (which harness can reach which service)

Capacity is scoped by **harness**, not just by service — the operator's boundary
(2026-10-15):

| Harness | Service pool | Credits |
|---|---|---|
| **zoder** | OPEN models/services only: MiniMax, DeepSeek, EIH/NGC, local compute (+ groq/gemini/xai/bedrock) | metered / local |
| **claude** CLI | **Anthropic only** | Claude Pro/Max subscription — usable *only* through the Claude CLI |
| **codex** CLI | **OpenAI only** | ChatGPT/Codex subscription — only through the Codex CLI |
| **opencode** | backup harness (runs on zoder hosts) | per its config |

Anthropic/OpenAI are **not reachable from zoder** — verified: `KNEMON_PROVIDERS`
contains zero `anthropic`/`openai` entries. So:

- The ledger keys by **(harness, service)** and **accounts per harness**.
- A job's **capability match includes its harness**; the router never sends an
  Anthropic/OpenAI job down the zoder path.
- `LLM_RATES`'s `anthropic`/`openai` rows are for **cost estimation of the
  claude/codex harnesses**, not for zoder routing.

**Encapsulation option (rejected as default):** zoder *could* wrap the claude/codex
CLIs as a "harness for those systems", but the subscription credits are only
usable through the native CLIs, and wrapping loses their native usage signal.
Keep them **separate harnesses** with their own accounting; revisit only if a
single dispatch surface is needed.

## What each service actually exposes (settled 2026-10-15)

| Service | Ceiling | Live remaining | Pre-flight | Balance |
|---|---|---|---|---|
| **Anthropic API** | ✅ Admin Rate-Limits API `GET /v1/organizations/limits` (+ `/workspaces/{id}/limits`) | ✅ `anthropic-ratelimit-{tokens,input-tokens,output-tokens}-remaining` | ✅ **`POST /v1/messages/count_tokens`** (free) | ❌ (`/v1/organizations/balance` → 404) |
| **OpenAI API** | ⚠️ per-model via `x-ratelimit-limit-*` (+ `/v1/fine_tuning/model_limits` for FT) | ✅ `x-ratelimit-remaining-*` | ⚠️ (no public count endpoint) | ❌ (dashboard only) |
| **DeepSeek** | ⚠️ headers | ⚠️ headers | ❌ | ✅ **`GET /user/balance`** (verified: $171.50) |
| **Groq / xAI** | ⚠️ `x-ratelimit-limit-*` | ✅ `x-ratelimit-remaining-*` | ❌ | ❌ |
| **Claude Pro/Max** (sub) | 🔶 declared | 🔶 CLI usage line | — | ❌ |
| **ChatGPT / Codex** (sub) | 🔶 declared | 🔶 CLI usage line | — | ❌ |
| **MiniMax $500/yr** (sub) | 🔶 declared (~5 slots) | 🔶 self-meter slots | — | ❌ |

Two honesty tiers:
- **API-key surfaces** → **accurate**: ceilings + live remaining + (Anthropic) pre-flight token count + (DeepSeek) balance.
- **Subscription surfaces** → **estimated, self-correcting**: operator-declared seed + CLI usage line + 429-learning.

No service exposes a *subscription remaining-quota* or *financial balance* API
(DeepSeek prepaid balance is the sole exception).

## Architecture

```
        registry (Phase 5 source): per-service {slots, rate{tpm,rpm}, credits{cap,window}, cost×time}
                 │
   the bus does NOT see LLM responses — the WORKER makes the call.
                 │
  WORKER per call:  body.prompt/completion/total_tokens  +  ratelimit headers  →  "quota" block
                 │                       (Anthropic: also POST count_tokens BEFORE sending)
                 ▼
        ledger  in_flight[s], remaining_rate[s], spent[s][window], limited_until[s]
                 │
        ADMISSION (claim/dequeue): dispatch iff capability_match
                 AND slots_free≥1 AND rate_remaining≥est_tokens(service) AND credits_ok
        REPORT: job PATCH folds the quota block into the ledger
        LEARN:  a 429/usage-limit sets limited_until[s] to the learned window and
                tightens the effective ceiling (EMA)
```

### Result-schema extension (job PATCH)

```json
"quota": {
  "provider": "anthropic", "model": "claude-…",
  "tokens": {"in": 8123, "out": 1904},
  "ratelimit": {"tokens_limit": 100000, "tokens_remaining": 31877, "reset": "2026-10-15T19:01:00Z"},
  "subscription": {"window": "5h", "used_pct": 62}   // CLI-sourced, subs only
}
```

## Admission rules

1. **Capability first** (arch, language, …) — host name is irrelevant (operator 2026-10-15).
2. Then **capacity**: slots free AND rate headroom AND credits OK.
3. **Cost-aware order**: cheapest acceptable, **offpeak-preferred** for deferrable work (DeepSeek ×2 peak); non-deferrable pays the peak multiplier.
4. **Hold vs reroute**: if no capable service has headroom, hold (or route to a capable alternate), never launch into a known rate limit.

## Concrete instances

- **MiniMax slot cap** (currently missing from `dequeue_next_job`) is the **first instance** of this general mechanism, not a special case: `slots≈5`, `credits=$500/yr`.
- **DeepSeek balance poller** (verified working) — periodic.
- **Anthropic pre-flight**: `count_tokens` before dispatch so a large job doesn't consume a window it can't finish.
- **429-learning** generalizes `oauth_cap_state` → `limited_until[service]`.

## Reuse / fix

- **Reuse** `~/.claude/rules/bin/deepseek-window.py` as `time_multiplier(provider, now)`.
- **Fix** `DEFAULT_PLAN_CAPS`: key by **service** (or move to the registry); MiniMax → `$500/yr`, `slots≈5` (code's `$200` is stale); drop the Anthropic-era `$200/$100` as the *subscription* seed (Claude Pro = `$100/$200` plans with rolling 5h + weekly windows; Max = 5–20× Pro).

## Refs
- `docs/refactor-plan-20261015.md` (Phase 5), `docs/decision-repo-registry-20261015.md` (same build pattern)
- `~/.claude/rules/deepseek-peak-pricing.md`
- Anthropic: Rate Limits API + `count_tokens`; OpenAI: rate-limit headers + `model_limits`; DeepSeek: `/user/balance`
