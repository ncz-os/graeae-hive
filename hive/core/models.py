"""Model + pricing registry — the bus's canonical model table.

Extracted from agent_bus.py (Phase 1a / Phase 6 seed). The long-term home is a
central registry that GENERATES this + the per-host zoder/zeroclaw TOMLs
(see docs/refactor-plan-20261015.md Phase 6); today it is the single in-repo
source for cost tiers + per-MTok rates.

MiniMax is direct only (operator 2026-10-15) — never via together.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

# zoder owns the pricing catalog (`zoder pricing refresh` syncs from LiteLLM +
# OpenRouter + an ncz peak-pricing overlay; `zoder pricing show <model>` reads
# it). The hive CONSUMES it rather than hardcoding rates — the built-in table
# below is a fallback for models the catalog does not carry.
ZODER_PRICING_JSON = Path(os.environ.get("ZODER_PRICING_JSON", str(Path.home() / ".zoder" / "pricing.json")))

# ── cost tiers ─────────────────────────────────────────────────────────────
# A = FREE / local / on-prem NGC (claim-anything; no spend)
# B = MID  (cheap paid commercial)
# C = PREMIUM (expensive, reserve)
PROVIDER_COST_TIER: dict[str, str] = {
    "ngc": "A",
    "nvidia": "A",
    "nvidia-ngc": "A",
    "local-llamacpp": "A",
    "local-vllm": "A",
    "ollama": "A",
    "ollama-cerberus": "A",
    "local": "A",
    "pantheon": "A",
    "groq": "B",
    "xai": "B",
    "deepseek": "B",
    "deepseek-direct": "B",
    "together": "B",
    "bedrock": "B",
    "gemini-flash": "B",
    "openai-mini": "B",
    "perplexity": "B",
    "anthropic": "C",
    "claude": "C",
    "openai": "C",
    "openai-pro": "C",
    "openai-gpt55": "C",
    "gemini": "C",
    "gemini-pro": "C",
    "together-pro": "C",
    "unknown": "A",
}
COST_TIERS = ["A", "B", "C"]


def cost_tier_for(provider: str) -> str:
    return PROVIDER_COST_TIER.get((provider or "unknown").lower(), "C")


# ── per-MTok rates (USD): (input, output) ──────────────────────────────────
# Source: ~/.claude/rules/llm-usage-policy-2026-05-22.md
LLM_RATES: dict[tuple[str, str], tuple[float, float]] = {
    ("anthropic", "claude-opus-4-7"): (5.00, 25.00),
    ("anthropic", "claude-opus-4-6"): (5.00, 25.00),
    ("anthropic", "claude-sonnet-4-6"): (3.00, 15.00),
    ("anthropic", "claude-sonnet-4-7"): (3.00, 15.00),
    ("anthropic", "claude-haiku-4-5"): (1.00, 5.00),
    ("anthropic", "*"): (3.00, 15.00),
    ("openai", "gpt-5.5"): (5.00, 30.00),
    ("openai", "gpt-5.5-pro"): (30.00, 180.00),
    ("openai", "gpt-5.4-nano"): (0.20, 1.25),
    ("openai", "o4-mini"): (0.55, 2.20),
    ("openai", "o3"): (2.00, 8.00),
    ("openai", "*"): (5.00, 30.00),
    ("xai", "grok-4.3"): (1.25, 2.50),
    ("xai", "grok-4.1-fast"): (0.20, 0.50),
    ("xai", "grok-4.20"): (2.00, 6.00),
    ("xai", "*"): (1.25, 2.50),
    ("groq", "llama-3.3-70b-versatile"): (0.59, 0.79),
    ("groq", "llama-3.1-8b-instant"): (0.05, 0.08),
    ("groq", "llama-4-scout-17b"): (0.11, 0.34),
    ("groq", "qwen3-32b"): (0.29, 0.59),
    ("groq", "gpt-oss-120b"): (0.15, 0.60),
    ("groq", "gpt-oss-20b"): (0.075, 0.30),
    ("groq", "*"): (0.29, 0.59),
    ("ngc-proxy", "gpt-5.5"): (0.0, 0.0),
    ("ngc-proxy", "*"): (0.0, 0.0),
    ("deepseek-direct", "deepseek-v4-pro"): (0.435, 0.87),
    ("deepseek-direct", "deepseek-v4-flash"): (0.14, 0.28),
    ("deepseek-direct", "*"): (0.435, 0.87),
    ("deepseek", "*"): (0.435, 0.87),
    ("minimax", "minimax-m3"): (0.40, 1.20),  # MiniMax DIRECT — never via together
    ("minimax", "*"): (0.40, 1.20),
    ("together", "deepseek-v4-pro"): (2.10, 4.40),
    ("together", "kimi-k2.6"): (1.20, 4.40),
    ("together", "glm-3.5-90"): (0.10, 0.15),
    ("together", "qwen2.5-coder-32b"): (0.05, 0.12),
    ("together", "*"): (0.40, 1.20),
    ("bedrock", "amazon.nova-micro-v1:0"): (0.035, 0.14),
    ("bedrock", "amazon.nova-lite-v1:0"): (0.06, 0.24),
    ("bedrock", "amazon.nova-pro-v1:0"): (0.80, 3.20),
    ("bedrock", "*"): (0.06, 0.24),
    ("gemini", "gemini-2.5-flash-lite"): (0.10, 0.40),
    ("gemini", "gemini-2.5-flash"): (0.30, 2.50),
    ("gemini", "gemini-3.1-pro"): (2.00, 12.00),
    ("gemini", "*"): (1.25, 10.00),
    ("perplexity", "sonar"): (1.00, 1.00),
    ("perplexity", "sonar-pro"): (3.00, 15.00),
    ("perplexity", "*"): (1.00, 1.00),
}


@lru_cache(maxsize=1)
def _zoder_catalog() -> dict[str, tuple[float, float]]:
    """Load zoder's pricing catalog keyed by MODEL LEAF (case-insensitive).

    Returns {} when the catalog is absent/unreadable, so the built-in table
    always works. Call ``_zoder_catalog.cache_clear()`` after a refresh.
    """
    try:
        data = json.loads(ZODER_PRICING_JSON.read_text())
    except Exception:  # noqa: BLE001 - absent/stale catalog must not break routing
        return {}
    out: dict[str, tuple[float, float]] = {}
    for model, rate in (data.get("models") or {}).items():
        if not isinstance(rate, dict):
            continue
        leaf = str(model).rsplit("/", 1)[-1].lower()
        try:
            out[leaf] = (
                float(rate.get("input_usd_per_mtok", 0.0)),
                float(rate.get("output_usd_per_mtok", 0.0)),
            )
        except (TypeError, ValueError):
            continue
    return out


def rate_for(provider: str, model: str) -> tuple[float, float]:
    p = (provider or "unknown").lower()
    m = (model or "").lower()
    # 1) zoder's fresh catalog (authoritative) — keyed by model leaf.
    catalog = _zoder_catalog()
    if m and m in catalog:
        return catalog[m]
    # 2) curated built-in exact, then provider wildcard.
    if (p, m) in LLM_RATES:
        return LLM_RATES[(p, m)]
    if (p, "*") in LLM_RATES:
        return LLM_RATES[(p, "*")]
    # 3) tier A providers = free; else conservative fallback.
    if cost_tier_for(p) == "A":
        return (0.0, 0.0)
    return (1.0, 5.0)


# ── routing corpus + cheapest-acceptable ranking ───────────────────────────
KNEMON_PROVIDERS: list[dict[str, Any]] = [
    # provider,         model,                 alias,                  in,    out
    {"provider": "groq", "model": "gpt-oss-20b", "alias": "hive_groq_1", "tier": "B"},
    {
        "provider": "gemini",
        "model": "gemini-2.5-flash-lite",
        "alias": "hive_gemini_1",
        "tier": "B",
    },
    # FORBIDDEN 2026-09-07 (operator): neither deepseek nor siliconflow may be
    # used directly, fleet-wide. Both providers removed from the candidate
    # pool (siliconflow was already disabled 2026-06-03 for an invalid key;
    # deepseek-direct's two aliases hive_deepseek_1/hive_deepseek_pro_1 are
    # struck below, not just disabled).
    {
        "provider": "minimax",  # MiniMax DIRECT (operator 2026-10-15: never via together)
        "model": "minimax-m3",
        "alias": "hive_minimax_1",
        "tier": "B",
    },
    {"provider": "xai", "model": "grok-4.1-fast", "alias": "hive_xai_1", "tier": "B"},
    # REMOVED 2026-06-17 (operator: "remove ngc-review from all agents" +
    # compute edict: EIH frontier forbidden). hive_ngc_1 (ngc-proxy gpt-5.5 via
    # the .4 EIH gateway) is no longer a coding/review candidate for any agent.
    # Bedrock Nova (funded AWS creds, worker agents shipped 2026-06-02):
    # cheap metered fallbacks (lite 0.06/0.24, micro 0.035/0.14).
    {
        "provider": "bedrock",
        "model": "amazon.nova-lite-v1:0",
        "alias": "hive_nova_1",
        "tier": "B",
    },
    {
        "provider": "bedrock",
        "model": "amazon.nova-micro-v1:0",
        "alias": "hive_nova_2",
        "tier": "B",
    },
]


def knemon_candidates(max_tier: str, kind: str = "") -> list[dict[str, Any]]:
    """Ordered provider/model candidates within budget, cheapest-acceptable first.

    Cheapest-first is keyed on the per-provider/model OUTPUT rate from LLM_RATES
    (output dominates code-gen spend), then input rate, then provider name for a
    stable deterministic tie-break. Providers whose cost tier exceeds the budget
    ceiling are excluded.
    """
    ceiling_idx = COST_TIERS.index(max_tier) if max_tier in COST_TIERS else len(COST_TIERS) - 1
    out: list[dict[str, Any]] = []
    for p in KNEMON_PROVIDERS:
        # Effective cost tier is the per-entry, MODEL-SPECIFIC tier hint
        # (e.g. gemini-flash-lite=B even though PROVIDER_COST_TIER['gemini']=C,
        # which is the gemini-PRO tier; siliconflow isn't in the static map at
        # all -> defaults to C). The static provider tier is only a fallback
        # when an entry omits its hint. This keeps cheap model variants from
        # being wrongly excluded under a tight budget.
        eff_tier = (p.get("tier") or cost_tier_for(p["provider"])).upper()
        if eff_tier not in COST_TIERS:
            eff_tier = cost_tier_for(p["provider"])
        if COST_TIERS.index(eff_tier) > ceiling_idx:
            continue
        in_rate, out_rate = rate_for(p["provider"], p["model"])
        out.append(
            {
                "provider": p["provider"],
                "model": p["model"],
                "alias": p["alias"],
                "cost_tier": eff_tier,
                "rate_in_per_1m": in_rate,
                "rate_out_per_1m": out_rate,
            }
        )
    out.sort(key=lambda c: (c["rate_out_per_1m"], c["rate_in_per_1m"], c["provider"]))
    return out
