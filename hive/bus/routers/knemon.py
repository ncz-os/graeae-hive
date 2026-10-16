"""KNEMON cost-aware router + OAuth-cap circuit-breaker (Phase 1c).

Deterministic, no external calls. Given a job's budget ceiling (max_cost_tier
or tier) + kind, return an ordered list of provider/model candidates that fit
the budget, cheapest-acceptable FIRST, then escalate.

COST-TIER CONVENTION (authoritative, from PROVIDER_COST_TIER in core.models):
  A = FREE/cheap-local   B = MID (cheap paid)   C = PREMIUM/reserve
  COST_TIERS = ["A","B","C"]  ordered cheap -> expensive.
`max_cost_tier` is the CEILING (most-expensive tier permitted). A job with
max_cost_tier="C" may use A/B/C providers; "B" -> A/B; "A" -> A only.
This matches the dequeue semantics + the operator intent ("generous budget =>
premium allowed; constrained budget => cheap only").

Endpoints (all under /v1/knemon): providers, route, bypass (get/set),
cap (get/set/clear/probe-ok), spend. All storage reads go through the
HiveMindRepository ABC.
"""

from __future__ import annotations

import os
import secrets
import time
from typing import Any

from fastapi import APIRouter, Depends, Request

from hive.bus.deps import hive_repo, require_mutation_actor
from hive.core.models import COST_TIERS, knemon_candidates

router = APIRouter()

# ── HIVE-LEVEL KNEMON BYPASS FLAG (operator 2026-06-03) ──────────────────────
# Presence of this file = bypass ON for the WHOLE hive. Durable across bus
# restarts; observable (ls/cat); togglable via the /v1/knemon/bypass endpoints
# below or a plain touch/rm. Cheap stat() per route call. When set, the router
# drops the codex/gpt sub-lead and returns the plain metered open-weight chain,
# so EVERY worker that polls /v1/knemon/route runs jobs directly on open-weights
# — the stabilization lever when the GPT-first lead is broken (e.g. hive_gpt
# 400 'missing input' cascade).
KNEMON_BYPASS_FILE = "/srv/agent-bus/knemon_bypass.flag"

KNEMON_CAP_FILE = "/srv/agent-bus/knemon_oauth_cap.json"
# ChatGPT-subscription codex/gpt allowance is a rolling ~5h window (operator/Gemini 2026-06-04).
OAUTH_CAP_RESET_SEC = 5 * 3600
# How long a corrupt/unreadable breaker file keeps the breaker CLOSED (fail-closed)
# before it self-heals — bounded so a transient read error can't pin it open forever.
OAUTH_CAP_STORAGE_BACKOFF_SEC = 300
# While the breaker is open, send a small share of eligible jobs through the
# OAuth lead as half-open probes. PYTHIA cannot probe OpenAI locally.
OAUTH_CAP_PROBE_RATE = 0.05


def knemon_bypass_active() -> bool:
    try:
        return os.path.exists(KNEMON_BYPASS_FILE)
    except Exception:
        return False


def oauth_cap_state() -> dict:
    """OAuth-cap circuit-breaker state. A worker that hits usage_limit on the codex/gpt
    lead POSTs /v1/knemon/cap, opening the breaker so knemon_route stops leading every job
    with the doomed OAuth model. Auto-expires at capped_until (then a probe
    resumes; a probe success clears the file via /v1/knemon/cap/clear)."""
    import json as _j
    import time as _t

    base = {
        "capped": False,
        "capped_until": None,
        "hit_at": None,
        "model": None,
        "reporter": None,
        "verified": False,
        "remaining_sec": 0,
    }
    try:
        with open(KNEMON_CAP_FILE) as f:
            d = _j.load(f)
    except FileNotFoundError:
        return base  # no breaker file = not capped
    except Exception as e:  # noqa: BLE001
        # Corrupt/unreadable breaker file: FAIL CLOSED (assume capped) so the
        # breaker does not silently open and burn the OAuth allowance. BOUND it
        # with a short capped_until so a transient read error self-heals instead
        # of pinning the breaker open forever (capped_until=None would).
        until = _t.time() + OAUTH_CAP_STORAGE_BACKOFF_SEC
        return {
            **base,
            "capped": True,
            "capped_until": until,
            "remaining_sec": OAUTH_CAP_STORAGE_BACKOFF_SEC,
            "storage_error": str(e),
        }
    try:
        until = float(d.get("capped_until") or 0)
    except (TypeError, ValueError):
        return {**base, "capped": True, "storage_error": "invalid capped_until"}
    now = _t.time()
    return {
        "capped": now < until,
        "capped_until": until,
        "hit_at": d.get("hit_at"),
        "model": d.get("model"),
        "reporter": d.get("reporter"),
        "verified": False,
        "remaining_sec": max(0, int(until - now)),
    }


def clear_oauth_cap() -> str | None:
    try:
        os.remove(KNEMON_CAP_FILE)  # no exists() pre-check (avoid TOCTOU)
    except FileNotFoundError:
        pass
    except Exception as e:
        return str(e)
    return None


@router.get("/v1/knemon/providers")
async def knemon_providers():
    """Provider denylist for the worker llm-* capability probe (Phase 2). A
    family listed here is treated as globally dead; workers drop its llm-* cap.
    Set HIVE_PROVIDER_DENYLIST=comma,list (e.g. nvidia,eih)."""
    denied = [p.strip().lower() for p in os.environ.get("HIVE_PROVIDER_DENYLIST", "").split(",") if p.strip()]
    return {"denied": denied}


@router.post("/v1/knemon/route")
async def knemon_route(req: Request):
    """Cost-aware routing. POST {max_cost_tier|tier, kind} -> ordered candidate
    list (cheapest-acceptable first) + the alias chain the worker can consume.
    Deterministic, no external calls, no DB write."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    raw_tier = body.get("max_cost_tier") or body.get("tier") or "C"
    max_tier = str(raw_tier).upper()
    if max_tier not in COST_TIERS:
        max_tier = "C"
    kind = str(body.get("kind") or "")
    lead_alias = str(body.get("lead_alias") or "").strip()
    cands = knemon_candidates(max_tier, kind)
    open_weight_chain = [c["alias"] for c in cands]
    # ── KNEMON sub-bucket dispatch (operator 2026-06-02): openai_codex sub is $0;
    # gpt-5.3/5.4 unlimited. Lead EVERY job with the right codex/gpt agent, then
    # fall back to metered open-weights. Job class -> served model:
    #   review/adversarial -> hive_codex (codex-auto-review)
    #   heavy (architecture/design/heavy:) -> hive_gpt_heavy (gpt-5.5, limited)
    #   light/narrow (triage/docs/investigation) -> hive_gpt_mini (gpt-5.4-mini)
    #   everything else (coding default) -> hive_gpt (gpt-5.4)
    kl = kind.lower()
    review_kind = any(kl.startswith(p) for p in ("review:", "codex", "doctor:codex-fix", "adversarial"))
    arch_kind = any(t in kl for t in ("architecture", "design")) or kl.startswith("heavy:")
    # DEEPSEEK POLICY (operator 2026-06-07) SUPERSEDED 2026-09-07: deepseek
    # direct is now FORBIDDEN fleet-wide, full stop — not just gated to
    # review/architecture. Both aliases are already struck from
    # KNEMON_PROVIDERS above; nothing left here to strip.
    # ADVERSARIAL-ONLY codex (operator 2026-06-17): codex/gpt OAuth leads ONLY
    # review/adversarial kinds. Every other kind leads with the open-weight /
    # local lineup (no OAuth lead) so coding goes local-first per the edict.
    if review_kind:
        CODEX_SUB_LEAD = ["hive_codex"]
    else:
        CODEX_SUB_LEAD = []
    # HIVE-LEVEL BYPASS: when the flag is set, drop the codex/gpt sub-lead and
    # hand back the plain metered open-weight chain so all workers run direct.
    bypass = knemon_bypass_active()
    cap = oauth_cap_state()
    oauth_cap_probe = False
    if bypass:
        CODEX_SUB_LEAD = []
        chain = list(open_weight_chain)
    else:
        # OAuth codex/gpt ($0) leads. Its allowance is a rolling ~5h window (operator/Gemini
        # 2026-06-04) and caps under heavy job load. When it 429s/usage_limit:
        #   review kinds  -> deepseek (REVIEW-ONLY sanction, operator 2026-06-07) then grok
        #   all job kinds -> grok-4.3 only (funded xai). Deepseek is FORBIDDEN as a
        #   job model — observed confabulating/failing heavy jobs AND now policy.
        # hive_ngc_1 = azure/openai/gpt-5.5 via the .4 NGC gateway (operator
        # 2026-06-07: gpt-5.5 ≈ codex quality, free NGC enterprise). It is a
        # proper OpenAI tool-driver (native tool_calls, unlike the earlier
        # gpt-oss/nemotron harmony/pseudo-XML no-ops) — so it LEADS the metered
        # fallback: when OAuth codex/gpt is capped it is the best available
        # driver AND $0. deepseek/nova/grok follow. (NGC nemotron stays as the
        # ngc-review CLI's direct-prompt reviewer, separate from this chain.)
        # ngc/EIH removed 2026-06-17. Open-weight lineup leads (cheap flash first;
        # minimax is DIRECT (hive_minimax_1); never via together (operator 2026-10-15).
        # direct $200 MiniMax key is wired into the .4 gateway alias).
        if review_kind:
            METERED_FALLBACK = ["hive_groq_3", "hive_xai_1"]
        elif arch_kind:
            # High-level architecture: gpt-oss-120b, grok. (deepseek-PRO struck
            # 2026-09-07 — deepseek direct is forbidden fleet-wide.)
            METERED_FALLBACK = ["hive_groq_3", "hive_xai_1"]
        else:
            # General coding rotation (operator 2026-09-07: deepseek-flash lead
            # struck, forbidden fleet-wide). groq qwen3 (fast coder) leads, then
            # groq gpt-oss-120b, grok, and MiniMax direct (hive_minimax_1).
            # MiniMax is NOT host-gated (operator 2026-10-15): ANY zoder/zeroclaw
            # host works ANY job it is dispatched, chosen by CAPABILITY. The
            # MiniMax pool cap is the concurrency guard for the TPM-limited $200
            # plan -- not a host-name rule.
            METERED_FALLBACK = ["hive_groq_2", "hive_groq_3", "hive_xai_1", "hive_minimax_1"]
        if cap.get("capped"):
            # Half-open breaker: most jobs use metered fallback, but a small share leads
            # OAuth so routed traffic can prove recovery before the 5h stale-report TTL.
            if CODEX_SUB_LEAD and secrets.randbelow(10000) < int(OAUTH_CAP_PROBE_RATE * 10000):
                oauth_cap_probe = True
                chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
            else:
                chain = list(METERED_FALLBACK)
        else:
            chain = list(CODEX_SUB_LEAD) + METERED_FALLBACK
    # JOB-SELECTABLE LEAD (operator 2026-06-19): a job may request a specific
    # lead alias (e.g. a host-local GPU coder: hive_cerberus_1 / hive_rocm_*).
    # Honor it as the chain head while keeping the rotation as fallback.
    # BANNED_LEAD_ALIASES (operator 2026-09-07, codex review 2026-09-14):
    # a caller could bypass the deepseek/siliconflow removal above by simply
    # requesting one of the struck aliases directly as lead_alias — this was
    # the removal's only unvalidated entry point. Refuse it there too.
    BANNED_LEAD_ALIASES = {
        "hive_deepseek_1",
        "hive_deepseek_pro_1",
        "hive_siliconflow_1",
    }
    if lead_alias and lead_alias not in BANNED_LEAD_ALIASES:
        chain = [lead_alias] + [a for a in chain if a != lead_alias]
    return {
        "ok": True,
        "max_cost_tier": max_tier,
        "kind": kind,
        "knemon_bypass": bypass,
        "oauth_cap": cap,
        "oauth_cap_probe": oauth_cap_probe,
        "oauth_cap_probe_rate": OAUTH_CAP_PROBE_RATE if cap.get("capped") and CODEX_SUB_LEAD else 0.0,
        "sub_lead": CODEX_SUB_LEAD,
        # codex/gpt sub agent first ($0 unlimited), metered open-weights as fallback
        "chain": chain,
        "candidates": cands,
        "convention": "openai_codex sub leads ($0); open-weights fallback; A=cheap..C=premium ceiling",
    }


@router.get("/v1/knemon/bypass")
async def knemon_bypass_get():
    """Read hive-level KNEMON bypass state."""
    return {
        "ok": True,
        "bypass": knemon_bypass_active(),
        "flag_file": KNEMON_BYPASS_FILE,
    }


@router.post("/v1/knemon/bypass", dependencies=[Depends(require_mutation_actor)])
async def knemon_bypass_set(req: Request):
    """Toggle hive-level KNEMON bypass. POST {enabled: bool, reason?: str}.
    When enabled, ALL workers skip the codex/gpt sub-lead (open-weights only)."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    enabled = bool(body.get("enabled"))
    reason = str(body.get("reason") or "")
    try:
        if enabled:
            with open(KNEMON_BYPASS_FILE, "w") as f:
                f.write((reason or "bypass enabled") + "\n")
        else:
            if os.path.exists(KNEMON_BYPASS_FILE):
                os.remove(KNEMON_BYPASS_FILE)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "bypass": knemon_bypass_active(), "reason": reason}


@router.get("/v1/knemon/cap")
async def knemon_cap_get():
    """OAuth-cap circuit-breaker state (dashboard 5h countdown reads this)."""
    return {
        "ok": True,
        "reset_sec": OAUTH_CAP_RESET_SEC,
        "flag_file": KNEMON_CAP_FILE,
        **oauth_cap_state(),
    }


@router.post("/v1/knemon/cap", dependencies=[Depends(require_mutation_actor)])
async def knemon_cap_set(req: Request):
    """Workers report OAuth cap state.

    POST {model?, reporter?, reset_sec?} opens the breaker on usage_limit.
    POST {success:true, model?, reporter?} closes it after a successful OAuth completion.
    """
    import json as _j
    import time as _t

    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if body.get("success") is True:
        err = clear_oauth_cap()
        if err:
            return {"ok": False, "error": err}
        return {"ok": True, "cleared": True, "success": True, **oauth_cap_state()}
    # debounce the worker stampede: the first usage_limit opens the breaker for the full
    # window; concurrent reporters within the window are no-ops (don't churn/extend it).
    cur = oauth_cap_state()
    if cur.get("capped"):
        return {"ok": True, "debounced": True, **cur}
    try:
        reset = float(body.get("reset_sec") or OAUTH_CAP_RESET_SEC)
    except (TypeError, ValueError):
        reset = float(OAUTH_CAP_RESET_SEC)
    if reset != reset or reset in (float("inf"), float("-inf")):  # NaN/Inf guard
        reset = float(OAUTH_CAP_RESET_SEC)
    reset = max(60.0, min(reset, 86400.0))
    now = _t.time()
    state = {
        "hit_at": now,
        "capped_until": now + reset,
        "model": str(body.get("model") or ""),
        "reporter": str(body.get("reporter") or body.get("worker") or body.get("host") or ""),
    }
    try:
        tmp = KNEMON_CAP_FILE + ".tmp"
        with open(tmp, "w") as f:
            _j.dump(state, f)
        os.replace(tmp, KNEMON_CAP_FILE)  # atomic publish (no partial-read race)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, **oauth_cap_state()}


@router.post("/v1/knemon/cap/clear")
async def knemon_cap_clear():
    """Confirm OAuth reset (a codex/gpt probe succeeded). Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, **oauth_cap_state()}


@router.post("/v1/knemon/cap/probe-ok")
async def knemon_cap_probe_ok():
    """Confirm a half-open OAuth probe succeeded. Closes the breaker."""
    err = clear_oauth_cap()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "cleared": True, **oauth_cap_state()}


@router.get("/v1/knemon/spend")
async def knemon_spend(since_hours: int = 168):
    """Total estimated spend over a window (visibility companion to the router)."""
    # clamp window to a sane range (1h .. 1 year) so a pathological value
    # cannot produce a nonsense cutoff.
    try:
        since_hours = max(1, min(int(since_hours), 24 * 366))
    except (TypeError, ValueError):
        since_hours = 168
    cutoff = time.time() - since_hours * 3600
    async with hive_repo().connection() as tx:
        summary: dict[str, Any] = await hive_repo().spend_summary(tx, cutoff=cutoff)
    return {"since_hours": since_hours, **summary}
