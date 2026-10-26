"""Submit-time `preferred_models` admission (defect 2026-10-25, silent fallback).

``JobCreate.preferred_models`` is a ROUTING PREFERENCE, not a binding: the
worker's ladder (``hive_worker.escalation_agent``) starts at the first rung it
recognises and silently falls back to the local coder (``qwen38``) when none of
the requested names resolves. A forbidden model, a typo, or a model no eligible
worker serves therefore produced a job that ran on qwen38 with no signal to the
submitter at all.

This module is the bus-side half of the fix. It is PURE (no DB, no network, no
FastAPI) so the submit path can call it inside the request and the tests can
call it directly:

  * :func:`forbidden_models` -- names the fleet's routing policy forbids. A job
    that asks for one of these is REJECTED at submit (422): accepting it would
    advertise a route that does not exist and quietly burn a fallback.
  * :func:`served_model_names` -- every model name some REGISTERED worker says
    it can serve (its registration ``model`` plus the catalog models its
    ``provider`` serves). Anything not in this set is a name no worker will
    ever run, so it is surfaced as a warning instead of being silently
    dropped.
  * :func:`admission_warnings` -- the non-fatal half: names no registered
    worker serves. The job is still accepted (a preference is not a
    requirement) but the submitter is TOLD, in the submit response's
    ``warnings[]``, that the name will not be honoured.

Design note: warnings are advisory, rejection is not. The difference matters:
a forbidden model can never run anywhere (hard error), while an unserved name
might be served by a worker that registers a minute later (soft warning).
"""

from __future__ import annotations

from typing import Any

from hive.core.models import LLM_RATES, cost_tier_for

# ── forbidden models ────────────────────────────────────────────────────────
# Operator directives, kept in code (not config) so the vocabulary an
# operator can act on never depends on an env var being set on one host:
#
#   * DeepSeek direct is forbidden FLEET-WIDE (operator 2026-09-07) and was
#     struck from KNEMON_PROVIDERS (their aliases removed, not disabled) —
#     see hive/core/models.py. A job may not request the route anyway.
#   * NVIDIA EIH may serve ONLY Nemotron or an open-weight model (operator
#     2026-10-17). Closed frontier models are forbidden on EIH, so the EIH
#     gpt-5.5/grok/claude labels must never be requested as a route.
FORBIDDEN_MODELS: frozenset[str] = frozenset(
    {
        # `deepseek-direct` is a PROVIDER, not a model: it lives in
        # FORBIDDEN_MODEL_PROVIDERS so `deepseek-direct/<anything>` is refused
        # at the provider half. Keeping it here as well meant
        # `deepseek-direct/flash` slipped through (provider not in the provider
        # set, leaf not in the model set) while `deepseek-direct/deepseek-pro`
        # was caught only by accident via the leaf.
        "deepseek-pro",
        "siliconflow",
        # Closed-frontier / EIH-banned labels (the EIH gpt-5.5 route no longer
        # exists; the hive normalizes it to MiniMax at registration).
        "gpt-5.5-eih",
        "eih/gpt-5.5",
        "ngc/gpt-5.5",
        "azure/gpt-5.5",
    }
)

#: Providers whose models are forbidden outright (the provider name alone is
#: enough: deepseek-direct is banned fleet-wide, siliconflow was disabled for
#: an invalid key, and the EIH provider may only carry open-weight models).
FORBIDDEN_MODEL_PROVIDERS: frozenset[str] = frozenset({"siliconflow", "deepseek-direct"})


def _norm_model(name: str) -> str:
    return (name or "").strip().lower()


def forbidden_model(name: str) -> bool:
    """Whether ``name`` is a model the fleet's routing policy forbids.

    A name is forbidden when it is (a) on :data:`FORBIDDEN_MODELS`, (b) the
    model half of a ``<provider>/<model>`` key whose model half is forbidden,
    or (c) served by a forbidden provider. Only the MODEL identity matters:
    ``deepseek/flash`` stays legal (DeepSeek through its sanctioned provider),
    while ``deepseek-direct/deepseek-pro`` does not.
    """
    n = _norm_model(name)
    if not n:
        return False
    if n in FORBIDDEN_MODELS or n in FORBIDDEN_MODEL_PROVIDERS:
        return True
    # `provider/model`: the leaf, or the provider, can be forbidden.
    if "/" in n:
        provider, _, model = n.partition("/")
        if provider in FORBIDDEN_MODEL_PROVIDERS or model in FORBIDDEN_MODELS:
            return True
    return False


def _served_by_provider(provider: str) -> set[str]:
    """Every model name the bus believes ``provider`` can serve for code work.

    Built from the routing tables (``LLM_RATES`` keys, the KNEMON candidate
    list) so it moves with the registry: a model added to the registry is a
    served name here without a second list to maintain.
    """
    p = _norm_model(provider)
    if not p:
        return set()
    names: set[str] = set()
    for prov_key, model_key in LLM_RATES:
        if prov_key != p or model_key == "*":
            continue
        names.add(model_key)
    try:
        from hive.core.models import KNEMON_PROVIDERS

        for entry in KNEMON_PROVIDERS:
            if _norm_model(entry.get("provider")) == p:
                names.add(_norm_model(entry.get("model")))
                alias = entry.get("alias")
                if isinstance(alias, str) and alias:
                    names.add(_norm_model(alias))
    except Exception:  # noqa: BLE001 - routing corpus is optional for this check
        pass
    return names


def local_model_names() -> set[str]:
    """Models hosted by a fleet engine (cost tier A) — always claimable."""
    return {model for provider, model in LLM_RATES if cost_tier_for(provider) == "A" and model != "*"}


def served_model_names(agents: list[dict[str, Any]] | None = None) -> set[str]:
    """Every model name a registered worker can actually run.

    ``agents`` is a list of ``{"model": str, "provider": str}`` (the shape
    :meth:`HiveMindRepository.active_agent_models` returns). Each worker
    contributes its registered model and the catalog models its provider
    serves — a worker registered as ``("unknown", "qwen38")`` (the fleet's
    bare-name convention, e.g. the ARGONAS/gateway workers) therefore makes
    ``qwen38`` a served name.

    Names are matched case-insensitively on the MODEL leaf so a submitter can
    write ``MiniMax-M3`` or ``minimax-m3`` and be understood either way.
    """
    served: set[str] = set(local_model_names())
    for agent in agents or []:
        provider = _norm_model(agent.get("provider") or "")
        model = _norm_model(agent.get("model") or "")
        if model and model != "unknown":
            served.add(model)
            # `provider/model` keys: the leaf is the name a submitter writes,
            # and the QUALIFIED key must stay in the set too — otherwise
            # `_is_served` can never match a `provider/model` preference the
            # fleet really serves (the qualified branch would be dead code).
            if "/" in model:
                served.add(model.rsplit("/", 1)[1])
            elif provider:
                served.add(f"{provider}/{model}")
        for name in _served_by_provider(agent.get("provider") or ""):
            served.add(name)
            if "/" in name:
                served.add(name.rsplit("/", 1)[1])
            elif provider:
                served.add(f"{provider}/{name}")
    served.discard("")
    served.discard("*")
    served.discard("unknown")
    return served


def _is_served(key: str, served: set[str]) -> bool:
    """Whether the normalized preference ``key`` names a model the fleet serves.

    A bare name matches the served set directly. A provider-QUALIFIED name
    (``someprovider/qwen38``) must match as a whole or have its provider
    registered — matching only the leaf would suppress the warning for a
    ``provider/model`` combination no worker actually serves, which is exactly
    the silent-fallback signal this change exists to surface.
    """
    if key in served:
        return True
    if "/" not in key:
        return False
    provider, _, leaf = key.partition("/")
    if provider in served:
        return True
    # Accept a qualified key whose provider is known and whose leaf it serves.
    return any(name.startswith(f"{provider}/") and name.rsplit("/", 1)[1] == leaf for name in served)


def admission_warnings(preferred_models: list[str] | None, served: set[str]) -> list[str]:
    """``warnings[]`` entries for requested models no registered worker serves.

    One message per distinct name, in submission order, naming the model and
    stating that the job will fall back. Empty when every name is served —
    which is exactly the negative control the tests pin.
    """
    warnings: list[str] = []
    # Deduplicate on the NORMALIZED key, not on the rendered message. The same
    # model written two ways ('MiniMax-M3' and 'minimax-m3') is one preference
    # the fleet cannot serve, and needs one warning -- the first spelling the
    # submitter used is quoted, and the others are folded in, so submission
    # order is preserved and the count matches the number of distinct models.
    seen: set[str] = set()
    for raw in preferred_models or []:
        key = _norm_model(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        if _is_served(key, served):
            continue
        warnings.append(
            f"preferred_model_not_served: {raw!r} is not served by any registered worker; "
            "this preference will be ignored and the job may run on a fallback model "
            "(e.g. qwen38)"
        )
    return warnings
