"""Pure job helpers + result-cache (Phase 1c/1a extraction).

Moved verbatim from agent_bus.py so the jobs router can import them without a
cycle into the monolith. No FastAPI, no request state — pure functions + the
cache read/write helpers that take a `db`.
"""

from __future__ import annotations

import hashlib as _hashlib
import json
import os
import re
import time

from hive.core.models import rate_for


def is_dedup_violation(exc: BaseException) -> bool:
    """Unique-violation from the active-only dedup index ONLY (never another PK/unique)."""
    m = str(exc)
    return "HIVE_JOBS_ACTIVE_DEDUP_UQ" in m.upper() or (
        "unique constraint failed" in m.lower() and "dedup_hash" in m.lower()
    )


def json_list(raw: str | None) -> list:
    """Parse a JSON-array-of-strings column; [] on any malformed/None input."""
    if not raw:
        return []
    try:
        val = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return []
    return [x for x in val if isinstance(x, str)] if isinstance(val, list) else []


MAX_REPORTED_TOKENS = 100_000_000  # sane ceiling; rejects absurd worker telemetry


def field_was_set(model, name: str) -> bool:
    """Pydantic v1/v2 'was this field explicitly supplied?' check."""
    fields_set = getattr(model, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(model, "__fields_set__", set())
    return name in fields_set


def _safe_nonneg_int(v, default: int = 0) -> int:
    """Coerce an untrusted worker-reported value to a clamped non-negative int.

    Worker result bodies are runtime/untrusted; a malformed token field
    (``"12.5"``, ``{}``, ``None``, a negative, ``Infinity``/``NaN`` which JSON
    can carry, a 100-digit string) must NOT raise on the hot job-completion
    path, decrement cumulative counters, or overflow the numeric column."""
    if isinstance(v, bool):  # bool is an int subclass — never a token count
        return default
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return default
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        return default
    if n <= 0:
        return 0
    return min(n, MAX_REPORTED_TOKENS)


def _safe_str(v, default: str = "unknown") -> str:
    if v is None:
        return default
    try:
        s = str(v).strip()
        return s or default
    except Exception:  # noqa: BLE001
        return default


def stable_worker_id(urn: str) -> str:
    """Collapse a session URN to a stable per-worker identity for stats.

    ``urn:agent:<kind>:<host>:<session_uuid>`` -> ``urn:agent:<kind>:<host>``.
    The session segment changes on every worker restart, so keying worker
    stats by the full URN fragments one worker's history across thousands of
    rows. Keying by the stable prefix aggregates a worker's runs across
    restarts. Non-conforming URNs are returned unchanged.
    """
    if not urn:
        return urn
    parts = urn.split(":")
    if len(parts) >= 5 and parts[0] == "urn" and parts[1] == "agent":
        return urn.rsplit(":", 1)[0]
    return urn

# Skip releases (host_declines_kind/released_by_host/no_workspace_for_kind) are
# legitimate "not me" routing signals. They must not burn the decline dead-letter
# threshold; real execution failures use the failed/retry path.
#
# 2026-10-27 (dead-letter-explains-itself): the fleet's `invalid_base_branch` /
# `invalid_head_branch` releases are ALSO job-level unschedulable — a branch
# that does not exist on origin can never be cloned by ANY host, so the job is
# doomed no matter who claims it. The old code left them OUT of the counted
# set (so they hot-looped the reclaim until max_retries) but DID record them
# as `worker_error` on the release, which the bus then counted as a job-level
# decline anyway (the 2026-10-27 incident: 3 jobs dead-lettered with
# retry_count=3 and no visible reason). Add them to DECLINE_REASON_PREFIXES so
# the counted set matches the real fleet vocabulary and the dead-letter
# transition carries the reason. The reason strings themselves are unchanged —
# existing writers (hive_worker.py) keep working.
MAX_DECLINE_REQUEUES = 3
DECLINE_REASON_PREFIXES: tuple[str, ...] = (
    "no_workspace_for_kind",
    "no_workspace_for_repo",
    "invalid_base_branch",
    "invalid_head_branch",
)  # job-level unschedulable -> count toward dead-letter (terminate reclaim loop); host_declines_kind/released_by_host stay OUT (matcher routes to capable hosts)
STATUS_TRANSITIONS: dict[str, set[str]] = {
    "queued": {"queued", "offered", "claimed", "cancelled"},
    "offered": {"queued", "claimed", "cancelled"},
    "claimed": {
        "queued",
        "claimed",
        "running",
        "done",
        "failed",
        "cancelled",
        "needs-review",
    },
    "running": {"queued", "running", "done", "failed", "cancelled", "needs-review"},
    "needs-review": {
        "needs-review",
        "done",
        "failed",
        "cancelled",
    },  # awaiting review; promotable
    "done": {
        "done",
        "cancelled",
    },  # allow cancelling fake/duplicate completions out of done
    # A failed job may be RETIRED as cancelled (superseded work), never
    # reopened -- same exception as done -> cancelled. Without it a
    # superseded failure stayed "failed" forever (2026-10-03).
    "failed": {"failed", "cancelled"},
    "cancelled": {"cancelled"},
    "dead-letter": {"dead-letter"},  # terminal; idempotent same-status PATCH only
}
CLAIM_LEASE_SECONDS = float(os.environ.get("CLAIM_LEASE_SECONDS", "1800"))


def valid_completion_proof(proof: dict) -> bool:
    """Fail closed on missing Git evidence, including successful subprocesses.

    NOTE (operator directive 2026-10-15): the `remote_verified` / `remote_sha`
    checks were removed together with the Spark remote-agent path (see
    docs/refactor-proposal-harness-agnostic-20261015.md § "Operator directive").
    Local Git evidence is now the sole completion gate — completion is proven
    at the bus against local repo state, not by a remote-agent attestation.
    The gate still fails closed: every listed field must be present and exact.
    """
    sha = proof.get("head_sha")
    commits = proof.get("commits")
    files = proof.get("files_changed")
    return bool(
        isinstance(sha, str)
        and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", sha)
        and isinstance(commits, list)
        and sha in commits
        and all(isinstance(c, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", c) for c in commits)
        and isinstance(files, list)
        and files
        and all(isinstance(f, str) and f for f in files)
        and proof.get("worktree_clean") is True
    )


def normalize_result_payload(status: str, result: dict | None) -> dict | None:
    if result is None or status != "done":
        return result
    normalized = dict(result)
    commits = normalized.get("commits")
    files_changed = normalized.get("files_changed")
    normalized["commits"] = commits if isinstance(commits, list) else []
    normalized["files_changed"] = files_changed if isinstance(files_changed, list) else []
    return normalized


# Per-million-token rates (USD). Workers SHOULD report tokens_in/tokens_out in PATCH result;
# hive computes estimated_cost_usd. Wildcard model "*" applies to any model on that provider.
SUBSCRIPTION_PROVIDERS = {
    "openai",
    "codex",
    "codex-oauth",
    "codex-cli",
    "gpt",
    "chatgpt",
}


def estimate_cost(provider: str, model: str, tokens_in: int, tokens_out: int) -> float:
    # openai + codex = ONE flat ChatGPT Pro subscription ($100/mo), NOT metered.
    if (provider or "").lower() in SUBSCRIPTION_PROVIDERS:
        return 0.0
    in_rate, out_rate = rate_for(provider, model)
    return round((tokens_in / 1_000_000) * in_rate + (tokens_out / 1_000_000) * out_rate, 6)


def hallucination_check(result: dict) -> str | None:
    """Return reason string if result looks like an LLM token-loop / hallucination,
    else None. Saves marking-done garbage that wastes a cache entry + claims completion.

    Heuristics:
    - >40% of stdout is a single repeated word/phrase
    - stdout >1500 chars with <50 unique words (Kimi-K2.6 'extension extension extension' loop)
    - stdout contains 'Rate limit exceeded' / 'Authentication error' / 'context-length exceeded'
    """
    if not isinstance(result, dict):
        return None
    stdout = result.get("stdout") or result.get("output") or ""
    if not isinstance(stdout, str) or len(stdout) < 200:
        return None
    if "rate limit exceeded" in stdout.lower():
        return "rate_limit_in_output"
    if "authentication error" in stdout.lower() or "authentication failed" in stdout.lower():
        return "auth_error_in_output"
    if "context length" in stdout.lower() and "exceed" in stdout.lower():
        return "context_overflow_in_output"
    # token-loop detection
    words = stdout.split()
    if len(words) > 200:
        from collections import Counter

        top_word, top_count = Counter(words).most_common(1)[0]
        if top_count / len(words) > 0.4:
            return f"token_loop:{top_word!r}_repeated_{top_count}_of_{len(words)}"
    return None


# RESULT CACHE — Nemotron killer feature 2026-05-23.
# Memoize (kind, description, max_cost_tier, sorted-required-capabilities) → result.
# When identical job submitted, return cached result instantly. Cuts LLM spend
# 30-70% on repetitive work + avoids NGC 429 storms from duplicate dispatches.

CACHE_TTL_SECONDS = 24 * 3600  # 24h default; idempotent work like compiles/lints often valid much longer


def cache_key_for(
    kind: str,
    description: str | None,
    max_cost_tier: str,
    required_capabilities: list[str] | None,
) -> str:
    norm_desc = (description or "").strip()
    norm_caps = ",".join(sorted(required_capabilities or []))
    payload = f"{kind}\n{norm_desc}\n{max_cost_tier}\n{norm_caps}"
    return _hashlib.sha256(payload.encode()).hexdigest()


async def cache_lookup(db, cache_key: str) -> dict | None:
    cutoff = time.time() - CACHE_TTL_SECONDS
    async with db.execute(
        "SELECT result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at "
        "FROM hive_cache WHERE cache_key=? AND cached_at >= ?",
        (cache_key, cutoff),
    ) as cur:
        r = await cur.fetchone()
    if not r:
        return None
    _res = json.loads(r[0]) if r[0] else None
    # Defensive: never SERVE a poisoned needs-review entry as a done answer,
    # even if one slipped into the table before the store-side guard landed
    # (orphan propagation, 2026-06-07).
    if isinstance(_res, dict) and (_res.get("needs_review") or _res.get("status") == "needs-review"):
        return None
    return {
        "result": _res,
        "source_job_id": r[1],
        "result_mnemos_id": r[2],
        "hit_count": r[3],
        "cost_saved_usd": r[4],
        "model": r[5],
        "provider": r[6],
        "cached_at": r[7],
    }


def _cache_json(result: dict, limit: int = 32000) -> str:
    """Serialize a job result for hive_cache without breaking JSON validity.

    The old code sliced the serialized string to 32000 chars; any result
    bigger than that (e.g. agentic results carrying a format-patch)
    became truncated NON-JSON and tripped the Oracle `result_json IS JSON`
    check constraint -> every PATCH on the job 500'd (2026-06-06). Trim the
    bulky FIELDS instead, and fall back to a stub if still oversized.
    """
    payload = json.dumps(result, default=str)
    if len(payload) <= limit:
        return payload
    slim = dict(result)
    for k in ("patch", "suggestion", "full_response", "context"):
        v = slim.get(k)
        if isinstance(v, str) and len(v) > 2000:
            slim[k] = v[:2000] + f"...[cache-trimmed {len(v) - 2000} chars]"
    payload = json.dumps(slim, default=str)
    if len(payload) <= limit:
        return payload
    return json.dumps(
        {
            "cache_truncated": True,
            "exit_code": result.get("exit_code"),
            "status": result.get("status"),
        },
        default=str,
    )


async def cache_store(
    db,
    cache_key: str,
    source_job_id: str,
    result: dict,
    result_mnemos_id: str | None,
    model: str,
    provider: str,
    cost_for_save: float,
):
    now = time.time()
    await db.execute(
        "INSERT INTO hive_cache (cache_key, result_json, source_job_id, result_mnemos_id, "
        "hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
        "VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
        "ON CONFLICT(cache_key) DO UPDATE SET "
        "result_json=excluded.result_json, source_job_id=excluded.source_job_id, "
        "result_mnemos_id=excluded.result_mnemos_id, cached_at=excluded.cached_at, "
        "model=excluded.model, provider=excluded.provider",
        (
            cache_key,
            _cache_json(result),
            source_job_id,
            result_mnemos_id,
            model,
            provider,
            now,
        ),
    )


async def cache_record_hit(db, cache_key: str, cost_saved: float):
    await db.execute(
        "UPDATE hive_cache SET hit_count = hit_count + 1, "
        "cost_saved_usd = COALESCE(cost_saved_usd,0) + ?, last_hit_at = ? "
        "WHERE cache_key=?",
        (cost_saved, time.time(), cache_key),
    )
