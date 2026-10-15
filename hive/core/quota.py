"""Worker-side telemetry capture for the capacity ledger.

The bus never sees the harness's LLM call — the WORKER does. So the worker
captures per-call signals and reports them in the job PATCH ``quota`` block
(see ``docs/decision-capacity-ledger-20261015.md``).

Two capture surfaces:
  * CLI output (json/mode) → token counts + (subscription) usage %
  * API response headers   → rate-limit ceiling/remaining/reset

Kept dependency-light: pure functions over dicts/strs so any adapter can use it.
"""

from __future__ import annotations

import json
from typing import Any

# Provider header prefixes for the rate-limit signals.
_HEADER_PREFIXES = ("x-ratelimit-", "anthropic-ratelimit-", "ratelimit-")


def parse_ratelimit_headers(headers: dict[str, str]) -> dict[str, Any]:
    """Extract rate-limit signals from response headers (case-insensitive)."""
    out: dict[str, Any] = {}
    for key, value in (headers or {}).items():
        lk = key.lower()
        if not any(lk.startswith(p) for p in _HEADER_PREFIXES):
            continue
        short = lk
        for p in _HEADER_PREFIXES:
            if lk.startswith(p):
                short = lk[len(p):]
                break
        # numeric where possible, else keep the raw (e.g. reset timestamps)
        try:
            out[short] = int(value)
        except (TypeError, ValueError):
            out[short] = value
    return out


def extract_tokens(payload: Any) -> dict[str, int] | None:
    """Best-effort token extraction from a JSON API body or CLI json output.

    Handles the common shapes: OpenAI/Anthropic ``usage`` blocks, a flat
    ``tokens_in``/``tokens_out``, and the CLI's own naming.
    """
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else payload

    def _get(*names: str) -> int | None:
        for n in names:
            v = usage.get(n)
            if isinstance(v, (int, float)):
                return int(v)
        return None

    tin = _get("prompt_tokens", "input_tokens", "tokens_in", "input")
    tout = _get("completion_tokens", "output_tokens", "tokens_out", "output")
    if tin is None and tout is None:
        return None
    return {"in": tin or 0, "out": tout or 0}


def parse_cli_json(stdout: str) -> dict[str, Any]:
    """Parse a harness CLI's json output. Best-effort; returns {} on failure."""
    if not stdout:
        return {}
    text = stdout.strip()
    # tolerate a json object embedded in log lines
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return {}
    try:
        return json.loads(text[start : end + 1])
    except (ValueError, TypeError):
        return {}


def quota_block(
    *,
    provider: str,
    model: str,
    tokens_in: int | None = None,
    tokens_out: int | None = None,
    ratelimit: dict[str, Any] | None = None,
    subscription: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the ``quota`` block attached to a job result."""
    block: dict[str, Any] = {
        "provider": provider,
        "model": model,
        "tokens": {"in": tokens_in or 0, "out": tokens_out or 0},
    }
    if ratelimit:
        block["ratelimit"] = ratelimit
    if subscription:
        block["subscription"] = subscription
    return block
