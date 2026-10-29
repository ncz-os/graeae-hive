"""Standing hourly probe of the permitted open-weight models on EIH.

EIH is the NVIDIA inference hub (``https://integrate.api.nvidia.com/v1``).  The
fleet's belief that "after hours is better" rested on a single spot check: the
usage ledgers hold no latency and no first-token time, and the failures we do
see are a mix of gateway outages (503), our own 429 bursts, reasoning models
burning the whole token budget before emitting an answer, client timeouts, one
anycast address unreachable from some hosts, and models that are listed but
not provisioned (404 ``Function not found``).  This tool measures all of that,
one row per model per run, appended to a daily JSONL file.

Design constraints (operator/zoder-owner approved, 2026-10-26):

* stdlib only -- no httpx, no requests, no SDK;
* strictly sequential, one request in flight, one retry at most;
* probes never raise past :func:`main`; failures are data (exit 0), only a
  configuration error exits non-zero;
* the API key never reaches a log line or an output row.

Key discovery mirrors how zoder is configured on a fleet host: the key is the
``api_key`` of the ``nvidia-eih`` provider / ``custom.eih_reviewer`` model
block in ``~/.zoder/config.toml`` (or ``config.fleet.toml``), and zoder also
accepts it through the environment because ``~/.zoder/shell-env.sh`` sources
the mode-600 ``~/.zoder/secrets.env`` (``KEY=value`` lines).  We take
``EIH_API_KEY`` first, then the key file (``EIH_KEY_FILE`` or
``~/.zoder/secrets.env``), then the same ``api_key`` field out of the zoder
config files, so an installed timer works with no extra plumbing.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

TOOL_VERSION = "1.0.0"

EIH_BASE_URL = "https://integrate.api.nvidia.com/v1"
EIH_CHAT_PATH = "/chat/completions"
PROVIDER_ID = "nvidia-eih"
PROBE_TOOL = "direct-interim"

# ---------------------------------------------------------------- models ----
# Permitted open-weight models only.  EIH carries Nemotron and other
# open-weight models; DeepSeek is direct-only, and the closed frontier
# families are forbidden on EIH outright.
DEFAULT_MODELS: tuple[str, ...] = (
    "nvidia/nemotron-3-ultra-550b-a55b",
    "nvidia/nemotron-3-super-120b-a12b",
    "nvidia/nemotron-3.5-lightning-30b-a3b",
    "z-ai/glm-5.3",
)

# Families that may never be probed on EIH (closed, or not carried there).
BANNED_MODEL_PATTERNS: tuple[str, ...] = (
    r"^deepseek",
    r"^minimax",
    r"^openai/gpt",
    r"^anthropic/",
    r"^google/gemini",
    r"^x-ai/grok",
)

# ------------------------------------------------------------- outcomes ----
OUTCOME_OK = "ok"
OUTCOME_BUDGET = "budget_exhausted"
OUTCOME_404 = "404_unprovisioned"
OUTCOME_TIMEOUT = "timeout"
OUTCOME_TRANSPORT = "transport"
OUTCOME_SKIPPED_LOAD = "skipped_load"
OUTCOME_SKIPPED_WINDOW = "skipped_window"

RETRYABLE_STATUSES = frozenset({429, 503, 504, 529})
RETRY_DELAY_SECS = 2.0
FIRST_TOKEN_TIMEOUT_SECS = 60.0
TOTAL_TIMEOUT_SECS = 300.0

PUBLISH_WINDOW_START_MINUTE = 21 * 60 + 55  # 21:55 UTC
PUBLISH_WINDOW_END_MINUTE = 22 * 60 + 30  # 22:30 UTC

MEDIUM_HOUR_MODULUS = 6
MEDIUM_PROMPT_TOKENS = 8000
TINY_MAX_TOKENS = 512
MEDIUM_MAX_TOKENS = 1024

ERROR_EXCERPT_LIMIT = 300
MAX_FILE_BYTES = 5 * 1024 * 1024
RETENTION_DAYS = 30
FILE_MODE = 0o600
DIR_MODE = 0o700

# The row contract, in order.  Every row has exactly these fields.
ROW_FIELDS: tuple[str, ...] = (
    "measured_at",
    "host",
    "resolved_ip",
    "resolved_ip_source",
    "provider_id",
    "model_id",
    "prompt_class",
    "probe_tool",
    "attempt",
    "latency_ms",
    "first_token_ms",
    "headers_ms",
    "http_status",
    "outcome",
    "finish_reason",
    "tokens_in",
    "tokens_out",
    "tokens_reasoning",
    "host_load1",
    "error_excerpt",
    "tool_version",
)

# ---------------------------------------------------------------- prompts ---
TINY_PROMPT = "Compute 17 * 23 + 19. Reply with the final integer only, no explanation and no punctuation."
TINY_PROMPT_ANSWER = str(17 * 23 + 19)

# Deterministic seed corpus for the medium (once-every-6h) prompt.  It is
# expanded by repetition with a per-block marker so the payload reaches
# roughly the 8k-token class without any network access.
MEDIUM_SEED = (
    "def settle(batch, ledger):\n"
    "    total = 0\n"
    "    for row in batch:\n"
    "        if row.skipped:\n"
    "            continue\n"
    "        total += row.amount\n"
    "    ledger.record(total)\n"
    "    return total\n"
)
MEDIUM_TASK = (
    "Review the Python functions below. In at most five short bullet points, "
    "name concrete defects (bugs, unhandled cases, misleading names, missing "
    "validation) and say for each whether it is a correctness bug or a style "
    "issue. Be terse. Do not restate the code."
)


def is_banned_model(model_id: str) -> bool:
    """True when ``model_id`` is not permitted on EIH."""
    lowered = (model_id or "").strip().lower()
    return any(re.search(pat, lowered) for pat in BANNED_MODEL_PATTERNS)


def resolve_models(env: dict[str, str] | None = None) -> tuple[str, ...]:
    """The model list for this run; ``EIH_PROBE_MODELS`` overrides the default.

    Raises :class:`ConfigError` for a banned id, an empty list, or an id that
    is not ``vendor/name`` shaped.
    """
    source = dict(os.environ if env is None else env)
    raw = source.get("EIH_PROBE_MODELS", "").strip()
    if not raw:
        return DEFAULT_MODELS
    models = [part.strip() for part in raw.split(",") if part.strip()]
    if not models:
        raise ConfigError("EIH_PROBE_MODELS is set but lists no model ids")
    for model_id in models:
        if is_banned_model(model_id):
            raise ConfigError(
                f"EIH_PROBE_MODELS contains a model that may not be probed on "
                f"EIH: {model_id!r} (EIH serves Nemotron and other open-weight "
                f"models only; DeepSeek and the closed frontier families are "
                f"forbidden there)"
            )
    return tuple(models)


def build_medium_prompt(target_tokens: int = MEDIUM_PROMPT_TOKENS) -> str:
    """Deterministically expand :data:`MEDIUM_SEED` to ~``target_tokens`` tokens.

    Rough sizing at 4 characters per token; no randomness, so every medium run
    sends the identical prompt.
    """
    target_chars = max(1, target_tokens) * 4
    blocks: list[str] = []
    size = 0
    index = 0
    while size < target_chars:
        block = f"# block {index:04d}\n{MEDIUM_SEED}"
        blocks.append(block)
        size += len(block)
        index += 1
    return f"{MEDIUM_TASK}\n\n```python\n{''.join(blocks)}```\n"


def prompt_for(prompt_class: str) -> str:
    if prompt_class == "medium":
        return build_medium_prompt()
    return TINY_PROMPT


def prompt_class_for(now: datetime) -> str:
    """``medium`` only on runs whose UTC hour is divisible by six."""
    return "medium" if now.astimezone(UTC).hour % MEDIUM_HOUR_MODULUS == 0 else "tiny"


# ------------------------------------------------------------------ key ----
_KEY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"nvapi-[A-Za-z0-9_-]+"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/-]+=*"),
)
_KEY_FILE_ENV = "EIH_KEY_FILE"
_KEY_ENV = "EIH_API_KEY"
_KEY_FILE_NAME = "nvapi.key"
DEFAULT_KEY_FILE = "~/.zoder/secrets.env"
ZODER_CONFIG_FILES: tuple[str, ...] = ("~/.zoder/config.toml", "~/.zoder/config.fleet.toml")
_ZODER_KEY_BLOCK = re.compile(r"^\[\s*[^\]]*\.(?:eih_reviewer|eih_glm|nvidia-eih)\s*\]\s*$")


class ConfigError(RuntimeError):
    """A configuration problem: the only condition that exits non-zero."""


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def key_from_file(path: str | Path) -> str | None:
    """``EIH_API_KEY``/``NVIDIA_API_KEY`` (or a bare token) from a key file."""
    text = _read_text(Path(path).expanduser())
    if text is None:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, _, value = stripped.partition("=")
        if name.strip() in {_KEY_ENV, "NVIDIA_API_KEY"}:
            token = value.strip().strip("\"'")
            if token:
                return token
    return None


def key_from_zoder_config(path: str | Path) -> str | None:
    """The ``api_key`` of the EIH block in a zoder TOML config, if present."""
    text = _read_text(Path(path).expanduser())
    if text is None:
        return None
    in_block = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_block = bool(_ZODER_KEY_BLOCK.match(stripped))
            continue
        if in_block and stripped.startswith("api_key"):
            _, _, value = stripped.partition("=")
            token = value.strip().strip("\"'")
            if token and token.startswith("nvapi-"):
                return token
    return None


def load_api_key(env: dict[str, str] | None = None) -> str:
    """Locate the EIH key: env var, then key file, then the zoder config."""
    source = dict(os.environ if env is None else env)
    token = (source.get(_KEY_ENV) or "").strip()
    if token:
        return token
    candidates: list[Path] = []
    if source.get(_KEY_FILE_ENV):
        candidates.append(Path(source[_KEY_FILE_ENV]).expanduser())
    else:
        candidates.append(Path(DEFAULT_KEY_FILE).expanduser())
        candidates.append(Path(f"~/.zoder/{_KEY_FILE_NAME}").expanduser())
    for candidate in candidates:
        found = key_from_file(candidate)
        if found:
            return found
    for config in ZODER_CONFIG_FILES:
        found = key_from_zoder_config(config)
        if found:
            return found
    raise ConfigError(
        f"no EIH API key found: set {_KEY_ENV}, or point {_KEY_FILE_ENV} at a "
        f"key file, or configure an EIH provider in ~/.zoder/config.toml"
    )


def redact(text: str | None, secret: str | None = None) -> str | None:
    """Strip the key (explicit or pattern-matched) from free text."""
    if not text:
        return text
    cleaned = text
    if secret:
        cleaned = cleaned.replace(secret, "[REDACTED]")
    for pattern in _KEY_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)
    return cleaned


def error_excerpt(text: str | None, secret: str | None = None) -> str | None:
    """Redacted, whitespace-collapsed, truncated tail of an error body."""
    cleaned = redact(text, secret)
    if not cleaned:
        return None
    collapsed = " ".join(str(cleaned).split())
    if not collapsed:
        return None
    if len(collapsed) > ERROR_EXCERPT_LIMIT:
        collapsed = collapsed[: ERROR_EXCERPT_LIMIT - 1] + "…"
    return collapsed


# ------------------------------------------------------------- transport ---
@dataclass
class StreamResult:
    """What one attempt observed over the wire."""

    http_status: int | None = None
    headers_ms: float | None = None
    first_token_ms: float | None = None
    latency_ms: float | None = None
    body: str = ""
    content: str = ""
    finish_reason: str | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    tokens_reasoning: int | None = None
    resolved_ip: str | None = None
    resolved_ip_source: str | None = None
    failure: str | None = None  # "timeout" | "transport" | None
    failure_detail: str | None = None


class Transport(Protocol):
    def __call__(
        self,
        *,
        base_url: str,
        payload: dict[str, Any],
        api_key: str,
        first_token_timeout: float,
        total_timeout: float,
    ) -> StreamResult: ...


def _resolve(host: str, port: int) -> tuple[str | None, str | None]:
    """First ``getaddrinfo`` address, used only when the peer is unavailable."""
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError:
        return None, None
    for info in infos:
        address = info[4][0]
        if isinstance(address, str):
            return address, "getaddrinfo"
    return None, None


def _parse_stream(
    stream: Any,
    deadline: float,
    first_token_deadline: float,
    started: float,
    result: StreamResult,
) -> bool:
    """Consume SSE chunks; return True once the first content token arrives.

    Two independent budgets apply: a first-token deadline (60 s by default)
    that stops the wait for the first byte of answer, and a total deadline
    (300 s) that stops the whole response.  A timeout observed after content
    has streamed is still recorded -- the caller keeps whatever arrived and
    the ``timeout`` outcome wins, because the reply is incomplete.
    """
    saw_content = False
    while True:
        now = time.monotonic()
        if now > deadline:
            result.failure = "timeout"
            result.failure_detail = "total deadline exceeded"
            return saw_content
        if not saw_content and now > first_token_deadline:
            result.failure = "timeout"
            result.failure_detail = "first token deadline exceeded"
            return saw_content
        try:
            raw = stream.readline()
        except TimeoutError:
            result.failure = "timeout"
            result.failure_detail = "read timeout"
            return saw_content
        except (OSError, http.client.HTTPException) as exc:
            result.failure = "transport"
            result.failure_detail = f"{type(exc).__name__}: {exc}"
            return saw_content
        if not raw:
            return saw_content
        line = raw.decode("utf-8", "replace").strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if data == "[DONE]":
            return saw_content
        try:
            chunk = json.loads(data)
        except ValueError:
            continue
        if not isinstance(chunk, dict):
            continue
        usage = chunk.get("usage")
        if isinstance(usage, dict):
            result.tokens_in = usage.get("prompt_tokens", result.tokens_in)
            result.tokens_out = usage.get("completion_tokens", result.tokens_out)
            reasoning = usage.get("completion_tokens_details") or {}
            if isinstance(reasoning, dict) and reasoning.get("reasoning_tokens") is not None:
                result.tokens_reasoning = reasoning["reasoning_tokens"]
        for choice in chunk.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            if choice.get("finish_reason"):
                result.finish_reason = choice["finish_reason"]
            delta = choice.get("delta") or {}
            if not isinstance(delta, dict):
                continue
            piece = delta.get("content")
            if isinstance(piece, str) and piece:
                result.content += piece
                if not saw_content:
                    saw_content = True
                    result.first_token_ms = round((time.monotonic() - started) * 1000.0, 1)


# ``main``'s clock, overridable by a test: ``probe.probe_now = lambda: fixed``.
# ``None`` means the real wall clock. (A module attribute, like ``http_transport``,
# so the override is a plain assignment and needs no signature surgery.)
probe_now: Any = None  # override for main()'s clock: a zero-arg callable, or None


def http_transport(
    *,
    base_url: str,
    payload: dict[str, Any],
    api_key: str,
    first_token_timeout: float,
    total_timeout: float,
) -> StreamResult:
    """One streaming POST over ``http.client`` -- the only real network path."""
    result = StreamResult()
    parsed = urllib.parse.urlsplit(base_url)
    host = parsed.hostname or ""
    if parsed.scheme not in {"https", "http"} or not host:
        result.failure = "transport"
        result.failure_detail = f"unsupported base url scheme/host for {base_url!r}"
        return result
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    fallback_ip, fallback_source = _resolve(host, port)
    result.resolved_ip = fallback_ip
    result.resolved_ip_source = fallback_source

    path = parsed.path.rstrip("/") + EIH_CHAT_PATH
    if parsed.query:
        path = f"{path}?{parsed.query}"
    started = time.monotonic()
    deadline = started + total_timeout
    first_token_deadline = min(started + first_token_timeout, deadline)
    conn: http.client.HTTPConnection | None = None
    try:
        if parsed.scheme == "https":
            conn = http.client.HTTPSConnection(
                host, port, timeout=first_token_timeout, context=ssl.create_default_context()
            )
        else:
            conn = http.client.HTTPConnection(host, port, timeout=first_token_timeout)
        body = json.dumps(payload)
        conn.request(
            "POST",
            path,
            body=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "text/event-stream",
                "Content-Type": "application/json",
            },
        )
        # The peer socket tells us the address we actually connected to, which
        # can differ from getaddrinfo's first answer behind an anycast VIP.
        try:
            peer = conn.sock.getpeername() if conn.sock is not None else None
        except (OSError, AttributeError):
            peer = None
        if peer and peer[0]:
            result.resolved_ip = str(peer[0])
            result.resolved_ip_source = "socket_peer"
        response = conn.getresponse()
        result.headers_ms = round((time.monotonic() - started) * 1000.0, 1)
        result.http_status = response.status
        if response.status >= 300:
            # Error bodies are JSON/text, not SSE: keep them whole so a
            # "Function not found" 404 can be told apart from a bad path.
            result.body = response.read().decode("utf-8", "replace")
        else:
            _parse_stream(response, deadline, first_token_deadline, started, result)
    except TimeoutError as exc:
        result.failure = "timeout"
        result.failure_detail = f"{type(exc).__name__}: {exc}"
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        result.failure = "transport"
        result.failure_detail = f"{type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            with contextlib.suppress(OSError):  # close is best effort
                conn.close()
        result.latency_ms = round((time.monotonic() - started) * 1000.0, 1)
    return result


# ----------------------------------------------------------- classification -
def classify(result: StreamResult) -> str:
    """Map one attempt to the outcome vocabulary."""
    status = result.http_status
    body = result.body or ""
    if result.failure == "timeout":
        return OUTCOME_TIMEOUT
    if result.failure == "transport" and status is None:
        return OUTCOME_TRANSPORT
    if status == 404 and "function not found" in body.lower():
        return OUTCOME_404
    if status in {429, 503, 504, 529}:
        return str(status)
    if status is None:
        return OUTCOME_TRANSPORT
    if 200 <= status < 300:
        if not result.content.strip() and result.finish_reason == "length":
            return OUTCOME_BUDGET
        return OUTCOME_OK
    return f"http_{status}"


# Outcomes worth exactly one retry: the transient gateway/burst classes and a
# client-side timeout or transport break.  4xx (including 404_unprovisioned),
# budget exhaustion and the skip outcomes are never retried.
RETRYABLE_OUTCOMES: frozenset[str] = frozenset(
    {str(code) for code in RETRYABLE_STATUSES} | {OUTCOME_TIMEOUT, OUTCOME_TRANSPORT}
)


def should_retry(outcome: str) -> bool:
    """One retry only, and only for the transient classes (not for 4xx/skip)."""
    return outcome in RETRYABLE_OUTCOMES


# ------------------------------------------------------------- row making --
def _iso(now: datetime) -> str:
    return now.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_row(
    *,
    now: datetime,
    model_id: str,
    prompt_class: str,
    outcome: str,
    host: str,
    host_load1: float | None,
    attempt: int = 1,
    metric: StreamResult | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """Build one JSONL row: exactly :data:`ROW_FIELDS`, missing numbers null."""
    observed = metric or StreamResult()
    excerpt = error_excerpt(observed.failure_detail, secret)
    if excerpt is None and observed.http_status and observed.http_status >= 400:
        excerpt = error_excerpt(observed.body, secret)
    row: dict[str, Any] = {
        "measured_at": _iso(now),
        "host": host,
        "resolved_ip": observed.resolved_ip,
        "resolved_ip_source": observed.resolved_ip_source,
        "provider_id": PROVIDER_ID,
        "model_id": model_id,
        "prompt_class": prompt_class,
        "probe_tool": PROBE_TOOL,
        "attempt": attempt,
        "latency_ms": observed.latency_ms,
        "first_token_ms": observed.first_token_ms,
        "headers_ms": observed.headers_ms,
        "http_status": observed.http_status,
        "outcome": outcome,
        "finish_reason": observed.finish_reason,
        "tokens_in": observed.tokens_in,
        "tokens_out": observed.tokens_out,
        "tokens_reasoning": observed.tokens_reasoning,
        "host_load1": host_load1,
        "error_excerpt": excerpt,
        "tool_version": TOOL_VERSION,
    }
    # Belt and braces: the key can never appear in an emitted row.
    if secret:
        for key, value in row.items():
            if isinstance(value, str) and secret in value:
                row[key] = redact(value, secret)
    return {name: row[name] for name in ROW_FIELDS}


def in_publish_window(now: datetime) -> bool:
    """True between 21:55 and 22:30 UTC inclusive of the start, exclusive of end."""
    utc = now.astimezone(UTC)
    minute_of_day = utc.hour * 60 + utc.minute
    return PUBLISH_WINDOW_START_MINUTE <= minute_of_day < PUBLISH_WINDOW_END_MINUTE


def load_gate(host_load1: float | None, cpu_count: int | None) -> bool:
    """True when the host is too busy to measure anything meaningful."""
    if host_load1 is None or not cpu_count:
        return False
    return host_load1 > cpu_count


# ----------------------------------------------------------------- output --
class RowWriter:
    """Append-only daily JSONL writer: 0600 files, 0700 dir, 30-day retention."""

    def __init__(
        self,
        directory: str | Path,
        *,
        max_bytes: int = MAX_FILE_BYTES,
        retention_days: int = RETENTION_DAYS,
        log: Any = None,
    ) -> None:
        self.directory = Path(directory).expanduser()
        self.max_bytes = max_bytes
        self.retention_days = retention_days
        self._log = log or (lambda _message: None)
        self._capped: set[str] = set()

    def _ensure_dir(self) -> None:
        self.directory.mkdir(mode=DIR_MODE, parents=True, exist_ok=True)
        with contextlib.suppress(OSError):  # pragma: no cover - odd filesystems
            self.directory.chmod(DIR_MODE)

    def path_for(self, now: datetime) -> Path:
        stamp = now.astimezone(UTC).strftime("%Y-%m-%d")
        return self.directory / f"eih-probe-{stamp}.jsonl"

    def prune(self, now: datetime | None = None) -> list[Path]:
        """Delete ``eih-probe-*.jsonl`` older than the retention window."""
        if not self.directory.is_dir():
            return []
        cutoff = (now or datetime.now(UTC)).astimezone(UTC).date() - timedelta(days=self.retention_days)
        removed: list[Path] = []
        for candidate in sorted(self.directory.glob("eih-probe-*.jsonl")):
            match = re.fullmatch(r"eih-probe-(\d{4}-\d{2}-\d{2})\.jsonl", candidate.name)
            if not match:
                continue
            try:
                day = datetime.strptime(match.group(1), "%Y-%m-%d").date()
            except ValueError:
                continue
            if day < cutoff:
                try:
                    candidate.unlink()
                    removed.append(candidate)
                except OSError:  # pragma: no cover - permissions
                    continue
        return removed

    def write(self, row: dict[str, Any], now: datetime) -> bool:
        """Append one row.  Returns False when the file is at its size cap."""
        self._ensure_dir()
        target = self.path_for(now)
        line = json.dumps(row, ensure_ascii=False) + "\n"
        encoded = len(line.encode("utf-8"))
        size = target.stat().st_size if target.exists() else 0
        if size + encoded > self.max_bytes:
            if target.name not in self._capped:
                self._capped.add(target.name)
                self._log(f"eih_probe: {target.name} reached the {self.max_bytes}-byte cap; not writing further rows")
            return False
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_APPEND, FILE_MODE)
        try:
            os.fchmod(descriptor, FILE_MODE)
            os.write(descriptor, line.encode("utf-8"))
        finally:
            os.close(descriptor)
        return True


# -------------------------------------------------------------- the probe --
@dataclass
class ProbeRun:
    """Outcome of one scheduled run: the rows written, in order."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    skipped: str | None = None


def _attempt(
    transport: Transport,
    *,
    base_url: str,
    payload: dict[str, Any],
    api_key: str,
    first_token_timeout: float,
    total_timeout: float,
) -> StreamResult:
    return transport(
        base_url=base_url,
        payload=payload,
        api_key=api_key,
        first_token_timeout=first_token_timeout,
        total_timeout=total_timeout,
    )


def probe_model(
    model_id: str,
    *,
    prompt_class: str,
    now: datetime,
    host: str,
    host_load1: float | None,
    transport: Transport,
    api_key: str,
    base_url: str = EIH_BASE_URL,
    first_token_timeout: float = FIRST_TOKEN_TIMEOUT_SECS,
    total_timeout: float = TOTAL_TIMEOUT_SECS,
    sleep: Any = time.sleep,
) -> list[dict[str, Any]]:
    """Probe one model: at most two sequential requests, one row each."""
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt_for(prompt_class)}],
        "max_tokens": MEDIUM_MAX_TOKENS if prompt_class == "medium" else TINY_MAX_TOKENS,
        "stream": True,
    }
    rows: list[dict[str, Any]] = []
    attempt = 1
    while True:
        result = _attempt(
            transport,
            base_url=base_url,
            payload=payload,
            api_key=api_key,
            first_token_timeout=first_token_timeout,
            total_timeout=total_timeout,
        )
        outcome = classify(result)
        rows.append(
            make_row(
                now=now,
                model_id=model_id,
                prompt_class=prompt_class,
                outcome=outcome,
                host=host,
                host_load1=host_load1,
                attempt=attempt,
                metric=result,
                secret=api_key,
            )
        )
        if attempt >= 2 or not should_retry(outcome):
            return rows
        sleep(RETRY_DELAY_SECS)
        attempt = 2


def run_probe(
    *,
    now: datetime | None = None,
    transport: Transport | None = None,
    api_key: str | None = None,
    models: tuple[str, ...] | None = None,
    base_url: str = EIH_BASE_URL,
    host: str | None = None,
    host_load1: float | None = None,
    cpu_count: int | None = None,
    sleep: Any = time.sleep,
    first_token_timeout: float = FIRST_TOKEN_TIMEOUT_SECS,
    total_timeout: float = TOTAL_TIMEOUT_SECS,
) -> ProbeRun:
    """One scheduled run.  Skip gates are decided once, before any request."""
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    resolved_models = models if models is not None else DEFAULT_MODELS
    prompt_class = prompt_class_for(moment)
    machine = host or socket.gethostname()

    if host_load1 is None:
        try:
            host_load1 = float(os.getloadavg()[0])
        except (OSError, AttributeError):  # pragma: no cover - non-POSIX
            host_load1 = None
    if cpu_count is None:
        cpu_count = os.cpu_count()

    skip_outcome: str | None = None
    if in_publish_window(moment):
        skip_outcome = OUTCOME_SKIPPED_WINDOW
    elif load_gate(host_load1, cpu_count):
        skip_outcome = OUTCOME_SKIPPED_LOAD

    if skip_outcome is not None:
        rows = [
            make_row(
                now=moment,
                model_id=model_id,
                prompt_class=prompt_class,
                outcome=skip_outcome,
                host=machine,
                host_load1=host_load1,
            )
            for model_id in resolved_models
        ]
        return ProbeRun(rows=rows, skipped=skip_outcome)

    active = transport or http_transport
    token = api_key if api_key is not None else load_api_key()
    rows = []
    for model_id in resolved_models:
        rows.extend(
            probe_model(
                model_id,
                prompt_class=prompt_class,
                now=moment,
                host=machine,
                host_load1=host_load1,
                transport=active,
                api_key=token,
                base_url=base_url,
                first_token_timeout=first_token_timeout,
                total_timeout=total_timeout,
                sleep=sleep,
            )
        )
    return ProbeRun(rows=rows)


def output_dir(env: dict[str, str] | None = None) -> Path:
    source = dict(os.environ if env is None else env)
    configured = source.get("EIH_PROBE_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path("~/Projects/eih-probe/").expanduser()


def log_line(message: str, secret: str | None = None) -> None:
    """Redacted single-line log to stderr (never contains the key)."""
    print(redact(message, secret), file=sys.stderr, flush=True)


def main(argv: list[str] | None = None, *, env: dict[str, str] | None = None) -> int:
    """Run one probe cycle.  Exit 0 for any probe result; 1/2 for config errors."""
    args = list(sys.argv[1:] if argv is None else argv)
    if "--help" in args or "-h" in args:
        print(
            "usage: python3 -m hive.tools.eih_probe [--dry-run]\n\n"
            "Env: EIH_API_KEY, EIH_KEY_FILE, EIH_PROBE_MODELS, EIH_PROBE_DIR"
        )
        return 0
    dry_run = "--dry-run" in args
    source = dict(os.environ if env is None else env)

    try:
        models = resolve_models(source)
    except ConfigError as exc:
        log_line(f"eih_probe: configuration error: {exc}")
        return 2

    moment = datetime.now(UTC)
    prompt_class = prompt_class_for(moment)
    if dry_run:
        # Self-check only: report what a run would do, make no request, write
        # no row (a dry run is not a measurement, so it must not enter the data).
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "measured_at": _iso(moment),
                    "prompt_class": prompt_class,
                    "skipped_window": in_publish_window(moment),
                    "models": list(models),
                    "output_dir": str(output_dir(source)),
                    "tool_version": TOOL_VERSION,
                },
                ensure_ascii=False,
            )
        )
        return 0

    try:
        api_key = load_api_key(source)
    except ConfigError as exc:
        log_line(f"eih_probe: configuration error: {exc}")
        return 2

    # The transport is read through the module attribute (probe.http_transport),
    # so a test that swaps it in is honoured without any new plumbing.
    # The clock is likewise overridable (probe.probe_now) so a test can pin the
    # moment -- and thereby name the daily output file the run writes -- instead
    # of racing the publish window or the UTC date at run time.
    if probe_now is not None:
        moment = probe_now()

    directory = output_dir(source)
    writer = RowWriter(directory, log=log_line)
    try:
        for stale in writer.prune(moment):
            log_line(f"eih_probe: pruned {stale.name} (older than {RETENTION_DAYS} days)")
    except OSError as exc:  # pragma: no cover - unwritable dir
        log_line(f"eih_probe: prune failed: {type(exc).__name__}")

    # The transport is read through the module attribute (probe.http_transport),
    # so a test that swaps it in is honoured here without any new plumbing.
    run = run_probe(now=moment, models=models, api_key=api_key)
    written = 0
    for row in run.rows:
        if writer.write(row, moment):
            written += 1
    outcomes = sorted({str(row["outcome"]) for row in run.rows})
    log_line(
        f"eih_probe: run={_iso(moment)} class={prompt_class} models={len(models)} "
        f"rows={written} outcomes={','.join(outcomes) or 'none'}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
