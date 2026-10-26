#!/usr/bin/env python3
"""DeepSeek month-to-date spend alert (alert only — never blocks calls).

The fleet's DeepSeek key is prepaid and metered. The operator ceiling is $100
per calendar month and the policy is: ALERT at 70% ($70) and again at 90% ($90)
of month-to-date spend, each at most once per calendar month, and NEVER refuse
a DeepSeek call because of spend.

The only record of real money is the DeepSeek account balance endpoint
(``GET https://api.deepseek.com/user/balance``, ``Authorization: Bearer <key>``,
whose JSON carries ``balance_infos[]`` rows of ``currency``, ``total_balance``,
``granted_balance`` and ``topped_up_balance``). Every run appends that reading
to a snapshot file; the spend figure is derived from the snapshots:

    month-to-date spend
        = start_total  (balance at the UTC calendar-month start: the baseline
                        for the month, else the first snapshot inside the month)
        + topped-up amounts added since that start
        - current total balance

A top-up is an INCREASE in ``topped_up_balance`` between two consecutive
snapshots of the same currency. Granted credits are never counted as an offset
(they are free money, not spend), so burning granted balance reads as spend —
which is exactly what the operator wants to see.

The tool is stdlib only and needs no imports from the hive package. It is run
hourly by ``deploy/systemd/hive-deepseek-spend-alert.{service,timer}``.

Secrets: the API key is read from ``DEEPSEEK_API_KEY`` at run time only. It is
never written to the snapshot, the state file or any log line, and never passed
on a command line.

Usage::

    python3 hive/tools/deepseek_spend_alert.py [--dry-run] [--no-bus]

Exit codes: 0 = ran (alert may or may not have fired), 1 = balance fetch
failed (nothing appended, nothing sent), 2 = configuration error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request
from collections.abc import Callable

# ── endpoints / identity ────────────────────────────────────────────────────
BALANCE_URL = "https://api.deepseek.com/user/balance"
DEFAULT_BUS_URL = "http://192.0.2.67:5005"
FETCH_TIMEOUT_SECS = 20.0
BUS_TIMEOUT_SECS = 10.0

#: Hive bus topic the alert is posted to. The bus has no topic registry: a
#: topic is just the free-text ``topic`` column on a message row, so this
#: follows the fleet's dotted event convention (``job.``, ``agent.``, ...).
ALERT_TOPIC = "fleet.alert"

#: Agent URN the tool registers with the bus. The bus builds URNs as
#: ``urn:agent:<kind>:<host>:<session_uuid>`` (hive/bus/deps.make_urn) from
#: ``kind``/``host``/a fresh session id — so re-registering each run is the
#: normal shape and the tool asks the bus for its URN instead of inventing one.
#: KIND CHOICE: ``human`` (runtime also ``human``) — it is the closest existing
#: kind. The tool is a read-only reporter acting for the operator, not a coding
#: worker (``claude``/``codex``/``opencode``/``zeroclaw`` kinds would show up in
#: the worker pool), not a fleet host daemon (``system``) and not a triage
#: authority (``doctor``); ``human`` is exactly the "operator-facing, no job
#: execution" identity the dashboard's own registration uses.
AGENT_KIND = "human"
AGENT_RUNTIME = "human"

# ── policy defaults (overridable from the environment) ──────────────────────
DEFAULT_CEILING_USD = 100.0
DEFAULT_THRESHOLDS = (70.0, 90.0)
#: October 2026 is the fleet's first metered month, so its "start of month"
#: balance IS the first recorded snapshot: 2026-10-25T23:30:00Z,
#: total $252.11 = topped up $240.71 + granted $11.39.
DEFAULT_BASELINE_TS = "2026-10-25T23:30:00Z"
DEFAULT_BASELINE_TOTAL = 252.11
DEFAULT_BASELINE_TOPPED_UP = 240.71
DEFAULT_BASELINE_MONTH = "2026-10"


class ConfigError(RuntimeError):
    """Bad or missing configuration — nothing is fetched or sent."""


class FetchError(RuntimeError):
    """The balance endpoint could not be read — no snapshot, no alert."""


class BusError(RuntimeError):
    """The hive bus could not be reached — the threshold stays unsent."""


# ── config ──────────────────────────────────────────────────────────────────
class Config:
    """Run configuration, resolved from the process environment."""

    def __init__(self, env: dict[str, str] | None = None):
        env = os.environ if env is None else env
        self.env = env
        self.api_key = (env.get("DEEPSEEK_API_KEY") or "").strip()
        self.currency = (env.get("DEEPSEEK_CURRENCY") or "USD").strip().upper()
        self.snapshot_path = pathlib.Path(
            env.get("DEEPSEEK_SNAPSHOT_PATH") or pathlib.Path.home() / ".hive" / "deepseek-balance.jsonl"
        ).expanduser()
        self.bus_url = (env.get("HIVE_BUS_URL") or DEFAULT_BUS_URL).rstrip("/")
        self.bus_token = (env.get("HIVE_SHARED_SECRET") or "").strip() or None
        self.ceiling = _env_float(env, "DEEPSEEK_MONTHLY_CEILING_USD", DEFAULT_CEILING_USD, positive=True)
        self.thresholds = _env_thresholds(env)
        # Baseline (only consulted for the month it names — see month_start_total).
        self.baseline_ts = (_env_str(env, "DEEPSEEK_BASELINE_TS") or DEFAULT_BASELINE_TS).strip()
        self.baseline_month = (_env_str(env, "DEEPSEEK_BASELINE_MONTH") or DEFAULT_BASELINE_MONTH).strip()
        self.baseline_total = _env_float(env, "DEEPSEEK_BASELINE_TOTAL", DEFAULT_BASELINE_TOTAL)
        self.baseline_topped_up = _env_float(env, "DEEPSEEK_BASELINE_TOPPED_UP", DEFAULT_BASELINE_TOPPED_UP)
        self.host = (_env_str(env, "HIVE_HOST_NAME") or _short_hostname()).strip()
        self.install_state_path = (_env_str(env, "DEEPSEEK_ALERT_INSTALL_PATH") or "").strip() or None

    @property
    def state_path(self) -> pathlib.Path:
        """Threshold bookkeeping lives next to the snapshots (snapshot dir)."""
        return self.snapshot_path.parent / "deepseek-spend-alert-state.json"

    def baseline_ts_utc(self) -> dt.datetime:
        return parse_ts(self.baseline_ts)


def _short_hostname() -> str:
    return (os.uname().nodename or "unknown").split(".")[0].lower() or "unknown"


def _env_str(env, name: str) -> str | None:
    value = env.get(name)
    return None if value is None or not str(value).strip() else str(value)


def _env_float(env: dict[str, str], name: str, default: float, *, positive: bool = False) -> float:
    raw = _env_str(env, name)
    if raw is None:
        return float(default)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if positive and value <= 0:
        raise ConfigError(f"{name} must be > 0, got {raw!r}")
    return value


def _env_thresholds(env: dict[str, str]) -> tuple[float, ...]:
    raw = _env_str(env, "DEEPSEEK_ALERT_THRESHOLDS")
    if raw is None:
        return DEFAULT_THRESHOLDS
    values: list[float] = []
    for part in raw.replace(",", " ").split():
        try:
            values.append(float(part))
        except ValueError as exc:
            raise ConfigError(f"DEEPSEEK_ALERT_THRESHOLDS must be numbers, got {part!r}") from exc
    if not values or any(v <= 0 for v in values):
        raise ConfigError(f"DEEPSEEK_ALERT_THRESHOLDS must be positive, got {raw!r}")
    return tuple(sorted(set(values)))


# ── time helpers ────────────────────────────────────────────────────────────
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def parse_ts(ts: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp; naive values are UTC."""
    text = str(ts).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def fmt_ts(ts: dt.datetime) -> str:
    return ts.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_key(ts: dt.datetime) -> str:
    return ts.astimezone(dt.UTC).strftime("%Y-%m")


def month_start(ts: dt.datetime) -> dt.datetime:
    ts = ts.astimezone(dt.UTC)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


# ── snapshot store ──────────────────────────────────────────────────────────
SNAPSHOT_FIELDS = ("ts_utc", "total", "granted", "topped_up", "currency")


def read_snapshots(path: pathlib.Path, currency: str) -> list[dict]:
    """Valid snapshots for ``currency`` in file order; malformed lines are skipped."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    out: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except (ValueError, TypeError):
            continue
        if not isinstance(row, dict) or any(f not in row for f in SNAPSHOT_FIELDS):
            continue
        if str(row.get("currency") or "").upper() != currency:
            continue
        try:
            row = {
                "ts_utc": fmt_ts(parse_ts(row["ts_utc"])),
                "ts": parse_ts(row["ts_utc"]),
                "total": float(row["total"]),
                "granted": float(row["granted"]),
                "topped_up": float(row["topped_up"]),
                "currency": currency,
            }
        except (TypeError, ValueError):
            continue
        out.append(row)
    return out


def append_snapshot(path: pathlib.Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({f: row[f] for f in SNAPSHOT_FIELDS}, sort_keys=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


# ── month-to-date spend ─────────────────────────────────────────────────────
def _round_cents(value: float) -> float:
    return round(float(value) + 0.0, 2)


def topups_between(snapshots: list[dict], start: dt.datetime, end: dt.datetime, start_topped_up: float) -> float:
    """TOPPED-UP money added over (start, end].

    A decrease is never a negative top-up: only strictly positive steps count.
    Consecutive snapshots are the basis (the month-start figure anchors the
    first step), so a refill after a draw-down counts as new money — the
    draw-down itself was spend, which the totals already price.

        added = sum(max(0, topped_up[i] - topped_up[i-1]))
    """
    previous = float(start_topped_up)
    added = 0.0
    for snap in sorted(snapshots, key=lambda s: s["ts"]):
        if not (start < snap["ts"] <= end):
            continue
        current = float(snap["topped_up"])
        delta = current - previous
        if delta > 0:
            added += delta
        previous = current
    return _round_cents(added)


def month_start_total(snapshots: list[dict], start: dt.datetime, now: dt.datetime, config: Config) -> tuple[float, str]:
    """(balance at the UTC month start, where that figure came from).

    Priority: the configured baseline when it names THIS month, else the first
    snapshot inside the month, else the newest snapshot before it (last month's
    closing balance, which is this month's start).
    """
    if config.baseline_month == month_key(start) and config.baseline_ts:
        baseline_ts = config.baseline_ts_utc()
        if month_start(baseline_ts) == start:
            return _round_cents(config.baseline_total), "baseline"
    inside = [s for s in snapshots if s["ts"] >= start and s["ts"] <= now]
    if inside:
        first = min(inside, key=lambda s: s["ts"])
        return _round_cents(first["total"]), f"first snapshot {first['ts_utc']}"
    before = [s for s in snapshots if s["ts"] < start]
    if before:
        last = max(before, key=lambda s: s["ts"])
        return _round_cents(last["total"]), f"last snapshot before month start {last['ts_utc']}"
    raise ConfigError(
        "no balance figure for the start of "
        f"{month_key(start)}: no baseline configured for it and the snapshot "
        f"file has no reading at or before that month"
    )


def month_start_topped_up(
    snapshots: list[dict], start: dt.datetime, now: dt.datetime, config: Config, total: float
) -> float:
    """Topped-up figure that pairs with the month-start total."""
    if config.baseline_month == month_key(start) and config.baseline_ts:
        baseline_ts = config.baseline_ts_utc()
        if month_start(baseline_ts) == start:
            return _round_cents(config.baseline_topped_up)
    inside = [s for s in snapshots if s["ts"] >= start and s["ts"] <= now]
    if inside:
        return _round_cents(min(inside, key=lambda s: s["ts"])["topped_up"])
    before = [s for s in snapshots if s["ts"] < start]
    if before:
        return _round_cents(max(before, key=lambda s: s["ts"])["topped_up"])
    # Only reachable with a baseline total but no baseline topped-up: treat the
    # whole starting balance as already-paid money (grants still never offset).
    return _round_cents(total)


def compute_spend(snapshots: list[dict], now: dt.datetime, config: Config) -> dict:
    start = month_start(now)
    start_total, source = month_start_total(snapshots, start, now, config)
    start_topped_up = month_start_topped_up(snapshots, start, now, config, start_total)
    topups = topups_between(snapshots, start, now, start_topped_up)
    current = [s for s in snapshots if s["ts"] <= now]
    if not current:
        raise ConfigError("no snapshot available to price current spend against")
    latest = max(current, key=lambda s: s["ts"])
    spend = _round_cents(start_total + topups - latest["total"])
    if spend < 0:
        spend = 0.0
    return {
        "month": month_key(start),
        "month_start": start,
        "start_total": start_total,
        "start_source": source,
        "start_topped_up": start_topped_up,
        "topups": topups,
        "current_total": _round_cents(latest["total"]),
        "current_granted": _round_cents(latest["granted"]),
        "current_topped_up": _round_cents(latest["topped_up"]),
        "snapshot_ts": latest.get("ts_utc") or fmt_ts(latest["ts"]),
        "spend": spend,
        "pct": 100.0 * spend / config.ceiling,
    }


# ── message + state ─────────────────────────────────────────────────────────
def format_alert_message(spend: dict, ceiling: float) -> str:
    """The operator-approved wording (cents for money, whole percent)."""
    pct = int(round(spend["pct"]))
    return (
        f"DeepSeek spend ${spend['spend']:.2f} this month ({pct}% of ${ceiling:.0f} ceiling); "
        f"balance ${spend['current_total']:.2f}; top-ups since month start ${spend['topups']:.2f}"
    )


def load_state(path: pathlib.Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_state(path: pathlib.Path, month: str, sent: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"month": month, "sent": sorted({round(float(v), 2) for v in sent})}
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def thresholds_due(spend_pct: float, thresholds: tuple[float, ...], state: dict, month: str) -> list[float]:
    """Thresholds reached this month that have not been sent yet, lowest first."""
    sent = state.get("sent") if state.get("month") == month else []
    sent_set = {round(float(v), 2) for v in (sent or []) if isinstance(v, (int, float))}
    return [t for t in thresholds if spend_pct >= t and round(float(t), 2) not in sent_set]


# ── HTTP ────────────────────────────────────────────────────────────────────
def _http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict | None = None,
    timeout: float = FETCH_TIMEOUT_SECS,
) -> dict:
    data = None
    hdrs = {"Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)  # noqa: S310 (fixed endpoints)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (fixed endpoints)
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200]
        except Exception:  # noqa: BLE001 — best-effort error text
            detail = ""
        raise FetchError(f"{method} {_redact_url(url)} -> HTTP {exc.code}: {detail}") from None
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise FetchError(f"{method} {_redact_url(url)} failed: {exc}") from None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FetchError(f"{method} {_redact_url(url)} returned non-JSON: {exc}") from None
    if not isinstance(parsed, dict):
        raise FetchError(f"{method} {_redact_url(url)} returned {type(parsed).__name__}, expected object")
    return parsed


def _redact_url(url: str) -> str:
    base, _, query = url.partition("?")
    return base if not query else f"{base}?<redacted>"


def fetch_balance(api_key: str, currency: str, now: dt.datetime | None = None) -> dict:
    """One snapshot row from /user/balance (raises FetchError)."""
    if not api_key:
        raise ConfigError("DEEPSEEK_API_KEY is not set; it is read from the environment at run time")
    payload = _http_json(BALANCE_URL, headers={"Authorization": f"Bearer {api_key}"})
    infos = payload.get("balance_infos")
    if not isinstance(infos, list) or not infos:
        raise FetchError("/user/balance response carries no balance_infos[]")
    row = None
    for info in infos:
        if isinstance(info, dict) and str(info.get("currency") or "").upper() == currency:
            row = info
            break
    if row is None:
        # A single-currency account is the common case: accept the only row.
        candidates = [i for i in infos if isinstance(i, dict)]
        if len(candidates) == 1:
            row = candidates[0]
        else:
            raise FetchError(
                f"/user/balance has no {currency} row (currencies: {[i.get('currency') for i in candidates]})"
            )
    try:
        total = float(row["total_balance"])
        granted = float(row.get("granted_balance") or 0.0)
        topped_up = float(row.get("topped_up_balance") or 0.0)
    except (KeyError, TypeError, ValueError) as exc:
        raise FetchError(f"/user/balance row is missing a numeric balance field: {exc}") from None
    ts = (now or utcnow()).astimezone(dt.UTC).replace(microsecond=0)
    return {
        "ts_utc": fmt_ts(ts),
        "ts": ts,
        "total": _round_cents(total),
        "granted": _round_cents(granted),
        "topped_up": _round_cents(topped_up),
        "currency": currency,
    }


# ── hive bus ────────────────────────────────────────────────────────────────
def register_agent(config: Config, *, transport: Callable[..., dict] | None = None) -> str:
    """Register this run's agent with the bus and return its URN.

    ``POST /v1/agents/register`` mints ``urn:agent:<kind>:<host>:<session>``
    itself, so the tool uses the URN the bus returns. ``kind``/``runtime`` must
    agree (RUNTIME_KIND_MAP) — this tool registers as ``human``/``human``.
    """
    send = transport or _http_json
    body = {
        "runtime": AGENT_RUNTIME,
        "kind": AGENT_KIND,
        "host": config.host,
        "model": "unknown",
        "provider": "deepseek",
        "autonomy_level": "autonomous",
        "auth_method": "api",
        "pid": os.getpid(),
        "capabilities": ["spend-alert", "read-only"],
        "version": "deepseek-spend-alert-1",
        # Stable per-tool identity: a re-run replaces last hour's row instead of
        # minting a new URN every hour (hive/bus/routers/agents.py dedup).
        "instance": "deepseek-spend-alert",
        "metadata": {"tool": "hive/tools/deepseek_spend_alert.py"},
        "subscription_pools": [],
    }
    try:
        resp = send(
            f"{config.bus_url}/v1/agents/register",
            method="POST",
            headers=_actor_headers(None, config),
            body=body,
            timeout=BUS_TIMEOUT_SECS,
        )
    except FetchError as exc:
        raise BusError(f"agent registration failed: {exc}") from None
    urn = str((resp or {}).get("urn") or "").strip()
    if not urn.startswith("urn:agent:"):
        raise BusError(f"registration returned no usable urn: {resp!r}"[:300])
    return urn


def heartbeat(config: Config, urn: str, *, transport: Callable[..., dict] | None = None) -> None:
    """One heartbeat so the bus sees the agent ACTIVE before the message post."""
    send = transport or _http_json
    try:
        send(
            f"{config.bus_url}/v1/agents/heartbeat",
            method="POST",
            headers=_actor_headers(urn, config),
            body={"urn": urn, "status": "online"},
            timeout=BUS_TIMEOUT_SECS,
        )
    except FetchError as exc:
        raise BusError(f"heartbeat failed: {exc}") from None


def post_alert(config: Config, urn: str, text: str, *, transport: Callable[..., dict] | None = None) -> dict:
    """Post the alert to the fleet.alert topic.

    ``POST /v1/messages`` requires the sender to be a REGISTERED, ACTIVE agent
    (``require_registered_agent(active_only=True)``) and takes ``from_urn`` from
    the body — which is why the URN above must be registration + heartbeat
    first. The bus applies ``X-Hive-Actor`` to its admin mutating routes; it is
    sent here too so the message carries the same actor identity as the
    operator-facing admin calls.
    """
    send = transport or _http_json
    body = {
        "from_urn": urn,
        "topic": ALERT_TOPIC,
        "payload": {"text": text, "kind": "deepseek-spend", "severity": "warning"},
    }
    try:
        return send(
            f"{config.bus_url}/v1/messages",
            method="POST",
            headers=_actor_headers(urn, config),
            body=body,
            timeout=BUS_TIMEOUT_SECS,
        )
    except FetchError as exc:
        raise BusError(f"posting to topic {ALERT_TOPIC} failed: {exc}") from None


def _actor_headers(urn: str | None, config: Config) -> dict[str, str]:
    headers = {"X-Hive-Actor": urn} if urn else {}
    if config.bus_token:
        headers["X-Hive-Token"] = config.bus_token
    return headers


# ── run ─────────────────────────────────────────────────────────────────────
def run(
    config: Config,
    *,
    now: dt.datetime | None = None,
    fetcher: Callable[..., dict] | None = None,
    transport: Callable[..., dict] | None = None,
    no_bus: bool = False,
    log: Callable[[str], None] = lambda m: print(m, flush=True),
) -> dict:
    """One full pass. Returns a report dict; raises ConfigError on bad setup.

    ``fetcher(api_key, currency, now)`` returns one snapshot row;
    ``transport(url, *, method, headers, body, timeout)`` talks to the bus.
    With ``no_bus`` the bus is never contacted and no threshold is marked.
    """
    now = (now or utcnow()).astimezone(dt.UTC).replace(microsecond=0)
    fetch = fetcher or fetch_balance
    # DEEPSEEK_API_KEY presence is the only secret check; the value is handed
    # straight to fetch and never logged or persisted.
    if not config.api_key:
        raise ConfigError("DEEPSEEK_API_KEY is not set; it is read from the environment at run time")

    snapshot = fetch(config.api_key, config.currency, now)
    # 1. Persist first: the reading is the only record of real money, so a bus
    #    failure later must never cost us the snapshot.
    append_snapshot(config.snapshot_path, snapshot)
    log(
        f"deepseek-spend: snapshot {snapshot['ts_utc']} total={snapshot['total']:.2f} "
        f"granted={snapshot['granted']:.2f} topped_up={snapshot['topped_up']:.2f} {snapshot['currency']}"
    )

    snapshots = read_snapshots(config.snapshot_path, config.currency)
    spend = compute_spend(snapshots, now, config)
    message = format_alert_message(spend, config.ceiling)
    log(f"deepseek-spend: {message}")

    state = load_state(config.state_path)
    due = thresholds_due(spend["pct"], config.thresholds, state, spend["month"])
    sent_this_run: list[float] = []
    bus_failures: list[str] = []
    if due and no_bus:
        log(f"deepseek-spend: --no-bus, thresholds {', '.join(f'{t:g}%' for t in due)} left unsent")
    if due and not no_bus:
        # Register + heartbeat once per run, not once per threshold.
        urn: str | None = None
        registration_error: BusError | None = None
        try:
            urn = register_agent(config, transport=transport)
            heartbeat(config, urn, transport=transport)
        except BusError as exc:
            registration_error = exc
        for threshold in due:
            if urn is None:
                # Not marked: the next hourly run retries this threshold.
                bus_failures.append(f"{threshold:g}%: {registration_error}")
                log(f"deepseek-spend: threshold {threshold:g}% NOT sent ({registration_error}); will retry next run")
                continue
            try:
                post_alert(config, urn, message, transport=transport)
            except BusError as exc:
                # Not marked: the next hourly run retries this threshold.
                bus_failures.append(f"{threshold:g}%: {exc}")
                log(f"deepseek-spend: threshold {threshold:g}% NOT sent ({exc}); will retry next run")
                continue
            sent_this_run.append(threshold)
            state = load_state(config.state_path)
            already = state.get("sent") if state.get("month") == spend["month"] else []
            save_state(config.state_path, spend["month"], list(already or []) + [threshold])
            log(f"deepseek-spend: alert sent to {ALERT_TOPIC} at {threshold:g}%")

    return {
        "ok": True,
        "month": spend["month"],
        "spend_usd": spend["spend"],
        "pct": round(spend["pct"], 2),
        "balance_usd": spend["current_total"],
        "topups_usd": spend["topups"],
        "message": message,
        "thresholds_due": due,
        "thresholds_sent": sent_this_run,
        "bus_failures": bus_failures,
        "snapshot_path": str(config.snapshot_path),
        "state_path": str(config.state_path),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DeepSeek month-to-date spend alert (alert only, never blocks).")
    parser.add_argument("--dry-run", action="store_true", help="print the message, write nothing, send nothing")
    parser.add_argument(
        "--no-bus",
        action="store_true",
        help="snapshot + measure, never touch the hive bus (bus thresholds stay unsent)",
    )
    parser.add_argument("--quiet", action="store_true", help="only report the final JSON summary")
    args = parser.parse_args(argv)

    log = (lambda m: None) if args.quiet else (lambda m: print(m, flush=True))
    try:
        config = Config()
        if args.dry_run:
            snapshot = fetch_balance(config.api_key, config.currency)
            snapshots = read_snapshots(config.snapshot_path, config.currency)
            merged = [s for s in snapshots if s["ts"] < snapshot["ts"]] + [snapshot]
            spend = compute_spend(merged, snapshot["ts"], config)
            print(json.dumps({"dry_run": True, "message": format_alert_message(spend, config.ceiling)}, indent=2))
            return 0
        if args.no_bus:
            # Measure only: snapshot + message, but never contact the bus and
            # never mark a threshold as sent, so the next bus-enabled run
            # still delivers. Success, not failure.
            report = run(config, transport=None, no_bus=True, log=log)
        else:
            report = run(config, log=log)
    except ConfigError as exc:
        print(f"deepseek-spend: config error: {exc}", file=sys.stderr)
        return 2
    except FetchError as exc:
        # No snapshot, no alert, non-zero exit: the operator (and systemd) sees
        # a failed run and the next hour retries.
        print(f"deepseek-spend: balance fetch failed: {exc}", file=sys.stderr)
        return 1
    if report["bus_failures"]:
        print(f"deepseek-spend: bus unreachable, thresholds pending: {report['bus_failures']}", file=sys.stderr)
        return 1
    print(json.dumps({k: report[k] for k in ("month", "spend_usd", "pct", "balance_usd", "topups_usd")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
