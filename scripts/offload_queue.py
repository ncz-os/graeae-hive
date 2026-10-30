#!/usr/bin/env python3
"""Planner and submit helper for a single-seat, hour-metered offload agent.

The seat runs ONE agent session at a time. A session is bound to one model and
is billed at least ``sessionMinimumHours`` times that model's multiplier. A
daily pool of hours EXPIRES at a reset time; a separate wallet does not expire
and is never planned (``walletPolicy: reserve``). Everything is configuration:
the job kind name (default ``freebuff``), the approved repos, the model band,
the minutes per task class. Nothing about any product is compiled in.

Inputs (read-only except ``submit`` and ``release``):

* the bus queue, ``GET /v1/jobs`` (base URL from ``HIVE_URL``);
* a balance snapshot, ``OFFLOAD_BALANCE_FILE``;
* a models snapshot, ``OFFLOAD_MODELS_FILE``;
* a config file, ``OFFLOAD_CONFIG`` (JSON).

Commands::

    offload_queue.py submit --task task.json [--dry-run]
    offload_queue.py plan [--json]
    offload_queue.py release [--dry-run]
    offload_queue.py status [--json]
    offload_queue.py evergreen list
    offload_queue.py evergreen pull N [--dry-run]

``--jobs FILE`` replaces the bus read with a JSON file (offline mode). The
bearer token, when ``HIVE_BUS_ADMIN_TOKEN`` is set, is sent and never printed.
``submit`` and ``release`` write to the bus; ``submit`` needs ``HIVE_ACTOR_URN``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_KIND = "freebuff"

#: The "iteration-friendly" classes the offload seat is FOR.
ITERATION_CLASSES = (
    "test-write",
    "test-harden",
    "flake-hunt",
    "lint-type-cleanup",
    "dead-code",
    "mechanical-migration",
    "characterization",
    "soak-loop",
    "doc-from-code",
)
#: Classes the planner REFUSES to submit.
REFUSED_CLASSES = ("design", "security", "hardware", "live-infra", "irreversible")
#: Classes whose brief must declare the automatic gates applied afterwards.
GATED_CLASSES = ("test-write", "test-harden", "lint-type-cleanup", "dead-code", "mechanical-migration")
GATES = ("no-assertion-weakening", "red-on-base", "no-new-skips", "suite-before-after")
#: Classes that build or run tests a lot: a warning when the seat is orchestration-only.
BUILD_HEAVY_CLASSES = ("soak-loop", "flake-hunt", "mechanical-migration")

REASON_REPO = "repo not approved for offload"
REASON_CLASS = "class not suitable for the offload seat"
REASON_BAND = "model outside the approved multiplier band ({lo:g}x floor, {hi:g}x cap)"
REASON_HOLD = "start-of-session hold exceeds the remaining pool"
REASON_MINIMUM = "session minimum exceeds the remaining pool"
REASON_TIME = "first task cannot finish before the daily reset minus margin"
REASON_FIT = "does not fit the remaining pool"

HEADER_MAGIC = "offload-task: v1"
HEADER_SEPARATOR = "---"
_EPS = 1e-9

DEFAULT_MINUTES = {
    "test-write": 45,
    "test-harden": 40,
    "flake-hunt": 60,
    "lint-type-cleanup": 30,
    "dead-code": 30,
    "mechanical-migration": 60,
    "characterization": 45,
    "soak-loop": 90,
    "doc-from-code": 30,
    "default": 30,
}

CONFIG_KEYS = frozenset(
    {
        "kind",
        "where",
        "allowedRepos",
        "minMultiplier",
        "maxMultiplier",
        "defaultMultiplier",
        "allowAbTest",
        "approvedAboveCap",
        "walletPolicy",
        "minutesPerTask",
        "sessionMinimumHours",
        "evergreenFile",
        "resetMarginMinutes",
        "staleSnapshotMinutes",
        "leaseSeconds",
        "priorityBands",
        "maxRetries",
        "branchPrefix",
        "baseBranch",
        "windowHours",
        "briefMaxBytes",
        "balancedSlackHours",
    }
)

# Statuses of a seat that is NOT holding an open session.
_CLOSED_STATES = frozenset({"", "idle", "closed", "ended", "stopped", "none", "inactive", "expired"})

TASK_KEYS = frozenset(
    {"title", "repo", "class", "brief", "model", "depends_on", "ab_test", "estimateMinutes", "base_branch"}
)


class ConfigError(ValueError):
    """The config file is unusable; the planner refuses to guess."""


# ---------------------------------------------------------------- helpers


def _ceil(x: float) -> int:
    return int(math.ceil(x - _EPS))


def _num(v) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int | float):
        return float(v) if math.isfinite(v) else None
    if isinstance(v, str):
        try:
            f = float(v.strip())
        except ValueError:
            return None
        return f if math.isfinite(f) else None
    return None


def parse_ts(v) -> float | None:
    """Epoch seconds from epoch seconds/milliseconds or an ISO-8601 string; None if unusable."""
    n = _num(v)
    if n is not None:
        return n / 1000.0 if n > 1e11 else n
    if isinstance(v, str) and v.strip():
        text = v.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    return None


def _iso(ts: float | None) -> str | None:
    return None if ts is None else datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_h(x: float | None) -> str:
    if x is None:
        return "unknown"
    return f"{x:.2f}".rstrip("0").rstrip(".") + "h"


def _slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")
    return s[:48] or "task"


def valid_branch_name(name: str) -> bool:
    """A conservative subset of git's ref-name rules."""
    if not name or len(name) > 200:
        return False
    if not re.fullmatch(r"[A-Za-z0-9._/-]+", name):
        return False
    return not (
        name.startswith(("-", "/", "."))
        or name.endswith(("/", ".", ".lock"))
        or ".." in name
        or "//" in name
        or "/." in name
    )


# ----------------------------------------------------------------- config


@dataclass
class Config:
    kind: str = DEFAULT_KIND
    where: str = ""
    allowed_repos: tuple[str, ...] = ()
    min_multiplier: float = 2.0
    max_multiplier: float = 4.0
    default_multiplier: float = 3.0
    allow_ab_test: bool = False
    approved_above_cap: tuple[str, ...] = ()
    wallet_policy: str = "reserve"
    minutes_per_task: dict = field(default_factory=lambda: dict(DEFAULT_MINUTES))
    session_minimum_hours: float = 1.0
    evergreen_file: str | None = None
    reset_margin_minutes: float = 30.0
    stale_snapshot_minutes: float = 15.0
    lease_seconds: float = 1800.0
    band_top: int = 90
    band_step: int = 10
    band_floor: int = 10
    band_deferred: int = 0
    max_retries: int = 0
    branch_prefix: str = "offload/"
    base_branch: str | None = "main"
    window_hours: float = 24.0
    brief_max_bytes: int = 48 * 1024
    balanced_slack_hours: float | None = None
    unknown_keys: tuple[str, ...] = ()

    @property
    def orchestration_only(self) -> bool:
        return "orchestration-only" in self.where.lower()

    @property
    def slack_hours(self) -> float:
        """Unused pool below this is BALANCED: no session smaller than it can be opened."""
        if self.balanced_slack_hours is not None:
            return self.balanced_slack_hours
        return self.session_minimum_hours * self.min_multiplier

    def minutes_for(self, task_class: str | None) -> float:
        table = self.minutes_per_task
        if task_class and task_class in table:
            return float(table[task_class])
        return float(table.get("default", DEFAULT_MINUTES["default"]))

    def repo_allowed(self, repo: str | None) -> bool:
        return bool(repo) and str(repo).strip() in self.allowed_repos

    def band(self, rank: int) -> int:
        return max(self.band_floor, self.band_top - max(0, rank) * self.band_step)


def _want_num(raw: dict, key: str, default: float, *, minimum: float | None = None) -> float:
    if key not in raw or raw[key] is None:
        return default
    n = _num(raw[key])
    if n is None or (isinstance(raw[key], str)):
        raise ConfigError(f"config {key} must be a number")
    if minimum is not None and n < minimum:
        raise ConfigError(f"config {key} must be >= {minimum:g}")
    return n


def _want_strs(raw: dict, key: str) -> tuple[str, ...]:
    value = raw.get(key)
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise ConfigError(f"config {key} must be a list of strings")
    return tuple(x.strip() for x in value if x.strip())


def config_from_dict(raw: dict) -> Config:
    if not isinstance(raw, dict):
        raise ConfigError("config must be a JSON object")
    cfg = Config()
    cfg.unknown_keys = tuple(sorted(k for k in raw if k not in CONFIG_KEYS and not str(k).startswith("_")))
    kind = raw.get("kind", DEFAULT_KIND)
    if not isinstance(kind, str) or not kind.strip():
        raise ConfigError("config kind must be a non-empty string")
    cfg.kind = kind.strip()
    where = raw.get("where", "")
    if where is not None and not isinstance(where, str):
        raise ConfigError("config where must be a string (a free-text host-class label)")
    cfg.where = (where or "").strip()
    cfg.allowed_repos = _want_strs(raw, "allowedRepos")
    cfg.min_multiplier = _want_num(raw, "minMultiplier", 2.0, minimum=0)
    cfg.max_multiplier = _want_num(raw, "maxMultiplier", 4.0, minimum=0)
    cfg.default_multiplier = _want_num(raw, "defaultMultiplier", 3.0, minimum=0)
    if cfg.min_multiplier > cfg.max_multiplier:
        raise ConfigError("config minMultiplier must not exceed maxMultiplier")
    if not cfg.min_multiplier - _EPS <= cfg.default_multiplier <= cfg.max_multiplier + _EPS:
        raise ConfigError("config defaultMultiplier must lie inside the minMultiplier..maxMultiplier band")
    allow_ab = raw.get("allowAbTest", False)
    if not isinstance(allow_ab, bool):
        raise ConfigError("config allowAbTest must be true or false")
    cfg.allow_ab_test = allow_ab
    cfg.approved_above_cap = _want_strs(raw, "approvedAboveCap")
    policy = raw.get("walletPolicy", "reserve")
    if policy != "reserve":
        raise ConfigError("config walletPolicy must be 'reserve': no other wallet policy is implemented")
    cfg.wallet_policy = "reserve"
    minutes = dict(DEFAULT_MINUTES)
    if raw.get("minutesPerTask") is not None:
        given = raw["minutesPerTask"]
        if not isinstance(given, dict):
            raise ConfigError("config minutesPerTask must be an object of class -> minutes")
        for name, value in given.items():
            n = _num(value)
            if n is None or n <= 0:
                raise ConfigError(f"config minutesPerTask[{name}] must be a positive number")
            minutes[str(name)] = n
    cfg.minutes_per_task = minutes
    smh = raw.get("sessionMinimumHours", 1)
    if isinstance(smh, dict):
        smh = smh.get("hours")
    smh_n = _num(smh)
    if smh_n is None or isinstance(smh, str) or smh_n <= 0:
        raise ConfigError("config sessionMinimumHours must be a positive number of hours")
    cfg.session_minimum_hours = smh_n
    ev = raw.get("evergreenFile")
    if ev is not None and not isinstance(ev, str):
        raise ConfigError("config evergreenFile must be a path string")
    cfg.evergreen_file = ev or None
    cfg.reset_margin_minutes = _want_num(raw, "resetMarginMinutes", 30.0, minimum=0)
    cfg.stale_snapshot_minutes = _want_num(raw, "staleSnapshotMinutes", 15.0, minimum=0)
    cfg.lease_seconds = _want_num(raw, "leaseSeconds", 1800.0, minimum=1)
    cfg.window_hours = _want_num(raw, "windowHours", 24.0, minimum=1)
    cfg.max_retries = int(_want_num(raw, "maxRetries", 0, minimum=0))
    cfg.brief_max_bytes = int(_want_num(raw, "briefMaxBytes", 48 * 1024, minimum=1))
    if raw.get("balancedSlackHours") is not None:
        cfg.balanced_slack_hours = _want_num(raw, "balancedSlackHours", 0.0, minimum=0)
    prefix = raw.get("branchPrefix", "offload/")
    if not isinstance(prefix, str) or (prefix and not valid_branch_name(prefix.rstrip("/") or "x")):
        raise ConfigError("config branchPrefix must be a valid branch-name prefix")
    cfg.branch_prefix = prefix
    base = raw.get("baseBranch", "main")
    if base is not None and not (isinstance(base, str) and valid_branch_name(base)):
        raise ConfigError("config baseBranch must be a valid branch name or null")
    cfg.base_branch = base
    bands = raw.get("priorityBands")
    if bands is not None:
        if not isinstance(bands, dict):
            raise ConfigError("config priorityBands must be an object {top, step, floor, deferred}")
        cfg.band_top = int(_want_num(bands, "top", cfg.band_top))
        cfg.band_step = int(_want_num(bands, "step", cfg.band_step, minimum=1))
        cfg.band_floor = int(_want_num(bands, "floor", cfg.band_floor))
        cfg.band_deferred = int(_want_num(bands, "deferred", cfg.band_deferred))
        if cfg.band_floor > cfg.band_top:
            raise ConfigError("config priorityBands.floor must not exceed priorityBands.top")
    return cfg


def load_config(path: str | None) -> Config:
    """No path means defaults: ``allowedRepos`` empty, so every submit is refused."""
    if not path:
        return Config()
    try:
        raw = json.loads(Path(path).read_text())
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    except ValueError as exc:
        raise ConfigError(f"config {path} is not valid JSON: {exc}") from exc
    return config_from_dict(raw)


# -------------------------------------------------------------- snapshots


@dataclass
class Balance:
    daily: float | None = None
    wallet: float | None = None
    spendable: float | None = None
    reset_ts: float | None = None
    updated_ts: float | None = None
    session_model: str | None = None
    session_multiplier: float | None = None
    session_started_ts: float | None = None
    state: str | None = None
    problem: str | None = None  # why nothing could be read

    @property
    def session_open(self) -> bool:
        if not self.session_model or self.session_started_ts is None:
            return False
        return (self.state or "").strip().lower() not in _CLOSED_STATES


def balance_from_dict(raw) -> Balance:
    if not isinstance(raw, dict):
        return Balance(problem="balance snapshot is not a JSON object")
    inner = raw.get("balance")
    inner = inner if isinstance(inner, dict) else {}

    def hours(key):
        n = _num(inner.get(key))
        return None if n is None else max(0.0, n)

    model = raw.get("sessionModel")
    state = raw.get("state")
    return Balance(
        daily=hours("dailyRemainingHours"),
        wallet=hours("walletHours"),
        spendable=hours("spendableHours"),
        reset_ts=parse_ts(inner.get("dailyResetAt")),
        updated_ts=parse_ts(inner.get("updatedAt")),
        session_model=model.strip() if isinstance(model, str) and model.strip() else None,
        session_multiplier=_num(raw.get("sessionMultiplier")),
        session_started_ts=parse_ts(raw.get("sessionStartedAt")),
        state=state if isinstance(state, str) else None,
    )


def load_balance(path: str | None) -> Balance:
    if not path:
        return Balance(problem="OFFLOAD_BALANCE_FILE is not set")
    try:
        return balance_from_dict(json.loads(Path(path).read_text()))
    except OSError as exc:
        return Balance(problem=f"cannot read balance snapshot: {exc}")
    except ValueError as exc:
        return Balance(problem=f"balance snapshot is not valid JSON: {exc}")


@dataclass
class Models:
    by_id: dict = field(default_factory=dict)
    problem: str | None = None

    def get(self, model_id: str) -> dict | None:
        return self.by_id.get(model_id)

    def multiplier(self, model_id: str | None) -> float | None:
        entry = self.by_id.get(model_id) if model_id else None
        return None if entry is None else entry["multiplier"]

    def default_model(self, multiplier: float) -> str | None:
        """The available model at ``multiplier``; the lowest id wins, so the choice is stable."""
        hits = sorted(
            mid
            for mid, m in self.by_id.items()
            if m["available"] and m["multiplier"] is not None and abs(m["multiplier"] - multiplier) < _EPS
        )
        return hits[0] if hits else None


def models_from_dict(raw) -> Models:
    rows = raw.get("models") if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        return Models(problem="models snapshot has no models list")
    out = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"].strip():
            continue
        out[row["id"].strip()] = {
            "id": row["id"].strip(),
            "displayName": row.get("displayName"),
            "available": row.get("available") is True,
            "multiplier": _num(row.get("costMultiplier")),
        }
    return Models(by_id=out)


def load_models(path: str | None) -> Models:
    if not path:
        return Models(problem="OFFLOAD_MODELS_FILE is not set")
    try:
        return models_from_dict(json.loads(Path(path).read_text()))
    except OSError as exc:
        return Models(problem=f"cannot read models snapshot: {exc}")
    except ValueError as exc:
        return Models(problem=f"models snapshot is not valid JSON: {exc}")


# ----------------------------------------------------------- model policy


def check_model(cfg: Config, models: Models, model_id: str, *, ab_test: bool = False) -> dict:
    """Apply the approved multiplier band to ``model_id``.

    Returns ``{ok, reason, detail, multiplier}``. 1x passes only with
    ``allowAbTest`` in the config AND an ``ab_test`` label on the job; a model
    above the cap needs to be listed in ``approvedAboveCap``.
    """
    entry = models.get(model_id)
    if entry is None:
        why = models.problem or "model is not in the models snapshot"
        return {"ok": False, "reason": "model not in the models snapshot", "detail": why, "multiplier": None}
    mult = entry["multiplier"]
    if not entry["available"]:
        return {"ok": False, "reason": "model not available", "detail": model_id, "multiplier": mult}
    if mult is None:
        return {
            "ok": False,
            "reason": "model multiplier unknown",
            "detail": f"{model_id} has no costMultiplier",
            "multiplier": None,
        }
    band = REASON_BAND.format(lo=cfg.min_multiplier, hi=cfg.max_multiplier)
    if mult < cfg.min_multiplier - _EPS:
        if abs(mult - 1.0) < _EPS and cfg.allow_ab_test and ab_test:
            return {"ok": True, "reason": None, "detail": "1x A/B test", "multiplier": mult}
        hint = f"{model_id} is {mult:g}x"
        if abs(mult - 1.0) < _EPS:
            hint += "; 1x needs allowAbTest in the config and an ab_test label on the job"
        return {"ok": False, "reason": band, "detail": hint, "multiplier": mult}
    if mult > cfg.max_multiplier + _EPS:
        if model_id in cfg.approved_above_cap:
            return {"ok": True, "reason": None, "detail": "above cap, operator-approved", "multiplier": mult}
        return {
            "ok": False,
            "reason": band,
            "detail": f"{model_id} is {mult:g}x; above-cap models need approvedAboveCap",
            "multiplier": mult,
        }
    return {"ok": True, "reason": None, "detail": None, "multiplier": mult}


# ---------------------------------------------------- brief and job format


def parse_brief(brief: str) -> dict:
    """The structured acceptance-gate lines of a brief.

    ``check:`` (exact command), ``branch:`` (name to push), ``gates:``
    (comma-separated). More than one distinct value for a line is an error:
    the verifier must not have to guess which one counts.
    """
    found: dict[str, list[str]] = {"check": [], "branch": [], "gates": [], "base": []}
    for line in brief.splitlines():
        m = re.match(r"^[ \t]*(check|branch|gates|base)[ \t]*:[ \t]*(.*?)[ \t]*$", line, re.IGNORECASE)
        if m:
            found[m.group(1).lower()].append(m.group(2))
    errors = []
    out: dict = {"errors": errors}
    for key in ("check", "branch", "base"):
        values = [v for v in found[key] if v]
        if len(set(values)) > 1:
            errors.append(f"more than one distinct {key}: line")
        out[key] = values[0] if values else None
    gates = []
    for raw in found["gates"]:
        gates.extend(g.strip().lower() for g in re.split(r"[,\s]+", raw) if g.strip())
    out["gates"] = list(dict.fromkeys(gates))
    return out


def build_description(meta: dict, brief: str) -> str:
    """A header the verifier and the planner can parse, a separator, then the brief."""
    lines = [HEADER_MAGIC]
    for key in ("title", "class", "model", "multiplier", "estimate-minutes", "ab_test", "gates", "check", "branch"):
        value = meta.get(key)
        if value is None or value == "":
            continue
        text = ",".join(value) if isinstance(value, list) else str(value)
        lines.append(f"{key}: {' '.join(text.split()) if key == 'title' else text}")
    return "\n".join(lines) + f"\n{HEADER_SEPARATOR}\n" + brief


def parse_header(description: str | None) -> dict | None:
    """Header fields of a job made by this tool, or None for a foreign job."""
    if not description:
        return None
    lines = description.splitlines()
    if not lines or lines[0].strip() != HEADER_MAGIC:
        return None
    out: dict = {}
    for line in lines[1:]:
        if line.strip() == HEADER_SEPARATOR:
            break
        key, sep, value = line.partition(":")
        if sep:
            out[key.strip()] = value.strip()
    return out


def idempotency_key(cfg: Config, task: dict, parsed: dict) -> str:
    """Stable for one task: same repo, class, branch, check and brief give the same key."""
    canon = json.dumps(
        {
            "kind": cfg.kind,
            "repo": str(task.get("repo") or "").strip(),
            "class": str(task.get("class") or "").strip(),
            "title": " ".join(str(task.get("title") or "").split()),
            "branch": parsed.get("branch"),
            "check": parsed.get("check"),
            "brief": str(task.get("brief") or "").strip(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "offload:" + hashlib.sha256(canon.encode("utf-8")).hexdigest()[:32]


# -------------------------------------------------------------- bus access


Transport = Callable[[str, str, dict | None], tuple[int, object]]


class BusError(RuntimeError):
    pass


def http_transport(base: str, timeout: float = 15.0) -> Transport:
    """urllib transport against ``base``. The token is read from the environment, never printed."""

    def call(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
        headers = {"accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"
        token = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
        if token:
            headers["authorization"] = "Bearer " + token
        req = urllib.request.Request(base.rstrip("/") + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - operator-configured URL
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            try:
                payload = json.loads(exc.read() or b"null")
            except ValueError:
                payload = None
            return exc.code, payload
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise BusError(f"cannot reach the bus: {getattr(exc, 'reason', exc)}") from exc

    return call


def fetch_jobs(transport: Transport, statuses: tuple[str, ...], kind: str | None) -> tuple[list[dict], list[str]]:
    """Jobs in ``statuses`` (optionally only of ``kind``) and warnings about truncated listings."""
    jobs: list[dict] = []
    warnings: list[str] = []
    for status in statuses:
        code, data = transport("GET", f"/v1/jobs?status={status}&limit=1000&include_result=false", None)
        if code != 200 or not isinstance(data, dict) or not isinstance(data.get("jobs"), list):
            raise BusError(f"GET /v1/jobs?status={status} answered {code}")
        rows = [j for j in data["jobs"] if isinstance(j, dict) and (kind is None or j.get("kind") == kind)]
        jobs.extend(rows)
        total = _num(data.get("total"))
        if total is not None and total > len(data["jobs"]):
            warnings.append(f"{status} listing truncated: {len(data['jobs'])} of {int(total)} rows read")
    return jobs, warnings


def load_jobs_file(path: str, kind: str | None) -> list[dict]:
    raw = json.loads(Path(path).read_text())
    rows = raw.get("jobs") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError(f"{path} must hold a jobs list")
    return [j for j in rows if isinstance(j, dict) and (kind is None or j.get("kind") == kind)]


# --------------------------------------------------------- plan: items


@dataclass
class Item:
    id: str
    title: str
    cls: str | None
    model: str | None
    multiplier: float | None
    minutes: float
    priority: int
    queued_at: float
    index: int
    ab_test: bool = False
    virtual: bool = False
    foreign: bool = False  # not made by this tool: no header

    def brief(self) -> dict:
        return {"id": self.id, "title": self.title, "class": self.cls, "model": self.model, "minutes": self.minutes}


def _job_models(job: dict) -> list[str]:
    value = job.get("preferred_models")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            value = [value]
    return [m for m in (value or []) if isinstance(m, str) and m.strip()]


def items_from_jobs(cfg: Config, models: Models, jobs: list[dict]) -> tuple[list[Item], list[dict]]:
    """Plannable items and the jobs left out (with the reason)."""
    items: list[Item] = []
    excluded: list[dict] = []
    default_id = models.default_model(cfg.default_multiplier)
    for index, job in enumerate(jobs):
        jid = str(job.get("id") or "")
        head = parse_header(job.get("description"))
        foreign = head is None
        head = head or {}
        cls = head.get("class") or None
        title = head.get("title") or jid[:12] or "(untitled)"
        if cls in REFUSED_CLASSES:
            excluded.append({"id": jid, "title": title, "reason": REASON_CLASS, "detail": cls})
            continue
        chosen = _job_models(job)
        model = chosen[0] if chosen else default_id
        ab = str(head.get("ab_test", "")).strip().lower() == "true"
        est = _num(head.get("estimate-minutes"))
        minutes = est if est and est > 0 else cfg.minutes_for(cls)
        mult = models.multiplier(model)
        if model is None:
            excluded.append(
                {
                    "id": jid,
                    "title": title,
                    "reason": "no model and no available model at the default multiplier",
                    "detail": f"defaultMultiplier {cfg.default_multiplier:g}",
                }
            )
            continue
        verdict = check_model(cfg, models, model, ab_test=ab)
        if not verdict["ok"]:
            excluded.append({"id": jid, "title": title, "reason": verdict["reason"], "detail": verdict["detail"]})
            continue
        items.append(
            Item(
                id=jid,
                title=title,
                cls=cls,
                model=model,
                multiplier=mult,
                minutes=minutes,
                priority=int(_num(job.get("priority")) or 0),
                queued_at=_num(job.get("started_at")) or 0.0,
                index=index,
                ab_test=ab,
                foreign=foreign,
            )
        )
    return items, excluded


def _task_order(it: Item):
    return (-it.priority, it.queued_at, it.index)


def count_switches(models_seq: list[str | None], start: str | None) -> int:
    n = 0
    prev = start
    for model in models_seq:
        if prev is not None and model != prev:
            n += 1
        prev = model
    return n


def order_blocks(items: list[Item], open_model: str | None) -> list[list[Item]]:
    """One block per model, ordered so the seat never returns to a model it left.

    The model of the open session first (it needs no new session minimum), then
    the block holding the highest-priority job, then the oldest job, then the
    model id so the order is deterministic.
    """
    groups: dict[str, list[Item]] = {}
    for it in sorted(items, key=_task_order):
        groups.setdefault(str(it.model), []).append(it)

    def key(model: str):
        tasks = groups[model]
        return (
            0 if model == open_model else 1,
            -max(t.priority for t in tasks),
            min(t.queued_at for t in tasks),
            model,
        )

    return [groups[m] for m in sorted(groups, key=key)]


def _fit(
    cfg: Config,
    blocks: list[list[Item]],
    *,
    remaining: float,
    window_minutes: float | None,
    open_model: str | None,
) -> tuple[list[dict], list[dict]]:
    """Walk the blocks in order against the pool and the time left. Returns (planned, deferred)."""
    planned: list[dict] = []
    deferred: list[dict] = []
    used = 0.0
    elapsed = 0.0
    for block in blocks:
        model = str(block[0].model)
        mult = float(block[0].multiplier or 0.0)
        continues = model == open_model
        avail = remaining - used

        def defer_all(reason: str, detail: str, tasks=block, model=model):
            for t in tasks:
                deferred.append({**t.brief(), "model": model, "reason": reason, "detail": detail})

        if not continues and mult > avail + _EPS:
            defer_all(REASON_HOLD, f"{mult:g}h hold > {avail:g}h remaining")
            continue
        if not continues and cfg.session_minimum_hours * mult > avail + _EPS:
            defer_all(REASON_MINIMUM, f"{cfg.session_minimum_hours * mult:g}h minimum > {avail:g}h remaining")
            continue
        hours_cap = math.floor(avail / mult + _EPS) if mult > 0 else 0
        time_left = None if window_minutes is None else window_minutes - elapsed
        chosen: list[Item] = []
        minutes = 0.0
        for t in block:
            if minutes + t.minutes > hours_cap * 60 + _EPS:
                deferred.append({**t.brief(), "reason": REASON_FIT, "detail": f"{t.minutes:g} min"})
                continue
            if time_left is not None and minutes + t.minutes > time_left + _EPS:
                reason = (
                    REASON_TIME if not chosen and not continues else "cannot finish before the daily reset minus margin"
                )
                deferred.append({**t.brief(), "reason": reason, "detail": f"{t.minutes:g} min"})
                continue
            chosen.append(t)
            minutes += t.minutes
        if not chosen:
            continue
        work_hours = _ceil(minutes / 60.0)
        session_hours = work_hours if continues else max(cfg.session_minimum_hours, float(work_hours))
        cost = session_hours * mult
        used += cost
        elapsed += minutes
        planned.append(
            {
                "model": model,
                "multiplier": mult,
                "items": chosen,
                "minutes": minutes,
                "sessionHours": session_hours,
                "costHours": cost,
                "continuesOpenSession": continues,
            }
        )
    return planned, deferred


# ----------------------------------------------------------- plan: build


def _window(balance: Balance, cfg: Config, now: float) -> tuple[float | None, list[str], list[str]]:
    """Minutes usable before the reset minus margin, plus warnings and what cannot be computed."""
    warnings: list[str] = []
    cannot: list[str] = []
    if balance.reset_ts is None:
        cannot.append("dailyResetAt is unknown: the time window and the projected unused hours at reset")
        return None, warnings, cannot
    if balance.reset_ts <= now:
        warnings.append("dailyResetAt is in the past: the snapshot predates the reset, so the pool is unknown")
        cannot.append("the pool after the reset: take a fresh balance snapshot")
        return 0.0, warnings, cannot
    return max(0.0, (balance.reset_ts - cfg.reset_margin_minutes * 60 - now) / 60.0), warnings, cannot


def build_plan(
    cfg: Config,
    models: Models,
    balance: Balance,
    jobs: list[dict],
    *,
    now: float,
    evergreen: list[Item] | None = None,
    evergreen_blocked: int = 0,
    extra_warnings: list[str] | None = None,
) -> dict:
    warnings = list(extra_warnings or [])
    cannot: list[str] = []
    if cfg.unknown_keys:
        warnings.append(f"config has unknown key(s) that are ignored: {', '.join(cfg.unknown_keys)}")
    if models.problem:
        cannot.append(f"model multipliers: {models.problem}")
    if balance.problem:
        cannot.append(f"the pool: {balance.problem}")

    items, excluded = items_from_jobs(cfg, models, jobs)
    for it in items:
        if it.foreign:
            warnings.append(f"job {it.id[:12]} was not submitted by offload_queue (no header): default class estimate")
    window_minutes, wwarn, wcannot = _window(balance, cfg, now)
    warnings.extend(wwarn)
    cannot.extend(wcannot)

    open_model = balance.session_model if balance.session_open else None
    remaining = balance.daily
    pool_known = remaining is not None and not (balance.reset_ts is not None and balance.reset_ts <= now)
    if balance.daily is None and not balance.problem:
        cannot.append("dailyRemainingHours is null: the fit against the pool and the verdict")
    if balance.updated_ts is None and not balance.problem:
        warnings.append("balance updatedAt is null: the snapshot age is unknown")
    elif balance.updated_ts is not None and now - balance.updated_ts > cfg.stale_snapshot_minutes * 60:
        warnings.append(
            f"balance snapshot is stale: {(now - balance.updated_ts) / 60:.0f} min old "
            f"(limit {cfg.stale_snapshot_minutes:g} min)"
        )

    fifo = sorted(items, key=_task_order)
    naive_switches = count_switches([i.model for i in fifo], open_model)
    blocks = order_blocks(items, open_model)

    planned: list[dict] = []
    deferred: list[dict] = []
    if pool_known:
        planned, deferred = _fit(
            cfg, blocks, remaining=float(remaining), window_minutes=window_minutes, open_model=open_model
        )
    planned_models = [p["model"] for p in planned]
    planned_switches = count_switches(planned_models, open_model)

    used = sum(p["costHours"] for p in planned)
    unused = None if not pool_known else max(0.0, float(remaining) - used)

    # Bands: planned blocks by rank, then blocks that did not start, then deferred tasks.
    ranked = list(planned_models) + [str(b[0].model) for b in blocks if str(b[0].model) not in planned_models]
    block_rows = []
    planned_ids: set[str] = set()
    for rank, model in enumerate(ranked):
        p = next((x for x in planned if x["model"] == model), None)
        group = next(b for b in blocks if str(b[0].model) == model)
        mult = float(group[0].multiplier or 0.0)
        if p:
            ids = [t.id for t in p["items"]]
            planned_ids.update(ids)
            minutes, session_hours, cost, cont = (
                p["minutes"],
                p["sessionHours"],
                p["costHours"],
                p["continuesOpenSession"],
            )
        elif pool_known:
            ids, minutes, session_hours, cost, cont = [], 0.0, 0.0, 0.0, False
        else:
            ids = [t.id for t in group]
            minutes = sum(t.minutes for t in group)
            cont = model == open_model
            work = _ceil(minutes / 60.0)
            session_hours = float(work) if cont else max(cfg.session_minimum_hours, float(work))
            cost = session_hours * mult
        block_rows.append(
            {
                "rank": rank,
                "model": model,
                "multiplier": mult,
                "state": "planned" if p else ("not-started" if pool_known else "unplaced"),
                "tasks": len(ids) if (p or not pool_known) else 0,
                "taskIds": ids,
                "queuedTasks": len(group),
                "minutes": minutes,
                "estimatedSessionHours": session_hours,
                "estimatedCostHours": cost,
                "continuesOpenSession": cont,
                "band": cfg.band(rank) if (p or not pool_known) else cfg.band_deferred,
            }
        )
    bands = {}
    for row in block_rows:
        for jid in row["taskIds"]:
            bands[jid] = row["band"]
    for it in items:
        bands.setdefault(it.id, cfg.band_deferred)

    verdict, vreasons = _verdict(cfg, items, planned, deferred, remaining if pool_known else None, unused)
    plan = {
        "kind": cfg.kind,
        "generatedAt": _iso(now),
        "seat": cfg.where or None,
        "walletPolicy": cfg.wallet_policy,
        "window": {
            "resetAt": _iso(balance.reset_ts),
            "marginMinutes": cfg.reset_margin_minutes,
            "usableMinutes": window_minutes,
        },
        "pool": {
            "dailyRemainingHours": balance.daily,
            "plannedHours": used if pool_known else None,
            "unusedHoursAtReset": unused,
            "walletHours": balance.wallet,
            "walletNote": "reserved: never planned",
        },
        "queued": len(jobs),
        "plannable": len(items),
        "blocks": block_rows,
        "bands": bands,
        "activeBlock": planned_models[0] if planned else None,
        "switches": {"naive": naive_switches, "planned": planned_switches},
        "deferred": deferred,
        "excluded": excluded,
        "verdict": verdict,
        "reasons": vreasons,
        "suggestion": None,
        "warnings": warnings,
        "cannotCompute": cannot,
    }
    if not items:
        plan["note"] = "no useful work queued"
    if cfg.orchestration_only:
        heavy = sorted({i.cls for i in items if i.cls in BUILD_HEAVY_CLASSES})
        if heavy:
            plan["warnings"].append(
                "seat is marked orchestration-only but the plan contains build/test-heavy classes: " + ", ".join(heavy)
            )
    if verdict == "UNDER-USED":
        plan["suggestion"] = _suggest(
            cfg, models, balance, items, evergreen, evergreen_blocked, now, remaining, window_minutes, open_model
        )
    return plan


def _verdict(cfg, items, planned, deferred, remaining, unused) -> tuple[str, list[str]]:
    if remaining is None:
        return "UNKNOWN", ["the pool is unknown, so no verdict can be given"]
    if not items:
        if remaining + _EPS >= cfg.slack_hours:
            return "UNDER-USED", [f"no useful work queued; {_fmt_h(remaining)} of the pool would expire unused"]
        return "BALANCED", ["no useful work queued; the pool left is below one session minimum"]
    if not planned:
        seen = []
        for d in deferred:
            if d["reason"] not in seen:
                seen.append(d["reason"])
        return "CANNOT-START", seen or ["no session can be started"]
    if deferred:
        return "OVER", [f"{len(deferred)} task(s) do not fit and are deferred"]
    if unused is not None and unused + _EPS >= cfg.slack_hours:
        return "UNDER-USED", [f"{_fmt_h(unused)} of the pool would expire unused"]
    return "BALANCED", ["the planned work uses the pool; what is left is below one session minimum"]


def _suggest(cfg, models, balance, items, evergreen, blocked, now, remaining, window_minutes, open_model) -> dict:
    out: dict = {"source": "evergreen", "pull": 0}
    if remaining is None:
        out["message"] = "the pool is unknown: nothing can be suggested"
        return out
    pool = list(evergreen or [])
    if not pool:
        out["message"] = (
            "no useful work queued and the evergreen backlog has nothing usable: do not invent work"
            if not items
            else "the evergreen backlog has nothing usable: do not invent work"
        )
        if blocked:
            out["message"] += f" ({blocked} backlog item(s) skipped: repo or class not approved)"
        return out
    # The largest prefix of the backlog that fits without displacing real queued work.
    count = 0
    ids: set[str] = set()
    for n in range(len(pool), 0, -1):
        blocks = order_blocks(items + pool[:n], open_model)
        planned, _ = _fit(cfg, blocks, remaining=remaining, window_minutes=window_minutes, open_model=open_model)
        placed = {t.id for p in planned for t in p["items"]}
        if all(i.id in placed for i in items) and all(v.id in placed for v in pool[:n]):
            count, ids = n, placed
            break
    out["pull"] = count
    out["items"] = [v.title for v in pool if v.id in ids] if count else []
    out["message"] = (
        f"pull {count} item(s) from the evergreen backlog" if count else "no evergreen item fits the unused pool"
    )
    return out


def render_plan(plan: dict) -> str:
    pool = plan["pool"]
    win = plan["window"]
    lines = [f"offload plan for kind {plan['kind']} at {plan['generatedAt']}"]
    if plan.get("seat"):
        lines.append(f"seat: {plan['seat']}")
    lines.append(
        f"pool: {_fmt_h(pool['dailyRemainingHours'])} remaining; reset at {win['resetAt'] or 'unknown'} "
        f"(margin {win['marginMinutes']:g} min)"
    )
    lines.append(f"wallet: {_fmt_h(pool['walletHours'])} (reserved, not planned)")
    lines.append(f"queued: {plan['queued']} job(s), {plan['plannable']} plannable")
    if plan.get("note"):
        lines.append(plan["note"])
    lines.append("blocks:")
    if not plan["blocks"]:
        lines.append("  (none)")
    for b in plan["blocks"]:
        lines.append(
            f"  {b['rank'] + 1}. {b['model']} x{b['multiplier']:g} [{b['state']}] "
            f"tasks={b['tasks']}/{b['queuedTasks']} work={b['minutes']:g} min "
            f"session={_fmt_h(b['estimatedSessionHours'])} cost={_fmt_h(b['estimatedCostHours'])} "
            f"band={b['band']}" + (" (continues the open session)" if b["continuesOpenSession"] else "")
        )
    lines.append(f"model switches: naive order {plan['switches']['naive']}, planned {plan['switches']['planned']}")
    lines.append(
        f"total planned: {_fmt_h(pool['plannedHours'])} of {_fmt_h(pool['dailyRemainingHours'])}; "
        f"projected unused at reset: {_fmt_h(pool['unusedHoursAtReset'])}"
    )
    if plan["deferred"]:
        lines.append("deferred (will not fit):")
        for d in plan["deferred"]:
            lines.append(f"  {d['id'][:12]} {d['title']} [{d['model']}]: {d['reason']} ({d['detail']})")
    if plan["excluded"]:
        lines.append("not planned (policy):")
        for e in plan["excluded"]:
            lines.append(f"  {e['id'][:12]} {e['title']}: {e['reason']}")
    lines.append(f"verdict: {plan['verdict']}")
    for reason in plan["reasons"]:
        lines.append(f"  - {reason}")
    sug = plan.get("suggestion")
    if sug:
        lines.append(f"suggestion: {sug['message']}")
        for title in sug.get("items", []):
            lines.append(f"  + {title}")
    if plan["cannotCompute"]:
        lines.append("cannot compute:")
        for c in plan["cannotCompute"]:
            lines.append(f"  - {c}")
    for w in plan["warnings"]:
        lines.append(f"warning: {w}")
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------- submit


def validate_task(
    cfg: Config,
    models: Models,
    task: dict,
    *,
    job_exists: Callable[[str], bool | None] | None = None,
) -> dict:
    """Check one task against every rule. Returns ``{ok, reasons, details, parsed, model, multiplier}``."""
    reasons: list[str] = []
    details: list[str] = []

    def refuse(reason: str, detail: str | None = None):
        if reason not in reasons:
            reasons.append(reason)
        if detail:
            details.append(detail)

    unknown = sorted(k for k in task if k not in TASK_KEYS and not str(k).startswith("_"))
    if unknown:
        refuse("unknown task key(s)", ", ".join(unknown))
    repo = str(task.get("repo") or "").strip()
    if not cfg.repo_allowed(repo):
        refuse(REASON_REPO, repo or "no repo given")
    cls = str(task.get("class") or "").strip()
    if cls in REFUSED_CLASSES:
        refuse(REASON_CLASS, cls)
    elif cls not in ITERATION_CLASSES:
        refuse("task class missing or unknown", cls or "no class given")
    brief = task.get("brief")
    if not isinstance(brief, str) or not brief.strip():
        refuse("brief is missing")
        brief = ""
    if len(brief.encode("utf-8")) >= cfg.brief_max_bytes:
        refuse(f"brief is not under {cfg.brief_max_bytes // 1024} KiB", f"{len(brief.encode('utf-8'))} bytes")
    parsed = parse_brief(brief)
    for err in parsed["errors"]:
        refuse(err)
    if not parsed["check"]:
        refuse("brief has no check: line (the exact command)")
    if not parsed["branch"]:
        refuse("brief has no branch: line (the branch name to push)")
    elif not valid_branch_name(parsed["branch"]):
        refuse("branch name is not valid", parsed["branch"])
    if cls in GATED_CLASSES:
        if not parsed["gates"]:
            refuse(f"class {cls} needs a gates: line declaring the automatic gates")
        else:
            bad = [g for g in parsed["gates"] if g not in GATES]
            if bad:
                refuse("unknown gate(s) in gates: line", ", ".join(bad))
    elif parsed["gates"]:
        bad = [g for g in parsed["gates"] if g not in GATES]
        if bad:
            refuse("unknown gate(s) in gates: line", ", ".join(bad))
    ab = task.get("ab_test", False)
    if ab not in (True, False, None):
        refuse("ab_test must be true or false")
    ab = ab is True

    model = task.get("model")
    multiplier = None
    if model in (None, ""):
        model = models.default_model(cfg.default_multiplier)
        if model is None:
            refuse(
                "no available model at the default multiplier",
                f"defaultMultiplier {cfg.default_multiplier:g}; {models.problem or 'none in the models snapshot'}",
            )
        else:
            multiplier = models.multiplier(model)
    elif not isinstance(model, str):
        refuse("model must be a model id string")
        model = None
    else:
        model = model.strip()
        verdict = check_model(cfg, models, model, ab_test=ab)
        multiplier = verdict["multiplier"]
        if not verdict["ok"]:
            refuse(verdict["reason"], verdict["detail"])
    est = task.get("estimateMinutes")
    if est is not None and (_num(est) is None or _num(est) <= 0):
        refuse("estimateMinutes must be a positive number")
    deps = task.get("depends_on")
    if deps is not None:
        if not isinstance(deps, list) or not all(isinstance(d, str) and d.strip() for d in deps):
            refuse("depends_on must be a list of job ids")
        elif job_exists is not None:
            missing = [d for d in deps if job_exists(d) is False]
            if missing:
                refuse("depends_on job does not exist", ", ".join(missing))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "details": details,
        "parsed": parsed,
        "model": model,
        "multiplier": multiplier,
        "ab_test": ab,
    }


def task_payload(cfg: Config, task: dict, check: dict, *, priority: int, actor: str | None) -> dict:
    parsed = check["parsed"]
    meta = {
        "title": task.get("title") or _slug(parsed["branch"] or "task"),
        "class": str(task["class"]).strip(),
        "model": check["model"],
        "multiplier": f"{check['multiplier']:g}" if check["multiplier"] is not None else None,
        "estimate-minutes": (
            f"{_num(task['estimateMinutes']):g}"
            if task.get("estimateMinutes")
            else f"{cfg.minutes_for(str(task['class']).strip()):g}"
        ),
        "ab_test": "true" if check["ab_test"] else None,
        "gates": parsed["gates"] or None,
        "check": parsed["check"],
        "branch": parsed["branch"],
    }
    payload = {
        "kind": cfg.kind,
        "description": build_description(meta, str(task["brief"]).strip()),
        "priority": priority,
        "eligible_kinds": [cfg.kind],
        "preferred_models": [check["model"]],
        "repo": str(task["repo"]).strip(),
        "head_branch": parsed["branch"],
        "idempotency_key": idempotency_key(cfg, task, parsed),
        "max_retries": cfg.max_retries,
        "submitter_urn": actor if actor else "<from HIVE_ACTOR_URN>",
    }
    base = task.get("base_branch") or parsed.get("base") or cfg.base_branch
    if base:
        payload["base_branch"] = base
    if task.get("depends_on"):
        payload["depends_on"] = list(task["depends_on"])
    if cfg.max_retries == 0:
        payload["zero_retry_reason"] = "offload seat is hour-metered: a failed run is reviewed, not silently re-spent"
    return payload


@dataclass
class Context:
    cfg: Config
    models: Models
    balance: Balance
    transport: Transport | None
    jobs_file: str | None = None
    now: float = field(default_factory=time.time)
    actor: str | None = None

    def queue(self) -> tuple[list[dict], list[str]]:
        """Queued jobs of the seat's kind; raises BusError when there is no source."""
        if self.jobs_file:
            rows = load_jobs_file(self.jobs_file, self.cfg.kind)
            return [j for j in rows if j.get("status", "queued") == "queued"], []
        if self.transport is None:
            raise BusError("HIVE_URL is not set (the bus URL comes from the environment)")
        return fetch_jobs(self.transport, ("queued",), self.cfg.kind)

    def job_exists(self, job_id: str) -> bool | None:
        if self.transport is None:
            return None
        code, _ = self.transport("GET", f"/v1/jobs/{job_id}", None)
        if code == 404:
            return False
        return True if code == 200 else None


def band_for_new_task(ctx: Context, model: str, multiplier: float | None, ab: bool) -> tuple[int, str | None]:
    """The plan band of the block this task would join. (band, warning)."""
    try:
        jobs, _ = ctx.queue()
    except BusError as exc:
        return ctx.cfg.band_floor, f"priority is the lowest band: {exc}"
    items, _ = items_from_jobs(ctx.cfg, ctx.models, jobs)
    items.append(
        Item(
            id="<new>",
            title="<new>",
            cls=None,
            model=model,
            multiplier=multiplier,
            minutes=0,
            priority=0,
            queued_at=ctx.now,
            index=len(items),
            ab_test=ab,
        )
    )
    open_model = ctx.balance.session_model if ctx.balance.session_open else None
    blocks = order_blocks(items, open_model)
    for rank, block in enumerate(blocks):
        if str(block[0].model) == model:
            return ctx.cfg.band(rank), None
    return ctx.cfg.band_floor, None


def submit_task(ctx: Context, task: dict, *, dry_run: bool) -> dict:
    result = validate_task(ctx.cfg, ctx.models, task, job_exists=ctx.job_exists)
    out: dict = {"ok": False, "refused": result["reasons"], "details": result["details"], "dryRun": dry_run}
    if not result["ok"]:
        return out
    priority, warn = band_for_new_task(ctx, result["model"], result["multiplier"], result["ab_test"])
    payload = task_payload(ctx.cfg, task, result, priority=priority, actor=None if dry_run else ctx.actor)
    out["payload"] = payload
    out["warnings"] = [warn] if warn else []
    if dry_run:
        out["ok"] = True
        return out
    if ctx.transport is None:
        raise BusError("HIVE_URL is not set (the bus URL comes from the environment)")
    if not ctx.actor:
        raise BusError("HIVE_ACTOR_URN is not set: it becomes the job's submitter_urn")
    code, data = ctx.transport("POST", "/v1/jobs", payload)
    if code != 200 or not isinstance(data, dict) or not data.get("id"):
        detail = data.get("detail") if isinstance(data, dict) else None
        raise BusError(f"the bus refused the job ({code}){': ' + str(detail)[:300] if detail else ''}")
    out["ok"] = True
    out["id"] = data["id"]
    out["coalesced"] = bool(data.get("coalesced"))
    out["busWarnings"] = data.get("warnings") or []
    return out


# ---------------------------------------------------------------- release


def _plan_for_queue(ctx: Context, evergreen: list[Item] | None = None, blocked: int = 0) -> tuple[dict, list[dict]]:
    jobs, warnings = ctx.queue()
    plan = build_plan(
        ctx.cfg,
        ctx.models,
        ctx.balance,
        jobs,
        now=ctx.now,
        evergreen=evergreen,
        evergreen_blocked=blocked,
        extra_warnings=warnings,
    )
    return plan, jobs


def release(ctx: Context, *, dry_run: bool, only_ids: list[str] | None = None) -> dict:
    """Assign the plan's priority bands to the queued jobs of the seat's kind.

    Never touches a job of another kind or one that is not queued. Each PATCH is
    read back: a bus that does not apply ``priority`` is reported, not assumed.
    """
    plan, jobs = _plan_for_queue(ctx)
    out: dict = {"ok": True, "dryRun": dry_run, "verdict": plan["verdict"], "changes": [], "refused": []}
    if plan["activeBlock"] is None:
        out["ok"] = False
        out["error"] = f"no active block to release (verdict {plan['verdict']})"
        return out
    out["activeBlock"] = plan["activeBlock"]
    by_id = {str(j.get("id")): j for j in jobs}
    wanted = plan["bands"]
    targets = list(wanted)
    if only_ids:
        for jid in only_ids:
            job = by_id.get(jid)
            if job is None or job.get("kind") != ctx.cfg.kind:
                out["refused"].append({"id": jid, "reason": f"not a queued job of kind {ctx.cfg.kind}"})
        targets = [j for j in targets if j in only_ids]
    for jid in targets:
        job = by_id[jid]
        want = wanted[jid]
        row = {"id": jid, "from": job.get("priority"), "to": want}
        if job.get("kind") != ctx.cfg.kind or job.get("status", "queued") != "queued":
            out["refused"].append({"id": jid, "reason": f"not a queued job of kind {ctx.cfg.kind}"})
            continue
        if job.get("priority") == want:
            row["action"] = "unchanged"
        elif dry_run:
            row["action"] = "would-patch"
        else:
            if ctx.transport is None:
                raise BusError("HIVE_URL is not set (the bus URL comes from the environment)")
            code, _ = ctx.transport("PATCH", f"/v1/jobs/{jid}", {"status": "queued", "priority": want})
            if code != 200:
                row["action"] = f"patch-refused-{code}"
                out["ok"] = False
            else:
                gcode, got = ctx.transport("GET", f"/v1/jobs/{jid}", None)
                applied = gcode == 200 and isinstance(got, dict) and got.get("priority") == want
                row["action"] = "patched" if applied else "not-applied"
                if not applied:
                    out["ok"] = False
        out["changes"].append(row)
    if any(c["action"] == "not-applied" for c in out["changes"]):
        out["error"] = (
            "the bus accepted the PATCH but did not change priority: PATCH /v1/jobs/{id} has no "
            "priority field in this bus version, so bands can only be set at submit time"
        )
    return out


def render_release(res: dict) -> str:
    lines = [f"release ({'dry run' if res['dryRun'] else 'applied'}); verdict {res['verdict']}"]
    if res.get("error"):
        lines.append(f"error: {res['error']}")
    if res.get("activeBlock"):
        lines.append(f"active block: {res['activeBlock']}")
    for c in res["changes"]:
        lines.append(f"  {c['id'][:12]} priority {c['from']} -> {c['to']}: {c['action']}")
    for r in res["refused"]:
        lines.append(f"  refused {r['id'][:12]}: {r['reason']}")
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------- status


def build_status(ctx: Context, plan: dict, counts: dict, jobs_active: list[dict], jobs_done: list[dict]) -> dict:
    cfg, now = ctx.cfg, ctx.now
    out: dict = {"kind": cfg.kind, "queue": counts, "warnings": list(plan["warnings"])}
    out["plannedHours"] = plan["pool"]["plannedHours"]
    # Spent is an ESTIMATE: done jobs since the window start, rounded per model as a session would be.
    reset = ctx.balance.reset_ts
    start = (reset - cfg.window_hours * 3600) if reset and reset > now else now - cfg.window_hours * 3600
    per_model: dict[str, float] = {}
    for job in jobs_done:
        ended = _num(job.get("ended_at"))
        if ended is None or ended < start:
            continue
        head = parse_header(job.get("description")) or {}
        model = job.get("claimed_model") or (_job_models(job) or [None])[0]
        est = _num(head.get("estimate-minutes")) or cfg.minutes_for(head.get("class"))
        per_model[str(model)] = per_model.get(str(model), 0.0) + est
    spent = 0.0
    spent_known = True
    for model, minutes in per_model.items():
        mult = ctx.models.multiplier(model)
        if mult is None:
            spent_known = False
            continue
        spent += max(cfg.session_minimum_hours, float(_ceil(minutes / 60.0))) * mult
    out["spentHoursEstimated"] = spent if spent_known else None
    out["spentNote"] = "estimate: done jobs since the window start, one session per model"
    updated = ctx.balance.updated_ts
    age = None if updated is None else max(0.0, (now - updated) / 60.0)
    out["snapshotAgeMinutes"] = age
    out["snapshotStale"] = age is None or age > cfg.stale_snapshot_minutes
    if age is None:
        out["warnings"].append("no balance snapshot age: the snapshot is missing or has no updatedAt")
    stuck = []
    for job in jobs_active:
        lease = _num(job.get("claim_lease_expires_at"))
        if lease is None:
            claimed = _num(job.get("claimed_at"))
            lease = None if claimed is None else claimed + cfg.lease_seconds
        if lease is not None and now > lease:
            stuck.append(
                {"id": str(job.get("id")), "status": job.get("status"), "overLeaseMinutes": (now - lease) / 60.0}
            )
    out["stuckClaimed"] = stuck
    return out


def render_status(st: dict) -> str:
    lines = [f"offload status for kind {st['kind']}", "queue:"]
    for state, n in sorted(st["queue"].items()):
        lines.append(f"  {state}: {n}")
    lines.append(f"planned today: {_fmt_h(st['plannedHours'])}")
    lines.append(f"spent today (estimate): {_fmt_h(st['spentHoursEstimated'])}")
    age = st["snapshotAgeMinutes"]
    lines.append("balance snapshot age: " + ("unknown" if age is None else f"{age:.0f} min"))
    if st["snapshotStale"]:
        lines.append("warning: balance snapshot is older than the limit or has no timestamp")
    lines.append(f"stuck claimed beyond the lease: {len(st['stuckClaimed'])}")
    for s in st["stuckClaimed"]:
        lines.append(f"  {s['id'][:12]} {s['status']} {s['overLeaseMinutes']:.0f} min over")
    ev = st["evergreen"]
    lines.append(f"evergreen backlog: {ev['total']} item(s), {ev['usable']} usable")
    for w in st["warnings"]:
        lines.append(f"warning: {w}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------- evergreen

_EVERGREEN_KEYS = ("title", "repo", "class", "estimateMinutes", "check", "branch", "gates", "model", "brief")


def load_evergreen(path: str | None) -> list[dict]:
    """Backlog items from JSON (a list or ``{"items": [...]}``) or the plain-text form.

    Plain text: items separated by a line ``---``; ``key: value`` lines; the
    ``brief:`` value runs over the following lines until the next key.
    """
    if not path:
        return []
    text = Path(path).read_text()
    stripped = text.lstrip()
    if stripped.startswith(("[", "{")):
        raw = json.loads(text)
        rows = raw.get("items") if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            raise ValueError("evergreen JSON must be a list or an object with an items list")
        return [r for r in rows if isinstance(r, dict)]
    items = []
    key_re = re.compile(r"^(" + "|".join(_EVERGREEN_KEYS) + r")\s*:\s?(.*)$")
    for chunk in re.split(r"(?m)^---\s*$", text):
        item: dict = {}
        current = None
        for line in chunk.splitlines():
            m = key_re.match(line)
            if m and (current is None or current != "brief" or m.group(1) != "brief"):
                current = m.group(1)
                item[current] = m.group(2).strip()
            elif current == "brief":
                item["brief"] += "\n" + line
        if item:
            if isinstance(item.get("brief"), str):
                item["brief"] = item["brief"].strip()
            items.append(item)
    return items


def evergreen_task(cfg: Config, item: dict, now: float) -> dict:
    """The submit task for a backlog item. The branch is derived when the item names none."""
    title = str(item.get("title") or "").strip()
    day = datetime.fromtimestamp(now, UTC).strftime("%Y%m%d")
    branch = str(item.get("branch") or "").strip() or f"{cfg.branch_prefix}{_slug(title)}-{day}"
    parts = [str(item.get("brief") or "").strip()]
    if item.get("check"):
        parts.append(f"check: {str(item['check']).strip()}")
    parts.append(f"branch: {branch}")
    gates = item.get("gates")
    if gates:
        parts.append("gates: " + (", ".join(gates) if isinstance(gates, list) else str(gates)))
    task = {
        "title": title,
        "repo": str(item.get("repo") or "").strip(),
        "class": str(item.get("class") or "").strip(),
        "brief": "\n".join(p for p in parts if p),
    }
    if item.get("estimateMinutes") is not None:
        task["estimateMinutes"] = item["estimateMinutes"]
    if item.get("model"):
        task["model"] = item["model"]
    return task


def evergreen_state(ctx: Context) -> tuple[list[dict], list[Item], int]:
    """(raw items, usable virtual plan items, number skipped because repo/class/validation)."""
    rows = load_evergreen(ctx.cfg.evergreen_file)
    usable: list[Item] = []
    skipped = 0
    default_id = ctx.models.default_model(ctx.cfg.default_multiplier)
    for n, row in enumerate(rows):
        task = evergreen_task(ctx.cfg, row, ctx.now)
        check = validate_task(ctx.cfg, ctx.models, task)
        if not check["ok"]:
            skipped += 1
            continue
        model = check["model"] or default_id
        usable.append(
            Item(
                id=f"evergreen:{n}",
                title=task["title"] or f"item {n + 1}",
                cls=task["class"],
                model=model,
                multiplier=check["multiplier"],
                minutes=_num(task.get("estimateMinutes")) or ctx.cfg.minutes_for(task["class"]),
                priority=0,
                queued_at=ctx.now + n,
                index=n,
                virtual=True,
            )
        )
    return rows, usable, skipped


def evergreen_pull(ctx: Context, count: int, *, dry_run: bool) -> dict:
    rows = load_evergreen(ctx.cfg.evergreen_file)
    out: dict = {"requested": count, "dryRun": dry_run, "submitted": [], "existing": [], "skipped": []}
    for row in rows:
        if len(out["submitted"]) >= count:
            break
        task = evergreen_task(ctx.cfg, row, ctx.now)
        label = task["title"] or "(untitled)"
        if not ctx.cfg.repo_allowed(task["repo"]):
            out["skipped"].append({"title": label, "reasons": [REASON_REPO]})
            continue
        res = submit_task(ctx, task, dry_run=dry_run)
        if not res["ok"]:
            out["skipped"].append({"title": label, "reasons": res["refused"]})
        elif res.get("coalesced"):
            out["existing"].append({"title": label, "id": res["id"]})
        else:
            out["submitted"].append({"title": label, "id": res.get("id"), "priority": res["payload"]["priority"]})
    return out


# -------------------------------------------------------------------- CLI


def _emit(obj, as_json: bool, text: str, stdout) -> None:
    stdout.write((json.dumps(obj, indent=2, sort_keys=True) + "\n") if as_json else text)


def _parse_now(value: str | None) -> float:
    if not value:
        return time.time()
    ts = parse_ts(value)
    if ts is None:
        raise ConfigError(f"--now {value!r} is not an epoch or ISO-8601 time")
    return ts


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="config file (default: $OFFLOAD_CONFIG)")
    common.add_argument("--balance", help="balance snapshot (default: $OFFLOAD_BALANCE_FILE)")
    common.add_argument("--models", help="models snapshot (default: $OFFLOAD_MODELS_FILE)")
    common.add_argument("--jobs", help="offline: JSON file with the jobs instead of reading the bus")
    common.add_argument("--now", help="override the clock (epoch or ISO-8601), for reproducible plans")
    common.add_argument("--json", action="store_true", help="machine-readable output")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("submit", parents=[common], help="create ONE job for ONE task")
    p.add_argument("--task", required=True, help="JSON task file ('-' reads stdin)")
    p.add_argument("--dry-run", action="store_true")
    sub.add_parser("plan", parents=[common], help="plan the current daily window")
    p = sub.add_parser("release", parents=[common], help="assign priority bands to the queued jobs")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--job-id", action="append", help="restrict to this job id (repeatable)")
    sub.add_parser("status", parents=[common], help="queue depth, spend, snapshot age, stuck claims")
    p = sub.add_parser("evergreen", help="the standing backlog")
    esub = p.add_subparsers(dest="evergreen_command", required=True)
    esub.add_parser("list", parents=[common])
    q = esub.add_parser("pull", parents=[common])
    q.add_argument("count", type=int)
    q.add_argument("--dry-run", action="store_true")
    return parser


def main(
    argv: list[str] | None = None,
    *,
    env: dict | None = None,
    transport: Transport | None = None,
    stdout=None,
    stderr=None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    env = os.environ if env is None else env
    args = build_parser().parse_args(argv)
    try:
        cfg = load_config(args.config or env.get("OFFLOAD_CONFIG"))
        now = _parse_now(args.now)
        balance = load_balance(args.balance or env.get("OFFLOAD_BALANCE_FILE"))
        models = load_models(args.models or env.get("OFFLOAD_MODELS_FILE"))
        base = (env.get("HIVE_URL") or "").strip()
        if transport is None and base and not args.jobs:
            transport = http_transport(base)
        ctx = Context(
            cfg=cfg,
            models=models,
            balance=balance,
            transport=transport,
            jobs_file=args.jobs,
            now=now,
            actor=(env.get("HIVE_ACTOR_URN") or "").strip() or None,
        )
        return _dispatch(args, ctx, stdout, stderr)
    except (ConfigError, BusError, ValueError, OSError) as exc:
        stderr.write(f"offload_queue: {exc}\n")
        return 2


def _dispatch(args, ctx: Context, stdout, stderr) -> int:
    cfg = ctx.cfg
    if args.command == "submit":
        raw = sys.stdin.read() if args.task == "-" else Path(args.task).read_text()
        task = json.loads(raw)
        if not isinstance(task, dict):
            raise ValueError("the task file must be a JSON object describing one task")
        res = submit_task(ctx, task, dry_run=args.dry_run)
        if args.json:
            stdout.write(json.dumps(res, indent=2, sort_keys=True) + "\n")
        elif not res["ok"]:
            stdout.write("REFUSED: " + "; ".join(res["refused"]) + "\n")
            for d in res["details"]:
                stdout.write(f"  detail: {d}\n")
        elif args.dry_run:
            stdout.write(json.dumps(res["payload"], indent=2, sort_keys=True) + "\n")
            stdout.write("# dry run: nothing sent\n")
        else:
            verb = "existing job" if res.get("coalesced") else "submitted job"
            stdout.write(f"{verb} {res['id']}\n")
        for w in res.get("warnings", []):
            stderr.write(f"warning: {w}\n")
        return 0 if res["ok"] else 3
    if args.command == "plan":
        _, usable, skipped = evergreen_state(ctx) if cfg.evergreen_file else ([], [], 0)
        jobs, warns = ctx.queue()
        plan = build_plan(
            cfg,
            ctx.models,
            ctx.balance,
            jobs,
            now=ctx.now,
            evergreen=usable,
            evergreen_blocked=skipped,
            extra_warnings=warns,
        )
        _emit(plan, args.json, render_plan(plan), stdout)
        return 0
    if args.command == "release":
        res = release(ctx, dry_run=args.dry_run, only_ids=args.job_id)
        _emit(res, args.json, render_release(res), stdout)
        return 0 if res["ok"] else 4
    if args.command == "status":
        return _status(args, ctx, stdout)
    if args.command == "evergreen":
        if args.evergreen_command == "list":
            rows, usable, skipped = evergreen_state(ctx)
            obj = {"total": len(rows), "usable": len(usable), "skipped": skipped, "items": rows}
            text = f"evergreen backlog: {len(rows)} item(s), {len(usable)} usable, {skipped} skipped\n"
            for r in rows:
                text += f"  {r.get('class', '?')}: {r.get('title', '(untitled)')} [{r.get('repo', '?')}]\n"
            _emit(obj, args.json, text, stdout)
            return 0
        res = evergreen_pull(ctx, args.count, dry_run=args.dry_run)
        text = (
            f"pull {res['requested']} ({'dry run' if res['dryRun'] else 'live'}): {len(res['submitted'])} submitted\n"
        )
        for s in res["submitted"]:
            text += f"  + {s['title']}\n"
        for s in res["existing"]:
            text += f"  = {s['title']} (already queued as {s['id']})\n"
        for s in res["skipped"]:
            text += f"  - {s['title']}: {'; '.join(s['reasons'])}\n"
        _emit(res, args.json, text, stdout)
        return 0
    return 2


def _status(args, ctx: Context, stdout) -> int:
    cfg = ctx.cfg
    if ctx.jobs_file:
        rows = load_jobs_file(ctx.jobs_file, cfg.kind)
        warns: list[str] = []
    else:
        if ctx.transport is None:
            raise BusError("HIVE_URL is not set (the bus URL comes from the environment)")
        rows, warns = fetch_jobs(
            ctx.transport, ("queued", "offered", "claimed", "running", "needs-review", "failed", "done"), cfg.kind
        )
    counts: dict[str, int] = {}
    for j in rows:
        counts[str(j.get("status") or "?")] = counts.get(str(j.get("status") or "?"), 0) + 1
    queued = [j for j in rows if j.get("status") == "queued"]
    ev_rows, usable, skipped = evergreen_state(ctx) if cfg.evergreen_file else ([], [], 0)
    plan = build_plan(
        cfg,
        ctx.models,
        ctx.balance,
        queued,
        now=ctx.now,
        evergreen=usable,
        evergreen_blocked=skipped,
        extra_warnings=warns,
    )
    st = build_status(
        ctx,
        plan,
        counts,
        [j for j in rows if j.get("status") in ("claimed", "running")],
        [j for j in rows if j.get("status") == "done"],
    )
    st["evergreen"] = {"total": len(ev_rows), "usable": len(usable), "skipped": skipped}
    _emit(st, args.json, render_status(st), stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
