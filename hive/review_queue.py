"""Review queue with a cross-family reviewer roster (operator rule 2026-10-31).

Always have an alternate review protocol or a review queue; add every local
and provider model to the roster; ALWAYS pair reviewer and author from
different model families.

Today a loop whose reviewer endpoint is busy ends ``reviewer_unavailable`` and
fails (all 8 slots of the CERBERUS gpt-oss-20b endpoint were busy and about 20
loops failed in one night). This module owns the pieces of the fix that are
pure data and pure functions, so the worker (``deploy/hive_worker.py``) and the
doctor (``hive/workers/doctor.py``) share ONE definition of each:

* **the model-family table** -- ``hive/core/model-families.json``, one source
  of truth; ``family_of`` answers for a model id, a role id (through the fleet
  role map) or an agent alias. An unknown model is its own family and matches
  nothing;
* **the reviewer roster** -- ``hive/core/reviewer-roster.json``, one row per
  reviewer-capable route (every local route and every provider model) with
  family, location, concurrency limit, cost class and flags. MiMo is never a
  reviewer; StepFun and Meta are contributor tiers for operator-approved repos
  only; the EIH Nemotron route is allowed but slow and sorts last;
* **the pairing function** -- ``pick_reviewers`` returns an ordered list of
  routes whose family differs from the author's (and every known co-author's),
  skipping unhealthy or saturated routes, with a deterministic tie-break.
  Empty is a valid answer: the job is PARKED, never self-reviewed;
* **route load** -- ``route_load_fn`` reads llama-server ``/slots`` (or vLLM
  metrics) for local routes, cached for 30 s and cheap;
* **the row state** -- ``review_state``/``make_review_state`` are the
  ``result.review_state`` block a ``needs-review`` row carries while it waits.
  There is NO separate review queue, table or status: the review queue IS the
  triage queue (the worker's ``needs-review`` rows the doctor's needs-review
  sweep already reads), and ``review_state='awaiting_review'`` says where a row
  is inside it.

Pure functions and stdlib only (the family table is read through the same
JSON loader on the worker and the bus lines); no I/O beyond the two data
files, the optional role map and the optional load probe.
"""

from __future__ import annotations

import fnmatch
import functools
import json
import math
import os
import re
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parent / "core"
FAMILY_TABLE_PATH = DATA_DIR / "model-families.json"
ROSTER_PATH = DATA_DIR / "reviewer-roster.json"

#: ``result.review_state.state`` of a row waiting for a cross-family review.
AWAITING_REVIEW = "awaiting_review"
#: How many review attempts a row gets before it waits for an owner.
MAX_REVIEW_ATTEMPTS = 3
#: First backoff step; each further attempt doubles it (REVIEW_BACKOFF*2**n).
REVIEW_BACKOFF_SEC = 900.0
#: Delays beyond two hours are pointless: the row is effectively owner-waiting.
MAX_BACKOFF_SEC = 7200.0
#: Load probes are cached this long (operator: "cheap, cached for 30 s").
LOAD_CACHE_SEC = 30.0

#: Repos the StepFun and Meta contributor tiers may review (operator list).
APPROVED_REPOS = ("graeae-hive", "fleet-ops", "tokenomics", "ncz-screensavers", "zoder")

#: Codex (operator 2026-10-31): a reviewer ROUTE served by the confined local
#: Hive Codex worker. Its roster row carries the ladder and the sandbox; the
#: gate below (worker online + config switch + plan usage + cooldown marker)
#: decides whether a sweep may use it at all. Nothing here logs in, refreshes
#: or reads the OAuth token: PYTHIA holds the master token, full stop.
CODEX_ROUTE_ID = "codex-worker"
CODEX_WORKER_KIND = "codex"
#: ``HIVE_REVIEW_CODEX_ENABLED``: unset means ON, but only while the codex
#: worker is online (and the usage/quota guard passes); 0/false/no/off is off.
CODEX_ENABLED_ENV = "HIVE_REVIEW_CODEX_ENABLED"
#: The plan-usage guard: skip Codex when less than this share remains.
CODEX_MIN_REMAINING_PCT = 30.0
#: Cooldown marker the worker writes when Codex reports a usage limit.
CODEX_COOLDOWN_PROVIDER = "codex"
CODEX_COOLDOWN_SECS = 6 * 3600.0
#: Review lanes: bulk (default) and hard (architecture or security-sensitive).
LANE_BULK = "bulk"
LANE_HARD = "hard"

#: Returned by a load probe when the route's load cannot be read (unknown).
LOAD_UNKNOWN = object()

UNKNOWN_FAMILY_PREFIX = "model:"

#: Families this fleet may never review with, whatever a roster says: the
#: routing rule forbids Anthropic-backed models and the reviewer can never be
#: the author's own family. Kept in step with the worker's forbidden-route rule.
FORBIDDEN_FAMILIES = frozenset({"anthropic"})
#: MiMo has no reviewer placement (operator 2026-10-26); belt and braces, the
#: model's family is refused even if a roster row were added by mistake.
NEVER_REVIEW_FAMILIES = frozenset({"xiaomi"})

_EXACT_CACHE: dict = {"key": None, "values": {}, "patterns": (), "lower": {}, "families": frozenset()}
_ROSTER_CACHE: dict = {"key": None, "routes": ()}
_ALIAS_CACHE: dict = {"key": None, "aliases": {}}
_SEED_LOCK = threading.Lock()

_ALIAS_RE = re.compile(r"^[a-z0-9]+([._-][a-z0-9]+)*$")
_ALIAS_STOPWORDS = frozenset(
    {"auto", "default", "local", "provider", "eih", "unknown", "none", "null", "reviewer", "coder"}
)


# ---------------------------------------------------------------------------
# The family table.
# ---------------------------------------------------------------------------


def _read_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path.name}: expected a JSON object")
    return data


def _load_exact(path: Path = FAMILY_TABLE_PATH) -> tuple[dict, tuple]:
    """The exact table plus its (pattern, family) list; refuses duplicate claims.

    One model may be claimed by many patterns of the SAME family (``gpt-oss*``
    and ``gpt-6*`` are both OpenAI); a model claimed by patterns of DIFFERENT
    families would make the answer order-dependent, so the loader refuses it.
    """
    try:
        st = path.stat()
        key = (st.st_mtime_ns, st.st_size)
    except OSError:
        return {}, ()
    if _EXACT_CACHE["key"] == key:
        return _EXACT_CACHE["values"], _EXACT_CACHE["patterns"]
    data = _read_json(path)
    values: dict[str, dict] = {}
    pattern_owner: dict[str, str] = {}
    for name, entry in (data.get("exact") or {}).items():
        if not isinstance(entry, dict) or not str(entry.get("family") or "").strip():
            raise ValueError(f"family table: {name} has no family")
        family = str(entry["family"]).strip().lower()
        patterns = [str(p).strip().lower() for p in (entry.get("patterns") or []) if str(p).strip()]
        if not patterns:
            raise ValueError(f"family table: {name} has no patterns")
        for pattern in patterns:
            owner = pattern_owner.get(pattern)
            if owner is not None and owner != family:
                raise ValueError(f"family table: pattern {pattern} claimed by {owner} and {family}")
            pattern_owner[pattern] = family
        values[name] = {"family": family, "patterns": patterns}
    patterns = tuple(sorted(pattern_owner.items(), key=lambda item: len(item[0]), reverse=True))
    lower = {name.lower(): entry["family"] for name, entry in values.items()}
    families = frozenset(entry["family"] for entry in values.values())
    with _SEED_LOCK:
        _EXACT_CACHE.update(key=key, values=values, patterns=patterns, lower=lower, families=families)
    return values, patterns


def load_family_table() -> dict:
    """The whole family table as data (``{"schema_version", "exact"}``)."""
    values, _ = _load_exact()
    return {"schema_version": 1, "exact": values}


def _table_family(name: str) -> str | None:
    """The table's family for ``name`` (case-insensitive), or None: not listed."""
    _load_exact()
    low = str(name or "").strip().lower()
    if not low:
        return None
    hit = _EXACT_CACHE["lower"].get(low)
    if hit:
        return hit
    for needle, family in _EXACT_CACHE["patterns"]:
        if fnmatch.fnmatchcase(low, needle):
            return family
    return None


def _roster_aliases(path: Path = ROSTER_PATH) -> dict:
    """``{lower-cased route id / role id / unique provider id: model id}``.

    Read straight from the roster file (not ``load_roster``, which validates
    each route through ``family_of`` and would recurse). A row may name the
    route it ran on (``tydeus-qwen38``) or the fleet role id it was served
    under (``cerberus-reviewer``) instead of the model: both are the route's
    model, and therefore its family.
    """
    try:
        st = path.stat()
        key = (st.st_mtime_ns, st.st_size)
    except OSError:
        return {}
    if _ALIAS_CACHE["key"] == key:
        return _ALIAS_CACHE["aliases"]
    try:
        routes = _read_json(path).get("routes") or []
    except (OSError, ValueError):
        return {}
    aliases: dict[str, str] = {}
    providers: dict[str, set] = {}
    for route in routes:
        if not isinstance(route, dict) or not route.get("model_id"):
            continue
        model = str(route["model_id"])
        for name in [route.get("id"), route.get("role_id"), *(route.get("roles") or [])]:
            if str(name or "").strip():
                aliases[str(name).strip().lower()] = model
        if str(route.get("provider_id") or "").strip():
            providers.setdefault(str(route["provider_id"]).strip().lower(), set()).add(model)
    for provider, models in providers.items():
        # ``local`` backs two CERBERUS models: a shared provider id names no model.
        if len(models) == 1:
            aliases.setdefault(provider, next(iter(models)))
    with _SEED_LOCK:
        _ALIAS_CACHE.update(key=key, aliases=aliases)
    return aliases


@functools.lru_cache(maxsize=2048)
def _family_of_cached(raw: str) -> str:
    # 1. the table itself (model ids, any letter case, any listed pattern);
    family = _table_family(raw)
    if family:
        return family
    # 2. a roster route id or role id -> the route's model;
    model = _roster_aliases().get(raw.lower())
    if model:
        family = _table_family(model)
        if family:
            return family
    # 3. the deployed role map / agent aliases, when importable.
    family = _resolve_role_or_alias(raw)
    if family:
        return family
    return UNKNOWN_FAMILY_PREFIX + raw.lower()


def family_of(model: str) -> str:
    """The operator's family for ``model``, or its own family when unknown.

    Accepts a model id (``gpt-oss-20b``, any letter case), a roster route id or
    fleet role id (``tydeus-qwen38``, ``cerberus-reviewer``) or an agent alias
    (``minimax_flash``). The table is asked first, so a known name never
    depends on which optional resolver modules happen to be importable; role
    ids then resolve through the roster, and only then through the deployed
    llm-roles map and ``provenance`` aliases. An unknown model returns
    ``model:<lower-cased id>`` -- never a guessed family, so it can never pair
    with anything but itself.
    """
    raw = str(model or "").strip()
    if not raw:
        return ""
    return _family_of_cached(raw)


def _resolve_role_or_alias(raw: str) -> str | None:
    """A family through the deployed role map or agent aliases; None: no answer."""
    try:
        from hive import llm_roles

        if llm_roles.is_role_id(raw):
            backing = llm_roles.backing_model(raw)
            family = _table_family(backing) if backing else None
            if family:
                return family
            mapped = str(llm_roles.family(raw) or "").strip().lower()
            if mapped:
                # The map knows the family but not a model this table lists.
                mapped = {"gemini": "gemma"}.get(mapped, mapped)
                family = _table_family(mapped)
                if family:
                    return family
                if mapped in _EXACT_CACHE["families"]:
                    return mapped
    except Exception:  # noqa: BLE001 -- the map is optional
        pass
    if _ALIAS_RE.match(raw.lower()) and raw.lower() not in _ALIAS_STOPWORDS:
        try:
            import provenance

            resolved = provenance.primary_model(raw)
            family = _table_family(str(resolved)) if resolved else None
            if family:
                return family
        except Exception:  # noqa: BLE001 -- provenance ships on the worker only
            pass
    return None


# ---------------------------------------------------------------------------
# The reviewer roster.
# ---------------------------------------------------------------------------


def load_roster(path: Path = ROSTER_PATH) -> list[dict]:
    """Every reviewer-capable route, in file order (a copied, frozen list).

    Each entry is validated once: required fields, a route id unique in the
    file, and a family that agrees with the family table. A roster that
    disagrees with the table is a configuration bug, not something to paper
    over at pick time.
    """
    try:
        st = path.stat()
        key = (st.st_mtime_ns, st.st_size)
    except OSError:
        return []
    if _ROSTER_CACHE["key"] == key:
        return list(_ROSTER_CACHE["routes"])
    data = _read_json(path)
    routes: list[dict] = []
    seen: set[str] = set()
    for raw in data.get("routes") or []:
        if not isinstance(raw, dict):
            raise ValueError("roster: route entries must be objects")
        entry = dict(raw)
        for field in ("model_id", "provider_id", "family", "location", "concurrency", "cost_class"):
            if field not in entry:
                raise ValueError(f"roster: {entry.get('id')} is missing {field}")
        route_id = str(entry.get("id") or entry["model_id"])
        if route_id in seen:
            raise ValueError(f"roster: duplicate route {route_id}")
        seen.add(route_id)
        entry["id"] = route_id
        entry["flags"] = dict(entry.get("flags") or {})
        expected = family_of(entry["model_id"])
        if entry["family"] != expected:
            raise ValueError(f"roster: {route_id} family {entry['family']} != table {expected}")
        routes.append(entry)
    with _SEED_LOCK:
        _ROSTER_CACHE.update(key=key, routes=tuple(routes))
    return list(routes)


def contributors(repo: str | None) -> list[str]:
    """Contributor-tier routes allowed on ``repo`` (operator-approved repos only)."""
    name = str(repo or "").strip()
    allowed = []
    for entry in load_roster():
        tier = entry["flags"].get("contributor_tier")
        if not isinstance(tier, dict):
            continue
        if name and name in (tier.get("repos") or []):
            allowed.append(entry["id"])
    return allowed


# ---------------------------------------------------------------------------
# Route load (cheap, cached 30 s).
# ---------------------------------------------------------------------------


class _Probe:
    """Per-route load cache: {route id: (expires_at, value)}."""

    def __init__(self, urlopen, clock):
        self.urlopen = urlopen
        self.clock = clock
        self.cache: dict[str, tuple[float, Any]] = {}

    def __call__(self, route: dict) -> Any:
        if is_codex_route(route):
            # A worker-driven route serves NO slot endpoint: its load is the
            # number of codex review jobs the worker is running, which the
            # caller adds from the in-flight count (the worker's own claim gate
            # enforces the concurrency cap of 1). Probing it would return
            # LOAD_UNKNOWN and silently retire the route from every pick.
            return 0
        if str(route.get("location")) != "local":
            # Provider routes expose no slot endpoint; there is nothing cheap
            # to read and the concurrency cap is enforced at dispatch time.
            return 0
        route_id = str(route.get("id") or route.get("model_id"))
        now = self.clock()
        hit = self.cache.get(route_id)
        if hit and hit[0] > now:
            return hit[1]
        value = self._fetch(route)
        self.cache[route_id] = (now + LOAD_CACHE_SEC, value)
        return value

    def _fetch(self, route: dict) -> Any:
        base = _server_root(route)
        if not base:
            return LOAD_UNKNOWN
        # The server kind is declared on the route, so a llama-server is never
        # probed with a vLLM metrics request (and vice versa): one request per
        # probe, and a 404 body cannot be mistaken for load.
        try:
            if str(route.get("server") or "") == "vllm":
                return _vllm_load(self._fetch_json(base + "/metrics"))
            slots = self._fetch_json(base + "/slots")
        except Exception:  # noqa: BLE001 -- any probe failure is "unknown"
            return LOAD_UNKNOWN
        if isinstance(slots, dict):
            slots = slots.get("slots")
        if not isinstance(slots, list):
            return LOAD_UNKNOWN
        busy = sum(1 for slot in slots if isinstance(slot, dict) and slot.get("is_processing"))
        return busy

    def _fetch_json(self, url: str):
        with self.urlopen(url, timeout=2) as resp:  # noqa: S310 -- operator-configured base
            body = resp.read(1 << 20)
        text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
        try:
            return json.loads(text)
        except ValueError:
            return text


def _server_root(route: dict) -> str:
    """``http://<inventory address>:<port>`` for a local route; ``""``: unknown.

    llama-server's ``/slots`` and vLLM's ``/metrics`` live at the server ROOT,
    not under the OpenAI ``/v1`` prefix. The address comes from the operator's
    fleet inventory (``HIVE_FLEET_INVENTORY``) by the route's ``host`` name --
    never from the roster, never the bare hostname, never a guess.
    """
    try:
        port = int(route.get("port"))
    except (TypeError, ValueError):
        return ""
    if not 0 < port < 65536:
        return ""
    try:
        from hive.core import fleet_inventory

        addr = fleet_inventory.address(route.get("host"))
    except Exception:  # noqa: BLE001 -- no inventory: the load is unknown
        return ""
    if not addr:
        return ""
    if ":" in addr and not addr.startswith("["):
        addr = f"[{addr}]"  # IPv6 literal
    return f"http://{addr}:{port}"


def _vllm_load(text: str) -> Any:
    """Running+waiting requests from vLLM's Prometheus text (None: unparsable)."""
    running = waiting = None
    for line in text.splitlines():
        if line.startswith("vllm:num_requests_running"):
            running = _metric_value(line)
        elif line.startswith("vllm:num_requests_waiting"):
            waiting = _metric_value(line)
    if running is None:
        return LOAD_UNKNOWN
    return running + (waiting or 0)


def _metric_value(line: str) -> float | None:
    try:
        return float(line.rsplit(None, 1)[-1])
    except (IndexError, ValueError):
        return None


def route_load_fn(urlopen=None, clock=None):
    """A cached per-route load reader (``urlopen``/``clock`` injectable for tests)."""
    return _Probe(urlopen or urllib.request.urlopen, clock or time.time)


# ---------------------------------------------------------------------------
# Pairing.
# ---------------------------------------------------------------------------


def _available(entry: dict, health, load) -> bool:
    """Healthy and not saturated (unknown health is unhealthy: fail closed)."""
    try:
        if not health(entry):
            return False
    except Exception:  # noqa: BLE001 -- a broken check is not proof of health
        return False
    try:
        current = load(entry)
    except Exception:  # noqa: BLE001 -- unreadable load is saturated
        return False
    if current is None or current is LOAD_UNKNOWN:
        return False
    limit = int(entry.get("concurrency") or 1)
    return float(current) < limit


# ---------------------------------------------------------------------------
# Codex (operator 2026-10-31: "codex needs to be part of our review system").
#
# Codex is a reviewer route like any other -- family openai -- served ONLY by
# the confined local Hive Codex worker (kind codex, workspace-write limited to
# the job workspace, CERBERUS ``hive-worker-codex.service``). It is never an
# EIH route, never an Azure/AWS/GCP/OCI route and never the retired :4100
# gateway. PYTHIA holds the master OAuth token; nothing in this module logs
# in, refreshes or reads it -- the worker that owns the token runs the job.
#
# The gate every automatic dispatch goes through:
#
#   1. the config switch ``HIVE_REVIEW_CODEX_ENABLED`` (unset = on, but only
#      while the worker is online; 0/false/no/off = off);
#   2. the codex worker must be ONLINE (a review job of kind codex is only
#      claimable by it -- an offline worker would strand the review);
#   3. the quota guard: the plan usage must be readable and at least 30 % of
#      the share must remain, and no cooldown marker may be live (the worker
#      writes one when Codex reports a usage limit);
#   4. the concurrency cap of 1 the roster row carries.
#
# Priority: BELOW local and DeepSeek/M3 reviewers for bulk review, ABOVE
# everything for the hard lane (architecture or security-sensitive changes).
# ---------------------------------------------------------------------------


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() not in ("0", "false", "no", "off")


def _codex_flags(entry: dict | None) -> dict:
    """The ``flags.codex`` block of a roster entry ({} when absent)."""
    if not isinstance(entry, dict):
        return {}
    flags = entry.get("flags")
    block = flags.get("codex") if isinstance(flags, dict) else None
    return block if isinstance(block, dict) else {}


def is_codex_route(entry: dict | None) -> bool:
    """True for the Codex reviewer route: the KIND is codex and it is local."""
    if not isinstance(entry, dict):
        return False
    return str(entry.get("kind") or "").lower() == CODEX_WORKER_KIND or entry.get("id") == CODEX_ROUTE_ID


def codex_ladder(entry: dict | None = None) -> list[str]:
    """The automatic model ladder: gpt-6-luna, then gpt-6.1-terra, then gpt-6-sol.

    ``gpt-6-astra`` is NOT on it: it is reachable only through an explicit
    operator authorization recorded in the job (``codex_job_allowed``).
    """
    entry = entry if entry is not None else next((e for e in load_roster() if is_codex_route(e)), None)
    ladder = [str(m).strip() for m in (_codex_flags(entry).get("ladder") or []) if str(m).strip()]
    return ladder or ["gpt-6-luna", "gpt-6.1-terra", "gpt-6-sol"]


def codex_job_allowed(entry: dict | None, model: str, *, job: dict | None = None) -> bool:
    """Whether a codex review job may run ``model`` for ``job``.

    A ladder model is always allowed; the authorization model (gpt-6-astra) is
    allowed ONLY when the job itself records the operator's authorization
    (``codex_model_authorization`` naming it). Anything else is refused.
    """
    wanted = str(model or "").strip().lower()
    if not wanted:
        return False
    if wanted in {m.lower() for m in codex_ladder(entry)}:
        return True
    authorized = str(_codex_flags(entry).get("authorization_model") or "").strip().lower()
    if not authorized or wanted != authorized:
        return False
    recorded = str((job or {}).get("codex_model_authorization") or "").strip().lower()
    return recorded == authorized


def codex_review_available(entry: dict | None = None, *, usage: Any = None) -> dict:
    """The Codex model a READY route would run: ``{"model", "ladder"}``.

    Never returns the authorization-only model: an automatic pick is always the
    first ladder rung (``gpt-6-luna``).
    """
    ladder = codex_ladder(entry)
    return {"model": ladder[0], "ladder": ladder}


def codex_cooldown_path(state_dir: str | None = None) -> Path:
    """Where the Codex usage-limit marker lives (worker state dir, or XDG)."""
    root = str(state_dir or "").strip()
    if not root:
        root = os.environ.get("HIVE_WORKER_STATE_DIR", "").strip() or os.path.join(
            os.environ.get("XDG_STATE_HOME", "").strip() or os.path.expanduser("~/.local/state"), "hive-worker"
        )
    return Path(os.path.expanduser(root)) / "cooldowns" / f"{CODEX_COOLDOWN_PROVIDER}.json"


def read_codex_cooldown(*, now: float | None = None, state_dir: str | None = None) -> dict | None:
    """The LIVE Codex cooldown marker, else None (expired/corrupt markers are inert)."""
    path = codex_cooldown_path(state_dir)
    try:
        marker = json.loads(path.read_text(encoding="utf-8"))
        expires_at = float(marker["expires_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if float(time.time() if now is None else now) >= expires_at:
        return None
    return marker


def write_codex_cooldown(
    *,
    reason: str = "",
    model: str = "",
    ttl_secs: float = CODEX_COOLDOWN_SECS,
    now: float | None = None,
    state_dir: str | None = None,
) -> dict:
    """Write the Codex usage-limit marker the gate honours (never shortened)."""
    path = codex_cooldown_path(state_dir)
    now_epoch = float(time.time() if now is None else now)
    marker = {
        "provider": CODEX_COOLDOWN_PROVIDER,
        "model": str(model or ""),
        "created_at": now_epoch,
        "expires_at": now_epoch + float(ttl_secs),
        "reason": str(reason or "")[:500],
    }
    current = read_codex_cooldown(now=now_epoch, state_dir=state_dir)
    if current is not None:
        marker["created_at"] = min(float(current.get("created_at") or now_epoch), now_epoch)
        marker["expires_at"] = max(float(current["expires_at"]), marker["expires_at"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(marker), encoding="utf-8")
    os.replace(tmp, path)
    return marker


def codex_gate(
    *,
    entry: dict | None = None,
    worker_online: bool | None,
    env: dict | None = None,
    usage: Any = None,
    now: float | None = None,
    state_dir: str | None = None,
) -> dict:
    """Whether an automatic Codex review may be dispatched, and why not.

    ``worker_online`` is the doctor's live view of the codex kind: ``None``
    means the bus could not say (fail closed as ``codex_worker_unknown``),
    ``False`` means it said the worker is not online. ``usage`` is the injected plan
    usage source returning ``{"remaining_pct": float}`` (or None when it cannot
    be read -- an unknown share fails closed). Returns
    ``{"allowed": bool, "reason": str, "model": str|None}`` with a reason from
    ``codex_disabled``, ``codex_worker_unknown``, ``codex_worker_offline``,
    ``codex_usage_unknown``,
    ``codex_quota_low``, ``codex_cooldown`` or ``""`` (allowed).
    """
    entry = entry if entry is not None else next((e for e in load_roster() if is_codex_route(e)), None)
    env = os.environ if env is None else env
    flags = _codex_flags(entry)
    minimum = float(flags.get("min_remaining_pct") or CODEX_MIN_REMAINING_PCT)
    model = codex_review_available(entry)["model"]

    def answer(allowed: bool, reason: str) -> dict:
        return {"allowed": allowed, "reason": reason, "model": model if allowed else None}

    if not _truthy(env.get(CODEX_ENABLED_ENV, "1")):
        return answer(False, "codex_disabled")
    if worker_online is None:
        # The doctor cannot see the worker list at all (bus down / probe
        # failed). That is not the same fact as "the codex worker is offline",
        # and it must never be read as permission: fail closed, but say which.
        return answer(False, "codex_worker_unknown")
    if not worker_online:
        return answer(False, "codex_worker_offline")
    if read_codex_cooldown(now=now, state_dir=state_dir) is not None:
        return answer(False, "codex_cooldown")
    try:
        reading = usage() if callable(usage) else usage
    except Exception:  # noqa: BLE001 -- a broken probe is an unknown share
        reading = None
    remaining = reading.get("remaining_pct") if isinstance(reading, dict) else None
    # A quota share is a FINITE number or nothing: ``inf`` would otherwise pass
    # the >= minimum test and open the gate on a reading that means "no data".
    # ``finite_load`` alone is not enough here because it accepts an infinite
    # float, and it is shared with the load reader where that is a separate
    # question. A numeric string ("45") is accepted rather than treated as an
    # unreadable share; anything else is unknown and fails closed.
    if isinstance(remaining, str) and remaining.strip():
        try:
            remaining = float(remaining)
        except ValueError:
            remaining = None
    if not isinstance(remaining, (int, float)) or isinstance(remaining, bool) or not math.isfinite(remaining):
        return answer(False, "codex_usage_unknown")
    if float(remaining) < minimum:
        return answer(False, "codex_quota_low")
    return answer(True, "")


#: Path components (case-insensitive) that make a change SECURITY-SENSITIVE:
#: security, auth, redaction and budget code (operator 2026-10-31).
_HARD_PATH_RES = (
    re.compile(r"(^|[/_.-])(security|secrets?|auth|authn|authz|oauth)($|[/_.-])", re.IGNORECASE),
    re.compile(r"(^|[/_.-])redact", re.IGNORECASE),
    re.compile(r"(^|[/_.-])budget", re.IGNORECASE),
    re.compile(r"(^|[/_.-])architect", re.IGNORECASE),
)


def review_lane(*, repo: str | None = None, paths: list[str] | None = None, kind: str | None = None) -> str:
    """``hard`` for architecture or security-sensitive work, else ``bulk``.

    The hard lane is where Codex is the FIRST reviewer (authoring review of
    architecture or a security/auth/redaction/budget change); everything else
    is bulk review, where local and DeepSeek/M3 reviewers come first.
    """
    text = " ".join([str(kind or ""), *(str(p) for p in paths or [])])
    for pattern in _HARD_PATH_RES:
        if pattern.search(text):
            return LANE_HARD
    return LANE_BULK


def _reviewable(entry: dict, *, repo: str | None, author_routes: set[str]) -> bool:
    family = str(entry.get("family") or "")
    if family in FORBIDDEN_FAMILIES or family in NEVER_REVIEW_FAMILIES:
        return False
    if str(entry.get("model_id") or "").lower().startswith("mimo"):
        return False
    if entry["id"] in author_routes:
        return False
    # Same reason as ``_bulk_key``: a hand-built route may carry no flags block.
    flags = entry.get("flags") if isinstance(entry.get("flags"), dict) else {}
    tier = flags.get("contributor_tier")
    # A contributor tier is allowed only on the operator-approved repos.
    return not (isinstance(tier, dict) and str(repo or "").strip() not in (tier.get("repos") or []))


def _bulk_key(entry: dict, lane: str = LANE_BULK) -> tuple:
    """Deterministic tie-break for a review LANE.

    Bulk: roster priority first (slow routes last, and Codex, which is lower
    priority than local and DeepSeek/M3 for bulk review, sorts behind them),
    then local before provider before eih, then id. Hard: ``hard_lane_priority``
    first, so Codex leads the authoring review of hard-lane work.
    """
    # ``load_roster`` normalises ``flags`` on every entry, but the pick is also
    # reached with hand-built routes (test doubles, probe callers), so a route
    # without the block sorts by its defaults instead of raising KeyError.
    flags = entry.get("flags") if isinstance(entry.get("flags"), dict) else {}
    if lane == LANE_HARD:
        hard = int(flags.get("hard_lane_priority") or 0)
        return (-hard, -int(flags.get("priority") or 0), entry["id"])
    priority = int(flags.get("priority") or 0)
    return (
        -priority,
        {"local": 0, "provider": 1, "eih": 2}.get(str(entry.get("location")), 3),
        entry["id"],
    )


def pick_reviewers(
    author_model: str,
    roster: list[dict] | None = None,
    health=None,
    load=None,
    *,
    co_authors: list[str] | None = None,
    repo: str | None = None,
    author_routes: list[str] | None = None,
    lane: str = LANE_BULK,
    codex_worker_online: bool | None = None,
    codex_usage: Any = None,
    codex_env: dict | None = None,
    codex_now: float | None = None,
) -> list[dict]:
    """Ordered reviewer routes whose family differs from the author's.

    ``author_model`` is the model id, role id or agent alias the author ran as;
    ``co_authors`` are further author models known for the job (all of them are
    excluded by family). ``health(route) -> bool`` and ``load(route) -> int``
    are injected; unhealthy or saturated routes are skipped. ``lane`` orders the
    cross-family pick (``hard`` puts Codex first, ``bulk`` last).

    The Codex route is only an option when its GATE passes: the config switch,
    an online codex worker (``codex_worker_online``; ``None``/unset means the
    gate cannot vouch for a worker -- fail closed), the injected plan usage
    (``codex_usage``, a callable or a ``{"remaining_pct"}`` dict), no live
    cooldown marker and the route's own concurrency cap.

    An empty result means there is NO cross-family reviewer: the caller PARKS
    the row -- it must never fall back to the author's own family or to a
    self-review.
    """
    if not str(author_model or "").strip():
        # No author means no family to exclude: there is no cross-family
        # guarantee to give, so the row PARKS (never "any route will do").
        return []
    routes = list(roster if roster is not None else load_roster())
    authors = [m for m in [author_model, *(co_authors or [])] if str(m or "").strip()]
    backer = str(author_model or "").split(":", 1)[-1].lower()
    author_routes = set(author_routes or [])
    if backer:
        author_routes.add(backer)
    blocked = {family_of(m) for m in authors}
    healthy = health or (lambda _route: True)
    loads = load or (lambda _route: 0)
    codex_ok = None
    picked = []
    for entry in routes:
        if entry.get("family") in blocked:
            continue
        if not _reviewable(entry, repo=repo, author_routes=author_routes):
            continue
        if is_codex_route(entry):
            # Every automatic Codex dispatch is behind the roster gate.
            if codex_ok is None:
                codex_ok = codex_gate(
                    entry=entry,
                    worker_online=codex_worker_online,
                    env=codex_env,
                    usage=codex_usage,
                    now=codex_now,
                )
            if not codex_ok["allowed"]:
                continue
        if not _available(entry, healthy, loads):
            continue
        picked.append(entry)
    picked.sort(key=lambda route: _bulk_key(route, lane))
    return picked


# ---------------------------------------------------------------------------
# Row state inside the triage queue.
# ---------------------------------------------------------------------------

#: Failure kinds that mean "the reviewer could not answer" -- retryable, and
#: NEVER an approval (a prose-only, empty, truncated, 503 or non-JSON review).
REVIEWER_FAILURE_KINDS = frozenset(
    {"review_busy", "review_timeout", "review_prose_only", "review_empty", "review_truncated", "review_http_503"}
)


def has_structured_verdict(evidence: Any, *, head_sha: str | None = None) -> bool:
    """True only when ``evidence`` carries a structured ``reviewers[].model`` verdict.

    The worker's answer to "did the reviewer actually review?": prose, an empty
    body, a truncated fragment, a 503 object or JSON without a reviewer list is
    NEVER an approval (operator 2026-10-31). A verdict naming a DIFFERENT head
    is a real review of the wrong commit -- also not a verdict for this head.
    """
    payload = evidence
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        payload = payload["result"]
    if not isinstance(payload, dict):
        return False
    if payload.get("http_status") == 503 or payload.get("status_code") == 503:
        return False
    entries = payload.get("reviewers")
    if not isinstance(entries, list) or not entries:
        return False
    models = []
    for entry in entries:
        if not isinstance(entry, dict):
            return False
        model = str(entry.get("model") or entry.get("model_id") or "").strip()
        if not model:
            return False
        models.append(model)
    reviewed = str(payload.get("head_sha") or "").strip()
    if head_sha and reviewed and not same_head(reviewed, head_sha):
        return False
    return bool(models)


def same_head(left: Any, right: Any) -> bool:
    """Shas refer to the same commit (full or abbreviated, either direction)."""
    a = str(left or "").strip().lower()
    b = str(right or "").strip().lower()
    if not a or not b:
        return False
    return a == b or a.startswith(b) or b.startswith(a)


def backoff_seconds(attempt: int) -> float:
    """Delay before review attempt ``attempt+1``: 15 min, 30 min, 1 h, ... <= 2 h."""
    step = max(0, int(attempt))
    return float(min(REVIEW_BACKOFF_SEC * (2**step), MAX_BACKOFF_SEC))


def make_review_state(
    author_model: str,
    tried: list[dict] | None,
    *,
    attempt: int = 0,
    next_attempt_at: float | None = None,
    now: float | None = None,
    author_family: str | None = None,
    parked_reason: str | None = None,
    last_failure: str | None = None,
) -> dict:
    """The ``result.review_state`` block for a row awaiting a cross-family review."""
    now = time.time() if now is None else float(now)
    entries = []
    for item in tried or []:
        if isinstance(item, dict) and item.get("model_id"):
            entry = {
                "model_id": str(item["model_id"]),
                "family": item.get("family") or family_of(str(item["model_id"])),
                "failure": str(item.get("failure") or ""),
                "at": item.get("at", now),
            }
            entries.append(entry)
    state = {
        "state": AWAITING_REVIEW,
        "author": str(author_model or ""),
        "author_family": author_family or family_of(str(author_model or "")),
        "tried": entries,
        "attempt": int(attempt),
        "max_attempts": MAX_REVIEW_ATTEMPTS,
        "next_attempt_at": next_attempt_at,
        "last_failure": last_failure,
        "updated_at": now,
    }
    if parked_reason:
        state["parked_reason"] = parked_reason
    return state


def review_state(job: dict) -> dict | None:
    """The ``awaiting_review`` block of a row, or None when the row is not one."""
    result = job.get("result") if isinstance(job, dict) else None
    if isinstance(result, (str, bytes)):
        # Some bus dialects hand ``result`` back as JSON text.
        try:
            result = json.loads(result)
        except ValueError:
            result = None
    result = result if isinstance(result, dict) else {}
    state = result.get("review_state")
    if not isinstance(state, dict):
        return None
    if str(state.get("state") or "") != AWAITING_REVIEW:
        return None
    return state


def awaiting_review_status(
    loop_teardown: str | None, *, exit_code: int, ready: bool, cancelled: bool
) -> tuple[str, str | None]:
    """Disposition for a loop that authored work but got no reviewer.

    ``reviewer_unavailable`` (a busy endpoint, a timeout, a 503) is the reviewer
    queue's problem, not the job's work: the branch is delivered and the row is
    ``needs-review``. Anything else keeps the worker's own mapping
    (``resolve_delivery_status``): a rescued branch is needs-review, a cancel or
    an author failure stays failed.
    """
    if cancelled:
        return "failed", "loop_unresolved"
    teardown = str(loop_teardown or "").strip().lower()
    if teardown == "reviewer_unavailable":
        return "needs-review", "reviewer_unavailable"
    if teardown == "author_failed":
        # The AUTHOR failed: delivering a branch does not make the work done.
        return "failed", "loop_unresolved"
    if exit_code == 0:
        return ("done", None) if ready else ("needs-review", "completion_evidence_missing")
    if ready:
        return "needs-review", "loop_unresolved_work_rescued"
    return "failed", "loop_unresolved"


def awaiting_review_reason(job: dict) -> str:
    """The human-readable reason a row is waiting, for the triage listing."""
    state = review_state(job) or {}
    tried = ", ".join(f"{t.get('model_id')}({t.get('failure') or 'unavailable'})" for t in state.get("tried") or [])
    base = "reviewer unavailable" if tried else "no cross-family reviewer attempted"
    family = state.get("author_family") or "unknown"
    text = f"{base}: author family {family}"
    if tried:
        text += f"; tried {tried}"
    # The last recorded outcome (``reviewer_unavailable``, ``review_busy``, ...)
    # is what an operator reading the triage listing needs most.
    if state.get("last_failure"):
        text += f"; last failure {state['last_failure']}"
    if state.get("parked_reason"):
        text += f"; {state['parked_reason']}"
    next_at = state.get("next_attempt_at")
    if isinstance(next_at, (int, float)) and next_at:
        text += f"; next attempt at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(next_at))}"
    return text


def triage_reason(job: dict) -> str | None:
    """``review-queue: ...`` when the row is an awaiting_review row, else None."""
    state = review_state(job)
    if state is None:
        return None
    return f"review-queue: {awaiting_review_reason(job)}"


def attempts_remain(state: dict | None) -> bool:
    if not state:
        return False
    return int(state.get("attempt") or 0) < int(state.get("max_attempts") or MAX_REVIEW_ATTEMPTS)


def in_backoff(state: dict | None, now: float | None = None) -> bool:
    if not state:
        return False
    next_at = state.get("next_attempt_at")
    if not isinstance(next_at, (int, float)) or not next_at:
        return False
    return float(next_at) > (time.time() if now is None else float(now))


def is_parked(state: dict | None) -> bool:
    """Parked: no cross-family reviewer is available (never a self-review)."""
    return bool(state and state.get("parked_reason") == "no_cross_family_reviewer")


def is_resolved(state: dict | None) -> bool:
    """A review finished: approved, or changes handed back to the author."""
    return bool(state and (state.get("approved") or state.get("request_changes")))


def finite_load(value: Any) -> bool:
    """A usable load reading (not unknown, not NaN)."""
    return value is not LOAD_UNKNOWN and isinstance(value, (int, float)) and not math.isnan(value)
