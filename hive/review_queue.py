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


def _reviewable(entry: dict, *, repo: str | None, author_routes: set[str]) -> bool:
    family = str(entry.get("family") or "")
    if family in FORBIDDEN_FAMILIES or family in NEVER_REVIEW_FAMILIES:
        return False
    if str(entry.get("model_id") or "").lower().startswith("mimo"):
        return False
    if entry["id"] in author_routes:
        return False
    tier = entry["flags"].get("contributor_tier")
    # A contributor tier is allowed only on the operator-approved repos.
    return not (isinstance(tier, dict) and str(repo or "").strip() not in (tier.get("repos") or []))


def _bulk_key(entry: dict) -> tuple:
    """Deterministic tie-break: priority (slow routes last), local first, id."""
    priority = int(entry["flags"].get("priority") or 0)
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
) -> list[dict]:
    """Ordered reviewer routes whose family differs from the author's.

    ``author_model`` is the model id, role id or agent alias the author ran as;
    ``co_authors`` are further author models known for the job (all of them are
    excluded by family). ``health(route) -> bool`` and ``load(route) -> int``
    are injected; unhealthy or saturated routes are skipped. An empty result
    means there is NO cross-family reviewer: the caller PARKS the row -- it must
    never fall back to the author's own family or to a self-review.
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
    picked = []
    for entry in routes:
        if entry.get("family") in blocked:
            continue
        if not _reviewable(entry, repo=repo, author_routes=author_routes):
            continue
        if not _available(entry, healthy, loads):
            continue
        picked.append(entry)
    picked.sort(key=_bulk_key)
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
    # A block written by a bus dialect or a partial repair may hold a non-list
    # ``tried``: the listing renders the row instead of failing (2026-10-31).
    rounds = state.get("tried") if isinstance(state.get("tried"), list) else []
    tried = ", ".join(
        f"{t.get('model_id')}({t.get('failure') or 'unavailable'})" for t in rounds if isinstance(t, dict)
    )
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
    """Attempts left: an unreadable ``attempt`` counts as 0 (never raises)."""
    if not state:
        return False
    attempt = state.get("attempt") if _is_int(state.get("attempt")) else 0
    limit = state.get("max_attempts") if _is_int(state.get("max_attempts")) else MAX_REVIEW_ATTEMPTS
    return attempt < limit


def in_backoff(state: dict | None, now: float | None = None) -> bool:
    if not state:
        return False
    next_at = state.get("next_attempt_at")
    if not isinstance(next_at, (int, float)) or isinstance(next_at, bool) or not next_at:
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


# ---------------------------------------------------------------------------
# Adopting legacy rows (operator 2026-10-31).
# ---------------------------------------------------------------------------
# The queue can only advance a row that carries ``result.review_state``, and
# only the new worker line writes that block. The deployed workers still run the
# older ``deploy/hive-canonical`` line, so their needs-review rows were invisible
# to the sweep: production logged ``review_queue scanned=0 submitted=0`` while
# thirteen rows sat unapproved. A legacy row is ADOPTED -- its review_state is
# built FROM the row, in pure data, status untouched.

#: Why a legacy row was adopted: the shape its ``result`` records.
ADOPTION_NO_REVIEWER = "no_reviewer"
ADOPTION_DID_NOT_APPROVE = "review_did_not_approve"
ADOPTION_SUPERSEDED_HEAD = "review_superseded_head"
#: The review_state fields a consumer reads. A block missing any is a partial
#: write, so such a row is adopted again instead of skipped forever.
ADOPTION_REQUIRED_FIELDS = ("author", "attempt", "max_attempts")


def _result_dict(value: Any) -> dict:
    """``result`` as a dict, tolerating the bus dialect that stores JSON text."""
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def verdict_word(value: Any) -> str:
    """Lower-case verdict with ``-`` read as ``_`` (``request-changes`` == ``request_changes``)."""
    return str(value or "").strip().lower().replace("-", "_")


def _reviewer_name(entry: dict) -> str:
    return str(entry.get("model") or entry.get("model_id") or entry.get("reviewer_model") or "").strip()


def _evidence_dicts(result: dict) -> list[dict]:
    """Every dict on a row that may record a review round (reading order)."""
    out: list[dict] = []

    def add(value):
        if isinstance(value, dict):
            out.append(value)

    add(result.get("review_evidence"))
    add(result.get("reviewer"))
    for key in ("hive_reviews", "review_rounds", "reviews"):
        for entry in result.get(key) or []:
            add(entry)
    loop = result.get("loop_result")
    if isinstance(loop, dict):
        for entry in loop.get("verdicts") or []:
            add(entry)
        add(loop.get("teardown"))
    for entry in result.get("result_history") or []:
        add(entry)
    return out


def _structured_approve_for_head(result: dict, head: str) -> bool:
    """A structured ``reviewers[]`` approve naming EXACTLY ``head``."""
    for evidence in [result, *_evidence_dicts(result)]:
        entries = evidence.get("reviewers")
        if not isinstance(entries, list) or not entries:
            continue
        words = [verdict_word(entry.get("verdict")) for entry in entries if isinstance(entry, dict)]
        if "approve" not in words or any(w in ("request_changes", "blocked") for w in words):
            continue
        reviewed = ""
        for key in ("head_sha", "head", "reviewed_head"):
            if isinstance(evidence.get(key), str) and evidence[key].strip():
                reviewed = evidence[key].strip()
                break
        if head and reviewed and same_head(reviewed, head):
            return True
    return False


def legacy_review_failure(result: dict) -> str | None:
    """The structured failure kind a legacy row's ``result`` records, or None."""
    texts = []
    for evidence in _evidence_dicts(result):
        for key in ("failure", "failure_reason", "verdict", "error", "final_verdict", "reason"):
            if isinstance(evidence.get(key), str):
                texts.append(evidence[key].lower())
    for key in ("failure_reason", "error", "verdict", "error_message"):
        if isinstance(result.get(key), str):
            texts.append(result[key].lower())
    haystack = " ".join(texts)
    for needle, kind in (
        ("no review chunks", "review_busy"),
        ("reviewer_unavailable", "reviewer_unavailable"),
        ("reviewer unavailable", "reviewer_unavailable"),
        ("review_busy", "review_busy"),
        ("review_timeout", "review_timeout"),
        ("timeout", "review_timeout"),
        ("timed out", "review_timeout"),
        ("prose", "review_prose_only"),
        ("truncat", "review_truncated"),
        ("empty", "review_empty"),
        ("503", "review_http_503"),
    ):
        if needle in haystack:
            return kind
    return None


def legacy_review_tried(result: dict) -> list[dict]:
    """``tried[]`` for an adopted row, from the reviewer/rounds its result records.

    Only NAMES are read from the row (the reviewer a round named); the failure
    kind is classified here, so a row can never inject its own outcome.
    """
    failure = legacy_review_failure(result)
    tried: list[dict] = []
    seen: set[str] = set()
    for evidence in _evidence_dicts(result):
        model = _reviewer_name(evidence)
        if not model or model.lower() in seen:
            continue
        seen.add(model.lower())
        tried.append({"model_id": model, "failure": failure or "review_reported"})
    return tried


def legacy_author_model(job: dict, result: dict | None = None) -> str:
    """The author model a legacy row names: ``claimed_model`` then ``result`` fields.

    ``claimed_model`` is the AUTHOR route of the job (hive/review_record.py); the
    result's ``models.author`` / ``author_model`` are the older line's fallbacks.
    No author recorded answers "" -- the caller PARKS, it never guesses.
    """
    result = result if isinstance(result, dict) else {}
    models = result.get("models") if isinstance(result.get("models"), dict) else {}
    for candidate in (
        job.get("claimed_model"),
        models.get("author"),
        result.get("author_model"),
        result.get("author"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def legacy_effective_head(job: dict, result: dict | None = None) -> str:
    """The head a legacy row delivered: ``result.head_sha``, the row, the last commit."""
    result = result if isinstance(result, dict) else {}
    for value in (result.get("head_sha"), job.get("head_sha") if isinstance(job, dict) else None):
        if isinstance(value, str) and value.strip():
            return value.strip()
    for entry in reversed(result.get("commits") if isinstance(result.get("commits"), list) else []):
        sha = entry.get("sha") if isinstance(entry, dict) else entry
        if isinstance(sha, str) and sha.strip():
            return sha.strip()
    return ""


def _is_int(value: Any) -> bool:
    """A real integer (bools are not counts); ``"3"`` is a string, not a count."""
    return isinstance(value, int) and not isinstance(value, bool)


def state_problem(state: dict | None) -> str:
    """Why a review_state block cannot be advanced as written; "" when it can.

    The fields the advance reads must be PRESENT and of the right shape:
    ``author`` a string, ``attempt``/``max_attempts`` integers (a block the
    worker wrote always has them; a partial write, a hand edit or a bus dialect
    that stringified the JSON does not). Naming the defect lets the caller both
    refuse the block and record what it repaired (review 2026-10-31).
    """
    if not isinstance(state, dict):
        return "no_state"
    missing = [field for field in ADOPTION_REQUIRED_FIELDS if field not in state]
    if missing:
        return "missing:" + ",".join(missing)
    malformed = [field for field in ("attempt", "max_attempts") if not _is_int(state.get(field))]
    if malformed:
        return "malformed:" + ",".join(malformed)
    if not isinstance(state.get("author"), str):
        return "malformed:author"
    if not isinstance(state.get("tried", []), list):
        return "malformed:tried"
    return ""


def usable_review_state(job: dict) -> dict | None:
    """The row's review_state ONLY when it is complete enough to advance.

    ``review_state()`` answers "is this row in the queue"; this answers "can a
    sweep act on it". A block missing a field the advance reads (author,
    attempt, max_attempts), or carrying one of the wrong shape, is a partial
    write: None here, and ``needs_adoption`` then REPAIRS it -- it is never a
    reason to skip the row forever (review 2026-10-31).
    """
    state = review_state(job)
    if state_problem(state):
        return None
    return state


def needs_adoption(job: dict) -> tuple[bool, str]:
    """Whether a needs-review row should be adopted, and the shape it records.

    Not adopted: no pushed head branch; the queue owns a COMPLETE row; a review
    is in flight; an approve already covers the delivered head (a LATER head is
    a different review and is adopted again). Otherwise the row is legacy, and
    the reason names its shape -- ``review_state_partial`` for a block the queue
    wrote but cannot advance (it is repaired, not skipped; review 2026-10-31,
    high) -- or the recorded failure kind (``reviewer_unavailable``,
    ``review_prose_only``, a 503 ...), ``review_superseded_head`` (evidence for
    an older head), ``review_did_not_approve`` or ``no_reviewer``.
    """
    if not isinstance(job, dict) or not str(job.get("head_branch") or "").strip():
        return False, ""
    result = _result_dict(job.get("result"))
    state = review_state(job)
    if state is not None and state.get("review_job_id"):
        return False, ""  # a review is in flight: never a second one
    if state is not None and state.get("approved"):
        return False, ""  # already approved
    head = legacy_effective_head(job, result)
    if _structured_approve_for_head(result, head):
        return False, ""
    # A legacy row (no block at all) and a PARTIAL block take the same path:
    # the block is rebuilt from the row. Only a complete block means "owned".
    if state is not None and not state_problem(state):
        return False, ""
    failure = legacy_review_failure(result)
    if failure:
        return True, failure
    has_review = bool(
        result.get("review_evidence")
        or result.get("reviewer")
        or result.get("hive_reviews")
        or result.get("review_rounds")
        or (isinstance(result.get("loop_result"), dict) and result["loop_result"].get("verdicts"))
    )
    if not has_review:
        return True, ADOPTION_NO_REVIEWER
    heads = []
    for evidence in _evidence_dicts(result):
        for key in ("head_sha", "head", "reviewed_head"):
            if isinstance(evidence.get(key), str) and evidence[key].strip():
                heads.append(evidence[key].strip())
    if heads and head and not any(same_head(h, head) for h in heads):
        return True, ADOPTION_SUPERSEDED_HEAD
    return True, ADOPTION_DID_NOT_APPROVE


def adoption_review_state(job: dict, *, now: float | None = None) -> dict | None:
    """The ``review_state`` an adopted legacy row gets, or None (not adoptable).

    Built FROM the row: author/family from ``claimed_model``/``result``,
    ``tried[]`` from the reviewer its result recorded, attempt 0 (a legacy row
    spent no queue attempts). ``adopted_from``/``adopted_head`` record the
    provenance of the block, so a later reader can tell an adopted row from one
    the worker tagged.

    A PARTIAL block on the row is repaired rather than skipped (review
    2026-10-31, high). Whatever of it is still trustworthy is kept -- the author
    it named, the attempts it spent, a pending backoff and a parked_reason -- so
    a repair neither re-forges a known author nor spends the retry budget twice;
    the defect is recorded in ``adopted_from``/``repaired_from``.
    """
    adopt, reason = needs_adoption(job)
    if not adopt:
        return None
    result = _result_dict(job.get("result"))
    partial = review_state(job)
    problem = state_problem(partial) if partial is not None else ""
    if partial is not None and not problem:
        return None  # complete: nothing to repair
    author = ""
    attempt = 0
    if partial is not None:
        if isinstance(partial.get("author"), str) and partial["author"].strip():
            author = partial["author"].strip()
        if _is_int(partial.get("attempt")):
            attempt = partial["attempt"]
    author = author or legacy_author_model(job, result)
    tried = legacy_review_tried(result)
    if partial is not None and isinstance(partial.get("tried"), list):
        kept = [entry for entry in partial["tried"] if isinstance(entry, dict) and entry.get("model_id")]
        if kept and not tried:
            tried = kept
    state = make_review_state(
        author,
        tried,
        attempt=attempt,
        last_failure=(partial or {}).get("last_failure") or reason or None,
        now=now,
    )
    if partial is not None:
        next_at = partial.get("next_attempt_at")
        if isinstance(next_at, (int, float)) and not isinstance(next_at, bool):
            state["next_attempt_at"] = next_at
        if isinstance(partial.get("parked_reason"), str) and partial["parked_reason"]:
            state["parked_reason"] = partial["parked_reason"]
        if isinstance(partial.get("review_job_id"), str) and partial["review_job_id"]:
            state["review_job_id"] = partial["review_job_id"]
        state["repaired_from"] = problem
        state["adopted_from"] = f"partial-review-state:{problem}"
    else:
        state["adopted_from"] = f"legacy-needs-review:{reason or 'unknown'}"
    state["adopted_head"] = legacy_effective_head(job, result)
    return state
