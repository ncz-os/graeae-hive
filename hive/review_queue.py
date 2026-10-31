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

_EXACT_CACHE: dict = {"key": None, "values": {}, "patterns": ()}
_ROSTER_CACHE: dict = {"key": None, "routes": ()}
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
    with _SEED_LOCK:
        _EXACT_CACHE.update(key=key, values=values, patterns=patterns)
    return values, patterns


def load_family_table() -> dict:
    """The whole family table as data (``{"schema_version", "exact"}``)."""
    values, _ = _load_exact()
    return {"schema_version": 1, "exact": values}


@functools.lru_cache(maxsize=2048)
def _family_of_cached(raw: str) -> str:
    exact, patterns = _load_exact()
    name = raw.lower()
    hit = exact.get(raw)
    if hit is not None:
        return hit["family"]
    for needle, family in patterns:
        if fnmatch.fnmatchcase(name, needle):
            return family
    return UNKNOWN_FAMILY_PREFIX + name


def family_of(model: str) -> str:
    """The operator's family for ``model``, or its own family when unknown.

    Accepts a model id (``gpt-oss-20b``), a fleet role id (``cerberus-reviewer``)
    or an agent alias (``minimax_flash``). Role ids resolve through the llm-roles
    map first: the map's ``family`` names the backing model's family, and its
    ``model`` is then resolved by this table. Aliases resolve through
    ``provenance`` when that module is importable (it is on the worker; the
    bus resolves the role map only). An unknown model returns
    ``model:<lower-cased id>`` -- never a guessed family, so it can never pair
    with anything but itself.
    """
    raw = str(model or "").strip()
    if not raw:
        return ""
    residue = _resolve_role_or_alias(raw)
    return _family_of_cached(residue) if residue else UNKNOWN_FAMILY_PREFIX + raw.lower()


def _resolve_role_or_alias(raw: str) -> str:
    """Resolve a role id or agent alias to a model id; else ``raw`` itself."""
    try:
        from hive import llm_roles

        if llm_roles.is_role_id(raw):
            backing = llm_roles.backing_model(raw)
            if backing:
                return backing
            mapped = llm_roles.family(raw)
            if mapped:
                # The map knows the family but not a model this table lists;
                # answer with a name the table's patterns resolve (gemma* ->
                # google, minimax* -> minimax...). No answer: unknown role.
                return {"gemma": "gemma", "gemini": "gemma"}.get(mapped, mapped)
    except Exception:  # noqa: BLE001 -- the map is optional
        pass
    if _ALIAS_RE.match(raw.lower()) and raw.lower() not in _ALIAS_STOPWORDS:
        try:
            import provenance

            resolved = provenance.primary_model(raw)
            if resolved:
                return str(resolved)
        except Exception:  # noqa: BLE001 -- provenance ships on the worker only
            pass
    return raw


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
        base = str(route.get("base_url") or "").rstrip("/")
        if not base:
            return LOAD_UNKNOWN
        try:
            metrics = self._fetch_json(base + "/metrics")
            if isinstance(metrics, str) and "vllm" in metrics:
                return _vllm_load(metrics)
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
    if isinstance(tier, dict) and str(repo or "").strip() not in (tier.get("repos") or []):
        return False
    return True


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
    result = result if isinstance(result, dict) else {}
    state = result.get("review_state")
    if not isinstance(state, dict):
        return None
    if str(state.get("state") or "") != AWAITING_REVIEW:
        return None
    return state


def awaiting_review_status(loop_teardown: str | None, *, exit_code: int, ready: bool, cancelled: bool) -> tuple[str, str | None]:
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
    tried = ", ".join(
        f"{t.get('model_id')}({t.get('failure') or 'unavailable'})" for t in state.get("tried") or []
    )
    base = "reviewer unavailable" if tried else "no cross-family reviewer attempted"
    family = state.get("author_family") or "unknown"
    text = f"{base}: author family {family}"
    if tried:
        text += f"; tried {tried}"
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
