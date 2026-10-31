"""Reviewer roster + model-family table (operator rule 2026-10-31).

Two data files next to this module are the ONE source of truth for review
pairing:

* ``model_families.json`` -- the family table. ``qwen38``/``Qwen*`` = alibaba,
  ``gemma*`` = google, ``gpt-oss*``/``gpt-6*``/``codex`` = openai,
  ``nemotron*`` = nvidia, ``MiniMax-M3``/``M3.1`` = minimax, ``deepseek*`` =
  deepseek, ``muse-spark*`` = meta, ``mimo*`` = xiaomi, ``stepfun*`` =
  stepfun, ``claude*`` = anthropic. An UNKNOWN model is its own family
  (``unknown:<model>``) so it never pairs with anything -- fail closed.
* ``reviewer_roster.json`` -- every reviewer-capable route: provider, model
  id, family, location (local/provider/eih), concurrency limit, cost class and
  flags. MiMo is listed but is never a reviewer; StepFun and Meta contributor
  tiers review only operator-approved repos; EIH Nemotron is allowed but is
  marked slow (measured 39 s for a one-word reply) and is the lowest priority
  for bulk work.

``pick_reviewers`` is the pairing function: an ordered, DETERMINISTIC list of
routes whose family differs from the author's (and from every known
co-author's), skipping unhealthy or saturated routes. An empty list means the
job is PARKED for a human/next sweep -- never reviewed by its own family.
"""

from __future__ import annotations

import fnmatch
import json
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
FAMILIES_PATH = HERE / "model_families.json"
ROSTER_PATH = HERE / "reviewer_roster.json"

LOCATIONS = frozenset({"local", "provider", "eih"})
COST_CLASSES = frozenset({"A", "B", "C"})
#: Cost-class ordering (cheapest first) for the deterministic tie-break.
COST_ORDER = {"A": 0, "B": 1, "C": 2}
#: Tie-break for locations at equal cost class: a LOCAL route is faster and
#: free, then a provider API, then EIH (the slowest measured path).
LOCATION_ORDER = {"local": 0, "provider": 1, "eih": 2}
#: ``health`` values ``pick_reviewers`` treats as usable; anything else that is
#: present (``down``, ``unavailable``, ``timeout``, ...) is skipped.
HEALTHY = frozenset({"ok", "up", "healthy", "ready", "online", ""})


@dataclass(frozen=True)
class Route:
    """One reviewer-capable route (a roster row)."""

    id: str
    provider: str
    model: str
    family: str
    location: str
    concurrency: int
    cost_class: str
    base_url: str = ""
    served_as: str = ""
    reviewer: bool = True
    flags: tuple[str, ...] = ()
    approved_repos: tuple[str, ...] = ()
    priority_rank: int = 0
    latency_ms: int = 0
    note: str = ""

    @property
    def slow(self) -> bool:
        return "slow" in self.flags

    @property
    def never_reviewer(self) -> bool:
        return "never_reviewer" in self.flags or not self.reviewer

    @property
    def approved_repo_only(self) -> bool:
        return "approved_repo" in self.flags


@dataclass
class Roster:
    """The parsed roster: routes plus the operator's approved-repo list."""

    approved_repos: tuple[str, ...] = ()
    routes: list[Route] = field(default_factory=list)

    def __iter__(self):
        return iter(self.routes)

    def __len__(self):
        return len(self.routes)

    def __getitem__(self, index):
        return self.routes[index]

    def get(self, route_id: str) -> Route | None:
        return route_by_id(route_id, self.routes)


_cache_lock = threading.Lock()
_family_cache: dict = {"key": None, "data": None}
_roster_cache: dict = {"key": None, "data": None}


def _stat_key(path: Path) -> tuple:
    try:
        st = path.stat()
        return (str(path), st.st_mtime_ns, st.st_size)
    except OSError:
        return (str(path), None, None)


def load_families(path: str | Path | None = None) -> dict[str, str]:
    """The family table: ``{model id or pattern: family}`` (lower-cased keys)."""
    p = Path(path) if path is not None else FAMILIES_PATH
    key = _stat_key(p)
    with _cache_lock:
        if _family_cache["key"] == key and _family_cache["data"] is not None:
            return dict(_family_cache["data"])
    raw = json.loads(p.read_text())
    families = raw.get("families") if isinstance(raw, dict) else None
    table = {str(k).lower(): str(v).strip().lower() for k, v in (families or {}).items() if str(v).strip()}
    with _cache_lock:
        _family_cache["key"] = key
        _family_cache["data"] = table
    return dict(table)


def _route_from_row(row: Mapping) -> Route:
    flags = tuple(str(f) for f in (row.get("flags") or ()))
    return Route(
        id=str(row["id"]),
        provider=str(row.get("provider") or ""),
        model=str(row.get("model") or ""),
        family=str(row.get("family") or "").strip().lower(),
        location=str(row.get("location") or "").strip().lower(),
        concurrency=int(row.get("concurrency") or 1),
        cost_class=str(row.get("cost_class") or "C").strip().upper(),
        base_url=str(row.get("base_url") or ""),
        served_as=str(row.get("served_as") or ""),
        reviewer=bool(row.get("reviewer", True)),
        flags=flags,
        approved_repos=tuple(str(r) for r in (row.get("approved_repos") or ())),
        priority_rank=int(row.get("priority_rank") or 0),
        latency_ms=int(row.get("latency_ms") or 0),
        note=str(row.get("note") or ""),
    )


def load_roster(path: str | Path | None = None) -> Roster:
    """The reviewer roster. Cached on file mtime; never raises on a bad row count."""
    p = Path(path) if path is not None else ROSTER_PATH
    key = _stat_key(p)
    with _cache_lock:
        if _roster_cache["key"] == key and _roster_cache["data"] is not None:
            return _roster_cache["data"]
    raw = json.loads(p.read_text())
    rows = raw.get("routes") if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        raise ValueError(f"{p}: reviewer roster needs a 'routes' list")
    roster = Roster(
        approved_repos=tuple(str(r) for r in (raw.get("approved_repos") or ())),
        routes=[_route_from_row(row) for row in rows if isinstance(row, dict)],
    )
    with _cache_lock:
        _roster_cache["key"] = key
        _roster_cache["data"] = roster
    return roster


def family_of(model: str, families: Mapping[str, str] | None = None) -> str:
    """The family of ``model``: exact id, else first prefix pattern, else unknown.

    An unknown model's family is ``unknown:<model>`` -- unique to that model id,
    so two unknown models never pair and an unknown model never pairs with
    itself (``families_pair`` refuses unknown families outright).
    """
    name = str(model or "").strip()
    table = load_families() if families is None else families
    lowered = name.lower()
    if lowered in table:
        return table[lowered]
    for pattern, family in table.items():
        if pattern.endswith("*") and fnmatch.fnmatchcase(lowered, pattern):
            return family
    return f"unknown:{lowered}"


def families_pair(left: str, right: str) -> bool:
    """Whether a reviewer family may pair with an author family.

    Cross-family ONLY: equal families never pair, and an unknown family (either
    side) never pairs with anything.
    """
    a = str(left or "").strip().lower()
    b = str(right or "").strip().lower()
    if not a or not b or a == b:
        return False
    # An unknown AUTHOR family can never prove a cross-family pairing (we
    # cannot know it differs from any reviewer), so it is refused. An unknown
    # REVIEWER family is its own family and fine to rely on for a known author.
    return not a.startswith("unknown:")


def is_reviewer(route: Route) -> bool:
    return not route.never_reviewer


def reviewer_routes(roster: Roster | Mapping[str, Route] | Iterable[Route]) -> list[Route]:
    """Routes that may review at all (MiMo and anything else flagged out)."""
    return [r for r in as_routes(roster) if is_reviewer(r)]


def route_allowed_for_repo(route: Route, repo: str | None) -> bool:
    """Contributor-tier routes review only their approved repos (a None repo fails)."""
    if not route.approved_repo_only:
        return True
    slug = str(repo or "").strip()
    return bool(slug) and slug in route.approved_repos


def as_routes(roster: Roster | Mapping[str, Route] | Iterable[Route] | None) -> list[Route]:
    """Accept a Roster, a ``{id: Route}`` mapping, a list of routes, or None."""
    if roster is None:
        return []
    values = roster.values() if isinstance(roster, Mapping) else roster
    return [r for r in values if isinstance(r, Route)]


def route_capacity(route: Route, roster: Roster | Mapping[str, Route] | Iterable[Route] | None = None) -> int:
    """Review calls one route can hold at once.

    Routes that front the same endpoint (the CERBERUS reviewer and narrator
    role ids are one 8-slot llama-server) share one budget: the capacity is the
    LOWEST concurrency among the roster routes on that base_url.
    """
    if not route.base_url:
        return max(1, route.concurrency)
    same = [r.concurrency for r in as_routes(roster) if r.base_url == route.base_url]
    return max(1, min(same)) if same else max(1, route.concurrency)


def _healthy(route: Route, health: Mapping[str, str] | None) -> bool:
    if not health:
        return True
    value = health.get(route.id)
    if value is None:
        return True
    return str(value).strip().lower() in HEALTHY


def _saturated(route: Route, load: Mapping[str, int] | None, roster: Roster | Iterable[Route]) -> bool:
    if not load:
        return False
    capacity = route_capacity(route, roster)
    inflight = 0
    for other in as_routes(roster):
        if other.base_url and other.base_url == route.base_url:
            inflight = max(inflight, int(load.get(other.id) or 0))
    return inflight >= capacity


def pick_reviewers(
    author_model: str,
    roster: Roster | Iterable[Route] | None = None,
    health: Mapping[str, str] | None = None,
    load: Mapping[str, int] | None = None,
    *,
    author_family: str | None = None,
    co_author_models: Iterable[str] | None = None,
    repo: str | None = None,
    bulk: bool = True,
    include_expensive: bool = False,
    families: Mapping[str, str] | None = None,
) -> list[Route]:
    """Cross-family reviewers for ``author_model``, best first (deterministic).

    Returns the ordered list of routes that MAY review this author; an EMPTY
    list means no cross-family reviewer is available and the job must be
    PARKED (never reviewed by its own family). Ordering is deterministic:

      1. local before provider before eih (same cost class), then
      2. cost class A -> B -> C, then
      3. the roster's ``priority_rank``, then
      4. the route id (stable last resort).

    Bulk work (``bulk=True``, the default for the queue) pushes ``slow`` routes
    to the end: EIH Nemotron measured 39 s for a one-word reply and must never
    be the first pick for a backlog. ``include_expensive`` lets cost class C
    routes in; ``repo`` applies the contributor-tier approved-repo flags.
    """
    routes = as_routes(roster) if roster is not None else list(load_roster())
    roster_obj = Roster(tuple(), routes)
    table = load_families() if families is None else families
    author_fam = (author_family or family_of(author_model, table)).strip().lower()
    forbidden = {author_fam}
    for model in co_author_models or ():
        forbidden.add(family_of(model, table).strip().lower())
    out: list[Route] = []
    for route in routes:
        if route.never_reviewer:
            continue
        if not families_pair(route.family, author_fam):
            continue
        if any(route.family == other for other in forbidden):
            continue
        if not route_allowed_for_repo(route, repo):
            continue
        if route.cost_class == "C" and not include_expensive:
            continue
        if not _healthy(route, health):
            continue
        if _saturated(route, load, roster_obj):
            continue
        if route.location not in LOCATIONS or route.cost_class not in COST_CLASSES:
            continue
        out.append(route)
    out.sort(
        key=lambda r: (
            1 if (bulk and r.slow) else 0,
            COST_ORDER.get(r.cost_class, len(COST_ORDER)),
            LOCATION_ORDER.get(r.location, len(LOCATION_ORDER)),
            r.priority_rank,
            r.id,
        )
    )
    return out


def route_by_id(route_id: str, roster: Roster | Mapping[str, Route] | Iterable[Route] | None = None) -> Route | None:
    for route in as_routes(roster) if roster is not None else load_roster():
        if route.id == route_id:
            return route
    return None
