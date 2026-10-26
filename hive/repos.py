"""Repo registry — load + query the built catalog (the hive's project index).

The catalog (``data/repos.json``) is BUILT by ``hive.registry`` from live sources
(GitLab/GitHub PATs, ARGONAS bare repos, Buildkite pipelines, R2 buckets) merged
with a curated overlay (``data/repos_overlay.json``). This module only *loads and
serves* it — it never hits the network.

Replaces the hand-maintained ``~/.claude/PROJECT_LOCATIONS.md`` (now GENERATED
from this catalog).

The catalog is a BUILD ARTIFACT (last built 2026-10-15), so a repo created on
ARGONAS afterwards has no entry — and there is no add-repo API, and the built
file is root-owned on the bus host. On a catalog MISS only, :func:`_raw_entry`
and :func:`find_exact` therefore fall back to a *synthesized* entry when a bare
repo ``<slug>.git`` actually exists under the ARGONAS git root as this host
sees it (``HIVE_ARGONAS_GIT_ROOT``, default: the ARGONAS NFS mount -- taken
from ``hive.registry``, the one place the fleet spells that path). See
:func:`_fallback_entry`.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

from hive.registry import ARGONAS_GIT_PATH_NFS, argonas_nfs_path, argonas_ssh_url

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = _REPO_ROOT / "data" / "repos.json"

# Source tag stamped on a synthesized (not-built-into-the-catalog) entry, so a
# consumer can tell live-ARGONAS answers from catalog rows.
LIVE_SOURCE = "argonas-live"


def _catalog_path(path: str | os.PathLike | None = None) -> Path:
    if path is not None:
        return Path(path)
    env = os.environ.get("HIVE_REPO_CATALOG")
    return Path(env) if env else DEFAULT_CATALOG_PATH


_CACHE: dict[str, Any] = {"catalog": None, "mtime": None, "path": None, "meta": None}
_LOCK = threading.Lock()


def load_catalog(path: str | os.PathLike | None = None, *, force: bool = False) -> dict[str, dict[str, Any]]:
    """Load + cache the catalog (id -> entry). Reloads when the file mtime changes."""
    p = _catalog_path(path)
    with _LOCK:
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            raise FileNotFoundError(f"repo catalog not found: {p}") from None
        if not force and _CACHE["catalog"] is not None and _CACHE["path"] == str(p) and _CACHE["mtime"] == mtime:
            return _CACHE["catalog"]
        data = json.loads(p.read_text())
        entries = {e["id"]: e for e in data.get("repos", []) if e.get("id")}
        _CACHE.update(catalog=entries, mtime=mtime, path=str(p), meta=data.get("_meta", {}))
        return entries


def _load_for_host(entry: dict[str, Any], host: str) -> dict[str, Any]:
    """Concrete load recipe for a host: live path if it hosts it, else clone."""
    h = (host or "").strip().lower()
    for target in entry.get("live") or []:
        if (target.get("host") or "").lower() == h:
            return {
                "mode": "live",
                "host": h,
                "path": target.get("path"),
                "service": target.get("service"),
                "note": "host already has the live tree — edit in place, do not re-clone",
            }
    return {
        "mode": "clone",
        "host": h,
        "ssh": entry.get("clone_ssh"),
        "nfs": entry.get("clone_nfs"),
        "default_branch": _default_branch(entry),
        "note": "clone fresh from ARGONAS; never trust a local ~/Projects copy as canonical",
    }


def _default_branch(entry: dict[str, Any]) -> str:
    for facet in ("gitlab", "github", "buildkite"):
        b = (entry.get(facet) or {}).get("default_branch")
        if b:
            return b
    return entry.get("default_branch", "master")


def _required_mnemos_queries(entry: dict[str, Any]) -> list[str]:
    """The MNEMOS searches an agent MUST run before working this repo."""
    mn = entry.get("mnemos") or {}
    if not mn.get("required"):
        return []
    queries = list(mn.get("queries") or [])
    tags = mn.get("tags") or []
    if not queries and tags:
        queries = [" ".join(tags)]
    return queries


def to_dict(entry: dict[str, Any], *, host: str | None = None) -> dict[str, Any]:
    """Normalize a catalog entry for API output.

    Emits ``required_mnemos_queries`` itself so every consumer (bus endpoint
    AND MCP bridge) sees the same field — no endpoint-side mutation needed.
    This is a fresh dict per call (never the cached entry), so callers may
    mutate the result safely.
    """
    remotes = {}
    for facet in ("gitlab", "github"):
        f = entry.get(facet) or {}
        if f.get("url") or f.get("clone_url") or f.get("http_url_to_repo"):
            remotes[facet] = f.get("url") or f.get("clone_url") or f.get("http_url_to_repo")
    out = {
        "id": entry["id"],
        "display_name": entry.get("display_name", entry["id"]),
        "aliases": entry.get("aliases", []),
        "sources": entry.get("sources", []),
        "argonas_bare": entry.get("argonas_bare"),
        "clone": {"ssh": entry.get("clone_ssh"), "nfs": entry.get("clone_nfs"), "remotes": remotes},
        "forges": {f: entry.get(f) for f in ("gitlab", "github") if entry.get(f)},
        "buildkite": entry.get("buildkite"),
        "live": entry.get("live", []),
        "default_branch": _default_branch(entry),
        "language": entry.get("language"),
        "commands": entry.get("commands", {}),
        "owner": entry.get("owner"),
        "visibility": entry.get("visibility"),
        "status": entry.get("status", "active"),
        "deps": entry.get("deps", []),
        "mnemos": entry.get("mnemos", {}),
        "required_mnemos_queries": _required_mnemos_queries(entry),
        "notes": entry.get("notes"),
    }
    if host is not None:
        out["load_for_host"] = _load_for_host(entry, host)
    return out


def list_repos(
    *,
    status: str | None = None,
    language: str | None = None,
    owner: str | None = None,
    tag: str | None = None,
    source: str | None = None,
    host: str | None = None,
) -> list[dict[str, Any]]:
    """Filtered view of the BUILT catalog.

    Catalog-only ON PURPOSE: it never consults the ARGONAS live fallback. This
    is an index over an artifact, and walking the mount to enumerate it would
    turn a cheap read into a network filesystem sweep on every dashboard poll.
    A repo created after the build is reachable by id (:func:`get_repo` /
    ``GET /v1/repos/{id}``), not by listing.
    """
    out = []
    for entry in load_catalog().values():
        if status and entry.get("status", "active") != status:
            continue
        if language and entry.get("language") != language:
            continue
        if owner and entry.get("owner") != owner:
            continue
        if tag and tag not in (entry.get("mnemos", {}).get("tags") or []):
            continue
        if source and source not in entry.get("sources", []):
            continue
        out.append(to_dict(entry, host=host))
    return sorted(out, key=lambda e: e["id"])


# --------------------------------------------------------------------------- #
# ARGONAS live fallback (catalog MISS only)
# --------------------------------------------------------------------------- #
# A job's `repo` is an arbitrary string from the submitter, so the slug is
# validated as hostile input before it is ever joined onto a path: no
# separators, no traversal, no `.git` suffix (callers pass the bare name).
_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
# Branches that must not be treated as repo state.
_PSEUDO_BRANCHES = frozenset({"HEAD", "refs"})
# Characters git refuses in a ref name (git-check-ref-format). Spelled out as a
# set, one entry per character, so there is no escape sequence to get wrong --
# the backslash included.
_FORBIDDEN_REF_CHARS = frozenset("~^:?*[\\\x7f") | frozenset(" \t\n\r\v\f")
_POSITIVE_TTL = 60.0  # bare repos appear rarely; a minute is plenty
_NEGATIVE_TTL = 10.0  # short: a repo created just now must not be denied long
_FALLBACK_MAX = 256


class _FallbackCache:
    """Tiny TTL cache for ARGONAS live lookups, ordered LRU-style.

    ONE lock guards the dict and the order together, so there is no second lock
    to take in the wrong order, no eviction race between a lookup and a pop,
    and no ``KeyError`` from a key that vanished mid-scan. The lock is held
    ONLY for these constant-time dict operations: callers do their filesystem
    probe between :meth:`lookup` and :meth:`store` with no lock held, so a
    stalled mount delays the one thread that touched it -- never another slug
    and never the bus.

    Over ``max_entries`` the LEAST-RECENTLY-USED entry is evicted, one at a
    time, so a flood of hostile job strings cannot erase every warm entry and
    stampede the mount with re-probes. The public surface hands back entries,
    never the internal ``(expires_at, entry)`` bookkeeping.
    """

    def __init__(self, max_entries: int = _FALLBACK_MAX) -> None:
        # slug -> (expires_at, entry-or-None), oldest first (dicts keep order).
        self._entries: OrderedDict[str, tuple[float, dict[str, Any] | None]] = OrderedDict()
        # At least one slot, so the eviction loop always terminates.
        self._max = max(1, max_entries)
        self._lock = threading.Lock()

    def lookup(self, slug: str) -> tuple[bool, dict[str, Any] | None]:
        """``(hit, entry)`` for a live (unexpired) slug; an expired one is dropped."""
        now = time.monotonic()
        with self._lock:
            item = self._entries.get(slug)
            if item is None:
                return False, None
            expires_at, entry = item
            if expires_at <= now:
                del self._entries[slug]
                return False, None
            self._entries.move_to_end(slug)
            return True, entry

    def store(self, slug: str, ttl: float, entry: dict[str, Any] | None) -> None:
        """Cache ``entry`` (possibly None) for ``ttl`` seconds, evicting only as needed."""
        with self._lock:
            self._entries[slug] = (time.monotonic() + ttl, entry)
            self._entries.move_to_end(slug)
            while len(self._entries) > self._max:
                self._entries.popitem(last=False)  # the LRU end, one at a time

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __contains__(self, slug: object) -> bool:
        with self._lock:
            return slug in self._entries


_FALLBACK = _FallbackCache()


def argonas_git_root() -> str:
    """The ARGONAS bare-repo root as THIS host sees it (env override, else the NFS mount).

    The default is the ``hive.registry`` mount CONSTANT itself -- the one place
    the fleet spells it -- not a path re-derived from a formatted slug.
    """
    env = (os.environ.get("HIVE_ARGONAS_GIT_ROOT") or "").strip()
    return env or ARGONAS_GIT_PATH_NFS


def _valid_slug(slug: str) -> bool:
    if not _SLUG_RE.match(slug or ""):
        return False
    return not slug.endswith(".git") and ".." not in slug


def _bare_head_branch(bare_dir: str) -> str:
    """Default branch named by the bare repo's ``HEAD``, else ``"master"``.

    Read as plain text (no git subprocess: the fallback must not block) and
    validated as a branch name, because the file's content is remote state.
    """
    try:
        head = Path(bare_dir, "HEAD").read_text(errors="replace").strip()
    except OSError:
        return "master"
    prefix = "ref: refs/heads/"
    if not head.startswith(prefix):
        return "master"  # detached HEAD, garbage, or unreadable
    name = head[len(prefix) :].strip()
    if not name or len(name) > 200 or ".." in name or name in _PSEUDO_BRANCHES:
        return "master"
    for part in name.split("/"):
        if part in ("", ".", "..", "HEAD") or part.startswith(".") or part.endswith(".lock"):
            return "master"
        if any((ch in _FORBIDDEN_REF_CHARS) or ch.isspace() for ch in part):
            return "master"
        if part.endswith("."):
            return "master"
    return name


def _contained(path: str, root: str) -> bool:
    """Whether ``path`` is ``root`` or lies beneath it (both already realpath'd).

    ``commonpath`` rather than ``startswith(root + os.sep)``: the string form
    turns a root of ``"/"`` into ``"//"``, which no absolute path starts with,
    so every candidate would have looked contained.
    """
    try:
        return os.path.commonpath([root, path]) == root
    except ValueError:  # different drives (Windows) or a mix of abs/rel
        return False


def _probe_argonas_live(slug: str) -> dict[str, Any] | None:
    """Synthesize a catalog-shaped entry for ``<root>/<slug>.git``, or None.

    Never raises: an unreadable/absent mount (or a root that is not a directory)
    answers None, exactly like a miss. Symlinks are refused when they resolve
    outside the real root, so a planted link cannot make the bus advertise (and
    have workers push to) a repository outside ARGONAS.
    """
    try:
        # Leading/trailing space is a submitter typo, not a different repo; the
        # slug is matched exactly otherwise (never lowercased -- ARGONAS paths
        # are case-sensitive, and returning a name the caller did not ask for
        # would hand a worker the wrong id).
        slug = (slug or "").strip()
        if not _valid_slug(slug):
            return None
        root = argonas_git_root()
        bare = os.path.join(root, slug + ".git")
        if not os.path.isdir(bare):
            return None
        # Never follow a link out of the root: a bare dir that resolves outside
        # the (realpath'd) root is not an ARGONAS repo, so a planted link cannot
        # make the bus advertise -- and workers push to -- anything else.
        real_root = os.path.realpath(root)
        real_bare = os.path.realpath(bare)
        if not _contained(real_bare, real_root):
            return None
        if not os.path.isdir(real_bare):
            return None
        return {
            "id": slug,
            "display_name": slug,
            "aliases": [],
            "sources": [LIVE_SOURCE],
            "argonas_bare": bare,
            "clone_ssh": argonas_ssh_url(slug),
            "clone_nfs": argonas_nfs_path(slug),
            "default_branch": _bare_head_branch(bare),
            "status": "active",
        }
    except Exception:  # noqa: BLE001 - a broken mount must never break a lookup
        return None


def _fallback_entry(slug: str) -> dict[str, Any] | None:
    """Cached ARGONAS live lookup for one slug (positive 60 s, negative 10 s).

    Three separate steps, so the lock is never held across filesystem I/O:
    read the cache, probe (one stat, unlocked, no lock held at all), write the
    cache. A stalled mount therefore delays only the caller that touched it.
    """
    hit, cached = _FALLBACK.lookup(slug)
    if hit:
        return cached
    entry = _probe_argonas_live(slug)
    _FALLBACK.store(slug, _POSITIVE_TTL if entry is not None else _NEGATIVE_TTL, entry)
    return entry


def clear_fallback_cache() -> None:
    """Drop cached ARGONAS live lookups (tests, and after creating a bare repo)."""
    _FALLBACK.clear()


def _raw_entry(repo_id: str) -> dict[str, Any] | None:
    """Raw cached catalog entry (not through to_dict). Exact id first, then alias.

    On a catalog MISS only, falls back to a synthesized entry for a bare
    ``<slug>.git`` that exists on ARGONAS right now (:func:`_fallback_entry`).
    """
    catalog = load_catalog()
    entry = catalog.get(repo_id)
    if entry is None and isinstance(repo_id, str):
        key = repo_id.strip().lower()
        for candidate in catalog.values():
            names = {candidate["id"].lower(), (candidate.get("display_name") or "").lower()}
            names |= {a.lower() for a in candidate.get("aliases", [])}
            if key in names:
                return candidate
    if entry is None:
        return _fallback_entry(repo_id)
    return entry


def get_repo(repo_id: str, *, host: str | None = None) -> dict[str, Any] | None:
    entry = _raw_entry(repo_id)
    return to_dict(entry, host=host) if entry else None


def find_exact(repo_id: str) -> dict[str, Any] | None:
    """EXACT catalog lookup: id, then case-insensitive id/display_name/alias.

    Deliberately NOT :func:`resolve`. ``resolve`` is a fuzzy prefix/substring
    ranker, so a query like ``"hive"`` matches the catalog entry ``graeae-hive``
    by SUBSTRING -- callers that need "is this repo genuinely in the catalog"
    (e.g. the decline classifier) would wrongly treat an absent repo as
    present. This helper only ever returns rank-0 matches.

    On a catalog MISS it falls back to the ARGONAS live entry (see
    :func:`_fallback_entry`), so a repo created after the catalog build is
    reported as present. That is the INTENDED effect on
    ``hive.bus.domain.jobs.repo_in_catalog``: a ``no_repo_mapping`` decline for
    such a repo is a STALE-CLIENT/deploy-drift signal (requeue and route), not
    a genuinely-absent repo that should burn the dead-letter budget. Only a
    slug with no bare repo on ARGONAS stays "absent".
    """
    key = (repo_id or "").strip()
    if not key:
        return None
    catalog = load_catalog()
    entry = catalog.get(key)
    if entry is not None:
        return entry
    lowered = key.lower()
    for candidate in catalog.values():
        names = {candidate["id"].lower(), (candidate.get("display_name") or "").lower()}
        names |= {a.lower() for a in (candidate.get("aliases") or [])}
        if lowered in names:
            return candidate
    # Catalog MISS only: a bare repo created on ARGONAS after the build is a
    # real repo, and workers must be able to clone it. Exact slug only -- the
    # fallback never fuzzy-matches.
    return _fallback_entry(key)


def resolve(query: str, *, limit: int = 25) -> list[dict[str, Any]]:
    """Ranked fuzzy match over id / display_name / aliases (exact > prefix > substring).

    Result count is capped at ``limit`` so a short query cannot return the
    whole catalog.

    Catalog-only ON PURPOSE (see :func:`list_repos`): the ARGONAS live fallback
    is an exact-id answer, never a search index.
    """
    q = query.strip().lower()
    if not q:
        return []
    scored: list[tuple[int, dict[str, Any]]] = []
    for entry in load_catalog().values():
        hay = [entry["id"].lower(), (entry.get("display_name") or "").lower()]
        hay += [a.lower() for a in entry.get("aliases", [])]
        rank: int | None = None
        for h in hay:
            if h == q:
                score = 0
            elif h.startswith(q):
                score = 1
            elif q in h:
                score = 2
            else:
                continue
            if rank is None or score < rank:
                rank = score
        if rank is not None:
            scored.append((rank, entry))
    scored.sort(key=lambda t: (t[0], t[1]["id"]))
    return [to_dict(e) for _score, e in scored[:limit]]


def required_mnemos_queries(repo_id: str) -> list[str]:
    """The MNEMOS searches an agent MUST run before working this repo.

    Computes from the RAW entry so it does not depend on to_dict having been
    applied to the caller's dict.
    """
    entry = _raw_entry(repo_id)
    return _required_mnemos_queries(entry) if entry else []


def catalog_meta() -> dict[str, Any]:
    p = _catalog_path()
    try:
        mtime = p.stat().st_mtime
    except FileNotFoundError:
        mtime = None
    catalog = load_catalog()
    return {"path": str(p), "count": len(catalog), "mtime": mtime, "generated": _CACHE.get("meta", {})}
