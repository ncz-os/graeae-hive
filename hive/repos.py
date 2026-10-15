"""Repo registry — load + query the built catalog (the hive's project index).

The catalog (``data/repos.json``) is BUILT by ``hive.registry`` from live sources
(GitLab/GitHub PATs, ARGONAS bare repos, Buildkite pipelines, R2 buckets) merged
with a curated overlay (``data/repos_overlay.json``). This module only *loads and
serves* it — it never hits the network.

Replaces the hand-maintained ``~/.claude/PROJECT_LOCATIONS.md`` (now GENERATED
from this catalog).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = _REPO_ROOT / "data" / "repos.json"


def _catalog_path(path: str | os.PathLike | None = None) -> Path:
    if path is not None:
        return Path(path)
    env = os.environ.get("HIVE_REPO_CATALOG")
    return Path(env) if env else DEFAULT_CATALOG_PATH


_CACHE: dict[str, Any] = {"catalog": None, "mtime": None, "path": None, "meta": None}
_LOCK = threading.Lock()


def load_catalog(
    path: str | os.PathLike | None = None, *, force: bool = False
) -> dict[str, dict[str, Any]]:
    """Load + cache the catalog (id -> entry). Reloads when the file mtime changes."""
    p = _catalog_path(path)
    with _LOCK:
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            raise FileNotFoundError(f"repo catalog not found: {p}") from None
        if (
            not force
            and _CACHE["catalog"] is not None
            and _CACHE["path"] == str(p)
            and _CACHE["mtime"] == mtime
        ):
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


def to_dict(entry: dict[str, Any], *, host: str | None = None) -> dict[str, Any]:
    """Normalize a catalog entry for API output."""
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


def get_repo(repo_id: str, *, host: str | None = None) -> dict[str, Any] | None:
    catalog = load_catalog()
    entry = catalog.get(repo_id)
    if entry is None:
        key = repo_id.strip().lower()
        for candidate in catalog.values():
            names = {candidate["id"].lower(), (candidate.get("display_name") or "").lower()}
            names |= {a.lower() for a in candidate.get("aliases", [])}
            if key in names:
                entry = candidate
                break
    return to_dict(entry, host=host) if entry else None


def resolve(query: str) -> list[dict[str, Any]]:
    q = query.strip().lower()
    if not q:
        return []
    scored: list[tuple[int, dict[str, Any]]] = []
    for entry in load_catalog().values():
        hay = [entry["id"].lower(), (entry.get("display_name") or "").lower()]
        hay += [a.lower() for a in entry.get("aliases", [])]
        best = None
        for h in hay:
            if h == q:
                best = min(best if best is not None else 100, 0)
            elif h.startswith(q):
                best = min(best if best is not None else 100, 1)
            elif q in h:
                best = min(best if best is not None else 100, 2)
        if best is not None:
            scored.append((best, entry))
    scored.sort(key=lambda t: (t[0], t[1]["id"]))
    return [to_dict(e) for _s, e in scored]


def required_mnemos_queries(repo_id: str) -> list[str]:
    """The MNEMOS searches an agent MUST run before working this repo."""
    entry = get_repo(repo_id)
    if entry is None:
        return []
    mn = entry.get("mnemos") or {}
    if not mn.get("required"):
        return []
    queries = list(mn.get("queries") or [])
    tags = mn.get("tags") or []
    if not queries and tags:
        queries = [" ".join(tags)]
    return queries


def catalog_meta() -> dict[str, Any]:
    p = _catalog_path()
    try:
        mtime = p.stat().st_mtime
    except FileNotFoundError:
        mtime = None
    catalog = load_catalog()
    return {"path": str(p), "count": len(catalog), "mtime": mtime, "generated": _CACHE.get("meta", {})}
