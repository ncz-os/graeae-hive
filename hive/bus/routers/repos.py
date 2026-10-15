"""Repo-registry endpoints (Phase 1b router extraction).

Self-contained: depends only on the repo registry, not the bus's DB/DI. Served
under /v1/repos.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from hive import repos as repo_registry

router = APIRouter()


@router.get("/v1/repos/resolve")
async def resolve_repos(q: str):
    """Fuzzy-resolve a project name to catalog entries (id/alias/display_name)."""
    try:
        matches = repo_registry.resolve(q)
    except FileNotFoundError as e:
        raise HTTPException(503, f"repo catalog unavailable: {e}") from e
    return {"query": q, "matches": matches}


@router.get("/v1/repos")
async def list_repos(
    status: str | None = None,
    language: str | None = None,
    owner: str | None = None,
    tag: str | None = None,
    source: str | None = None,
    host: str | None = None,
):
    """List repos in the registry. `host` adds per-host load instructions."""
    try:
        repos = repo_registry.list_repos(
            status=status, language=language, owner=owner, tag=tag, source=source, host=host
        )
    except FileNotFoundError as e:
        raise HTTPException(503, f"repo catalog unavailable: {e}") from e
    return {"count": len(repos), "repos": repos, "meta": repo_registry.catalog_meta()}


@router.get("/v1/repos/{repo_id}")
async def get_repo(repo_id: str, host: str | None = None):
    """One repo: location, remotes, forges, Buildkite pipeline, load recipe
    (when `host` is given), and the REQUIRED MNEMOS queries to run first."""
    try:
        entry = repo_registry.get_repo(repo_id, host=host)
    except FileNotFoundError as e:
        raise HTTPException(503, f"repo catalog unavailable: {e}") from e
    if entry is None:
        raise HTTPException(404, f"repo not found: {repo_id!r}")
    return entry  # includes required_mnemos_queries (emitted by to_dict)
