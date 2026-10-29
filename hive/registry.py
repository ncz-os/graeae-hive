"""Repo-registry discovery — build the catalog from the real sources.

Five discovery sources, correlated by repo slug:

  forge-gitlab   GitLab API (PAT)      -> projects (namespace, path, visibility, branch)
  forge-github   GitHub API (PAT)      -> repos    (owner, name, visibility, branch)
  argonas        ssh `ls *.git`        -> canonical bare repos present on the NAS
  buildkite      Buildkite API (token) -> CI pipelines per repo
  r2             Cloudflare R2 API     -> object-storage buckets

Discovered FACTS are merged with a curated OVERLAY (``data/repos_overlay.json``:
live host, build/test commands, required MNEMOS queries, deps, notes) into
``data/repos.json``, which the bus/MCP serve. Discovery hits external APIs and
is therefore a BUILD-TIME step (``scripts/build_repo_registry.py``), not a
per-request path — the bus only reads the built artifact.

Credentials never live in git: read from env first, then ``~/.api_keys_master.json``.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from hive.core import fleet_inventory
from hive.core.config import get_settings

_API_KEYS = Path.home() / ".api_keys_master.json"

# ARGONAS canonical bare-repo location, in both spellings. The directory on the
# NAS is /mnt/datapool/git; /mnt/argonas_git is the NFS mount of the SAME dir on
# other hosts. Derived from these constants so argonas_bare / clone_ssh /
# clone_nfs can never drift apart (review finding).
ARGONAS_SSH_USER = "jasonperlow"  # documented clone user (ssh://jasonperlow@...); needs a key
# ARGONAS's address is operator config (operator rule 2026-10-29), never source:
# set ARGONAS_SSH_HOST. Unset skips ARGONAS discovery and emits no clone recipe
# rather than publishing a URL that points nowhere.
ARGONAS_SSH_HOST = os.environ.get("ARGONAS_SSH_HOST", "")
ARGONAS_GIT_PATH_SSH = "/mnt/datapool/git"  # real directory on the NAS
ARGONAS_GIT_PATH_NFS = "/mnt/argonas_git"  # NFS mount spelling of the same dir
ARGONAS_DISCOVER_USER = "root"  # proven user for the NAS listing (password/sshpass)


def argonas_ssh_url(slug: str) -> str:
    """The ARGONAS clone URL, or ``""`` when ARGONAS_SSH_HOST is not configured."""
    if not ARGONAS_SSH_HOST:
        fleet_inventory.warn_once(
            "registry.argonas.host",
            "ARGONAS_SSH_HOST is not set: no ARGONAS clone URL can be built",
        )
        return ""
    return f"ssh://{ARGONAS_SSH_USER}@{ARGONAS_SSH_HOST}{ARGONAS_GIT_PATH_SSH}/{slug}.git"


def argonas_nfs_path(slug: str) -> str:
    return f"{ARGONAS_GIT_PATH_NFS}/{slug}.git"


# --------------------------------------------------------------------------- #
# credential + http helpers
# --------------------------------------------------------------------------- #
def _keys() -> dict[str, Any]:
    try:
        return json.loads(_API_KEYS.read_text())
    except Exception:
        return {}


def _http_json(url: str, headers: dict[str, str]) -> tuple[int, Any]:
    req = urllib.request.Request(url, headers={**headers, "User-Agent": "hive-registry"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:  # noqa: S310
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def slugify(name: str) -> str:
    """Canonical correlation key: last path segment, strip .git, lowercase."""
    n = name.rstrip("/").split("/")[-1]
    if n.endswith(".git"):
        n = n[:-4]
    return n.strip().lower()


# --------------------------------------------------------------------------- #
# sources
# --------------------------------------------------------------------------- #
def discover_gitlab(pat: str, base: str = "https://gitlab.com") -> dict[str, dict[str, Any]]:
    """All projects the PAT is a member of, paginated. Keyed by slug."""
    out: dict[str, dict[str, Any]] = {}
    page = 1
    while True:
        url = f"{base}/api/v4/projects?membership=true&per_page=100&simple=true&order_by=name&page={page}"
        status, body = _http_json(url, {"PRIVATE-TOKEN": pat})
        if status != 200 or not isinstance(body, list) or not body:
            break
        for p in body:
            slug = slugify(p["path_with_namespace"])
            out.setdefault(slug, {})["gitlab"] = {
                "path_with_namespace": p["path_with_namespace"],
                "url": p.get("web_url"),
                "ssh_url_to_repo": p.get("ssh_url_to_repo"),
                "http_url_to_repo": p.get("http_url_to_repo"),
                "visibility": p.get("visibility"),
                "default_branch": p.get("default_branch"),
                "last_activity_at": p.get("last_activity_at"),
            }
        if len(body) < 100:
            break
        page += 1
    return out


def discover_github(token: str, user: str | None = None) -> dict[str, dict[str, Any]]:
    """Owner + org repos for the token. Keyed by slug."""
    out: dict[str, dict[str, Any]] = {}
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

    def _ingest(repos: list[dict[str, Any]]) -> None:
        for r in repos:
            slug = slugify(r["full_name"])
            out.setdefault(slug, {})["github"] = {
                "full_name": r["full_name"],
                "url": r.get("html_url"),
                "ssh_url": r.get("ssh_url"),
                "clone_url": r.get("clone_url"),
                "private": r.get("private"),
                "default_branch": r.get("default_branch"),
                "pushed_at": r.get("pushed_at"),
            }

    for page in range(1, 11):
        status, body = _http_json(
            f"https://api.github.com/user/repos?per_page=100&affiliation=owner&page={page}",
            headers,
        )
        if status != 200 or not isinstance(body, list) or not body:
            break
        _ingest(body)
        if len(body) < 100:
            break
    return out


def discover_argonas(
    *,
    host: str | None = None,
    user: str = ARGONAS_DISCOVER_USER,
    password: str | None = None,
    git_dir: str = ARGONAS_GIT_PATH_SSH,
) -> set[str]:
    """Canonical bare repos present on ARGONAS (`ls *.git`). Returns slugs."""
    host = host or ARGONAS_SSH_HOST
    if not host:
        fleet_inventory.warn_once(
            "registry.argonas.discover",
            "ARGONAS_SSH_HOST is not set: ARGONAS discovery is skipped",
        )
        return set()
    ssh = [
        "ssh",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        f"{user}@{host}",
        f"ls -d {git_dir}/*.git 2>/dev/null",
    ]
    cmd = ssh
    if password:
        cmd = ["sshpass", "-p", password, *ssh]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return set()
    slugs = set()
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.endswith(".git"):
            slugs.add(slugify(line))
    return slugs


def discover_buildkite(token: str, org: str = "ncz-os") -> dict[str, dict[str, Any]]:
    """Buildkite pipelines for an org. Keyed by repo slug (from the pipeline repo URL)."""
    out: dict[str, dict[str, Any]] = {}
    headers = {"Authorization": f"Bearer {token}"}
    for page in range(1, 11):
        status, body = _http_json(
            f"https://api.buildkite.com/v2/organizations/{org}/pipelines?per_page=100&page={page}",
            headers,
        )
        if status != 200 or not isinstance(body, list) or not body:
            break
        for p in body:
            repo = p.get("repository") or ""
            slug = slugify(repo) if repo else slugify(p.get("slug", ""))
            out.setdefault(slug, {})["buildkite"] = {
                "org": org,
                "pipeline": p.get("slug"),
                "repository": repo,
                "default_branch": p.get("default_branch"),
                "web_url": p.get("web_url"),
            }
        if len(body) < 100:
            break
    return out


def discover_r2(account_id: str, token: str) -> list[dict[str, Any]]:
    """Cloudflare R2 buckets (account-scoped)."""
    status, body = _http_json(
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets",
        {"Authorization": f"Bearer {token}"},
    )
    if status != 200 or not isinstance(body, dict):
        return []
    return body.get("result", {}).get("buckets", [])


# --------------------------------------------------------------------------- #
# correlate + merge
# --------------------------------------------------------------------------- #
def correlate(*sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Merge slug-keyed source maps into one entry per slug."""
    merged: dict[str, dict[str, Any]] = {}
    for src in sources:
        for slug, facts in src.items():
            merged.setdefault(slug, {"id": slug, "sources": []})
            for facet, value in facts.items():
                merged[slug][facet] = value
                if facet not in merged[slug]["sources"]:
                    merged[slug]["sources"].append(facet)
    return merged


def merge_overlay(entries: dict[str, dict[str, Any]], overlay: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Apply curated overlay fields onto discovered entries.

    Overlay keys win for curated facets (commands/live/mnemos/deps/notes/etc.);
    discovered facets (forges/argonas/buildkite) are preserved.
    """
    for slug_key, extra in overlay.items():
        slug = slugify(slug_key)
        entry = entries.setdefault(slug, {"id": slug, "sources": []})
        entry["id"] = slug
        for key, value in extra.items():
            entry[key] = value
        if "overlay" not in entry["sources"]:
            entry["sources"].append("overlay")
    return entries


def _default_overlay_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "repos_overlay.json"


def build_registry(
    *,
    gitlab_pat: str | None = None,
    github_token: str | None = None,
    buildkite_token: str | None = None,
    r2_account_id: str | None = None,
    r2_token: str | None = None,
    argonas_password: str | None = None,
    overlay_path: str | os.PathLike | None = None,
    skip: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Run every enabled source, correlate with the overlay, return the registry."""
    keys = _keys()
    gl = keys.get("gitlab", {})
    gh = keys.get("github", {})
    bk = keys.get("buildkite", {})
    cf = keys.get("cloudflare", {})
    settings = get_settings()

    gitlab_pat = gitlab_pat or settings.gitlab_pat or gl.get("ncz_os_pat")
    github_token = github_token or settings.github_pat or gh.get("oauth_token_perlowja")
    buildkite_token = buildkite_token or settings.buildkite_api_token or bk.get("api_access_token")
    r2_account_id = r2_account_id or settings.r2_account_id or cf.get("r2_ncz_apt_account_id")
    r2_token = r2_token or settings.cloudflare_r2_api_token or cf.get("r2_ncz_apt_token")
    argonas_password = argonas_password or settings.argonas_ssh_pass or None

    forge_facts: dict[str, dict[str, Any]] = {}
    argonas_slugs: set[str] = set()
    source_maps: list[dict[str, dict[str, Any]]] = []
    if "gitlab" not in skip and gitlab_pat:
        source_maps.append(discover_gitlab(gitlab_pat))
    if "github" not in skip and github_token:
        source_maps.append(discover_github(github_token))
    if "argonas" not in skip:
        argonas_slugs = discover_argonas(password=argonas_password)
    if "buildkite" not in skip and buildkite_token:
        source_maps.append(discover_buildkite(buildkite_token))
    if source_maps:
        forge_facts = correlate(*source_maps)
    r2_buckets: list[dict[str, Any]] = []
    if "r2" not in skip and r2_account_id and r2_token:
        r2_buckets = discover_r2(r2_account_id, r2_token)

    # Tag ARGONAS presence.
    for slug in argonas_slugs:
        entry = forge_facts.setdefault(slug, {"id": slug, "sources": []})
        entry["argonas_bare"] = argonas_nfs_path(slug)
        entry["argonas_present"] = True
        if "argonas" not in entry["sources"]:
            entry["sources"].append("argonas")

    overlay_file = Path(overlay_path) if overlay_path else _default_overlay_path()
    overlay = json.loads(overlay_file.read_text()) if overlay_file.exists() else {}
    merged = merge_overlay(forge_facts, overlay.get("repos", {}))

    # Fill clone recipes for any entry that has a forge or ARGONAS presence.
    for entry in merged.values():
        if entry.get("argonas_present"):
            entry.setdefault("clone_ssh", argonas_ssh_url(entry["id"]))
            entry.setdefault("clone_nfs", argonas_nfs_path(entry["id"]))

    repos = sorted(merged.values(), key=lambda e: e["id"])
    return {
        "_meta": {
            "generated_by": "hive.registry.build_registry",
            "count": len(repos),
            "sources": sorted({s for e in repos for s in e.get("sources", [])}),
            "r2_buckets": [b.get("name") for b in r2_buckets],
        },
        "repos": repos,
    }
