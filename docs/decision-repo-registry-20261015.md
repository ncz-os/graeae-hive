# Decision: Hive repo registry (project catalog)

**Status:** Accepted (2026-10-15)
**Owner:** opencode (operator-directed)
**Related:** `docs/decision-mcp-a2a-plane-20261015.md`, `docs/refactor-proposal-harness-agnostic-20261015.md`

---

## Problem

Agents had no machine-queryable answer to "where does repo X live and how do I
load it?". The only source was a hand-maintained markdown file
(`~/.claude/PROJECT_LOCATIONS.md`, last refreshed 2026-05-24, STUDIO-only, not
queryable, stale). Every agent re-derived locations by guessing paths or
sweeping the LAN. There was no required-memory step, so agents re-solved
already-solved problems.

## Decision

The hive maintains a **repo registry** built from the real, live sources and
served by the bus + MCP. It is **discovered, not hand-maintained**, with a small
curated overlay for what cannot be discovered.

Sources (correlated by repo slug):

| Source | Auth | Yields |
|---|---|---|
| GitLab API | PAT | 64 projects (namespace, URL, visibility, default branch, activity) |
| GitHub API | PAT | 41 repos (owner, URL, visibility, default branch, pushed_at) |
| ARGONAS (`ls *.git`) | ssh | canonical bare repos present on the NAS (81) |
| Buildkite API | token | CI pipelines per repo (35) |
| Cloudflare R2 API | token | object-storage buckets (2: `mnemos`, `ncz-apt`) |

Curated **overlay** (`data/repos_overlay.json`) adds what discovery cannot know:
live deploy host/path/service, build/test/lint commands, **required MNEMOS
queries**, deps, notes, display name/aliases.

Result: `data/repos.json` (built) → served read-only.

## Architecture

```
gitlab(PAT) github(PAT) argonas(ssh) buildkite(token) r2(token)
        │
        ▼
scripts/build_repo_registry.py   (build-time; hive/registry.py)
        │  discover → correlate by slug → merge overlay
        ▼
data/repos.json                  (artifact; committed, served)
        │
hive/repos.py  ──►  GET /v1/repos, /v1/repos/resolve, /v1/repos/{id}
        │
mcp_bridge.py  ──►  hive_repo_list, hive_repo_get, hive_repo_context
```

Deliberately **file-backed, not a DB table**: the catalog is reference data
that changes rarely and is reviewed like code. A per-backend migration across
SQLite/Oracle/DB2 buys nothing here and the live backend is Oracle. Discovery
hits external APIs, so it is a build-time step — the bus only reads the
artifact (fast, no PATs in the request path).

## The value-add: `hive_repo_context`

One call returns a repo's location/load recipe **plus** the results of its
required MNEMOS searches. This is the operator's "full project awareness
including required mnemos memory search" — the agent's mandated pre-flight.

## Per-host load recipe

`GET /v1/repos/{id}?host=<h>` returns `load_for_host`:
- host **is** a live-deploy host → `{mode: live, path, service}` (edit in place);
- otherwise → `{mode: clone, ssh, nfs, default_branch}` (clone from ARGONAS).

## `PROJECT_LOCATIONS.md` becomes generated

The markdown file is now downstream of this catalog. A `--format md` emitter is
a follow-up; until then the catalog is authoritative and the file is a stale
mirror to be regenerated.

## Out of scope / follow-ups

- Credentials: read from env first, then `~/.api_keys_master.json`; never in git.
  (The catalog output contains no secrets — names/URLs/visibility only.)
- Rebuild cadence: run `scripts/build_repo_registry.py` on a timer or Buildkite
  pipeline; commit the refreshed `data/repos.json`.
- Runtime state (last-seen commit, health) can layer on later if needed.
- `PROJECT_LOCATIONS.md --format md` emitter.

## Late-binding: repos created after the build (`hive/repos.py`)

The catalog is an artifact, so a repo created on ARGONAS after a build has no
row — and jobs for it failed with `no_repo_mapping` / `no_private_remote` at the
worker. There is no add-repo API, and on the bus host `data/repos.json` is
root-owned, so "just rebuild or edit it" is not available to the service. The
bus therefore answers from ARGONAS itself on a **catalog MISS only**:

- `_raw_entry` (behind `get_repo` → `GET /v1/repos/{id}`) and `find_exact` fall
  back to a *synthesized* entry when the bare repo `<slug>.git` exists under the
  ARGONAS git root **as the bus host sees it** (`HIVE_ARGONAS_GIT_ROOT`,
  default `/mnt/argonas_git`). Fields: `id`/`display_name` = the slug,
  `aliases: []`, `sources: ["argonas-live"]`, `argonas_bare`, `clone_ssh` and
  `clone_nfs` built from the `hive/registry.py` constants (one spelling only —
  never a second copy of the user/host/path), `status: "active"`, and
  `default_branch` read from the bare repo's `HEAD`
  (`ref: refs/heads/<name>`, validated) — `master` when it is unreadable,
  detached or malformed.
- The slug comes from a **job string**, so it is treated as hostile:
  `^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`, no `.git` suffix, no `..`; the test is
  `os.path.isdir(root/slug + ".git")` and a bare dir whose realpath escapes the
  realpath of the root is refused (a planted symlink cannot redirect a worker's
  push).
- Results are cached in a small locked dict: positives 60 s, negatives 10 s. The
  fallback never raises (an unreadable mount is a miss) and costs one `stat` call
  per lookup; it runs lock-free so a stalled mount cannot serialize the bus.
- **A catalog entry always wins** — the fallback is a MISS path, never an
  override, so curated fields (forge facets, branch, commands, live host) can
  never be replaced by synthesized ones.
- `list_repos` and `resolve` stay **catalog-only**: they are an index over an
  artifact, and enumerating the mount would turn a dashboard poll into a network
  filesystem sweep. A late repo is reachable by id, not by listing.
- `find_exact` feeds `hive.bus.domain.jobs.repo_in_catalog`, so a
  `no_repo_mapping` decline for a late repo now counts as *stale client* drift
  (requeue/route) instead of a genuinely-absent repo burning the dead-letter
  budget. That is intended and noted in the docstring.
- Error and no write path: the synthesized entry only advertises a repo that is
  already there; nothing is created or edited.

Tests: `test_repo_argonas_fallback.py` (hit/miss, hostile slugs, symlink escape,
catalog-wins, unreadable root, negative-cache expiry, and a contrast run against
the pre-fallback `hive/repos.py`).

## Refs

- `hive/registry.py` (discovery + merge), `hive/repos.py` (load + query)
- `scripts/build_repo_registry.py`, `data/repos_overlay.json`, `data/repos.json`
- `test_repo_registry.py` (12 tests), `test_repo_argonas_fallback.py` (after-the-build repos)
