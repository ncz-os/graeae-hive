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

## Refs

- `hive/registry.py` (discovery + merge), `hive/repos.py` (load + query)
- `scripts/build_repo_registry.py`, `data/repos_overlay.json`, `data/repos.json`
- `test_repo_registry.py` (12 tests)
