# Investigation: `no_repo_mapping` (and the job-release thrash class)

**Date:** 2026-10-16  **Owner:** opencode  **Status:** root-caused; thrash fixed, worker resolution fix pending

## Symptom

Jobs fail / re-queue with `result.worker_error = "no_repo_mapping"`. Live example
observed this session:

```
id   01a146dc-e19c-72b6-8163-ffdc71b5c8c0
kind investigation:magnetar-installer-pool-online
status queued   retry_count 5   result {"worker_error":"no_repo_mapping","via":"hive_worker"}
```

A `retry_count` that keeps climbing with the job stuck in `queued` is the tell:
the job is **thrashing** (claimed → declined → requeued) rather than terminating.

## Where it is emitted

`hive_worker.py` (deployed `/opt/graeae/hive_worker.py`), the thin
claim/execute worker:

```python
repo_url = resolve_repo_url(job)
if not repo_url:
    release_job(job_id, "no_repo_mapping")   # -> PATCH status=queued + worker_error
```

`resolve_repo_url()` resolves **only** against a hardcoded `REPO_MAP` (17 repos)
plus three URL-normalization regexes (`_REPO_URL_RES`) that still require the
extracted slug to already be a `REPO_MAP` key:

```python
def resolve_repo_url(job):
    repo = (job.get("repo") or "").strip()
    if not repo: return None
    if repo in REPO_MAP: return REPO_MAP[repo]
    for rx in _REPO_URL_RES:
        m = rx.match(repo)
        if m and m.group(1) in REPO_MAP: return REPO_MAP[m.group(1)]
    return None
```

The worker emits exactly three release reasons — `no_repo_mapping`,
`malformed_job_id`, `invalid_head_branch` — all job-level unschedulable.

## Root causes

### 1. The worker ignores the PAT-generated repo index (primary)

The bus already builds and serves the authoritative, auto-updating repo catalog:

- build: `hive/registry.py:build_registry()` (gitlab PAT + github token + ARGONAS +
  buildkite + R2) → `data/repos.json` (built by `scripts/build_repo_registry.py`).
- serve: `GET /v1/repos`, `GET /v1/repos/{id}`, `GET /v1/repos/resolve?q=`.

Live: **123 repos** (sources `argonas, buildkite, github, gitlab, overlay`).
Worker `REPO_MAP`: **17**. So every repo not hand-added to `REPO_MAP` (and every
newly-created repo) is invisible to workers until someone edits the worker and
redeploys — which is exactly what the PAT index was meant to end.

### 2. Repo-less / noncommit kinds can never run

The bus classifies `investigation`/`analysis`/`review`/`triage`/`docs:` as
noncommit (`hive/bus/domain/workspace.py:ADMISSION_NONCOMMIT_PREFIXES`) — no repo
required at admission. But the worker **hard-requires** a repo for every job, so a
repo-less noncommit kind (like the magnetar `investigation:` job, `repo=null`)
is declined by every worker and can never complete. The two components disagree.

### 3. Requeued jobs never dead-lettered (FIXED — `f2ceeb9`)

The bus release path increments `retry_count` + backoff on **every** release, but
only increments `decline_count` (→ dead-letter at `MAX_DECLINE_REQUEUES=3`) when
`worker_error ∈ DECLINE_REASON_PREFIXES`. That tuple held only
`no_workspace_for_kind`/`no_workspace_for_repo` — **none** of the worker's three
reasons. Result: job-level-unschedulable jobs requeue forever (magnetar hit
`retry_count=5`). Fix adds the three reasons + `is_unresolvable_release()`.

### 4. Process: the worker is not on `master`

`hive_worker.py` exists only on deploy branches (`feat/hive-worker-finalizer-20261004`,
`deploy/hive-canonical-20261003`, `wip/studio/2026-06-15-register-dedup`), never on
`master`; and the **live** file's blob (`bf7a8eb4…`) matches **no committed blob**
— so the running worker has on-host drift. Fixes to the worker cannot land cleanly
until it is reconciled onto a tracked branch and redeployed.

## Impact

- Real work (esp. `investigation:`/`analysis:` jobs) silently bounces and never
  runs; orchestrators see "queued" not "failed".
- Wasted claim cycles + backoff churn; no dead-letter signal.
- Repo catalog investment (PAT index) is unused by the worker.

## Fixes

| # | Fix | State |
|---|-----|-------|
| 3 | count the three worker reasons toward dead-letter (`is_unresolvable_release`) | **done** `f2ceeb9` |
| 1 | worker resolves via `GET $HIVE_URL/v1/repos/{id}` (cached, TTL) with `REPO_MAP` as offline fallback; keep the "URL must come from the trusted catalog, never the job string" guarantee | recommended |
| 2 | noncommit kinds (bus's `ADMISSION_NONCOMMIT_PREFIXES`) run in a scratch workspace when `repo` is empty; skip clone/push in the finalizer | recommended |
| 4 | land `hive_worker.py` on `master` (or a single tracked deploy branch) and reconcile the live file | recommended |

## Verify

```sh
# live: is a job thrashing? (retry climbing + status queued + worker_error)
curl -s http://192.0.2.67:5005/v1/jobs?limit=50 |
  python3 -c 'import sys,json;[print(j["id"],j["status"],j.get("retry_count"),j.get("result")) for j in json.load(sys.stdin)["jobs"]]'

# the PAT index the worker should use:
curl -s 'http://192.0.2.67:5005/v1/repos/resolve?q=graeae-hive'
```

## Refs

- `hive/bus/routers/jobs.py` (release path), `hive/bus/domain/jobs.py`
  (`DECLINE_REASON_PREFIXES`, `is_unresolvable_release`), `test_decline_reasons.py`
- `hive/registry.py`, `scripts/build_repo_registry.py`, `data/repos.json`
- `hive/bus/domain/workspace.py` (`ADMISSION_NONCOMMIT_PREFIXES`)
- worker: `hive_worker.py` on `feat/hive-worker-finalizer-20261004`
