# Lineage Reconciliation: `deploy/integrate-20261017` vs `deploy/hive-canonical-20261029`

**Date:** 2026-10-29
**Purpose:** A read-only reconciliation of the two live deploy lineages of
`graeae-hive`, answering *what differs, what lives where, what is intended vs
drift, and what a single converged branch would have to contain*. This is an
**analysis and a proposal — it is not a merge**, and no branch was modified.

The two lines have diverged since June 2026. One is the **bus + doctor**
(PYTHIA's `/srv/agent-bus` runs it); the other is the **worker** (the fleet's
GitOps reconciler deploys `hive_worker.py` and the `hive/*.py` modules from it).
Both still share the same persistence/queue core, which is why they *look* like
two versions of one repo even though they now carry hundreds of commits each.

## Tips (date-stamped)

```
$ git rev-parse deploy/integrate-20261017
1b7a986cf3892a4f3719712dcc33212024f490a3     # A -- the bus + doctor line
$ git show -s --format=%ci deploy/integrate-20261017
2026-10-29 16:57:37 -0400

$ git rev-parse origin/deploy/hive-canonical-20261029
d096a7bc437d478c1bb743511fc3123543a8b062     # B -- the worker line
$ git show -s --format=%ci origin/deploy/hive-canonical-20261029
2026-10-29 16:36:47 -0400

$ git merge-base deploy/integrate-20261017 origin/deploy/hive-canonical-20261029
184afb7379930a2807a69d3dbeb12715ac5b485b     # shared ancestor, 2026-06-14
$ git rev-list --count 184afb73..deploy/integrate-20261017            # A: 397 commits
$ git rev-list --count 184afb73..origin/deploy/hive-canonical-20261029 # B: 191 commits
```

## How to re-run this analysis

The whole analysis is reproducible with `scripts/lineage_diff.py` (deliverable 2).
Every row below came from exactly these commands, run from the repo root:

```
# Per-file classification table (one row per path in either tip):
python3 scripts/lineage_diff.py deploy/integrate-20261017 origin/deploy/hive-canonical-20261029

# Machine-readable (JSON) -- used to produce the summaries in this doc:
python3 scripts/lineage_diff.py deploy/integrate-20261017 origin/deploy/hive-canonical-20261029 --json

# Commit counts since the merge-base (for the header):
git rev-list --count 184afb7379930a2807a69d3dbeb12715ac5b485b..deploy/integrate-20261017
git rev-list --count 184afb7379930a2807a69d3dbeb12715ac5b485b..origin/deploy/hive-canonical-20261029

# A single file's content delta, e.g. the biggest one:
git diff --stat deploy/integrate-20261017 origin/deploy/hive-canonical-20261029 -- agent_bus.py
```

The script classifies by **blob object id** read from each tip's tree
(`git ls-tree -r`), so the row set is the **union of both tips** — files that are
identical on both sides appear as `same`, which a plain tip-vs-tip `git diff`
never shows. It is read-only: neither ref, the index, nor the working tree is
touched, and no object is written.

**Totals at the tips above:** 327 paths — `same=6`, `differs=19`,
`only-A=214`, `only-B=88`.

---

## 1. Files that differ in content (present on both tips) — 19

| Path | A (bus) vs B (worker) | Reading |
|---|---|---|
| `.gitignore` | +6 −7 | drift |
| `agent_bus.py` | +356 −4624 | intended (bus) — see §4 |
| `bs_audit.py` | +6 −6 | drift |
| `dashboard.html` | +327 −1306 | intended (bus) |
| `hive-auto-restore.py` | +13 −13 | drift |
| `hive-auto-restore.service` | +2 −0 | intended (bus) |
| `hive-auto-restore.timer` | +8 −7 | intended (bus) |
| `hive/persistence/_sqlite_async.py` | +2 −3 | drift |
| `hive/persistence/base.py` | +150 −150 | **drift — shared core, both edited** |
| `hive/persistence/db2.py` | +204 −205 | **drift — shared core** |
| `hive/persistence/factory.py` | +14 −14 | **drift — shared core** |
| `hive/persistence/oracle.py` | +164 −164 | **drift — shared core** |
| `hive/persistence/sqlite.py` | +2039 −340 | **drift — shared core, largest** |
| `mcp_bridge.py` | +156 −157 | intended (bus) |
| `migrations/2026-06-03-jobs-dedup.sql` | +3 −3 | drift |
| `retry_policy.py` | +14 −13 | **drift — shared core** |
| `system_watcher.py` | +190 −190 | intended (bus) |
| `test_queue_logic.py` | +6 −5 | **drift — shared test, both edited** |
| `zeroclaw_doctor.py` | +333 −1330 | intended (bus/doctor) |

`+`/`−` are line counts from A's perspective (lines A adds / removes relative to
B), from `git diff --stat B A`.

## 2. Files on only one side

**Only on A (the bus line) — 214 paths**, the bulk of it the entire
`hive/bus/**` service tree, `hive/workers/doctor*`, `hive/harness/**`,
`hive/core/**`, `hive/registry.py`, `hive/repos.py`, the dashboard
(`static/dashboard.{css,js}`, `dashboard.html`), `data/repos*.json`, a large set
of `docs/decision-*.md`, and ~90 bus-side test files (see §3).

**Only on B (the worker line) — 88 paths**, the worker runtime:
`hive_worker.py`, `hive/codex_delivery.py`, `hive/llm_roles.py`,
`hive/push_policy.py`, `hive/review_evidence.py`, `hive_budget.py`,
`provenance.py`, `wss_driver.py`, `zeroclaw_wss_worker_v2.py`, the fleet's
`deploy/hive-worker-deploy.{sh,service,timer}`, per-host
`hosts/*/hive-worker.conf`, the `systemd/hive-worker-*.service` set, and ~48
worker-side test files under `tests/` (see §3). `agent_bus.py.bak2` is a B-only
backup artifact that should not be converged in.

## 3. Where the tests live

* **Only on A (bus/doctor) — 90 test paths.** Notable: `test_doctor_*.py` (the
  whole doctor suite: claim scope, decline paths/registry, DLQ protect,
  failover, grants, idempotency, loop outcomes, orphan sweep, protection review,
  re-submit budget, retry budget, review reconciliation, safety boundary, SOP
  executor, state migrate, triage actions/no-repeat, unclaimable-queued, verdict
  completion), `test_repo_argonas_fallback.py`, `test_repo_registry.py`,
  `test_eih_probe.py`, `test_capacity_ledger.py`, `test_scheduler.py`,
  `test_hive_core.py`, `test_hive_models.py`, `test_hive_secrets.py`,
  `test_worker_fanout.py`, `test_worker_execution_evidence.py`, plus fixtures
  under `tests/fixtures/`.
* **Only on B (worker) — 48 test paths**, all under `tests/` and named
  `test_hive_worker_*` / `test_codex_*` / `test_completion_*`: e.g.
  `test_hive_worker_delivery_integrity.py`, `test_hive_worker_argonas_only_push.py`,
  `test_hive_worker_argonas_preflight.py`, `test_hive_worker_provenance*.py`,
  `test_hive_worker_lease.py`, `test_hive_worker_sandbox_containment.py`,
  `test_hive_worker_workspace.py`, `test_hive_worker_finalizer.py`,
  `test_codex_worker_delivery.py`, `test_completion_evidence.py`,
  `test_provenance_resolver.py`, `test_review_evidence.py`,
  `test_hive_llm_roles.py`.
* **On both but different — 1:** `test_queue_logic.py` (a shared core test the
  two lines edited differently — drift).

The two sides' tests are almost entirely **disjoint** (different namespaces:
bus/doctor tests at the repo root, worker tests under `tests/`). That disjointness
is the good news for convergence: very few tests collide.

## 4. Intended differences vs drift

**Intended (each line's real job):**
* A owns the **bus + doctor**: `hive/bus/**`, `agent_bus.py`,
  `zeroclaw_doctor.py`, `hive/workers/doctor*`, `hive/harness/**`,
  `mcp_bridge.py`, `dashboard.html`, `system_watcher.py`, `hive/registry.py`,
  `hive/repos.py`, the `hive-auto-restore.*` trio, and `data/repos*.json`.
* B owns the **worker**: `hive_worker.py`, `hive/codex_delivery.py`,
  `hive/llm_roles.py`, `hive/push_policy.py`, `hive/review_evidence.py`,
  `hive_budget.py`, `provenance.py`, `wss_driver.py`,
  `zeroclaw_wss_worker_v2.py`, and the `deploy/hive-worker-*` /
  `hosts/*/hive-worker.conf` / `systemd/hive-worker-*` deploy surface.

These are two products that happen to share a core; they are *supposed* to
differ here.

**Drift (the same file changed differently, or a change that should have been on
both sides) — the part a convergence has to resolve, file by file:**
* **The shared persistence core:** `hive/persistence/{base,db2,factory,oracle,
  sqlite,_sqlite_async}.py`. Both lines edited these independently; `sqlite.py`
  diverged most (+2039/−340). This is the highest-risk overlap — two different
  evolutions of the same storage layer.
* **`retry_policy.py`, `test_queue_logic.py`, `bs_audit.py`,
  `migrations/2026-06-03-jobs-dedup.{sql,md}`** — small shared files each side
  touched differently.
* **`.gitignore`** — trivial, take the union.
* **`hive-auto-restore.py/.service/.timer`** — present on both but diverged;
  confirm whether both lines intend to run the auto-restorer.

## 5. What a single converged branch would have to contain (proposal)

A converged branch is the **union of both products plus a reconciled shared
core**, not a merge of history. Concretely:

1. **Keep the shared core on ONE reconciled version.** Pick the superset of the
   persistence changes (or re-derive them) so `hive/persistence/*`,
   `retry_policy.py`, `test_queue_logic.py`, `bs_audit.py`, and the
   `2026-06-03-jobs-dedup` migration exist **once**, with both lines' behavior
   preserved. This is the only real conflict surface.
2. **Keep A's product tree:** `hive/bus/**`, `agent_bus.py`,
   `zeroclaw_doctor.py`, `hive/workers/**`, `hive/harness/**`, `hive/core/**`,
   `hive/registry.py`, `hive/repos.py`, `mcp_bridge.py`, `dashboard.html`,
   `static/*`, `system_watcher.py`, `hive-auto-restore.*`, `data/*.json`,
   `pyproject.toml`, and the A-only `docs/` and bus tests.
3. **Keep B's product tree:** `hive_worker.py`, `hive/codex_delivery.py`,
   `hive/llm_roles.py`, `hive/push_policy.py`, `hive/review_evidence.py`,
   `hive_budget.py`, `provenance.py`, `wss_driver.py`,
   `zeroclaw_wss_worker_v2.py`, `deploy/hive-worker-*`, `hosts/*/hive-worker.conf`,
   `systemd/hive-worker-*`, and the B-only `tests/` worker suite.
4. **Keep the 6 files already identical** (`hive/__init__.py`,
   `hive/persistence/__init__.py`, `hive/review_record.py`,
   `migrations/2026-06-03-jobs-dedup.md`, `queue_logic.py`, `repository.py`) as-is.
5. **Drop B's `agent_bus.py.bak2`** (a backup artifact, not a deliverable).
6. **Union `.gitignore`**; reconcile the two `conftest.py` (root vs `tests/`)
   so the combined test tree collects cleanly.

The converged branch would carry **both** deploy surfaces (`deploy/hive_worker.py`
from A and `deploy/hive-worker-deploy.*` from B) so the bus and the worker can
ship from one ref; the operator decides the actual cut.

> This is a **proposal only**. No merge, rebase, cherry-pick, or push to either
> deploy branch was performed; only this branch (`freebuff/g2-20261029`) was
> written.
