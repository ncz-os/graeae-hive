# zoder review response — round 2 (2026-10-15)

**Reviewer:** deepseek-flash (via `zoder review --base origin/master --scope branch`)
**Reviewed tree:** `wip/hive-eff/2026-10-15` @ `2904b4c`
**Verdict:** `request_changes` ($0.0480, 10 chunks, complete)
**Response commit:** see `git log` for `docs: zoder review round-2 response`

---

## Summary

Round 1 flagged the Spark removal as "looks accidental" and asked for five
small fixes. Commit `2904b4c` documented the operator directive and applied
them. Round 2 re-reviewed the **whole branch diff** (not the round-1 delta),
which re-surfaced several round-1 items now batched under "spark removal" —
**all intentional and now documented** — plus three findings that are
**demonstrably false positives** (reviewer misread the pre-image, not the
delta). Evidence for each is below so the next reviewer can verify rather
than trust.

---

## Disposition

| # | Sev | Finding | Disposition |
|---|-----|---------|-------------|
| 1 | high | Spark reservation removed from `job_agent_eligible` | **Intended** (operator directive 2026-10-15). Doc § "Operator directive". Retiring the path *is* the change; there is no replacement routing policy — the reservation existed only to serve a Spark that is no longer integrated. |
| 2 | high | `note_spark_seen_urn` removed from `heartbeat` + `dequeue_next_job` | **Intended** — same directive. Verified **zero** dangling references (see Evidence A). Both the helper *and* its only call sites were removed together, so there is no "orphaned bookkeeping". |
| 3 | high | `now` deleted from `admin_requeue_jobs` but still referenced → NameError | **FALSE POSITIVE.** `now` was never referenced in that function, on master *or* HEAD (Evidence B). `ruff F841` removed an unused binding. No NameError. |
| 4 | high | `load_script` called with a path containing `/` → invalid module id | **FALSE POSITIVE.** `load_script(name, filename)`; the call passes `name="zeroclaw_worker_under_test"` (valid id) and the path as the *second* arg (Evidence C). Reviewer read the args in the wrong order. |
| 5 | med | `valid_completion_proof` drops `remote_verified`/`remote_sha` | **Intended** — local git evidence is now the sole gate (docstring). No producer or consumer references either field anywhere in the tree (Evidence D). |
| 6 | low | `QUARANTINED_HOSTS` "changed from populated set to empty set" | **FALSE POSITIVE.** Master already had `QUARANTINED_HOSTS = set()` (empty, "all hosts un-quarantined 2026-06-14"); HEAD removes the now-dead constant entirely. No live quarantine existed to disable (Evidence E). |
| 7 | low | `list_enabled_harnesses` defined but not re-exported | **FIXED.** Added `list_enabled_harnesses` + `register_external_adapter` to `hive/harness/__init__.py` imports + `__all__`. |
| 8 | info | Doc could be read as "Phase A fully done" | **FIXED.** Added an explicit "A4/A5 PENDING" callout + ⧗ markers. |
| 9 | low/info | Parenthesized `async with` needs Python ≥3.10 | **Acknowledged, no action.** Diff already uses PEP 604 (`X | None`) throughout, so the 3.10+ floor is a pre-existing project property. |
| 10 | info | Large mechanical reformat dominates chunks | **Expected.** `3c0474a` is the ruff pass; semantic deltas within it are the Spark removal (documented) + F841/F401 removals (verified no-op). |
| 11 | info | `get_harness_adapter` allows arbitrary `module:Class` import | **Acknowledged for Phase D.** No callers today (function is inert); noted in doc that dispatch must validate `harness_kind` against the registry before the fallback. |

---

## Evidence

### A — no dangling references to removed Spark helpers
```
$ for sym in spark_online _job_queued_age note_spark_seen \
      note_spark_seen_urn SPARK_HOSTS SPARK_TAKEOVER_KINDS \
      SPARK_RESERVE_GRACE_SEC; do
    printf '%s: ' "$sym"; grep -rn "$sym" --include='*.py' --include='*.html' . | wc -l
  done
spark_online: 0
_job_queued_age: 0
note_spark_seen: 0
note_spark_seen_urn: 0
SPARK_HOSTS: 0
SPARK_TAKEOVER_KINDS: 0
SPARK_RESERVE_GRACE_SEC: 0
```

### B — `now` unused in `admin_requeue_jobs` (master pre-image)
```
$ git show origin/master:agent_bus.py \
    | sed -n '/async def admin_requeue_jobs/,/return results/p'
async def admin_requeue_jobs(req: RequeueRequest):
    """Force-requeue terminal (failed/cancelled) or any non-done jobs back to queued."""
    now = time.time()                      # <-- bound, never read below
    results = {"ok": [], "not_found": [], "skipped_done": []}
    async with connect_db() as db:
        for job_id in req.job_ids:
            ...
            await db.execute(
                """UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL,
                   retry_count=0, retry_backoff_until=NULL
                   WHERE id=?""",          # <-- no `now` in the UPDATE
                (job_id,),
            )
            ...
    return results
```
The only delta in this function between master and HEAD is `-    now = time.time()`.
`ruff F841` (per `3c0474a` message) removed it because it was dead.

### C — `load_script` argument order
```
def load_script(name, filename):                       # name is arg 1
    spec = importlib.util.spec_from_file_location(name, filename)

worker = load_script("zeroclaw_worker_under_test",     # <- valid identifier
                     "tests/fixtures/zeroclaw_wss_worker_v2.py")  # <- location
```

### D — `remote_verified` / `remote_sha` have no other producers/consumers
```
$ grep -rn "remote_verified\|remote_sha" --include='*.py' --include='*.html' .
./agent_bus.py:1332:    NOTE (operator directive 2026-10-15): the `remote_verified` / `remote_sha`
```
Only the explanatory docstring matches. Nothing writes or reads the fields.

### E — `QUARANTINED_HOSTS` was already empty on master
```
$ git show origin/master:agent_bus.py | grep "QUARANTINED_HOSTS ="
QUARANTINED_HOSTS = set()  # all hosts un-quarantined 2026-06-14: ...
$ grep -rn "QUARANTINED_HOSTS" --include='*.py' . | wc -l
0
```

---

## Next
- Re-run `zoder review` on the round-2 response commit.
- If verdict is `approve` (or only the documented-intended items remain),
  merge `wip/hive-eff/2026-10-15` → `master` and proceed to Phase B.
