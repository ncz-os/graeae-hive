# Adversarial review — doctor triage actions (`b335554`)

**Reviewer:** deepseek (author loop, read-only pass)
**Reviewed commit:** `b3355544d661471ce77989330e553b5d720a0fad` ("fix(doctor): act on the failed job, not the triage wrapper")
**Base:** `ad1f717` ("fix(doctor): triage each failed job, and leave protected owners alone")
**Scope:** `hive/workers/doctor.py`, `test_doctor_triage_actions.py`
**Mode:** originally READ-ONLY; the findings below were **fixed** in the follow-up
commit on this branch (see "Resolution" at the end).
**Verdict:** **REQUEST CHANGES** — the four headline fixes are directionally correct
and the new tests pass, but two of them carry false-positive / loop paths that can
re-enter the exact bugs they set out to remove, and a third uses the wrong worker
kind vocabulary. Details and evidence below.

---

## Resolution (follow-up commit)

All findings are addressed; the six findings below are preserved as the original
review record.

| # | Fix |
|---|-----|
| F1 | `_UNSCHEDULABLE_RE` no longer contains `no_repo_mapping`. A new `_is_unschedulable()` classifies it per job through `hive.bus.domain.jobs.repo_in_catalog` — catalog-present (stale worker) falls through to normal triage, catalog-absent escalates. Missing branch / unknown ref still escalate with no LLM call. |
| F2 | `RELEASABLE_KINDS` is now `{zeroclaw, opencode, zoder, claude}` — the fleet harness kinds minus `generic`/`codex`. `doctor` and job kinds remain excluded. |
| F3 | `_match_known_signature` ignores doctor-authored failures (kind `doctor:*`/`triage:*`, or claimed by `_urn`) so the doctor cannot read its own failure as transient; `_release_job()` returns the real bus outcome; `apply_release()` propagates it; the signature-phase `released_to_queue` now tracks the actual call instead of a constant; `main()` still PATCHes terminally when it is False. |
| F4 | Unchanged by request — a no-op resubmit still marks the wrapper done; the summary text says so. |
| F5 | `action_cancel_jobs` pulls children of purged parents into the purge via the bus `parent_job_id` filter, and reports a `PARTIAL:` marker when the API confirms fewer deletes than requested. |
| F6 | Out of scope for this round (failed-scan window); noted above. |

Test evidence: the new cases fail against `b335554`'s `doctor.py` (3 failed, 7
errors) and pass after the fix (14 passed).

---

## What the commit claims, and what it delivers

| # | Claim | Delivered? | Notes |
|---|-------|-----------|-------|
| 1 | `release_to_queue` on a `triage:<kind>` wrapper requeues the **original failed jobs** | Yes | `apply_release()` (`doctor.py:1082-1092`) routes wrappers to `action_resubmit_jobs(base_kind)` instead of releasing the wrapper. Fixes the live bug (wrapper parked in queue, nothing could claim it). |
| 2 | Release kinds filtered to real worker kinds; no codex default | Partly | Filter works (`doctor.py:1057`), but the allowlist is the wrong vocabulary and is self-referential. See F2. |
| 3 | Unschedulable → escalate **without an LLM call** | Partly | The short-circuit is real (placed before the LLM at `doctor.py:1314`), but the matcher over-matches. See F1. |
| 4 | `cancel_jobs` purges failed jobs by explicit ids | Yes (contract verified) | `POST /v1/jobs/purge` with `{ids, status:"failed"}` deletes `failed` rows; confirmed end-to-end against a temp sqlite DB. See F5 for the annotations. |

---

## Findings

### F1 — HIGH — `no_repo_mapping` is a **false** unschedulable match (retryable → escalated)

`hive/workers/doctor.py:1038-1042` (pattern) and `:1314-1318` (match site)

```
r"Remote branch \S+ not found|couldn't find remote ref|no_repo_mapping"
r"|unknown repo|could not resolve repo"
```

The repo's own classifier says `no_repo_mapping` is **deployment-dependent**, not
job-level: `hive/bus/domain/jobs.py:155-156` returns `not repo_in_catalog(job_repo)`,
and `test_decline_reasons.py:44-48` asserts that `no_repo_mapping` on a
**catalog-present** repo is a **stale worker** whose job "must be requeued/routed,
never killed". The new regex ignores the repo entirely and escalates it.

This is fatal on the primary path, because `action_resubmit_jobs()` is itself the
thing that parks the job back on the queue with that reason. Verified in-process:

```
'{"worker_error": "no_repo_mapping:graeae-hive"}' -> MATCH: no_repo_mapping
```

Sequence: triage releases the job (claim #1) → `apply_release` → `action_resubmit_jobs`
→ bus requeues with `no_repo_mapping` (catalog-present, so no decline count) → a
stale worker declines it again (claim #2) → `_match_known_signature` now sees
`no_repo_mapping` in `failed[]` and returns `escalate, confidence 1.0` → the wrapper
is marked **done** and **no further action is taken**. The doctor stops retrying
and reports "the submitter must create the branch or map the repo", which is
false — the repo is catalog-present. It also overrides `fetch_failed_jobs_for_kind`,
whose prefix match (`doctor.py:537`) can pull in already-escalated history.

Also note the match is on `search_text` which includes `job["description"]`
(`doctor.py:1305`) plus up to 8 failed results (`:1307-1313`) — arbitrary job text,
not just structured error fields.

**Fix direction:** match `no_repo_mapping` only when the job's repo is absent from
the catalog (reuse `hive.bus.domain.jobs.is_unresolvable_release(..., job_repo=...)`),
or drop it from the regex and leave it to the LLM. Prefer anchored/structured
fields over free-form description text.

### F2 — MEDIUM — `RELEASABLE_KINDS` is the wrong vocabulary and is self-referential

`hive/workers/doctor.py:1032-1034`, used at `:1057`

```
DEFAULT_RELEASE_KINDS = ["zeroclaw", "opencode"]
RELEASABLE_KINDS = frozenset(DEFAULT_RELEASE_KINDS)
```

`RELEASABLE_KINDS` is defined *as* the default list, so the filter can only ever
accept the two hardcoded names. Two problems:

1. **Wrong vocabulary for "real worker kinds".** The fleet's harness kinds are
   `zoder`, `opencode`, `codex`, `claude` (`hive/harness/base.py:46`,
   `hive/harness/registry.py:22-31`, `hive/workers/run.py:218-219`). `zoder` is the
   fleet's primary author (`hive/harness/base.py:7`) and appears directly as an
   agent kind in tests (`test_*.py`: `"zoder"` x5, `"claude"` x2). An LLM answer of
   `["zoder"]` is silently dropped, and the job is broadened to
   `["zeroclaw","opencode"]` (`:1058-1061`) — i.e. the fix routes work *away* from
   the very harness that emitted it.
2. **Exclusion is a whitelist, not a denylist.** The old code excluded `doctor`
   (the documented loop hazard, `:1049-1052`); the new code excludes every unknown
   kind, so a new/fleet-specific harness kind can never be targeted. The docstring
   still describes the old exclude-doctor rule.

**Fix direction:** make the denylist explicit (`{"doctor"}` plus job-level kinds
that cannot be claimed), or derive the allowlist from the live agent registry
(`fetch_agent_registry_summary()["by_kind"]`) instead of a frozen literal.

### F3 — MEDIUM — Loop risk: the doctor releases itself on its own failure text

`hive/workers/doctor.py:1314-1328` (signature phase), `:1092` (`apply_release`),
`:1860-1862` (`main()` honours `released_to_queue`)

The signature phase inspects `failed` results of `base_kind`. For a **directly
claimed** `doctor:*` job, `base_kind` is the doctor's own kind
(`doctor:wakeup`), so the "failed jobs" it inspects are **its own previous
attempts** — whose `result` is the result dict this code wrote. Reproduced with
the declared env (`_UNSCHEDULABLE_RE` unused, `invoke_doctor_agent` set to raise
if called):

```
kind: doctor:wakeup:[fix:hive-one]
stored result: {"exit_code":2,"stdout":"doctor agent failed","stderr":"ws_exception: ConnectionClosed"}
-> action=release_to_queue (transient ws/gateway error)   # LLM NOT called
-> released_to_queue: True
-> release step 1 FAILED code=404 resp={'detail': 'job not found: d1'}
```

Three compounding defects:

1. **False transient match.** `"ConnectionClosed"` / `"ws_exception"` in the
   doctor's *own* failure is treated as a transient worker error and released.
2. **`released: bool` is unconditionally `True`** (`:1092`) regardless of the
   `_http` result. `action_release_to_queue` returns `"release step 1 FAILED
   code=404"` (`:1069-1070`), yet the caller reports `released_to_queue: True`.
3. **Consequence in `main()`** (`:1860`): when `released_to_queue` is truthy the
   job is **never PATCHed to done/failed**. Here the job is still in
   `running` (claimed by this doctor), step 1 404'd, so nothing transitioned it.
   The claim lease eventually expires and the job is offered again →
   `_seen_job_attempts` keyed by `job_id` (`:1528`) escalates on attempt 3, but
   only after burning two more cycles. This is precisely the
   "release→fail→release" burn the loop detector exists to contain.

The same shape hits any *explicit* self-failure: a `triage:` wrapper that fails in
`invoke_doctor_agent` is itself stored as `failed` for the wrapper's `base_kind`
only if kinds collide; for `doctor:*` jobs they always do.

**Fix direction:** exclude doctor-authored kinds/`AGENT_HOST` results from the
signature `search_text`; make `apply_release` return success from the actual HTTP
outcome (parse the summary or return a status), not a constant; and in `main()`
fall back to the terminal patch when the release did not re-queue the job.

### F4 — LOW/MEDIUM — Silent no-op when `action_resubmit_jobs` finds nothing

`hive/workers/doctor.py:1090-1091` → `:974-996`

`apply_release` on a wrapper *always* returns `released=False`, so the wrapper is
PATCHed `done` with whatever text `action_resubmit_jobs` produced. If that text is
`"no failed jobs of base_kind=..."` (`:992`) — e.g. the failures were already
re-tried after triage was submitted (`submit_auto_triage` has no idempotency,
`:1455-1474`) — the triage job is marked **done** and the cluster is silently
dropped. The old code at least left an artifact in the queue. Consider returning
`no_action`/`escalate` and surfacing `failed_count_seen == 0` (`:1667`).

### F5 — LOW — `cancel_jobs` is an irreversible hard delete, and the annotation is misleading

`hive/workers/doctor.py:1014-1029`

Contract **verified** (not assumed): `POST /v1/jobs/purge` uses
`JobPurgeRequest`, where `status` is a LIKE-free equality filter
(`hive/bus/schemas.py:211-220`, `hive/bus/routers/admin.py:217-219`), and
`_cascade_delete_jobs` runs `DELETE FROM jobs ... AND status NOT IN
(queued,offered,claimed,running)` (`hive/bus/deps.py:208-212`). A row with
`status='failed'` **is** deleted. Confirmed against a temp sqlite DB:

```
purge result: {'deleted': 1, 'matched_ids': ['a']}   # 'remaining rows: []'
```

Residual risks:

- **Destructive, not "cancelled".** There is no `failed → cancelled` transition
  here; the row and its `job_audit_log` are gone (`deps.py:219-223`). The in-code
  comment "The ids stay on record in this triage job's result" (`:1015`) is only
  true if the doctor's own job survives — and it does not survive when the
  doctor's job is itself the one being purged, and `emit_event` writes a single
  aggregate `jobs.purged {count}` (`admin.py:255`) which is not a durable
  per-job record.
- **Cancel never recurses into child jobs.** `parent_job_id` children of the
  cancelled work remain queueable.
- **Count semantics.** `removed` is `resp2["deleted"]` — an `int` from
  `JobPurgeRequest` — but `"/".join` on `target_ids` vs `removed` mixes the
  *requested* count with the *confirmed* count without saying so; a partial
  delete (race with a worker) prints `cancelled 0/5` with no failure signal, and
  the broadcast is emitted regardless of `code2`.
- `action_cancel_jobs` reuses `"resubmit_base_kind"`/`"max_resubmits"` from the
  LLM action (`:1642-1644`), so the cancel cap is driven by a field named for a
  different action.

### F6 — LOW — Ordering: doctor-failed jobs poison the failed-job scan

`hive/workers/doctor.py:1007` and `:984` (and `:538` for the fetch)

`strip_re.sub("", kind)` strips only a trailing `[...]`; the doctor's terminal
patch appends `-> failed` to the **kind** (observed in the reproduction above:
`kind` matching yields `fix:hive-one -> failed`). `GET /v1/jobs?status=failed&limit=200`
returns newest-first and is then sliced by `max_resubmits`/`max_cancels` *before*
the client-side kind filter is applied (`action_resubmit_jobs` builds the whole
`target_ids` list first, so this is bounded by 200 — but `action_cancel_jobs`
has the same shape and both rely on the bus ordering). Any doctor-annotated kind
therefore consumes slots and can starve genuine `fix:<kind>` failures out of the
window. Strip the `-> <status>` suffix in `_base_kind` (`:1394-1396`).

---

## Loop-risk / deletion summary (what the task asked for)

- **Wrong deletions:** none found in the purge itself — the `status:"failed"` +
  explicit-`ids` guard is sound and cannot touch in-flight or history. But the
  delete is *permanent* (F5), so a bad `base_kind` from the LLM (`resubmit_base_kind`,
  `:1642`) is now unrecoverable where before it only recorded an intent.
- **False unschedulable matches:** confirmed (F1), and it fires on the doctor's own
  requeue path — the most likely real-world text after this very commit.
- **Loop risks:** confirmed and reproduced (F3) — self-failure release with a
  constant `released=True` and a `main()` branch that skips the terminal patch.

---

## Test gap

`test_doctor_triage_actions.py` (100 lines, 6 tests) is a good start and all 6
pass, but it **monkeypatches `action_resubmit_jobs` and `action_release_to_queue`
to stubs**, so it cannot observe either of the two real defects:

- `test_missing_branch_escalates_without_an_llm_call` asserts `calls["llm"] == 0`
  — it would pass even if the matcher keyed off the wrong text, because the LLM
  fake is never reached on the escalate path.
- No test covers `no_repo_mapping` on a catalog-present repo (F1), a doctor kind
  self-failure (F3), or `released_to_queue` when the release 404s (F3).
- `test_cancel_removes_the_failed_jobs` asserts the request body, not the bus
  response contract, so it cannot catch a `failed`-status purge being refused.

Suggested additions: a table-driven test over `_UNSCHEDULABLE_RE` with
`released_by_host` / catalog-present `no_repo_mapping` negatives; a test that
`apply_release` reports `False` when `_http` returns 404 on step 1; and a test
that a `failed` row with `status="failed"` is actually removed via the real
`purge_jobs` handler (the temp-DB harness above works).

---

## Verification performed

```
/usr/bin/python3 -m pytest test_doctor_triage_actions.py -q          -> 6 passed
/usr/bin/python3 -m pytest test_doctor_*.py test_worker_fanout.py -q -> 91 passed, 0 failed
```

Worktree clean at `b335554`; no source files modified by this review.
