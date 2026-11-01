# Doctor remediation (2026-10-31)

*Operator rule 2026-10-31: "the doctor has to review everything in the triage
queue, that is the point of the doctor. it needs to actually fix jobs to alter
the fields to make them run correctly and get completed jobs reviewed if the
auto reviewer is busy."*

The doctor used to classify a stuck row and report it. This module makes it
**fix** the row: classify the cause, change the FIELDS that made it unrunnable,
and re-enter the row through the existing admin reclassify path. Every change is
recorded with its before/after values and a reason.

## Review round 2 (2026-10-31) — what changed and why

A structured review by a different model family (muse-spark-1.3-contributor)
raised six findings against the first attempt. Each was checked against the
source, each was real, and each now has a regression test in
`test_doctor_remediation_20261031.py` (the `test_r2_*` family) that fails on
`f557f7c` and passes with the fix:

- **A health probe that raises is not health.** `model_problem` called the
  probe bare, so an exception escaped `classify`. It now reads a raising probe
  as an unusable route, the same way `healthy_alternative` does.
- **`max_cost_tier` is a ceiling.** `healthy_alternative` ranked a route whose
  `cost_class` could not be read *below* every known class, so it could be
  chosen above the ceiling. A route that cannot be proven under the ceiling is
  never chosen.
- **No head is not the head `"none"`.** `delivered_head` stringified `None`
  into the truthy string `"none"`, so a row with no delivered commit satisfied
  every `if delivered_head(...)` guard. Missing (and the JSON placeholder
  spellings) now read as no head.
- **A marker is not its own clock.** `_marked_within(now=None)` compared the
  marker against its own stamp, so the age was always zero and every marker
  read as fresh forever. `now=None` means the real clock; only the age of the
  stamp is judged.
- **A head-only fix must not look already applied.** `_fields_unchanged`
  skipped `result.head_sha` when the row had no head, so a fix whose *only*
  change is the head read as a no-op after a crash between the field write and
  the status move.
- **A future stamp is not a recent fix.** `recent_audit` accepted any stamp
  ahead of the clock, so a clock-skewed stamp blocked the row's own fix
  indefinitely. Only `0 <= age < within` counts.

Two further gaps found while verifying those findings are fixed in the same
round: the sweep's `health` probe never reached `classify` (so a capped route
was invisible to the classifier), and the doctor's call site now passes the
real reviewer roster and its own route-health view.

### Invariants

The module never approves, merges, deletes a branch or rewrites a review
verdict; it only changes fields through the admin reclassify path, records
before/after plus a reason on every change, acts at most `MAX_AUTO_REMEDIATIONS`
times per row before escalating `doctor.needs_review_stuck`, caps each sweep,
excludes lane B entirely, is idempotent across sweeps and restarts via the
durable per-(row, cause, head) marker, and honours `HIVE_DOCTOR_REMEDIATE=0`
and the dry-run switch.

## Review round 5 (2026-10-31) — one key for the head sha

A continuation review found one remaining inconsistency in the plan's change
dictionary: the head was written under `result.head_sha` while the rest of the
mapping used bare row field names (`baseline`, `verify_baseline`), and the
executor (`apply_fields`, which posts `fields: {name: after}` to the admin
reclassify path) cannot tell a holder qualifier from a field name. One change
dict carried two dialects, and `_fields_unchanged` special-cased the qualified
spelling rather than fixing it.

The plan's `changes` mapping is now keyed **only** by row field name — the same
name the worker writes and `delivered_head` reads. The holder qualifier
(`result.<key>`) stays where it belongs: on `Cause.fields`, which is how the
classifier says which holder it read a bad value from. The reader resolves a
bare key to the job column when the row keeps that field there and to the
result otherwise, so writer and reader cannot disagree about where a head went.

Regression tests: `test_r5_*` in `test_doctor_remediation_20261031.py` (all
three fail on `0d1d402` and pass with this change).

## Review round 6 (2026-10-31) — a field present on both holders

Round 5's reader resolved a bare key by asking "is it on the job dict?", which
made the job column win for every field the row keeps there. That is wrong for
the fields this module writes: `plan_for` documents `changes` as
`{field: {before, after}}` **on the row's own result**, and the executor writes
them there. A row that was fixed once can still carry a stale copy of the same
name on its column (the legacy shape keeps `baseline` there), so the reader
compared the plan's target against the stale copy and reported an already-fixed
row as unfixed.

The resolution now follows where the executor writes: a bare key reads the
result when the result holds it, the job column otherwise. That keeps the
column path for `base_branch`/`head_branch` intact.

Regression test: `test_r6_a_result_owned_field_beats_a_stale_job_column_copy`
(fails on `901f2e3`). Two further `test_r6_*` cases are pinning tests that pass
on both trees — they guard the fix against over-correction in either direction.

A reported "duplicate `head_sha` key" in `plan_for` was **disproved**: the cause
loop iterates `BASELINE_FIELDS`, which does not contain `head_sha`, so the key
cannot collide. `test_r6_the_head_key_can_never_collide_with_a_cause_field`
pins that invariant.

## Review round 8 (2026-10-31) — the patch now actually lands

Round 8 again repeated the disproved claims (the `head_sha` container, the
`_fields_unchanged` ordering, `_REPLAY_OK_STATUS`, `_MODEL_CALL_LEDGER`,
`doctor_rules` exports, the `loop_outcome` import) — all re-verified against
source and all still false, with the existing pins holding. Chasing the one
substantive sentence ("verify the executor's expected change-dict key format
by checking the admin reclassify path") did surface a **real** defect one
layer below the review's claim:

`POST /v1/admin/jobs/reclassify` accepted only `job_ids`/`status`/`reason`/
`override_*`. The doctor's executor (`doctor._remediate_apply_fields`) posts
`fields: {name: after}`, and pydantic ignores unknown keys — so the route
returned `200 ok`, moved the status, and **dropped the field patch**. The
doctor reported a repair, the row kept its broken `baseline`/`head_sha`, and
the next sweep classified the same cause again until the attempt cap escalated
a row the doctor believed it had fixed. Every remediation test passed because
the suite's `apply_fields` was a fake in-process callable that never crossed
the wire.

`ReclassifyRequest.fields` is now accepted and merged into the row's `result`
on the non-queued path — a merge, not a replace, so the evidence the doctor did
not touch (error text, commits, review contract) survives. A row going back to
`queued` still archives and clears its result (FB51); writing fields there would
be discarded, so the merge is deliberately confined to the in-place path.

Regressions: `test_reclassify_fields_20261031.py` — the field write, in-place
overwrite of a bad value, merge preservation, and a full plan→executor→route→
reader round trip (all four fail on `a7b71dd`); the no-`fields` and requeue
cases are pinning tests that pass both.

Two review findings on the first cut were real and are fixed here:

- **Lost update.** The merge read `result`, then wrote it back unconditionally.
  A concurrent writer that replaced the payload between the two steps lost its
  write. The merge is now ONE conditional UPDATE whose WHERE also pins
  `result IS <the value read>`, so a lost race misses and is reported in
  `skipped_conflict` rather than clobbering the other write.
- **Non-object results were discarded.** A `result` that is valid JSON but not
  an object (a legacy bare string or list) was reset to `{}` and replaced. It is
  now preserved under `previous_result`: a repair never destroys evidence it
  cannot merge into.

## Review round 9

Nine of the claims repeat what rounds 7-8 already disproved (and the round-9
wording now argues itself into the opposite of the truth: it says `result IS ?`
fails on a bound NULL and asks for `= ?`. `IS` matches NULL, `=` does not --
switching would break exactly the empty-result rows the finding was worried
about). Verified again against sqlite and pinned.

One finding was real: a patch field literally named `previous_result` could
claim the preservation slot and destroy the value that key exists to hold. The
merge now refuses that key, so the slot can only ever carry the row's own
preserved payload.

## Review round 10 (r6 branch, 2026-10-31) — the hand-over, the raw payload, the full pin

A structured review by muse-spark-1.3-contributor (a different model family)
returned `request_changes` against r5 with three findings. Each was checked
against the source first; the regression tests are the `test_r6_*` family in
`test_doctor_remediation_r6_20261031.py` (fixtures reuse the r5 row shapes).

- **HIGH — the review hand-over dropped `result.*` changes. REAL, FIXED.**
  `remediate_one` built the row it passes to `advance_review` from
  `{... if not k.startswith("result.")}`, while `plan_for(WAITING_REVIEW)`
  writes exactly one such field: `result.review_state`, the block
  `review_queue.review_state` consumes. The field write landed on the row, but
  the row the doctor then announced to the review queue carried no state, so
  the queued review had nothing to adopt. The hand-over now carries every
  planned `after` value. `test_r6_high_*` fails on the r5 base and passes here.
- **MEDIUM — an invalid-JSON result was discarded. REAL, FIXED (on the row).**
  The bus already preserved a non-object payload on the column
  (`_merge_field_patch` parks it under `previous_result`), but on the ROW every
  reader in this module returned `{}`: the preserved text was invisible to
  classification, to `_fields_unchanged` and to the hand-over, and the next
  patch had nothing to re-write it from. `result_dict` now keeps the raw text
  under `raw` (the key `doctor._job_result_dict` and the loop finaliser already
  use) for a string that does not decode, and `json.dumps` of a non-string,
  non-object value. A dict result and an empty/null one are unchanged.
  `test_r6_medium_*` fail on the r5 base and pass here.
- **MEDIUM — host-pin handling triggers on any overlap. REAL, FIXED.**
  `host_pin_fields` returned a pin on `any(h in failed for h in hosts)`. The
  bus refuses only the hosts that failed the row
  (`hive/bus/routers/jobs.py:_claimant_allowed`), so with `eligible_hosts =
  [cerberus, tydeus]` and only `cerberus` failed the row is still offered to
  `tydeus`: clearing that pin widened a deliberate pin for no reason. The rule
  is now `all(...)` — a pin is cleared only when EVERY eligible host has
  failed. The single-host fixture (the observed shape) still classifies exactly
  as before, and the new `test_r6_overlap_pin_is_disproved_*` tests pin both
  sides so a regression back to `any` fails.

### Second reviewer run — dispositions

A panel of a different family (MiniMax-M3.1-Flash, nemotron-3-ultra) reviewed
the branch at `8185c70`. Two findings were raised against this work and one
against a file that the merge from `deploy/integrate-20261017` brought in:

- **HIGH — "nested `result.*` changes passed to `apply_fields`. DISPROVED.**
  The hand-over sample and the executor's payload are two different dicts:
  `apply_fields` receives `plan.changes` (`doctor._remediate_apply_fields`
  flattens `{name: after}` from it) and the `result` merge only builds the
  sample handed to `advance_review`. No `result.`-qualified key exists in
  `plan.changes` — `plan_for` writes the bare key, as the r5 round pinned —
  and the new `test_r6_the_executor_never_sees_a_nested_result_change` proves
  the executor's payload stays top-level.
- **LOW — the test module's docstring said no code changed for the host pin.
  REAL (documentation only), FIXED.** The r6 branch does change `any` -> `all`
  (r5 shipped `any`). The docstring now says so, and the pinning tests cover
  both directions.
- The remaining findings name `scripts/mutation_lite_fb63.py` and
  `test_mutation_killers_20261031.py`, which arrive from the integrate merge
  and are outside this task's scope: not changed here.

### Third reviewer run — the qualified key

MiniMax-M3.1-Flash read the comment "the plan writes `result.review_state`"
as a literal key and asked for the hand-over to nest it. Verified against the
source: the production plan key is BARE (`plan_for` returns
`{"review_state": ...}`; a `result.<key>` key is never emitted, as the r5 round
pinned) so no qualified key could ever have reached the merge. The comment was
the crude part and now says "the result field `review_state`".

Defensively, the hand-over now goes through `_handover_result`, which unpacks
a `result.<key>` key into the result if one is ever produced — the payload can
never carry a literal `"result.review_state"` key. Two tests cover it: the
production bare-key path read through `review_queue.review_state`, and a
qualified plan key unpacked into the holder (both fail without the helper).

## Review round 11 (r7, 2026-10-31) — findings against r6

muse-spark-1.3-contributor reviewed r6. Each finding was checked against the
source first; the tests are in `test_doctor_remediation_r7_20261031.py`.

- **HIGH — `healthy_alternative` does not catch probe exceptions. DISPROVED.**
  The probe call is already wrapped (`except Exception: continue`). Pinned by
  `test_r7_disproved_healthy_alternative_treats_a_raising_probe_as_unhealthy`
  and `test_r7_disproved_a_raising_probe_does_not_crash_the_sweep`.
- **HIGH — the INFRA_RETRIES requeue lacks the override. DISPROVED for the
  plan.** The plan sets `override=True` with a non-empty `override_reason`, the
  doctor's executor posts `override_max_retries`/`override_reason`, and the live
  admin route requeues a spent `needs-review`/`failed`/`dead-letter` row with
  that body (`TestInfraOverrideAgainstTheRealRoute`). Verifying it found a
  **real** defect one layer down: the route answers `200` when it REFUSES a row
  (`skipped_retry_budget`, `skipped_conflict`, ...), and `remediate_one` judged
  success on the HTTP code alone, so a refused requeue was recorded as applied
  and spent an attempt. It now reads the per-row lists
  (`test_r7_real_a_200_that_refused_the_requeue_is_not_recorded_as_applied`).
- **HIGH — MISSING_BRANCH keeps a valid-but-missing head branch. REAL, FIXED.**
  Only the base branch was probed. The head branch is now probed too: a head
  absent on the remote is pushed from the preserved workspace HEAD before the
  requeue, and with no workspace (or an unreadable probe) the row is not
  requeued (`test_r7_real_a_missing_head_branch_*`,
  `test_r7_real_a_failed_workspace_push_does_not_requeue`,
  `test_r7_real_an_unreadable_head_probe_is_not_proof_the_head_exists`).
- **MEDIUM — inconsistent case normalization of `capped_routes`. DISPROVED.**
  Every consumer lower-cases both sides before comparing; the spelling never
  changes the cause or the chosen route
  (`test_r7_disproved_capped_route_spelling_does_not_change_the_fix`).
- **INFO — `is_composite` broader than its comment. REAL, FIXED.** It accepted
  any 36-hex value with a letter (and rejected an all-digit composite). It now
  checks the real shape: 36 hex characters, the `head_sha[:28]` prefix and the
  workspace-uuid tail when the caller names them, and never a value the
  repository resolves (`test_r7_real_is_composite_*`).
- **Earlier: absolute workspace path in the fixture.** The fixture still
  carried `/var/tmp/...`; it now uses a `{workspace_root}` placeholder that the
  test helper roots at `tmp_path` (`test_fixture_paths_are_portable`, tightened).

## Closing self-review (2026-10-31)

- **REAL, FIXED — the field patch ran before the workspace push.** A plan that
  pushes (stranded WIP, or a head branch missing on the remote) records
  `workspace_pushed: True`. That patch was written before the push, so a
  rejected push, or a missing rescuer, left the claim on the row, and every
  later sweep read the row as `already_fixed`: never pushed, never moved. The
  push now runs first and nothing is written unless it succeeds
  (`test_close_*` in `test_doctor_remediation_r7_20261031.py`).
- `remediate_one` now lower-cases `capped_routes` at both call sites
  (behaviour-neutral: the consumers already lower-cased).
- Not wired in production yet: `doctor.run_remediation_sweep` passes no
  `default_branch`, `branch_exists`, `ref_exists`, `resolve_ref` or
  `rescue_workspace`. With them unset, MISSING_BRANCH ends `no_provable_fix`,
  STRANDED_WIP and missing-head repairs end `FAILED no_workspace_rescuer`
  (nothing written), and the composite check is shape-only.

## Review round 12 (2026-11-01) — the sweep is finally CALLED, and a first live sweep works

The remediation module and `doctor.run_remediation_sweep` shipped to PYTHIA on
2026-10-31, but nothing ever called the sweep. The doctor's main loop ran the
DLQ scan, the needs-review sweep, the orphan-failure sweep and the
unclaimable-queued sweep and never the one that fixes rows: the live log showed
`needs-review report-only` lines and zero `remediation` lines, so the doctor
still only reported the backlog it was built to drain. The loop's per-interval
body is now the function `doctor.doctor_loop_body(last)` (extracted from `main`
so the order is testable, and so the next un-called sweep is a failing test
rather than a live discovery), and it runs the remediation sweep once per
`NEEDS_REVIEW_SCAN_INTERVAL`, after `run_needs_review_sweep` (whose
classification it reads) and inside its own `try/except` that logs
`remediation sweep error: ...` and never kills the loop. Its summary is logged
as `remediation sweep: <summary>`, the same shape as its siblings; the startup
line now says `remediation enabled|disabled dry_run=... max_per_sweep=...
max_attempts_per_row=...` so an operator can tell from the first log line
whether the doctor will fix and whether that is only a preview. The kill switch
(`HIVE_DOCTOR_REMEDIATE=0`) and the dry-run flag are honoured where they always
were — inside `run_remediation_sweep` — and `MAX_REMEDIATIONS_PER_SWEEP` still
caps a pass. Three defects that would have stopped or poisoned a first live
sweep are fixed with it: the durable-state guard now precedes the bus listing (a
refused sweep no longer reads the board first), an unreadable/empty reviewer
roster is a refused sweep instead of "no route serves any model" (which, with a
missing roster file, is every row), and the `remediation:` key prefix now has a
`COUNTER_TTLS` window — without one the store pruned those keys at the longest
configured window, so a per-head "already fixed" marker never aged on its own
clock. The doctor's `STATE` is a file at `DOCTOR_STATE_PATH` (default
`$XDG_STATE_HOME`/`$HOME/.local/state/hive-doctor/state.json`); the container
mounts `/srv/agent-bus`, which is the BUS's database path, not the doctor's
state path, so the deployment must set `DOCTOR_STATE_PATH` inside that writable
mount for the sweep to run at all — otherwise every pass refuses with
`remediation refused: durable state unavailable`. Regressions:
`test_doctor_remediation_wiring_20261101.py` and the `remediation` TTL pin in
`test_doctor_remediation_20261031.py`; all fail on the base.

## Continuation rounds (2026-11-01) — request_changes → continuation → re-review

The orchestrator used to run this loop by hand: a cross-family review asked for
changes, someone wrote a continuation job carrying the findings, and the
continuation's head went back to the review queue. The doctor now runs it
(`doctor_remediation.continuation_sweep`, called from `run_remediation_sweep`
after the field-fix sweep, summary under `continuations`). Conflicts and
merged-tree failures are **not** handled here; they belong to the merge-pass
tool (fleet-ops). Only the review-driven path lives in this module.

**Who qualifies.** A `needs-review` WORK row (not `review:`/`triage:`, not
lane B, with a `head_branch`) whose BEST non-author review of the current tip
is `request_changes` with located findings, and whose lineage has had fewer
than `CONTINUATION_MAX_ROUNDS` (2, `HIVE_DOCTOR_CONTINUATION_MAX_ROUNDS`)
automatic rounds. The reviews are the row's `review:` children
(`_needs_review_child_review_jobs`). A review counts only if:

* it is not refused as overreach;
* its verdict reads as approve or request_changes (`review_verdict.child_verdict`, else
  the `review_evidence.reviewers` verdicts);
* its reviewer family differs from the author's (an unknown author family proves
  nothing and counts nothing);
* its head is the row's delivered tip, or a **verdict-only descendant** of it proven
  on disk (`classify_review_head` → `doctor_head_match.VERDICT_ONLY`; the live
  2026-11-01 shape where the reviewer reports the commit adding its verdict
  artifact). An unproven descendant is not the tip.

Any approve of the tip outranks every request_changes. Among the request_changes
reviews with located findings, the most severe finding wins, then the most
findings, then the newest. A finding is *located* when it names a place: a
`location` string, a `location` object (`path`/`file` + `line`) or top-level
`file`/`path` + `line`. It also needs a title or a body. Findings are read from
`result.findings`, `verdict`/`reviewer`/`review_evidence` and their per-reviewer
entries (an approving reviewer's notes are skipped), the loop log's last
iteration and a JSON `tail`/`stdout`.

**The job.** One continuation per row and tip:

* the same `kind`, `repo` and `project`;
* `base_branch` = the row's `head_branch`, `head_branch` = `<slug>-rN`. The slug
  loses a previous `-rN` only when the row is itself a continuation;
* `parent_job_id` = the WORK row. A review row is refused by `continuation_spec`,
  so the never-review-a-review guard stands;
* submitted as the doctor (`submit_job_spec`);
* `idempotency_key` = `doctor-continuation:<root>:r<N>:<tip12>`.

The brief names the round, the base branch and tip, and the new branch. It then
quotes every located finding of the best review verbatim (severity, location,
title, body) and ends with the standing instruction (`CONTINUATION_INSTRUCTION`):
*verify every finding against source first; failing test first when it holds;
pinning test and a note when it does not; never change correct code to satisfy
a disproved claim; push your final HEAD and confirm with git ls-remote.*

**Model ladder.**

* **Round 1:** the author's own model (same family as the author), refused if
  that family requested changes.
* **Round 2:** the strongest hard-lane route, meaning roster routes with a
  positive `hard_lane_priority`, ordered by `review_queue._bulk_key(..., LANE_HARD)`.
  The route's family must not have requested changes on the tip and must not be
  forbidden. Today that is `codex-worker` (`gpt-6-luna`, openai). The job gets
  `eligible_kinds: ["codex"]` and `max_concurrency: 1`, like a Codex review, and
  its cost tier is raised to the route's class.
* **No model:** when no model qualifies (for example an openai reviewer in round
  2), the row escalates.

**Bounds.**

* The marker `remediation:continuation:<row>:<tip12>:r<N>` is written after a
  successful submit. A row with a marker for its tip at any round is a no-op, so
  the original row does not mint round 2 while its round-1 continuation is in
  review.
* The lineage `{root, round}` is recorded on the new job id, and the per-root
  round count is durable, so a continuation whose idempotency key the bus dropped
  still counts its rounds.
* After two rounds, or with no ladder model, the doctor escalates ONCE per root
  through `doctor.needs_review_stuck` (`_remediate_escalate`) and stops.
* At most `CONTINUATION_MAX_PER_SWEEP` (3, `HIVE_DOCTOR_CONTINUATION_MAX_PER_SWEEP`)
  new continuations per sweep, oldest first. The rest are `deferred`.
* A failed submit writes no marker and is retried next sweep. The idempotency
  key keeps the bus from minting a duplicate.
* `HIVE_DOCTOR_REMEDIATE=0` stops the pass before any read.
* A dry run (`HIVE_DOCTOR_REMEDIATE_DRY_RUN`, or the doctor-wide `DRY_RUN`)
  submits, marks and escalates nothing and lists `would_submit`.

Regressions: `test_doctor_continuation_rounds_20261101.py` (fixture
`tests/fixtures/doctor_continuation_rows.json`, plus the `waiting_review` row
of `doctor_remediation_rows.json` and the live `work_root` of
`doctor_review_chain_rows.json`); all fail on the base.

Not verified: the operator's routing note
`~/.claude/rules/coding-slot-routing-20261031.md` was not available. The round-2
"stronger hard-lane model" is taken from `hive/core/reviewer-roster.json`
(`hard_lane_priority`) and `docs/REVIEW_QUEUE.md`, not from that note. The
Codex gate (plan usage, cooldown, online worker) is not consulted at submit
time: the job waits for a codex worker to claim it.

### The continuation pass fails closed (2026-11-01, DeepSeek HIGH)

This pass makes the doctor CREATE jobs by itself, and a remediation runaway
earlier the same day built a ten-deep review chain. The round number came from
`rounds_spent` = max(the row's own lineage, the per-root counter). The own
lineage is known only from the durable `_lineage_key(<job id>)` record
(`MARK_TTL_SEC`) or a `doctor-continuation:` idempotency key. With both gone it
fell back to `(own id, 0)`, so the row became round 1 again on the author's
family and started a new chain (the regression reproduced
`hv/...-r1` → `hv/...-r1-r1`). If the per-root counter is lost too, that repeats
forever. Three guards now stand in front of every submit. Each one works on
its own.

1. **`lineage_unknown`: a lost continuation is refused, not restarted.** When
   neither the lineage record nor a well-formed key exists,
   `continuation_evidence` looks for continuation markers that do not depend on
   either. The markers are: a `doctor-continuation:` key prefix; the brief's
   `Continuation round N of M for work job` line (or the standing instruction)
   in `description`/`brief`/`prompt`/`task` on the row or its result; a `-rN`
   head branch; a parent the store shows the doctor continued (a lineage,
   rounds or continuation marker); or, through `_chain_job`, a parent chain
   where a work row's `head_branch` is this row's `base_branch` or carries a
   marker itself. When any marker is present, the doctor creates nothing. The
   outcome is `refused` with `lineage_unknown`, and the doctor logs one WARNING
   and escalates once through `doctor.needs_review_stuck`. The escalation
   repeats at most once per `ESCALATE_TTL_SEC`
   (`remediation:continuation_lineage_unknown:<id>`). Repeat refusals are
   logged at DEBUG. A dry run escalates nothing. If the parent chain can't be
   read, the result is `refused`/`lineage_lookup_failed`: nothing is created
   or escalated, and the next sweep reads the chain again. A genuinely new root
   with no marker anywhere still gets round 1. A parent whose head is not this
   row's base, and which the doctor never continued, does not count as a
   marker.
2. **A global hourly brake.** At most `HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR`
   (default 6) continuation jobs per rolling hour across ALL rows. The limit is
   counted in its own durable record (`remediation:continuation_hourly`, a list
   of stamps) and read independently of every per-row record. The stamp is
   written BEFORE the submit, so a submit that fails (and may still have
   created the job) also counts. A clock that moves backwards never frees the
   brake. Rows over the limit are `throttled`, and the sweep logs one WARNING.
   Dry runs do not count. `0` creates nothing, and an unreadable value means 6.
3. **A durable store or nothing.** `continuation_sweep` refuses before any read
   (`refused: durable_state_unavailable`) when `STATE.loaded_ok` or
   `STATE.persist_ok` is false, and `doctor.run_continuation_sweep` raises.
   This is the same rule `run_remediation_sweep` already applied. Each submit
   checks the store again, before and after the brake's own write, because a
   counter that did not persist is no brake.

**Kill switch.** `HIVE_DOCTOR_REMEDIATE=0` stops both halves (the field-fix
sweep and the continuation pass) before any read. `HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR=0`
stops only the creation of continuation jobs.

Regressions: `test_doctor_continuation_guards_20261101.py`. Every test in it
failed on `836a28d`.

## Grind until green (2026-11-01)

*Operator rule 2026-11-01, amended after a soundness review: "doctor reviews
GRIND UNTIL GREEN with ALTERNATING FIX-IN-PLACE reviewers, with hard limits."*

The old continuation path ran two automatic rounds and then escalated. It is
replaced by a **bounded loop**. Each round is one of two kinds on the same row:
a `fix` round when the last review found blocking problems, or a `review` round
when it did not. The loop stops the moment the row is objectively green.

### Green is objective

A row is green only when all three hold:

1. the merged-tree suite is clean (`suite_gate`),
2. the import preflight is clean, and
3. a **structured `APPROVE` from a STRONG reviewer family** lands on the exact
   head being judged.

*Strong* families are `muse`, `deepseek`, `codex` (review-only, per the ladder)
and `opus`. `cerberus-reviewer/gpt-oss-20b`, `qwen38` and the other local
models may still review, but their approval never counts toward green.
`astra` never counts.

### Only high and critical block

A `request_changes` verdict blocks only on **high** and **critical** findings.
**Medium and low** findings are written to a follow-up row whose
`parent_job_id` is the work row — recorded, never re-opening the loop, never
blocking. One follow-up row is written per finding.

### Each round reviews the delta

A round reviews the **delta since the last reviewed head**, plus whether the
previous round's blocking findings were resolved. It is not a fresh full
review. A verdict-only head advance (the fix round changed nothing but the
verdict) keeps the existing green review rather than forcing a re-review.

### Rotation

- The reviewer family differs from the **branch author** family.
- The reviewer family differs from the **previous round's reviewer**.
- A reviewer that finds blocking problems gets them fixed by a **fix job in
  place** (a continuation commit on the branch); the next round is reviewed by
  a family that is **not the fixer**.
- A fixer never approves its own commit.

`_reviewer_pick` orders the roster hard-lane routes first, then every other
eligible route, and skips any family that is blocked, is the author's, or is
the fixer's.

### Verify before fixing (mandatory)

The fix job first **reproduces each finding against source with a failing
test**. If it cannot reproduce it, it adds a pinning test and a note, and the
finding is **closed as disproved**. Correct code is never changed to satisfy a
disproved claim.

### Test integrity guard

A fix commit that **deletes or weakens** existing assertions/tests, **skips or
xfails** tests, or grows the diff by more than the per-round budget (default
**+400 lines**, `GRIND_DIFF_BUDGET`) **fails the round and is never merged**.

### Limits and escalation

| Limit | Env | Default |
| --- | --- | --- |
| Rounds per row | `HIVE_DOCTOR_GRIND_MAX_ROUNDS` | 6 |
| Per-row spend cap | `GRIND_MAX_SPEND_USD` | configurable |
| Hourly global brake | `HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR` | 6 |
| Diff budget per round | `GRIND_DIFF_BUDGET` | 400 |

- **Stall detection** is by finding fingerprint: the *same* fingerprint in two
  rounds is a stall.
- On **any** limit — round cap, spend cap, stall — the doctor escalates
  **once** to the operator with the evidence (rounds, findings, spend) and
  stops.

### Rails kept

- Fail **closed** on unknown lineage.
- **Durable marker** keyed on `(row, tip, round)`.
- `HIVE_DOCTOR_REMEDIATE=0` and `DRY_RUN` are honoured; a dry run fixes
  nothing and marks nothing.
- **Never review a review.**
- **Never touch lane-B rows.**
- Merge + deploy stays under the existing standing rule, and only after green.

Regressions: `test_doctor_grind_until_green_20261101.py`. Every test in it
failed on the base (`6710615`) and passes with this change.

## Gates from verify evidence (2026-11-01)

Until this change nothing in production supplied the `gates` that
`continue_one` needs, so `green_blocker` always answered `gates_unknown` and no
row could reach GREEN. The gates are now **durable evidence**.

### The gate record

`GateProvider.for_row(row, tip)` (wired in `doctor.run_continuation_sweep`)
returns `{"suite": {"clean", "detail"}, "import_preflight": {"clean", "detail"}}`
from the record `remediation:gate:<repo>:<base_branch>:<full tip>` in the
doctor's `DurableStateStore`. The record is written **only** from a verify
job's result (`read_verify_result`) for that exact repo, base and tip.

- **Absent** → `None` → `gates_unknown` (never clean).
- **Stale** (a record for another tip, base or repo) → `None`.
- **Malformed** (a missing gate, a non-boolean `clean`, a non-int
  `returncode`, a result naming another tip/base) → `None`.
- Called with a bare tip (no row, so no repo/base) → `None`.

### The verify request

A STRONG, non-fixer `APPROVE` on the exact tip with no gate record makes the
sweep submit **one** job:

| Field | Value |
| --- | --- |
| `kind` | `verify:build:zeroclaw` (`VERIFY_KIND`) |
| `eligible_kinds` | `["zeroclaw"]` — no `eligible_hosts` pin |
| `repo` / `project` | the row's |
| `base_branch` | the row's **head** branch (what the worker clones) |
| `head_branch` | none: there is no output branch |
| `parent_job_id` | the work row |
| `idempotency_key` | `doctor-verify:<row>:<tip[:12]>` |
| `description` | `VERIFY_BRIEF`: check out the exact tip, merge the base, run the full suite and the import preflight, finish with one JSON line `{returncode, summary, import_preflight: {clean, detail}, base, tip}` |

- One durable marker per **(row, tip)** (`remediation:verify:<row>:<tip12>`),
  written only after the bus accepts the job; the next sweep reports
  `verify_pending` until the result arrives.
- Capped per sweep (`HIVE_DOCTOR_VERIFY_MAX_PER_SWEEP`, 3) and per hour
  (`HIVE_DOCTOR_VERIFY_MAX_PER_HOUR`, 6), separately from the fix brake.
- `HIVE_DOCTOR_REMEDIATE=0` and DRY_RUN are honoured. Lane-B rows, review jobs
  and verify jobs never get a verify request.

**The reader.** On a later sweep the provider looks up the requested job
(`_chain_job`). If the job is settled (`done`, or `needs-review`/`failed`) it
copies the result JSON into the gate record. The JSON is taken from the result
fields or, failing that, from the **last** object in `result.tail` that names
`returncode` and `import_preflight`, because the worker does not lift a model's
JSON into the result. The suite gate is `suite_gate({returncode, summary})`, so
a skip, an xfail or "no tests ran" is not clean.

### The sweep

| Gates for the tip | What the sweep does |
| --- | --- |
| no record | request a verify, once (`verify_requested`, then `verify_pending`) |
| clean | **GREEN**: the existing green marker plus a PATCH of the parent row's `result.review_state` to `state: approved_mergeable` (`record_green_mergeable`) |
| dirty (suite or preflight) | the existing **fix** path; the dirty gate becomes a high finding at location `suite` / `import_preflight` |
| stale | ignored; the current tip is verified |

An approve from a family that fixed the row is ignored: no verify and no GREEN.

`record_green_mergeable` writes the same shape as the head-match path
(`approved`, `approved_mergeable`). It adds `reviewer_model`, `reviewed_sha` and
the `gates` that the merge pass reads. The row **stays `needs-review`**. If the
PATCH fails, no GREEN marker is written and the next sweep tries again.
**The doctor never merges.**

### Bus constraints this design works around

- `verify:` is not a read-only kind on the bus (`READONLY_KINDS`), so a job
  that carries a `repo` is held to the commit-evidence gate. A commitless
  verify run therefore lands `needs-review` with `completion_evidence_missing`.
  The reader accepts that status. `ensure_review_followup` skips verify rows
  (`skipped_verify_job`), and the remediation classifier never "fixes" their
  missing `head_branch`.
- With no `head_branch`, the bus does no output-branch probe, so
  `output_branch_exists` cannot refuse the job.

Regressions: `test_doctor_gates_provider_20261101.py`. Every test in it failed
on the base (`f471b91`) and passes with this change.
