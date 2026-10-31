# Job dependencies (`depends_on`)

How `depends_on` edges are created, evaluated, and torn down.
Citations are `file:line` against `deploy/integrate-20261017` (`f50f9d7`);
behaviour is enforced by `test_job_dependencies_20261031.py`, which parses
the status table below and executes every row against the real code.

## 1. Creating edges

- Edges are **write-once at submit**: `JobCreate.depends_on` lists job ids
  that must reach `status='done'` before the new job is dequeueable
  (`hive/bus/schemas.py:99`). `JobOut.depends_on` round-trips them
  (`hive/bus/schemas.py:151`).
- Submit validates (`hive/bus/routers/jobs.py:606-614`): a self-reference
  is refused with 422 (`depends_on cannot include self`); ids with no
  jobs row are refused with 422 (`depends_on references unknown job ids`).
- There is **no API path that edits an edge**: `JobUpdate` has no
  `depends_on` field (`hive/bus/schemas.py:173-210`), so PATCH can never
  add, remove, or rewrite a dependency. The only writers of the column
  after submit are the release paths in section 3 and the claim-time
  purge drop in section 4.

## 2. The claim gate

- A job is dequeueable only when **every** listed id has a jobs row with
  `status='done'`. The evaluation is `_deps_all_done`
  (`hive/persistence/sqlite.py:1315-1337`): NULL/JSON-null/`[]` (or a
  list of only nulls) counts as satisfied; anything else — a missing
  row, malformed JSON, a non-array, a non-done row — blocks.
  **Fail closed**: an unreadable value never lets a job skip its
  prerequisites.
- The gate runs twice per claim: an advisory pre-scan over the candidate
  set (`jobs_with_unsatisfied_deps`, `sqlite.py:1259-1290`, used by the
  dequeue route at `jobs.py:1032-1039` with a named 409 for the unsatisfied
  ids at `jobs.py:2000-2030`) and the binding in-transaction re-check
  (`_require_dependencies_done`, `sqlite.py:1517-1523`) inside `claim_job`
  immediately before the claim compare-and-set (`sqlite.py:1545-1560`).
  A pre-scan miss can never hand out a blocked job; a dep that finishes
  after the pre-scan is handled correctly by the re-check.
- `dependencies_satisfied` (`sqlite.py:1301-1313`) exposes the same
  evaluation for readers. `completed_job_ids` (`sqlite.py:1292-1299`)
  is the batch `status='done'` probe the pre-scan uses.
- The claimed payload carries `depends_on` for the worker, already
  enforced (`jobs.py:1157-1162`).

## 3. Release: when a dependency dies

`DEP_DEAD_STATUSES = {"failed", "failed_completion", "dead-letter",
"cancelled"}` (`hive/bus/deps.py`, after `IN_FLIGHT_JOB_STATUSES`):
terminal statuses a dependent can never satisfy.

- A job **entering** one of these releases every `queued`/`offered` job
  listing it, **in the same transaction as the status write**
  (`jobs.py:1820-1832`), so no intermediate state is observable. The dep
  id is removed from the dependent's `depends_on`; an emptied list
  becomes NULL (the canonical no-dependencies value — the job is
  claimable again); a surviving list keeps blocking on the remaining ids.
  One `job.dependency_released` event per released dependent
  (`{dependent_id, failed_dep_id, dep_status, remaining}`) is emitted in
  the same transaction (`hive/bus/deps.py: release_dependencies`).
- Dependents that are already `claimed`/`running` are **never touched** —
  their work is in flight; the edge stays as submitted.
- `done` is deliberately absent: a completed dep satisfies its
  dependents the normal way and **the edge is kept for provenance**.
- A dep that fails but is retried (final status `queued` in that
  transaction) does **not** release — the dep is retrying, not dead
  (`jobs.py:1829-1831`).
- Purges release with `dep_status='purged'`: dead-letter purge
  (`routers/admin.py:205`), bulk purge (`routers/admin.py:265`),
  retention sweep (`hive/bus/app.py:445`). The stale-job canceller
  releases as `cancelled` (`hive/bus/app.py:37`).

## 4. Dependencies that vanish without a status write

A row deleted by an admin purge that predates the release fix leaves
dependents listing an id that no longer exists — fail-closed, that id
can never reach `done`. `claim_job`/`claim_next_job` therefore call
`drop_missing_dependencies` in-transaction before the gate re-check
(`sqlite.py:1422-1515`): ids with no jobs row are stripped from
`queued`/`offered` dependents (compare-and-set on the status read in
the same transaction; a lost race skips the drop and lets the gate
decide), one `job.dependency_released` event per dropped id with
`dep_status='purged'`. Ids that still exist are never touched; a
malformed `depends_on` is left to the gate (still fails closed).

## 5. Status table (normative)

`<!-- FB54-STATUS-TABLE-START -->`

| dep status | queued dependent | edge after dep reaches this status |
|---|---|---|
| `queued` | blocked | kept |
| `offered` | blocked | kept |
| `claimed` | blocked | kept |
| `running` | blocked | kept |
| `needs-review` | blocked | kept |
| `done` | claimable | kept |
| `failed` | released then claimable | stripped |
| `failed_completion` | released then claimable | stripped |
| `dead-letter` | released then claimable | stripped |
| `cancelled` | released then claimable | stripped |
| `purged` | released then claimable | stripped |

`<!-- FB54-STATUS-TABLE-END -->`

Reading the table:

- **blocked**: a claim attempt raises `DependencyNotReady`
  (repository) / HTTP 409 (route); the row stays `queued`.
- **claimable**: the claim succeeds (single-dep case).
- **released then claimable**: the release path strips the dep id
  (emitting `job.dependency_released`); with the list emptied to NULL
  the dependent claims normally.
- **kept / stripped**: the dependent's `depends_on` column afterwards.
- `purged` means the dep row is gone: via the purge release paths
  (section 3), or — for a purge that predates them — via the
  claim-time drop (section 4), which the table test also executes.

## 6. Chains, diamonds, and fan-out

`depends_on` is evaluated one edge at a time; transitivity is emergent:
in a chain A → B → C (C lists B, B lists A), C stays blocked until B is
`done`, and B stays blocked until A is `done`. Completing out of order
(C's claim attempted while B is still pending) changes nothing. A
diamond (D lists B and C, both list A) resolves only when **both** B
and C are `done`; the death of one parent strips just that id and D
stays blocked on the other. None of this needs graph traversal code —
it falls out of the per-job gate in section 2 plus the per-edge
release in section 3.

## 7. Cycles

- **Through the API, cycles are unconstructible.** Submit refuses a
  self-edge and unknown ids (section 1), and edges are write-once, so a
  two-cycle A ↔ B would require the first-submitted job to reference a
  job that does not exist yet — refused. The table test's companion
  cycle tests prove the self-edge 422, the unknown-id 422, and that no
  update route accepts `depends_on`.
- **Below the API (direct row writes), a cycle blocks forever.** A ↔ B
  with both `queued` can never reach `done` through the gate, and no
  release path fires (neither side is dead). This is the documented
  steady state, not a hang to fix in the claim path: the escape is an
  admin purge (which releases with `purged`) or cancelling a member
  (which releases the other side as `cancelled`). FINDING FB54-1 below
  records the residual gap.

## 8. `needs-review` dependencies

`needs-review` is a quarantine state, not a terminal one: it is absent
from both `DEP_DEAD_STATUSES` and the `done` check, so a dependent of a
needs-review job **stays blocked with the edge kept** until the dep
resolves. If the dep later reaches `done`, the dependent claims
normally; if it reaches a dead status, the section-3 release fires;
if it is purged, section 3/4 applies. A dependent is never released
*by* the needs-review transition itself.

## 9. Findings

- **FB54-1 (open, needs an operator decision): nothing detects a
  dependency cycle or a permanently unsatisfiable edge.** A repo-level
  cycle, a dependent on a dep that sits in `needs-review` forever, or a
  dependent on a dep stuck `claimed` by a dead worker with no lease
  expiry are all silent: the dependent waits with no event, no metric,
  and no escalation. The claim gate, the release paths, and the reaper
  are each correct for the states they own — the gap is that nobody
  watches for "blocked longer than any dep can legitimately take".
  Suggested (not implemented): a doctor check or a bus metric over
  `depends_on` wait age. The cycle/blocked-forever tests pin the
  current behaviour so a future watchdog has regression coverage to
  build on.
