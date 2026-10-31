# FB53 — queue logic property tests

## Premise check

The brief says "`hive/bus/queue_logic.py`: hypothesis tests for ordering by priority
and age, `eligible_kinds` / `eligible_hosts` filtering, `depends_on` gating, claim
fairness."

`queue_logic.py` contains only `dedup_key` (submission dedup) and `match`
(HARD/SOFT capability matching). The four behaviours the task names live here:

| behaviour | implementation |
|---|---|
| ordering by priority (and job age) | `SqliteHiveRepository.queued_candidates` `ORDER BY priority DESC, started_at ASC`, then `hive/scheduler.order_candidates_for_worker` (priority, role penalty, utilization, deficit, worker age, id) in `POST /v1/jobs/next` |
| `eligible_kinds` / `eligible_hosts` filtering | `hive/bus/domain/eligibility.job_agent_eligible`, called per candidate by the dequeue route |
| `depends_on` gating | candidate pre-scan (`jobs_with_unsatisfied_deps`) + the binding in-transaction re-check in `claim_job` (`DependencyNotReady`), release-on-death in `hive/bus/deps.release_dependencies` |
| claim fairness | scheduler sort key + the single-claimant `UPDATE ... WHERE status='queued'` compare-and-set, plus the starvation boost in `select_best_job` |

So the task is real but its file pointer is wrong. The tests cover the real
implementations and `queue_logic` itself, and the module docstring says so. No
production code is changed.

## Files

- `test_queue_logic_properties_20261031.py` (new, root of the repo, matches the
  repo's dated-root-test convention).
- No `pyproject.toml` change: `hypothesis` is already in the `dev` extra
  (`[project.optional-dependencies] dev = ["pytest>=7", "ruff", "hypothesis"]`),
  and the module uses `pytest.importorskip("hypothesis")` so it skips cleanly
  where the dev extra is absent — the same pattern as
  `test_job_lifecycle_stateful_20261030.py`.
- No conftest change: the in-process bus harness is local to the file (temp
  sqlite + `deps._HIVE_REPO` patch + `ASGITransport`), copied from
  `test_api_contract_20261029.py`.

## Properties (each invariant is stated in the test docstring)

1. `queue_logic.match`: eligible iff every `hard:` label is an exact capability
   (a `*` capability does not satisfy a hard label); adding a capability never
   removes eligibility and never lowers the score; score in `[-0.25, 1]` for load
   in `[0, 1]`; load is monotone; deterministic.
2. `queue_logic.dedup_key`: deterministic; tenant/kind/description/version/window
   each distinguish (separator-free values, so the `\x1f` basis is unambiguous);
   surrounding whitespace never distinguishes; `idempotency_key` overrides every
   other field.
3. `scheduler`: higher priority outranks every other key; the ordering is a total
   permutation of the input (no loss, no duplication), deterministic, and stable
   for exact ties; role preference prefers general over build for non-build work
   and the reverse for build work; the starvation boost cannot change a *single*
   worker's pick (defect/limitation to report — `deficit` is one constant in the
   sort key, so it never orders two candidates).
4. `eligibility.job_agent_eligible`: `eligible_kinds=None/[]` admits anyone,
   `["*"]` admits anyone, otherwise the agent's alias set must intersect;
   `eligible_hosts` admits only the pinned hosts (or `*`); required capabilities
   reuse the HARD rule; a higher agent cost tier than the job's `max_cost_tier` is
   refused; every refusal carries a reason.
5. Repository: `queued_candidates` returns priority DESC then `started_at` ASC
   (job age), honours `retry_backoff_until`, and its blocked-id filter is only a
   scan bound. `claim_job` refuses (raises `DependencyNotReady`) unless every
   `depends_on` id exists with status `done`; `jobs_with_unsatisfied_deps` and
   `dependencies_satisfied` agree with that ground truth. `release_dependents`
   strips a dead id only from queued/offered dependents, leaving claimed/running
   ones untouched and NULLing an emptied list.
6. End-to-end through `POST /v1/jobs/next`: distinct priorities are handed out
   highest first and each job at most once; equal priority goes oldest first; a
   worker never receives a job whose `eligible_kinds`/`eligible_hosts` exclude
   it, and no job is lost; a dependent is never handed out while its dependency
   is not `done`, and becomes claimable once it is.

## Risks

- Runtime: each DB-backed example builds a temp sqlite bus; keep `max_examples`
  low (4–8) and `deadline=None`. Measure the whole file before pushing.
- Flakiness: no sleeps except one 10 ms gap in the fixed equal-priority test; no
  real clock arithmetic in assertions beyond "submitted earlier wins".
- The e2e ordering assertions use distinct priorities so the result does not
  depend on which of two equal `started_at` values sqlite returns first.
