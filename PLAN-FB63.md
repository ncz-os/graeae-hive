# PLAN-FB63 — Mutation-lite on critical functions

## Brief
Hand-rolled mutation testing of `loop_outcome.decide`, `valid_completion_proof`,
`auto_retry_allowed` and the verdict parser (`review_verdict.row_verdict`):
a script applies 10–20 single-operator mutants each in a temp copy, runs the
targeted tests, lists survivors. Add tests that kill the survivors. Never
modify the real tree (mutants run in a /tmp copy; the tree gains only the
script + new killer tests).

## Targets and targeted tests (baseline must be green on unmutated copy)
1. `hive/workers/loop_outcome.py::decide` → `test_doctor_loop_outcomes_20261029.py` + `test_replay_loop_outcomes.py`
2. `hive/bus/domain/jobs.py::valid_completion_proof` → `test_bus_readonly_kinds_no_commit.py` (+ lifecycle doc test if fast)
3. `retry_policy.py::auto_retry_allowed` → `test_auto_retry_protection.py`
4. `hive/review_verdict.py::row_verdict` → `test_review_verdict_fuzz_20261030.py` (hypothesis in venv ✓) + `test_doctor_verdict_completion.py`

## Script: `scripts/mutation_lite_fb63.py` (stdlib only)
- `MUTANTS: [(target_file, old, new, note)]`, 12–16 per function, each a
  single-operator change (flip `>=`→`>`, `==`→`!=`, drop one `and` clause,
  swap a `return` constant, `True`→`False`, boundary `>0`→`>=0`).
- For each mutant: `cp -r` tree to a temp dir (7.7 MB, no .git), apply the
  replacement (assert exactly-1 occurrence), run the targeted pytest files
  with `PYTHONPATH=<tmp>`, record killed (tests fail) vs survived (green).
- Prints a table; exit nonzero only on harness errors, never on survivors.
- Runtime budget: ~60 mutants × ~10–20 s ≈ 15–25 min. Batch + run in background
  if needed.

## Killer tests
- New `test_mutation_killers_20261031.py`: one focused test per SURVIVOR,
  asserting the unmutated behaviour the existing suite missed. Re-run the
  script to confirm the survivors now die (or document equivalent mutants —
  e.g. two mutants killed by the same test).
- Verdict: keep survivors that encode intended leniency? No — the brief says
  add tests that kill the survivors. If a mutant is behaviour-identical
  (provably equivalent), say so and drop it from the survivor list instead.

## Risks
- Mutant string replacements must be unique in the file; assert count == 1.
- Temp-copy test runs need the venv's pytest + hypothesis; set PYTHONPATH and
  use the same interpreter.
- `decide`/`row_verdict` mutants may be slow via hypothesis profiles; use the
  targeted files only, `-p no:randomly`, `-x -q`.
