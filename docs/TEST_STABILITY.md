# Test stability audit (FB42)

Date: 2026-10-31. Line: `deploy/integrate-20261017` tip `db4d910` (contains
FB37, FB38, FB39, FB40, FB46, FB51, FB52). Runner: macOS 27.2 on darwin arm64, Python
3.14.5, `pytest` 9.2.0 from a venv holding the project's dev extra plus
`pytest-randomly`, invoked as `python -m pytest` from the repo root.

Two questions, two answers:

1. **Can a test file depend on this host?** `tests/test_worker_build_busy.py`
   could, and now cannot.
2. **Can a test depend on the order it runs in?** One could, and now cannot.

Everything below is measured on this host. Nothing in the product changed.

## 1. Hermetic build/pipeline busy gate

`tests/test_worker_build_busy.py` tested `deploy/hive_worker.py:build_busy` by
starting real processes and reading the host's real `/proc`. Consequences on the
base commit:

- Three of its six tests were `skipif(not os.path.isdir('/proc'))`, so on this
  macOS seat the majority of the gate's behaviour (busy orchestrator name,
  named pipeline stage, CI-user job vs runner daemon) was **never executed**.
- On a Linux worker host they executed but consulted whatever else was running:
  an unrelated process whose argv matched `HIVE_WORKER_BUSY_PROCS`,
  `HIVE_WORKER_PIPELINE_PROCS` or a listed CI uid could flip a scan-only
  assertion, and the module resolved `~` at import time, so it read the real
  `~/.config/hive-worker`.

Red proof that the old tests really needed the host scan — not just that they
were skipped: take the base file, delete the `skipif` markers and run it here.
`3 failed`:

```
FAILED test_running_build_orchestrator_makes_the_host_busy
FAILED test_named_pipeline_process_makes_the_host_busy_as_a_pipeline
FAILED test_ci_user_process_is_a_pipeline_job_but_the_runner_daemon_is_not
```

The file now patches three things and keeps every assertion:

| Host dependency | Now |
| --- | --- |
| `/proc` listing, `/proc/<pid>/cmdline`, `/proc/<pid>` uid | `install_fake_proc()` rebinds the loaded worker module's `os` and `open` to a `pid -> (argv, uid)` table, so a scan that leaked to the host would see an empty table |
| `~` (`BUSY_DIR`, drain file, zoder paths) resolved at import | `HOME` and `XDG_STATE_HOME` point at `tmp_path` **before** the module is imported, and `HIVE_WORKER_BUSY_DIR` is removed from the env |
| Wall clock for the marker TTL and marker age | `build_busy(now=...)` is passed explicitly |
| Real child processes | the fake table, plus one test (`test_real_host_processes_are_ignored`) that starts a real process named like a pipeline stage and asserts the gate stays idle |

Result: 8 tests, none skipped, 8 passed.

### Proof: 20 runs with, 20 runs without a hostile host

The hostile run starts a real `hive-pipeline-test-stage.sh` in the background
and exports `HIVE_WORKER_PIPELINE_PROCS`/`HIVE_WORKER_BUSY_PROCS` to that name
and `HIVE_WORKER_BUSY_DIR` to a directory holding a live pidless marker, i.e.
exactly the host state that used to decide these tests.

```
hostile run 01 exit=0 8 passed in 0.51s        (20/20 identical)
HOSTILE: 0 failures / 20
clean   run 01 exit=0 8 passed in 0.50s        (20/20 identical)
CLEAN: 0 failures / 20
```

## 2. Order audit of the full integrate-line suite

Method: `python -m pytest -p no:randomly` for file order, and
`python -m pytest -p randomly --randomly-seed=<seed>` for shuffled order
(`pytest-randomly` in the venv; it is not a project dependency and no test
imports it). Three runs of each, as the task requires.

### Before the fix

| Run | Result |
| --- | --- |
| ordered 1 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (145 s) |
| ordered 2 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (144 s) |
| ordered 3 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (145 s) |
| shuffled 1 | **`1 failed`, 1816 passed, 65 skipped, 3 xfailed, 214 subtests passed** (144 s) |
| shuffled 2 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (142 s) |
| shuffled 3 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (144 s) |

### The one differing test

`test_reaper_schedules.py::SchedulerReaperTests::test_firing_the_template_creates_a_job`
— `AssertionError: 2 != 1` at `self.assertEqual(len(due), 1)`.

Root cause: the module sets `AGENT_BUS_DB` to one `mkdtemp` directory at
import, and `hive/bus/deps.py:hive_repo()` memoises a single repository, so all
three tests in the class share one sqlite file with no per-test cleanup.
`test_select_due_only_enabled_and_past` leaves an enabled, past-due row
(`id="due"`, `next_fire = NOW - 5`) behind. The firing test asserts *exactly
one* due row and only passed because `unittest` sorts methods alphabetically,
putting `test_firing_...` before `test_select_due_...`; once shuffled, the
leftover row makes two.

Reproduced deterministically on the module alone, and fixed:

```
before:  seed 1 exit=1, seed 2 exit=1, seeds 3-5 exit=0
after :  file order x3 -> 3 passed (x3)
         seeds 1-10    -> 3 passed (x10), 0 failures
```

Fix (commit `984819a`, test-only): `asyncSetUp` clears `scheduled_jobs` before
each test, with a comment saying why. No assertion changed, no production code
touched.

### After the fix

| Run | Result |
| --- | --- |
| ordered 1 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (144 s) |
| ordered 2 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (145 s) |
| ordered 3 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (145 s) |
| shuffled seed 20261031 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (144 s) |
| shuffled seed 7 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (143 s) |
| shuffled seed 424242 | `1817 passed, 65 skipped, 3 xfailed, 214 subtests passed` (145 s) |

No test needed `xfail(strict=False)`: the audit found exactly one order
dependency and it had a minimal real fix.

The 65 skips are environment- or backend-conditional (missing DSNs, optional
dependencies, macOS-only cases) and are identical in every run; the 3 xfails
are the pinned FB38/FB51 findings. See `docs/TEST_PORTABILITY_INTEGRATE.md` for
the skips that have their own justification.

## Rules this audit leaves behind

1. **Do not read the host.** Patch the process scan, point `HOME` at
   `tmp_path` before importing a module that resolves `~`, and pass the clock.
   A test that skips because `/proc` is absent is a test that never ran.
2. **Give each test its own state.** A module-level `mkdtemp` plus a memoised
   repository means the file is shared by every test in the module; clear the
   tables a test asserts on, or assert relatively (deltas, your own ids).
3. **Never rely on alphabetical order.** `unittest` sorts methods; pytest-
   randomly does not. Any assertion of the form "there is exactly one row" has
   to be preceded by cleanup.
4. **Re-run shuffled when touching shared DB state.** `python -m pytest -p
   randomly --randomly-seed=1` caught this one on the first try.

## Limits of this audit

- Three ordered and three shuffled runs is the task's bar, not a proof of
  order independence for all ~1800 tests; the shuffled runs cover three global
  orders, and the module-level sweep above covers the one file that differed.
- The busy-gate proof runs on macOS, which has no `/proc`, so the "leak to the
  host" property is demonstrated by the fake table plus
  `test_real_host_processes_are_ignored` rather than by observing a Linux
  `/proc` leak. The rebinding is unconditional, so the same code path is
  exercised on either OS.
- `pytest-randomly` is not added to `pyproject.toml`: no test imports it, and
  the audit is reproducible without changing the project's dependencies.
