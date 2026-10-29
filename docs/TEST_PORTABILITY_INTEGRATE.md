# Test portability on integrate (macOS / environment)

Base: `deploy/integrate-20261017` tip `f4d9f3c`. Runner: macOS darwin,
`/Users/jasonperlow/Projects/.venv-graeae/bin/python -m pytest . -p
no:cacheprovider --tb=no` from the repo root.

- Before: `6 failed, 1310 passed, 13 skipped, 131 subtests passed`.
- After: `1312 passed, 17 skipped, 131 subtests passed` (0 failed).

Production code is unchanged. Only three test files are touched; no seam
was needed because every case could be made hermetic or precisely skipped
without weakening an assertion.

## Table

| Test id | Why it failed | What was done | Why |
| --- | --- | --- | --- |
| `test_eih_probe.py::OutputFileTests::test_main_writes_rows_and_exits_zero_on_probe_failure` | Date-dependent: `probe.main()` writes `eih-probe-<today>.jsonl` via `RowWriter.path_for(now=datetime.now(UTC))` (`hive/tools/eih_probe.py:RowWriter.path_for`, `main`), but the test read the hardcoded `eih-probe-2026-10-26.jsonl`. Passed on 2026-10-26, `FileNotFoundError` on any other date (here 2026-10-29, `rows=8 outcomes=503` logged but file looked up under the old name). | Test now globs `eih-probe-*.jsonl` and asserts exactly one file, then checks row count/outcomes on it. Network stays faked (`FakeTransport` 503s); no production change. | Hermetic: the test can run — date is not a contract. Assertion strength unchanged (same row count + all `503`). |
| `test_gitguardian_config.py::GitGuardianConfigShapeTests::test_config_declares_version_2`, `test_ignored_paths_is_present_and_iterable`, `test_ignored_paths_is_scoped_to_test_fixtures` (3 tests) | Missing optional dep: each does a local `import yaml`, raising `ModuleNotFoundError: No module named 'yaml'` in this env (pyyaml not installed). | Each now `try: import yaml / except ModuleNotFoundError: self.skipTest("pyyaml not installed: cannot parse .gitguardian.yaml")`. The passing `test_config_file_exists` (no yaml needed) is untouched. | Genuinely environment-only skip with a precise reason. Never skipped when yaml is present; no assertion weakened. Alternative (vendoring a YAML parser) would duplicate the dep for no gain. |
| `test_repo_argonas_fallback.py::IdContractTests::test_case_is_not_a_tolerance_for_the_fallback` | macOS-only: asserts `find_exact("LowerCase")` is `None` when only `lowercase.git` exists. On Linux (case-sensitive FS) it holds; on macOS default APFS (case-insensitive) the OS resolves the differently-cased path and the fallback correctly answers, so the test fails. | Skip on `sys.platform == "darwin"` with reason `case-sensitive filesystem required: macOS APFS is case-insensitive`. | Genuinely macOS/Linux-only skip with a precise reason. Production fallback (`hive/repos.py`) is correct on both; changing it to fake case-sensitivity would weaken the product. |
| `test_repo_argonas_fallback.py::OldCodeContrastTests::test_old_lookup_misses_what_the_new_one_resolves` | Moving baseline, not macOS: `BASELINE = "origin/deploy/integrate-20261017"` used to predate the ARGONAS fallback (`cf0dd19 feat(bus): resolve repos ... straight from ARGONAS`). After the merges to `f4d9f3c` the named branch already contains the fallback, so "old" code resolves too and `assertIsNone` fails on any OS. | Pinned `BASELINE` to the pre-fallback commit `8307fa1be2aa258652a074a5e0ad0ce511ccceff` (= `cf0dd19^`, verified `show` works) with a comment explaining the pin. The existing `skipTest` for missing git/shallow stays. | Hermetic + deterministic: restores the intended old-vs-new contrast without network or branch movement. Same assertions, now meaningful again on every platform. |

## Verification

- Affected: `pytest test_eih_probe.py::OutputFileTests::
  test_main_writes_rows_and_exits_zero_on_probe_failure
  test_gitguardian_config.py
  test_repo_argonas_fallback.py::IdContractTests::
  test_case_is_not_a_tolerance_for_the_fallback
  test_repo_argonas_fallback.py::OldCodeContrastTests::
  test_old_lookup_misses_what_the_new_one_resolves` →
  `5 passed, 4 skipped` (was `6 failed, 3 passed`).
- Full: `1312 passed, 17 skipped, 131 subtests passed`, 0 failed (was
  `6 failed, 1310 passed, 13 skipped`).
- `ruff check` on the three touched files reports only the pre-existing
  `I001` in `test_gitguardian_config.py` (present before this change via
  `git stash` check); no new lint. `ruff format` applied to the three files.

## What was deliberately not done

- No test that can run was skipped: the eih_probe and baseline-pin cases
  were fixed, not skipped.
- No assertion was weakened: row counts, outcomes, version, ignored-paths
  scoping, and old-miss/new-hit properties are identical when the test runs.
- No production file was changed; no network, `/proc`, daemon, or address
  book is touched (eih_probe tests already inject `FakeTransport`; catalog
  roots are `tmp_path`).
