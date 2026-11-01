# PLAN — FB42. Hermetic worker tests and flakiness audit (graeae-hive)

## Premise check

`tests/test_worker_build_busy.py` on `origin/deploy/integrate-20261017` has 6 tests, 3 of
them marked `skipif(not os.path.isdir('/proc'))`, so on the macOS seat three tests never
run and on a Linux worker host they depend on what else is running. Partially done work
existed from an earlier seat on this branch name (uncommitted in this clone, and a second
attempt committed but unpushed in `~/Projects/freebuff-hermetic`); this clone's work is
verified, completed and committed here.

## Files

- `tests/test_worker_build_busy.py` — rewritten hermetic (fake process table behind the
  module's `os`/`open`, `HOME`/`XDG_STATE_HOME` to a temp dir before the worker module is
  imported, explicit `now=` for the marker TTL, no `skipif`).
- `docs/TEST_STABILITY.md` — the audit: method, the six runs, every differing test and its
  root cause, and the standing rules that keep the suite order-independent.
- `PLAN-FB42.md` — this file.

## Risks

- `deploy/hive_worker.py` resolves `~` at import time; a test that imports it without
  redirecting `HOME` reads and writes the operator's real `~/.config/hive-worker`.
- Rebinding the module's `os` must not hide anything `build_busy` calls; the proxy
  forwards every name except `/proc` listing, `/proc/<pid>` stat and `/proc/<pid>/cmdline`.
- The flakiness audit runs the suite six times; a test that fails only under shuffle must
  be fixed (own commit) or `xfail(strict=False)` with the reason written down, never
  silently skipped.

## Steps

1. Hermetic tests + proof (20 runs with and without a pipeline-named background process).
2. Full integrate-line suite 3x in file order, 3x shuffled.
3. Root-cause and fix (or pin) each differing test.
4. `docs/TEST_STABILITY.md`, ruff, structured review, push, ls-remote, summary.
