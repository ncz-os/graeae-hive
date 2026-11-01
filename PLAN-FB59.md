# PLAN — FB59: repo catalog generator (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `0f3a5a7`. Branch `freebuff/fb59-20261030`.

## Survey

- Generator: `hive/registry.py::build_registry` (5 live sources + overlay merge
  + clone-recipe fill + id-sorted output) via `scripts/build_repo_registry.py`
  (canonical `json.dumps(..., indent=2, sort_keys=True)` write + overlay-only report).
- Bus knowledge: `hive/repos.py::load_catalog` reads `data/repos.json`
  (+ ARGONAS-live fallback for post-build repos).
- Existing `test_repo_registry.py`: loader + correlate/merge-overlay + shipped
  shape tests. No overlap: FB59 tests determinism (hash-seed subprocesses),
  no-drop across sources, missing-remote flagging/resilience, bus round-trip,
  and the file↔generator drift check.

## Tests (`test_repo_catalog_generator_20261031.py`; no network, no creds)

All live sources mocked (`mock.patch` on `hive.registry.discover_*`); overlay
via temp file; `HOME` pointed at temp dir in subprocesses so no real
`~/.api_keys_master.json` is read.

1. **deterministic**: two `build_registry` runs in subprocesses with
   `PYTHONHASHSEED=0` vs `1` over the same mocked inputs → byte-identical
   canonical JSON. (Mocks live in a `sitecustomize`-style helper? No — simpler:
   run with all live sources skipped + a fixed temp overlay; the only inputs
   are the overlay file + code. Plus an in-process double-build over mocked
   discover outputs containing sets/dict-ordering hazards.)
2. **no-drop**: each source contributes a unique slug (gitlab-only,
   github-only, argonas-only via `discover_argonas` mock, buildkite-only,
   overlay-only) → all five present with correct `sources` tags.
3. **missing remote flagged**: (a) overlay-only entries carry
   `sources==["overlay"]` and NO clone recipe (identifiable, not silently
   healthy); (b) an unreachable source (mocked `discover_gitlab` raising /
   empty + no creds) still yields a successful build of the remaining inputs —
   resilience, gap visible as overlay-only.
4. **bus round-trip**: generated registry → temp file → `load_catalog` has
   every id → `find_exact` resolves each (generator output is bus knowledge).
5. **drift**: (a) shipped `data/repos.json` is canonical
   (load + re-dump `indent=2, sort_keys=True` == file bytes — a hand-edit
   without canonical form fails); `_meta.count == len(repos)`, repos sorted by
   id. (b) overlay↔file agreement: `build_registry(skip=all five)` over the
   SHIPPED overlay must reproduce every overlay slug + curated fields in the
   shipped file — editing the overlay without rebuilding fails the check.

## Risks

- `get_settings` caching: call `cache_clear()` (guarded) around env mutation.
- `ARGONAS_SSH_HOST` may be set in this shell — clear it (and creds env) in tests.
- `merge_overlay` mutates the passed entries dict — pass fresh copies per build
  in determinism comparisons (or compare across processes only).
- Full suite ~150 s; file first, then full suite once.

## Continuation 2026-10-31 (Linux/HYDRA): the NoDrop failures were a host leak, not ordering

Merged with `deploy/integrate-20261017` the Linux build host failed
`NoDropTests` on both tests; integrate alone passed, and two macOS sessions
could not reproduce. Cause: `_build` cleared the credential ENV but
`build_registry` falls back to `_keys()` (`~/.api_keys_master.json`) and to
`get_settings()` (`lru_cache`, read on first use — `conftest` had already
imported doctor). Only a source **with** a credential is discovered; on a box
where that fallback is empty, gitlab/github/buildkite/R2 are silently skipped
and `g-only` comes back overlay-only, so the tag assertion fails. Not
`os.listdir`/glob order, case, clone path or a tracked file — discovery here is
mocked; the leak is in which sources are enabled.

Fix (test-only; generator behaviour unchanged): `_build` passes fixed fake
credentials explicitly, pins `_keys` and `regmod.get_settings`, and the two
NoDrop tests now assert each `discover_*` was actually called — a skipped
source can no longer pass/fail the tag check vacuously. Verified: base helper +
new assertions → 6 failed (incl. a `SimpleNamespace` R2-credential AttributeError,
same leak class); fixed test → 9 passed with an empty HOME and with the real one.
No `sorted()` added to `discover_r2`: that is the pinned emitted order, unchanged.
