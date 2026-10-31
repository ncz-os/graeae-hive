# PLAN — FB54: dependency graph semantics (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `f50f9d7`. Branch `freebuff/fb54-20261030`.

## Premise check

`depends_on` edges are write-once at submit (`JobCreate.depends_on`;
`JobUpdate` has no `depends_on` field, so no PATCH path can add one).
Submit validates: self-reference → 422, unknown ids → 422
(`hive/bus/routers/jobs.py:606-614`). Multi-node cycles are therefore
unconstructible through the API (the second edge's target must already
exist, and the first edge's target did not exist when the first job was
submitted). Repo-level cycles (inserted bypassing the API) block
forever — documented as a finding, tested at the gate level.

Claim gate: `_deps_all_done` fail-closed — every listed id must be
status='done'; missing rows, malformed values, non-arrays all block
(`hive/persistence/sqlite.py:1315-1337`, `_require_dependencies_done`,
in-transaction in `claim_job` + advisory pre-scan in the dequeue route).

Release: entering `failed`/`failed_completion`/`dead-letter`/`cancelled`
strips the id from queued/offered dependents in the same transaction
(`jobs.py:1831-1832`); purges release with `dep_status='purged'`
(`admin.py:205,265`, `app.py:445`); stale-cancel releases as
`cancelled` (`app.py:37`). `done` keeps the edge (provenance).
`needs-review` is neither done nor dead → dependent stays blocked.
Pre-existing purges are dropped at claim time (`drop_missing_dependencies`).

## Files

- `docs/JOB_DEPENDENCIES.md` (new): semantics with file:line citations +
  a normative status table delimited by
  `<!-- FB54-STATUS-TABLE-START -->` / `<!-- FB54-STATUS-TABLE-END -->`
  markers that the test parses.
- `test_job_dependencies_20261031.py` (new, repo root): table-driven test
  (each table row executed against a temp sqlite repo through the real
  `release_dependencies` + `claim_job` paths) plus cycle validation,
  deep-chain, and needs-review resolution tests. In-process `POST
  /v1/jobs/submit` (ASGITransport, temp DB) for the 422 cases.
- No `pyproject.toml` change: stdlib + pytest + httpx only.

## Risks

- Line-number citations drift: verified against this base; the table test
  asserts behaviour, not line numbers, so it survives refactors.
- needs-review → done transition in the test goes through the status
  update + release path the router uses (no release fires for
  needs-review; claim succeeds once done).
- Full-suite time ~150 s; run file first, then full suite once.
